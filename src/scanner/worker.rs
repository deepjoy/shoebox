use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::scanner::backpressure::ScannerResources;
use crate::scanner::levels;
use crate::scanner::scheduler::{Priority, ScanJob, ScanLevel, ScanScheduler, MAX_RETRIES};
use crate::scanner::scope::ScanScope;

/// Run the scan worker loop — polls the scheduler for jobs and executes them.
///
/// The worker checks `token.is_cancelled()` between jobs and yields to the
/// runtime when paused by backpressure.
///
/// A brief startup grace period lets the API server accept initial requests
/// before background scans begin consuming I/O bandwidth.
pub async fn run_scan_workers(
    metadata: MetadataStore,
    root: PathBuf,
    scheduler: Arc<Mutex<ScanScheduler>>,
    resources: Arc<ScannerResources>,
    token: CancellationToken,
) {
    tracing::debug!("Scanner worker started");

    loop {
        // Get next job from scheduler, or wait for new work
        let job = loop {
            {
                let mut sched = scheduler.lock().await;
                if let Some(job) = sched.next_job() {
                    break job;
                }
            }
            // No pending work — poll with a short sleep, checking for shutdown
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("Scanner worker shutting down");
                    return;
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        };

        // Check backpressure
        if resources.should_pause(job.priority) {
            // Re-queue the job and wait
            {
                let mut sched = scheduler.lock().await;
                sched.fail(job.id); // Remove from active
                sched.schedule(job.clone()); // Re-queue
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        tracing::debug!(
            job_id = %job.id,
            priority = ?job.priority,
            target_level = ?job.target_level,
            "Executing scan job"
        );

        let result = execute_scan_job(
            &metadata,
            &root,
            &job.scope,
            job.target_level,
            job.is_continuation(),
            job.l2_cursor.as_deref(),
            job.l3_cursor.as_deref(),
            job.l3_bytes_per_sec,
        )
        .await;

        {
            let mut sched = scheduler.lock().await;
            match result {
                Ok((has_remaining, l2_cursor, l3_cursor, l3_bytes_per_sec)) => {
                    sched.complete(job.id);
                    if has_remaining {
                        tracing::debug!(
                            priority = ?job.priority,
                            target_level = ?job.target_level,
                            l2_cursor = ?l2_cursor,
                            l3_cursor = ?l3_cursor,
                            l3_bytes_per_sec = ?l3_bytes_per_sec,
                            "Batch limit reached, scheduling continuation job"
                        );
                        sched.schedule(ScanJob::new_continuation(
                            job.priority,
                            job.scope.clone(),
                            job.target_level,
                            l2_cursor,
                            l3_cursor,
                            l3_bytes_per_sec,
                        ));
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if e.is_retryable() && job.retry_count < MAX_RETRIES {
                        let mut retry_job = job.clone();
                        retry_job.retry_count += 1;
                        retry_job.last_error = Some(error_msg.clone());
                        retry_job.status = crate::scanner::scheduler::JobStatus::Pending;
                        let attempt = retry_job.retry_count;
                        sched.fail(job.id); // Remove from active set
                        let backoff = Duration::from_secs(1 << (attempt - 1)); // 1s, 2s, 4s
                        tracing::warn!(
                            job_id = %retry_job.id,
                            error = %error_msg,
                            attempt = attempt,
                            max_retries = MAX_RETRIES,
                            backoff_secs = backoff.as_secs(),
                            "Scan job failed (transient), retrying after backoff"
                        );
                        drop(sched); // Release lock during backoff sleep
                        tokio::time::sleep(backoff).await;
                        let mut sched = scheduler.lock().await;
                        sched.schedule(retry_job);
                    } else {
                        let mut failed_job = job.clone();
                        failed_job.status = crate::scanner::scheduler::JobStatus::Failed;
                        failed_job.last_error = Some(error_msg.clone());
                        sched.fail(job.id); // Remove from active set
                        sched.record_failure(failed_job);
                        tracing::error!(
                            job_id = %job.id,
                            error = %error_msg,
                            retry_count = job.retry_count,
                            "Scan job failed permanently"
                        );
                    }
                }
            }
        }

        // Check for shutdown between jobs
        if token.is_cancelled() {
            tracing::info!("Scanner worker shutting down");
            break;
        }

        // Yield to let other tasks run between consecutive jobs
        tokio::task::yield_now().await;
    }

    tracing::debug!("Scanner worker exited");
}

/// Process filesystem watch events — converts WatchEvents to scan jobs.
pub async fn run_watch_processor(
    metadata: MetadataStore,
    root: PathBuf,
    mut rx: tokio::sync::mpsc::Receiver<crate::scanner::watcher::WatchEvent>,
    scheduler: Arc<Mutex<ScanScheduler>>,
    token: CancellationToken,
) {
    use crate::scanner::watcher::WatchEvent;

    tracing::debug!("Watch processor started");

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("Watch processor shutting down");
                break;
            }
            event = rx.recv() => {
                let event = match event {
                    Some(e) => e,
                    None => {
                        tracing::debug!("Watch channel closed");
                        break;
                    }
                };

                match event {
                    WatchEvent::Changed(path) => {
                        if let Ok(rel) = path.strip_prefix(&root) {
                            let key = rel.to_string_lossy().to_string();
                            if !key.is_empty() {
                                tracing::debug!(key = %key, "Watcher: file changed");
                                // Check if the file actually changed before scheduling a rescan
                                let needs_scan = handle_file_changed(&metadata, &root, &key)
                                    .await
                                    .unwrap_or(false);
                                if needs_scan {
                                    let job = ScanJob::new(
                                        Priority::Reconcile,
                                        ScanScope::Files(vec![key.clone()]),
                                        ScanLevel::Content,
                                    );
                                    let mut sched = scheduler.lock().await;
                                    sched.schedule(job);
                                }
                            }
                        }
                    }
                    WatchEvent::Deleted(path) => {
                        if let Ok(rel) = path.strip_prefix(&root) {
                            let key = rel.to_string_lossy().to_string();
                            if !key.is_empty() {
                                tracing::debug!(key = %key, "Watcher: file deleted");
                                let _ = metadata.delete_object(&key).await;
                            }
                        }
                    }
                }
            }
        }
    }

    tracing::debug!("Watch processor exited");
}

/// Handle a changed file: ensure it's in the DB and check if rescan is needed.
///
/// Returns `true` if a scan job should be scheduled (new file or actual change),
/// `false` if the file hasn't changed (e.g. watcher fired due to atime update
/// from the scanner's own reads).
async fn handle_file_changed(
    metadata: &MetadataStore,
    root: &std::path::Path,
    key: &str,
) -> Result<bool, S3Error> {
    let path = root.join(key);
    if !path.exists() {
        return Ok(false);
    }

    let fs_meta = tokio::fs::symlink_metadata(&path).await?;

    // Check if already tracked
    if let Some(obj) = metadata.get_object(key).await? {
        // Compare current mtime and size with stored values to detect real changes.
        // The watcher can fire spuriously (e.g. atime updates from scanner reads),
        // so we only reset scan_level when the file content may have changed.
        let current_mtime: Option<time::OffsetDateTime> =
            fs_meta.modified().ok().map(time::OffsetDateTime::from);
        let current_size = fs_meta.len() as i64;

        if obj.file_mtime == current_mtime && obj.size == Some(current_size) {
            return Ok(false);
        }

        // File actually changed — mark for rescan
        metadata.reset_scan_level(key, 1).await?;
        return Ok(true);
    }

    // New file — insert record
    let is_symlink = fs_meta.file_type().is_symlink();
    let size = Some(fs_meta.len() as i64);

    let parent = key
        .rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .unwrap_or_default();

    let symlink_target = if is_symlink {
        std::fs::read_link(&path)
            .ok()
            .map(|t| t.to_string_lossy().to_string())
    } else {
        None
    };

    let content_type = mime_guess::from_path(key)
        .first_or_octet_stream()
        .to_string();

    let now = time::OffsetDateTime::now_utc();
    let obj = crate::metadata::sqlite::ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        key: key.to_string(),
        parent_directory: parent,
        is_directory: false,
        is_symlink,
        symlink_target,
        size,
        content_type: Some(content_type),
        scan_level: 1,
        last_modified: now,
        created_at: now,
        ..Default::default()
    };

    metadata.insert_object(&obj).await?;
    Ok(true)
}

const L2_BATCH_LIMIT: i64 = 10_000;

/// Maximum L3 hashing concurrency (file count processed in parallel).
const L3_MAX_CONCURRENCY: usize = 32;

/// Seed byte budget for the first L3 batch (50 MB). Small enough to finish
/// quickly and calibrate throughput.
const L3_SEED_BYTES: i64 = 50 * 1024 * 1024;

/// Target wall-clock time per L3 batch (2 minutes).
const L3_TARGET_SECS: f64 = 120.0;

/// Upper bound on L3 byte budget to avoid scheduling enormous batches on very
/// fast storage.
const L3_MAX_BUDGET: i64 = 50 * 1024 * 1024 * 1024;

/// Execute a single scan job — runs L1, then optionally L2 and L3 up to target level.
///
/// Returns `(has_remaining, l2_cursor, l3_cursor, l3_bytes_per_sec)`. When
/// `has_remaining` is true the caller should schedule a continuation job
/// carrying the cursors and throughput estimate so the next batch can skip
/// directly to unprocessed work via keyset pagination and size its byte budget
/// appropriately.
///
/// L2 and L3 use independent cursors so that each level advances through the
/// keyspace at its own pace. Without this, L3 would inherit L2's cursor and
/// skip files that L2 just promoted to level 2 but that still need L3 hashing.
#[allow(clippy::too_many_arguments)]
async fn execute_scan_job(
    metadata: &MetadataStore,
    root: &Path,
    scope: &ScanScope,
    target_level: ScanLevel,
    is_continuation: bool,
    l2_cursor: Option<&str>,
    l3_cursor: Option<&str>,
    l3_bytes_per_sec: Option<f64>,
) -> Result<(bool, Option<String>, Option<String>, Option<f64>), S3Error> {
    // L1 discovery is always needed for non-Files scopes.
    // Only run on the first batch — subsequent continuation jobs skip L1
    // since discovery is already complete.
    if !is_continuation {
        match scope {
            ScanScope::Files(_) => {}
            _ => {
                levels::scan_l1(metadata, root, scope).await?;
            }
        }
    }

    let mut has_remaining = false;
    let mut new_l2_cursor: Option<String> = l2_cursor.map(|s| s.to_string());
    let mut new_l3_cursor: Option<String> = l3_cursor.map(|s| s.to_string());
    let mut new_l3_bytes_per_sec: Option<f64> = l3_bytes_per_sec;

    if target_level.as_i32() >= ScanLevel::Metadata.as_i32() {
        let keys = match scope {
            ScanScope::Files(keys) => keys.clone(),
            _ => {
                let keys = metadata
                    .list_keys_below_scan_level(2, L2_BATCH_LIMIT, l2_cursor)
                    .await?;
                if keys.len() as i64 >= L2_BATCH_LIMIT {
                    has_remaining = true;
                }
                keys
            }
        };
        if let Some(k) = keys.last() {
            new_l2_cursor = Some(k.clone());
        }
        if !keys.is_empty() {
            levels::scan_l2(metadata, root, &keys).await?;
        }
    }

    if target_level.as_i32() >= ScanLevel::Content.as_i32() {
        let (keys, concurrency) = match scope {
            ScanScope::Files(keys) => {
                let c = keys.len().clamp(1, L3_MAX_CONCURRENCY);
                (keys.clone(), c)
            }
            _ => {
                // Compute byte budget from previous throughput or use seed
                let byte_budget = match l3_bytes_per_sec {
                    Some(rate) => ((rate * L3_TARGET_SECS) as i64).min(L3_MAX_BUDGET),
                    None => L3_SEED_BYTES,
                };

                tracing::info!(
                    byte_budget = levels::format_human_size(byte_budget as u64),
                    throughput = l3_bytes_per_sec.map(|r| levels::format_human_size(r as u64)),
                    "L3 batch byte budget"
                );

                let (selected, exhausted, selected_bytes) = metadata
                    .list_keys_by_byte_budget(3, byte_budget, l3_cursor)
                    .await?;
                if !exhausted {
                    has_remaining = true;
                }

                // Compute concurrency from average file size of *actually selected*
                // files, not the byte budget. When remaining data is smaller than
                // the budget the two diverge significantly.
                let avg_size = selected_bytes as usize / selected.len().max(1);
                let c = match avg_size {
                    0..=524_288 => 32,          // ≤ 500 KB
                    524_289..=1_048_576 => 16,  // 500 KB – 1 MB
                    1_048_577..=8_388_608 => 8, // 1 MB – 8 MB
                    _ => 1,                     // > 8 MB
                }
                .min(L3_MAX_CONCURRENCY);

                (selected, c)
            }
        };
        if let Some(k) = keys.last() {
            new_l3_cursor = Some(k.clone());
        }
        if !keys.is_empty() {
            let batch_files = keys.len();
            let l3_start = std::time::Instant::now();
            let report = levels::scan_l3(metadata, root, &keys, concurrency).await?;
            let elapsed = l3_start.elapsed().as_secs_f64();

            // Use total files attempted (hashed + skipped) to account for
            // wall-clock time spent on files that were skipped (not found,
            // modified during scan, etc.). The byte estimate for skipped files
            // comes from the DB sizes we selected, so we use the full batch
            // count to compute throughput.
            let total_attempted = report.hashed + report.skipped;
            if elapsed > 0.0 && total_attempted > 0 {
                // Estimate total bytes attempted: scale report.bytes by the
                // ratio of total files to successfully hashed files.
                let estimated_bytes = if report.hashed > 0 {
                    (report.bytes as f64) * (total_attempted as f64 / report.hashed as f64)
                } else {
                    // All files skipped — use a minimal estimate so we don't
                    // divide by zero. The next batch will recalibrate.
                    report.bytes as f64
                };
                let measured_rate = estimated_bytes / elapsed;
                // Use EWMA to smooth throughput estimates across batches.
                // The first batch sets the baseline; subsequent batches
                // blend 30% new measurement with 70% previous estimate to
                // dampen oscillations from cache effects, file size skew,
                // and other I/O variability.
                const EWMA_ALPHA: f64 = 0.3;
                new_l3_bytes_per_sec = Some(match l3_bytes_per_sec {
                    Some(prev) => EWMA_ALPHA * measured_rate + (1.0 - EWMA_ALPHA) * prev,
                    None => measured_rate,
                });
                tracing::info!(
                    files = batch_files,
                    hashed = report.hashed,
                    skipped = report.skipped,
                    bytes = levels::format_human_size(report.bytes),
                    elapsed_secs = format_args!("{:.1}", elapsed),
                    throughput = levels::format_human_size(measured_rate as u64),
                    smoothed = levels::format_human_size(new_l3_bytes_per_sec.unwrap() as u64),
                    "L3 batch throughput"
                );
            }
        }
    }

    // Log remaining work for non-Files scopes when there's more to do
    if has_remaining && !matches!(scope, ScanScope::Files(_)) {
        log_scan_remaining(metadata, target_level).await;
    }

    Ok((
        has_remaining,
        new_l2_cursor,
        new_l3_cursor,
        new_l3_bytes_per_sec,
    ))
}

/// Log the number of files and bytes remaining for L2 and L3 scans.
async fn log_scan_remaining(metadata: &MetadataStore, target_level: ScanLevel) {
    if target_level.as_i32() >= ScanLevel::Metadata.as_i32() {
        if let Ok((files, bytes)) = metadata.count_remaining_below_scan_level(2).await {
            if files > 0 {
                tracing::info!(
                    remaining_files = files,
                    remaining_bytes = levels::format_human_size(bytes as u64),
                    "Scan progress: L2 metadata"
                );
            }
        }
    }
    if target_level.as_i32() >= ScanLevel::Content.as_i32() {
        if let Ok((files, bytes)) = metadata.count_remaining_below_scan_level(3).await {
            if files > 0 {
                tracing::info!(
                    remaining_files = files,
                    remaining_bytes = levels::format_human_size(bytes as u64),
                    "Scan progress: L3 content-hash"
                );
            }
        }
    }
}
