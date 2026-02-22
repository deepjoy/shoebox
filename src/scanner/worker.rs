use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::scanner::backpressure::ScannerResources;
use crate::scanner::levels;
use crate::scanner::scheduler::{Priority, ScanLevel, ScanScheduler};
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
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("Scanner worker shutting down");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                // Poll for work
            }
        }

        // Get next job from scheduler
        let job = {
            let mut sched = scheduler.lock().await;
            sched.next_job()
        };

        let job = match job {
            Some(j) => j,
            None => continue,
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

        let result = execute_scan_job(&metadata, &root, &job.scope, job.target_level).await;

        {
            let mut sched = scheduler.lock().await;
            match result {
                Ok(()) => sched.complete(job.id),
                Err(e) => {
                    tracing::warn!(job_id = %job.id, error = %e, "Scan job failed");
                    sched.fail(job.id);
                }
            }
        }
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
    use crate::scanner::scheduler::ScanJob;
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

/// Execute a single scan job — runs L1, then optionally L2 and L3 up to target level.
async fn execute_scan_job(
    metadata: &MetadataStore,
    root: &Path,
    scope: &ScanScope,
    target_level: ScanLevel,
) -> Result<(), S3Error> {
    // L1 discovery is always needed for non-Files scopes
    match scope {
        ScanScope::Files(_) => {}
        _ => {
            levels::scan_l1(metadata, root, scope).await?;
        }
    }

    if target_level.as_i32() >= ScanLevel::Metadata.as_i32() {
        let keys = match scope {
            ScanScope::Files(keys) => keys.clone(),
            _ => metadata.list_keys_below_scan_level(2, 10000).await?,
        };
        if !keys.is_empty() {
            levels::scan_l2(metadata, root, &keys).await?;
        }
    }

    if target_level.as_i32() >= ScanLevel::Content.as_i32() {
        let keys = match scope {
            ScanScope::Files(keys) => keys.clone(),
            _ => metadata.list_keys_below_scan_level(3, 1000).await?,
        };
        if !keys.is_empty() {
            levels::scan_l3(metadata, root, &keys).await?;
        }
    }

    Ok(())
}
