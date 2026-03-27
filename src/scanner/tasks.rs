use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use taskmill::{DomainKey, DomainTaskContext, TaskError, TypedExecutor, TypedTask};

use crate::metadata::sqlite::L1WriteOp;
use crate::scanner::app_state::ScanAppState;
use crate::scanner::levels;
use crate::scanner::scope::ScanScope;

// ── Domain key ──────────────────────────────────────────────────────

pub struct Scanner;

impl DomainKey for Scanner {
    const NAME: &'static str = "scanner";
}

// ── Constants ────────────────────────────────────────────────────────

/// Maximum keys per L2 batch before scheduling a continuation.
const L2_BATCH_LIMIT: i64 = 10_000;

/// Maximum L3 hashing concurrency (file count processed in parallel).
const L3_MAX_CONCURRENCY: usize = 32;

/// Seed byte budget for the first L3 batch (50 MB).
const L3_SEED_BYTES: i64 = 50 * 1024 * 1024;

/// Target wall-clock time per L3 batch (2 minutes).
const L3_TARGET_SECS: f64 = 120.0;

/// Upper bound on L3 byte budget.
const L3_MAX_BUDGET: i64 = 50 * 1024 * 1024 * 1024;

/// EWMA smoothing factor for L3 throughput estimates.
const EWMA_ALPHA: f64 = 0.3;

// ── Task types ───────────────────────────────────────────────────────

/// Memo persisted by `ScanL1Executor::execute()` and received by `finalize()`.
///
/// Holds the epoch-nanosecond scan start time so post-scan reconciliation
/// can distinguish newly discovered objects from pre-existing ones.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL1Memo {
    pub scan_start_ns: i64,
}

/// L1: Discover files on disk and insert new records into the metadata DB.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL1Task {
    pub bucket: String,
    pub scope: ScanScope,
    pub target_level: i32,
}

impl TypedTask for ScanL1Task {
    type Domain = Scanner;
    const TASK_TYPE: &'static str = "scan-l1";
}

/// L1 directory task: scan one directory and enqueue its subdirectories.
///
/// Submitted by `ScanL1Executor` (root dir via `spawn_child_with`) and by
/// `ScanL1DirExecutor` itself (subdirs via `spawn_sibling_with`).  The `key()`
/// implementation enables dedup — duplicate submissions for the same directory
/// return `Upgraded`, `Requeued`, or `Duplicate` instead of creating redundant work.
///
/// API handlers may also submit this task at `Priority::REALTIME` to trigger
/// an on-demand scan before serving a listing.  Tasks without a `parent_id`
/// scan one directory without enqueuing children (standalone mode).
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL1DirTask {
    pub bucket: String,
    /// Directory prefix to scan (e.g. `""` for root, `"photos/2024/"` for nested).
    pub dir_prefix: String,
    pub scope: ScanScope,
}

impl TypedTask for ScanL1DirTask {
    type Domain = Scanner;
    const TASK_TYPE: &'static str = "scan-l1-dir";

    fn key(&self) -> Option<String> {
        Some(format!("{}:{}", self.bucket, self.dir_prefix))
    }
}

/// Soft deadline: target wall-clock duration per BFS chunk.  The chunk
/// yields after this duration PROVIDED the remaining frontier fits in a
/// single continuation payload.
const CHUNK_TARGET: Duration = Duration::from_secs(1);

/// Hard deadline: maximum wall-clock duration per chunk.  If the frontier
/// still exceeds one payload at this point, the chunk yields and splits
/// (rare — only for extremely wide trees).  Bounds worst-case preemption
/// latency for REALTIME API tasks.
const CHUNK_HARD_LIMIT: Duration = Duration::from_secs(5);

/// Byte budget for the `queue` array in each continuation chunk.
/// 800 KB leaves ~200 KB headroom for the `bucket`, `scope`, and JSON
/// framing within taskmill's 1 MB payload limit.
const CHUNK_QUEUE_BYTE_BUDGET: usize = 800_000;

/// A time-boxed BFS chunk.  Scans directories from `queue` for up to
/// ~1 second, then spawns a continuation chunk with the remaining frontier.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL1ChunkTask {
    pub bucket: String,
    /// BFS frontier: directory prefixes to scan in this chunk and beyond.
    pub queue: Vec<String>,
    pub scope: ScanScope,
}

impl TypedTask for ScanL1ChunkTask {
    type Domain = Scanner;
    const TASK_TYPE: &'static str = "scan-l1-chunk";
    // No dedup key — chunks are spawned in a controlled sequence by the
    // orchestrator / previous chunk.  External code never submits these.
}

/// L2: Collect filesystem metadata (size, mtime, ctime, inode) for objects
/// that haven't reached scan_level 2 yet.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL2Task {
    pub bucket: String,
    pub cursor: Option<String>,
}

impl TypedTask for ScanL2Task {
    type Domain = Scanner;
    const TASK_TYPE: &'static str = "scan-l2";
}

/// L3: Read files and compute content hashes (MD5 + SHA-256).
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL3Task {
    pub bucket: String,
    pub cursor: Option<String>,
    pub bytes_per_sec: Option<f64>,
}

impl TypedTask for ScanL3Task {
    type Domain = Scanner;
    const TASK_TYPE: &'static str = "scan-l3";
}

// ── Executors ────────────────────────────────────────────────────────

pub struct ScanL1Executor;

/// `ScanL1Task` orchestrator: records scan_start_ns, spawns the root
/// `ScanL1ChunkTask` as a child, then waits.  When all chunk children
/// settle, `finalize()` runs orphan cleanup, schedules L2/L3, and marks
/// the scan as complete.
impl TypedExecutor<ScanL1Task, ScanL1Memo> for ScanL1Executor {
    async fn execute(
        &self,
        task: ScanL1Task,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<ScanL1Memo, TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| {
            TaskError::new(format!(
                "bucket '{}' not found in ScanAppState",
                task.bucket
            ))
        })?;

        // Handle Files scope directly (no BFS needed)
        if let ScanScope::Files(_) = &task.scope {
            bucket_state.l1_running.store(true, Ordering::Release);
            let result =
                levels::scan_l1(&bucket_state.metadata, &bucket_state.root, &task.scope).await;
            bucket_state.l1_running.store(false, Ordering::Release);
            match &result {
                Ok(_) => {}
                Err(e) if !e.is_retryable() => {
                    bucket_state.l1_failed.store(true, Ordering::Release);
                    app.l1_notify.notify_waiters();
                }
                Err(_) => {}
            }
            result.map_err(|e| {
                if e.is_retryable() {
                    TaskError::retryable(format!("L1 scan failed: {e}"))
                } else {
                    TaskError::new(format!("L1 scan failed: {e}"))
                }
            })?;
            // For Files scope there are no children, so finalize() runs immediately.
            let scan_start_ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
            return Ok(ScanL1Memo { scan_start_ns });
        }

        if matches!(task.scope, ScanScope::Bucket) {
            bucket_state.l1_running.store(true, Ordering::Release);
        }

        let scan_start_ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;

        // Derive root dir_prefix from scope
        let root_dir_prefix = match &task.scope {
            ScanScope::Bucket => String::new(),
            ScanScope::Subtree { prefix } => {
                // Normalise to a directory prefix ending in '/'
                if prefix.ends_with('/') {
                    prefix.clone()
                } else {
                    // e.g. "photos/2024" → "photos/2024/"
                    format!("{prefix}/")
                }
            }
            ScanScope::Files(_) => unreachable!("handled above"),
        };

        // Set up the write channel: a single writer task drains L1WriteOps from
        // all concurrent ScanL1DirExecutors and commits them in large batches,
        // eliminating per-directory write-transaction contention on SQLite.
        let (write_tx, write_rx) = tokio::sync::mpsc::channel::<L1WriteOp>(4096);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = bucket_state.l1_write_tx.lock().await;
            *guard = Some(write_tx);
            let mut done_guard = bucket_state.l1_write_done.lock().await;
            *done_guard = Some(done_rx);
        }
        let metadata_for_writer = bucket_state.metadata.clone();
        tokio::spawn(async move {
            run_l1_batch_writer(write_rx, metadata_for_writer).await;
            let _ = done_tx.send(());
        });

        // Initialize the chunk counter (1 = the root chunk we're about to spawn).
        bucket_state
            .l1_chunks_remaining
            .store(1, Ordering::Release);

        // Spawn the root BFS chunk as a direct child of this orchestrator.
        // The chunk will scan directories for ~1 second, then spawn continuation
        // chunks as siblings (inheriting this task as their parent).
        ctx.spawn_child_with(ScanL1ChunkTask {
            bucket: task.bucket.clone(),
            queue: vec![root_dir_prefix],
            scope: task.scope.clone(),
        })
        .await
        .map_err(|e| TaskError::new(format!("spawn root chunk task failed: {e}")))?;

        // Spawn a lightweight progress reporter that logs periodically on
        // chunk completions.  Throttled to every 10 chunks to avoid flooding
        // logs on warm scans where chunks complete in rapid succession.
        let orchestrator_id = ctx.record().id;
        let bucket_for_reporter = task.bucket.clone();
        let metadata = bucket_state.metadata.clone();
        let chunks_remaining = bucket_state.l1_chunks_remaining.clone();
        let mut event_stream = ctx.domain::<Scanner>().task_events::<ScanL1ChunkTask>();
        tokio::spawn(async move {
            let mut chunks_completed = 0u64;
            loop {
                match event_stream.recv().await {
                    Ok(taskmill::TaskEvent::Completed { record, .. }) => {
                        if record.parent_id == Some(orchestrator_id) {
                            chunks_completed += 1;
                            if chunks_completed.is_multiple_of(10) {
                                let remaining =
                                    chunks_remaining.load(Ordering::Acquire);
                                let discovered = metadata
                                    .count_objects_since(scan_start_ns)
                                    .await
                                    .unwrap_or(0);
                                tracing::info!(
                                    bucket = %bucket_for_reporter,
                                    chunks_completed,
                                    chunks_remaining = remaining,
                                    objects_discovered = discovered,
                                    "L1 scan progress"
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break, // channel closed or lagged
                }
            }
        });

        // Return memo — orchestrator enters Waiting state until all children settle.
        Ok(ScanL1Memo { scan_start_ns })
    }

    async fn finalize(
        &self,
        task: ScanL1Task,
        memo: ScanL1Memo,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| {
            TaskError::new(format!(
                "bucket '{}' not found in ScanAppState",
                task.bucket
            ))
        })?;

        let scan_start_ns = memo.scan_start_ns;

        // 1. Close the write channel (drop stored Sender) and wait for the batch
        //    writer to flush all pending ops.  By the time finalize() is called,
        //    all ScanL1DirExecutor tasks have completed and dropped their own
        //    Sender clones, so dropping the stored copy here closes the channel.
        {
            let mut guard = bucket_state.l1_write_tx.lock().await;
            *guard = None; // drop stored Sender → channel closes when last clone drops
        }
        let done_rx = {
            let mut guard = bucket_state.l1_write_done.lock().await;
            guard.take()
        };
        if let Some(rx) = done_rx {
            let _ = rx.await; // wait for writer to flush remaining ops
        }

        // 2. Cross-directory move reconciliation (no-op: stale deletion removes
        //    old entries before finalize runs; moves result in new object_ids).
        let _moved = bucket_state
            .metadata
            .l1_reconcile_moves(scan_start_ns)
            .await
            .map_err(|e| TaskError::new(format!("move reconciliation failed: {e}")))?;

        // 2. Orphan directory cleanup (bucket-wide scans only).
        let orphan_deleted = if matches!(task.scope, ScanScope::Bucket) {
            bucket_state
                .metadata
                .l1_cleanup_orphan_dirs(&bucket_state.root, scan_start_ns)
                .await
                .map_err(|e| TaskError::new(format!("orphan dir cleanup failed: {e}")))?
        } else {
            0
        };

        let discovered = bucket_state
            .metadata
            .count_objects_since(scan_start_ns)
            .await
            .map_err(|e| TaskError::new(format!("count_objects_since failed: {e}")))?;
        let deleted = bucket_state
            .metadata
            .count_deleted_during_scan(scan_start_ns)
            .await
            .map_err(|e| TaskError::new(format!("count_deleted_during_scan failed: {e}")))?;

        tracing::info!(discovered, deleted, orphan_deleted, "L1 scan complete");

        // 3. Mark scan as complete and notify waiters.
        if matches!(task.scope, ScanScope::Bucket) {
            bucket_state.l1_running.store(false, Ordering::Release);
            bucket_state
                .l1_completed_once
                .store(true, Ordering::Release);
            app.l1_notify.notify_waiters();
        }

        // 4. Schedule downstream L2/L3.
        let priority = ctx.record().priority;
        if task.target_level >= 2 {
            let _ = ctx
                .domain::<Scanner>()
                .submit_with(ScanL2Task {
                    bucket: task.bucket.clone(),
                    cursor: None,
                })
                .priority(priority)
                .await;
        }
        if task.target_level >= 3 {
            let _ = ctx
                .domain::<Scanner>()
                .submit_with(ScanL3Task {
                    bucket: task.bucket.clone(),
                    cursor: None,
                    bytes_per_sec: None,
                })
                .priority(priority)
                .await;
        }

        Ok(())
    }

    async fn on_cancel(
        &self,
        task: ScanL1Task,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;
        if let Some(bucket_state) = app.buckets.get(&task.bucket) {
            if matches!(task.scope, ScanScope::Bucket) {
                bucket_state.l1_running.store(false, Ordering::Release);
                bucket_state.l1_failed.store(true, Ordering::Release);
                app.l1_notify.notify_waiters();
            }
        }
        Ok(())
    }
}

pub struct ScanL1DirExecutor;

/// Execute a single-directory BFS scan task.
///
/// Reads the directory, upserts new/changed files, deletes stale entries, and
/// enqueues subdirectory tasks as siblings of the orchestrating `ScanL1Task`.
/// Tasks submitted without a `parent_id` (API on-demand scans) scan one
/// directory only and do not enqueue children.
impl TypedExecutor<ScanL1DirTask> for ScanL1DirExecutor {
    async fn execute(
        &self,
        task: ScanL1DirTask,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| {
            TaskError::new(format!(
                "bucket '{}' not found in ScanAppState",
                task.bucket
            ))
        })?;

        let scan = levels::scan_l1_dir(
            &bucket_state.metadata,
            &bucket_state.root,
            &task.dir_prefix,
            &task.scope,
        )
        .await
        .map_err(|e| {
            if e.is_retryable() {
                TaskError::retryable(format!("L1 dir scan failed: {e}"))
            } else {
                TaskError::new(format!("L1 dir scan failed: {e}"))
            }
        })?;

        tracing::debug!(
            dir = %task.dir_prefix,
            new = scan.new_count,
            stale = scan.stale_count,
            unchanged = scan.unchanged,
            subdirs = scan.child_dirs.len(),
            "L1 dir scan"
        );

        // Send the write op to the shared writer task (only when there is
        // something to write). Standalone API-submitted tasks (no parent_id)
        // also send here so their writes are batched the same way.
        if !scan.write_op.inserts.is_empty() || !scan.write_op.stale_names.is_empty() {
            let maybe_tx = bucket_state.l1_write_tx.lock().await.clone();
            if let Some(tx) = maybe_tx {
                // Channel is large (4096); send is only backpressure if the
                // writer can't keep up, which slows this worker gracefully.
                let _ = tx.send(scan.write_op).await;
            } else {
                // No channel (standalone mode or Files scope): write directly.
                if let Err(e) = bucket_state
                    .metadata
                    .l1_batch_write(&[scan.write_op])
                    .await
                {
                    tracing::warn!("L1 standalone write failed: {e}");
                }
            }
        }

        // Only enqueue children when this task is a BFS child (has a parent_id).
        // Standalone API-submitted tasks have no parent and scan one directory only.
        if ctx.record().parent_id.is_some() {
            for subdir in scan.child_dirs {
                let _ = ctx
                    .spawn_sibling_with(ScanL1DirTask {
                        bucket: task.bucket.clone(),
                        dir_prefix: subdir,
                        scope: task.scope.clone(),
                    })
                    .await;
            }
        }

        Ok(())
    }
}

pub struct ScanL1ChunkExecutor;

/// Execute a time-boxed BFS chunk: scan directories from the frontier,
/// then spawn a single continuation with the remaining frontier.
///
/// Two deadlines govern yielding:
/// - **Soft deadline** (`CHUNK_TARGET`, 1 s): yield only if the remaining
///   frontier fits in a single continuation payload.  This keeps the chunk
///   chain linear (one continuation per chunk) in the common case.
/// - **Hard deadline** (`CHUNK_HARD_LIMIT`, 5 s): yield unconditionally.
///   If the frontier still exceeds one payload (extremely wide trees), fall
///   back to byte-budget splitting.  This caps worst-case preemption latency.
impl TypedExecutor<ScanL1ChunkTask> for ScanL1ChunkExecutor {
    async fn execute(
        &self,
        task: ScanL1ChunkTask,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| {
            TaskError::new(format!(
                "bucket '{}' not found in ScanAppState",
                task.bucket
            ))
        })?;

        let ScanL1ChunkTask {
            bucket,
            queue: initial_queue,
            scope,
        } = task;
        let mut queue: VecDeque<String> = initial_queue.into();
        // Track estimated serialized byte size of the queue incrementally
        // so we can check the payload-fits condition without iterating.
        let mut queue_bytes: usize = queue.iter().map(|p| p.len() + 4).sum();
        let now = Instant::now();
        let soft_deadline = now + CHUNK_TARGET;
        let hard_deadline = now + CHUNK_HARD_LIMIT;
        let mut dirs_scanned: u64 = 0;
        let mut dirs_failed: u64 = 0;

        // Clone the write-channel sender once for the entire chunk rather
        // than re-acquiring the mutex on every directory iteration.
        let write_tx = bucket_state.l1_write_tx.lock().await.clone();

        while let Some(prefix) = queue.pop_front() {
            queue_bytes -= prefix.len() + 4;

            let scan = match levels::scan_l1_dir(
                &bucket_state.metadata,
                &bucket_state.root,
                &prefix,
                &scope,
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    // Log and skip — a single directory failure (e.g.
                    // permission denied) must not kill the entire chunk.
                    tracing::warn!(dir = %prefix, "L1 dir scan failed, skipping: {e}");
                    dirs_failed += 1;
                    continue;
                }
            };

            // Send write op to the shared batch writer (same channel as before).
            if !scan.write_op.inserts.is_empty() || !scan.write_op.stale_names.is_empty() {
                if let Some(ref tx) = write_tx {
                    let _ = tx.send(scan.write_op).await;
                }
            }

            // BFS expansion: discovered subdirs join the frontier.
            for dir in scan.child_dirs {
                queue_bytes += dir.len() + 4;
                queue.push_back(dir);
            }
            dirs_scanned += 1;

            // Yield decision — two-level deadline:
            if !queue.is_empty() {
                let now = Instant::now();
                if now >= hard_deadline {
                    break;
                }
                if now >= soft_deadline && queue_bytes <= CHUNK_QUEUE_BYTE_BUDGET {
                    break;
                }
            }
        }

        tracing::debug!(
            dirs_scanned,
            dirs_failed,
            remaining = queue.len(),
            queue_bytes,
            "L1 chunk complete"
        );

        // Spawn continuation(s) with remaining frontier and update the
        // chunks-remaining counter for progress reporting.
        let mut continuations_spawned = 0u64;
        if !queue.is_empty() {
            if queue_bytes <= CHUNK_QUEUE_BYTE_BUDGET {
                // Common case: single continuation (linear chain).
                ctx.spawn_sibling_with(ScanL1ChunkTask {
                    bucket: bucket.clone(),
                    queue: queue.into_iter().collect(),
                    scope: scope.clone(),
                })
                .await
                .map_err(|e| TaskError::new(format!("spawn continuation chunk: {e}")))?;
                continuations_spawned = 1;
            } else {
                // Rare fallback (hard deadline hit on extremely wide tree):
                // split into the minimum number of continuations that fit.
                let mut chunk_queue: Vec<String> = Vec::new();
                let mut chunk_bytes: usize = 0;

                for path in queue {
                    let entry_bytes = path.len() + 4;
                    if !chunk_queue.is_empty()
                        && chunk_bytes + entry_bytes > CHUNK_QUEUE_BYTE_BUDGET
                    {
                        ctx.spawn_sibling_with(ScanL1ChunkTask {
                            bucket: bucket.clone(),
                            queue: std::mem::take(&mut chunk_queue),
                            scope: scope.clone(),
                        })
                        .await
                        .map_err(|e| {
                            TaskError::new(format!("spawn continuation chunk: {e}"))
                        })?;
                        continuations_spawned += 1;
                        chunk_bytes = 0;
                    }
                    chunk_bytes += entry_bytes;
                    chunk_queue.push(path);
                }

                if !chunk_queue.is_empty() {
                    ctx.spawn_sibling_with(ScanL1ChunkTask {
                        bucket: bucket.clone(),
                        queue: chunk_queue,
                        scope: scope.clone(),
                    })
                    .await
                    .map_err(|e| TaskError::new(format!("spawn continuation chunk: {e}")))?;
                    continuations_spawned += 1;
                }
            }
        }

        // Update counter: +continuations spawned, −1 for this chunk completing.
        // A single fetch_add(spawned - 1) would underflow when spawned == 0,
        // so split into two operations.
        if continuations_spawned > 0 {
            bucket_state
                .l1_chunks_remaining
                .fetch_add(continuations_spawned, Ordering::Relaxed);
        }
        bucket_state
            .l1_chunks_remaining
            .fetch_sub(1, Ordering::Release);

        Ok(())
    }
}

pub struct ScanL2Executor;

impl TypedExecutor<ScanL2Task> for ScanL2Executor {
    async fn execute(
        &self,
        task: ScanL2Task,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app
            .buckets
            .get(&task.bucket)
            .ok_or_else(|| TaskError::new(format!("bucket '{}' not found", task.bucket)))?;

        let keys = bucket_state
            .metadata
            .list_keys_below_scan_level(2, L2_BATCH_LIMIT, task.cursor.as_deref())
            .await
            .map_err(|e| {
                if e.is_retryable() {
                    TaskError::retryable(format!("L2 key listing failed: {e}"))
                } else {
                    TaskError::new(format!("L2 key listing failed: {e}"))
                }
            })?;

        let has_remaining = keys.len() as i64 >= L2_BATCH_LIMIT;

        if !keys.is_empty() {
            levels::scan_l2(&bucket_state.metadata, &bucket_state.root, &keys)
                .await
                .map_err(|e| {
                    if e.is_retryable() {
                        TaskError::retryable(format!("L2 scan failed: {e}"))
                    } else {
                        TaskError::new(format!("L2 scan failed: {e}"))
                    }
                })?;
        }

        // Log remaining work.
        if has_remaining {
            if let Ok((files, bytes)) = bucket_state
                .metadata
                .count_remaining_below_scan_level(2)
                .await
            {
                if files > 0 {
                    tracing::info!(
                        remaining_files = files,
                        remaining_bytes = levels::format_human_size(bytes as u64),
                        "Scan progress: L2 metadata"
                    );
                }
            }
        }

        // Schedule continuation if more keys remain.
        if has_remaining {
            let _ = ctx
                .domain::<Scanner>()
                .submit_with(ScanL2Task {
                    bucket: task.bucket.clone(),
                    cursor: keys.last().cloned(),
                })
                .priority(ctx.record().priority)
                .await;
        }

        Ok(())
    }
}

pub struct ScanL3Executor;

impl TypedExecutor<ScanL3Task> for ScanL3Executor {
    async fn execute(
        &self,
        task: ScanL3Task,
        ctx: DomainTaskContext<'_, Scanner>,
    ) -> Result<(), TaskError> {
        let app = ctx
            .state::<ScanAppState>()
            .ok_or_else(|| TaskError::new("ScanAppState not set"))?;

        let bucket_state = app
            .buckets
            .get(&task.bucket)
            .ok_or_else(|| TaskError::new(format!("bucket '{}' not found", task.bucket)))?;

        // Compute byte budget from previous throughput or use seed.
        let byte_budget = match task.bytes_per_sec {
            Some(rate) => ((rate * L3_TARGET_SECS) as i64).min(L3_MAX_BUDGET),
            None => L3_SEED_BYTES,
        };

        tracing::info!(
            byte_budget = levels::format_human_size(byte_budget as u64),
            throughput = task
                .bytes_per_sec
                .map(|r| levels::format_human_size(r as u64)),
            "L3 batch byte budget"
        );

        let (keys, exhausted, selected_bytes) = bucket_state
            .metadata
            .list_keys_by_byte_budget(3, byte_budget, task.cursor.as_deref())
            .await
            .map_err(|e| {
                if e.is_retryable() {
                    TaskError::retryable(format!("L3 key listing failed: {e}"))
                } else {
                    TaskError::new(format!("L3 key listing failed: {e}"))
                }
            })?;

        let has_remaining = !exhausted;

        let mut new_bytes_per_sec = task.bytes_per_sec;

        if !keys.is_empty() {
            // Compute concurrency from average file size of selected files.
            let avg_size = selected_bytes as usize / keys.len().max(1);
            let concurrency = match avg_size {
                0..=524_288 => 32,
                524_289..=1_048_576 => 16,
                1_048_577..=8_388_608 => 8,
                _ => 1,
            }
            .min(L3_MAX_CONCURRENCY);

            let batch_files = keys.len();
            let l3_start = std::time::Instant::now();
            let report = levels::scan_l3(
                &bucket_state.metadata,
                &bucket_state.root,
                &keys,
                concurrency,
            )
            .await
            .map_err(|e| {
                if e.is_retryable() {
                    TaskError::retryable(format!("L3 scan failed: {e}"))
                } else {
                    TaskError::new(format!("L3 scan failed: {e}"))
                }
            })?;
            let elapsed = l3_start.elapsed().as_secs_f64();
            ctx.record_read_bytes(report.bytes as i64);

            let total_attempted = report.hashed + report.skipped;
            if elapsed > 0.0 && total_attempted > 0 {
                let estimated_bytes = if report.hashed > 0 {
                    (report.bytes as f64) * (total_attempted as f64 / report.hashed as f64)
                } else {
                    report.bytes as f64
                };
                let measured_rate = estimated_bytes / elapsed;
                new_bytes_per_sec = Some(match task.bytes_per_sec {
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
                    smoothed = levels::format_human_size(new_bytes_per_sec.unwrap() as u64),
                    "L3 batch throughput"
                );
            }
        }

        // Log remaining work.
        if has_remaining {
            log_scan_remaining(&bucket_state.metadata).await;
        }

        // Schedule continuation if more keys remain.
        if has_remaining {
            let _ = ctx
                .domain::<Scanner>()
                .submit_with(ScanL3Task {
                    bucket: task.bucket.clone(),
                    cursor: keys.last().cloned(),
                    bytes_per_sec: new_bytes_per_sec,
                })
                .priority(ctx.record().priority)
                .await;
        }

        Ok(())
    }
}

// ── L1 batch writer ───────────────────────────────────────────────────────────

/// Drain `L1WriteOp`s from the channel and commit them in large batches.
///
/// Runs as a single background task per bucket per L1 scan, eliminating the
/// write-lock contention that occurs when N taskmill workers each try to commit
/// their own per-directory transaction concurrently.
///
/// Flushes when the pending row count reaches `MAX_BATCH_ROWS` OR when
/// `FLUSH_INTERVAL` elapses without a new op — whichever comes first.  Exits
/// when the channel is closed (all dir-task senders have been dropped).
async fn run_l1_batch_writer(
    mut rx: tokio::sync::mpsc::Receiver<L1WriteOp>,
    metadata: crate::metadata::MetadataStore,
) {
    const MAX_BATCH_ROWS: usize = 2_000;
    const FLUSH_INTERVAL: Duration = Duration::from_millis(50);

    let mut pending: Vec<L1WriteOp> = Vec::new();
    let mut pending_rows: usize = 0;
    let mut flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;

    loop {
        tokio::select! {
            biased; // prefer draining the channel over the timeout
            msg = rx.recv() => {
                match msg {
                    Some(op) => {
                        pending_rows += op.inserts.len() + op.stale_names.len();
                        pending.push(op);
                        if pending_rows >= MAX_BATCH_ROWS {
                            l1_flush_batch(&metadata, &mut pending, &mut pending_rows).await;
                            flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                        }
                    }
                    None => break, // all Senders dropped → scan complete
                }
            }
            _ = tokio::time::sleep_until(flush_deadline) => {
                if !pending.is_empty() {
                    l1_flush_batch(&metadata, &mut pending, &mut pending_rows).await;
                }
                flush_deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
            }
        }
    }

    // Final flush for any ops that arrived after the last timed flush.
    if !pending.is_empty() {
        l1_flush_batch(&metadata, &mut pending, &mut pending_rows).await;
    }
}

async fn l1_flush_batch(
    metadata: &crate::metadata::MetadataStore,
    pending: &mut Vec<L1WriteOp>,
    pending_rows: &mut usize,
) {
    let ops = std::mem::take(pending);
    *pending_rows = 0;
    let total_inserts: usize = ops.iter().map(|o| o.inserts.len()).sum();
    let total_deletes: usize = ops.iter().map(|o| o.stale_names.len()).sum();
    if let Err(e) = metadata.l1_batch_write(&ops).await {
        tracing::error!(
            dirs = ops.len(),
            inserts = total_inserts,
            deletes = total_deletes,
            "L1 batch write failed: {e}"
        );
    } else {
        tracing::debug!(
            dirs = ops.len(),
            inserts = total_inserts,
            deletes = total_deletes,
            "L1 batch write committed"
        );
    }
}

/// Log the number of files and bytes remaining for L2 and L3 scans.
async fn log_scan_remaining(metadata: &crate::metadata::MetadataStore) {
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
