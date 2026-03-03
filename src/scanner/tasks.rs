use serde::{Deserialize, Serialize};
use taskmill::{TaskContext, TaskError, TaskExecutor, TaskResult, TypedTask};

use crate::scanner::app_state::ScanAppState;
use crate::scanner::levels;
use crate::scanner::scope::ScanScope;

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

/// L1: Discover files on disk and insert new records into the metadata DB.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL1Task {
    pub bucket: String,
    pub scope: ScanScope,
    pub target_level: i32,
}

impl TypedTask for ScanL1Task {
    const TASK_TYPE: &'static str = "scan-l1";
}

/// L2: Collect filesystem metadata (size, mtime, ctime, inode) for objects
/// that haven't reached scan_level 2 yet.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScanL2Task {
    pub bucket: String,
    pub cursor: Option<String>,
}

impl TypedTask for ScanL2Task {
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
    const TASK_TYPE: &'static str = "scan-l3";
}

// ── Executors ────────────────────────────────────────────────────────

pub struct ScanL1Executor;

impl TaskExecutor for ScanL1Executor {
    async fn execute<'a>(&'a self, ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
        let task: ScanL1Task = ctx
            .deserialize_typed()
            .map_err(|e| TaskError {
                message: format!("failed to deserialize ScanL1Task: {e}"),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?
            .ok_or_else(|| TaskError {
                message: "missing payload".into(),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        let app = ctx.state::<ScanAppState>().ok_or_else(|| TaskError {
            message: "ScanAppState not set".into(),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| TaskError {
            message: format!("bucket '{}' not found in ScanAppState", task.bucket),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        levels::scan_l1(&bucket_state.metadata, &bucket_state.root, &task.scope)
            .await
            .map_err(|e| TaskError {
                message: format!("L1 scan failed: {e}"),
                retryable: e.is_retryable(),
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        // Schedule downstream L2 (and optionally L3) if target_level warrants it.
        let scheduler = bucket_state.scheduler.get().ok_or_else(|| TaskError {
            message: "scheduler not initialised yet".into(),
            retryable: true,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        // Propagate the current task's priority to downstream scans.
        let priority = ctx.record.priority;

        if task.target_level >= 2 {
            let _ = scheduler
                .submit_typed_at(
                    &ScanL2Task {
                        bucket: task.bucket.clone(),
                        cursor: None,
                    },
                    priority,
                )
                .await;
        }
        if task.target_level >= 3 {
            let _ = scheduler
                .submit_typed_at(
                    &ScanL3Task {
                        bucket: task.bucket.clone(),
                        cursor: None,
                        bytes_per_sec: None,
                    },
                    priority,
                )
                .await;
        }

        Ok(TaskResult {
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })
    }
}

pub struct ScanL2Executor;

impl TaskExecutor for ScanL2Executor {
    async fn execute<'a>(&'a self, ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
        let task: ScanL2Task = ctx
            .deserialize_typed()
            .map_err(|e| TaskError {
                message: format!("failed to deserialize ScanL2Task: {e}"),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?
            .ok_or_else(|| TaskError {
                message: "missing payload".into(),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        let app = ctx.state::<ScanAppState>().ok_or_else(|| TaskError {
            message: "ScanAppState not set".into(),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| TaskError {
            message: format!("bucket '{}' not found", task.bucket),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        let keys = bucket_state
            .metadata
            .list_keys_below_scan_level(2, L2_BATCH_LIMIT, task.cursor.as_deref())
            .await
            .map_err(|e| TaskError {
                message: format!("L2 key listing failed: {e}"),
                retryable: e.is_retryable(),
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        let has_remaining = keys.len() as i64 >= L2_BATCH_LIMIT;

        if !keys.is_empty() {
            levels::scan_l2(&bucket_state.metadata, &bucket_state.root, &keys)
                .await
                .map_err(|e| TaskError {
                    message: format!("L2 scan failed: {e}"),
                    retryable: e.is_retryable(),
                    actual_read_bytes: 0,
                    actual_write_bytes: 0,
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
            if let Some(scheduler) = bucket_state.scheduler.get() {
                let _ = scheduler
                    .submit_typed_at(
                        &ScanL2Task {
                            bucket: task.bucket.clone(),
                            cursor: keys.last().cloned(),
                        },
                        ctx.record.priority,
                    )
                    .await;
            }
        }

        Ok(TaskResult {
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })
    }
}

pub struct ScanL3Executor;

impl TaskExecutor for ScanL3Executor {
    async fn execute<'a>(&'a self, ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
        let task: ScanL3Task = ctx
            .deserialize_typed()
            .map_err(|e| TaskError {
                message: format!("failed to deserialize ScanL3Task: {e}"),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?
            .ok_or_else(|| TaskError {
                message: "missing payload".into(),
                retryable: false,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        let app = ctx.state::<ScanAppState>().ok_or_else(|| TaskError {
            message: "ScanAppState not set".into(),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

        let bucket_state = app.buckets.get(&task.bucket).ok_or_else(|| TaskError {
            message: format!("bucket '{}' not found", task.bucket),
            retryable: false,
            actual_read_bytes: 0,
            actual_write_bytes: 0,
        })?;

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
            .map_err(|e| TaskError {
                message: format!("L3 key listing failed: {e}"),
                retryable: e.is_retryable(),
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;

        let has_remaining = !exhausted;

        let mut actual_read_bytes: i64 = 0;
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
            .map_err(|e| TaskError {
                message: format!("L3 scan failed: {e}"),
                retryable: e.is_retryable(),
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })?;
            let elapsed = l3_start.elapsed().as_secs_f64();
            actual_read_bytes = report.bytes as i64;

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
            if let Some(scheduler) = bucket_state.scheduler.get() {
                let _ = scheduler
                    .submit_typed_at(
                        &ScanL3Task {
                            bucket: task.bucket.clone(),
                            cursor: keys.last().cloned(),
                            bytes_per_sec: new_bytes_per_sec,
                        },
                        ctx.record.priority,
                    )
                    .await;
            }
        }

        Ok(TaskResult {
            actual_read_bytes,
            actual_write_bytes: 0,
        })
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
