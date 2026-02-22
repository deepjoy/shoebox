use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::scanner::backpressure::ScannerResources;
use crate::scanner::levels;
use crate::scanner::scheduler::{ScanLevel, ScanScheduler};
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
