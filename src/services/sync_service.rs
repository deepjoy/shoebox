use crate::error::S3Error;
use crate::scanner::scope::ScanScope;
use crate::scanner::tasks::{ScanL1Task, ScanL2Task, Scanner};

/// Sync reconciles the SQLite metadata with the current filesystem state
/// by submitting L1 + L2 scan tasks to TaskMill at elevated priorities.
///
/// L1 runs at HIGH priority (preempts running background work).
/// L2 runs at NORMAL priority (runs before BACKGROUND tasks).
/// L3 (content hashing) is NOT triggered — it runs in the background via
/// taskmill at its own pace.
///
/// Sync is always async — it returns immediately.
pub async fn sync(scheduler: &taskmill::Scheduler, bucket: &str) -> Result<(), S3Error> {
    let scanner = scheduler.domain::<Scanner>();

    // Submit L1 at HIGH — preempts any running background scan.
    scanner
        .submit_with(ScanL1Task {
            bucket: bucket.to_string(),
            scope: ScanScope::Bucket,
            target_level: 2,
        })
        .priority(taskmill::Priority::HIGH)
        .await
        .map_err(|_| S3Error::InternalError)?;

    // Also submit L2 at NORMAL priority.
    scanner
        .submit_with(ScanL2Task {
            bucket: bucket.to_string(),
            cursor: None,
        })
        .priority(taskmill::Priority::NORMAL)
        .await
        .map_err(|_| S3Error::InternalError)?;

    Ok(())
}
