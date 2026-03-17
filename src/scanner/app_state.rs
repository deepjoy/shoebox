use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use crate::metadata::MetadataStore;

/// Shared state accessible by all scan task executors via `TaskContext::state()`.
///
/// Each bucket has its own `BucketScanState` containing the metadata store
/// and the filesystem root. Executors submit follow-up tasks via
/// `ctx.submit_typed_at()` on the [`TaskContext`](taskmill::TaskContext).
pub struct ScanAppState {
    pub buckets: HashMap<String, BucketScanState>,
    /// Notified whenever any bucket's L1 scan completes (or permanently fails)
    /// for the first time. Used by `Shoebox::wait_for_initial_scan()`.
    pub l1_notify: tokio::sync::Notify,
}

pub struct BucketScanState {
    pub metadata: MetadataStore,
    pub root: PathBuf,
    /// `true` while a bucket-wide L1 scan is executing. Set before
    /// `scan_l1()` starts and cleared when it finishes.
    pub l1_running: AtomicBool,
    /// Set to `true` once the first bucket-wide L1 scan has completed.
    /// Used by `Shoebox::wait_for_initial_scan()`.
    pub l1_completed_once: AtomicBool,
    /// Set to `true` if L1 scan fails permanently (non-retryable error).
    /// Used by `Shoebox::wait_for_initial_scan()` to avoid hanging forever.
    pub l1_failed: AtomicBool,
}
