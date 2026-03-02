use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::metadata::MetadataStore;

/// Shared state accessible by all scan task executors via `TaskContext::state()`.
///
/// Each bucket has its own `BucketScanState` containing the metadata store,
/// filesystem root, and a lazily-initialised scheduler handle. The `OnceLock`
/// breaks the circular dependency: the scheduler needs `ScanAppState` at build
/// time, but executors need access to the scheduler for submitting continuation
/// tasks.
pub struct ScanAppState {
    pub buckets: HashMap<String, BucketScanState>,
}

pub struct BucketScanState {
    pub metadata: MetadataStore,
    pub root: PathBuf,
    /// Set after the scheduler is built — executors use this to submit
    /// continuation tasks from within their `execute()` methods.
    pub scheduler: OnceLock<taskmill::Scheduler>,
}
