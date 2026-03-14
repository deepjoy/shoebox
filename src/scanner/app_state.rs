use std::collections::HashMap;
use std::path::PathBuf;

use crate::metadata::MetadataStore;

/// Shared state accessible by all scan task executors via `TaskContext::state()`.
///
/// Each bucket has its own `BucketScanState` containing the metadata store
/// and the filesystem root. Executors submit follow-up tasks via
/// `ctx.submit_typed_at()` on the [`TaskContext`](taskmill::TaskContext).
pub struct ScanAppState {
    pub buckets: HashMap<String, BucketScanState>,
}

pub struct BucketScanState {
    pub metadata: MetadataStore,
    pub root: PathBuf,
}
