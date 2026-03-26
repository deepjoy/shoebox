use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use crate::metadata::sqlite::L1WriteOp;
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

    /// Write channel set by `ScanL1Executor::execute()` for the duration of a
    /// bucket-wide L1 scan.  Each `ScanL1DirExecutor` clones the sender and
    /// sends its `L1WriteOp` here; a single writer task drains the channel and
    /// commits in large batches.  `finalize()` takes (drops) the stored sender
    /// to close the channel, then awaits `l1_write_done`.
    pub l1_write_tx: tokio::sync::Mutex<Option<tokio::sync::mpsc::Sender<L1WriteOp>>>,

    /// Resolved when the batch writer has flushed all pending ops after the
    /// channel closes.
    pub l1_write_done: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
