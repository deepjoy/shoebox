use std::path::PathBuf;
use std::time::Duration;

use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebounceEventResult, DebouncedEvent};
use tokio::sync::mpsc;

use crate::config::SHOEBOX_DIR;

/// Filesystem change event (after debouncing).
///
/// `notify-debouncer-mini` does not distinguish create/modify — it only reports
/// that a path had activity. `Changed` means the file exists; `Deleted` means
/// the path no longer exists on disk.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    Changed(PathBuf),
    Deleted(PathBuf),
}

/// Watches a bucket root for filesystem changes.
///
/// Dropping the struct stops the inner `notify` watcher and closes the channel.
/// No `CancellationToken` needed — the watch processor exits when `rx.recv()`
/// returns `None`.
pub struct FilesystemWatcher {
    _debouncer: notify_debouncer_mini::Debouncer<notify::RecommendedWatcher>,
}

impl FilesystemWatcher {
    /// Start watching `root` recursively, sending debounced events to `tx`.
    ///
    /// The underlying `notify` watcher performs a blocking recursive directory
    /// traversal to register OS-level watches (inotify on Linux). Use
    /// [`spawn`] instead when calling from async context to avoid blocking
    /// the tokio runtime.
    pub fn new(root: PathBuf, tx: mpsc::Sender<WatchEvent>) -> Result<Self, notify::Error> {
        let mut debouncer = new_debouncer(
            Duration::from_millis(200),
            move |result: DebounceEventResult| {
                if let Ok(events) = result {
                    for event in events {
                        Self::handle_event(&event, &tx);
                    }
                }
            },
        )?;

        debouncer.watcher().watch(&root, RecursiveMode::Recursive)?;

        Ok(Self {
            _debouncer: debouncer,
        })
    }

    /// Async version of [`new`](Self::new) that runs the blocking watcher
    /// setup on a dedicated thread via `spawn_blocking`.
    pub async fn spawn(root: PathBuf, tx: mpsc::Sender<WatchEvent>) -> Result<Self, notify::Error> {
        tokio::task::spawn_blocking(move || Self::new(root, tx))
            .await
            .expect("watcher spawn_blocking panicked")
    }

    fn handle_event(event: &DebouncedEvent, tx: &mpsc::Sender<WatchEvent>) {
        // Filter out .shoebox directory
        if is_shoebox_path(&event.path) {
            return;
        }

        if event.path.exists() {
            // Only send events for files, not directories
            if event.path.is_file() || event.path.is_symlink() {
                tx.blocking_send(WatchEvent::Changed(event.path.clone()))
                    .ok();
            }
        } else {
            tx.blocking_send(WatchEvent::Deleted(event.path.clone()))
                .ok();
        }
    }
}

/// Check if a path is inside a .shoebox directory.
fn is_shoebox_path(path: &std::path::Path) -> bool {
    path.components()
        .any(|c| c.as_os_str().to_str().is_some_and(|s| s == SHOEBOX_DIR))
}
