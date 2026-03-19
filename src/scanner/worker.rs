use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::scanner::scope::ScanScope;
use crate::scanner::tasks::{ScanL1Task, ScanL2Task, ScanL3Task, Scanner};

/// Process filesystem watch events — converts WatchEvents to taskmill tasks.
pub async fn run_watch_processor(
    metadata: MetadataStore,
    root: PathBuf,
    mut rx: tokio::sync::mpsc::Receiver<crate::scanner::watcher::WatchEvent>,
    scheduler: taskmill::Scheduler,
    bucket_name: String,
    drop_counter: Arc<AtomicU64>,
    token: CancellationToken,
) {
    use crate::scanner::watcher::WatchEvent;

    tracing::debug!("Watch processor started");

    let mut drop_check = tokio::time::interval(Duration::from_secs(10));
    drop_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("Watch processor shutting down");
                break;
            }
            _ = drop_check.tick() => {
                let dropped = drop_counter.swap(0, Ordering::Relaxed);
                if dropped > 0 {
                    tracing::warn!(
                        dropped_events = dropped,
                        "Watch channel overflow — scheduling full reconcile to catch missed files"
                    );
                    let _ = scheduler.domain::<Scanner>()
                        .submit_with(ScanL1Task {
                            bucket: bucket_name.clone(),
                            scope: ScanScope::Bucket,
                            target_level: 3,
                        })
                        .priority(taskmill::Priority::BACKGROUND)
                        .await;
                }
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
                                let needs_scan = handle_file_changed(&metadata, &root, &key)
                                    .await
                                    .unwrap_or(false);
                                if needs_scan {
                                    // Submit L2+L3 for the changed file.
                                    // The file is already in the DB at scan_level 1.
                                    let scanner = scheduler.domain::<Scanner>();
                                    let _ = scanner.submit(ScanL2Task {
                                        bucket: bucket_name.clone(),
                                        cursor: None,
                                    }).await;
                                    let _ = scanner.submit(ScanL3Task {
                                        bucket: bucket_name.clone(),
                                        cursor: None,
                                        bytes_per_sec: None,
                                    }).await;
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
        let current_mtime: Option<crate::metadata::sqlite::SqliteTimestamp> = fs_meta
            .modified()
            .ok()
            .map(|t| crate::metadata::sqlite::SqliteTimestamp(time::OffsetDateTime::from(t)));
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
    let dir_id = metadata.get_or_create_dir_id(&parent).await?;

    let symlink_target = if is_symlink {
        std::fs::read_link(&path)
            .ok()
            .map(|t| t.to_string_lossy().to_string())
    } else {
        None
    };

    let ct_mime = mime_guess::from_path(key)
        .first_or_octet_stream()
        .to_string();
    let ct_id = metadata.get_or_create_content_type_id(&ct_mime).await?;

    let (_, filename) = crate::metadata::sqlite::split_key(key);
    let now = crate::metadata::sqlite::SqliteTimestamp::now();
    let obj = crate::metadata::sqlite::ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        name: filename.to_string(),
        parent_dir_id: dir_id,
        key: key.to_string(),
        is_symlink,
        symlink_target,
        size,
        content_type_id: Some(ct_id),
        scan_level: 1,
        last_modified: now,
        created_at: now,
        ..Default::default()
    };

    metadata.insert_object(&obj).await?;
    Ok(true)
}
