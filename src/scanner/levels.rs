use std::path::Path;

use async_walkdir::{Filtering, WalkDir};
use futures::StreamExt;

use crate::config::SHOEBOX_DIR;
use crate::error::S3Error;
use crate::metadata::sqlite::{ObjectMetadataUpdate, ObjectRecord};
use crate::metadata::MetadataStore;
use crate::scanner::platform;
use crate::scanner::scope::ScanScope;

/// Maximum number of rows per batch transaction.
const BATCH_SIZE: usize = 1000;
/// Maximum time to hold a batch before flushing.
const BATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Result of an L1 (discovery) scan.
#[derive(Debug, Default)]
pub struct L1Report {
    pub discovered: u64,
    pub deleted: u64,
    pub unchanged: u64,
}

/// Result of an L2 (metadata) scan.
#[derive(Debug, Default)]
pub struct L2Report {
    pub updated: u64,
    pub errors: u64,
}

/// Check whether a path component represents the .shoebox directory.
fn is_shoebox_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name == SHOEBOX_DIR)
}

/// L1: Fast directory walk — discovers files on disk and inserts new records.
///
/// This is a free function per the library-first design principle. Both HTTP
/// handlers and `Shoebox` library methods can call it directly.
pub async fn scan_l1(
    metadata: &MetadataStore,
    root: &Path,
    scope: &ScanScope,
) -> Result<L1Report, S3Error> {
    let mut report = L1Report::default();
    let scan_start = std::time::Instant::now();

    // Load all known keys up front — one query instead of per-file lookups
    let db_keys: std::collections::HashSet<String> =
        metadata.get_all_keys().await?.into_iter().collect();
    tracing::info!(db_keys = db_keys.len(), "L1 loaded existing keys");

    // Collect all keys currently on disk within scope
    let mut disk_keys = std::collections::HashSet::new();
    let mut batch: Vec<ObjectRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut batch_start = std::time::Instant::now();
    let mut last_progress = std::time::Instant::now();

    let mut walker = WalkDir::new(root);
    walker = walker.filter(|entry| async move {
        if is_shoebox_dir(&entry.path()) {
            Filtering::IgnoreDir
        } else {
            Filtering::Continue
        }
    });

    while let Some(entry) = walker.next().await {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("L1 scan walkdir error: {e}");
                continue;
            }
        };

        let path = entry.path();

        // Skip the root directory itself
        if path == root {
            continue;
        }

        let key = match path.strip_prefix(root) {
            Ok(rel) => rel.to_string_lossy().to_string(),
            Err(_) => continue,
        };

        if key.is_empty() {
            continue;
        }

        if !scope.includes(&key) {
            continue;
        }

        let file_type = entry.file_type().await;
        let file_type = match file_type {
            Ok(ft) => ft,
            Err(e) => {
                tracing::warn!("L1 scan: cannot read file type for {key}: {e}");
                continue;
            }
        };
        let is_dir = file_type.is_dir();
        let is_symlink = file_type.is_symlink();

        // Skip directories from object tracking — S3 doesn't list directories as objects
        if is_dir {
            continue;
        }

        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

        disk_keys.insert(key.clone());

        // Log progress every 5 seconds
        if last_progress.elapsed() >= std::time::Duration::from_secs(5) {
            let total = report.discovered + report.unchanged;
            tracing::info!(
                files = total,
                discovered = report.discovered,
                unchanged = report.unchanged,
                elapsed = ?scan_start.elapsed(),
                "L1 scan in progress"
            );
            last_progress = std::time::Instant::now();
        }

        // Check if already in DB (in-memory lookup)
        if db_keys.contains(&key) {
            report.unchanged += 1;
            continue;
        }

        // Read symlink target if applicable
        let symlink_target = if is_symlink {
            std::fs::read_link(&path)
                .ok()
                .map(|t| t.to_string_lossy().to_string())
        } else {
            None
        };

        // Infer content type from file extension
        let content_type = mime_guess::from_path(&key)
            .first_or_octet_stream()
            .to_string();

        // Stat the file to get size so it's available immediately after L1
        let size = tokio::fs::symlink_metadata(&path)
            .await
            .ok()
            .map(|m| m.len() as i64);

        let now = time::OffsetDateTime::now_utc();
        let obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            key: key.clone(),
            parent_directory: parent,
            is_symlink,
            symlink_target,
            size,
            content_type: Some(content_type),
            scan_level: 1,
            last_modified: now,
            created_at: now,
            ..Default::default()
        };

        batch.push(obj);
        report.discovered += 1;

        if batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && batch_start.elapsed() >= BATCH_TIMEOUT)
        {
            metadata.insert_objects_batch(&batch).await?;
            batch.clear();
            batch_start = std::time::Instant::now();
        }
    }

    // Flush remaining batch
    if !batch.is_empty() {
        metadata.insert_objects_batch(&batch).await?;
    }

    // Find deleted files: objects in DB but not on disk
    if matches!(scope, ScanScope::Bucket) {
        tracing::info!(
            disk_files = disk_keys.len(),
            elapsed = ?scan_start.elapsed(),
            "L1 walk complete, checking for deleted files"
        );
        let to_delete: Vec<String> = db_keys
            .into_iter()
            .filter(|k| !disk_keys.contains(k))
            .collect();
        for chunk in to_delete.chunks(BATCH_SIZE) {
            report.deleted += metadata.delete_objects(chunk).await?;
        }
    }

    Ok(report)
}

/// L2: stat() each file for metadata (size, mtime, ctime, inode, device_id).
pub async fn scan_l2(
    metadata: &MetadataStore,
    root: &Path,
    keys: &[String],
) -> Result<L2Report, S3Error> {
    let mut report = L2Report::default();
    let total = keys.len();

    tracing::info!(files = total, "L2 metadata scan starting");

    let mut batch: Vec<(String, ObjectMetadataUpdate)> = Vec::with_capacity(BATCH_SIZE);
    let mut batch_start = std::time::Instant::now();

    for (i, key) in keys.iter().enumerate() {
        let path = root.join(key);
        let fs_meta = match tokio::fs::symlink_metadata(&path).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("L2 scan: cannot stat {key}: {e}");
                report.errors += 1;
                continue;
            }
        };

        let (inode, device_id) = platform::file_identity(&fs_meta);

        let size = fs_meta.len() as i64;
        let file_mtime = fs_meta.modified().ok().map(time::OffsetDateTime::from);
        let file_ctime = fs_meta.created().ok().map(time::OffsetDateTime::from);

        let update = ObjectMetadataUpdate {
            size,
            file_mtime,
            file_ctime,
            inode,
            device_id,
            scan_level: 2,
        };
        batch.push((key.clone(), update));
        report.updated += 1;

        tracing::info!(
            key = %key,
            size = size,
            progress = format_args!("[{}/{}]", i + 1, total),
            "L2 stat complete"
        );

        if batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && batch_start.elapsed() >= BATCH_TIMEOUT)
        {
            metadata.update_objects_metadata_batch(&batch).await?;
            batch.clear();
            batch_start = std::time::Instant::now();
        }
    }

    // Flush remaining batch
    if !batch.is_empty() {
        metadata.update_objects_metadata_batch(&batch).await?;
    }

    tracing::info!(
        updated = report.updated,
        errors = report.errors,
        "L2 metadata scan complete"
    );

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn make_store(tmp: &TempDir) -> MetadataStore {
        let db_path = tmp.path().join("test.db");
        MetadataStore::new(&db_path).await.unwrap()
    }

    #[tokio::test]
    async fn test_l1_discovers_files() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("hello.txt"), "hello").unwrap();
        std::fs::create_dir_all(bucket_root.join("subdir")).unwrap();
        std::fs::write(bucket_root.join("subdir/nested.txt"), "nested").unwrap();

        let store = make_store(&tmp).await;
        let report = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        assert_eq!(report.discovered, 2);
        assert_eq!(report.deleted, 0);

        // Verify records exist with size populated from stat()
        let obj = store.get_object("hello.txt").await.unwrap().unwrap();
        assert_eq!(obj.scan_level, 1);
        assert_eq!(obj.size, Some(5)); // "hello" = 5 bytes
        assert_eq!(obj.content_type.as_deref(), Some("text/plain"));

        let nested = store
            .get_object("subdir/nested.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(nested.parent_directory, "subdir");
    }

    #[tokio::test]
    async fn test_l1_skips_shoebox_dir() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(bucket_root.join(".shoebox")).unwrap();
        std::fs::write(bucket_root.join(".shoebox/config.toml"), "secret").unwrap();
        std::fs::write(bucket_root.join("visible.txt"), "visible").unwrap();

        let store = make_store(&tmp).await;
        let report = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        assert_eq!(report.discovered, 1);
        assert!(store
            .get_object(".shoebox/config.toml")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_l1_detects_deleted_files() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("will-delete.txt"), "bye").unwrap();

        let store = make_store(&tmp).await;

        // First scan
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        // Delete the file
        std::fs::remove_file(bucket_root.join("will-delete.txt")).unwrap();

        // Second scan should detect deletion
        let report = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        assert_eq!(report.deleted, 1);
        assert!(store.get_object("will-delete.txt").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_l1_idempotent() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("file.txt"), "data").unwrap();

        let store = make_store(&tmp).await;

        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 1);

        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.discovered, 0);
        assert_eq!(r2.unchanged, 1);
    }

    #[tokio::test]
    async fn test_l2_collects_metadata() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("file.txt"), "hello world").unwrap();

        let store = make_store(&tmp).await;
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        let report = scan_l2(&store, &bucket_root, &["file.txt".to_string()])
            .await
            .unwrap();
        assert_eq!(report.updated, 1);

        let obj = store.get_object("file.txt").await.unwrap().unwrap();
        assert_eq!(obj.scan_level, 2);
        assert_eq!(obj.size, Some(11)); // "hello world" = 11 bytes
        assert!(obj.file_mtime.is_some());
    }
}
