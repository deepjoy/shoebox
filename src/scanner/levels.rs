use std::path::Path;

use async_walkdir::{Filtering, WalkDir};
use futures::StreamExt;
use md5::Md5;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

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

/// Result of an L3 (content hash) scan.
#[derive(Debug, Default)]
pub struct L3Report {
    pub hashed: u64,
    pub bytes: u64,
    pub skipped: u64,
}

/// Check whether a path component represents the .shoebox directory.
fn is_shoebox_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name == SHOEBOX_DIR)
}

/// L1: Fast directory walk — discovers files on disk and inserts new records.
///
/// Uses a SQLite temp table to collect all discovered disk keys during the walk,
/// then merges against the `objects` table in two SQL statements (INSERT new,
/// DELETE stale). This keeps memory usage O(1) regardless of file count — all
/// working set pressure is handled by SQLite's page cache.
///
/// This is a free function per the library-first design principle. Both HTTP
/// handlers and `Shoebox` library methods can call it directly.
pub async fn scan_l1(
    metadata: &MetadataStore,
    root: &Path,
    scope: &ScanScope,
) -> Result<L1Report, S3Error> {
    let scan_start = std::time::Instant::now();

    // Acquire a dedicated connection and create a temp table for disk keys.
    // The connection must be reused for all temp table operations.
    let mut conn = metadata.l1_scan_begin().await?;
    tracing::info!("L1 scan started, temp table created");

    // Walk the filesystem and batch-insert every discovered file into the temp table
    let mut batch: Vec<ObjectRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut batch_start = std::time::Instant::now();
    let mut last_progress = std::time::Instant::now();
    let mut files_walked: u64 = 0;

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

        files_walked += 1;

        // Log progress every 5 seconds
        if last_progress.elapsed() >= std::time::Duration::from_secs(5) {
            tracing::info!(
                files = files_walked,
                elapsed = ?scan_start.elapsed(),
                "L1 scan in progress"
            );
            last_progress = std::time::Instant::now();
        }

        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

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

        if batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && batch_start.elapsed() >= BATCH_TIMEOUT)
        {
            MetadataStore::l1_scan_insert_batch(&mut conn, &batch).await?;
            batch.clear();
            batch_start = std::time::Instant::now();
        }
    }

    // Flush remaining batch
    if !batch.is_empty() {
        MetadataStore::l1_scan_insert_batch(&mut conn, &batch).await?;
    }

    tracing::info!(
        files = files_walked,
        elapsed = ?scan_start.elapsed(),
        "L1 walk complete, merging into catalog"
    );

    // Merge: insert new objects and delete stale ones in two SQL statements
    let delete_stale = matches!(scope, ScanScope::Bucket);
    let (discovered, deleted) = MetadataStore::l1_scan_finish(&mut conn, delete_stale).await?;

    let unchanged = files_walked.saturating_sub(discovered);

    tracing::info!(
        discovered = discovered,
        unchanged = unchanged,
        deleted = deleted,
        elapsed = ?scan_start.elapsed(),
        "L1 scan complete"
    );

    Ok(L1Report {
        discovered,
        deleted,
        unchanged,
    })
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

/// L3: Read file and compute hashes (MD5 for ETag, SHA-256 for content_hash).
pub async fn scan_l3(
    metadata: &MetadataStore,
    root: &Path,
    keys: &[String],
) -> Result<L3Report, S3Error> {
    let mut report = L3Report::default();
    let total = keys.len();

    tracing::info!(files = total, "L3 content-hash scan starting");

    let mut batch: Vec<(String, String, String, i32)> = Vec::with_capacity(BATCH_SIZE);
    let mut batch_start = std::time::Instant::now();

    for (i, key) in keys.iter().enumerate() {
        let path = root.join(key);

        // Record mtime before reading
        let pre_meta = match tokio::fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("L3 scan: cannot access {key}: {e}");
                report.skipped += 1;
                continue;
            }
        };

        // Skip directories (e.g. symlinks whose target is a directory)
        if pre_meta.is_dir() {
            report.skipped += 1;
            continue;
        }

        let mtime_before = pre_meta.modified().ok();

        // Stream through MD5 and SHA-256
        let file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("L3 scan: cannot open {key}: {e}");
                report.skipped += 1;
                continue;
            }
        };
        let mut reader = tokio::io::BufReader::new(file);

        let mut md5_hasher = Md5::new();
        let mut sha256_hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        let mut size = 0u64;

        loop {
            let n = match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("L3 scan: read error for {key}: {e}");
                    report.skipped += 1;
                    break;
                }
            };
            md5_hasher.update(&buf[..n]);
            sha256_hasher.update(&buf[..n]);
            size += n as u64;
        }

        // Verify mtime unchanged (file wasn't modified during scan)
        let mtime_after = tokio::fs::metadata(&path)
            .await
            .ok()
            .and_then(|m| m.modified().ok());

        if mtime_before != mtime_after {
            tracing::info!(
                key = %key,
                progress = format_args!("[{}/{}]", i + 1, total),
                "L3 skipped (modified during scan)"
            );
            report.skipped += 1;
            continue;
        }

        let etag = format!("\"{}\"", hex::encode(md5_hasher.finalize()));
        let content_hash = format!("sha256:{}", hex::encode(sha256_hasher.finalize()));

        report.hashed += 1;
        report.bytes += size;

        batch.push((key.clone(), etag, content_hash, 3));

        tracing::info!(
            key = %key,
            size = format_human_size(size),
            progress = format_args!("[{}/{}]", i + 1, total),
            "L3 hash complete"
        );

        if batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && batch_start.elapsed() >= BATCH_TIMEOUT)
        {
            metadata.update_objects_hashes_batch(&batch).await?;
            batch.clear();
            batch_start = std::time::Instant::now();
        }
    }

    // Flush remaining batch
    if !batch.is_empty() {
        metadata.update_objects_hashes_batch(&batch).await?;
    }

    tracing::info!(
        hashed = report.hashed,
        bytes = report.bytes,
        skipped = report.skipped,
        "L3 content-hash scan complete"
    );

    Ok(report)
}

/// Format bytes as a human-readable size string.
fn format_human_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
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

    #[tokio::test]
    async fn test_l3_computes_hashes() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("file.txt"), "hello world").unwrap();

        let store = make_store(&tmp).await;
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        let report = scan_l3(&store, &bucket_root, &["file.txt".to_string()])
            .await
            .unwrap();
        assert_eq!(report.hashed, 1);
        assert_eq!(report.bytes, 11);

        let obj = store.get_object("file.txt").await.unwrap().unwrap();
        assert_eq!(obj.scan_level, 3);
        assert!(obj.etag.is_some());
        assert!(obj.content_hash.as_ref().unwrap().starts_with("sha256:"));

        // Verify MD5 of "hello world"
        let expected_md5 = format!("\"{}\"", hex::encode(Md5::digest(b"hello world")));
        assert_eq!(obj.etag.unwrap(), expected_md5);
    }
}
