use std::path::Path;

use async_walkdir::{Filtering, WalkDir};
use base64::Engine;
use futures::StreamExt;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::config::SHOEBOX_DIR;
use crate::error::S3Error;
use crate::metadata::sqlite::{L3HashUpdate, ObjectMetadataUpdate, ObjectRecord};
use crate::metadata::MetadataStore;
use crate::scanner::platform;
use crate::scanner::scope::ScanScope;
use crate::types::ChecksumValues;

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
    pub moved: u64,
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
        let is_symlink = file_type.is_symlink();

        // Only catalog regular files and symlinks — skip directories, named pipes,
        // sockets, and device nodes which could block or consume unbounded memory.
        if !(file_type.is_file() || is_symlink) {
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

        // Stat the file to get size and inode/device_id so they're
        // available immediately after L1 (inode is needed for move detection).
        let fs_meta = tokio::fs::symlink_metadata(&path).await.ok();
        let size = fs_meta.as_ref().map(|m| m.len() as i64);
        let (inode, device_id) = fs_meta
            .as_ref()
            .map(platform::file_identity)
            .unwrap_or((None, None));

        let now = time::OffsetDateTime::now_utc();
        let obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            key: key.clone(),
            parent_directory: parent,
            is_symlink,
            symlink_target,
            size,
            inode: inode.map(|v| v as i64),
            device_id: device_id.map(|v| v as i64),
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

    // Merge: detect moves, insert new objects, and delete stale ones
    let delete_stale = matches!(scope, ScanScope::Bucket);
    let (discovered, deleted, moved) =
        MetadataStore::l1_scan_finish(&mut conn, delete_stale).await?;

    let unchanged = files_walked.saturating_sub(discovered + moved);

    tracing::info!(
        discovered = discovered,
        moved = moved,
        unchanged = unchanged,
        deleted = deleted,
        elapsed = ?scan_start.elapsed(),
        "L1 scan complete"
    );

    Ok(L1Report {
        discovered,
        deleted,
        unchanged,
        moved,
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

    if total > 1 {
        tracing::info!(files = total, "L2 metadata scan starting");
    } else {
        tracing::debug!(files = total, "L2 metadata scan starting");
    }

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

        tracing::debug!(
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

    if total > 1 {
        tracing::info!(
            updated = report.updated,
            errors = report.errors,
            "L2 metadata scan complete"
        );
    } else {
        tracing::debug!(
            updated = report.updated,
            errors = report.errors,
            "L2 metadata scan complete"
        );
    }

    Ok(report)
}

/// Result of hashing a single file.
enum L3FileResult {
    Hashed {
        key: String,
        etag: String,
        checksums: ChecksumValues,
        size: u64,
    },
    /// File was a symlink — promote to scan_level 3 without hashing.
    Symlink {
        key: String,
    },
    Skipped,
}

/// Hash a single file, computing MD5 (ETag) and all S3 checksums.
///
/// Returns `L3FileResult::Hashed` on success or `L3FileResult::Skipped` if the
/// file is missing, unreadable, a directory, or was modified during the scan.
async fn hash_one_file(root: &Path, key: &str, index: usize, total: usize) -> L3FileResult {
    let path = root.join(key);

    // Skip symlinks — they don't have independently hashable content in the
    // S3 model.  Promote them to scan_level 3 so they aren't re-queued.
    match tokio::fs::symlink_metadata(&path).await {
        Ok(m) if m.file_type().is_symlink() => {
            tracing::debug!("L3 scan: skipping symlink {key}");
            return L3FileResult::Symlink {
                key: key.to_string(),
            };
        }
        Err(e) => {
            tracing::warn!("L3 scan: cannot stat {key}: {e}");
            return L3FileResult::Skipped;
        }
        Ok(_) => {} // regular file — continue to hashing
    }

    // Record mtime before reading
    let pre_meta = match tokio::fs::metadata(&path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!("L3 scan: skipping dangling symlink {key}");
            return L3FileResult::Skipped;
        }
        Err(e) => {
            tracing::warn!("L3 scan: cannot access {key}: {e}");
            return L3FileResult::Skipped;
        }
    };

    // Skip directories (e.g. symlinks whose target is a directory)
    if pre_meta.is_dir() {
        return L3FileResult::Skipped;
    }

    let mtime_before = pre_meta.modified().ok();

    // Stream through MD5 and SHA-256
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("L3 scan: cannot open {key}: {e}");
            return L3FileResult::Skipped;
        }
    };
    let mut reader = tokio::io::BufReader::new(file);

    let mut md5_hasher = Md5::new();
    let mut sha256_hasher = Sha256::new();
    let mut sha1_hasher = Sha1::new();
    let mut crc32_hasher = crc32fast::Hasher::new();
    let mut crc32c_value: u32 = 0;
    let mut buf = [0u8; 64 * 1024];
    let mut size = 0u64;
    let mut read_error = false;

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                tracing::warn!("L3 scan: read error for {key}: {e}");
                read_error = true;
                break;
            }
        };
        md5_hasher.update(&buf[..n]);
        sha256_hasher.update(&buf[..n]);
        sha1_hasher.update(&buf[..n]);
        crc32_hasher.update(&buf[..n]);
        crc32c_value = crc32c::crc32c_append(crc32c_value, &buf[..n]);
        size += n as u64;
    }

    if read_error {
        return L3FileResult::Skipped;
    }

    // Verify mtime unchanged (file wasn't modified during scan)
    let mtime_after = tokio::fs::metadata(&path)
        .await
        .ok()
        .and_then(|m| m.modified().ok());

    if mtime_before != mtime_after {
        tracing::debug!(
            key = %key,
            progress = format_args!("[{}/{}]", index + 1, total),
            "L3 skipped (modified during scan)"
        );
        return L3FileResult::Skipped;
    }

    let etag = format!("\"{}\"", hex::encode(md5_hasher.finalize()));
    let b64 = base64::engine::general_purpose::STANDARD;
    let checksums = ChecksumValues {
        sha256: Some(b64.encode(sha256_hasher.finalize())),
        sha1: Some(b64.encode(sha1_hasher.finalize())),
        crc32: Some(b64.encode(crc32_hasher.finalize().to_be_bytes())),
        crc32c: Some(b64.encode(crc32c_value.to_be_bytes())),
    };

    tracing::debug!(
        key = %key,
        size = format_human_size(size),
        progress = format_args!("[{}/{}]", index + 1, total),
        "L3 hash complete"
    );

    L3FileResult::Hashed {
        key: key.to_string(),
        etag,
        checksums,
        size,
    }
}

/// L3: Read files and compute hashes (MD5 for ETag, plus all S3 checksums).
///
/// Files are hashed concurrently using `buffer_unordered(concurrency)`.
/// Use higher concurrency for small files (syscall-bound) and lower for large
/// files (I/O-bound).
pub async fn scan_l3(
    metadata: &MetadataStore,
    root: &Path,
    keys: &[String],
    concurrency: usize,
) -> Result<L3Report, S3Error> {
    let mut report = L3Report::default();
    let total = keys.len();

    if total > 1 {
        tracing::info!(files = total, concurrency, "L3 content-hash scan starting");
    } else {
        tracing::debug!(files = total, concurrency, "L3 content-hash scan starting");
    }

    // Hash files concurrently — clone keys up-front so the futures are 'static
    let root = root.to_owned();
    let tasks: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let root = root.clone();
            let key = key.clone();
            async move { hash_one_file(&root, &key, i, total).await }
        })
        .collect();
    let results: Vec<L3FileResult> = futures::stream::iter(tasks)
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Batch-write results to the database
    let mut batch: Vec<L3HashUpdate> = Vec::with_capacity(BATCH_SIZE);
    let mut symlink_keys: Vec<String> = Vec::new();
    for result in results {
        match result {
            L3FileResult::Hashed {
                key,
                etag,
                checksums,
                size,
            } => {
                report.hashed += 1;
                report.bytes += size;
                batch.push(L3HashUpdate {
                    key,
                    etag,
                    checksums,
                    scan_level: 3,
                });
                if batch.len() >= BATCH_SIZE {
                    metadata.update_objects_hashes_batch(&batch).await?;
                    batch.clear();
                }
            }
            L3FileResult::Symlink { key } => {
                report.skipped += 1;
                symlink_keys.push(key);
            }
            L3FileResult::Skipped => {
                report.skipped += 1;
            }
        }
    }

    // Flush remaining batch
    if !batch.is_empty() {
        metadata.update_objects_hashes_batch(&batch).await?;
    }

    // Promote symlinks to scan_level 3 so they aren't re-queued
    if !symlink_keys.is_empty() {
        tracing::debug!(
            count = symlink_keys.len(),
            "promoting symlinks to scan_level 3"
        );
        metadata.promote_scan_level_batch(&symlink_keys, 3).await?;
    }

    if total > 1 {
        tracing::info!(
            hashed = report.hashed,
            bytes = format_human_size(report.bytes),
            skipped = report.skipped,
            "L3 content-hash scan complete"
        );
    } else {
        tracing::debug!(
            hashed = report.hashed,
            bytes = format_human_size(report.bytes),
            skipped = report.skipped,
            "L3 content-hash scan complete"
        );
    }

    Ok(report)
}

/// Format bytes as a human-readable size string.
pub fn format_human_size(bytes: u64) -> String {
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

        let report = scan_l3(&store, &bucket_root, &["file.txt".to_string()], 1)
            .await
            .unwrap();
        assert_eq!(report.hashed, 1);
        assert_eq!(report.bytes, 11);

        let obj = store.get_object("file.txt").await.unwrap().unwrap();
        assert_eq!(obj.scan_level, 3);
        assert!(obj.etag.is_some());
        assert!(obj.checksum_sha256.is_some());

        // Verify MD5 of "hello world"
        let expected_md5 = format!("\"{}\"", hex::encode(Md5::digest(b"hello world")));
        assert_eq!(obj.etag.unwrap(), expected_md5);
    }

    #[tokio::test]
    async fn test_l3_skips_symlinks_and_promotes() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("target.txt"), "real content").unwrap();

        // Create a symlink and a broken symlink
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                bucket_root.join("target.txt"),
                bucket_root.join("link.txt"),
            )
            .unwrap();
            std::os::unix::fs::symlink(
                bucket_root.join("nonexistent"),
                bucket_root.join("broken.txt"),
            )
            .unwrap();
        }

        let store = make_store(&tmp).await;
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        // All three should be discovered
        let keys: Vec<String> = vec!["broken.txt".into(), "link.txt".into(), "target.txt".into()];
        scan_l2(&store, &bucket_root, &keys).await.unwrap();

        // L3 should hash only target.txt, skip the symlinks
        let report = scan_l3(&store, &bucket_root, &keys, 1).await.unwrap();
        assert_eq!(report.hashed, 1); // only target.txt
        assert_eq!(report.skipped, 2); // link.txt + broken.txt

        // Symlinks should be promoted to scan_level 3
        let link = store.get_object("link.txt").await.unwrap().unwrap();
        assert_eq!(link.scan_level, 3);
        assert!(link.etag.is_none()); // no hash for symlinks

        let broken = store.get_object("broken.txt").await.unwrap().unwrap();
        assert_eq!(broken.scan_level, 3);

        // Regular file should have hashes
        let target = store.get_object("target.txt").await.unwrap().unwrap();
        assert_eq!(target.scan_level, 3);
        assert!(target.etag.is_some());
    }
}
