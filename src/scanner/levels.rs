use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::SystemTime;

use async_walkdir::{Filtering, WalkDir};
use base64::Engine;
use futures::StreamExt;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::config::SHOEBOX_DIR;
use crate::error::S3Error;
use crate::metadata::sqlite::{L3HashUpdate, ObjectMetadataUpdate, ObjectRecord, SqliteTimestamp};
use crate::metadata::MetadataStore;

/// Convert a `SystemTime` to `OffsetDateTime` without panicking.
///
/// `time::OffsetDateTime::from(SystemTime)` panics on overflow (e.g. corrupt
/// timestamps, far-future dates).  This helper goes through
/// `from_unix_timestamp_nanos` which returns `Result` instead.
pub fn system_time_to_odt(t: SystemTime) -> Option<time::OffsetDateTime> {
    let nanos: i128 = match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    };
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}
use crate::scanner::platform;
use crate::scanner::scope::ScanScope;
use crate::services::duplicates_service;
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
    /// Files skipped because (inode, device_id, size) matched the catalog.
    pub skipped: u64,
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
/// Uses an **incremental** approach when the catalog is non-empty:
///
/// 1. Load all known files into an in-memory `KnownFiles` map (one query).
/// 2. Walk the filesystem. For each file:
///    - If `(parent_dir_id, name, inode, device_id, size)` match the catalog → skip (unchanged).
///    - If inode+device_id match an existing object at a different location → move (update in-place).
///    - Otherwise → insert into a temp table as a new file.
/// 3. Merge the (much smaller) temp table into the catalog.
/// 4. Delete stale objects whose keys were not seen during the walk.
///
/// When the catalog is empty (first scan), this degrades gracefully to the
/// full temp-table approach since no files will match.
///
/// Memory: ~30 MB for 100K files with average 100-byte keys.
pub async fn scan_l1(
    metadata: &MetadataStore,
    root: &Path,
    scope: &ScanScope,
) -> Result<L1Report, S3Error> {
    let scan_start = std::time::Instant::now();
    let scan_start_ts = SqliteTimestamp::now();

    // Phase 1: Load known files for incremental comparison
    let known = metadata.l1_scan_load_known_files().await?;
    let known_count = known.by_key.len();
    tracing::info!(known_files = known_count, "L1 scan started (incremental)");

    // Phase 2: Acquire a dedicated connection and create a temp table for NEW disk keys.
    let mut conn = metadata.l1_scan_begin().await?;

    // Local caches to avoid per-file DB round-trips
    let mut dir_cache: HashMap<String, i64> = HashMap::with_capacity(known_count / 10);
    let mut ct_cache: HashMap<String, i64> = HashMap::new();

    // Track all seen keys for deletion detection
    let mut seen: HashSet<(i64, String)> = HashSet::with_capacity(known_count);
    let mut skipped: u64 = 0;
    let mut moved: u64 = 0;
    let mut files_walked: u64 = 0;
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
                skipped = skipped,
                elapsed = ?scan_start.elapsed(),
                "L1 scan in progress"
            );
            last_progress = std::time::Instant::now();
        }

        // Stat the file to get size and inode/device_id
        let fs_meta = tokio::fs::symlink_metadata(&path).await.ok();
        let size = fs_meta.as_ref().map(|m| m.len() as i64);
        let (inode, device_id) = fs_meta
            .as_ref()
            .map(platform::file_identity)
            .unwrap_or((None, None));
        let inode_i64 = inode.map(|v| v as i64);
        let device_id_i64 = device_id.map(|v| v as i64);

        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();
        let dir_id = match dir_cache.get(&parent) {
            Some(&id) => id,
            None => {
                let id = metadata.get_or_create_dir_id(&parent).await?;
                dir_cache.insert(parent.clone(), id);
                id
            }
        };
        let (_, filename) = crate::metadata::sqlite::split_key(&key);
        let filename = filename.to_string();

        // Mark this key as seen for stale-deletion detection
        seen.insert((dir_id, filename.clone()));

        // Check 1: Does this file exist at the same location with matching identity?
        if let Some(identity) = known.by_key.get(&(dir_id, filename.clone())) {
            if identity.inode == inode_i64
                && identity.device_id == device_id_i64
                && identity.size == size
            {
                // File unchanged — skip temp table insert entirely
                skipped += 1;
                continue;
            }
        }

        // Check 2: Is this a moved file? (same inode, different location)
        if let (Some(ino), Some(dev)) = (inode_i64, device_id_i64) {
            if let Some((old_parent_dir_id, old_name)) = known.by_inode.get(&(ino, dev)) {
                if *old_parent_dir_id != dir_id || *old_name != filename {
                    // Move detected — update the existing row in-place
                    let now = SqliteTimestamp::now();
                    let old_prefix = parent_prefix_for_log(metadata, *old_parent_dir_id).await;
                    metadata
                        .l1_scan_apply_move(
                            &mut conn,
                            *old_parent_dir_id,
                            old_name,
                            dir_id,
                            &filename,
                            now,
                        )
                        .await?;
                    tracing::info!(
                        old_key = %format!("{}{}", old_prefix, old_name),
                        new_key = %key,
                        "Move detected (incremental), preserving object_id"
                    );
                    moved += 1;
                    continue;
                }
            }
        }

        // New or changed file — insert into temp table for merge
        let symlink_target = if is_symlink {
            std::fs::read_link(&path)
                .ok()
                .map(|t| t.to_string_lossy().to_string())
        } else {
            None
        };

        let content_type = mime_guess::from_path(&key)
            .first_or_octet_stream()
            .to_string();
        let ct_id = match ct_cache.get(&content_type) {
            Some(&id) => id,
            None => {
                let id = metadata
                    .get_or_create_content_type_id(&content_type)
                    .await?;
                ct_cache.insert(content_type.clone(), id);
                id
            }
        };

        let now = SqliteTimestamp::now();
        let obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            name: filename,
            parent_dir_id: dir_id,
            key: key.clone(),
            is_symlink,
            symlink_target,
            size,
            inode: inode_i64,
            device_id: device_id_i64,
            content_type_id: Some(ct_id),
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
        skipped = skipped,
        moved = moved,
        elapsed = ?scan_start.elapsed(),
        "L1 walk complete, merging new files into catalog"
    );

    // Phase 3: Merge the temp table (only new/changed files) — moves already applied
    let (discovered, _temp_deleted, temp_moved) =
        MetadataStore::l1_scan_finish(&mut conn, false).await?;
    moved += temp_moved;

    // Phase 4: Delete stale objects (only for bucket-wide scans)
    // Exclude objects created after the scan started to avoid racing with
    // concurrent API uploads (put_object inserts at scan_level 3).
    let deleted = if matches!(scope, ScanScope::Bucket) {
        metadata.l1_scan_delete_stale(&seen, scan_start_ts).await?
    } else {
        0
    };

    let unchanged = files_walked.saturating_sub(discovered + moved + skipped);

    tracing::info!(
        discovered = discovered,
        moved = moved,
        skipped = skipped,
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
        skipped,
    })
}

// ── BFS L1 per-directory scan ──────────────────────────────────────────────

/// Result of a single BFS directory scan task.
///
/// The `write_op` field carries the inserts and stale-deletes for this directory.
/// The caller (executor) sends it to the shared write channel; the single writer
/// task commits it in a large batch together with other directories' ops.
pub struct L1DirScan {
    /// Relative prefixes of immediate subdirectories to enqueue as sibling tasks.
    pub child_dirs: Vec<String>,
    pub unchanged: u64,
    /// Estimated new files (= inserts queued; actual rows affected known after commit).
    pub new_count: u64,
    /// Estimated stale files (= deletes queued).
    pub stale_count: u64,
    pub write_op: crate::metadata::sqlite::L1WriteOp,
}

/// Scan a single directory for the BFS L1 pass.
///
/// Reads immediate children of `root.join(dir_prefix)` via `tokio::fs::read_dir`,
/// compares files against the per-directory catalog snapshot, upserts new/changed
/// files, deletes stale entries, and returns the list of subdirectory prefixes to
/// enqueue next.
///
/// `scan_start_ns` is the epoch-nanosecond timestamp at which the orchestrator
/// started the scan; it is used as the stale-deletion boundary (objects created
/// after this timestamp by concurrent API uploads are excluded from deletion).
pub async fn scan_l1_dir(
    metadata: &MetadataStore,
    root: &Path,
    dir_prefix: &str,
    scope: &ScanScope,
) -> Result<L1DirScan, S3Error> {
    let dir_path = if dir_prefix.is_empty() {
        root.to_path_buf()
    } else {
        root.join(dir_prefix.trim_end_matches('/'))
    };

    // Resolve (or create) the directory id for this prefix.
    let parent_key = if dir_prefix.is_empty() {
        String::new()
    } else {
        // dir_prefix is "photos/2024/" — strip trailing slash for get_or_create_dir_id
        dir_prefix.trim_end_matches('/').to_string()
    };
    let dir_id = metadata.get_or_create_dir_id(&parent_key).await?;

    // Load known files in this directory for unchanged-detection.
    let known = metadata.l1_load_dir_objects(dir_id).await?;

    let mut child_dirs: Vec<String> = Vec::new();
    let mut batch: Vec<ObjectRecord> = Vec::new();
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut unchanged: u64 = 0;

    let mut read_dir = match tokio::fs::read_dir(&dir_path).await {
        Ok(rd) => rd,
        Err(e) => {
            tracing::warn!(dir = %dir_path.display(), "L1 dir scan: read_dir failed: {e}");
            return Ok(L1DirScan {
                child_dirs: Vec::new(),
                unchanged: 0,
                new_count: 0,
                stale_count: 0,
                write_op: crate::metadata::sqlite::L1WriteOp {
                    dir_id,
                    inserts: Vec::new(),
                    stale_names: Vec::new(),
                },
            });
        }
    };

    let mut ct_cache: HashMap<String, i64> = HashMap::new();

    while let Some(entry) = read_dir.next_entry().await.transpose() {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(dir = %dir_path.display(), "L1 dir scan entry error: {e}");
                continue;
            }
        };

        let entry_name = entry.file_name().to_string_lossy().to_string();

        // Build the full key for scope filtering
        let key = if dir_prefix.is_empty() {
            entry_name.clone()
        } else {
            format!("{}{}", dir_prefix, entry_name)
        };

        let file_type = match entry.file_type().await {
            Ok(ft) => ft,
            Err(e) => {
                tracing::warn!(path = %entry.path().display(), "L1 dir scan: file_type error: {e}");
                continue;
            }
        };

        if file_type.is_dir() {
            // Skip .shoebox directory
            if entry_name == SHOEBOX_DIR {
                continue;
            }
            // Collect subdirectory prefix (always ends with '/')
            let sub_prefix = format!("{key}/");
            if scope.includes(&key) || scope.includes(&sub_prefix) {
                child_dirs.push(sub_prefix);
            }
            continue;
        }

        // Only catalog regular files and symlinks
        let is_symlink = file_type.is_symlink();
        if !(file_type.is_file() || is_symlink) {
            continue;
        }

        if !scope.includes(&key) {
            continue;
        }

        seen_names.insert(entry_name.clone());

        // Stat the file to detect changes
        let path = entry.path();
        let fs_meta = tokio::fs::symlink_metadata(&path).await.ok();
        let size = fs_meta.as_ref().map(|m| m.len() as i64);
        let (inode, device_id) = fs_meta
            .as_ref()
            .map(platform::file_identity)
            .unwrap_or((None, None));
        let inode_i64 = inode.map(|v| v as i64);
        let device_id_i64 = device_id.map(|v| v as i64);

        // Skip unchanged files (identity matches catalog)
        if let Some(identity) = known.get(&entry_name) {
            if identity.inode == inode_i64
                && identity.device_id == device_id_i64
                && identity.size == size
            {
                unchanged += 1;
                continue;
            }
        }

        // New or changed file — prepare for upsert
        let symlink_target = if is_symlink {
            std::fs::read_link(&path)
                .ok()
                .map(|t| t.to_string_lossy().to_string())
        } else {
            None
        };

        let content_type = mime_guess::from_path(&key)
            .first_or_octet_stream()
            .to_string();
        let ct_id = match ct_cache.get(&content_type) {
            Some(&id) => id,
            None => {
                let id = metadata
                    .get_or_create_content_type_id(&content_type)
                    .await?;
                ct_cache.insert(content_type.clone(), id);
                id
            }
        };

        let now = crate::metadata::sqlite::SqliteTimestamp::now();
        batch.push(ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            name: entry_name,
            parent_dir_id: dir_id,
            key,
            is_symlink,
            symlink_target,
            size,
            inode: inode_i64,
            device_id: device_id_i64,
            content_type_id: Some(ct_id),
            scan_level: 1,
            last_modified: now,
            created_at: now,
            ..Default::default()
        });
    }

    // Compute stale names from the snapshot we already loaded — no second SELECT.
    // `known` was populated before the directory walk; any file in the catalog
    // at that moment that we did not see on disk is considered stale.
    let stale_names: Vec<String> = known
        .into_keys()
        .filter(|name| !seen_names.contains(name))
        .collect();

    let new_count = batch.len() as u64;
    let stale_count = stale_names.len() as u64;

    Ok(L1DirScan {
        child_dirs,
        unchanged,
        new_count,
        stale_count,
        write_op: crate::metadata::sqlite::L1WriteOp {
            dir_id,
            inserts: batch,
            stale_names,
        },
    })
}

/// Helper: get directory prefix for logging move detection.
async fn parent_prefix_for_log(metadata: &MetadataStore, parent_dir_id: i64) -> String {
    metadata
        .get_directory_prefix(parent_dir_id)
        .await
        .unwrap_or_default()
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
        let file_mtime = fs_meta.modified().ok().and_then(system_time_to_odt);
        let file_ctime = fs_meta.created().ok().and_then(system_time_to_odt);

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

    // Recompute directory hashes for parent directories of hashed files.
    let parent_dirs: HashSet<String> = keys
        .iter()
        .filter_map(|k| k.rsplit_once('/').map(|(p, _)| p.to_string()))
        .collect();
    for parent in &parent_dirs {
        if let Err(e) = duplicates_service::compute_single_directory_hash(metadata, parent).await {
            tracing::warn!(parent_dir = %parent, error = %e, "failed to compute directory hash");
        }
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
        assert_eq!(
            store.resolve_content_type(obj.content_type_id).await,
            "text/plain"
        );

        let nested = store
            .get_object("subdir/nested.txt")
            .await
            .unwrap()
            .unwrap();
        let nested_dir_prefix = store
            .get_directory_prefix(nested.parent_dir_id)
            .await
            .unwrap();
        assert_eq!(nested_dir_prefix, "subdir/");
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
        assert_eq!(r2.skipped, 1); // unchanged file detected by inode/size match
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

    // -- Incremental L1 scan tests (Phase 11b) --

    #[tokio::test]
    async fn test_l1_incremental_skips_unchanged() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("a.txt"), "aaa").unwrap();
        std::fs::write(bucket_root.join("b.txt"), "bbb").unwrap();

        let store = make_store(&tmp).await;

        // First scan: discovers all files
        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 2);
        assert_eq!(r1.skipped, 0);

        // Second scan: files unchanged, should be skipped
        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.discovered, 0);
        assert_eq!(r2.skipped, 2);
        assert_eq!(r2.deleted, 0);
        assert_eq!(r2.moved, 0);
    }

    #[tokio::test]
    async fn test_l1_incremental_detects_new_files() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("existing.txt"), "existing").unwrap();

        let store = make_store(&tmp).await;

        // First scan
        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 1);

        // Add a new file
        std::fs::write(bucket_root.join("new.txt"), "new").unwrap();

        // Second scan: should discover the new file, skip the existing one
        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.discovered, 1);
        assert_eq!(r2.skipped, 1);

        // Both files should be in the catalog
        assert!(store.get_object("existing.txt").await.unwrap().is_some());
        assert!(store.get_object("new.txt").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_l1_incremental_detects_deleted_files() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("keep.txt"), "keep").unwrap();
        std::fs::write(bucket_root.join("remove.txt"), "remove").unwrap();

        let store = make_store(&tmp).await;

        // First scan
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        // Delete one file
        std::fs::remove_file(bucket_root.join("remove.txt")).unwrap();

        // Second scan: should detect the deletion
        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.deleted, 1);
        assert_eq!(r2.skipped, 1); // keep.txt unchanged
        assert!(store.get_object("remove.txt").await.unwrap().is_none());
        assert!(store.get_object("keep.txt").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_l1_incremental_detects_moves() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("original.txt"), "move me").unwrap();

        let store = make_store(&tmp).await;

        // First scan
        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 1);

        // Get original object ID
        let original = store.get_object("original.txt").await.unwrap().unwrap();
        let original_id = original.id.clone();

        // Rename (move) the file
        std::fs::rename(
            bucket_root.join("original.txt"),
            bucket_root.join("renamed.txt"),
        )
        .unwrap();

        // Second scan: should detect the move
        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.moved, 1);
        assert_eq!(r2.discovered, 0);
        assert_eq!(r2.deleted, 0);

        // Object ID should be preserved
        let renamed = store.get_object("renamed.txt").await.unwrap().unwrap();
        assert_eq!(renamed.id, original_id);

        // Old key should not exist
        assert!(store.get_object("original.txt").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_l1_incremental_first_scan_identical() {
        // On an empty catalog, incremental scan should behave identically
        // to the old full-scan approach.
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("file.txt"), "data").unwrap();
        std::fs::create_dir_all(bucket_root.join("sub")).unwrap();
        std::fs::write(bucket_root.join("sub/nested.txt"), "nested").unwrap();

        let store = make_store(&tmp).await;

        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 2);
        assert_eq!(r1.skipped, 0);
        assert_eq!(r1.deleted, 0);
        assert_eq!(r1.moved, 0);

        // Verify records
        let obj = store.get_object("file.txt").await.unwrap().unwrap();
        assert_eq!(obj.scan_level, 1);
        assert_eq!(obj.size, Some(4)); // "data" = 4 bytes
    }

    #[tokio::test]
    async fn test_l1_incremental_mixed_scenario() {
        // Simulate: some unchanged, one deleted, one moved, one new.
        // We separate delete+create to avoid inode reuse confusing move detection.
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("unchanged.txt"), "stable").unwrap();
        std::fs::write(bucket_root.join("to-move.txt"), "mobile").unwrap();
        // Create brand-new.txt BEFORE first scan so its inode is known,
        // then we'll remove and re-create it with guaranteed-different inode.
        std::fs::write(bucket_root.join("placeholder.txt"), "placeholder").unwrap();

        let store = make_store(&tmp).await;

        // First scan
        let r1 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r1.discovered, 3);

        let move_id = store
            .get_object("to-move.txt")
            .await
            .unwrap()
            .unwrap()
            .id
            .clone();

        // Make changes:
        // 1. Delete one file
        std::fs::remove_file(bucket_root.join("placeholder.txt")).unwrap();
        // 2. Move one file
        std::fs::rename(
            bucket_root.join("to-move.txt"),
            bucket_root.join("moved.txt"),
        )
        .unwrap();

        // Second scan: 1 unchanged (skipped), 1 moved, 1 deleted
        let r2 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r2.skipped, 1, "unchanged.txt should be skipped");
        assert_eq!(r2.moved, 1, "to-move.txt → moved.txt");
        assert_eq!(r2.discovered, 0, "no new files");
        assert_eq!(r2.deleted, 1, "placeholder.txt deleted");

        // Verify catalog state
        assert!(store.get_object("unchanged.txt").await.unwrap().is_some());
        assert!(store.get_object("placeholder.txt").await.unwrap().is_none());
        assert!(store.get_object("to-move.txt").await.unwrap().is_none());
        let moved = store.get_object("moved.txt").await.unwrap().unwrap();
        assert_eq!(moved.id, move_id, "Move should preserve object ID");

        // Now add a brand-new file and scan again
        std::fs::write(bucket_root.join("brand-new.txt"), "hello").unwrap();

        let r3 = scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();
        assert_eq!(r3.discovered, 1, "brand-new.txt");
        assert_eq!(r3.skipped, 2, "unchanged.txt + moved.txt");
        assert!(store.get_object("brand-new.txt").await.unwrap().is_some());
    }

    /// Regression test: an API upload (put_object, scan_level=3) that lands in
    /// the DB while an L1 scan is in progress must NOT be deleted as stale.
    ///
    /// Simulates the race by calling `l1_scan_delete_stale` directly with a
    /// controlled `scan_start` timestamp, so we can insert an object with
    /// `created_at > scan_start` without needing real concurrency.
    #[tokio::test]
    async fn test_l1_does_not_delete_concurrent_api_upload() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("bucket");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::write(bucket_root.join("on-disk.txt"), "disk file").unwrap();

        let store = make_store(&tmp).await;

        // First scan to populate the catalog
        scan_l1(&store, &bucket_root, &ScanScope::Bucket)
            .await
            .unwrap();

        // Record a timestamp representing when a hypothetical second scan starts
        let scan_start = SqliteTimestamp::now();

        // Small delay so the API upload's created_at is strictly after scan_start
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        // Simulate an API upload that arrives during the scan.
        // The file exists only in the DB (scan_level=3), not on disk, mimicking
        // a put_object that wrote to storage but the walk already passed that
        // directory.
        let now = SqliteTimestamp::now();
        let dir_id = store.get_or_create_dir_id("").await.unwrap();
        let ct_id = store
            .get_or_create_content_type_id("text/plain")
            .await
            .unwrap();
        let api_obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            name: "api-upload.txt".to_string(),
            parent_dir_id: dir_id,
            key: "api-upload.txt".to_string(),
            size: Some(42),
            content_type_id: Some(ct_id),
            scan_level: 3,
            last_modified: now,
            created_at: now,
            ..Default::default()
        };
        store.insert_object(&api_obj).await.unwrap();

        // Build a `seen` set containing only the on-disk file (simulating a
        // walk that never encountered api-upload.txt)
        let (_, on_disk_name) = crate::metadata::sqlite::split_key("on-disk.txt");
        let mut seen = std::collections::HashSet::new();
        seen.insert((dir_id, on_disk_name.to_string()));

        // Call l1_scan_delete_stale with the pre-upload timestamp.
        // Without the fix, api-upload.txt would be deleted here.
        let deleted = store.l1_scan_delete_stale(&seen, scan_start).await.unwrap();
        assert_eq!(deleted, 0, "API upload must not be deleted as stale");

        // Verify the API-uploaded object still exists in the catalog
        assert!(
            store.get_object("api-upload.txt").await.unwrap().is_some(),
            "API-uploaded object should survive stale deletion"
        );
        // And the on-disk file is still there too
        assert!(store.get_object("on-disk.txt").await.unwrap().is_some());
    }
}
