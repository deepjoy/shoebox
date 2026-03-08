use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use base64::Engine;
use sha2::{Digest, Sha256};

use crate::error::S3Error;
use crate::metadata::sqlite::{DirectoryHashRecord, ObjectRecord};
use crate::metadata::MetadataStore;

// ── Types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateFile {
    pub object_id: String,
    pub key: String,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateFileGroup {
    pub checksum_sha256: String,
    pub size: i64,
    pub files: Vec<DuplicateFile>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateReport {
    pub bucket: String,
    pub duplicates: Vec<DuplicateFileGroup>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
    pub scan_complete: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CrossBucketFile {
    pub bucket: String,
    pub object_id: String,
    pub key: String,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CrossBucketDuplicateGroup {
    pub checksum_sha256: String,
    pub files: Vec<CrossBucketFile>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CrossBucketDuplicateReport {
    pub duplicates: Vec<CrossBucketDuplicateGroup>,
    pub is_truncated: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateDir {
    pub prefix: String,
    pub file_count: i32,
    pub total_size: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateDirGroup {
    pub dir_hash: String,
    pub dirs: Vec<DuplicateDir>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DuplicateDirReport {
    pub bucket: String,
    pub duplicate_dirs: Vec<DuplicateDirGroup>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DirRef {
    pub bucket: String,
    pub path: String,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FileDifference {
    pub key: String,
    pub status: String,
    pub left_hash: Option<String>,
    pub right_hash: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ComparisonSummary {
    pub files_identical: usize,
    pub files_only_in_left: usize,
    pub files_only_in_right: usize,
    pub files_different: usize,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DirComparison {
    pub left: DirRef,
    pub right: DirRef,
    pub identical: bool,
    pub summary: ComparisonSummary,
    pub differences: Vec<FileDifference>,
}

// ── Per-Bucket Duplicates (8.1) ─────────────────────────────────────

/// Find duplicates within a single bucket with keyset pagination and optional
/// key-contains filter.
pub async fn find_bucket_duplicates(
    metadata: &MetadataStore,
    bucket: &str,
    max_results: i32,
    allow_partial: bool,
    continuation_token: Option<&str>,
    key_contains: Option<&str>,
) -> Result<DuplicateReport, S3Error> {
    let status = metadata.get_scan_status().await?;
    let scan_complete = status.total_files == 0 || status.files_at_level_3 >= status.total_files;

    if !scan_complete && !allow_partial {
        return Err(S3Error::ScanPending {
            operation: "FindBucketDuplicates",
            files_pending: status.total_files - status.files_at_level_3,
            percent_complete: if status.total_files > 0 {
                (status.files_at_level_3 as f64 / status.total_files as f64) * 100.0
            } else {
                100.0
            },
        });
    }

    let cursor = continuation_token.and_then(decode_duplicate_cursor);

    // Fetch one extra row to detect truncation.
    let mut duplicates = metadata
        .find_duplicate_hashes(
            max_results + 1,
            cursor.as_ref().map(|(s, h)| (*s, h.as_str())),
            key_contains,
        )
        .await?;

    let is_truncated = duplicates.len() as i32 > max_results;
    if is_truncated {
        duplicates.truncate(max_results as usize);
    }

    let next_continuation_token = if is_truncated {
        duplicates
            .last()
            .map(|d| encode_duplicate_cursor(d.total_size, &d.checksum_sha256))
    } else {
        None
    };

    let mut groups = Vec::new();
    for dup in duplicates {
        let files = metadata.get_objects_by_hash(&dup.checksum_sha256).await?;
        groups.push(DuplicateFileGroup {
            checksum_sha256: dup.checksum_sha256,
            size: files.first().map(|f| f.size.unwrap_or(0)).unwrap_or(0),
            files: files
                .into_iter()
                .map(|f| DuplicateFile {
                    object_id: f.id,
                    key: f.key,
                })
                .collect(),
        });
    }

    Ok(DuplicateReport {
        bucket: bucket.to_string(),
        duplicates: groups,
        is_truncated,
        next_continuation_token,
        scan_complete,
    })
}

fn encode_duplicate_cursor(total_size: i64, checksum: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", total_size, checksum))
}

fn decode_duplicate_cursor(token: &str) -> Option<(i64, String)> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let (size_str, hash) = s.split_once(':')?;
    Some((size_str.parse().ok()?, hash.to_string()))
}

// ── Cross-Bucket Duplicates (8.2) ───────────────────────────────────

const PAGE_SIZE: i32 = 500;

struct BucketCursor {
    bucket_name: String,
    metadata: MetadataStore,
    last_hash: Option<String>,
    buffer: VecDeque<ObjectRecord>,
    current: Option<ObjectRecord>,
    exhausted: bool,
}

impl BucketCursor {
    async fn open(metadata: &MetadataStore, bucket: &str) -> Result<Self, S3Error> {
        let mut cursor = Self {
            bucket_name: bucket.to_string(),
            metadata: metadata.clone(),
            last_hash: None,
            buffer: VecDeque::new(),
            current: None,
            exhausted: false,
        };

        cursor.fill_buffer().await?;
        cursor.pop_next();
        Ok(cursor)
    }

    async fn fill_buffer(&mut self) -> Result<(), S3Error> {
        if self.exhausted {
            return Ok(());
        }

        let page = self
            .metadata
            .fetch_objects_by_hash_page(self.last_hash.as_deref(), PAGE_SIZE)
            .await?;

        if (page.len() as i32) < PAGE_SIZE {
            self.exhausted = true;
        }

        if let Some(last) = page.last() {
            self.last_hash = last.checksum_sha256.clone();
        }

        self.buffer.extend(page);
        Ok(())
    }

    fn pop_next(&mut self) {
        self.current = self.buffer.pop_front();
    }

    fn current_hash(&self) -> Option<String> {
        self.current
            .as_ref()
            .and_then(|o| o.checksum_sha256.clone())
    }

    fn current_key(&self) -> Option<String> {
        self.current.as_ref().map(|o| o.key.clone())
    }

    fn current_object_id(&self) -> Option<String> {
        self.current.as_ref().map(|o| o.id.clone())
    }

    async fn advance(&mut self) -> bool {
        self.pop_next();
        if self.current.is_none() && !self.exhausted && self.fill_buffer().await.is_ok() {
            self.pop_next();
        }
        self.current.is_some()
    }
}

impl Ord for BucketCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap by checksum_sha256 (reversed for BinaryHeap which is a max-heap)
        other.current_hash().cmp(&self.current_hash())
    }
}

impl PartialOrd for BucketCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BucketCursor {
    fn eq(&self, other: &Self) -> bool {
        self.current_hash() == other.current_hash()
    }
}

impl Eq for BucketCursor {}

/// Find duplicates across all buckets using streaming merge.
pub async fn find_cross_bucket_duplicates(
    buckets: &[(&str, &MetadataStore)],
    max_results: i32,
) -> Result<CrossBucketDuplicateReport, S3Error> {
    let mut cursors: BinaryHeap<BucketCursor> = BinaryHeap::new();

    for (name, metadata) in buckets {
        let cursor = BucketCursor::open(metadata, name).await?;
        if cursor.current.is_some() {
            cursors.push(cursor);
        }
    }

    let mut groups = Vec::new();

    while let Some(target_hash) = cursors.peek().and_then(|c| c.current_hash()) {
        let mut files = Vec::new();

        // Collect all files with this hash from any bucket
        while cursors
            .peek()
            .and_then(|c| c.current_hash())
            .map(|h| h == target_hash)
            .unwrap_or(false)
        {
            let mut cursor = cursors.pop().unwrap();
            files.push(CrossBucketFile {
                bucket: cursor.bucket_name.clone(),
                object_id: cursor.current_object_id().unwrap_or_default(),
                key: cursor.current_key().unwrap_or_default(),
            });

            if cursor.advance().await {
                cursors.push(cursor);
            }
        }

        // Only emit if multiple files share this hash
        if files.len() > 1 {
            groups.push(CrossBucketDuplicateGroup {
                checksum_sha256: target_hash,
                files,
            });

            if groups.len() >= max_results as usize {
                break;
            }
        }
    }

    Ok(CrossBucketDuplicateReport {
        duplicates: groups,
        is_truncated: !cursors.is_empty(),
    })
}

// ── Directory Hashing (8.8) ─────────────────────────────────────────

/// Compute and upsert the directory hash for a single parent directory.
/// Returns `Ok(true)` if the hash was computed, `Ok(false)` if skipped
/// (e.g. no direct children or not all children have checksums yet).
pub async fn compute_single_directory_hash(
    metadata: &MetadataStore,
    parent_dir: &str,
) -> Result<bool, S3Error> {
    let prefix = if parent_dir.is_empty() {
        String::new()
    } else {
        format!("{}/", parent_dir)
    };

    let files = metadata.get_objects_with_prefix(&prefix).await?;
    // Only include direct children (not nested)
    let direct_children: Vec<&ObjectRecord> = files
        .iter()
        .filter(|f| {
            let suffix = f.key.strip_prefix(&prefix).unwrap_or(&f.key);
            !suffix.contains('/')
        })
        .collect();

    if direct_children.is_empty() {
        return Ok(false);
    }

    // Build sorted list of (relative_key, checksum_sha256) for hashing
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    let mut all_hashed = true;
    for f in &direct_children {
        let rel_key = f.key.strip_prefix(&prefix).unwrap_or(&f.key);
        if let Some(ref hash) = f.checksum_sha256 {
            pairs.push((rel_key, hash.as_str()));
        } else {
            all_hashed = false;
        }
    }

    if !all_hashed || pairs.is_empty() {
        return Ok(false);
    }

    pairs.sort_by_key(|(k, _)| *k);

    let mut hasher = Sha256::new();
    for (key, hash) in &pairs {
        hasher.update(key.as_bytes());
        hasher.update(b":");
        hasher.update(hash.as_bytes());
        hasher.update(b"\n");
    }
    let dir_hash = hex::encode(hasher.finalize());

    let total_size: i64 = direct_children.iter().map(|f| f.size.unwrap_or(0)).sum();

    let record = DirectoryHashRecord {
        id: uuid::Uuid::new_v4().to_string(),
        prefix: prefix.clone(),
        dir_hash,
        file_count: direct_children.len() as i32,
        total_size,
        computed_at: time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        stale: false,
    };

    metadata.upsert_directory_hash(&record).await?;
    Ok(true)
}

/// Recompute directory hashes for all parent directories that are missing or stale.
pub async fn recompute_stale_directory_hashes(metadata: &MetadataStore) -> Result<(), S3Error> {
    let parents = metadata.list_unhashed_parent_directories().await?;

    for parent in parents {
        compute_single_directory_hash(metadata, &parent).await?;
    }

    Ok(())
}

/// Find duplicate directories within a single bucket.
pub async fn find_bucket_duplicate_dirs(
    metadata: &MetadataStore,
    bucket: &str,
    min_files: i32,
    max_results: i32,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
) -> Result<DuplicateDirReport, S3Error> {
    // Recompute stale/missing directory hashes first
    recompute_stale_directory_hashes(metadata).await?;

    let cursor = continuation_token.and_then(decode_dir_cursor);

    // Fetch one extra to detect truncation.
    let mut dup_groups = metadata
        .find_duplicate_dir_hashes(min_files, max_results + 1, prefix, cursor.as_deref())
        .await?;

    let is_truncated = dup_groups.len() as i32 > max_results;
    if is_truncated {
        dup_groups.truncate(max_results as usize);
    }

    let next_continuation_token = if is_truncated {
        dup_groups.last().map(|g| encode_dir_cursor(&g.dir_hash))
    } else {
        None
    };

    let mut groups = Vec::new();
    for dg in dup_groups {
        let dirs = metadata.get_dirs_by_hash(&dg.dir_hash).await?;
        groups.push(DuplicateDirGroup {
            dir_hash: dg.dir_hash,
            dirs: dirs
                .into_iter()
                .map(|d| DuplicateDir {
                    prefix: d.prefix,
                    file_count: d.file_count,
                    total_size: d.total_size,
                })
                .collect(),
        });
    }

    Ok(DuplicateDirReport {
        bucket: bucket.to_string(),
        duplicate_dirs: groups,
        is_truncated,
        next_continuation_token,
    })
}

fn encode_dir_cursor(dir_hash: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(dir_hash)
}

fn decode_dir_cursor(token: &str) -> Option<String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(token)
        .ok()?;
    String::from_utf8(decoded).ok()
}

// ── Compare Directories (8.7) ───────────────────────────────────────

/// Compare two directories across buckets.
pub async fn compare_dirs(
    left_metadata: &MetadataStore,
    left_bucket: &str,
    left_path: &str,
    right_metadata: &MetadataStore,
    right_bucket: &str,
    right_path: &str,
) -> Result<DirComparison, S3Error> {
    let left_files = left_metadata.get_objects_with_prefix(left_path).await?;
    let right_files = right_metadata.get_objects_with_prefix(right_path).await?;

    let left_map: HashMap<String, &ObjectRecord> = left_files
        .iter()
        .map(|f| {
            (
                f.key.strip_prefix(left_path).unwrap_or(&f.key).to_string(),
                f,
            )
        })
        .collect();

    let right_map: HashMap<String, &ObjectRecord> = right_files
        .iter()
        .map(|f| {
            (
                f.key.strip_prefix(right_path).unwrap_or(&f.key).to_string(),
                f,
            )
        })
        .collect();

    let all_keys: HashSet<&String> = left_map.keys().chain(right_map.keys()).collect();

    let mut differences = Vec::new();
    let mut identical = 0usize;

    for key in all_keys {
        match (left_map.get(key), right_map.get(key)) {
            (Some(left), Some(right)) => {
                if left.checksum_sha256 == right.checksum_sha256 {
                    identical += 1;
                } else {
                    differences.push(FileDifference {
                        key: key.clone(),
                        status: "modified".to_string(),
                        left_hash: left.checksum_sha256.clone(),
                        right_hash: right.checksum_sha256.clone(),
                    });
                }
            }
            (Some(_), None) => {
                differences.push(FileDifference {
                    key: key.clone(),
                    status: "only_in_left".to_string(),
                    ..Default::default()
                });
            }
            (None, Some(_)) => {
                differences.push(FileDifference {
                    key: key.clone(),
                    status: "only_in_right".to_string(),
                    ..Default::default()
                });
            }
            (None, None) => unreachable!(),
        }
    }

    Ok(DirComparison {
        left: DirRef {
            bucket: left_bucket.to_string(),
            path: left_path.to_string(),
        },
        right: DirRef {
            bucket: right_bucket.to_string(),
            path: right_path.to_string(),
        },
        identical: identical == left_files.len()
            && identical == right_files.len()
            && !left_files.is_empty(),
        summary: ComparisonSummary {
            files_identical: identical,
            files_only_in_left: differences
                .iter()
                .filter(|d| d.status == "only_in_left")
                .count(),
            files_only_in_right: differences
                .iter()
                .filter(|d| d.status == "only_in_right")
                .count(),
            files_different: differences
                .iter()
                .filter(|d| d.status == "modified")
                .count(),
        },
        differences,
    })
}
