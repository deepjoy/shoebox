use std::collections::BTreeSet;
use std::path::Path;
use std::pin::Pin;

use futures::stream::TryStreamExt;
use futures::Stream;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::error::S3Error;
use crate::types::ChecksumValues;

/// Batch update entry for L3 content hashes.
pub struct L3HashUpdate {
    pub key: String,
    pub etag: String,
    pub checksums: ChecksumValues,
    pub scan_level: i32,
}

/// A single entry from a streaming list operation.
#[derive(Debug, Clone)]
pub enum ListEntry {
    /// A concrete object.
    Object(Box<ObjectRecord>),
    /// A collapsed common prefix (only emitted when a delimiter is provided).
    CommonPrefix(String),
}

/// Object metadata record, matching the `objects` table schema.
///
/// Timestamp fields use `time::OffsetDateTime`. The sqlx `time` feature
/// serialises these as RFC 3339 TEXT in SQLite, giving direct comparisons
/// without runtime parsing and human-readable storage.
///
/// ## ETag Convention
/// The `etag` field stores values WITH surrounding double-quote characters,
/// e.g. `"\"d41d8cd98f00b204e9800998ecf8427e\""`. This matches the S3 wire
/// format where ETags are always quoted per RFC 7232. Code should store
/// and return the value as-is without adding or stripping quotes.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ObjectRecord {
    pub id: String,
    pub key: String,
    pub parent_directory: String,
    pub is_directory: bool,
    pub is_symlink: bool,
    pub symlink_target: Option<String>,

    // L2 metadata (None until scanned)
    pub size: Option<i64>,
    pub file_mtime: Option<time::OffsetDateTime>,
    pub file_ctime: Option<time::OffsetDateTime>,
    pub inode: Option<i64>,
    pub device_id: Option<i64>,

    // L3 metadata (None until content-hashed)
    pub etag: Option<String>,

    // S3 checksums (base64-encoded, None until content-hashed)
    pub checksum_sha256: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,

    // S3 metadata
    pub content_type: Option<String>,
    pub last_modified: time::OffsetDateTime,
    pub created_at: time::OffsetDateTime,
    pub metadata: Option<String>,

    pub scan_level: i32,
}

impl Default for ObjectRecord {
    fn default() -> Self {
        Self {
            id: String::new(),
            key: String::new(),
            parent_directory: String::new(),
            is_directory: false,
            is_symlink: false,
            symlink_target: None,
            size: None,
            file_mtime: None,
            file_ctime: None,
            inode: None,
            device_id: None,
            etag: None,
            checksum_sha256: None,
            checksum_sha1: None,
            checksum_crc32: None,
            checksum_crc32c: None,
            content_type: None,
            last_modified: time::OffsetDateTime::UNIX_EPOCH,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            metadata: None,
            scan_level: 0,
        }
    }
}

/// SQLite-backed metadata store for a single bucket.
#[derive(Clone)]
pub struct MetadataStore {
    pool: SqlitePool,
}

impl MetadataStore {
    /// Open (or create) the metadata database at the given path and run migrations.
    pub async fn new(db_path: &Path) -> Result<Self, S3Error> {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .pragma("synchronous", "normal")
            .pragma("cache_size", "-64000")
            .pragma("journal_size_limit", "67108864")
            .pragma("temp_store", "memory")
            .busy_timeout(std::time::Duration::from_secs(5));

        // TODO(#4): Make max_connections configurable (per-bucket or global).
        // https://github.com/deepjoy/shoebox/issues/4
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| {
                tracing::error!("Failed to connect to database: {e}");
                S3Error::InternalError
            })?;

        // Run migrations from the compiled-in migrations directory
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to run migrations: {e}");
                S3Error::InternalError
            })?;

        Ok(Self { pool })
    }

    // TODO(#5): Replace `SELECT *` with explicit column lists in get/list queries
    // to avoid loading heavy columns (e.g. `metadata` JSON) when not needed.
    // https://github.com/deepjoy/shoebox/issues/5

    /// Retrieve an object record by key.
    pub async fn get_object(&self, key: &str) -> Result<Option<ObjectRecord>, S3Error> {
        let record = sqlx::query_as::<_, ObjectRecord>("SELECT * FROM objects WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        Ok(record)
    }

    /// Insert a new object record.
    pub async fn insert_object(&self, obj: &ObjectRecord) -> Result<(), S3Error> {
        sqlx::query(
            r#"INSERT INTO objects (
                id, key, parent_directory, is_directory, is_symlink, symlink_target,
                size, file_mtime, file_ctime, inode, device_id,
                etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                content_type, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&obj.id)
        .bind(&obj.key)
        .bind(&obj.parent_directory)
        .bind(obj.is_directory)
        .bind(obj.is_symlink)
        .bind(&obj.symlink_target)
        .bind(obj.size)
        .bind(obj.file_mtime)
        .bind(obj.file_ctime)
        .bind(obj.inode)
        .bind(obj.device_id)
        .bind(&obj.etag)
        .bind(&obj.checksum_sha256)
        .bind(&obj.checksum_sha1)
        .bind(&obj.checksum_crc32)
        .bind(&obj.checksum_crc32c)
        .bind(&obj.content_type)
        .bind(obj.last_modified)
        .bind(obj.created_at)
        .bind(&obj.metadata)
        .bind(obj.scan_level)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert multiple object records in a single transaction.
    pub async fn insert_objects_batch(&self, objects: &[ObjectRecord]) -> Result<(), S3Error> {
        if objects.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        for obj in objects {
            sqlx::query(
                r#"INSERT INTO objects (
                    id, key, parent_directory, is_directory, is_symlink, symlink_target,
                    size, file_mtime, file_ctime, inode, device_id,
                    etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                    content_type, last_modified, created_at, metadata, scan_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&obj.id)
            .bind(&obj.key)
            .bind(&obj.parent_directory)
            .bind(obj.is_directory)
            .bind(obj.is_symlink)
            .bind(&obj.symlink_target)
            .bind(obj.size)
            .bind(obj.file_mtime)
            .bind(obj.file_ctime)
            .bind(obj.inode)
            .bind(obj.device_id)
            .bind(&obj.etag)
            .bind(&obj.checksum_sha256)
            .bind(&obj.checksum_sha1)
            .bind(&obj.checksum_crc32)
            .bind(&obj.checksum_crc32c)
            .bind(&obj.content_type)
            .bind(obj.last_modified)
            .bind(obj.created_at)
            .bind(&obj.metadata)
            .bind(obj.scan_level)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Insert or update an object record, keyed by `key`.
    ///
    /// On conflict the existing row's `id` and `created_at` are preserved
    /// (i.e. the values in the supplied `ObjectRecord` are ignored for those
    /// two columns on update). All other fields are overwritten.
    pub async fn upsert_object(&self, obj: &ObjectRecord) -> Result<(), S3Error> {
        sqlx::query(
            r#"INSERT INTO objects (
                id, key, parent_directory, is_directory, is_symlink, symlink_target,
                size, file_mtime, file_ctime, inode, device_id,
                etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                content_type, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                parent_directory = excluded.parent_directory,
                is_directory = excluded.is_directory,
                is_symlink = excluded.is_symlink,
                symlink_target = excluded.symlink_target,
                size = excluded.size,
                file_mtime = excluded.file_mtime,
                file_ctime = excluded.file_ctime,
                inode = excluded.inode,
                device_id = excluded.device_id,
                etag = excluded.etag,
                checksum_sha256 = excluded.checksum_sha256,
                checksum_sha1 = excluded.checksum_sha1,
                checksum_crc32 = excluded.checksum_crc32,
                checksum_crc32c = excluded.checksum_crc32c,
                content_type = excluded.content_type,
                last_modified = excluded.last_modified,
                metadata = excluded.metadata,
                scan_level = excluded.scan_level"#,
        )
        .bind(&obj.id)
        .bind(&obj.key)
        .bind(&obj.parent_directory)
        .bind(obj.is_directory)
        .bind(obj.is_symlink)
        .bind(&obj.symlink_target)
        .bind(obj.size)
        .bind(obj.file_mtime)
        .bind(obj.file_ctime)
        .bind(obj.inode)
        .bind(obj.device_id)
        .bind(&obj.etag)
        .bind(&obj.checksum_sha256)
        .bind(&obj.checksum_sha1)
        .bind(&obj.checksum_crc32)
        .bind(&obj.checksum_crc32c)
        .bind(&obj.content_type)
        .bind(obj.last_modified)
        .bind(obj.created_at)
        .bind(&obj.metadata)
        .bind(obj.scan_level)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete an object record by key. Returns true if a row was deleted.
    pub async fn delete_object(&self, key: &str) -> Result<bool, S3Error> {
        let result = sqlx::query("DELETE FROM objects WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Bulk-delete object records by key in a single SQL statement.
    pub async fn delete_objects(&self, keys: &[String]) -> Result<u64, S3Error> {
        if keys.is_empty() {
            return Ok(0);
        }
        // Build `DELETE FROM objects WHERE key IN (?, ?, ...)`
        let placeholders: Vec<&str> = keys.iter().map(|_| "?").collect();
        let sql = format!(
            "DELETE FROM objects WHERE key IN ({})",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for key in keys {
            query = query.bind(key);
        }
        let result = query.execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// List objects matching a prefix, up to `max_keys`.
    pub async fn list_objects(
        &self,
        prefix: &str,
        max_keys: i32,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let records = sqlx::query_as::<_, ObjectRecord>(
            "SELECT * FROM objects WHERE key LIKE ? ESCAPE '\\' ORDER BY key LIMIT ?",
        )
        .bind(&pattern)
        .bind(max_keys)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Stream list entries matching a prefix, ordered by key.
    ///
    /// When `delimiter` is `None`, every matching row is yielded as
    /// `ListEntry::Object`. When a delimiter is provided, keys that contain
    /// the delimiter after the prefix are collapsed into
    /// `ListEntry::CommonPrefix` (each unique prefix emitted exactly once).
    ///
    /// Unlike `list_objects` / `list_objects_v2`, this does not load all
    /// results into memory. The caller controls consumption by dropping the
    /// stream or using `.take(n)`.
    pub fn list_objects_stream(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
    ) -> Pin<Box<dyn Stream<Item = Result<ListEntry, S3Error>> + Send + '_>> {
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped}%");

        let raw_stream: Pin<Box<dyn Stream<Item = Result<ObjectRecord, sqlx::Error>> + Send + '_>> =
            if let Some(after) = start_after {
                let after = after.to_string();
                Box::pin(
                    sqlx::query_as::<_, ObjectRecord>(
                        "SELECT * FROM objects WHERE key LIKE ? ESCAPE '\\' AND key > ? ORDER BY key",
                    )
                    .bind(pattern)
                    .bind(after)
                    .fetch(&self.pool),
                )
            } else {
                Box::pin(
                    sqlx::query_as::<_, ObjectRecord>(
                        "SELECT * FROM objects WHERE key LIKE ? ESCAPE '\\' ORDER BY key",
                    )
                    .bind(pattern)
                    .fetch(&self.pool),
                )
            };

        match delimiter {
            None => {
                // Flat listing — every row is an object.
                Box::pin(
                    raw_stream
                        .map_ok(|r| ListEntry::Object(Box::new(r)))
                        .map_err(S3Error::from),
                )
            }
            Some(delim) => {
                // Delimiter grouping — collapse matching keys into common prefixes,
                // deduplicating so each prefix is emitted only once.
                let prefix_owned = prefix.to_string();
                let delim_owned = delim.to_string();
                let prefix_len = prefix.len();

                Box::pin(futures::stream::try_unfold(
                    (
                        raw_stream,
                        BTreeSet::new(),
                        prefix_owned,
                        delim_owned,
                        prefix_len,
                    ),
                    |(mut stream, mut seen, pfx, delim, pfx_len)| async move {
                        loop {
                            match stream.try_next().await.map_err(S3Error::from)? {
                                None => return Ok(None),
                                Some(record) => {
                                    let suffix = &record.key[pfx_len..];
                                    if let Some(pos) = suffix.find(delim.as_str()) {
                                        let cp = format!("{}{}", pfx, &suffix[..pos + delim.len()]);
                                        if seen.insert(cp.clone()) {
                                            return Ok(Some((
                                                ListEntry::CommonPrefix(cp),
                                                (stream, seen, pfx, delim, pfx_len),
                                            )));
                                        }
                                        // Already emitted this common prefix — skip row.
                                    } else {
                                        return Ok(Some((
                                            ListEntry::Object(Box::new(record)),
                                            (stream, seen, pfx, delim, pfx_len),
                                        )));
                                    }
                                }
                            }
                        }
                    },
                ))
            }
        }
    }

    /// List objects within a specific parent directory.
    pub async fn list_by_parent(&self, parent: &str) -> Result<Vec<ObjectRecord>, S3Error> {
        let records = sqlx::query_as::<_, ObjectRecord>(
            "SELECT * FROM objects WHERE parent_directory = ? ORDER BY key",
        )
        .bind(parent)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Gracefully close the metadata store, flushing WAL to the main database.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// ListObjectsV2-compatible query with prefix, delimiter, pagination.
    ///
    /// Returns `(objects, common_prefixes, is_truncated, next_continuation_token)`.
    pub async fn list_objects_v2(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i32,
        start_after: Option<&str>,
    ) -> Result<(Vec<ObjectRecord>, Vec<String>, bool, Option<String>), S3Error> {
        let escaped_prefix = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{escaped_prefix}%");
        let max = max_keys as usize;

        // No delimiter: flat list with one extra row for truncation detection.
        let Some(delim) = delimiter else {
            let limit = max_keys as i64 + 1;
            let records = self.fetch_page(&pattern, start_after, limit).await?;
            let is_truncated = records.len() > max;
            let mut objects = records;
            if is_truncated {
                objects.truncate(max);
            }
            let next_token = if is_truncated {
                objects.last().map(|o| o.key.clone())
            } else {
                None
            };
            return Ok((objects, Vec::new(), is_truncated, next_token));
        };

        // With delimiter: requery loop to fill the page.
        // We collapse keys into common prefixes which reduces the count,
        // so a single fetch may not yield enough entries to fill max_keys.
        let prefix_len = prefix.len();
        let mut objects = Vec::new();
        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut cursor = start_after.map(|s| s.to_string());
        let batch_size = (max_keys as i64 + 1).max(100);

        loop {
            let records = self
                .fetch_page(&pattern, cursor.as_deref(), batch_size)
                .await?;
            let exhausted = (records.len() as i64) < batch_size;

            for record in records {
                let count = objects.len() + common_prefixes.len();
                if count > max {
                    // We have enough to detect truncation; stop.
                    break;
                }

                cursor = Some(record.key.clone());
                let suffix = &record.key[prefix_len..];
                if let Some(pos) = suffix.find(delim) {
                    let cp = format!("{}{}", prefix, &suffix[..pos + delim.len()]);
                    common_prefixes.insert(cp);
                } else {
                    objects.push(record);
                }
            }

            let count = objects.len() + common_prefixes.len();
            if count > max || exhausted {
                break;
            }
        }

        let count = objects.len() + common_prefixes.len();
        let is_truncated = count > max;

        // Trim to exactly max_keys entries. Remove excess objects first
        // (common prefixes sort earlier and are typically fewer).
        if is_truncated {
            while objects.len() + common_prefixes.len() > max {
                // Pop the lexicographically last item.
                let last_obj = objects.last().map(|o| o.key.as_str());
                let last_cp = common_prefixes.iter().next_back().map(|s| s.as_str());
                match (last_obj, last_cp) {
                    (Some(o), Some(c)) if o > c => {
                        objects.pop();
                    }
                    (_, Some(_)) => {
                        common_prefixes.pop_last();
                    }
                    (Some(_), None) => {
                        objects.pop();
                    }
                    _ => break,
                }
            }
        }

        // The continuation token is the last key in sort order among the
        // items we're returning. For common prefixes, the token must be
        // the last *actual key* that falls under that prefix so that the
        // next page starts after all keys in the group.
        let next_token = if is_truncated { cursor } else { None };
        let cp_vec: Vec<String> = common_prefixes.into_iter().collect();

        Ok((objects, cp_vec, is_truncated, next_token))
    }

    /// Fetch a page of records matching a LIKE pattern, optionally after a cursor key.
    async fn fetch_page(
        &self,
        pattern: &str,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        if let Some(after) = after {
            sqlx::query_as::<_, ObjectRecord>(
                "SELECT * FROM objects WHERE key LIKE ? ESCAPE '\\' AND key > ? ORDER BY key LIMIT ?",
            )
            .bind(pattern)
            .bind(after)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
        } else {
            sqlx::query_as::<_, ObjectRecord>(
                "SELECT * FROM objects WHERE key LIKE ? ESCAPE '\\' ORDER BY key LIMIT ?",
            )
            .bind(pattern)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
        }
    }

    /// Get the total count of objects in the store.
    pub async fn count_objects(&self) -> Result<i64, S3Error> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }

    /// Get all tags for an object (looked up by key).
    pub async fn rename_object(&self, src_key: &str, dst_key: &str) -> Result<(), S3Error> {
        let parent = dst_key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();
        let now = time::OffsetDateTime::now_utc();
        let result = sqlx::query(
            "UPDATE objects SET key = ?, parent_directory = ?, last_modified = ? WHERE key = ?",
        )
        .bind(dst_key)
        .bind(&parent)
        .bind(now)
        .bind(src_key)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(S3Error::NoSuchKey);
        }
        Ok(())
    }

    pub async fn get_object_tags(&self, key: &str) -> Result<Vec<Tag>, S3Error> {
        let tags = sqlx::query_as::<_, Tag>(
            "SELECT t.key, t.value FROM object_tags t \
             INNER JOIN objects o ON t.object_id = o.id \
             WHERE o.key = ? ORDER BY t.key",
        )
        .bind(key)
        .fetch_all(&self.pool)
        .await?;

        Ok(tags)
    }

    /// Insert a single tag for an object (looked up by key).
    pub async fn insert_object_tag(&self, key: &str, tag: &Tag) -> Result<(), S3Error> {
        let object_id: Option<(String,)> = sqlx::query_as("SELECT id FROM objects WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        let (object_id,) = object_id.ok_or(S3Error::NoSuchKey)?;
        let tag_id = uuid::Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO object_tags (id, object_id, key, value) VALUES (?, ?, ?, ?) \
             ON CONFLICT(object_id, key) DO UPDATE SET value = excluded.value",
        )
        .bind(&tag_id)
        .bind(&object_id)
        .bind(&tag.key)
        .bind(&tag.value)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete all tags for an object (looked up by key).
    pub async fn delete_object_tags(&self, key: &str) -> Result<(), S3Error> {
        sqlx::query(
            "DELETE FROM object_tags WHERE object_id IN \
             (SELECT id FROM objects WHERE key = ?)",
        )
        .bind(key)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Scanner Methods (Phase 6)
    // -------------------------------------------------------------------------

    /// Count files and total bytes remaining below a given scan level.
    ///
    /// Returns `(file_count, total_bytes)` for objects with `scan_level < level`.
    /// `total_bytes` sums the `size` column (NULLs are treated as 0).
    pub async fn count_remaining_below_scan_level(
        &self,
        level: i32,
    ) -> Result<(i64, i64), S3Error> {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM objects WHERE scan_level < ?",
        )
        .bind(level)
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }

    /// List object keys with scan_level below the given threshold.
    ///
    /// When `after_key` is provided, only keys lexicographically greater than it
    /// are returned (keyset pagination). This lets continuation jobs skip
    /// directly to unprocessed work instead of re-scanning the index from the
    /// beginning.
    pub async fn list_keys_below_scan_level(
        &self,
        level: i32,
        limit: i64,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, S3Error> {
        let rows: Vec<(String,)> = match after_key {
            Some(key) => {
                sqlx::query_as(
                    "SELECT key FROM objects WHERE scan_level < ? AND key > ? ORDER BY key LIMIT ?",
                )
                .bind(level)
                .bind(key)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as("SELECT key FROM objects WHERE scan_level < ? ORDER BY key LIMIT ?")
                    .bind(level)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?
            }
        };

        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// List object keys with sizes, with scan_level below the given threshold,
    /// accumulating until a byte budget is reached.
    ///
    /// Returns `(keys, exhausted)` where `exhausted` is true when fewer rows
    /// remain than the safety cap (i.e. all remaining work fits in this batch).
    /// Rows are fetched up to a 10,000-row safety cap and accumulated in memory
    /// until `byte_budget` is exceeded.
    pub async fn list_keys_by_byte_budget(
        &self,
        level: i32,
        byte_budget: i64,
        after_key: Option<&str>,
    ) -> Result<(Vec<String>, bool, i64), S3Error> {
        const ROW_CAP: i64 = 10_000;

        let rows: Vec<(String, i64)> = match after_key {
            Some(key) => {
                sqlx::query_as(
                    "SELECT key, COALESCE(size, 0) FROM objects WHERE scan_level < ? AND key > ? ORDER BY key LIMIT ?",
                )
                .bind(level)
                .bind(key)
                .bind(ROW_CAP)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT key, COALESCE(size, 0) FROM objects WHERE scan_level < ? ORDER BY key LIMIT ?",
                )
                .bind(level)
                .bind(ROW_CAP)
                .fetch_all(&self.pool)
                .await?
            }
        };

        let hit_row_cap = rows.len() as i64 >= ROW_CAP;
        let mut keys = Vec::new();
        let mut cumulative: i64 = 0;
        let mut budget_exceeded = false;

        for (key, size) in &rows {
            // Always include at least one file (even if it alone exceeds budget)
            if budget_exceeded {
                break;
            }
            cumulative += size;
            keys.push(key.clone());
            if cumulative >= byte_budget {
                budget_exceeded = true;
            }
        }

        let exhausted = !budget_exceeded && !hit_row_cap;
        Ok((keys, exhausted, cumulative))
    }

    /// Begin an L1 scan session by acquiring a dedicated connection and creating
    /// a temp table to collect discovered disk keys. The returned connection must
    /// be reused for all subsequent `l1_scan_*` calls so the temp table remains
    /// visible.
    pub(crate) async fn l1_scan_begin(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, S3Error> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query(
            "CREATE TEMP TABLE l1_disk (
                key TEXT NOT NULL PRIMARY KEY,
                id TEXT NOT NULL,
                parent_directory TEXT NOT NULL,
                is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
                symlink_target TEXT,
                size INTEGER,
                content_type TEXT,
                last_modified TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
        )
        .execute(&mut *conn)
        .await?;
        Ok(conn)
    }

    /// Batch-insert discovered disk files into the L1 temp table.
    pub(crate) async fn l1_scan_insert_batch(
        conn: &mut sqlx::SqliteConnection,
        records: &[ObjectRecord],
    ) -> Result<(), S3Error> {
        if records.is_empty() {
            return Ok(());
        }
        let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
        for obj in records {
            sqlx::query(
                "INSERT OR IGNORE INTO l1_disk (
                    key, id, parent_directory, is_symlink, symlink_target,
                    size, content_type, last_modified, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&obj.key)
            .bind(&obj.id)
            .bind(&obj.parent_directory)
            .bind(obj.is_symlink)
            .bind(&obj.symlink_target)
            .bind(obj.size)
            .bind(&obj.content_type)
            .bind(obj.last_modified)
            .bind(obj.created_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Merge the L1 temp table into `objects`: insert newly discovered files,
    /// optionally delete stale entries, and return `(discovered, deleted)`.
    /// Drops the temp table when done.
    pub(crate) async fn l1_scan_finish(
        conn: &mut sqlx::SqliteConnection,
        delete_stale: bool,
    ) -> Result<(u64, u64), S3Error> {
        // Insert new objects that exist on disk but not in the catalog
        let inserted = sqlx::query(
            "INSERT INTO objects (
                id, key, parent_directory, is_directory, is_symlink, symlink_target,
                size, content_type, last_modified, created_at, scan_level
            )
            SELECT
                d.id, d.key, d.parent_directory, FALSE, d.is_symlink, d.symlink_target,
                d.size, d.content_type, d.last_modified, d.created_at, 1
            FROM l1_disk d
            WHERE d.key NOT IN (SELECT key FROM objects)",
        )
        .execute(&mut *conn)
        .await?;
        let discovered = inserted.rows_affected();

        // Delete objects that are in the catalog but no longer on disk
        let deleted = if delete_stale {
            let result =
                sqlx::query("DELETE FROM objects WHERE key NOT IN (SELECT key FROM l1_disk)")
                    .execute(&mut *conn)
                    .await?;
            result.rows_affected()
        } else {
            0
        };

        // Clean up
        sqlx::query("DROP TABLE IF EXISTS l1_disk")
            .execute(&mut *conn)
            .await?;

        Ok((discovered, deleted))
    }

    /// Reset an object's scan level (e.g. after a file is modified on disk).
    pub async fn reset_scan_level(&self, key: &str, level: i32) -> Result<(), S3Error> {
        sqlx::query("UPDATE objects SET scan_level = ? WHERE key = ? AND scan_level > ?")
            .bind(level)
            .bind(key)
            .bind(level)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Update L2 metadata for an object (size, timestamps, file identity).
    pub async fn update_object_metadata(
        &self,
        key: &str,
        update: &ObjectMetadataUpdate,
    ) -> Result<(), S3Error> {
        let result = sqlx::query(
            "UPDATE objects SET size = ?, file_mtime = ?, file_ctime = ?, \
             inode = ?, device_id = ?, scan_level = ?, last_modified = ? \
             WHERE key = ? AND scan_level < ?",
        )
        .bind(update.size)
        .bind(update.file_mtime)
        .bind(update.file_ctime)
        .bind(update.inode.map(|v| v as i64))
        .bind(update.device_id.map(|v| v as i64))
        .bind(update.scan_level)
        .bind(time::OffsetDateTime::now_utc())
        .bind(key)
        .bind(update.scan_level)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            tracing::trace!(
                key,
                "L2 update skipped (already at target level or missing)"
            );
        }
        Ok(())
    }

    /// Update L2 metadata for multiple objects in a single transaction.
    pub async fn update_objects_metadata_batch(
        &self,
        updates: &[(String, ObjectMetadataUpdate)],
    ) -> Result<(), S3Error> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let now = time::OffsetDateTime::now_utc();
        for (key, update) in updates {
            sqlx::query(
                "UPDATE objects SET size = ?, file_mtime = ?, file_ctime = ?, \
                 inode = ?, device_id = ?, scan_level = ?, last_modified = ? \
                 WHERE key = ? AND scan_level < ?",
            )
            .bind(update.size)
            .bind(update.file_mtime)
            .bind(update.file_ctime)
            .bind(update.inode.map(|v| v as i64))
            .bind(update.device_id.map(|v| v as i64))
            .bind(update.scan_level)
            .bind(now)
            .bind(key.as_str())
            .bind(update.scan_level)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Update L3 content hashes for an object.
    pub async fn update_object_hashes(
        &self,
        key: &str,
        etag: &str,
        checksums: &crate::types::ChecksumValues,
        scan_level: i32,
    ) -> Result<(), S3Error> {
        let result = sqlx::query(
            "UPDATE objects SET etag = ?, \
             checksum_sha256 = ?, checksum_sha1 = ?, checksum_crc32 = ?, checksum_crc32c = ?, \
             scan_level = ?, last_modified = ? \
             WHERE key = ? AND scan_level < ?",
        )
        .bind(etag)
        .bind(&checksums.sha256)
        .bind(&checksums.sha1)
        .bind(&checksums.crc32)
        .bind(&checksums.crc32c)
        .bind(scan_level)
        .bind(time::OffsetDateTime::now_utc())
        .bind(key)
        .bind(scan_level)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            tracing::trace!(
                key,
                "L3 update skipped (already at target level or missing)"
            );
        }
        Ok(())
    }

    /// Promote scan_level for symlinks that don't need content hashing.
    pub async fn promote_scan_level_batch(
        &self,
        keys: &[String],
        target_level: i32,
    ) -> Result<(), S3Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let now = time::OffsetDateTime::now_utc();
        for key in keys {
            sqlx::query(
                "UPDATE objects SET scan_level = ?, last_modified = ? \
                 WHERE key = ? AND scan_level < ?",
            )
            .bind(target_level)
            .bind(now)
            .bind(key.as_str())
            .bind(target_level)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Update L3 content hashes for multiple objects in a single transaction.
    pub async fn update_objects_hashes_batch(
        &self,
        updates: &[L3HashUpdate],
    ) -> Result<(), S3Error> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let now = time::OffsetDateTime::now_utc();
        for update in updates {
            sqlx::query(
                "UPDATE objects SET etag = ?, \
                 checksum_sha256 = ?, checksum_sha1 = ?, checksum_crc32 = ?, checksum_crc32c = ?, \
                 scan_level = ?, last_modified = ? \
                 WHERE key = ? AND scan_level < ?",
            )
            .bind(&update.etag)
            .bind(&update.checksums.sha256)
            .bind(&update.checksums.sha1)
            .bind(&update.checksums.crc32)
            .bind(&update.checksums.crc32c)
            .bind(update.scan_level)
            .bind(now)
            .bind(update.key.as_str())
            .bind(update.scan_level)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Multipart Upload Methods (Phase 5)
    // -------------------------------------------------------------------------

    /// Insert a new multipart upload record.
    pub async fn insert_multipart_upload(
        &self,
        upload: &crate::types::multipart::MultipartUpload,
    ) -> Result<(), S3Error> {
        sqlx::query(
            "INSERT INTO multipart_uploads (id, key, initiated_at, content_type, metadata) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&upload.id)
        .bind(&upload.key)
        .bind(&upload.initiated_at)
        .bind(&upload.content_type)
        .bind(&upload.metadata)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get a multipart upload by ID.
    pub async fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Result<Option<crate::types::multipart::MultipartUpload>, S3Error> {
        let upload = sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
            "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads WHERE id = ?",
        )
        .bind(upload_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(upload)
    }

    /// Delete a multipart upload (cascades to parts).
    pub async fn delete_multipart_upload(&self, upload_id: &str) -> Result<(), S3Error> {
        sqlx::query("DELETE FROM multipart_uploads WHERE id = ?")
            .bind(upload_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Insert or update a part record.
    pub async fn upsert_part(&self, part: &crate::types::multipart::Part) -> Result<(), S3Error> {
        sqlx::query(
            "INSERT INTO parts (id, upload_id, part_number, size, etag, uploaded_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(upload_id, part_number) DO UPDATE SET \
                size = excluded.size, \
                etag = excluded.etag, \
                uploaded_at = excluded.uploaded_at",
        )
        .bind(&part.id)
        .bind(&part.upload_id)
        .bind(part.part_number)
        .bind(part.size)
        .bind(&part.etag)
        .bind(&part.uploaded_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// List all parts for a multipart upload.
    pub async fn list_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<crate::types::multipart::Part>, S3Error> {
        let parts = sqlx::query_as::<_, crate::types::multipart::Part>(
            "SELECT id, upload_id, part_number, size, etag, uploaded_at FROM parts \
             WHERE upload_id = ? ORDER BY part_number",
        )
        .bind(upload_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(parts)
    }

    /// List parts with pagination.
    pub async fn list_parts_paginated(
        &self,
        upload_id: &str,
        max_parts: i32,
        part_number_marker: Option<i32>,
    ) -> Result<Vec<crate::types::multipart::Part>, S3Error> {
        let parts = if let Some(marker) = part_number_marker {
            sqlx::query_as::<_, crate::types::multipart::Part>(
                "SELECT id, upload_id, part_number, size, etag, uploaded_at FROM parts \
                 WHERE upload_id = ? AND part_number > ? ORDER BY part_number LIMIT ?",
            )
            .bind(upload_id)
            .bind(marker)
            .bind(max_parts)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, crate::types::multipart::Part>(
                "SELECT id, upload_id, part_number, size, etag, uploaded_at FROM parts \
                 WHERE upload_id = ? ORDER BY part_number LIMIT ?",
            )
            .bind(upload_id)
            .bind(max_parts)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(parts)
    }

    /// List all multipart uploads, optionally filtered by prefix.
    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        max_uploads: i32,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<Vec<crate::types::multipart::MultipartUpload>, S3Error> {
        let uploads = match (prefix, key_marker, upload_id_marker) {
            (Some(pfx), Some(km), Some(um)) => {
                let pattern = format!("{}%", pfx.replace('%', "\\%").replace('_', "\\_"));
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     WHERE key LIKE ? ESCAPE '\\' AND (key > ? OR (key = ? AND id > ?)) \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(&pattern)
                .bind(km)
                .bind(km)
                .bind(um)
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            (Some(pfx), Some(km), None) => {
                let pattern = format!("{}%", pfx.replace('%', "\\%").replace('_', "\\_"));
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     WHERE key LIKE ? ESCAPE '\\' AND key > ? \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(&pattern)
                .bind(km)
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            (Some(pfx), None, None) => {
                let pattern = format!("{}%", pfx.replace('%', "\\%").replace('_', "\\_"));
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     WHERE key LIKE ? ESCAPE '\\' \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(&pattern)
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            (None, Some(km), Some(um)) => {
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     WHERE (key > ? OR (key = ? AND id > ?)) \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(km)
                .bind(km)
                .bind(um)
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            (None, Some(km), None) => {
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     WHERE key > ? \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(km)
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            (None, None, None) => {
                sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
                    "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
                     ORDER BY key, id LIMIT ?",
                )
                .bind(max_uploads)
                .fetch_all(&self.pool)
                .await?
            }
            _ => Vec::new(), // Invalid marker combination
        };

        Ok(uploads)
    }

    /// Find abandoned multipart uploads older than cutoff time.
    pub async fn find_abandoned_uploads(
        &self,
        cutoff: &str,
    ) -> Result<Vec<crate::types::multipart::MultipartUpload>, S3Error> {
        let uploads = sqlx::query_as::<_, crate::types::multipart::MultipartUpload>(
            "SELECT id, key, initiated_at, content_type, metadata FROM multipart_uploads \
             WHERE initiated_at < ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;

        Ok(uploads)
    }
}

/// L2 metadata update payload (used by scanner).
pub struct ObjectMetadataUpdate {
    pub size: i64,
    pub file_mtime: Option<time::OffsetDateTime>,
    pub file_ctime: Option<time::OffsetDateTime>,
    pub inode: Option<u64>,
    pub device_id: Option<u64>,
    pub scan_level: i32,
}

/// Object tag key-value pair.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    async fn make_store(tmp: &TempDir) -> MetadataStore {
        let db_path = tmp.path().join("test.db");
        MetadataStore::new(&db_path).await.unwrap()
    }

    fn make_record(key: &str) -> ObjectRecord {
        let now = OffsetDateTime::now_utc();
        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();

        ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            key: key.to_string(),
            parent_directory: parent,
            size: Some(1024),
            file_mtime: Some(now),
            content_type: Some("application/octet-stream".to_string()),
            last_modified: now,
            created_at: now,
            scan_level: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let obj = make_record("photos/cat.jpg");
        store.insert_object(&obj).await.unwrap();

        let fetched = store.get_object("photos/cat.jpg").await.unwrap().unwrap();
        assert_eq!(fetched.key, "photos/cat.jpg");
        assert_eq!(fetched.size, Some(1024));
        assert!(!fetched.is_symlink);
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let result = store.get_object("nope.txt").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_object() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let obj = make_record("to-delete.txt");
        store.insert_object(&obj).await.unwrap();

        let deleted = store.delete_object("to-delete.txt").await.unwrap();
        assert!(deleted);

        let result = store.get_object("to-delete.txt").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_returns_false() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let deleted = store.delete_object("nope.txt").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_list_objects_by_prefix() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        store
            .insert_object(&make_record("photos/a.jpg"))
            .await
            .unwrap();
        store
            .insert_object(&make_record("photos/b.jpg"))
            .await
            .unwrap();
        store
            .insert_object(&make_record("docs/readme.md"))
            .await
            .unwrap();

        let photos = store.list_objects("photos/", 100).await.unwrap();
        assert_eq!(photos.len(), 2);

        let docs = store.list_objects("docs/", 100).await.unwrap();
        assert_eq!(docs.len(), 1);

        let all = store.list_objects("", 100).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_list_objects_respects_max_keys() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        store.insert_object(&make_record("a.txt")).await.unwrap();
        store.insert_object(&make_record("b.txt")).await.unwrap();
        store.insert_object(&make_record("c.txt")).await.unwrap();

        let limited = store.list_objects("", 2).await.unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_upsert_object() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let mut obj = make_record("test.txt");
        obj.size = Some(100);
        store.upsert_object(&obj).await.unwrap();

        let fetched = store.get_object("test.txt").await.unwrap().unwrap();
        assert_eq!(fetched.size, Some(100));

        // Upsert with new size
        obj.size = Some(200);
        store.upsert_object(&obj).await.unwrap();

        let fetched = store.get_object("test.txt").await.unwrap().unwrap();
        assert_eq!(fetched.size, Some(200));
    }

    #[tokio::test]
    async fn test_list_by_parent() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        store
            .insert_object(&make_record("photos/a.jpg"))
            .await
            .unwrap();
        store
            .insert_object(&make_record("photos/b.jpg"))
            .await
            .unwrap();
        store
            .insert_object(&make_record("photos/sub/c.jpg"))
            .await
            .unwrap();

        let photos = store.list_by_parent("photos").await.unwrap();
        assert_eq!(photos.len(), 2);

        let sub = store.list_by_parent("photos/sub").await.unwrap();
        assert_eq!(sub.len(), 1);
    }

    #[tokio::test]
    async fn test_count_objects() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        assert_eq!(store.count_objects().await.unwrap(), 0);

        store.insert_object(&make_record("a.txt")).await.unwrap();
        store.insert_object(&make_record("b.txt")).await.unwrap();

        assert_eq!(store.count_objects().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_symlink_record() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let now = OffsetDateTime::now_utc();
        let obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            key: "my-link".to_string(),
            is_symlink: true,
            symlink_target: Some("/some/target".to_string()),
            content_type: Some("application/x-symlink".to_string()),
            last_modified: now,
            created_at: now,
            scan_level: 1,
            ..Default::default()
        };

        store.insert_object(&obj).await.unwrap();

        let fetched = store.get_object("my-link").await.unwrap().unwrap();
        assert!(fetched.is_symlink);
        assert_eq!(fetched.symlink_target.as_deref(), Some("/some/target"));
    }
}
