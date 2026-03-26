use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::TryStreamExt;
use futures::Stream;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use tokio::sync::RwLock;

use crate::error::S3Error;
use crate::types::ChecksumValues;

// ---------------------------------------------------------------------------
// KnownFiles — in-memory catalog snapshot for incremental L1 scanning
// ---------------------------------------------------------------------------

/// Identity of a file in the catalog, used for change detection.
#[derive(Debug, Clone)]
pub struct FileIdentity {
    pub inode: Option<i64>,
    pub device_id: Option<i64>,
    pub size: Option<i64>,
}

/// Preloaded catalog snapshot for incremental L1 scanning.
///
/// Contains two lookup maps:
/// - `by_key`: for checking if a file at a given location is unchanged
/// - `by_inode`: for detecting file moves (same inode, different path)
pub struct KnownFiles {
    /// `(parent_dir_id, name) → FileIdentity`
    pub by_key: HashMap<(i64, String), FileIdentity>,
    /// `(inode, device_id) → (parent_dir_id, name)` for move detection
    pub by_inode: HashMap<(i64, i64), (i64, String)>,
}

// ---------------------------------------------------------------------------
// SqliteTimestamp — newtype for storing OffsetDateTime as INTEGER (epoch nanos)
// ---------------------------------------------------------------------------

/// Wrapper that stores `time::OffsetDateTime` as an `i64` (Unix epoch
/// nanoseconds) in SQLite.  Implements the sqlx encode/decode traits so
/// `sqlx::FromRow` derive works transparently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqliteTimestamp(pub time::OffsetDateTime);

impl SqliteTimestamp {
    pub fn now() -> Self {
        Self(time::OffsetDateTime::now_utc())
    }

    fn to_nanos(self) -> i64 {
        self.0.unix_timestamp_nanos() as i64
    }

    pub(crate) fn from_nanos(ns: i64) -> Self {
        Self(
            time::OffsetDateTime::from_unix_timestamp_nanos(ns as i128)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
        )
    }
}

impl std::ops::Deref for SqliteTimestamp {
    type Target = time::OffsetDateTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<time::OffsetDateTime> for SqliteTimestamp {
    fn from(dt: time::OffsetDateTime) -> Self {
        Self(dt)
    }
}

impl sqlx::Type<sqlx::Sqlite> for SqliteTimestamp {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <i64 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }

    fn compatible(ty: &sqlx::sqlite::SqliteTypeInfo) -> bool {
        <i64 as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for SqliteTimestamp {
    fn encode_by_ref(
        &self,
        args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as sqlx::Encode<'q, sqlx::Sqlite>>::encode_by_ref(&self.to_nanos(), args)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for SqliteTimestamp {
    fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let nanos = <i64 as sqlx::Decode<'r, sqlx::Sqlite>>::decode(value)?;
        Ok(Self::from_nanos(nanos))
    }
}

/// Batch update entry for L3 content hashes.
pub struct L3HashUpdate {
    /// Full S3 key (e.g. `"photos/2024/vacation.jpg"`).
    pub key: String,
    pub etag: String,
    pub checksums: ChecksumValues,
    pub scan_level: i32,
}

/// Split a full S3 key into `(directory_prefix, filename)`.
///
/// The directory prefix includes a trailing `/` to match the
/// `directories.prefix` column format. Root-level files return an empty
/// prefix.
///
/// # Examples
///
/// - `"photos/2024/vacation.jpg"` → `("photos/2024/", "vacation.jpg")`
/// - `"file.txt"` → `("", "file.txt")`
pub(crate) fn split_key(key: &str) -> (String, &str) {
    match key.rsplit_once('/') {
        Some((dir, name)) => (format!("{dir}/"), name),
        None => (String::new(), key),
    }
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
/// Timestamp fields use [`SqliteTimestamp`] which wraps `time::OffsetDateTime`
/// and stores as INTEGER (Unix epoch nanoseconds) in SQLite.
///
/// ## ETag Convention
/// The `etag` field stores values WITH surrounding double-quote characters,
/// e.g. `"\"d41d8cd98f00b204e9800998ecf8427e\""`. This matches the S3 wire
/// format where ETags are always quoted per RFC 7232. Code should store
/// and return the value as-is without adding or stripping quotes.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ObjectRecord {
    pub id: String,
    /// Filename only (e.g. `"vacation.jpg"`), matching the `objects.name` column.
    pub name: String,
    pub parent_dir_id: i64,
    /// Full S3 key, reconstructed via `directories.prefix || objects.name` in
    /// JOINed queries. Empty when loaded via `SELECT * FROM objects` without JOIN.
    #[sqlx(default)]
    pub key: String,
    pub is_symlink: bool,
    pub symlink_target: Option<String>,

    // L2 metadata (None until scanned)
    pub size: Option<i64>,
    pub file_mtime: Option<SqliteTimestamp>,
    pub file_ctime: Option<SqliteTimestamp>,
    pub inode: Option<i64>,
    pub device_id: Option<i64>,

    // L3 metadata (None until content-hashed)
    pub etag: Option<String>,

    // S3 checksums (base64-encoded, None until content-hashed)
    pub checksum_sha256: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,

    // S3 metadata (interned FK into content_types table)
    pub content_type_id: Option<i64>,
    pub last_modified: SqliteTimestamp,
    pub created_at: SqliteTimestamp,
    pub metadata: Option<String>,

    pub scan_level: i32,
}

impl Default for ObjectRecord {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            parent_dir_id: 0,
            key: String::new(),
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
            content_type_id: None,
            last_modified: SqliteTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            created_at: SqliteTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            metadata: None,
            scan_level: 0,
        }
    }
}

/// Compute the exclusive upper-bound for a prefix range scan.
///
/// For a prefix like `"photos/"`, returns `Some("photos0")` — where `0` is
/// the character after `/` in ASCII (0x30 vs 0x2F). The caller can then use
/// `col >= prefix AND col < upper` instead of `col LIKE 'photos/%'`,
/// which lets SQLite use a B-tree range seek.
///
/// Returns `None` for an empty prefix (meaning "match all rows").
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    // Walk backwards to find a byte we can increment without overflow.
    while let Some(last) = bytes.last_mut() {
        if *last < 0xFF {
            *last += 1;
            // Safety: incrementing a valid UTF-8 byte may produce a non-UTF-8
            // sequence, but SQLite compares as raw bytes so this is fine for
            // the range bound. We use from_utf8_lossy to get a String.
            return Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        bytes.pop();
    }
    // All bytes were 0xFF — no upper bound needed (prefix is the max).
    None
}

/// SQLite-backed metadata store for a single bucket.
#[derive(Clone)]
pub struct MetadataStore {
    pool: SqlitePool,
    /// In-memory cache: content_type_id → MIME string.
    content_types: Arc<RwLock<HashMap<i64, String>>>,
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

        // Seed the content_type cache from existing rows.
        let ct_rows: Vec<(i64, String)> = sqlx::query_as("SELECT id, mime FROM content_types")
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
        let content_types: HashMap<i64, String> = ct_rows.into_iter().collect();

        Ok(Self {
            pool,
            content_types: Arc::new(RwLock::new(content_types)),
        })
    }

    // ----- directory helpers -----

    /// Resolve a parent-directory path to its `directories.id`, inserting if needed.
    pub async fn get_or_create_dir_id(&self, parent_dir_id: &str) -> Result<i64, S3Error> {
        let prefix = if parent_dir_id.is_empty() {
            String::new()
        } else {
            format!("{parent_dir_id}/")
        };
        // Try lookup first (fast path — no write lock).
        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM directories WHERE prefix = ?")
                .bind(&prefix)
                .fetch_optional(&self.pool)
                .await?;
        if let Some((id,)) = existing {
            return Ok(id);
        }
        // Insert (race-safe via ON CONFLICT).
        let depth = prefix.matches('/').count() as i32;
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO directories (prefix, depth) VALUES (?, ?) \
             ON CONFLICT(prefix) DO UPDATE SET prefix = excluded.prefix \
             RETURNING id",
        )
        .bind(&prefix)
        .bind(depth)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    /// Look up the prefix string for a directory id.
    pub async fn get_directory_prefix(&self, id: i64) -> Result<String, S3Error> {
        let row: (String,) = sqlx::query_as("SELECT prefix FROM directories WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }

    // ----- content-type helpers -----

    /// Resolve a MIME string to its `content_types.id`, inserting if needed.
    pub async fn get_or_create_content_type_id(&self, mime: &str) -> Result<i64, S3Error> {
        // Fast path: check cache.
        {
            let cache = self.content_types.read().await;
            for (&id, m) in cache.iter() {
                if m == mime {
                    return Ok(id);
                }
            }
        }
        // Slow path: insert then update cache.
        let row: (i64,) = sqlx::query_as(
            "INSERT INTO content_types (mime) VALUES (?) \
             ON CONFLICT(mime) DO UPDATE SET mime = excluded.mime \
             RETURNING id",
        )
        .bind(mime)
        .fetch_one(&self.pool)
        .await?;
        self.content_types
            .write()
            .await
            .insert(row.0, mime.to_string());
        Ok(row.0)
    }

    /// Resolve a `content_type_id` back to its MIME string.
    pub async fn resolve_content_type(&self, id: Option<i64>) -> String {
        let Some(id) = id else {
            return "application/octet-stream".to_string();
        };
        let cache = self.content_types.read().await;
        cache
            .get(&id)
            .cloned()
            .unwrap_or_else(|| "application/octet-stream".to_string())
    }

    // TODO(#5): Replace `SELECT *` with explicit column lists in get/list queries
    // to avoid loading heavy columns (e.g. `metadata` JSON) when not needed.
    // https://github.com/deepjoy/shoebox/issues/5

    /// Retrieve an object record by its full S3 key.
    pub async fn get_object(&self, key: &str) -> Result<Option<ObjectRecord>, S3Error> {
        let (dir_prefix, filename) = split_key(key);
        let record = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o \
             JOIN directories d ON o.parent_dir_id = d.id \
             WHERE d.prefix = ? AND o.name = ?",
        )
        .bind(&dir_prefix)
        .bind(filename)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Insert a new object record.
    pub async fn insert_object(&self, obj: &ObjectRecord) -> Result<(), S3Error> {
        sqlx::query(
            r#"INSERT INTO objects (
                id, name, parent_dir_id, is_symlink, symlink_target,
                size, file_mtime, file_ctime, inode, device_id,
                etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                content_type_id, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&obj.id)
        .bind(&obj.name)
        .bind(obj.parent_dir_id)
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
        .bind(obj.content_type_id)
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
                    id, name, parent_dir_id, is_symlink, symlink_target,
                    size, file_mtime, file_ctime, inode, device_id,
                    etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                    content_type_id, last_modified, created_at, metadata, scan_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(&obj.id)
            .bind(&obj.name)
            .bind(obj.parent_dir_id)
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
            .bind(obj.content_type_id)
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

    /// Insert or update an object record, keyed by `(parent_dir_id, name)`.
    ///
    /// On conflict the existing row's `id` and `created_at` are preserved
    /// (i.e. the values in the supplied `ObjectRecord` are ignored for those
    /// two columns on update). All other fields are overwritten.
    ///
    /// Returns the persisted `id` — on insert this is the supplied id; on
    /// update (overwrite) it is the existing row's id.
    pub async fn upsert_object(&self, obj: &ObjectRecord) -> Result<String, S3Error> {
        let row: (String,) = sqlx::query_as(
            r#"INSERT INTO objects (
                id, name, parent_dir_id, is_symlink, symlink_target,
                size, file_mtime, file_ctime, inode, device_id,
                etag, checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32c,
                content_type_id, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(parent_dir_id, name) DO UPDATE SET
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
                content_type_id = excluded.content_type_id,
                last_modified = excluded.last_modified,
                metadata = excluded.metadata,
                scan_level = excluded.scan_level
            RETURNING id"#,
        )
        .bind(&obj.id)
        .bind(&obj.name)
        .bind(obj.parent_dir_id)
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
        .bind(obj.content_type_id)
        .bind(obj.last_modified)
        .bind(obj.created_at)
        .bind(&obj.metadata)
        .bind(obj.scan_level)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0)
    }

    /// Delete an object record by key. Returns true if a row was deleted.
    pub async fn delete_object(&self, key: &str) -> Result<bool, S3Error> {
        let (dir_prefix, filename) = split_key(key);
        let result = sqlx::query(
            "DELETE FROM objects WHERE parent_dir_id = \
             (SELECT id FROM directories WHERE prefix = ?) AND name = ?",
        )
        .bind(&dir_prefix)
        .bind(filename)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Bulk-delete object records by key.
    pub async fn delete_objects(&self, keys: &[String]) -> Result<u64, S3Error> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        for key in keys {
            let (dir_prefix, filename) = split_key(key);
            let result = sqlx::query(
                "DELETE FROM objects WHERE parent_dir_id = \
                 (SELECT id FROM directories WHERE prefix = ?) AND name = ?",
            )
            .bind(&dir_prefix)
            .bind(filename)
            .execute(&self.pool)
            .await?;
            total += result.rows_affected();
        }
        Ok(total)
    }

    /// List objects matching a prefix, up to `max_keys`.
    pub async fn list_objects(
        &self,
        prefix: &str,
        max_keys: i32,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let (dir_prefix, name_prefix) = split_key(prefix);
        let records = if name_prefix.is_empty() {
            // Prefix ends with `/` or is empty — match entire directory subtree.
            if let Some(upper) = prefix_upper_bound(&dir_prefix) {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(&upper)
                .bind(max_keys)
                .fetch_all(&self.pool)
                .await?
            } else {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(max_keys)
                .fetch_all(&self.pool)
                .await?
            }
        } else {
            // Partial filename prefix — exact dir + name range, plus subdirectories.
            let name_upper = prefix_upper_bound(name_prefix);
            let dir_upper = prefix_upper_bound(prefix);
            match (name_upper, dir_upper) {
                (Some(nu), Some(du)) => {
                    sqlx::query_as::<_, ObjectRecord>(
                        "SELECT o.*, d.prefix || o.name AS key \
                         FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                         WHERE (d.prefix = ? AND o.name >= ? AND o.name < ?) \
                            OR (d.prefix > ? AND d.prefix < ?) \
                         ORDER BY d.prefix, o.name LIMIT ?",
                    )
                    .bind(&dir_prefix)
                    .bind(name_prefix)
                    .bind(&nu)
                    .bind(&dir_prefix)
                    .bind(&du)
                    .bind(max_keys)
                    .fetch_all(&self.pool)
                    .await?
                }
                _ => Vec::new(),
            }
        };

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
        // Decompose start_after into (dir_prefix, name) for keyset pagination.
        let after_parts = start_after.map(|s| {
            let (dp, n) = split_key(s);
            (dp, n.to_string())
        });
        let (dir_prefix, name_prefix) = split_key(prefix);
        let name_prefix = name_prefix.to_string();
        let dir_upper = prefix_upper_bound(prefix);

        // All bind values are passed as owned Strings so the stream owns them.
        let raw_stream: Pin<Box<dyn Stream<Item = Result<ObjectRecord, sqlx::Error>> + Send + '_>> =
            if name_prefix.is_empty() {
                // Prefix ends with `/` or is empty.
                let pfx_upper = prefix_upper_bound(&dir_prefix);
                match (after_parts, pfx_upper) {
                    (Some((ad, an)), Some(ub)) => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             WHERE d.prefix >= ? AND d.prefix < ? \
                               AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                             ORDER BY d.prefix, o.name",
                        )
                        .bind(dir_prefix.clone())
                        .bind(ub)
                        .bind(ad.clone())
                        .bind(ad)
                        .bind(an)
                        .fetch(&self.pool),
                    ),
                    (Some((ad, an)), None) => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             WHERE (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                             ORDER BY d.prefix, o.name",
                        )
                        .bind(ad.clone())
                        .bind(ad)
                        .bind(an)
                        .fetch(&self.pool),
                    ),
                    (None, Some(ub)) => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             WHERE d.prefix >= ? AND d.prefix < ? \
                             ORDER BY d.prefix, o.name",
                        )
                        .bind(dir_prefix.clone())
                        .bind(ub)
                        .fetch(&self.pool),
                    ),
                    (None, None) => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             ORDER BY d.prefix, o.name",
                        )
                        .fetch(&self.pool),
                    ),
                }
            } else {
                // Partial filename prefix — exact dir + name range, plus subdirectories.
                let name_upper = prefix_upper_bound(&name_prefix).unwrap_or_default();
                let du = dir_upper.clone().unwrap_or_default();
                match after_parts {
                    Some((ad, an)) => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             WHERE ((d.prefix = ? AND o.name >= ? AND o.name < ?) \
                                 OR (d.prefix > ? AND d.prefix < ?)) \
                               AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                             ORDER BY d.prefix, o.name",
                        )
                        .bind(dir_prefix.clone())
                        .bind(name_prefix.clone())
                        .bind(name_upper)
                        .bind(dir_prefix)
                        .bind(du)
                        .bind(ad.clone())
                        .bind(ad)
                        .bind(an)
                        .fetch(&self.pool),
                    ),
                    None => Box::pin(
                        sqlx::query_as::<_, ObjectRecord>(
                            "SELECT o.*, d.prefix || o.name AS key \
                             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                             WHERE (d.prefix = ? AND o.name >= ? AND o.name < ?) \
                                OR (d.prefix > ? AND d.prefix < ?) \
                             ORDER BY d.prefix, o.name",
                        )
                        .bind(dir_prefix.clone())
                        .bind(name_prefix)
                        .bind(name_upper)
                        .bind(dir_prefix)
                        .bind(du)
                        .fetch(&self.pool),
                    ),
                }
            };

        let prefix_owned = prefix.to_string();

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
                let delim_owned = delim.to_string();
                let prefix_len = prefix_owned.len();

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
        let prefix = if parent.is_empty() {
            String::new()
        } else {
            format!("{parent}/")
        };
        let dir_id: Option<(i64,)> = sqlx::query_as("SELECT id FROM directories WHERE prefix = ?")
            .bind(&prefix)
            .fetch_optional(&self.pool)
            .await?;
        let Some((dir_id,)) = dir_id else {
            return Ok(Vec::new());
        };
        let records = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
             WHERE o.parent_dir_id = ? ORDER BY o.name",
        )
        .bind(dir_id)
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
        let max = max_keys as usize;

        // No delimiter: flat list with one extra row for truncation detection.
        let Some(delim) = delimiter else {
            let limit = max_keys as i64 + 1;
            let records = self.fetch_page(prefix, start_after, limit).await?;
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

        // With delimiter: two-phase approach for efficiency.
        // Phase 1 scans only the `key` column (covering-index scan) to
        // classify keys into objects vs common prefixes. Phase 2 fetches
        // full ObjectRecords only for the (few) actual object keys we return.
        let prefix_len = prefix.len();
        let mut object_keys: Vec<String> = Vec::new();
        let mut common_prefixes = std::collections::BTreeSet::new();
        let mut cursor: Option<String> = None;

        // Fetch keys in bounded chunks to avoid loading millions of rows
        // into memory when the bucket is large.
        let chunk_size = (max as i64 + 1) * 10;
        let mut fetch_after = start_after.map(|s| s.to_string());

        loop {
            let keys = self
                .fetch_keys(prefix, fetch_after.as_deref(), chunk_size)
                .await?;
            let chunk_len = keys.len();

            for key in keys {
                let count = object_keys.len() + common_prefixes.len();
                if count > max {
                    break;
                }
                cursor = Some(key.clone());
                let suffix = &key[prefix_len..];
                if let Some(pos) = suffix.find(delim) {
                    let cp = format!("{}{}", prefix, &suffix[..pos + delim.len()]);
                    common_prefixes.insert(cp);
                } else {
                    object_keys.push(key);
                }
            }

            let count = object_keys.len() + common_prefixes.len();
            // Stop if we have enough results, or the chunk was smaller than
            // requested (table exhausted).
            if count > max || (chunk_len as i64) < chunk_size {
                break;
            }
            fetch_after = cursor.clone();
        }

        let count = object_keys.len() + common_prefixes.len();
        let is_truncated = count > max;

        // Trim to exactly max_keys entries. Remove excess items,
        // popping the lexicographically last item each iteration.
        if is_truncated {
            while object_keys.len() + common_prefixes.len() > max {
                let last_obj = object_keys.last().map(|o| o.as_str());
                let last_cp = common_prefixes.iter().next_back().map(|s| s.as_str());
                match (last_obj, last_cp) {
                    (Some(o), Some(c)) if o > c => {
                        object_keys.pop();
                    }
                    (_, Some(_)) => {
                        common_prefixes.pop_last();
                    }
                    (Some(_), None) => {
                        object_keys.pop();
                    }
                    _ => break,
                }
            }
        }

        // Phase 2: fetch full records only for the object keys we're returning.
        let objects = self.fetch_objects_by_keys(&object_keys).await?;

        let next_token = if is_truncated { cursor } else { None };
        let cp_vec: Vec<String> = common_prefixes.into_iter().collect();

        Ok((objects, cp_vec, is_truncated, next_token))
    }

    /// Fetch a page of records matching a prefix range, optionally after a cursor key.
    async fn fetch_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let (dir_prefix, name_prefix) = split_key(prefix);
        let after_parts = after.map(|a| {
            let (dp, n) = split_key(a);
            (dp, n.to_string())
        });

        if name_prefix.is_empty() {
            let pfx_upper = prefix_upper_bound(&dir_prefix);
            match (after_parts, pfx_upper) {
                (Some((ad, an)), Some(ub)) => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(&ub)
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (Some((ad, an)), None) => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (None, Some(ub)) => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(&ub)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (None, None) => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
            }
        } else {
            let name_upper = prefix_upper_bound(name_prefix).unwrap_or_default();
            let du = prefix_upper_bound(prefix).unwrap_or_default();
            match after_parts {
                Some((ad, an)) => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE ((d.prefix = ? AND o.name >= ? AND o.name < ?) \
                         OR (d.prefix > ? AND d.prefix < ?)) \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(name_prefix)
                .bind(&name_upper)
                .bind(&dir_prefix)
                .bind(&du)
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                None => sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE (d.prefix = ? AND o.name >= ? AND o.name < ?) \
                        OR (d.prefix > ? AND d.prefix < ?) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(name_prefix)
                .bind(&name_upper)
                .bind(&dir_prefix)
                .bind(&du)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
            }
        }
    }

    /// Fetch only reconstructed keys for objects matching a prefix, optionally
    /// starting after `after`, returning at most `limit` rows.
    async fn fetch_keys(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: i64,
    ) -> Result<Vec<String>, S3Error> {
        let (dir_prefix, name_prefix) = split_key(prefix);
        let after_parts = after.map(|a| {
            let (dp, n) = split_key(a);
            (dp, n.to_string())
        });

        if name_prefix.is_empty() {
            let pfx_upper = prefix_upper_bound(&dir_prefix);
            match (after_parts, pfx_upper) {
                (Some((ad, an)), Some(ub)) => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(&ub)
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (Some((ad, an)), None) => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (None, Some(ub)) => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(&ub)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                (None, None) => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
            }
        } else {
            let name_upper = prefix_upper_bound(name_prefix).unwrap_or_default();
            let du = prefix_upper_bound(prefix).unwrap_or_default();
            match after_parts {
                Some((ad, an)) => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE ((d.prefix = ? AND o.name >= ? AND o.name < ?) \
                         OR (d.prefix > ? AND d.prefix < ?)) \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(name_prefix)
                .bind(&name_upper)
                .bind(&dir_prefix)
                .bind(&du)
                .bind(&ad)
                .bind(&ad)
                .bind(&an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
                None => sqlx::query_scalar(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE (d.prefix = ? AND o.name >= ? AND o.name < ?) \
                        OR (d.prefix > ? AND d.prefix < ?) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(&dir_prefix)
                .bind(name_prefix)
                .bind(&name_upper)
                .bind(&dir_prefix)
                .bind(&du)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(Into::into),
            }
        }
    }

    /// Fetch full ObjectRecords for a specific set of full S3 keys.
    async fn fetch_objects_by_keys(&self, keys: &[String]) -> Result<Vec<ObjectRecord>, S3Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Decompose each key and query individually, collecting results.
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let (dir_prefix, filename) = split_key(key);
            let record = sqlx::query_as::<_, ObjectRecord>(
                "SELECT o.*, d.prefix || o.name AS key \
                 FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                 WHERE d.prefix = ? AND o.name = ?",
            )
            .bind(&dir_prefix)
            .bind(filename)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(r) = record {
                results.push(r);
            }
        }
        Ok(results)
    }

    /// Get the total count of objects in the store.
    pub async fn count_objects(&self) -> Result<i64, S3Error> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }

    /// Rename (move) an object from one key to another.
    pub async fn rename_object(&self, src_key: &str, dst_key: &str) -> Result<(), S3Error> {
        let (src_dir, src_name) = split_key(src_key);
        let parent = dst_key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();
        let dir_id = self.get_or_create_dir_id(&parent).await?;
        let (_, dst_name) = split_key(dst_key);
        let now = SqliteTimestamp::now();
        let result = sqlx::query(
            "UPDATE objects SET name = ?, parent_dir_id = ?, last_modified = ? \
             WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) AND name = ?",
        )
        .bind(dst_name)
        .bind(dir_id)
        .bind(now)
        .bind(&src_dir)
        .bind(src_name)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(S3Error::NoSuchKey);
        }
        Ok(())
    }

    pub async fn get_object_tags(&self, key: &str) -> Result<Vec<Tag>, S3Error> {
        let (dir_prefix, filename) = split_key(key);
        let tags = sqlx::query_as::<_, Tag>(
            "SELECT t.key, t.value FROM object_tags t \
             INNER JOIN objects o ON t.object_id = o.id \
             INNER JOIN directories d ON o.parent_dir_id = d.id \
             WHERE d.prefix = ? AND o.name = ? ORDER BY t.key",
        )
        .bind(&dir_prefix)
        .bind(filename)
        .fetch_all(&self.pool)
        .await?;

        Ok(tags)
    }

    /// Insert a single tag for an object (looked up by key).
    pub async fn insert_object_tag(&self, key: &str, tag: &Tag) -> Result<(), S3Error> {
        let (dir_prefix, filename) = split_key(key);
        let object_id: Option<(String,)> = sqlx::query_as(
            "SELECT o.id FROM objects o \
             JOIN directories d ON o.parent_dir_id = d.id \
             WHERE d.prefix = ? AND o.name = ?",
        )
        .bind(&dir_prefix)
        .bind(filename)
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
        let (dir_prefix, filename) = split_key(key);
        sqlx::query(
            "DELETE FROM object_tags WHERE object_id IN \
             (SELECT o.id FROM objects o \
              JOIN directories d ON o.parent_dir_id = d.id \
              WHERE d.prefix = ? AND o.name = ?)",
        )
        .bind(&dir_prefix)
        .bind(filename)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Scanner Methods (Phase 6)
    // -------------------------------------------------------------------------

    /// Look up an object by inode and device_id (for move detection).
    ///
    /// Returns the first matching record, or `None` if no object has this
    /// inode+device_id combination. Used by the L1 scanner to detect file
    /// moves (same inode, different key).
    pub async fn get_object_by_inode(
        &self,
        inode: i64,
        device_id: i64,
    ) -> Result<Option<ObjectRecord>, S3Error> {
        let record = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
             WHERE o.inode = ? AND o.device_id = ? LIMIT 1",
        )
        .bind(inode)
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

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
    /// are returned (keyset pagination).
    pub async fn list_keys_below_scan_level(
        &self,
        level: i32,
        limit: i64,
        after_key: Option<&str>,
    ) -> Result<Vec<String>, S3Error> {
        let rows: Vec<(String,)> = match after_key {
            Some(key) => {
                let (ad, an) = split_key(key);
                sqlx::query_as(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.scan_level < ? \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(level)
                .bind(&ad)
                .bind(&ad)
                .bind(an)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT d.prefix || o.name FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.scan_level < ? \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
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
                let (ad, an) = split_key(key);
                sqlx::query_as(
                    "SELECT d.prefix || o.name, COALESCE(o.size, 0) FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.scan_level < ? \
                       AND (d.prefix > ? OR (d.prefix = ? AND o.name > ?)) \
                     ORDER BY d.prefix, o.name LIMIT ?",
                )
                .bind(level)
                .bind(&ad)
                .bind(&ad)
                .bind(an)
                .bind(ROW_CAP)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as(
                    "SELECT d.prefix || o.name, COALESCE(o.size, 0) FROM objects o \
                     JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.scan_level < ? \
                     ORDER BY d.prefix, o.name LIMIT ?",
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

    /// Load all known files from the catalog for incremental L1 scanning.
    ///
    /// Returns a `KnownFiles` struct containing two lookup maps:
    /// - `by_key`: `(parent_dir_id, name) → FileIdentity` for quick unchanged-file detection
    /// - `by_inode`: `(inode, device_id) → (parent_dir_id, name)` for move detection
    ///
    /// Memory usage: ~30 MB for 100K files with average 100-byte keys.
    pub(crate) async fn l1_scan_load_known_files(&self) -> Result<KnownFiles, S3Error> {
        type Row = (String, i64, Option<i64>, Option<i64>, Option<i64>);
        let rows: Vec<Row> =
            sqlx::query_as("SELECT name, parent_dir_id, inode, device_id, size FROM objects")
                .fetch_all(&self.pool)
                .await?;

        let mut by_key = HashMap::with_capacity(rows.len());
        let mut by_inode: HashMap<(i64, i64), (i64, String)> = HashMap::new();

        for (name, parent_dir_id, inode, device_id, size) in rows {
            by_key.insert(
                (parent_dir_id, name.clone()),
                FileIdentity {
                    inode,
                    device_id,
                    size,
                },
            );
            if let (Some(ino), Some(dev)) = (inode, device_id) {
                by_inode.insert((ino, dev), (parent_dir_id, name));
            }
        }

        Ok(KnownFiles { by_key, by_inode })
    }

    /// Delete stale objects whose keys were not seen during the incremental walk.
    ///
    /// Takes a set of `(parent_dir_id, name)` tuples representing all files that
    /// were observed during the walk. Any object NOT in this set is deleted.
    /// Returns the number of deleted rows.
    pub(crate) async fn l1_scan_delete_stale(
        &self,
        seen: &std::collections::HashSet<(i64, String)>,
        scan_start: SqliteTimestamp,
    ) -> Result<u64, S3Error> {
        // Only consider objects that existed before the scan started.
        // Objects created after scan_start (e.g. via concurrent API uploads)
        // are excluded to avoid a race where put_object inserts a record that
        // the filesystem walk never saw.
        let all_objects: Vec<(i64, String)> =
            sqlx::query_as("SELECT parent_dir_id, name FROM objects WHERE created_at < ?")
                .bind(scan_start)
                .fetch_all(&self.pool)
                .await?;

        let mut deleted: u64 = 0;
        for (parent_dir_id, name) in all_objects {
            if !seen.contains(&(parent_dir_id, name.clone())) {
                let result =
                    sqlx::query("DELETE FROM objects WHERE parent_dir_id = ? AND name = ?")
                        .bind(parent_dir_id)
                        .bind(&name)
                        .execute(&self.pool)
                        .await?;
                deleted += result.rows_affected();
            }
        }

        Ok(deleted)
    }

    /// Apply a file move detected during incremental L1 scanning.
    ///
    /// Updates the existing object row to reflect the new location, preserving
    /// the object ID and all other metadata.
    pub(crate) async fn l1_scan_apply_move(
        &self,
        conn: &mut sqlx::SqliteConnection,
        old_parent_dir_id: i64,
        old_name: &str,
        new_parent_dir_id: i64,
        new_name: &str,
        now: SqliteTimestamp,
    ) -> Result<(), S3Error> {
        sqlx::query(
            "UPDATE objects SET name = ?, parent_dir_id = ?, last_modified = ? \
             WHERE parent_dir_id = ? AND name = ?",
        )
        .bind(new_name)
        .bind(new_parent_dir_id)
        .bind(now)
        .bind(old_parent_dir_id)
        .bind(old_name)
        .execute(&mut *conn)
        .await?;
        Ok(())
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
                name TEXT NOT NULL,
                parent_dir_id INTEGER NOT NULL,
                id TEXT NOT NULL,
                is_symlink BOOLEAN NOT NULL DEFAULT FALSE,
                symlink_target TEXT,
                size INTEGER,
                inode INTEGER,
                device_id INTEGER,
                content_type_id INTEGER,
                last_modified INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (parent_dir_id, name)
            )",
        )
        .execute(&mut *conn)
        .await?;
        Ok(conn)
    }

    /// Batch-insert discovered disk files into the L1 temp table using
    /// multi-value INSERT statements to reduce per-statement overhead.
    ///
    /// Each statement inserts up to `MULTI_INSERT_CHUNK` rows (500 × 11 cols =
    /// 5500 bind params, well within SQLite's 32766 limit).
    pub(crate) async fn l1_scan_insert_batch(
        conn: &mut sqlx::SqliteConnection,
        records: &[ObjectRecord],
    ) -> Result<(), S3Error> {
        if records.is_empty() {
            return Ok(());
        }

        /// Max rows per multi-value INSERT (500 × 11 cols = 5500 params).
        const MULTI_INSERT_CHUNK: usize = 500;

        let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
        for chunk in records.chunks(MULTI_INSERT_CHUNK) {
            let placeholders: String = (0..chunk.len())
                .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT OR IGNORE INTO l1_disk (
                    name, parent_dir_id, id, is_symlink, symlink_target,
                    size, inode, device_id, content_type_id, last_modified, created_at
                ) VALUES {placeholders}"
            );
            let mut query = sqlx::query(&sql);
            for obj in chunk {
                query = query
                    .bind(&obj.name)
                    .bind(obj.parent_dir_id)
                    .bind(&obj.id)
                    .bind(obj.is_symlink)
                    .bind(&obj.symlink_target)
                    .bind(obj.size)
                    .bind(obj.inode)
                    .bind(obj.device_id)
                    .bind(obj.content_type_id)
                    .bind(obj.last_modified)
                    .bind(obj.created_at);
            }
            query.execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Merge the L1 temp table into `objects`: detect moves, insert newly
    /// discovered files, optionally delete stale entries, and return
    /// `(discovered, deleted, moved)`. Drops the temp table when done.
    ///
    /// Move detection: when a new `(parent_dir_id, name)` (in l1_disk but not
    /// in objects) has an inode+device_id that matches an existing object under
    /// a different location, we treat it as a move — updating the existing
    /// row's name/parent rather than inserting a new one.
    pub(crate) async fn l1_scan_finish(
        conn: &mut sqlx::SqliteConnection,
        delete_stale: bool,
    ) -> Result<(u64, u64, u64), S3Error> {
        // Step 1: Detect moves — new (parent_dir_id, name) combos whose
        // inode+device_id match an existing object at a different location.
        //
        // Returns (new_name, new_parent_dir_id, old_key_reconstructed,
        //          new_key_reconstructed, object_id).
        let moves: Vec<(String, i64, String, String, String)> = sqlx::query_as(
            "SELECT d.name, d.parent_dir_id, \
                    dir_old.prefix || o.name, \
                    dir_new.prefix || d.name, \
                    o.id \
             FROM l1_disk d \
             INNER JOIN objects o ON d.inode = o.inode AND d.device_id = o.device_id \
             INNER JOIN directories dir_old ON o.parent_dir_id = dir_old.id \
             INNER JOIN directories dir_new ON d.parent_dir_id = dir_new.id \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM objects o2 \
                 WHERE o2.parent_dir_id = d.parent_dir_id AND o2.name = d.name \
             ) \
             AND d.inode IS NOT NULL \
             AND (o.parent_dir_id != d.parent_dir_id OR o.name != d.name)",
        )
        .fetch_all(&mut *conn)
        .await?;

        let mut moved: u64 = 0;
        let now = SqliteTimestamp::now();
        for (new_name, new_parent_dir_id, old_key, new_key, object_id) in &moves {
            sqlx::query(
                "UPDATE objects SET name = ?, parent_dir_id = ?, last_modified = ? \
                 WHERE id = ?",
            )
            .bind(new_name)
            .bind(new_parent_dir_id)
            .bind(now)
            .bind(object_id)
            .execute(&mut *conn)
            .await?;
            tracing::info!(
                old_key = %old_key,
                new_key = %new_key,
                object_id = %object_id,
                "Move detected, preserving object_id"
            );
            moved += 1;
        }

        // Step 2: Insert truly new objects (not in catalog AND not a move)
        let inserted = sqlx::query(
            "INSERT INTO objects (
                id, name, parent_dir_id, is_symlink, symlink_target,
                size, inode, device_id, content_type_id, last_modified, created_at, scan_level
            )
            SELECT
                d.id, d.name, d.parent_dir_id, d.is_symlink, d.symlink_target,
                d.size, d.inode, d.device_id, d.content_type_id, d.last_modified, d.created_at, 1
            FROM l1_disk d
            WHERE NOT EXISTS (
                SELECT 1 FROM objects o
                WHERE o.parent_dir_id = d.parent_dir_id AND o.name = d.name
            )",
        )
        .execute(&mut *conn)
        .await?;
        let discovered = inserted.rows_affected();

        // Step 3: Delete objects that are in the catalog but no longer on disk
        let deleted = if delete_stale {
            let result = sqlx::query(
                "DELETE FROM objects WHERE NOT EXISTS ( \
                     SELECT 1 FROM l1_disk d \
                     WHERE d.parent_dir_id = objects.parent_dir_id AND d.name = objects.name \
                 )",
            )
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

        Ok((discovered, deleted, moved))
    }

    // ── BFS L1 per-directory helpers ────────────────────────────────────────

    /// Check if a directory entry exists in the catalog (without creating it).
    /// Returns `None` if the directory has never been scanned.
    pub(crate) async fn get_dir_id(&self, prefix: &str) -> Result<Option<i64>, S3Error> {
        let canonical = if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM directories WHERE prefix = ?")
                .bind(&canonical)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(id,)| id))
    }

    /// Load known files for a single directory (BFS per-directory L1 scanning).
    ///
    /// Returns a map of `name → FileIdentity` containing only objects whose
    /// `parent_dir_id` matches `dir_id`. Used to detect unchanged files and
    /// avoid redundant upserts.
    pub(crate) async fn l1_load_dir_objects(
        &self,
        dir_id: i64,
    ) -> Result<HashMap<String, FileIdentity>, S3Error> {
        type Row = (String, Option<i64>, Option<i64>, Option<i64>);
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT name, inode, device_id, size FROM objects WHERE parent_dir_id = ?",
        )
        .bind(dir_id)
        .fetch_all(&self.pool)
        .await?;

        let mut map = HashMap::with_capacity(rows.len());
        for (name, inode, device_id, size) in rows {
            map.insert(name, FileIdentity { inode, device_id, size });
        }
        Ok(map)
    }

    /// Batch-insert newly discovered files during a BFS per-directory scan.
    ///
    /// Uses `INSERT OR IGNORE` — L1 is discovery-only. Files that already exist
    /// in the catalog (e.g. API-uploaded objects or previously scanned files) are
    /// silently skipped; their metadata is left untouched for L2 to update later.
    /// Returns the number of newly inserted rows.
    pub(crate) async fn l1_upsert_dir_files(
        &self,
        records: &[ObjectRecord],
    ) -> Result<u64, S3Error> {
        if records.is_empty() {
            return Ok(0);
        }

        /// Max rows per multi-value INSERT (500 × 12 cols = 6000 params — within
        /// SQLite's 32766 limit).
        const CHUNK: usize = 500;

        let mut total = 0u64;
        let mut tx = self.pool.begin().await?;

        for chunk in records.chunks(CHUNK) {
            let placeholders: String = (0..chunk.len())
                .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT OR IGNORE INTO objects (
                    id, name, parent_dir_id, is_symlink, symlink_target,
                    size, inode, device_id, content_type_id,
                    last_modified, created_at, scan_level
                ) VALUES {placeholders}"
            );
            let mut query = sqlx::query(&sql);
            for obj in chunk {
                query = query
                    .bind(&obj.id)
                    .bind(&obj.name)
                    .bind(obj.parent_dir_id)
                    .bind(obj.is_symlink)
                    .bind(&obj.symlink_target)
                    .bind(obj.size)
                    .bind(obj.inode)
                    .bind(obj.device_id)
                    .bind(obj.content_type_id)
                    .bind(obj.last_modified)
                    .bind(obj.created_at)
                    .bind(1i32);
            }
            let result = query.execute(&mut *tx).await?;
            total += result.rows_affected();
        }

        tx.commit().await?;
        Ok(total)
    }

    /// Delete stale objects in one directory that were not seen during the
    /// current scan pass. Only objects created before `scan_start_ns` are
    /// considered (concurrent API uploads are excluded).
    ///
    /// Returns the number of deleted rows.
    pub(crate) async fn l1_delete_stale_in_dir(
        &self,
        dir_id: i64,
        seen_names: &std::collections::HashSet<String>,
        scan_start_ns: i64,
    ) -> Result<u64, S3Error> {
        let scan_start = SqliteTimestamp::from_nanos(scan_start_ns);
        let all: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM objects WHERE parent_dir_id = ? AND created_at < ?",
        )
        .bind(dir_id)
        .bind(scan_start)
        .fetch_all(&self.pool)
        .await?;

        let stale: Vec<String> = all
            .into_iter()
            .map(|(n,)| n)
            .filter(|n| !seen_names.contains(n))
            .collect();

        if stale.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        for name in &stale {
            sqlx::query("DELETE FROM objects WHERE parent_dir_id = ? AND name = ?")
                .bind(dir_id)
                .bind(name)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(stale.len() as u64)
    }

    /// Post-scan cross-directory move reconciliation.
    ///
    /// With per-directory stale deletion, old entries are removed inline before
    /// `finalize()` runs, so there are typically no inode duplicates to reconcile.
    /// This is a no-op placeholder — cross-directory moves result in a new
    /// object_id for the destination. Intra-directory renames are handled by
    /// the per-directory stale-deletion + upsert cycle.
    pub(crate) async fn l1_reconcile_moves(&self, _scan_start_ns: i64) -> Result<u64, S3Error> {
        Ok(0)
    }

    /// Delete objects in directories that were removed from disk between scans.
    ///
    /// Since the BFS traversal only enqueues tasks for directories it finds
    /// during `read_dir`, deleted directories never get a task and their catalog
    /// entries accumulate as stale. This post-scan pass finds any directory whose
    /// pre-scan-start objects still exist in the catalog but whose path on disk is
    /// gone, then deletes those objects.
    ///
    /// Only run for bucket-wide scans (`ScanScope::Bucket`).
    pub(crate) async fn l1_cleanup_orphan_dirs(
        &self,
        root: &std::path::Path,
        scan_start_ns: i64,
    ) -> Result<u64, S3Error> {
        let scan_start = SqliteTimestamp::from_nanos(scan_start_ns);
        // Find directories that have pre-scan-start objects (i.e. were scanned
        // before) — the existence of such objects implies the directory existed.
        let dirs: Vec<(i64, String)> = sqlx::query_as(
            "SELECT d.id, d.prefix FROM directories d \
             WHERE EXISTS ( \
                 SELECT 1 FROM objects o \
                 WHERE o.parent_dir_id = d.id AND o.created_at < ? \
             )",
        )
        .bind(scan_start)
        .fetch_all(&self.pool)
        .await?;

        let mut total_deleted = 0u64;
        for (dir_id, prefix) in dirs {
            let dir_path = if prefix.is_empty() {
                root.to_path_buf()
            } else {
                // prefix always ends with '/' (stored that way in directories table)
                root.join(prefix.trim_end_matches('/'))
            };

            if !dir_path.exists() {
                let result = sqlx::query(
                    "DELETE FROM objects WHERE parent_dir_id = ? AND created_at < ?",
                )
                .bind(dir_id)
                .bind(scan_start)
                .execute(&self.pool)
                .await?;
                total_deleted += result.rows_affected();
            }
        }

        Ok(total_deleted)
    }

    /// Count objects created at or after `scan_start_ns` (newly discovered
    /// during the current scan). Used for progress stats in `finalize()`.
    pub(crate) async fn count_objects_since(&self, scan_start_ns: i64) -> Result<u64, S3Error> {
        let scan_start = SqliteTimestamp::from_nanos(scan_start_ns);
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM objects WHERE created_at >= ?")
                .bind(scan_start)
                .fetch_one(&self.pool)
                .await?;
        Ok(row.0 as u64)
    }

    /// Count objects deleted during the scan window. Deletion timestamps are
    /// not tracked in the current schema, so this always returns 0.
    pub(crate) async fn count_deleted_during_scan(
        &self,
        _scan_start_ns: i64,
    ) -> Result<u64, S3Error> {
        Ok(0)
    }

    /// Reset an object's scan level (e.g. after a file is modified on disk).
    pub async fn reset_scan_level(&self, key: &str, level: i32) -> Result<(), S3Error> {
        let (dir_prefix, filename) = split_key(key);
        sqlx::query(
            "UPDATE objects SET scan_level = ? \
             WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
               AND name = ? AND scan_level > ?",
        )
        .bind(level)
        .bind(&dir_prefix)
        .bind(filename)
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
        let (dir_prefix, filename) = split_key(key);
        let result = sqlx::query(
            "UPDATE objects SET size = ?, file_mtime = ?, file_ctime = ?, \
             inode = ?, device_id = ?, scan_level = ?, last_modified = ? \
             WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
               AND name = ? AND scan_level < ?",
        )
        .bind(update.size)
        .bind(update.file_mtime.map(SqliteTimestamp::from))
        .bind(update.file_ctime.map(SqliteTimestamp::from))
        .bind(update.inode.map(|v| v as i64))
        .bind(update.device_id.map(|v| v as i64))
        .bind(update.scan_level)
        .bind(SqliteTimestamp::now())
        .bind(&dir_prefix)
        .bind(filename)
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
        let now = SqliteTimestamp::now();
        for (key, update) in updates {
            let (dir_prefix, filename) = split_key(key);
            sqlx::query(
                "UPDATE objects SET size = ?, file_mtime = ?, file_ctime = ?, \
                 inode = ?, device_id = ?, scan_level = ?, last_modified = ? \
                 WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
                   AND name = ? AND scan_level < ?",
            )
            .bind(update.size)
            .bind(update.file_mtime.map(SqliteTimestamp::from))
            .bind(update.file_ctime.map(SqliteTimestamp::from))
            .bind(update.inode.map(|v| v as i64))
            .bind(update.device_id.map(|v| v as i64))
            .bind(update.scan_level)
            .bind(now)
            .bind(&dir_prefix)
            .bind(filename)
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
        let (dir_prefix, filename) = split_key(key);
        let result = sqlx::query(
            "UPDATE objects SET etag = ?, \
             checksum_sha256 = ?, checksum_sha1 = ?, checksum_crc32 = ?, checksum_crc32c = ?, \
             scan_level = ?, last_modified = ? \
             WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
               AND name = ? AND scan_level < ?",
        )
        .bind(etag)
        .bind(&checksums.sha256)
        .bind(&checksums.sha1)
        .bind(&checksums.crc32)
        .bind(&checksums.crc32c)
        .bind(scan_level)
        .bind(SqliteTimestamp::now())
        .bind(&dir_prefix)
        .bind(filename)
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
        let now = SqliteTimestamp::now();
        for key in keys {
            let (dir_prefix, filename) = split_key(key);
            sqlx::query(
                "UPDATE objects SET scan_level = ?, last_modified = ? \
                 WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
                   AND name = ? AND scan_level < ?",
            )
            .bind(target_level)
            .bind(now)
            .bind(&dir_prefix)
            .bind(filename)
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
        let now = SqliteTimestamp::now();
        for update in updates {
            let (dir_prefix, filename) = split_key(&update.key);
            sqlx::query(
                "UPDATE objects SET etag = ?, \
                 checksum_sha256 = ?, checksum_sha1 = ?, checksum_crc32 = ?, checksum_crc32c = ?, \
                 scan_level = ?, last_modified = ? \
                 WHERE parent_dir_id = (SELECT id FROM directories WHERE prefix = ?) \
                   AND name = ? AND scan_level < ?",
            )
            .bind(&update.etag)
            .bind(&update.checksums.sha256)
            .bind(&update.checksums.sha1)
            .bind(&update.checksums.crc32)
            .bind(&update.checksums.crc32c)
            .bind(update.scan_level)
            .bind(now)
            .bind(&dir_prefix)
            .bind(filename)
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

    // -------------------------------------------------------------------------
    // Phase 8: Duplicate Detection + Integrity Methods
    // -------------------------------------------------------------------------

    /// Get scan status: total files and files at each scan level.
    pub async fn get_scan_status(&self) -> Result<ScanStatus, S3Error> {
        let rows: Vec<(i32, i64)> =
            sqlx::query_as("SELECT scan_level, COUNT(*) FROM objects GROUP BY scan_level")
                .fetch_all(&self.pool)
                .await?;

        let mut total_files: i64 = 0;
        let mut files_at_level_3: i64 = 0;
        for (level, count) in &rows {
            total_files += count;
            if *level >= 3 {
                files_at_level_3 += count;
            }
        }

        Ok(ScanStatus {
            total_files,
            files_at_level_3,
        })
    }

    /// Find duplicate hash groups with keyset pagination and optional key filter.
    ///
    /// Results are ordered by `total_size DESC, checksum_sha256 ASC`.
    /// The cursor is `(total_size, checksum_sha256)` from the last row of the
    /// previous page.  When `key_contains` is set, only groups that contain at
    /// least one file whose key matches the substring are returned.
    pub async fn find_duplicate_hashes(
        &self,
        max_results: i32,
        cursor: Option<(i64, &str)>,
        key_contains: Option<&str>,
        max_depth: Option<i32>,
    ) -> Result<Vec<DuplicateGroup>, S3Error> {
        let use_filter = key_contains.is_some();
        let use_cursor = cursor.is_some();
        let use_depth = max_depth.is_some() && key_contains.is_some();

        let mut sql = String::new();

        if use_filter {
            sql.push_str(
                "WITH matching AS (\
                   SELECT DISTINCT obj.checksum_sha256 FROM objects obj \
                   JOIN directories dir ON obj.parent_dir_id = dir.id \
                   WHERE obj.checksum_sha256 IS NOT NULL \
                     AND (dir.prefix || obj.name) LIKE '%' || ? || '%'",
            );
            if use_depth {
                sql.push_str(
                    " AND (LENGTH(dir.prefix) - LENGTH(REPLACE(dir.prefix, '/', ''))) <= ?",
                );
            }
            sql.push_str(") ");
            sql.push_str(
                "SELECT o.checksum_sha256, COUNT(*) as count, \
                        SUM(COALESCE(o.size, 0)) as total_size \
                 FROM objects o \
                 INNER JOIN matching m ON o.checksum_sha256 = m.checksum_sha256 \
                 GROUP BY o.checksum_sha256 \
                 HAVING COUNT(*) > 1",
            );
        } else {
            sql.push_str(
                "SELECT checksum_sha256, COUNT(*) as count, \
                        SUM(COALESCE(size, 0)) as total_size \
                 FROM objects \
                 WHERE checksum_sha256 IS NOT NULL \
                 GROUP BY checksum_sha256 \
                 HAVING COUNT(*) > 1",
            );
        }

        if use_cursor {
            if use_filter {
                sql.push_str(
                    " AND (total_size < ? OR (total_size = ? AND \
                     o.checksum_sha256 > ?))",
                );
            } else {
                sql.push_str(
                    " AND (total_size < ? OR (total_size = ? AND \
                     checksum_sha256 > ?))",
                );
            }
        }

        if use_filter {
            sql.push_str(" ORDER BY total_size DESC, o.checksum_sha256 ASC LIMIT ?");
        } else {
            sql.push_str(" ORDER BY total_size DESC, checksum_sha256 ASC LIMIT ?");
        }

        // Bind parameters in the same order as placeholders above.
        let max_slash_count =
            max_depth.and_then(|d| key_contains.map(|kc| kc.matches('/').count() as i32 + d));

        let mut query = sqlx::query_as::<_, DuplicateGroup>(&sql);

        if let Some(term) = key_contains {
            query = query.bind(term);
        }
        if let Some(ms) = max_slash_count {
            query = query.bind(ms);
        }
        if let Some((size, hash)) = cursor {
            query = query.bind(size).bind(size).bind(hash);
        }
        query = query.bind(max_results);

        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows)
    }

    /// Get all objects with a specific checksum_sha256.
    pub async fn get_objects_by_hash(
        &self,
        checksum_sha256: &str,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let records = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
             WHERE o.checksum_sha256 = ? ORDER BY d.prefix, o.name",
        )
        .bind(checksum_sha256)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Keyset-paginated fetch of objects ordered by checksum_sha256.
    /// Used by the cross-bucket merge cursor.
    pub async fn fetch_objects_by_hash_page(
        &self,
        after: Option<&str>,
        limit: i32,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let records = match after {
            Some(hash) => {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.checksum_sha256 IS NOT NULL AND o.checksum_sha256 >= ? \
                     ORDER BY o.checksum_sha256, d.prefix, o.name LIMIT ?",
                )
                .bind(hash)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE o.checksum_sha256 IS NOT NULL \
                     ORDER BY o.checksum_sha256, d.prefix, o.name LIMIT ?",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };

        Ok(records)
    }

    /// Lightweight query for directory hash computation: returns only
    /// (name, checksum_sha256, size) for direct children of a directory.
    /// Uses `parent_dir_id` FK instead of a prefix range scan.
    pub async fn get_dir_children_for_hashing(
        &self,
        parent_dir: &str,
    ) -> Result<Vec<(String, Option<String>, Option<i64>)>, S3Error> {
        let prefix = if parent_dir.is_empty() {
            String::new()
        } else {
            format!("{parent_dir}/")
        };
        let dir_id: Option<(i64,)> = sqlx::query_as("SELECT id FROM directories WHERE prefix = ?")
            .bind(&prefix)
            .fetch_optional(&self.pool)
            .await?;
        let Some((dir_id,)) = dir_id else {
            return Ok(Vec::new());
        };
        let rows: Vec<(String, Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT name, checksum_sha256, size FROM objects \
             WHERE parent_dir_id = ? ORDER BY name",
        )
        .bind(dir_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Get all objects with a given key prefix.
    pub async fn get_objects_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<ObjectRecord>, S3Error> {
        let (dir_prefix, name_prefix) = split_key(prefix);
        let records = if name_prefix.is_empty() {
            if let Some(upper) = prefix_upper_bound(&dir_prefix) {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     WHERE d.prefix >= ? AND d.prefix < ? \
                     ORDER BY d.prefix, o.name",
                )
                .bind(&dir_prefix)
                .bind(&upper)
                .fetch_all(&self.pool)
                .await?
            } else {
                sqlx::query_as::<_, ObjectRecord>(
                    "SELECT o.*, d.prefix || o.name AS key \
                     FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                     ORDER BY d.prefix, o.name",
                )
                .fetch_all(&self.pool)
                .await?
            }
        } else {
            let name_upper = prefix_upper_bound(name_prefix).unwrap_or_default();
            let du = prefix_upper_bound(prefix).unwrap_or_default();
            sqlx::query_as::<_, ObjectRecord>(
                "SELECT o.*, d.prefix || o.name AS key \
                 FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
                 WHERE (d.prefix = ? AND o.name >= ? AND o.name < ?) \
                    OR (d.prefix > ? AND d.prefix < ?) \
                 ORDER BY d.prefix, o.name",
            )
            .bind(&dir_prefix)
            .bind(name_prefix)
            .bind(&name_upper)
            .bind(&dir_prefix)
            .bind(&du)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(records)
    }

    /// Get all objects at scan level 3 (content-hashed).
    pub async fn get_all_objects_at_level_3(&self) -> Result<Vec<ObjectRecord>, S3Error> {
        let records = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
             WHERE o.scan_level >= 3 ORDER BY d.prefix, o.name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get a single object by its UUID id.
    pub async fn get_object_by_id(&self, id: &str) -> Result<Option<ObjectRecord>, S3Error> {
        let record = sqlx::query_as::<_, ObjectRecord>(
            "SELECT o.*, d.prefix || o.name AS key \
             FROM objects o JOIN directories d ON o.parent_dir_id = d.id \
             WHERE o.id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    /// Delete an object by its UUID id. Returns true if a row was deleted.
    pub async fn delete_object_by_id(&self, id: &str) -> Result<bool, S3Error> {
        let result = sqlx::query("DELETE FROM objects WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    // -------------------------------------------------------------------------
    // Phase 8: Directory Methods
    // -------------------------------------------------------------------------

    /// Update hash data for an existing directory row.
    pub async fn upsert_directory_hash(&self, record: &DirectoryRecord) -> Result<(), S3Error> {
        sqlx::query(
            "UPDATE directories SET \
                dir_hash = ?, \
                file_count = ?, \
                total_size = ?, \
                computed_at = ?, \
                stale = FALSE \
             WHERE prefix = ?",
        )
        .bind(&record.dir_hash)
        .bind(record.file_count)
        .bind(record.total_size)
        .bind(record.computed_at)
        .bind(&record.prefix)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Find duplicate directory groups by dir_hash.
    pub async fn find_duplicate_dir_hashes(
        &self,
        min_files: i32,
        max_results: i32,
        prefix: Option<&str>,
        after_hash: Option<&str>,
        max_depth: Option<i32>,
    ) -> Result<Vec<DuplicateDirGroup>, S3Error> {
        let mut sql = String::from(
            "SELECT dir_hash, COUNT(*) as count, \
             MIN(file_count) as file_count, SUM(total_size) as total_size \
             FROM directories \
             WHERE stale = FALSE AND file_count >= ?",
        );

        if prefix.is_some() || max_depth.is_some() {
            let mut sub_conditions = vec!["stale = FALSE".to_string()];
            if prefix.is_some() {
                sub_conditions.push("prefix LIKE ?".to_string());
            }
            if max_depth.is_some() {
                sub_conditions
                    .push("(LENGTH(prefix) - LENGTH(REPLACE(prefix, '/', ''))) <= ?".to_string());
            }
            sql.push_str(&format!(
                " AND dir_hash IN (SELECT dir_hash FROM directories WHERE {})",
                sub_conditions.join(" AND ")
            ));
        }

        if after_hash.is_some() {
            sql.push_str(" AND dir_hash > ?");
        }

        sql.push_str(" GROUP BY dir_hash HAVING COUNT(*) > 1 ORDER BY dir_hash ASC LIMIT ?");

        let like_pattern = prefix.map(|p| format!("{}%", p));
        let max_slash_count = max_depth.map(|d| {
            let base_slashes = prefix.map_or(0, |p| p.matches('/').count() as i32);
            base_slashes + d
        });

        let mut query = sqlx::query_as::<_, DuplicateDirGroup>(&sql);
        query = query.bind(min_files);
        if let Some(ref lp) = like_pattern {
            query = query.bind(lp.as_str());
        }
        if let Some(ms) = max_slash_count {
            query = query.bind(ms);
        }
        if let Some(ah) = after_hash {
            query = query.bind(ah);
        }
        query = query.bind(max_results);

        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows)
    }

    /// Get all directory records with a specific dir_hash.
    pub async fn get_dirs_by_hash(&self, dir_hash: &str) -> Result<Vec<DirectoryRecord>, S3Error> {
        let records = sqlx::query_as::<_, DirectoryRecord>(
            "SELECT * FROM directories WHERE dir_hash = ? ORDER BY prefix",
        )
        .bind(dir_hash)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get all distinct parent directory prefixes.
    pub async fn list_parent_directories(&self) -> Result<Vec<String>, S3Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT prefix FROM directories \
             WHERE prefix != '' ORDER BY prefix",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(p,)| p).collect())
    }

    /// Get parent directories that need hash (re)computation:
    /// either not yet computed or marked stale.
    pub async fn list_unhashed_parent_directories(&self) -> Result<Vec<String>, S3Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT prefix FROM directories \
             WHERE prefix != '' AND stale = TRUE \
             ORDER BY prefix",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(p,)| p.strip_suffix('/').unwrap_or(&p).to_string())
            .collect())
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

    // -- CORS rules (Phase 9) --

    /// Get CORS rules from the bucket_config table.
    pub async fn get_cors_rules(&self) -> Result<Vec<crate::types::cors::CorsRule>, S3Error> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM bucket_config WHERE key = 'cors_rules'")
                .fetch_optional(&self.pool)
                .await?;
        match row {
            Some((json,)) => serde_json::from_str(&json).map_err(|e| {
                tracing::error!("Failed to parse CORS rules JSON: {e}");
                S3Error::InternalError
            }),
            None => Ok(Vec::new()),
        }
    }

    /// Set CORS rules in the bucket_config table (upsert).
    pub async fn set_cors_rules(
        &self,
        rules: &[crate::types::cors::CorsRule],
    ) -> Result<(), S3Error> {
        let json = serde_json::to_string(rules).map_err(|e| {
            tracing::error!("Failed to serialize CORS rules: {e}");
            S3Error::InternalError
        })?;
        let now = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO bucket_config (key, value, updated_at) VALUES ('cors_rules', ?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = ?2",
        )
        .bind(&json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Delete CORS rules from the bucket_config table.
    pub async fn delete_cors_rules(&self) -> Result<(), S3Error> {
        sqlx::query("DELETE FROM bucket_config WHERE key = 'cors_rules'")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // -- Webhook configs (Phase 9) --

    /// Get webhook configurations from the bucket_config table.
    pub async fn get_webhook_configs(
        &self,
    ) -> Result<Vec<crate::types::notification::WebhookConfig>, S3Error> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM bucket_config WHERE key = 'webhooks'")
                .fetch_optional(&self.pool)
                .await?;
        match row {
            Some((json,)) => serde_json::from_str(&json).map_err(|e| {
                tracing::error!("Failed to parse webhook configs JSON: {e}");
                S3Error::InternalError
            }),
            None => Ok(Vec::new()),
        }
    }

    /// Set webhook configurations in the bucket_config table (upsert).
    pub async fn set_webhook_configs(
        &self,
        webhooks: &[crate::types::notification::WebhookConfig],
    ) -> Result<(), S3Error> {
        let json = serde_json::to_string(webhooks).map_err(|e| {
            tracing::error!("Failed to serialize webhook configs: {e}");
            S3Error::InternalError
        })?;
        let now = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO bucket_config (key, value, updated_at) VALUES ('webhooks', ?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = ?1, updated_at = ?2",
        )
        .bind(&json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Log a webhook delivery attempt.
    pub async fn log_delivery(
        &self,
        webhook_id: &str,
        object_key: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<(), S3Error> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        sqlx::query(
            "INSERT INTO notification_delivery_log (id, webhook_id, event_type, object_key, delivered_at, attempts, last_error, status) \
             VALUES (?1, ?2, 'webhook', ?3, ?4, 1, ?5, ?6)",
        )
        .bind(&id)
        .bind(webhook_id)
        .bind(object_key)
        .bind(&now)
        .bind(error)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // -- Bucket stats --

    /// Aggregate stats for a bucket: total files, total size,
    /// duplicate file/folder counts, and reclaimable storage.
    pub async fn get_bucket_stats(&self) -> Result<BucketStats, S3Error> {
        // 1) Total files & total size
        let (total_files, total_size): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM objects")
                .fetch_one(&self.pool)
                .await?;

        // 2) Duplicate files count & reclaimable bytes
        let (duplicate_files, storage_reclaimable): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(cnt - 1), 0), \
                    COALESCE(SUM(total_size - (total_size / cnt)), 0) \
             FROM ( \
               SELECT COUNT(*) AS cnt, SUM(COALESCE(size, 0)) AS total_size \
               FROM objects \
               WHERE checksum_sha256 IS NOT NULL \
               GROUP BY checksum_sha256 \
               HAVING COUNT(*) > 1 \
             )",
        )
        .fetch_one(&self.pool)
        .await?;

        // 3) Duplicate folders count (groups of dirs sharing a hash)
        let (duplicate_folders,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM ( \
               SELECT dir_hash FROM directories \
               WHERE stale = FALSE AND file_count > 0 \
               GROUP BY dir_hash \
               HAVING COUNT(*) > 1 \
             )",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(BucketStats {
            total_files,
            total_size,
            duplicate_files,
            duplicate_folders,
            storage_reclaimable,
        })
    }
}

/// Bucket-level aggregate statistics.
pub struct BucketStats {
    pub total_files: i64,
    pub total_size: i64,
    pub duplicate_files: i64,
    pub duplicate_folders: i64,
    pub storage_reclaimable: i64,
}

/// Scan status summary for a bucket.
pub struct ScanStatus {
    pub total_files: i64,
    pub files_at_level_3: i64,
}

/// A group of files sharing the same checksum_sha256.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DuplicateGroup {
    pub checksum_sha256: String,
    pub count: i32,
    pub total_size: i64,
}

/// A group of directories sharing the same dir_hash.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DuplicateDirGroup {
    pub dir_hash: String,
    pub count: i32,
    pub file_count: i32,
    pub total_size: i64,
}

/// A directory record from the `directories` table.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DirectoryRecord {
    pub id: i64,
    pub prefix: String,
    pub dir_hash: Option<String>,
    pub file_count: Option<i32>,
    pub total_size: Option<i64>,
    pub computed_at: Option<SqliteTimestamp>,
    pub stale: bool,
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

    async fn make_store(tmp: &TempDir) -> MetadataStore {
        let db_path = tmp.path().join("test.db");
        MetadataStore::new(&db_path).await.unwrap()
    }

    async fn make_record(store: &MetadataStore, key: &str) -> ObjectRecord {
        let now = SqliteTimestamp::now();
        let parent = key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default();
        let dir_id = store.get_or_create_dir_id(&parent).await.unwrap();
        let ct_id = store
            .get_or_create_content_type_id("application/octet-stream")
            .await
            .unwrap();
        let (_, filename) = split_key(key);

        ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            name: filename.to_string(),
            parent_dir_id: dir_id,
            key: key.to_string(),
            size: Some(1024),
            file_mtime: Some(now),
            content_type_id: Some(ct_id),
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

        let obj = make_record(&store, "photos/cat.jpg").await;
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

        let obj = make_record(&store, "to-delete.txt").await;
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

        let r = make_record(&store, "photos/a.jpg").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "photos/b.jpg").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "docs/readme.md").await;
        store.insert_object(&r).await.unwrap();

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

        let r = make_record(&store, "a.txt").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "b.txt").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "c.txt").await;
        store.insert_object(&r).await.unwrap();

        let limited = store.list_objects("", 2).await.unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_upsert_object() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let mut obj = make_record(&store, "test.txt").await;
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

        let r = make_record(&store, "photos/a.jpg").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "photos/b.jpg").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "photos/sub/c.jpg").await;
        store.insert_object(&r).await.unwrap();

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

        let r = make_record(&store, "a.txt").await;
        store.insert_object(&r).await.unwrap();
        let r = make_record(&store, "b.txt").await;
        store.insert_object(&r).await.unwrap();

        assert_eq!(store.count_objects().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_symlink_record() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp).await;

        let now = SqliteTimestamp::now();
        let dir_id = store.get_or_create_dir_id("").await.unwrap();
        let ct_id = store
            .get_or_create_content_type_id("application/x-symlink")
            .await
            .unwrap();
        let obj = ObjectRecord {
            id: uuid::Uuid::new_v4().to_string(),
            name: "my-link".to_string(),
            parent_dir_id: dir_id,
            key: "my-link".to_string(),
            is_symlink: true,
            symlink_target: Some("/some/target".to_string()),
            content_type_id: Some(ct_id),
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
