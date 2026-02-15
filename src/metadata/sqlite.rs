use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::error::S3Error;

/// Object metadata record, matching the `objects` table schema.
///
/// Timestamp fields use `time::OffsetDateTime`. The sqlx `time` feature
/// serialises these as RFC 3339 TEXT in SQLite, giving direct comparisons
/// without runtime parsing and human-readable storage.
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

    // L3 metadata (None until content-hashed)
    pub etag: Option<String>,
    pub content_hash: Option<String>,

    // S3 metadata
    pub content_type: Option<String>,
    pub last_modified: time::OffsetDateTime,
    pub created_at: time::OffsetDateTime,
    pub metadata: Option<String>,

    pub scan_level: i32,
}

/// SQLite-backed metadata store for a single bucket.
pub struct MetadataStore {
    pool: SqlitePool,
}

impl MetadataStore {
    /// Open (or create) the metadata database at the given path and run migrations.
    pub async fn new(db_path: &Path) -> Result<Self, S3Error> {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);

        // TODO: Make max_connections configurable (per-bucket or global).
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

    // TODO: Replace `SELECT *` with explicit column lists in get/list queries
    // to avoid loading heavy columns (e.g. `metadata` JSON) when not needed.

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
                size, file_mtime, etag, content_hash,
                content_type, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&obj.id)
        .bind(&obj.key)
        .bind(&obj.parent_directory)
        .bind(obj.is_directory)
        .bind(obj.is_symlink)
        .bind(&obj.symlink_target)
        .bind(obj.size)
        .bind(obj.file_mtime)
        .bind(&obj.etag)
        .bind(&obj.content_hash)
        .bind(&obj.content_type)
        .bind(obj.last_modified)
        .bind(obj.created_at)
        .bind(&obj.metadata)
        .bind(obj.scan_level)
        .execute(&self.pool)
        .await?;

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
                size, file_mtime, etag, content_hash,
                content_type, last_modified, created_at, metadata, scan_level
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                parent_directory = excluded.parent_directory,
                is_directory = excluded.is_directory,
                is_symlink = excluded.is_symlink,
                symlink_target = excluded.symlink_target,
                size = excluded.size,
                file_mtime = excluded.file_mtime,
                etag = excluded.etag,
                content_hash = excluded.content_hash,
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
        .bind(&obj.etag)
        .bind(&obj.content_hash)
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

    /// Get the total count of objects in the store.
    pub async fn count_objects(&self) -> Result<i64, S3Error> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }
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
            is_directory: false,
            is_symlink: false,
            symlink_target: None,
            size: Some(1024),
            file_mtime: Some(now),
            etag: None,
            content_hash: None,
            content_type: Some("application/octet-stream".to_string()),
            last_modified: now,
            created_at: now,
            metadata: None,
            scan_level: 1,
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
            parent_directory: String::new(),
            is_directory: false,
            is_symlink: true,
            symlink_target: Some("/some/target".to_string()),
            size: None,
            file_mtime: None,
            etag: None,
            content_hash: None,
            content_type: Some("application/x-symlink".to_string()),
            last_modified: now,
            created_at: now,
            metadata: None,
            scan_level: 1,
        };

        store.insert_object(&obj).await.unwrap();

        let fetched = store.get_object("my-link").await.unwrap().unwrap();
        assert!(fetched.is_symlink);
        assert_eq!(fetched.symlink_target.as_deref(), Some("/some/target"));
    }
}
