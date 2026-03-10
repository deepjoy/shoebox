use time::OffsetDateTime;

use crate::error::S3Error;
use crate::metadata::sqlite::ObjectRecord;
use crate::metadata::MetadataStore;
use crate::storage::FilesystemStorage;

/// Conditions for a CopyObject request (from x-amz-copy-source-* headers).
#[derive(Default)]
pub struct CopyConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

/// Result of a successful CopyObject.
pub struct CopyResult {
    /// The persisted object ID (from the `objects.id` column).
    pub object_id: String,
    /// Size of the copied object in bytes.
    pub size: i64,
    pub etag: String,
    pub last_modified: OffsetDateTime,
}

/// Copy an object from one location to another (same or cross bucket).
pub async fn copy_object(
    src_storage: &FilesystemStorage,
    src_metadata: &MetadataStore,
    src_key: &str,
    dst_storage: &FilesystemStorage,
    dst_metadata: &MetadataStore,
    dst_key: &str,
    conditions: &CopyConditions,
) -> Result<CopyResult, S3Error> {
    // Get source metadata
    let src_record = src_metadata
        .get_object(src_key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    // Check conditional headers
    check_copy_conditions(&src_record, conditions)?;

    // Perform copy
    let src_path = src_storage.resolve_path(src_key).await?;
    let dst_path = dst_storage.resolve_path(dst_key).await?;

    // Create parent directories
    if let Some(parent) = dst_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Kernel-level copy (efficient, no app memory buffering)
    tokio::fs::copy(&src_path, &dst_path).await?;

    let now = crate::metadata::sqlite::SqliteTimestamp::now();
    let parent = dst_key
        .rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .unwrap_or_default();
    let dir_id = dst_metadata.get_or_create_dir_id(&parent).await?;

    // Copy metadata to destination
    let (_, filename) = crate::metadata::sqlite::split_key(dst_key);
    let dst_record = ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        name: filename.to_string(),
        parent_dir_id: dir_id,
        key: dst_key.to_string(),
        size: src_record.size,
        etag: src_record.etag.clone(),
        checksum_sha256: src_record.checksum_sha256.clone(),
        checksum_sha1: src_record.checksum_sha1.clone(),
        checksum_crc32: src_record.checksum_crc32.clone(),
        checksum_crc32c: src_record.checksum_crc32c.clone(),
        content_type_id: src_record.content_type_id,
        last_modified: now,
        created_at: now,
        metadata: src_record.metadata.clone(),
        scan_level: src_record.scan_level,
        ..Default::default()
    };

    let object_id = dst_metadata.upsert_object(&dst_record).await?;

    Ok(CopyResult {
        object_id,
        size: src_record.size.unwrap_or(0),
        etag: src_record.etag.unwrap_or_default(),
        last_modified: *dst_record.last_modified,
    })
}

/// Rename (move) an object within the same bucket.
/// Uses an atomic filesystem rename (O(1) on the same filesystem).
pub async fn rename_object(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    src_key: &str,
    dst_key: &str,
    overwrite: bool,
) -> Result<(), S3Error> {
    let src_path = storage.resolve_path(src_key).await?;
    let dst_path = storage.resolve_path(dst_key).await?;

    // Check source exists
    if !tokio::fs::try_exists(&src_path).await.unwrap_or(false) {
        return Err(S3Error::NoSuchKey);
    }

    // Check destination
    if tokio::fs::try_exists(&dst_path).await.unwrap_or(false) && !overwrite {
        return Err(S3Error::Conflict("Destination already exists".to_string()));
    }

    // Create parent directories
    if let Some(parent) = dst_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Atomic rename (O(1) operation on same filesystem)
    tokio::fs::rename(&src_path, &dst_path).await?;

    // Update database
    metadata.rename_object(src_key, dst_key).await?;

    Ok(())
}

fn check_copy_conditions(
    src_record: &ObjectRecord,
    conditions: &CopyConditions,
) -> Result<(), S3Error> {
    if let Some(ref if_match) = conditions.if_match {
        if src_record.etag.as_ref() != Some(if_match) {
            return Err(S3Error::PreconditionFailed);
        }
    }

    if let Some(ref if_none_match) = conditions.if_none_match {
        if src_record.etag.as_ref() == Some(if_none_match) {
            return Err(S3Error::PreconditionFailed);
        }
    }

    if let Some(if_modified_since) = conditions.if_modified_since {
        if src_record.last_modified <= crate::metadata::sqlite::SqliteTimestamp(if_modified_since) {
            return Err(S3Error::PreconditionFailed);
        }
    }

    if let Some(if_unmodified_since) = conditions.if_unmodified_since {
        if src_record.last_modified > crate::metadata::sqlite::SqliteTimestamp(if_unmodified_since)
        {
            return Err(S3Error::PreconditionFailed);
        }
    }

    Ok(())
}
