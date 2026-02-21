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

    let now = OffsetDateTime::now_utc();

    // Copy metadata to destination
    let dst_record = ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        key: dst_key.to_string(),
        parent_directory: dst_key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default(),
        size: src_record.size,
        etag: src_record.etag.clone(),
        content_hash: src_record.content_hash.clone(),
        content_type: src_record.content_type.clone(),
        last_modified: now,
        created_at: now,
        metadata: src_record.metadata.clone(),
        scan_level: src_record.scan_level,
        ..Default::default()
    };

    dst_metadata.upsert_object(&dst_record).await?;

    Ok(CopyResult {
        etag: src_record.etag.unwrap_or_default(),
        last_modified: dst_record.last_modified,
    })
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
        if src_record.last_modified <= if_modified_since {
            return Err(S3Error::PreconditionFailed);
        }
    }

    if let Some(if_unmodified_since) = conditions.if_unmodified_since {
        if src_record.last_modified > if_unmodified_since {
            return Err(S3Error::PreconditionFailed);
        }
    }

    Ok(())
}
