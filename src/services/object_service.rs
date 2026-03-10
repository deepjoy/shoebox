use std::collections::HashMap;

use crate::error::S3Error;
use crate::metadata::sqlite::ObjectRecord;
use crate::metadata::MetadataStore;
use crate::storage::filesystem::{FileContent, FilesystemStorage};
use crate::types::ChecksumValues;

/// Result of a GetObject operation.
pub struct GetObjectResult {
    pub content: FileContent,
    pub record: ObjectRecord,
}

/// Input parameters for PutObject.
pub struct PutObjectInput {
    pub content_type: String,
    pub user_metadata: HashMap<String, String>,
    /// Base64-encoded MD5 from the `Content-MD5` header, if provided.
    pub content_md5: Option<String>,
    /// Client-provided checksums for validation (base64-encoded).
    pub checksum_sha256: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
}

/// Result of a PutObject operation.
pub struct PutObjectResult {
    /// The persisted object ID (from the `objects.id` column).
    pub object_id: String,
    /// Size of the written content in bytes.
    pub size: i64,
    /// Quoted ETag, e.g. `"d41d8cd98f00b204e9800998ecf8427e"`.
    pub etag: String,
    /// Computed checksums for all four S3 algorithms.
    pub checksums: ChecksumValues,
}

/// Get an object's content and metadata.
pub async fn get_object(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    key: &str,
) -> Result<GetObjectResult, S3Error> {
    let record = metadata.get_object(key).await?.ok_or(S3Error::NoSuchKey)?;
    let content = storage.get(key).await?;
    Ok(GetObjectResult { content, record })
}

/// Upload an object, computing ETag and checksums, optionally validating
/// Content-MD5 and client-provided additional checksums.
pub async fn put_object(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    key: &str,
    stream: impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
    input: PutObjectInput,
) -> Result<PutObjectResult, S3Error> {
    let result = storage.put(key, stream).await?;
    let etag = format!("\"{}\"", result.md5_hex);

    // Validate Content-MD5 if the client provided it.
    if let Some(ref client_md5) = input.content_md5 {
        use base64::Engine;
        let md5_bytes = hex::decode(&result.md5_hex).map_err(|_| S3Error::InternalError)?;
        let computed = base64::engine::general_purpose::STANDARD.encode(&md5_bytes);
        if *client_md5 != computed {
            let _ = storage.delete(key).await;
            return Err(S3Error::BadDigest);
        }
    }

    // Validate additional checksums if the client provided them.
    let checks: &[(&Option<String>, &Option<String>)] = &[
        (&input.checksum_sha256, &result.checksums.sha256),
        (&input.checksum_sha1, &result.checksums.sha1),
        (&input.checksum_crc32, &result.checksums.crc32),
        (&input.checksum_crc32c, &result.checksums.crc32c),
    ];
    for (client, computed) in checks {
        if let (Some(client_val), Some(computed_val)) = (client, computed) {
            if client_val != computed_val {
                let _ = storage.delete(key).await;
                return Err(S3Error::BadDigest);
            }
        }
    }

    let now = crate::metadata::sqlite::SqliteTimestamp::now();
    let parent = key
        .rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .unwrap_or_default();
    let dir_id = metadata.get_or_create_dir_id(&parent).await?;
    let ct_id = metadata
        .get_or_create_content_type_id(&input.content_type)
        .await?;
    let obj = ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        key: key.to_string(),
        parent_dir_id: dir_id,
        size: Some(result.bytes_written as i64),
        file_mtime: Some(now),
        etag: Some(etag.clone()),
        checksum_sha256: result.checksums.sha256.clone(),
        checksum_sha1: result.checksums.sha1.clone(),
        checksum_crc32: result.checksums.crc32.clone(),
        checksum_crc32c: result.checksums.crc32c.clone(),
        content_type_id: Some(ct_id),
        last_modified: now,
        created_at: now,
        metadata: if input.user_metadata.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&input.user_metadata).unwrap())
        },
        scan_level: 3,
        ..Default::default()
    };

    let object_id = metadata.upsert_object(&obj).await?;
    Ok(PutObjectResult {
        object_id,
        size: result.bytes_written as i64,
        etag,
        checksums: result.checksums,
    })
}

/// Delete an object. S3 returns success even if the key doesn't exist.
pub async fn delete_object(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    key: &str,
) -> Result<(), S3Error> {
    match storage.delete(key).await {
        Ok(()) => {}
        Err(S3Error::NoSuchKey) => {}
        Err(e) => return Err(e),
    }
    // _ discards the bool return value; the ? still propagates errors (clippy::let_underscore_must_use)
    let _ = metadata.delete_object(key).await?;
    Ok(())
}

/// Head an object — get metadata and verify the file still exists on disk.
pub async fn head_object(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    key: &str,
) -> Result<ObjectRecord, S3Error> {
    let record = metadata.get_object(key).await?.ok_or(S3Error::NoSuchKey)?;
    if !storage.exists(key).await? {
        return Err(S3Error::NoSuchKey);
    }
    Ok(record)
}
