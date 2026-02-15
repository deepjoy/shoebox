use std::collections::HashMap;

use crate::error::S3Error;
use crate::metadata::sqlite::ObjectRecord;
use crate::metadata::MetadataStore;
use crate::storage::filesystem::{FileContent, FilesystemStorage};

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
}

/// Result of a PutObject operation.
pub struct PutObjectResult {
    /// Quoted ETag, e.g. `"d41d8cd98f00b204e9800998ecf8427e"`.
    pub etag: String,
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

/// Upload an object, computing ETag and optionally validating Content-MD5.
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
            // Digest mismatch — remove the file we just wrote.
            let _ = storage.delete(key).await;
            return Err(S3Error::BadDigest);
        }
    }

    let now = time::OffsetDateTime::now_utc();
    let obj = ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        key: key.to_string(),
        parent_directory: key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default(),
        is_directory: false,
        is_symlink: false,
        symlink_target: None,
        size: Some(result.bytes_written as i64),
        file_mtime: Some(now),
        etag: Some(etag.clone()),
        content_hash: Some(result.md5_hex.clone()),
        content_type: Some(input.content_type),
        last_modified: now,
        created_at: now,
        metadata: if input.user_metadata.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&input.user_metadata).unwrap())
        },
        scan_level: 3,
    };

    metadata.upsert_object(&obj).await?;
    Ok(PutObjectResult { etag })
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
    let _ = metadata.delete_object(key).await;
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
