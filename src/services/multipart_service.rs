use std::collections::HashMap;
use std::path::Path;

use futures::StreamExt;
use md5::{Digest, Md5};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWriteExt;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::types::multipart::*;

/// Initiate a new multipart upload.
pub async fn initiate(
    metadata: &MetadataStore,
    parts_dir: &Path,
    key: &str,
    content_type: Option<&str>,
    user_metadata: Option<HashMap<String, String>>,
) -> Result<String, S3Error> {
    let upload_id = uuid::Uuid::new_v4().to_string();

    // Create parts directory for this upload
    let upload_parts_dir = parts_dir.join(&upload_id);
    tokio::fs::create_dir_all(&upload_parts_dir).await?;

    // Record in database
    let upload = MultipartUpload {
        id: upload_id.clone(),
        key: key.to_string(),
        initiated_at: time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
        content_type: content_type.map(String::from),
        metadata: user_metadata.map(|m| serde_json::to_string(&m).unwrap()),
    };

    metadata.insert_multipart_upload(&upload).await?;

    Ok(upload_id)
}

/// Upload a single part of a multipart upload.
pub async fn upload_part<S>(
    metadata: &MetadataStore,
    parts_dir: &Path,
    upload_id: &str,
    part_number: i32,
    stream: S,
) -> Result<String, S3Error>
where
    S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
{
    // Verify upload exists
    metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or(S3Error::NoSuchUpload)?;

    // Validate part number (1-10000)
    if !(1..=10000).contains(&part_number) {
        return Err(S3Error::InvalidPart);
    }

    // Stream part body to disk, computing MD5 incrementally
    let part_path = parts_dir
        .join(upload_id)
        .join(format!("{:05}", part_number));

    let mut file = tokio::fs::File::create(&part_path).await?;
    let mut hasher = Md5::new();
    let mut written: u64 = 0;

    let mut stream = std::pin::pin!(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| S3Error::InternalError)?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        written += chunk.len() as u64;
    }
    file.flush().await?;

    let etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    // Record part in database
    let part = Part {
        id: uuid::Uuid::new_v4().to_string(),
        upload_id: upload_id.to_string(),
        part_number,
        size: written as i64,
        etag: etag.clone(),
        uploaded_at: time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap(),
    };

    metadata.upsert_part(&part).await?;

    Ok(etag)
}
