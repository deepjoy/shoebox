use std::collections::HashMap;
use std::path::Path;

use futures::StreamExt;
use md5::{Digest, Md5};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWriteExt;

use crate::error::S3Error;
use crate::metadata::sqlite::ObjectRecord;
use crate::metadata::MetadataStore;
use crate::storage::FilesystemStorage;
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

/// Complete a multipart upload by assembling all parts.
pub async fn complete(
    storage: &FilesystemStorage,
    metadata: &MetadataStore,
    parts_dir: &Path,
    bucket_name: &str,
    upload_id: &str,
    parts: Vec<(i32, String)>, // (part_number, etag)
) -> Result<CompleteResult, S3Error> {
    let upload = metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or(S3Error::NoSuchUpload)?;

    // Verify all parts exist and ETags match
    let db_parts = metadata.list_parts(upload_id).await?;
    let db_parts_map: HashMap<i32, &Part> = db_parts.iter().map(|p| (p.part_number, p)).collect();

    let mut total_size = 0i64;
    let mut md5_hasher = Md5::new();

    for (part_num, expected_etag) in &parts {
        let db_part = db_parts_map.get(part_num).ok_or(S3Error::InvalidPart)?;

        if &db_part.etag != expected_etag {
            return Err(S3Error::InvalidPart);
        }

        // Include part MD5 in composite hash
        let md5_bytes = hex::decode(db_part.etag.trim_matches('"')).unwrap();
        md5_hasher.update(&md5_bytes);

        total_size += db_part.size;
    }

    // Create final file by streaming parts sequentially — no full-part buffering
    let final_path = storage.resolve_path(&upload.key).await?;

    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut final_file = tokio::fs::File::create(&final_path).await?;

    for (part_num, _) in &parts {
        let part_path = parts_dir.join(upload_id).join(format!("{:05}", part_num));

        let mut part_file = tokio::fs::File::open(&part_path).await?;
        tokio::io::copy(&mut part_file, &mut final_file).await?;
    }

    final_file.flush().await?;

    // Compute multipart ETag: MD5(MD5(part1) + MD5(part2) + ...) + "-" + num_parts
    let composite_md5 = hex::encode(md5_hasher.finalize());
    let etag = format!("\"{}-{}\"", composite_md5, parts.len());

    // Create object record
    let obj = ObjectRecord {
        id: uuid::Uuid::new_v4().to_string(),
        key: upload.key.clone(),
        parent_directory: upload
            .key
            .rsplit_once('/')
            .map(|(p, _)| p.to_string())
            .unwrap_or_default(),
        size: Some(total_size),
        etag: Some(etag.clone()),
        content_type: Some(
            upload
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        ),
        last_modified: time::OffsetDateTime::now_utc(),
        created_at: time::OffsetDateTime::now_utc(),
        metadata: upload.metadata,
        scan_level: 2, // Has metadata, needs L3 for content_hash
        ..Default::default()
    };

    metadata.upsert_object(&obj).await?;

    // Clean up parts directory
    tokio::fs::remove_dir_all(parts_dir.join(upload_id)).await?;

    // Remove upload record
    metadata.delete_multipart_upload(upload_id).await?;

    Ok(CompleteResult {
        location: format!("/{}/{}", bucket_name, upload.key),
        bucket: bucket_name.to_string(),
        key: upload.key,
        etag,
    })
}

/// Abort a multipart upload and clean up its parts.
pub async fn abort(
    metadata: &MetadataStore,
    parts_dir: &Path,
    upload_id: &str,
) -> Result<(), S3Error> {
    // Verify upload exists
    metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or(S3Error::NoSuchUpload)?;

    // Remove parts directory
    let upload_parts_dir = parts_dir.join(upload_id);
    if upload_parts_dir.exists() {
        tokio::fs::remove_dir_all(&upload_parts_dir).await?;
    }

    // Remove from database (cascades to parts table)
    metadata.delete_multipart_upload(upload_id).await?;

    Ok(())
}

/// List parts of a multipart upload.
pub async fn list_parts(
    metadata: &MetadataStore,
    bucket_name: &str,
    upload_id: &str,
    max_parts: i32,
    part_number_marker: Option<i32>,
) -> Result<ListPartsResult, S3Error> {
    let upload = metadata
        .get_multipart_upload(upload_id)
        .await?
        .ok_or(S3Error::NoSuchUpload)?;

    let parts = metadata
        .list_parts_paginated(upload_id, max_parts, part_number_marker)
        .await?;

    let is_truncated = parts.len() as i32 == max_parts;
    let next_marker = if is_truncated {
        parts.last().map(|p| p.part_number)
    } else {
        None
    };

    Ok(ListPartsResult {
        bucket: bucket_name.to_string(),
        key: upload.key,
        upload_id: upload_id.to_string(),
        parts: parts
            .into_iter()
            .map(|p| PartInfo {
                part_number: p.part_number,
                last_modified: p.uploaded_at,
                etag: p.etag,
                size: p.size,
            })
            .collect(),
        is_truncated,
        next_part_number_marker: next_marker,
    })
}
