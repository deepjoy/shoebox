use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use quick_xml::se::to_string;
use serde::Deserialize;

use crate::api::responses::inject_xmlns;
use crate::error::S3Error;
use crate::services::{bucket_service::AppState, multipart_service};
use crate::types::multipart::*;

/// Query parameters for UploadPart
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPartQuery {
    pub part_number: Option<i32>,
    pub upload_id: Option<String>,
}

/// Query parameters for CompleteMultipartUpload
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompleteMultipartQuery {
    pub upload_id: Option<String>,
}

/// Query parameters for AbortMultipartUpload
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AbortMultipartQuery {
    pub upload_id: Option<String>,
}

/// Query parameters for ListParts
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListPartsQuery {
    pub upload_id: Option<String>,
    pub max_parts: Option<i32>,
    pub part_number_marker: Option<i32>,
}

/// POST /{bucket}/{key}?uploads — Initiate a multipart upload.
pub async fn initiate_multipart_upload(
    State(state): State<AppState>,
    AxumPath((bucket_name, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Parse x-amz-meta-* headers
    let mut user_metadata = std::collections::HashMap::new();
    for (name, value) in &headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(val) = value.to_str() {
                user_metadata.insert(meta_key.to_string(), val.to_string());
            }
        }
    }

    let upload_id = multipart_service::initiate(
        &bucket.metadata,
        &bucket.parts_dir,
        &key,
        content_type.as_deref(),
        if user_metadata.is_empty() {
            None
        } else {
            Some(user_metadata)
        },
    )
    .await?;

    #[derive(serde::Serialize)]
    struct InitiateMultipartUploadResult {
        #[serde(rename = "Bucket")]
        bucket: String,
        #[serde(rename = "Key")]
        key: String,
        #[serde(rename = "UploadId")]
        upload_id: String,
    }

    let result = InitiateMultipartUploadResult {
        bucket: bucket_name,
        key,
        upload_id,
    };

    let xml = to_string(&result).unwrap();
    let xml = inject_xmlns(&xml);

    Ok((StatusCode::OK, ([("content-type", "application/xml")], xml)).into_response())
}

/// PUT /{bucket}/{key}?partNumber=X&uploadId=Y — Upload a part.
pub async fn upload_part(
    State(state): State<AppState>,
    AxumPath((bucket_name, _key)): AxumPath<(String, String)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let part_number = query
        .part_number
        .ok_or_else(|| S3Error::BadRequest("Missing partNumber".to_string()))?;
    let upload_id = query
        .upload_id
        .ok_or_else(|| S3Error::BadRequest("Missing uploadId".to_string()))?;

    // Convert Axum body to stream with error mapping
    use futures::TryStreamExt;
    let stream = body.into_data_stream().map_err(std::io::Error::other);

    let etag = multipart_service::upload_part(
        &bucket.metadata,
        &bucket.parts_dir,
        &upload_id,
        part_number,
        stream,
    )
    .await?;

    Ok((StatusCode::OK, ([("etag", etag)], "")).into_response())
}

/// POST /{bucket}/{key}?uploadId=X — Complete a multipart upload.
pub async fn complete_multipart_upload(
    State(state): State<AppState>,
    AxumPath((bucket_name, _key)): AxumPath<(String, String)>,
    Query(query): Query<CompleteMultipartQuery>,
    body: String,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let upload_id = query
        .upload_id
        .ok_or_else(|| S3Error::BadRequest("Missing uploadId".to_string()))?;

    // Parse XML request body
    let request: CompleteMultipartUploadRequest = quick_xml::de::from_str(&body).map_err(|e| {
        tracing::warn!("Failed to parse CompleteMultipartUpload request: {e}");
        S3Error::BadRequest("Malformed XML".to_string())
    })?;

    let parts: Vec<(i32, String)> = request
        .parts
        .into_iter()
        .map(|p| (p.part_number, p.etag))
        .collect();

    let result = multipart_service::complete(
        &bucket.storage,
        &bucket.metadata,
        &bucket.parts_dir,
        &bucket_name,
        &upload_id,
        parts,
    )
    .await?;

    #[derive(serde::Serialize)]
    struct CompleteMultipartUploadResult {
        #[serde(rename = "Location")]
        location: String,
        #[serde(rename = "Bucket")]
        bucket: String,
        #[serde(rename = "Key")]
        key: String,
        #[serde(rename = "ETag")]
        etag: String,
    }

    let xml_result = CompleteMultipartUploadResult {
        location: result.location,
        bucket: result.bucket,
        key: result.key,
        etag: result.etag,
    };

    let xml = to_string(&xml_result).unwrap();
    let xml = inject_xmlns(&xml);

    Ok((StatusCode::OK, ([("content-type", "application/xml")], xml)).into_response())
}

/// DELETE /{bucket}/{key}?uploadId=X — Abort a multipart upload.
pub async fn abort_multipart_upload(
    State(state): State<AppState>,
    AxumPath((bucket_name, _key)): AxumPath<(String, String)>,
    Query(query): Query<AbortMultipartQuery>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let upload_id = query
        .upload_id
        .ok_or_else(|| S3Error::BadRequest("Missing uploadId".to_string()))?;

    multipart_service::abort(&bucket.metadata, &bucket.parts_dir, &upload_id).await?;

    Ok((StatusCode::NO_CONTENT, "").into_response())
}

/// GET /{bucket}/{key}?uploadId=X — List parts of a multipart upload.
pub async fn list_parts(
    State(state): State<AppState>,
    AxumPath((bucket_name, _key)): AxumPath<(String, String)>,
    Query(query): Query<ListPartsQuery>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let upload_id = query
        .upload_id
        .ok_or_else(|| S3Error::BadRequest("Missing uploadId".to_string()))?;

    let max_parts = query.max_parts.unwrap_or(1000).min(1000);

    let result = multipart_service::list_parts(
        &bucket.metadata,
        &bucket_name,
        &upload_id,
        max_parts,
        query.part_number_marker,
    )
    .await?;

    #[derive(serde::Serialize)]
    struct ListPartsResponse {
        #[serde(rename = "Bucket")]
        bucket: String,
        #[serde(rename = "Key")]
        key: String,
        #[serde(rename = "UploadId")]
        upload_id: String,
        #[serde(rename = "Part")]
        #[serde(skip_serializing_if = "Vec::is_empty")]
        parts: Vec<PartEntry>,
        #[serde(rename = "IsTruncated")]
        is_truncated: bool,
        #[serde(rename = "NextPartNumberMarker")]
        #[serde(skip_serializing_if = "Option::is_none")]
        next_part_number_marker: Option<i32>,
    }

    #[derive(serde::Serialize)]
    struct PartEntry {
        #[serde(rename = "PartNumber")]
        part_number: i32,
        #[serde(rename = "LastModified")]
        last_modified: String,
        #[serde(rename = "ETag")]
        etag: String,
        #[serde(rename = "Size")]
        size: i64,
    }

    let response = ListPartsResponse {
        bucket: result.bucket,
        key: result.key,
        upload_id: result.upload_id,
        parts: result
            .parts
            .into_iter()
            .map(|p| PartEntry {
                part_number: p.part_number,
                last_modified: p.last_modified,
                etag: p.etag,
                size: p.size,
            })
            .collect(),
        is_truncated: result.is_truncated,
        next_part_number_marker: result.next_part_number_marker,
    };

    let xml = to_string(&response).unwrap();
    let xml = inject_xmlns(&xml);

    Ok((StatusCode::OK, ([("content-type", "application/xml")], xml)).into_response())
}
