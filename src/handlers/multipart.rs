use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use quick_xml::se::to_string;
use serde::Deserialize;

use crate::api::responses::inject_xmlns;
use crate::error::S3Error;
use crate::services::{bucket_service::AppState, multipart_service};

/// Query parameters for UploadPart
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPartQuery {
    pub part_number: Option<i32>,
    pub upload_id: Option<String>,
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
