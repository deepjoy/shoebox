use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures::StreamExt;

use crate::api::extractors::extract_metadata_headers;
use crate::api::responses::ObjectResponse;
use crate::error::S3Error;
use crate::services::object_service::{self, PutObjectInput};
use crate::services::AppState;
use crate::storage::filesystem::FileContent;

/// HTTP-date format per RFC 7231 (e.g. "Wed, 01 Jan 2024 00:00:00 GMT").
/// Used for Last-Modified and Date HTTP headers. S3 XML responses use ISO 8601 (Rfc3339) instead.
const HTTP_DATE: &[time::format_description::FormatItem<'static>] = time::macros::format_description!(
    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
);

const EPOCH_HTTP_DATE: &str = "Thu, 01 Jan 1970 00:00:00 GMT";

fn format_http_date(dt: &time::OffsetDateTime) -> String {
    dt.format(HTTP_DATE)
        .unwrap_or_else(|_| EPOCH_HTTP_DATE.to_string())
}

/// GET /{bucket}/{key} — download an object.
pub async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket)?;
    let result = object_service::get_object(&bucket.storage, &bucket.metadata, &key).await?;

    let metadata = result.record;
    match result.content {
        FileContent::Regular(file) => {
            let stream = tokio_util::io::ReaderStream::new(file);
            let body = axum::body::Body::from_stream(stream);

            Ok(ObjectResponse {
                body,
                content_length: metadata.size.unwrap_or(0) as u64,
                content_type: metadata
                    .content_type
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
                etag: metadata.etag.unwrap_or_default(),
                last_modified: format_http_date(&metadata.last_modified),
                metadata: parse_metadata_json(&metadata.metadata),
            }
            .into_response())
        }
        FileContent::Symlink { target, len } => {
            let body = axum::body::Body::from(target.into_bytes());

            Ok(ObjectResponse {
                body,
                content_length: len,
                content_type: "application/x-symlink".to_string(),
                etag: metadata.etag.unwrap_or_default(),
                last_modified: format_http_date(&metadata.last_modified),
                metadata: parse_metadata_json(&metadata.metadata),
            }
            .into_response())
        }
    }
}

/// PUT /{bucket}/{key} — upload an object.
pub async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket)?;

    let stream = body
        .into_data_stream()
        .map(|result| result.map_err(std::io::Error::other));

    let input = PutObjectInput {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string(),
        user_metadata: extract_metadata_headers(&headers),
        content_md5: headers
            .get("content-md5")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
    };

    let result =
        object_service::put_object(&bucket.storage, &bucket.metadata, &key, stream, input).await?;

    Ok(([(header::ETAG, result.etag)], StatusCode::OK).into_response())
}

/// DELETE /{bucket}/{key} — delete an object.
pub async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket)?;
    object_service::delete_object(&bucket.storage, &bucket.metadata, &key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// HEAD /{bucket}/{key} — get object metadata without body.
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket)?;
    let metadata = object_service::head_object(&bucket.storage, &bucket.metadata, &key).await?;

    let user_meta = parse_metadata_json(&metadata.metadata);
    let mut headers = vec![
        (
            header::CONTENT_TYPE.to_string(),
            metadata
                .content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        ),
        (
            header::CONTENT_LENGTH.to_string(),
            (metadata.size.unwrap_or(0) as u64).to_string(),
        ),
        (header::ETAG.to_string(), metadata.etag.unwrap_or_default()),
        (
            header::LAST_MODIFIED.to_string(),
            format_http_date(&metadata.last_modified),
        ),
    ];

    for (key, value) in &user_meta {
        headers.push((format!("x-amz-meta-{}", key), value.clone()));
    }

    let mut builder = axum::http::Response::builder().status(StatusCode::OK);
    for (k, v) in &headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    Ok(builder.body(axum::body::Body::empty()).unwrap_or_else(|_| {
        axum::http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap()
    }))
}

fn parse_metadata_json(raw: &Option<String>) -> HashMap<String, String> {
    raw.as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}
