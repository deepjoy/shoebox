use std::collections::HashMap;

use axum::{
    extract::{Path, RawQuery, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::api::extractors::extract_metadata_headers;
use crate::api::responses::ObjectResponse;
use crate::error::S3Error;
use crate::handlers::tagging;
use crate::metadata::sqlite::ObjectRecord;
use crate::services::copy_service::{self, CopyConditions};
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

fn parse_http_date(s: &str) -> Result<time::OffsetDateTime, S3Error> {
    // Parse as PrimitiveDateTime because the format uses literal "GMT" rather
    // than an offset specifier, then assume UTC.
    time::PrimitiveDateTime::parse(s, HTTP_DATE)
        .map(|dt| dt.assume_utc())
        .map_err(|_| S3Error::InvalidArgument)
}

/// Check conditional headers against an object record.
/// Returns `Some(Response)` for 304 Not Modified, or `Err` for precondition failures.
fn check_conditional(
    record: &ObjectRecord,
    headers: &HeaderMap,
) -> Result<Option<Response>, S3Error> {
    // If-Match: Return only if ETag matches
    if let Some(if_match) = headers.get(header::IF_MATCH) {
        let etag = if_match.to_str().map_err(|_| S3Error::InvalidArgument)?;
        if record.etag.as_deref() != Some(etag) {
            return Err(S3Error::PreconditionFailed);
        }
    }

    // If-None-Match: Return only if ETag differs (for caching)
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
        let etag = if_none_match
            .to_str()
            .map_err(|_| S3Error::InvalidArgument)?;
        if record.etag.as_deref() == Some(etag) {
            return Ok(Some(
                (
                    StatusCode::NOT_MODIFIED,
                    [(header::ETAG, record.etag.clone().unwrap_or_default())],
                )
                    .into_response(),
            ));
        }
    }

    // If-Modified-Since
    if let Some(ims) = headers.get(header::IF_MODIFIED_SINCE) {
        let since = parse_http_date(ims.to_str().map_err(|_| S3Error::InvalidArgument)?)?;
        if record.last_modified <= since {
            return Ok(Some(StatusCode::NOT_MODIFIED.into_response()));
        }
    }

    // If-Unmodified-Since
    if let Some(ius) = headers.get(header::IF_UNMODIFIED_SINCE) {
        let since = parse_http_date(ius.to_str().map_err(|_| S3Error::InvalidArgument)?)?;
        if record.last_modified > since {
            return Err(S3Error::PreconditionFailed);
        }
    }

    Ok(None) // No condition triggered, proceed normally
}

/// GET /{bucket}/{key} — download an object with range and conditional support.
/// Also dispatches to GetObjectTagging if `?tagging` is present.
pub async fn get_object(
    state: State<AppState>,
    path: Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    // Dispatch to tagging handler if ?tagging is present
    if has_query_param(query.as_deref(), "tagging") {
        return tagging::get_object_tagging(state, path)
            .await
            .map(IntoResponse::into_response);
    }

    let State(state) = state;
    let Path((bucket_name, key)) = path;
    let bucket = state.get_bucket(&bucket_name)?;
    let record = bucket
        .metadata
        .get_object(&key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    // Check conditional headers
    if let Some(response) = check_conditional(&record, &headers)? {
        return Ok(response);
    }

    let file_size = record.size.unwrap_or(0) as u64;
    let content_type = record
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let etag = record.etag.clone().unwrap_or_default();
    let last_modified = format_http_date(&record.last_modified);
    let user_meta = parse_metadata_json(&record.metadata);

    // Check for Range header
    if let Some(range_header) = headers.get(header::RANGE) {
        let range_str = range_header.to_str().map_err(|_| S3Error::InvalidRange)?;

        let (start, end) = parse_range(range_str, file_size)?;

        // Validate range
        if start >= file_size {
            return Err(S3Error::RangeNotSatisfiable);
        }

        let end = end.min(file_size - 1);
        let length = end - start + 1;

        // Open file handle for seeking
        let mut file = bucket.storage.get_file_handle(&key).await?;
        file.seek(SeekFrom::Start(start)).await?;
        let stream = tokio_util::io::ReaderStream::new(file.take(length));
        let body = axum::body::Body::from_stream(stream);

        let mut builder = axum::http::Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_TYPE, &content_type)
            .header(header::CONTENT_LENGTH, length.to_string())
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, file_size),
            )
            .header(header::ETAG, &etag)
            .header(header::LAST_MODIFIED, &last_modified);

        for (k, v) in &user_meta {
            builder = builder.header(format!("x-amz-meta-{}", k), v);
        }

        return Ok(builder
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()));
    }

    // Full content response
    let content = bucket.storage.get(&key).await?;
    match content {
        FileContent::Regular(file) => {
            let stream = tokio_util::io::ReaderStream::new(file);
            let body = axum::body::Body::from_stream(stream);

            Ok(ObjectResponse {
                body,
                content_length: file_size,
                content_type,
                etag,
                last_modified,
                metadata: user_meta,
            }
            .into_response())
        }
        FileContent::Symlink { target, len } => {
            let body = axum::body::Body::from(target.into_bytes());

            Ok(ObjectResponse {
                body,
                content_length: len,
                content_type: "application/x-symlink".to_string(),
                etag,
                last_modified,
                metadata: user_meta,
            }
            .into_response())
        }
    }
}

/// PUT /{bucket}/{key} — upload, copy, rename, or set tags.
/// Dispatches to PutObjectTagging if `?tagging` is present.
pub async fn put_object(
    state: State<AppState>,
    path: Path<(String, String)>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<Response, S3Error> {
    // Dispatch to tagging handler if ?tagging is present
    if has_query_param(query.as_deref(), "tagging") {
        return tagging::put_object_tagging(state, path, body)
            .await
            .map(IntoResponse::into_response);
    }

    let State(state) = state;
    let Path((bucket_name, key)) = path;

    // Check for copy-source header
    if let Some(copy_source) = headers.get("x-amz-copy-source") {
        let is_rename = headers
            .get("x-shoebox-rename")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "true")
            .unwrap_or(false);

        let copy_source_str = copy_source.to_str().map_err(|_| S3Error::InvalidArgument)?;
        let (src_bucket_name, src_key) = parse_copy_source(copy_source_str)?;

        if is_rename {
            // Atomic rename (same bucket only)
            if src_bucket_name != bucket_name {
                return Err(S3Error::InvalidArgument);
            }

            let bucket = state.get_bucket(&bucket_name)?;
            let overwrite = headers
                .get("x-shoebox-overwrite")
                .and_then(|v| v.to_str().ok())
                .map(|v| v == "true")
                .unwrap_or(false);

            copy_service::rename_object(
                &bucket.storage,
                &bucket.metadata,
                &src_key,
                &key,
                overwrite,
            )
            .await?;

            return Ok(StatusCode::OK.into_response());
        }

        // Regular copy — resolve both buckets
        let src_bucket = state.get_bucket(&src_bucket_name)?;
        let dst_bucket = state.get_bucket(&bucket_name)?;

        let conditions = extract_copy_conditions(&headers);

        let result = copy_service::copy_object(
            &src_bucket.storage,
            &src_bucket.metadata,
            &src_key,
            &dst_bucket.storage,
            &dst_bucket.metadata,
            &key,
            &conditions,
        )
        .await?;

        // Return CopyObjectResult XML
        let last_modified = result
            .last_modified
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <ETag>{}</ETag>
  <LastModified>{}</LastModified>
</CopyObjectResult>"#,
            result.etag, last_modified
        );

        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            xml,
        )
            .into_response());
    }

    // Regular PUT
    let bucket = state.get_bucket(&bucket_name)?;

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

/// DELETE /{bucket}/{key} — delete an object or its tags.
/// Dispatches to DeleteObjectTagging if `?tagging` is present.
pub async fn delete_object(
    state: State<AppState>,
    path: Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Result<Response, S3Error> {
    // Dispatch to tagging handler if ?tagging is present
    if has_query_param(query.as_deref(), "tagging") {
        return tagging::delete_object_tagging(state, path)
            .await
            .map(IntoResponse::into_response);
    }

    let State(state) = state;
    let Path((bucket, key)) = path;
    let bucket = state.get_bucket(&bucket)?;
    object_service::delete_object(&bucket.storage, &bucket.metadata, &key).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// HEAD /{bucket}/{key} — get object metadata without body.
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket)?;
    let metadata = object_service::head_object(&bucket.storage, &bucket.metadata, &key).await?;

    // Check conditional headers
    if let Some(response) = check_conditional(&metadata, &headers)? {
        return Ok(response);
    }

    let user_meta = parse_metadata_json(&metadata.metadata);
    let mut resp_headers = vec![
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
        resp_headers.push((format!("x-amz-meta-{}", key), value.clone()));
    }

    let mut builder = axum::http::Response::builder().status(StatusCode::OK);
    for (k, v) in &resp_headers {
        builder = builder.header(k.as_str(), v.as_str());
    }
    Ok(builder.body(axum::body::Body::empty()).unwrap_or_else(|_| {
        axum::http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap()
    }))
}

/// Parse the `x-amz-copy-source` header value into (bucket, key).
/// Format: `/bucket/key` or `bucket/key`
fn parse_copy_source(source: &str) -> Result<(String, String), S3Error> {
    let source = source.strip_prefix('/').unwrap_or(source);

    // URL-decode the source path
    let decoded = url::form_urlencoded::parse(source.as_bytes())
        .map(|(k, v)| {
            if v.is_empty() {
                k.to_string()
            } else {
                format!("{}={}", k, v)
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    let source = if decoded.is_empty() {
        source.to_string()
    } else {
        decoded
    };

    let (bucket, key) = source.split_once('/').ok_or(S3Error::InvalidArgument)?;

    if bucket.is_empty() || key.is_empty() {
        return Err(S3Error::InvalidArgument);
    }

    Ok((bucket.to_string(), key.to_string()))
}

/// Extract copy conditions from x-amz-copy-source-* headers.
fn extract_copy_conditions(headers: &HeaderMap) -> CopyConditions {
    CopyConditions {
        if_match: headers
            .get("x-amz-copy-source-if-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        if_none_match: headers
            .get("x-amz-copy-source-if-none-match")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        if_modified_since: headers
            .get("x-amz-copy-source-if-modified-since")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_http_date(s).ok()),
        if_unmodified_since: headers
            .get("x-amz-copy-source-if-unmodified-since")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_http_date(s).ok()),
    }
}

fn parse_range(range: &str, file_size: u64) -> Result<(u64, u64), S3Error> {
    let range = range.strip_prefix("bytes=").ok_or(S3Error::InvalidRange)?;

    if let Some(stripped) = range.strip_prefix('-') {
        // Suffix range: last N bytes
        let suffix: u64 = stripped.parse().map_err(|_| S3Error::InvalidRange)?;
        let start = file_size.saturating_sub(suffix);
        Ok((start, file_size - 1))
    } else if let Some(stripped) = range.strip_suffix('-') {
        // From offset to end
        let start: u64 = stripped.parse().map_err(|_| S3Error::InvalidRange)?;
        Ok((start, file_size - 1))
    } else {
        // Explicit range
        let (start_str, end_str) = range.split_once('-').ok_or(S3Error::InvalidRange)?;
        let start: u64 = start_str.parse().map_err(|_| S3Error::InvalidRange)?;
        let end: u64 = end_str.parse().map_err(|_| S3Error::InvalidRange)?;
        Ok((start, end))
    }
}

fn parse_metadata_json(raw: &Option<String>) -> HashMap<String, String> {
    raw.as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

/// POST /{bucket}/{key} — initiate or complete multipart upload.
/// Dispatches to InitiateMultipartUpload if `?uploads` is present,
/// and CompleteMultipartUpload if `?uploadId` is present.
pub async fn post_object(
    _state: State<AppState>,
    _path: Path<(String, String)>,
    RawQuery(_query): RawQuery,
    _headers: HeaderMap,
    _body: axum::body::Body,
) -> Result<Response, S3Error> {
    Err(S3Error::MethodNotAllowed)
}

/// Check if a query string contains a specific parameter (e.g. "tagging").
fn has_query_param(query: Option<&str>, param: &str) -> bool {
    match query {
        Some(q) => {
            q == param
                || q.starts_with(&format!("{}&", param))
                || q.contains(&format!("&{}", param))
                || q.starts_with(&format!("{}=", param))
        }
        None => false,
    }
}

/// Parse query string into a HashMap.
#[allow(dead_code)] // Used in upcoming multipart dispatch commits
fn parse_query_params(query: Option<&str>) -> HashMap<String, String> {
    query
        .map(|q| {
            q.split('&')
                .filter_map(|part| {
                    let mut split = part.splitn(2, '=');
                    let key = split.next()?.to_string();
                    let value = split.next().unwrap_or("").to_string();
                    Some((key, value))
                })
                .collect()
        })
        .unwrap_or_default()
}
