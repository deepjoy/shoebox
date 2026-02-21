use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::services::tagging_service;
use crate::services::AppState;
use crate::types::tagging::{TagEntry, TagSet, Tagging};

/// GET /{bucket}/{key}?tagging — get object tags.
pub async fn get_object_tagging(
    State(state): State<AppState>,
    Path((bucket_name, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    let tags = tagging_service::get_tags(&bucket.metadata, &key).await?;

    Ok(XmlResponse(Tagging {
        tag_set: TagSet {
            tags: tags.into_iter().map(TagEntry::from).collect(),
        },
    }))
}

/// PUT /{bucket}/{key}?tagging — set object tags.
pub async fn put_object_tagging(
    State(state): State<AppState>,
    Path((bucket_name, key)): Path<(String, String)>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    // Parse XML body
    let bytes = axum::body::to_bytes(body, 64 * 1024)
        .await
        .map_err(|_| S3Error::BadRequest("Failed to read request body".to_string()))?;

    let tagging: Tagging = quick_xml::de::from_reader(bytes.as_ref())
        .map_err(|_| S3Error::BadRequest("Invalid tagging XML".to_string()))?;

    let tags = tagging
        .tag_set
        .tags
        .into_iter()
        .map(|t| crate::metadata::sqlite::Tag {
            key: t.key,
            value: t.value,
        })
        .collect();

    tagging_service::put_tags(&bucket.metadata, &key, tags).await?;

    Ok(StatusCode::OK)
}

/// DELETE /{bucket}/{key}?tagging — delete all object tags.
pub async fn delete_object_tagging(
    State(state): State<AppState>,
    Path((bucket_name, key)): Path<(String, String)>,
) -> Result<StatusCode, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    tagging_service::delete_tags(&bucket.metadata, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}
