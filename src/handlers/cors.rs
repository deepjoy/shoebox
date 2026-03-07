//! CORS configuration handlers: GetBucketCors, PutBucketCors, DeleteBucketCors.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::error::S3Error;
use crate::services::{cors_service, AppState};

/// GET /{bucket}?cors — return current CORS configuration.
pub async fn get_bucket_cors(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    let rules = cors_service::get_rules(&bucket.metadata).await?;

    let json = serde_json::to_string(&rules).map_err(|_| S3Error::InternalError)?;
    Ok((StatusCode::OK, [("content-type", "application/json")], json).into_response())
}

/// PUT /{bucket}?cors — set CORS configuration.
pub async fn put_bucket_cors(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let rules: Vec<crate::types::cors::CorsRule> = serde_json::from_slice(&body).map_err(|e| {
        tracing::warn!("Failed to parse CORS rules: {e}");
        S3Error::BadRequest("Invalid CORS configuration JSON".to_string())
    })?;

    cors_service::set_rules(&bucket.metadata, rules).await?;
    cors_service::invalidate_cache(&bucket.cors_cache).await;

    Ok(StatusCode::OK.into_response())
}

/// DELETE /{bucket}?cors — remove CORS configuration.
pub async fn delete_bucket_cors(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    cors_service::delete_rules(&bucket.metadata).await?;
    cors_service::invalidate_cache(&bucket.cors_cache).await;

    Ok(StatusCode::NO_CONTENT.into_response())
}
