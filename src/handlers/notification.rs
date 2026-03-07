//! Bucket notification configuration handlers: GetBucketNotification, PutBucketNotification.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::error::S3Error;
use crate::services::{notification_service, AppState};

/// GET /{bucket}?notification — return current notification configuration.
pub async fn get_bucket_notification(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    let webhooks: Vec<crate::types::notification::WebhookConfig> =
        notification_service::get_webhook_config(&bucket.metadata).await?;

    let json = serde_json::to_string(&webhooks).map_err(|_| S3Error::InternalError)?;
    Ok((StatusCode::OK, [("content-type", "application/json")], json).into_response())
}

/// PUT /{bucket}?notification — set notification configuration.
pub async fn put_bucket_notification(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let webhooks: Vec<crate::types::notification::WebhookConfig> = serde_json::from_slice(&body)
        .map_err(|e| {
            tracing::warn!("Failed to parse notification config: {e}");
            S3Error::BadRequest("Invalid notification configuration JSON".to_string())
        })?;

    notification_service::set_webhook_config(&bucket.metadata, webhooks).await?;

    Ok(StatusCode::OK.into_response())
}
