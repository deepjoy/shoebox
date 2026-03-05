use axum::{
    extract::{Path, State},
    http::StatusCode,
};

use crate::error::S3Error;
use crate::services::{sync_service, AppState};

/// POST /{bucket}?sync — trigger a sync for the bucket.
///
/// Submits L1 (HIGH) + L2 (NORMAL) tasks to TaskMill and returns immediately.
/// There is no synchronous mode — the priority system ensures sync tasks
/// run before background work.
pub async fn sync_bucket(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
) -> Result<StatusCode, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    sync_service::sync(&bucket.scheduler, &bucket_name).await?;
    Ok(StatusCode::OK)
}
