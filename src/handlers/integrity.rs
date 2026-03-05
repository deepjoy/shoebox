use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::services::{integrity_service, AppState};

// ── XML Response Types ──────────────────────────────────────────────

#[derive(serde::Serialize)]
#[serde(rename = "IntegrityCheckResult")]
struct IntegrityCheckResultXml {
    #[serde(rename = "CheckId")]
    check_id: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "FilesChecked")]
    files_checked: usize,
    #[serde(rename = "BytesChecked")]
    bytes_checked: u64,
    #[serde(rename = "FilesOk")]
    files_ok: usize,
    #[serde(rename = "Discrepancy")]
    discrepancies: Vec<DiscrepancyXml>,
}

#[derive(serde::Serialize)]
struct DiscrepancyXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "ObjectId")]
    object_id: String,
    #[serde(rename = "Reason")]
    reason: String,
    #[serde(rename = "StoredHash", skip_serializing_if = "Option::is_none")]
    stored_hash: Option<String>,
    #[serde(rename = "ComputedHash", skip_serializing_if = "Option::is_none")]
    computed_hash: Option<String>,
    #[serde(rename = "MtimeChanged")]
    mtime_changed: bool,
}

// ── Handlers ────────────────────────────────────────────────────────

/// GET /{bucket}?integrity-check — run an integrity check.
///
/// When `async=true` is set, spawns a background task and returns immediately
/// with `status: "in_progress"`. Use `?integrity-status&check_id=...` to poll.
pub async fn check_integrity(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;
    let scope = params.get("scope").map(|s| s.as_str());
    let is_async = params.get("async").map(|v| v == "true").unwrap_or(false);
    let check_id = uuid::Uuid::new_v4();

    if is_async {
        // Store an initial in_progress result
        let initial = integrity_service::IntegrityCheckResult {
            check_id: check_id.to_string(),
            status: "in_progress".to_string(),
            ..Default::default()
        };
        {
            let mut checks = state.integrity_checks.write().await;
            checks.insert(check_id.to_string(), initial.clone());
        }

        // Spawn background task
        let metadata = bucket.metadata.clone();
        let root = bucket.storage.root().to_path_buf();
        let scope_owned = scope.map(String::from);
        let token = state.shutdown_token.clone();
        let store = state.integrity_checks.clone();

        tokio::spawn(async move {
            let result = integrity_service::execute_check(
                &metadata,
                &root,
                check_id,
                scope_owned.as_deref(),
                token,
            )
            .await;

            if let Ok(result) = result {
                let mut checks = store.write().await;
                checks.insert(check_id.to_string(), result);
            }
        });

        Ok(
            XmlResponse(result_to_xml(integrity_service::IntegrityCheckResult {
                check_id: check_id.to_string(),
                status: "in_progress".to_string(),
                ..Default::default()
            }))
            .into_response(),
        )
    } else {
        // Synchronous check
        let result = integrity_service::execute_check(
            &bucket.metadata,
            bucket.storage.root(),
            check_id,
            scope,
            state.shutdown_token.clone(),
        )
        .await?;

        Ok(XmlResponse(result_to_xml(result)).into_response())
    }
}

/// GET /{bucket}?integrity-status&check_id=... — poll async integrity check status.
pub async fn get_integrity_status(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    // Verify bucket exists
    state.get_bucket(&bucket_name)?;

    let check_id = params.get("check_id").ok_or(S3Error::InvalidArgument)?;

    let checks = state.integrity_checks.read().await;
    let result = checks.get(check_id).ok_or(S3Error::NoSuchKey)?;

    Ok(XmlResponse(result_to_xml(result.clone())).into_response())
}

fn result_to_xml(result: integrity_service::IntegrityCheckResult) -> IntegrityCheckResultXml {
    IntegrityCheckResultXml {
        check_id: result.check_id,
        status: result.status,
        files_checked: result.files_checked,
        bytes_checked: result.bytes_checked,
        files_ok: result.files_ok,
        discrepancies: result
            .discrepancies
            .into_iter()
            .map(|d| DiscrepancyXml {
                key: d.key,
                object_id: d.object_id,
                reason: d.reason,
                stored_hash: d.stored_hash,
                computed_hash: d.computed_hash,
                mtime_changed: d.mtime_changed,
            })
            .collect(),
    }
}
