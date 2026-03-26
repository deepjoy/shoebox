use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use time::format_description::well_known::Rfc3339;

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::scanner::scope::ScanScope;
use crate::scanner::tasks::{ScanL1DirTask, Scanner};
use crate::services::AppState;
use crate::types::s3::*;

/// Derive the directory prefix (as stored in `directories.prefix`) from an S3
/// request prefix.  The result always ends with `/` (or is empty for root).
///
/// Examples:
/// - `""`              → `""`         (root)
/// - `"photos/"`       → `"photos/"`
/// - `"photos/2024/"`  → `"photos/2024/"`
/// - `"photos/2024/v"` → `"photos/2024/"`
fn dir_prefix_for_listing(prefix: &str) -> String {
    match prefix.rsplit_once('/') {
        Some((parent, _)) if parent.is_empty() => String::new(),
        Some((parent, _)) => format!("{parent}/"),
        None => String::new(),
    }
}

/// GET /{bucket}?list-type=2 — ListObjectsV2.
///
/// Called directly from `bucket_or_list` with the raw query map; we
/// extract the typed fields manually to avoid double-parsing issues.
pub async fn list_objects_v2(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<XmlResponse<ListBucketResult>, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned();
    let max_keys = params
        .get("max-keys")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1000)
        .min(1000);
    let continuation_token = params.get("continuation-token").cloned();
    let start_after_param = params.get("start-after").cloned();
    // Per S3 spec, continuation-token takes precedence over start-after.
    let start_after = continuation_token.clone().or(start_after_param.clone());

    // ── On-demand scan ─────────────────────────────────────────────────────
    // If the requested directory has never been catalogued, trigger a REALTIME
    // scan and wait for it before serving the listing.  This keeps the first
    // ListObjects response consistent even before the background BFS has reached
    // that part of the tree.
    let dir_prefix = dir_prefix_for_listing(&prefix);
    if bucket.metadata.get_dir_id(&dir_prefix).await?.is_none() {
        let scan_start_ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
        let scanner = bucket.scheduler.domain::<Scanner>();

        // Create the event stream BEFORE submitting so we can't miss the
        // completion event if the task runs very quickly.
        let mut stream = scanner.task_events::<ScanL1DirTask>();

        let outcome = scanner
            .submit_with(ScanL1DirTask {
                bucket: bucket_name.clone(),
                dir_prefix: dir_prefix.clone(),
                scan_start_ns,
                scope: ScanScope::Subtree {
                    prefix: dir_prefix.clone(),
                },
            })
            .priority(taskmill::Priority::REALTIME)
            .await
            .map_err(|e| {
                tracing::warn!(dir = %dir_prefix, "on-demand scan submit failed: {e}");
                S3Error::InternalError
            })?;

        // Wait for completion for Inserted / Upgraded / Requeued / Superseded.
        // For Duplicate / Rejected the directory was already scanned — proceed.
        if let Some(task_id) = outcome.id() {
            loop {
                match stream.recv().await {
                    Ok(taskmill::TaskEvent::Completed { id, .. }) if id == task_id => break,
                    Ok(taskmill::TaskEvent::Failed {
                        id,
                        will_retry,
                        error,
                        ..
                    }) if id == task_id => {
                        if !will_retry {
                            tracing::warn!(
                                dir = %dir_prefix,
                                "on-demand L1 scan failed: {error}"
                            );
                            break;
                        }
                    }
                    Ok(taskmill::TaskEvent::DeadLettered { id, error, .. }) if id == task_id => {
                        tracing::warn!(dir = %dir_prefix, "on-demand L1 scan dead-lettered: {error}");
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break, // broadcast channel closed or lagged
                }
            }
        }
    }
    // ── End on-demand scan ─────────────────────────────────────────────────

    let (objects, common_prefixes, is_truncated, next_token) = bucket
        .metadata
        .list_objects_v2(
            &prefix,
            delimiter.as_deref(),
            max_keys,
            start_after.as_deref(),
        )
        .await?;

    let contents: Vec<ObjectInfo> = objects
        .into_iter()
        .map(|o| ObjectInfo {
            key: o.key,
            last_modified: o.last_modified.format(&Rfc3339).unwrap(),
            etag: o.etag.unwrap_or_default(),
            size: o.size.unwrap_or(0).max(0) as u64,
            storage_class: "STANDARD".to_string(),
        })
        .collect();
    let cp_list: Vec<CommonPrefix> = common_prefixes
        .into_iter()
        .map(|p| CommonPrefix { prefix: p })
        .collect();
    let key_count = (contents.len() + cp_list.len()) as u32;

    Ok(XmlResponse(ListBucketResult {
        name: bucket_name,
        prefix: Some(prefix),
        key_count,
        max_keys,
        delimiter,
        is_truncated,
        start_after: start_after_param,
        continuation_token,
        contents,
        common_prefixes: cp_list,
        next_continuation_token: next_token,
    }))
}
