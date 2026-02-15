use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use time::format_description::well_known::Rfc3339;

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::services::AppState;
use crate::types::s3::*;

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
            size: o.size.unwrap_or(0) as u64,
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
