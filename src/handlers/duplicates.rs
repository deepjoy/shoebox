use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::services::{duplicates_service, merge_service, AppState};

// ── XML Response Types ──────────────────────────────────────────────

#[derive(serde::Serialize)]
#[serde(rename = "DuplicateReport")]
struct DuplicateReportXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "ScanComplete")]
    scan_complete: bool,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "DuplicateGroup")]
    groups: Vec<DuplicateGroupXml>,
}

#[derive(serde::Serialize)]
struct DuplicateGroupXml {
    #[serde(rename = "ContentHash")]
    content_hash: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "File")]
    files: Vec<DuplicateFileXml>,
}

#[derive(serde::Serialize)]
struct DuplicateFileXml {
    #[serde(rename = "ObjectId")]
    object_id: String,
    #[serde(rename = "Key")]
    key: String,
}

#[derive(serde::Serialize)]
#[serde(rename = "CrossBucketDuplicateReport")]
struct CrossBucketDuplicateReportXml {
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "DuplicateGroup")]
    groups: Vec<CrossBucketDuplicateGroupXml>,
}

#[derive(serde::Serialize)]
struct CrossBucketDuplicateGroupXml {
    #[serde(rename = "ContentHash")]
    content_hash: String,
    #[serde(rename = "File")]
    files: Vec<CrossBucketFileXml>,
}

#[derive(serde::Serialize)]
struct CrossBucketFileXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "ObjectId")]
    object_id: String,
    #[serde(rename = "Key")]
    key: String,
}

#[derive(serde::Serialize)]
#[serde(rename = "DuplicateDirReport")]
struct DuplicateDirReportXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "DuplicateDirGroup")]
    groups: Vec<DuplicateDirGroupXml>,
}

#[derive(serde::Serialize)]
struct DuplicateDirGroupXml {
    #[serde(rename = "DirHash")]
    dir_hash: String,
    #[serde(rename = "Directory")]
    dirs: Vec<DuplicateDirXml>,
}

#[derive(serde::Serialize)]
struct DuplicateDirXml {
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "FileCount")]
    file_count: i32,
    #[serde(rename = "TotalSize")]
    total_size: i64,
}

#[derive(serde::Serialize)]
#[serde(rename = "DirComparison")]
struct DirComparisonXml {
    #[serde(rename = "Left")]
    left: DirRefXml,
    #[serde(rename = "Right")]
    right: DirRefXml,
    #[serde(rename = "Identical")]
    identical: bool,
    #[serde(rename = "Summary")]
    summary: ComparisonSummaryXml,
    #[serde(rename = "Difference")]
    differences: Vec<FileDifferenceXml>,
}

#[derive(serde::Serialize)]
struct DirRefXml {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Path")]
    path: String,
}

#[derive(serde::Serialize)]
struct ComparisonSummaryXml {
    #[serde(rename = "FilesIdentical")]
    files_identical: usize,
    #[serde(rename = "FilesOnlyInLeft")]
    files_only_in_left: usize,
    #[serde(rename = "FilesOnlyInRight")]
    files_only_in_right: usize,
    #[serde(rename = "FilesDifferent")]
    files_different: usize,
}

#[derive(serde::Serialize)]
struct FileDifferenceXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "LeftHash", skip_serializing_if = "Option::is_none")]
    left_hash: Option<String>,
    #[serde(rename = "RightHash", skip_serializing_if = "Option::is_none")]
    right_hash: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename = "MergeResult")]
struct MergeResultXml {
    #[serde(rename = "WinnerObjectId")]
    winner_object_id: String,
    #[serde(rename = "LosersMerged")]
    losers_merged: usize,
}

// ── Merge Request Body ──────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct MergeRequest {
    winner_key: String,
    loser_keys: Vec<String>,
}

// ── Handlers ────────────────────────────────────────────────────────

/// GET /{bucket}?duplicates — find duplicate files within a bucket.
pub async fn find_bucket_duplicates(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let max_results = params
        .get("max-results")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let allow_partial = params
        .get("allow-partial")
        .map(|v| v == "true")
        .unwrap_or(false);

    let report = duplicates_service::find_bucket_duplicates(
        &bucket.metadata,
        &bucket_name,
        max_results,
        allow_partial,
    )
    .await?;

    let xml = DuplicateReportXml {
        bucket: report.bucket,
        scan_complete: report.scan_complete,
        is_truncated: report.is_truncated,
        groups: report
            .duplicates
            .into_iter()
            .map(|g| DuplicateGroupXml {
                content_hash: g.checksum_sha256,
                size: g.size,
                files: g
                    .files
                    .into_iter()
                    .map(|f| DuplicateFileXml {
                        object_id: f.object_id,
                        key: f.key,
                    })
                    .collect(),
            })
            .collect(),
    };

    Ok(XmlResponse(xml).into_response())
}

/// GET /?duplicates — find duplicate files across all buckets.
pub async fn find_cross_bucket_duplicates(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    let max_results = params
        .get("max-results")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let bucket_pairs: Vec<(&str, &crate::metadata::MetadataStore)> = state
        .buckets
        .iter()
        .map(|(name, b)| (name.as_str(), &b.metadata))
        .collect();

    let report =
        duplicates_service::find_cross_bucket_duplicates(&bucket_pairs, max_results).await?;

    let xml = CrossBucketDuplicateReportXml {
        is_truncated: report.is_truncated,
        groups: report
            .duplicates
            .into_iter()
            .map(|g| CrossBucketDuplicateGroupXml {
                content_hash: g.checksum_sha256,
                files: g
                    .files
                    .into_iter()
                    .map(|f| CrossBucketFileXml {
                        bucket: f.bucket,
                        object_id: f.object_id,
                        key: f.key,
                    })
                    .collect(),
            })
            .collect(),
    };

    Ok(XmlResponse(xml).into_response())
}

/// GET /{bucket}?duplicate-dirs — find duplicate directories within a bucket.
pub async fn find_bucket_duplicate_dirs(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let min_files = params
        .get("min-files")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let max_results = params
        .get("max-results")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let report = duplicates_service::find_bucket_duplicate_dirs(
        &bucket.metadata,
        &bucket_name,
        min_files,
        max_results,
    )
    .await?;

    let xml = DuplicateDirReportXml {
        bucket: report.bucket,
        groups: report
            .duplicate_dirs
            .into_iter()
            .map(|g| DuplicateDirGroupXml {
                dir_hash: g.dir_hash,
                dirs: g
                    .dirs
                    .into_iter()
                    .map(|d| DuplicateDirXml {
                        prefix: d.prefix,
                        file_count: d.file_count,
                        total_size: d.total_size,
                    })
                    .collect(),
            })
            .collect(),
    };

    Ok(XmlResponse(xml).into_response())
}

/// GET /?compare-dirs — compare two directories across buckets.
pub async fn compare_dirs(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    let left = params.get("left").ok_or(S3Error::InvalidArgument)?;
    let right = params.get("right").ok_or(S3Error::InvalidArgument)?;

    let (left_bucket, left_path) = parse_bucket_path(left)?;
    let (right_bucket, right_path) = parse_bucket_path(right)?;

    let left_state = state.get_bucket(left_bucket)?;
    let right_state = state.get_bucket(right_bucket)?;

    let comparison = duplicates_service::compare_dirs(
        &left_state.metadata,
        left_bucket,
        left_path,
        &right_state.metadata,
        right_bucket,
        right_path,
    )
    .await?;

    let xml = DirComparisonXml {
        left: DirRefXml {
            bucket: comparison.left.bucket,
            path: comparison.left.path,
        },
        right: DirRefXml {
            bucket: comparison.right.bucket,
            path: comparison.right.path,
        },
        identical: comparison.identical,
        summary: ComparisonSummaryXml {
            files_identical: comparison.summary.files_identical,
            files_only_in_left: comparison.summary.files_only_in_left,
            files_only_in_right: comparison.summary.files_only_in_right,
            files_different: comparison.summary.files_different,
        },
        differences: comparison
            .differences
            .into_iter()
            .map(|d| FileDifferenceXml {
                key: d.key,
                status: d.status,
                left_hash: d.left_hash,
                right_hash: d.right_hash,
            })
            .collect(),
    };

    Ok(XmlResponse(xml).into_response())
}

/// POST /{bucket}?merge — merge duplicate objects.
pub async fn merge_duplicates(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let req: MergeRequest = serde_json::from_slice(&body)
        .map_err(|e| S3Error::BadRequest(format!("Invalid JSON body: {}", e)))?;

    // Resolve keys to object_ids
    let winner = bucket
        .metadata
        .get_object(&req.winner_key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;
    let mut loser_ids = Vec::new();
    for key in &req.loser_keys {
        let obj = bucket
            .metadata
            .get_object(key)
            .await?
            .ok_or(S3Error::NoSuchKey)?;
        loser_ids.push(obj.id);
    }

    let loser_refs: Vec<&str> = loser_ids.iter().map(|s| s.as_str()).collect();

    let result = merge_service::merge_duplicates(&bucket.metadata, &winner.id, &loser_refs).await?;

    // Delete loser objects from DB and disk
    for key in &req.loser_keys {
        crate::services::object_service::delete_object(&bucket.storage, &bucket.metadata, key)
            .await?;
    }

    let xml = MergeResultXml {
        winner_object_id: result.winner_object_id,
        losers_merged: result.losers_merged,
    };

    Ok(XmlResponse(xml).into_response())
}

/// Parse "bucket/path" into (bucket, path).
fn parse_bucket_path(s: &str) -> Result<(&str, &str), S3Error> {
    s.split_once('/').ok_or(S3Error::InvalidArgument)
}
