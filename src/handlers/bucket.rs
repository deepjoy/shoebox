use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::api::responses::XmlResponse;
use crate::error::S3Error;
use crate::services::AppState;
use crate::types::s3::*;

use super::list::list_objects_v2;

/// GET / — list all buckets.
pub async fn list_buckets(
    State(state): State<AppState>,
) -> Result<XmlResponse<ListAllMyBucketsResult>, S3Error> {
    let buckets: Vec<BucketInfo> = state
        .buckets
        .values()
        .map(|b| BucketInfo {
            name: b.name.clone(),
            // TODO(#10): persist and return actual bucket creation date
            creation_date: "1970-01-01T00:00:00.000Z".to_string(),
        })
        .collect();

    Ok(XmlResponse(ListAllMyBucketsResult {
        owner: Owner {
            id: "shoebox".to_string(),
            display_name: "Shoebox".to_string(),
        },
        buckets: Buckets { bucket: buckets },
    }))
}

/// HEAD /{bucket} — check bucket exists.
pub async fn head_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<StatusCode, S3Error> {
    state.get_bucket(&bucket)?;
    Ok(StatusCode::OK)
}

/// GET /{bucket} dispatcher — routes on query string.
///
/// SDKs hit `?location` and `?versioning` automatically; if neither
/// is present, fall through to ListObjectsV2.
pub async fn bucket_or_list(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, S3Error> {
    if params.contains_key("uploads") {
        use crate::handlers::multipart::ListMultipartUploadsQuery;
        let list_uploads_query = ListMultipartUploadsQuery {
            uploads: params.get("uploads").cloned(),
            prefix: params.get("prefix").cloned(),
            delimiter: params.get("delimiter").cloned(),
            max_uploads: params.get("max-uploads").and_then(|s| s.parse().ok()),
            key_marker: params.get("key-marker").cloned(),
            upload_id_marker: params.get("upload-id-marker").cloned(),
        };
        return crate::handlers::multipart::list_multipart_uploads(
            State(state),
            axum::extract::Path(bucket),
            axum::extract::Query(list_uploads_query),
        )
        .await;
    }
    if params.contains_key("location") {
        return get_bucket_location(State(state), Path(bucket))
            .await
            .map(IntoResponse::into_response);
    }
    if params.contains_key("versioning") {
        return get_bucket_versioning(State(state), Path(bucket))
            .await
            .map(IntoResponse::into_response);
    }
    // Default: ListObjectsV2 — re-parse with the typed query struct.
    list_objects_v2(State(state), Path(bucket), Query(params))
        .await
        .map(IntoResponse::into_response)
}

/// Stub — always returns `us-east-1`.
async fn get_bucket_location(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<XmlResponse<LocationConstraint>, S3Error> {
    state.get_bucket(&bucket)?;
    Ok(XmlResponse(LocationConstraint {
        location: "us-east-1".to_string(),
    }))
}

/// Stub — returns versioning-not-enabled (empty Status element).
async fn get_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<XmlResponse<VersioningConfiguration>, S3Error> {
    state.get_bucket(&bucket)?;
    Ok(XmlResponse(VersioningConfiguration { status: None }))
}

/// POST /{bucket} dispatcher — routes on query string.
///
/// Currently `?delete` is the only supported POST operation.
pub async fn post_bucket_dispatcher(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Result<Response, S3Error> {
    if params.contains_key("delete") {
        return delete_objects(State(state), Path(bucket), body)
            .await
            .map(IntoResponse::into_response);
    }
    Err(S3Error::MethodNotAllowed)
}

/// POST /{bucket}?delete — bulk delete objects.
async fn delete_objects(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    body: axum::body::Bytes,
) -> Result<XmlResponse<DeleteResult>, S3Error> {
    let bucket = state.get_bucket(&bucket_name)?;

    let req: DeleteRequest =
        quick_xml::de::from_str(std::str::from_utf8(&body).map_err(|_| S3Error::InvalidArgument)?)
            .map_err(|_| S3Error::InvalidArgument)?;

    let keys: Vec<String> = req.objects.iter().map(|o| o.key.clone()).collect();
    let (deleted_keys, error_pairs) =
        crate::services::bucket_service::delete_objects_bulk(bucket, &keys).await;

    let deleted: Vec<DeletedObject> = deleted_keys
        .into_iter()
        .map(|key| DeletedObject { key })
        .collect();
    let errors: Vec<DeleteError> = error_pairs
        .into_iter()
        .map(|(key, e)| DeleteError {
            key,
            code: e.code().to_string(),
            message: e.message(),
        })
        .collect();

    Ok(XmlResponse(DeleteResult {
        deleted: if req.quiet { Vec::new() } else { deleted },
        errors,
    }))
}
