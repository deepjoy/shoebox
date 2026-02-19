use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, Request};
use axum::middleware::Next;
use axum::response::Response;

use crate::auth::provider::{CredentialProvider, ResolvedCredential};
use crate::auth::{presigned, sigv4};
use crate::error::S3Error;

/// Auth middleware: validates AWS SigV4 signatures or pre-signed URLs.
pub async fn auth_middleware(
    State(provider): State<Arc<tokio::sync::RwLock<CredentialProvider>>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, S3Error> {
    // Admin endpoints under /_shoebox/ still go through auth
    let query_string = request.uri().query().unwrap_or("");

    // 1. Pre-signed URL path
    if query_string.contains("X-Amz-Signature") {
        let query_params = presigned::parse_query_string(query_string);
        let access_key_id = presigned::extract_access_key_from_query(&query_params)?;

        let provider_guard = provider.read().await;
        let credential = provider_guard
            .lookup(&access_key_id)
            .ok_or(S3Error::InvalidAccessKeyId)?
            .clone();
        drop(provider_guard);

        presigned::validate_presigned(
            request.method().as_str(),
            request.uri().path(),
            &query_params,
            request.headers(),
            &credential.secret_access_key,
        )?;

        check_permission(&request, &credential)?;
        request.extensions_mut().insert(credential);
        return Ok(next.run(request).await);
    }

    // 2. Authorization header path
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let Some(auth_header) = auth_header else {
        return Err(S3Error::AccessDenied);
    };

    let auth_parts = sigv4::parse_auth_header(auth_header)?;

    let provider_guard = provider.read().await;
    let credential = provider_guard
        .lookup(&auth_parts.access_key_id)
        .ok_or(S3Error::InvalidAccessKeyId)?
        .clone();
    drop(provider_guard);

    let body_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    sigv4::verify_header(
        request.method().as_str(),
        request.uri().path(),
        request.uri().query().unwrap_or(""),
        request.headers(),
        body_hash,
        &credential.secret_access_key,
        &auth_parts,
    )?;

    check_permission(&request, &credential)?;
    request.extensions_mut().insert(credential);
    Ok(next.run(request).await)
}

// Helper functions for auth middleware

fn check_permission(
    request: &Request<axum::body::Body>,
    credential: &ResolvedCredential,
) -> Result<(), S3Error> {
    let operation = determine_operation(request);
    let bucket = extract_bucket_from_path(request.uri().path());

    if let Some(bucket_name) = bucket {
        if !credential.has_permission(&operation, &bucket_name) {
            return Err(S3Error::AccessDenied);
        }
    }
    // For operations without a bucket context (ListBuckets), check with empty bucket
    else if !credential.has_permission(&operation, "") {
        return Err(S3Error::AccessDenied);
    }

    Ok(())
}

/// Determine the S3 operation from the HTTP request.
fn determine_operation(request: &Request<axum::body::Body>) -> String {
    let method = request.method().as_str();
    let path = request.uri().path();
    let query = request.uri().query().unwrap_or("");

    // Check if path starts with /_shoebox/ (admin endpoints)
    if path.starts_with("/_shoebox/") {
        return "Admin".to_string();
    }

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let has_key = segments.len() >= 2;

    match (method, has_key) {
        ("GET", false) if path == "/" => "ListBuckets",
        ("GET", false) if query.contains("location") => "GetBucketLocation",
        ("GET", false) if query.contains("versioning") => "GetBucketVersioning",
        ("GET", false) => "ListObjectsV2",
        ("GET", true) => "GetObject",
        ("HEAD", false) => "HeadBucket",
        ("HEAD", true) => "HeadObject",
        ("PUT", true) => "PutObject",
        ("DELETE", true) => "DeleteObject",
        ("POST", false) if query.contains("delete") => "DeleteObjects",
        _ => "Unknown",
    }
    .to_string()
}

/// Extract bucket name from a path-style URL (first path segment).
fn extract_bucket_from_path(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    segments.first().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_from_path() {
        assert_eq!(
            extract_bucket_from_path("/photos/sunset.jpg"),
            Some("photos".to_string())
        );
        assert_eq!(extract_bucket_from_path("/"), None);
        assert_eq!(extract_bucket_from_path(""), None);
    }

    #[test]
    fn test_determine_operation() {
        use axum::body::Body;
        use axum::http::Request;

        // ListBuckets
        let req = Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();
        assert_eq!(determine_operation(&req), "ListBuckets");

        // ListObjectsV2
        let req = Request::builder()
            .method("GET")
            .uri("/photos")
            .body(Body::empty())
            .unwrap();
        assert_eq!(determine_operation(&req), "ListObjectsV2");

        // GetObject
        let req = Request::builder()
            .method("GET")
            .uri("/photos/sunset.jpg")
            .body(Body::empty())
            .unwrap();
        assert_eq!(determine_operation(&req), "GetObject");

        // PutObject
        let req = Request::builder()
            .method("PUT")
            .uri("/photos/sunset.jpg")
            .body(Body::empty())
            .unwrap();
        assert_eq!(determine_operation(&req), "PutObject");
    }
}
