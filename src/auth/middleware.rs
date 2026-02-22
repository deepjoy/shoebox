use std::sync::Arc;

use axum::extract::State;
use axum::http::{self, header, Request, Uri};
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

// Virtual-host middleware

pub async fn virtual_host_middleware(
    State(bucket_names): State<Arc<Vec<String>>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if let Some(bucket) = extract_bucket_from_host(request.headers(), &bucket_names) {
        let original_path = request.uri().path();
        let new_path = format!("/{}{}", bucket, original_path);
        let new_uri = rebuild_uri(request.uri(), &new_path);
        *request.uri_mut() = new_uri;
    }
    next.run(request).await
}

// Helper functions

/// Check if the credential has permission for the determined operation.
fn check_permission(
    request: &Request<axum::body::Body>,
    credential: &ResolvedCredential,
) -> Result<(), S3Error> {
    let operation = determine_operation(request);

    // Admin endpoints (/_shoebox/*) are not bucket-scoped in the URL,
    // so use the credential's own bucket for the scope check.
    if operation == "Admin" {
        let bucket = credential.bucket_name.as_deref().unwrap_or("");
        if !credential.has_permission(&operation, bucket) {
            return Err(S3Error::AccessDenied);
        }
        return Ok(());
    }

    // ListBuckets has no bucket context — any authenticated credential may list
    // bucket names, matching AWS behaviour.
    if operation == "ListBuckets" {
        return Ok(());
    }

    let bucket = extract_bucket_from_path(request.uri().path());

    if let Some(bucket_name) = bucket {
        if !credential.has_permission(&operation, &bucket_name) {
            return Err(S3Error::AccessDenied);
        }
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
        ("GET", false) if query.contains("uploads") => "ListMultipartUploads",
        ("GET", false) => "ListObjectsV2",
        ("GET", true) if query.contains("uploadId") => "ListParts",
        ("GET", true) => "GetObject",
        ("HEAD", false) => "HeadBucket",
        ("HEAD", true) => "HeadObject",
        ("PUT", true) if query.contains("partNumber") => "UploadPart",
        ("PUT", true) => "PutObject",
        ("DELETE", true) if query.contains("uploadId") => "AbortMultipartUpload",
        ("DELETE", true) => "DeleteObject",
        ("POST", false) if query.contains("delete") => "DeleteObjects",
        ("POST", true) if query.contains("uploads") => "InitiateMultipartUpload",
        ("POST", true) if query.contains("uploadId") => "CompleteMultipartUpload",
        _ => "Unknown",
    }
    .to_string()
}

/// Extract bucket name from a path-style URL (first path segment).
fn extract_bucket_from_path(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    segments.first().map(|s| s.to_string())
}

/// Extract bucket name from Host header subdomain.
fn extract_bucket_from_host(headers: &http::HeaderMap, bucket_names: &[String]) -> Option<String> {
    let host = headers.get(header::HOST)?.to_str().ok()?;
    let host_no_port = host.split(':').next().unwrap_or(host);
    let parts: Vec<&str> = host_no_port.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let subdomain = parts[0];
    bucket_names
        .iter()
        .find(|b| b.as_str() == subdomain)
        .cloned()
}

/// Rebuild URI preserving query string.
fn rebuild_uri(original: &Uri, new_path: &str) -> Uri {
    let path_and_query = match original.query() {
        Some(q) => format!("{}?{}", new_path, q),
        None => new_path.to_string(),
    };
    path_and_query.parse().unwrap_or_else(|_| original.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_from_host() {
        let bucket_names = vec!["photos".to_string(), "docs".to_string()];

        let mut headers = http::HeaderMap::new();
        headers.insert(header::HOST, "photos.localhost:9000".parse().unwrap());
        assert_eq!(
            extract_bucket_from_host(&headers, &bucket_names),
            Some("photos".to_string())
        );

        let mut headers = http::HeaderMap::new();
        headers.insert(header::HOST, "unknown.localhost:9000".parse().unwrap());
        assert_eq!(extract_bucket_from_host(&headers, &bucket_names), None);

        let mut headers = http::HeaderMap::new();
        headers.insert(header::HOST, "localhost:9000".parse().unwrap());
        assert_eq!(extract_bucket_from_host(&headers, &bucket_names), None);
    }

    #[test]
    fn test_extract_bucket_from_path() {
        assert_eq!(
            extract_bucket_from_path("/photos/sunset.jpg"),
            Some("photos".to_string())
        );
        assert_eq!(
            extract_bucket_from_path("/photos"),
            Some("photos".to_string())
        );
        assert_eq!(extract_bucket_from_path("/"), None);
    }

    #[test]
    fn test_rebuild_uri() {
        let uri: Uri = "/key?list-type=2".parse().unwrap();
        let new_uri = rebuild_uri(&uri, "/photos/key");
        assert_eq!(new_uri.path(), "/photos/key");
        assert_eq!(new_uri.query(), Some("list-type=2"));

        let uri: Uri = "/key".parse().unwrap();
        let new_uri = rebuild_uri(&uri, "/photos/key");
        assert_eq!(new_uri.path(), "/photos/key");
        assert_eq!(new_uri.query(), None);
    }

    /// Build a minimal request for testing determine_operation.
    fn make_request(method: &str, uri: &str) -> Request<axum::body::Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[test]
    fn test_determine_operation_basic() {
        assert_eq!(
            determine_operation(&make_request("GET", "/")),
            "ListBuckets"
        );
        assert_eq!(
            determine_operation(&make_request("GET", "/photos")),
            "ListObjectsV2"
        );
        assert_eq!(
            determine_operation(&make_request("GET", "/photos/key.jpg")),
            "GetObject"
        );
        assert_eq!(
            determine_operation(&make_request("HEAD", "/photos")),
            "HeadBucket"
        );
        assert_eq!(
            determine_operation(&make_request("HEAD", "/photos/key.jpg")),
            "HeadObject"
        );
        assert_eq!(
            determine_operation(&make_request("PUT", "/photos/key.jpg")),
            "PutObject"
        );
        assert_eq!(
            determine_operation(&make_request("DELETE", "/photos/key.jpg")),
            "DeleteObject"
        );
        assert_eq!(
            determine_operation(&make_request("POST", "/photos?delete")),
            "DeleteObjects"
        );
    }

    #[test]
    fn test_determine_operation_multipart() {
        // InitiateMultipartUpload: POST /{bucket}/{key}?uploads
        assert_eq!(
            determine_operation(&make_request("POST", "/photos/big.zip?uploads")),
            "InitiateMultipartUpload"
        );

        // UploadPart: PUT /{bucket}/{key}?partNumber=1&uploadId=abc
        assert_eq!(
            determine_operation(&make_request(
                "PUT",
                "/photos/big.zip?partNumber=1&uploadId=abc"
            )),
            "UploadPart"
        );

        // CompleteMultipartUpload: POST /{bucket}/{key}?uploadId=abc
        assert_eq!(
            determine_operation(&make_request("POST", "/photos/big.zip?uploadId=abc")),
            "CompleteMultipartUpload"
        );

        // AbortMultipartUpload: DELETE /{bucket}/{key}?uploadId=abc
        assert_eq!(
            determine_operation(&make_request("DELETE", "/photos/big.zip?uploadId=abc")),
            "AbortMultipartUpload"
        );

        // ListParts: GET /{bucket}/{key}?uploadId=abc
        assert_eq!(
            determine_operation(&make_request("GET", "/photos/big.zip?uploadId=abc")),
            "ListParts"
        );

        // ListMultipartUploads: GET /{bucket}?uploads
        assert_eq!(
            determine_operation(&make_request("GET", "/photos?uploads")),
            "ListMultipartUploads"
        );
    }

    #[test]
    fn test_determine_operation_admin() {
        assert_eq!(
            determine_operation(&make_request("GET", "/_shoebox/credentials")),
            "Admin"
        );
    }
}
