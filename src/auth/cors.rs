//! CORS preflight middleware — hand-rolled for per-bucket dynamic rules.
//!
//! Runs outermost in the middleware stack (before auth and virtual-host routing).
//! Browsers send unauthenticated OPTIONS preflight requests, so this must run
//! before SigV4 auth which would reject them.

use axum::{
    extract::State,
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::services::cors_service;
use crate::services::AppState;
use crate::types::cors::CorsHeaders;

/// CORS middleware function — extracts bucket name from path, checks CORS rules.
pub async fn cors_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    // Extract preflight request-method header before consuming request
    let requested_method = request
        .headers()
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let bucket_name = extract_bucket_from_path(&path);

    // Handle preflight requests
    if method == Method::OPTIONS {
        if let Some(ref origin) = origin {
            let rm = requested_method.as_deref().unwrap_or("GET");
            if let Some(cors) = find_cors_match(&state, bucket_name.as_deref(), origin, rm).await {
                return build_preflight_response(&cors);
            }
        }

        return StatusCode::FORBIDDEN.into_response();
    }

    // Regular request — run handler
    let mut response = next.run(request).await;

    // Add CORS headers to response
    if let Some(ref origin) = origin {
        if let Some(cors) =
            find_cors_match(&state, bucket_name.as_deref(), origin, method.as_str()).await
        {
            let headers = response.headers_mut();
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_ORIGIN,
                cors.allow_origin.parse().unwrap(),
            );
            headers.insert(header::VARY, "Origin".parse().unwrap());
            // Always expose S3 checksum headers so the browser can read them.
            let expose = merge_expose_headers(
                &cors.expose_headers,
                &[
                    "x-amz-checksum-sha256",
                    "x-amz-checksum-sha1",
                    "x-amz-checksum-crc32",
                    "x-amz-checksum-crc32c",
                ],
            );
            if !expose.is_empty() {
                headers.insert(
                    header::ACCESS_CONTROL_EXPOSE_HEADERS,
                    expose.parse().unwrap(),
                );
            }
        }
    }

    response
}

/// Find a CORS match for the given origin and method.
///
/// When `bucket_name` is Some, checks only that bucket's rules.
/// When None (e.g. ListBuckets at `/`), checks all loaded buckets and
/// returns the first match — any bucket allowing the origin is sufficient
/// for server-level operations.
async fn find_cors_match(
    state: &AppState,
    bucket_name: Option<&str>,
    origin: &str,
    method: &str,
) -> Option<CorsHeaders> {
    if let Some(name) = bucket_name {
        let bucket = state.get_bucket(name).ok()?;
        let rules = cors_service::get_rules_cached(&bucket.cors_cache, &bucket.metadata)
            .await
            .ok()?;
        return cors_service::check_origin(&rules, origin, method);
    }

    // No bucket in path — check all buckets for a matching CORS rule
    for bucket in state.buckets.values() {
        if let Ok(rules) =
            cors_service::get_rules_cached(&bucket.cors_cache, &bucket.metadata).await
        {
            if let Some(cors) = cors_service::check_origin(&rules, origin, method) {
                return Some(cors);
            }
        }
    }
    None
}

fn build_preflight_response(cors: &CorsHeaders) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        cors.allow_origin.parse().unwrap(),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        cors.allow_methods.parse().unwrap(),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        cors.allow_headers.parse().unwrap(),
    );
    headers.insert(header::VARY, "Origin".parse().unwrap());
    // Allow requests from public sites (e.g. GitHub Pages) to reach localhost.
    // See https://wicg.github.io/private-network-access/
    headers.insert(
        "Access-Control-Allow-Private-Network"
            .parse::<header::HeaderName>()
            .unwrap(),
        "true".parse().unwrap(),
    );

    if let Some(max_age) = cors.max_age {
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            max_age.to_string().parse().unwrap(),
        );
    }

    (StatusCode::OK, headers).into_response()
}

/// Merge user-configured expose headers with always-required S3 headers,
/// deduplicating case-insensitively.
fn merge_expose_headers(user_headers: &str, extra: &[&str]) -> String {
    let mut parts: Vec<String> = if user_headers.is_empty() {
        Vec::new()
    } else {
        user_headers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };
    for &h in extra {
        if !parts.iter().any(|p| p.eq_ignore_ascii_case(h)) {
            parts.push(h.to_string());
        }
    }
    parts.join(", ")
}

/// Extract bucket name from a path-style URL.
/// `/{bucket}` → Some("bucket"), `/{bucket}/{key}` → Some("bucket"), `/` → None
fn extract_bucket_from_path(path: &str) -> Option<String> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return None;
    }
    let bucket = path.split('/').next()?;
    if bucket.is_empty() || bucket.starts_with('_') {
        return None;
    }
    Some(bucket.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_from_path() {
        assert_eq!(
            extract_bucket_from_path("/photos/key.jpg"),
            Some("photos".to_string())
        );
        assert_eq!(
            extract_bucket_from_path("/photos"),
            Some("photos".to_string())
        );
        assert_eq!(extract_bucket_from_path("/"), None);
        assert_eq!(extract_bucket_from_path(""), None);
        // Admin endpoints should be skipped
        assert_eq!(extract_bucket_from_path("/_shoebox/credentials"), None);
    }
}
