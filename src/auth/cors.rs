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

    // Handle preflight requests
    if method == Method::OPTIONS {
        if let Some(ref origin) = origin {
            let bucket_name = extract_bucket_from_path(&path);

            if let Some(bucket_name) = bucket_name {
                if let Ok(bucket) = state.get_bucket(&bucket_name) {
                    if let Ok(rules) =
                        cors_service::get_rules_cached(&bucket.cors_cache, &bucket.metadata).await
                    {
                        let rm = requested_method.as_deref().unwrap_or("GET");

                        if let Some(cors) = cors_service::check_origin(&rules, origin, rm) {
                            return build_preflight_response(&cors);
                        }
                    }
                }
            }
        }

        return StatusCode::FORBIDDEN.into_response();
    }

    // Extract bucket BEFORE consuming request
    let bucket_name = extract_bucket_from_path(&path);

    // Regular request — run handler
    let mut response = next.run(request).await;

    // Add CORS headers to response
    if let Some(ref origin) = origin {
        if let Some(bucket_name) = bucket_name {
            if let Ok(bucket) = state.get_bucket(&bucket_name) {
                if let Ok(rules) =
                    cors_service::get_rules_cached(&bucket.cors_cache, &bucket.metadata).await
                {
                    if let Some(cors) = cors_service::check_origin(&rules, origin, method.as_str())
                    {
                        let headers = response.headers_mut();
                        headers.insert(
                            header::ACCESS_CONTROL_ALLOW_ORIGIN,
                            cors.allow_origin.parse().unwrap(),
                        );
                        headers.insert(header::VARY, "Origin".parse().unwrap());
                        if !cors.expose_headers.is_empty() {
                            headers.insert(
                                header::ACCESS_CONTROL_EXPOSE_HEADERS,
                                cors.expose_headers.parse().unwrap(),
                            );
                        }
                    }
                }
            }
        }
    }

    response
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

    if let Some(max_age) = cors.max_age {
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            max_age.to_string().parse().unwrap(),
        );
    }

    (StatusCode::OK, headers).into_response()
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
