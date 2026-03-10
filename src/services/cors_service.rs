//! CORS configuration service — free functions, no struct.
//!
//! CORS rules are stored as JSON in the `bucket_config` table (key: `cors_rules`).
//! The CORS middleware uses `get_rules_cached()` to avoid a SQLite round-trip per request.

use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::types::cors::{CorsHeaders, CorsRule};

pub async fn get_rules(metadata: &MetadataStore) -> Result<Vec<CorsRule>, S3Error> {
    metadata.get_cors_rules().await
}

/// Read CORS rules from in-memory cache, falling back to SQLite on miss.
pub async fn get_rules_cached(
    cache: &tokio::sync::RwLock<Option<Vec<CorsRule>>>,
    metadata: &MetadataStore,
) -> Result<Vec<CorsRule>, S3Error> {
    // Fast path: read lock
    {
        let guard = cache.read().await;
        if let Some(ref rules) = *guard {
            return Ok(rules.clone());
        }
    }
    // Cold miss: load from SQLite, populate cache
    let rules = metadata.get_cors_rules().await?;
    let mut guard = cache.write().await;
    // Double-check after acquiring write lock
    if let Some(ref existing) = *guard {
        return Ok(existing.clone());
    }
    *guard = Some(rules.clone());
    Ok(rules)
}

/// Invalidate cache after PutBucketCors or DeleteBucketCors.
pub async fn invalidate_cache(cache: &tokio::sync::RwLock<Option<Vec<CorsRule>>>) {
    let mut guard = cache.write().await;
    *guard = None;
}

pub async fn set_rules(metadata: &MetadataStore, rules: Vec<CorsRule>) -> Result<(), S3Error> {
    metadata.set_cors_rules(&rules).await
}

pub async fn delete_rules(metadata: &MetadataStore) -> Result<(), S3Error> {
    metadata.delete_cors_rules().await
}

/// Check if origin matches any rule and return CORS headers.
/// Pure function — no service state needed.
pub fn check_origin(rules: &[CorsRule], origin: &str, method: &str) -> Option<CorsHeaders> {
    for rule in rules {
        if origin_matches(&rule.allowed_origins, origin)
            && rule
                .allowed_methods
                .iter()
                .any(|m| m == "*" || m.eq_ignore_ascii_case(method))
        {
            // The CORS spec says the `*` wildcard for Access-Control-Allow-Headers
            // does NOT cover `Authorization` — it must be listed explicitly.
            let allow_headers = if rule.allowed_headers.iter().any(|h| h == "*") {
                let mut headers: Vec<&str> = vec!["*", "Authorization"];
                // Include any other explicitly listed headers
                for h in &rule.allowed_headers {
                    if h != "*"
                        && !headers
                            .iter()
                            .any(|existing| existing.eq_ignore_ascii_case(h))
                    {
                        headers.push(h);
                    }
                }
                headers.join(", ")
            } else {
                rule.allowed_headers.join(", ")
            };

            return Some(CorsHeaders {
                allow_origin: origin.to_string(),
                allow_methods: rule.allowed_methods.join(", "),
                allow_headers,
                expose_headers: rule.expose_headers.join(", "),
                max_age: rule.max_age_seconds,
            });
        }
    }
    None
}

fn origin_matches(patterns: &[String], origin: &str) -> bool {
    for pattern in patterns {
        if pattern == "*" {
            return true;
        }
        if let Some(star_pos) = pattern.find('*') {
            // Wildcard pattern: e.g. "https://*.example.com" or "*.example.com"
            let prefix = &pattern[..star_pos];
            let suffix = &pattern[star_pos + 1..];
            if origin.starts_with(prefix)
                && origin.ends_with(suffix)
                && origin.len() >= prefix.len() + suffix.len()
            {
                return true;
            }
        } else if pattern == origin {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(origins: &[&str], methods: &[&str]) -> CorsRule {
        CorsRule {
            allowed_origins: origins.iter().map(|s| s.to_string()).collect(),
            allowed_methods: methods.iter().map(|s| s.to_string()).collect(),
            allowed_headers: vec!["*".to_string()],
            expose_headers: vec!["ETag".to_string()],
            max_age_seconds: Some(3600),
        }
    }

    #[test]
    fn test_exact_origin_match() {
        let rules = vec![rule(&["https://example.com"], &["GET", "PUT"])];
        assert!(check_origin(&rules, "https://example.com", "GET").is_some());
        assert!(check_origin(&rules, "https://other.com", "GET").is_none());
    }

    #[test]
    fn test_wildcard_origin() {
        let rules = vec![rule(&["*"], &["GET"])];
        assert!(check_origin(&rules, "https://anything.com", "GET").is_some());
    }

    #[test]
    fn test_subdomain_wildcard() {
        let rules = vec![rule(&["*.example.com"], &["GET"])];
        assert!(check_origin(&rules, "https://sub.example.com", "GET").is_some());
        assert!(check_origin(&rules, "https://example.com", "GET").is_none());

        // Full-URL wildcard (the realistic config format)
        let rules = vec![rule(&["https://*.example.com"], &["GET"])];
        assert!(check_origin(&rules, "https://sub.example.com", "GET").is_some());
        assert!(check_origin(&rules, "https://cdn.example.com", "GET").is_some());
        assert!(check_origin(&rules, "http://sub.example.com", "GET").is_none());
        assert!(check_origin(&rules, "https://evil.com", "GET").is_none());
    }

    #[test]
    fn test_method_mismatch() {
        let rules = vec![rule(&["*"], &["GET"])];
        assert!(check_origin(&rules, "https://example.com", "PUT").is_none());
    }

    #[test]
    fn test_wildcard_method() {
        let rules = vec![rule(&["*"], &["*"])];
        assert!(check_origin(&rules, "https://example.com", "DELETE").is_some());
    }

    #[test]
    fn test_method_case_insensitive() {
        let rules = vec![rule(&["*"], &["get"])];
        assert!(check_origin(&rules, "https://example.com", "GET").is_some());
    }

    #[test]
    fn test_wildcard_headers_includes_authorization() {
        let rules = vec![rule(&["*"], &["GET"])];
        let cors = check_origin(&rules, "https://example.com", "GET").unwrap();
        assert!(
            cors.allow_headers.contains("Authorization"),
            "Wildcard allow_headers must explicitly include Authorization: got '{}'",
            cors.allow_headers
        );
        assert!(cors.allow_headers.contains("*"));
    }
}
