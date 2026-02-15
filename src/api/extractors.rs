use std::collections::HashMap;

use axum::http::HeaderMap;

/// Extract `x-amz-meta-*` headers into a plain map.
pub fn extract_metadata_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name_str = name.as_str();
            name_str.strip_prefix("x-amz-meta-").map(|meta_key| {
                (
                    meta_key.to_string(),
                    value.to_str().unwrap_or("").to_string(),
                )
            })
        })
        .collect()
}
