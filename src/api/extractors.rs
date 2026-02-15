use std::collections::HashMap;

use axum::http::HeaderMap;

/// Extract `x-amz-meta-*` headers into a plain map.
pub fn extract_metadata_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            name.as_str()
                .strip_prefix("x-amz-meta-")
                .map(|k| (k.to_string(), value.to_str().unwrap_or("").to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metadata_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-author", "alice".parse().unwrap());
        headers.insert("x-amz-meta-tag", "photo".parse().unwrap());
        headers.insert("content-type", "image/jpeg".parse().unwrap());

        let meta = extract_metadata_headers(&headers);
        assert_eq!(meta.len(), 2);
        assert_eq!(meta.get("author").unwrap(), "alice");
        assert_eq!(meta.get("tag").unwrap(), "photo");
    }

    #[test]
    fn test_extract_metadata_headers_empty() {
        let headers = HeaderMap::new();
        let meta = extract_metadata_headers(&headers);
        assert!(meta.is_empty());
    }
}
