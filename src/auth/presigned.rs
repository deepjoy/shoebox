use std::collections::HashMap;

use axum::http::HeaderMap;

use crate::auth::sigv4;
use crate::error::S3Error;

/// Maximum pre-signed URL expiration: 7 days (604800 seconds).
const MAX_EXPIRES: u64 = 604800;

/// Validate a pre-signed URL request.
pub fn validate_presigned(
    method: &str,
    path: &str,
    query_params: &HashMap<String, String>,
    headers: &HeaderMap,
    secret_key: &str,
) -> Result<(), S3Error> {
    let amz_algorithm = query_params
        .get("X-Amz-Algorithm")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    if amz_algorithm != "AWS4-HMAC-SHA256" {
        return Err(S3Error::AuthorizationHeaderMalformed);
    }

    let amz_credential = query_params
        .get("X-Amz-Credential")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let amz_date = query_params
        .get("X-Amz-Date")
        .ok_or(S3Error::MissingSecurityHeader)?;
    let amz_expires = query_params
        .get("X-Amz-Expires")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let amz_signed_headers = query_params
        .get("X-Amz-SignedHeaders")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let amz_signature = query_params
        .get("X-Amz-Signature")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;

    // Parse expiration
    let expires_secs: u64 = amz_expires.parse().map_err(|_| S3Error::InvalidArgument)?;
    if expires_secs > MAX_EXPIRES {
        return Err(S3Error::InvalidArgument);
    }

    // Check expiration
    if is_expired(amz_date, expires_secs) {
        return Err(S3Error::ExpiredToken);
    }

    // Parse credential scope
    let cred_parts: Vec<&str> = amz_credential.split('/').collect();
    if cred_parts.len() != 5 {
        return Err(S3Error::AuthorizationHeaderMalformed);
    }
    let date = cred_parts[1];
    let region = cred_parts[2];

    let signed_headers: Vec<String> = amz_signed_headers
        .split(';')
        .map(|s| s.to_string())
        .collect();

    // Build canonical query WITHOUT X-Amz-Signature
    let canonical_query = build_presigned_canonical_query(query_params);

    // For pre-signed URLs, the payload is always UNSIGNED-PAYLOAD
    let canonical_request = sigv4::build_canonical_request(
        method,
        path,
        &canonical_query,
        headers,
        &signed_headers,
        "UNSIGNED-PAYLOAD",
    );

    let scope = format!("{}/{}/s3/aws4_request", date, region);
    let string_to_sign = sigv4::build_string_to_sign(amz_date, &scope, &canonical_request);
    let signing_key = sigv4::derive_signing_key(secret_key, date, region, "s3");
    let expected = hex::encode(sigv4::hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    if expected != *amz_signature {
        return Err(S3Error::SignatureDoesNotMatch);
    }
    Ok(())
}

/// Extract access key from X-Amz-Credential query param.
pub fn extract_access_key_from_query(
    query_params: &HashMap<String, String>,
) -> Result<String, S3Error> {
    let credential = query_params
        .get("X-Amz-Credential")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let access_key = credential
        .split('/')
        .next()
        .ok_or(S3Error::AuthorizationHeaderMalformed)?;
    Ok(access_key.to_string())
}

/// Generate a pre-signed GET URL.
pub fn generate_presigned_get(
    endpoint: &str,
    bucket: &str,
    key: &str,
    access_key_id: &str,
    secret_key: &str,
    expires_secs: u64,
) -> String {
    generate_presigned_url(
        "GET",
        endpoint,
        bucket,
        key,
        access_key_id,
        secret_key,
        expires_secs,
        None,
    )
}

/// Generate a pre-signed PUT URL.
pub fn generate_presigned_put(
    endpoint: &str,
    bucket: &str,
    key: &str,
    access_key_id: &str,
    secret_key: &str,
    expires_secs: u64,
    content_type: Option<&str>,
) -> String {
    generate_presigned_url(
        "PUT",
        endpoint,
        bucket,
        key,
        access_key_id,
        secret_key,
        expires_secs,
        content_type,
    )
}

#[allow(clippy::too_many_arguments)]
fn generate_presigned_url(
    method: &str,
    endpoint: &str,
    bucket: &str,
    key: &str,
    access_key_id: &str,
    secret_key: &str,
    expires_secs: u64,
    _content_type: Option<&str>,
) -> String {
    let now = time::OffsetDateTime::now_utc();
    let date_format = time::format_description::parse("[year][month][day]").unwrap();
    let datetime_format =
        time::format_description::parse("[year][month][day]T[hour][minute][second]Z").unwrap();
    let date_str = now.format(&date_format).unwrap_or_default();
    let datetime_str = now.format(&datetime_format).unwrap_or_default();

    let region = "us-east-1";
    let credential = format!("{}/{}/{}/s3/aws4_request", access_key_id, date_str, region);

    let signed_headers = "host";
    let path = format!("/{}/{}", bucket, key);

    // Parse the endpoint to get the host
    let parsed_url = url::Url::parse(endpoint)
        .unwrap_or_else(|_| url::Url::parse("http://localhost:9000").unwrap());
    let host = parsed_url.host_str().unwrap_or("localhost");
    let host_with_port = match parsed_url.port() {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_string(),
    };

    // Build canonical query (sorted, without signature)
    let mut query_params = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential", credential),
        ("X-Amz-Date", datetime_str.clone()),
        ("X-Amz-Expires", expires_secs.to_string()),
        ("X-Amz-SignedHeaders", signed_headers.to_string()),
    ];
    query_params.sort_by(|a, b| a.0.cmp(b.0));

    let canonical_query: String = query_params
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    // Build headers for signing
    let mut headers = HeaderMap::new();
    headers.insert("host", host_with_port.parse().unwrap());

    let signed_headers_vec = vec!["host".to_string()];

    let canonical_request = sigv4::build_canonical_request(
        method,
        &path,
        &canonical_query,
        &headers,
        &signed_headers_vec,
        "UNSIGNED-PAYLOAD",
    );

    let scope = format!("{}/{}/s3/aws4_request", date_str, region);
    let string_to_sign = sigv4::build_string_to_sign(&datetime_str, &scope, &canonical_request);
    let signing_key = sigv4::derive_signing_key(secret_key, &date_str, region, "s3");
    let signature = hex::encode(sigv4::hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!(
        "{}{}?{}&X-Amz-Signature={}",
        endpoint.trim_end_matches('/'),
        path,
        canonical_query,
        signature
    )
}

/// Build canonical query string for pre-signed validation, excluding X-Amz-Signature.
fn build_presigned_canonical_query(params: &HashMap<String, String>) -> String {
    let mut pairs: Vec<(String, String)> = params
        .iter()
        .filter(|(k, _)| k.as_str() != "X-Amz-Signature")
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// URI-encode a string (RFC 3986 unreserved chars pass through).
fn uri_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

/// Check if a pre-signed URL has expired.
fn is_expired(amz_date: &str, expires_secs: u64) -> bool {
    // amz_date format: YYYYMMDDTHHMMSSZ
    let format = time::format_description::parse("[year][month][day]T[hour][minute][second]Z");
    let Ok(format) = format else {
        return true;
    };
    // Parse as PrimitiveDateTime since the format has no offset component,
    // then assume UTC.
    let Ok(signed_time) = time::PrimitiveDateTime::parse(amz_date, &format) else {
        return true;
    };
    let signed_time = signed_time.assume_utc();
    let now = time::OffsetDateTime::now_utc();
    let elapsed = (now - signed_time).whole_seconds();
    elapsed < 0 || elapsed as u64 > expires_secs
}

/// Parse duration strings like "1h", "30m", "7d" into seconds.
pub fn parse_duration(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".to_string());
    }

    let (num_str, suffix) = s.split_at(s.len() - 1);
    let num: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration: {}", s))?;

    match suffix {
        "s" => Ok(num),
        "m" => Ok(num * 60),
        "h" => Ok(num * 3600),
        "d" => Ok(num * 86400),
        _ => Err(format!("unknown duration suffix: {}", suffix)),
    }
}

/// Parse a query string into a HashMap.
pub fn parse_query_string(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = url_decode(parts.next()?);
            let value = url_decode(parts.next().unwrap_or(""));
            Some((key, value))
        })
        .collect()
}

/// Simple URL decode.
fn url_decode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            result.push(b' ');
            i += 1;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("1h").unwrap(), 3600);
        assert_eq!(parse_duration("30m").unwrap(), 1800);
        assert_eq!(parse_duration("7d").unwrap(), 604800);
        assert_eq!(parse_duration("60s").unwrap(), 60);
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn test_is_expired() {
        // A date far in the past should be expired
        assert!(is_expired("20200101T000000Z", 3600));
    }

    #[test]
    fn test_extract_access_key_from_query() {
        let mut params = HashMap::new();
        params.insert(
            "X-Amz-Credential".to_string(),
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
        );
        let key = extract_access_key_from_query(&params).unwrap();
        assert_eq!(key, "AKIAIOSFODNN7EXAMPLE");
    }

    #[test]
    fn test_generate_presigned_get_is_valid_url() {
        let url = generate_presigned_get(
            "http://localhost:9000",
            "photos",
            "sunset.jpg",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            3600,
        );
        assert!(url.starts_with("http://localhost:9000/photos/sunset.jpg?"));
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Credential="));
        assert!(url.contains("X-Amz-Date="));
        assert!(url.contains("X-Amz-Expires=3600"));
        assert!(url.contains("X-Amz-SignedHeaders=host"));
        assert!(url.contains("X-Amz-Signature="));
    }

    #[test]
    fn test_presigned_roundtrip() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let access_key = "AKIAIOSFODNN7EXAMPLE";

        let url = generate_presigned_get(
            "http://localhost:9000",
            "photos",
            "sunset.jpg",
            access_key,
            secret,
            3600,
        );

        // Parse the URL and validate the signature
        let parsed = url::Url::parse(&url).unwrap();
        let query_params: HashMap<String, String> = parsed
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:9000".parse().unwrap());

        let result = validate_presigned("GET", parsed.path(), &query_params, &headers, secret);
        assert!(
            result.is_ok(),
            "Presigned roundtrip failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_query_string() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=3600";
        let params = parse_query_string(query);
        assert_eq!(params.get("X-Amz-Algorithm").unwrap(), "AWS4-HMAC-SHA256");
        assert_eq!(params.get("X-Amz-Expires").unwrap(), "3600");
    }
}
