use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::S3Error;

type HmacSha256 = Hmac<Sha256>;

/// Parsed components from the Authorization header.
#[derive(Debug)]
pub struct AuthParts {
    pub access_key_id: String,
    pub date: String,
    pub region: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

/// Parse an AWS SigV4 Authorization header.
///
/// Format: `AWS4-HMAC-SHA256 Credential=AKID/20240115/us-east-1/s3/aws4_request,
///          SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc123`
pub fn parse_auth_header(header: &str) -> Result<AuthParts, S3Error> {
    let header = header
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or(S3Error::AuthorizationHeaderMalformed)?
        .trim();

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in header.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val.trim());
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val.trim());
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val.trim());
        }
    }

    let credential = credential.ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let signed_headers_str = signed_headers.ok_or(S3Error::AuthorizationHeaderMalformed)?;
    let signature = signature.ok_or(S3Error::AuthorizationHeaderMalformed)?;

    // Parse credential: AKID/YYYYMMDD/region/s3/aws4_request
    let cred_parts: Vec<&str> = credential.split('/').collect();
    if cred_parts.len() != 5 || cred_parts[3] != "s3" || cred_parts[4] != "aws4_request" {
        return Err(S3Error::AuthorizationHeaderMalformed);
    }

    let signed_headers_vec: Vec<String> = signed_headers_str
        .split(';')
        .map(|s| s.to_string())
        .collect();

    Ok(AuthParts {
        access_key_id: cred_parts[0].to_string(),
        date: cred_parts[1].to_string(),
        region: cred_parts[2].to_string(),
        signed_headers: signed_headers_vec,
        signature: signature.to_string(),
    })
}

/// Verify an AWS Signature V4 Authorization header.
pub fn verify_header(
    method: &str,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    body_hash: &str,
    secret_key: &str,
    auth_parts: &AuthParts,
) -> Result<(), S3Error> {
    let canonical_request = build_canonical_request(
        method,
        path,
        query,
        headers,
        &auth_parts.signed_headers,
        body_hash,
    );

    let scope = format!("{}/{}/s3/aws4_request", auth_parts.date, auth_parts.region);
    let datetime = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or(S3Error::MissingSecurityHeader)?;

    let string_to_sign = build_string_to_sign(datetime, &scope, &canonical_request);
    let signing_key = derive_signing_key(secret_key, &auth_parts.date, &auth_parts.region, "s3");
    let expected = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    if expected != auth_parts.signature {
        return Err(S3Error::SignatureDoesNotMatch);
    }
    Ok(())
}

/// Build a canonical request string.
///
/// Format: METHOD\nURI\nQUERY\nHEADERS\nSIGNED_HEADERS\nBODY_HASH
pub fn build_canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    signed_headers: &[String],
    body_hash: &str,
) -> String {
    let canonical_uri = uri_encode_path(path);
    let canonical_query = canonicalize_query_string(query);

    let mut canonical_headers = String::new();
    for header_name in signed_headers {
        let value = headers
            .get(header_name.as_str())
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", header_name, trim_header_value(value)));
    }

    let signed_headers_str = signed_headers.join(";");

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers_str, body_hash,
    )
}

/// Build the string to sign.
///
/// Format: AWS4-HMAC-SHA256\nDATETIME\nSCOPE\nSHA256(canonical_request)
pub fn build_string_to_sign(datetime: &str, scope: &str, canonical_request: &str) -> String {
    let hash = sha256_hex(canonical_request.as_bytes());
    format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", datetime, scope, hash)
}

/// HMAC-SHA256 key derivation chain:
///   AWS4+secret -> date -> region -> service -> aws4_request
pub fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{}", secret).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Compute HMAC-SHA256.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute SHA256 hex digest.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Decode percent-encoded bytes (e.g. `%2F` → `/`).
fn percent_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                result.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_else(|_| s.to_string())
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// URI-encode a path, preserving `/` separators.
/// Decodes first to avoid double-encoding percent-encoded characters.
fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(|c| uri_encode_component(&percent_decode(c)))
        .collect::<Vec<_>>()
        .join("/")
}

/// URI-encode a single path component (RFC 3986 unreserved chars pass through).
fn uri_encode_component(s: &str) -> String {
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

/// Canonicalize a query string: sort by key, then value.
/// Decodes percent-encoded values first to avoid double-encoding
/// (e.g. `delimiter=%2F` must not become `delimiter=%252F`).
fn canonicalize_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            (
                uri_encode_component(&percent_decode(key)),
                uri_encode_component(&percent_decode(value)),
            )
        })
        .collect();

    params.sort();
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Trim and collapse whitespace in a header value.
fn trim_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_auth_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";
        let parts = parse_auth_header(header).unwrap();
        assert_eq!(parts.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parts.date, "20130524");
        assert_eq!(parts.region, "us-east-1");
        assert_eq!(
            parts.signed_headers,
            vec!["host", "range", "x-amz-content-sha256", "x-amz-date"]
        );
        assert_eq!(
            parts.signature,
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024"
        );
    }

    #[test]
    fn test_parse_auth_header_invalid_prefix() {
        assert!(parse_auth_header("Bearer token123").is_err());
    }

    #[test]
    fn test_derive_signing_key() {
        // AWS test vector from documentation
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_sha256_hex() {
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_canonicalize_query_string() {
        let query = "prefix=test&delimiter=/&list-type=2";
        let canonical = canonicalize_query_string(query);
        assert_eq!(canonical, "delimiter=%2F&list-type=2&prefix=test");
    }

    #[test]
    fn test_canonicalize_query_string_already_encoded() {
        // AWS CLI sends delimiter=%2F (already percent-encoded).
        // Must not double-encode to %252F.
        let query = "list-type=2&prefix=&delimiter=%2F&encoding-type=url";
        let canonical = canonicalize_query_string(query);
        assert_eq!(
            canonical,
            "delimiter=%2F&encoding-type=url&list-type=2&prefix="
        );
    }

    #[test]
    fn test_canonicalize_query_string_empty() {
        assert_eq!(canonicalize_query_string(""), "");
    }

    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("%2F"), "/");
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("no-encoding"), "no-encoding");
        assert_eq!(percent_decode("%"), "%");
        assert_eq!(percent_decode("%2"), "%2");
        assert_eq!(percent_decode("%ZZ"), "%ZZ");
    }

    #[test]
    fn test_uri_encode_path() {
        assert_eq!(uri_encode_path("/bucket/key"), "/bucket/key");
        assert_eq!(
            uri_encode_path("/bucket/my file.txt"),
            "/bucket/my%20file.txt"
        );
    }

    #[test]
    fn test_trim_header_value() {
        assert_eq!(trim_header_value("  hello   world  "), "hello world");
    }

    #[test]
    fn test_full_sigv4_verification() {
        // Build a request and verify the signature we compute matches
        let secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let date = "20130524";
        let region = "us-east-1";
        let datetime = "20130524T000000Z";

        let method = "GET";
        let path = "/test.txt";
        let query = "";
        let body_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let mut headers = HeaderMap::new();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert("x-amz-date", datetime.parse().unwrap());
        headers.insert("x-amz-content-sha256", body_hash.parse().unwrap());

        let signed_headers = vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ];

        let canonical_request =
            build_canonical_request(method, path, query, &headers, &signed_headers, body_hash);

        let scope = format!("{}/{}/s3/aws4_request", date, region);
        let string_to_sign = build_string_to_sign(datetime, &scope, &canonical_request);
        let signing_key = derive_signing_key(secret_key, date, region, "s3");
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        // Now verify
        let auth_parts = AuthParts {
            access_key_id: access_key_id.to_string(),
            date: date.to_string(),
            region: region.to_string(),
            signed_headers,
            signature,
        };

        let result = verify_header(
            method,
            path,
            query,
            &headers,
            body_hash,
            secret_key,
            &auth_parts,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_signature_mismatch() {
        let secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let datetime = "20130524T000000Z";
        let body_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let mut headers = HeaderMap::new();
        headers.insert("host", "examplebucket.s3.amazonaws.com".parse().unwrap());
        headers.insert("x-amz-date", datetime.parse().unwrap());
        headers.insert("x-amz-content-sha256", body_hash.parse().unwrap());

        let auth_parts = AuthParts {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            date: "20130524".to_string(),
            region: "us-east-1".to_string(),
            signed_headers: vec![
                "host".to_string(),
                "x-amz-content-sha256".to_string(),
                "x-amz-date".to_string(),
            ],
            signature: "invalidsignature".to_string(),
        };

        let result = verify_header(
            "GET",
            "/test.txt",
            "",
            &headers,
            body_hash,
            secret_key,
            &auth_parts,
        );
        assert!(result.is_err());
    }
}
