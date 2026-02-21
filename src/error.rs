use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("The specified bucket does not exist")]
    NoSuchBucket,

    #[error("The specified key does not exist")]
    NoSuchKey,

    #[error("Access Denied")]
    AccessDenied,

    #[error("The request signature we calculated does not match the signature you provided")]
    SignatureDoesNotMatch,

    #[error("The specified bucket is not valid")]
    InvalidBucketName,

    #[error("Invalid Argument")]
    InvalidArgument,

    #[error("The Content-MD5 you specified did not match what we received")]
    BadDigest,

    #[error("The specified method is not allowed against this resource")]
    MethodNotAllowed,

    #[error("The specified bucket already exists")]
    BucketAlreadyExists,

    #[error("Your previous request to create the named bucket succeeded and you already own it")]
    BucketAlreadyOwnedByYou,

    #[error("The AWS Access Key Id you provided does not exist in our records")]
    InvalidAccessKeyId,

    #[error("The provided token has expired")]
    ExpiredToken,

    #[error("The authorization header is malformed")]
    AuthorizationHeaderMalformed,

    #[error("Your request was missing a required header")]
    MissingSecurityHeader,

    #[error("The specified credential does not exist")]
    NoSuchCredential,

    #[error("At least one of the pre-conditions you specified did not hold")]
    PreconditionFailed,

    #[error("Not Modified")]
    NotModified,

    #[error("The Range header is invalid")]
    InvalidRange,

    #[error("The requested range is not satisfiable")]
    RangeNotSatisfiable,

    #[error("{0}")]
    Conflict(String),

    #[error("Bad Request: {0}")]
    BadRequest(String),

    #[error("We encountered an internal error, please try again")]
    InternalError,
}

impl S3Error {
    /// S3 error code string.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::AccessDenied => "AccessDenied",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::InvalidBucketName => "InvalidBucketName",
            Self::InvalidArgument => "InvalidArgument",
            Self::BadDigest => "BadDigest",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::ExpiredToken => "ExpiredToken",
            Self::AuthorizationHeaderMalformed => "AuthorizationHeaderMalformed",
            Self::MissingSecurityHeader => "MissingSecurityHeader",
            Self::NoSuchCredential => "NoSuchCredential",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::NotModified => "NotModified",
            Self::InvalidRange => "InvalidRange",
            Self::RangeNotSatisfiable => "InvalidRange",
            Self::Conflict(_) => "Conflict",
            Self::BadRequest(_) => "BadRequest",
            Self::InternalError => "InternalError",
        }
    }

    /// Human-readable error message.
    pub fn message(&self) -> String {
        self.to_string()
    }

    /// HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NoSuchBucket | Self::NoSuchKey => StatusCode::NOT_FOUND,
            Self::AccessDenied | Self::SignatureDoesNotMatch | Self::InvalidAccessKeyId => {
                StatusCode::FORBIDDEN
            }
            Self::InvalidBucketName
            | Self::InvalidArgument
            | Self::BadDigest
            | Self::ExpiredToken
            | Self::AuthorizationHeaderMalformed
            | Self::MissingSecurityHeader
            | Self::InvalidRange
            | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NoSuchCredential => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::BucketAlreadyExists | Self::BucketAlreadyOwnedByYou | Self::Conflict(_) => {
                StatusCode::CONFLICT
            }
            Self::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            Self::NotModified => StatusCode::NOT_MODIFIED,
            Self::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Render to S3-compatible XML error response.
    pub fn to_xml(&self, request_id: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <RequestId>{}</RequestId>
</Error>"#,
            self.code(),
            escape_xml(&self.message()),
            escape_xml(request_id)
        )
    }
}

/// Escape XML special characters in a string.
fn escape_xml(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        // 304 Not Modified must not have a body per HTTP spec.
        if matches!(self, Self::NotModified) {
            return axum::http::Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .body(axum::body::Body::empty())
                .unwrap_or_else(|_| {
                    axum::http::Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(axum::body::Body::empty())
                        .expect("minimal 500 response must not fail")
                });
        }

        let request_id = uuid::Uuid::new_v4().to_string();
        let body = self.to_xml(&request_id);

        axum::http::Response::builder()
            .status(self.status_code())
            .header("content-type", "application/xml")
            .header("x-amz-request-id", &request_id)
            .body(axum::body::Body::from(body))
            .unwrap_or_else(|_| {
                axum::http::Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(axum::body::Body::empty())
                    .expect("minimal 500 response must not fail")
            })
    }
}

impl From<std::io::Error> for S3Error {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => Self::NoSuchKey,
            std::io::ErrorKind::PermissionDenied => Self::AccessDenied,
            _ => {
                tracing::error!("IO error: {err}");
                Self::InternalError
            }
        }
    }
}

impl From<sqlx::Error> for S3Error {
    fn from(err: sqlx::Error) -> Self {
        tracing::error!("Database error: {err}");
        Self::InternalError
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BucketNameError {
    #[error("Bucket name must be between 3 and 63 characters long, got {0}")]
    InvalidLength(usize),

    #[error("Bucket name can only contain lowercase letters, numbers, hyphens, and periods")]
    InvalidCharacters,

    #[error("Bucket name must start and end with a letter or number")]
    InvalidStartEnd,

    #[error("Bucket name must not contain consecutive hyphens or periods")]
    ConsecutiveHyphensOrPeriods,

    #[error("Bucket name must not be formatted as an IP address")]
    LooksLikeIpAddress,
}

/// Validate a bucket name against S3 naming rules.
pub fn validate_bucket_name(name: &str) -> Result<(), BucketNameError> {
    if name.len() < 3 || name.len() > 63 {
        return Err(BucketNameError::InvalidLength(name.len()));
    }
    // Check IP format before character validation (IPs contain dots
    // which would otherwise be rejected as invalid characters).
    if looks_like_ip_address(name) {
        return Err(BucketNameError::LooksLikeIpAddress);
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return Err(BucketNameError::InvalidCharacters);
    }
    if name.starts_with(['-', '.']) || name.ends_with(['-', '.']) {
        return Err(BucketNameError::InvalidStartEnd);
    }
    if name.contains("--") || name.contains("..") {
        return Err(BucketNameError::ConsecutiveHyphensOrPeriods);
    }
    Ok(())
}

/// Check if a string looks like an IP address (e.g. "192.168.1.1").
fn looks_like_ip_address(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    parts.iter().all(|p| p.parse::<u8>().is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_bucket_names() {
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("bucket123").is_ok());
        assert!(validate_bucket_name("abc").is_ok());
        assert!(validate_bucket_name("a-b-c").is_ok());
    }

    #[test]
    fn test_invalid_bucket_name_length() {
        assert!(matches!(
            validate_bucket_name("ab"),
            Err(BucketNameError::InvalidLength(2))
        ));
        let long_name = "a".repeat(64);
        assert!(matches!(
            validate_bucket_name(&long_name),
            Err(BucketNameError::InvalidLength(64))
        ));
    }

    #[test]
    fn test_invalid_bucket_name_characters() {
        assert!(matches!(
            validate_bucket_name("My-Bucket"),
            Err(BucketNameError::InvalidCharacters)
        ));
        assert!(matches!(
            validate_bucket_name("my_bucket"),
            Err(BucketNameError::InvalidCharacters)
        ));
        assert!(matches!(
            validate_bucket_name("my bucket"),
            Err(BucketNameError::InvalidCharacters)
        ));
    }

    #[test]
    fn test_invalid_bucket_name_start_end() {
        assert!(matches!(
            validate_bucket_name("-my-bucket"),
            Err(BucketNameError::InvalidStartEnd)
        ));
        assert!(matches!(
            validate_bucket_name("my-bucket-"),
            Err(BucketNameError::InvalidStartEnd)
        ));
    }

    #[test]
    fn test_invalid_bucket_name_consecutive_hyphens() {
        assert!(matches!(
            validate_bucket_name("my--bucket"),
            Err(BucketNameError::ConsecutiveHyphensOrPeriods)
        ));
    }

    #[test]
    fn test_invalid_bucket_name_consecutive_periods() {
        assert!(matches!(
            validate_bucket_name("my..bucket"),
            Err(BucketNameError::ConsecutiveHyphensOrPeriods)
        ));
    }

    #[test]
    fn test_valid_bucket_name_with_dots() {
        assert!(validate_bucket_name("my.bucket.name").is_ok());
        assert!(validate_bucket_name("a.b.c").is_ok());
    }

    #[test]
    fn test_invalid_bucket_name_dot_at_edges() {
        assert!(matches!(
            validate_bucket_name(".my-bucket"),
            Err(BucketNameError::InvalidStartEnd)
        ));
        assert!(matches!(
            validate_bucket_name("my-bucket."),
            Err(BucketNameError::InvalidStartEnd)
        ));
    }

    #[test]
    fn test_invalid_bucket_name_ip_address() {
        assert!(matches!(
            validate_bucket_name("192.168.1.1"),
            Err(BucketNameError::LooksLikeIpAddress)
        ));
    }

    #[test]
    fn test_not_ip_address() {
        // This has hyphens, not dots - should be fine
        assert!(validate_bucket_name("192-168-1-1").is_ok());
    }

    #[test]
    fn test_s3_error_xml_format() {
        let err = S3Error::NoSuchKey;
        let xml = err.to_xml("test-request-id");
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
        assert!(xml.contains("<Message>The specified key does not exist</Message>"));
        assert!(xml.contains("<RequestId>test-request-id</RequestId>"));
    }

    #[test]
    fn test_s3_error_status_codes() {
        assert_eq!(S3Error::NoSuchBucket.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::NoSuchKey.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::AccessDenied.status_code(), StatusCode::FORBIDDEN);
        assert_eq!(
            S3Error::InvalidArgument.status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            S3Error::InternalError.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
