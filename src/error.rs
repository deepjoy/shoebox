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
            Self::AccessDenied | Self::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            Self::InvalidBucketName | Self::InvalidArgument | Self::BadDigest => {
                StatusCode::BAD_REQUEST
            }
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::BucketAlreadyExists | Self::BucketAlreadyOwnedByYou => StatusCode::CONFLICT,
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
            self.message(),
            request_id
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = self.to_xml(&request_id);

        axum::http::Response::builder()
            .status(self.status_code())
            .header("content-type", "application/xml")
            .header("x-amz-request-id", &request_id)
            .body(axum::body::Body::from(body))
            .unwrap()
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
