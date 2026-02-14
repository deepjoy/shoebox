use axum::http::StatusCode;

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
}
