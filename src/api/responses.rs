use std::collections::HashMap;

use axum::{
    http::{header, HeaderName, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// Wraps a `Serialize`-able value and renders it as an S3-style XML response.
pub struct XmlResponse<T: Serialize>(pub T);

impl<T: Serialize> IntoResponse for XmlResponse<T> {
    fn into_response(self) -> Response {
        match quick_xml::se::to_string(&self.0) {
            Ok(xml) => {
                let body = format!(r#"<?xml version="1.0" encoding="UTF-8"?>{}"#, xml);
                ([(header::CONTENT_TYPE, "application/xml")], body).into_response()
            }
            Err(e) => {
                tracing::error!("XML serialization failed: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

/// Response for GetObject / HeadObject — streams the body with S3-style headers.
pub struct ObjectResponse {
    pub body: axum::body::Body,
    pub content_length: u64,
    pub content_type: String,
    pub etag: String,
    pub last_modified: String,
    pub metadata: HashMap<String, String>,
}

impl IntoResponse for ObjectResponse {
    fn into_response(self) -> Response {
        let mut builder = axum::http::Response::builder()
            .header(header::CONTENT_TYPE, &self.content_type)
            .header(header::CONTENT_LENGTH, self.content_length.to_string())
            .header(header::ETAG, &self.etag)
            .header(header::LAST_MODIFIED, &self.last_modified);

        for (key, value) in &self.metadata {
            if let Ok(name) = HeaderName::from_bytes(format!("x-amz-meta-{}", key).as_bytes()) {
                builder = builder.header(name, value);
            }
        }

        builder
            .body(self.body)
            .unwrap_or_else(|_| {
                axum::http::Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(axum::body::Body::empty())
                    .unwrap()
            })
            .into_response()
    }
}
