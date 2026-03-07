use serde::{Deserialize, Serialize};

/// A single CORS rule for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsRule {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    #[serde(default)]
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

/// Resolved CORS headers to attach to a response.
#[derive(Debug)]
pub struct CorsHeaders {
    pub allow_origin: String,
    pub allow_methods: String,
    pub allow_headers: String,
    pub expose_headers: String,
    pub max_age: Option<u32>,
}
