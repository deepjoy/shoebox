use serde::{Deserialize, Serialize};

/// A multipart upload record stored in the database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MultipartUpload {
    pub id: String,
    pub key: String,
    pub initiated_at: String,
    pub content_type: Option<String>,
    pub metadata: Option<String>,
}

/// A part of a multipart upload stored in the database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Part {
    pub id: String,
    pub upload_id: String,
    pub part_number: i32,
    pub size: i64,
    pub etag: String,
    pub uploaded_at: String,
}

/// Result of completing a multipart upload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteResult {
    pub location: String,
    pub bucket: String,
    pub key: String,
    pub etag: String,
}

/// Information about a single part for ListParts response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartInfo {
    pub part_number: i32,
    pub last_modified: String,
    pub etag: String,
    pub size: i64,
}

/// Result of ListParts operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPartsResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<PartInfo>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<i32>,
}

/// Information about an in-progress upload for ListMultipartUploads response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadInfo {
    pub key: String,
    pub upload_id: String,
    pub initiated: String,
}

/// Result of ListMultipartUploads operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListMultipartUploadsResult {
    pub bucket: String,
    pub uploads: Vec<UploadInfo>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

/// Report of cleanup operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupReport {
    pub cleaned: usize,
    pub bytes_freed: u64,
}

/// Part specification for CompleteMultipartUpload request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletePart {
    #[serde(rename = "PartNumber")]
    pub part_number: i32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// CompleteMultipartUpload request body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMultipartUploadRequest {
    #[serde(rename = "Part")]
    pub parts: Vec<CompletePart>,
}
