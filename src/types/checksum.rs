/// Base64-encoded checksums for all four S3 additional checksum algorithms.
///
/// Values are stored and transmitted as standard base64 (not hex), matching
/// the S3 wire format for `x-amz-checksum-*` headers.
#[derive(Debug, Clone, Default)]
pub struct ChecksumValues {
    pub sha256: Option<String>,
    pub sha1: Option<String>,
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
}

impl ChecksumValues {
    /// Returns true if all checksum fields are None.
    pub fn is_empty(&self) -> bool {
        self.sha256.is_none()
            && self.sha1.is_none()
            && self.crc32.is_none()
            && self.crc32c.is_none()
    }
}
