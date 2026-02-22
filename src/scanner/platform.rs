/// Extract platform-specific file identity (inode, device_id) from metadata.
///
/// Both fields are `Option<u64>` — platforms that don't support them return `None`.
#[cfg(unix)]
pub fn file_identity(metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;
    (Some(metadata.ino()), Some(metadata.dev()))
}

#[cfg(windows)]
pub fn file_identity(metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::windows::fs::MetadataExt;
    (
        metadata.file_index(),
        metadata.volume_serial_number().map(|v| v as u64),
    )
}

#[cfg(not(any(unix, windows)))]
pub fn file_identity(_metadata: &std::fs::Metadata) -> (Option<u64>, Option<u64>) {
    (None, None)
}
