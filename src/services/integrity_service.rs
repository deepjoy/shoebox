use std::path::Path;

use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::S3Error;
use crate::metadata::sqlite::ObjectRecord;
use crate::metadata::MetadataStore;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IntegrityCheckResult {
    pub check_id: String,
    pub status: String,
    pub files_checked: usize,
    pub bytes_checked: u64,
    pub files_ok: usize,
    pub discrepancies: Vec<Discrepancy>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Discrepancy {
    pub key: String,
    pub object_id: String,
    pub reason: String,
    pub stored_hash: Option<String>,
    pub computed_hash: Option<String>,
    pub mtime_changed: bool,
}

/// Run an integrity check over objects in a single bucket.
pub async fn execute_check(
    metadata: &MetadataStore,
    root: &Path,
    check_id: Uuid,
    scope: Option<&str>,
    shutdown: CancellationToken,
) -> Result<IntegrityCheckResult, S3Error> {
    let files = match scope {
        Some(prefix) => metadata.get_objects_with_prefix(prefix).await?,
        None => metadata.get_all_objects_at_level_3().await?,
    };

    let mut discrepancies = Vec::new();
    let mut files_ok = 0usize;
    let mut bytes_checked = 0u64;

    for file in &files {
        if shutdown.is_cancelled() {
            return Ok(IntegrityCheckResult {
                check_id: check_id.to_string(),
                status: "cancelled".to_string(),
                files_checked: files_ok + discrepancies.len(),
                bytes_checked,
                files_ok,
                discrepancies,
            });
        }

        match verify_file(root, file).await {
            Ok(()) => {
                files_ok += 1;
                bytes_checked += file.size.unwrap_or(0) as u64;
            }
            Err(discrepancy) => {
                discrepancies.push(discrepancy);
            }
        }
    }

    Ok(IntegrityCheckResult {
        check_id: check_id.to_string(),
        status: "complete".to_string(),
        files_checked: files.len(),
        bytes_checked,
        files_ok,
        discrepancies,
    })
}

/// Verify a single file's on-disk content against its stored metadata.
async fn verify_file(root: &Path, record: &ObjectRecord) -> Result<(), Discrepancy> {
    let path = root.join(&record.key);

    let stored_hash = match record.checksum_sha256.as_ref() {
        Some(h) => h,
        None => return Ok(()), // No hash stored, skip
    };

    // Check file exists
    let fs_meta = tokio::fs::metadata(&path).await.map_err(|_| Discrepancy {
        key: record.key.clone(),
        object_id: record.id.clone(),
        reason: "file_missing".to_string(),
        ..Default::default()
    })?;

    // Check mtime
    let current_mtime: Option<crate::metadata::sqlite::SqliteTimestamp> = fs_meta
        .modified()
        .ok()
        .and_then(crate::scanner::levels::system_time_to_odt)
        .map(crate::metadata::sqlite::SqliteTimestamp);

    let mtime_changed = record.file_mtime != current_mtime;

    // Compute fresh SHA-256
    let computed_hash = compute_sha256(&path).await.map_err(|_| Discrepancy {
        key: record.key.clone(),
        object_id: record.id.clone(),
        reason: "read_error".to_string(),
        ..Default::default()
    })?;

    if computed_hash != *stored_hash {
        return Err(Discrepancy {
            key: record.key.clone(),
            object_id: record.id.clone(),
            stored_hash: Some(stored_hash.clone()),
            computed_hash: Some(computed_hash),
            mtime_changed,
            reason: if mtime_changed {
                "content_mismatch_mtime_changed".to_string()
            } else {
                "content_mismatch_no_mtime_change".to_string()
            },
        });
    }

    Ok(())
}

/// Compute base64-encoded SHA-256 of a file.
async fn compute_sha256(path: &Path) -> Result<String, std::io::Error> {
    use base64::Engine;
    use tokio::io::AsyncReadExt;

    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hash = hasher.finalize();
    Ok(base64::engine::general_purpose::STANDARD.encode(hash))
}
