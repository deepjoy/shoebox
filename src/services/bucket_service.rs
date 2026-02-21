use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::provider::CredentialProvider;
use crate::config::BucketConfig;
use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::storage::FilesystemStorage;

/// Runtime state for a single loaded bucket.
pub struct LoadedBucket {
    pub name: String,
    pub config: BucketConfig,
    pub storage: FilesystemStorage,
    pub metadata: MetadataStore,
    pub parts_dir: std::path::PathBuf,
}

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub buckets: Arc<HashMap<String, LoadedBucket>>,
    pub credential_provider: Arc<tokio::sync::RwLock<CredentialProvider>>,
    pub bucket_names: Arc<Vec<String>>,
}

impl AppState {
    /// Look up a loaded bucket by name.
    pub fn get_bucket(&self, name: &str) -> Result<&LoadedBucket, S3Error> {
        self.buckets.get(name).ok_or(S3Error::NoSuchBucket)
    }
}

/// Bulk delete objects by removing files from storage and then
/// deleting all metadata records in a single SQL query.
pub async fn delete_objects_bulk(
    bucket: &LoadedBucket,
    keys: &[String],
) -> (Vec<String>, Vec<(String, S3Error)>) {
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    // Phase 1: delete files from storage
    for key in keys {
        match bucket.storage.delete(key).await {
            Ok(()) | Err(S3Error::NoSuchKey) => deleted.push(key.clone()),
            Err(e) => errors.push((key.clone(), e)),
        }
    }

    // Phase 2: bulk-delete metadata for all successfully-deleted keys
    if !deleted.is_empty() {
        if let Err(e) = bucket.metadata.delete_objects(&deleted).await {
            // If the bulk metadata delete fails, report all keys as errors.
            let failed: Vec<(String, S3Error)> = deleted
                .drain(..)
                .map(|k| (k, S3Error::InternalError))
                .collect();
            errors.extend(failed);
            tracing::error!("Bulk metadata delete failed: {e}");
        }
    }

    (deleted, errors)
}
