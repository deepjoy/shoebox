use std::sync::Arc;

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
}

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub buckets: Arc<Vec<LoadedBucket>>,
}

impl AppState {
    /// Look up a loaded bucket by name.
    pub fn get_bucket(&self, name: &str) -> Result<&LoadedBucket, S3Error> {
        self.buckets
            .iter()
            .find(|b| b.name == name)
            .ok_or(S3Error::NoSuchBucket)
    }
}

/// Bulk delete objects, returning deleted keys and errors.
pub async fn delete_objects_bulk(
    bucket: &LoadedBucket,
    keys: &[String],
) -> (Vec<String>, Vec<(String, S3Error)>) {
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for key in keys {
        match super::object_service::delete_object(&bucket.storage, &bucket.metadata, key).await {
            Ok(()) => deleted.push(key.clone()),
            Err(e) => errors.push((key.clone(), e)),
        }
    }

    (deleted, errors)
}
