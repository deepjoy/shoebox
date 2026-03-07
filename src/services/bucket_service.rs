use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::provider::CredentialProvider;
use crate::config::BucketConfig;
use crate::error::S3Error;
use crate::metadata::MetadataStore;
use crate::scanner::watcher::FilesystemWatcher;
use crate::storage::FilesystemStorage;
use crate::types::cors::CorsRule;
use crate::types::notification::EventBus;

/// Runtime state for a single loaded bucket.
///
/// Holds both the core S3 state (storage, metadata, config) and scanner
/// state (watcher, scheduler). Used by both the HTTP layer (`AppState`)
/// and the library API (`Shoebox`).
pub struct LoadedBucket {
    pub name: String,
    pub config: BucketConfig,
    pub storage: FilesystemStorage,
    pub metadata: MetadataStore,
    pub parts_dir: std::path::PathBuf,
    /// Filesystem watcher — kept alive to receive change events.
    /// Dropping this stops the watcher.
    pub watcher: Option<FilesystemWatcher>,
    /// Taskmill scheduler for background scan tasks (L1/L2/L3).
    /// `Scheduler` is `Clone` (internally `Arc`-wrapped).
    pub scheduler: taskmill::Scheduler,
    /// True when this bucket's config was generated for the first time during build.
    pub freshly_created: bool,
    /// In-memory cache for CORS rules. Populated on first request, invalidated
    /// on PutBucketCors / DeleteBucketCors.
    pub cors_cache: Arc<tokio::sync::RwLock<Option<Vec<CorsRule>>>>,
    /// Per-bucket event bus for S3 events (object created, deleted, etc.).
    /// Dropping this causes all subscribers (NotificationService::listen) to exit.
    pub event_bus: EventBus,
}

/// Shared storage for async integrity check results.
pub type IntegrityCheckStore = Arc<
    tokio::sync::RwLock<HashMap<String, crate::services::integrity_service::IntegrityCheckResult>>,
>;

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    pub buckets: Arc<HashMap<String, LoadedBucket>>,
    pub credential_provider: Arc<tokio::sync::RwLock<CredentialProvider>>,
    pub bucket_names: Arc<Vec<String>>,
    /// Async integrity check results, keyed by check_id.
    pub integrity_checks: IntegrityCheckStore,
    /// Shutdown token for cancelling background tasks.
    pub shutdown_token: tokio_util::sync::CancellationToken,
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
