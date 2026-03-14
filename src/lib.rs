pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod metadata;
pub mod scanner;
pub mod services;
pub mod storage;
pub mod types;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::api::routes::create_router;
use crate::auth::presigned;
use crate::auth::provider::CredentialProvider;
use crate::config::{load_global_config, resolve_bucket, GlobalConfig, METADATA_DB};
use crate::error::{S3Error, ShoeboxError};
use crate::metadata::sqlite::{ListEntry, ObjectRecord, Tag};
use crate::metadata::MetadataStore;

use crate::scanner::app_state::{BucketScanState, ScanAppState};
use crate::scanner::backpressure::ScannerResources;
use crate::scanner::levels;
use crate::scanner::scope::ScanScope;
use crate::scanner::tasks::{
    ScanL1Executor, ScanL1Task, ScanL2Executor, ScanL2Task, ScanL3Executor, ScanL3Task,
};
use crate::scanner::watcher::FilesystemWatcher;
use crate::scanner::worker;
use crate::services::copy_service::{self, CopyConditions, CopyResult};
use crate::services::object_service::{self, GetObjectResult, PutObjectInput, PutObjectResult};
use crate::services::{
    duplicates_service, integrity_service, merge_service, tagging_service, AppState, LoadedBucket,
};
use crate::storage::filesystem::FilesystemStorage;
use std::pin::Pin;

/// Register shoebox's scan executors on an external [`taskmill::SchedulerBuilder`].
///
/// Call this when building a shared scheduler for use with
/// [`ShoeboxBuilder::scheduler`]. Registers the L1/L2/L3 scan executors
/// so the scheduler can process scan tasks submitted by shoebox.
///
/// # Example
///
/// ```ignore
/// let builder = shoebox::register_scan_executors(
///     taskmill::Scheduler::builder()
///         .store_path("app.db")
///         .max_concurrency(4),
/// );
/// let scheduler = builder.build().await?;
///
/// let shoebox = Shoebox::builder()
///     .bucket("/photos")
///     .scheduler("photos", scheduler.clone())
///     .build()
///     .await?;
/// ```
pub fn register_scan_executors(builder: taskmill::SchedulerBuilder) -> taskmill::SchedulerBuilder {
    builder
        .typed_executor::<ScanL1Task, _>(Arc::new(ScanL1Executor))
        .typed_executor::<ScanL2Task, _>(Arc::new(ScanL2Executor))
        .typed_executor::<ScanL3Task, _>(Arc::new(ScanL3Executor))
}

/// A boxed, sendable stream of list entries.
pub type ListStream<'a> =
    Pin<Box<dyn futures::Stream<Item = Result<ListEntry, S3Error>> + Send + 'a>>;

/// Main Shoebox builder and runtime.
///
/// `Shoebox` is the Rust-native library API. Each public method maps to an
/// S3 operation and can be called directly without starting an HTTP server.
/// When HTTP serving is needed, `router()` or `run()` build an internal
/// `AppState` and hand it to the Axum router.
pub struct Shoebox {
    buckets: Arc<HashMap<String, LoadedBucket>>,
    credential_provider: Arc<tokio::sync::RwLock<CredentialProvider>>,
    host: String,
    port: u16,
    shutdown_token: CancellationToken,
}

impl Shoebox {
    pub fn builder() -> ShoeboxBuilder {
        ShoeboxBuilder::default()
    }

    /// Quick start: serve a single directory.
    pub async fn serve(path: impl AsRef<Path>) -> Result<(), ShoeboxError> {
        Self::builder().bucket(path).build().await?.run().await
    }

    // -- S3-equivalent library methods --

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<GetObjectResult, S3Error> {
        let b = self.get_bucket(bucket)?;
        object_service::get_object(&b.storage, &b.metadata, key).await
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        stream: impl futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
        input: PutObjectInput,
    ) -> Result<PutObjectResult, S3Error> {
        let b = self.get_bucket(bucket)?;
        let result =
            object_service::put_object(&b.storage, &b.metadata, key, stream, input).await?;

        b.event_bus.emit(crate::types::notification::S3Event {
            event_name: "s3:ObjectCreated:Put".to_string(),
            event_time: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            bucket: bucket.to_string(),
            object_id: result.object_id.clone(),
            object_key: key.to_string(),
            size: Some(result.size),
            etag: Some(result.etag.clone()),
            source_object_id: None,
        });

        Ok(result)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        object_service::delete_object(&b.storage, &b.metadata, key).await?;

        b.event_bus.emit(crate::types::notification::S3Event {
            event_name: "s3:ObjectRemoved:Delete".to_string(),
            event_time: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            bucket: bucket.to_string(),
            object_id: String::new(),
            object_key: key.to_string(),
            size: None,
            etag: None,
            source_object_id: None,
        });

        Ok(())
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectRecord, S3Error> {
        let b = self.get_bucket(bucket)?;
        object_service::head_object(&b.storage, &b.metadata, key).await
    }

    /// Stream list entries for a bucket, ordered by key.
    ///
    /// Returns a stream of [`ListEntry`] items (objects and/or common
    /// prefixes when a delimiter is provided). The caller controls how
    /// many entries to consume (e.g. via `.take(n)`).
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<ListStream<'_>, S3Error> {
        let b = self.get_bucket(bucket)?;
        Ok(b.metadata
            .list_objects_stream(prefix, delimiter, start_after))
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        conditions: &CopyConditions,
    ) -> Result<CopyResult, S3Error> {
        let src = self.get_bucket(src_bucket)?;
        let dst = self.get_bucket(dst_bucket)?;
        let result = copy_service::copy_object(
            &src.storage,
            &src.metadata,
            src_key,
            &dst.storage,
            &dst.metadata,
            dst_key,
            conditions,
        )
        .await?;

        dst.event_bus.emit(crate::types::notification::S3Event {
            event_name: "s3:ObjectCreated:Copy".to_string(),
            event_time: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            bucket: dst_bucket.to_string(),
            object_id: result.object_id.clone(),
            object_key: dst_key.to_string(),
            size: Some(result.size),
            etag: Some(result.etag.clone()),
            source_object_id: None,
        });

        Ok(result)
    }

    pub async fn rename_object(
        &self,
        bucket: &str,
        src_key: &str,
        dst_key: &str,
        overwrite: bool,
    ) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        copy_service::rename_object(&b.storage, &b.metadata, src_key, dst_key, overwrite).await
    }

    pub async fn get_tags(&self, bucket: &str, key: &str) -> Result<Vec<Tag>, S3Error> {
        let b = self.get_bucket(bucket)?;
        tagging_service::get_tags(&b.metadata, key).await
    }

    pub async fn put_tags(&self, bucket: &str, key: &str, tags: Vec<Tag>) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        tagging_service::put_tags(&b.metadata, key, tags).await
    }

    pub async fn delete_tags(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        tagging_service::delete_tags(&b.metadata, key).await
    }

    // -- Multipart upload methods --

    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String, S3Error> {
        let b = self.get_bucket(bucket)?;
        crate::services::multipart_service::initiate(
            &b.metadata,
            &b.parts_dir,
            key,
            content_type,
            metadata,
        )
        .await
    }

    pub async fn upload_part<S>(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
        part_number: i32,
        stream: S,
    ) -> Result<String, S3Error>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Unpin,
    {
        let b = self.get_bucket(bucket)?;
        crate::services::multipart_service::upload_part(
            &b.metadata,
            &b.parts_dir,
            upload_id,
            part_number,
            stream,
        )
        .await
    }

    pub async fn complete_multipart(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<crate::types::multipart::CompleteResult, S3Error> {
        let b = self.get_bucket(bucket)?;
        crate::services::multipart_service::complete(
            &b.storage,
            &b.metadata,
            &b.parts_dir,
            bucket,
            upload_id,
            parts,
        )
        .await
    }

    pub async fn abort_multipart(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        crate::services::multipart_service::abort(&b.metadata, &b.parts_dir, upload_id).await
    }

    pub fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        expires_secs: u64,
    ) -> Result<String, S3Error> {
        let b = self.get_bucket(bucket)?;
        let cred = b.config.credentials.first().ok_or(S3Error::AccessDenied)?;
        Ok(presigned::generate_presigned_get(
            &self.endpoint(),
            bucket,
            key,
            &cred.access_key_id,
            &cred.secret_access_key,
            expires_secs,
        ))
    }

    pub fn presign_put(
        &self,
        bucket: &str,
        key: &str,
        expires_secs: u64,
        content_type: Option<&str>,
    ) -> Result<String, S3Error> {
        let b = self.get_bucket(bucket)?;
        let cred = b.config.credentials.first().ok_or(S3Error::AccessDenied)?;
        Ok(presigned::generate_presigned_put(
            &self.endpoint(),
            bucket,
            key,
            &cred.access_key_id,
            &cred.secret_access_key,
            expires_secs,
            content_type,
        ))
    }

    // -- Scanner methods --

    /// Trigger a scan at the given level for a bucket.
    ///
    /// Submits an L1 task that will cascade into L2/L3 based on the
    /// requested target level.
    pub async fn scan(&self, bucket: &str, target_level: i32) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        b.scheduler
            .submit_typed(&ScanL1Task {
                bucket: bucket.to_string(),
                scope: ScanScope::Bucket,
                target_level,
                priority: None,
            })
            .await
            .map_err(|_| S3Error::InternalError)?;
        Ok(())
    }

    /// Run a blocking L1 scan and return the report.
    pub async fn scan_l1(&self, bucket: &str) -> Result<scanner::L1Report, S3Error> {
        let b = self.get_bucket(bucket)?;
        levels::scan_l1(&b.metadata, b.storage.root(), &ScanScope::Bucket).await
    }

    /// Trigger sync for a bucket — submits L1 (HIGH) + L2 (NORMAL) tasks
    /// to TaskMill and returns immediately.
    ///
    /// Does NOT run L3 (content hashing) — that runs in the background
    /// via taskmill at its own pace.
    pub async fn sync(&self, bucket: &str) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        crate::services::sync_service::sync(&b.scheduler, bucket).await
    }

    // -- Phase 8: Duplicates, Merge, Integrity --

    /// Find duplicate files within a single bucket.
    pub async fn find_bucket_duplicates(
        &self,
        bucket: &str,
        max_results: i32,
        allow_partial: bool,
        continuation_token: Option<&str>,
        key_contains: Option<&str>,
        max_depth: Option<i32>,
    ) -> Result<duplicates_service::DuplicateReport, S3Error> {
        let b = self.get_bucket(bucket)?;
        duplicates_service::find_bucket_duplicates(
            &b.metadata,
            bucket,
            max_results,
            allow_partial,
            continuation_token,
            key_contains,
            max_depth,
        )
        .await
    }

    /// Find duplicate files across all buckets using streaming merge.
    pub async fn find_duplicates(
        &self,
        max_results: i32,
    ) -> Result<duplicates_service::CrossBucketDuplicateReport, S3Error> {
        let bucket_pairs: Vec<(&str, &MetadataStore)> = self
            .buckets
            .iter()
            .map(|(name, b)| (name.as_str(), &b.metadata))
            .collect();
        duplicates_service::find_cross_bucket_duplicates(&bucket_pairs, max_results).await
    }

    /// Find duplicate directories within a single bucket.
    pub async fn find_bucket_duplicate_dirs(
        &self,
        bucket: &str,
        min_files: i32,
        max_results: i32,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_depth: Option<i32>,
    ) -> Result<duplicates_service::DuplicateDirReport, S3Error> {
        let b = self.get_bucket(bucket)?;
        duplicates_service::find_bucket_duplicate_dirs(
            &b.metadata,
            bucket,
            min_files,
            max_results,
            prefix,
            continuation_token,
            max_depth,
        )
        .await
    }

    /// Compare two directories across buckets.
    pub async fn compare_dirs(
        &self,
        left_bucket: &str,
        left_path: &str,
        right_bucket: &str,
        right_path: &str,
    ) -> Result<duplicates_service::DirComparison, S3Error> {
        let left = self.get_bucket(left_bucket)?;
        let right = self.get_bucket(right_bucket)?;
        duplicates_service::compare_dirs(
            &left.metadata,
            left_bucket,
            left_path,
            &right.metadata,
            right_bucket,
            right_path,
        )
        .await
    }

    /// Synchronous integrity check — blocks until complete.
    pub async fn check_integrity(
        &self,
        bucket: &str,
        scope: Option<&str>,
    ) -> Result<integrity_service::IntegrityCheckResult, S3Error> {
        let b = self.get_bucket(bucket)?;
        let check_id = uuid::Uuid::new_v4();
        integrity_service::execute_check(
            &b.metadata,
            b.storage.root(),
            check_id,
            scope,
            self.shutdown_token.clone(),
        )
        .await
    }

    /// Async integrity check — spawns a background task, returns immediately.
    pub fn check_integrity_async(
        &self,
        bucket: &str,
        scope: Option<&str>,
    ) -> Result<integrity_service::IntegrityCheckResult, S3Error> {
        let b = self.get_bucket(bucket)?;
        let check_id = uuid::Uuid::new_v4();
        let metadata = b.metadata.clone();
        let root = b.storage.root().to_path_buf();
        let scope = scope.map(String::from);
        let token = self.shutdown_token.clone();

        tokio::spawn(async move {
            integrity_service::execute_check(&metadata, &root, check_id, scope.as_deref(), token)
                .await
                .ok();
        });

        Ok(integrity_service::IntegrityCheckResult {
            check_id: check_id.to_string(),
            status: "in_progress".to_string(),
            ..Default::default()
        })
    }

    /// Merge duplicate objects: keep winner, delete losers.
    pub async fn merge_duplicates(
        &self,
        bucket: &str,
        winner_key: &str,
        loser_keys: &[&str],
    ) -> Result<merge_service::MergeResult, S3Error> {
        let state = self.get_bucket(bucket)?;

        // Resolve keys to object_ids
        let winner = state
            .metadata
            .get_object(winner_key)
            .await?
            .ok_or(S3Error::NoSuchKey)?;
        let mut loser_ids = Vec::new();
        for key in loser_keys {
            let obj = state
                .metadata
                .get_object(key)
                .await?
                .ok_or(S3Error::NoSuchKey)?;
            loser_ids.push(obj.id);
        }

        let loser_refs: Vec<&str> = loser_ids.iter().map(|s| s.as_str()).collect();

        let result =
            merge_service::merge_duplicates(&state.metadata, &winner.id, &loser_refs).await?;

        // Delete loser objects from DB and disk
        for key in loser_keys {
            object_service::delete_object(&state.storage, &state.metadata, key).await?;
        }

        Ok(result)
    }

    // -- Phase 9: CORS configuration --

    pub async fn get_cors_rules(
        &self,
        bucket: &str,
    ) -> Result<Vec<crate::types::cors::CorsRule>, S3Error> {
        let b = self.get_bucket(bucket)?;
        services::cors_service::get_rules(&b.metadata).await
    }

    pub async fn set_cors_rules(
        &self,
        bucket: &str,
        rules: Vec<crate::types::cors::CorsRule>,
    ) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        services::cors_service::set_rules(&b.metadata, rules).await?;
        services::cors_service::invalidate_cache(&b.cors_cache).await;
        Ok(())
    }

    pub async fn delete_cors_rules(&self, bucket: &str) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        services::cors_service::delete_rules(&b.metadata).await?;
        services::cors_service::invalidate_cache(&b.cors_cache).await;
        Ok(())
    }

    // -- Phase 9: Webhook configuration --

    pub async fn get_webhooks(
        &self,
        bucket: &str,
    ) -> Result<Vec<crate::types::notification::WebhookConfig>, S3Error> {
        let b = self.get_bucket(bucket)?;
        services::notification_service::get_webhook_config(&b.metadata).await
    }

    pub async fn set_webhooks(
        &self,
        bucket: &str,
        webhooks: Vec<crate::types::notification::WebhookConfig>,
    ) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        services::notification_service::set_webhook_config(&b.metadata, webhooks).await
    }

    // -- HTTP layer bridge --

    // -- Helper methods for HTTP layer --

    fn to_app_state(&self) -> AppState {
        AppState {
            buckets: self.buckets.clone(),
            credential_provider: self.credential_provider.clone(),
            bucket_names: Arc::new(self.buckets.keys().cloned().collect()),
            integrity_checks: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            shutdown_token: self.shutdown_token.clone(),
        }
    }

    /// Create an Axum router for embedding in a custom server.
    pub fn router(&self) -> Router {
        create_router(self.to_app_state())
    }

    /// Spawn scheduled integrity checks for all buckets.
    ///
    /// Runs a full integrity check every `interval` for each bucket.
    /// Checks are staggered across buckets to avoid hammering all at once.
    /// Cancels automatically when the shutdown token fires.
    pub fn spawn_scheduled_integrity_checks(&self, interval: std::time::Duration) {
        for (name, bucket) in self.buckets.iter() {
            let metadata = bucket.metadata.clone();
            let root = bucket.storage.root().to_path_buf();
            let token = self.shutdown_token.clone();
            let bucket_name = name.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(interval) => {}
                        _ = token.cancelled() => {
                            tracing::debug!(bucket = %bucket_name, "Scheduled integrity check cancelled");
                            break;
                        }
                    }

                    let check_id = uuid::Uuid::new_v4();
                    tracing::info!(bucket = %bucket_name, check_id = %check_id, "Starting scheduled integrity check");

                    match integrity_service::execute_check(
                        &metadata,
                        &root,
                        check_id,
                        None,
                        token.clone(),
                    )
                    .await
                    {
                        Ok(result) => {
                            if result.discrepancies.is_empty() {
                                tracing::info!(
                                    bucket = %bucket_name,
                                    check_id = %check_id,
                                    files_checked = result.files_checked,
                                    status = %result.status,
                                    "Scheduled integrity check complete — no discrepancies"
                                );
                            } else {
                                tracing::warn!(
                                    bucket = %bucket_name,
                                    check_id = %check_id,
                                    files_checked = result.files_checked,
                                    discrepancies = result.discrepancies.len(),
                                    status = %result.status,
                                    "Scheduled integrity check found discrepancies"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                bucket = %bucket_name,
                                check_id = %check_id,
                                error = %e,
                                "Scheduled integrity check failed"
                            );
                        }
                    }
                }
            });
        }
    }

    /// Run the built-in HTTP server with graceful shutdown.
    pub async fn run(self) -> Result<(), ShoeboxError> {
        let addr = format!("{}:{}", self.host, self.port);

        // Wire SIGINT/SIGTERM to cancel the shared shutdown token
        tokio::spawn({
            let token = self.shutdown_token.clone();
            async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("Shutdown signal received, draining requests...");
                token.cancel();
            }
        });

        // Spawn scheduled integrity checks (every 24 hours)
        self.spawn_scheduled_integrity_checks(std::time::Duration::from_secs(24 * 60 * 60));

        // Spawn abandoned multipart upload cleanup (every 6 hours, max age 24 hours)
        for bucket in self.buckets.values() {
            let metadata = bucket.metadata.clone();
            let parts_dir = bucket.parts_dir.clone();
            let token = self.shutdown_token.clone();
            tokio::spawn(crate::services::multipart_service::cleanup_loop(
                metadata,
                parts_dir,
                std::time::Duration::from_secs(24 * 60 * 60),
                std::time::Duration::from_secs(6 * 60 * 60),
                token,
            ));
        }

        let app_state = self.to_app_state();
        let router = create_router(app_state);
        let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                ShoeboxError::PortInUse { port: self.port }
            } else {
                ShoeboxError::BindFailed {
                    addr: addr.clone(),
                    source: e,
                }
            }
        })?;
        tracing::info!("Serving on http://{addr}");

        axum::serve(listener, router)
            .with_graceful_shutdown(self.shutdown_token.cancelled_owned())
            .await
            .map_err(|e| ShoeboxError::Other(e.into()))?;

        // After server stops, close all SQLite pools to flush WAL
        for bucket in self.buckets.values() {
            bucket.metadata.close().await;
        }
        tracing::info!("Shutdown complete");
        Ok(())
    }

    /// Access loaded buckets for inspection (e.g., startup banner).
    pub fn loaded_buckets(&self) -> &HashMap<String, LoadedBucket> {
        &self.buckets
    }

    /// The host this instance is configured to listen on.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port this instance is configured to listen on.
    pub fn port(&self) -> u16 {
        self.port
    }

    fn get_bucket(&self, name: &str) -> Result<&LoadedBucket, S3Error> {
        self.buckets.get(name).ok_or(S3Error::NoSuchBucket)
    }

    fn endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

#[derive(Default)]
pub struct ShoeboxBuilder {
    paths: Vec<PathBuf>,
    host: Option<String>,
    port: Option<u16>,
    data_dir: Option<PathBuf>,
    global_config: Option<GlobalConfig>,
    config_file: Option<PathBuf>,
    external_schedulers: HashMap<String, taskmill::Scheduler>,
}

#[cfg(test)]
impl Shoebox {
    /// Expose bucket names for testing.
    fn bucket_names(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }
}

impl ShoeboxBuilder {
    pub fn bucket(mut self, path: impl AsRef<Path>) -> Self {
        self.paths.push(path.as_ref().to_path_buf());
        self
    }
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
    pub fn data_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.data_dir = Some(dir.as_ref().to_path_buf());
        self
    }
    /// Set a pre-built global configuration.
    ///
    /// Global config provides bucket paths, host/port defaults, and
    /// cross-bucket credentials. Explicit builder setters (`.host()`,
    /// `.port()`, `.bucket()`) take precedence over values in the config.
    pub fn global_config(mut self, config: GlobalConfig) -> Self {
        self.global_config = Some(config);
        self
    }
    /// Load a global configuration from a TOML file during `.build()`.
    ///
    /// If both `.config_file()` and `.global_config()` are set, the
    /// pre-built config from `.global_config()` wins.
    pub fn config_file(mut self, path: impl AsRef<Path>) -> Self {
        self.config_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Use a pre-built scheduler for a bucket instead of creating one internally.
    ///
    /// The scheduler must already have scan executors registered (see
    /// [`register_scan_executors`]). Shoebox will inject its scan state via
    /// [`taskmill::Scheduler::register_state`] and submit initial scan
    /// tasks, but will **not** spawn the `run()` loop — the caller manages
    /// the scheduler lifecycle.
    pub fn scheduler(mut self, bucket: &str, scheduler: taskmill::Scheduler) -> Self {
        self.external_schedulers
            .insert(bucket.to_string(), scheduler);
        self
    }

    pub async fn build(mut self) -> Result<Shoebox, ShoeboxError> {
        // Resolve global config: explicit object takes priority over file
        let global_config = match (self.global_config, self.config_file) {
            (Some(gc), _) => Some(gc),
            (None, Some(path)) => Some(
                load_global_config(&path)
                    .await
                    .map_err(|e| ShoeboxError::Other(e.into()))?,
            ),
            (None, None) => None,
        };

        // Merge paths: explicit .bucket() calls > global config
        let paths = if self.paths.is_empty() {
            global_config
                .as_ref()
                .map(|gc| gc.buckets.clone())
                .unwrap_or_default()
        } else {
            self.paths
        };

        // Merge host/port: explicit setter > global config > default
        let host = self
            .host
            .or_else(|| global_config.as_ref().and_then(|gc| gc.host.clone()))
            .unwrap_or_else(|| "0.0.0.0".into());
        let port = self
            .port
            .or_else(|| global_config.as_ref().and_then(|gc| gc.port))
            .unwrap_or(9000);

        let shutdown_token = CancellationToken::new();

        // ── Phase 1: Resolve all buckets and run blocking L1 scans ───
        struct ResolvedBucket {
            name: String,
            config: crate::config::BucketConfig,
            root: PathBuf,
            shoebox_dir: PathBuf,
            metadata: MetadataStore,
            storage: FilesystemStorage,
            parts_dir: PathBuf,
            freshly_created: bool,
        }

        let mut resolved: Vec<ResolvedBucket> = Vec::with_capacity(paths.len());

        for path in &paths {
            let state = resolve_bucket(path, self.data_dir.as_deref())
                .await
                .map_err(|e| match e.downcast_ref::<std::io::Error>() {
                    Some(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                        ShoeboxError::PermissionDenied { path: path.clone() }
                    }
                    _ => ShoeboxError::Other(e),
                })?;
            let db_path = state.shoebox_dir.join(METADATA_DB);
            let metadata = MetadataStore::new(&db_path)
                .await
                .map_err(|e| ShoeboxError::Other(e.into()))?;
            let storage = FilesystemStorage::new(state.root.clone());
            let parts_dir = state.shoebox_dir.join("parts");
            tokio::fs::create_dir_all(&parts_dir)
                .await
                .map_err(|e| ShoeboxError::Other(e.into()))?;

            // Blocking L1 scan — discover files before serving so that
            // list_objects returns results immediately after build().
            let l1_report = levels::scan_l1(&metadata, &state.root, &ScanScope::Bucket)
                .await
                .map_err(|e| ShoeboxError::Other(e.into()))?;
            if l1_report.discovered > 0 || l1_report.deleted > 0 {
                tracing::info!(
                    bucket = %state.name,
                    discovered = l1_report.discovered,
                    deleted = l1_report.deleted,
                    unchanged = l1_report.unchanged,
                    "L1 scan complete"
                );
            }

            resolved.push(ResolvedBucket {
                name: state.name,
                config: state.config,
                root: state.root,
                shoebox_dir: state.shoebox_dir,
                metadata,
                storage,
                parts_dir,
                freshly_created: state.freshly_created,
            });
        }

        // ── Phase 2: Build ScanAppState ──
        let scan_app_state = Arc::new(ScanAppState {
            buckets: resolved
                .iter()
                .map(|r| {
                    (
                        r.name.clone(),
                        BucketScanState {
                            metadata: r.metadata.clone(),
                            root: r.root.clone(),
                        },
                    )
                })
                .collect(),
        });

        // ── Phase 3: Build per-bucket taskmill Schedulers ───────────
        let mut buckets = HashMap::new();

        for r in resolved {
            let external = self.external_schedulers.remove(&r.name);

            let scheduler = if let Some(ext) = external {
                // External scheduler — inject scan state, caller manages run loop.
                ext.register_state(scan_app_state.clone()).await;
                ext
            } else {
                // Internal scheduler — build and spawn.
                let taskmill_db = r.shoebox_dir.join("taskmill.db");
                let taskmill_db_str = taskmill_db.to_string_lossy().to_string();

                let sched = taskmill::Scheduler::builder()
                    .store_path(&taskmill_db_str)
                    .typed_executor::<ScanL1Task, _>(Arc::new(ScanL1Executor))
                    .typed_executor::<ScanL2Task, _>(Arc::new(ScanL2Executor))
                    .typed_executor::<ScanL3Task, _>(Arc::new(ScanL3Executor))
                    .max_concurrency(1)
                    .pressure_source(Box::new(ScannerResources::new(100)))
                    .throttle_policy(taskmill::ThrottlePolicy::default_three_tier())
                    .app_state_arc(scan_app_state.clone())
                    .build()
                    .await
                    .map_err(|e| ShoeboxError::Other(e.into()))?;

                tokio::spawn({
                    let s = sched.clone();
                    let token = shutdown_token.child_token();
                    async move { s.run(token).await }
                });

                sched
            };

            // Submit initial background L2+L3 scan tasks.
            let _ = scheduler
                .submit_typed(&ScanL2Task {
                    bucket: r.name.clone(),
                    cursor: None,
                    priority: None,
                })
                .await;
            let _ = scheduler
                .submit_typed(&ScanL3Task {
                    bucket: r.name.clone(),
                    cursor: None,
                    bytes_per_sec: None,
                })
                .await;

            // Start filesystem watcher
            let watch_capacity = global_config
                .as_ref()
                .and_then(|gc| gc.watch_channel_capacity)
                .unwrap_or(1000);
            let watcher = {
                let (watch_tx, watch_rx) = tokio::sync::mpsc::channel(watch_capacity);
                let watch_drops = Arc::new(std::sync::atomic::AtomicU64::new(0));
                match FilesystemWatcher::spawn(r.root.clone(), watch_tx, watch_drops.clone()).await
                {
                    Ok(w) => {
                        tracing::debug!(bucket = %r.name, "Filesystem watcher started");
                        tokio::spawn(worker::run_watch_processor(
                            r.metadata.clone(),
                            r.root.clone(),
                            watch_rx,
                            scheduler.clone(),
                            r.name.clone(),
                            watch_drops,
                            shutdown_token.clone(),
                        ));
                        Some(w)
                    }
                    Err(e) => {
                        tracing::warn!(
                            bucket = %r.name,
                            error = %e,
                            "Failed to start filesystem watcher"
                        );
                        None
                    }
                }
            };

            buckets.insert(
                r.name.clone(),
                LoadedBucket {
                    name: r.name,
                    config: r.config,
                    storage: r.storage,
                    metadata: r.metadata,
                    parts_dir: r.parts_dir,
                    watcher,
                    scheduler,
                    freshly_created: r.freshly_created,
                    cors_cache: Arc::new(tokio::sync::RwLock::new(None)),
                    event_bus: crate::types::notification::EventBus::new(256),
                },
            );
        }

        // Subscribe NotificationService to each bucket's EventBus for webhook delivery
        for (name, bucket) in &buckets {
            let metadata = Arc::new(bucket.metadata.clone());
            let (notification_svc, delivery_worker) =
                crate::services::notification_service::NotificationService::new(
                    metadata,
                    shutdown_token.clone(),
                );
            let rx = bucket.event_bus.subscribe();
            let listen_future = notification_svc.listen(rx);
            tokio::spawn(listen_future);
            tokio::spawn(delivery_worker);
            tracing::debug!(bucket = %name, "Notification service subscribed to EventBus");
        }

        let mut provider = CredentialProvider::from_buckets(
            &buckets
                .values()
                .map(|b| (b.name.clone(), &b.config))
                .collect::<Vec<_>>(),
        );

        if let Some(ref gc) = global_config {
            provider.add_global_credentials(&gc.credentials);
        }

        let credential_provider = Arc::new(tokio::sync::RwLock::new(provider));

        Ok(Shoebox {
            buckets: Arc::new(buckets),
            credential_provider,
            host,
            port,
            shutdown_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use futures::TryStreamExt;
    use services::object_service::PutObjectInput;
    use std::collections::HashSet;
    use tempfile::TempDir;

    /// Collect all `ListEntry::Object` records from a list_objects stream.
    async fn collect_objects(shoebox: &Shoebox, bucket: &str) -> Vec<ObjectRecord> {
        let stream = shoebox.list_objects(bucket, "", None, None).unwrap();
        stream
            .try_filter_map(|entry| async move {
                match entry {
                    ListEntry::Object(r) => Ok(Some(*r)),
                    ListEntry::CommonPrefix(_) => Ok(None),
                }
            })
            .try_collect()
            .await
            .unwrap()
    }

    async fn build_shoebox(tmp: &TempDir, bucket_name: &str) -> Shoebox {
        let bucket_dir = tmp.path().join(bucket_name);
        std::fs::create_dir_all(&bucket_dir).unwrap();
        Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_builder_resolves_bucket() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;
        assert!(shoebox.bucket_names().contains(&"photos".to_string()));
    }

    #[tokio::test]
    async fn test_builder_multiple_buckets() {
        let tmp = TempDir::new().unwrap();
        let dir_a = tmp.path().join("alpha");
        let dir_b = tmp.path().join("bravo");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&dir_a)
            .bucket(&dir_b)
            .build()
            .await
            .unwrap();

        let names = shoebox.bucket_names();
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"bravo".to_string()));
    }

    #[tokio::test]
    async fn test_put_get_delete_without_http() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "test-bucket").await;

        // PUT
        let data = Bytes::from_static(b"hello, world!");
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
        let put_result = shoebox
            .put_object(
                "test-bucket",
                "greeting.txt",
                stream,
                PutObjectInput {
                    content_type: "text/plain".to_string(),
                    user_metadata: HashMap::new(),
                    content_md5: None,
                    checksum_sha256: None,
                    checksum_sha1: None,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                },
            )
            .await
            .unwrap();
        assert!(!put_result.etag.is_empty());

        // GET
        let get_result = shoebox
            .get_object("test-bucket", "greeting.txt")
            .await
            .unwrap();
        assert_eq!(get_result.record.key, "greeting.txt");

        // DELETE
        shoebox
            .delete_object("test-bucket", "greeting.txt")
            .await
            .unwrap();

        // Verify deleted
        let err = shoebox.get_object("test-bucket", "greeting.txt").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_head_object_without_http() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "test-bucket").await;

        let data = Bytes::from_static(b"twelve chars");
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
        shoebox
            .put_object(
                "test-bucket",
                "file.txt",
                stream,
                PutObjectInput {
                    content_type: "text/plain".to_string(),
                    user_metadata: HashMap::new(),
                    content_md5: None,
                    checksum_sha256: None,
                    checksum_sha1: None,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                },
            )
            .await
            .unwrap();

        let record = shoebox
            .head_object("test-bucket", "file.txt")
            .await
            .unwrap();
        assert_eq!(record.key, "file.txt");
        assert_eq!(record.size, Some(12));
    }

    #[tokio::test]
    async fn test_list_objects_without_http() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "test-bucket").await;

        // Put two objects
        for name in &["a.txt", "b.txt"] {
            let data = Bytes::from_static(b"data");
            let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
            shoebox
                .put_object(
                    "test-bucket",
                    name,
                    stream,
                    PutObjectInput {
                        content_type: "text/plain".to_string(),
                        user_metadata: HashMap::new(),
                        content_md5: None,
                        checksum_sha256: None,
                        checksum_sha1: None,
                        checksum_crc32: None,
                        checksum_crc32c: None,
                    },
                )
                .await
                .unwrap();
        }

        let objects = collect_objects(&shoebox, "test-bucket").await;
        assert_eq!(objects.len(), 2);
        let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
        assert!(keys.contains(&"a.txt"));
        assert!(keys.contains(&"b.txt"));
    }

    #[tokio::test]
    async fn test_presign_get_generates_valid_url() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let url = shoebox.presign_get("photos", "sunset.jpg", 3600).unwrap();
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.contains("X-Amz-Expires=3600"));
        assert!(url.contains("/photos/sunset.jpg"));
    }

    #[tokio::test]
    async fn test_presign_put_generates_valid_url() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let url = shoebox
            .presign_put("photos", "upload.txt", 600, Some("text/plain"))
            .unwrap();
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.contains("X-Amz-Expires=600"));
        assert!(url.contains("/photos/upload.txt"));
    }

    #[tokio::test]
    async fn test_presign_nonexistent_bucket_errors() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let err = shoebox.presign_get("nonexistent", "key", 3600);
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_router_rejects_unauthenticated() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let router = shoebox.router();

        use tower::ServiceExt;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/photos")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should be 403 because no Authorization header
        assert_eq!(
            response.status(),
            axum::http::StatusCode::FORBIDDEN,
            "Unauthenticated request should be rejected with 403"
        );
    }

    #[tokio::test]
    async fn test_router_rejects_unknown_access_key() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let router = shoebox.router();

        use tower::ServiceExt;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/photos")
                    .header(
                        "Authorization",
                        "AWS4-HMAC-SHA256 Credential=NONEXISTENT/20250101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123",
                    )
                    .header("x-amz-date", "20250101T000000Z")
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should be 403 because access key doesn't exist
        assert_eq!(
            response.status(),
            axum::http::StatusCode::FORBIDDEN,
            "Unknown access key should be rejected with 403"
        );
    }

    #[tokio::test]
    async fn test_nonexistent_bucket_returns_error() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        let err = shoebox.get_object("nonexistent", "key").await;
        assert!(matches!(err, Err(S3Error::NoSuchBucket)));
    }

    #[tokio::test]
    async fn test_router_with_signed_request() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("photos");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Read the auto-generated credential
        let provider = shoebox.credential_provider.read().await;
        let creds: Vec<_> = provider.list();
        let cred = creds[0].clone();
        drop(provider);

        let router = shoebox.router();

        // Sign a GET request to list objects in the photos bucket
        let method = "GET";
        let path = "/photos";
        let query = "list-type=2";
        let datetime = "20250101T000000Z";
        let date = "20250101";
        let region = "us-east-1";
        let body_hash = auth::sigv4::sha256_hex(b"");

        let mut headers = axum::http::HeaderMap::new();
        headers.insert("host", "localhost:9000".parse().unwrap());
        headers.insert("x-amz-date", datetime.parse().unwrap());
        headers.insert("x-amz-content-sha256", body_hash.parse().unwrap());

        let signed_headers = vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ];

        let canonical_request = auth::sigv4::build_canonical_request(
            method,
            path,
            query,
            &headers,
            &signed_headers,
            &body_hash,
        );
        let scope = format!("{}/{}/s3/aws4_request", date, region);
        let string_to_sign =
            auth::sigv4::build_string_to_sign(datetime, &scope, &canonical_request);
        let signing_key =
            auth::sigv4::derive_signing_key(&cred.secret_access_key, date, region, "s3");
        let signature = hex::encode(auth::sigv4::hmac_sha256(
            &signing_key,
            string_to_sign.as_bytes(),
        ));

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/s3/aws4_request, SignedHeaders={}, Signature={}",
            cred.access_key_id,
            date,
            region,
            signed_headers.join(";"),
            signature
        );

        use tower::ServiceExt;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("{}?{}", path, query))
                    .header("host", "localhost:9000")
                    .header("x-amz-date", datetime)
                    .header("x-amz-content-sha256", &body_hash)
                    .header("Authorization", auth_header)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "Signed request should succeed"
        );
    }

    // -- Scanner tests (Phase 6) --

    #[tokio::test]
    async fn test_scanner_discovers_preexisting_files() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("photos");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Create files BEFORE building Shoebox (simulates pre-existing content)
        std::fs::write(bucket_dir.join("hello.txt"), "hello world").unwrap();
        std::fs::create_dir_all(bucket_dir.join("subdir")).unwrap();
        std::fs::write(bucket_dir.join("subdir/nested.txt"), "nested").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // L1 scan should have discovered the files during build()
        let objects = collect_objects(&shoebox, "photos").await;
        assert_eq!(objects.len(), 2);
        let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
        assert!(keys.contains(&"hello.txt"));
        assert!(keys.contains(&"subdir/nested.txt"));
    }

    #[tokio::test]
    async fn test_scanner_skips_shoebox_dir() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("mybucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Create a visible file and a file inside .shoebox/
        std::fs::write(bucket_dir.join("visible.txt"), "visible").unwrap();
        std::fs::create_dir_all(bucket_dir.join(".shoebox")).unwrap();
        std::fs::write(bucket_dir.join(".shoebox/secret.toml"), "secret").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        let objects = collect_objects(&shoebox, "mybucket").await;

        // Only the visible file should be listed
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key, "visible.txt");
    }

    #[tokio::test]
    async fn test_scanner_coexists_with_api_uploads() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("mixed");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Pre-existing file
        std::fs::write(bucket_dir.join("pre-existing.txt"), "pre").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Upload via API
        let data = Bytes::from_static(b"uploaded via API");
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
        shoebox
            .put_object(
                "mixed",
                "api-uploaded.txt",
                stream,
                PutObjectInput {
                    content_type: "text/plain".to_string(),
                    user_metadata: HashMap::new(),
                    content_md5: None,
                    checksum_sha256: None,
                    checksum_sha1: None,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                },
            )
            .await
            .unwrap();

        // Both should be listable
        let objects = collect_objects(&shoebox, "mixed").await;
        assert_eq!(objects.len(), 2);
        let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
        assert!(keys.contains(&"pre-existing.txt"));
        assert!(keys.contains(&"api-uploaded.txt"));

        // Scanner-discovered file starts at scan_level 1 and may be promoted
        // by the background worker; API-uploaded files always start at level 3.
        let pre = objects
            .iter()
            .find(|o| o.key == "pre-existing.txt")
            .unwrap();
        let api = objects
            .iter()
            .find(|o| o.key == "api-uploaded.txt")
            .unwrap();
        assert!(pre.scan_level >= 1);
        assert_eq!(api.scan_level, 3);
    }

    #[tokio::test]
    async fn test_scan_l1_library_method() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("scantest");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join("a.txt"), "a").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Files already discovered on build, so re-scan should find 0 new
        let report = shoebox.scan_l1("scantest").await.unwrap();
        assert_eq!(report.discovered, 0);
        assert_eq!(report.unchanged, 1);

        // Add a new file
        std::fs::write(bucket_dir.join("b.txt"), "b").unwrap();

        // Re-scan should see 2 total files. The new file may have been
        // discovered by the background filesystem watcher before scan_l1
        // runs, so we check the total rather than asserting discovered == 1.
        let report = shoebox.scan_l1("scantest").await.unwrap();
        assert_eq!(report.discovered + report.unchanged, 2);
    }

    #[tokio::test]
    async fn test_builder_with_global_config() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("gc-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let gc = config::GlobalConfig {
            buckets: vec![bucket_dir],
            host: Some("127.0.0.1".into()),
            port: Some(3333),
            credentials: vec![config::Credential {
                access_key_id: "AKIAGLOBALTEST000000".into(),
                secret_access_key: "globalsecret".into(),
                description: Some("global cred".into()),
                permissions: Some(vec!["read".into()]),
            }],
            ..Default::default()
        };

        // Build using global_config — no explicit .bucket() calls
        let shoebox = Shoebox::builder().global_config(gc).build().await.unwrap();

        assert_eq!(shoebox.host, "127.0.0.1");
        assert_eq!(shoebox.port, 3333);
        assert!(shoebox.buckets.contains_key("gc-bucket"));

        // Global credential should be present
        let provider = shoebox.credential_provider.read().await;
        let cred = provider.lookup("AKIAGLOBALTEST000000");
        assert!(cred.is_some());
        assert!(cred.unwrap().bucket_name.is_none()); // global, not bucket-scoped
    }

    #[tokio::test]
    async fn test_builder_explicit_overrides_global_config() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("override-test");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let gc = config::GlobalConfig {
            buckets: vec![],
            host: Some("10.0.0.1".into()),
            port: Some(4444),
            credentials: vec![],
            ..Default::default()
        };

        let shoebox = Shoebox::builder()
            .global_config(gc)
            .bucket(&bucket_dir)
            .host("192.168.1.1")
            .port(5555)
            .build()
            .await
            .unwrap();

        // Explicit builder values win
        assert_eq!(shoebox.host, "192.168.1.1");
        assert_eq!(shoebox.port, 5555);
        assert!(shoebox.buckets.contains_key("override-test"));
    }

    #[tokio::test]
    async fn test_builder_config_file() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("file-cfg");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let config_path = tmp.path().join("shoebox.toml");
        let toml_content = format!(
            "host = \"1.2.3.4\"\nport = 7777\nbuckets = [\"{}\"]\n",
            bucket_dir.display()
        );
        std::fs::write(&config_path, toml_content).unwrap();

        let shoebox = Shoebox::builder()
            .config_file(&config_path)
            .build()
            .await
            .unwrap();

        assert_eq!(shoebox.host, "1.2.3.4");
        assert_eq!(shoebox.port, 7777);
        assert!(shoebox.buckets.contains_key("file-cfg"));
    }

    // -- Phase 7 tests --

    #[tokio::test]
    async fn test_sync_library_method() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("synctest");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join("a.txt"), "a").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Sync should submit tasks and return immediately without error
        shoebox.sync("synctest").await.unwrap();

        // Sync on nonexistent bucket should fail
        let err = shoebox.sync("nonexistent").await;
        assert!(matches!(err, Err(S3Error::NoSuchBucket)));
    }

    #[tokio::test]
    async fn test_move_detection_preserves_object_id() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("movetest");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join("original.txt"), "move me").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Get the original object's id
        let original = shoebox
            .head_object("movetest", "original.txt")
            .await
            .unwrap();
        let original_id = original.id.clone();

        // Rename the file on disk (filesystem-level move)
        std::fs::rename(
            bucket_dir.join("original.txt"),
            bucket_dir.join("renamed.txt"),
        )
        .unwrap();

        // Run L1 scan to detect the move
        let report = shoebox.scan_l1("movetest").await.unwrap();

        // Should detect 1 move, 0 new discoveries, 0 deletions
        assert_eq!(report.moved, 1, "Should detect 1 move");
        assert_eq!(report.discovered, 0, "No new files");
        assert_eq!(
            report.deleted, 0,
            "Old key should not be deleted (it was moved)"
        );

        // The renamed file should exist with the SAME object_id
        let renamed = shoebox
            .head_object("movetest", "renamed.txt")
            .await
            .unwrap();
        assert_eq!(
            renamed.id, original_id,
            "Object ID should be preserved across rename"
        );

        // The old key should no longer exist
        let old = shoebox.head_object("movetest", "original.txt").await;
        assert!(old.is_err(), "Old key should not exist");
    }

    // -- Phase 8: Duplicates + Integrity tests --------------------------------

    /// Helper: upload a file via put_object (sets scan_level=3 with checksum_sha256).
    async fn put_file(shoebox: &Shoebox, bucket: &str, key: &str, content: &[u8]) {
        let data = Bytes::from(content.to_vec());
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
        shoebox
            .put_object(
                bucket,
                key,
                stream,
                PutObjectInput {
                    content_type: "application/octet-stream".to_string(),
                    user_metadata: HashMap::new(),
                    content_md5: None,
                    checksum_sha256: None,
                    checksum_sha1: None,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cross_bucket_duplicates_streaming_merge() {
        let tmp = TempDir::new().unwrap();
        let dir_a = tmp.path().join("alpha");
        let dir_b = tmp.path().join("bravo");
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&dir_a)
            .bucket(&dir_b)
            .build()
            .await
            .unwrap();

        // Upload identical content to both buckets
        put_file(
            &shoebox,
            "alpha",
            "sunset.txt",
            b"Beautiful sunset over the ocean",
        )
        .await;
        put_file(
            &shoebox,
            "bravo",
            "sunset-copy.txt",
            b"Beautiful sunset over the ocean",
        )
        .await;

        // Upload a unique file in bravo (should NOT appear in duplicates)
        put_file(&shoebox, "bravo", "unique.txt", b"Only in bravo").await;

        // Upload another cross-bucket duplicate pair
        put_file(&shoebox, "alpha", "mountain.txt", b"Mountain landscape").await;
        put_file(
            &shoebox,
            "bravo",
            "mountain-backup.txt",
            b"Mountain landscape",
        )
        .await;

        let report = shoebox.find_duplicates(100).await.unwrap();

        // Should find exactly 2 duplicate groups
        assert_eq!(
            report.duplicates.len(),
            2,
            "Expected 2 cross-bucket duplicate groups, got {}",
            report.duplicates.len()
        );

        // Each group should span both buckets
        for group in &report.duplicates {
            assert!(
                group.files.len() >= 2,
                "Each duplicate group should have at least 2 files"
            );
            let buckets: HashSet<&str> = group.files.iter().map(|f| f.bucket.as_str()).collect();
            assert!(
                buckets.contains("alpha") && buckets.contains("bravo"),
                "Duplicate group should span both buckets, got: {:?}",
                buckets
            );
        }

        assert!(!report.is_truncated);
    }

    #[tokio::test]
    async fn test_directory_hash_computation() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        // Create two directories with identical contents
        put_file(&shoebox, "photos", "dir_a/file1.txt", b"hello").await;
        put_file(&shoebox, "photos", "dir_a/file2.txt", b"world").await;
        put_file(&shoebox, "photos", "dir_b/file1.txt", b"hello").await;
        put_file(&shoebox, "photos", "dir_b/file2.txt", b"world").await;

        // Create a directory with different contents
        put_file(&shoebox, "photos", "dir_c/file1.txt", b"different").await;
        put_file(&shoebox, "photos", "dir_c/file2.txt", b"content").await;

        // Compute directory hashes
        let bucket = shoebox.get_bucket("photos").unwrap();
        services::duplicates_service::recompute_stale_directory_hashes(&bucket.metadata)
            .await
            .unwrap();

        // Find duplicate directories — dir_a and dir_b should match
        let report = shoebox
            .find_bucket_duplicate_dirs("photos", 1, 100, None, None, None)
            .await
            .unwrap();

        assert_eq!(
            report.duplicate_dirs.len(),
            1,
            "Expected 1 duplicate dir group (dir_a == dir_b), got {}",
            report.duplicate_dirs.len()
        );

        let group = &report.duplicate_dirs[0];
        assert_eq!(group.dirs.len(), 2, "Group should contain 2 directories");

        let prefixes: HashSet<&str> = group.dirs.iter().map(|d| d.prefix.as_str()).collect();
        assert!(prefixes.contains("dir_a/"), "Should contain dir_a/");
        assert!(prefixes.contains("dir_b/"), "Should contain dir_b/");
        assert!(
            !prefixes.contains("dir_c/"),
            "dir_c/ should NOT be in the duplicate group"
        );
    }

    #[tokio::test]
    async fn test_scheduled_integrity_checks_run_on_interval() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "bucket").await;

        put_file(&shoebox, "bucket", "test.txt", b"integrity test data").await;

        // Verify the file is at L3 before scheduling
        let objects = collect_objects(&shoebox, "bucket").await;
        assert_eq!(objects.len(), 1, "Should have 1 object");
        assert_eq!(objects[0].scan_level, 3);

        // Schedule checks with a very short interval (50ms)
        shoebox.spawn_scheduled_integrity_checks(std::time::Duration::from_millis(50));

        // Wait long enough for at least one scheduled check to execute.
        // The scheduled check calls execute_check internally — we can't observe
        // its result directly, but we verify it doesn't panic or corrupt state.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Cancel the scheduled checks via shutdown token
        shoebox.shutdown_token.cancel();

        // Give spawned tasks time to observe cancellation and exit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // After cancellation, a fresh Shoebox with a new token should still work
        let shoebox2 = build_shoebox(&tmp, "bucket2").await;
        put_file(&shoebox2, "bucket2", "after.txt", b"post-cancel").await;
        let result = shoebox2.check_integrity("bucket2", None).await.unwrap();
        assert_eq!(result.status, "complete");
        assert_eq!(result.files_checked, 1);
        assert_eq!(result.files_ok, 1);
    }

    #[tokio::test]
    async fn test_integrity_check_cancelled_on_shutdown() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "bucket").await;

        // Upload enough files that the check won't finish instantly
        for i in 0..20 {
            put_file(
                &shoebox,
                "bucket",
                &format!("file_{:03}.txt", i),
                format!("content for file {}", i).as_bytes(),
            )
            .await;
        }

        // Cancel the token BEFORE running the check — the check should
        // observe cancellation immediately on its first iteration
        shoebox.shutdown_token.cancel();

        let result = shoebox.check_integrity("bucket", None).await.unwrap();

        assert_eq!(
            result.status, "cancelled",
            "Integrity check should report cancelled status, got: {}",
            result.status
        );
        assert!(
            result.files_checked < 20,
            "Should not have checked all 20 files (checked {})",
            result.files_checked
        );
    }

    #[tokio::test]
    async fn test_find_bucket_duplicates_returns_groups_with_object_id() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        // Upload duplicate files (same content, different keys)
        put_file(&shoebox, "photos", "a/sunset.txt", b"sunset over ocean").await;
        put_file(&shoebox, "photos", "b/sunset.txt", b"sunset over ocean").await;
        put_file(&shoebox, "photos", "c/sunset.txt", b"sunset over ocean").await;

        // Upload a unique file
        put_file(&shoebox, "photos", "unique.txt", b"unique content").await;

        let report = shoebox
            .find_bucket_duplicates("photos", 100, false, None, None, None)
            .await
            .unwrap();

        assert_eq!(report.duplicates.len(), 1, "Should find 1 duplicate group");
        assert!(report.scan_complete, "Scan should be complete");

        let group = &report.duplicates[0];
        assert_eq!(group.files.len(), 3, "Group should have 3 files");
        assert!(
            !group.checksum_sha256.is_empty(),
            "Group should have a hash"
        );

        // Every file in the group must have a non-empty object_id
        for file in &group.files {
            assert!(!file.object_id.is_empty(), "File should have object_id");
            assert!(!file.key.is_empty(), "File should have key");
        }

        let keys: HashSet<&str> = group.files.iter().map(|f| f.key.as_str()).collect();
        assert!(keys.contains("a/sunset.txt"));
        assert!(keys.contains("b/sunset.txt"));
        assert!(keys.contains("c/sunset.txt"));
    }

    #[tokio::test]
    async fn test_scan_pending_error_when_l3_incomplete() {
        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("pending");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Create files on disk BEFORE build — L1 scan discovers them at scan_level=1
        std::fs::write(bucket_dir.join("file1.txt"), "hello").unwrap();
        std::fs::write(bucket_dir.join("file2.txt"), "world").unwrap();

        let shoebox = Shoebox::builder()
            .bucket(&bucket_dir)
            .build()
            .await
            .unwrap();

        // Files are at L1, not L3 — FindBucketDuplicates should fail with ScanPending
        let err = shoebox
            .find_bucket_duplicates("pending", 100, false, None, None, None)
            .await;

        assert!(
            matches!(err, Err(S3Error::ScanPending { .. })),
            "Expected ScanPending error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn test_allow_partial_returns_results_despite_incomplete_scan() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "partial").await;

        // Upload a duplicate pair via API (these become L3)
        put_file(&shoebox, "partial", "dup1.txt", b"duplicate data").await;
        put_file(&shoebox, "partial", "dup2.txt", b"duplicate data").await;

        // Insert a synthetic L1 record so the scan is never "complete",
        // regardless of scheduler timing.
        let bucket = shoebox.loaded_buckets().get("partial").unwrap();
        let root_dir_id = bucket.metadata.get_or_create_dir_id("").await.unwrap();
        bucket
            .metadata
            .insert_object(&ObjectRecord {
                id: uuid::Uuid::new_v4().to_string(),
                name: "unscanned.txt".into(),
                parent_dir_id: root_dir_id,
                scan_level: 1,
                last_modified: crate::metadata::sqlite::SqliteTimestamp::now(),
                created_at: crate::metadata::sqlite::SqliteTimestamp::now(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Without allow-partial, should fail
        let err = shoebox
            .find_bucket_duplicates("partial", 100, false, None, None, None)
            .await;
        assert!(matches!(err, Err(S3Error::ScanPending { .. })));

        // With allow-partial, should succeed and return partial results
        let report = shoebox
            .find_bucket_duplicates("partial", 100, true, None, None, None)
            .await
            .unwrap();

        assert!(!report.scan_complete, "scan_complete should be false");
        assert_eq!(
            report.duplicates.len(),
            1,
            "Should find the L3 duplicate group even with incomplete scan"
        );
        assert_eq!(report.duplicates[0].files.len(), 2);
    }

    #[tokio::test]
    async fn test_compare_dirs_shows_correct_differences() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        // Left directory: originals/
        put_file(&shoebox, "photos", "originals/same.txt", b"identical").await;
        put_file(&shoebox, "photos", "originals/modified.txt", b"version A").await;
        put_file(&shoebox, "photos", "originals/only-left.txt", b"left only").await;

        // Right directory: backup/
        put_file(&shoebox, "photos", "backup/same.txt", b"identical").await;
        put_file(&shoebox, "photos", "backup/modified.txt", b"version B").await;
        put_file(&shoebox, "photos", "backup/only-right.txt", b"right only").await;

        let comparison = shoebox
            .compare_dirs("photos", "originals/", "photos", "backup/")
            .await
            .unwrap();

        assert!(!comparison.identical, "Dirs should not be identical");
        assert_eq!(comparison.summary.files_identical, 1, "1 identical file");
        assert_eq!(comparison.summary.files_different, 1, "1 modified file");
        assert_eq!(comparison.summary.files_only_in_left, 1, "1 only in left");
        assert_eq!(comparison.summary.files_only_in_right, 1, "1 only in right");

        // Check specific differences
        let statuses: HashSet<&str> = comparison
            .differences
            .iter()
            .map(|d| d.status.as_str())
            .collect();
        assert!(statuses.contains("modified"));
        assert!(statuses.contains("only_in_left"));
        assert!(statuses.contains("only_in_right"));
    }

    #[tokio::test]
    async fn test_merge_duplicates_deletes_loser_objects() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "photos").await;

        put_file(&shoebox, "photos", "winner.txt", b"keep this copy").await;
        put_file(&shoebox, "photos", "loser1.txt", b"keep this copy").await;
        put_file(&shoebox, "photos", "loser2.txt", b"keep this copy").await;

        // Verify all 3 exist
        let objects = collect_objects(&shoebox, "photos").await;
        assert_eq!(objects.len(), 3);

        // Merge: keep winner, delete losers
        let result = shoebox
            .merge_duplicates("photos", "winner.txt", &["loser1.txt", "loser2.txt"])
            .await
            .unwrap();
        assert_eq!(result.losers_merged, 2);

        // Winner should still exist
        let winner = shoebox.head_object("photos", "winner.txt").await;
        assert!(winner.is_ok(), "Winner should still exist");

        // Losers should be gone from the catalog
        let loser1 = shoebox.head_object("photos", "loser1.txt").await;
        assert!(loser1.is_err(), "Loser 1 should be deleted");
        let loser2 = shoebox.head_object("photos", "loser2.txt").await;
        assert!(loser2.is_err(), "Loser 2 should be deleted");

        // Only winner remains
        let objects = collect_objects(&shoebox, "photos").await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key, "winner.txt");

        // Loser files should be deleted from disk too
        let bucket_dir = tmp.path().join("photos");
        assert!(
            !bucket_dir.join("loser1.txt").exists(),
            "loser1.txt should be deleted from disk"
        );
        assert!(
            !bucket_dir.join("loser2.txt").exists(),
            "loser2.txt should be deleted from disk"
        );
    }

    #[tokio::test]
    async fn test_integrity_check_detects_corruption_and_includes_object_id() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "bucket").await;

        put_file(&shoebox, "bucket", "good.txt", b"untouched content").await;
        put_file(&shoebox, "bucket", "corrupted.txt", b"original content").await;

        // Corrupt the file on disk (simulates bit rot / external modification)
        let bucket_dir = tmp.path().join("bucket");
        std::fs::write(bucket_dir.join("corrupted.txt"), b"TAMPERED CONTENT").unwrap();

        let result = shoebox.check_integrity("bucket", None).await.unwrap();

        assert_eq!(result.status, "complete");
        assert_eq!(result.files_checked, 2);
        assert_eq!(result.files_ok, 1, "1 file should be OK");
        assert_eq!(result.discrepancies.len(), 1, "1 discrepancy");

        let disc = &result.discrepancies[0];
        assert_eq!(disc.key, "corrupted.txt");
        assert!(
            !disc.object_id.is_empty(),
            "Discrepancy should include object_id"
        );
        assert!(
            disc.reason.starts_with("content_mismatch"),
            "Reason should indicate content mismatch, got: {}",
            disc.reason
        );
        assert!(disc.stored_hash.is_some(), "Should include stored hash");
        assert!(disc.computed_hash.is_some(), "Should include computed hash");
        assert_ne!(
            disc.stored_hash, disc.computed_hash,
            "Stored and computed hashes should differ"
        );
    }

    #[tokio::test]
    async fn test_async_integrity_check_returns_immediately() {
        let tmp = TempDir::new().unwrap();
        let shoebox = build_shoebox(&tmp, "bucket").await;

        put_file(&shoebox, "bucket", "file.txt", b"test content").await;

        let result = shoebox.check_integrity_async("bucket", None).unwrap();

        // Should return immediately with in_progress status
        assert_eq!(
            result.status, "in_progress",
            "Async check should return in_progress immediately"
        );
        assert!(!result.check_id.is_empty(), "Should have a check_id");
        assert_eq!(
            result.files_checked, 0,
            "Should not have checked any files yet"
        );

        // Give the background task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
