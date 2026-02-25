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
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::api::routes::create_router;
use crate::auth::presigned;
use crate::auth::provider::CredentialProvider;
use crate::config::{load_global_config, resolve_bucket, GlobalConfig, METADATA_DB};
use crate::error::S3Error;
use crate::metadata::sqlite::{ListEntry, ObjectRecord, Tag};
use crate::metadata::MetadataStore;

use crate::scanner::backpressure::ScannerResources;
use crate::scanner::levels;
use crate::scanner::scheduler::{Priority, ScanJob, ScanLevel, ScanScheduler};
use crate::scanner::scope::ScanScope;
use crate::scanner::watcher::FilesystemWatcher;
use crate::scanner::worker;
use crate::services::copy_service::{self, CopyConditions, CopyResult};
use crate::services::object_service::{self, GetObjectResult, PutObjectInput, PutObjectResult};
use crate::services::{tagging_service, AppState, LoadedBucket};
use crate::storage::filesystem::FilesystemStorage;
use std::pin::Pin;

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
    pub async fn serve(path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
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
        object_service::put_object(&b.storage, &b.metadata, key, stream, input).await
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        object_service::delete_object(&b.storage, &b.metadata, key).await
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
        copy_service::copy_object(
            &src.storage,
            &src.metadata,
            src_key,
            &dst.storage,
            &dst.metadata,
            dst_key,
            conditions,
        )
        .await
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

    // -- Scanner methods (Phase 6) --

    /// Trigger a scan at the given level for a bucket.
    pub async fn scan(&self, bucket: &str, level: ScanLevel) -> Result<(), S3Error> {
        let b = self.get_bucket(bucket)?;
        let job = ScanJob::new(Priority::Reconcile, ScanScope::Bucket, level);
        let mut sched = b.scheduler.lock().await;
        sched.schedule(job);
        Ok(())
    }

    /// Run a blocking L1 scan and return the report.
    pub async fn scan_l1(&self, bucket: &str) -> Result<scanner::L1Report, S3Error> {
        let b = self.get_bucket(bucket)?;
        levels::scan_l1(&b.metadata, b.storage.root(), &ScanScope::Bucket).await
    }

    // -- HTTP layer bridge --

    // -- Helper methods for HTTP layer --

    fn to_app_state(&self) -> AppState {
        AppState {
            buckets: self.buckets.clone(),
            credential_provider: self.credential_provider.clone(),
            bucket_names: Arc::new(self.buckets.keys().cloned().collect()),
        }
    }

    /// Create an Axum router for embedding in a custom server.
    pub fn router(&self) -> Router {
        create_router(self.to_app_state())
    }

    /// Run the built-in HTTP server with graceful shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
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

        let app_state = self.to_app_state();
        let router = create_router(app_state);
        let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                format!(
                    "Port {} is already in use. Is another Shoebox instance running?\n\
                     Try a different port with .port(<PORT>)",
                    self.port
                )
            } else {
                format!("Failed to bind to {}: {}", addr, e)
            }
        })?;
        tracing::info!("Serving on http://{addr}");

        axum::serve(listener, router)
            .with_graceful_shutdown(self.shutdown_token.cancelled_owned())
            .await?;

        // After server stops, close all SQLite pools to flush WAL
        for bucket in self.buckets.values() {
            bucket.metadata.close().await;
        }
        tracing::info!("Shutdown complete");
        Ok(())
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

    pub async fn build(self) -> Result<Shoebox, Box<dyn std::error::Error>> {
        // Resolve global config: explicit object takes priority over file
        let global_config = match (self.global_config, self.config_file) {
            (Some(gc), _) => Some(gc),
            (None, Some(path)) => Some(load_global_config(&path).await?),
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
        let resources = Arc::new(ScannerResources::new(100));
        let mut buckets = HashMap::new();

        for path in &paths {
            let state = resolve_bucket(path, self.data_dir.as_deref())
                .await
                .map_err(|e| match e.downcast_ref::<std::io::Error>() {
                    Some(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                        format!(
                            "{}: permission denied; use --data-dir to store state elsewhere",
                            path.display()
                        )
                    }
                    _ => e.to_string(),
                })?;
            let db_path = state.shoebox_dir.join(METADATA_DB);
            let metadata = MetadataStore::new(&db_path).await?;
            let storage = FilesystemStorage::new(state.root.clone());
            let parts_dir = state.shoebox_dir.join("parts");
            tokio::fs::create_dir_all(&parts_dir).await?;

            // Phase 6: Blocking L1 scan — discover files before serving
            let l1_report = levels::scan_l1(&metadata, &state.root, &ScanScope::Bucket).await?;
            if l1_report.discovered > 0 || l1_report.deleted > 0 {
                tracing::info!(
                    bucket = %state.name,
                    discovered = l1_report.discovered,
                    deleted = l1_report.deleted,
                    unchanged = l1_report.unchanged,
                    "L1 scan complete"
                );
            }

            let scheduler = Arc::new(Mutex::new(ScanScheduler::new()));

            // Schedule background L2+L3 scan
            {
                let mut sched = scheduler.lock().await;
                sched.schedule(ScanJob::new(
                    Priority::Reconcile,
                    ScanScope::Bucket,
                    ScanLevel::Content,
                ));
            }

            // Start filesystem watcher (uses spawn_blocking to avoid blocking the runtime
            // during recursive inotify watch setup on large directory trees)
            let watcher = {
                let (watch_tx, watch_rx) = tokio::sync::mpsc::channel(1000);
                match FilesystemWatcher::spawn(state.root.clone(), watch_tx).await {
                    Ok(w) => {
                        tracing::debug!(bucket = %state.name, "Filesystem watcher started");
                        // Spawn watch processor
                        tokio::spawn(worker::run_watch_processor(
                            metadata.clone(),
                            state.root.clone(),
                            watch_rx,
                            scheduler.clone(),
                            shutdown_token.clone(),
                        ));
                        Some(w)
                    }
                    Err(e) => {
                        tracing::warn!(
                            bucket = %state.name,
                            error = %e,
                            "Failed to start filesystem watcher"
                        );
                        None
                    }
                }
            };

            // Spawn scan worker for this bucket
            tokio::spawn(worker::run_scan_workers(
                metadata.clone(),
                state.root.clone(),
                scheduler.clone(),
                resources.clone(),
                shutdown_token.clone(),
            ));

            buckets.insert(
                state.name.clone(),
                LoadedBucket {
                    name: state.name,
                    config: state.config,
                    storage,
                    metadata,
                    parts_dir,
                    watcher,
                    scheduler,
                },
            );
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

        // Re-scan should find the new file
        let report = shoebox.scan_l1("scantest").await.unwrap();
        assert_eq!(report.discovered, 1);
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
}
