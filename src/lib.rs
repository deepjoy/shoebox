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
use crate::config::{resolve_bucket, BucketConfig, METADATA_DB};
use crate::error::S3Error;
use crate::metadata::sqlite::{ObjectRecord, Tag};
use crate::metadata::MetadataStore;
use crate::services::copy_service::{self, CopyConditions, CopyResult};
use crate::services::object_service::{self, GetObjectResult, PutObjectInput, PutObjectResult};
use crate::services::{tagging_service, AppState, LoadedBucket};
use crate::storage::filesystem::FilesystemStorage;

/// Per-bucket runtime state owned by Shoebox.
struct BucketRuntime {
    name: String,
    config: BucketConfig,
    storage: FilesystemStorage,
    metadata: MetadataStore,
    parts_dir: std::path::PathBuf,
}

/// Main Shoebox builder and runtime.
///
/// `Shoebox` is the Rust-native library API. Each public method maps to an
/// S3 operation and can be called directly without starting an HTTP server.
/// When HTTP serving is needed, `router()` or `run()` build an internal
/// `AppState` and hand it to the Axum router.
pub struct Shoebox {
    buckets: HashMap<String, BucketRuntime>,
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

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: i32,
        start_after: Option<&str>,
    ) -> Result<(Vec<ObjectRecord>, Vec<String>, bool, Option<String>), S3Error> {
        let b = self.get_bucket(bucket)?;
        b.metadata
            .list_objects_v2(prefix, delimiter, max_keys, start_after)
            .await
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

    // -- HTTP layer bridge --

    // -- Helper methods for HTTP layer --

    fn to_app_state(&self) -> AppState {
        AppState {
            buckets: Arc::new(
                self.buckets
                    .iter()
                    .map(|(name, b)| {
                        (
                            name.clone(),
                            LoadedBucket {
                                name: b.name.clone(),
                                config: b.config.clone(),
                                storage: b.storage.clone(),
                                metadata: b.metadata.clone(),
                                parts_dir: b.parts_dir.clone(),
                            },
                        )
                    })
                    .collect(),
            ),
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
        let listener = tokio::net::TcpListener::bind(&addr).await?;
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

    fn get_bucket(&self, name: &str) -> Result<&BucketRuntime, S3Error> {
        self.buckets.get(name).ok_or(S3Error::NoSuchBucket)
    }

    fn endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

pub struct ShoeboxBuilder {
    paths: Vec<PathBuf>,
    host: String,
    port: u16,
    data_dir: Option<PathBuf>,
}

#[cfg(test)]
impl Shoebox {
    /// Expose bucket names for testing.
    fn bucket_names(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }
}

impl Default for ShoeboxBuilder {
    fn default() -> Self {
        Self {
            paths: vec![],
            host: "0.0.0.0".into(),
            port: 9000,
            data_dir: None,
        }
    }
}

impl ShoeboxBuilder {
    pub fn bucket(mut self, path: impl AsRef<Path>) -> Self {
        self.paths.push(path.as_ref().to_path_buf());
        self
    }
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn data_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.data_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    pub async fn build(self) -> Result<Shoebox, Box<dyn std::error::Error>> {
        let mut buckets = HashMap::new();
        for path in &self.paths {
            let state = resolve_bucket(path, self.data_dir.as_deref()).await?;
            let db_path = state.shoebox_dir.join(METADATA_DB);
            let metadata = MetadataStore::new(&db_path).await?;
            let storage = FilesystemStorage::new(state.root.clone());
            let parts_dir = state.shoebox_dir.join("parts");
            tokio::fs::create_dir_all(&parts_dir).await?;
            buckets.insert(
                state.name.clone(),
                BucketRuntime {
                    name: state.name,
                    config: state.config,
                    storage,
                    metadata,
                    parts_dir,
                },
            );
        }

        let credential_provider =
            Arc::new(tokio::sync::RwLock::new(CredentialProvider::from_buckets(
                &buckets
                    .values()
                    .map(|b| (b.name.clone(), &b.config))
                    .collect::<Vec<_>>(),
            )));

        Ok(Shoebox {
            buckets,
            credential_provider,
            host: self.host,
            port: self.port,
            shutdown_token: CancellationToken::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use services::object_service::PutObjectInput;
    use tempfile::TempDir;

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

        let (objects, _prefixes, _is_truncated, _next_token) = shoebox
            .list_objects("test-bucket", "", None, 100, None)
            .await
            .unwrap();
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
}
