//! Notification service — webhook delivery with retry.
//!
//! Config operations (get/set webhooks) are free functions per Phase 2 pattern.
//! The `NotificationService` struct owns an HTTP client and delivery worker.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::error::S3Error;
use crate::metadata::MetadataStore;
#[cfg(test)]
use crate::types::notification::WebhookFilter;
use crate::types::notification::{S3Event, WebhookConfig};

// --- Config operations: free functions per Phase 2 pattern ---

pub async fn get_webhook_config(metadata: &MetadataStore) -> Result<Vec<WebhookConfig>, S3Error> {
    metadata.get_webhook_configs().await
}

pub async fn set_webhook_config(
    metadata: &MetadataStore,
    webhooks: Vec<WebhookConfig>,
) -> Result<(), S3Error> {
    metadata.set_webhook_configs(&webhooks).await
}

// --- NotificationService: stateful struct for webhook delivery ---

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

#[derive(Clone)]
pub struct NotificationService {
    metadata: Arc<MetadataStore>,
    http_client: HttpClient,
    delivery_queue: mpsc::Sender<DeliveryJob>,
}

impl NotificationService {
    /// Create service and delivery worker. Caller decides when to spawn the worker.
    pub fn new(
        metadata: Arc<MetadataStore>,
        shutdown: CancellationToken,
    ) -> (Self, impl std::future::Future<Output = ()>) {
        let (tx, rx) = mpsc::channel(1000);

        let http_client = Client::builder(TokioExecutor::new()).build_http();

        let service = Self {
            metadata,
            http_client,
            delivery_queue: tx,
        };

        let worker = {
            let svc = service.clone();
            async move { svc.delivery_worker(rx, shutdown).await }
        };

        (service, worker)
    }

    /// Subscribe to an EventBus and deliver webhooks for matching events.
    pub async fn listen(self, mut rx: broadcast::Receiver<S3Event>) {
        while let Ok(event) = rx.recv().await {
            self.notify(&event).await;
        }
    }

    /// Send notification for an event.
    pub async fn notify(&self, event: &S3Event) {
        let webhooks = match get_webhook_config(&self.metadata).await {
            Ok(w) => w,
            Err(_) => return,
        };

        for webhook in webhooks {
            if matches_webhook(&webhook, event) {
                self.delivery_queue
                    .send(DeliveryJob {
                        webhook_id: webhook.id.clone(),
                        url: webhook.url.clone(),
                        event: event.clone(),
                        attempt: 0,
                    })
                    .await
                    .ok();
            }
        }
    }

    async fn delivery_worker(
        &self,
        mut rx: mpsc::Receiver<DeliveryJob>,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    // Drain remaining jobs with best-effort delivery (no retries)
                    rx.close();
                    while let Some(job) = rx.recv().await {
                        self.deliver_once(&job).await;
                    }
                    tracing::info!("Delivery worker shut down, remaining jobs drained");
                    break;
                }
                job = rx.recv() => match job {
                    Some(j) => self.deliver_with_retry(j).await,
                    None => break, // Channel closed (EventBus dropped)
                }
            }
        }
    }

    /// Best-effort single delivery attempt (used during shutdown drain).
    async fn deliver_once(&self, job: &DeliveryJob) {
        let payload = serde_json::json!({
            "Records": [{ "eventName": &job.event.event_name, "s3": {
                "bucket": {"name": &job.event.bucket},
                "object": {"key": &job.event.object_key}
            }}]
        });
        let body = serde_json::to_vec(&payload).unwrap();
        let req = hyper::Request::builder()
            .method("POST")
            .uri(&job.url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.http_client.request(req)).await;
    }

    async fn deliver_with_retry(&self, mut job: DeliveryJob) {
        const MAX_ATTEMPTS: u32 = 3;
        const BACKOFF: [u64; 2] = [1, 5];

        loop {
            job.attempt += 1;

            let payload = serde_json::json!({
                "Records": [{
                    "eventVersion": "2.1",
                    "eventSource": "shoebox:s3",
                    "eventTime": job.event.event_time,
                    "eventName": job.event.event_name,
                    "s3": {
                        "bucket": {"name": job.event.bucket},
                        "object": {
                            "objectId": job.event.object_id,
                            "key": job.event.object_key,
                            "size": job.event.size,
                            "eTag": job.event.etag,
                            "sourceObjectId": job.event.source_object_id,
                        }
                    }
                }]
            });

            let body = serde_json::to_vec(&payload).unwrap();
            let req = hyper::Request::builder()
                .method("POST")
                .uri(&job.url)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap();

            let result =
                tokio::time::timeout(Duration::from_secs(30), self.http_client.request(req)).await;

            match result {
                Ok(Ok(response)) if response.status().is_success() => {
                    self.metadata
                        .log_delivery(&job.webhook_id, &job.event.object_key, "delivered", None)
                        .await
                        .ok();
                    return;
                }
                Ok(Ok(response)) => {
                    let error = format!("HTTP {}", response.status());
                    self.metadata
                        .log_delivery(
                            &job.webhook_id,
                            &job.event.object_key,
                            "failed",
                            Some(&error),
                        )
                        .await
                        .ok();
                }
                Ok(Err(e)) => {
                    self.metadata
                        .log_delivery(
                            &job.webhook_id,
                            &job.event.object_key,
                            "failed",
                            Some(&e.to_string()),
                        )
                        .await
                        .ok();
                }
                Err(_) => {
                    self.metadata
                        .log_delivery(
                            &job.webhook_id,
                            &job.event.object_key,
                            "failed",
                            Some("timeout"),
                        )
                        .await
                        .ok();
                }
            }

            if job.attempt >= MAX_ATTEMPTS {
                tracing::warn!(
                    webhook_id = %job.webhook_id,
                    key = %job.event.object_key,
                    "Webhook delivery failed after {} attempts",
                    job.attempt
                );
                return;
            }

            let delay = BACKOFF
                .get((job.attempt - 1) as usize)
                .copied()
                .unwrap_or(60);
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }
}

/// Pure function — check if a webhook matches an event.
fn matches_webhook(webhook: &WebhookConfig, event: &S3Event) -> bool {
    let event_matches = webhook.events.iter().any(|pattern| {
        if pattern.ends_with(":*") {
            let prefix = &pattern[..pattern.len() - 1];
            event.event_name.starts_with(prefix)
        } else {
            pattern == &event.event_name
        }
    });

    if !event_matches {
        return false;
    }

    if let Some(ref filter) = webhook.filter {
        if let Some(ref prefix) = filter.prefix {
            if !event.object_key.starts_with(prefix) {
                return false;
            }
        }
        if let Some(ref suffix) = filter.suffix {
            if !event.object_key.ends_with(suffix) {
                return false;
            }
        }
    }

    true
}

#[derive(Clone)]
struct DeliveryJob {
    webhook_id: String,
    url: String,
    event: S3Event,
    attempt: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(name: &str, key: &str) -> S3Event {
        S3Event {
            event_name: name.to_string(),
            event_time: "2025-01-01T00:00:00Z".to_string(),
            bucket: "test".to_string(),
            object_id: "test-id".to_string(),
            object_key: key.to_string(),
            size: Some(100),
            etag: Some("\"abc\"".to_string()),
            source_object_id: None,
        }
    }

    fn make_webhook(events: &[&str], filter: Option<WebhookFilter>) -> WebhookConfig {
        WebhookConfig {
            id: "test".to_string(),
            url: "http://example.com/webhook".to_string(),
            events: events.iter().map(|s| s.to_string()).collect(),
            filter,
        }
    }

    #[test]
    fn test_exact_event_match() {
        let webhook = make_webhook(&["s3:ObjectCreated:Put"], None);
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "test.txt")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectRemoved:Delete", "test.txt")
        ));
    }

    #[test]
    fn test_wildcard_event_match() {
        let webhook = make_webhook(&["s3:ObjectCreated:*"], None);
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "test.txt")
        ));
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Copy", "test.txt")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectRemoved:Delete", "test.txt")
        ));
    }

    #[test]
    fn test_prefix_filter() {
        let webhook = make_webhook(
            &["s3:ObjectCreated:*"],
            Some(WebhookFilter {
                prefix: Some("uploads/".to_string()),
                suffix: None,
            }),
        );
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "uploads/photo.jpg")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "docs/readme.md")
        ));
    }

    #[test]
    fn test_suffix_filter() {
        let webhook = make_webhook(
            &["s3:ObjectCreated:*"],
            Some(WebhookFilter {
                prefix: None,
                suffix: Some(".jpg".to_string()),
            }),
        );
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "photo.jpg")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "photo.png")
        ));
    }

    #[test]
    fn test_combined_filter() {
        let webhook = make_webhook(
            &["s3:ObjectCreated:*"],
            Some(WebhookFilter {
                prefix: Some("uploads/".to_string()),
                suffix: Some(".jpg".to_string()),
            }),
        );
        assert!(matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "uploads/photo.jpg")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "uploads/photo.png")
        ));
        assert!(!matches_webhook(
            &webhook,
            &make_event("s3:ObjectCreated:Put", "docs/photo.jpg")
        ));
    }
}
