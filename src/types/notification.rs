use serde::{Deserialize, Serialize};

/// Configuration for a single webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub id: String,
    pub url: String,
    /// Event patterns: `s3:ObjectCreated:*`, `s3:ObjectRemoved:Delete`, etc.
    pub events: Vec<String>,
    pub filter: Option<WebhookFilter>,
}

/// Key prefix/suffix filter for webhook events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookFilter {
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

/// An S3-compatible event emitted by the EventBus.
#[derive(Debug, Clone)]
pub struct S3Event {
    pub event_name: String,
    pub event_time: String,
    pub bucket: String,
    /// Stable UUID from `objects.id`.
    pub object_id: String,
    pub object_key: String,
    pub size: Option<i64>,
    pub etag: Option<String>,
    /// For derived events: the source object.
    pub source_object_id: Option<String>,
}

/// Per-bucket event bus backed by a `broadcast` channel.
///
/// Shutdown is implicit: dropping the EventBus drops the `broadcast::Sender`,
/// causing all subscribers to receive `RecvError::Closed` and exit.
pub struct EventBus {
    sender: tokio::sync::broadcast::Sender<S3Event>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self { sender }
    }

    /// Emit an event. Silently ignores send errors (no active subscribers).
    pub fn emit(&self, event: S3Event) {
        let _ = self.sender.send(event);
    }

    /// Subscribe to receive events from this bus.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<S3Event> {
        self.sender.subscribe()
    }
}
