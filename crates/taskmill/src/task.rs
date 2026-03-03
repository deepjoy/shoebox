use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::priority::Priority;

/// Maximum payload size in bytes (1 MiB).
pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;

/// Lifecycle state of a task in the active queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Paused,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            other => Err(format!("unknown TaskStatus: {other}")),
        }
    }
}

/// Terminal state of a task in history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    Completed,
    Failed,
}

impl HistoryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::str::FromStr for HistoryStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown HistoryStatus: {other}")),
        }
    }
}

/// A task in the active queue (pending, running, or paused).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub task_type: String,
    pub key: String,
    pub priority: Priority,
    pub status: TaskStatus,
    pub payload: Option<Vec<u8>>,
    pub expected_read_bytes: i64,
    pub expected_write_bytes: i64,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub requeue: bool,
    pub requeue_priority: Option<Priority>,
}

impl TaskRecord {
    /// Deserialize the payload blob into a typed value.
    ///
    /// Returns `None` if the payload is absent, or an error if deserialization fails.
    pub fn deserialize_payload<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<Option<T>, serde_json::Error> {
        match &self.payload {
            Some(bytes) => serde_json::from_slice(bytes).map(Some),
            None => Ok(None),
        }
    }
}

/// A task that has completed or permanently failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHistoryRecord {
    pub id: i64,
    pub task_type: String,
    pub key: String,
    pub priority: Priority,
    pub status: HistoryStatus,
    pub payload: Option<Vec<u8>>,
    pub expected_read_bytes: i64,
    pub expected_write_bytes: i64,
    pub actual_read_bytes: Option<i64>,
    pub actual_write_bytes: Option<i64>,
    pub retry_count: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
}

/// Reported by the executor on successful completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub actual_read_bytes: i64,
    pub actual_write_bytes: i64,
}

/// Reported by the executor on failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskError {
    pub message: String,
    pub retryable: bool,
    pub actual_read_bytes: i64,
    pub actual_write_bytes: i64,
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TaskError {}

/// Result of a task submission attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Task was inserted as new.
    Inserted(i64),
    /// Duplicate key existed; its priority was upgraded (pending/paused tasks only).
    Upgraded(i64),
    /// Duplicate key existed and is running/paused; marked for re-queue after completion.
    Requeued(i64),
    /// Duplicate key existed; no changes were made.
    Duplicate,
}

impl SubmitOutcome {
    /// Returns the task ID if the task was inserted, upgraded, or requeued.
    pub fn id(&self) -> Option<i64> {
        match self {
            Self::Inserted(id) | Self::Upgraded(id) | Self::Requeued(id) => Some(*id),
            Self::Duplicate => None,
        }
    }

    /// Returns `true` if a new task was inserted.
    pub fn is_inserted(&self) -> bool {
        matches!(self, Self::Inserted(_))
    }
}

/// Generate a dedup key by hashing the task type and payload.
///
/// Produces a hex-encoded SHA-256 digest of `task_type` concatenated with
/// the payload bytes (or an empty slice when there is no payload).
pub fn generate_dedup_key(task_type: &str, payload: Option<&[u8]>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(task_type.as_bytes());
    hasher.update(b":");
    if let Some(p) = payload {
        hasher.update(p);
    }
    format!("{:x}", hasher.finalize())
}

/// Parameters for submitting a new task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSubmission {
    pub task_type: String,
    /// Optional dedup key. When `None`, the key is auto-generated by hashing
    /// `task_type` and `payload`, so two submissions with the same type and
    /// payload are deduplicated automatically.
    pub key: Option<String>,
    pub priority: Priority,
    pub payload: Option<Vec<u8>>,
    pub expected_read_bytes: i64,
    pub expected_write_bytes: i64,
}

impl TaskSubmission {
    /// Resolve the effective dedup key. Always incorporates the task type
    /// so different task types never collide, even with the same logical key.
    ///
    /// - Explicit key: `hash(task_type + ":" + key)`
    /// - No key: `hash(task_type + ":" + payload)`
    pub fn effective_key(&self) -> String {
        match &self.key {
            Some(k) => generate_dedup_key(&self.task_type, Some(k.as_bytes())),
            None => generate_dedup_key(&self.task_type, self.payload.as_deref()),
        }
    }

    /// Create a submission with a typed payload serialized to JSON bytes.
    ///
    /// The dedup key is auto-generated from the task type and serialized payload.
    /// Use `TaskRecord::deserialize_payload()` on the executor side to recover the type.
    pub fn with_payload<T: serde::Serialize>(
        task_type: &str,
        priority: Priority,
        data: &T,
        expected_read_bytes: i64,
        expected_write_bytes: i64,
    ) -> Result<Self, serde_json::Error> {
        let payload = serde_json::to_vec(data)?;
        Ok(Self {
            task_type: task_type.to_string(),
            key: None,
            priority,
            payload: Some(payload),
            expected_read_bytes,
            expected_write_bytes,
        })
    }
}

/// A strongly-typed task that bundles serialization, task type name, and default
/// IO estimates.
///
/// Implementing this trait collapses the 6 fields of [`TaskSubmission`] into a
/// derive-friendly pattern. Use [`Scheduler::submit_typed`] to submit and
/// [`TaskContext::deserialize_typed`] on the executor side.
///
/// # Example
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use taskmill::{TypedTask, Priority};
///
/// #[derive(Serialize, Deserialize)]
/// struct Thumbnail { path: String, size: u32 }
///
/// impl TypedTask for Thumbnail {
///     const TASK_TYPE: &'static str = "thumbnail";
///     fn expected_read_bytes(&self) -> i64 { 4096 }
///     fn expected_write_bytes(&self) -> i64 { 1024 }
/// }
/// ```
pub trait TypedTask: Serialize + DeserializeOwned + Send + 'static {
    /// Unique name used to register and look up the executor.
    const TASK_TYPE: &'static str;

    /// Estimated bytes this task will read. Default: 0.
    fn expected_read_bytes(&self) -> i64 {
        0
    }

    /// Estimated bytes this task will write. Default: 0.
    fn expected_write_bytes(&self) -> i64 {
        0
    }

    /// Scheduling priority. Default: [`Priority::NORMAL`].
    fn priority(&self) -> Priority {
        Priority::NORMAL
    }
}

impl TaskSubmission {
    /// Create a submission from a [`TypedTask`], serializing the payload and
    /// pulling task type, priority, and IO estimates from the trait.
    pub fn from_typed<T: TypedTask>(task: &T) -> Result<Self, serde_json::Error> {
        let payload = serde_json::to_vec(task)?;
        Ok(Self {
            task_type: T::TASK_TYPE.to_string(),
            key: None,
            priority: task.priority(),
            payload: Some(payload),
            expected_read_bytes: task.expected_read_bytes(),
            expected_write_bytes: task.expected_write_bytes(),
        })
    }
}

/// Unified lookup result for querying a task by its dedup inputs.
///
/// Returned by [`TaskStore::task_lookup`] and [`Scheduler::task_lookup`].
/// Tells the caller whether a task is currently active (pending, running,
/// or paused) or has finished (completed or failed), without requiring
/// them to manually compute the dedup key or query two tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "location", content = "record")]
pub enum TaskLookup {
    /// Task is in the active queue (pending, running, or paused).
    Active(TaskRecord),
    /// Task has finished and is in the history table.
    /// Contains the most recent history entry for that key.
    History(TaskHistoryRecord),
    /// No task with this key exists in either table.
    NotFound,
}

/// Aggregate statistics for a task type from history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TypeStats {
    pub count: i64,
    pub avg_duration_ms: f64,
    pub avg_read_bytes: f64,
    pub avg_write_bytes: f64,
    pub failure_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Thumbnail {
        path: String,
        size: u32,
    }

    impl TypedTask for Thumbnail {
        const TASK_TYPE: &'static str = "thumbnail";

        fn expected_read_bytes(&self) -> i64 {
            4096
        }

        fn expected_write_bytes(&self) -> i64 {
            1024
        }
    }

    #[test]
    fn typed_task_to_submission() {
        let task = Thumbnail {
            path: "/photos/a.jpg".into(),
            size: 256,
        };
        let sub = TaskSubmission::from_typed(&task).unwrap();

        assert_eq!(sub.task_type, "thumbnail");
        assert_eq!(sub.priority, Priority::NORMAL);
        assert_eq!(sub.expected_read_bytes, 4096);
        assert_eq!(sub.expected_write_bytes, 1024);
        assert!(sub.key.is_none());

        // Payload round-trips correctly.
        let recovered: Thumbnail = serde_json::from_slice(sub.payload.as_ref().unwrap()).unwrap();
        assert_eq!(recovered, task);
    }

    #[test]
    fn typed_task_custom_priority() {
        #[derive(Serialize, Deserialize)]
        struct Urgent {
            id: u64,
        }

        impl TypedTask for Urgent {
            const TASK_TYPE: &'static str = "urgent";

            fn priority(&self) -> Priority {
                Priority::HIGH
            }
        }

        let sub = TaskSubmission::from_typed(&Urgent { id: 42 }).unwrap();
        assert_eq!(sub.priority, Priority::HIGH);
        assert_eq!(sub.task_type, "urgent");
    }

    #[test]
    fn typed_task_defaults() {
        #[derive(Serialize, Deserialize)]
        struct Minimal;

        impl TypedTask for Minimal {
            const TASK_TYPE: &'static str = "minimal";
        }

        let sub = TaskSubmission::from_typed(&Minimal).unwrap();
        assert_eq!(sub.expected_read_bytes, 0);
        assert_eq!(sub.expected_write_bytes, 0);
        assert_eq!(sub.priority, Priority::NORMAL);
    }
}
