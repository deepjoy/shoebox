use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::task::{TaskError, TaskRecord, TaskResult};

/// Executes tasks of a registered type.
///
/// Each executor is associated with a named task type (e.g. `"scan-l3"`, `"exif"`).
/// When the scheduler pops a task, it looks up the executor by `task_type` and
/// calls `execute` with the persisted record and a cancellation token.
///
/// Implementors deserialize the task's `payload` blob themselves — taskmill
/// treats it as opaque bytes.
///
/// # Example
///
/// ```ignore
/// use taskmill::{TaskExecutor, TaskRecord, TaskResult, TaskError};
/// use tokio_util::sync::CancellationToken;
///
/// struct MyExecutor;
///
/// impl TaskExecutor for MyExecutor {
///     async fn execute<'a>(
///         &'a self,
///         record: &'a TaskRecord,
///         token: CancellationToken,
///     ) -> Result<TaskResult, TaskError> {
///         Ok(TaskResult { actual_read_bytes: 0, actual_write_bytes: 0 })
///     }
/// }
/// ```
pub trait TaskExecutor: Send + Sync + 'static {
    /// Execute a task.
    ///
    /// - `record`: The full task record including payload, priority, and IO estimates.
    /// - `token`: Cancelled when the task is preempted. Check `token.is_cancelled()`
    ///   at natural yield points and return early if set.
    ///
    /// On success, return actual IO bytes consumed. On failure, return a
    /// `TaskError` indicating whether retry is appropriate.
    fn execute<'a>(
        &'a self,
        record: &'a TaskRecord,
        token: CancellationToken,
    ) -> impl Future<Output = Result<TaskResult, TaskError>> + Send + 'a;
}

/// Registry mapping task type names to their executors.
///
/// Built during application startup before the scheduler begins popping tasks.
/// After construction, the registry is immutable (shared via `Arc`).
pub struct TaskTypeRegistry {
    types: HashMap<String, Arc<dyn ErasedExecutor>>,
}

/// Object-safe wrapper around [`TaskExecutor`] for dynamic dispatch in the registry.
///
/// This trait exists because RPITIT (`impl Future`) in `TaskExecutor` is not
/// object-safe. The blanket impl below automatically wraps any `TaskExecutor`
/// so callers never interact with `ErasedExecutor` directly.
pub(crate) trait ErasedExecutor: Send + Sync + 'static {
    fn execute_erased<'a>(
        &'a self,
        record: &'a TaskRecord,
        token: CancellationToken,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<TaskResult, TaskError>> + Send + 'a>>;
}

impl<T: TaskExecutor> ErasedExecutor for T {
    fn execute_erased<'a>(
        &'a self,
        record: &'a TaskRecord,
        token: CancellationToken,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<TaskResult, TaskError>> + Send + 'a>> {
        Box::pin(self.execute(record, token))
    }
}

impl TaskTypeRegistry {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
        }
    }

    /// Register an executor for a named task type.
    ///
    /// Panics if the name is already registered (catch configuration errors
    /// at startup, not at runtime).
    pub fn register<E: TaskExecutor>(&mut self, name: &str, executor: Arc<E>) {
        if self.types.contains_key(name) {
            panic!("task type '{name}' already registered");
        }
        self.types
            .insert(name.to_string(), executor as Arc<dyn ErasedExecutor>);
    }

    /// Look up the executor for a task type.
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<dyn ErasedExecutor>> {
        self.types.get(name)
    }

    /// All registered type names.
    pub fn type_names(&self) -> Vec<&str> {
        self.types.keys().map(|s| s.as_str()).collect()
    }

    /// Number of registered types.
    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    /// Register a pre-erased executor. Used by the builder which already holds
    /// `Arc<dyn ErasedExecutor>`.
    pub(crate) fn register_erased(&mut self, name: &str, executor: Arc<dyn ErasedExecutor>) {
        if self.types.contains_key(name) {
            panic!("task type '{name}' already registered");
        }
        self.types.insert(name.to_string(), executor);
    }
}

impl Default for TaskTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopExecutor;

    impl TaskExecutor for NoopExecutor {
        async fn execute<'a>(
            &'a self,
            _record: &'a TaskRecord,
            _token: CancellationToken,
        ) -> Result<TaskResult, TaskError> {
            Ok(TaskResult {
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut reg = TaskTypeRegistry::new();
        reg.register("test-type", Arc::new(NoopExecutor));

        assert!(reg.get("test-type").is_some());
        assert!(reg.get("unknown").is_none());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn duplicate_registration_panics() {
        let mut reg = TaskTypeRegistry::new();
        reg.register("dup", Arc::new(NoopExecutor));
        reg.register("dup", Arc::new(NoopExecutor));
    }
}
