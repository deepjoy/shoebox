use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::scheduler::ProgressReporter;
use crate::task::{TaskError, TaskRecord, TaskResult, TypedTask};

// ── State Map ────────────────────────────────────────────────────────

/// Type-keyed map of shared application state.
///
/// Multiple state types can be registered (one value per concrete type).
/// Executors retrieve them via [`TaskContext::state::<T>()`]. This is the
/// same pattern used by Axum `Extensions` and Tauri `State`.
///
/// The map supports post-build insertion via [`Scheduler::register_state`]
/// so that library consumers (e.g. shoebox inside a Tauri app) can inject
/// state after the scheduler has been constructed by the parent.
#[derive(Default)]
pub struct StateMap {
    inner: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl StateMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a `StateMap` from pre-collected entries.
    pub(crate) fn from_entries(entries: Vec<(TypeId, Arc<dyn Any + Send + Sync>)>) -> Self {
        Self {
            inner: RwLock::new(entries.into_iter().collect()),
        }
    }

    /// Insert a state value. Overwrites any previous value of the same type.
    pub async fn insert<T: Send + Sync + 'static>(&self, value: Arc<T>) {
        self.inner.write().await.insert(TypeId::of::<T>(), value);
    }
}

/// Snapshot of state for passing into a [`TaskContext`].
///
/// Created by cloning the inner map under the lock once, then used
/// lock-free for the lifetime of the task execution.
#[derive(Clone, Default)]
pub(crate) struct StateSnapshot {
    entries: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl StateSnapshot {
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.entries
            .get(&TypeId::of::<T>())
            .and_then(|arc| arc.downcast_ref::<T>())
    }
}

impl StateMap {
    /// Take a lock-free snapshot for use inside a task context.
    pub(crate) async fn snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            entries: self.inner.read().await.clone(),
        }
    }
}

// ── Task Context ─────────────────────────────────────────────────────

/// Execution context passed to a [`TaskExecutor`].
///
/// Bundles the task record, cancellation token, progress reporter, and
/// optional application state into a single value. This keeps the executor
/// signature stable when new contextual data is added in the future.
pub struct TaskContext {
    /// The full task record including payload, priority, and IO estimates.
    pub record: TaskRecord,
    /// Cancelled when the task is preempted. Check `token.is_cancelled()`
    /// at natural yield points and return early if set.
    pub token: CancellationToken,
    /// Report progress back to the scheduler (0.0–1.0).
    pub progress: ProgressReporter,
    /// Shared application state set via [`SchedulerBuilder::app_state`].
    pub(crate) app_state: StateSnapshot,
}

impl TaskContext {
    /// Deserialize the payload as a [`TypedTask`].
    ///
    /// Convenience wrapper around [`TaskRecord::deserialize_payload`] that
    /// mirrors the typed submission API.
    pub fn deserialize_typed<T: TypedTask>(&self) -> Result<Option<T>, serde_json::Error> {
        self.record.deserialize_payload()
    }

    /// Retrieve shared application state registered via
    /// [`SchedulerBuilder::app_state`] or [`Scheduler::register_state`].
    ///
    /// Returns `None` if the type was never registered. Multiple types can
    /// coexist — each is keyed by its concrete `TypeId`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// struct MyServices { db: DatabasePool, http: reqwest::Client }
    ///
    /// // In the executor:
    /// let svc = ctx.state::<MyServices>().expect("app state not set");
    /// svc.db.query("...").await?;
    /// ```
    pub fn state<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.app_state.get::<T>()
    }
}

/// Executes tasks of a registered type.
///
/// Each executor is associated with a named task type (e.g. `"scan-l3"`, `"exif"`).
/// When the scheduler pops a task, it looks up the executor by `task_type` and
/// calls `execute` with a [`TaskContext`] containing the persisted record,
/// a cancellation token, and a progress reporter.
///
/// Implementors deserialize the task's `payload` blob themselves — taskmill
/// treats it as opaque bytes.
///
/// # Example
///
/// ```ignore
/// use taskmill::{TaskExecutor, TaskContext, TaskResult, TaskError};
///
/// struct MyExecutor;
///
/// impl TaskExecutor for MyExecutor {
///     async fn execute<'a>(
///         &'a self,
///         ctx: &'a TaskContext,
///     ) -> Result<TaskResult, TaskError> {
///         ctx.progress.report(0.5, Some("halfway".into()));
///         Ok(TaskResult { actual_read_bytes: 0, actual_write_bytes: 0 })
///     }
/// }
/// ```
pub trait TaskExecutor: Send + Sync + 'static {
    /// Execute a task.
    ///
    /// - `ctx`: Execution context with the task record, cancellation token,
    ///   and progress reporter.
    ///
    /// On success, return actual IO bytes consumed. On failure, return a
    /// `TaskError` indicating whether retry is appropriate.
    fn execute<'a>(
        &'a self,
        ctx: &'a TaskContext,
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
        ctx: &'a TaskContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<TaskResult, TaskError>> + Send + 'a>>;
}

impl<T: TaskExecutor> ErasedExecutor for T {
    fn execute_erased<'a>(
        &'a self,
        ctx: &'a TaskContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<TaskResult, TaskError>> + Send + 'a>> {
        Box::pin(self.execute(ctx))
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
        async fn execute<'a>(&'a self, _ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
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
