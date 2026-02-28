pub(crate) mod dispatch;
pub(crate) mod gate;
pub mod progress;

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::backpressure::{CompositePressure, ThrottlePolicy};
use crate::priority::Priority;
use crate::registry::{TaskExecutor, TaskTypeRegistry};
use crate::resource::sampler::{SamplerConfig, SmoothedReader};
use crate::resource::{ResourceReader, ResourceSampler};
use crate::store::{StoreConfig, StoreError, TaskStore};
use crate::task::{TaskSubmission, TypedTask};

use dispatch::ActiveTaskMap;
use gate::{DefaultDispatchGate, GateContext};

pub use progress::{EstimatedProgress, ProgressReporter};

// ── Events ──────────────────────────────────────────────────────────

/// Events emitted by the scheduler for UI integration and observability.
///
/// Subscribe via the `tokio::sync::broadcast::Receiver` returned by
/// [`Scheduler::subscribe`] or passed through the builder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SchedulerEvent {
    /// A task was dispatched and is now running.
    Dispatched {
        task_id: i64,
        task_type: String,
        key: String,
    },
    /// A task completed successfully.
    Completed {
        task_id: i64,
        task_type: String,
        key: String,
    },
    /// A task failed (may be retried or permanently failed).
    Failed {
        task_id: i64,
        task_type: String,
        key: String,
        error: String,
        will_retry: bool,
    },
    /// A task was preempted by higher-priority work.
    Preempted {
        task_id: i64,
        task_type: String,
        key: String,
    },
    /// A task was cancelled by the application.
    Cancelled {
        task_id: i64,
        task_type: String,
        key: String,
    },
    /// Progress update from a running task.
    Progress {
        task_id: i64,
        task_type: String,
        key: String,
        /// Progress percentage (0.0 to 1.0).
        percent: f32,
        /// Optional human-readable message from the executor.
        message: Option<String>,
    },
}

// ── Config ──────────────────────────────────────────────────────────

/// How the scheduler behaves during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownMode {
    /// Cancel all running tasks immediately (default).
    Hard,
    /// Stop accepting new dispatches, wait for running tasks to complete
    /// (up to the given timeout), then cancel any remaining.
    Graceful(Duration),
}

/// Scheduler configuration.
pub struct SchedulerConfig {
    /// Maximum concurrent running tasks. Adjusted dynamically via
    /// [`Scheduler::set_max_concurrency`].
    pub max_concurrency: usize,
    /// Maximum retries before permanent failure. Default: 3.
    pub max_retries: i32,
    /// Priority threshold: tasks at or above this priority (lower numeric value)
    /// trigger preemption of lower-priority running tasks.
    pub preempt_priority: Priority,
    /// Interval between scheduler polls when idle. Default: 500ms.
    pub poll_interval: Duration,
    /// How many recent tasks to consider for IO throughput estimation.
    pub throughput_sample_size: i32,
    /// Shutdown behavior. Default: Hard.
    pub shutdown_mode: ShutdownMode,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            max_retries: 3,
            preempt_priority: Priority::REALTIME,
            poll_interval: Duration::from_millis(500),
            throughput_sample_size: 20,
            shutdown_mode: ShutdownMode::Hard,
        }
    }
}

// ── Scheduler ───────────────────────────────────────────────────────

/// Shared inner state behind `Arc` so `Scheduler` can be `Clone`.
#[allow(dead_code)]
struct SchedulerInner {
    store: TaskStore,
    max_concurrency: AtomicUsize,
    max_retries: i32,
    preempt_priority: Priority,
    poll_interval: Duration,
    throughput_sample_size: i32,
    shutdown_mode: ShutdownMode,
    registry: Arc<TaskTypeRegistry>,
    gate: Box<dyn gate::DispatchGate>,
    resource_reader: Mutex<Option<Arc<dyn ResourceReader>>>,
    /// In-memory tracking of active tasks and their cancellation tokens.
    active: ActiveTaskMap,
    /// Broadcast channel for lifecycle events.
    event_tx: tokio::sync::broadcast::Sender<SchedulerEvent>,
    /// Token to cancel the background resource sampler (if started).
    sampler_token: CancellationToken,
}

/// IO-aware priority scheduler.
///
/// Coordinates task execution by:
/// 1. Popping highest-priority pending tasks from the SQLite store
/// 2. Checking IO budget against running task estimates and system capacity
/// 3. Applying backpressure throttling based on external pressure sources
/// 4. Preempting lower-priority tasks when high-priority work arrives
/// 5. Managing retries and failure recording
/// 6. Emitting lifecycle events for UI integration
///
/// `Scheduler` is `Clone` — each clone shares the same underlying state.
/// This makes it easy to hold in `tauri::State<Scheduler>` or share across
/// async tasks.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

impl Scheduler {
    pub fn new(
        store: TaskStore,
        config: SchedulerConfig,
        registry: Arc<TaskTypeRegistry>,
        pressure: CompositePressure,
        policy: ThrottlePolicy,
    ) -> Self {
        let gate = Box::new(DefaultDispatchGate::new(pressure, policy));
        Self::with_gate(store, config, registry, gate)
    }

    /// Create a scheduler with a custom dispatch gate.
    fn with_gate(
        store: TaskStore,
        config: SchedulerConfig,
        registry: Arc<TaskTypeRegistry>,
        gate: Box<dyn gate::DispatchGate>,
    ) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            inner: Arc::new(SchedulerInner {
                store,
                max_concurrency: AtomicUsize::new(config.max_concurrency),
                max_retries: config.max_retries,
                preempt_priority: config.preempt_priority,
                poll_interval: config.poll_interval,
                throughput_sample_size: config.throughput_sample_size,
                shutdown_mode: config.shutdown_mode,
                registry,
                gate,
                resource_reader: Mutex::new(None),
                active: ActiveTaskMap::new(),
                event_tx,
                sampler_token: CancellationToken::new(),
            }),
        }
    }

    /// Create a [`SchedulerBuilder`] for ergonomic construction.
    pub fn builder() -> SchedulerBuilder {
        SchedulerBuilder::new()
    }

    /// Subscribe to scheduler lifecycle events.
    ///
    /// Returns a broadcast receiver. Events are emitted on task dispatch,
    /// completion, failure, preemption, cancellation, and progress. Useful for
    /// bridging to a Tauri frontend or updating UI state.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SchedulerEvent> {
        self.inner.event_tx.subscribe()
    }

    /// Set the resource reader for IO-aware scheduling.
    pub async fn set_resource_reader(&self, reader: Arc<dyn ResourceReader>) {
        *self.inner.resource_reader.lock().await = Some(reader);
    }

    /// Get a reference to the underlying store for direct queries.
    pub fn store(&self) -> &TaskStore {
        &self.inner.store
    }

    /// Submit a task. Returns `Ok(Some(id))` if enqueued, `Ok(None)` if deduped.
    ///
    /// If the task's priority meets the preemption threshold, running tasks
    /// with lower priority are preempted (their cancellation tokens are cancelled
    /// and they are paused in the store).
    pub async fn submit(&self, sub: &TaskSubmission) -> Result<Option<i64>, StoreError> {
        let id = self.inner.store.submit(sub).await?;

        // Preempt if this is a high-priority task.
        if id.is_some() && sub.priority.value() <= self.inner.preempt_priority.value() {
            self.inner
                .active
                .preempt_below(sub.priority, &self.inner.store, &self.inner.event_tx)
                .await;
        }

        Ok(id)
    }

    /// Submit a [`TypedTask`], handling serialization automatically.
    ///
    /// Equivalent to converting the task into a [`TaskSubmission`] via `TryFrom`
    /// and calling [`submit`](Self::submit).
    pub async fn submit_typed<T: TypedTask>(&self, task: &T) -> Result<Option<i64>, StoreError> {
        let sub = TaskSubmission::from_typed(task)?;
        self.submit(&sub).await
    }

    /// Cancel a task by id.
    ///
    /// If the task is currently running, its cancellation token is triggered
    /// and it is removed from the active map. If it is pending or paused,
    /// it is deleted from the store. Returns `true` if the task was found
    /// and cancelled.
    pub async fn cancel(&self, task_id: i64) -> Result<bool, StoreError> {
        // Check if it's an active (running) task first.
        if let Some(at) = self.inner.active.remove(task_id).await {
            at.token.cancel();
            self.inner.store.delete(task_id).await?;
            let _ = self.inner.event_tx.send(SchedulerEvent::Cancelled {
                task_id,
                task_type: at.record.task_type.clone(),
                key: at.record.key.clone(),
            });
            return Ok(true);
        }

        // Not active — try to delete from the queue (pending/paused).
        let deleted = self.inner.store.delete(task_id).await?;
        Ok(deleted)
    }

    /// Try to pop and execute the next task.
    ///
    /// Returns `true` if a task was dispatched, `false` if no work was available
    /// (empty queue, concurrency limit, IO budget exhausted, or throttled).
    pub async fn try_dispatch(&self) -> Result<bool, StoreError> {
        // Check concurrency limit.
        let active_count = self.inner.active.count().await;
        let max = self.inner.max_concurrency.load(AtomicOrdering::Relaxed);
        if active_count >= max {
            return Ok(false);
        }

        // Pop atomically — no peek-then-pop race.
        let Some(task) = self.inner.store.pop_next().await? else {
            return Ok(false);
        };

        // Build gate context from current state.
        let reader_guard = self.inner.resource_reader.lock().await;
        let gate_ctx = GateContext {
            store: &self.inner.store,
            resource_reader: reader_guard.as_ref(),
        };

        // Admission check via the dispatch gate.
        if !self.inner.gate.admit(&task, &gate_ctx).await? {
            drop(reader_guard);
            self.inner.store.requeue(task.id).await?;
            return Ok(false);
        }
        drop(reader_guard);

        // Look up executor.
        let Some(executor) = self.inner.registry.get(&task.task_type) else {
            tracing::error!(
                task_type = task.task_type,
                "no executor registered — failing task"
            );
            self.inner
                .store
                .fail(
                    task.id,
                    &format!("no executor registered for type '{}'", task.task_type),
                    false,
                    0,
                    0,
                    0,
                )
                .await?;
            return Ok(true);
        };
        let executor = Arc::clone(executor);

        // Spawn the task — this inserts into the active map, builds the
        // context, emits Dispatched, and wires up completion handling.
        dispatch::spawn_task(
            task,
            executor,
            self.inner.store.clone(),
            self.inner.active.clone(),
            self.inner.event_tx.clone(),
            self.inner.max_retries,
        )
        .await;

        Ok(true)
    }

    /// Run the scheduler loop until the cancellation token is triggered.
    ///
    /// This is the main entry point. It continuously polls for work,
    /// dispatches tasks, and adjusts concurrency based on resource monitoring.
    pub async fn run(&self, token: CancellationToken) {
        tracing::info!(
            max_concurrency = self.inner.max_concurrency.load(AtomicOrdering::Relaxed),
            "taskmill scheduler started"
        );

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("taskmill scheduler shutting down");
                    self.shutdown().await;
                    break;
                }
                _ = tokio::time::sleep(self.inner.poll_interval) => {
                    // Resume paused tasks only if no active preemptors exist.
                    if let Ok(paused) = self.inner.store.paused_tasks().await {
                        for task in paused {
                            if !self.inner.active.has_preemptors_for(
                                task.priority,
                                self.inner.preempt_priority,
                            ).await {
                                let _ = self.inner.store.resume(task.id).await;
                            }
                        }
                    }

                    // Try to dispatch tasks until we can't.
                    loop {
                        match self.try_dispatch().await {
                            Ok(true) => continue,
                            Ok(false) => break,
                            Err(e) => {
                                tracing::error!(error = %e, "scheduler dispatch error");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Perform shutdown according to the configured `ShutdownMode`.
    async fn shutdown(&self) {
        // Stop the resource sampler.
        self.inner.sampler_token.cancel();

        match self.inner.shutdown_mode {
            ShutdownMode::Hard => {
                self.inner.active.cancel_all().await;
            }
            ShutdownMode::Graceful(timeout) => {
                tracing::info!(
                    timeout_ms = timeout.as_millis() as u64,
                    "graceful shutdown — waiting for running tasks"
                );

                let deadline = tokio::time::Instant::now() + timeout;
                loop {
                    let count = self.inner.active.count().await;
                    if count == 0 {
                        tracing::info!("all tasks completed during graceful shutdown");
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(
                            remaining = count,
                            "graceful shutdown timeout — cancelling remaining tasks"
                        );
                        self.inner.active.cancel_all().await;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Snapshot of currently active (in-memory) tasks.
    pub async fn active_tasks(&self) -> Vec<crate::task::TaskRecord> {
        self.inner.active.records().await
    }

    /// Get estimated progress for all running tasks.
    ///
    /// Combines executor-reported progress with throughput-based extrapolation
    /// using historical average duration for each task type.
    pub async fn estimated_progress(&self) -> Vec<EstimatedProgress> {
        let snapshots: Vec<_> = self.inner.active.progress_snapshots().await;
        let mut results = Vec::with_capacity(snapshots.len());
        for (record, reported, reported_at) in snapshots {
            results.push(
                progress::extrapolate(&record, reported, reported_at, &self.inner.store).await,
            );
        }
        results
    }

    /// Update max concurrency at runtime (e.g., from adaptive controller or
    /// in response to battery/thermal state).
    pub fn set_max_concurrency(&self, limit: usize) {
        self.inner
            .max_concurrency
            .store(limit, AtomicOrdering::Relaxed);
        tracing::info!(new_limit = limit, "concurrency limit updated");
    }

    /// Read current max concurrency setting.
    pub fn max_concurrency(&self) -> usize {
        self.inner.max_concurrency.load(AtomicOrdering::Relaxed)
    }
}

// ── Builder ─────────────────────────────────────────────────────────

/// Ergonomic builder for constructing a [`Scheduler`] with all its dependencies.
///
/// Hides the `Arc<Mutex<...>>` wiring and manages the resource sampler lifecycle.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use std::sync::Arc;
/// use taskmill::{Scheduler, Priority};
///
/// let scheduler = Scheduler::builder()
///     .store_path("tasks.db")
///     // .executor("scan", Arc::new(my_scan_executor))
///     .max_concurrency(8)
///     .with_resource_monitoring()
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct SchedulerBuilder {
    store_path: Option<String>,
    store_config: StoreConfig,
    store: Option<TaskStore>,
    executors: Vec<(String, Arc<dyn crate::registry::ErasedExecutor>)>,
    config: SchedulerConfig,
    pressure_sources: Vec<Box<dyn crate::backpressure::PressureSource + 'static>>,
    policy: Option<ThrottlePolicy>,
    enable_resource_monitoring: bool,
    custom_sampler: Option<Box<dyn ResourceSampler>>,
    sampler_config: SamplerConfig,
}

impl SchedulerBuilder {
    pub fn new() -> Self {
        Self {
            store_path: None,
            store_config: StoreConfig::default(),
            store: None,
            executors: Vec::new(),
            config: SchedulerConfig::default(),
            pressure_sources: Vec::new(),
            policy: None,
            enable_resource_monitoring: false,
            custom_sampler: None,
            sampler_config: SamplerConfig::default(),
        }
    }

    /// Set the SQLite database path. Either this or [`store`] must be called.
    pub fn store_path(mut self, path: &str) -> Self {
        self.store_path = Some(path.to_string());
        self
    }

    /// Configure the SQLite connection pool.
    pub fn store_config(mut self, config: StoreConfig) -> Self {
        self.store_config = config;
        self
    }

    /// Use a pre-opened [`TaskStore`] instead of opening one from a path.
    pub fn store(mut self, store: TaskStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Register a task executor for a named type.
    pub fn executor<E: TaskExecutor>(mut self, name: &str, executor: Arc<E>) -> Self {
        self.executors.push((
            name.to_string(),
            executor as Arc<dyn crate::registry::ErasedExecutor>,
        ));
        self
    }

    /// Register an executor using the task type name from a [`TypedTask`].
    ///
    /// Equivalent to `.executor(T::TASK_TYPE, executor)`.
    pub fn typed_executor<T: TypedTask, E: TaskExecutor>(self, executor: Arc<E>) -> Self {
        self.executor(T::TASK_TYPE, executor)
    }

    /// Set maximum concurrent tasks. Default: 4.
    pub fn max_concurrency(mut self, limit: usize) -> Self {
        self.config.max_concurrency = limit;
        self
    }

    /// Set maximum retries before permanent failure. Default: 3.
    pub fn max_retries(mut self, retries: i32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set the priority threshold for preemption. Default: REALTIME.
    pub fn preempt_priority(mut self, priority: Priority) -> Self {
        self.config.preempt_priority = priority;
        self
    }

    /// Set the poll interval. Default: 500ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    /// Set the shutdown mode. Default: Hard.
    pub fn shutdown_mode(mut self, mode: ShutdownMode) -> Self {
        self.config.shutdown_mode = mode;
        self
    }

    /// Add a backpressure source (used by the default gate).
    pub fn pressure_source(
        mut self,
        source: Box<dyn crate::backpressure::PressureSource + 'static>,
    ) -> Self {
        self.pressure_sources.push(source);
        self
    }

    /// Set a custom throttle policy (used by the default gate). Default: three-tier.
    pub fn throttle_policy(mut self, policy: ThrottlePolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Enable platform resource monitoring (CPU, disk IO) using `sysinfo`.
    ///
    /// This starts a background sampler task that feeds IO data to the
    /// scheduler for budget-based dispatch decisions. The sampler is
    /// automatically stopped when the scheduler shuts down.
    pub fn with_resource_monitoring(mut self) -> Self {
        self.enable_resource_monitoring = true;
        self
    }

    /// Provide a custom [`ResourceSampler`] instead of the default platform one.
    pub fn resource_sampler(mut self, sampler: Box<dyn ResourceSampler>) -> Self {
        self.custom_sampler = Some(sampler);
        self.enable_resource_monitoring = true;
        self
    }

    /// Configure the resource sampler loop.
    pub fn sampler_config(mut self, config: SamplerConfig) -> Self {
        self.sampler_config = config;
        self
    }

    /// Build the scheduler. Opens the database and wires all components.
    ///
    /// If resource monitoring is enabled, the sampler background loop is
    /// started and will be stopped automatically when the scheduler shuts
    /// down (via the token passed to [`Scheduler::run`]).
    pub async fn build(self) -> Result<Scheduler, StoreError> {
        // Open or use provided store.
        let store = if let Some(store) = self.store {
            store
        } else if let Some(path) = &self.store_path {
            TaskStore::open_with_config(path, self.store_config).await?
        } else {
            return Err(StoreError::Database(
                "SchedulerBuilder requires either store_path() or store()".into(),
            ));
        };

        // Build registry.
        let mut registry = TaskTypeRegistry::new();
        for (name, executor) in self.executors {
            if registry.get(&name).is_some() {
                panic!("task type '{name}' already registered");
            }
            registry.register_erased(&name, executor);
        }

        // Build gate from pressure sources + policy.
        let mut pressure = CompositePressure::new();
        for source in self.pressure_sources {
            pressure.add_source(source);
        }
        let policy = self
            .policy
            .unwrap_or_else(ThrottlePolicy::default_three_tier);
        let gate = Box::new(DefaultDispatchGate::new(pressure, policy));

        let scheduler = Scheduler::with_gate(store, self.config, Arc::new(registry), gate);

        // Set up resource monitoring.
        if self.enable_resource_monitoring {
            #[cfg(feature = "sysinfo-monitor")]
            let sampler: Box<dyn ResourceSampler> = self
                .custom_sampler
                .unwrap_or_else(|| crate::resource::platform_sampler());

            #[cfg(not(feature = "sysinfo-monitor"))]
            let sampler: Box<dyn ResourceSampler> = self
                .custom_sampler
                .expect("resource monitoring enabled but no custom sampler provided and sysinfo-monitor feature is disabled");

            let reader = SmoothedReader::new();
            scheduler
                .set_resource_reader(Arc::new(reader.clone()))
                .await;

            // Spawn sampler loop — it will stop when the scheduler's sampler_token is cancelled.
            let sampler_arc = Arc::new(tokio::sync::Mutex::new(sampler));
            let sampler_config = self.sampler_config;
            let sampler_token = scheduler.inner.sampler_token.clone();
            tokio::spawn(crate::resource::sampler::run_sampler(
                sampler_arc,
                reader,
                sampler_config,
                sampler_token,
            ));
        }

        Ok(scheduler)
    }
}

impl Default for SchedulerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{TaskContext, TaskExecutor};
    use crate::task::{TaskError, TaskResult};

    struct InstantExecutor;

    impl TaskExecutor for InstantExecutor {
        async fn execute<'a>(&'a self, _ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
            Ok(TaskResult {
                actual_read_bytes: 100,
                actual_write_bytes: 50,
            })
        }
    }

    struct SlowExecutor;

    impl TaskExecutor for SlowExecutor {
        async fn execute<'a>(&'a self, ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
            tokio::select! {
                _ = ctx.token.cancelled() => {
                    Err(TaskError {
                        message: "cancelled".into(),
                        retryable: false,
                        actual_read_bytes: 0,
                        actual_write_bytes: 0,
                    })
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    Ok(TaskResult {
                        actual_read_bytes: 100,
                        actual_write_bytes: 50,
                    })
                }
            }
        }
    }

    #[allow(dead_code)]
    struct FailingExecutor;

    impl TaskExecutor for FailingExecutor {
        async fn execute<'a>(&'a self, _ctx: &'a TaskContext) -> Result<TaskResult, TaskError> {
            Err(TaskError {
                message: "boom".into(),
                retryable: true,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            })
        }
    }

    async fn setup(executor: Arc<dyn crate::registry::ErasedExecutor>) -> Scheduler {
        let store = TaskStore::open_memory().await.unwrap();
        let mut registry = TaskTypeRegistry::new();
        registry.register_erased("test", executor);

        Scheduler::new(
            store,
            SchedulerConfig::default(),
            Arc::new(registry),
            CompositePressure::new(),
            ThrottlePolicy::default_three_tier(),
        )
    }

    fn arc_erased<E: TaskExecutor>(e: E) -> Arc<dyn crate::registry::ErasedExecutor> {
        Arc::new(e) as Arc<dyn crate::registry::ErasedExecutor>
    }

    #[tokio::test]
    async fn dispatch_executes_task() {
        let sched = setup(arc_erased(InstantExecutor)).await;

        sched
            .submit(&TaskSubmission {
                task_type: "test".into(),
                key: Some("k1".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap();

        let dispatched = sched.try_dispatch().await.unwrap();
        assert!(dispatched);

        // Give spawned task time to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Task should be completed and in history.
        let k1 = crate::task::generate_dedup_key("test", Some(b"k1"));
        assert!(sched.store().task_by_key(&k1).await.unwrap().is_none());
        let hist = sched.store().history_by_key(&k1).await.unwrap();
        assert_eq!(hist.len(), 1);
    }

    #[tokio::test]
    async fn dispatch_returns_false_when_empty() {
        let sched = setup(arc_erased(InstantExecutor)).await;
        let dispatched = sched.try_dispatch().await.unwrap();
        assert!(!dispatched);
    }

    #[tokio::test]
    async fn unregistered_type_fails_task() {
        let store = TaskStore::open_memory().await.unwrap();
        let registry = TaskTypeRegistry::new(); // empty — no executors

        let sched = Scheduler::new(
            store,
            SchedulerConfig::default(),
            Arc::new(registry),
            CompositePressure::new(),
            ThrottlePolicy::default_three_tier(),
        );

        sched
            .submit(&TaskSubmission {
                task_type: "unknown".into(),
                key: Some("k".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap();

        sched.try_dispatch().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let failed = sched.store().failed_tasks(10).await.unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[tokio::test]
    async fn dedup_via_scheduler() {
        let sched = setup(arc_erased(InstantExecutor)).await;

        let sub = TaskSubmission {
            task_type: "test".into(),
            key: Some("dup".into()),
            priority: Priority::NORMAL,
            payload: None,
            expected_read_bytes: 0,
            expected_write_bytes: 0,
        };

        let first = sched.submit(&sub).await.unwrap();
        let second = sched.submit(&sub).await.unwrap();
        assert!(first.is_some());
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn set_max_concurrency_works() {
        let sched = setup(arc_erased(InstantExecutor)).await;
        assert_eq!(sched.max_concurrency(), 4);
        sched.set_max_concurrency(8);
        assert_eq!(sched.max_concurrency(), 8);
    }

    #[tokio::test]
    async fn cancel_pending_task() {
        let sched = setup(arc_erased(InstantExecutor)).await;

        let id = sched
            .submit(&TaskSubmission {
                task_type: "test".into(),
                key: Some("cancel-me".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap()
            .unwrap();

        let cancelled = sched.cancel(id).await.unwrap();
        assert!(cancelled);

        // Task should be gone.
        let cancel_key = crate::task::generate_dedup_key("test", Some(b"cancel-me"));
        assert!(sched
            .store()
            .task_by_key(&cancel_key)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn cancel_running_task() {
        let sched = setup(arc_erased(SlowExecutor)).await;

        let id = sched
            .submit(&TaskSubmission {
                task_type: "test".into(),
                key: Some("cancel-running".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap()
            .unwrap();

        // Dispatch it so it's running.
        sched.try_dispatch().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cancelled = sched.cancel(id).await.unwrap();
        assert!(cancelled);
    }

    #[tokio::test]
    async fn event_emitted_on_complete() {
        let sched = setup(arc_erased(InstantExecutor)).await;
        let mut rx = sched.subscribe();

        sched
            .submit(&TaskSubmission {
                task_type: "test".into(),
                key: Some("evt".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap();

        sched.try_dispatch().await.unwrap();

        // Should get Dispatched event.
        let evt = rx.recv().await.unwrap();
        assert!(matches!(evt, SchedulerEvent::Dispatched { .. }));

        // Wait for completion.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let evt = rx.recv().await.unwrap();
        assert!(matches!(evt, SchedulerEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn scheduler_is_clone() {
        let sched = setup(arc_erased(InstantExecutor)).await;
        let sched2 = sched.clone();

        // Both should share the same store.
        sched
            .submit(&TaskSubmission {
                task_type: "test".into(),
                key: Some("shared".into()),
                priority: Priority::NORMAL,
                payload: None,
                expected_read_bytes: 0,
                expected_write_bytes: 0,
            })
            .await
            .unwrap();

        // The clone can see the task.
        let shared_key = crate::task::generate_dedup_key("test", Some(b"shared"));
        let task = sched2.store().task_by_key(&shared_key).await.unwrap();
        assert!(task.is_some());
    }

    #[tokio::test]
    async fn submit_typed_enqueues_task() {
        use serde::{Deserialize as De, Serialize as Ser};

        #[derive(Ser, De, Debug, PartialEq)]
        struct Thumb {
            path: String,
        }

        impl crate::task::TypedTask for Thumb {
            const TASK_TYPE: &'static str = "test";

            fn expected_read_bytes(&self) -> i64 {
                4096
            }

            fn expected_write_bytes(&self) -> i64 {
                512
            }
        }

        let sched = setup(arc_erased(InstantExecutor)).await;

        let task = Thumb {
            path: "/a.jpg".into(),
        };
        let id = sched.submit_typed(&task).await.unwrap();
        assert!(id.is_some());

        // Verify the stored record has correct metadata.
        let record = sched
            .store()
            .task_by_id(id.unwrap())
            .await
            .unwrap()
            .expect("task should exist");
        assert_eq!(record.task_type, "test");
        assert_eq!(record.expected_read_bytes, 4096);
        assert_eq!(record.expected_write_bytes, 512);

        // Payload round-trips.
        let recovered: Thumb = record.deserialize_payload().unwrap().unwrap();
        assert_eq!(recovered, task);
    }
}
