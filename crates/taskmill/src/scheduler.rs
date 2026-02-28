use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::backpressure::{CompositePressure, ThrottlePolicy};
use crate::priority::Priority;
use crate::registry::{TaskContext, TaskExecutor, TaskTypeRegistry};
use crate::resource::sampler::{SamplerConfig, SmoothedReader};
use crate::resource::{ResourceReader, ResourceSampler};
use crate::store::{StoreConfig, StoreError, TaskStore};
use crate::task::{TaskRecord, TaskSubmission};

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

// ── Progress ────────────────────────────────────────────────────────

/// Handle passed to executors for reporting progress back to the scheduler.
///
/// Progress reports are emitted as `SchedulerEvent::Progress` events,
/// making them available to the UI via the same broadcast channel.
#[derive(Clone)]
pub struct ProgressReporter {
    task_id: i64,
    task_type: String,
    key: String,
    event_tx: tokio::sync::broadcast::Sender<SchedulerEvent>,
}

impl ProgressReporter {
    /// Report progress as a percentage (0.0 to 1.0) with an optional message.
    pub fn report(&self, percent: f32, message: Option<String>) {
        let _ = self.event_tx.send(SchedulerEvent::Progress {
            task_id: self.task_id,
            task_type: self.task_type.clone(),
            key: self.key.clone(),
            percent: percent.clamp(0.0, 1.0),
            message,
        });
    }

    /// Report progress as a fraction (completed / total) with an optional message.
    pub fn report_fraction(&self, completed: u64, total: u64, message: Option<String>) {
        let percent = if total == 0 {
            1.0
        } else {
            completed as f32 / total as f32
        };
        self.report(percent, message);
    }
}

// ── Estimated Progress ──────────────────────────────────────────────

/// Estimated progress for a running task, combining executor-reported progress
/// with throughput-based extrapolation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstimatedProgress {
    pub task_id: i64,
    pub task_type: String,
    pub key: String,
    /// Executor-reported progress (0.0 to 1.0), if available.
    pub reported_percent: Option<f32>,
    /// Throughput-extrapolated progress (0.0 to 1.0), if history data exists.
    pub extrapolated_percent: Option<f32>,
    /// Best available progress estimate.
    pub percent: f32,
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

/// Handle to a running task for preemption tracking.
struct ActiveTask {
    record: TaskRecord,
    token: CancellationToken,
    /// Last reported progress from the executor (0.0 to 1.0).
    reported_progress: Option<f32>,
}

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
    pressure: Mutex<CompositePressure>,
    policy: ThrottlePolicy,
    resource_reader: Mutex<Option<Arc<dyn ResourceReader>>>,
    /// In-memory tracking of active tasks and their cancellation tokens.
    active: Mutex<HashMap<i64, ActiveTask>>,
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
                pressure: Mutex::new(pressure),
                policy,
                resource_reader: Mutex::new(None),
                active: Mutex::new(HashMap::new()),
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
            self.preempt_lower_priority(sub.priority).await;
        }

        Ok(id)
    }

    /// Cancel a task by id.
    ///
    /// If the task is currently running, its cancellation token is triggered
    /// and it is removed from the active map. If it is pending or paused,
    /// it is deleted from the store. Returns `true` if the task was found
    /// and cancelled.
    pub async fn cancel(&self, task_id: i64) -> Result<bool, StoreError> {
        // Check if it's an active (running) task first.
        let mut active = self.inner.active.lock().await;
        if let Some(at) = active.remove(&task_id) {
            at.token.cancel();
            // Remove from store (it was running).
            self.inner.store.delete(task_id).await?;
            let _ = self.inner.event_tx.send(SchedulerEvent::Cancelled {
                task_id,
                task_type: at.record.task_type.clone(),
                key: at.record.key.clone(),
            });
            return Ok(true);
        }
        drop(active);

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
        let active_count = self.inner.active.lock().await.len();
        let max = self.inner.max_concurrency.load(AtomicOrdering::Relaxed);
        if active_count >= max {
            return Ok(false);
        }

        // Pop atomically — no peek-then-pop race.
        let Some(task) = self.inner.store.pop_next().await? else {
            return Ok(false);
        };

        // Backpressure check: if throttled, requeue the task.
        let current_pressure = self.inner.pressure.lock().await.pressure();
        if self
            .inner
            .policy
            .should_throttle(task.priority, current_pressure)
        {
            tracing::trace!(
                priority = task.priority.value(),
                pressure = current_pressure,
                "task throttled by backpressure — requeuing"
            );
            self.inner.store.requeue(task.id).await?;
            return Ok(false);
        }

        // IO budget check: don't saturate disk.
        if !self.has_io_headroom(&task).await? {
            tracing::trace!(
                task_type = task.task_type,
                expected_read = task.expected_read_bytes,
                expected_write = task.expected_write_bytes,
                "task deferred — IO budget exhausted — requeuing"
            );
            self.inner.store.requeue(task.id).await?;
            return Ok(false);
        }

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

        // Create cancellation token for preemption.
        let child_token = CancellationToken::new();
        self.inner.active.lock().await.insert(
            task.id,
            ActiveTask {
                record: task.clone(),
                token: child_token.clone(),
                reported_progress: None,
            },
        );

        // Build the execution context for the executor.
        let ctx = TaskContext {
            record: task.clone(),
            token: child_token.clone(),
            progress: ProgressReporter {
                task_id: task.id,
                task_type: task.task_type.clone(),
                key: task.key.clone(),
                event_tx: self.inner.event_tx.clone(),
            },
        };

        // Emit dispatched event.
        let _ = self.inner.event_tx.send(SchedulerEvent::Dispatched {
            task_id: task.id,
            task_type: task.task_type.clone(),
            key: task.key.clone(),
        });

        // Spawn the task.
        let store = self.inner.store.clone();
        let inner_for_task = Arc::clone(&self.inner);
        let max_retries = self.inner.max_retries;
        let event_tx = self.inner.event_tx.clone();

        // Subscribe to progress events to track reported_progress in the active map.
        let inner_for_progress = Arc::clone(&self.inner);
        let mut progress_rx = self.inner.event_tx.subscribe();
        let progress_task_id = task.id;
        tokio::spawn(async move {
            while let Ok(evt) = progress_rx.recv().await {
                if let SchedulerEvent::Progress {
                    task_id, percent, ..
                } = evt
                {
                    if task_id == progress_task_id {
                        if let Some(at) = inner_for_progress.active.lock().await.get_mut(&task_id) {
                            at.reported_progress = Some(percent);
                        }
                        if percent >= 1.0 {
                            break;
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            let task_id = task.id;
            let result = executor.execute_erased(&ctx).await;

            // Remove from active tracking.
            inner_for_task.active.lock().await.remove(&task_id);

            // Drop the context (and its progress reporter) — executor is done.
            drop(ctx);

            match result {
                Ok(tr) => {
                    if let Err(e) = store.complete(task_id, &tr).await {
                        tracing::error!(task_id, error = %e, "failed to record task completion");
                    }
                    let _ = event_tx.send(SchedulerEvent::Completed {
                        task_id,
                        task_type: task.task_type.clone(),
                        key: task.key.clone(),
                    });
                }
                Err(te) => {
                    // If cancelled (preempted), the scheduler already paused it.
                    if child_token.is_cancelled() {
                        return;
                    }
                    let will_retry = te.retryable && task.retry_count < max_retries;
                    if let Err(e) = store
                        .fail(
                            task_id,
                            &te.message,
                            te.retryable,
                            max_retries,
                            te.actual_read_bytes,
                            te.actual_write_bytes,
                        )
                        .await
                    {
                        tracing::error!(task_id, error = %e, "failed to record task failure");
                    }
                    let _ = event_tx.send(SchedulerEvent::Failed {
                        task_id,
                        task_type: task.task_type.clone(),
                        key: task.key.clone(),
                        error: te.message,
                        will_retry,
                    });
                }
            }
        });

        Ok(true)
    }

    /// Check if there is IO headroom for a task given current running IO
    /// and system capacity.
    async fn has_io_headroom(&self, task: &TaskRecord) -> Result<bool, StoreError> {
        let reader = self.inner.resource_reader.lock().await;
        let Some(ref reader) = *reader else {
            // No monitor configured — always allow.
            return Ok(true);
        };

        let snapshot = reader.latest();
        // If we have no IO data yet, allow the task.
        if snapshot.io_read_bytes_per_sec == 0.0 && snapshot.io_write_bytes_per_sec == 0.0 {
            return Ok(true);
        }

        let (running_read, running_write) = self.inner.store.running_io_totals().await?;

        // Simple heuristic: if running tasks' expected IO already exceeds
        // 80% of observed system throughput (per second × 2s budget window),
        // defer new work.
        let read_capacity = snapshot.io_read_bytes_per_sec * 2.0;
        let write_capacity = snapshot.io_write_bytes_per_sec * 2.0;

        let read_ok = read_capacity == 0.0
            || (running_read + task.expected_read_bytes) as f64 <= read_capacity * 0.8;
        let write_ok = write_capacity == 0.0
            || (running_write + task.expected_write_bytes) as f64 <= write_capacity * 0.8;

        Ok(read_ok && write_ok)
    }

    /// Preempt active tasks with priority lower than the given threshold.
    async fn preempt_lower_priority(&self, incoming_priority: Priority) {
        let mut active = self.inner.active.lock().await;
        let to_preempt: Vec<i64> = active
            .iter()
            .filter(|(_, at)| at.record.priority.value() > incoming_priority.value())
            .map(|(id, _)| *id)
            .collect();

        for id in to_preempt {
            if let Some(at) = active.remove(&id) {
                tracing::info!(
                    task_id = id,
                    task_type = at.record.task_type,
                    "preempting task for higher-priority work"
                );
                at.token.cancel();
                // Pause in the store (best-effort — the spawned task will also
                // notice cancellation and exit).
                let _ = self.inner.store.pause(id).await;
                let _ = self.inner.event_tx.send(SchedulerEvent::Preempted {
                    task_id: id,
                    task_type: at.record.task_type.clone(),
                    key: at.record.key.clone(),
                });
            }
        }
    }

    /// Check whether any active tasks would preempt the given paused tasks.
    /// Returns true if there are running tasks with priority at or above the
    /// preemption threshold that would preempt the given priority.
    async fn has_active_preemptors(&self, priority: Priority) -> bool {
        let active = self.inner.active.lock().await;
        active.values().any(|at| {
            at.record.priority.value() <= self.inner.preempt_priority.value()
                && at.record.priority.value() < priority.value()
        })
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
                            if !self.has_active_preemptors(task.priority).await {
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
                let mut active = self.inner.active.lock().await;
                for (_, at) in active.drain() {
                    at.token.cancel();
                }
            }
            ShutdownMode::Graceful(timeout) => {
                tracing::info!(
                    timeout_ms = timeout.as_millis() as u64,
                    "graceful shutdown — waiting for running tasks"
                );

                // Wait for running tasks to complete, polling periodically.
                let deadline = tokio::time::Instant::now() + timeout;
                loop {
                    let count = self.inner.active.lock().await.len();
                    if count == 0 {
                        tracing::info!("all tasks completed during graceful shutdown");
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(
                            remaining = count,
                            "graceful shutdown timeout — cancelling remaining tasks"
                        );
                        let mut active = self.inner.active.lock().await;
                        for (_, at) in active.drain() {
                            at.token.cancel();
                        }
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Snapshot of currently active (in-memory) tasks.
    pub async fn active_tasks(&self) -> Vec<TaskRecord> {
        self.inner
            .active
            .lock()
            .await
            .values()
            .map(|at| at.record.clone())
            .collect()
    }

    /// Get estimated progress for all running tasks.
    ///
    /// Combines executor-reported progress with throughput-based extrapolation
    /// using historical average duration for each task type.
    pub async fn estimated_progress(&self) -> Vec<EstimatedProgress> {
        let active = self.inner.active.lock().await;
        let mut results = Vec::with_capacity(active.len());

        for (_, at) in active.iter() {
            let reported = at.reported_progress;

            // Extrapolate from elapsed time vs. historical average duration.
            let extrapolated = if let Some(started) = at.record.started_at {
                let elapsed_ms = (chrono::Utc::now() - started).num_milliseconds() as f64;
                if let Ok(stats) = self.inner.store.history_stats(&at.record.task_type).await {
                    if stats.avg_duration_ms > 0.0 {
                        Some((elapsed_ms / stats.avg_duration_ms).min(0.99) as f32)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Best estimate: prefer reported, fall back to extrapolated, then 0.
            let percent = reported.or(extrapolated).unwrap_or(0.0);

            results.push(EstimatedProgress {
                task_id: at.record.id,
                task_type: at.record.task_type.clone(),
                key: at.record.key.clone(),
                reported_percent: reported,
                extrapolated_percent: extrapolated,
                percent,
            });
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

    /// Add a backpressure source.
    pub fn pressure_source(
        mut self,
        source: Box<dyn crate::backpressure::PressureSource + 'static>,
    ) -> Self {
        self.pressure_sources.push(source);
        self
    }

    /// Set a custom throttle policy. Default: three-tier.
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
            // Insert directly into the registry's inner map via a helper.
            // Since we already have Arc<dyn ErasedExecutor>, we use the raw insert.
            registry.register_erased(&name, executor);
        }

        // Build pressure.
        let mut pressure = CompositePressure::new();
        for source in self.pressure_sources {
            pressure.add_source(source);
        }

        let policy = self
            .policy
            .unwrap_or_else(ThrottlePolicy::default_three_tier);

        let scheduler = Scheduler::new(store, self.config, Arc::new(registry), pressure, policy);

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
}
