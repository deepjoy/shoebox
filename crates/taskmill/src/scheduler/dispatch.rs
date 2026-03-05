use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::priority::Priority;
use crate::registry::TaskContext;
use crate::store::TaskStore;
use crate::task::TaskRecord;

use super::progress::ProgressReporter;
use super::SchedulerEvent;

// ── Active Task ────────────────────────────────────────────────────

/// Handle to a running task for preemption and progress tracking.
pub(crate) struct ActiveTask {
    pub record: TaskRecord,
    pub token: CancellationToken,
    /// Last reported progress from the executor (0.0 to 1.0).
    pub reported_progress: Option<f32>,
    /// When the last progress report was received.
    pub reported_at: Option<chrono::DateTime<chrono::Utc>>,
}

// ── Active Task Map ────────────────────────────────────────────────

/// Thread-safe map of currently running tasks.
///
/// Wraps the active-task bookkeeping that was previously inlined in
/// `Scheduler`, making preemption and progress queries independently
/// testable.
#[derive(Clone)]
pub(crate) struct ActiveTaskMap {
    inner: Arc<Mutex<HashMap<i64, ActiveTask>>>,
}

impl ActiveTaskMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn count(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn insert(&self, id: i64, task: ActiveTask) {
        self.inner.lock().await.insert(id, task);
    }

    pub async fn remove(&self, id: i64) -> Option<ActiveTask> {
        self.inner.lock().await.remove(&id)
    }

    /// Snapshot of all active task records.
    pub async fn records(&self) -> Vec<TaskRecord> {
        self.inner
            .lock()
            .await
            .values()
            .map(|at| at.record.clone())
            .collect()
    }

    /// Snapshot of progress data for all active tasks.
    pub async fn progress_snapshots(
        &self,
    ) -> Vec<(
        TaskRecord,
        Option<f32>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> {
        self.inner
            .lock()
            .await
            .values()
            .map(|at| (at.record.clone(), at.reported_progress, at.reported_at))
            .collect()
    }

    /// Update reported progress for a specific task.
    pub async fn update_progress(&self, task_id: i64, percent: f32) {
        let mut map = self.inner.lock().await;
        if let Some(at) = map.get_mut(&task_id) {
            at.reported_progress = Some(percent);
            at.reported_at = Some(chrono::Utc::now());
        }
    }

    /// Preempt active tasks with priority lower than the incoming priority.
    ///
    /// Cancels their tokens, pauses them in the store, and emits
    /// `SchedulerEvent::Preempted`. Returns the IDs of preempted tasks.
    pub async fn preempt_below(
        &self,
        incoming_priority: Priority,
        store: &TaskStore,
        event_tx: &tokio::sync::broadcast::Sender<SchedulerEvent>,
    ) -> Vec<i64> {
        let mut active = self.inner.lock().await;
        let to_preempt: Vec<i64> = active
            .iter()
            .filter(|(_, at)| at.record.priority.value() > incoming_priority.value())
            .map(|(id, _)| *id)
            .collect();

        let mut preempted = Vec::new();
        for id in to_preempt {
            if let Some(at) = active.remove(&id) {
                tracing::info!(
                    task_id = id,
                    task_type = at.record.task_type,
                    "preempting task for higher-priority work"
                );
                at.token.cancel();
                let _ = store.pause(id).await;
                let _ = event_tx.send(SchedulerEvent::Preempted {
                    task_id: id,
                    task_type: at.record.task_type.clone(),
                    key: at.record.key.clone(),
                });
                preempted.push(id);
            }
        }

        preempted
    }

    /// Check whether any active task would preempt work at the given priority.
    pub async fn has_preemptors_for(
        &self,
        priority: Priority,
        preempt_threshold: Priority,
    ) -> bool {
        let active = self.inner.lock().await;
        active.values().any(|at| {
            at.record.priority.value() <= preempt_threshold.value()
                && at.record.priority.value() < priority.value()
        })
    }

    /// Cancel all active tasks (for shutdown).
    pub async fn cancel_all(&self) {
        let mut active = self.inner.lock().await;
        for (_, at) in active.drain() {
            at.token.cancel();
        }
    }

    /// Pause all active tasks: cancel their tokens and move them to paused
    /// state in the store. Returns the number of tasks paused.
    pub async fn pause_all(
        &self,
        store: &TaskStore,
        event_tx: &tokio::sync::broadcast::Sender<SchedulerEvent>,
    ) -> usize {
        let mut active = self.inner.lock().await;
        let count = active.len();
        for (id, at) in active.drain() {
            at.token.cancel();
            let _ = store.pause(id).await;
            let _ = event_tx.send(SchedulerEvent::Preempted {
                task_id: id,
                task_type: at.record.task_type.clone(),
                key: at.record.key.clone(),
            });
        }
        count
    }
}

// ── Spawn ──────────────────────────────────────────────────────────

/// Spawn a task executor and wire up completion/failure handling.
///
/// Inserts the task into the active map, starts a progress listener,
/// and spawns the executor.
pub(crate) async fn spawn_task(
    task: TaskRecord,
    executor: Arc<dyn crate::registry::ErasedExecutor>,
    store: TaskStore,
    active: ActiveTaskMap,
    event_tx: tokio::sync::broadcast::Sender<SchedulerEvent>,
    max_retries: i32,
    app_state: crate::registry::StateSnapshot,
) {
    let child_token = CancellationToken::new();

    // Insert into active map before spawning to avoid races.
    active
        .insert(
            task.id,
            ActiveTask {
                record: task.clone(),
                token: child_token.clone(),
                reported_progress: None,
                reported_at: None,
            },
        )
        .await;

    // Build execution context.
    let ctx = TaskContext {
        record: task.clone(),
        token: child_token.clone(),
        progress: ProgressReporter::new(
            task.id,
            task.task_type.clone(),
            task.key.clone(),
            event_tx.clone(),
        ),
        app_state,
    };

    // Emit dispatched event.
    let _ = event_tx.send(SchedulerEvent::Dispatched {
        task_id: task.id,
        task_type: task.task_type.clone(),
        key: task.key.clone(),
    });

    // Spawn progress listener — bridges broadcast events into the active map.
    let active_for_progress = active.clone();
    let mut progress_rx = event_tx.subscribe();
    let progress_task_id = task.id;
    tokio::spawn(async move {
        while let Ok(evt) = progress_rx.recv().await {
            if let SchedulerEvent::Progress {
                task_id, percent, ..
            } = evt
            {
                if task_id == progress_task_id {
                    active_for_progress.update_progress(task_id, percent).await;
                    if percent >= 1.0 {
                        break;
                    }
                }
            }
        }
    });

    // Spawn executor.
    let token_for_spawn = child_token.clone();
    tokio::spawn(async move {
        let task_id = task.id;
        let result = executor.execute_erased(&ctx).await;

        // Drop the context (and its progress reporter) — executor is done.
        drop(ctx);

        match result {
            Ok(tr) => {
                if let Err(e) = store.complete(task_id, &tr).await {
                    tracing::error!(task_id, error = %e, "failed to record task completion");
                }
                // Remove from active tracking AFTER the store write completes.
                // This keeps the concurrency slot occupied, preventing the
                // scheduler from dispatching new tasks that would create
                // concurrent SQLite write transactions (which cause SQLITE_BUSY).
                active.remove(task_id).await;
                let _ = event_tx.send(SchedulerEvent::Completed {
                    task_id,
                    task_type: task.task_type.clone(),
                    key: task.key.clone(),
                });
            }
            Err(te) => {
                // If cancelled (preempted), the scheduler already paused it.
                if token_for_spawn.is_cancelled() {
                    active.remove(task_id).await;
                    return;
                }
                let will_retry = te.retryable && task.retry_count < max_retries;
                tracing::warn!(
                    task_id,
                    task_type = task.task_type,
                    error = %te.message,
                    retryable = te.retryable,
                    will_retry,
                    "task failed"
                );
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
                // Remove from active tracking AFTER the store write completes.
                active.remove(task_id).await;
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
}
