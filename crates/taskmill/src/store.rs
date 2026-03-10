use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};

use crate::priority::Priority;
use crate::task::{
    HistoryStatus, SubmitOutcome, TaskHistoryRecord, TaskLookup, TaskRecord, TaskResult,
    TaskStatus, TaskSubmission, TypeStats, MAX_PAYLOAD_BYTES,
};

/// Serde-friendly error type for Tauri IPC and API boundaries.
///
/// Wraps the internal `sqlx::Error` into a serializable form so that
/// callers do not need manual conversion at every call site.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum StoreError {
    #[error("payload exceeds maximum size of {MAX_PAYLOAD_BYTES} bytes")]
    PayloadTooLarge,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        StoreError::Database(e.to_string())
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

/// History retention policy for automatic pruning of old records.
///
/// Applied during `complete()` and `fail()` to keep the `task_history`
/// table bounded. Set to `None` to disable auto-pruning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Keep at most this many history records (oldest pruned first).
    MaxCount(i64),
    /// Keep records from the last N days.
    MaxAgeDays(i64),
}

/// Configuration for the SQLite connection pool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Maximum number of connections in the pool.
    ///
    /// Higher values reduce contention when multiple Tauri commands and
    /// background tasks access the store concurrently. Setting this too
    /// high on a single SQLite file provides diminishing returns since
    /// SQLite serializes writes.
    ///
    /// Default: 16.
    pub max_connections: u32,

    /// Optional retention policy for automatic history pruning.
    ///
    /// When set, completed/failed tasks are pruned during `complete()` and
    /// `fail()` to keep the history table bounded.
    pub retention_policy: Option<RetentionPolicy>,

    /// How many completions between automatic prune runs.
    ///
    /// Pruning runs once every `prune_interval` calls to `complete()` or
    /// `fail()` instead of on every call. Default: 100.
    pub prune_interval: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            retention_policy: None,
            prune_interval: 100,
        }
    }
}

/// SQLite-backed persistence layer for the task queue and history.
#[derive(Clone)]
pub struct TaskStore {
    pool: SqlitePool,
    retention_policy: Option<RetentionPolicy>,
    prune_interval: u64,
    completion_count: std::sync::Arc<AtomicU64>,
}

impl TaskStore {
    /// Open (or create) a taskmill database at the given path with default config.
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        Self::open_with_config(path, StoreConfig::default()).await
    }

    /// Open (or create) a taskmill database at the given path with custom config.
    pub async fn open_with_config(path: &str, config: StoreConfig) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(opts)
            .await?;

        let store = Self {
            pool,
            retention_policy: config.retention_policy,
            prune_interval: config.prune_interval,
            completion_count: std::sync::Arc::new(AtomicU64::new(0)),
        };
        store.migrate().await?;
        store.recover_running().await?;
        Ok(store)
    }

    /// Open an in-memory database (for testing).
    pub async fn open_memory() -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(":memory:")
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;

        let store = Self {
            pool,
            retention_policy: None,
            prune_interval: 100,
            completion_count: std::sync::Arc::new(AtomicU64::new(0)),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Run the migration SQL.
    async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::raw_sql(include_str!("../migrations/001_tasks.sql"))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Restart recovery: reset any `running` tasks back to `pending`.
    async fn recover_running(&self) -> Result<(), StoreError> {
        let result = sqlx::query(
            "UPDATE tasks SET status = 'pending', started_at = NULL WHERE status = 'running'",
        )
        .execute(&self.pool)
        .await?;
        let count = result.rows_affected();
        if count > 0 {
            tracing::info!(count, "recovered interrupted tasks back to pending");
        }
        Ok(())
    }

    /// Get a reference to the underlying connection pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Begin an IMMEDIATE transaction for write operations.
    ///
    /// Unlike `pool.begin()` which uses `BEGIN DEFERRED`, this acquires the
    /// write lock upfront. This prevents deadlocks when multiple transactions
    /// read-then-write concurrently — the busy_timeout is properly honored
    /// instead of SQLite returning SQLITE_BUSY immediately.
    ///
    /// The returned connection auto-rollbacks on drop (sqlx resets pooled
    /// connections with open transactions).
    async fn begin_write(&self) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, StoreError> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
        Ok(conn)
    }

    // ── Submit ──────────────────────────────────────────────────────

    /// Submit a new task.
    ///
    /// Returns [`SubmitOutcome::Inserted`] if the task was enqueued,
    /// [`SubmitOutcome::Upgraded`] if a duplicate existed but its priority
    /// was upgraded, or [`SubmitOutcome::Duplicate`] if a duplicate existed
    /// with equal or higher priority.
    ///
    /// When `sub.key` is `None`, the dedup key is auto-generated by hashing
    /// the task type and payload.
    pub async fn submit(&self, sub: &TaskSubmission) -> Result<SubmitOutcome, StoreError> {
        if let Some(ref p) = sub.payload {
            if p.len() > MAX_PAYLOAD_BYTES {
                return Err(StoreError::PayloadTooLarge);
            }
        }

        let key = sub.effective_key();
        let priority = sub.priority.value() as i32;
        tracing::debug!(task_type = %sub.task_type, "store.submit: INSERT start");
        let result = sqlx::query(
            "INSERT OR IGNORE INTO tasks (task_type, key, priority, payload, expected_read_bytes, expected_write_bytes)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&sub.task_type)
        .bind(&key)
        .bind(priority)
        .bind(&sub.payload)
        .bind(sub.expected_read_bytes)
        .bind(sub.expected_write_bytes)
        .execute(&self.pool)
        .await?;
        tracing::debug!(task_type = %sub.task_type, "store.submit: INSERT end");

        if result.rows_affected() > 0 {
            return Ok(SubmitOutcome::Inserted(result.last_insert_rowid()));
        }

        // Dedup hit — try to upgrade priority on pending/paused tasks.
        // Lower numeric value = higher priority, so `priority > ?` means
        // the existing task has lower importance than the new submission.
        let row = sqlx::query(
            "UPDATE tasks SET priority = ?
             WHERE key = ? AND status IN ('pending', 'paused') AND priority > ?
             RETURNING id",
        )
        .bind(priority)
        .bind(&key)
        .bind(priority)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            return Ok(SubmitOutcome::Upgraded(r.get("id")));
        }

        // Dedup hit on running/paused task — mark for re-queue so the task
        // runs again after the current execution completes.
        let row = sqlx::query(
            "UPDATE tasks SET requeue = 1, requeue_priority = ?
             WHERE key = ? AND status IN ('running', 'paused')
               AND (requeue = 0 OR requeue_priority > ?)
             RETURNING id",
        )
        .bind(priority)
        .bind(&key)
        .bind(priority)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(SubmitOutcome::Requeued(r.get("id"))),
            None => Ok(SubmitOutcome::Duplicate),
        }
    }

    /// Submit multiple tasks in a single transaction. Returns a `Vec` with one
    /// [`SubmitOutcome`] per input.
    ///
    /// This is significantly faster than calling [`submit`](Self::submit) in a
    /// loop because all inserts share a single SQLite transaction (one
    /// `BEGIN`/`COMMIT` pair instead of N implicit transactions).
    pub async fn submit_batch(
        &self,
        submissions: &[TaskSubmission],
    ) -> Result<Vec<SubmitOutcome>, StoreError> {
        // Pre-validate all payloads before starting the transaction
        // to avoid partial inserts on validation errors.
        for sub in submissions {
            if let Some(ref p) = sub.payload {
                if p.len() > MAX_PAYLOAD_BYTES {
                    return Err(StoreError::PayloadTooLarge);
                }
            }
        }

        let mut results = Vec::with_capacity(submissions.len());

        let mut conn = self.begin_write().await?;

        for sub in submissions {
            let key = sub.effective_key();
            let priority = sub.priority.value() as i32;
            let result = sqlx::query(
                "INSERT OR IGNORE INTO tasks (task_type, key, priority, payload, expected_read_bytes, expected_write_bytes)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&sub.task_type)
            .bind(&key)
            .bind(priority)
            .bind(&sub.payload)
            .bind(sub.expected_read_bytes)
            .bind(sub.expected_write_bytes)
            .execute(&mut *conn)
            .await?;

            if result.rows_affected() > 0 {
                results.push(SubmitOutcome::Inserted(result.last_insert_rowid()));
            } else {
                // Dedup hit — try to upgrade priority on pending/paused tasks.
                let row = sqlx::query(
                    "UPDATE tasks SET priority = ?
                     WHERE key = ? AND status IN ('pending', 'paused') AND priority > ?
                     RETURNING id",
                )
                .bind(priority)
                .bind(&key)
                .bind(priority)
                .fetch_optional(&mut *conn)
                .await?;

                if let Some(r) = row {
                    results.push(SubmitOutcome::Upgraded(r.get("id")));
                } else {
                    // Try requeue on running/paused tasks.
                    let row = sqlx::query(
                        "UPDATE tasks SET requeue = 1, requeue_priority = ?
                         WHERE key = ? AND status IN ('running', 'paused')
                           AND (requeue = 0 OR requeue_priority > ?)
                         RETURNING id",
                    )
                    .bind(priority)
                    .bind(&key)
                    .bind(priority)
                    .fetch_optional(&mut *conn)
                    .await?;

                    match row {
                        Some(r) => results.push(SubmitOutcome::Requeued(r.get("id"))),
                        None => results.push(SubmitOutcome::Duplicate),
                    }
                }
            }
        }

        sqlx::query("COMMIT").execute(&mut *conn).await?;
        Ok(results)
    }

    // ── Pop / lifecycle ─────────────────────────────────────────────

    /// Peek at the highest-priority pending task without modifying it.
    /// Returns `None` if the queue is empty.
    pub async fn peek_next(&self) -> Result<Option<TaskRecord>, StoreError> {
        let row = sqlx::query(
            "SELECT * FROM tasks
             WHERE status = 'pending'
             ORDER BY priority ASC, id ASC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.as_ref().map(row_to_task_record))
    }

    /// Atomically claim a specific pending task by id, setting it to running.
    /// Returns `None` if the task is no longer pending (e.g. claimed by another
    /// dispatcher or cancelled).
    pub async fn pop_by_id(&self, id: i64) -> Result<Option<TaskRecord>, StoreError> {
        tracing::debug!(task_id = id, "store.pop_by_id: UPDATE start");
        let row = sqlx::query(
            "UPDATE tasks SET status = 'running', started_at = datetime('now')
             WHERE id = ? AND status = 'pending'
             RETURNING *",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        tracing::debug!(task_id = id, "store.pop_by_id: UPDATE end");

        Ok(row.as_ref().map(row_to_task_record))
    }

    /// Pop the highest-priority pending task and mark it as running.
    /// Returns `None` if the queue is empty.
    pub async fn pop_next(&self) -> Result<Option<TaskRecord>, StoreError> {
        // Single atomic statement: find + update + return.
        let row = sqlx::query(
            "UPDATE tasks SET status = 'running', started_at = datetime('now')
             WHERE id = (
                 SELECT id FROM tasks
                 WHERE status = 'pending'
                 ORDER BY priority ASC, id ASC
                 LIMIT 1
             )
             RETURNING *",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| row_to_task_record(&r)))
    }

    /// Atomically requeue a running task back to pending.
    ///
    /// Used when a task is popped but then rejected by backpressure or IO
    /// budget checks. Unlike pause+resume, this is a single atomic operation
    /// that never puts the task in an intermediate state visible to queries.
    pub async fn requeue(&self, id: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE tasks SET status = 'pending', started_at = NULL WHERE id = ? AND status = 'running'",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark a task as completed and move it to history.
    pub async fn complete(&self, id: i64, result: &TaskResult) -> Result<(), StoreError> {
        tracing::debug!(task_id = id, "store.complete: BEGIN tx");
        let mut conn = self.begin_write().await?;

        // Fetch the task to move.
        let row = sqlx::query("SELECT * FROM tasks WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?;

        let Some(row) = row else { return Ok(()) };
        let task = row_to_task_record(&row);

        // Compute duration.
        let duration_ms: Option<i64> = if task.started_at.is_some() {
            sqlx::query_scalar(
                "SELECT CAST((julianday('now') - julianday(?)) * 86400000 AS INTEGER)",
            )
            .bind(
                task.started_at
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
            )
            .fetch_one(&mut *conn)
            .await?
        } else {
            None
        };

        // Insert into history.
        sqlx::query(
            "INSERT INTO task_history (task_type, key, priority, status, payload,
                expected_read_bytes, expected_write_bytes, actual_read_bytes, actual_write_bytes,
                retry_count, last_error, created_at, started_at, duration_ms)
             VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.task_type)
        .bind(&task.key)
        .bind(task.priority.value() as i32)
        .bind(&task.payload)
        .bind(task.expected_read_bytes)
        .bind(task.expected_write_bytes)
        .bind(result.actual_read_bytes)
        .bind(result.actual_write_bytes)
        .bind(task.retry_count)
        .bind(&task.last_error)
        .bind(task.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(
            task.started_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        )
        .bind(duration_ms)
        .execute(&mut *conn)
        .await?;

        if task.requeue {
            // Requeue flag set — reset to pending with requeue_priority
            // instead of removing from the active queue.
            let requeue_priority = task
                .requeue_priority
                .map(|p| p.value() as i32)
                .unwrap_or(task.priority.value() as i32);
            sqlx::query(
                "UPDATE tasks SET status = 'pending', priority = ?,
                    started_at = NULL, retry_count = 0, last_error = NULL,
                    requeue = 0, requeue_priority = NULL
                 WHERE id = ?",
            )
            .bind(requeue_priority)
            .bind(id)
            .execute(&mut *conn)
            .await?;
        } else {
            // Remove from active queue.
            sqlx::query("DELETE FROM tasks WHERE id = ?")
                .bind(id)
                .execute(&mut *conn)
                .await?;
        }

        sqlx::query("COMMIT").execute(&mut *conn).await?;
        tracing::debug!(task_id = id, "store.complete: COMMIT ok");

        self.maybe_prune().await;

        Ok(())
    }

    /// Mark a task as failed. If `retryable` and under max retries, requeue
    /// it as pending with the same priority. Otherwise move to history as failed.
    pub async fn fail(
        &self,
        id: i64,
        error: &str,
        retryable: bool,
        max_retries: i32,
        actual_read_bytes: i64,
        actual_write_bytes: i64,
    ) -> Result<(), StoreError> {
        tracing::debug!(task_id = id, "store.fail: BEGIN tx");
        let mut conn = self.begin_write().await?;
        tracing::debug!(task_id = id, "store.fail: BEGIN acquired");

        let row = sqlx::query("SELECT * FROM tasks WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?;

        let Some(row) = row else { return Ok(()) };
        let task = row_to_task_record(&row);

        if retryable && task.retry_count < max_retries {
            // Requeue with incremented retry count, same priority.
            sqlx::query(
                "UPDATE tasks SET status = 'pending', started_at = NULL,
                    retry_count = retry_count + 1, last_error = ?
                 WHERE id = ?",
            )
            .bind(error)
            .bind(id)
            .execute(&mut *conn)
            .await?;
        } else {
            // Permanent failure — move to history.
            let duration_ms: Option<i64> = if task.started_at.is_some() {
                sqlx::query_scalar(
                    "SELECT CAST((julianday('now') - julianday(?)) * 86400000 AS INTEGER)",
                )
                .bind(
                    task.started_at
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
                )
                .fetch_one(&mut *conn)
                .await?
            } else {
                None
            };

            sqlx::query(
                "INSERT INTO task_history (task_type, key, priority, status, payload,
                    expected_read_bytes, expected_write_bytes, actual_read_bytes, actual_write_bytes,
                    retry_count, last_error, created_at, started_at, duration_ms)
                 VALUES (?, ?, ?, 'failed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&task.task_type)
            .bind(&task.key)
            .bind(task.priority.value() as i32)
            .bind(&task.payload)
            .bind(task.expected_read_bytes)
            .bind(task.expected_write_bytes)
            .bind(actual_read_bytes)
            .bind(actual_write_bytes)
            .bind(task.retry_count + 1)
            .bind(error)
            .bind(task.created_at.format("%Y-%m-%d %H:%M:%S").to_string())
            .bind(task.started_at.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()))
            .bind(duration_ms)
            .execute(&mut *conn)
            .await?;

            sqlx::query("DELETE FROM tasks WHERE id = ?")
                .bind(id)
                .execute(&mut *conn)
                .await?;
        }

        sqlx::query("COMMIT").execute(&mut *conn).await?;
        tracing::debug!(task_id = id, "store.fail: COMMIT ok");

        self.maybe_prune().await;

        Ok(())
    }

    /// Pause a running task (for preemption). Sets status to paused.
    pub async fn pause(&self, id: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE tasks SET status = 'paused', started_at = NULL WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Resume a paused task back to pending.
    pub async fn resume(&self, id: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE tasks SET status = 'pending' WHERE id = ? AND status = 'paused'")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ── Query: active queue ─────────────────────────────────────────

    /// All currently running tasks.
    pub async fn running_tasks(&self) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM tasks WHERE status = 'running' ORDER BY priority ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_task_record).collect())
    }

    /// Count of running tasks.
    pub async fn running_count(&self) -> Result<i64, StoreError> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks WHERE status = 'running'")
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0)
    }

    /// Pending tasks, ordered by priority then age. Limit controls page size.
    pub async fn pending_tasks(&self, limit: i32) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM tasks WHERE status = 'pending' ORDER BY priority ASC, id ASC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_task_record).collect())
    }

    /// Count of pending tasks.
    pub async fn pending_count(&self) -> Result<i64, StoreError> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks WHERE status = 'pending'")
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0)
    }

    /// Pending tasks filtered by type.
    pub async fn pending_by_type(&self, task_type: &str) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM tasks WHERE status = 'pending' AND task_type = ? ORDER BY priority ASC, id ASC",
        )
        .bind(task_type)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_task_record).collect())
    }

    /// Count of paused tasks.
    pub async fn paused_count(&self) -> Result<i64, StoreError> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks WHERE status = 'paused'")
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0)
    }

    /// Paused tasks.
    pub async fn paused_tasks(&self) -> Result<Vec<TaskRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM tasks WHERE status = 'paused' ORDER BY priority ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_task_record).collect())
    }

    /// Look up an active task by its row id. Returns `None` if no active
    /// task with that id exists.
    pub async fn task_by_id(&self, id: i64) -> Result<Option<TaskRecord>, StoreError> {
        let row = sqlx::query("SELECT * FROM tasks WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(row_to_task_record))
    }

    /// Look up an active task by its dedup key. Returns `None` if no active
    /// task with that key exists.
    pub async fn task_by_key(&self, key: &str) -> Result<Option<TaskRecord>, StoreError> {
        let row = sqlx::query("SELECT * FROM tasks WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(row_to_task_record))
    }

    /// Sum of expected read/write bytes for all running tasks.
    pub async fn running_io_totals(&self) -> Result<(i64, i64), StoreError> {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(expected_read_bytes), 0), COALESCE(SUM(expected_write_bytes), 0)
             FROM tasks WHERE status = 'running'",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    // ── Query: history ──────────────────────────────────────────────

    /// Look up a history record by its row id.
    pub async fn history_by_id(&self, id: i64) -> Result<Option<TaskHistoryRecord>, StoreError> {
        let row = sqlx::query("SELECT * FROM task_history WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().map(row_to_history_record))
    }

    /// Recent history entries, newest first.
    pub async fn history(
        &self,
        limit: i32,
        offset: i32,
    ) -> Result<Vec<TaskHistoryRecord>, StoreError> {
        let rows =
            sqlx::query("SELECT * FROM task_history ORDER BY completed_at DESC LIMIT ? OFFSET ?")
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.iter().map(row_to_history_record).collect())
    }

    /// History filtered by task type.
    pub async fn history_by_type(
        &self,
        task_type: &str,
        limit: i32,
    ) -> Result<Vec<TaskHistoryRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM task_history WHERE task_type = ? ORDER BY completed_at DESC LIMIT ?",
        )
        .bind(task_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_history_record).collect())
    }

    /// History for a specific key (all past runs of that key).
    pub async fn history_by_key(&self, key: &str) -> Result<Vec<TaskHistoryRecord>, StoreError> {
        let rows =
            sqlx::query("SELECT * FROM task_history WHERE key = ? ORDER BY completed_at DESC")
                .bind(key)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.iter().map(row_to_history_record).collect())
    }

    /// Failed tasks from history.
    pub async fn failed_tasks(&self, limit: i32) -> Result<Vec<TaskHistoryRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM task_history WHERE status = 'failed' ORDER BY completed_at DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_history_record).collect())
    }

    /// Aggregate stats for a task type from completed history.
    pub async fn history_stats(&self, task_type: &str) -> Result<TypeStats, StoreError> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) as total,
                COALESCE(AVG(CASE WHEN status = 'completed' THEN duration_ms END), 0.0) as avg_dur,
                COALESCE(AVG(CASE WHEN status = 'completed' THEN actual_read_bytes END), 0.0) as avg_read,
                COALESCE(AVG(CASE WHEN status = 'completed' THEN actual_write_bytes END), 0.0) as avg_write,
                CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS REAL) / MAX(COUNT(*), 1) as fail_rate
             FROM task_history WHERE task_type = ?",
        )
        .bind(task_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(TypeStats {
            count: row.get::<i64, _>("total"),
            avg_duration_ms: row.get::<f64, _>("avg_dur"),
            avg_read_bytes: row.get::<f64, _>("avg_read"),
            avg_write_bytes: row.get::<f64, _>("avg_write"),
            failure_rate: row.get::<f64, _>("fail_rate"),
        })
    }

    /// Average IO throughput (bytes/sec) for recently completed tasks of a type.
    /// Used by the scheduler for IO budget estimation.
    pub async fn avg_throughput(
        &self,
        task_type: &str,
        recent_limit: i32,
    ) -> Result<(f64, f64), StoreError> {
        let row: (f64, f64) = sqlx::query_as(
            "SELECT
                COALESCE(AVG(CASE WHEN duration_ms > 0 THEN actual_read_bytes * 1000.0 / duration_ms END), 0),
                COALESCE(AVG(CASE WHEN duration_ms > 0 THEN actual_write_bytes * 1000.0 / duration_ms END), 0)
             FROM (
                 SELECT actual_read_bytes, actual_write_bytes, duration_ms
                 FROM task_history
                 WHERE task_type = ? AND status = 'completed' AND duration_ms > 0
                 ORDER BY completed_at DESC
                 LIMIT ?
             )",
        )
        .bind(task_type)
        .bind(recent_limit)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    // ── Unified lookup ──────────────────────────────────────────────

    /// Look up a task by its dedup key, checking the active queue first
    /// and falling back to history.
    ///
    /// This is the low-level building block for [`Scheduler::task_lookup`].
    /// The `key` parameter is the pre-computed SHA-256 dedup key (as
    /// returned by [`generate_dedup_key`](crate::task::generate_dedup_key)
    /// or [`TaskSubmission::effective_key`]).
    pub async fn task_lookup(&self, key: &str) -> Result<TaskLookup, StoreError> {
        // Check active queue first (pending / running / paused).
        if let Some(record) = self.task_by_key(key).await? {
            return Ok(TaskLookup::Active(record));
        }

        // Fall back to the most recent history entry.
        let row = sqlx::query(
            "SELECT * FROM task_history WHERE key = ? ORDER BY completed_at DESC LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => Ok(TaskLookup::History(row_to_history_record(&r))),
            None => Ok(TaskLookup::NotFound),
        }
    }

    // ── Pruning ─────────────────────────────────────────────────────

    /// Prune history records older than `max_age_days` days.
    /// Returns the number of records deleted.
    pub async fn prune_history_by_age(&self, max_age_days: i64) -> Result<u64, StoreError> {
        let result =
            sqlx::query("DELETE FROM task_history WHERE completed_at < datetime('now', ?)")
                .bind(format!("-{max_age_days} days"))
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected())
    }

    /// Prune history to keep at most `keep_latest` records.
    /// Returns the number of records deleted.
    pub async fn prune_history_by_count(&self, keep_latest: i64) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "DELETE FROM task_history WHERE id NOT IN (
                 SELECT id FROM task_history ORDER BY completed_at DESC LIMIT ?
             )",
        )
        .bind(keep_latest)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Increment the completion counter and prune every `prune_interval` completions.
    /// Errors are logged rather than propagated since the task itself already committed.
    async fn maybe_prune(&self) {
        if self.retention_policy.is_none() {
            return;
        }
        let count = self.completion_count.fetch_add(1, Ordering::Relaxed);
        if count % self.prune_interval != 0 {
            return;
        }
        if let Err(e) = self.auto_prune().await {
            tracing::warn!("history prune failed: {e}");
        }
    }

    /// Apply the configured retention policy, if any.
    async fn auto_prune(&self) -> Result<(), StoreError> {
        match &self.retention_policy {
            Some(RetentionPolicy::MaxCount(n)) => {
                self.prune_history_by_count(*n).await?;
            }
            Some(RetentionPolicy::MaxAgeDays(days)) => {
                self.prune_history_by_age(*days).await?;
            }
            None => {}
        }
        Ok(())
    }

    /// Close the store and flush WAL.
    pub async fn close(&self) {
        // Consolidate the WAL file into the main database before closing.
        if let Err(e) = sqlx::raw_sql("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await
        {
            tracing::warn!(error = %e, "WAL checkpoint failed during close");
        }
        self.pool.close().await;
    }

    /// Delete a task from the active queue by id. Returns true if a row was deleted.
    pub async fn delete(&self, id: i64) -> Result<bool, StoreError> {
        let result = sqlx::query("DELETE FROM tasks WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

// ── Row mapping helpers ─────────────────────────────────────────────

fn parse_datetime(s: &str) -> DateTime<Utc> {
    // SQLite stores as "YYYY-MM-DD HH:MM:SS". Parse with chrono.
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .map(|ndt| ndt.and_utc())
        .unwrap_or_default()
}

fn row_to_task_record(row: &sqlx::sqlite::SqliteRow) -> TaskRecord {
    let priority_val: i32 = row.get("priority");
    let status_str: String = row.get("status");
    let created_at_str: String = row.get("created_at");
    let started_at_str: Option<String> = row.get("started_at");

    let requeue_val: i32 = row.get("requeue");
    let requeue_priority_val: Option<i32> = row.get("requeue_priority");

    TaskRecord {
        id: row.get("id"),
        task_type: row.get("task_type"),
        key: row.get("key"),
        priority: Priority::new(priority_val as u8),
        status: status_str.parse().unwrap_or(TaskStatus::Pending),
        payload: row.get("payload"),
        expected_read_bytes: row.get("expected_read_bytes"),
        expected_write_bytes: row.get("expected_write_bytes"),
        retry_count: row.get("retry_count"),
        last_error: row.get("last_error"),
        created_at: parse_datetime(&created_at_str),
        started_at: started_at_str.map(|s| parse_datetime(&s)),
        requeue: requeue_val != 0,
        requeue_priority: requeue_priority_val.map(|p| Priority::new(p as u8)),
    }
}

fn row_to_history_record(row: &sqlx::sqlite::SqliteRow) -> TaskHistoryRecord {
    let priority_val: i32 = row.get("priority");
    let status_str: String = row.get("status");
    let created_at_str: String = row.get("created_at");
    let started_at_str: Option<String> = row.get("started_at");
    let completed_at_str: String = row.get("completed_at");

    TaskHistoryRecord {
        id: row.get("id"),
        task_type: row.get("task_type"),
        key: row.get("key"),
        priority: Priority::new(priority_val as u8),
        status: status_str.parse().unwrap_or(HistoryStatus::Failed),
        payload: row.get("payload"),
        expected_read_bytes: row.get("expected_read_bytes"),
        expected_write_bytes: row.get("expected_write_bytes"),
        actual_read_bytes: row.get("actual_read_bytes"),
        actual_write_bytes: row.get("actual_write_bytes"),
        retry_count: row.get("retry_count"),
        last_error: row.get("last_error"),
        created_at: parse_datetime(&created_at_str),
        started_at: started_at_str.map(|s| parse_datetime(&s)),
        completed_at: parse_datetime(&completed_at_str),
        duration_ms: row.get("duration_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_store() -> TaskStore {
        TaskStore::open_memory().await.unwrap()
    }

    fn make_submission(key: &str, priority: Priority) -> TaskSubmission {
        TaskSubmission {
            task_type: "test".into(),
            key: Some(key.into()),
            priority,
            payload: Some(b"hello".to_vec()),
            expected_read_bytes: 1000,
            expected_write_bytes: 500,
        }
    }

    #[tokio::test]
    async fn submit_and_pop() {
        let store = test_store().await;
        let sub = make_submission("job-1", Priority::NORMAL);
        let expected_key = sub.effective_key();

        let outcome = store.submit(&sub).await.unwrap();
        assert!(outcome.is_inserted());

        let task = store.pop_next().await.unwrap().unwrap();
        assert_eq!(task.key, expected_key);
        assert_eq!(task.status, TaskStatus::Running);
        assert!(task.started_at.is_some());
    }

    #[tokio::test]
    async fn dedup_prevents_duplicate_key() {
        let store = test_store().await;
        let sub = make_submission("dup-key", Priority::NORMAL);

        let first = store.submit(&sub).await.unwrap();
        assert!(first.is_inserted());

        let second = store.submit(&sub).await.unwrap();
        assert_eq!(second, SubmitOutcome::Duplicate); // same priority → no upgrade
    }

    #[tokio::test]
    async fn dedup_upgrades_priority() {
        let store = test_store().await;

        // Submit at NORMAL priority.
        let sub_normal = make_submission("upgrade-me", Priority::NORMAL);
        let first = store.submit(&sub_normal).await.unwrap();
        assert!(first.is_inserted());

        // Submit same key at HIGH priority — should upgrade.
        let sub_high = make_submission("upgrade-me", Priority::HIGH);
        let second = store.submit(&sub_high).await.unwrap();
        assert!(matches!(second, SubmitOutcome::Upgraded(_)));

        // Verify the stored priority was upgraded.
        let key = sub_normal.effective_key();
        let task = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(task.priority, Priority::HIGH);

        // Submit at BACKGROUND (lower importance) — should not upgrade.
        let sub_bg = make_submission("upgrade-me", Priority::BACKGROUND);
        let third = store.submit(&sub_bg).await.unwrap();
        assert_eq!(third, SubmitOutcome::Duplicate);

        // Priority should still be HIGH.
        let task = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(task.priority, Priority::HIGH);
    }

    #[tokio::test]
    async fn dedup_requeues_when_running() {
        let store = test_store().await;

        // Submit and pop (transitions to running).
        let sub = make_submission("running-task", Priority::NORMAL);
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        // Submit same key at HIGH priority — should be Requeued since task is running.
        let sub_high = make_submission("running-task", Priority::HIGH);
        let outcome = store.submit(&sub_high).await.unwrap();
        assert!(matches!(outcome, SubmitOutcome::Requeued(_)));

        // Verify the requeue flag is set on the running task.
        let key = sub.effective_key();
        let running = store.task_by_key(&key).await.unwrap().unwrap();
        assert!(running.requeue);
        assert_eq!(running.requeue_priority, Some(Priority::HIGH));

        // Complete the running task — should reset to pending with requeue_priority.
        store
            .complete(
                task.id,
                &TaskResult {
                    actual_read_bytes: 0,
                    actual_write_bytes: 0,
                },
            )
            .await
            .unwrap();

        // Task should now be pending at HIGH priority.
        let requeued = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(requeued.status, TaskStatus::Pending);
        assert_eq!(requeued.priority, Priority::HIGH);
        assert!(!requeued.requeue);
        assert_eq!(requeued.requeue_priority, None);

        // Pop should return it.
        let popped = store.pop_next().await.unwrap().unwrap();
        assert_eq!(popped.id, task.id);
    }

    #[tokio::test]
    async fn dedup_requeue_already_requeued_same_priority() {
        let store = test_store().await;

        let sub = make_submission("rq-dup", Priority::NORMAL);
        store.submit(&sub).await.unwrap();
        store.pop_next().await.unwrap();

        // First requeue at HIGH.
        let sub_high = make_submission("rq-dup", Priority::HIGH);
        let outcome = store.submit(&sub_high).await.unwrap();
        assert!(matches!(outcome, SubmitOutcome::Requeued(_)));

        // Second requeue at same priority — should be Duplicate.
        let outcome2 = store.submit(&sub_high).await.unwrap();
        assert_eq!(outcome2, SubmitOutcome::Duplicate);
    }

    #[tokio::test]
    async fn dedup_requeue_upgrades_priority() {
        let store = test_store().await;

        let sub = make_submission("rq-upgrade", Priority::BACKGROUND);
        store.submit(&sub).await.unwrap();
        store.pop_next().await.unwrap();

        // First requeue at NORMAL.
        let sub_normal = make_submission("rq-upgrade", Priority::NORMAL);
        let outcome = store.submit(&sub_normal).await.unwrap();
        assert!(matches!(outcome, SubmitOutcome::Requeued(_)));

        // Second requeue at HIGH — should upgrade requeue_priority.
        let sub_high = make_submission("rq-upgrade", Priority::HIGH);
        let outcome2 = store.submit(&sub_high).await.unwrap();
        assert!(matches!(outcome2, SubmitOutcome::Requeued(_)));

        let key = sub.effective_key();
        let task = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(task.requeue_priority, Some(Priority::HIGH));
    }

    #[tokio::test]
    async fn permanent_failure_drops_requeue() {
        let store = test_store().await;

        let sub = make_submission("fail-rq", Priority::NORMAL);
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        // Mark for requeue.
        let sub_high = make_submission("fail-rq", Priority::HIGH);
        store.submit(&sub_high).await.unwrap();

        // Permanent failure — requeue flag is dropped.
        store.fail(task.id, "boom", false, 0, 0, 0).await.unwrap();

        // Key should be free for reuse.
        let outcome = store.submit(&sub).await.unwrap();
        assert!(outcome.is_inserted());
    }

    #[tokio::test]
    async fn dedup_allows_same_key_different_types() {
        let store = test_store().await;

        let sub_a = TaskSubmission {
            task_type: "type_a".into(),
            key: Some("shared-key".into()),
            priority: Priority::NORMAL,
            payload: None,
            expected_read_bytes: 0,
            expected_write_bytes: 0,
        };
        let sub_b = TaskSubmission {
            task_type: "type_b".into(),
            key: Some("shared-key".into()),
            priority: Priority::NORMAL,
            payload: None,
            expected_read_bytes: 0,
            expected_write_bytes: 0,
        };

        let first = store.submit(&sub_a).await.unwrap();
        assert!(first.is_inserted());

        // Same logical key, different task type — should NOT dedup.
        let second = store.submit(&sub_b).await.unwrap();
        assert!(second.is_inserted());
    }

    #[tokio::test]
    async fn dedup_by_payload_when_no_key() {
        let store = test_store().await;

        let sub = TaskSubmission {
            task_type: "ingest".into(),
            key: None,
            priority: Priority::NORMAL,
            payload: Some(b"same-data".to_vec()),
            expected_read_bytes: 0,
            expected_write_bytes: 0,
        };

        let first = store.submit(&sub).await.unwrap();
        assert!(first.is_inserted());

        // Same type + payload → dedup.
        let second = store.submit(&sub).await.unwrap();
        assert_eq!(second, SubmitOutcome::Duplicate);

        // Different payload → no dedup.
        let sub2 = TaskSubmission {
            payload: Some(b"different-data".to_vec()),
            ..sub.clone()
        };
        let third = store.submit(&sub2).await.unwrap();
        assert!(third.is_inserted());
    }

    #[tokio::test]
    async fn priority_ordering() {
        let store = test_store().await;

        let bg = make_submission("bg", Priority::BACKGROUND);
        let rt = make_submission("rt", Priority::REALTIME);
        let normal = make_submission("normal", Priority::NORMAL);

        let bg_key = bg.effective_key();
        let rt_key = rt.effective_key();
        let normal_key = normal.effective_key();

        store.submit(&bg).await.unwrap();
        store.submit(&rt).await.unwrap();
        store.submit(&normal).await.unwrap();

        let first = store.pop_next().await.unwrap().unwrap();
        assert_eq!(first.key, rt_key);

        let second = store.pop_next().await.unwrap().unwrap();
        assert_eq!(second.key, normal_key);

        let third = store.pop_next().await.unwrap().unwrap();
        assert_eq!(third.key, bg_key);
    }

    #[tokio::test]
    async fn complete_moves_to_history() {
        let store = test_store().await;
        let sub = make_submission("done", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        store
            .complete(
                task.id,
                &TaskResult {
                    actual_read_bytes: 2000,
                    actual_write_bytes: 1000,
                },
            )
            .await
            .unwrap();

        // Should be gone from active queue.
        assert!(store.task_by_key(&key).await.unwrap().is_none());

        // Should be in history.
        let hist = store.history_by_key(&key).await.unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].status, HistoryStatus::Completed);
        assert_eq!(hist[0].actual_read_bytes, Some(2000));
    }

    #[tokio::test]
    async fn fail_retryable_requeues() {
        let store = test_store().await;
        let sub = make_submission("retry-me", Priority::HIGH);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        store
            .fail(task.id, "transient error", true, 3, 0, 0)
            .await
            .unwrap();

        // Should still be in active queue as pending with retry_count=1.
        let requeued = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(requeued.status, TaskStatus::Pending);
        assert_eq!(requeued.retry_count, 1);
        assert_eq!(requeued.last_error.as_deref(), Some("transient error"));
    }

    #[tokio::test]
    async fn fail_exhausted_retries_moves_to_history() {
        let store = test_store().await;
        let sub = make_submission("permanent", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        // First fail: retry_count 0 < 1, requeued with retry_count=1.
        store.fail(task.id, "err1", true, 1, 0, 0).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();
        assert_eq!(task.retry_count, 1);
        // Second fail: retry_count 1 >= max_retries 1, moves to history.
        store.fail(task.id, "err2", true, 1, 100, 50).await.unwrap();

        // Should be in history now.
        assert!(store.task_by_key(&key).await.unwrap().is_none());
        let hist = store.failed_tasks(10).await.unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].status, HistoryStatus::Failed);
    }

    #[tokio::test]
    async fn payload_size_limit() {
        let store = test_store().await;
        let mut sub = make_submission("big", Priority::NORMAL);
        sub.payload = Some(vec![0u8; MAX_PAYLOAD_BYTES + 1]);

        let err = store.submit(&sub).await.unwrap_err();
        assert!(matches!(err, StoreError::PayloadTooLarge));
    }

    #[tokio::test]
    async fn running_io_totals() {
        let store = test_store().await;

        let mut sub = make_submission("io-1", Priority::NORMAL);
        sub.expected_read_bytes = 5000;
        sub.expected_write_bytes = 2000;
        store.submit(&sub).await.unwrap();

        let mut sub2 = make_submission("io-2", Priority::NORMAL);
        sub2.expected_read_bytes = 3000;
        sub2.expected_write_bytes = 1000;
        store.submit(&sub2).await.unwrap();

        // Pop both so they're running.
        store.pop_next().await.unwrap();
        store.pop_next().await.unwrap();

        let (read, write) = store.running_io_totals().await.unwrap();
        assert_eq!(read, 8000);
        assert_eq!(write, 3000);
    }

    #[tokio::test]
    async fn key_freed_after_completion() {
        let store = test_store().await;
        let sub = make_submission("reuse", Priority::NORMAL);
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();
        store
            .complete(
                task.id,
                &TaskResult {
                    actual_read_bytes: 0,
                    actual_write_bytes: 0,
                },
            )
            .await
            .unwrap();

        // Key should be free for reuse.
        let outcome = store.submit(&sub).await.unwrap();
        assert!(outcome.is_inserted());
    }

    #[tokio::test]
    async fn history_stats_computation() {
        let store = test_store().await;

        // Complete a few tasks.
        for i in 0..3 {
            let sub = make_submission(&format!("stat-{i}"), Priority::NORMAL);
            store.submit(&sub).await.unwrap();
            let task = store.pop_next().await.unwrap().unwrap();
            store
                .complete(
                    task.id,
                    &TaskResult {
                        actual_read_bytes: 1000,
                        actual_write_bytes: 500,
                    },
                )
                .await
                .unwrap();
        }

        let stats = store.history_stats("test").await.unwrap();
        assert_eq!(stats.count, 3);
        assert!(stats.failure_rate == 0.0);
    }

    #[tokio::test]
    async fn pause_and_resume() {
        let store = test_store().await;
        store
            .submit(&make_submission("pausable", Priority::NORMAL))
            .await
            .unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        store.pause(task.id).await.unwrap();
        let paused = store.paused_tasks().await.unwrap();
        assert_eq!(paused.len(), 1);
        assert_eq!(paused[0].status, TaskStatus::Paused);

        store.resume(task.id).await.unwrap();
        let pending = store.pending_tasks(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn open_with_custom_config() {
        let store = TaskStore::open_memory().await.unwrap();
        // Basic smoke test — store is usable.
        let count = store.pending_count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_task() {
        let store = test_store().await;
        let sub = make_submission("del-me", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();

        let task = store.task_by_key(&key).await.unwrap().unwrap();
        assert!(store.delete(task.id).await.unwrap());
        assert!(store.task_by_key(&key).await.unwrap().is_none());

        // Deleting again returns false.
        assert!(!store.delete(task.id).await.unwrap());
    }

    #[tokio::test]
    async fn task_by_id_lookup() {
        let store = test_store().await;
        let sub = make_submission("by-id", Priority::NORMAL);
        let id = store.submit(&sub).await.unwrap().id().unwrap();

        let task = store.task_by_id(id).await.unwrap().unwrap();
        assert_eq!(task.id, id);
        assert_eq!(task.key, sub.effective_key());

        // Non-existent id returns None.
        assert!(store.task_by_id(9999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn history_by_id_lookup() {
        let store = test_store().await;
        let sub = make_submission("hist-id", Priority::NORMAL);
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();

        store
            .complete(
                task.id,
                &TaskResult {
                    actual_read_bytes: 100,
                    actual_write_bytes: 50,
                },
            )
            .await
            .unwrap();

        // Fetch from history by key to get the history id.
        let hist = store.history_by_key(&sub.effective_key()).await.unwrap();
        assert_eq!(hist.len(), 1);
        let hist_id = hist[0].id;

        let record = store.history_by_id(hist_id).await.unwrap().unwrap();
        assert_eq!(record.key, sub.effective_key());
        assert_eq!(record.actual_read_bytes, Some(100));

        // Non-existent id returns None.
        assert!(store.history_by_id(9999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn requeue_running_task() {
        let store = test_store().await;
        let sub = make_submission("rq", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();
        assert_eq!(task.status, TaskStatus::Running);

        store.requeue(task.id).await.unwrap();
        let t = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(t.status, TaskStatus::Pending);
        assert!(t.started_at.is_none());
    }

    #[tokio::test]
    async fn peek_next_does_not_modify_status() {
        let store = test_store().await;
        let sub = make_submission("peek-me", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();

        // Peek should return the task but leave it pending.
        let peeked = store.peek_next().await.unwrap().unwrap();
        assert_eq!(peeked.key, key);
        assert_eq!(peeked.status, TaskStatus::Pending);

        // Verify it's still pending in the store.
        let t = store.task_by_key(&key).await.unwrap().unwrap();
        assert_eq!(t.status, TaskStatus::Pending);
        assert!(t.started_at.is_none());

        // Peeking again returns the same task.
        let peeked2 = store.peek_next().await.unwrap().unwrap();
        assert_eq!(peeked2.id, peeked.id);
    }

    #[tokio::test]
    async fn peek_next_empty_queue() {
        let store = test_store().await;
        assert!(store.peek_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pop_by_id_claims_pending_task() {
        let store = test_store().await;
        let sub = make_submission("claim-me", Priority::NORMAL);
        let key = sub.effective_key();
        let id = store.submit(&sub).await.unwrap().id().unwrap();

        let task = store.pop_by_id(id).await.unwrap().unwrap();
        assert_eq!(task.key, key);
        assert_eq!(task.status, TaskStatus::Running);
        assert!(task.started_at.is_some());
    }

    #[tokio::test]
    async fn pop_by_id_returns_none_if_already_running() {
        let store = test_store().await;
        let sub = make_submission("already-taken", Priority::NORMAL);
        store.submit(&sub).await.unwrap();

        // Pop via pop_next first.
        let task = store.pop_next().await.unwrap().unwrap();

        // pop_by_id on the same task should return None (already running).
        assert!(store.pop_by_id(task.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pop_by_id_returns_none_for_nonexistent() {
        let store = test_store().await;
        assert!(store.pop_by_id(9999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn peek_then_pop_by_id_workflow() {
        let store = test_store().await;
        let sub = make_submission("peek-pop", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();

        // Peek, then claim.
        let peeked = store.peek_next().await.unwrap().unwrap();
        let claimed = store.pop_by_id(peeked.id).await.unwrap().unwrap();
        assert_eq!(claimed.key, key);
        assert_eq!(claimed.status, TaskStatus::Running);

        // Queue should now be empty for peek.
        assert!(store.peek_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn prune_by_count() {
        let store = test_store().await;

        // Complete 5 tasks.
        for i in 0..5 {
            let sub = make_submission(&format!("prune-{i}"), Priority::NORMAL);
            store.submit(&sub).await.unwrap();
            let task = store.pop_next().await.unwrap().unwrap();
            store
                .complete(
                    task.id,
                    &TaskResult {
                        actual_read_bytes: 0,
                        actual_write_bytes: 0,
                    },
                )
                .await
                .unwrap();
        }

        let hist = store.history(100, 0).await.unwrap();
        assert_eq!(hist.len(), 5);

        let deleted = store.prune_history_by_count(3).await.unwrap();
        assert_eq!(deleted, 2);

        let hist = store.history(100, 0).await.unwrap();
        assert_eq!(hist.len(), 3);
    }

    #[tokio::test]
    async fn submit_batch_inserts_all() {
        let store = test_store().await;
        let subs: Vec<_> = (0..5)
            .map(|i| make_submission(&format!("batch-{i}"), Priority::NORMAL))
            .collect();

        let results = store.submit_batch(&subs).await.unwrap();
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.is_inserted()));

        let count = store.pending_count().await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn submit_batch_dedup() {
        let store = test_store().await;
        let sub = make_submission("dup", Priority::NORMAL);

        let results = store
            .submit_batch(&[sub.clone(), sub.clone()])
            .await
            .unwrap();
        assert!(results[0].is_inserted());
        assert_eq!(results[1], SubmitOutcome::Duplicate); // dedup within same batch

        // Submitting again should also dedup.
        let results = store.submit_batch(&[sub]).await.unwrap();
        assert_eq!(results[0], SubmitOutcome::Duplicate);
    }

    #[tokio::test]
    async fn submit_batch_empty() {
        let store = test_store().await;
        let results = store.submit_batch(&[]).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn task_lookup_active() {
        let store = test_store().await;
        let sub = make_submission("lookup-active", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();

        let result = store.task_lookup(&key).await.unwrap();
        assert!(matches!(result, TaskLookup::Active(ref r) if r.status == TaskStatus::Pending));

        // Pop so it's running.
        store.pop_next().await.unwrap();
        let result = store.task_lookup(&key).await.unwrap();
        assert!(matches!(result, TaskLookup::Active(ref r) if r.status == TaskStatus::Running));
    }

    #[tokio::test]
    async fn task_lookup_history() {
        let store = test_store().await;
        let sub = make_submission("lookup-hist", Priority::NORMAL);
        let key = sub.effective_key();
        store.submit(&sub).await.unwrap();
        let task = store.pop_next().await.unwrap().unwrap();
        store
            .complete(
                task.id,
                &TaskResult {
                    actual_read_bytes: 0,
                    actual_write_bytes: 0,
                },
            )
            .await
            .unwrap();

        let result = store.task_lookup(&key).await.unwrap();
        assert!(
            matches!(result, TaskLookup::History(ref r) if r.status == HistoryStatus::Completed)
        );
    }

    #[tokio::test]
    async fn task_lookup_not_found() {
        let store = test_store().await;
        let key = crate::task::generate_dedup_key("nope", Some(b"nope"));
        let result = store.task_lookup(&key).await.unwrap();
        assert!(matches!(result, TaskLookup::NotFound));
    }

    #[tokio::test]
    async fn submit_batch_rejects_oversized_payload() {
        let store = test_store().await;
        let sub = make_submission("ok", Priority::NORMAL);
        let big = TaskSubmission {
            task_type: "test".into(),
            key: Some("big".into()),
            priority: Priority::NORMAL,
            payload: Some(vec![0u8; MAX_PAYLOAD_BYTES + 1]),
            expected_read_bytes: 0,
            expected_write_bytes: 0,
        };

        // The oversized payload should fail the entire batch — no partial inserts.
        let err = store.submit_batch(&[sub.clone(), big]).await.unwrap_err();
        assert!(matches!(err, StoreError::PayloadTooLarge));

        // The first task should NOT have been committed (transaction rolled back).
        let count = store.pending_count().await.unwrap();
        assert_eq!(count, 0);
    }
}
