-- Active queue: pending, running, and paused tasks.
-- The UNIQUE(key) constraint provides key-based deduplication —
-- submitting a task with an existing key is a no-op (INSERT OR IGNORE).
-- When a duplicate is submitted while the existing task is running or paused,
-- the requeue flag is set so the task re-runs after the current execution.
CREATE TABLE IF NOT EXISTS tasks (
    id                   INTEGER PRIMARY KEY,
    task_type            TEXT    NOT NULL,
    key                  TEXT    NOT NULL,
    priority             INTEGER NOT NULL,
    status               TEXT    NOT NULL DEFAULT 'pending',
    payload              BLOB,
    expected_read_bytes  INTEGER NOT NULL DEFAULT 0,
    expected_write_bytes INTEGER NOT NULL DEFAULT 0,
    retry_count          INTEGER NOT NULL DEFAULT 0,
    last_error           TEXT,
    created_at           TEXT    NOT NULL DEFAULT (datetime('now')),
    started_at           TEXT,
    requeue              INTEGER NOT NULL DEFAULT 0,
    requeue_priority     INTEGER,
    UNIQUE(key)
);

-- Index for the scheduler hot path: pop highest-priority pending task.
CREATE INDEX IF NOT EXISTS idx_tasks_pending
    ON tasks (status, priority ASC, id ASC)
    WHERE status = 'pending';

-- Completed and failed task history for queries and IO learning.
CREATE TABLE IF NOT EXISTS task_history (
    id                   INTEGER PRIMARY KEY,
    task_type            TEXT    NOT NULL,
    key                  TEXT    NOT NULL,
    priority             INTEGER NOT NULL,
    status               TEXT    NOT NULL,
    payload              BLOB,
    expected_read_bytes  INTEGER NOT NULL DEFAULT 0,
    expected_write_bytes INTEGER NOT NULL DEFAULT 0,
    actual_read_bytes    INTEGER,
    actual_write_bytes   INTEGER,
    retry_count          INTEGER NOT NULL DEFAULT 0,
    last_error           TEXT,
    created_at           TEXT    NOT NULL,
    started_at           TEXT,
    completed_at         TEXT    NOT NULL DEFAULT (datetime('now')),
    duration_ms          INTEGER
);

-- Index for IO learning: recent completions by task type.
CREATE INDEX IF NOT EXISTS idx_history_type_completed
    ON task_history (task_type, completed_at DESC)
    WHERE status = 'completed';

-- Index for task lookup by key (used by task dedup and status checks).
CREATE INDEX IF NOT EXISTS idx_history_key
    ON task_history (key, completed_at DESC);

-- Index for paginating and pruning history by completion time.
CREATE INDEX IF NOT EXISTS idx_history_completed
    ON task_history (completed_at DESC);

-- Index for filtering history by status (e.g. listing failed tasks).
CREATE INDEX IF NOT EXISTS idx_history_status
    ON task_history (status, completed_at DESC);
