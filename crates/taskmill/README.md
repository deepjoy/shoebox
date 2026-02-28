# Taskmill

Adaptive priority work scheduler with IO-aware concurrency and SQLite persistence.

Taskmill is an async task queue for Rust applications that persists work to SQLite,
schedules by priority with IO-budget awareness, and supports preemption, retries, and
composable backpressure. It is designed for desktop apps (Tauri, etc.) and background
services where tasks have measurable IO costs and the system needs to avoid saturating
disk throughput.

## Features

- **SQLite persistence** — tasks survive process restarts; tasks left running during a crash are recovered to pending on startup
- **Key-based deduplication** — `UNIQUE(key)` constraint with `INSERT OR IGNORE` prevents duplicate work
- **Priority queue** — 256 levels (0 = highest), popped via `ORDER BY priority ASC, id ASC`
- **Expected/actual IO tracking** — submit estimated IO; executors report actual bytes on completion
- **IO-aware scheduling** — compares running task IO estimates against system throughput before dispatching more work
- **Cross-platform resource monitoring** — CPU and disk IO via `sysinfo` on Linux, macOS, and Windows (optional, feature-gated)
- **Task type registry** — `TaskExecutor` trait lets consumers register executors by name; enables restart recovery
- **Composable backpressure** — any `PressureSource` impl feeds into a `ThrottlePolicy` that gates dispatch by priority
- **Preemption** — high-priority tasks cancel lower-priority running work via `CancellationToken`
- **Task cancellation** — cancel running or queued tasks via `Scheduler::cancel(task_id)`
- **Retries** — failed retryable tasks are requeued at the same priority with `retry_count += 1`
- **Lifecycle events** — subscribe to `SchedulerEvent` for UI integration (dispatch, complete, fail, preempt, cancel, progress)
- **Progress reporting** — executors report progress via `ProgressReporter`; throughput-based extrapolation fills gaps
- **Graceful shutdown** — configurable drain timeout waits for running tasks before force-cancelling
- **Builder pattern** — ergonomic `Scheduler::builder()` hides `Arc<Mutex<...>>` wiring
- **Clone-friendly scheduler** — `Scheduler` is `Clone` for easy sharing in Tauri state and across async tasks
- **Serde on all public types** — `Serialize`/`Deserialize` always enabled for Tauri IPC compatibility
- **Serde-friendly errors** — `StoreError` is serializable for direct use in Tauri command returns
- **Typed payloads** — `TaskSubmission::with_payload()` and `TaskRecord::deserialize_payload()` for ergonomic typed data
- **Typed timestamps** — `chrono::DateTime<Utc>` instead of raw strings
- **History retention** — configurable auto-pruning by count or age to keep the history table bounded
- **Full query APIs** — running, pending, paused, history, by-type, by-key, stats, throughput

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
taskmill = { path = "crates/taskmill" }

# Without platform resource monitoring (e.g., for mobile targets):
# taskmill = { path = "crates/taskmill", default-features = false }
```

### Implement an executor

Each task type needs a `TaskExecutor` implementation. The executor receives a
`TaskContext` containing the full `TaskRecord` (including an opaque `payload` blob
up to 8 KiB), a `CancellationToken` for preemption support, and a `ProgressReporter`
for reporting progress back to the scheduler.

```rust
use std::sync::Arc;
use taskmill::{TaskExecutor, TaskContext, TaskResult, TaskError};

struct MyExecutor;

impl TaskExecutor for MyExecutor {
    async fn execute<'a>(
        &'a self,
        ctx: &'a TaskContext,
    ) -> Result<TaskResult, TaskError> {
        // Deserialize payload from ctx.record, do work,
        // check ctx.token.is_cancelled() periodically,
        // and report progress via ctx.progress.
        ctx.progress.report(0.5, Some("halfway done".into()));
        Ok(TaskResult {
            actual_read_bytes: 4096,
            actual_write_bytes: 1024,
        })
    }
}
```

### Wire up with the builder

```rust
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use taskmill::{Scheduler, Priority, TaskSubmission, ShutdownMode};

#[tokio::main]
async fn main() {
    // Build the scheduler — opens DB, registers executors, starts resource monitoring.
    let scheduler = Scheduler::builder()
        .store_path("tasks.db")
        .executor("my-task", Arc::new(MyExecutor))
        .max_concurrency(8)
        .shutdown_mode(ShutdownMode::Graceful(Duration::from_secs(10)))
        .with_resource_monitoring()
        .build()
        .await
        .unwrap();

    // Scheduler is Clone — share freely across async tasks and Tauri state.
    let sched2 = scheduler.clone();

    // Subscribe to lifecycle events (for UI updates, logging, etc.).
    let mut events = scheduler.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            println!("Event: {:?}", event);
        }
    });

    // Submit tasks with typed payloads.
    scheduler.submit(&TaskSubmission::with_payload(
        "my-task",
        Priority::NORMAL,
        &serde_json::json!({"path": "/photos/image.jpg"}),
        4096,
        1024,
    ).unwrap()).await.unwrap();

    // Run the scheduler loop (blocks until token is cancelled).
    let token = CancellationToken::new();
    scheduler.run(token).await;
}
```

### Advanced: manual wiring

For full control, use `Scheduler::new()` directly:

```rust
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use taskmill::{
    CompositePressure, Scheduler, SchedulerConfig,
    TaskStore, TaskTypeRegistry, ThrottlePolicy,
};

let store = TaskStore::open("tasks.db").await.unwrap();

let mut registry = TaskTypeRegistry::new();
registry.register("my-task", Arc::new(MyExecutor));

let pressure = CompositePressure::new();
let policy = ThrottlePolicy::default_three_tier();

let scheduler = Scheduler::new(
    store,
    SchedulerConfig::default(),
    Arc::new(registry),
    pressure,
    policy,
);
```

## Priority levels

| Constant     | Value | Behavior                                         |
|--------------|-------|--------------------------------------------------|
| `REALTIME`   | 0     | Never throttled. Triggers preemption of lower work.|
| `HIGH`       | 64    | Throttled only under extreme pressure.            |
| `NORMAL`     | 128   | Standard background operations.                   |
| `BACKGROUND` | 192   | Pauses under significant load.                    |
| `IDLE`       | 255   | Runs only when the system is otherwise idle.      |

Custom values between tiers are supported: `Priority::new(100)`.

## Lifecycle events

Subscribe via `scheduler.subscribe()` to receive `SchedulerEvent` variants:

| Event       | When                                           |
|-------------|------------------------------------------------|
| `Dispatched`| Task popped and executor spawned               |
| `Completed` | Task finished successfully                     |
| `Failed`    | Task failed (includes `will_retry` flag)       |
| `Preempted` | Task paused for higher-priority work           |
| `Cancelled` | Task cancelled via `scheduler.cancel(id)`      |
| `Progress`  | Progress update from a running executor        |

In a Tauri app, bridge these to the frontend:

```rust
let mut events = scheduler.subscribe();
let handle = app_handle.clone();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        handle.emit("taskmill-event", &event).unwrap();
    }
});
```

## Progress reporting

Executors receive a `ProgressReporter` via `ctx.progress`. Report progress from
inside the executor:

```rust
// Inside your TaskExecutor::execute() impl:
ctx.progress.report(0.5, Some("processing images".into()));

// Or use fraction-based reporting:
ctx.progress.report_fraction(processed, total, None);

// For tasks that don't report progress, the scheduler extrapolates progress
// based on elapsed time vs. historical average duration for the task type.
```

Query estimated progress for all running tasks:

```rust
let progress = scheduler.estimated_progress().await;
for p in &progress {
    println!("{}: {:.0}% (reported: {:?}, extrapolated: {:?})",
        p.key, p.percent * 100.0, p.reported_percent, p.extrapolated_percent);
}
```

## Graceful shutdown

By default, the scheduler hard-cancels all running tasks on shutdown. For desktop
apps, configure graceful shutdown to wait for in-progress work:

```rust
use std::time::Duration;
use taskmill::ShutdownMode;

let scheduler = Scheduler::builder()
    .store_path("tasks.db")
    .shutdown_mode(ShutdownMode::Graceful(Duration::from_secs(30)))
    .build()
    .await?;
```

In graceful mode, the scheduler:
1. Stops dispatching new tasks
2. Waits for running tasks to complete (up to the timeout)
3. Force-cancels any remaining tasks after the timeout
4. Stops the resource sampler background task

## Task cancellation

Cancel running or queued tasks:

```rust
// Cancel by task ID — works for running, pending, or paused tasks.
let was_cancelled = scheduler.cancel(task_id).await?;
```

Running tasks have their `CancellationToken` triggered. Pending/paused tasks are
deleted from the queue.

## Deduplication

Tasks are deduplicated by a SHA-256 key derived from the task type and payload (or an
explicit key when provided). The `tasks` table has a `UNIQUE(key)` constraint and
inserts use `INSERT OR IGNORE`. Submitting a task that hashes to the same key as one
already pending, running, or paused returns `Ok(None)` instead of creating a duplicate.

The task type is always incorporated into the hash, so different task types can never
collide — even when using the same explicit key or identical payloads of different types.

Once a task completes or fails (moved to `task_history`), the key is freed and can be
resubmitted.

## Typed payloads

Submit tasks with structured data:

```rust
use taskmill::{TaskSubmission, Priority};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct ScanTask { path: String, depth: u32 }

// Key is auto-generated from hash(task_type + payload).
let sub = TaskSubmission::with_payload(
    "scan",
    Priority::NORMAL,
    &ScanTask { path: "/photos".into(), depth: 3 },
    50_000,
    10_000,
)?;
scheduler.submit(&sub).await?;

// In the executor:
let data: Option<ScanTask> = record.deserialize_payload()?;
```

## IO tracking

Each `TaskSubmission` includes `expected_read_bytes` and `expected_write_bytes` — the
caller's estimate of how much IO the task will perform. On completion, the executor
reports `actual_read_bytes` and `actual_write_bytes` in `TaskResult`.

These values serve two purposes:

1. **Scheduling** — the scheduler sums expected IO across running tasks and compares
   against observed system throughput (from `ResourceReader`). New work is deferred
   when running IO exceeds 80% of system capacity over a 2-second budget window.
2. **Learning** — history queries like `avg_throughput()` and `history_stats()` use
   actual IO to compute per-type averages, enabling callers to refine future estimates.

## Resource monitoring

Taskmill uses the `sysinfo` crate (optional, enabled by default) for cross-platform
CPU and disk IO monitoring:

- **Linux**: full CPU and disk IO stats
- **macOS**: full CPU and disk IO stats
- **Windows**: full CPU and disk IO stats

Enable via the builder:

```rust
Scheduler::builder()
    .with_resource_monitoring()  // uses platform_sampler() automatically
    .build()
    .await?;
```

Or provide a custom `ResourceSampler` implementation (works without the `sysinfo-monitor` feature):

```rust
Scheduler::builder()
    .resource_sampler(Box::new(my_custom_sampler))
    .build()
    .await?;
```

The resource monitoring system is split into two traits:

- **`ResourceSampler`** — takes raw platform samples (implemented by your monitor)
- **`ResourceReader`** — read-only access to EWMA-smoothed snapshots (consumed by the scheduler)

The `SmoothedReader` bridges the two, with a background loop applying EWMA smoothing
(alpha=0.3, configurable via `SamplerConfig`).

## Backpressure

Backpressure is composed from two independent mechanisms:

### PressureSource + ThrottlePolicy

Implement the `PressureSource` trait to expose a `0.0..=1.0` pressure signal from any
external source (API load, memory pressure, queue depth, etc.). Add sources to
`CompositePressure`; the aggregate is the **maximum** across all sources.

`ThrottlePolicy` maps `(priority, pressure)` to throttle decisions. The default
three-tier policy:

- `BACKGROUND` and below: throttled at >50% pressure
- `NORMAL` and below: throttled at >75% pressure
- `HIGH` and `REALTIME`: never throttled

### IO budget

When resource monitoring is enabled, the scheduler reads EWMA-smoothed disk
throughput and defers tasks whose cumulative expected IO would exceed 80% of observed
capacity. This is independent of the pressure/policy mechanism.

## Preemption

When a task with priority at or above `SchedulerConfig::preempt_priority` (default:
`REALTIME`) is submitted, the scheduler:

1. Cancels the `CancellationToken` of all active tasks with lower priority
2. Pauses those tasks in the store (status → `paused`, `started_at` cleared)
3. Emits `Preempted` events for each affected task
4. Resumes paused tasks only when no active preemptors remain (prevents thrashing)

Executors should check `token.is_cancelled()` at yield points and return early.

## Retries

When an executor returns `TaskError { retryable: true, .. }`:

- If `retry_count < max_retries` (default 3): the task is set back to `pending` with
  `retry_count += 1` and `last_error` recorded. It re-enters the priority queue at its
  original priority.
- Otherwise: the task is moved to `task_history` with status `failed`.

Non-retryable errors move to history immediately.

## History retention

Configure automatic pruning to prevent unbounded history growth:

```rust
use taskmill::{StoreConfig, RetentionPolicy};

let config = StoreConfig {
    retention_policy: Some(RetentionPolicy::MaxCount(10_000)),
    ..Default::default()
};

// Or by age:
let config = StoreConfig {
    retention_policy: Some(RetentionPolicy::MaxAgeDays(90)),
    ..Default::default()
};
```

Manual pruning is also available:

```rust
let deleted = store.prune_history_by_count(5_000).await?;
let deleted = store.prune_history_by_age(30).await?;
```

## Query APIs

All queries are available on `TaskStore` (accessed via `scheduler.store()`):

| Method                              | Returns                                 |
|-------------------------------------|-----------------------------------------|
| `running_tasks()`                   | All running tasks, ordered by priority  |
| `running_count()`                   | Count of running tasks                  |
| `pending_tasks(limit)`              | Pending tasks, ordered by priority/age  |
| `pending_count()`                   | Count of pending tasks                  |
| `pending_by_type(task_type)`        | Pending tasks filtered by type          |
| `paused_tasks()`                    | All paused tasks                        |
| `task_by_key(key)`                  | Look up an active task by dedup key     |
| `running_io_totals()`              | Sum of expected read/write for running  |
| `history(limit, offset)`            | Paginated history, newest first         |
| `history_by_type(task_type, limit)` | History filtered by type                |
| `history_by_key(key)`               | All past runs of a key                  |
| `failed_tasks(limit)`               | Recent failures                         |
| `history_stats(task_type)`          | Aggregate stats: count, avg duration, avg IO, failure rate |
| `avg_throughput(task_type, limit)`  | Average read/write bytes per second     |
| `prune_history_by_count(keep)`      | Prune history to N most recent          |
| `prune_history_by_age(days)`        | Prune history older than N days         |

## Configuration

### SchedulerConfig

| Field                   | Default      | Description                                    |
|-------------------------|--------------|------------------------------------------------|
| `max_concurrency`       | 4            | Maximum concurrent running tasks (runtime-adjustable via `set_max_concurrency`) |
| `max_retries`           | 3            | Retry limit before permanent failure           |
| `preempt_priority`      | `REALTIME`   | Tasks at or above this trigger preemption      |
| `poll_interval`         | 500ms        | Sleep between scheduler dispatch cycles        |
| `throughput_sample_size`| 20           | Recent completions used for throughput estimate |
| `shutdown_mode`         | `Hard`       | `Hard` (cancel all) or `Graceful(Duration)` (wait then cancel) |

### StoreConfig

| Field              | Default | Description                                                |
|--------------------|---------|------------------------------------------------------------|
| `max_connections`  | 16      | SQLite connection pool size                                |
| `retention_policy` | None    | `MaxCount(n)` or `MaxAgeDays(n)` for auto-pruning         |

### SamplerConfig

| Field      | Default | Description                              |
|------------|---------|------------------------------------------|
| `interval` | 1s      | How often to sample system resources     |
| `ewma_alpha` | 0.3   | EWMA smoothing factor (higher = more responsive) |

## Feature flags

| Feature            | Default | Description |
|--------------------|---------|-------------|
| `sysinfo-monitor`  | Yes     | Enables the built-in `SysinfoSampler` for cross-platform CPU and disk IO monitoring. Disable for mobile targets or when providing a custom `ResourceSampler`. |

## License

MIT
