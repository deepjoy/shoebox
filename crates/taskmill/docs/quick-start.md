# Quick Start

## Installation

Add taskmill to your `Cargo.toml`:

```toml
[dependencies]
taskmill = { path = "crates/taskmill" }
```

To disable platform resource monitoring (e.g., for mobile targets):

```toml
[dependencies]
taskmill = { path = "crates/taskmill", default-features = false }
```

## Implement an executor

Each task type needs a `TaskExecutor` implementation. The executor receives a `TaskContext` containing:

- `record` — the full `TaskRecord` with payload (up to 1 MiB), priority, retry count, etc.
- `token` — a `CancellationToken` for preemption support
- `progress` — a `ProgressReporter` for reporting progress back to the scheduler
- Shared application state (if registered via `.app_state()` or `register_state()`)

```rust
use std::sync::Arc;
use taskmill::{TaskExecutor, TaskContext, TaskResult, TaskError};

struct ImageResizer;

impl TaskExecutor for ImageResizer {
    async fn execute<'a>(
        &'a self,
        ctx: &'a TaskContext,
    ) -> Result<TaskResult, TaskError> {
        // Deserialize your payload
        let data: Option<serde_json::Value> = ctx.record.deserialize_payload()?;

        // Check for preemption at yield points
        if ctx.token.is_cancelled() {
            return Err(TaskError {
                message: "preempted".into(),
                retryable: true,
                actual_read_bytes: 0,
                actual_write_bytes: 0,
            });
        }

        // Report progress
        ctx.progress.report(0.5, Some("resizing".into()));

        // Do work...

        Ok(TaskResult {
            actual_read_bytes: 4096,
            actual_write_bytes: 1024,
        })
    }
}
```

## Build and run the scheduler

```rust
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use taskmill::{Scheduler, Priority, TaskSubmission, ShutdownMode};

#[tokio::main]
async fn main() {
    // Build the scheduler — opens the DB, registers executors, starts monitoring.
    let scheduler = Scheduler::builder()
        .store_path("tasks.db")
        .executor("resize", Arc::new(ImageResizer))
        .max_concurrency(8)
        .shutdown_mode(ShutdownMode::Graceful(Duration::from_secs(10)))
        .with_resource_monitoring()
        .build()
        .await
        .unwrap();

    // Scheduler is Clone — share freely across async tasks and Tauri state.
    let sched = scheduler.clone();

    // Subscribe to lifecycle events for logging or UI updates.
    let mut events = scheduler.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            println!("Event: {:?}", event);
        }
    });

    // Submit a single task with a typed payload.
    scheduler.submit(&TaskSubmission::with_payload(
        "resize",
        Priority::NORMAL,
        &serde_json::json!({"path": "/photos/image.jpg", "width": 300}),
        4096,  // expected read bytes
        1024,  // expected write bytes
    ).unwrap()).await.unwrap();

    // Submit tasks in bulk (single SQLite transaction).
    let paths = vec!["/a.jpg", "/b.jpg", "/c.jpg"];
    let batch: Vec<_> = paths.iter().map(|p| {
        TaskSubmission::with_payload(
            "resize",
            Priority::NORMAL,
            &serde_json::json!({"path": p}),
            4096, 1024,
        ).unwrap()
    }).collect();
    let ids = scheduler.submit_batch(&batch).await.unwrap();
    // ids[i] is Some(row_id) if inserted, None if deduplicated.

    // Run the scheduler loop (blocks until the token is cancelled).
    let token = CancellationToken::new();
    scheduler.run(token).await;
}
```

## Using typed tasks

For stronger type safety, implement the `TypedTask` trait:

```rust
use serde::{Serialize, Deserialize};
use taskmill::{TypedTask, Priority};

#[derive(Serialize, Deserialize)]
struct ResizeTask {
    path: String,
    width: u32,
}

impl TypedTask for ResizeTask {
    const TASK_TYPE: &'static str = "resize";

    fn expected_read_bytes(&self) -> i64 { 4096 }
    fn expected_write_bytes(&self) -> i64 { 1024 }
    fn priority(&self) -> Priority { Priority::NORMAL }
}

// Submit:
scheduler.submit_typed(&ResizeTask {
    path: "/photos/img.jpg".into(),
    width: 300,
}).await?;

// In the executor:
let task: Option<ResizeTask> = ctx.deserialize_typed()?;
```

## Manual wiring

For full control over components, use `Scheduler::new()` directly:

```rust
use std::sync::Arc;
use taskmill::{
    CompositePressure, Scheduler, SchedulerConfig,
    TaskStore, TaskTypeRegistry, ThrottlePolicy,
};

let store = TaskStore::open("tasks.db").await.unwrap();

let mut registry = TaskTypeRegistry::new();
registry.register("resize", Arc::new(ImageResizer));

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

## Tauri integration

Taskmill is designed for Tauri. A typical setup:

```rust
use tauri::Manager;
use taskmill::{Scheduler, SchedulerSnapshot, StoreError};

// Expose scheduler status to the frontend.
#[tauri::command]
async fn scheduler_status(
    scheduler: tauri::State<'_, Scheduler>,
) -> Result<SchedulerSnapshot, StoreError> {
    scheduler.snapshot().await
}

// Bridge events to the frontend.
fn setup_events(app: &tauri::App, scheduler: &Scheduler) {
    let mut events = scheduler.subscribe();
    let handle = app.handle().clone();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            handle.emit("taskmill-event", &event).unwrap();
        }
    });
}
```
