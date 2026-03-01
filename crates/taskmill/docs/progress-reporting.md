# Progress Reporting

Taskmill provides real-time progress tracking for running tasks, combining executor-reported values with throughput-based extrapolation.

## Reporting from executors

Executors receive a `ProgressReporter` via `ctx.progress`:

```rust
impl TaskExecutor for MyExecutor {
    async fn execute<'a>(
        &'a self, ctx: &'a TaskContext,
    ) -> Result<TaskResult, TaskError> {
        let items = get_work_items();

        for (i, item) in items.iter().enumerate() {
            process(item).await;

            // Percentage-based (0.0 to 1.0)
            ctx.progress.report(
                (i + 1) as f32 / items.len() as f32,
                Some(format!("processed {}/{}", i + 1, items.len())),
            );
        }

        Ok(TaskResult { actual_read_bytes: 0, actual_write_bytes: 0 })
    }
}
```

### Fraction-based reporting

For count-based progress:

```rust
ctx.progress.report_fraction(processed, total, Some("importing".into()));
// Automatically computes: processed as f32 / total as f32
```

## Progress events

Every `report()` call emits a `SchedulerEvent::Progress`:

```rust
SchedulerEvent::Progress {
    task_id: 42,
    task_type: "resize".into(),
    key: "abc123".into(),
    percent: 0.5,
    message: Some("resizing".into()),
}
```

Subscribe to events for real-time UI updates:

```rust
let mut events = scheduler.subscribe();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        if let SchedulerEvent::Progress { task_id, percent, message, .. } = event {
            update_ui(task_id, percent, message);
        }
    }
});
```

## Throughput-based extrapolation

For tasks that don't report progress (or between reports), the scheduler extrapolates based on historical data:

1. Fetch `history_stats(task_type)` to get the average duration for this task type.
2. Compute throughput: `1.0 / avg_duration_ms` (completion fraction per millisecond).
3. Multiply by elapsed time since `started_at` to get an extrapolated percentage.
4. If the executor has reported partial progress, blend the historical throughput with the current rate.
5. Cap at 99% — extrapolation never reaches 100% to avoid false "complete" signals.

This means even tasks with no explicit progress reporting show movement in UI dashboards.

## Querying progress

### All running tasks

```rust
let progress = scheduler.estimated_progress().await;
for p in &progress {
    println!("{} ({}): {:.0}%", p.task_type, p.key, p.percent * 100.0);
    // p.reported_percent  — last executor-reported value (if any)
    // p.extrapolated_percent — throughput-based estimate (if any)
    // p.percent — best available: reported if present, else extrapolated
}
```

### Via snapshot

The `SchedulerSnapshot` includes progress for all running tasks:

```rust
let snap = scheduler.snapshot().await?;
for p in &snap.progress {
    println!("{}: {:.0}%", p.key, p.percent * 100.0);
}
```

## Lifecycle events

All scheduler state changes are broadcast as `SchedulerEvent` variants:

| Event | When |
|-------|------|
| `Dispatched { task_id, task_type, key }` | Task popped from queue and executor spawned |
| `Completed { task_id, task_type, key }` | Task finished successfully |
| `Failed { task_id, task_type, key, error, will_retry }` | Task failed (includes whether it will be retried) |
| `Preempted { task_id, task_type, key }` | Task paused for higher-priority work |
| `Cancelled { task_id, task_type, key }` | Task cancelled via `scheduler.cancel()` |
| `Progress { task_id, task_type, key, percent, message }` | Progress update from executor |
| `Paused` | Scheduler globally paused via `pause_all()` |
| `Resumed` | Scheduler resumed via `resume_all()` |

### Tauri bridge

Bridge events to the frontend in a Tauri app:

```rust
let mut events = scheduler.subscribe();
let handle = app_handle.clone();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        handle.emit("taskmill-event", &event).unwrap();
    }
});
```

All events derive `Serialize`, so they can be sent directly over Tauri IPC.

## Dashboard snapshot

For UI dashboards, `Scheduler::snapshot()` gathers all scheduler state in a single call:

```rust
let snap = scheduler.snapshot().await?;
// snap.running          — Vec<TaskRecord> of currently executing tasks
// snap.pending_count    — number of tasks waiting to dispatch
// snap.paused_count     — number of preempted tasks
// snap.progress         — Vec<EstimatedProgress> for every running task
// snap.pressure         — aggregate backpressure (0.0–1.0)
// snap.pressure_breakdown — per-source diagnostics: Vec<(String, f32)>
// snap.max_concurrency  — current concurrency limit
// snap.is_paused        — whether the scheduler is globally paused
```

Return directly from a Tauri command:

```rust
#[tauri::command]
async fn scheduler_status(
    scheduler: tauri::State<'_, Scheduler>,
) -> Result<SchedulerSnapshot, StoreError> {
    scheduler.snapshot().await
}
```
