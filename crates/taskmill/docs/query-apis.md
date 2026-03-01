# Query APIs

All queries are available on `TaskStore`, accessed via `scheduler.store()`.

## Active task queries

| Method | Returns | Description |
|--------|---------|-------------|
| `running_tasks()` | `Vec<TaskRecord>` | All running tasks, ordered by priority. |
| `running_count()` | `i64` | Count of running tasks. |
| `pending_tasks(limit)` | `Vec<TaskRecord>` | Pending tasks, ordered by priority then age. |
| `pending_count()` | `i64` | Count of pending tasks. |
| `pending_by_type(task_type)` | `Vec<TaskRecord>` | Pending tasks filtered by type. |
| `paused_tasks()` | `Vec<TaskRecord>` | All paused (preempted) tasks. |
| `paused_count()` | `i64` | Count of paused tasks. |
| `task_by_id(id)` | `Option<TaskRecord>` | Look up an active task by row ID. |
| `task_by_key(key)` | `Option<TaskRecord>` | Look up an active task by dedup key. |
| `running_io_totals()` | `(i64, i64)` | Sum of `(expected_read_bytes, expected_write_bytes)` across running tasks. |

## History queries

| Method | Returns | Description |
|--------|---------|-------------|
| `history(limit, offset)` | `Vec<TaskHistoryRecord>` | Paginated history, newest first. |
| `history_by_type(task_type, limit)` | `Vec<TaskHistoryRecord>` | History filtered by task type. |
| `history_by_key(key)` | `Vec<TaskHistoryRecord>` | All past runs matching a dedup key. |
| `failed_tasks(limit)` | `Vec<TaskHistoryRecord>` | Recent failures. |

## Aggregate queries

| Method | Returns | Description |
|--------|---------|-------------|
| `history_stats(task_type)` | `TypeStats` | Aggregate stats: count, avg duration, avg IO, failure rate. |
| `avg_throughput(task_type, limit)` | `(f64, f64)` | Average `(read_bytes/sec, write_bytes/sec)` from recent completions. |

### TypeStats fields

| Field | Type | Description |
|-------|------|-------------|
| `count` | `i64` | Total completed tasks of this type. |
| `avg_duration_ms` | `f64` | Average wall-clock duration. |
| `avg_read_bytes` | `f64` | Average actual read bytes. |
| `avg_write_bytes` | `f64` | Average actual write bytes. |
| `failure_rate` | `f64` | Fraction of tasks that failed (0.0–1.0). |

## Unified lookup

Search both active and history tables by dedup key:

```rust
use taskmill::TaskLookup;

let lookup = scheduler.task_lookup("resize", "/photos/img.jpg").await?;
match lookup {
    TaskLookup::Active(record) => {
        println!("Status: {:?}, priority: {}", record.status, record.priority.value());
    }
    TaskLookup::History(record) => {
        println!("Completed at: {:?}, duration: {}ms", record.completed_at, record.duration_ms);
    }
    TaskLookup::NotFound => {
        println!("No task found with this key");
    }
}
```

Or with typed tasks:

```rust
let lookup = scheduler.lookup_typed(&ResizeTask {
    path: "/photos/img.jpg".into(),
    width: 300,
}).await?;
```

## Pruning

| Method | Returns | Description |
|--------|---------|-------------|
| `prune_history_by_count(keep)` | `u64` | Delete all but the N most recent history records. Returns count deleted. |
| `prune_history_by_age(days)` | `u64` | Delete history records older than N days. Returns count deleted. |

## Usage example

```rust
let store = scheduler.store();

// Dashboard data
let running = store.running_count().await?;
let pending = store.pending_count().await?;
let (read_io, write_io) = store.running_io_totals().await?;

// Per-type analytics
let stats = store.history_stats("thumbnail").await?;
println!(
    "thumbnail: {} completed, avg {:.0}ms, {:.1}% failure rate",
    stats.count, stats.avg_duration_ms, stats.failure_rate * 100.0,
);

// Paginated history for a UI table
let page = store.history(50, 0).await?;     // first 50
let page2 = store.history(50, 50).await?;   // next 50
```
