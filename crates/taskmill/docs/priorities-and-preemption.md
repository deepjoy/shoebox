# Priorities & Preemption

## Priority levels

Taskmill uses a 256-level priority scale where lower values mean higher priority. Five named constants are provided:

| Constant     | Value | Behavior |
|--------------|-------|----------|
| `REALTIME`   | 0     | Never throttled. Triggers preemption of lower-priority work. |
| `HIGH`       | 64    | Throttled only under extreme pressure (>75%). |
| `NORMAL`     | 128   | Standard operations. Throttled at >75% pressure. |
| `BACKGROUND` | 192   | Deferred under moderate load. Throttled at >50% pressure. |
| `IDLE`       | 255   | Runs only when the system is otherwise idle. Throttled at >50% pressure. |

Custom values between tiers are supported:

```rust
use taskmill::Priority;

let custom = Priority::new(100); // between HIGH and NORMAL
```

## Queue ordering

Tasks are popped from the queue in strict priority order (`ORDER BY priority ASC, id ASC`). Within the same priority tier, tasks are dispatched in insertion order (FIFO).

A partial index on `(status, priority, id) WHERE status = 'pending'` keeps pop operations fast regardless of history size.

## Preemption

When a task with priority at or above `preempt_priority` (default: `REALTIME`) is submitted, the scheduler preempts lower-priority running work:

1. **Cancel tokens** — the `CancellationToken` of every active task with lower priority (higher numeric value) is triggered.
2. **Pause in store** — preempted tasks are moved to `paused` status with `started_at` cleared.
3. **Emit events** — a `SchedulerEvent::Preempted` is emitted for each affected task.
4. **Resume later** — paused tasks are only re-dispatched when no active preemptors remain, preventing thrashing between competing priority tiers.

### Handling preemption in executors

Executors should check for cancellation at natural yield points:

```rust
impl TaskExecutor for MyExecutor {
    async fn execute<'a>(
        &'a self, ctx: &'a TaskContext,
    ) -> Result<TaskResult, TaskError> {
        for chunk in chunks {
            // Check before each unit of work
            if ctx.token.is_cancelled() {
                return Err(TaskError {
                    message: "preempted".into(),
                    retryable: true,
                    actual_read_bytes: bytes_read_so_far,
                    actual_write_bytes: bytes_written_so_far,
                });
            }

            process(chunk).await;
            ctx.progress.report_fraction(i, total, None);
        }

        Ok(TaskResult { actual_read_bytes: total_read, actual_write_bytes: total_written })
    }
}
```

Returning a retryable error on preemption is optional — the scheduler handles pausing regardless. But it gives the executor a chance to report partial IO and clean up.

### Configuring preemption threshold

```rust
let scheduler = Scheduler::builder()
    .preempt_priority(Priority::HIGH)  // now HIGH and REALTIME both trigger preemption
    .build()
    .await?;
```

## Throttle behavior

Throttling is independent of preemption. It controls whether a pending task is *dispatched*, not whether a running task is *interrupted*.

The default three-tier `ThrottlePolicy`:

| Priority tier | Throttled when pressure exceeds |
|---------------|-------------------------------|
| `BACKGROUND` (192+) | 50% |
| `NORMAL` (128+) | 75% |
| `HIGH` / `REALTIME` | Never |

Pressure is an aggregate `0.0..=1.0` value from all registered `PressureSource` implementations (see [IO & Backpressure](io-and-backpressure.md)).

### Custom throttle policies

```rust
use taskmill::{ThrottlePolicy, Priority};

// Custom: throttle IDLE at 30%, BACKGROUND at 60%, NORMAL at 80%
let policy = ThrottlePolicy::new(vec![
    (Priority::IDLE, 0.3),
    (Priority::BACKGROUND, 0.6),
    (Priority::NORMAL, 0.8),
]);
```

Thresholds are evaluated from lowest priority (highest numeric value) first. A task is throttled if its priority is at or below the threshold tier and pressure exceeds the limit.
