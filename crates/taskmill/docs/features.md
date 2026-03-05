# Features

A complete list of taskmill's capabilities.

## Persistence

- **SQLite-backed queue** — all tasks are stored in SQLite with WAL journal mode. Tasks survive process restarts, crashes, and power loss.
- **Crash recovery** — tasks left in `running` state during a crash are automatically reset to `pending` on startup. Dedup keys remain occupied so no duplicates sneak in during recovery.
- **Connection pooling** — configurable pool size (default 16) for concurrent reads.

## Scheduling

- **256-level priority queue** — priorities range from 0 (highest, `REALTIME`) to 255 (lowest, `IDLE`). Five named tiers are provided: `REALTIME`, `HIGH`, `NORMAL`, `BACKGROUND`, `IDLE`. Custom values like `Priority::new(100)` work too.
- **FIFO within tier** — tasks at the same priority are dispatched in insertion order.
- **Atomic dispatch** — pop operations use `UPDATE ... WHERE id = (SELECT ...) RETURNING *` for race-free claiming with no lost tasks.
- **Runtime-adjustable concurrency** — change `max_concurrency` at runtime via `set_max_concurrency()`.

## Deduplication

- **Key-based dedup** — each task gets a SHA-256 key derived from `task_type + payload` (or an explicit key). A `UNIQUE(key)` constraint with `INSERT OR IGNORE` prevents duplicate submissions.
- **Type-scoped keys** — the task type is always part of the hash, so different task types never collide even with identical payloads.
- **Lifecycle-aware** — keys are occupied while a task is pending, running, paused, or retrying. The key is freed when the task moves to history (completed or failed).
- **Batch-safe** — deduplication applies within `submit_batch()` transactions too.

## IO Awareness

- **Expected/actual IO tracking** — submit estimated read/write bytes; executors report actual bytes on completion.
- **IO budget gating** — the scheduler compares running task IO estimates against EWMA-smoothed system throughput. New work is deferred when cumulative IO would exceed 80% of observed disk capacity.
- **Learning from history** — `avg_throughput()` and `history_stats()` compute per-type IO averages from actual completions, enabling callers to refine estimates over time.

## Resource Monitoring

- **Cross-platform** — CPU and disk IO via `sysinfo` on Linux, macOS, and Windows. Feature-gated under `sysinfo-monitor` (enabled by default).
- **EWMA smoothing** — raw samples are smoothed with an exponentially weighted moving average (alpha=0.3, configurable) to avoid spiky readings.
- **Two-trait design** — `ResourceSampler` (raw platform readings) and `ResourceReader` (smoothed snapshots) are separated for testability and custom implementations.
- **Custom samplers** — disable the `sysinfo-monitor` feature and provide your own `ResourceSampler` for containers, cgroups, or mobile platforms.

## Backpressure

- **Composable pressure sources** — implement the `PressureSource` trait to expose a `0.0..=1.0` signal from any source (API load, memory, battery, queue depth). `CompositePressure` aggregates sources; the aggregate is the maximum across all.
- **Throttle policies** — `ThrottlePolicy` maps `(priority, pressure)` to dispatch decisions. The default three-tier policy throttles `BACKGROUND` tasks at >50% pressure, `NORMAL` at >75%, and never throttles `HIGH` or `REALTIME`.
- **Custom policies** — define your own thresholds for fine-grained control.

## Preemption

- **Priority-based preemption** — when a task at or above `preempt_priority` (default: `REALTIME`) is submitted, all lower-priority running tasks are cancelled and paused.
- **Token-based cancellation** — preempted tasks have their `CancellationToken` triggered. Executors should check `token.is_cancelled()` at yield points.
- **Anti-thrash protection** — paused tasks only resume when no active preemptors remain.

## Retries

- **Automatic requeue** — retryable failures (`TaskError { retryable: true }`) are requeued at the same priority with `retry_count += 1`.
- **Configurable limit** — `max_retries` (default 3) controls how many times a task can be retried before permanent failure.
- **Dedup preserved** — the key stays occupied during retries, preventing duplicate submission of in-progress work.

## Progress Reporting

- **Executor-reported progress** — report percentage or fraction-based progress via `ctx.progress.report()` or `ctx.progress.report_fraction()`.
- **Throughput-based extrapolation** — for tasks without explicit reports, the scheduler extrapolates progress from historical average duration, capped at 99% to avoid false completion signals.
- **Event-driven** — progress updates are emitted as `SchedulerEvent::Progress` for real-time UI updates.

## Lifecycle Events

- **Broadcast channel** — subscribe via `scheduler.subscribe()` to receive `SchedulerEvent` variants: `Dispatched`, `Completed`, `Failed`, `Preempted`, `Cancelled`, `Progress`, `Paused`, `Resumed`.
- **Tauri-ready** — all events are `Serialize`, designed for direct bridging to frontend via `app_handle.emit()`.

## Task Management

- **Task cancellation** — cancel running, pending, or paused tasks via `scheduler.cancel(task_id)`.
- **Global pause/resume** — `pause_all()` stops dispatch and pauses running tasks; `resume_all()` resumes on the next cycle. Emits events for UI integration.
- **Task lookup by dedup key** — `task_lookup()` searches both active and history tables for a task matching a given type and dedup input.

## Typed Payloads

- **Structured submission** — `TaskSubmission::with_payload()` serializes any `Serialize` type to JSON bytes.
- **Type-safe deserialization** — `TaskRecord::deserialize_payload::<T>()` in executors.
- **TypedTask trait** — define `TASK_TYPE`, default priority, and expected IO on your struct. Submit with `scheduler.submit_typed()` and deserialize with `ctx.deserialize_typed()`.

## Batch Operations

- **Bulk enqueue** — `submit_batch()` wraps many inserts in a single SQLite transaction. Returns `Vec<Option<i64>>` where `None` indicates deduplication.

## Graceful Shutdown

- **Hard mode** (default) — immediately cancels all running tasks.
- **Graceful mode** — stops dispatching, waits for running tasks up to a configurable timeout, then force-cancels stragglers.

## Application State

- **Type-keyed state map** — register multiple state types on the builder via `.app_state()` / `.app_state_arc()`. Each type is keyed by `TypeId`; access from any executor via `ctx.state::<T>()`.
- **Post-build injection** — call `scheduler.register_state(arc)` after build to let libraries inject their own state into a shared scheduler.
- **Arc-based sharing** — state is wrapped in `Arc` internally; all tasks share the same instance.

## History & Pruning

- **Automatic retention** — configure `RetentionPolicy::MaxCount(n)` or `RetentionPolicy::MaxAgeDays(n)` for automatic history pruning.
- **Amortized pruning** — pruning runs every N completions (default 100, configurable) to avoid per-task overhead.
- **Manual pruning** — `prune_history_by_count()` and `prune_history_by_age()` for on-demand cleanup.

## Dashboard

- **Single-call snapshot** — `scheduler.snapshot()` returns a serializable `SchedulerSnapshot` with running tasks, queue depths, progress estimates, pressure readings, and concurrency limits.
- **Designed for Tauri commands** — return the snapshot directly from a `#[tauri::command]` handler.

## Ergonomics

- **Builder pattern** — `Scheduler::builder()` provides fluent construction with sensible defaults.
- **Clone-friendly** — `Scheduler` is `Clone` via `Arc<SchedulerInner>` for easy sharing in Tauri state and across async tasks.
- **Serde on all public types** — every public struct and enum derives `Serialize`/`Deserialize` for Tauri IPC.
- **Serializable errors** — `StoreError` is serializable for direct use in Tauri command returns.
