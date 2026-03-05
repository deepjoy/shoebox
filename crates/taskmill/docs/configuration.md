# Configuration

## SchedulerConfig

Controls scheduling behavior. Set via builder methods or pass directly to `Scheduler::new()`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_concurrency` | `usize` | 4 | Maximum concurrent running tasks. Adjustable at runtime via `set_max_concurrency()`. |
| `max_retries` | `i32` | 3 | Retry limit before a task is permanently failed. |
| `preempt_priority` | `Priority` | `REALTIME` (0) | Tasks at or above this priority trigger preemption of lower-priority work. |
| `poll_interval` | `Duration` | 500ms | Sleep between scheduler dispatch cycles. The scheduler also wakes on `Notify` signals. |
| `throughput_sample_size` | `i32` | 20 | Number of recent completions used for throughput-based progress extrapolation. |
| `shutdown_mode` | `ShutdownMode` | `Hard` | `Hard` cancels all tasks immediately. `Graceful(Duration)` waits up to the timeout. |

### Builder methods

```rust
use std::time::Duration;
use taskmill::{Scheduler, Priority, ShutdownMode};

let scheduler = Scheduler::builder()
    .max_concurrency(8)
    .max_retries(5)
    .preempt_priority(Priority::HIGH)
    .poll_interval(Duration::from_millis(250))
    .shutdown_mode(ShutdownMode::Graceful(Duration::from_secs(30)))
    .build()
    .await?;
```

## StoreConfig

Controls the SQLite connection pool and history retention.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_connections` | `u32` | 16 | SQLite connection pool size. |
| `retention_policy` | `Option<RetentionPolicy>` | `None` | Automatic history pruning. `MaxCount(n)` or `MaxAgeDays(n)`. |
| `prune_interval` | `u64` | 100 | Number of task completions between automatic prune runs. |

### Builder method

```rust
use taskmill::{StoreConfig, RetentionPolicy};

let scheduler = Scheduler::builder()
    .store_config(StoreConfig {
        max_connections: 32,
        retention_policy: Some(RetentionPolicy::MaxCount(10_000)),
        prune_interval: 50,
        ..Default::default()
    })
    .build()
    .await?;
```

## SamplerConfig

Controls the resource monitoring background loop.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval` | `Duration` | 1s | How often to sample system resources. |
| `ewma_alpha` | `f64` | 0.3 | EWMA smoothing factor. Higher = more responsive to changes, lower = smoother. |

### Builder method

```rust
use std::time::Duration;
use taskmill::SamplerConfig;

let scheduler = Scheduler::builder()
    .with_resource_monitoring()
    .sampler_config(SamplerConfig {
        interval: Duration::from_millis(500),
        ewma_alpha: 0.5,
    })
    .build()
    .await?;
```

## ShutdownMode

| Variant | Behavior |
|---------|----------|
| `Hard` | Cancel all running tasks immediately when the scheduler stops. |
| `Graceful(Duration)` | Stop dispatching new tasks, wait for running tasks to complete (up to the timeout), then force-cancel any remaining. Stops the resource sampler afterward. |

## RetentionPolicy

| Variant | Behavior |
|---------|----------|
| `MaxCount(i64)` | Keep the N most recent history records, prune the rest. |
| `MaxAgeDays(i64)` | Keep records from the last N days, prune older entries. |

## Priority constants

| Constant | Value | Notes |
|----------|-------|-------|
| `Priority::REALTIME` | 0 | Highest. Never throttled. Triggers preemption. |
| `Priority::HIGH` | 64 | |
| `Priority::NORMAL` | 128 | Default for most tasks. |
| `Priority::BACKGROUND` | 192 | |
| `Priority::IDLE` | 255 | Lowest. |

Custom: `Priority::new(n)` for any `u8` value.

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `sysinfo-monitor` | Enabled | Cross-platform CPU and disk IO monitoring via `sysinfo`. Disable for mobile targets or custom samplers. |

### Disabling platform monitoring

```toml
[dependencies]
taskmill = { path = "crates/taskmill", default-features = false }
```

When disabled, you can still provide a custom `ResourceSampler` via `.resource_sampler()`.

## Builder reference

All `SchedulerBuilder` methods:

| Method | Description |
|--------|-------------|
| `store_path(path)` | Path to the SQLite database file. |
| `store(store)` | Use a pre-opened `TaskStore`. |
| `store_config(config)` | Pool size and retention settings. |
| `executor(name, executor)` | Register a `TaskExecutor` by name. |
| `typed_executor::<T>(executor)` | Register using `T::TASK_TYPE` as the name. |
| `max_concurrency(n)` | Set initial max concurrent tasks. |
| `max_retries(n)` | Set retry limit. |
| `preempt_priority(p)` | Set preemption threshold. |
| `poll_interval(d)` | Set dispatch cycle interval. |
| `shutdown_mode(mode)` | Set shutdown behavior. |
| `pressure_source(source)` | Add a `PressureSource` to the composite. |
| `throttle_policy(policy)` | Set a custom `ThrottlePolicy`. |
| `with_resource_monitoring()` | Enable platform resource monitoring. |
| `resource_sampler(sampler)` | Provide a custom `ResourceSampler`. |
| `sampler_config(config)` | Configure sample interval and smoothing. |
| `app_state(state)` | Register a state type (multiple types can coexist). |
| `app_state_arc(arc)` | Register a state type from a pre-existing `Arc`. |
| `build()` | Build and return the `Scheduler`. |
