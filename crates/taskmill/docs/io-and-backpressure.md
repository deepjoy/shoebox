# IO Tracking & Backpressure

Taskmill combines two independent gating mechanisms — IO budget tracking and composable backpressure — to avoid saturating system resources.

## IO tracking

### Submission estimates

Every `TaskSubmission` includes expected IO:

```rust
let sub = TaskSubmission {
    task_type: "scan".into(),
    key: None,
    priority: Priority::NORMAL,
    payload: Some(data),
    expected_read_bytes: 50_000,   // caller's estimate
    expected_write_bytes: 10_000,
};
```

### Completion actuals

Executors report actual IO in `TaskResult`:

```rust
Ok(TaskResult {
    actual_read_bytes: 48_312,
    actual_write_bytes: 9_876,
})
```

Actual values are stored in `task_history` for learning.

### IO budget gating

When resource monitoring is enabled, the scheduler checks IO headroom before dispatching:

1. Query EWMA-smoothed disk throughput from the `ResourceReader`.
2. Sum expected IO across all running tasks.
3. Compute a 2-second capacity window: `capacity = bytes_per_sec * 2.0`.
4. If running IO + candidate IO would exceed 80% of capacity on either axis (read or write), the task is deferred.

This prevents the scheduler from piling up IO-heavy tasks that would saturate the disk.

### Learning from history

Use store queries to refine future estimates:

```rust
let store = scheduler.store();

// Average read/write bytes per second for a task type (from recent completions)
let (avg_read_bps, avg_write_bps) = store.avg_throughput("scan", 20).await?;

// Aggregate stats: count, avg duration, avg IO, failure rate
let stats = store.history_stats("scan").await?;
```

## Resource monitoring

### Built-in platform sampler

Enabled by default via the `sysinfo-monitor` feature flag. Provides CPU and disk IO on Linux, macOS, and Windows.

```rust
let scheduler = Scheduler::builder()
    .with_resource_monitoring()  // uses SysinfoSampler automatically
    .build()
    .await?;
```

### Custom samplers

For containers, cgroups, or mobile platforms, provide your own `ResourceSampler`:

```rust
use taskmill::{ResourceSampler, ResourceSnapshot};

struct CgroupSampler;

impl ResourceSampler for CgroupSampler {
    fn sample(&mut self) -> ResourceSnapshot {
        ResourceSnapshot {
            cpu_usage: read_cgroup_cpu(),         // 0.0–1.0
            io_read_bytes_per_sec: read_blkio_read(),
            io_write_bytes_per_sec: read_blkio_write(),
        }
    }
}

let scheduler = Scheduler::builder()
    .resource_sampler(Box::new(CgroupSampler))
    .build()
    .await?;
```

### EWMA smoothing

Raw samples are smoothed via a `SmoothedReader` background loop:

```
smoothed = alpha * raw + (1 - alpha) * previous
```

- Default alpha: 0.3 (configurable via `SamplerConfig`)
- Default sample interval: 1 second
- Readers access snapshots via `RwLock` (readers never block each other)

Configure smoothing:

```rust
use std::time::Duration;
use taskmill::SamplerConfig;

let scheduler = Scheduler::builder()
    .with_resource_monitoring()
    .sampler_config(SamplerConfig {
        interval: Duration::from_millis(500),  // sample faster
        ewma_alpha: 0.5,                       // more responsive
    })
    .build()
    .await?;
```

## Backpressure

### Pressure sources

Implement the `PressureSource` trait to expose a `0.0..=1.0` signal from any external source:

```rust
use taskmill::PressureSource;

struct MemoryPressure;

impl PressureSource for MemoryPressure {
    fn pressure(&self) -> f32 {
        let used = sys_info::mem_used();
        let total = sys_info::mem_total();
        (used as f32 / total as f32).min(1.0)
    }

    fn name(&self) -> &str { "memory" }
}
```

### Composite pressure

Multiple sources are aggregated via `CompositePressure`. The aggregate pressure is the **maximum** across all sources:

```rust
use taskmill::CompositePressure;

let mut pressure = CompositePressure::new();
pressure.add_source(Arc::new(MemoryPressure));
pressure.add_source(Arc::new(QueueDepthPressure));
// Aggregate = max(memory_pressure, queue_pressure)
```

Or via the builder:

```rust
let scheduler = Scheduler::builder()
    .pressure_source(Arc::new(MemoryPressure))
    .pressure_source(Arc::new(QueueDepthPressure))
    .build()
    .await?;
```

### Throttle policies

`ThrottlePolicy` maps `(priority, pressure)` to dispatch decisions:

```rust
use taskmill::{ThrottlePolicy, Priority};

// Default: BACKGROUND >50%, NORMAL >75%, HIGH/REALTIME never
let policy = ThrottlePolicy::default_three_tier();

// Custom thresholds
let policy = ThrottlePolicy::new(vec![
    (Priority::IDLE, 0.3),       // throttle IDLE at 30%
    (Priority::BACKGROUND, 0.6), // throttle BACKGROUND at 60%
    (Priority::NORMAL, 0.8),     // throttle NORMAL at 80%
]);
```

### How gating works

The default `DispatchGate` combines both mechanisms. A task is dispatched only when **both** pass:

1. **Backpressure check** — `ThrottlePolicy::should_throttle(priority, pressure)` returns false.
2. **IO budget check** — `has_io_headroom()` confirms the task won't saturate disk throughput.

If either check fails, the task stays in the queue and is retried on the next poll cycle.

### Diagnostics

The `SchedulerSnapshot` includes pressure readings for debugging:

```rust
let snap = scheduler.snapshot().await?;
println!("Aggregate pressure: {:.0}%", snap.pressure * 100.0);
for (name, value) in &snap.pressure_breakdown {
    println!("  {}: {:.0}%", name, value * 100.0);
}
```
