//! # Taskmill
//!
//! Adaptive priority work scheduler with IO-aware concurrency and SQLite persistence.
//!
//! Taskmill provides a generic task scheduling system that:
//! - Persists tasks to SQLite so the queue survives restarts
//! - Schedules by priority (0 = highest, 255 = lowest) with named tiers
//! - Deduplicates tasks by key — submitting an already-queued key is a no-op
//! - Tracks expected and actual IO bytes per task for budget-based scheduling
//! - Monitors system CPU and disk throughput to adjust concurrency
//! - Supports composable backpressure from arbitrary external sources
//! - Preempts lower-priority work when high-priority tasks arrive
//! - Retries failed tasks at the same priority level
//! - Records completed/failed task history for queries and IO learning
//! - Emits lifecycle events including progress for UI integration (via broadcast channel)
//! - Supports graceful shutdown with configurable drain timeout
//!
//! # Feature flags
//!
//! - **`sysinfo-monitor`** (default): Enables the built-in `SysinfoSampler` for
//!   cross-platform CPU and disk IO monitoring. Disable for mobile targets or
//!   when providing a custom `ResourceSampler`.

pub mod backpressure;
pub mod priority;
pub mod registry;
pub mod resource;
pub mod scheduler;
pub mod store;
pub mod task;

// Convenience re-exports.
pub use backpressure::{CompositePressure, PressureSource, ThrottlePolicy};
pub use priority::Priority;
pub use registry::{StateMap, TaskContext, TaskExecutor};
pub use resource::sampler::{SamplerConfig, SmoothedReader};
pub use resource::{ResourceReader, ResourceSampler, ResourceSnapshot};
pub use scheduler::{
    EstimatedProgress, ProgressReporter, Scheduler, SchedulerBuilder, SchedulerConfig,
    SchedulerEvent, SchedulerSnapshot, ShutdownMode,
};
pub use store::{RetentionPolicy, StoreConfig, StoreError, TaskStore};
pub use task::{
    generate_dedup_key, HistoryStatus, SubmitOutcome, TaskError, TaskHistoryRecord, TaskLookup,
    TaskRecord, TaskResult, TaskStatus, TaskSubmission, TypeStats, TypedTask,
};

#[cfg(feature = "sysinfo-monitor")]
pub use resource::platform_sampler;
