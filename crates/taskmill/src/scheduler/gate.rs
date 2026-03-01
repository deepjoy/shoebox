use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::backpressure::{CompositePressure, ThrottlePolicy};
use crate::resource::ResourceReader;
use crate::store::{StoreError, TaskStore};
use crate::task::TaskRecord;

/// Boxed future returned by [`DispatchGate`] methods.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ── Gate Context ───────────────────────────────────────────────────

/// Context provided to a [`DispatchGate`] for admission decisions.
///
/// Built by the scheduler each dispatch cycle so gate implementations
/// can query store state and resource snapshots without owning them.
pub struct GateContext<'a> {
    /// The task store — available for queries like running IO totals.
    pub store: &'a TaskStore,
    /// The current resource reader, if monitoring is enabled.
    pub resource_reader: Option<&'a Arc<dyn ResourceReader>>,
}

// ── Dispatch Gate ──────────────────────────────────────────────────

/// Decides whether a popped task should be dispatched or requeued.
///
/// The scheduler calls [`admit`](DispatchGate::admit) after popping a
/// task from the store but before spawning the executor. Returning
/// `Ok(false)` causes the task to be requeued for a later cycle.
///
/// The default [`DefaultDispatchGate`] applies backpressure throttling
/// and IO-budget checks. Custom implementations can add per-type rate
/// limiting, cost-model gating, feature flags, etc.
///
/// # Example
///
/// ```ignore
/// use taskmill::scheduler::gate::{DispatchGate, GateContext};
/// use taskmill::store::StoreError;
/// use taskmill::task::TaskRecord;
///
/// struct AlwaysAdmit;
///
/// impl DispatchGate for AlwaysAdmit {
///     fn admit<'a>(
///         &'a self,
///         _task: &'a TaskRecord,
///         _ctx: &'a GateContext<'a>,
///     ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, StoreError>> + Send + 'a>> {
///         Box::pin(async { Ok(true) })
///     }
/// }
/// ```
pub trait DispatchGate: Send + Sync + 'static {
    /// Check whether `task` should be dispatched given the current context.
    ///
    /// Return `Ok(true)` to dispatch, `Ok(false)` to requeue.
    fn admit<'a>(
        &'a self,
        task: &'a TaskRecord,
        ctx: &'a GateContext<'a>,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Current aggregate pressure (0.0–1.0). Returns 0.0 by default.
    fn pressure<'a>(&'a self) -> BoxFuture<'a, f32> {
        Box::pin(async { 0.0 })
    }

    /// Per-source pressure breakdown for diagnostics. Empty by default.
    fn pressure_breakdown<'a>(&'a self) -> BoxFuture<'a, Vec<(String, f32)>> {
        Box::pin(async { Vec::new() })
    }
}

// ── Default Gate ───────────────────────────────────────────────────

/// Default gate: backpressure throttling + IO budget.
///
/// This is what the scheduler uses unless you provide a custom gate via
/// [`SchedulerBuilder::dispatch_gate`](super::SchedulerBuilder::dispatch_gate).
pub struct DefaultDispatchGate {
    pub(crate) pressure: tokio::sync::Mutex<CompositePressure>,
    pub(crate) policy: ThrottlePolicy,
}

impl DefaultDispatchGate {
    pub fn new(pressure: CompositePressure, policy: ThrottlePolicy) -> Self {
        Self {
            pressure: tokio::sync::Mutex::new(pressure),
            policy,
        }
    }
}

impl DispatchGate for DefaultDispatchGate {
    fn admit<'a>(
        &'a self,
        task: &'a TaskRecord,
        ctx: &'a GateContext<'a>,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            // Backpressure check.
            let current_pressure = self.pressure.lock().await.pressure();
            if self.policy.should_throttle(task.priority, current_pressure) {
                tracing::trace!(
                    priority = task.priority.value(),
                    pressure = current_pressure,
                    "task throttled by backpressure — requeuing"
                );
                return Ok(false);
            }

            // IO budget check.
            if !has_io_headroom(task, ctx).await? {
                tracing::trace!(
                    task_type = task.task_type,
                    expected_read = task.expected_read_bytes,
                    expected_write = task.expected_write_bytes,
                    "task deferred — IO budget exhausted — requeuing"
                );
                return Ok(false);
            }

            Ok(true)
        })
    }

    fn pressure<'a>(&'a self) -> BoxFuture<'a, f32> {
        Box::pin(async { self.pressure.lock().await.pressure() })
    }

    fn pressure_breakdown<'a>(&'a self) -> BoxFuture<'a, Vec<(String, f32)>> {
        Box::pin(async {
            self.pressure
                .lock()
                .await
                .breakdown()
                .into_iter()
                .map(|(name, val)| (name.to_owned(), val))
                .collect()
        })
    }
}

// ── IO Budget ──────────────────────────────────────────────────────

/// Check if there is IO headroom for a task given current running IO
/// and system capacity.
///
/// This is a utility function that custom [`DispatchGate`] implementations
/// can reuse if they want IO-budget awareness alongside their own logic.
pub async fn has_io_headroom(task: &TaskRecord, ctx: &GateContext<'_>) -> Result<bool, StoreError> {
    let Some(reader) = ctx.resource_reader else {
        // No monitor configured — always allow.
        return Ok(true);
    };

    let snapshot = reader.latest();
    // If we have no IO data yet, allow the task.
    if snapshot.io_read_bytes_per_sec == 0.0 && snapshot.io_write_bytes_per_sec == 0.0 {
        return Ok(true);
    }

    let (running_read, running_write) = ctx.store.running_io_totals().await?;

    // Simple heuristic: if running tasks' expected IO already exceeds
    // 80% of observed system throughput (per second × 2s budget window),
    // defer new work.
    let read_capacity = snapshot.io_read_bytes_per_sec * 2.0;
    let write_capacity = snapshot.io_write_bytes_per_sec * 2.0;

    let read_ok = read_capacity == 0.0
        || (running_read + task.expected_read_bytes) as f64 <= read_capacity * 0.8;
    let write_ok = write_capacity == 0.0
        || (running_write + task.expected_write_bytes) as f64 <= write_capacity * 0.8;

    Ok(read_ok && write_ok)
}
