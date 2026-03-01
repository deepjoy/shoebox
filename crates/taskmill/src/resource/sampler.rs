use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::{ResourceReader, ResourceSampler, ResourceSnapshot};

/// Configuration for the background resource sampling loop.
pub struct SamplerConfig {
    /// How often to sample system resources. Default: 1 second.
    pub interval: Duration,
    /// EWMA smoothing factor (alpha). Default: 0.3.
    /// Higher = more responsive to changes, lower = smoother.
    pub ewma_alpha: f64,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            ewma_alpha: 0.3,
        }
    }
}

/// Apply EWMA smoothing: new_value = alpha * raw + (1 - alpha) * old.
fn ewma(old: f64, raw: f64, alpha: f64) -> f64 {
    if old == 0.0 {
        raw // First sample — no history to blend with.
    } else {
        alpha * raw + (1.0 - alpha) * old
    }
}

/// Shared, lock-protected store for the latest smoothed snapshot.
///
/// The sampler loop writes to this; the scheduler reads from it.
/// Uses `RwLock` so readers never block each other.
#[derive(Clone)]
pub struct SmoothedReader {
    inner: Arc<RwLock<ResourceSnapshot>>,
}

impl SmoothedReader {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ResourceSnapshot::default())),
        }
    }

    async fn update(&self, snapshot: ResourceSnapshot) {
        *self.inner.write().await = snapshot;
    }
}

impl Default for SmoothedReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceReader for SmoothedReader {
    fn latest(&self) -> ResourceSnapshot {
        // Use try_read to avoid async in a sync trait method.
        // If the lock is held by the writer, return the default (zero) snapshot
        // which makes the scheduler skip IO budgeting for that cycle.
        self.inner
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

/// Run the resource sampling loop in the background.
///
/// Periodically calls `sampler.sample()`, applies EWMA smoothing, and
/// stores the result in the `reader`. The scheduler reads
/// `reader.latest()` when making IO budget decisions.
pub async fn run_sampler(
    sampler: Arc<tokio::sync::Mutex<Box<dyn ResourceSampler>>>,
    reader: SmoothedReader,
    config: SamplerConfig,
    token: CancellationToken,
) {
    tracing::debug!(
        interval_ms = config.interval.as_millis() as u64,
        alpha = config.ewma_alpha,
        "resource sampler started"
    );

    let mut smoothed = ResourceSnapshot::default();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::debug!("resource sampler shutting down");
                break;
            }
            _ = tokio::time::sleep(config.interval) => {
                let raw = sampler.lock().await.sample();

                smoothed.cpu_usage = ewma(smoothed.cpu_usage, raw.cpu_usage, config.ewma_alpha);
                smoothed.io_read_bytes_per_sec = ewma(
                    smoothed.io_read_bytes_per_sec,
                    raw.io_read_bytes_per_sec,
                    config.ewma_alpha,
                );
                smoothed.io_write_bytes_per_sec = ewma(
                    smoothed.io_write_bytes_per_sec,
                    raw.io_write_bytes_per_sec,
                    config.ewma_alpha,
                );

                reader.update(smoothed.clone()).await;

                tracing::trace!(
                    cpu = format!("{:.1}%", smoothed.cpu_usage * 100.0),
                    read_mbps = format!("{:.1}", smoothed.io_read_bytes_per_sec / 1_048_576.0),
                    write_mbps = format!("{:.1}", smoothed.io_write_bytes_per_sec / 1_048_576.0),
                    "resource sample"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_first_sample_is_raw() {
        assert_eq!(ewma(0.0, 42.0, 0.3), 42.0);
    }

    #[test]
    fn ewma_blends_with_history() {
        let result = ewma(100.0, 0.0, 0.3);
        // 0.3 * 0 + 0.7 * 100 = 70
        assert!((result - 70.0).abs() < 0.01);
    }

    #[test]
    fn ewma_converges() {
        let mut v = 0.0;
        for _ in 0..50 {
            v = ewma(v, 100.0, 0.3);
        }
        // Should converge to ~100
        assert!((v - 100.0).abs() < 1.0);
    }
}
