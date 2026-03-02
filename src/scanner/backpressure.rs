use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Controls concurrency between API operations and scanner workers.
///
/// API operations always succeed; scanner workers pause when API load is high.
/// Also implements [`taskmill::PressureSource`] so the scheduler can use the
/// same signal for its throttle policy.
pub struct ScannerResources {
    api_active: Arc<AtomicU32>,
    total_permits: u32,
}

impl ScannerResources {
    pub fn new(total_permits: u32) -> Self {
        Self {
            api_active: Arc::new(AtomicU32::new(0)),
            total_permits,
        }
    }

    /// Record an API operation starting.
    pub fn api_start(&self) {
        self.api_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an API operation finishing.
    pub fn api_end(&self) {
        self.api_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current API load as a fraction (0.0–1.0).
    pub fn api_load(&self) -> f32 {
        self.api_active.load(Ordering::Relaxed) as f32 / self.total_permits as f32
    }
}

impl taskmill::PressureSource for ScannerResources {
    fn pressure(&self) -> f32 {
        self.api_load()
    }

    fn name(&self) -> &str {
        "api-load"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_source() {
        let resources = ScannerResources::new(10);
        assert_eq!(taskmill::PressureSource::pressure(&resources), 0.0);

        for _ in 0..5 {
            resources.api_start();
        }
        assert!((taskmill::PressureSource::pressure(&resources) - 0.5).abs() < f32::EPSILON);
    }
}
