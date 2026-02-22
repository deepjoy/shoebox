use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::scanner::scheduler::Priority;

/// Controls concurrency between API operations and scanner workers.
///
/// API operations always succeed; scanner workers pause when API load is high.
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

    /// Check if a scanner at the given priority should pause.
    pub fn should_pause(&self, priority: Priority) -> bool {
        let api_load = self.api_active.load(Ordering::Relaxed) as f32 / self.total_permits as f32;

        match priority {
            Priority::Background => api_load > 0.5,
            Priority::Reconcile => api_load > 0.75,
            Priority::Realtime => false,
        }
    }

    /// Get current API load as a fraction.
    pub fn api_load(&self) -> f32 {
        self.api_active.load(Ordering::Relaxed) as f32 / self.total_permits as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_background_pauses_under_load() {
        let resources = ScannerResources::new(10);

        // No load — should not pause
        assert!(!resources.should_pause(Priority::Background));

        // Simulate 6 concurrent API calls (>50%)
        for _ in 0..6 {
            resources.api_start();
        }
        assert!(resources.should_pause(Priority::Background));
        assert!(!resources.should_pause(Priority::Reconcile)); // <75%
        assert!(!resources.should_pause(Priority::Realtime));

        // Simulate 8 concurrent API calls (>75%)
        for _ in 0..2 {
            resources.api_start();
        }
        assert!(resources.should_pause(Priority::Reconcile));
        assert!(!resources.should_pause(Priority::Realtime)); // Never pauses
    }
}
