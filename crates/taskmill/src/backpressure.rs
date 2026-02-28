use crate::priority::Priority;

/// A source of pressure that signals the scheduler to slow down.
///
/// Consumers implement this trait to feed external signals (API load, memory
/// pressure, queue depth, etc.) into the scheduler's throttle decisions.
pub trait PressureSource: Send + Sync + 'static {
    /// Current pressure level between 0.0 (idle) and 1.0 (saturated).
    fn pressure(&self) -> f32;

    /// Human-readable name for diagnostics and tracing.
    fn name(&self) -> &str;
}

/// Maps (priority, pressure) pairs to throttle decisions.
///
/// Contains a list of thresholds: a task at or below a given priority
/// (higher numeric value = lower priority) is throttled when pressure
/// exceeds the associated limit.
///
/// Thresholds are evaluated from lowest priority to highest. The first
/// matching rule applies.
pub struct ThrottlePolicy {
    /// Sorted from lowest priority (highest numeric value) to highest.
    /// Each entry: (priority_floor, pressure_limit).
    thresholds: Vec<(Priority, f32)>,
}

impl ThrottlePolicy {
    /// Create a policy with custom thresholds.
    ///
    /// Each `(priority, limit)` means: any task with priority value >= `priority`
    /// (i.e. lower or equal priority) is throttled when pressure > `limit`.
    ///
    /// Thresholds should be ordered from lowest priority to highest.
    pub fn new(thresholds: Vec<(Priority, f32)>) -> Self {
        Self { thresholds }
    }

    /// Default three-tier policy matching Shoebox's original behavior:
    /// - BACKGROUND (192+): pause at >50% pressure
    /// - NORMAL (128+): pause at >75% pressure
    /// - Everything else: never pause
    pub fn default_three_tier() -> Self {
        Self {
            thresholds: vec![(Priority::BACKGROUND, 0.50), (Priority::NORMAL, 0.75)],
        }
    }

    /// Should a task at this priority be throttled given current pressure?
    pub fn should_throttle(&self, priority: Priority, pressure: f32) -> bool {
        for &(threshold_priority, pressure_limit) in &self.thresholds {
            // If the task's priority value is >= threshold (lower or equal priority)
            if priority.value() >= threshold_priority.value() && pressure > pressure_limit {
                return true;
            }
        }
        false
    }
}

/// Combines multiple pressure sources into a single aggregate signal.
///
/// The aggregate pressure is the maximum across all sources — the system
/// is as pressured as its most constrained resource.
pub struct CompositePressure {
    sources: Vec<Box<dyn PressureSource + 'static>>,
}

impl CompositePressure {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    /// Add a pressure source.
    pub fn add_source(&mut self, source: Box<dyn PressureSource + 'static>) {
        self.sources.push(source);
    }

    /// Aggregate pressure: max across all sources.
    pub fn pressure(&self) -> f32 {
        self.sources
            .iter()
            .map(|s| s.pressure())
            .fold(0.0f32, f32::max)
    }

    /// Per-source breakdown for diagnostics.
    pub fn breakdown(&self) -> Vec<(&str, f32)> {
        self.sources
            .iter()
            .map(|s| (s.name(), s.pressure()))
            .collect()
    }
}

impl Default for CompositePressure {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedPressure {
        value: f32,
        name: &'static str,
    }

    impl PressureSource for FixedPressure {
        fn pressure(&self) -> f32 {
            self.value
        }
        fn name(&self) -> &str {
            self.name
        }
    }

    #[test]
    fn default_policy_background_throttles() {
        let policy = ThrottlePolicy::default_three_tier();

        // Background at 60% pressure → throttled (>50%)
        assert!(policy.should_throttle(Priority::BACKGROUND, 0.6));
        // Background at 40% → not throttled
        assert!(!policy.should_throttle(Priority::BACKGROUND, 0.4));

        // Normal at 60% → not throttled (<75%)
        assert!(!policy.should_throttle(Priority::NORMAL, 0.6));
        // Normal at 80% → throttled
        assert!(policy.should_throttle(Priority::NORMAL, 0.8));

        // Realtime never throttled
        assert!(!policy.should_throttle(Priority::REALTIME, 1.0));
        assert!(!policy.should_throttle(Priority::HIGH, 0.6));
    }

    #[test]
    fn composite_takes_max() {
        let mut comp = CompositePressure::new();
        comp.add_source(Box::new(FixedPressure {
            value: 0.3,
            name: "api",
        }));
        comp.add_source(Box::new(FixedPressure {
            value: 0.7,
            name: "disk",
        }));

        assert!((comp.pressure() - 0.7).abs() < f32::EPSILON);
    }

    #[test]
    fn empty_composite_is_zero() {
        let comp = CompositePressure::new();
        assert_eq!(comp.pressure(), 0.0);
    }
}
