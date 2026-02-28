use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Numeric priority level. Lower values = higher priority.
///
/// Provides named constants for common tiers while allowing any value 0–255
/// for fine-grained control.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(u8);

impl Priority {
    /// User-interactive work. Never throttled, triggers preemption.
    pub const REALTIME: Self = Self(0);
    /// Functionality-blocking tasks. Throttled only under extreme load.
    pub const HIGH: Self = Self(64);
    /// Normal background operations. Yields to interactive work.
    pub const NORMAL: Self = Self(128);
    /// Low priority. Pauses under significant load.
    pub const BACKGROUND: Self = Self(192);
    /// Idle-only work. Runs only when system is otherwise idle.
    pub const IDLE: Self = Self(255);

    /// Construct a priority from a raw value. 0 = highest, 255 = lowest.
    pub const fn new(level: u8) -> Self {
        Self(level)
    }

    /// Raw numeric value.
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// Ordering: lower numeric value = higher priority = compares as Greater.
/// This makes `BinaryHeap` (max-heap) pop the highest-priority item first.
impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: lower value is "greater" (higher priority).
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.0 {
            0 => "REALTIME",
            64 => "HIGH",
            128 => "NORMAL",
            192 => "BACKGROUND",
            255 => "IDLE",
            _ => return write!(f, "Priority({})", self.0),
        };
        write!(f, "Priority::{label}")
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u8> for Priority {
    fn from(v: u8) -> Self {
        Self(v)
    }
}

impl From<Priority> for u8 {
    fn from(p: Priority) -> Self {
        p.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realtime_is_highest() {
        assert!(Priority::REALTIME > Priority::HIGH);
        assert!(Priority::HIGH > Priority::NORMAL);
        assert!(Priority::NORMAL > Priority::BACKGROUND);
        assert!(Priority::BACKGROUND > Priority::IDLE);
    }

    #[test]
    fn custom_priorities_between_tiers() {
        let p = Priority::new(96);
        assert!(p < Priority::HIGH); // lower priority than HIGH
        assert!(p > Priority::NORMAL); // higher priority than NORMAL
    }

    #[test]
    fn debug_named_tiers() {
        assert_eq!(format!("{:?}", Priority::REALTIME), "Priority::REALTIME");
        assert_eq!(format!("{:?}", Priority::new(42)), "Priority(42)");
    }
}
