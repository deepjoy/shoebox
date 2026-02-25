use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use time::OffsetDateTime;
use uuid::Uuid;

use crate::scanner::scope::ScanScope;

/// Scanner priority levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[repr(u8)]
pub enum Priority {
    /// API call waiting — blocks until complete. L2 max.
    Realtime = 0,
    /// Background reconciliation — yields to API.
    Reconcile = 1,
    /// Lowest priority — pauses under API load.
    Background = 2,
}

/// Target scan depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[repr(i32)]
pub enum ScanLevel {
    Discovery = 1,
    Metadata = 2,
    Content = 3,
}

impl ScanLevel {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Job status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum JobStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// A single scan job in the priority queue.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanJob {
    pub id: Uuid,
    pub priority: Priority,
    pub scope: ScanScope,
    pub target_level: ScanLevel,
    pub created_at: OffsetDateTime,
    pub status: JobStatus,
    /// Keyset pagination cursor for L2 — when set, L2 queries skip keys ≤ this value.
    pub l2_cursor: Option<String>,
    /// Keyset pagination cursor for L3 — when set, L3 queries skip keys ≤ this value.
    pub l3_cursor: Option<String>,
    /// Estimated L3 throughput from the previous batch (bytes/sec).
    /// Used to compute the byte budget for the next L3 batch so each batch
    /// targets ~2 minutes of wall-clock time.
    pub l3_bytes_per_sec: Option<f64>,
}

impl ScanJob {
    pub fn new(priority: Priority, scope: ScanScope, target_level: ScanLevel) -> Self {
        Self {
            id: Uuid::new_v4(),
            priority,
            scope,
            target_level,
            created_at: OffsetDateTime::now_utc(),
            status: JobStatus::Pending,
            l2_cursor: None,
            l3_cursor: None,
            l3_bytes_per_sec: None,
        }
    }

    /// Create a continuation job that resumes from where the previous batch left off.
    pub fn new_continuation(
        priority: Priority,
        scope: ScanScope,
        target_level: ScanLevel,
        l2_cursor: Option<String>,
        l3_cursor: Option<String>,
        l3_bytes_per_sec: Option<f64>,
    ) -> Self {
        Self {
            l2_cursor,
            l3_cursor,
            l3_bytes_per_sec,
            ..Self::new(priority, scope, target_level)
        }
    }

    /// Returns true when this is a continuation of a previous batch.
    pub fn is_continuation(&self) -> bool {
        self.l2_cursor.is_some() || self.l3_cursor.is_some()
    }
}

// BinaryHeap is a max-heap, so higher priority (lower numeric value) should compare as Greater.
impl Eq for ScanJob {}

impl PartialEq for ScanJob {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for ScanJob {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first (lower numeric value = higher priority).
        // BinaryHeap is a max-heap, so "Greater" is popped first.
        other
            .priority
            .cmp(&self.priority)
            // Then older jobs first (earlier created_at).
            // Reversed comparison so earlier timestamps are "Greater" in the max-heap.
            .then_with(|| other.created_at.cmp(&self.created_at))
    }
}

impl PartialOrd for ScanJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Priority-based scan scheduler.
pub struct ScanScheduler {
    jobs: BinaryHeap<ScanJob>,
    active: HashMap<Uuid, ScanJob>,
}

impl Default for ScanScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanScheduler {
    pub fn new() -> Self {
        Self {
            jobs: BinaryHeap::new(),
            active: HashMap::new(),
        }
    }

    /// Schedule a new scan job. Returns the job ID.
    pub fn schedule(&mut self, job: ScanJob) -> Uuid {
        let id = job.id;

        // P0 jobs preempt running P1/P2 jobs
        if job.priority == Priority::Realtime {
            self.preempt_lower_priority();
        }

        self.jobs.push(job);
        id
    }

    /// Get next job to run.
    pub fn next_job(&mut self) -> Option<ScanJob> {
        if let Some(mut job) = self.jobs.pop() {
            job.status = JobStatus::Running;
            let id = job.id;
            self.active.insert(id, job.clone());
            Some(job)
        } else {
            None
        }
    }

    /// Mark a job as completed and remove from active set.
    pub fn complete(&mut self, id: Uuid) {
        self.active.remove(&id);
    }

    /// Mark a job as failed and remove from active set.
    pub fn fail(&mut self, id: Uuid) {
        self.active.remove(&id);
    }

    /// Check if there are pending jobs.
    pub fn has_pending(&self) -> bool {
        !self.jobs.is_empty()
    }

    /// Return a snapshot of all currently active (running) jobs.
    pub fn active_jobs(&self) -> Vec<&ScanJob> {
        self.active.values().collect()
    }

    /// Return a snapshot of all pending jobs in the queue.
    pub fn pending_jobs(&self) -> Vec<&ScanJob> {
        self.jobs.iter().collect()
    }

    /// Preempt lower priority jobs by moving them back to pending.
    fn preempt_lower_priority(&mut self) {
        let preempted: Vec<Uuid> = self
            .active
            .iter()
            .filter(|(_, job)| job.priority != Priority::Realtime)
            .map(|(id, _)| *id)
            .collect();

        for id in preempted {
            if let Some(mut job) = self.active.remove(&id) {
                job.status = JobStatus::Paused;
                self.jobs.push(job);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::scope::ScanScope;

    #[test]
    fn test_priority_ordering() {
        let mut scheduler = ScanScheduler::new();

        let bg_job = ScanJob::new(Priority::Background, ScanScope::Bucket, ScanLevel::Content);
        let reconcile_job =
            ScanJob::new(Priority::Reconcile, ScanScope::Bucket, ScanLevel::Metadata);
        let rt_job = ScanJob::new(
            Priority::Realtime,
            ScanScope::Files(vec!["a.txt".into()]),
            ScanLevel::Metadata,
        );

        scheduler.schedule(bg_job);
        scheduler.schedule(reconcile_job);
        scheduler.schedule(rt_job);

        // Should come out in priority order: Realtime, Reconcile, Background
        let first = scheduler.next_job().unwrap();
        assert_eq!(first.priority, Priority::Realtime);

        let second = scheduler.next_job().unwrap();
        assert_eq!(second.priority, Priority::Reconcile);

        let third = scheduler.next_job().unwrap();
        assert_eq!(third.priority, Priority::Background);
    }

    #[test]
    fn test_preemption() {
        let mut scheduler = ScanScheduler::new();

        let bg_job = ScanJob::new(Priority::Background, ScanScope::Bucket, ScanLevel::Content);
        scheduler.schedule(bg_job);

        // Start the background job
        let running = scheduler.next_job().unwrap();
        assert_eq!(running.priority, Priority::Background);
        assert_eq!(scheduler.active.len(), 1);

        // Schedule a realtime job — should preempt
        let rt_job = ScanJob::new(
            Priority::Realtime,
            ScanScope::Files(vec!["urgent.txt".into()]),
            ScanLevel::Metadata,
        );
        scheduler.schedule(rt_job);

        // Background job should be back in the queue as paused
        assert_eq!(scheduler.active.len(), 0);
        // Queue should have both: realtime + preempted background
        assert_eq!(scheduler.jobs.len(), 2);
    }
}
