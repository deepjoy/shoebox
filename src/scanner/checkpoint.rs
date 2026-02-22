use uuid::Uuid;

/// Tracks scan progress for pause/resume.
///
/// When a scan is interrupted (by preemption or shutdown), the checkpoint
/// records the last successfully processed key so the scan can resume
/// from that point.
#[derive(Debug, Clone)]
pub struct ScanCheckpoint {
    pub job_id: Uuid,
    pub last_processed_key: Option<String>,
    pub files_completed: u64,
    pub files_total: u64,
}

impl ScanCheckpoint {
    pub fn new(job_id: Uuid) -> Self {
        Self {
            job_id,
            last_processed_key: None,
            files_completed: 0,
            files_total: 0,
        }
    }

    /// Record progress after processing a key.
    pub fn advance(&mut self, key: &str) {
        self.last_processed_key = Some(key.to_string());
        self.files_completed += 1;
    }

    /// Completion percentage (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        if self.files_total == 0 {
            0.0
        } else {
            self.files_completed as f64 / self.files_total as f64
        }
    }
}
