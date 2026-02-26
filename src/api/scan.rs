use axum::extract::State;
use serde::Serialize;
use time::format_description::well_known::Rfc3339;

use crate::api::responses::XmlResponse;
use crate::scanner::ScanJob;
use crate::services::AppState;

#[derive(Serialize)]
#[serde(rename = "ScanStatus")]
pub struct ScanStatusResponse {
    #[serde(rename = "Bucket")]
    buckets: Vec<BucketStatus>,
}

#[derive(Serialize)]
pub struct BucketStatus {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "ActiveJobs")]
    active_jobs: JobList,
    #[serde(rename = "PendingJobs")]
    pending_jobs: JobList,
    #[serde(rename = "FailedJobs")]
    failed_jobs: JobList,
}

#[derive(Serialize)]
pub struct JobList {
    #[serde(rename = "Job")]
    jobs: Vec<JobEntry>,
}

/// XML-friendly representation of a scan job.
#[derive(Serialize)]
pub struct JobEntry {
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Priority")]
    priority: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "TargetLevel")]
    target_level: String,
    #[serde(rename = "ScopeType")]
    scope_type: String,
    #[serde(rename = "ScopeData")]
    scope_data: Option<String>,
    #[serde(rename = "CreatedAt")]
    created_at: String,
    #[serde(rename = "L2Cursor")]
    l2_cursor: Option<String>,
    #[serde(rename = "L3Cursor")]
    l3_cursor: Option<String>,
    #[serde(rename = "RetryCount")]
    retry_count: u32,
    #[serde(rename = "LastError")]
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

impl From<&ScanJob> for JobEntry {
    fn from(job: &ScanJob) -> Self {
        Self {
            id: job.id.to_string(),
            priority: format!("{:?}", job.priority),
            status: job.status.as_str().to_string(),
            target_level: format!("{:?}", job.target_level),
            scope_type: job.scope.scope_type().to_string(),
            scope_data: job.scope.scope_data(),
            created_at: job.created_at.format(&Rfc3339).unwrap_or_default(),
            l2_cursor: job.l2_cursor.clone(),
            l3_cursor: job.l3_cursor.clone(),
            retry_count: job.retry_count,
            last_error: job.last_error.clone(),
        }
    }
}

/// GET /_shoebox/scan/status — Return scan status for all buckets.
pub async fn scan_status(State(state): State<AppState>) -> XmlResponse<ScanStatusResponse> {
    let mut buckets = Vec::new();

    for (name, bucket) in state.buckets.iter() {
        let scheduler = bucket.scheduler.lock().await;
        buckets.push(BucketStatus {
            name: name.clone(),
            active_jobs: JobList {
                jobs: scheduler
                    .active_jobs()
                    .iter()
                    .map(|j| JobEntry::from(*j))
                    .collect(),
            },
            pending_jobs: JobList {
                jobs: scheduler
                    .pending_jobs()
                    .iter()
                    .map(|j| JobEntry::from(*j))
                    .collect(),
            },
            failed_jobs: JobList {
                jobs: scheduler.failed_jobs().iter().map(JobEntry::from).collect(),
            },
        });
    }

    XmlResponse(ScanStatusResponse { buckets })
}
