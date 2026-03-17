use std::sync::atomic::Ordering;

use axum::extract::State;
use serde::Serialize;

use crate::api::responses::XmlResponse;
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
    #[serde(rename = "RunningCount")]
    running_count: usize,
    #[serde(rename = "PendingCount")]
    pending_count: i64,
    #[serde(rename = "PausedCount")]
    paused_count: i64,
    #[serde(rename = "Pressure")]
    pressure: f32,
    #[serde(rename = "MaxConcurrency")]
    max_concurrency: usize,
    #[serde(rename = "IsPaused")]
    is_paused: bool,
    #[serde(rename = "L1Running")]
    l1_running: bool,
    #[serde(rename = "RunningTask")]
    running_tasks: Vec<TaskEntry>,
    #[serde(rename = "Progress")]
    progress: Vec<ProgressEntry>,
}

#[derive(Serialize)]
pub struct TaskEntry {
    #[serde(rename = "Id")]
    id: i64,
    #[serde(rename = "TaskType")]
    task_type: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Priority")]
    priority: u8,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "RetryCount")]
    retry_count: i32,
    #[serde(rename = "LastError")]
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Serialize)]
pub struct ProgressEntry {
    #[serde(rename = "TaskId")]
    task_id: i64,
    #[serde(rename = "TaskType")]
    task_type: String,
    #[serde(rename = "Percent")]
    percent: f32,
}

/// GET /_shoebox/scan/status — Return scan status for all buckets.
pub async fn scan_status(State(state): State<AppState>) -> XmlResponse<ScanStatusResponse> {
    let mut buckets = Vec::new();

    for (name, bucket) in state.buckets.iter() {
        let snapshot = match bucket.scheduler.snapshot().await {
            Ok(s) => s,
            Err(_) => continue,
        };

        let l1_running = state
            .scan_app_state
            .buckets
            .get(name)
            .map(|s| s.l1_running.load(Ordering::Acquire))
            .unwrap_or(false);

        buckets.push(BucketStatus {
            name: name.clone(),
            running_count: snapshot.running.len(),
            pending_count: snapshot.pending_count,
            paused_count: snapshot.paused_count,
            pressure: snapshot.pressure,
            max_concurrency: snapshot.max_concurrency,
            is_paused: snapshot.is_paused,
            l1_running,
            running_tasks: snapshot
                .running
                .iter()
                .map(|t| TaskEntry {
                    id: t.id,
                    task_type: t.task_type.clone(),
                    key: t.key.clone(),
                    priority: t.priority.value(),
                    status: t.status.as_str().to_string(),
                    retry_count: t.retry_count,
                    last_error: t.last_error.clone(),
                })
                .collect(),
            progress: snapshot
                .progress
                .iter()
                .map(|p| ProgressEntry {
                    task_id: p.task_id,
                    task_type: p.task_type.clone(),
                    percent: p.percent,
                })
                .collect(),
        });
    }

    XmlResponse(ScanStatusResponse { buckets })
}
