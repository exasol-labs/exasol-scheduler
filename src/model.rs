use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRow {
    pub task_id: String,
    pub enabled: bool,
    pub schedule: String,
    pub statement: String,
    pub after: Option<String>,
    pub is_final: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub run_id: Uuid,
    pub graph_run_id: Option<Uuid>,
    pub task_id: String,
    pub graph_phase: String,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub error_message: Option<String>,
}
