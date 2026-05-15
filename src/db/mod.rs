mod exasol;

use chrono::{DateTime, Utc};
use thiserror::Error;

pub use exasol::{ExasolDb, ExasolDbConfig, build_tasks_last_changed_query};

use crate::model::{HistoryEvent, TaskRow};

#[derive(Debug, Error)]
pub enum DbError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("runtime initialization failed: {0}")]
    RuntimeInit(String),
    #[error("connection failed during {operation}: {source}")]
    Connection {
        operation: &'static str,
        #[source]
        source: exarrow::ConnectionError,
    },
    #[error("query failed during {operation}: {source}; sql={sql}")]
    Query {
        operation: &'static str,
        sql: String,
        #[source]
        source: exarrow::QueryError,
    },
    #[error("decoding failed during {operation}: {message}")]
    Decode {
        operation: &'static str,
        message: String,
    },
    #[error("no rows returned during {operation}")]
    NotFound { operation: &'static str },
    #[error("db error: {0}")]
    Other(String),
}

#[cfg_attr(test, mockall::automock)]
pub trait SchedulerDb: Send + Sync {
    fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError>;
    fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError>;
    fn execute_statement(&self, sql: &str) -> Result<(), DbError>;
    fn write_history(&self, event: &HistoryEvent) -> Result<(), DbError>;
}
