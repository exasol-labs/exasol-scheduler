mod exasol;

use chrono::{DateTime, Utc};
use thiserror::Error;

pub use exasol::{
    EnsureTablesResult, ExasolDb, ExasolDbConfig, build_create_history_table_sql,
    build_create_tasks_table_sql, build_tasks_last_changed_query, build_write_history_sql,
};

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
    #[error("query failed during {operation}: {source}; hint: {hint}; sql={sql}")]
    QueryWithHint {
        operation: &'static str,
        sql: String,
        #[source]
        source: exarrow::QueryError,
        hint: String,
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

impl DbError {
    pub(crate) fn query_with_schema_hint(
        operation: &'static str,
        sql: String,
        source: exarrow::QueryError,
        schema: Option<&str>,
    ) -> Self {
        if let Some(schema) = schema {
            if let Some(hint) = schema_query_hint(&source, schema) {
                return Self::QueryWithHint {
                    operation,
                    sql,
                    source,
                    hint,
                };
            }
        }

        Self::Query {
            operation,
            sql,
            source,
        }
    }
}

fn schema_query_hint(source: &exarrow::QueryError, schema: &str) -> Option<String> {
    let exarrow::QueryError::ExecutionFailed(message) = source else {
        return None;
    };

    let message = message.to_ascii_lowercase();
    let quoted_schema = quote_identifier_for_hint(schema);
    if message.contains("schema")
        && (message.contains("not found") || message.contains("does not exist"))
    {
        return Some(format!(
            "schema {quoted_schema} does not exist or is not visible to the scheduler user. \
             Create it with `CREATE SCHEMA {quoted_schema};`, grant access, or set EXA_SCHEMA to an existing schema"
        ));
    }

    if (message.contains("insufficient") || message.contains("not authorized"))
        && message.contains("create")
        && message.contains("schema")
    {
        return Some(format!(
            "the scheduler could not create schema {quoted_schema}. \
             Create it manually with `CREATE SCHEMA {quoted_schema};` or grant CREATE SCHEMA to the scheduler user"
        ));
    }

    None
}

fn quote_identifier_for_hint(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg_attr(test, mockall::automock)]
pub trait SchedulerDb: Send + Sync {
    fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError>;
    fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError>;
    fn execute_statement(&self, sql: &str) -> Result<(), DbError>;
    fn write_history(&self, event: &HistoryEvent) -> Result<(), DbError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_missing_query_error_gets_actionable_hint() {
        let err = DbError::query_with_schema_hint(
            "ensure_tables",
            "CREATE TABLE \"PUBLIC\".\"SCHED_TASKS\" (...)".to_string(),
            exarrow::QueryError::ExecutionFailed("schema PUBLIC not found".to_string()),
            Some("PUBLIC"),
        );

        let rendered = err.to_string();
        assert!(rendered.contains("CREATE SCHEMA \"PUBLIC\";"));
        assert!(rendered.contains("EXA_SCHEMA"));
    }

    #[test]
    fn schema_hint_quotes_identifiers() {
        let err = DbError::query_with_schema_hint(
            "ensure_tables",
            "CREATE TABLE \"MY\"\"SCHEMA\".\"SCHED_TASKS\" (...)".to_string(),
            exarrow::QueryError::ExecutionFailed("schema MY\"SCHEMA does not exist".to_string()),
            Some("MY\"SCHEMA"),
        );

        assert!(err.to_string().contains("CREATE SCHEMA \"MY\"\"SCHEMA\";"));
    }

    #[test]
    fn non_schema_query_error_stays_plain() {
        let err = DbError::query_with_schema_hint(
            "execute_statement",
            "SELECT broken".to_string(),
            exarrow::QueryError::ExecutionFailed("syntax error".to_string()),
            Some("SCHED"),
        );

        assert!(matches!(err, DbError::Query { .. }));
    }
}
