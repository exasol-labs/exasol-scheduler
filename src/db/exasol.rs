use std::future::Future;

use arrow::array::{
    Array, BooleanArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use exarrow::adbc::Driver;

use crate::db::{DbError, SchedulerDb};
use crate::model::{HistoryEvent, TaskRow};

#[derive(Debug, Clone)]
pub struct ExasolDbConfig {
    pub dsn: String,
    pub schema: String,
    pub tasks_table: String,
    pub history_table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureTablesResult {
    pub tasks_table_created: bool,
    pub history_table_created: bool,
    pub sql_text_column_renamed: bool,
    pub parallel_children_added: bool,
    pub schedule_nullable_altered: bool,
}

/// Production Exasol adapter backed by exarrow-rs.
///
/// The scheduler core keeps using the `SchedulerDb` trait, so tests can continue
/// to use in-memory fakes while this adapter handles real Exasol I/O.
pub struct ExasolDb {
    config: ExasolDbConfig,
}

impl ExasolDb {
    pub fn new(config: ExasolDbConfig) -> Result<Self, DbError> {
        if config.dsn.trim().is_empty() {
            return Err(DbError::Config("EXA DSN must not be empty".to_string()));
        }
        if config.schema.trim().is_empty() {
            return Err(DbError::Config("schema must not be empty".to_string()));
        }
        if config.tasks_table.trim().is_empty() {
            return Err(DbError::Config("tasks table must not be empty".to_string()));
        }
        if config.history_table.trim().is_empty() {
            return Err(DbError::Config(
                "history table must not be empty".to_string(),
            ));
        }

        Ok(Self { config })
    }

    /// Single place for EXA_ALL_OBJECTS SQL so Exasol-version tweaks are isolated.
    pub fn tasks_last_changed_sql(&self) -> String {
        build_tasks_last_changed_query(&self.config.schema, &self.config.tasks_table)
    }

    fn load_tasks_sql(&self) -> String {
        format!(
            "SELECT \"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"SQL_TEXT\", \
             \"AFTER\", \"IS_FINAL\", \"COMMENT\", \"PARALLEL_CHILDREN\" FROM {}.{}",
            quote_identifier(&self.config.schema),
            quote_identifier(&self.config.tasks_table)
        )
    }

    fn run_async<T>(
        operation: &'static str,
        future: impl Future<Output = Result<T, DbError>>,
    ) -> Result<T, DbError> {
        // Each DB call gets an isolated runtime so the trait can remain synchronous
        // without coupling scheduler-core to async execution details.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| DbError::RuntimeInit(format!("{operation}: {err}")))?;

        runtime.block_on(future)
    }

    async fn connect(&self, operation: &'static str) -> Result<exarrow::Connection, DbError> {
        let driver = Driver::new();
        let database = driver
            .open(&self.config.dsn)
            .map_err(|source| DbError::Connection { operation, source })?;

        database
            .connect()
            .await
            .map_err(|source| DbError::Connection { operation, source })
    }

    pub fn query_batches(
        &self,
        operation: &'static str,
        sql: String,
    ) -> Result<Vec<RecordBatch>, DbError> {
        Self::run_async(operation, async {
            let mut connection = self.connect(operation).await?;
            let result = connection
                .query(sql.clone())
                .await
                .map_err(|source| DbError::Query {
                    operation,
                    sql,
                    source,
                });

            if let Err(close_err) = connection.close().await {
                tracing::warn!(operation, error = %close_err, "failed to close Exasol connection");
            }

            result
        })
    }

    /// Creates `SCHED_TASKS` and `SCHED_HISTORY` if they do not already exist.
    /// Safe to call on every startup — it is a no-op when both tables are present.
    pub fn ensure_tables(&self) -> Result<EnsureTablesResult, DbError> {
        self.execute_schema_statement(&build_create_schema_sql(&self.config.schema))?;

        let tasks_table_created = if !self
            .table_exists(&self.config.schema, &self.config.tasks_table)?
        {
            tracing::info!(
                schema = self.config.schema.as_str(),
                table = self.config.tasks_table.as_str(),
                "creating table"
            );
            let sql = build_create_tasks_table_sql(&self.config.schema, &self.config.tasks_table);
            self.execute_schema_statement(&sql)?;
            true
        } else {
            tracing::debug!(
                table = self.config.tasks_table.as_str(),
                "table already exists"
            );
            false
        };

        let sql_text_column_renamed = if !tasks_table_created {
            self.ensure_sql_text_column(&self.config.schema, &self.config.tasks_table)?
        } else {
            false
        };

        let parallel_children_added = if !tasks_table_created {
            self.ensure_parallel_children_column(&self.config.schema, &self.config.tasks_table)?
        } else {
            false
        };

        let schedule_nullable_altered = if !tasks_table_created {
            self.ensure_schedule_nullable(&self.config.schema, &self.config.tasks_table)?
        } else {
            false
        };

        let history_table_created =
            if !self.table_exists(&self.config.schema, &self.config.history_table)? {
                tracing::info!(
                    schema = self.config.schema.as_str(),
                    table = self.config.history_table.as_str(),
                    "creating table"
                );
                let sql =
                    build_create_history_table_sql(&self.config.schema, &self.config.history_table);
                self.execute_schema_statement(&sql)?;
                true
            } else {
                tracing::debug!(
                    table = self.config.history_table.as_str(),
                    "table already exists"
                );
                false
            };

        Ok(EnsureTablesResult {
            tasks_table_created,
            history_table_created,
            sql_text_column_renamed,
            parallel_children_added,
            schedule_nullable_altered,
        })
    }

    fn ensure_sql_text_column(&self, schema: &str, table: &str) -> Result<bool, DbError> {
        if self.column_exists(schema, table, "SQL_TEXT")? {
            return Ok(false);
        }

        if !self.column_exists(schema, table, "STATEMENT")? {
            return Err(DbError::Config(format!(
                "task table {schema}.{table} is missing required SQL_TEXT column"
            )));
        }

        let alter_sql = build_rename_statement_column_sql(schema, table);
        tracing::warn!(
            schema,
            table,
            "renaming legacy task column STATEMENT to SQL_TEXT"
        );
        self.execute_schema_statement(&alter_sql)?;
        Ok(true)
    }

    fn ensure_parallel_children_column(&self, schema: &str, table: &str) -> Result<bool, DbError> {
        if self.column_exists(schema, table, "PARALLEL_CHILDREN")? {
            return Ok(false);
        }
        let alter_sql = format!(
            "ALTER TABLE {schema}.{table} ADD COLUMN \
             \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE",
            schema = quote_identifier(schema),
            table = quote_identifier(table),
        );
        tracing::warn!(
            schema,
            table,
            "adding PARALLEL_CHILDREN column (schema migration) — \
             all existing parent tasks will now execute children in parallel; \
             set PARALLEL_CHILDREN=FALSE on any task that requires sequential ordering"
        );
        self.execute_schema_statement(&alter_sql)?;
        Ok(true)
    }

    fn ensure_schedule_nullable(&self, schema: &str, table: &str) -> Result<bool, DbError> {
        let alter_sql = build_make_schedule_nullable_sql(schema, table);
        tracing::warn!(schema, table, "making SCHEDULE nullable for child tasks");
        self.execute_schema_statement(&alter_sql)?;
        Ok(true)
    }

    fn column_exists(&self, schema: &str, table: &str, column: &str) -> Result<bool, DbError> {
        let sql = format!(
            "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS \
             WHERE UPPER(COLUMN_SCHEMA) = UPPER({schema}) \
             AND UPPER(COLUMN_TABLE) = UPPER({table}) \
             AND UPPER(COLUMN_NAME) = UPPER({column}) \
             LIMIT 1",
            schema = quote_literal(schema),
            table = quote_literal(table),
            column = quote_literal(column),
        );
        let batches = self.query_batches("ensure_task_column", sql)?;
        Ok(batches.iter().any(|b| b.num_rows() > 0))
    }

    fn execute_schema_statement(&self, sql: &str) -> Result<(), DbError> {
        self.execute_statement_internal(sql, Some(self.config.schema.as_str()))
    }

    fn execute_statement_internal(
        &self,
        sql: &str,
        schema_hint: Option<&str>,
    ) -> Result<(), DbError> {
        let operation = "execute_statement";
        let sql_text = sql.to_string();

        Self::run_async(operation, async {
            let mut connection = self.connect(operation).await?;

            let result = async {
                let result_set = connection
                    .execute(sql_text.clone())
                    .await
                    .map_err(|source| {
                        DbError::query_with_schema_hint(
                            operation,
                            sql_text.clone(),
                            source,
                            schema_hint,
                        )
                    })?;

                if result_set.row_count().is_some() {
                    return Ok(());
                }

                // Scheduled statements may be SELECTs; fetch and discard rows so
                // server-side result handles are released while preserving success.
                result_set.fetch_all().await.map(|_| ()).map_err(|source| {
                    DbError::query_with_schema_hint(
                        operation,
                        sql_text.clone(),
                        source,
                        schema_hint,
                    )
                })
            }
            .await;

            if let Err(close_err) = connection.close().await {
                tracing::warn!(operation, error = %close_err, "failed to close Exasol connection");
            }

            result
        })
    }

    fn table_exists(&self, schema: &str, table: &str) -> Result<bool, DbError> {
        let sql = format!(
            "SELECT OBJECT_NAME FROM SYS.EXA_ALL_OBJECTS \
             WHERE ROOT_TYPE = 'SCHEMA' \
             AND UPPER(ROOT_NAME) = UPPER({}) \
             AND UPPER(OBJECT_NAME) = UPPER({}) \
             LIMIT 1",
            quote_literal(schema),
            quote_literal(table),
        );
        let batches = self.query_batches("ensure_tables", sql)?;
        let found = batches.iter().any(|b| b.num_rows() > 0);
        Ok(found)
    }
}

impl SchedulerDb for ExasolDb {
    fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
        let operation = "get_last_changed";
        let sql = self.tasks_last_changed_sql();
        let batches = self.query_batches(operation, sql)?;
        decode_last_changed_from_batches(operation, &batches)
    }

    fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError> {
        let operation = "load_tasks";
        let sql = self.load_tasks_sql();
        let batches = self.query_batches(operation, sql)?;
        decode_task_rows_from_batches(operation, &batches)
    }

    fn execute_statement(&self, sql: &str) -> Result<(), DbError> {
        self.execute_statement_internal(sql, None)
    }

    fn write_history(&self, event: &HistoryEvent) -> Result<(), DbError> {
        let operation = "write_history";
        let sql = build_write_history_sql(&self.config.schema, &self.config.history_table, event);
        Self::run_async(operation, async {
            let mut connection = self.connect(operation).await?;
            let result = connection
                .execute_update(sql.clone())
                .await
                .map(|_| ())
                .map_err(|source| DbError::Query {
                    operation,
                    sql,
                    source,
                });
            if let Err(close_err) = connection.close().await {
                tracing::warn!(operation, error = %close_err, "failed to close Exasol connection");
            }
            result
        })
    }
}

fn decode_last_changed_from_batches(
    operation: &'static str,
    batches: &[RecordBatch],
) -> Result<DateTime<Utc>, DbError> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let column = batch
            .column_by_name("LAST_CHANGED")
            .ok_or_else(|| DbError::Decode {
                operation,
                message: "LAST_CHANGED column missing from EXA_ALL_OBJECTS query".to_string(),
            })?;

        return timestamp_at(column.as_ref(), 0, operation);
    }

    Err(DbError::NotFound { operation })
}

fn decode_task_rows_from_batches(
    operation: &'static str,
    batches: &[RecordBatch],
) -> Result<Vec<TaskRow>, DbError> {
    let mut rows = Vec::new();
    for batch in batches {
        let task_ids = as_string_array(
            required_column(batch, "TASK_ID", operation)?,
            "TASK_ID",
            operation,
        )?;
        let enabled = as_bool_array(
            required_column(batch, "ENABLED", operation)?,
            "ENABLED",
            operation,
        )?;
        let schedules = as_string_array(
            required_column(batch, "SCHEDULE", operation)?,
            "SCHEDULE",
            operation,
        )?;
        let statements = as_string_array(
            required_column(batch, "SQL_TEXT", operation)?,
            "SQL_TEXT",
            operation,
        )?;
        let after = as_string_array(
            required_column(batch, "AFTER", operation)?,
            "AFTER",
            operation,
        )?;
        let is_final = as_bool_array(
            required_column(batch, "IS_FINAL", operation)?,
            "IS_FINAL",
            operation,
        )?;
        let comments = as_string_array(
            required_column(batch, "COMMENT", operation)?,
            "COMMENT",
            operation,
        )?;
        let parallel_children_col = batch
            .column_by_name("PARALLEL_CHILDREN")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());

        for row_idx in 0..batch.num_rows() {
            let parallel_children = parallel_children_col
                .and_then(|a| {
                    if a.is_null(row_idx) {
                        None
                    } else {
                        Some(a.value(row_idx))
                    }
                })
                .unwrap_or(true);

            let after_value = optional_string(after, row_idx);
            let schedule = match (optional_string(schedules, row_idx), after_value.as_ref()) {
                (Some(schedule), _) => schedule,
                (None, Some(_)) => String::new(),
                (None, None) => {
                    return Err(DbError::Decode {
                        operation,
                        message: format!(
                            "SCHEDULE is NULL for root task at row {row_idx}; roots require a schedule"
                        ),
                    });
                }
            };

            rows.push(TaskRow {
                task_id: required_string(task_ids, row_idx, "TASK_ID", operation)?,
                enabled: required_bool(enabled, row_idx, "ENABLED", operation)?,
                schedule,
                statement: required_string(statements, row_idx, "SQL_TEXT", operation)?,
                after: after_value,
                is_final: required_bool(is_final, row_idx, "IS_FINAL", operation)?,
                comment: optional_string(comments, row_idx),
                parallel_children,
            });
        }
    }

    Ok(rows)
}

pub fn build_tasks_last_changed_query(schema: &str, table: &str) -> String {
    // ROOT_NAME is the schema identifier in EXA_ALL_OBJECTS (Exasol 2025+/Nano).
    // ROOT_TYPE = 'SCHEMA' excludes virtual schemas and other root types.
    format!(
        "SELECT LAST_COMMIT AS LAST_CHANGED FROM SYS.EXA_ALL_OBJECTS \
         WHERE ROOT_TYPE = 'SCHEMA' \
         AND UPPER(ROOT_NAME) = UPPER({}) \
         AND UPPER(OBJECT_NAME) = UPPER({}) \
         ORDER BY LAST_COMMIT DESC LIMIT 1",
        quote_literal(schema),
        quote_literal(table)
    )
}

fn build_create_schema_sql(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {}", quote_identifier(schema))
}

fn build_rename_statement_column_sql(schema: &str, table: &str) -> String {
    format!(
        "ALTER TABLE {schema}.{table} RENAME COLUMN \"STATEMENT\" TO \"SQL_TEXT\"",
        schema = quote_identifier(schema),
        table = quote_identifier(table),
    )
}

pub fn build_create_tasks_table_sql(schema: &str, table: &str) -> String {
    format!(
        "CREATE TABLE {schema}.{table} (\
            \"TASK_ID\" VARCHAR(128) NOT NULL, \
            \"ENABLED\" BOOLEAN DEFAULT TRUE, \
            \"SCHEDULE\" VARCHAR(512), \
            \"SQL_TEXT\" VARCHAR(2000000) NOT NULL, \
            \"AFTER\" VARCHAR(128), \
            \"IS_FINAL\" BOOLEAN DEFAULT FALSE, \
            \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE, \
            \"COMMENT\" VARCHAR(2000), \
            PRIMARY KEY (\"TASK_ID\"))",
        schema = quote_identifier(schema),
        table = quote_identifier(table),
    )
}

pub fn build_make_schedule_nullable_sql(schema: &str, table: &str) -> String {
    format!(
        "ALTER TABLE {schema}.{table} MODIFY COLUMN \"SCHEDULE\" VARCHAR(512)",
        schema = quote_identifier(schema),
        table = quote_identifier(table),
    )
}

pub fn build_create_history_table_sql(schema: &str, table: &str) -> String {
    format!(
        "CREATE TABLE {schema}.{table} (\
            \"RUN_ID\" VARCHAR(36) NOT NULL, \
            \"GRAPH_RUN_ID\" VARCHAR(36), \
            \"TASK_ID\" VARCHAR(128) NOT NULL, \
            \"GRAPH_PHASE\" VARCHAR(16) NOT NULL, \
            \"SCHEDULED_FOR\" TIMESTAMP, \
            \"STARTED_AT\" TIMESTAMP NOT NULL, \
            \"FINISHED_AT\" TIMESTAMP, \
            \"STATUS\" VARCHAR(16) NOT NULL, \
            \"ERROR_MESSAGE\" VARCHAR(2000000), \
            PRIMARY KEY (\"RUN_ID\"))",
        schema = quote_identifier(schema),
        table = quote_identifier(table),
    )
}

pub fn build_write_history_sql(schema: &str, history_table: &str, event: &HistoryEvent) -> String {
    let graph_run_id = event
        .graph_run_id
        .map(|id| quote_literal(&id.to_string()))
        .unwrap_or_else(|| "NULL".to_string());
    let scheduled_for = event
        .scheduled_for
        .map(|ts| format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.3f")))
        .unwrap_or_else(|| "NULL".to_string());
    let started_at = format!(
        "TIMESTAMP '{}'",
        event.started_at.format("%Y-%m-%d %H:%M:%S%.3f")
    );
    let finished_at = event
        .finished_at
        .map(|ts| format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.3f")))
        .unwrap_or_else(|| "NULL".to_string());
    let error_message = event
        .error_message
        .as_deref()
        .map(quote_literal)
        .unwrap_or_else(|| "NULL".to_string());

    format!(
        "INSERT INTO {schema}.{table} \
         (RUN_ID, GRAPH_RUN_ID, TASK_ID, GRAPH_PHASE, \
          SCHEDULED_FOR, STARTED_AT, FINISHED_AT, STATUS, ERROR_MESSAGE) \
         VALUES ({run_id}, {graph_run_id}, {task_id}, {graph_phase}, \
                 {scheduled_for}, {started_at}, {finished_at}, {status}, {error_message})",
        schema = quote_identifier(schema),
        table = quote_identifier(history_table),
        run_id = quote_literal(&event.run_id.to_string()),
        graph_run_id = graph_run_id,
        task_id = quote_literal(&event.task_id),
        graph_phase = quote_literal(&event.graph_phase),
        scheduled_for = scheduled_for,
        started_at = started_at,
        finished_at = finished_at,
        status = quote_literal(&event.status),
        error_message = error_message,
    )
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn required_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    operation: &'static str,
) -> Result<&'a dyn Array, DbError> {
    batch
        .column_by_name(name)
        .map(|col| col.as_ref())
        .ok_or_else(|| DbError::Decode {
            operation,
            message: format!("missing expected column {name}"),
        })
}

fn as_string_array<'a>(
    array: &'a dyn Array,
    column: &str,
    operation: &'static str,
) -> Result<&'a StringArray, DbError> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Decode {
            operation,
            message: format!("column {column} is not Utf8"),
        })
}

fn as_bool_array<'a>(
    array: &'a dyn Array,
    column: &str,
    operation: &'static str,
) -> Result<&'a BooleanArray, DbError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DbError::Decode {
            operation,
            message: format!("column {column} is not Boolean"),
        })
}

fn required_string(
    array: &StringArray,
    row: usize,
    column: &str,
    operation: &'static str,
) -> Result<String, DbError> {
    if array.is_null(row) {
        return Err(DbError::Decode {
            operation,
            message: format!("column {column} has NULL at row {row}"),
        });
    }

    Ok(array.value(row).to_string())
}

fn optional_string(array: &StringArray, row: usize) -> Option<String> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row).to_string())
    }
}

fn required_bool(
    array: &BooleanArray,
    row: usize,
    column: &str,
    operation: &'static str,
) -> Result<bool, DbError> {
    if array.is_null(row) {
        return Err(DbError::Decode {
            operation,
            message: format!("column {column} has NULL at row {row}"),
        });
    }

    Ok(array.value(row))
}

fn timestamp_at(
    array: &dyn Array,
    row: usize,
    operation: &'static str,
) -> Result<DateTime<Utc>, DbError> {
    if array.is_null(row) {
        return Err(DbError::Decode {
            operation,
            message: format!("LAST_CHANGED is NULL at row {row}"),
        });
    }

    match array.data_type() {
        arrow::datatypes::DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: "failed to downcast LAST_CHANGED to TimestampSecondArray".to_string(),
                })?;
            let seconds = values.value(row);
            Utc.timestamp_opt(seconds, 0)
                .single()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: format!("invalid second timestamp value: {seconds}"),
                })
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: "failed to downcast LAST_CHANGED to TimestampMillisecondArray"
                        .to_string(),
                })?;
            let millis = values.value(row);
            let secs = millis.div_euclid(1_000);
            let nanos = (millis.rem_euclid(1_000) as u32) * 1_000_000;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: format!("invalid millisecond timestamp value: {millis}"),
                })
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: "failed to downcast LAST_CHANGED to TimestampMicrosecondArray"
                        .to_string(),
                })?;
            let micros = values.value(row);
            let secs = micros.div_euclid(1_000_000);
            let nanos = (micros.rem_euclid(1_000_000) as u32) * 1_000;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: format!("invalid microsecond timestamp value: {micros}"),
                })
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: "failed to downcast LAST_CHANGED to TimestampNanosecondArray"
                        .to_string(),
                })?;
            let nanos_total = values.value(row);
            let secs = nanos_total.div_euclid(1_000_000_000);
            let nanos = nanos_total.rem_euclid(1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| DbError::Decode {
                    operation,
                    message: format!("invalid nanosecond timestamp value: {nanos_total}"),
                })
        }
        _ => parse_timestamp_from_string(array, row, operation),
    }
}

fn parse_timestamp_from_string(
    array: &dyn Array,
    row: usize,
    operation: &'static str,
) -> Result<DateTime<Utc>, DbError> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Decode {
            operation,
            message: format!(
                "LAST_CHANGED must be Arrow timestamp or Utf8, got {:?}",
                array.data_type()
            ),
        })?;

    let raw = values.value(row);

    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed.with_timezone(&Utc));
    }

    let parsed = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f").map_err(|err| {
        DbError::Decode {
            operation,
            message: format!("failed to parse LAST_CHANGED '{raw}': {err}"),
        }
    })?;

    Ok(Utc.from_utc_datetime(&parsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchedulerDb;
    use arrow::array::ArrayRef;
    use chrono::{TimeZone, Timelike};
    use std::sync::Arc;
    use uuid::Uuid;

    fn config(dsn: &str, schema: &str, tasks_table: &str) -> ExasolDbConfig {
        ExasolDbConfig {
            dsn: dsn.to_string(),
            schema: schema.to_string(),
            tasks_table: tasks_table.to_string(),
            history_table: "SCHED_HISTORY".to_string(),
        }
    }

    fn sample_task_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["task_1"])) as ArrayRef,
            ),
            (
                "ENABLED",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec!["CRON 0 * * * * * TZ=UTC"])) as ArrayRef,
            ),
            (
                "SQL_TEXT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "IS_FINAL",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![Some("note")])) as ArrayRef,
            ),
            (
                "PARALLEL_CHILDREN",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
        ])
        .expect("sample batch should be valid")
    }

    fn last_changed_batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "LAST_CHANGED",
            Arc::new(TimestampSecondArray::from(values)) as ArrayRef,
        )])
        .expect("last_changed batch should be valid")
    }

    #[test]
    fn constructor_rejects_empty_required_fields() {
        assert!(matches!(
            ExasolDb::new(config("   ", "PUBLIC", "SCHED_TASKS")),
            Err(DbError::Config(message)) if message.contains("DSN")
        ));
        assert!(matches!(
            ExasolDb::new(config("exasol://u:p@h:8563", " ", "SCHED_TASKS")),
            Err(DbError::Config(message)) if message.contains("schema")
        ));
        assert!(matches!(
            ExasolDb::new(config("exasol://u:p@h:8563", "PUBLIC", " ")),
            Err(DbError::Config(message)) if message.contains("tasks table")
        ));
        assert!(matches!(
            ExasolDb::new(ExasolDbConfig {
                dsn: "exasol://u:p@h:8563".to_string(),
                schema: "PUBLIC".to_string(),
                tasks_table: "SCHED_TASKS".to_string(),
                history_table: "  ".to_string(),
            }),
            Err(DbError::Config(message)) if message.contains("history table")
        ));
    }

    #[test]
    fn sql_builders_quote_schema_and_table_correctly() {
        let db = ExasolDb::new(config(
            "exasol://sys:pw@localhost:8563",
            "APP\"SCHEMA",
            "TASK\"TABLE",
        ))
        .expect("db config should parse");

        let load_tasks_sql = db.load_tasks_sql();
        assert!(load_tasks_sql.contains("FROM \"APP\"\"SCHEMA\".\"TASK\"\"TABLE\""));

        let changed_sql = db.tasks_last_changed_sql();
        assert!(changed_sql.contains("LAST_COMMIT AS LAST_CHANGED"));
        assert!(changed_sql.contains("SYS.EXA_ALL_OBJECTS"));
        assert!(changed_sql.contains("ROOT_NAME"));
    }

    #[test]
    fn last_changed_query_escapes_single_quotes_and_uses_root_name() {
        let sql = build_tasks_last_changed_query("SCHE'MA", "TA'BLE");
        assert!(sql.contains("UPPER('SCHE''MA')"));
        assert!(sql.contains("UPPER('TA''BLE')"));
        assert!(sql.contains("ROOT_NAME"));
        assert!(sql.contains("ROOT_TYPE = 'SCHEMA'"));
    }

    #[test]
    fn quote_helpers_escape_values() {
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
        assert_eq!(quote_literal("A'B"), "'A''B'");
    }

    #[test]
    fn required_column_returns_error_when_missing() {
        let batch = sample_task_batch();
        let err = required_column(&batch, "MISSING", "decode").unwrap_err();
        assert!(matches!(
            err,
            DbError::Decode { operation, message }
                if operation == "decode" && message.contains("missing expected column MISSING")
        ));
    }

    #[test]
    fn type_conversions_validate_expected_arrow_types() {
        let batch = sample_task_batch();
        let task_id_column = required_column(&batch, "TASK_ID", "decode").unwrap();
        let enabled_column = required_column(&batch, "ENABLED", "decode").unwrap();

        assert_eq!(
            as_string_array(task_id_column, "TASK_ID", "decode")
                .unwrap()
                .value(0),
            "task_1"
        );
        assert!(
            as_bool_array(enabled_column, "ENABLED", "decode")
                .unwrap()
                .value(0)
        );

        let err = as_string_array(enabled_column, "ENABLED", "decode").unwrap_err();
        assert!(matches!(
            err,
            DbError::Decode { message, .. } if message.contains("column ENABLED is not Utf8")
        ));

        let err = as_bool_array(task_id_column, "TASK_ID", "decode").unwrap_err();
        assert!(matches!(
            err,
            DbError::Decode { message, .. } if message.contains("column TASK_ID is not Boolean")
        ));
    }

    #[test]
    fn required_and_optional_value_helpers_handle_nulls() {
        let strings = StringArray::from(vec![Some("value"), None]);
        let bools = BooleanArray::from(vec![Some(true), None]);

        assert_eq!(
            required_string(&strings, 0, "COL", "decode").unwrap(),
            "value".to_string()
        );
        assert_eq!(optional_string(&strings, 0), Some("value".to_string()));
        assert_eq!(optional_string(&strings, 1), None);
        assert!(required_bool(&bools, 0, "FLAG", "decode").unwrap());

        let string_err = required_string(&strings, 1, "COL", "decode").unwrap_err();
        assert!(matches!(
            string_err,
            DbError::Decode { message, .. } if message.contains("column COL has NULL at row 1")
        ));

        let bool_err = required_bool(&bools, 1, "FLAG", "decode").unwrap_err();
        assert!(matches!(
            bool_err,
            DbError::Decode { message, .. } if message.contains("column FLAG has NULL at row 1")
        ));
    }

    #[test]
    fn timestamp_at_supports_all_timestamp_units() {
        let seconds = TimestampSecondArray::from(vec![1_i64]);
        let millis = TimestampMillisecondArray::from(vec![1_500_i64]);
        let micros = TimestampMicrosecondArray::from(vec![1_500_000_i64]);
        let nanos = TimestampNanosecondArray::from(vec![1_500_000_000_i64]);

        let expected = Utc.timestamp_opt(1, 500_000_000).single().unwrap();

        assert_eq!(
            timestamp_at(&seconds, 0, "decode").unwrap(),
            Utc.timestamp_opt(1, 0).single().unwrap()
        );
        assert_eq!(timestamp_at(&millis, 0, "decode").unwrap(), expected);
        assert_eq!(timestamp_at(&micros, 0, "decode").unwrap(), expected);
        assert_eq!(timestamp_at(&nanos, 0, "decode").unwrap(), expected);
    }

    #[test]
    fn timestamp_at_and_string_parsing_validate_errors() {
        let null_ts = TimestampSecondArray::from(vec![None::<i64>]);
        let null_err = timestamp_at(&null_ts, 0, "decode").unwrap_err();
        assert!(matches!(
            null_err,
            DbError::Decode { message, .. } if message.contains("LAST_CHANGED is NULL")
        ));

        let rfc = StringArray::from(vec!["2026-01-02T03:04:05Z"]);
        assert_eq!(
            timestamp_at(&rfc, 0, "decode").unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap()
        );

        let naive = StringArray::from(vec!["2026-01-02 03:04:05.123456"]);
        assert_eq!(
            timestamp_at(&naive, 0, "decode").unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
                .unwrap()
                .with_nanosecond(123_456_000)
                .unwrap()
        );

        let invalid = StringArray::from(vec!["not-a-timestamp"]);
        let invalid_err = timestamp_at(&invalid, 0, "decode").unwrap_err();
        assert!(matches!(
            invalid_err,
            DbError::Decode { message, .. } if message.contains("failed to parse LAST_CHANGED")
        ));

        let bools = BooleanArray::from(vec![true]);
        let type_err = parse_timestamp_from_string(&bools, 0, "decode").unwrap_err();
        assert!(matches!(
            type_err,
            DbError::Decode { message, .. } if message.contains("LAST_CHANGED must be Arrow timestamp or Utf8")
        ));
    }

    #[test]
    fn decode_last_changed_from_batches_handles_empty_missing_and_valid_batches() {
        let empty = RecordBatch::try_from_iter(vec![(
            "LAST_CHANGED",
            Arc::new(TimestampSecondArray::from(Vec::<Option<i64>>::new())) as ArrayRef,
        )])
        .unwrap();
        let valid = last_changed_batch(vec![Some(1)]);

        let parsed = decode_last_changed_from_batches("op", &[empty.clone(), valid]).unwrap();
        assert_eq!(parsed, Utc.timestamp_opt(1, 0).single().unwrap());

        let not_found = decode_last_changed_from_batches("op", &[empty]).unwrap_err();
        assert!(matches!(not_found, DbError::NotFound { operation } if operation == "op"));

        let missing_col = RecordBatch::try_from_iter(vec![(
            "NOT_LAST_CHANGED",
            Arc::new(TimestampSecondArray::from(vec![Some(1)])) as ArrayRef,
        )])
        .unwrap();
        let err = decode_last_changed_from_batches("op", &[missing_col]).unwrap_err();
        assert!(matches!(
            err,
            DbError::Decode { message, .. }
                if message.contains("LAST_CHANGED column missing from EXA_ALL_OBJECTS query")
        ));
    }

    #[test]
    fn decode_task_rows_from_batches_decodes_rows_and_validates_columns() {
        let rows = decode_task_rows_from_batches("load_tasks", &[sample_task_batch()])
            .expect("decode should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].task_id, "task_1");
        assert!(rows[0].enabled);
        assert_eq!(rows[0].schedule, "CRON 0 * * * * * TZ=UTC");
        assert_eq!(rows[0].statement, "SELECT 1");
        assert_eq!(rows[0].after, None);
        assert!(!rows[0].is_final);
        assert_eq!(rows[0].comment, Some("note".to_string()));
        assert!(
            rows[0].parallel_children,
            "PARALLEL_CHILDREN=true must decode correctly"
        );

        let missing_col_batch = RecordBatch::try_from_iter(vec![(
            "TASK_ID",
            Arc::new(StringArray::from(vec!["task_1"])) as ArrayRef,
        )])
        .unwrap();
        let err = decode_task_rows_from_batches("load_tasks", &[missing_col_batch]).unwrap_err();
        assert!(matches!(
            err,
            DbError::Decode { message, .. } if message.contains("missing expected column ENABLED")
        ));
    }

    fn sample_event() -> HistoryEvent {
        HistoryEvent {
            run_id: Uuid::nil(),
            graph_run_id: None,
            task_id: "task_1".to_string(),
            graph_phase: "MAIN".to_string(),
            scheduled_for: Some(Utc.with_ymd_and_hms(2026, 1, 2, 3, 0, 0).unwrap()),
            started_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            finished_at: Some(Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 6).unwrap()),
            status: "SUCCEEDED".to_string(),
            error_message: None,
        }
    }

    #[test]
    fn write_history_sql_contains_all_required_fields() {
        let event = sample_event();
        let sql = build_write_history_sql("PUBLIC", "SCHED_HISTORY", &event);
        assert!(sql.contains("INSERT INTO \"PUBLIC\".\"SCHED_HISTORY\""));
        assert!(sql.contains("RUN_ID, GRAPH_RUN_ID, TASK_ID, GRAPH_PHASE"));
        assert!(sql.contains("SCHEDULED_FOR, STARTED_AT, FINISHED_AT, STATUS, ERROR_MESSAGE"));
        assert!(sql.contains(&format!("'{}'", Uuid::nil())));
        assert!(sql.contains("'task_1'"));
        assert!(sql.contains("'MAIN'"));
        assert!(sql.contains("'SUCCEEDED'"));
        assert!(sql.contains("TIMESTAMP '2026-01-02 03:00:00.000'"));
        assert!(sql.contains("TIMESTAMP '2026-01-02 03:04:05.000'"));
        assert!(sql.contains("TIMESTAMP '2026-01-02 03:04:06.000'"));
    }

    #[test]
    fn write_history_sql_uses_null_for_absent_optional_fields() {
        let mut event = sample_event();
        event.graph_run_id = None;
        event.scheduled_for = None;
        event.finished_at = None;
        event.error_message = None;
        let sql = build_write_history_sql("PUBLIC", "SCHED_HISTORY", &event);
        // Four NULLs: graph_run_id, scheduled_for, finished_at, error_message
        assert_eq!(sql.matches("NULL").count(), 4);
    }

    #[test]
    fn write_history_sql_includes_graph_run_id_when_set() {
        let mut event = sample_event();
        let gid = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        event.graph_run_id = Some(gid);
        let sql = build_write_history_sql("PUBLIC", "SCHED_HISTORY", &event);
        assert!(sql.contains("'11111111-1111-1111-1111-111111111111'"));
    }

    #[test]
    fn write_history_sql_includes_error_message_when_set() {
        let mut event = sample_event();
        event.status = "FAILED".to_string();
        event.error_message = Some("it broke".to_string());
        let sql = build_write_history_sql("PUBLIC", "SCHED_HISTORY", &event);
        assert!(sql.contains("'FAILED'"));
        assert!(sql.contains("'it broke'"));
    }

    #[test]
    fn write_history_sql_escapes_single_quotes_in_error_message() {
        let mut event = sample_event();
        event.error_message = Some("can't connect".to_string());
        let sql = build_write_history_sql("PUBLIC", "SCHED_HISTORY", &event);
        assert!(sql.contains("'can''t connect'"));
    }

    #[test]
    fn write_history_sql_quotes_schema_and_table_identifiers() {
        let event = sample_event();
        let sql = build_write_history_sql("MY\"SCHEMA", "HIST\"TABLE", &event);
        assert!(sql.contains("\"MY\"\"SCHEMA\".\"HIST\"\"TABLE\""));
    }

    #[test]
    fn write_history_returns_connection_error_for_bad_dsn() {
        let db = ExasolDb::new(config("definitely-not-a-dsn", "PUBLIC", "SCHED_TASKS"))
            .expect("constructor only validates non-empty values");
        let err = db.write_history(&sample_event()).unwrap_err();
        assert!(matches!(
            err,
            DbError::Connection { operation, .. } if operation == "write_history"
        ));
    }

    #[test]
    fn db_methods_return_connection_errors_for_bad_dsn() {
        let db = ExasolDb::new(config("definitely-not-a-dsn", "PUBLIC", "SCHED_TASKS"))
            .expect("constructor only validates non-empty values");

        let last_changed_err = db.get_last_changed().unwrap_err();
        assert!(matches!(
            last_changed_err,
            DbError::Connection { operation, .. } if operation == "get_last_changed"
        ));

        let load_tasks_err = db.load_tasks().unwrap_err();
        assert!(matches!(
            load_tasks_err,
            DbError::Connection { operation, .. } if operation == "load_tasks"
        ));

        let execute_err = db.execute_statement("SELECT 1").unwrap_err();
        assert!(matches!(
            execute_err,
            DbError::Connection { operation, .. } if operation == "execute_statement"
        ));
    }

    #[test]
    fn create_tasks_table_sql_contains_all_required_columns() {
        let sql = build_create_tasks_table_sql("PUBLIC", "SCHED_TASKS");
        assert!(sql.contains("\"PUBLIC\".\"SCHED_TASKS\""));
        assert!(sql.contains("\"TASK_ID\""));
        assert!(sql.contains("\"ENABLED\""));
        assert!(sql.contains("\"SCHEDULE\""));
        assert!(sql.contains("\"SQL_TEXT\""));
        assert!(!sql.contains("\"STATEMENT\""));
        assert!(sql.contains("\"AFTER\""));
        assert!(sql.contains("\"IS_FINAL\""));
        assert!(sql.contains("\"PARALLEL_CHILDREN\""));
        assert!(sql.contains("\"COMMENT\""));
        assert!(sql.contains("PRIMARY KEY"));
        assert!(!sql.contains("\"SCHEDULE\" VARCHAR(512) NOT NULL"));
    }

    #[test]
    fn make_schedule_nullable_sql_quotes_identifiers() {
        assert_eq!(
            build_make_schedule_nullable_sql("MY\"SCHEMA", "TASKS"),
            "ALTER TABLE \"MY\"\"SCHEMA\".\"TASKS\" MODIFY COLUMN \"SCHEDULE\" VARCHAR(512)"
        );
    }

    #[test]
    fn create_history_table_sql_contains_all_required_columns() {
        let sql = build_create_history_table_sql("PUBLIC", "SCHED_HISTORY");
        assert!(sql.contains("\"PUBLIC\".\"SCHED_HISTORY\""));
        assert!(sql.contains("\"RUN_ID\""));
        assert!(sql.contains("\"GRAPH_RUN_ID\""));
        assert!(sql.contains("\"TASK_ID\""));
        assert!(sql.contains("\"GRAPH_PHASE\""));
        assert!(sql.contains("\"SCHEDULED_FOR\""));
        assert!(sql.contains("\"STARTED_AT\""));
        assert!(sql.contains("\"FINISHED_AT\""));
        assert!(sql.contains("\"STATUS\""));
        assert!(sql.contains("\"ERROR_MESSAGE\""));
        assert!(sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_sql_functions_quote_identifiers() {
        let schema_sql = build_create_schema_sql("MY\"SCHEMA");
        assert_eq!(schema_sql, "CREATE SCHEMA IF NOT EXISTS \"MY\"\"SCHEMA\"");

        let rename_sql = build_rename_statement_column_sql("MY\"SCHEMA", "MY\"TABLE");
        assert_eq!(
            rename_sql,
            "ALTER TABLE \"MY\"\"SCHEMA\".\"MY\"\"TABLE\" RENAME COLUMN \"STATEMENT\" TO \"SQL_TEXT\""
        );

        let tasks_sql = build_create_tasks_table_sql("MY\"SCHEMA", "MY\"TABLE");
        assert!(tasks_sql.contains("\"MY\"\"SCHEMA\".\"MY\"\"TABLE\""));

        let history_sql = build_create_history_table_sql("MY\"SCHEMA", "HIST\"TABLE");
        assert!(history_sql.contains("\"MY\"\"SCHEMA\".\"HIST\"\"TABLE\""));
    }

    #[test]
    fn build_create_tasks_table_includes_parallel_children_column() {
        let sql = build_create_tasks_table_sql("PUBLIC", "SCHED_TASKS");
        assert!(
            sql.contains("\"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE"),
            "DDL must include PARALLEL_CHILDREN with DEFAULT TRUE, got:\n{sql}"
        );
    }

    #[test]
    fn ensure_parallel_children_alter_sql_uses_default_true() {
        // The alter SQL generated by ensure_parallel_children_column must use DEFAULT TRUE.
        // We test the SQL text directly since the helper is private but the SQL is the
        // critical property (not the network call).
        let schema = "PUBLIC";
        let table = "SCHED_TASKS";
        let alter_sql = format!(
            "ALTER TABLE {schema}.{table} ADD COLUMN \
             \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE",
            schema = format!("\"{}\"", schema),
            table = format!("\"{}\"", table),
        );
        assert!(
            alter_sql.contains("DEFAULT TRUE"),
            "ALTER must use DEFAULT TRUE, not FALSE"
        );
        assert!(
            alter_sql.contains("PARALLEL_CHILDREN"),
            "ALTER must name the column"
        );
    }

    #[test]
    fn decode_task_rows_allows_null_schedule_for_child() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["child"])) as ArrayRef,
            ),
            (
                "ENABLED",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "SQL_TEXT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![Some("root")])) as ArrayRef,
            ),
            (
                "IS_FINAL",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "PARALLEL_CHILDREN",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
        ])
        .unwrap();

        let rows = decode_task_rows_from_batches("load_tasks", &[batch]).unwrap();
        assert_eq!(rows[0].after.as_deref(), Some("root"));
        assert_eq!(rows[0].schedule, "");
    }

    #[test]
    fn decode_parallel_children_true_from_task_batch() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["t"])) as ArrayRef,
            ),
            (
                "ENABLED",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec!["CRON 0 * * * * *"])) as ArrayRef,
            ),
            (
                "SQL_TEXT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "IS_FINAL",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "PARALLEL_CHILDREN",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
        ])
        .unwrap();
        let rows = decode_task_rows_from_batches("test", &[batch]).unwrap();
        assert!(rows[0].parallel_children);
    }

    #[test]
    fn decode_parallel_children_false_from_task_batch() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["t"])) as ArrayRef,
            ),
            (
                "ENABLED",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec!["CRON 0 * * * * *"])) as ArrayRef,
            ),
            (
                "SQL_TEXT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "IS_FINAL",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "PARALLEL_CHILDREN",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
        ])
        .unwrap();
        let rows = decode_task_rows_from_batches("test", &[batch]).unwrap();
        assert!(!rows[0].parallel_children);
    }

    #[test]
    fn decode_parallel_children_null_defaults_to_true() {
        // A NULL value in PARALLEL_CHILDREN (e.g. column present but not set) must default to true.
        let null_bool: BooleanArray = vec![None::<bool>].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["t"])) as ArrayRef,
            ),
            (
                "ENABLED",
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec!["CRON 0 * * * * *"])) as ArrayRef,
            ),
            (
                "SQL_TEXT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            (
                "IS_FINAL",
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            ("PARALLEL_CHILDREN", Arc::new(null_bool) as ArrayRef),
        ])
        .unwrap();
        let rows = decode_task_rows_from_batches("test", &[batch]).unwrap();
        assert!(
            rows[0].parallel_children,
            "NULL PARALLEL_CHILDREN must default to true"
        );
    }
}
