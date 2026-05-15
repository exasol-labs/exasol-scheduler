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

        Ok(Self { config })
    }

    /// Single place for EXA_ALL_OBJECTS SQL so Exasol-version tweaks are isolated.
    pub fn tasks_last_changed_sql(&self) -> String {
        build_tasks_last_changed_query(&self.config.schema, &self.config.tasks_table)
    }

    fn load_tasks_sql(&self) -> String {
        format!(
            "SELECT \"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"STATEMENT\", \"AFTER\", \"IS_FINAL\", \"COMMENT\" FROM {}.{}",
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

    fn query_batches(
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
        let operation = "execute_statement";
        let sql_text = sql.to_string();

        Self::run_async(operation, async {
            let mut connection = self.connect(operation).await?;

            // Execute the statement exactly as stored (no client-side splitting/rewrite).
            let result = connection
                .execute_update(sql_text.clone())
                .await
                .map(|_| ())
                .map_err(|source| DbError::Query {
                    operation,
                    sql: sql_text,
                    source,
                });

            if let Err(close_err) = connection.close().await {
                tracing::warn!(operation, error = %close_err, "failed to close Exasol connection");
            }

            result
        })
    }

    fn write_history(&self, _event: &HistoryEvent) -> Result<(), DbError> {
        // Stage-1b intentionally does not persist execution history yet.
        Ok(())
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
        let task_ids = as_string_array(required_column(batch, "TASK_ID", operation)?, "TASK_ID", operation)?;
        let enabled = as_bool_array(required_column(batch, "ENABLED", operation)?, "ENABLED", operation)?;
        let schedules = as_string_array(
            required_column(batch, "SCHEDULE", operation)?,
            "SCHEDULE",
            operation,
        )?;
        let statements = as_string_array(
            required_column(batch, "STATEMENT", operation)?,
            "STATEMENT",
            operation,
        )?;
        let after = as_string_array(required_column(batch, "AFTER", operation)?, "AFTER", operation)?;
        let is_final = as_bool_array(
            required_column(batch, "IS_FINAL", operation)?,
            "IS_FINAL",
            operation,
        )?;
        let comments = as_string_array(required_column(batch, "COMMENT", operation)?, "COMMENT", operation)?;

        for row_idx in 0..batch.num_rows() {
            rows.push(TaskRow {
                task_id: required_string(task_ids, row_idx, "TASK_ID", operation)?,
                enabled: required_bool(enabled, row_idx, "ENABLED", operation)?,
                schedule: required_string(schedules, row_idx, "SCHEDULE", operation)?,
                statement: required_string(statements, row_idx, "STATEMENT", operation)?,
                after: optional_string(after, row_idx),
                is_final: required_bool(is_final, row_idx, "IS_FINAL", operation)?,
                comment: optional_string(comments, row_idx),
            });
        }
    }

    Ok(rows)
}

pub fn build_tasks_last_changed_query(schema: &str, table: &str) -> String {
    // LAST_COMMIT is the current Stage-1b metadata signal. If your Exasol version
    // exposes a different "changed" column, update this function only.
    format!(
        "SELECT LAST_COMMIT AS LAST_CHANGED FROM SYS.EXA_ALL_OBJECTS WHERE UPPER(OBJECT_SCHEMA) = UPPER({}) AND UPPER(OBJECT_NAME) = UPPER({}) ORDER BY LAST_COMMIT DESC LIMIT 1",
        quote_literal(schema),
        quote_literal(table)
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
    use arrow::array::ArrayRef;
    use chrono::{TimeZone, Timelike};
    use crate::db::SchedulerDb;
    use std::sync::Arc;
    use uuid::Uuid;

    fn config(dsn: &str, schema: &str, tasks_table: &str) -> ExasolDbConfig {
        ExasolDbConfig {
            dsn: dsn.to_string(),
            schema: schema.to_string(),
            tasks_table: tasks_table.to_string(),
        }
    }

    fn sample_task_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "TASK_ID",
                Arc::new(StringArray::from(vec!["task_1"])) as ArrayRef,
            ),
            ("ENABLED", Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
            (
                "SCHEDULE",
                Arc::new(StringArray::from(vec!["CRON 0 * * * * * TZ=UTC"])) as ArrayRef,
            ),
            (
                "STATEMENT",
                Arc::new(StringArray::from(vec!["SELECT 1"])) as ArrayRef,
            ),
            (
                "AFTER",
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
            ("IS_FINAL", Arc::new(BooleanArray::from(vec![false])) as ArrayRef),
            (
                "COMMENT",
                Arc::new(StringArray::from(vec![Some("note")])) as ArrayRef,
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
    }

    #[test]
    fn last_changed_query_escapes_single_quotes() {
        let sql = build_tasks_last_changed_query("SCHE'MA", "TA'BLE");
        assert!(sql.contains("UPPER('SCHE''MA')"));
        assert!(sql.contains("UPPER('TA''BLE')"));
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
        assert!(as_bool_array(enabled_column, "ENABLED", "decode")
            .unwrap()
            .value(0));

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

    #[test]
    fn write_history_is_noop_in_stage_1b() {
        let db = ExasolDb::new(config(
            "exasol://sys:pw@localhost:8563?tls=0",
            "PUBLIC",
            "SCHED_TASKS",
        ))
        .expect("constructor should accept non-empty values");

        let event = HistoryEvent {
            run_id: Uuid::nil(),
            graph_run_id: None,
            task_id: "task_1".to_string(),
            graph_phase: "ROOT".to_string(),
            scheduled_for: None,
            started_at: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            finished_at: None,
            status: "SUCCEEDED".to_string(),
            error_message: None,
        };

        assert!(db.write_history(&event).is_ok());
    }
}
