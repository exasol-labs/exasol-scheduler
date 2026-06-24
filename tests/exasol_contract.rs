use arrow::array::StringArray;
use chrono::Timelike;
use chrono::Utc;
use exasol_scheduler::config::AppConfig;
use exasol_scheduler::db::{ExasolDb, ExasolDbConfig, SchedulerDb};
use exasol_scheduler::model::HistoryEvent;
use exasol_scheduler::schedule::LocalTimeZone;
use exasol_scheduler::scheduler::Scheduler;
use exasol_scheduler::time::SystemClock;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn nano_dsn() -> String {
    std::env::var("EXA_DSN").unwrap_or_else(|_| {
        "exasol://sys:exasol@localhost:8563?tls=1&validateservercertificate=0".to_string()
    })
}

fn nano_config() -> ExasolDbConfig {
    config_for_schema("PUBLIC")
}

fn config_for_schema(schema: &str) -> ExasolDbConfig {
    ExasolDbConfig {
        dsn: nano_dsn(),
        schema: schema.to_string(),
        tasks_table: "SCHED_TASKS".to_string(),
        history_table: "SCHED_HISTORY".to_string(),
    }
}

fn skip_unless_enabled() -> bool {
    if std::env::var("EXA_CONTRACT_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping exasol contract test (set EXA_CONTRACT_TESTS=1 to enable)");
        return true;
    }
    false
}

fn contract_schema_name(prefix: &str) -> String {
    format!("{}_{}", prefix, Uuid::new_v4().simple()).to_uppercase()
}

struct SchemaDropGuard {
    dsn: String,
    schema: String,
}

impl SchemaDropGuard {
    fn new(dsn: String, schema: String) -> Self {
        Self { dsn, schema }
    }
}

impl Drop for SchemaDropGuard {
    fn drop(&mut self) {
        let _ = execute_update_direct(
            &self.dsn,
            &format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema),
        );
    }
}

fn execute_update_direct(dsn: &str, sql: &str) -> Result<i64, String> {
    let dsn = dsn.to_string();
    let sql = sql.to_string();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build test runtime: {err}"))?;

    runtime.block_on(async move {
        let driver = exarrow::adbc::Driver::new();
        let database = driver
            .open(&dsn)
            .map_err(|err| format!("failed to open Exasol DSN: {err}"))?;
        let mut connection = database
            .connect()
            .await
            .map_err(|err| format!("failed to connect to Exasol: {err}"))?;
        let result = connection
            .execute_update(sql.clone())
            .await
            .map_err(|err| format!("failed to execute setup SQL {sql:?}: {err}"));

        if let Err(close_err) = connection.close().await {
            eprintln!("warning: failed to close Exasol setup connection: {close_err}");
        }

        result
    })
}

fn must_execute_update_direct(dsn: &str, sql: &str) {
    execute_update_direct(dsn, sql).unwrap_or_else(|err| panic!("{err}"));
}

fn setup_tables(db: &ExasolDb) {
    let dsn = nano_dsn();
    for sql in [
        "CREATE SCHEMA IF NOT EXISTS PUBLIC",
        "DROP TABLE IF EXISTS PUBLIC.SCHED_HISTORY",
        "DROP TABLE IF EXISTS PUBLIC.SCHED_TASKS",
        "CREATE TABLE PUBLIC.SCHED_TASKS (\
            \"TASK_ID\" VARCHAR(128) NOT NULL, \
            \"ENABLED\" BOOLEAN DEFAULT TRUE, \
            \"SCHEDULE\" VARCHAR(512) NOT NULL, \
            \"SQL_TEXT\" VARCHAR(2000000) NOT NULL, \
            \"AFTER\" VARCHAR(128), \
            \"IS_FINAL\" BOOLEAN DEFAULT FALSE, \
            \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE, \
            \"COMMENT\" VARCHAR(2000), \
            PRIMARY KEY (\"TASK_ID\"))",
        "CREATE TABLE PUBLIC.SCHED_HISTORY (\
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
        "INSERT INTO PUBLIC.SCHED_TASKS \
            (\"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"SQL_TEXT\") \
            VALUES ('contract_task', TRUE, 'CRON 0 * * * * * TZ=UTC', 'SELECT 1')",
    ] {
        must_execute_update_direct(&dsn, sql);
    }

    assert!(
        table_exists(db, "PUBLIC", "SCHED_TASKS"),
        "setup must create PUBLIC.SCHED_TASKS"
    );
    assert!(
        table_exists(db, "PUBLIC", "SCHED_HISTORY"),
        "setup must create PUBLIC.SCHED_HISTORY"
    );
}

fn table_exists(db: &ExasolDb, schema: &str, table: &str) -> bool {
    let sql = format!(
        "SELECT TABLE_NAME FROM EXA_ALL_TABLES \
         WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
        schema, table
    );
    db.query_batches("contract_table_exists", sql)
        .expect("table existence query should succeed")
        .iter()
        .any(|batch| batch.num_rows() > 0)
}

fn column_exists(db: &ExasolDb, schema: &str, table: &str, column: &str) -> bool {
    let sql = format!(
        "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS \
         WHERE UPPER(COLUMN_SCHEMA) = UPPER('{}') \
         AND UPPER(COLUMN_TABLE) = UPPER('{}') \
         AND UPPER(COLUMN_NAME) = UPPER('{}')",
        schema, table, column
    );
    db.query_batches("contract_column_exists", sql)
        .expect("column existence query should succeed")
        .iter()
        .any(|batch| batch.num_rows() > 0)
}

fn count_history_rows(db: &ExasolDb, run_id: &str) -> usize {
    let sql = format!("SELECT RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'");
    let batches = db.query_batches("count_history", sql).unwrap_or_default();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn read_history_status(db: &ExasolDb, run_id: &str) -> Option<String> {
    let sql = format!("SELECT STATUS FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'");
    let batches = db.query_batches("read_history", sql).unwrap_or_default();
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("STATUS") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    return Some(arr.value(0).to_string());
                }
            }
        }
    }
    None
}

// --- smoke test: get_last_changed and load_tasks ---

#[test]
fn exasol_contract_smoke_test() {
    if skip_unless_enabled() {
        return;
    }

    let config = AppConfig::from_env().unwrap_or_else(|_| AppConfig {
        exasol: nano_config(),
        poll_interval: std::time::Duration::from_secs(10),
    });
    let db = ExasolDb::new(config.exasol).expect("failed to build ExasolDb");
    setup_tables(&db);

    let last_changed = db.get_last_changed().expect("get_last_changed failed");
    let tasks = db.load_tasks().expect("load_tasks failed");

    eprintln!(
        "smoke OK: last_changed={last_changed:?}, tasks={}",
        tasks.len()
    );
    assert!(!tasks.is_empty(), "expected at least the contract_task row");
}

#[test]
fn execute_statement_accepts_select_result_sets() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");

    db.execute_statement("SELECT 1")
        .expect("execute_statement must accept SELECT result sets for scheduled tasks");
}

#[test]
fn execute_statement_accepts_dml_row_counts() {
    if skip_unless_enabled() {
        return;
    }

    let dsn = nano_dsn();
    let schema = contract_schema_name("CODEX_DML");
    let _guard = SchemaDropGuard::new(dsn.clone(), schema.clone());
    must_execute_update_direct(&dsn, &format!("CREATE SCHEMA {schema}"));
    must_execute_update_direct(
        &dsn,
        &format!("CREATE TABLE {schema}.DML_TARGET (\"ID\" DECIMAL(18,0))"),
    );

    let db = ExasolDb::new(config_for_schema(&schema)).expect("failed to build ExasolDb");

    db.execute_statement(&format!(
        "INSERT INTO {schema}.DML_TARGET (\"ID\") VALUES (1)"
    ))
    .expect("execute_statement must accept DML row-count results");

    let batches = db
        .query_batches(
            "count_dml_target_rows",
            format!("SELECT CAST(COUNT(*) AS VARCHAR(20)) AS CNT FROM {schema}.DML_TARGET"),
        )
        .expect("DML verification query should succeed");
    let count = batches
        .iter()
        .find_map(|batch| {
            batch
                .column_by_name("CNT")
                .and_then(|col| col.as_any().downcast_ref::<StringArray>())
                .map(|values| {
                    values
                        .value(0)
                        .parse::<i64>()
                        .expect("CNT should be numeric")
                })
        })
        .expect("COUNT query should return one row");

    assert_eq!(count, 1);
}

// --- write_history inserts a SUCCEEDED row ---

#[test]
fn write_history_inserts_succeeded_row_into_sched_history() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_tables(&db);

    let run_id = Uuid::new_v4();
    let now = Utc::now();
    let event = HistoryEvent {
        run_id,
        graph_run_id: None,
        task_id: "contract_task".to_string(),
        graph_phase: "MAIN".to_string(),
        scheduled_for: Some(now),
        started_at: now,
        finished_at: Some(now),
        status: "SUCCEEDED".to_string(),
        error_message: None,
    };

    db.write_history(&event).expect("write_history failed");

    let count = count_history_rows(&db, &run_id.to_string());
    assert_eq!(
        count, 1,
        "expected exactly one SCHED_HISTORY row for the run_id"
    );

    let status = read_history_status(&db, &run_id.to_string());
    assert_eq!(status.as_deref(), Some("SUCCEEDED"));
}

// --- write_history inserts a FAILED row with error message ---

#[test]
fn write_history_inserts_failed_row_with_error_message() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_tables(&db);

    let run_id = Uuid::new_v4();
    let now = Utc::now();
    let event = HistoryEvent {
        run_id,
        graph_run_id: None,
        task_id: "contract_task".to_string(),
        graph_phase: "MAIN".to_string(),
        scheduled_for: None,
        started_at: now,
        finished_at: Some(now),
        status: "FAILED".to_string(),
        error_message: Some("something broke".to_string()),
    };

    db.write_history(&event).expect("write_history failed");

    let status = read_history_status(&db, &run_id.to_string());
    assert_eq!(status.as_deref(), Some("FAILED"));
}

// --- write_history with graph_run_id ---

#[test]
fn write_history_stores_graph_run_id_when_set() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_tables(&db);

    let run_id = Uuid::new_v4();
    let graph_run_id = Uuid::new_v4();
    let now = Utc::now();
    let event = HistoryEvent {
        run_id,
        graph_run_id: Some(graph_run_id),
        task_id: "contract_task".to_string(),
        graph_phase: "MAIN".to_string(),
        scheduled_for: Some(now),
        started_at: now,
        finished_at: Some(now),
        status: "SUCCEEDED".to_string(),
        error_message: None,
    };

    db.write_history(&event).expect("write_history failed");

    let sql = format!(
        "SELECT GRAPH_RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{}'",
        run_id
    );
    let batches = db
        .query_batches("read_graph_run_id", sql)
        .unwrap_or_default();
    let mut found = false;
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("GRAPH_RUN_ID") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    assert_eq!(arr.value(0), graph_run_id.to_string());
                    found = true;
                }
            }
        }
    }
    assert!(found, "GRAPH_RUN_ID row not found in SCHED_HISTORY");
}

// --- write_history with special characters in error message ---

#[test]
fn write_history_handles_single_quotes_in_error_message() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_tables(&db);

    let run_id = Uuid::new_v4();
    let now = Utc::now();
    let event = HistoryEvent {
        run_id,
        graph_run_id: None,
        task_id: "contract_task".to_string(),
        graph_phase: "MAIN".to_string(),
        scheduled_for: None,
        started_at: now,
        finished_at: Some(now),
        status: "FAILED".to_string(),
        error_message: Some("can't connect: it's broken".to_string()),
    };

    db.write_history(&event)
        .expect("write_history with quotes should not fail");

    let count = count_history_rows(&db, &run_id.to_string());
    assert_eq!(count, 1);
}

// --- Stage 3 contract tests: DAG execution against a real Exasol instance ---

fn setup_dag_tasks(db: &ExasolDb) {
    setup_tables(db);
    let dsn = nano_dsn();
    for sql in [
        "INSERT INTO PUBLIC.SCHED_TASKS \
            (\"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"SQL_TEXT\", \"AFTER\", \"IS_FINAL\") \
            VALUES ('dag_child', TRUE, 'CRON 0 * * * * * TZ=UTC', 'SELECT 2', 'contract_task', FALSE)",
        "INSERT INTO PUBLIC.SCHED_TASKS \
            (\"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"SQL_TEXT\", \"AFTER\", \"IS_FINAL\") \
            VALUES ('dag_finalizer', TRUE, 'CRON 0 * * * * * TZ=UTC', 'SELECT 99', 'contract_task', TRUE)",
    ] {
        must_execute_update_direct(&dsn, sql);
    }
}

fn count_history_rows_for_graph(db: &ExasolDb, graph_run_id: &str) -> usize {
    let sql =
        format!("SELECT RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE GRAPH_RUN_ID = '{graph_run_id}'");
    let batches = db
        .query_batches("count_graph_rows", sql)
        .unwrap_or_default();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn read_history_phase(db: &ExasolDb, run_id: &str) -> Option<String> {
    let sql = format!("SELECT GRAPH_PHASE FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'");
    let batches = db
        .query_batches("read_graph_phase", sql)
        .unwrap_or_default();
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("GRAPH_PHASE") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    return Some(arr.value(0).to_string());
                }
            }
        }
    }
    None
}

fn read_history_graph_run_id(db: &ExasolDb, run_id: &str) -> Option<String> {
    let sql = format!("SELECT GRAPH_RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'");
    let batches = db
        .query_batches("read_graph_run_id_by_run", sql)
        .unwrap_or_default();
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("GRAPH_RUN_ID") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    return Some(arr.value(0).to_string());
                }
            }
        }
    }
    None
}

// Helper: run the scheduler for one tick, crossing a minute boundary.
// Returns the loaded Scheduler after the execution tick.
fn run_one_graph(db_config: ExasolDbConfig) -> exasol_scheduler::scheduler::TickResult {
    use exasol_scheduler::db::ExasolDb;

    let exa_db = Arc::new(ExasolDb::new(db_config).expect("ExasolDb::new failed"));
    let clock = Arc::new(SystemClock);
    let mut scheduler = Scheduler::with_local_timezone_and_poll_interval(
        exa_db,
        clock,
        LocalTimeZone::Named(chrono_tz::UTC),
        Duration::from_secs(300),
    );

    // First tick: loads snapshot without executing (we're mid-minute, not at :00)
    // We can't reliably cross a minute boundary in a unit test, so we call tick twice
    // with a tiny sleep. Contract tests are marked slow.
    scheduler.tick().expect("first tick failed");

    // Sleep until the next :00
    let now = chrono::Utc::now();
    let secs_to_next_minute = 60 - now.second() as u64;
    std::thread::sleep(Duration::from_secs(secs_to_next_minute + 1));

    scheduler.tick().expect("second tick failed")
}

#[test]
fn contract_dag_root_with_child_writes_two_history_rows() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_dag_tasks(&db);

    let result = run_one_graph(nano_config());
    assert_eq!(result.executed_roots, 1, "expected one root to execute");

    // contract_task (root) + dag_child (child) must each have a history row
    // We can't know graph_run_id ahead of time, so count all rows added after setup
    let sql = "SELECT COUNT(*) AS CNT FROM PUBLIC.SCHED_HISTORY WHERE \
               \"TASK_ID\" IN ('contract_task', 'dag_child')";
    let batches = db
        .query_batches("count_dag_rows", sql.to_string())
        .unwrap_or_default();
    let mut found = false;
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("CNT") {
                use arrow::array::Int64Array;
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    assert!(arr.value(0) >= 2, "expected at least 2 history rows");
                    found = true;
                }
            }
        }
    }
    assert!(found, "COUNT query did not return a result");
}

#[test]
fn contract_graph_run_id_matches_across_root_and_child() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_dag_tasks(&db);

    run_one_graph(nano_config());

    // Find the most recent contract_task history row
    let root_sql = "SELECT RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE \
                    \"TASK_ID\" = 'contract_task' ORDER BY \"STARTED_AT\" DESC LIMIT 1";
    let batches = db
        .query_batches("find_root_run_id", root_sql.to_string())
        .unwrap_or_default();
    let mut root_run_id = String::new();
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("RUN_ID") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    root_run_id = arr.value(0).to_string();
                }
            }
        }
    }
    assert!(!root_run_id.is_empty(), "no root history row found");

    let root_graph_id = read_history_graph_run_id(&db, &root_run_id);
    assert!(root_graph_id.is_some(), "root must have GRAPH_RUN_ID");
    let gid = root_graph_id.unwrap();

    let child_count = count_history_rows_for_graph(&db, &gid);
    assert!(
        child_count >= 2,
        "both root and child must share the graph_run_id; got {child_count}"
    );
}

// --- ensure_tables contract tests ---

#[test]
fn ensure_tables_creates_tables_when_they_do_not_exist() {
    if skip_unless_enabled() {
        return;
    }

    let dsn = nano_dsn();
    let schema = contract_schema_name("CODEX_FRESH");
    let _guard = SchemaDropGuard::new(dsn.clone(), schema.clone());

    let db = ExasolDb::new(config_for_schema(&schema)).expect("failed to build ExasolDb");

    let result = db.ensure_tables().expect("ensure_tables should succeed");
    assert!(
        result.tasks_table_created,
        "tasks table should have been created"
    );
    assert!(
        result.history_table_created,
        "history table should have been created"
    );
    assert!(
        table_exists(&db, &schema, "SCHED_TASKS"),
        "SCHED_TASKS should exist after ensure_tables"
    );
    assert!(
        table_exists(&db, &schema, "SCHED_HISTORY"),
        "SCHED_HISTORY should exist after ensure_tables"
    );
}

#[test]
fn ensure_tables_is_idempotent_when_tables_already_exist() {
    if skip_unless_enabled() {
        return;
    }

    let dsn = nano_dsn();
    let schema = contract_schema_name("CODEX_EXISTING");
    let _guard = SchemaDropGuard::new(dsn.clone(), schema.clone());
    for sql in [
        format!("CREATE SCHEMA {schema}"),
        format!(
            "CREATE TABLE {schema}.SCHED_TASKS (\
                \"TASK_ID\" VARCHAR(128) NOT NULL, \
                \"ENABLED\" BOOLEAN DEFAULT TRUE, \
                \"SCHEDULE\" VARCHAR(512) NOT NULL, \
                \"SQL_TEXT\" VARCHAR(2000000) NOT NULL, \
                \"AFTER\" VARCHAR(128), \
                \"IS_FINAL\" BOOLEAN DEFAULT FALSE, \
                \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE, \
                \"COMMENT\" VARCHAR(2000), \
                PRIMARY KEY (\"TASK_ID\"))"
        ),
        format!(
            "CREATE TABLE {schema}.SCHED_HISTORY (\
                \"RUN_ID\" VARCHAR(36) NOT NULL, \
                \"GRAPH_RUN_ID\" VARCHAR(36), \
                \"TASK_ID\" VARCHAR(128) NOT NULL, \
                \"GRAPH_PHASE\" VARCHAR(16) NOT NULL, \
                \"SCHEDULED_FOR\" TIMESTAMP, \
                \"STARTED_AT\" TIMESTAMP NOT NULL, \
                \"FINISHED_AT\" TIMESTAMP, \
                \"STATUS\" VARCHAR(16) NOT NULL, \
                \"ERROR_MESSAGE\" VARCHAR(2000000), \
                PRIMARY KEY (\"RUN_ID\"))"
        ),
    ] {
        must_execute_update_direct(&dsn, &sql);
    }

    let db = ExasolDb::new(config_for_schema(&schema)).expect("failed to build ExasolDb");

    let result = db
        .ensure_tables()
        .expect("ensure_tables should succeed on existing tables");
    assert!(
        !result.tasks_table_created,
        "tasks table already exists, should not be re-created"
    );
    assert!(
        !result.history_table_created,
        "history table already exists, should not be re-created"
    );
    assert!(
        !result.parallel_children_added,
        "existing current-schema task table should not be migrated"
    );
    assert!(
        !result.sql_text_column_renamed,
        "existing current-schema task table should not rename SQL_TEXT"
    );
}

#[test]
fn ensure_tables_renames_legacy_statement_column_to_sql_text() {
    if skip_unless_enabled() {
        return;
    }

    let dsn = nano_dsn();
    let schema = contract_schema_name("CODEX_LEGACY");
    let _guard = SchemaDropGuard::new(dsn.clone(), schema.clone());
    for sql in [
        format!("CREATE SCHEMA {schema}"),
        format!(
            "CREATE TABLE {schema}.SCHED_TASKS (\
                \"TASK_ID\" VARCHAR(128) NOT NULL, \
                \"ENABLED\" BOOLEAN DEFAULT TRUE, \
                \"SCHEDULE\" VARCHAR(512) NOT NULL, \
                \"STATEMENT\" VARCHAR(2000000) NOT NULL, \
                \"AFTER\" VARCHAR(128), \
                \"IS_FINAL\" BOOLEAN DEFAULT FALSE, \
                \"PARALLEL_CHILDREN\" BOOLEAN DEFAULT TRUE, \
                \"COMMENT\" VARCHAR(2000), \
                PRIMARY KEY (\"TASK_ID\"))"
        ),
        format!(
            "CREATE TABLE {schema}.SCHED_HISTORY (\
                \"RUN_ID\" VARCHAR(36) NOT NULL, \
                \"GRAPH_RUN_ID\" VARCHAR(36), \
                \"TASK_ID\" VARCHAR(128) NOT NULL, \
                \"GRAPH_PHASE\" VARCHAR(16) NOT NULL, \
                \"SCHEDULED_FOR\" TIMESTAMP, \
                \"STARTED_AT\" TIMESTAMP NOT NULL, \
                \"FINISHED_AT\" TIMESTAMP, \
                \"STATUS\" VARCHAR(16) NOT NULL, \
                \"ERROR_MESSAGE\" VARCHAR(2000000), \
                PRIMARY KEY (\"RUN_ID\"))"
        ),
    ] {
        must_execute_update_direct(&dsn, &sql);
    }

    let db = ExasolDb::new(config_for_schema(&schema)).expect("failed to build ExasolDb");

    let result = db
        .ensure_tables()
        .expect("ensure_tables should migrate legacy task table");
    assert!(
        result.sql_text_column_renamed,
        "legacy STATEMENT column should be renamed"
    );
    assert!(column_exists(&db, &schema, "SCHED_TASKS", "SQL_TEXT"));
    assert!(!column_exists(&db, &schema, "SCHED_TASKS", "STATEMENT"));
}

#[test]
fn contract_finalizer_phase_is_recorded_as_final() {
    if skip_unless_enabled() {
        return;
    }

    let db = ExasolDb::new(nano_config()).expect("failed to build ExasolDb");
    setup_dag_tasks(&db);

    run_one_graph(nano_config());

    // Find the most recent dag_finalizer history row
    let sql = "SELECT RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE \
               \"TASK_ID\" = 'dag_finalizer' ORDER BY \"STARTED_AT\" DESC LIMIT 1";
    let batches = db
        .query_batches("find_finalizer_run_id", sql.to_string())
        .unwrap_or_default();
    let mut fin_run_id = String::new();
    for batch in &batches {
        if batch.num_rows() > 0 {
            if let Some(col) = batch.column_by_name("RUN_ID") {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    fin_run_id = arr.value(0).to_string();
                }
            }
        }
    }
    assert!(!fin_run_id.is_empty(), "no finalizer history row found");

    let phase = read_history_phase(&db, &fin_run_id);
    assert_eq!(
        phase.as_deref(),
        Some("FINAL"),
        "finalizer phase must be FINAL"
    );
}
