use arrow::array::StringArray;
use chrono::Utc;
use exasol_scheduler::config::AppConfig;
use exasol_scheduler::db::{ExasolDb, ExasolDbConfig, SchedulerDb};
use exasol_scheduler::model::HistoryEvent;
use uuid::Uuid;

fn nano_dsn() -> String {
    std::env::var("EXA_DSN")
        .unwrap_or_else(|_| "exasol://sys:exasol@localhost:8563?tls=1&validateservercertificate=0".to_string())
}

fn nano_config() -> ExasolDbConfig {
    ExasolDbConfig {
        dsn: nano_dsn(),
        schema: "PUBLIC".to_string(),
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

fn setup_tables(db: &ExasolDb) {
    for sql in [
        "CREATE SCHEMA IF NOT EXISTS PUBLIC",
        "DROP TABLE IF EXISTS PUBLIC.SCHED_HISTORY",
        "DROP TABLE IF EXISTS PUBLIC.SCHED_TASKS",
        "CREATE TABLE PUBLIC.SCHED_TASKS (\
            \"TASK_ID\" VARCHAR(128) NOT NULL, \
            \"ENABLED\" BOOLEAN DEFAULT TRUE, \
            \"SCHEDULE\" VARCHAR(512) NOT NULL, \
            \"STATEMENT\" VARCHAR(2000000) NOT NULL, \
            \"AFTER\" VARCHAR(128), \
            \"IS_FINAL\" BOOLEAN DEFAULT FALSE, \
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
            (\"TASK_ID\", \"ENABLED\", \"SCHEDULE\", \"STATEMENT\") \
            VALUES ('contract_task', TRUE, 'CRON 0 * * * * * TZ=UTC', 'SELECT 1')",
    ] {
        db.execute_statement(sql)
            .unwrap_or_else(|e| eprintln!("setup_tables warning: {e}"));
    }
}

fn count_history_rows(db: &ExasolDb, run_id: &str) -> usize {
    let sql = format!(
        "SELECT RUN_ID FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'"
    );
    let batches = db.query_batches("count_history", sql).unwrap_or_default();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn read_history_status(db: &ExasolDb, run_id: &str) -> Option<String> {
    let sql = format!(
        "SELECT STATUS FROM PUBLIC.SCHED_HISTORY WHERE RUN_ID = '{run_id}'"
    );
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
    if skip_unless_enabled() { return; }

    let config = AppConfig::from_env()
        .unwrap_or_else(|_| AppConfig {
            exasol: nano_config(),
            poll_interval: std::time::Duration::from_secs(10),
        });
    let db = ExasolDb::new(config.exasol).expect("failed to build ExasolDb");
    setup_tables(&db);

    let last_changed = db.get_last_changed().expect("get_last_changed failed");
    let tasks = db.load_tasks().expect("load_tasks failed");

    eprintln!("smoke OK: last_changed={last_changed:?}, tasks={}", tasks.len());
    assert!(!tasks.is_empty(), "expected at least the contract_task row");
}

// --- write_history inserts a SUCCEEDED row ---

#[test]
fn write_history_inserts_succeeded_row_into_sched_history() {
    if skip_unless_enabled() { return; }

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
    assert_eq!(count, 1, "expected exactly one SCHED_HISTORY row for the run_id");

    let status = read_history_status(&db, &run_id.to_string());
    assert_eq!(status.as_deref(), Some("SUCCEEDED"));
}

// --- write_history inserts a FAILED row with error message ---

#[test]
fn write_history_inserts_failed_row_with_error_message() {
    if skip_unless_enabled() { return; }

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
    if skip_unless_enabled() { return; }

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
    let batches = db.query_batches("read_graph_run_id", sql).unwrap_or_default();
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
    if skip_unless_enabled() { return; }

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

    db.write_history(&event).expect("write_history with quotes should not fail");

    let count = count_history_rows(&db, &run_id.to_string());
    assert_eq!(count, 1);
}
