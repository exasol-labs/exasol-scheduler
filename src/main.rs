use std::sync::Arc;

use exasol_scheduler::config::{AppConfig, connection_target_from_dsn};
use exasol_scheduler::db::ExasolDb;
use exasol_scheduler::scheduler::{Scheduler, SchedulerError};
use exasol_scheduler::time::{Clock, SystemClock};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(std::env::args().nth(1))
}

fn is_help_flag(arg: &str) -> bool {
    arg == "--help" || arg == "-h"
}

fn run(cli_dsn: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    if cli_dsn.as_deref().is_some_and(is_help_flag) {
        println!(
            "Usage: exasol_scheduler [DSN]\n\n\
             DSN  exasol://user:password@host:port?tls=1&validateservercertificate=0\n\n\
             All options can also be set via environment variables.\n\
             See docs/configuration.md for the full reference.\n\n\
             Environment variables:\n\
             EXA_HOST, EXA_PORT, EXA_USER, EXA_PASSWORD\n\
             EXA_TLS, EXA_VALIDATE_SERVER_CERT\n\
             EXA_SCHEMA, EXA_TASKS_TABLE, EXA_HISTORY_TABLE\n\
             POLL_INTERVAL_SECS, RUST_LOG"
        );
        std::process::exit(0);
    }
    init_tracing();

    // Optional positional CLI argument: exarrow-rs DSN (exasol://...).
    // This allows running without EXA_* env vars for credentials/host.
    let config = AppConfig::from_env_and_optional_dsn(cli_dsn)?;
    if let Some(connection_target) = connection_target_from_dsn(&config.exasol.dsn) {
        tracing::info!(
            host = connection_target.host.as_str(),
            port = connection_target.port,
            schema = config.exasol.schema.as_str(),
            tasks_table = config.exasol.tasks_table.as_str(),
            history_table = config.exasol.history_table.as_str(),
            poll_interval_secs = config.poll_interval.as_secs(),
            "starting exasol scheduler"
        );
    } else {
        tracing::info!(
            host = "unparsed",
            schema = config.exasol.schema.as_str(),
            tasks_table = config.exasol.tasks_table.as_str(),
            history_table = config.exasol.history_table.as_str(),
            poll_interval_secs = config.poll_interval.as_secs(),
            "starting exasol scheduler"
        );
    }

    let db = ExasolDb::new(config.exasol)?;

    let init = db.ensure_tables()?;
    if init.tasks_table_created
        || init.history_table_created
        || init.sql_text_column_renamed
        || init.parallel_children_added
        || init.schedule_nullable_altered
    {
        tracing::info!(
            tasks_table_created = init.tasks_table_created,
            history_table_created = init.history_table_created,
            sql_text_column_renamed = init.sql_text_column_renamed,
            parallel_children_added = init.parallel_children_added,
            schedule_nullable_altered = init.schedule_nullable_altered,
            "initialized scheduler database objects"
        );
    }

    let db = Arc::new(db);
    let clock = Arc::new(SystemClock);
    let mut scheduler = Scheduler::with_poll_interval(db, clock.clone(), config.poll_interval);

    run_scheduler_loop(&mut scheduler, clock)
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })
}

fn run_scheduler_loop(
    scheduler: &mut Scheduler,
    clock: Arc<dyn Clock>,
) -> Result<(), SchedulerError> {
    loop {
        let delay = run_scheduler_once(scheduler, clock.clone())?;
        std::thread::sleep(delay);
    }
}

fn run_scheduler_once(
    scheduler: &mut Scheduler,
    clock: Arc<dyn Clock>,
) -> Result<std::time::Duration, SchedulerError> {
    let tick = scheduler.tick()?;

    if let Some(reload) = tick.reload {
        tracing::info!(
            added = reload.added,
            removed = reload.removed,
            changed = reload.changed,
            "task snapshot reloaded"
        );
    }

    if tick.executed_roots > 0 {
        tracing::info!(
            executed_roots = tick.executed_roots,
            "executed due root tasks"
        );
    }

    Ok(scheduler.next_wake_delay(clock.now()))
}

fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use exasol_scheduler::db::{DbError, SchedulerDb};
    use exasol_scheduler::model::{HistoryEvent, TaskRow};
    use exasol_scheduler::time::FakeClock;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
    }

    fn root_task(task_id: &str, schedule: &str, statement: &str) -> TaskRow {
        TaskRow {
            task_id: task_id.to_string(),
            enabled: true,
            schedule: schedule.to_string(),
            statement: statement.to_string(),
            after: None,
            is_final: false,
            comment: None,
            parallel_children: true,
        }
    }

    #[derive(Debug)]
    struct InMemoryDb {
        last_changed: DateTime<Utc>,
        tasks: Vec<TaskRow>,
        executed_sql: Mutex<Vec<String>>,
    }

    impl InMemoryDb {
        fn new(last_changed: DateTime<Utc>, tasks: Vec<TaskRow>) -> Self {
            Self {
                last_changed,
                tasks,
                executed_sql: Mutex::new(Vec::new()),
            }
        }

        fn executed_sql(&self) -> Vec<String> {
            self.executed_sql
                .lock()
                .expect("executed_sql mutex poisoned")
                .clone()
        }
    }

    impl SchedulerDb for InMemoryDb {
        fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
            Ok(self.last_changed)
        }

        fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError> {
            Ok(self.tasks.clone())
        }

        fn execute_statement(&self, sql: &str) -> Result<(), DbError> {
            self.executed_sql
                .lock()
                .expect("executed_sql mutex poisoned")
                .push(sql.to_string());
            Ok(())
        }

        fn write_history(&self, _event: &HistoryEvent) -> Result<(), DbError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct AlwaysFailDb;

    impl SchedulerDb for AlwaysFailDb {
        fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
            Err(DbError::Other("forced failure".to_string()))
        }

        fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError> {
            Ok(Vec::new())
        }

        fn execute_statement(&self, _sql: &str) -> Result<(), DbError> {
            Ok(())
        }

        fn write_history(&self, _event: &HistoryEvent) -> Result<(), DbError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailAfterOneTickDb {
        calls: AtomicUsize,
    }

    impl SchedulerDb for FailAfterOneTickDb {
        fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            if index == 0 {
                Ok(dt(2026, 2, 1, 12, 0, 0))
            } else {
                Err(DbError::Other("second tick failure".to_string()))
            }
        }

        fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError> {
            Ok(Vec::new())
        }

        fn execute_statement(&self, _sql: &str) -> Result<(), DbError> {
            Ok(())
        }

        fn write_history(&self, _event: &HistoryEvent) -> Result<(), DbError> {
            Ok(())
        }
    }

    fn dummy_event() -> HistoryEvent {
        HistoryEvent {
            run_id: Uuid::nil(),
            graph_run_id: None,
            task_id: "task".to_string(),
            graph_phase: "ROOT".to_string(),
            scheduled_for: None,
            started_at: dt(2026, 2, 1, 12, 0, 0),
            finished_at: None,
            status: "SUCCEEDED".to_string(),
            error_message: None,
        }
    }

    #[test]
    fn run_scheduler_once_computes_delay_and_executes_due_roots() {
        init_tracing();

        let now = dt(2026, 2, 1, 12, 0, 59);
        let clock = Arc::new(FakeClock::new(now));
        let db = Arc::new(InMemoryDb::new(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT 1")],
        ));
        let mut scheduler =
            Scheduler::with_poll_interval(db.clone(), clock.clone(), Duration::from_secs(300));

        let first_delay =
            run_scheduler_once(&mut scheduler, clock.clone()).expect("first tick should succeed");
        assert_eq!(first_delay, Duration::from_secs(1));
        assert!(db.executed_sql().is_empty());

        clock.advance(first_delay);
        let second_delay =
            run_scheduler_once(&mut scheduler, clock.clone()).expect("second tick should succeed");
        assert_eq!(second_delay, Duration::from_secs(60));
        assert_eq!(db.executed_sql(), vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn run_scheduler_loop_returns_error_when_tick_fails() {
        init_tracing();

        let now = dt(2026, 2, 1, 12, 0, 0);
        let clock = Arc::new(FakeClock::new(now));
        let db = Arc::new(AlwaysFailDb);
        let mut scheduler =
            Scheduler::with_poll_interval(db, clock.clone(), Duration::from_secs(1));

        let err = run_scheduler_loop(&mut scheduler, clock).unwrap_err();
        assert!(err.to_string().contains("forced failure"));
    }

    #[test]
    fn run_scheduler_loop_sleeps_and_then_surfaces_second_tick_error() {
        init_tracing();

        let now = dt(2026, 2, 1, 12, 0, 0);
        let clock = Arc::new(FakeClock::new(now));
        let db = Arc::new(FailAfterOneTickDb {
            calls: AtomicUsize::new(0),
        });
        let mut scheduler = Scheduler::with_poll_interval(db, clock, Duration::ZERO);

        let err = run_scheduler_loop(&mut scheduler, Arc::new(FakeClock::new(now))).unwrap_err();
        assert!(err.to_string().contains("second tick failure"));
    }

    #[test]
    fn run_returns_error_when_exasol_connection_cannot_be_opened() {
        init_tracing();
        let err = run(Some("exasol://sys:pw@localhost:8563?tls=0".to_string())).unwrap_err();
        assert!(
            err.to_string()
                .contains("connection failed during execute_statement")
        );
    }

    #[test]
    fn is_help_flag_recognises_help_and_h() {
        assert!(is_help_flag("--help"));
        assert!(is_help_flag("-h"));
        assert!(!is_help_flag("--version"));
        assert!(!is_help_flag("exasol://u:p@h:8563"));
        assert!(!is_help_flag(""));
    }

    #[test]
    fn test_db_helpers_cover_noop_trait_methods() {
        let event = dummy_event();

        let mem = InMemoryDb::new(dt(2026, 2, 1, 12, 0, 0), Vec::new());
        assert!(mem.write_history(&event).is_ok());

        let fail = AlwaysFailDb;
        assert!(fail.load_tasks().unwrap().is_empty());
        assert!(fail.execute_statement("SELECT 1").is_ok());
        assert!(fail.write_history(&event).is_ok());
    }
}
