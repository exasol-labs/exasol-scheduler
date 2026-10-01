mod common;

use chrono::{DateTime, TimeZone, Utc};
use common::{DbVersion, ProgrammableDb, root_task};
use exasol_scheduler::model::{HistoryEvent, TaskRow};
use exasol_scheduler::schedule::LocalTimeZone;
use exasol_scheduler::scheduler::Scheduler;
use exasol_scheduler::time::FakeClock;
use pretty_assertions::assert_eq;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
}

fn version(last_changed: DateTime<Utc>, tasks: Vec<TaskRow>) -> DbVersion {
    DbVersion {
        last_changed,
        tasks,
    }
}

fn make_scheduler(db: Arc<ProgrammableDb>, clock: Arc<FakeClock>) -> Scheduler {
    Scheduler::with_local_timezone_and_poll_interval(
        db,
        clock,
        LocalTimeZone::Named(chrono_tz::UTC),
        Duration::from_secs(300),
    )
}

// --- history written on successful execution ---

#[test]
fn history_is_written_after_successful_root_execution() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SELECT 1")],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());

    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    let events = db.history_events();
    assert_eq!(events.len(), 1);

    let e = &events[0];
    assert_eq!(e.task_id, "root_a");
    assert_eq!(e.graph_phase, "MAIN");
    assert_eq!(e.status, "SUCCEEDED");
    assert!(e.error_message.is_none());
    assert_eq!(e.scheduled_for, Some(dt(2026, 2, 1, 12, 1, 0)));
    assert!(e.finished_at.is_some());
    assert!(e.started_at <= e.finished_at.unwrap());
    assert!(e.graph_run_id.is_some());
}

// --- history written on failed execution, scheduler keeps running ---

#[test]
fn history_is_written_after_failed_root_execution_and_tick_succeeds() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_fail", "CRON 0 * * * * * TZ=UTC", "BAD SQL")],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    let _ = scheduler.tick().unwrap();

    db.set_failure_for_task_id("root_fail", "intentional failure");
    clock.advance(Duration::from_secs(1));
    let tick = scheduler.tick().unwrap();
    assert_eq!(tick.executed_roots, 1);
    assert_eq!(tick.failed_roots, 1);

    let events = db.history_events();
    assert_eq!(events.len(), 1, "history must be written even on failure");
    let e = &events[0];
    assert_eq!(e.task_id, "root_fail");
    assert_eq!(e.status, "FAILED");
    assert!(
        e.error_message
            .as_deref()
            .unwrap()
            .contains("intentional failure")
    );
    assert_eq!(
        e.db_calls_after_failure(db.write_history_calls()),
        (),
        "sanity: write_history was called"
    );
}

// helper trait extension (local to this test file)
trait SanityCheck {
    fn db_calls_after_failure(&self, calls: usize) -> ();
}
impl SanityCheck for HistoryEvent {
    fn db_calls_after_failure(&self, calls: usize) -> () {
        assert_eq!(calls, 1, "write_history must have been called exactly once");
    }
}

// --- write_history failure does not crash the scheduler ---

#[test]
fn write_history_failure_does_not_prevent_execution_or_crash_scheduler() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_ok", "CRON 0 * * * * * TZ=UTC", "SELECT 1")],
        )],
        clock.clone(),
    ));
    db.set_write_history_error("history table unavailable");
    let mut scheduler = make_scheduler(db.clone(), clock.clone());

    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));

    // tick must succeed despite write_history returning an error
    let result = scheduler.tick().unwrap();
    assert_eq!(result.executed_roots, 1);
    assert_eq!(
        db.execute_calls(),
        1,
        "statement must still have been executed"
    );
    assert_eq!(db.write_history_calls(), 1, "write_history was attempted");
}

// --- run_id is unique per execution ---

#[test]
fn each_execution_gets_a_unique_run_id() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SELECT 1")],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());

    let _ = scheduler.tick().unwrap();

    // Fire twice — minute 1 and minute 2
    clock.advance(Duration::from_secs(1));
    scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(60));
    scheduler.tick().unwrap();

    let events = db.history_events();
    assert_eq!(events.len(), 2);
    let ids: HashSet<_> = events.iter().map(|e| e.run_id).collect();
    assert_eq!(ids.len(), 2, "each execution must have a distinct run_id");
}

// --- started_at and finished_at are recorded correctly ---

#[test]
fn history_records_started_at_and_finished_at_from_clock() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SELECT 1")],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));
    scheduler.tick().unwrap();

    let e = &db.history_events()[0];
    // started_at and finished_at are both captured from the FakeClock which
    // does not advance during synchronous execution, so they should be equal.
    assert_eq!(e.started_at, e.finished_at.unwrap());
    // started_at must be at or after the tick time
    assert!(e.started_at >= dt(2026, 2, 1, 12, 1, 0));
}

// --- multiple roots each get their own history event ---

#[test]
fn multiple_due_roots_each_produce_a_history_event() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SELECT A"),
                root_task("root_b", "CRON 0 * * * * * TZ=UTC", "SELECT B"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 2);
    let events = db.history_events();
    assert_eq!(events.len(), 2);
    let task_ids: HashSet<_> = events.iter().map(|e| e.task_id.as_str()).collect();
    assert!(task_ids.contains("root_a"));
    assert!(task_ids.contains("root_b"));
    for e in &events {
        assert_eq!(e.status, "SUCCEEDED");
        assert_eq!(e.graph_phase, "MAIN");
    }
}

// --- a failing root does not stop other due roots (BUG-36) ---

#[test]
fn first_root_failure_does_not_stop_remaining_roots() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                // root_a sorts before root_b and will fail first
                root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SELECT A"),
                root_task("root_b", "CRON 0 * * * * * TZ=UTC", "SELECT B"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    let _ = scheduler.tick().unwrap();

    db.set_failure_for_statement("SELECT A", "root_a failed");
    clock.advance(Duration::from_secs(1));
    let tick = scheduler.tick().unwrap();
    assert_eq!(tick.executed_roots, 2);
    assert_eq!(tick.failed_roots, 1);

    let events = db.history_events();
    assert_eq!(events.len(), 2);
    let status_of = |id: &str| {
        events
            .iter()
            .find(|e| e.task_id == id)
            .map(|e| e.status.clone())
            .unwrap()
    };
    assert_eq!(status_of("root_a"), "FAILED");
    assert_eq!(status_of("root_b"), "SUCCEEDED");

    // The failed root stays scheduled and runs again on its next occurrence.
    db.clear_failures();
    clock.advance(Duration::from_secs(60));
    let next = scheduler.tick().unwrap();
    assert_eq!(next.executed_roots, 2);
    assert_eq!(next.failed_roots, 0);
}
