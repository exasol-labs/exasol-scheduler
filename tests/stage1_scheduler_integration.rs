mod common;

use chrono::{DateTime, TimeZone, Utc};
use common::{DbVersion, ProgrammableDb, child_task, disabled_task, finalizer_task, root_task};
use exasol_scheduler::db::{DbError, SchedulerDb};
use exasol_scheduler::model::{HistoryEvent, TaskRow};
use exasol_scheduler::schedule::LocalTimeZone;
use exasol_scheduler::scheduler::{DEFAULT_POLL_INTERVAL, ReloadStats, Scheduler};
use exasol_scheduler::time::FakeClock;
use pretty_assertions::assert_eq;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
}

fn version(last_changed: DateTime<Utc>, tasks: Vec<exasol_scheduler::model::TaskRow>) -> DbVersion {
    DbVersion {
        last_changed,
        tasks,
    }
}

#[derive(Debug)]
struct FailAfterFirstTickDb {
    first_last_changed: DateTime<Utc>,
    get_last_changed_calls: AtomicUsize,
}

impl FailAfterFirstTickDb {
    fn new(first_last_changed: DateTime<Utc>) -> Self {
        Self {
            first_last_changed,
            get_last_changed_calls: AtomicUsize::new(0),
        }
    }
}

impl SchedulerDb for FailAfterFirstTickDb {
    fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
        let call_index = self.get_last_changed_calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            Ok(self.first_last_changed)
        } else {
            Err(DbError::Other("forced run_forever stop".to_string()))
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

#[test]
fn incremental_diff_updates_only_affected_roots_and_stale_heap_entries_are_ignored() {
    // Version 1 has roots A, B, D. Version 2 adds C, removes B, changes A schedule, and changes
    // only D statement. The scheduler should update only affected trigger state.
    let now = dt(2026, 2, 1, 12, 0, 0);
    let clock = Arc::new(FakeClock::new(now));

    let v1 = version(
        dt(2026, 2, 1, 11, 59, 0),
        vec![
            root_task("root_a", "CRON 5 * * * * * TZ=UTC", "SQL A1"),
            root_task("root_b", "CRON 10 * * * * * TZ=UTC", "SQL B"),
            root_task("root_d", "CRON 25 * * * * * TZ=UTC", "SQL D1"),
        ],
    );
    let v2 = version(
        dt(2026, 2, 1, 11, 59, 30),
        vec![
            root_task("root_a", "CRON 20 * * * * * TZ=UTC", "SQL A1"),
            root_task("root_c", "CRON 15 * * * * * TZ=UTC", "SQL C"),
            root_task("root_d", "CRON 25 * * * * * TZ=UTC", "SQL D2"),
        ],
    );

    let db = Arc::new(ProgrammableDb::new(vec![v1, v2], clock.clone()));
    let mut scheduler = Scheduler::with_local_timezone_and_poll_interval(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
        Duration::from_secs(60),
    );

    let first = scheduler.tick().unwrap();
    assert_eq!(
        first.reload,
        Some(ReloadStats {
            added: 3,
            removed: 0,
            changed: 0
        })
    );
    assert_eq!(scheduler.root_count(), 3);

    let before_a = scheduler.root_debug_state("root_a").unwrap();
    let before_d = scheduler.root_debug_state("root_d").unwrap();

    db.set_version(1);
    let second = scheduler.tick().unwrap();
    assert_eq!(
        second.reload,
        Some(ReloadStats {
            added: 1,
            removed: 1,
            changed: 2
        })
    );
    assert_eq!(scheduler.root_count(), 3);

    let after_a = scheduler.root_debug_state("root_a").unwrap();
    let after_d = scheduler.root_debug_state("root_d").unwrap();

    assert_eq!(after_a.generation, before_a.generation + 1);
    assert_eq!(after_a.next_due, dt(2026, 2, 1, 12, 0, 20));

    assert_eq!(after_d.generation, before_d.generation);
    assert_eq!(after_d.next_due, before_d.next_due);

    assert_eq!(
        scheduler.next_wake_delay(now),
        Duration::from_secs(15),
        "new root_c at second 15 should be earliest wakeup"
    );

    // The old root_a due (second 5) and removed root_b due (second 10) stay in heap as stale
    // entries and must be ignored when they become due.
    clock.set_now(dt(2026, 2, 1, 12, 0, 5));
    let third = scheduler.tick().unwrap();
    assert_eq!(third.executed_roots, 0);

    clock.set_now(dt(2026, 2, 1, 12, 0, 10));
    let fourth = scheduler.tick().unwrap();
    assert_eq!(fourth.executed_roots, 0);

    clock.set_now(dt(2026, 2, 1, 12, 0, 15));
    let fifth = scheduler.tick().unwrap();
    assert_eq!(fifth.executed_roots, 1);
    assert_eq!(db.executions()[0].statement, "SQL C");
}

#[test]
fn executes_due_root_and_reschedules_next_occurrence() {
    // A root that becomes due is executed once and then scheduled for the next period.
    let now = dt(2026, 2, 1, 12, 0, 30);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SQL A")],
        )],
        clock.clone(),
    ));
    let mut scheduler = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );

    let first = scheduler.tick().unwrap();
    assert_eq!(first.executed_roots, 0);

    clock.advance(Duration::from_secs(30));
    let second = scheduler.tick().unwrap();
    assert_eq!(second.executed_roots, 1);
    assert_eq!(db.executions().len(), 1);
    assert_eq!(db.executions()[0].at, dt(2026, 2, 1, 12, 1, 0));

    let root = scheduler.root_debug_state("root_a").unwrap();
    assert_eq!(root.generation, 2);
    assert_eq!(root.next_due, dt(2026, 2, 1, 12, 2, 0));
}

#[test]
fn does_not_execute_disabled_child_finalizer_or_invalid_root_tasks() {
    // Stage-1 executes only valid roots. Disabled/child/finalizer/invalid schedules are inert.
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("good_root", "CRON 0 * * * * * TZ=UTC", "SQL GOOD"),
                disabled_task("disabled", "CRON 0 * * * * * TZ=UTC", "SQL DISABLED"),
                child_task("child", "good_root", "CRON 0 * * * * * TZ=UTC", "SQL CHILD"),
                finalizer_task(
                    "finalizer",
                    "good_root",
                    "CRON 0 * * * * * TZ=UTC",
                    "SQL FINALIZER",
                ),
                root_task("invalid_tz", "CRON 0 * * * * * TZ=Nope/Zone", "SQL BAD TZ"),
                root_task("invalid_cron", "CRON 0 * * * *", "SQL BAD CRON"),
            ],
        )],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );

    let first = scheduler.tick().unwrap();
    assert_eq!(first.executed_roots, 0);
    assert_eq!(scheduler.root_count(), 1);

    clock.advance(Duration::from_secs(1));
    let second = scheduler.tick().unwrap();
    assert_eq!(second.executed_roots, 1);

    let executions = db.executions();
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].statement, "SQL GOOD");
}

#[test]
fn executes_multiple_due_roots_in_deterministic_task_id_order() {
    // Equal due-times should be deterministic so tests and operations are reproducible.
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root_b", "CRON 0 * * * * * TZ=UTC", "SQL B"),
                root_task("root_a", "CRON 0 * * * * * TZ=UTC", "SQL A"),
            ],
        )],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );

    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));

    let result = scheduler.tick().unwrap();
    assert_eq!(result.executed_roots, 2);

    let executed_sql: Vec<String> = db
        .executions()
        .into_iter()
        .map(|record| record.statement)
        .collect();
    assert_eq!(executed_sql, vec!["SQL A".to_string(), "SQL B".to_string()]);
}

#[test]
fn execution_failure_is_returned_and_root_stays_rescheduled() {
    // A failing execute_statement returns error, but the trigger is already advanced so the
    // scheduler can continue on later ticks.
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task(
                "root_fail",
                "CRON 0 * * * * * TZ=UTC",
                "SQL FAIL",
            )],
        )],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );
    let _ = scheduler.tick().unwrap();

    db.set_failure_for_task_id("root_fail", "forced failure");
    clock.advance(Duration::from_secs(1));

    let err = scheduler.tick().unwrap_err();
    assert!(err.to_string().contains("forced failure"));
    assert_eq!(db.execute_calls(), 1);

    let failed_root_state = scheduler.root_debug_state("root_fail").unwrap();
    assert_eq!(failed_root_state.generation, 2);
    assert_eq!(failed_root_state.next_due, dt(2026, 2, 1, 12, 2, 0));

    db.clear_failures();
    clock.advance(Duration::from_secs(60));
    let recovery = scheduler.tick().unwrap();
    assert_eq!(recovery.executed_roots, 1);
    assert_eq!(db.execute_calls(), 2);
}

#[test]
fn restart_does_not_backfill_missed_occurrences() {
    // Fresh scheduler state must schedule from "now" and ignore older missed fire times.
    let boot_time = dt(2026, 2, 1, 12, 0, 30);
    let clock = Arc::new(FakeClock::new(boot_time));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![root_task("root", "CRON 0 * * * * * TZ=UTC", "SQL ROOT")],
        )],
        clock.clone(),
    ));

    // Scheduler process starts, then is "down" across multiple minute boundaries.
    let mut first_instance = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );
    let first_tick = first_instance.tick().unwrap();
    assert_eq!(first_tick.executed_roots, 0);
    drop(first_instance);

    clock.set_now(dt(2026, 2, 1, 12, 3, 30));

    // New process starts from scratch after downtime.
    let mut restarted = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );
    let restart_tick = restarted.tick().unwrap();
    assert_eq!(restart_tick.executed_roots, 0);
    assert_eq!(db.execute_calls(), 0);

    clock.advance(Duration::from_secs(30));
    let next_tick = restarted.tick().unwrap();
    assert_eq!(next_tick.executed_roots, 1);
    assert_eq!(db.execute_calls(), 1);
    assert_eq!(db.executions()[0].at, dt(2026, 2, 1, 12, 4, 0));
}

#[test]
fn unchanged_last_changed_skips_reload() {
    // Polling with unchanged last_changed should avoid load_tasks and preserve in-memory state.
    let now = dt(2026, 2, 1, 12, 0, 0);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 11, 59, 0),
            vec![root_task("root", "CRON 30 * * * * * TZ=UTC", "SQL ROOT")],
        )],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::with_local_timezone(
        db.clone(),
        clock.clone(),
        LocalTimeZone::Named(chrono_tz::UTC),
    );

    let first = scheduler.tick().unwrap();
    assert!(first.reload.is_some());
    assert_eq!(db.load_tasks_calls(), 1);
    assert_eq!(scheduler.snapshot_size(), 1);

    for _ in 0..3 {
        clock.advance(Duration::from_secs(5));
        let step = scheduler.tick().unwrap();
        assert!(step.reload.is_none());
    }

    assert_eq!(db.get_last_changed_calls(), 4);
    assert_eq!(db.load_tasks_calls(), 1);
    assert_eq!(scheduler.snapshot_size(), 1);
}

#[test]
fn missing_tz_is_deterministic_when_local_timezone_is_injected() {
    // This verifies that TZ-missing schedules are testable and do not depend on host timezone.
    let now = dt(2026, 1, 1, 7, 30, 0);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 1, 1, 7, 0, 0),
            vec![root_task("root", "CRON 0 0 9 * * *", "SQL ROOT")],
        )],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::with_local_timezone_and_poll_interval(
        db,
        clock,
        LocalTimeZone::Named(chrono_tz::Europe::Copenhagen),
        Duration::from_secs(3600),
    );

    let _ = scheduler.tick().unwrap();
    assert_eq!(scheduler.next_wake_delay(now), Duration::from_secs(30 * 60));
}

#[test]
fn new_constructor_uses_default_poll_interval_when_idle() {
    let now = dt(2026, 2, 1, 12, 0, 0);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(dt(2026, 2, 1, 11, 59, 0), Vec::new())],
        clock.clone(),
    ));

    let mut scheduler = Scheduler::new(db, clock);
    assert_eq!(scheduler.next_wake_delay(now), DEFAULT_POLL_INTERVAL);
}

#[test]
fn next_wake_delay_is_zero_when_next_due_is_already_in_the_past() {
    let now = dt(2026, 2, 1, 12, 0, 0);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 11, 59, 0),
            vec![root_task("root", "CRON 0 1 * * * * TZ=UTC", "SQL ROOT")],
        )],
        clock.clone(),
    ));

    let mut scheduler =
        Scheduler::with_poll_interval(db.clone(), clock.clone(), Duration::from_secs(3600));
    let _ = scheduler.tick().unwrap();

    let after_due = dt(2026, 2, 1, 12, 2, 0);
    assert_eq!(scheduler.next_wake_delay(after_due), Duration::ZERO);
}

#[tokio::test]
async fn run_forever_returns_error_after_first_successful_tick() {
    let now = dt(2026, 2, 1, 12, 0, 0);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(FailAfterFirstTickDb::new(dt(2026, 2, 1, 11, 59, 0)));
    let mut scheduler = Scheduler::with_poll_interval(db, clock, Duration::from_millis(1));

    let err = scheduler.run_forever().await.unwrap_err();
    assert!(err.to_string().contains("forced run_forever stop"));
}
