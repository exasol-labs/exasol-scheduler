mod common;

use chrono::{DateTime, TimeZone, Utc};
use common::{DbVersion, ProgrammableDb, child_task, disabled_child_task, finalizer_task, root_task};
use exasol_scheduler::model::TaskRow;
use exasol_scheduler::schedule::LocalTimeZone;
use exasol_scheduler::scheduler::Scheduler;
use exasol_scheduler::time::FakeClock;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
}

fn version(last_changed: DateTime<Utc>, tasks: Vec<exasol_scheduler::model::TaskRow>) -> DbVersion {
    DbVersion { last_changed, tasks }
}

fn make_scheduler(db: Arc<ProgrammableDb>, clock: Arc<FakeClock>) -> Scheduler {
    Scheduler::with_local_timezone_and_poll_interval(
        db,
        clock,
        LocalTimeZone::Named(chrono_tz::UTC),
        Duration::from_secs(300),
    )
}

// Advance clock past the top of the next minute so all CRON 0 * * * * * tasks fire.
fn tick_past_minute(clock: &FakeClock, scheduler: &mut Scheduler) {
    let _ = scheduler.tick().unwrap(); // load snapshot
    clock.advance(Duration::from_secs(1)); // cross the minute boundary
}

// --- root with two children and a finalizer ---

#[test]
fn root_with_two_children_and_finalizer_executes_in_order() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 0);

    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(execs[0], "SELECT root");
    assert_eq!(execs[3], "SELECT fin", "finalizer must run last");
    let middle: HashSet<_> = execs[1..3].iter().map(|s| s.as_str()).collect();
    assert!(middle.contains("SELECT child_a"));
    assert!(middle.contains("SELECT child_b"));

    let events = db.history_events();
    assert_eq!(events.len(), 4);
    for e in &events {
        assert!(e.graph_run_id.is_some(), "all events must have graph_run_id");
        assert_eq!(e.graph_run_id, events[0].graph_run_id, "same graph_run_id");
    }
}

// --- graph_run_id is consistent across all events in a run ---

#[test]
fn graph_run_id_is_set_on_every_history_event_in_the_run() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    assert_eq!(events.len(), 2);
    let gid = events[0].graph_run_id.expect("root must have graph_run_id");
    assert_eq!(events[1].graph_run_id, Some(gid));
}

// --- root history event has scheduled_for, children do not ---

#[test]
fn root_history_event_has_scheduled_for_children_do_not() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let root_event = events.iter().find(|e| e.task_id == "root").unwrap();
    let child_event = events.iter().find(|e| e.task_id == "child").unwrap();
    let fin_event = events.iter().find(|e| e.task_id == "fin").unwrap();

    assert!(root_event.scheduled_for.is_some(), "root must have scheduled_for");
    assert!(child_event.scheduled_for.is_none(), "child must not have scheduled_for");
    assert!(fin_event.scheduled_for.is_none(), "finalizer must not have scheduled_for");
}

// --- root failure skips children but finalizer still runs ---

#[test]
fn dag_root_failure_skips_children_runs_finalizer() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root failed");
    let err = scheduler.tick().unwrap_err();
    assert!(err.to_string().contains("root failed"));

    let events = db.history_events();
    assert_eq!(events.len(), 3, "root + child (SKIPPED) + finalizer");

    let root_ev = events.iter().find(|e| e.task_id == "root").unwrap();
    let child_ev = events.iter().find(|e| e.task_id == "child").unwrap();
    let fin_ev = events.iter().find(|e| e.task_id == "fin").unwrap();

    assert_eq!(root_ev.status, "FAILED");
    assert_eq!(child_ev.status, "SKIPPED");
    assert_eq!(fin_ev.status, "SUCCEEDED");
    assert_eq!(fin_ev.graph_phase, "FINAL");

    // child was SKIPPED so not actually executed
    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert!(!execs.contains(&"SELECT child".to_string()));
    assert!(execs.contains(&"SELECT fin".to_string()));
}

// --- child failure skips grandchildren but finalizer still runs ---

#[test]
fn child_failure_skips_grandchildren_but_finalizer_still_runs() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
                child_task("grandchild", "child", "CRON 0 * * * * * TZ=UTC", "SELECT grandchild"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT child", "child failed");
    let result = scheduler.tick().unwrap(); // root succeeded → tick returns Ok

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 1);

    let events = db.history_events();
    assert_eq!(events.len(), 4);

    let statuses: std::collections::HashMap<_, _> = events
        .iter()
        .map(|e| (e.task_id.as_str(), e.status.as_str()))
        .collect();

    assert_eq!(statuses["root"], "SUCCEEDED");
    assert_eq!(statuses["child"], "FAILED");
    assert_eq!(statuses["grandchild"], "SKIPPED");
    assert_eq!(statuses["fin"], "SUCCEEDED");
}

// --- finalizer failure does not stop sibling finalizers ---

#[test]
fn finalizer_failure_does_not_stop_sibling_finalizers() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                finalizer_task("fin_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin_a"),
                finalizer_task("fin_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT fin_a", "fin_a failed");
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 1);

    let events = db.history_events();
    let statuses: std::collections::HashMap<_, _> = events
        .iter()
        .map(|e| (e.task_id.as_str(), e.status.as_str()))
        .collect();

    assert_eq!(statuses["root"], "SUCCEEDED");
    // Both finalizers must have been attempted
    assert!(statuses.contains_key("fin_a"));
    assert!(statuses.contains_key("fin_b"));
    assert_eq!(statuses["fin_a"], "FAILED");
    assert_eq!(statuses["fin_b"], "SUCCEEDED");
}

// --- multi-level DAG executes full depth-first chain ---

#[test]
fn multi_level_dag_executes_full_depth_first_chain() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
                child_task("grandchild", "child", "CRON 0 * * * * * TZ=UTC", "SELECT grandchild"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 0);

    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(execs, vec!["SELECT root", "SELECT child", "SELECT grandchild"]);
}

// --- orphan task is never executed ---

#[test]
fn dag_orphan_task_is_never_executed() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                // orphan: AFTER points to a task not in the snapshot
                child_task("orphan", "nonexistent", "CRON 0 * * * * * TZ=UTC", "SELECT orphan"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(execs, vec!["SELECT root"]);
    assert!(db.history_events().iter().all(|e| e.task_id != "orphan"));
}

// --- cyclic tasks are silently excluded ---

#[test]
fn dag_cyclic_tasks_are_silently_excluded() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                // mutual cycle — neither should execute
                child_task("x", "y", "CRON 0 * * * * * TZ=UTC", "SELECT x"),
                child_task("y", "x", "CRON 0 * * * * * TZ=UTC", "SELECT y"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(execs, vec!["SELECT root"]);
}

// --- snapshot reload rebuilds DAG indexes ---

#[test]
fn snapshot_reload_rebuilds_dag_indexes_mid_scenario() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));

    // v1: root only
    // v2: root + child added
    let v1 = version(
        dt(2026, 2, 1, 12, 0, 0),
        vec![root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root")],
    );
    let v2 = version(
        dt(2026, 2, 1, 12, 0, 30),
        vec![
            root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
            child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
        ],
    );
    let db = Arc::new(ProgrammableDb::new(vec![v1, v2], clock.clone()));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());

    // First tick: load v1, root-only
    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));
    let result1 = scheduler.tick().unwrap();
    assert_eq!(result1.executed_roots, 1);
    assert_eq!(db.executions().len(), 1, "only root in v1");

    // Switch to v2, which adds child
    db.set_version(1);
    clock.advance(Duration::from_secs(60)); // reach next minute (12:01:00 + 60s = 12:02:00)
    let result2 = scheduler.tick().unwrap();
    assert_eq!(result2.executed_roots, 1);
    let execs: Vec<_> = db.executions().into_iter().skip(1).map(|r| r.statement).collect();
    assert_eq!(execs, vec!["SELECT root", "SELECT child"]);
}

// --- failed_children count reported in TickResult ---

#[test]
fn failed_children_count_is_reported_in_tick_result() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT child_a", "child_a failed");
    db.set_failure_for_statement("SELECT fin", "fin failed");
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 2, "child_a and fin both failed");
}

// --- write_history failure in graph does not stop execution ---

#[test]
fn write_history_failure_in_graph_does_not_stop_execution() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
            ],
        )],
        clock.clone(),
    ));
    db.set_write_history_error("history unavailable");
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    let result = scheduler.tick().unwrap();
    assert_eq!(result.executed_roots, 1);
    // Both root and child executed despite write_history failures
    assert_eq!(db.execute_calls(), 2);
    assert_eq!(db.write_history_calls(), 2);
}

// --- finalizer runs after all children regardless of root status ---

#[test]
fn finalizer_runs_after_all_children_on_root_success() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(execs.last().unwrap(), "SELECT fin");
}

#[test]
fn finalizer_runs_after_all_children_on_root_failure() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root broken");
    let _ = scheduler.tick().unwrap_err();

    let fin_event = db.history_events().into_iter().find(|e| e.task_id == "fin");
    assert!(fin_event.is_some(), "finalizer must have run");
    assert_eq!(fin_event.unwrap().graph_phase, "FINAL");
}

// --- disabled child is SKIPPED, not executed (BUG-003) ---

#[test]
fn disabled_child_is_skipped_not_executed_when_parent_succeeds() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                disabled_child_task("disabled_child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT disabled"),
                child_task("enabled_child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT enabled"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);

    // disabled_child must not have been executed
    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert!(!execs.contains(&"SELECT disabled".to_string()), "disabled child must not execute");
    assert!(execs.contains(&"SELECT enabled".to_string()), "enabled child must execute");

    // disabled_child must have a SKIPPED history entry
    let events = db.history_events();
    let skipped = events.iter().find(|e| e.task_id == "disabled_child");
    assert!(skipped.is_some(), "disabled child must have a history entry");
    assert_eq!(skipped.unwrap().status, "SKIPPED");
}

#[test]
fn disabled_finalizer_is_skipped_not_executed() {
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));

    let disabled_fin = TaskRow {
        task_id: "disabled_fin".to_string(),
        enabled: false,
        schedule: "CRON 0 * * * * * TZ=UTC".to_string(),
        statement: "SELECT disabled_fin".to_string(),
        after: Some("root".to_string()),
        is_final: true,
        comment: None,
        parallel_children: true,
    };

    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                finalizer_task("enabled_fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT enabled_fin"),
                disabled_fin,
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let execs: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert!(!execs.contains(&"SELECT disabled_fin".to_string()), "disabled finalizer must not execute");
    assert!(execs.contains(&"SELECT enabled_fin".to_string()), "enabled finalizer must execute");

    let events = db.history_events();
    let skipped = events.iter().find(|e| e.task_id == "disabled_fin");
    assert!(skipped.is_some(), "disabled finalizer must have a history entry");
    assert_eq!(skipped.unwrap().status, "SKIPPED");
}

// --- Gap: finalizer sub-children path (execute_finalizer → execute_children_of) ---

#[test]
fn finalizer_executes_its_own_sub_children() {
    // root → finalizer → sub_child
    // sub_child is a non-finalizer child of the finalizer.
    // This test closes the untested path: execute_finalizer → execute_children_of.
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
                child_task("sub_child", "fin", "CRON 0 * * * * * TZ=UTC", "SELECT sub_child"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let sub_child_ev = events.iter().find(|e| e.task_id == "sub_child");
    assert!(sub_child_ev.is_some(), "sub_child of a finalizer must appear in history");
    assert_eq!(sub_child_ev.unwrap().status, "SUCCEEDED");
    assert_eq!(sub_child_ev.unwrap().graph_phase, "MAIN",
        "children of finalizers execute as MAIN, not FINAL");
}

// --- Gap: SKIPPED-due-to-parent has error_message = None ---

#[test]
fn child_skipped_because_parent_failed_has_no_error_message() {
    // Root fails → child must be SKIPPED with error_message = None.
    // This is different from SKIPPED-due-to-disabled which carries "task is disabled".
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root failed");
    let _ = scheduler.tick().unwrap_err();

    let events = db.history_events();
    let child_ev = events.iter().find(|e| e.task_id == "child").unwrap();
    assert_eq!(child_ev.status, "SKIPPED");
    assert!(
        child_ev.error_message.is_none(),
        "SKIPPED-due-to-parent-failure must have no error_message, got: {:?}",
        child_ev.error_message
    );
}

// --- Gap: SKIPPED-due-to-disabled has error_message = "task is disabled" ---

#[test]
fn disabled_child_skipped_with_task_is_disabled_message() {
    // Root succeeds → disabled child must be SKIPPED with exactly "task is disabled".
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                disabled_child_task("disabled_child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT disabled"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let child_ev = events.iter().find(|e| e.task_id == "disabled_child").unwrap();
    assert_eq!(child_ev.status, "SKIPPED");
    assert_eq!(
        child_ev.error_message.as_deref(),
        Some("task is disabled"),
        "SKIPPED-due-to-disabled must carry 'task is disabled' in error_message"
    );
}

// --- Gap: finalizer always runs even when root failed (regression for the &self refactor) ---

#[test]
fn finalizer_of_failed_root_runs_and_is_not_skipped() {
    // If execute_finalizer accidentally forwarded the real parent_status (FAILED) to
    // execute_and_record, the finalizer would be SKIPPED instead of SUCCEEDED.
    let now = dt(2026, 2, 1, 12, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 2, 1, 12, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root failed");
    let _ = scheduler.tick().unwrap_err();

    let events = db.history_events();
    let fin_ev = events.iter().find(|e| e.task_id == "fin").unwrap();
    assert_eq!(fin_ev.status, "SUCCEEDED",
        "finalizer must run (SUCCEEDED) even when root failed; got: {}", fin_ev.status);
    assert_eq!(fin_ev.graph_phase, "FINAL");
}
