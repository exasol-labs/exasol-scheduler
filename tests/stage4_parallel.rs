mod common;

use chrono::{DateTime, TimeZone, Utc};
use common::{DbVersion, ProgrammableDb, child_task, finalizer_task, root_task, sequential_root_task};
use exasol_scheduler::model::TaskRow;
use exasol_scheduler::schedule::LocalTimeZone;
use exasol_scheduler::scheduler::{Scheduler, diff_task_rows};
use exasol_scheduler::time::FakeClock;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

fn dt(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).unwrap()
}

fn version(last_changed: DateTime<Utc>, tasks: Vec<TaskRow>) -> DbVersion {
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

fn tick_past_minute(clock: &FakeClock, scheduler: &mut Scheduler) {
    let _ = scheduler.tick().unwrap();
    clock.advance(Duration::from_secs(1));
}

// ─── Core parallel behaviour ──────────────────────────────────────────────────

#[test]
fn parallel_children_run_concurrently() {
    // Two children of a default (PARALLEL_CHILDREN=TRUE) parent both appear in
    // history as SUCCEEDED. No ordering guarantee between siblings.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.executed_roots, 1);
    assert_eq!(result.failed_children, 0);

    let events = db.history_events();
    let statuses: std::collections::HashMap<_, _> = events.iter().map(|e| (e.task_id.as_str(), e.status.as_str())).collect();
    assert_eq!(statuses.get("child_a"), Some(&"SUCCEEDED"), "child_a must have SUCCEEDED");
    assert_eq!(statuses.get("child_b"), Some(&"SUCCEEDED"), "child_b must have SUCCEEDED");
}

#[test]
fn sequential_children_run_in_order() {
    // PARALLEL_CHILDREN=FALSE preserves alphabetical execution order.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                sequential_root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.failed_children, 0);
    let stmts: Vec<_> = db.executions().into_iter().map(|r| r.statement).collect();
    assert_eq!(stmts[0], "SELECT root");
    assert_eq!(stmts[1], "SELECT child_a", "alpha-first child must run first in sequential mode");
    assert_eq!(stmts[2], "SELECT child_b");
}

#[test]
fn parallel_children_both_complete_when_one_fails() {
    // Child A fails; child B (concurrent sibling) still completes.
    // Both have history rows; failed_children == 1.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT child_a", "child_a failed");
    let result = scheduler.tick().unwrap();

    assert_eq!(result.failed_children, 1);

    let events = db.history_events();
    let statuses: std::collections::HashMap<_, _> = events.iter().map(|e| (e.task_id.as_str(), e.status.as_str())).collect();
    assert_eq!(statuses["child_a"], "FAILED");
    assert_eq!(statuses["child_b"], "SUCCEEDED", "sibling must still complete when peer fails");
}

#[test]
fn parallel_children_failure_count_is_sum_of_all_branches() {
    // Three parallel children, two fail; TickResult.failed_children == 2.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT a"),
                child_task("b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT b"),
                child_task("c", "root", "CRON 0 * * * * * TZ=UTC", "SELECT c"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT a", "a failed");
    db.set_failure_for_statement("SELECT c", "c failed");
    let result = scheduler.tick().unwrap();

    assert_eq!(result.failed_children, 2, "both failed children must be counted");
}

#[test]
fn parallel_grandchildren_respect_own_parent_flag() {
    // root (parallel=true) → child_a (parallel=false) → grandchild_1, grandchild_2
    //                       → child_b
    // child_a's children run sequentially (its own flag). root's children run in parallel.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));

    let child_a_sequential = TaskRow {
        parallel_children: false,
        ..child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a")
    };

    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_a_sequential,
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
                child_task("grandchild_1", "child_a", "CRON 0 * * * * * TZ=UTC", "SELECT gc1"),
                child_task("grandchild_2", "child_a", "CRON 0 * * * * * TZ=UTC", "SELECT gc2"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    let result = scheduler.tick().unwrap();

    assert_eq!(result.failed_children, 0);
    let events = db.history_events();
    let task_ids: HashSet<_> = events.iter().map(|e| e.task_id.as_str()).collect();
    assert!(task_ids.contains("grandchild_1"), "grandchild_1 must execute");
    assert!(task_ids.contains("grandchild_2"), "grandchild_2 must execute");

    // grandchild_1 must come before grandchild_2 in executions (sequential under child_a)
    let execs = db.executions();
    let stmts: Vec<_> = execs.iter().map(|r| r.statement.as_str()).collect();
    let gc1_pos = stmts.iter().position(|s| *s == "SELECT gc1").unwrap();
    let gc2_pos = stmts.iter().position(|s| *s == "SELECT gc2").unwrap();
    assert!(gc1_pos < gc2_pos, "grandchildren of sequential parent must run alphabetically");
}

#[test]
fn finalizers_run_after_all_parallel_children_complete() {
    // Finalizer must not start until all parallel children (including any slow ones) finish.
    // ProgrammableDb records wall-clock insertion order via Mutex, so the finalizer's
    // execution record must appear after both children.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
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

    let execs = db.executions();
    let stmts: Vec<_> = execs.iter().map(|r| r.statement.as_str()).collect();
    let fin_pos = stmts.iter().position(|s| *s == "SELECT fin").unwrap();
    let ca_pos = stmts.iter().position(|s| *s == "SELECT child_a").unwrap();
    let cb_pos = stmts.iter().position(|s| *s == "SELECT child_b").unwrap();
    assert!(fin_pos > ca_pos, "finalizer must execute after child_a");
    assert!(fin_pos > cb_pos, "finalizer must execute after child_b");
}

#[test]
fn parallel_children_skipped_when_parent_fails() {
    // Root fails → all children cascade-skip sequentially.
    // failed_children == 0 (skips are not failures).
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("child_a", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_a"),
                child_task("child_b", "root", "CRON 0 * * * * * TZ=UTC", "SELECT child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root failed");
    let _ = scheduler.tick().unwrap_err();

    let events = db.history_events();
    let statuses: std::collections::HashMap<_, _> = events.iter().map(|e| (e.task_id.as_str(), e.status.as_str())).collect();
    assert_eq!(statuses["child_a"], "SKIPPED");
    assert_eq!(statuses["child_b"], "SKIPPED");
    assert!(
        statuses["child_a"] == "SKIPPED" && statuses["child_b"] == "SKIPPED",
        "both children must be SKIPPED, not counted as failures"
    );
}

#[test]
fn parallel_flag_change_triggers_snapshot_diff() {
    // Changing PARALLEL_CHILDREN TRUE→FALSE must be detected as "changed" in the diff.
    let old = vec![TaskRow { parallel_children: true,  ..root_task("t", "CRON 0 * * * * * TZ=UTC", "SELECT 1") }];
    let new = vec![TaskRow { parallel_children: false, ..root_task("t", "CRON 0 * * * * * TZ=UTC", "SELECT 1") }];
    let diff = diff_task_rows(&old, &new);
    assert_eq!(diff.changed, vec!["t"]);
    assert!(diff.added.is_empty() && diff.removed.is_empty());
}

#[test]
fn parallel_children_with_disabled_sibling() {
    // Parallel run: enabled child runs, disabled sibling is SKIPPED with "task is disabled".
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));

    let disabled_child = TaskRow {
        task_id: "disabled".to_string(),
        enabled: false,
        schedule: "CRON 0 * * * * * TZ=UTC".to_string(),
        statement: "SELECT disabled".to_string(),
        after: Some("root".to_string()),
        is_final: false,
        comment: None,
        parallel_children: true,
    };

    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                disabled_child,
                child_task("enabled_child", "root", "CRON 0 * * * * * TZ=UTC", "SELECT enabled"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let statuses: std::collections::HashMap<_, _> = events.iter().map(|e| (e.task_id.as_str(), e.status.as_str())).collect();
    assert_eq!(statuses["enabled_child"], "SUCCEEDED");
    assert_eq!(statuses["disabled"], "SKIPPED");

    let disabled_ev = events.iter().find(|e| e.task_id == "disabled").unwrap();
    assert_eq!(disabled_ev.error_message.as_deref(), Some("task is disabled"));
}

// ─── Call-site coverage ───────────────────────────────────────────────────────

#[test]
fn root_direct_children_use_parallel_execution() {
    // The root's DIRECT children are dispatched via run() at depth=0.
    // This test verifies that execute_children_of is called from run(), not just from
    // execute_node(). Without this, a bug where run() still used the old sequential
    // loop would be invisible.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("ca", "root", "CRON 0 * * * * * TZ=UTC", "SELECT ca"),
                child_task("cb", "root", "CRON 0 * * * * * TZ=UTC", "SELECT cb"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let task_ids: HashSet<_> = events.iter().map(|e| e.task_id.as_str()).collect();
    assert!(task_ids.contains("ca"), "ca must have been dispatched via run()");
    assert!(task_ids.contains("cb"), "cb must have been dispatched via run()");
}

#[test]
fn finalizer_children_run_in_parallel_by_default() {
    // A finalizer's own child tasks (via execute_finalizer → execute_children_of) both execute.
    // Verifies the third call site of execute_children_of.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin"),
                child_task("fin_child_a", "fin", "CRON 0 * * * * * TZ=UTC", "SELECT fin_child_a"),
                child_task("fin_child_b", "fin", "CRON 0 * * * * * TZ=UTC", "SELECT fin_child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let task_ids: HashSet<_> = events.iter().map(|e| e.task_id.as_str()).collect();
    assert!(task_ids.contains("fin_child_a"), "finalizer sub-child fin_child_a must execute");
    assert!(task_ids.contains("fin_child_b"), "finalizer sub-child fin_child_b must execute");
}

#[test]
fn finalizer_children_respect_sequential_flag() {
    // A finalizer with PARALLEL_CHILDREN=FALSE runs its children sequentially.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));

    let sequential_fin = TaskRow {
        parallel_children: false,
        ..finalizer_task("fin", "root", "CRON 0 * * * * * TZ=UTC", "SELECT fin")
    };

    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                sequential_fin,
                child_task("fin_child_a", "fin", "CRON 0 * * * * * TZ=UTC", "SELECT fin_child_a"),
                child_task("fin_child_b", "fin", "CRON 0 * * * * * TZ=UTC", "SELECT fin_child_b"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let execs = db.executions();
    let stmts: Vec<_> = execs.iter().map(|r| r.statement.as_str()).collect();
    let pos_a = stmts.iter().position(|s| *s == "SELECT fin_child_a").unwrap();
    let pos_b = stmts.iter().position(|s| *s == "SELECT fin_child_b").unwrap();
    assert!(pos_a < pos_b, "sequential finalizer children must run alphabetically: a before b");
}

// ─── History event correctness under parallel execution ───────────────────────

#[test]
fn parallel_children_share_graph_run_id() {
    // All parallel siblings in one graph run must share the root's graph_run_id.
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("ca", "root", "CRON 0 * * * * * TZ=UTC", "SELECT ca"),
                child_task("cb", "root", "CRON 0 * * * * * TZ=UTC", "SELECT cb"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);
    scheduler.tick().unwrap();

    let events = db.history_events();
    let root_ev = events.iter().find(|e| e.task_id == "root").unwrap();
    let root_gid = root_ev.graph_run_id.expect("root must have graph_run_id");

    for ev in &events {
        assert_eq!(
            ev.graph_run_id,
            Some(root_gid),
            "task {} has wrong graph_run_id", ev.task_id
        );
    }
}

#[test]
fn parallel_skipped_children_have_no_error_message() {
    // When parallel children cascade-skip (parent failed), each SKIPPED event must have
    // error_message == None (not "task is disabled" — different SKIPPED reason).
    let now = dt(2026, 3, 1, 8, 0, 59);
    let clock = Arc::new(FakeClock::new(now));
    let db = Arc::new(ProgrammableDb::new(
        vec![version(
            dt(2026, 3, 1, 8, 0, 0),
            vec![
                root_task("root", "CRON 0 * * * * * TZ=UTC", "SELECT root"),
                child_task("ca", "root", "CRON 0 * * * * * TZ=UTC", "SELECT ca"),
                child_task("cb", "root", "CRON 0 * * * * * TZ=UTC", "SELECT cb"),
            ],
        )],
        clock.clone(),
    ));
    let mut scheduler = make_scheduler(db.clone(), clock.clone());
    tick_past_minute(&clock, &mut scheduler);

    db.set_failure_for_statement("SELECT root", "root failed");
    let _ = scheduler.tick().unwrap_err();

    let events = db.history_events();
    for ev in events.iter().filter(|e| e.task_id != "root") {
        assert_eq!(ev.status, "SKIPPED");
        assert!(
            ev.error_message.is_none(),
            "cascade-skipped task {} must have no error_message", ev.task_id
        );
    }
}
