use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use uuid::Uuid;

use crate::db::{DbError, SchedulerDb};
use crate::model::{HistoryEvent, TaskRow};
use crate::schedule::{LocalTimeZone, ParsedSchedule};
use crate::time::Clock;

pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MAX_GRAPH_DEPTH: usize = 20;

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Db(#[from] DbError),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReloadStats {
    pub added: usize,
    pub removed: usize,
    pub changed: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickResult {
    pub executed_roots: usize,
    pub failed_roots: usize,
    pub failed_children: usize,
    pub reload: Option<ReloadStats>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootDebugState {
    pub task_id: String,
    pub generation: u64,
    pub next_due: DateTime<Utc>,
}

pub struct Scheduler {
    db: Arc<dyn SchedulerDb>,
    clock: Arc<dyn Clock>,
    poll_interval: Duration,
    local_tz: LocalTimeZone,
    state: SchedulerState,
    last_changed: Option<DateTime<Utc>>,
}

impl Scheduler {
    pub fn new(db: Arc<dyn SchedulerDb>, clock: Arc<dyn Clock>) -> Self {
        Self::with_local_timezone_and_poll_interval(
            db,
            clock,
            LocalTimeZone::System,
            DEFAULT_POLL_INTERVAL,
        )
    }

    pub fn with_poll_interval(
        db: Arc<dyn SchedulerDb>,
        clock: Arc<dyn Clock>,
        poll_interval: Duration,
    ) -> Self {
        Self::with_local_timezone_and_poll_interval(db, clock, LocalTimeZone::System, poll_interval)
    }

    pub fn with_local_timezone(
        db: Arc<dyn SchedulerDb>,
        clock: Arc<dyn Clock>,
        local_tz: LocalTimeZone,
    ) -> Self {
        Self::with_local_timezone_and_poll_interval(db, clock, local_tz, DEFAULT_POLL_INTERVAL)
    }

    pub fn with_local_timezone_and_poll_interval(
        db: Arc<dyn SchedulerDb>,
        clock: Arc<dyn Clock>,
        local_tz: LocalTimeZone,
        poll_interval: Duration,
    ) -> Self {
        Self {
            db,
            clock,
            poll_interval,
            local_tz,
            state: SchedulerState::default(),
            last_changed: None,
        }
    }

    pub fn tick(&mut self) -> Result<TickResult, SchedulerError> {
        let now = self.clock.now();
        let reload = self.reload_if_changed(now)?;
        let outcome = self.execute_due_roots(now);

        Ok(TickResult {
            executed_roots: outcome.executed_roots,
            failed_roots: outcome.failed_roots,
            failed_children: outcome.failed_children,
            reload,
        })
    }

    pub fn next_wake_delay(&mut self, now: DateTime<Utc>) -> Duration {
        let next_due_delay = self
            .state
            .next_due_utc()
            .map(|due| duration_until(now, due))
            .unwrap_or(self.poll_interval);
        std::cmp::min(next_due_delay, self.poll_interval)
    }

    pub fn root_count(&self) -> usize {
        self.state.roots.len()
    }

    pub fn snapshot_size(&self) -> usize {
        self.state.snapshot.len()
    }

    pub fn root_debug_state(&self, task_id: &str) -> Option<RootDebugState> {
        self.state.roots.get(task_id).map(|root| RootDebugState {
            task_id: task_id.to_string(),
            generation: root.generation,
            next_due: root.next_due,
        })
    }

    pub async fn run_forever(&mut self) -> Result<(), SchedulerError> {
        loop {
            let result = self.tick()?;
            tracing::debug!(
                executed_roots = result.executed_roots,
                failed_roots = result.failed_roots,
                failed_children = result.failed_children,
                reload = ?result.reload,
                "tick completed"
            );

            let delay = self.next_wake_delay(self.clock.now());
            tokio::time::sleep(delay).await;
        }
    }

    fn reload_if_changed(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<Option<ReloadStats>, SchedulerError> {
        let current_last_changed = self.db.get_last_changed()?;
        let should_reload = self.last_changed != Some(current_last_changed);

        if !should_reload {
            return Ok(None);
        }

        let rows = self.db.load_tasks()?;
        let mut new_snapshot = HashMap::with_capacity(rows.len());
        for row in rows {
            let task = TaskDef::from_row(row);
            new_snapshot.insert(task.task_id.clone(), task);
        }

        let stats = self.state.apply_snapshot(new_snapshot, now, self.local_tz);
        self.last_changed = Some(current_last_changed);
        Ok(Some(stats))
    }

    /// Runs every due graph. Task failures (root, child or finalizer) are recorded in
    /// history and logged, but never abort the polling loop: a broken task must not stop
    /// unrelated pipelines served by the same process.
    fn execute_due_roots(&mut self, now: DateTime<Utc>) -> DueRootsOutcome {
        let mut outcome = DueRootsOutcome::default();

        while let Some(due) = self.state.pop_due_root(now, self.local_tz) {
            tracing::info!(
                task_id = due.task_id.as_str(),
                scheduled_for = %due.scheduled_for,
                "executing graph run"
            );

            let graph_run_id = Uuid::new_v4();
            let runner = GraphRunner {
                db: &*self.db,
                clock: &*self.clock,
                snapshot: &self.state.snapshot,
                children_of: &self.state.children_of,
                finalizers_of: &self.state.finalizers_of,
                graph_run_id,
            };
            let result = runner.run(&due);
            outcome.executed_roots += 1;
            outcome.failed_children += result.failed_children;

            if let Some(error) = &result.root_error {
                outcome.failed_roots += 1;
                tracing::warn!(
                    task_id = due.task_id.as_str(),
                    graph_run_id = %graph_run_id,
                    error = error.as_str(),
                    "root task failed; descendants skipped"
                );
            }

            if result.failed_children > 0 {
                tracing::warn!(
                    task_id = due.task_id.as_str(),
                    graph_run_id = %graph_run_id,
                    failed_children = result.failed_children,
                    "graph run completed with child failures"
                );
            }
        }

        outcome
    }
}

// --- DAG index construction ---

fn find_cycles(snapshot: &HashMap<String, TaskDef>) -> HashSet<String> {
    let mut in_cycle: HashSet<String> = HashSet::new();
    let mut done: HashSet<String> = HashSet::new();

    for start_id in snapshot.keys() {
        if done.contains(start_id.as_str()) {
            continue;
        }

        let mut path: Vec<String> = Vec::new();
        let mut current = start_id.clone();

        loop {
            if done.contains(&current) {
                break;
            }
            if let Some(pos) = path.iter().position(|n| n == &current) {
                for node in &path[pos..] {
                    in_cycle.insert(node.clone());
                }
                in_cycle.insert(current.clone());
                break;
            }

            path.push(current.clone());

            let next = snapshot
                .get(&current)
                .and_then(|t| t.after.clone())
                .filter(|parent| snapshot.contains_key(parent));

            match next {
                Some(parent) => current = parent,
                None => break,
            }
        }

        for node in path {
            done.insert(node);
        }
    }

    in_cycle
}

pub(crate) fn build_dag_indexes(
    snapshot: &HashMap<String, TaskDef>,
) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
    let invalid = find_cycles(snapshot);

    let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
    let mut finalizers_of: HashMap<String, Vec<String>> = HashMap::new();

    for (task_id, task) in snapshot {
        if invalid.contains(task_id) {
            continue;
        }
        let Some(parent_id) = &task.after else {
            continue;
        };
        if !snapshot.contains_key(parent_id) {
            continue; // orphan
        }
        if invalid.contains(parent_id) {
            continue;
        }
        if task.is_final {
            finalizers_of
                .entry(parent_id.clone())
                .or_default()
                .push(task_id.clone());
        } else {
            children_of
                .entry(parent_id.clone())
                .or_default()
                .push(task_id.clone());
        }
    }

    for vec in children_of.values_mut() {
        vec.sort();
    }
    for vec in finalizers_of.values_mut() {
        vec.sort();
    }

    (children_of, finalizers_of)
}

// --- GraphRunner ---

#[derive(Default)]
struct DueRootsOutcome {
    executed_roots: usize,
    failed_roots: usize,
    failed_children: usize,
}

struct GraphRunResult {
    root_error: Option<String>,
    failed_children: usize,
}

struct GraphRunner<'a> {
    db: &'a dyn SchedulerDb,
    clock: &'a dyn Clock,
    snapshot: &'a HashMap<String, TaskDef>,
    children_of: &'a HashMap<String, Vec<String>>,
    finalizers_of: &'a HashMap<String, Vec<String>>,
    graph_run_id: Uuid,
}

impl<'a> GraphRunner<'a> {
    fn run(&self, due: &DueRoot) -> GraphRunResult {
        let started_at = self.clock.now();
        let exec_result = self.db.execute_statement(&due.statement);
        let finished_at = self.clock.now();

        let (status, error_message) = match exec_result {
            Ok(_) => ("SUCCEEDED".to_string(), None),
            Err(e) => ("FAILED".to_string(), Some(e.to_string())),
        };

        let event = HistoryEvent {
            run_id: Uuid::new_v4(),
            graph_run_id: Some(self.graph_run_id),
            task_id: due.task_id.clone(),
            graph_phase: "MAIN".to_string(),
            scheduled_for: Some(due.scheduled_for),
            started_at,
            finished_at: Some(finished_at),
            status: status.clone(),
            error_message: error_message.clone(),
        };
        if let Err(e) = self.db.write_history(&event) {
            tracing::warn!(task_id = due.task_id.as_str(), error = %e, "write_history failed");
        }

        let child_failures = self.execute_children_of(&due.task_id, &status, 0);
        let finalizer_failures = self.execute_finalizers_of(&due.task_id, 0);

        GraphRunResult {
            root_error: error_message,
            failed_children: child_failures + finalizer_failures,
        }
    }

    fn execute_node(&self, task_id: &str, parent_status: &str, depth: usize) -> usize {
        if depth > MAX_GRAPH_DEPTH {
            tracing::warn!(task_id, "max graph depth exceeded, skipping node");
            return 0;
        }

        let Some(task) = self.snapshot.get(task_id).cloned() else {
            return 0;
        };

        let (status, own_failures) = self.execute_and_record(&task, parent_status, "MAIN");
        let child_failures = self.execute_children_of(task_id, &status, depth);
        let finalizer_failures = self.execute_finalizers_of(task_id, depth);
        own_failures + child_failures + finalizer_failures
    }

    fn execute_finalizer(&self, task_id: &str, depth: usize) -> usize {
        if depth > MAX_GRAPH_DEPTH {
            tracing::warn!(task_id, "max graph depth exceeded, skipping finalizer");
            return 0;
        }

        let Some(task) = self.snapshot.get(task_id).cloned() else {
            return 0;
        };

        // Pass "SUCCEEDED" so execute_and_record's parent-status check never fires —
        // finalizers are never skipped due to parent outcome, only due to being disabled.
        let (status, own_failures) = self.execute_and_record(&task, "SUCCEEDED", "FINAL");
        let child_failures = self.execute_children_of(task_id, &status, depth);
        // Finalizers do not recurse into their own finalizers.
        own_failures + child_failures
    }

    fn execute_and_record(
        &self,
        task: &TaskDef,
        parent_status: &str,
        graph_phase: &str,
    ) -> (String, usize) {
        let (status, started_at, finished_at, error_message) = if parent_status != "SUCCEEDED" {
            let t = self.clock.now();
            ("SKIPPED".to_string(), t, None, None)
        } else if !task.enabled {
            let t = self.clock.now();
            (
                "SKIPPED".to_string(),
                t,
                None,
                Some("task is disabled".to_string()),
            )
        } else {
            let started_at = self.clock.now();
            let exec_result = self.db.execute_statement(&task.statement);
            let finished_at = self.clock.now();
            match exec_result {
                Ok(_) => ("SUCCEEDED".to_string(), started_at, Some(finished_at), None),
                Err(e) => (
                    "FAILED".to_string(),
                    started_at,
                    Some(finished_at),
                    Some(e.to_string()),
                ),
            }
        };

        let own_failures = usize::from(status == "FAILED");

        let event = HistoryEvent {
            run_id: Uuid::new_v4(),
            graph_run_id: Some(self.graph_run_id),
            task_id: task.task_id.clone(),
            graph_phase: graph_phase.to_string(),
            scheduled_for: None,
            started_at,
            finished_at,
            status: status.clone(),
            error_message,
        };
        if let Err(e) = self.db.write_history(&event) {
            tracing::warn!(task_id = task.task_id.as_str(), error = %e, "write_history failed");
        }

        (status, own_failures)
    }

    fn execute_children_of(&self, parent_id: &str, parent_status: &str, depth: usize) -> usize {
        let children = self.children_of.get(parent_id).cloned().unwrap_or_default();

        if children.is_empty() {
            return 0;
        }

        let parallel = self
            .snapshot
            .get(parent_id)
            .map(|t| t.parallel_children)
            .unwrap_or(true);

        if !parallel || parent_status != "SUCCEEDED" {
            // Sequential: explicit opt-out, or cascading SKIPPED (nothing useful to parallelise).
            children
                .iter()
                .map(|child_id| self.execute_node(child_id, parent_status, depth + 1))
                .sum()
        } else {
            // Parallel (default). Collect ALL handles before joining any — joining before all
            // are spawned blocks other threads from starting and destroys concurrency.
            let mut total = 0usize;
            std::thread::scope(|s| {
                let handles: Vec<_> = children
                    .iter()
                    .map(|child_id| {
                        std::thread::Builder::new()
                            .name(format!("sched-child-{child_id}"))
                            .spawn_scoped(s, || self.execute_node(child_id, "SUCCEEDED", depth + 1))
                            .expect("failed to spawn scheduler thread")
                    })
                    .collect();
                for handle in handles {
                    match handle.join() {
                        Ok(failures) => total += failures,
                        Err(payload) => {
                            let msg = payload
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| payload.downcast_ref::<&str>().copied())
                                .unwrap_or("<non-string panic payload>");
                            tracing::error!(
                                parent_id,
                                panic_message = msg,
                                "child execution thread panicked"
                            );
                            total += 1;
                        }
                    }
                }
            });
            total
        }
    }

    fn execute_finalizers_of(&self, parent_id: &str, depth: usize) -> usize {
        self.finalizers_of
            .get(parent_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|finalizer_id| self.execute_finalizer(&finalizer_id, depth + 1))
            .sum()
    }
}

// --- SchedulerState ---

#[derive(Default)]
struct SchedulerState {
    snapshot: HashMap<String, TaskDef>,
    children_of: HashMap<String, Vec<String>>,
    finalizers_of: HashMap<String, Vec<String>>,
    roots: HashMap<String, RootState>,
    heap: BinaryHeap<HeapItem>,
}

impl SchedulerState {
    fn apply_snapshot(
        &mut self,
        mut new_snapshot: HashMap<String, TaskDef>,
        now: DateTime<Utc>,
        local_tz: LocalTimeZone,
    ) -> ReloadStats {
        let diff = diff_snapshots(&self.snapshot, &new_snapshot);

        for removed_id in &diff.removed {
            self.snapshot.remove(removed_id);
            self.roots.remove(removed_id);
        }

        for added_id in &diff.added {
            if let Some(task) = new_snapshot.remove(added_id) {
                self.snapshot.insert(added_id.clone(), task);
                self.reconcile_root(added_id, now, local_tz);
            }
        }

        for changed_id in &diff.changed {
            if let Some(new_task) = new_snapshot.remove(changed_id) {
                let schedule_changed = self
                    .snapshot
                    .get(changed_id)
                    .map(|old| old.schedule_fingerprint != new_task.schedule_fingerprint)
                    .unwrap_or(true);

                self.snapshot.insert(changed_id.clone(), new_task);

                if schedule_changed {
                    self.reconcile_root(changed_id, now, local_tz);
                }
            }
        }

        let (children_of, finalizers_of) = build_dag_indexes(&self.snapshot);
        self.children_of = children_of;
        self.finalizers_of = finalizers_of;

        ReloadStats {
            added: diff.added.len(),
            removed: diff.removed.len(),
            changed: diff.changed.len(),
        }
    }

    fn pop_due_root(&mut self, now: DateTime<Utc>, local_tz: LocalTimeZone) -> Option<DueRoot> {
        self.discard_stale_heap_head();

        let next_due = self.heap.peek()?;
        if next_due.due_at > now {
            return None;
        }

        let next_due = self.heap.pop()?;
        let task = self.snapshot.get(&next_due.task_id).cloned()?;

        if !task.is_active_root() {
            return None;
        }

        let due = DueRoot {
            task_id: next_due.task_id.clone(),
            statement: task.statement.clone(),
            scheduled_for: next_due.due_at,
        };

        self.reschedule_root(&next_due.task_id, now, local_tz);
        Some(due)
    }

    fn next_due_utc(&mut self) -> Option<DateTime<Utc>> {
        self.discard_stale_heap_head();
        self.heap.peek().map(|item| item.due_at)
    }

    fn reconcile_root(&mut self, task_id: &str, now: DateTime<Utc>, local_tz: LocalTimeZone) {
        let Some(task) = self.snapshot.get(task_id) else {
            self.roots.remove(task_id);
            return;
        };

        if !task.is_active_root() {
            self.roots.remove(task_id);
            return;
        }

        let Some(next_due) = task.next_due_after(now, local_tz) else {
            self.roots.remove(task_id);
            return;
        };

        let next_generation = self
            .roots
            .get(task_id)
            .map(|root| root.generation + 1)
            .unwrap_or(1);

        self.roots.insert(
            task_id.to_string(),
            RootState {
                generation: next_generation,
                next_due,
            },
        );

        self.heap.push(HeapItem {
            task_id: task_id.to_string(),
            generation: next_generation,
            due_at: next_due,
        });
    }

    fn reschedule_root(&mut self, task_id: &str, now: DateTime<Utc>, local_tz: LocalTimeZone) {
        let Some(task) = self.snapshot.get(task_id) else {
            self.roots.remove(task_id);
            return;
        };

        let Some(root_state) = self.roots.get_mut(task_id) else {
            return;
        };

        let Some(next_due) = task.next_due_after(now, local_tz) else {
            self.roots.remove(task_id);
            return;
        };

        root_state.generation += 1;
        root_state.next_due = next_due;

        self.heap.push(HeapItem {
            task_id: task_id.to_string(),
            generation: root_state.generation,
            due_at: next_due,
        });
    }

    fn discard_stale_heap_head(&mut self) {
        loop {
            let Some(head) = self.heap.peek() else {
                break;
            };

            let keep = self
                .roots
                .get(&head.task_id)
                .map(|root| root.generation == head.generation && root.next_due == head.due_at)
                .unwrap_or(false);

            if keep {
                break;
            }

            let _ = self.heap.pop();
        }
    }
}

#[derive(Debug)]
struct RootState {
    generation: u64,
    next_due: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct DueRoot {
    task_id: String,
    statement: String,
    scheduled_for: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskDef {
    pub(crate) task_id: String,
    pub(crate) enabled: bool,
    pub(crate) parsed_schedule: Option<ParsedSchedule>,
    pub(crate) statement: String,
    pub(crate) after: Option<String>,
    pub(crate) is_final: bool,
    pub(crate) parallel_children: bool,
    pub(crate) schedule_fingerprint: u64,
    pub(crate) fingerprint: u64,
}

impl TaskDef {
    pub(crate) fn from_row(row: TaskRow) -> Self {
        let after = row.after.and_then(|value| {
            if value.trim().is_empty() {
                None
            } else {
                Some(value)
            }
        });
        let is_root = after.is_none() && !row.is_final;
        let parsed_schedule = if row.enabled && is_root {
            match ParsedSchedule::parse(&row.schedule) {
                Ok(ps) => Some(ps),
                Err(e) => {
                    tracing::warn!(
                        task_id = row.task_id.as_str(),
                        schedule = row.schedule.as_str(),
                        error = %e,
                        "task has unparseable schedule and will never fire"
                    );
                    None
                }
            }
        } else {
            None
        };

        let normalized_schedule = parsed_schedule
            .as_ref()
            .map(|schedule| schedule.normalized().to_string())
            .unwrap_or_else(|| normalize_schedule_text(&row.schedule));

        let schedule_fingerprint = schedule_fingerprint(
            row.enabled,
            &normalized_schedule,
            after.as_deref(),
            row.is_final,
        );
        let statement_hash = stable_hash(&row.statement);
        let fingerprint =
            full_fingerprint(schedule_fingerprint, statement_hash, row.parallel_children);

        Self {
            task_id: row.task_id,
            enabled: row.enabled,
            parsed_schedule,
            statement: row.statement,
            after,
            is_final: row.is_final,
            parallel_children: row.parallel_children,
            schedule_fingerprint,
            fingerprint,
        }
    }

    fn is_active_root(&self) -> bool {
        self.enabled && self.after.is_none() && !self.is_final && self.parsed_schedule.is_some()
    }

    fn next_due_after(&self, now: DateTime<Utc>, local_tz: LocalTimeZone) -> Option<DateTime<Utc>> {
        self.parsed_schedule
            .as_ref()
            .and_then(|schedule| schedule.next_after_with_local(now, local_tz))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapItem {
    task_id: String,
    generation: u64,
    due_at: DateTime<Utc>,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .due_at
            .cmp(&self.due_at)
            .then_with(|| other.task_id.cmp(&self.task_id))
            .then_with(|| other.generation.cmp(&self.generation))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskFingerprints {
    pub schedule_fingerprint: u64,
    pub full_fingerprint: u64,
}

fn diff_snapshots(
    old_snapshot: &HashMap<String, TaskDef>,
    new_snapshot: &HashMap<String, TaskDef>,
) -> SnapshotDiff {
    let old_ids: BTreeSet<String> = old_snapshot.keys().cloned().collect();
    let new_ids: BTreeSet<String> = new_snapshot.keys().cloned().collect();

    let added = new_ids.difference(&old_ids).cloned().collect();
    let removed = old_ids.difference(&new_ids).cloned().collect();

    let changed = old_ids
        .intersection(&new_ids)
        .filter_map(|task_id| {
            let old_task = old_snapshot.get(task_id)?;
            let new_task = new_snapshot.get(task_id)?;
            if old_task.fingerprint != new_task.fingerprint {
                Some(task_id.clone())
            } else {
                None
            }
        })
        .collect();

    SnapshotDiff {
        added,
        removed,
        changed,
    }
}

pub fn diff_task_rows(old_rows: &[TaskRow], new_rows: &[TaskRow]) -> SnapshotDiff {
    let old_snapshot = old_rows
        .iter()
        .cloned()
        .map(TaskDef::from_row)
        .map(|task| (task.task_id.clone(), task))
        .collect();
    let new_snapshot = new_rows
        .iter()
        .cloned()
        .map(TaskDef::from_row)
        .map(|task| (task.task_id.clone(), task))
        .collect();
    diff_snapshots(&old_snapshot, &new_snapshot)
}

pub fn fingerprints_for_row(row: &TaskRow) -> TaskFingerprints {
    let task = TaskDef::from_row(row.clone());
    TaskFingerprints {
        schedule_fingerprint: task.schedule_fingerprint,
        full_fingerprint: task.fingerprint,
    }
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn schedule_fingerprint(
    enabled: bool,
    normalized_schedule: &str,
    after: Option<&str>,
    is_final: bool,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    enabled.hash(&mut hasher);
    normalized_schedule.hash(&mut hasher);
    after.hash(&mut hasher);
    is_final.hash(&mut hasher);
    hasher.finish()
}

fn full_fingerprint(
    schedule_fingerprint: u64,
    statement_hash: u64,
    parallel_children: bool,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schedule_fingerprint.hash(&mut hasher);
    statement_hash.hash(&mut hasher);
    parallel_children.hash(&mut hasher);
    hasher.finish()
}

fn normalize_schedule_text(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn duration_until(now: DateTime<Utc>, due: DateTime<Utc>) -> Duration {
    if due <= now {
        return Duration::ZERO;
    }

    (due - now).to_std().unwrap_or(Duration::from_millis(1))
}

#[cfg(test)]
mod dag_index_tests {
    use super::*;

    fn make_task(task_id: &str, after: Option<&str>, is_final: bool) -> TaskDef {
        TaskDef {
            task_id: task_id.to_string(),
            enabled: true,
            parsed_schedule: None,
            statement: format!("SELECT {task_id}"),
            after: after.map(str::to_string),
            is_final,
            parallel_children: true,
            schedule_fingerprint: 0,
            fingerprint: 0,
        }
    }

    fn make_row(task_id: &str) -> crate::model::TaskRow {
        crate::model::TaskRow {
            task_id: task_id.to_string(),
            enabled: true,
            schedule: "CRON 0 * * * * * TZ=UTC".to_string(),
            statement: format!("SELECT {task_id}"),
            after: None,
            is_final: false,
            comment: None,
            parallel_children: true,
        }
    }

    fn snapshot(tasks: Vec<TaskDef>) -> HashMap<String, TaskDef> {
        tasks.into_iter().map(|t| (t.task_id.clone(), t)).collect()
    }

    #[test]
    fn index_build_assigns_children_and_finalizers_correctly() {
        let snap = snapshot(vec![
            make_task("root", None, false),
            make_task("child_a", Some("root"), false),
            make_task("child_b", Some("root"), false),
            make_task("fin", Some("root"), true),
        ]);
        let (children, finalizers) = build_dag_indexes(&snap);
        let mut kids = children["root"].clone();
        kids.sort();
        assert_eq!(kids, vec!["child_a", "child_b"]);
        assert_eq!(finalizers["root"], vec!["fin"]);
        assert!(!children.contains_key("child_a"));
    }

    #[test]
    fn index_build_sorts_children_by_task_id_for_determinism() {
        let snap = snapshot(vec![
            make_task("root", None, false),
            make_task("z_child", Some("root"), false),
            make_task("a_child", Some("root"), false),
            make_task("m_child", Some("root"), false),
        ]);
        let (children, _) = build_dag_indexes(&snap);
        assert_eq!(children["root"], vec!["a_child", "m_child", "z_child"]);
    }

    #[test]
    fn orphan_task_is_excluded_from_indexes() {
        let snap = snapshot(vec![
            make_task("root", None, false),
            make_task("orphan", Some("nonexistent_parent"), false),
        ]);
        let (children, finalizers) = build_dag_indexes(&snap);
        assert!(children.is_empty());
        assert!(finalizers.is_empty());
    }

    #[test]
    fn cycle_detection_excludes_cycle_members_from_indexes() {
        // x → y → x (mutual cycle)
        let snap2 = snapshot(vec![
            make_task("root", None, false),
            make_task("x", Some("y"), false), // x depends on y
            make_task("y", Some("x"), false), // y depends on x (cycle!)
        ]);
        let (children, _) = build_dag_indexes(&snap2);
        // x and y are in a cycle, neither should appear in indexes
        assert!(!children.contains_key("x"));
        assert!(!children.contains_key("y"));
        // root has no children (x and y are excluded)
        assert!(!children.contains_key("root"));
    }

    #[test]
    fn two_node_mutual_cycle_detected() {
        let snap = snapshot(vec![
            make_task("a", Some("b"), false),
            make_task("b", Some("a"), false),
        ]);
        let cycles = find_cycles(&snap);
        assert!(cycles.contains("a"));
        assert!(cycles.contains("b"));
    }

    #[test]
    fn no_false_positive_cycles_in_valid_dag() {
        let snap = snapshot(vec![
            make_task("root", None, false),
            make_task("child_a", Some("root"), false),
            make_task("child_b", Some("root"), false),
            make_task("grandchild", Some("child_a"), false),
        ]);
        let cycles = find_cycles(&snap);
        assert!(cycles.is_empty());

        let (children, _) = build_dag_indexes(&snap);
        assert_eq!(children["root"], vec!["child_a", "child_b"]);
        assert_eq!(children["child_a"], vec!["grandchild"]);
    }

    #[test]
    fn from_row_propagates_parallel_children_true() {
        let row = crate::model::TaskRow {
            parallel_children: true,
            ..make_row("t")
        };
        let task = TaskDef::from_row(row);
        assert!(task.parallel_children);
    }

    #[test]
    fn from_row_propagates_parallel_children_false() {
        let row = crate::model::TaskRow {
            parallel_children: false,
            ..make_row("t")
        };
        let task = TaskDef::from_row(row);
        assert!(!task.parallel_children);
    }
}

/// Property-based tests: compare the real cycle detection and schedule queue against
/// deliberately naive reference implementations on randomly generated inputs.
#[cfg(test)]
mod proptests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    // --- find_cycles / build_dag_indexes ---

    /// Parent index per task; an index >= n refers to a task that does not exist.
    fn graph_strategy() -> impl Strategy<Value = Vec<(Option<usize>, bool)>> {
        (1usize..=10).prop_flat_map(|n| {
            prop::collection::vec((prop::option::of(0..n + 2), any::<bool>()), n)
        })
    }

    fn graph_snapshot(graph: &[(Option<usize>, bool)]) -> HashMap<String, TaskDef> {
        graph
            .iter()
            .enumerate()
            .map(|(i, (parent, is_final))| {
                let task = TaskDef {
                    task_id: format!("t{i}"),
                    enabled: true,
                    parsed_schedule: None,
                    statement: String::new(),
                    after: parent.map(|p| format!("t{p}")),
                    is_final: *is_final,
                    parallel_children: true,
                    schedule_fingerprint: 0,
                    fingerprint: 0,
                };
                (task.task_id.clone(), task)
            })
            .collect()
    }

    /// Reference: a task is on a cycle iff following existing parents leads back to it.
    fn reference_on_cycle(graph: &[(Option<usize>, bool)], start: usize) -> bool {
        let parent = |i: usize| graph[i].0.filter(|&p| p < graph.len());
        let mut current = parent(start);
        for _ in 0..graph.len() {
            match current {
                Some(node) if node == start => return true,
                Some(node) => current = parent(node),
                None => return false,
            }
        }
        false
    }

    proptest! {
        #[test]
        fn find_cycles_marks_exactly_the_tasks_on_a_cycle(graph in graph_strategy()) {
            let cycles = find_cycles(&graph_snapshot(&graph));
            for i in 0..graph.len() {
                prop_assert_eq!(
                    cycles.contains(&format!("t{i}")),
                    reference_on_cycle(&graph, i),
                    "task t{} in {:?}", i, graph
                );
            }
        }

        #[test]
        fn dag_indexes_list_every_valid_task_once_under_its_parent(graph in graph_strategy()) {
            let (children_of, finalizers_of) = build_dag_indexes(&graph_snapshot(&graph));

            let mut expected_children: HashMap<String, Vec<String>> = HashMap::new();
            let mut expected_finalizers: HashMap<String, Vec<String>> = HashMap::new();
            for (i, (parent, is_final)) in graph.iter().enumerate() {
                let Some(p) = parent.filter(|&p| p < graph.len()) else {
                    continue; // root or orphan
                };
                if reference_on_cycle(&graph, i) || reference_on_cycle(&graph, p) {
                    continue;
                }
                let target = if *is_final { &mut expected_finalizers } else { &mut expected_children };
                target.entry(format!("t{p}")).or_default().push(format!("t{i}"));
            }
            for list in expected_children.values_mut().chain(expected_finalizers.values_mut()) {
                list.sort();
            }

            prop_assert_eq!(children_of, expected_children);
            prop_assert_eq!(finalizers_of, expected_finalizers);
        }
    }

    // --- SchedulerState vs. a naive model ---

    const IDS: [&str; 4] = ["a", "b", "c", "d"];

    /// The first two differ only in whitespace and must be treated as the same schedule.
    const SCHEDULES: [&str; 7] = [
        "CRON 0 * * * * * TZ=UTC",
        "CRON  0 * * * * *   TZ=UTC",
        "CRON */20 * * * * * TZ=UTC",
        "CRON 0 */2 * * * * TZ=Europe/Berlin",
        "CRON 30 * * * * *",
        "not a schedule",
        "",
    ];

    const LOCAL_TZ: LocalTimeZone = LocalTimeZone::Named(chrono_tz::UTC);

    #[derive(Debug, Clone)]
    enum Op {
        /// Replace the whole task table; `None` means the task id is absent.
        Reload(Vec<Option<TaskRow>>),
        Advance(i64),
    }

    fn row_strategy(task_id: &'static str) -> impl Strategy<Value = TaskRow> {
        let after = prop_oneof![
            4 => Just(None),
            2 => prop::sample::select(IDS.to_vec()).prop_map(|id| Some(id.to_string())),
            1 => Just(Some("missing".to_string())),
            1 => Just(Some(String::new())), // treated as no parent
        ];
        (
            prop::bool::weighted(0.8),
            prop::sample::select(SCHEDULES.to_vec()),
            prop::sample::select(vec!["SELECT 1", "SELECT 2"]),
            after,
            prop::bool::weighted(0.15),
            any::<bool>(),
        )
            .prop_map(
                move |(enabled, schedule, statement, after, is_final, parallel)| TaskRow {
                    task_id: task_id.to_string(),
                    enabled,
                    schedule: schedule.to_string(),
                    statement: statement.to_string(),
                    after,
                    is_final,
                    comment: None,
                    parallel_children: parallel,
                },
            )
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        let table = IDS
            .iter()
            .map(|id| prop::option::weighted(0.75, row_strategy(id)).boxed())
            .collect::<Vec<_>>();
        prop_oneof![
            1 => table.prop_map(Op::Reload),
            2 => (1i64..=150).prop_map(Op::Advance),
        ]
    }

    /// Naive model: per task, its row and the next due time if it is an active root.
    #[derive(Default)]
    struct Model {
        tasks: HashMap<String, (TaskRow, Option<DateTime<Utc>>)>,
    }

    fn model_next_due(row: &TaskRow, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let has_parent = row.after.as_deref().is_some_and(|a| !a.trim().is_empty());
        if !row.enabled || has_parent || row.is_final {
            return None;
        }
        ParsedSchedule::parse(&row.schedule)
            .ok()?
            .next_after_with_local(now, LOCAL_TZ)
    }

    /// The fields that decide when a root fires; any change restarts its schedule.
    fn schedule_key(row: &TaskRow) -> (bool, String, Option<String>, bool) {
        let after = row.after.clone().filter(|a| !a.trim().is_empty());
        (
            row.enabled,
            normalize_schedule_text(&row.schedule),
            after,
            row.is_final,
        )
    }

    impl Model {
        fn reload(&mut self, rows: &[TaskRow], now: DateTime<Utc>) {
            let mut next = HashMap::new();
            for row in rows {
                let next_due = match self.tasks.get(&row.task_id) {
                    Some((old, due)) if schedule_key(old) == schedule_key(row) => *due,
                    _ => model_next_due(row, now),
                };
                next.insert(row.task_id.clone(), (row.clone(), next_due));
            }
            self.tasks = next;
        }

        fn drain(&mut self, now: DateTime<Utc>) -> Vec<(String, DateTime<Utc>)> {
            let mut fired: Vec<(String, DateTime<Utc>)> = self
                .tasks
                .iter()
                .filter_map(|(id, (_, due))| due.filter(|d| *d <= now).map(|d| (id.clone(), d)))
                .collect();
            fired.sort_by(|x, y| x.1.cmp(&y.1).then_with(|| x.0.cmp(&y.0)));
            for (id, _) in &fired {
                let (row, due) = self.tasks.get_mut(id).expect("fired task exists");
                *due = model_next_due(row, now);
            }
            fired
        }

        fn next_due(&self) -> Option<DateTime<Utc>> {
            self.tasks.values().filter_map(|(_, due)| *due).min()
        }
    }

    fn drain_state(state: &mut SchedulerState, now: DateTime<Utc>) -> Vec<(String, DateTime<Utc>)> {
        std::iter::from_fn(|| state.pop_due_root(now, LOCAL_TZ))
            .map(|due| (due.task_id, due.scheduled_for))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn scheduler_state_fires_like_naive_model(
            ops in prop::collection::vec(op_strategy(), 1..40),
        ) {
            // Start just before the Europe/Berlin DST switch (2026-03-29 01:00 UTC).
            let mut now = Utc.with_ymd_and_hms(2026, 3, 29, 0, 58, 0).unwrap();
            let mut state = SchedulerState::default();
            let mut model = Model::default();

            for (step, op) in ops.iter().enumerate() {
                match op {
                    Op::Reload(table) => {
                        let rows: Vec<TaskRow> = table.iter().flatten().cloned().collect();
                        let snapshot = rows
                            .iter()
                            .cloned()
                            .map(TaskDef::from_row)
                            .map(|task| (task.task_id.clone(), task))
                            .collect();
                        state.apply_snapshot(snapshot, now, LOCAL_TZ);
                        model.reload(&rows, now);
                    }
                    Op::Advance(secs) => now += chrono::Duration::seconds(*secs),
                }

                prop_assert_eq!(drain_state(&mut state, now), model.drain(now), "step {}", step);
                prop_assert_eq!(state.next_due_utc(), model.next_due(), "step {}", step);

                // Exactly the active roots are tracked, and none is lost from the heap.
                // A root removed and re-added before time advances restarts at
                // generation 1 with the same due time, so its stale entry matches the
                // new one; that is harmless (the first pop bumps the generation and the
                // duplicate is then discarded), so require at least one live entry.
                let mut tracked: Vec<&String> = state.roots.keys().collect();
                let mut expected: Vec<&String> = model
                    .tasks
                    .iter()
                    .filter(|(_, (_, due))| due.is_some())
                    .map(|(id, _)| id)
                    .collect();
                tracked.sort();
                expected.sort();
                prop_assert_eq!(tracked, expected, "step {}", step);
                for (id, root) in &state.roots {
                    let live = state
                        .heap
                        .iter()
                        .filter(|item| {
                            &item.task_id == id
                                && item.generation == root.generation
                                && item.due_at == root.next_due
                        })
                        .count();
                    prop_assert!(live >= 1, "root {} has no live heap entry at step {}", id, step);
                }
            }
        }
    }
}
