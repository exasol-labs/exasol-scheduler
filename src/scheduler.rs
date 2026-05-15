use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
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
        let executed_roots = self.execute_due_roots(now)?;

        Ok(TickResult {
            executed_roots,
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

    fn execute_due_roots(&mut self, now: DateTime<Utc>) -> Result<usize, SchedulerError> {
        let mut executed = 0usize;

        while let Some(due) = self.state.pop_due_root(now, self.local_tz) {
            tracing::info!(task_id = due.task_id.as_str(), scheduled_for = %due.scheduled_for, "executing root task");

            let run_id = Uuid::new_v4();
            let started_at = self.clock.now();
            let exec_result = self.db.execute_statement(&due.statement);
            let finished_at = self.clock.now();

            let (status, error_message) = match &exec_result {
                Ok(_) => ("SUCCEEDED".to_string(), None),
                Err(e) => ("FAILED".to_string(), Some(e.to_string())),
            };

            let event = HistoryEvent {
                run_id,
                graph_run_id: None,
                task_id: due.task_id.clone(),
                graph_phase: "MAIN".to_string(),
                scheduled_for: Some(due.scheduled_for),
                started_at,
                finished_at: Some(finished_at),
                status,
                error_message,
            };
            if let Err(e) = self.db.write_history(&event) {
                tracing::warn!(task_id = due.task_id.as_str(), error = %e, "write_history failed");
            }

            exec_result?;
            executed += 1;
        }

        Ok(executed)
    }
}

#[derive(Default)]
struct SchedulerState {
    snapshot: HashMap<String, TaskDef>,
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
            // Invalid or non-computable schedule is treated as inactive for Stage-1.
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
struct TaskDef {
    task_id: String,
    enabled: bool,
    parsed_schedule: Option<ParsedSchedule>,
    statement: String,
    after: Option<String>,
    is_final: bool,
    schedule_fingerprint: u64,
    fingerprint: u64,
}

impl TaskDef {
    fn from_row(row: TaskRow) -> Self {
        let after = row.after.and_then(|value| {
            if value.trim().is_empty() {
                None
            } else {
                Some(value)
            }
        });
        let is_root = after.is_none() && !row.is_final;
        let parsed_schedule = if row.enabled && is_root {
            ParsedSchedule::parse(&row.schedule).ok()
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
        let fingerprint = full_fingerprint(schedule_fingerprint, statement_hash);

        Self {
            task_id: row.task_id,
            enabled: row.enabled,
            parsed_schedule,
            statement: row.statement,
            after,
            is_final: row.is_final,
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

fn full_fingerprint(schedule_fingerprint: u64, statement_hash: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schedule_fingerprint.hash(&mut hasher);
    statement_hash.hash(&mut hasher);
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
