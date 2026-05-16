use chrono::{DateTime, Utc};
use exasol_scheduler::db::{DbError, SchedulerDb};
use exasol_scheduler::model::{HistoryEvent, TaskRow};
use exasol_scheduler::time::{Clock, FakeClock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct DbVersion {
    pub last_changed: DateTime<Utc>,
    pub tasks: Vec<TaskRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRecord {
    pub statement: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ProgrammableDb {
    versions: Vec<DbVersion>,
    active_version: Mutex<usize>,
    clock: Arc<FakeClock>,
    failures_by_statement: Mutex<HashMap<String, String>>,
    executions: Mutex<Vec<ExecutionRecord>>,
    history_events: Mutex<Vec<HistoryEvent>>,
    write_history_error: Mutex<Option<String>>,
    get_last_changed_calls: AtomicUsize,
    load_tasks_calls: AtomicUsize,
    execute_calls: AtomicUsize,
    write_history_calls: AtomicUsize,
}

impl ProgrammableDb {
    pub fn new(versions: Vec<DbVersion>, clock: Arc<FakeClock>) -> Self {
        assert!(!versions.is_empty(), "at least one DB version is required");
        Self {
            versions,
            active_version: Mutex::new(0),
            clock,
            failures_by_statement: Mutex::new(HashMap::new()),
            executions: Mutex::new(Vec::new()),
            history_events: Mutex::new(Vec::new()),
            write_history_error: Mutex::new(None),
            get_last_changed_calls: AtomicUsize::new(0),
            load_tasks_calls: AtomicUsize::new(0),
            execute_calls: AtomicUsize::new(0),
            write_history_calls: AtomicUsize::new(0),
        }
    }

    pub fn set_version(&self, index: usize) {
        assert!(index < self.versions.len(), "version index out of bounds");
        *self.active_version.lock().expect("active_version poisoned") = index;
    }

    pub fn set_failure_for_statement(&self, statement: &str, message: &str) {
        self.failures_by_statement
            .lock()
            .expect("failures_by_statement poisoned")
            .insert(statement.to_string(), message.to_string());
    }

    pub fn set_failure_for_task_id(&self, task_id: &str, message: &str) {
        if let Some(statement) = self
            .active_version_data()
            .tasks
            .iter()
            .find(|task| task.task_id == task_id)
            .map(|task| task.statement.clone())
        {
            self.set_failure_for_statement(&statement, message);
        }
    }

    pub fn clear_failures(&self) {
        self.failures_by_statement
            .lock()
            .expect("failures_by_statement poisoned")
            .clear();
    }

    pub fn executions(&self) -> Vec<ExecutionRecord> {
        self.executions.lock().expect("executions poisoned").clone()
    }

    pub fn history_events(&self) -> Vec<HistoryEvent> {
        self.history_events.lock().expect("history_events poisoned").clone()
    }

    pub fn set_write_history_error(&self, message: &str) {
        *self.write_history_error.lock().expect("write_history_error poisoned") = Some(message.to_string());
    }

    pub fn clear_write_history_error(&self) {
        *self.write_history_error.lock().expect("write_history_error poisoned") = None;
    }

    pub fn get_last_changed_calls(&self) -> usize {
        self.get_last_changed_calls.load(Ordering::SeqCst)
    }

    pub fn load_tasks_calls(&self) -> usize {
        self.load_tasks_calls.load(Ordering::SeqCst)
    }

    pub fn execute_calls(&self) -> usize {
        self.execute_calls.load(Ordering::SeqCst)
    }

    pub fn write_history_calls(&self) -> usize {
        self.write_history_calls.load(Ordering::SeqCst)
    }

    fn active_version_data(&self) -> DbVersion {
        let index = *self.active_version.lock().expect("active_version poisoned");
        self.versions[index].clone()
    }
}

impl SchedulerDb for ProgrammableDb {
    fn get_last_changed(&self) -> Result<DateTime<Utc>, DbError> {
        self.get_last_changed_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.active_version_data().last_changed)
    }

    fn load_tasks(&self) -> Result<Vec<TaskRow>, DbError> {
        self.load_tasks_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.active_version_data().tasks)
    }

    fn execute_statement(&self, sql: &str) -> Result<(), DbError> {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        self.executions
            .lock()
            .expect("executions poisoned")
            .push(ExecutionRecord {
                statement: sql.to_string(),
                at: self.clock.now(),
            });

        if let Some(message) = self
            .failures_by_statement
            .lock()
            .expect("failures_by_statement poisoned")
            .get(sql)
            .cloned()
        {
            return Err(DbError::Other(message));
        }

        Ok(())
    }

    fn write_history(&self, event: &HistoryEvent) -> Result<(), DbError> {
        self.write_history_calls.fetch_add(1, Ordering::SeqCst);
        self.history_events
            .lock()
            .expect("history_events poisoned")
            .push(event.clone());
        if let Some(message) = self
            .write_history_error
            .lock()
            .expect("write_history_error poisoned")
            .clone()
        {
            return Err(DbError::Other(message));
        }
        Ok(())
    }
}

pub fn root_task(task_id: &str, schedule: &str, statement: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled: true,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: None,
        is_final: false,
        comment: None,
    }
}

pub fn disabled_task(task_id: &str, schedule: &str, statement: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled: false,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: None,
        is_final: false,
        comment: None,
    }
}

pub fn child_task(task_id: &str, parent: &str, schedule: &str, statement: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled: true,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: Some(parent.to_string()),
        is_final: false,
        comment: None,
    }
}

pub fn disabled_child_task(task_id: &str, parent: &str, schedule: &str, statement: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled: false,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: Some(parent.to_string()),
        is_final: false,
        comment: None,
    }
}

pub fn finalizer_task(task_id: &str, parent: &str, schedule: &str, statement: &str) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled: true,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: Some(parent.to_string()),
        is_final: true,
        comment: None,
    }
}
