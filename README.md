<div align="center">

# Exasol Scheduler

**Lightweight table-driven SQL job scheduling for Exasol**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)

*Define jobs in SQL. Run them with SQL. Audit them with SQL.*

</div>

---

## Why a table-driven scheduler?

Most schedulers store their task definitions in a proprietary database or configuration files that your SQL tooling cannot query. This creates a gap between "what data exists" and "what code runs against it."

With Exasol Scheduler, task definitions live in a standard Exasol table:

- **Full SQL access.** Query, join, and report on task definitions and execution history with the same tools you use for your data warehouse.
- **Instant hot-reload.** Add, remove, or change a task with a plain `UPDATE` or `INSERT`. The scheduler notices on its next poll — no restart required.
- **Dependency graphs.** Chain tasks via the `AFTER` column to express multi-step pipelines. The scheduler skips downstream steps on failure and always runs designated finalizers for cleanup and notifications.
- **Auditable history.** Every execution is written to a history table in the same database. No external log aggregator needed.
- **Zero infrastructure.** A single stateless binary. No message broker, no controller plane, no distributed state.

---

## How it works

1. The scheduler polls `SYS.EXA_ALL_OBJECTS` to detect when the task table last changed.
2. If it changed, reload all task rows and recompute the schedule.
3. For every task whose next scheduled time has passed, execute its SQL statement, walk its dependency graph, and write a result row to the history table.
4. Sleep until the next task is due (at most `POLL_INTERVAL_SECS` seconds).

---

## Quick Start

### 1. Start the scheduler

```bash
# Positional DSN argument:
exasol_scheduler "exasol://myuser:mypassword@exasol-host:8563?tls=1&validateservercertificate=0"

# Or via environment variables:
export EXA_HOST=exasol-host
export EXA_USER=myuser
export EXA_PASSWORD=mypassword
export EXA_TLS=true
exasol_scheduler
```

See [docs/configuration.md](docs/configuration.md) for the full list of environment variables.

### 2. Add your first task

The scheduler creates `SCHED_TASKS` and `SCHED_HISTORY` automatically on first startup — no DDL required. Once the binary is running, add tasks with plain SQL:

```sql
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT")
VALUES (
    'hourly_cleanup',
    'CRON 0 0 * * * * TZ=UTC',
    'DELETE FROM MY_SCHEMA.STAGING WHERE created_at < ADD_DAYS(CURRENT_TIMESTAMP, -7)'
);
```
---

## Defining tasks

Tasks live in `SCHED_TASKS`. Each row is one executable unit.

| Column | Description |
|---|---|
| `TASK_ID` | Unique identifier. Child tasks refer to their parent by this name. |
| `ENABLED` | Set to `FALSE` to pause without deleting. Default `TRUE`. |
| `SCHEDULE` | When to run. See [Schedule syntax](#schedule-syntax) below. |
| `STATEMENT` | The SQL to execute — any valid Exasol SQL. |
| `AFTER` | Parent task's `TASK_ID`. `NULL` for independently scheduled root tasks. |
| `IS_FINAL` | When `TRUE`, this task always runs after its parent, even on failure. Default `FALSE`. |
| `COMMENT` | Free-text description. Ignored by the scheduler. |

### Root tasks

A root task has no `AFTER` value and fires on its own cron schedule.

```sql
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT")
VALUES ('load_sales', 'CRON 0 0 6 * * * TZ=Europe/Berlin', 'EXECUTE SCRIPT ETL.LOAD_SALES()');
```

### Child tasks

A child runs after its parent succeeds. Set `AFTER` to the parent's `TASK_ID`.

```sql
-- Step 1: root
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT")
VALUES ('extract', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.EXTRACT()');

-- Step 2: runs only when extract succeeds
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT", "AFTER")
VALUES ('transform', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.TRANSFORM()', 'extract');

-- Step 3: runs only when transform succeeds
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT", "AFTER")
VALUES ('load', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.LOAD()', 'transform');
```

If `transform` fails, `load` is skipped and recorded as `SKIPPED` in the history table.

### Finalizer tasks

A finalizer has `IS_FINAL = TRUE` and always runs after its parent — even if the parent failed or was skipped. Useful for notifications and cleanup.

```sql
INSERT INTO PUBLIC.SCHED_TASKS ("TASK_ID", "SCHEDULE", "STATEMENT", "AFTER", "IS_FINAL")
VALUES ('notify', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.SEND_STATUS()', 'extract', TRUE);
```

A parent can have multiple children and multiple finalizers. Children run first (alphabetical by `TASK_ID`), then finalizers run (also alphabetical). A failing finalizer does not stop its siblings.

---

## Schedule syntax

```
CRON <second> <minute> <hour> <day-of-month> <month> <day-of-week> [TZ=<timezone>]
```

Fields use standard cron syntax. The `TZ=` suffix accepts any [IANA timezone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones); when omitted, the server's local timezone is used.

| Schedule | Fires |
|---|---|
| `CRON 0 0 * * * * TZ=UTC` | Every hour on the hour (UTC) |
| `CRON 0 0 6 * * * TZ=Europe/Berlin` | Daily at 06:00 Berlin time |
| `CRON 0 0 9 * * 1-5 TZ=America/New_York` | Weekdays at 09:00 New York time |
| `CRON 0 */15 * * * *` | Every 15 minutes (server local time) |
| `CRON 0 30 23 L * * TZ=UTC` | Last day of each month at 23:30 UTC |

---

## Managing tasks

Because tasks are just table rows, all management is plain SQL:

```sql
-- Pause and resume
UPDATE PUBLIC.SCHED_TASKS SET "ENABLED" = FALSE WHERE "TASK_ID" = 'load_sales';
UPDATE PUBLIC.SCHED_TASKS SET "ENABLED" = TRUE  WHERE "TASK_ID" = 'load_sales';

-- Change schedule or statement
UPDATE PUBLIC.SCHED_TASKS SET "SCHEDULE"  = 'CRON 0 0 7 * * * TZ=UTC' WHERE "TASK_ID" = 'load_sales';
UPDATE PUBLIC.SCHED_TASKS SET "STATEMENT" = 'EXECUTE SCRIPT ETL.LOAD_SALES_V2()' WHERE "TASK_ID" = 'load_sales';

-- Remove
DELETE FROM PUBLIC.SCHED_TASKS WHERE "TASK_ID" = 'obsolete_job';
```

The scheduler picks up every change on its next poll — no restart required.

---

## Execution history

Every execution writes a row to `SCHED_HISTORY`. `STATUS` is `SUCCEEDED`, `FAILED`, or `SKIPPED`. All tasks in the same graph run share a `GRAPH_RUN_ID`.

```sql
-- All steps in the most recent run of a pipeline
SELECT TASK_ID, GRAPH_PHASE, STATUS, ERROR_MESSAGE, STARTED_AT
FROM PUBLIC.SCHED_HISTORY
WHERE GRAPH_RUN_ID = (
    SELECT GRAPH_RUN_ID FROM PUBLIC.SCHED_HISTORY
    WHERE TASK_ID = 'extract'
    ORDER BY STARTED_AT DESC LIMIT 1
)
ORDER BY STARTED_AT;
```

---

## Graph execution rules

- **Root tasks trigger independently** on their cron schedule. Each trigger starts a new graph run with a shared `GRAPH_RUN_ID`.
- **Children run depth-first**, in alphabetical `TASK_ID` order. A failed or skipped parent causes its children to be skipped (and recorded in history as `SKIPPED`).
- **Finalizers always run**, even if their parent failed. They run after all regular children complete.
- **Root failure is fatal** — the process supervisor should restart the binary. Child and finalizer failures are non-fatal: the scheduler logs a warning and continues.
- **Cycles and orphans are silently excluded** from execution. Tasks whose `AFTER` forms a loop, or points to a nonexistent `TASK_ID`, never execute.

---

## Further reading

- [docs/configuration.md](docs/configuration.md) — full environment variable reference and DSN format
- [docs/operations.md](docs/operations.md) — building, systemd/Docker deployment, security, contract tests
