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

## Building from source

Requires [Rust](https://rustup.rs) 1.85 or later.

```bash
git clone https://github.com/exasol-labs/exasol-scheduler.git
cd exasol-scheduler
cargo build --release
# Binary: target/release/exasol_scheduler
```

See [docs/operations.md](docs/operations.md) for coverage reports, CI setup, and Docker packaging.

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

The scheduler creates the default `SCHED` schema plus `SCHED_TASKS` and `SCHED_HISTORY` automatically on first startup — no DDL required. Once the binary is running, add tasks with plain SQL. If your deployment sets `EXA_SCHEMA`, replace `SCHED` in the examples with that schema.

**Running SQL against Exasol:** You'll need an Exasol-compatible client. Options: [ExaPlus](https://docs.exasol.com/) (Exasol's native client), [DBeaver](https://dbeaver.io/) (community edition with the Exasol JDBC driver), or [pyexasol](https://github.com/exasol/pyexasol) (Python). Exasol uses a WebSocket-based protocol — standard tools like `psql` or `curl` are not compatible.

```sql
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT")
VALUES (
    'hourly_cleanup',
    'CRON 0 0 * * * * TZ=UTC',
    'DELETE FROM MY_SCHEMA.STAGING WHERE created_at < ADD_DAYS(CURRENT_TIMESTAMP, -7)'
);
```
---

## Defining tasks

Tasks live in `SCHED_TASKS`. Each row is one executable unit.

Always double-quote scheduler column names in SQL. `SQL_TEXT` replaces the legacy `STATEMENT` column name, which conflicted with an Exasol reserved word.

| Column | Description |
|---|---|
| `TASK_ID` | Unique identifier. Child tasks refer to their parent by this name. |
| `ENABLED` | Set to `FALSE` to pause without deleting. Default `TRUE`. |
| `SCHEDULE` | When to run. See [Schedule syntax](#schedule-syntax) below. |
| `SQL_TEXT` | The SQL to execute — any valid Exasol SQL. |
| `AFTER` | Parent task's `TASK_ID`. `NULL` for independently scheduled root tasks. |
| `IS_FINAL` | When `TRUE`, this task always runs after its parent, even on failure. Default `FALSE`. |
| `PARALLEL_CHILDREN` | When `TRUE` (default), all direct children of this task run in parallel threads. Set to `FALSE` to run children sequentially in alphabetical `TASK_ID` order. |
| `COMMENT` | Free-text description. Ignored by the scheduler. |

### Root tasks

A root task has no `AFTER` value and fires on its own cron schedule.

```sql
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT")
VALUES ('load_sales', 'CRON 0 0 6 * * * TZ=Europe/Berlin', 'EXECUTE SCRIPT ETL.LOAD_SALES()');
```

### Child tasks

A child runs after its parent succeeds. Set `AFTER` to the parent's `TASK_ID`.

```sql
-- Step 1: root
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT")
VALUES ('extract', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.EXTRACT()');

-- Step 2: runs only when extract succeeds
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT", "AFTER")
VALUES ('transform', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.TRANSFORM()', 'extract');

-- Step 3: runs only when transform succeeds
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT", "AFTER")
VALUES ('load', 'CRON 0 0 2 * * * TZ=UTC', 'EXECUTE SCRIPT ETL.LOAD()', 'transform');
```

If `transform` fails, `load` is skipped and recorded as `SKIPPED` in the history table.

### Finalizer tasks

A finalizer has `IS_FINAL = TRUE` and always runs after its parent — even if the parent failed or was skipped. Useful for notifications and cleanup.

```sql
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID", "SCHEDULE", "SQL_TEXT", "AFTER", "IS_FINAL")
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

Day-of-week uses **standard cron numbering: 0=Sunday, 1=Monday, …, 6=Saturday** (7 is also accepted as a Sunday alias). Named days (`SUN`, `MON`, `TUE`, `WED`, `THU`, `FRI`, `SAT`) and ranges like `MON-FRI` are also accepted.

---

## Managing tasks

Because tasks are just table rows, all management is plain SQL:

```sql
-- Pause and resume
UPDATE SCHED.SCHED_TASKS SET "ENABLED" = FALSE WHERE "TASK_ID" = 'load_sales';
UPDATE SCHED.SCHED_TASKS SET "ENABLED" = TRUE  WHERE "TASK_ID" = 'load_sales';

-- Change schedule or SQL text
UPDATE SCHED.SCHED_TASKS SET "SCHEDULE"  = 'CRON 0 0 7 * * * TZ=UTC' WHERE "TASK_ID" = 'load_sales';
UPDATE SCHED.SCHED_TASKS SET "SQL_TEXT" = 'EXECUTE SCRIPT ETL.LOAD_SALES_V2()' WHERE "TASK_ID" = 'load_sales';

-- Remove
DELETE FROM SCHED.SCHED_TASKS WHERE "TASK_ID" = 'obsolete_job';
```

The scheduler picks up every change on its next poll — no restart required.

Hot reload does not make a sequence of separately committed statements atomic. If a
multi-row task graph is deployed with one commit per `INSERT`, the scheduler (and other
readers of `SCHED_TASKS`) can observe and reload an intermediate set of rows between
commits. Where the SQL client supports transactions, deploy all related task changes in
one transaction and commit once so the complete graph becomes visible together.

---

## Execution history

Every execution writes a row to `SCHED_HISTORY`. `STATUS` is `SUCCEEDED`, `FAILED`, or `SKIPPED`. All tasks in the same graph run share a `GRAPH_RUN_ID`.

```sql
-- All steps in the most recent run of a pipeline
SELECT "TASK_ID", "GRAPH_PHASE", "STATUS", "ERROR_MESSAGE", "STARTED_AT"
FROM SCHED.SCHED_HISTORY
WHERE "GRAPH_RUN_ID" = (
    SELECT "GRAPH_RUN_ID" FROM SCHED.SCHED_HISTORY
    WHERE "TASK_ID" = 'extract'
    ORDER BY "STARTED_AT" DESC LIMIT 1
)
ORDER BY "STARTED_AT";
```

---

## Graph execution rules

- **Root tasks trigger independently** on their cron schedule. Each trigger starts a new graph run with a shared `GRAPH_RUN_ID`.
- **Children execute in parallel by default.** All direct children of a parent task run concurrently in separate threads. Set `PARALLEL_CHILDREN = FALSE` on the parent to run its children sequentially in alphabetical `TASK_ID` order instead.
- **A failed or skipped parent** causes all its children to be skipped (recorded in history as `SKIPPED`).
- **Finalizers always run**, even if their parent failed. They run after all regular children complete.
- **Root failure is fatal** — the process supervisor should restart the binary. Child and finalizer failures are non-fatal: the scheduler logs a warning and continues.
- **Cycles and orphans are silently excluded** from execution. Tasks whose `AFTER` forms a loop, or points to a nonexistent `TASK_ID`, never execute.

> **Validating schedules:** There is no built-in dry-run command. To verify a schedule fires at the expected time, insert a test task with `ENABLED = TRUE`, observe the scheduler logs and `SCHED_HISTORY`, then delete it.

---

## Further reading

- [docs/configuration.md](docs/configuration.md) — full environment variable reference and DSN format
- [docs/security.md](docs/security.md) — trust model, least-privilege setup, credential management, hardening checklist
- [docs/operations.md](docs/operations.md) — building, systemd/Docker deployment, contract tests
- [docs/agent-skill.md](docs/agent-skill.md) — normative reference for autonomous agents managing scheduled pipelines
