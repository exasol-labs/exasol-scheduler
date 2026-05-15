# Exasol Lightweight Task Scheduler

A lightweight, database-native scheduler for Exasol, implemented in Rust.

Design goals:
- stateless operation
- deterministic behavior
- mockable architecture
- incremental reload (diff, not full rebuild)

## Current Status

The project implements **Stage-1 + Stage-1b**:
- Stage-1 scheduler core:
  - root-only CRON scheduling
  - incremental snapshot diff
  - generation-token heap invalidation
  - no catch-up on restart
- Stage-1b Exasol support:
  - real `ExasolDb` adapter using `exarrow-rs`
  - runnable binary (`src/main.rs`) with polling loop
  - env-driven configuration

Not implemented yet:
- Stage-2 history writes (`SCHED_HISTORY`)
- Stage-3 DAG traversal (`AFTER`) and finalizer execution (`IS_FINAL`)
- schedule kinds beyond `CRON`
- security - tasks are not tied to user roles

## Audience Guide

- If you are a **database user**, see `Database User Guide`.
- If you are an **operator/admin**, see `Operator Guide`.

## Database User Guide

### Table You Maintain

The scheduler reads task definitions from `SCHED_TASKS`.

```sql
CREATE TABLE SCHED_TASKS (
  TASK_ID        VARCHAR(128) NOT NULL,
  ENABLED        BOOLEAN DEFAULT TRUE,
  SCHEDULE       VARCHAR(512) NOT NULL,
  STATEMENT      VARCHAR(2000000) NOT NULL,
  AFTER          VARCHAR(128),
  IS_FINAL       BOOLEAN DEFAULT FALSE,
  COMMENT        VARCHAR(2000),
  PRIMARY KEY (TASK_ID)
);
```

### Stage-1 Execution Semantics

A row is runnable in Stage-1 only if:
- `ENABLED = TRUE`
- `AFTER IS NULL`
- `IS_FINAL = FALSE`
- `SCHEDULE` parses as supported CRON

Rows that are disabled, child tasks, finalizers, or invalid schedules are not executed.

### Supported Schedule Grammar

```text
CRON <sec> <min> <hour> <dom> <mon> <dow> [TZ=<timezone>]
```

Rules:
- `CRON` prefix is required
- exactly 6 cron fields
- `TZ=UTC` or IANA timezone (for example `Europe/Copenhagen`)
- if `TZ` is omitted, scheduler uses configured local timezone behavior

Examples:

```text
CRON 0 * * * * *
CRON 0 */5 * * * * TZ=UTC
CRON 0 0 9 * * * TZ=Europe/Copenhagen
```

### Typical SQL Operations

Create a root task:

```sql
INSERT INTO SCHED_TASKS (TASK_ID, ENABLED, SCHEDULE, STATEMENT)
VALUES (
  'refresh_daily_sales',
  TRUE,
  'CRON 0 0 2 * * * TZ=UTC',
  'CALL ETL.REFRESH_DAILY_SALES()'
);
```

Disable a task:

```sql
UPDATE SCHED_TASKS
SET ENABLED = FALSE
WHERE TASK_ID = 'refresh_daily_sales';
```

Change schedule:

```sql
UPDATE SCHED_TASKS
SET SCHEDULE = 'CRON 0 30 2 * * * TZ=UTC'
WHERE TASK_ID = 'refresh_daily_sales';
```

Change statement only:

```sql
UPDATE SCHED_TASKS
SET STATEMENT = 'CALL ETL.REFRESH_DAILY_SALES_V2()'
WHERE TASK_ID = 'refresh_daily_sales';
```

Delete task:

```sql
DELETE FROM SCHED_TASKS
WHERE TASK_ID = 'refresh_daily_sales';
```

### User-Facing Behavior Notes

- `COMMENT` changes are cosmetic and do not change execution behavior.
- Invalid schedules are skipped.
- Restart does not replay missed executions; next run is computed from current time.

## Operator Guide

### Architecture

- single scheduler service (no leader election)
- stateless process
- Exasol stores durable configuration
- scheduler core depends only on `SchedulerDb` trait

Core files:
- `src/scheduler.rs`: scheduler state, diff, heap scheduling, execution
- `src/schedule.rs`: CRON + TZ parsing
- `src/db/mod.rs`: DB trait + errors
- `src/db/exasol.rs`: production Exasol adapter (exarrow-rs)
- `src/main.rs`: service bootstrap and run loop

### Runtime Flow

The binary loop does:
1. poll task-table last-changed timestamp (via `EXA_ALL_OBJECTS`)
2. reload + diff if changed
3. execute due root tasks
4. sleep until `min(next_due, poll_interval)`

### Environment Configuration

Connection and table settings:
- `EXA_DSN`
- `EXA_HOST` (required if `EXA_DSN` is not set)
- `EXA_PORT` (default `8563`)
- `EXA_USER` (required if `EXA_DSN` is not set)
- `EXA_PASSWORD` (required if `EXA_DSN` is not set)
- `EXA_TLS` (default `false`)
- `EXA_VALIDATE_SERVER_CERT` (default `true`)
- `EXA_QUERY_TIMEOUT_SECS` (optional)

Scheduler settings:
- `EXA_SCHEMA` (default `PUBLIC`)
- `EXA_TASKS_TABLE` (default `SCHED_TASKS`)
- `POLL_INTERVAL_SECS` (default `10`)

CLI override:
- positional argument `exasol://...` (exarrow-rs DSN format)

Precedence:
1. positional DSN argument (`cargo run -- exasol://...`)
2. `EXA_DSN`
3. `EXA_HOST` + `EXA_PORT` + `EXA_USER` + `EXA_PASSWORD`

If `EXA_SCHEMA` is not set and the DSN path includes a schema (`.../MY_SCHEMA`), that schema is used automatically.

### Running Locally

Build:

```bash
cargo build
```

Run tests (no Exasol required):

```bash
cargo test
```

Measure test coverage:

```bash
# one-time install
cargo install cargo-llvm-cov

# terminal coverage summary
cargo coverage

# HTML report in target/llvm-cov/html/index.html
cargo coverage-html

# LCOV output for CI tooling
cargo coverage-lcov
```

Run scheduler:

```bash
export EXA_HOST=localhost
export EXA_PORT=8563
export EXA_USER=sys
export EXA_PASSWORD=exasol
export EXA_SCHEMA=PUBLIC
export EXA_TASKS_TABLE=SCHED_TASKS
export POLL_INTERVAL_SECS=10
export RUST_LOG=info
cargo run
```

Or with DSN from env:

```bash
export EXA_DSN='exasol://sys:exasol@localhost:8563?tls=0&validateservercertificate=0'
export EXA_SCHEMA=PUBLIC
export EXA_TASKS_TABLE=SCHED_TASKS
cargo run
```

Or without any credential env vars (single URL argument):

```bash
cargo run -- 'exasol://sys:exasol@localhost:8563/PUBLIC?tls=0&validateservercertificate=0'
```

### Exasol Contract Test (Optional)

A smoke contract test exists in `tests/exasol_contract.rs`.

It is skipped unless enabled:

```bash
export EXA_CONTRACT_TESTS=1
# plus normal EXA_* variables
cargo test exasol_contract -- --nocapture
```

This keeps CI and local default tests independent of a running Exasol instance.

### Error Behavior

`DbError` carries context for:
- connection failures
- query failures (includes SQL text)
- Arrow decoding failures
- not-found metadata rows
- config/runtime init issues

Execution behavior (Stage-1):
- task SQL is executed exactly as stored in `STATEMENT`
- scheduler does not split semicolon-separated SQL client-side
- if execution fails, the tick returns error and should be handled by process supervision

### Operations Runbook

Add job:
1. Insert row into `SCHED_TASKS`.
2. Validate schedule syntax/timezone.
3. Verify reload + execution logs.

Pause job:
1. Set `ENABLED=FALSE`.
2. Verify reload reflects changed row.

Change schedule:
1. Update `SCHEDULE`.
2. Verify next due time updates.

Restart service:
1. Restart process.
2. Expect no backfill of missed windows.
3. Verify next scheduled run only.

### Security Notes

Use a dedicated scheduler DB user with least privilege:
- read metadata and task table
- execute task SQL
- future stages will need write access to `SCHED_HISTORY`

Do not share admin credentials with scheduler service credentials.

## Development Notes

- Rust stable
- production DB library: `exarrow-rs`
- scheduler core remains testable via trait abstractions
- all default tests run without Exasol

## Stage Roadmap

- Stage-2: execution history (`SCHED_HISTORY`)
- Stage-3: DAG traversal + finalizers
- Stage-4: schedule grammar extensions

## Repository Layout

- `src/main.rs`: runnable service entrypoint
- `src/config.rs`: env-based runtime configuration
- `src/db/mod.rs`: `SchedulerDb` trait and `DbError`
- `src/db/exasol.rs`: Exasol adapter implementation
- `src/scheduler.rs`: scheduler engine
- `src/schedule.rs`: schedule parser
- `src/model.rs`: shared row/event models
- `src/time.rs`: clock abstraction
- `tests/`: unit and integration tests (plus optional Exasol contract test)
