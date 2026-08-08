# Exasol Scheduler — Agent Skill

Reference for autonomous agents managing scheduled SQL pipelines. Read this document
completely before issuing any SQL. Every rule below is load-bearing.

---

## What the scheduler does

The scheduler polls `SCHED_TASKS` for due tasks and executes their `SQL_TEXT` column
verbatim against Exasol. It is the only component that writes to `SCHED_HISTORY`. Your
job as an agent is to manage `SCHED_TASKS` rows via SQL and read `SCHED_HISTORY` for
status. You never interact with the scheduler process directly.

---

## Execution model — normative rules

Understand these before writing any SQL.

### Graph runs

Every root task trigger creates a **graph run** identified by `GRAPH_RUN_ID` (UUID).
All history rows produced by that trigger — root, children, and finalizers — share the
same `GRAPH_RUN_ID`. This is your primary key for correlating a pipeline run.

### Task roles

| Condition | Role | Fires when |
|---|---|---|
| `AFTER IS NULL` and `IS_FINAL = FALSE` | **Root** | Own `SCHEDULE` fires |
| `AFTER IS NOT NULL` and `IS_FINAL = FALSE` | **Child** | Parent succeeds |
| `IS_FINAL = TRUE` | **Finalizer** | After parent and all its children complete, regardless of outcome |

### Ordering

Children of the same parent execute **in parallel by default** (the `PARALLEL_CHILDREN`
column on the parent defaults to `TRUE`). All direct children are launched in separate
threads and may complete in any order.

Set `PARALLEL_CHILDREN = FALSE` on the parent to run its children **sequentially in
alphabetical `TASK_ID` order**. Use this when child tasks have ordering dependencies or
when you need deterministic `STARTED_AT` sequencing in history.

Finalizers execute after all regular children finish, sequentially in alphabetical order.
A failing finalizer does not stop its siblings.

> **History ordering note:** In parallel mode, sibling `STARTED_AT` timestamps are
> non-deterministic. Do not rely on alphabetical `TASK_ID` order to reconstruct
> execution sequence — use `STARTED_AT` and `FINISHED_AT` instead.

### SCHEDULE field for non-root tasks

`SCHEDULE` is `NOT NULL` in the schema. **For children and finalizers, any value
satisfies the constraint but the value is completely ignored at runtime** — the scheduler
never parses the schedule of a non-root task. Convention: use the parent's schedule
string or any syntactically valid cron expression.

### Failure model — the most important section

| Task type | Failure effect |
|---|---|
| **Root fails** | **Scheduler process exits immediately.** All other root tasks due in the same tick are dropped. Process supervisor (systemd, Docker, etc.) must restart the binary. |
| **Child fails** | Recorded as `FAILED`. Its own children are `SKIPPED`. Its siblings continue. Finalizers still run. Non-fatal for the scheduler process. |
| **Finalizer fails** | Recorded as `FAILED`. Sibling finalizers continue. Non-fatal. |

### ENABLED flag

- **Root with `ENABLED = FALSE`**: never placed on the schedule heap; never fires.
- **Child or finalizer with `ENABLED = FALSE`**: executes as `SKIPPED` and writes a history row with `ERROR_MESSAGE = 'task is disabled'`. Does not propagate the skip to its own children.

### Orphan and cycle exclusion

- **Orphan**: `AFTER` points to a `TASK_ID` that does not exist in `SCHED_TASKS`. The orphan is silently excluded — it never runs, is never recorded as SKIPPED.
- **Cycle**: any set of tasks forming a cycle via `AFTER` is silently excluded from the DAG. None of the cycle members execute.

Both exclusions are re-evaluated on every snapshot reload. Fixing the bad reference
causes the task to become active on the next reload.

### Hot reload

The scheduler detects `SCHED_TASKS` changes within `POLL_INTERVAL_SECS` (default: 10 s)
by polling `SYS.EXA_ALL_OBJECTS`. **No scheduler restart is needed after any SQL change
to `SCHED_TASKS`.** Changes propagate automatically.

"No restart required" does not mean that separately committed changes to multiple rows
are reloaded atomically. Each poll reloads the committed table state visible at that
time, so a deployment made as several independent commits can be split across multiple
polls and expose a temporarily incomplete task graph. Where the SQL client supports
transactions, issue all related `INSERT`, `UPDATE`, and `DELETE` statements in one
transaction and commit once.

### Missed executions

When the scheduler restarts, it computes the next fire time from the current wall clock.
Executions missed during downtime are **never replayed**.

### Maximum depth

The scheduler enforces a maximum DAG depth of 20. Tasks at depth > 20 are silently
skipped with a warning log.

---

## SQL conventions — mandatory

The default scheduler schema is `SCHED`. All templates below use `SCHED.SCHED_TASKS`
and `SCHED.SCHED_HISTORY`. If the deployment sets `EXA_SCHEMA` to another value,
replace `SCHED` with that configured schema in every SQL statement.

The scheduler creates the configured schema, task table, and history table
automatically on startup. As an agent managing pipelines, do not issue bootstrap DDL
unless the user explicitly asks you to repair or provision scheduler storage.

**All column names must be double-quoted.** `SQL_TEXT` is the current SQL body column.
Do not use the legacy `STATEMENT` column name in new SQL. The following scheduler
columns include reserved Exasol keywords and will cause a syntax error if unquoted:

`AFTER`, `SCHEDULE`, `COMMENT`, `IS_FINAL`, `STATUS`, `ENABLED`,
`STARTED_AT`, `FINISHED_AT`, `GRAPH_PHASE`, `SCHEDULED_FOR`, `ERROR_MESSAGE`

**Always use double-quoted identifiers in every query.**

---

## Task management SQL

Use these templates. Replace values in `ALL_CAPS`. Do not change the quoting.

### Insert a root task

```sql
INSERT INTO SCHED.SCHED_TASKS (
    "TASK_ID", "ENABLED", "SCHEDULE", "SQL_TEXT", "COMMENT"
)
VALUES (
    'TASK_ID',
    TRUE,
    'CRON 0 0 6 * * * TZ=UTC',
    'EXECUTE SCRIPT MY_SCHEMA.MY_PROC()',
    'Optional description'
);
```

### Insert a child task

```sql
INSERT INTO SCHED.SCHED_TASKS (
    "TASK_ID", "ENABLED", "SCHEDULE", "SQL_TEXT", "AFTER", "COMMENT"
)
VALUES (
    'CHILD_ID',
    TRUE,
    'CRON 0 0 6 * * * TZ=UTC',   -- required but ignored; use parent schedule by convention
    'EXECUTE SCRIPT MY_SCHEMA.CHILD_PROC()',
    'PARENT_TASK_ID',
    'Optional description'
);
```

### Insert a finalizer

```sql
INSERT INTO SCHED.SCHED_TASKS (
    "TASK_ID", "ENABLED", "SCHEDULE", "SQL_TEXT", "AFTER", "IS_FINAL", "COMMENT"
)
VALUES (
    'FINAL_ID',
    TRUE,
    'CRON 0 0 6 * * * TZ=UTC',   -- required but ignored
    'EXECUTE SCRIPT MY_SCHEMA.CLEANUP()',
    'PARENT_TASK_ID',
    TRUE,
    'Always runs after PARENT_TASK_ID'
);
```

### Enable / disable a task

```sql
UPDATE SCHED.SCHED_TASKS SET "ENABLED" = FALSE WHERE "TASK_ID" = 'TASK_ID';
UPDATE SCHED.SCHED_TASKS SET "ENABLED" = TRUE  WHERE "TASK_ID" = 'TASK_ID';
```

### Update schedule or SQL text

```sql
UPDATE SCHED.SCHED_TASKS SET "SCHEDULE"  = 'CRON 0 0 8 * * * TZ=UTC'
WHERE "TASK_ID" = 'TASK_ID';

UPDATE SCHED.SCHED_TASKS SET "SQL_TEXT" = 'EXECUTE SCRIPT MY_SCHEMA.NEW_PROC()'
WHERE "TASK_ID" = 'TASK_ID';
```

### Delete a task

Deleting a parent does not cascade. Children become orphans and are silently excluded
from execution on the next snapshot reload.

```sql
DELETE FROM SCHED.SCHED_TASKS WHERE "TASK_ID" = 'TASK_ID';
```

### Delete a pipeline (root + all descendants)

Collect the full set of `TASK_ID` values first via the inspection queries below, then:

```sql
DELETE FROM SCHED.SCHED_TASKS
WHERE "TASK_ID" IN ('root', 'child_a', 'child_b', 'finalizer');
```

---

## Schedule syntax

```
CRON <sec> <min> <hour> <day-of-month> <month> <day-of-week> [TZ=<iana-tz>]
```

All six positional fields are required. The `TZ=` suffix is optional; when omitted the
scheduler's local timezone is used. Always specify `TZ=` explicitly.

### Day-of-week numbering (standard cron)

| Number | Day |
|---|---|
| 0 or 7 | Sunday |
| 1 | Monday |
| 2 | Tuesday |
| 3 | Wednesday |
| 4 | Thursday |
| 5 | Friday |
| 6 | Saturday |

Named days (`SUN`, `MON`, `TUE`, `WED`, `THU`, `FRI`, `SAT`) and ranges (`MON-FRI`) are
also accepted and are unambiguous.

### Examples

| Schedule | Fires |
|---|---|
| `CRON 0 0 * * * * TZ=UTC` | Every hour on the hour (UTC) |
| `CRON 0 0 6 * * * TZ=Europe/Berlin` | Daily at 06:00 Berlin time |
| `CRON 0 0 9 * * 1-5 TZ=America/New_York` | Weekdays at 09:00 New York time |
| `CRON 0 30 8 * * MON-FRI TZ=UTC` | Weekdays at 08:30 UTC (named days) |
| `CRON 0 */15 * * * * TZ=UTC` | Every 15 minutes |
| `CRON 0 0 2 1 * * TZ=UTC` | First day of each month at 02:00 UTC |
| `CRON 0 0 0 * * 0 TZ=UTC` | Every Sunday at midnight UTC |

### Validation

There is no dry-run command. To verify a schedule before committing to production:
1. Insert a test task with `ENABLED = TRUE` and a cron expression that fires shortly.
2. Observe `SCHED_HISTORY` for a row with `STATUS = 'SUCCEEDED'`.
3. Delete the test task.

An unparseable schedule emits a `WARN` log and the task silently never fires. No history
row is written for a task that failed to schedule.

---

## Monitoring queries

### Check scheduler liveness

The scheduler writes to `SCHED_HISTORY` on every successful execution. If no rows appear
within `2 × POLL_INTERVAL_SECS` of an expected fire time, the process may have crashed.

```sql
SELECT MAX("STARTED_AT") AS "LAST_ACTIVITY"
FROM SCHED.SCHED_HISTORY;
```

### Get the latest run of a pipeline

```sql
SELECT "TASK_ID", "GRAPH_PHASE", "STATUS", "ERROR_MESSAGE",
       "SCHEDULED_FOR", "STARTED_AT", "FINISHED_AT"
FROM SCHED.SCHED_HISTORY
WHERE "GRAPH_RUN_ID" = (
    SELECT "GRAPH_RUN_ID" FROM SCHED.SCHED_HISTORY
    WHERE "TASK_ID" = 'ROOT_TASK_ID'
    ORDER BY "STARTED_AT" DESC LIMIT 1
)
ORDER BY "STARTED_AT";
```

### Check for recent failures (last hour)

```sql
SELECT "TASK_ID", "STATUS", "ERROR_MESSAGE", "STARTED_AT"
FROM SCHED.SCHED_HISTORY
WHERE "STATUS" = 'FAILED'
  AND "STARTED_AT" > ADD_SECONDS(CURRENT_TIMESTAMP, -3600)
ORDER BY "STARTED_AT" DESC;
```

### Confirm a task succeeded since a given time

```sql
SELECT COUNT(*) AS "SUCCESS_COUNT"
FROM SCHED.SCHED_HISTORY
WHERE "TASK_ID"  = 'TASK_ID'
  AND "STATUS"   = 'SUCCEEDED'
  AND "STARTED_AT" > TIMESTAMP '2026-01-01 00:00:00';
```

### Inspect current task configuration

```sql
-- All root tasks
SELECT "TASK_ID", "ENABLED", "SCHEDULE"
FROM SCHED.SCHED_TASKS
WHERE "AFTER" IS NULL AND "IS_FINAL" = FALSE
ORDER BY "TASK_ID";

-- Full pipeline tree
SELECT "TASK_ID", "ENABLED", "AFTER", "IS_FINAL", "SCHEDULE"
FROM SCHED.SCHED_TASKS
ORDER BY "AFTER" NULLS FIRST, "IS_FINAL", "TASK_ID";

-- Single task
SELECT "TASK_ID", "ENABLED", "SCHEDULE", "SQL_TEXT", "AFTER", "IS_FINAL", "PARALLEL_CHILDREN", "COMMENT"
FROM SCHED.SCHED_TASKS
WHERE "TASK_ID" = 'TASK_ID';
```

### History status values

| `STATUS` | `GRAPH_PHASE` | Meaning |
|---|---|---|
| `SUCCEEDED` | `MAIN` or `FINAL` | Statement executed without error |
| `FAILED` | `MAIN` or `FINAL` | Statement returned an error; `ERROR_MESSAGE` is populated |
| `SKIPPED` | `MAIN` | Parent did not succeed, or task has `ENABLED = FALSE` |

---

## Error response protocol

### Scenario: root task fails

**Signal**: `STATUS = 'FAILED'` for a root task in `SCHED_HISTORY`, and no subsequent
activity in `SCHED_HISTORY` (scheduler process has exited).

**Protocol**:
1. Read `ERROR_MESSAGE` from the failed row.
2. Decide whether to fix the underlying issue immediately or disable the task.
3. Disable the failing task to prevent crash-loop on restart:
   ```sql
   UPDATE SCHED.SCHED_TASKS SET "ENABLED" = FALSE WHERE "TASK_ID" = 'FAILING_ROOT';
   ```
4. Signal the process supervisor to restart the scheduler binary.
5. Fix the root cause (update `SQL_TEXT`, grant missing privileges, etc.).
6. Re-enable the task:
   ```sql
   UPDATE SCHED.SCHED_TASKS SET "ENABLED" = TRUE WHERE "TASK_ID" = 'FAILING_ROOT';
   ```

### Scenario: child task fails repeatedly

**Signal**: `STATUS = 'FAILED'` for a non-root task; root continues firing.

**Protocol**:
1. Read `ERROR_MESSAGE`.
2. Optionally disable to stop downstream SKIPPED noise:
   ```sql
   UPDATE SCHED.SCHED_TASKS SET "ENABLED" = FALSE WHERE "TASK_ID" = 'FAILING_CHILD';
   ```
3. Fix the root cause.
4. Re-enable.

### Scenario: task never fires

**Possible causes** (check in order):
1. `ENABLED = FALSE` — check with the single-task inspection query above.
2. Unparseable schedule — check scheduler logs for `WARN task has unparseable schedule`.
3. Orphan — `AFTER` points to a `TASK_ID` that does not exist; confirm parent exists.
4. Cycle — task participates in an `AFTER` cycle; check the full pipeline tree query.
5. Schedule fires less frequently than expected — verify DOW numbering, TZ, and field positions.

### Scenario: task executes twice per trigger

**Cause**: two scheduler instances are running against the same `SCHED_TASKS` table.
The scheduler has no distributed lock. Each instance independently fires every due root.

**Fix**: ensure exactly one instance is running. Check with your process supervisor.

---

## Common agent patterns

### Add a 3-step pipeline with a cleanup finalizer

```sql
-- Root
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","COMMENT")
VALUES ('etl_extract','TRUE','CRON 0 0 2 * * * TZ=UTC','EXECUTE SCRIPT ETL.EXTRACT()','Step 1');

-- Child (fires when extract succeeds)
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","AFTER","COMMENT")
VALUES ('etl_transform',TRUE,'CRON 0 0 2 * * * TZ=UTC','EXECUTE SCRIPT ETL.TRANSFORM()','etl_extract','Step 2');

-- Grandchild (fires when transform succeeds)
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","AFTER","COMMENT")
VALUES ('etl_load',TRUE,'CRON 0 0 2 * * * TZ=UTC','EXECUTE SCRIPT ETL.LOAD()','etl_transform','Step 3');

-- Finalizer on root (always runs, regardless of pipeline outcome)
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","AFTER","IS_FINAL","COMMENT")
VALUES ('etl_notify',TRUE,'CRON 0 0 2 * * * TZ=UTC','EXECUTE SCRIPT ETL.SEND_STATUS()','etl_extract',TRUE,'Notify on complete or failure');
```

### Pause an entire pipeline without deleting it

```sql
UPDATE SCHED.SCHED_TASKS SET "ENABLED" = FALSE
WHERE "TASK_ID" IN ('etl_extract','etl_transform','etl_load','etl_notify');
```

Disabling only the root is sufficient to stop the pipeline from firing. Children will
never be triggered. Disabling children individually records `SKIPPED` entries when the
root does fire.

### Change a pipeline's schedule

Only the root task's `SCHEDULE` matters. Update only the root:

```sql
UPDATE SCHED.SCHED_TASKS SET "SCHEDULE" = 'CRON 0 0 4 * * * TZ=UTC'
WHERE "TASK_ID" = 'etl_extract';
```

The change is picked up on the next poll. No restart required.

### Express fan-out (parallel branches under one root)

`PARALLEL_CHILDREN = TRUE` (the default) runs all children of a parent concurrently.
Model parallel branches as siblings under a shared root:

```sql
-- Root: triggers the fan-out
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT")
VALUES ('pipeline_root',TRUE,'CRON 0 0 3 * * * TZ=UTC','SELECT 1');

-- Branch A (runs in parallel with branch_b when root succeeds)
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","AFTER")
VALUES ('branch_a',TRUE,'CRON 0 0 3 * * * TZ=UTC','EXECUTE SCRIPT ETL.BRANCH_A()','pipeline_root');

-- Branch B (runs in parallel with branch_a)
INSERT INTO SCHED.SCHED_TASKS ("TASK_ID","ENABLED","SCHEDULE","SQL_TEXT","AFTER")
VALUES ('branch_b',TRUE,'CRON 0 0 3 * * * TZ=UTC','EXECUTE SCRIPT ETL.BRANCH_B()','pipeline_root');
```

All branches share the same `GRAPH_RUN_ID`, so history is automatically correlated.

To force sequential execution instead, set `PARALLEL_CHILDREN = FALSE` on the parent:

```sql
UPDATE SCHED.SCHED_TASKS SET "PARALLEL_CHILDREN" = FALSE WHERE "TASK_ID" = 'pipeline_root';
```

### Trigger a pipeline outside its normal schedule

There is no manual trigger API. To run a task immediately:

1. Temporarily change the schedule to fire within the next poll interval:
   ```sql
   UPDATE SCHED.SCHED_TASKS SET "SCHEDULE" = 'CRON 0 * * * * * TZ=UTC'
   WHERE "TASK_ID" = 'etl_extract';
   ```
2. Wait for the execution to appear in `SCHED_HISTORY` (within `POLL_INTERVAL_SECS`).
3. Restore the original schedule:
   ```sql
   UPDATE SCHED.SCHED_TASKS SET "SCHEDULE" = 'CRON 0 0 2 * * * TZ=UTC'
   WHERE "TASK_ID" = 'etl_extract';
   ```

### Verify a pipeline completed successfully today

```sql
SELECT "TASK_ID", "STATUS", "STARTED_AT", "FINISHED_AT"
FROM SCHED.SCHED_HISTORY
WHERE "GRAPH_RUN_ID" = (
    SELECT "GRAPH_RUN_ID" FROM SCHED.SCHED_HISTORY
    WHERE "TASK_ID"    = 'etl_extract'
      AND "STATUS"     = 'SUCCEEDED'
      AND "STARTED_AT" >= TRUNC(CURRENT_TIMESTAMP)
    ORDER BY "STARTED_AT" DESC LIMIT 1
)
ORDER BY "STARTED_AT";
```

---

## Invariants — do not violate

| Rule | Consequence of violation |
|---|---|
| One scheduler process per `SCHED_TASKS` table | Double execution; duplicate `SCHED_HISTORY` rows |
| `TASK_ID` must be unique | `INSERT` fails with primary key violation |
| `AFTER` must match an existing `TASK_ID` or be NULL | Task becomes orphan; silently never runs |
| Never put a cycle in `AFTER` references | All cycle participants silently excluded |
| `SCHEDULE` must be a non-empty string (even for children) | `INSERT` fails with NOT NULL violation |
| Never grant `INSERT`/`UPDATE` on `SCHED_TASKS` to untrusted users | `SQL_TEXT` is executed verbatim; it is a code execution surface |
| Child `TASK_ID` values must sort correctly if sequential order matters | Set `PARALLEL_CHILDREN = FALSE` on the parent; children then execute alphabetically |
| A root task failure exits the scheduler process | Do not let a failing root task loop; disable it before the supervisor restarts |
