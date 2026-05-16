# Configuration Reference

All configuration is via environment variables. A DSN can also be passed as the first command-line argument, which takes precedence over `EXA_DSN`.

**Priority order:**
1. Positional CLI argument — `exasol_scheduler "exasol://..."`
2. `EXA_DSN` environment variable
3. Individual `EXA_HOST`, `EXA_PORT`, `EXA_USER`, `EXA_PASSWORD` variables

---

## Connection

| Variable | Default | Description |
|---|---|---|
| `EXA_DSN` | — | Full connection string (see DSN format below). Takes precedence over the individual vars below. |
| `EXA_HOST` | — | Hostname or IP of the Exasol instance. Required when `EXA_DSN` is not set. |
| `EXA_PORT` | `8563` | Port. |
| `EXA_USER` | — | Username. Required when `EXA_DSN` is not set. |
| `EXA_PASSWORD` | — | Password. Required when `EXA_DSN` is not set. |
| `EXA_TLS` | `false` | Enable TLS. Accepts `1`, `true`, `yes`, `on` (case-insensitive). |
| `EXA_VALIDATE_SERVER_CERT` | `true` | Validate the server certificate. Set to `false` for self-signed certs. |
| `EXA_QUERY_TIMEOUT_SECS` | — | Per-statement timeout in seconds. Applied to every SQL execution. |

### DSN format

```
exasol://user:password@host:port?tls=1&validateservercertificate=0&query_timeout=30
```

The `tls` and `validateservercertificate` query parameters accept `0` or `1`. The `query_timeout` parameter is in seconds and is optional.

Boolean environment variables (`EXA_TLS`, `EXA_VALIDATE_SERVER_CERT`) accept: `1`, `true`, `yes`, `on` for true; `0`, `false`, `no`, `off` for false (all case-insensitive).

---

## Tables

| Variable | Default | Description |
|---|---|---|
| `EXA_SCHEMA` | `PUBLIC` | Schema that contains the task and history tables. The schema must already exist — the scheduler will not create it. |
| `EXA_TASKS_TABLE` | `SCHED_TASKS` | Name of the task definitions table. |
| `EXA_HISTORY_TABLE` | `SCHED_HISTORY` | Name of the execution history table. |

> **Note:** If you use a non-default `EXA_SCHEMA`, create the schema before starting the scheduler:
> ```sql
> CREATE SCHEMA my_schema;
> ```
> The scheduler creates the task and history tables within the schema automatically, but will fail with an error if the schema itself does not exist.

---

## Behaviour

| Variable | Default | Description |
|---|---|---|
| `POLL_INTERVAL_SECS` | `10` | Maximum seconds between polls. The scheduler wakes earlier when a task is due sooner. |
| `RUST_LOG` | `info` | Log verbosity. Options: `error`, `warn`, `info`, `debug`, `trace`. |

---

## Minimal example

```bash
export EXA_HOST=exasol.internal
export EXA_USER=scheduler_user
export EXA_PASSWORD=secret
export EXA_TLS=true
exasol_scheduler
```

## Full example with all options

```bash
export EXA_DSN="exasol://scheduler_user:secret@exasol.internal:8563?tls=1&validateservercertificate=1&query_timeout=120"
export EXA_SCHEMA=SCHEDULER
export EXA_TASKS_TABLE=SCHED_TASKS
export EXA_HISTORY_TABLE=SCHED_HISTORY
export POLL_INTERVAL_SECS=10
export RUST_LOG=info
exasol_scheduler
```

---

## Table DDL Reference

The scheduler creates these tables automatically on first startup (if they do not already exist). To create them manually — or to inspect their schema — use this DDL. Replace `"PUBLIC"` with your `EXA_SCHEMA` value if different.

```sql
CREATE TABLE "PUBLIC"."SCHED_TASKS" (
    "TASK_ID"           VARCHAR(128) NOT NULL,
    "ENABLED"           BOOLEAN DEFAULT TRUE,
    "SCHEDULE"          VARCHAR(512) NOT NULL,
    "STATEMENT"         VARCHAR(2000000) NOT NULL,
    "AFTER"             VARCHAR(128),
    "IS_FINAL"          BOOLEAN DEFAULT FALSE,
    "PARALLEL_CHILDREN" BOOLEAN DEFAULT TRUE,
    "COMMENT"           VARCHAR(2000),
    PRIMARY KEY ("TASK_ID")
);

CREATE TABLE "PUBLIC"."SCHED_HISTORY" (
    "RUN_ID"        VARCHAR(36) NOT NULL,
    "GRAPH_RUN_ID"  VARCHAR(36),
    "TASK_ID"       VARCHAR(128) NOT NULL,
    "GRAPH_PHASE"   VARCHAR(16) NOT NULL,
    "SCHEDULED_FOR" TIMESTAMP,
    "STARTED_AT"    TIMESTAMP NOT NULL,
    "FINISHED_AT"   TIMESTAMP,
    "STATUS"        VARCHAR(16) NOT NULL,
    "ERROR_MESSAGE" VARCHAR(2000000),
    PRIMARY KEY ("RUN_ID")
);
```

If you create the tables manually before first startup, you can skip granting `CREATE TABLE` to the scheduler user entirely. See [security.md](security.md#minimum-privileges-at-first-startup).

> **Upgrading from an earlier version:** If `SCHED_TASKS` already exists without the `PARALLEL_CHILDREN` column, the scheduler adds it automatically on startup via `ALTER TABLE ... ADD COLUMN "PARALLEL_CHILDREN" BOOLEAN DEFAULT TRUE`. This sets all existing tasks to parallel execution (the new default). If any pipeline requires sequential child execution, update those root tasks before or after upgrading:
> ```sql
> UPDATE PUBLIC.SCHED_TASKS SET "PARALLEL_CHILDREN" = FALSE WHERE "TASK_ID" = 'your_root_task';
> ```
