# Security Guide

## The trust model

The scheduler executes SQL statements by running them verbatim against Exasol under the credentials it was started with. There is no sandboxing, templating, or parameterisation — the `SQL_TEXT` column is passed directly to the database driver.

This means **the `SCHED_TASKS` table is a code-execution surface**. Any principal that can `INSERT` or `UPDATE` a row in that table can run arbitrary SQL as the scheduler user. Treat write access to `SCHED_TASKS` with the same gravity as write access to your CI/CD pipeline or production deployment scripts.

---

## Set up a dedicated scheduler user

Never run the scheduler as `SYS` or any DBA account. Create a purpose-built user with the minimum privileges required.

### Minimum privileges at first startup

On first startup the scheduler creates the configured schema, `SCHED_TASKS`, and `SCHED_HISTORY` if they do not exist. If you let the scheduler bootstrap these objects, grant schema and table creation only for first startup, then revoke those privileges after the objects exist.

```sql
-- Create the dedicated user (Exasol requires double-quoted password literals)
CREATE USER scheduler_svc IDENTIFIED BY "YourStrongPasswordHere";
GRANT CREATE SESSION TO scheduler_svc;

-- Change-detection: the scheduler reads SYS.EXA_ALL_OBJECTS
-- This view reflects objects the session can already see, so no extra grant is needed.

-- First-startup bootstrap (can be revoked after schema and tables exist)
GRANT CREATE SCHEMA TO scheduler_svc;
GRANT CREATE TABLE TO scheduler_svc;
```

Once the schema and tables are confirmed to exist, revoke the bootstrap privileges:

```sql
REVOKE CREATE SCHEMA FROM scheduler_svc;
REVOKE CREATE TABLE FROM scheduler_svc;
```

This bootstrap path is convenient, but the scheduler user may remain the owner of the objects it created. For strict least privilege, create the schema and tables yourself before starting the scheduler for the first time (reference DDL is in [configuration.md](configuration.md#table-ddl-reference)), and never grant `CREATE SCHEMA` or `CREATE TABLE` at all:

```sql
-- Task table (read-only for the scheduler itself)
GRANT SELECT ON TABLE SCHED.SCHED_TASKS TO scheduler_svc;

-- History table (append-only)
GRANT INSERT ON TABLE SCHED.SCHED_HISTORY TO scheduler_svc;
```

### Runtime privileges for scheduled SQL

The scheduler user also needs whatever privileges the SQL in each task's `SQL_TEXT` column requires. Grant these as specifically as possible:

```sql
-- Prefer: EXECUTE on a specific script
GRANT EXECUTE ON SCRIPT ETL.LOAD_SALES TO scheduler_svc;

-- Avoid where possible: broad table access
-- GRANT SELECT, INSERT, DELETE ON TABLE STAGING.SALES TO scheduler_svc;
```

Wrapping ETL logic in stored scripts and granting `EXECUTE` on those scripts keeps the attack surface narrow: even if a malicious task is inserted, it cannot do more than call the scripts the scheduler user is authorised to run.

---

## Control who can write to SCHED_TASKS

The `SCHED_TASKS` table should only be writable by a small, audited set of principals. A typical strict least-privilege access model:

| Role | SCHED_TASKS | SCHED_HISTORY |
|---|---|---|
| `scheduler_svc` (the running process) | SELECT | INSERT |
| `dba` / deployment pipeline | SELECT, INSERT, UPDATE, DELETE | SELECT |
| Analysts / application users | SELECT (read-only view of the schedule) | SELECT |
| Everyone else | none | none |

Grant `SELECT` on both tables to any user who needs to query the schedule or audit execution history. Do **not** grant `INSERT` or `UPDATE` on `SCHED_TASKS` to application users, ETL pipeline users, or any account that is exposed to untrusted input.

```sql
-- Example: a read-only monitoring role
CREATE ROLE scheduler_reader;
GRANT SELECT ON TABLE SCHED.SCHED_TASKS   TO scheduler_reader;
GRANT SELECT ON TABLE SCHED.SCHED_HISTORY TO scheduler_reader;
GRANT scheduler_reader TO analyst_user;
```

---

## What a compromised task can do

If an attacker inserts a row into `SCHED_TASKS`, they can run any SQL that `scheduler_svc` is authorised to execute. Depending on the privileges granted, that could include:

- Reading or exfiltrating any table the scheduler has SELECT on
- Inserting or deleting data in tables the scheduler has DML on
- Executing any script the scheduler has EXECUTE on
- Creating or dropping objects in schemas where the scheduler has CREATE/DROP

This is not a defect in the scheduler — it is the deliberate design. The mitigation is strict access control on `SCHED_TASKS` and a tightly scoped `scheduler_svc` user.

---

## Credential and connection security

**Use environment variables, not command-line arguments.** Anything passed as a positional argument (`exasol_scheduler "exasol://user:pass@host"`) is visible in process listings (`ps aux`). Use environment variables or an environment file instead:

```bash
# /etc/exasol-scheduler/env  (mode 0600, owned by the service OS user)
EXA_HOST=exasol.internal
EXA_USER=scheduler_svc
EXA_PASSWORD=<strong-password>
EXA_TLS=true
EXA_VALIDATE_SERVER_CERT=true
```

**Always enable TLS.** Set `EXA_TLS=true`. In production, also set `EXA_VALIDATE_SERVER_CERT=true` and ensure the server's certificate is issued by a trusted CA. Disabling certificate validation (`validateservercertificate=0`) makes the connection vulnerable to interception; only use it in isolated development or test environments.

**Rotate credentials** regularly. The scheduler reconnects on every operation, so a credential rotation only requires restarting the process with the new password in the environment — no in-flight connections are affected.

**Use a secrets manager** where available. Inject credentials at runtime from Vault, AWS Secrets Manager, or your platform's equivalent rather than persisting plaintext passwords in environment files.

---

## Auditing and monitoring

Exasol's built-in audit log (`EXA_DBA_AUDIT_SQL`) records every SQL statement executed, including the session user. Enable it and retain logs according to your compliance requirements. This gives you a full record of what the scheduler ran, independent of the `SCHED_HISTORY` table.

The `SCHED_HISTORY` table itself is an append-only audit trail of scheduler-level outcomes
(`SUCCEEDED`, `FAILED`, `SKIPPED`, `INVALID`). `INVALID` records a task row rejected
during snapshot loading. History records which task ran or was rejected and when, but
not the full SQL text. For the full SQL, join `SCHED_HISTORY` against `SCHED_TASKS` on
`TASK_ID`, or consult Exasol's audit log.

To detect unexpected changes to the task schedule, run these two queries:

```sql
-- When was the task table last modified?
SELECT LAST_COMMIT
FROM   SYS.EXA_ALL_OBJECTS
WHERE  ROOT_TYPE = 'SCHEMA'
AND    UPPER(ROOT_NAME)   = UPPER('SCHED')
AND    UPPER(OBJECT_NAME) = UPPER('SCHED_TASKS');

-- What tasks are currently scheduled?
SELECT "TASK_ID", "ENABLED", "SCHEDULE", "SQL_TEXT"
FROM   SCHED.SCHED_TASKS
ORDER  BY "TASK_ID";
```

Alert when the `LAST_COMMIT` timestamp advances outside of planned deployment windows. Or simply watch for the scheduler logging a `"task snapshot reloaded"` message with `added > 0` or `changed > 0`.

---

## Hardening checklist

- [ ] The scheduler connects as a dedicated, non-DBA user (`scheduler_svc`)
- [ ] `CREATE SCHEMA` and `CREATE TABLE` have been revoked from `scheduler_svc` after first startup
- [ ] `INSERT`/`UPDATE`/`DELETE` on `SCHED_TASKS` is restricted to the DBA and deployment pipeline only
- [ ] Application users and ETL users have at most `SELECT` on `SCHED_TASKS`
- [ ] TLS is enabled (`EXA_TLS=true`)
- [ ] Certificate validation is enabled (`EXA_VALIDATE_SERVER_CERT=true`)
- [ ] Credentials are stored in a mode-600 environment file or injected from a secrets manager
- [ ] The scheduler password is not passed on the command line
- [ ] Exasol audit logging is enabled and retained
- [ ] Task changes outside of deployment windows trigger an alert
