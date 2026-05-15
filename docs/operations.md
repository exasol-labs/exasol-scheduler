# Operations Guide

## Building from source

```bash
cargo build --release
# Binary is at target/release/exasol_scheduler
```

Requires Rust 1.85 or later (edition 2024).

**Run the test suite** (no Exasol required):

```bash
cargo test
```

**Test coverage** (requires `cargo-llvm-cov`):

```bash
cargo install cargo-llvm-cov  # one-time

cargo coverage        # terminal summary
cargo coverage-html   # HTML report at target/llvm-cov/html/index.html
cargo coverage-lcov   # LCOV output for CI tooling
```

---

## Running as a systemd service

Create `/etc/systemd/system/exasol-scheduler.service`:

```ini
[Unit]
Description=Exasol Scheduler
After=network.target

[Service]
Type=simple
User=exasol-scheduler
EnvironmentFile=/etc/exasol-scheduler/env
ExecStart=/usr/local/bin/exasol_scheduler
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Create `/etc/exasol-scheduler/env` (mode `0600`):

```
EXA_HOST=exasol.internal
EXA_USER=scheduler_user
EXA_PASSWORD=secret
EXA_TLS=true
EXA_VALIDATE_SERVER_CERT=true
EXA_SCHEMA=PUBLIC
RUST_LOG=info
```

```bash
systemctl daemon-reload
systemctl enable --now exasol-scheduler
```

---

## Running as a Docker container

```dockerfile
FROM debian:bookworm-slim
COPY exasol_scheduler /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/exasol_scheduler"]
```

```bash
docker run -d \
  -e EXA_HOST=exasol.internal \
  -e EXA_USER=scheduler_user \
  -e EXA_PASSWORD=secret \
  -e EXA_TLS=true \
  -e RUST_LOG=info \
  exasol-scheduler:latest
```

---

## Security

- Use a dedicated Exasol user for the scheduler — not `SYS` or any admin account.
- The scheduler user needs **SELECT** on `SCHED_TASKS`, **INSERT** on `SCHED_HISTORY`, and whatever permissions the SQL in `SCHED_TASKS."STATEMENT"` requires.
- Store credentials in environment files with restricted permissions (`chmod 600`), not embedded in SQL statements or baked into container images.
- Restarting the scheduler after a failure does not replay missed executions. The next scheduled occurrence is computed from the current time.

---

## Contract tests (optional)

The test suite includes optional integration tests that verify behaviour against a live Exasol instance:

```bash
EXA_CONTRACT_TESTS=1 cargo test --test exasol_contract -- --test-threads=1
```

Without `EXA_CONTRACT_TESTS=1` the tests are skipped, and the normal suite runs without any database. The tests default to `localhost:8563` with credentials `sys:exasol`. Override with `EXA_DSN`:

```bash
EXA_CONTRACT_TESTS=1 \
EXA_DSN="exasol://myuser:mypassword@myhost:8563?tls=1&validateservercertificate=0" \
  cargo test --test exasol_contract -- --test-threads=1
```

> **Important:** Always use `--test-threads=1`. The tests share a schema and recreate tables between runs — parallel execution causes race conditions.

---

## Repository layout

```
src/
  main.rs          Service entry point and polling loop
  config.rs        Environment-driven configuration
  scheduler.rs     Scheduler engine (heap, diff, graph runner)
  schedule.rs      CRON schedule parser
  model.rs         TaskRow and HistoryEvent data types
  db/
    mod.rs         SchedulerDb trait and DbError
    exasol.rs      Production Exasol adapter (exarrow-rs)
  time.rs          Clock abstraction (real + fake for testing)
tests/
  common/          Shared test helpers (ProgrammableDb, FakeClock)
  stage1_scheduler_integration.rs
  stage2_history.rs
  stage3_dag.rs
  schedule_parsing.rs
  diff_fingerprinting.rs
  time_clock.rs
  exasol_contract.rs   (opt-in, requires a real Exasol instance)
```
