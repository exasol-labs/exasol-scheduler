# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- **A failing task no longer stops the scheduler process (BUG-36).** Previously, a
  failed root task exited the process after its graph run finished, which stopped every
  other pipeline until a supervisor restarted it. Now the failed root is recorded as
  `FAILED`, its descendants are recorded as `SKIPPED`, `IS_FINAL` finalizers still run,
  a warning is logged, and polling continues. The root runs again at its next scheduled
  occurrence.
- The process still exits when it cannot read its own task table (for example, the
  database is unreachable). Keep running it under a process supervisor.
- Documentation updated to match: `README.md`, `docs/operations.md`, and
  `docs/agent-skill.md` (failure model, error response protocol). Disabling a failing
  root before a restart is no longer required.

### Added

- `TickResult::failed_roots`: the number of root tasks that failed in a poll. It is also
  included in the scheduler's log output.

### Fixed

- Other roots due in the same poll as a failing root now run. Previously they were not
  attempted, and the scheduler exited before reaching them.

## [0.2] - 2026-08-14

### Changed

- `SCHEDULE` is now nullable. Only root tasks need a schedule; use `NULL` for child
  and finalizer tasks. Existing rows with a non-null value are still accepted, and the
  value is ignored for non-root tasks.
- Existing task tables are migrated automatically on startup
  (`ALTER TABLE … MODIFY COLUMN "SCHEDULE" VARCHAR(512) NULL`).
- Dependency updates: `exarrow-rs` 0.14, `arrow` 58, `chrono-tz` 0.10, `cron` 0.17,
  `thiserror` 2, `mockall` 0.15.
- Clarified documentation on capabilities, boundaries, root failure handling, and
  monitoring liveness.

### Fixed

- A root task with `SCHEDULE = NULL` is skipped with a warning during snapshot loading
  instead of failing the whole load. Other valid rows are still loaded and executed.
- Rejected task rows are now recorded in `SCHED_HISTORY` with `STATUS = 'INVALID'`,
  `GRAPH_PHASE = 'VALIDATION'`, and the validation error in `ERROR_MESSAGE`.

## [0.1.0] - 2026-06-24

Initial release.

### Added

- Table-driven scheduling: tasks are rows in `SCHED_TASKS`, with hot reload on the next
  poll and no restart required.
- Cron schedules with seconds precision and per-task time zones (`CRON … TZ=…`).
- Dependency graphs via `AFTER`, with downstream `SKIPPED` on failure and `IS_FINAL`
  finalizers that always run.
- Parallel child execution by default, with `PARALLEL_CHILDREN = FALSE` for sequential
  order.
- Execution history in `SCHED_HISTORY`, with a shared `GRAPH_RUN_ID` per graph run.
- Automatic creation of the task and history tables on startup.
- Configuration through a DSN argument or `EXA_*` environment variables.
- Agent skill documentation (`docs/agent-skill.md`) and security guidance.
- Release builds for a target matrix of runners.

[Unreleased]: https://github.com/exasol-labs/exasol-scheduler/compare/v0.2...HEAD
[0.2]: https://github.com/exasol-labs/exasol-scheduler/compare/v0.1.0...v0.2
[0.1.0]: https://github.com/exasol-labs/exasol-scheduler/releases/tag/v0.1.0
