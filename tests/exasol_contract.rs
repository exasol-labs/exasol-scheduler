use exasol_scheduler::config::AppConfig;
use exasol_scheduler::db::{ExasolDb, SchedulerDb};

#[test]
fn exasol_contract_smoke_test() {
    // Optional contract test: only executes when explicitly enabled.
    if std::env::var("EXA_CONTRACT_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping exasol contract test (set EXA_CONTRACT_TESTS=1 to enable)");
        return;
    }

    let config = AppConfig::from_env().expect("contract test requires valid Exasol env config");
    let db = ExasolDb::new(config.exasol).expect("failed to build ExasolDb");

    let last_changed = db
        .get_last_changed()
        .expect("failed get_last_changed contract call");
    let tasks = db.load_tasks().expect("failed load_tasks contract call");

    eprintln!(
        "contract test OK: last_changed={last_changed:?}, tasks_loaded={}",
        tasks.len()
    );
}
