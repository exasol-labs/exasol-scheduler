use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use exasol_scheduler::time::{Clock, FakeClock, SystemClock};
use std::time::Duration;

#[test]
fn system_clock_now_is_close_to_utc_now() {
    let clock = SystemClock;
    let before = Utc::now();
    let now = clock.now();
    let after = Utc::now();
    assert!(now >= before && now <= after);
}

#[test]
fn fake_clock_set_now_and_set_update_current_time() {
    let initial = Utc.with_ymd_and_hms(2026, 2, 1, 12, 0, 0).unwrap();
    let clock = FakeClock::new(initial);

    let first = Utc.with_ymd_and_hms(2026, 2, 1, 12, 0, 30).unwrap();
    clock.set_now(first);
    assert_eq!(clock.now(), first);

    let second = Utc.with_ymd_and_hms(2026, 2, 1, 12, 1, 0).unwrap();
    clock.set(second);
    assert_eq!(clock.now(), second);
}

#[test]
fn fake_clock_advance_moves_time_forward() {
    let initial = Utc.with_ymd_and_hms(2026, 2, 1, 12, 0, 0).unwrap();
    let clock = FakeClock::new(initial);

    clock.advance(Duration::from_secs(90));
    assert_eq!(
        clock.now(),
        initial + ChronoDuration::seconds(90),
        "advance should add provided duration"
    );
}

#[test]
fn fake_clock_advance_ignores_out_of_range_std_duration() {
    let initial = Utc.with_ymd_and_hms(2026, 2, 1, 12, 0, 0).unwrap();
    let clock = FakeClock::new(initial);

    clock.advance(Duration::from_secs(u64::MAX));
    assert_eq!(
        clock.now(),
        initial,
        "out-of-range std::time::Duration should be ignored"
    );
}
