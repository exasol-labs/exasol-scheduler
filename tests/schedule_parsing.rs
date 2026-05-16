use chrono::{TimeZone, Utc};
use exasol_scheduler::schedule::{
    LocalTimeZone, ParsedSchedule, ParsedTimeZone, ScheduleParseError,
};
use pretty_assertions::assert_eq;

#[test]
fn parses_valid_cron_without_timezone() {
    // Missing TZ is retained as LocalDefault so caller controls local-time semantics.
    let parsed = ParsedSchedule::parse("CRON 0 * * * * *").unwrap();
    assert_eq!(parsed.normalized(), "CRON 0 * * * * *");
    assert_eq!(parsed.timezone(), ParsedTimeZone::LocalDefault);
}

#[test]
fn parses_valid_cron_with_utc_timezone() {
    let parsed = ParsedSchedule::parse("CRON 0 */5 * * * * TZ=UTC").unwrap();
    assert_eq!(parsed.normalized(), "CRON 0 */5 * * * * TZ=UTC");
    assert_eq!(parsed.timezone(), ParsedTimeZone::Utc);
}

#[test]
fn parses_valid_cron_with_iana_timezone() {
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * * TZ=Europe/Copenhagen").unwrap();
    assert_eq!(parsed.normalized(), "CRON 0 0 9 * * * TZ=Europe/Copenhagen");
    assert_eq!(
        parsed.timezone(),
        ParsedTimeZone::Iana(chrono_tz::Europe::Copenhagen)
    );
}

#[test]
fn rejects_missing_cron_prefix() {
    let err = ParsedSchedule::parse("0 * * * * *").unwrap_err();
    assert!(matches!(err, ScheduleParseError::MissingPrefix));
}

#[test]
fn rejects_wrong_field_count() {
    let err = ParsedSchedule::parse("CRON 0 * * * *").unwrap_err();
    assert!(matches!(err, ScheduleParseError::InvalidFieldCount { .. }));
}

#[test]
fn rejects_invalid_numeric_ranges() {
    let err = ParsedSchedule::parse("CRON 0 99 * * * * TZ=UTC").unwrap_err();
    assert!(matches!(err, ScheduleParseError::InvalidCron(_)));
}

#[test]
fn rejects_invalid_timezone_value() {
    let err = ParsedSchedule::parse("CRON 0 * * * * * TZ=Neverland").unwrap_err();
    assert!(matches!(err, ScheduleParseError::InvalidTimeZone(_)));
}

#[test]
fn rejects_empty_timezone_suffix() {
    let err = ParsedSchedule::parse("CRON 0 * * * * * TZ=").unwrap_err();
    assert!(matches!(err, ScheduleParseError::InvalidTimeZone(value) if value.is_empty()));
}

#[test]
fn rejects_unknown_postfix_token() {
    let err = ParsedSchedule::parse("CRON 0 * * * * * EXTRA=token").unwrap_err();
    assert!(matches!(err, ScheduleParseError::InvalidFieldCount { .. }));
}

#[test]
fn normalizes_whitespace_between_fields() {
    let parsed = ParsedSchedule::parse(" CRON   0   0  9  *  *  *   TZ=UTC   ").unwrap();
    assert_eq!(parsed.normalized(), "CRON 0 0 9 * * * TZ=UTC");
}

#[test]
fn local_timezone_can_be_tested_deterministically() {
    // 09:00 in Copenhagen is 08:00 UTC in winter.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 7, 0, 0).unwrap();
    let next = parsed
        .next_after_with_local(now, LocalTimeZone::Named(chrono_tz::Europe::Copenhagen))
        .unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap());
}

#[test]
fn local_timezone_choice_changes_next_fire_for_missing_tz() {
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 7, 0, 0).unwrap();

    let next_in_utc = parsed
        .next_after_with_local(now, LocalTimeZone::Named(chrono_tz::UTC))
        .unwrap();
    let next_in_cph = parsed
        .next_after_with_local(now, LocalTimeZone::Named(chrono_tz::Europe::Copenhagen))
        .unwrap();

    assert_eq!(
        next_in_utc,
        Utc.with_ymd_and_hms(2026, 1, 1, 9, 0, 0).unwrap()
    );
    assert_eq!(
        next_in_cph,
        Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap()
    );
}

#[test]
fn convenience_next_after_works_for_utc_schedule() {
    let parsed = ParsedSchedule::parse("CRON 0 * * * * * TZ=UTC").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 30).unwrap();
    let next = parsed.next_after(now).unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap());
}

#[test]
fn missing_tz_can_use_system_local_timezone_path() {
    let parsed = ParsedSchedule::parse("CRON 0 * * * * *").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 30).unwrap();
    let next = parsed
        .next_after_with_local(now, LocalTimeZone::System)
        .expect("next occurrence should be computable");
    assert!(next > now);
}

#[test]
fn iana_timezone_path_produces_expected_next_fire_time() {
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * * TZ=Europe/Copenhagen").unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 7, 0, 0).unwrap();
    let next = parsed.next_after_with_local(now, LocalTimeZone::Named(chrono_tz::UTC));
    assert_eq!(next, Some(Utc.with_ymd_and_hms(2026, 1, 1, 8, 0, 0).unwrap()));
}

// DOW remapping: standard cron (0/7=Sun, 1=Mon…6=Sat) → cron-rs (1=Sun, 2=Mon…7=Sat)

#[test]
fn dow_numeric_1_means_monday_standard_cron() {
    // Standard cron: 1 = Monday. 2026-01-05 is a Monday.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * 1 TZ=UTC").unwrap();
    let sunday = Utc.with_ymd_and_hms(2026, 1, 4, 10, 0, 0).unwrap(); // Sunday 2026-01-04
    let next = parsed.next_after(sunday).unwrap();
    // Should fire Monday 2026-01-05 09:00 UTC, not Sunday
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 5, 9, 0, 0).unwrap());
}

#[test]
fn dow_numeric_0_means_sunday_standard_cron() {
    // Standard cron: 0 = Sunday. 2026-01-04 is a Sunday.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * 0 TZ=UTC").unwrap();
    let saturday = Utc.with_ymd_and_hms(2026, 1, 3, 10, 0, 0).unwrap(); // Saturday 2026-01-03
    let next = parsed.next_after(saturday).unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 4, 9, 0, 0).unwrap());
}

#[test]
fn dow_numeric_7_is_sunday_alias() {
    // Standard cron: 7 = Sunday (alias for 0). 2026-01-04 is a Sunday.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * 7 TZ=UTC").unwrap();
    let saturday = Utc.with_ymd_and_hms(2026, 1, 3, 10, 0, 0).unwrap();
    let next = parsed.next_after(saturday).unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 4, 9, 0, 0).unwrap());
}

#[test]
fn dow_range_1_to_5_means_monday_to_friday() {
    // Standard cron: 1-5 = Mon-Fri.
    // 2026-01-05 is Monday; test from Sunday to verify next is Monday.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * 1-5 TZ=UTC").unwrap();
    // normalized() should preserve the user-facing form
    assert_eq!(parsed.normalized(), "CRON 0 0 9 * * 1-5 TZ=UTC");

    let sunday = Utc.with_ymd_and_hms(2026, 1, 4, 10, 0, 0).unwrap(); // 2026-01-04 Sun
    let next = parsed.next_after(sunday).unwrap();
    // Next weekday after Sunday is Monday 2026-01-05
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 5, 9, 0, 0).unwrap());

    // Advance to Friday — next should be following Monday, not Saturday
    let friday = Utc.with_ymd_and_hms(2026, 1, 9, 10, 0, 0).unwrap(); // 2026-01-09 Fri
    let after_friday = parsed.next_after(friday).unwrap();
    assert_eq!(after_friday, Utc.with_ymd_and_hms(2026, 1, 12, 9, 0, 0).unwrap());
}

#[test]
fn dow_named_mon_fri_matches_numeric_1_to_5() {
    // Named days should fire on the same days as standard numeric 1-5.
    let numeric = ParsedSchedule::parse("CRON 0 0 9 * * 1-5 TZ=UTC").unwrap();
    let named = ParsedSchedule::parse("CRON 0 0 9 * * MON-FRI TZ=UTC").unwrap();
    let sunday = Utc.with_ymd_and_hms(2026, 1, 4, 10, 0, 0).unwrap();
    assert_eq!(numeric.next_after(sunday), named.next_after(sunday));
}

#[test]
fn dow_numeric_6_means_saturday() {
    // Standard cron: 6 = Saturday. 2026-01-10 is a Saturday.
    let parsed = ParsedSchedule::parse("CRON 0 0 9 * * 6 TZ=UTC").unwrap();
    let friday = Utc.with_ymd_and_hms(2026, 1, 9, 10, 0, 0).unwrap(); // 2026-01-09 Fri
    let next = parsed.next_after(friday).unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 10, 9, 0, 0).unwrap());
}
