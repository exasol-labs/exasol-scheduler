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
