use exasol_scheduler::model::TaskRow;
use exasol_scheduler::scheduler::{diff_task_rows, fingerprints_for_row};
use pretty_assertions::assert_eq;

fn task(
    task_id: &str,
    enabled: bool,
    schedule: &str,
    statement: &str,
    after: Option<&str>,
    is_final: bool,
    comment: Option<&str>,
) -> TaskRow {
    TaskRow {
        task_id: task_id.to_string(),
        enabled,
        schedule: schedule.to_string(),
        statement: statement.to_string(),
        after: after.map(|value| value.to_string()),
        is_final,
        comment: comment.map(|value| value.to_string()),
    }
}

#[test]
fn diff_detects_added_removed_and_changed() {
    // A task removed, one changed, and one added should all be reported.
    let old = vec![
        task(
            "a",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 1",
            None,
            false,
            None,
        ),
        task(
            "b",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 2",
            None,
            false,
            None,
        ),
    ];
    let new = vec![
        task(
            "b",
            true,
            "CRON 0 */5 * * * * TZ=UTC",
            "SELECT 2",
            None,
            false,
            None,
        ),
        task(
            "c",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 3",
            None,
            false,
            None,
        ),
    ];

    let diff = diff_task_rows(&old, &new);
    assert_eq!(diff.added, vec!["c".to_string()]);
    assert_eq!(diff.removed, vec!["a".to_string()]);
    assert_eq!(diff.changed, vec!["b".to_string()]);
}

#[test]
fn diff_marks_enabled_schedule_statement_after_and_is_final_changes() {
    // Every behavior-driving field in the fingerprint should produce a "changed" row.
    let base = task(
        "root",
        true,
        "CRON 0 * * * * * TZ=UTC",
        "SELECT 1",
        None,
        false,
        Some("comment-a"),
    );

    let variants = vec![
        task(
            "root",
            false,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 1",
            None,
            false,
            Some("comment-a"),
        ),
        task(
            "root",
            true,
            "CRON 0 */5 * * * * TZ=UTC",
            "SELECT 1",
            None,
            false,
            Some("comment-a"),
        ),
        task(
            "root",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 9",
            None,
            false,
            Some("comment-a"),
        ),
        task(
            "root",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 1",
            Some("parent"),
            false,
            Some("comment-a"),
        ),
        task(
            "root",
            true,
            "CRON 0 * * * * * TZ=UTC",
            "SELECT 1",
            Some("parent"),
            true,
            Some("comment-a"),
        ),
    ];

    for variant in variants {
        let diff = diff_task_rows(std::slice::from_ref(&base), std::slice::from_ref(&variant));
        assert_eq!(diff.changed, vec!["root".to_string()]);
    }
}

#[test]
fn diff_ignores_comment_only_changes() {
    // Comments are cosmetic and must not affect fingerprint/diff.
    let old = task(
        "root",
        true,
        "CRON 0 * * * * * TZ=UTC",
        "SELECT 1",
        None,
        false,
        Some("comment-a"),
    );
    let new = task(
        "root",
        true,
        "CRON 0 * * * * * TZ=UTC",
        "SELECT 1",
        None,
        false,
        Some("comment-b"),
    );

    let diff = diff_task_rows(std::slice::from_ref(&old), std::slice::from_ref(&new));
    assert!(diff.changed.is_empty());
}

#[test]
fn statement_change_does_not_modify_schedule_fingerprint() {
    // Statement updates should be detectable, while schedule fingerprint stays stable.
    let old = task(
        "root",
        true,
        "CRON 0 * * * * * TZ=UTC",
        "SELECT 1",
        None,
        false,
        None,
    );
    let new = task(
        "root",
        true,
        "CRON 0 * * * * * TZ=UTC",
        "SELECT 2",
        None,
        false,
        None,
    );

    let old_fp = fingerprints_for_row(&old);
    let new_fp = fingerprints_for_row(&new);

    assert_ne!(old_fp.full_fingerprint, new_fp.full_fingerprint);
    assert_eq!(old_fp.schedule_fingerprint, new_fp.schedule_fingerprint);
}
