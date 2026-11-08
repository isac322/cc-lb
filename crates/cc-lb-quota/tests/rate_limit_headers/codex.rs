use cc_lb_quota::parse_openai_codex_quota_headers;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};

use super::headers;

#[test]
fn parses_team_codex_windows_by_duration() {
    let observations = parse_openai_codex_quota_headers(&headers(&[
        ("x-codex-primary-used-percent", "42.5"),
        ("x-codex-primary-window-minutes", "300"),
        ("x-codex-primary-reset-at", "1800000300"),
        ("x-codex-secondary-used-percent", "100"),
        ("x-codex-secondary-window-minutes", "10080"),
        ("x-codex-secondary-reset-at", "1800600000"),
    ]));

    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
    assert_eq!(observations[0].utilization, Some(0.425));
    assert_eq!(
        observations[0].status,
        Some(SubscriptionQuotaStatus::Allowed)
    );
    assert_eq!(observations[0].resets_at_unix_secs, Some(1_800_000_300));
    assert_eq!(observations[1].window, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(observations[1].utilization, Some(1.0));
    assert_eq!(
        observations[1].status,
        Some(SubscriptionQuotaStatus::Rejected)
    );
}

#[test]
fn parses_pro_primary_as_seven_day_and_drops_null_secondary() {
    let observations = parse_openai_codex_quota_headers(&headers(&[
        ("x-codex-primary-used-percent", "12.5"),
        ("x-codex-primary-window-minutes", "10080"),
        ("x-codex-primary-reset-at", "1800600000"),
    ]));

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(observations[0].utilization, Some(0.125));
}

#[test]
fn drops_codex_windows_with_unknown_duration() {
    let observations = parse_openai_codex_quota_headers(&headers(&[
        ("x-codex-primary-used-percent", "12.5"),
        ("x-codex-primary-window-minutes", "60"),
        ("x-codex-primary-reset-at", "1800600000"),
        ("x-codex-secondary-used-percent", "25"),
        ("x-codex-secondary-window-minutes", "0"),
    ]));

    assert!(observations.is_empty());
}

#[test]
fn ignores_unrelated_codex_headers() {
    assert!(
        parse_openai_codex_quota_headers(&headers(&[("x-codex-plan-type", "plus",)])).is_empty()
    );
}
