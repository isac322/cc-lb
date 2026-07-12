use cc_lb_quota::rate_limit_headers::parse_anthropic_unified_headers;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};

use crate::headers;

#[test]
fn unified_window_prefixes_are_disjoint_and_stably_ordered() {
    let observations = parse_anthropic_unified_headers(&headers(&[
        ("anthropic-ratelimit-unified-status", "allowed"),
        ("anthropic-ratelimit-unified-5h-status", "allowed"),
        ("anthropic-ratelimit-unified-7d-status", "rejected"),
        ("anthropic-ratelimit-unified-7d-sonnet-status", "allowed"),
        (
            "anthropic-ratelimit-unified-7d-opus-status",
            "allowed_warning",
        ),
        (
            "anthropic-ratelimit-unified-7d_oi-status",
            "allowed_warning",
        ),
        (
            "anthropic-ratelimit-unified-overage-status",
            "exceeded_overage",
        ),
    ]));

    let windows = observations
        .iter()
        .map(|observation| observation.window)
        .collect::<Vec<_>>();
    assert_eq!(
        windows,
        [
            SubscriptionQuotaWindow::Unified,
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDaySonnet,
            SubscriptionQuotaWindow::SevenDayOpus,
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::Overage,
        ]
    );
    assert_eq!(
        observations[3].status,
        Some(SubscriptionQuotaStatus::AllowedWarning)
    );
    assert_eq!(
        observations[4].status,
        Some(SubscriptionQuotaStatus::AllowedWarning)
    );
    assert_eq!(
        observations[5].status,
        Some(SubscriptionQuotaStatus::Rejected)
    );
}

#[test]
fn unified_window_fields_preserve_values_and_normalize_utilization() {
    let observations = parse_anthropic_unified_headers(&headers(&[
        ("anthropic-ratelimit-unified-5h-utilization", "0.42"),
        ("anthropic-ratelimit-unified-5h-reset", "1700000001"),
        ("anthropic-ratelimit-unified-5h-surpassed-threshold", "0.75"),
        ("anthropic-ratelimit-unified-7d_oi-utilization", "1.28"),
        ("anthropic-ratelimit-unified-7d_oi-reset", "1800000004"),
        (
            "anthropic-ratelimit-unified-overage-disabled-reason",
            "quota exhausted",
        ),
        ("anthropic-ratelimit-unified-overage-reset", "1700000002"),
    ]));

    assert_eq!(observations.len(), 3);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
    assert_eq!(observations[0].utilization, Some(0.42));
    assert_eq!(observations[0].resets_at_unix_secs, Some(1_700_000_001));
    assert_eq!(observations[0].surpassed_threshold, Some(0.75));
    assert_eq!(
        observations[1].window,
        SubscriptionQuotaWindow::SevenDayFable
    );
    assert_eq!(observations[1].utilization, Some(1.0));
    assert_eq!(observations[1].resets_at_unix_secs, Some(1_800_000_004));
    assert_eq!(observations[2].window, SubscriptionQuotaWindow::Overage);
    assert_eq!(observations[2].resets_at_unix_secs, Some(1_700_000_002));
    assert_eq!(
        observations[2].disabled_reason.as_deref(),
        Some("quota exhausted")
    );
}

#[test]
fn malformed_unified_fields_do_not_discard_other_fields_in_the_window() {
    let observations = parse_anthropic_unified_headers(&headers(&[
        (
            "anthropic-ratelimit-unified-7d-reset",
            "2026-05-20T00:00:00Z",
        ),
        ("anthropic-ratelimit-unified-7d-utilization", "many"),
        ("anthropic-ratelimit-unified-7d-status", "allowed"),
        (
            "anthropic-ratelimit-unified-7d-sonnet-status",
            "almost_allowed",
        ),
        ("anthropic-ratelimit-unified-7d-sonnet-reset", "1700000003"),
    ]));

    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations[0].window,
        SubscriptionQuotaWindow::SevenDaySonnet
    );
    assert_eq!(observations[0].status, None);
    assert_eq!(observations[0].resets_at_unix_secs, Some(1_700_000_003));
    assert_eq!(observations[1].window, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(observations[1].utilization, None);
    assert_eq!(observations[1].resets_at_unix_secs, None);
    assert_eq!(
        observations[1].status,
        Some(SubscriptionQuotaStatus::Allowed)
    );
}
