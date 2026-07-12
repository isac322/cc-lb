use cc_lb_quota::rate_limit_headers::parse_anthropic_unified_headers;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};

use crate::headers;

#[test]
fn unified_top_level_fields_are_normalized_and_clamped() {
    let observations = parse_anthropic_unified_headers(&headers(&[
        ("anthropic-ratelimit-unified-status", "allowed"),
        ("anthropic-ratelimit-unified-reset", "1800000000"),
        (
            "anthropic-ratelimit-unified-representative-claim",
            " org:claim ",
        ),
        ("anthropic-ratelimit-unified-fallback-percentage", "1.5"),
        ("anthropic-ratelimit-unified-fallback", "available"),
        ("anthropic-ratelimit-unified-overage-in-use", "true"),
        (
            "anthropic-ratelimit-unified-overage-period-monthly-utilization",
            "1.5",
        ),
        (
            "anthropic-ratelimit-unified-upgrade-paths",
            "team_growth , max_5x,team_growth, ",
        ),
    ]));

    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation.window, SubscriptionQuotaWindow::Unified);
    assert_eq!(observation.status, Some(SubscriptionQuotaStatus::Allowed));
    assert_eq!(observation.resets_at_unix_secs, Some(1_800_000_000));
    assert_eq!(
        observation.representative_claim.as_deref(),
        Some("org:claim")
    );
    assert_eq!(observation.fallback_percentage, Some(1.0));
    assert_eq!(observation.fallback_available, Some(true));
    assert_eq!(observation.overage_in_use, Some(true));
    assert_eq!(observation.overage_period_monthly_utilization, Some(1.0));
    assert_eq!(
        observation.upgrade_paths.as_deref(),
        Some(["max_5x".to_owned(), "team_growth".to_owned()].as_slice())
    );
}

#[test]
fn non_matching_top_level_boolean_values_are_false() {
    let observations = parse_anthropic_unified_headers(&headers(&[
        ("anthropic-ratelimit-unified-fallback", "unavailable"),
        ("anthropic-ratelimit-unified-overage-in-use", "false"),
    ]));

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
    assert_eq!(observations[0].fallback_available, Some(false));
    assert_eq!(observations[0].overage_in_use, Some(false));
}

#[test]
fn empty_unified_headers_produce_no_observation() {
    assert!(parse_anthropic_unified_headers(&http::HeaderMap::new()).is_empty());
}
