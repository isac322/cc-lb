use cc_lb_domain::{RateLimitKind, RateLimitObservation};
use cc_lb_quota::rate_limit_headers::{
    clamp_utilization_fraction, parse_anthropic_rate_limit_headers, percent_to_utilization_fraction,
};

use crate::headers;

#[test]
fn parses_default_request_and_token_families() {
    let snapshots = parse_anthropic_rate_limit_headers(&headers(&[
        ("anthropic-ratelimit-requests-limit", "1000"),
        ("anthropic-ratelimit-requests-remaining", "997"),
        ("anthropic-ratelimit-requests-reset", "2026-05-20T00:00:01Z"),
        ("anthropic-ratelimit-tokens-limit", "100000"),
        ("anthropic-ratelimit-tokens-remaining", "99990"),
        ("anthropic-ratelimit-tokens-reset", "2026-05-20T00:00:02Z"),
    ]));

    assert_eq!(
        snapshots,
        [
            RateLimitObservation {
                kind: RateLimitKind::Requests,
                window: "default".to_owned(),
                limit: Some(1000),
                remaining: Some(997),
                reset: Some("2026-05-20T00:00:01Z".to_owned()),
            },
            RateLimitObservation {
                kind: RateLimitKind::Tokens,
                window: "default".to_owned(),
                limit: Some(100000),
                remaining: Some(99990),
                reset: Some("2026-05-20T00:00:02Z".to_owned()),
            },
        ]
    );
}

#[test]
fn preserves_pre_and_post_field_window_identity() {
    let snapshots = parse_anthropic_rate_limit_headers(&headers(&[
        ("anthropic-ratelimit-requests-limit-5h", "5000"),
        ("anthropic-ratelimit-requests-remaining-5h", "4999"),
        (
            "anthropic-ratelimit-tokens-reset-weekly",
            "2026-05-27T00:00:00Z",
        ),
        ("anthropic-ratelimit-input-tokens-limit", "25000"),
        ("anthropic-ratelimit-input-tokens-remaining", "24000"),
        ("anthropic-ratelimit-output-tokens-limit-weekly", "75000"),
        (
            "anthropic-ratelimit-output-tokens-reset-weekly",
            "2026-05-28T00:00:00Z",
        ),
    ]));

    assert_eq!(snapshots.len(), 4);
    assert_eq!(snapshots[0].kind, RateLimitKind::Requests);
    assert_eq!(snapshots[0].window, "5h");
    assert_eq!(snapshots[0].limit, Some(5000));
    assert_eq!(snapshots[0].remaining, Some(4999));
    assert_eq!(snapshots[1].kind, RateLimitKind::Tokens);
    assert_eq!(snapshots[1].window, "weekly");
    assert_eq!(snapshots[2].kind, RateLimitKind::InputTokens);
    assert_eq!(snapshots[2].limit, Some(25000));
    assert_eq!(snapshots[3].kind, RateLimitKind::OutputTokens);
    assert_eq!(snapshots[3].window, "weekly");
    assert_eq!(snapshots[3].limit, Some(75000));
}

#[test]
fn ignores_malformed_and_unrelated_rate_limit_headers() {
    let snapshots = parse_anthropic_rate_limit_headers(&headers(&[
        ("anthropic-ratelimit-requests-limit", "not-a-number"),
        ("anthropic-ratelimit-tokens-remaining", ""),
        ("anthropic-ratelimit-unknown-limit", "9"),
        ("x-ratelimit-requests-limit", "1"),
    ]));

    assert!(snapshots.is_empty());
    assert!(parse_anthropic_rate_limit_headers(&http::HeaderMap::new()).is_empty());
}

#[test]
fn utilization_fraction_helpers_clamp_without_changing_scale() {
    assert_eq!(clamp_utilization_fraction(0.0), 0.0);
    assert_eq!(clamp_utilization_fraction(0.5), 0.5);
    assert_eq!(clamp_utilization_fraction(1.5), 1.0);
    assert_eq!(clamp_utilization_fraction(-0.5), 0.0);
    assert_eq!(percent_to_utilization_fraction(10.0), 0.10);
    assert_eq!(percent_to_utilization_fraction(125.0), 1.0);
    assert_eq!(percent_to_utilization_fraction(-10.0), 0.0);
}
