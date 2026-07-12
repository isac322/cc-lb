use cc_lb_quota::rate_limit_headers::UnifiedQuotaObservation;
use cc_lb_quota::{build_subscription_quota_samples, unified_observation_to_sample};
use cc_lb_storage_api::{
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
};
use http::header::{HeaderName, HeaderValue};
use uuid::Uuid;

#[test]
fn builds_header_samples_from_normalized_unified_observations() {
    let upstream_id = Uuid::from_u128(1);
    let mut headers = http::HeaderMap::new();
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-utilization"),
        HeaderValue::from_static("0.42"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-7d-sonnet-status"),
        HeaderValue::from_static("allowed_warning"),
    );
    headers.insert(
        HeaderName::from_static("anthropic-ratelimit-unified-fallback"),
        HeaderValue::from_static("available"),
    );

    let records = build_subscription_quota_samples(&headers, upstream_id, 123_456);

    assert_eq!(records.len(), 2);
    let sonnet = records
        .iter()
        .find(|record| record.window == SubscriptionQuotaWindow::SevenDaySonnet)
        .expect("7d-sonnet record");
    assert_eq!(sonnet.upstream_id, upstream_id);
    assert_eq!(sonnet.source, SubscriptionQuotaSource::Header);
    assert_eq!(sonnet.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(sonnet.observed_at_unix_millis, 123_456);
    assert_eq!(sonnet.ingested_at_unix_millis, 123_456);
    assert_ne!(sonnet.sample_id, Uuid::nil());
    assert_eq!(sonnet.utilization, Some(0.42));
    assert_eq!(sonnet.status, Some(SubscriptionQuotaStatus::AllowedWarning));
    assert!(
        records
            .iter()
            .any(|record| record.fallback_available == Some(true))
    );
}

#[test]
fn builds_a_sample_from_a_fixed_observation_without_changing_fields() {
    let record = unified_observation_to_sample(
        Uuid::nil(),
        UnifiedQuotaObservation {
            window: SubscriptionQuotaWindow::Overage,
            utilization: Some(0.2),
            status: Some(SubscriptionQuotaStatus::Rejected),
            disabled_reason: Some("quota exhausted".to_owned()),
            ..UnifiedQuotaObservation::default()
        },
        999,
    );

    assert_eq!(record.window, SubscriptionQuotaWindow::Overage);
    assert_eq!(record.utilization, Some(0.2));
    assert_eq!(record.status, Some(SubscriptionQuotaStatus::Rejected));
    assert_eq!(record.disabled_reason.as_deref(), Some("quota exhausted"));
    assert_eq!(record.observed_at_unix_millis, 999);
    assert_eq!(record.ingested_at_unix_millis, 999);
}
