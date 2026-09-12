use axum::http::StatusCode;
use cc_lb_storage_api::{
    RequestEvent, SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
};
use serde_json::json;
use uuid::Uuid;

use crate::admin_test_common;

pub(crate) const NOW_UNIX_SECS: u64 = 1_800_003_600;

pub(crate) async fn create_oauth_upstream(
    server: &admin_test_common::SpawnedAdminServer,
    name: &str,
) -> Uuid {
    let (status, _, body) = server
        .client
        .post_json(
            "/admin/v1/upstreams",
            json!({ "name": name, "kind": "anthropic_oauth" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    body["id"]
        .as_str()
        .expect("created upstream has id")
        .parse()
        .expect("created upstream id is uuid")
}

pub(crate) fn quota_checkpoint(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at_unix_secs: u64,
    sample_seed: u128,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(&SubscriptionQuotaSample {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: observed_at_unix_secs.saturating_mul(1_000),
        sample_id: Uuid::from_u128(sample_seed),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(resets_at_unix_secs),
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: observed_at_unix_secs.saturating_mul(1_000),
    })
}

pub(crate) fn usage_event(
    timestamp: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(timestamp.saturating_mul(1_000)),
        request_id: request_id.to_owned(),
        principal_id: Some("quota-parity-principal".to_owned()),
        key_id: Some("quota-parity-key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("quota-parity-model".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cost_usd_micros: Some(0),
        duration_ms: 1,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..Default::default()
    }
}

pub(crate) fn legacy_inclusive_tokens(
    buckets: &[(u64, u64)],
    start_unix_secs: u64,
    end_unix_secs: u64,
) -> u64 {
    buckets
        .iter()
        .filter(|(bucket_start_unix_secs, _)| {
            *bucket_start_unix_secs >= start_unix_secs && *bucket_start_unix_secs <= end_unix_secs
        })
        .map(|(_, tokens)| *tokens)
        .sum()
}

pub(crate) fn assert_close(actual: &serde_json::Value, expected: f64) {
    let actual = actual.as_f64().expect("aggregate field is numeric");
    assert!(
        (actual - expected).abs() <= 1e-9,
        "expected {expected}, got {actual}"
    );
}
