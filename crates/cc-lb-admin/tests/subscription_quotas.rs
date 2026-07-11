mod admin_test_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamSubscriptionQuotaStore, UsageRollupStore,
};
use serde_json::json;
use uuid::Uuid;

const CHECKPOINT_TEST_NOW_UNIX_SECS: u64 = 1_800_000_000;

#[tokio::test]
async fn subscription_quota_latest_is_registered_on_v1_and_legacy_paths() {
    let server = admin_test_common::spawn_admin_server().await;
    let (status, _, body) = server
        .client
        .post_json(
            "/admin/v1/upstreams",
            json!({ "name": "quota-upstream", "kind": "anthropic_oauth" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let upstream_id = body["id"].as_str().unwrap();

    for path in [
        "/admin/v1/subscription-quotas/latest",
        "/admin/subscription-quotas/latest",
    ] {
        let (status, _, body) = server.client.get(path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body["upstreams"][0]["upstream_id"], upstream_id);
        assert_eq!(body["upstreams"][0]["upstream_name"], "quota-upstream");
    }
}

#[tokio::test]
async fn subscription_quota_series_defaults_missing_upstream_ids_to_all() {
    let server = admin_test_common::spawn_admin_server().await;
    let (status, _, _) = server
        .client
        .post_json(
            "/admin/v1/upstreams",
            json!({ "name": "quota-series", "kind": "anthropic_oauth" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/series?since_unix_secs=1&until_unix_secs=120")
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "merged");
    assert_eq!(body["bucket_secs"], 300);
    assert!(body["series"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn subscription_quota_series_defaults_exclude_stored_fable_window() {
    // Given: only a Fable-scoped weekly checkpoint is stored.
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = create_oauth_upstream(&server, "fable-series").await;
    let mut fable = quota_observation(
        upstream_id,
        120,
        30,
        SubscriptionQuotaSource::Api,
        0.28,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    fable.window = SubscriptionQuotaWindow::SevenDayFable;
    server
        .storage
        .put_subscription_quota_checkpoint(&checkpoint_record(fable))
        .await
        .unwrap();

    // When: series windows are omitted.
    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/series?upstream_ids={upstream_id}&since_unix_secs=60&until_unix_secs=180&bucket_secs=60"
        ))
        .await;

    // Then: the historical 5h/7d default excludes the Fable series.
    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().expect("series are returned");
    assert!(series.is_empty());
}

#[tokio::test]
async fn subscription_quota_series_explicit_fable_window_includes_stored_series() {
    // Given: only a Fable-scoped weekly checkpoint is stored.
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = create_oauth_upstream(&server, "explicit-fable-series").await;
    let mut fable = quota_observation(
        upstream_id,
        120,
        31,
        SubscriptionQuotaSource::Api,
        0.28,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    fable.window = SubscriptionQuotaWindow::SevenDayFable;
    server
        .storage
        .put_subscription_quota_checkpoint(&checkpoint_record(fable))
        .await
        .unwrap();

    // When: the Fable window is requested explicitly.
    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/series?upstream_ids={upstream_id}&windows=7d_fable&since_unix_secs=60&until_unix_secs=180&bucket_secs=60"
        ))
        .await;

    // Then: the stored Fable series is returned.
    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().expect("series are returned");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["window"], "7d_fable");
    assert_eq!(series[0]["buckets"][1]["utilization_last"], 0.28);
}

#[tokio::test]
async fn subscription_quota_analysis_defaults_missing_upstream_ids_to_all() {
    let server = admin_test_common::spawn_admin_server().await;

    let (status, _, _) = server
        .client
        .get("/admin/v1/subscription-quotas/analysis?since_unix_secs=1&until_unix_secs=120")
        .await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn subscription_quota_checkpoint_series_returns_steps_without_fabricated_leading_zeroes() {
    let server = admin_test_common::spawn_admin_server().await;
    let upstream_id = create_oauth_upstream(&server, "checkpoint-series").await;
    server
        .storage
        .put_subscription_quota_checkpoints(&[
            checkpoint_record(quota_observation(
                upstream_id,
                30,
                1,
                SubscriptionQuotaSource::Header,
                0.10,
                Some(SubscriptionQuotaStatus::Allowed),
            )),
            checkpoint_record(quota_observation(
                upstream_id,
                75,
                2,
                SubscriptionQuotaSource::Header,
                0.20,
                Some(SubscriptionQuotaStatus::Allowed),
            )),
            checkpoint_record(quota_observation(
                upstream_id,
                180,
                3,
                SubscriptionQuotaSource::Header,
                0.60,
                Some(SubscriptionQuotaStatus::AllowedWarning),
            )),
        ])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/series?upstream_ids={upstream_id}&windows=5h&source=header&since_unix_secs=60&until_unix_secs=240&bucket_secs=60"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    let buckets = body["series"][0]["buckets"]
        .as_array()
        .expect("series buckets are returned");
    assert_eq!(bucket_starts(buckets), vec![0, 60, 120, 180, 240]);
    assert_eq!(buckets[0]["utilization_last"], 0.10);
    assert_eq!(buckets[1]["utilization_last"], 0.20);
    assert_eq!(buckets[2]["utilization_last"], 0.20);
    assert_eq!(buckets[3]["utilization_last"], 0.60);
    assert!(
        buckets
            .iter()
            .all(|bucket| bucket["utilization_last"] != 0.0)
    );

    let no_anchor_upstream_id = create_oauth_upstream(&server, "checkpoint-series-no-anchor").await;
    server
        .storage
        .put_subscription_quota_checkpoint(&checkpoint_record(quota_observation(
            no_anchor_upstream_id,
            180,
            4,
            SubscriptionQuotaSource::Header,
            0.80,
            Some(SubscriptionQuotaStatus::Allowed),
        )))
        .await
        .unwrap();

    let (status, _, no_anchor_body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/series?upstream_ids={no_anchor_upstream_id}&windows=5h&source=header&since_unix_secs=60&until_unix_secs=170&bucket_secs=60"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        no_anchor_body["series"]
            .as_array()
            .expect("no-anchor response has series array")
            .is_empty()
    );
}

#[tokio::test]
async fn subscription_quota_checkpoint_analysis_uses_exact_checkpoint_intervals() {
    let server = admin_test_common::spawn_admin_server().await;
    let flat_upstream_id = create_oauth_upstream(&server, "checkpoint-analysis-flat").await;
    let rising_upstream_id = create_oauth_upstream(&server, "checkpoint-analysis-rising").await;
    let latest_only_upstream_id =
        create_oauth_upstream(&server, "checkpoint-analysis-latest").await;
    server
        .storage
        .put_subscription_quota_checkpoints(&[
            checkpoint_record(quota_observation(
                flat_upstream_id,
                60,
                11,
                SubscriptionQuotaSource::Header,
                0.20,
                Some(SubscriptionQuotaStatus::Allowed),
            )),
            checkpoint_record(quota_observation(
                flat_upstream_id,
                660,
                12,
                SubscriptionQuotaSource::Header,
                0.20,
                Some(SubscriptionQuotaStatus::AllowedWarning),
            )),
            checkpoint_record(quota_observation(
                rising_upstream_id,
                60,
                13,
                SubscriptionQuotaSource::Header,
                0.20,
                Some(SubscriptionQuotaStatus::Allowed),
            )),
            checkpoint_record(quota_observation(
                rising_upstream_id,
                660,
                14,
                SubscriptionQuotaSource::Header,
                0.50,
                Some(SubscriptionQuotaStatus::Allowed),
            )),
        ])
        .await
        .unwrap();
    server
        .storage
        .record_subscription_quota_sample(&quota_observation(
            latest_only_upstream_id,
            2_000,
            15,
            SubscriptionQuotaSource::Header,
            0.90,
            Some(SubscriptionQuotaStatus::Allowed),
        ))
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/analysis?upstream_ids={flat_upstream_id},{rising_upstream_id},{latest_only_upstream_id}&windows=5h&source=header&since_unix_secs=0&until_unix_secs=900"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    let flat = analysis_window_for(&body, flat_upstream_id);
    let rising = analysis_window_for(&body, rising_upstream_id);
    assert_eq!(flat["actual_account_burn"]["utilization_per_hour"], 0.0);
    assert_eq!(flat["actual_account_burn"]["interval_count"], 1);
    assert!(flat["actual_account_burn"].get("sample_count").is_none());
    assert!(
        rising["actual_account_burn"]["utilization_per_hour"]
            .as_f64()
            .expect("rising burn is numeric")
            > 0.0
    );
    assert_eq!(rising["actual_account_burn"]["interval_count"], 1);

    let latest_only = body["upstreams"]
        .as_array()
        .expect("analysis upstreams")
        .iter()
        .find(|upstream| upstream["upstream_id"] == latest_only_upstream_id.to_string())
        .expect("latest-only upstream is present");
    assert_eq!(
        latest_only["windows"]
            .as_array()
            .expect("latest-only windows")
            .len(),
        0
    );
}

#[tokio::test]
async fn subscription_quota_checkpoint_aggregate_carries_unaligned_checkpoints_to_evaluation_time()
{
    let clock = test_clock();
    let server = admin_test_common::spawn_admin_server_with_clock(clock).await;
    let first_upstream_id = create_oauth_upstream(&server, "checkpoint-aggregate-a").await;
    let second_upstream_id = create_oauth_upstream(&server, "checkpoint-aggregate-b").await;
    let first_checkpoint = CHECKPOINT_TEST_NOW_UNIX_SECS - 1_200;
    let second_checkpoint = CHECKPOINT_TEST_NOW_UNIX_SECS - 600;
    let reset_at = CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600;
    server
        .storage
        .put_subscription_quota_checkpoints(&[
            checkpoint_record(quota_observation_with_reset(
                first_upstream_id,
                first_checkpoint,
                21,
                SubscriptionQuotaSource::Header,
                0.25,
                reset_at,
            )),
            checkpoint_record(quota_observation_with_reset(
                second_upstream_id,
                second_checkpoint,
                22,
                SubscriptionQuotaSource::Header,
                0.50,
                reset_at,
            )),
        ])
        .await
        .unwrap();
    server
        .storage
        .append_request_event(&usage_event(
            CHECKPOINT_TEST_NOW_UNIX_SECS - 300,
            "first-after-checkpoint",
            first_upstream_id,
            "checkpoint-aggregate-a",
            250,
        ))
        .await
        .unwrap();
    server
        .storage
        .append_request_event(&usage_event(
            CHECKPOINT_TEST_NOW_UNIX_SECS - 300,
            "second-after-checkpoint",
            second_upstream_id,
            "checkpoint-aggregate-b",
            500,
        ))
        .await
        .unwrap();
    server.storage.rollup_usage_once().await.unwrap();

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={first_upstream_id},{second_upstream_id}&windows=5h&source=header"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    let lots = body["windows"][0]["provider_lots"]
        .as_array()
        .expect("aggregate provider lots");
    assert_eq!(lots.len(), 2);
    for lot in lots {
        assert!(
            lot["capacity_estimate_tokens"].as_f64().is_some(),
            "provider lot should use carried-forward checkpoint state at the common evaluation time: {lot:?}"
        );
    }
    assert_eq!(body["windows"][0]["contributing_upstreams"], 2);
}

async fn create_oauth_upstream(server: &admin_test_common::SpawnedAdminServer, name: &str) -> Uuid {
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

fn checkpoint_record(record: SubscriptionQuotaSample) -> SubscriptionQuotaCheckpointRecord {
    SubscriptionQuotaCheckpointRecord::from(&record)
}

fn quota_observation(
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    sample_seed: u128,
    source: SubscriptionQuotaSource,
    utilization: f64,
    status: Option<SubscriptionQuotaStatus>,
) -> SubscriptionQuotaSample {
    quota_observation_with_status_and_reset(
        upstream_id,
        observed_at_unix_secs,
        sample_seed,
        source,
        utilization,
        status,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600,
    )
}

fn quota_observation_with_reset(
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    sample_seed: u128,
    source: SubscriptionQuotaSource,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaSample {
    quota_observation_with_status_and_reset(
        upstream_id,
        observed_at_unix_secs,
        sample_seed,
        source,
        utilization,
        Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs,
    )
}

fn quota_observation_with_status_and_reset(
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    sample_seed: u128,
    source: SubscriptionQuotaSource,
    utilization: f64,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis: observed_at_unix_secs.saturating_mul(1_000),
        sample_id: Uuid::from_u128(sample_seed),
        utilization: Some(utilization),
        status,
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
    }
}

fn analysis_window_for(body: &serde_json::Value, upstream_id: Uuid) -> &serde_json::Value {
    &body["upstreams"]
        .as_array()
        .expect("analysis upstreams")
        .iter()
        .find(|upstream| upstream["upstream_id"] == upstream_id.to_string())
        .expect("analysis upstream exists")["windows"][0]
}

fn bucket_starts(buckets: &[serde_json::Value]) -> Vec<u64> {
    buckets
        .iter()
        .map(|bucket| {
            bucket["bucket_start_unix_secs"]
                .as_u64()
                .expect("bucket start is u64")
        })
        .collect()
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS))
}

fn usage_event(
    ts: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts.saturating_mul(1_000)),
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some("test-key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cost_usd_micros: Some(0),
        duration_ms: 10,
        ..Default::default()
    }
}
