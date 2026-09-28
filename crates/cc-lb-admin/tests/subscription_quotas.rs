use crate::admin_test_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_storage_api::{
    PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, RequestEvent, RequestEventStore,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaStore, UsageRollupStore,
};
use serde_json::json;
use uuid::Uuid;

const CHECKPOINT_TEST_NOW_UNIX_SECS: u64 = 1_800_000_000;

fn pool_history_record(snapshot_at_unix_secs: i64, utilization: f64) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs,
        window: SubscriptionQuotaWindow::SevenDayFable,
        utilization: Some(utilization),
        weighted_utilization_sum: utilization,
        capacity_ratio_sum: 1.0,
        eligible_upstreams: 1,
        contributing_upstreams: 1,
        stale_upstreams: 0,
        missing_observation_upstreams: 0,
        missing_metadata_upstreams: 0,
        header_contributing_upstreams: 0,
        api_contributing_upstreams: 1,
        max_observed_at_unix_millis: Some(snapshot_at_unix_secs * 1_000),
        computed_at_unix_millis: snapshot_at_unix_secs * 1_000,
        policy_version: 1,
    }
}

#[tokio::test]
async fn subscription_quota_latest_lists_registered_upstreams() {
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

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/latest")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstreams"][0]["upstream_id"], upstream_id);
    assert_eq!(body["upstreams"][0]["upstream_name"], "quota-upstream");
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
        .put_subscription_quota_checkpoints(&[checkpoint_record(fable)])
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
        .put_subscription_quota_checkpoints(&[checkpoint_record(fable)])
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
async fn subscription_quota_pool_history_accepts_explicit_fable_window() {
    let server = admin_test_common::spawn_admin_server().await;
    server
        .storage
        .record_pool_quota_snapshots(&[PoolQuotaSnapshotRecord {
            snapshot_at_unix_secs: 120,
            window: SubscriptionQuotaWindow::SevenDayFable,
            utilization: Some(0.28),
            weighted_utilization_sum: 1.4,
            capacity_ratio_sum: 5.0,
            eligible_upstreams: 1,
            contributing_upstreams: 1,
            stale_upstreams: 0,
            missing_observation_upstreams: 0,
            missing_metadata_upstreams: 0,
            header_contributing_upstreams: 0,
            api_contributing_upstreams: 1,
            max_observed_at_unix_millis: Some(120_000),
            computed_at_unix_millis: 120_000,
            policy_version: 1,
        }])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get(
            "/admin/v1/subscription-quotas/pool-history?windows=7d_fable&since_unix_secs=60&until_unix_secs=180",
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["windows"][0]["window"], "7d_fable");
    assert_close(
        body["windows"][0]["series"][0]["utilization_percent"].as_f64(),
        28.0,
    );
    assert_eq!(body["windows"][0]["series"][0]["contributing_upstreams"], 1);

    let (status, _, body) = server
        .client
        .get(
            "/admin/v1/subscription-quotas/pool-history?windows=7d_fable&since_unix_secs=60&until_unix_secs=180&series_projection=chart",
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    let compact_point = body["windows"][0]["series"][0]
        .as_object()
        .expect("compact chart point");
    assert_eq!(compact_point.len(), 2);
    assert_eq!(compact_point["snapshot_at_unix_secs"], 120);
    assert_close(compact_point["utilization_percent"].as_f64(), 28.0);
}

#[tokio::test]
async fn subscription_quota_pool_history_caps_chart_points_and_preserves_peak() {
    let server = admin_test_common::spawn_admin_server().await;
    server
        .storage
        .record_pool_quota_snapshots(&[
            pool_history_record(60, 0.10),
            pool_history_record(120, 0.20),
            pool_history_record(180, 0.40),
            pool_history_record(239, 0.30),
            pool_history_record(240, 0.20),
        ])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get(
            "/admin/v1/subscription-quotas/pool-history?windows=7d_fable&since_unix_secs=60&until_unix_secs=240&series_projection=chart&max_points_per_series=3",
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    let series = body["windows"][0]["series"]
        .as_array()
        .expect("bucketed chart series");
    assert_eq!(series.len(), 3);
    assert_eq!(series[0]["snapshot_at_unix_secs"], 0);
    assert_eq!(series[1]["snapshot_at_unix_secs"], 90);
    assert_eq!(series[2]["snapshot_at_unix_secs"], 180);
    assert_close(series[2]["utilization_percent"].as_f64(), 40.0);
    assert_eq!(body["windows"][0]["latest"]["snapshot_at_unix_secs"], 240);
    assert_close(
        body["windows"][0]["latest"]["utilization_percent"].as_f64(),
        20.0,
    );

    let (status, _, _) = server
        .client
        .get(
            "/admin/v1/subscription-quotas/pool-history?windows=7d_fable&series_projection=chart&max_points_per_series=1",
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
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
    assert_eq!(bucket_starts(buckets), vec![60, 120, 180, 240]);
    assert_eq!(buckets[0]["utilization_last"], 0.20);
    assert_eq!(buckets[1]["utilization_last"], 0.20);
    assert_eq!(buckets[2]["utilization_last"], 0.60);
    assert_eq!(buckets[3]["utilization_last"], 0.60);
    assert!(
        buckets
            .iter()
            .all(|bucket| bucket["utilization_last"] != 0.0)
    );

    let no_anchor_upstream_id = create_oauth_upstream(&server, "checkpoint-series-no-anchor").await;
    server
        .storage
        .put_subscription_quota_checkpoints(&[checkpoint_record(quota_observation(
            no_anchor_upstream_id,
            180,
            4,
            SubscriptionQuotaSource::Header,
            0.80,
            Some(SubscriptionQuotaStatus::Allowed),
        ))])
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

#[tokio::test]
async fn subscription_quota_aggregate_matches_legacy_golden_for_multiple_windows_and_boundaries() {
    // Given: two upstreams with 5h/7d observations and usage exactly on the
    // provider-start and cc-lb-start interval boundaries.
    let now_unix_secs = CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600;
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(now_unix_secs));
    let server = admin_test_common::spawn_admin_server_with_clock(clock).await;
    let first_upstream_id = create_oauth_upstream(&server, "aggregate-golden-a").await;
    let second_upstream_id = create_oauth_upstream(&server, "aggregate-golden-b").await;
    let five_hour_reset = now_unix_secs + 3_600;
    let seven_day_reset = now_unix_secs + 2 * 24 * 3_600;
    let five_hour_provider_start = five_hour_reset - 5 * 3_600;
    let seven_day_provider_start = seven_day_reset - 7 * 24 * 3_600;
    let five_hour_cc_start = now_unix_secs - now_unix_secs % (5 * 3_600);
    let seven_day_cc_start = now_unix_secs - now_unix_secs % (7 * 24 * 3_600);

    let mut checkpoints = Vec::new();
    for (upstream_id, seed, five_hour_utilization, seven_day_utilization) in [
        (first_upstream_id, 100_u128, 0.25, 0.40),
        (second_upstream_id, 200_u128, 0.50, 0.20),
    ] {
        checkpoints.push(checkpoint_record(quota_observation_with_status_and_reset(
            upstream_id,
            now_unix_secs - 1_200,
            seed,
            SubscriptionQuotaSource::Header,
            five_hour_utilization,
            Some(SubscriptionQuotaStatus::Allowed),
            five_hour_reset,
        )));
        let mut seven_day = quota_observation_with_status_and_reset(
            upstream_id,
            now_unix_secs - 1_800,
            seed + 1,
            SubscriptionQuotaSource::Header,
            seven_day_utilization,
            Some(SubscriptionQuotaStatus::Allowed),
            seven_day_reset,
        );
        seven_day.window = SubscriptionQuotaWindow::SevenDay;
        checkpoints.push(checkpoint_record(seven_day));
    }
    server
        .storage
        .put_subscription_quota_checkpoints(&checkpoints)
        .await
        .unwrap();

    for (upstream_id, upstream_name, tokens) in [
        (
            first_upstream_id,
            "aggregate-golden-a",
            [200_u64, 300, 100, 400],
        ),
        (
            second_upstream_id,
            "aggregate-golden-b",
            [75_u64, 125, 50, 175],
        ),
    ] {
        for (index, (timestamp, token_count)) in [
            seven_day_provider_start,
            seven_day_cc_start,
            five_hour_provider_start,
            five_hour_cc_start,
        ]
        .into_iter()
        .zip(tokens)
        .enumerate()
        {
            server
                .storage
                .append_request_event(&usage_event(
                    timestamp,
                    &format!("aggregate-golden-{upstream_id}-{index}"),
                    upstream_id,
                    upstream_name,
                    token_count,
                ))
                .await
                .unwrap();
        }
    }
    server.storage.rollup_usage_once().await.unwrap();

    // When: the public aggregate endpoint computes both windows.
    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={first_upstream_id},{second_upstream_id}&windows=5h,7d&source=header"
        ))
        .await;

    // Then: the pre-slim-path response remains the numeric golden authority.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstream_count"], 2);
    let windows = body["windows"].as_array().expect("aggregate windows");
    assert_eq!(windows.len(), 2);
    assert_aggregate_window_matches_golden(
        &windows[0],
        "5h",
        575,
        0.375,
        1_725.0,
        1_150.0,
        &[(0.25, 2_000.0, 500), (0.50, 450.0, 225)],
    );
    assert_aggregate_window_matches_golden(
        &windows[1],
        "7d",
        1_150,
        0.30,
        3_925.0,
        2_775.0,
        &[(0.40, 2_500.0, 500), (0.20, 2_125.0, 200)],
    );
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

fn assert_aggregate_window_matches_golden(
    window: &serde_json::Value,
    expected_window: &str,
    expected_used_tokens: u64,
    expected_utilization: f64,
    expected_capacity_to_now: f64,
    expected_remaining_to_now: f64,
    expected_lots: &[(f64, f64, u64)],
) {
    assert_eq!(window["window"], expected_window);
    assert_eq!(window["used_tokens"], expected_used_tokens);
    assert_eq!(window["confidence"], "plan_weighted");
    assert_close(window["utilization"].as_f64(), expected_utilization);
    assert_close(
        window["capacity_to_now_tokens_estimate"].as_f64(),
        expected_capacity_to_now,
    );
    assert_close(
        window["remaining_to_now_tokens_estimate"].as_f64(),
        expected_remaining_to_now,
    );

    let lots = window["provider_lots"]
        .as_array()
        .expect("provider lots are returned");
    assert_eq!(lots.len(), expected_lots.len());
    for (lot, (utilization, capacity, used_before)) in lots.iter().zip(expected_lots) {
        assert_eq!(lot["source"], "header");
        assert_eq!(lot["state"], "fresh");
        assert_eq!(lot["confidence"], "estimated");
        assert_eq!(lot["used_before_cc_window_tokens"], *used_before);
        assert_close(lot["utilization"].as_f64(), *utilization);
        assert_close(lot["capacity_estimate_tokens"].as_f64(), *capacity);
    }
}

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("golden aggregate field is numeric");
    assert!(
        (actual - expected).abs() <= 1e-9,
        "expected {expected}, got {actual}"
    );
}
