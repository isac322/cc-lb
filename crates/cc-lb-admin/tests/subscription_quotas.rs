use crate::admin_test_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_storage_api::{
    PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, Provider, RequestEvent, RequestEventStore,
    SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaAggregateStore, UpstreamSubscriptionQuotaStore, UsageRollupStore,
};
use serde_json::json;
use uuid::Uuid;

const CHECKPOINT_TEST_NOW_UNIX_SECS: u64 = 1_800_000_000;

fn pool_history_record(snapshot_at_unix_secs: i64, utilization: f64) -> PoolQuotaSnapshotRecord {
    PoolQuotaSnapshotRecord {
        provider: Provider::Anthropic,
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

async fn subscription_quota_pool_history_accepts_explicit_fable_window() {
    let server = admin_test_common::spawn_admin_server().await;
    server
        .storage
        .record_pool_quota_snapshots(&[PoolQuotaSnapshotRecord {
            provider: Provider::Anthropic,
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

async fn subscription_quota_aggregate_refreshes_observed_at_from_latest_when_checkpoint_deadbanded()
{
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let upstream_id = create_oauth_upstream(&server, "deadband-latest-refresh").await;
    let mut first_sample = quota_observation(
        upstream_id,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 600,
        1,
        SubscriptionQuotaSource::Api,
        0.0,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    first_sample.window = SubscriptionQuotaWindow::SevenDayFable;
    let mut refreshed_unchanged = first_sample.clone();
    refreshed_unchanged.sample_id = Uuid::from_u128(2);
    refreshed_unchanged.observed_at_unix_millis = (CHECKPOINT_TEST_NOW_UNIX_SECS - 10) * 1_000;
    refreshed_unchanged.ingested_at_unix_millis = (CHECKPOINT_TEST_NOW_UNIX_SECS - 10) * 1_000;
    server
        .storage
        .record_subscription_quota_samples(&[first_sample, refreshed_unchanged])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream_id}&windows=7d_fable&source=api&max_staleness_secs=1800"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    let window = &body["windows"][0];
    assert_eq!(window["missing_capacity_upstreams"], 0);
    assert_eq!(window["stale_upstreams"], 0);
    assert_eq!(
        window["provider_lots"][0]["observed_at_unix_millis"],
        (CHECKPOINT_TEST_NOW_UNIX_SECS - 10) * 1_000
    );

    let (status, _, zero_stale_body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream_id}&windows=7d_fable&source=api&max_staleness_secs=0"
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(zero_stale_body["windows"][0]["stale_upstreams"], 1);
    assert_eq!(
        zero_stale_body["windows"][0]["provider_lots"][0]["state"],
        "stale"
    );

    let initial_aggregate = cc_lb_admin::subscription_quotas::build_cc_lb_aggregate_response(
        server.storage.as_ref(),
        &server.dynamic_view,
        Some(vec![upstream_id]),
        vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::SevenDayFable,
        ],
        cc_lb_storage_api::SubscriptionQuotaSourceMerge::Api,
        1_800,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    cc_lb_admin::subscription_quotas::record_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &initial_aggregate,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();

    let mut moved_five_hour = quota_observation(
        upstream_id,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 60,
        3,
        SubscriptionQuotaSource::Api,
        0.25,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    moved_five_hour.window = SubscriptionQuotaWindow::FiveHour;
    server
        .storage
        .record_subscription_quota_samples(&[moved_five_hour])
        .await
        .unwrap();
    let second_aggregate = cc_lb_admin::subscription_quotas::build_cc_lb_aggregate_response(
        server.storage.as_ref(),
        &server.dynamic_view,
        Some(vec![upstream_id]),
        vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::SevenDayFable,
        ],
        cc_lb_storage_api::SubscriptionQuotaSourceMerge::Api,
        1_800,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS + 60),
    )
    .await
    .unwrap();
    cc_lb_admin::subscription_quotas::record_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &second_aggregate,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS + 60),
    )
    .await
    .unwrap();

    let (status, _, history_body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/pool-history?windows=5h,7d,7d_fable&since_unix_secs={}&until_unix_secs={}&series_projection=chart",
            CHECKPOINT_TEST_NOW_UNIX_SECS,
            CHECKPOINT_TEST_NOW_UNIX_SECS + 60
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    for idx in 0..3 {
        let series = history_body["windows"][idx]["series"].as_array().unwrap();
        assert_eq!(
            series.len(),
            2,
            "every pooled window must be recorded atomically at both ticks"
        );
    }
}

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

struct AllWindowsCache;

impl cc_lb_control::SubscriptionQuotaCacheLike for AllWindowsCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        _upstream_id: Uuid,
        _now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
        SubscriptionQuotaWindow::all()
            .iter()
            .map(|window| {
                serde_json::from_value(json!({
                    "window": window.as_str(),
                    "state": "unobserved",
                    "max_staleness_secs": max_staleness_secs,
                }))
                .expect("unobserved snapshot")
            })
            .collect()
    }
}

async fn provider_quotas_list_all_oauth_kinds_and_scope_series() {
    use cc_lb_storage_api::{UpstreamCreate, UpstreamStore, upstream::UpstreamKind};

    let server = admin_test_common::spawn_admin_server().await;
    let claude = create_oauth_upstream(&server, "provider-claude").await;
    let mut ids = vec![claude];
    for (name, kind) in [
        ("provider-codex", UpstreamKind::OpenaiOauth),
        ("provider-gemini", UpstreamKind::GeminiOauth),
    ] {
        let record = UpstreamStore::create(
            server.storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("create provider fixture");
        ids.push(record.id);
    }
    let view = cc_lb_control::DynamicViewBuilder::from_view(&server.dynamic_view.load())
        .subscription_quota_cache(Arc::new(AllWindowsCache))
        .build();
    server.dynamic_view.store(view);
    let (status, _, latest) = server
        .client
        .get("/admin/v1/subscription-quotas/latest")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(latest["upstreams"].as_array().expect("upstreams").len(), 3);
    for upstream in latest["upstreams"].as_array().expect("upstreams") {
        let windows = upstream["windows"]
            .as_array()
            .expect("windows")
            .iter()
            .map(|window| window["window"].as_str().expect("window"))
            .collect::<std::collections::BTreeSet<_>>();
        let expected = match upstream["upstream_name"].as_str().expect("name") {
            "provider-claude" => std::collections::BTreeSet::from([
                "5h",
                "7d",
                "7d_sonnet",
                "7d_opus",
                "7d_fable",
                "overage",
                "unified",
            ]),
            "provider-codex" => {
                std::collections::BTreeSet::from(["5h", "7d", "overage", "unified"])
            }
            "provider-gemini" => std::collections::BTreeSet::from([
                "gemini_5h",
                "gemini_7d",
                "third_party_5h",
                "third_party_7d",
            ]),
            other => panic!("unexpected fixture {other}"),
        };
        assert_eq!(windows, expected);
    }

    let mut records = Vec::new();
    for (index, upstream) in ids.iter().enumerate() {
        for (offset, window) in [
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::GeminiFiveHour,
        ]
        .into_iter()
        .enumerate()
        {
            let mut sample = quota_observation(
                *upstream,
                120,
                (index * 2 + offset + 1) as u128,
                SubscriptionQuotaSource::Api,
                0.25,
                Some(SubscriptionQuotaStatus::Allowed),
            );
            sample.window = window;
            records.push(checkpoint_record(sample));
        }
    }
    server
        .storage
        .put_subscription_quota_checkpoints(&records)
        .await
        .expect("seed quota checkpoints");
    let (status, _, body) = server.client.get(
        "/admin/v1/subscription-quotas/series?since_unix_secs=60&until_unix_secs=180&bucket_secs=60"
    ).await;
    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().expect("series");
    let pairs = series
        .iter()
        .map(|item| {
            (
                item["upstream_name"].as_str().expect("name"),
                item["window"].as_str().expect("window"),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        pairs,
        std::collections::BTreeSet::from([
            ("provider-claude", "5h"),
            ("provider-codex", "5h"),
            ("provider-gemini", "gemini_5h"),
        ])
    );
    let (status, _, aggregate) = server
        .client
        .get("/admin/v1/subscription-quotas/aggregate")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(aggregate["upstream_count"], 1);
    for endpoint in ["aggregate", "pool-history"] {
        let (status, _, _) = server
            .client
            .get(&format!(
                "/admin/v1/subscription-quotas/{endpoint}?windows=gemini_5h"
            ))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

async fn subscription_quota_anthropic_absent_five_hour_is_excluded_from_pool_aggregate() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let absent_upstream = create_oauth_upstream(&server, "anthropic-ultra").await;
    let sampled_upstream = create_oauth_upstream(&server, "anthropic-pro").await;
    // Older header checkpoints must not resurrect a provider-absent window.
    server
        .storage
        .record_subscription_quota_samples(&[quota_observation(
            absent_upstream,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
            700,
            SubscriptionQuotaSource::Header,
            0.90,
            Some(SubscriptionQuotaStatus::AllowedWarning),
        )])
        .await
        .unwrap();
    server
        .storage
        .record_subscription_quota_samples(&[
            quota_absent_observation(
                absent_upstream,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
                701,
                SubscriptionQuotaSource::Api,
            ),
            quota_observation(
                sampled_upstream,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
                702,
                SubscriptionQuotaSource::Api,
                0.40,
                Some(SubscriptionQuotaStatus::Allowed),
            ),
        ])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/aggregate?windows=5h")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstream_count"], 2);
    assert_eq!(body["windows"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["windows"][0]["window"], "5h");
    assert_eq!(body["windows"][0]["eligible_upstreams"], 1);
    assert_eq!(body["windows"][0]["contributing_upstreams"], 1);
    assert_close(body["windows"][0]["utilization"].as_f64(), 0.40);
    assert_eq!(
        body["windows"][0]["provider_lots"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        body["windows"][0]["provider_lots"][0]["upstream_id"],
        sampled_upstream.to_string()
    );
    let aggregate = cc_lb_admin::subscription_quotas::build_cc_lb_aggregate_response(
        server.storage.as_ref(),
        &server.dynamic_view,
        None,
        vec![SubscriptionQuotaWindow::FiveHour],
        cc_lb_storage_api::SubscriptionQuotaSourceMerge::Merged,
        1_800,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    let snapshots = cc_lb_admin::subscription_quotas::pool_quota_snapshots_from_aggregate(
        &aggregate,
        CHECKPOINT_TEST_NOW_UNIX_SECS as i64,
        CHECKPOINT_TEST_NOW_UNIX_SECS as i64 * 1_000,
    );
    let snapshot = snapshots.first().expect("one eligible pooled window");
    assert_eq!(snapshot.eligible_upstreams, 1);
    assert_eq!(snapshot.contributing_upstreams, 1);
    assert_eq!(snapshot.missing_observation_upstreams, 0);
    assert_eq!(snapshot.utilization, Some(0.40));
}

async fn subscription_quota_anthropic_all_absent_five_hour_drops_from_pool_outputs() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let first = create_oauth_upstream(&server, "anthropic-ultra-a").await;
    let second = create_oauth_upstream(&server, "anthropic-ultra-b").await;
    server
        .storage
        .record_subscription_quota_samples(&[
            quota_absent_observation(
                first,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
                703,
                SubscriptionQuotaSource::Api,
            ),
            quota_absent_observation(
                second,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
                704,
                SubscriptionQuotaSource::Api,
            ),
        ])
        .await
        .unwrap();
    let mut weekly = quota_observation(
        first,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
        705,
        SubscriptionQuotaSource::Api,
        0.60,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    weekly.window = SubscriptionQuotaWindow::SevenDay;
    server
        .storage
        .record_subscription_quota_samples(&[weekly])
        .await
        .unwrap();

    let (status, _, aggregate) = server
        .client
        .get("/admin/v1/subscription-quotas/aggregate?windows=5h,7d")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        aggregate["windows"]
            .as_array()
            .expect("aggregate windows")
            .iter()
            .all(|window| window["window"] != "5h")
    );
    assert_eq!(aggregate["windows"].as_array().map(Vec::len), Some(1));
    assert_eq!(aggregate["windows"][0]["window"], "7d");
    assert_close(aggregate["windows"][0]["utilization"].as_f64(), 0.60);
    let oauth = cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response(
        server.storage.as_ref(),
        &server.dynamic_view,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    assert!(oauth.five_hour.is_none());
    assert!(oauth.seven_day.is_some());

    let (status, _, history) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/pool-history?windows=5h,7d&since_unix_secs={}&until_unix_secs={}",
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        history["windows"]
            .as_array()
            .expect("pool history windows")
            .iter()
            .all(|window| window["window"] != "5h")
    );
}

async fn subscription_quota_five_hour_absence_honors_requested_source() {
    struct CachedQuota(Uuid, cc_lb_domain::SubscriptionQuotaCandidateSnapshot);
    impl cc_lb_control::SubscriptionQuotaCacheLike for CachedQuota {
        fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {
            panic!("read-only cache fixture");
        }

        fn snapshot_for_upstream(
            &self,
            upstream_id: Uuid,
            _now_unix_millis: u64,
            _max_staleness_secs: u64,
        ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
            if upstream_id == self.0 {
                vec![self.1.clone()]
            } else {
                Vec::new()
            }
        }
    }
    let now = CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600;
    let server =
        admin_test_common::spawn_admin_server_with_clock(Arc::new(TestClock::new_at_secs(now)))
            .await;
    let upstream = create_oauth_upstream(&server, "source-specific-absence").await;
    server
        .storage
        .record_subscription_quota_samples(&[
            quota_observation_with_reset(
                upstream,
                now - 120,
                800,
                SubscriptionQuotaSource::Header,
                0.40,
                now + 3_600,
            ),
            quota_absent_observation(upstream, now - 60, 801, SubscriptionQuotaSource::Api),
        ])
        .await
        .unwrap();
    server
        .storage
        .append_request_event(&usage_event(
            now - 180,
            "source-specific-usage",
            upstream,
            "source-specific-absence",
            250,
        ))
        .await
        .unwrap();
    server.storage.rollup_usage_once().await.unwrap();

    for newer_header in [false, true] {
        if newer_header {
            server
                .storage
                .record_subscription_quota_samples(&[quota_observation_with_reset(
                    upstream,
                    now - 30,
                    802,
                    SubscriptionQuotaSource::Header,
                    0.50,
                    now + 3_600,
                )])
                .await
                .unwrap();
        }
        for cached in [false, true] {
            let cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike> = if cached {
                let snapshot = serde_json::from_value(json!({
                    "window": "5h",
                    "state": if newer_header { "fresh" } else { "absent" },
                    "source": if newer_header { "header" } else { "api" },
                    "utilization": newer_header.then_some(0.50),
                    "resets_at_unix_secs": newer_header.then_some(now + 3_600),
                    "observed_at_unix_millis": (now - if newer_header { 30 } else { 60 }) * 1_000,
                    "max_staleness_secs": 1_800,
                }))
                .expect("merged quota cache fixture");
                Arc::new(CachedQuota(upstream, snapshot))
            } else {
                Arc::new(cc_lb_control::NoopSubscriptionQuotaCache)
            };
            let next = cc_lb_control::DynamicViewBuilder::from_view(&server.dynamic_view.load())
                .subscription_quota_cache(cache)
                .build();
            server.dynamic_view.store(next);
            for source in ["header", "api", "merged"] {
                let (status, _, body) = server
                    .client
                    .get(&format!(
                        "/admin/v1/subscription-quotas/aggregate?windows=5h&source={source}"
                    ))
                    .await;
                assert_eq!(status, StatusCode::OK);
                let windows = body["windows"].as_array().expect("aggregate windows");
                if source == "api" || (source == "merged" && !newer_header) {
                    assert!(
                        windows.is_empty(),
                        "source={source}, newer_header={newer_header}"
                    );
                    continue;
                }
                assert_eq!(
                    windows.len(),
                    1,
                    "source={source}, newer_header={newer_header}"
                );
                let window = &windows[0];
                assert_eq!(window["eligible_upstreams"], 1);
                assert_eq!(window["contributing_upstreams"], 1);
                assert_eq!(window["used_tokens"], 250);
                assert_close(
                    window["utilization"].as_f64(),
                    if newer_header { 0.50 } else { 0.40 },
                );
                let lots = window["provider_lots"].as_array().expect("provider lots");
                assert_eq!(lots.len(), 1);
                assert_eq!(
                    lots[0]["source"],
                    if source == "merged" {
                        "merged"
                    } else {
                        "header"
                    }
                );
                assert_close(
                    lots[0]["capacity_estimate_tokens"].as_f64(),
                    if newer_header { 500.0 } else { 625.0 },
                );
            }
        }
    }
}

async fn subscription_quota_openai_pool_uses_equal_mean_and_provider_isolation() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let openai_a = create_kind_oauth_upstream(&server, "openai-a", "openai_oauth").await;
    let openai_b = create_kind_oauth_upstream(&server, "openai-b", "openai_oauth").await;
    let claude = create_oauth_upstream(&server, "claude-isolated").await;

    let mut openai_a_five = quota_observation(
        openai_a,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
        101,
        SubscriptionQuotaSource::Api,
        0.20,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    openai_a_five.window = SubscriptionQuotaWindow::FiveHour;
    let mut openai_a_seven = openai_a_five.clone();
    openai_a_seven.sample_id = Uuid::from_u128(102);
    openai_a_seven.window = SubscriptionQuotaWindow::SevenDay;
    openai_a_seven.utilization = Some(0.40);
    let mut openai_b_seven = openai_a_five.clone();
    openai_b_seven.sample_id = Uuid::from_u128(103);
    openai_b_seven.upstream_id = openai_b;
    openai_b_seven.window = SubscriptionQuotaWindow::SevenDay;
    openai_b_seven.utilization = Some(0.20);
    let mut claude_five = openai_a_five.clone();
    claude_five.sample_id = Uuid::from_u128(104);
    claude_five.upstream_id = claude;
    claude_five.utilization = Some(0.99);

    server
        .storage
        .record_subscription_quota_samples(&[
            openai_a_five,
            openai_a_seven,
            openai_b_seven,
            claude_five,
        ])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/aggregate?provider=openai&windows=5h,7d&source=api")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["weighting"], "equal");
    assert_eq!(body["upstream_count"], 2);
    assert_close(body["windows"][0]["utilization"].as_f64(), 0.20);
    assert_close(body["windows"][1]["utilization"].as_f64(), 0.30);
    assert_eq!(
        body["windows"][1]["provider_lots"]
            .as_array()
            .expect("openai provider lots")
            .len(),
        2
    );
    assert!(
        body["windows"]
            .as_array()
            .expect("openai windows")
            .iter()
            .flat_map(|window| window["provider_lots"].as_array().into_iter().flatten())
            .all(|lot| lot["upstream_name"] != "claude-isolated")
    );
    cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &server.dynamic_view,
        Provider::Openai,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    let (status, _, history) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/pool-history?provider=openai&windows=5h,7d&since_unix_secs={}&until_unix_secs={}&series_projection=chart",
            CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(history["weighting"], "equal");
    assert!(
        history["windows"]
            .as_array()
            .expect("openai history windows")
            .iter()
            .all(|window| window["series"]
                .as_array()
                .is_some_and(|series| !series.is_empty()))
    );
    assert!(
        history["windows"][0]["series"]
            .as_array()
            .expect("openai 5h history")
            .iter()
            .any(|point| {
                point["utilization_percent"]
                    .as_f64()
                    .is_some_and(|value| (value - 20.0).abs() < 1e-9)
            })
    );
    assert!(
        history["windows"][1]["series"]
            .as_array()
            .expect("openai 7d history")
            .iter()
            .any(|point| {
                point["utilization_percent"]
                    .as_f64()
                    .is_some_and(|value| (value - 30.0).abs() < 1e-9)
            })
    );
    assert!(
        history["windows"]
            .as_array()
            .expect("openai history windows")
            .iter()
            .flat_map(|window| window["series"].as_array().into_iter().flatten())
            .all(|point| point["utilization_percent"] != 99.0)
    );
}

async fn subscription_quota_gemini_pool_uses_equal_mean() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let gemini_a = create_kind_oauth_upstream(&server, "gemini-a", "gemini_oauth").await;
    let gemini_b = create_kind_oauth_upstream(&server, "gemini-b", "gemini_oauth").await;
    let mut gemini_a_5h = quota_observation(
        gemini_a,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
        111,
        SubscriptionQuotaSource::Api,
        0.20,
        Some(SubscriptionQuotaStatus::Allowed),
    );
    gemini_a_5h.window = SubscriptionQuotaWindow::GeminiFiveHour;
    let mut gemini_b_5h = gemini_a_5h.clone();
    gemini_b_5h.sample_id = Uuid::from_u128(112);
    gemini_b_5h.upstream_id = gemini_b;
    gemini_b_5h.utilization = Some(0.80);
    let mut gemini_a_7d = gemini_a_5h.clone();
    gemini_a_7d.sample_id = Uuid::from_u128(113);
    gemini_a_7d.window = SubscriptionQuotaWindow::GeminiSevenDay;
    gemini_a_7d.utilization = Some(0.40);
    let mut gemini_b_7d = gemini_a_7d.clone();
    gemini_b_7d.sample_id = Uuid::from_u128(114);
    gemini_b_7d.upstream_id = gemini_b;
    gemini_b_7d.utilization = Some(0.60);
    let mut third_party_5h = gemini_a_5h.clone();
    third_party_5h.sample_id = Uuid::from_u128(115);
    third_party_5h.window = SubscriptionQuotaWindow::ThirdPartyFiveHour;
    third_party_5h.utilization = Some(0.60);
    let mut third_party_7d = gemini_a_7d.clone();
    third_party_7d.sample_id = Uuid::from_u128(116);
    third_party_7d.window = SubscriptionQuotaWindow::ThirdPartySevenDay;
    third_party_7d.utilization = Some(0.70);

    server
        .storage
        .record_subscription_quota_samples(&[
            gemini_a_5h,
            gemini_b_5h,
            gemini_a_7d,
            gemini_b_7d,
            third_party_5h,
            third_party_7d,
        ])
        .await
        .unwrap();

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/aggregate?provider=gemini")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["weighting"], "equal");
    assert_eq!(body["upstream_count"], 2);
    assert_eq!(body["windows"].as_array().map(Vec::len), Some(4));
    assert_close(body["windows"][0]["utilization"].as_f64(), 0.50);
    assert_close(body["windows"][1]["utilization"].as_f64(), 0.50);
    assert_close(body["windows"][2]["utilization"].as_f64(), 0.60);
    assert_close(body["windows"][3]["utilization"].as_f64(), 0.70);
    for (index, expected_reset) in [
        CHECKPOINT_TEST_NOW_UNIX_SECS + 18_000,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 489_600,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 18_000,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 489_600,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            body["windows"][index]["cc_window_reset_unix_secs"],
            expected_reset
        );
    }
    cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &server.dynamic_view,
        Provider::Gemini,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    let (status, _, history) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/pool-history?provider=gemini&since_unix_secs={}&until_unix_secs={}&series_projection=chart",
            CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(history["weighting"], "equal");
    assert_eq!(history["windows"].as_array().map(Vec::len), Some(4));
    assert_eq!(history["windows"][0]["window"], "gemini_5h");
    assert_eq!(history["windows"][1]["window"], "gemini_7d");
    assert_eq!(history["windows"][2]["window"], "third_party_5h");
    assert_eq!(history["windows"][3]["window"], "third_party_7d");
    for (index, expected_utilization) in [(0, 50.0), (1, 50.0), (2, 60.0), (3, 70.0)] {
        assert!(
            history["windows"][index]["series"]
                .as_array()
                .expect("Gemini history")
                .iter()
                .any(|point| point["utilization_percent"] == expected_utilization),
            "window index {index} has no expected utilization"
        );
    }
}

async fn subscription_quota_pool_rejects_unknown_and_cross_provider_windows() {
    let server = admin_test_common::spawn_admin_server().await;
    for path in [
        "/admin/v1/subscription-quotas/aggregate?provider=wat",
        "/admin/v1/subscription-quotas/aggregate?provider=openai&windows=gemini_5h",
        "/admin/v1/subscription-quotas/pool-history?provider=gemini&windows=5h",
    ] {
        let (status, _, _) = server.client.get(path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "path={path}");
    }
}

async fn create_kind_oauth_upstream(
    server: &admin_test_common::SpawnedAdminServer,
    name: &str,
    kind: &str,
) -> Uuid {
    use cc_lb_storage_api::{UpstreamCreate, UpstreamStore, upstream::UpstreamKind};

    let kind = match kind {
        "openai_oauth" => UpstreamKind::OpenaiOauth,
        "gemini_oauth" => UpstreamKind::GeminiOauth,
        other => panic!("unexpected provider fixture kind: {other}"),
    };
    UpstreamStore::create(
        server.storage.as_ref(),
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind,
            base_url: None,
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    .expect("create provider fixture")
    .id
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

fn quota_absent_observation(
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    sample_seed: u128,
    source: SubscriptionQuotaSource,
) -> SubscriptionQuotaSample {
    let mut sample = quota_observation(
        upstream_id,
        observed_at_unix_secs,
        sample_seed,
        source,
        0.0,
        None,
    );
    sample.sample_kind = SubscriptionQuotaSampleKind::Absent;
    sample.utilization = None;
    sample.resets_at_unix_secs = None;
    sample
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
        credits: None,
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
        event_id: Some(format!("event-{request_id}")),
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

async fn subscription_quota_anthropic_five_hour_absence_is_not_unknown_and_is_reversible() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let upstream = create_oauth_upstream(&server, "anthropic-changing-window").await;
    let path = format!(
        "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream}&windows=5h&max_staleness_secs=0"
    );
    // Never observed is unknown, not an absent quota.
    let (status, _, unknown) = server.client.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(unknown["windows"].as_array().map(Vec::len), Some(1));

    server
        .storage
        .record_subscription_quota_samples(&[quota_absent_observation(
            upstream,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            710,
            SubscriptionQuotaSource::Api,
        )])
        .await
        .unwrap();
    // Explicit absence remains authoritative when its observation ages.
    let (status, _, absent) = server.client.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(absent["windows"].as_array().map(Vec::len), Some(0));

    server
        .storage
        .record_subscription_quota_samples(&[quota_observation(
            upstream,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 30,
            711,
            SubscriptionQuotaSource::Api,
            0.0,
            Some(SubscriptionQuotaStatus::Allowed),
        )])
        .await
        .unwrap();
    // A returned sample, including a stale 0%, restores normal 5h behavior.
    let (status, _, restored) = server.client.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(restored["windows"].as_array().map(Vec::len), Some(1));
    assert_eq!(restored["windows"][0]["eligible_upstreams"], 1);
    assert_close(restored["windows"][0]["utilization"].as_f64(), 0.0);
    assert_eq!(restored["windows"][0]["stale_upstreams"], 1);
}

async fn provider_pool_history_does_not_refresh_stale_checkpoint_readings() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    for (provider, kind, window) in [
        ("openai", "openai_oauth", SubscriptionQuotaWindow::FiveHour),
        (
            "gemini",
            "gemini_oauth",
            SubscriptionQuotaWindow::GeminiFiveHour,
        ),
    ] {
        let upstream = create_kind_oauth_upstream(&server, provider, kind).await;
        let mut sample = quota_observation(
            upstream,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 3_600,
            if provider == "openai" { 8_001 } else { 8_002 },
            SubscriptionQuotaSource::Api,
            0.50,
            Some(SubscriptionQuotaStatus::Allowed),
        );
        sample.window = window;
        server
            .storage
            .record_subscription_quota_samples(&[sample])
            .await
            .unwrap();
        let (status, _, body) = server
            .client
            .get(&format!(
                "/admin/v1/subscription-quotas/pool-history?provider={provider}&windows={}&since_unix_secs={}&until_unix_secs={}",
                window.as_str(),
                CHECKPOINT_TEST_NOW_UNIX_SECS - 3_600,
                CHECKPOINT_TEST_NOW_UNIX_SECS,
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let latest = &body["windows"][0]["latest"];
        assert_eq!(latest["stale_upstreams"], 1, "{provider}: {body}");
        assert!(latest["utilization"].is_null(), "{provider}: {body}");
        assert_eq!(latest["contributing_upstreams"], 0, "{provider}: {body}");
    }
}

async fn provider_pool_history_freshness_boundary_recovery_and_projection() {
    for (provider, name, kind, windows) in [
        (
            Provider::Openai,
            "openai",
            "openai_oauth",
            vec![
                SubscriptionQuotaWindow::FiveHour,
                SubscriptionQuotaWindow::SevenDay,
            ],
        ),
        (
            Provider::Gemini,
            "gemini",
            "gemini_oauth",
            vec![
                SubscriptionQuotaWindow::GeminiFiveHour,
                SubscriptionQuotaWindow::GeminiSevenDay,
                SubscriptionQuotaWindow::ThirdPartyFiveHour,
                SubscriptionQuotaWindow::ThirdPartySevenDay,
            ],
        ),
    ] {
        let clock = Arc::new(TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS));
        let server = admin_test_common::spawn_admin_server_with_clock(clock.clone()).await;
        let upstream = create_kind_oauth_upstream(&server, name, kind).await;
        let samples = windows
            .iter()
            .enumerate()
            .map(|(index, window)| {
                let mut sample = quota_observation(
                    upstream,
                    CHECKPOINT_TEST_NOW_UNIX_SECS - 1_800,
                    9_000 + index as u128,
                    SubscriptionQuotaSource::Api,
                    0.50,
                    Some(SubscriptionQuotaStatus::Allowed),
                );
                sample.window = *window;
                sample
            })
            .collect::<Vec<_>>();
        server
            .storage
            .record_subscription_quota_samples(&samples)
            .await
            .unwrap();
        let path = format!(
            "/admin/v1/subscription-quotas/pool-history?provider={name}&since_unix_secs={}&until_unix_secs={}",
            CHECKPOINT_TEST_NOW_UNIX_SECS - 3_600,
            CHECKPOINT_TEST_NOW_UNIX_SECS + 10,
        );
        let (status, _, boundary) = server.client.get(&path).await;
        assert_eq!(status, StatusCode::OK);
        for window in boundary["windows"].as_array().unwrap() {
            assert_close(window["latest"]["utilization"].as_f64(), 0.50);
            assert_eq!(window["latest"]["stale_upstreams"], 0);
            assert_eq!(window["latest"]["contributing_upstreams"], 1);
            assert!(
                window["series"].as_array().unwrap().is_empty(),
                "no checkpoint backfill"
            );
        }
        cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
            server.storage.as_ref(),
            &server.dynamic_view,
            provider,
            clock.as_ref(),
        )
        .await
        .unwrap();
        clock.advance(std::time::Duration::from_millis(1));
        let (_, _, stale) = server.client.get(&path).await;
        for window in stale["windows"].as_array().unwrap() {
            assert!(window["latest"]["utilization"].is_null());
            assert_eq!(window["latest"]["stale_upstreams"], 1);
            assert_eq!(window["latest"]["contributing_upstreams"], 0);
            assert_eq!(
                window["latest"]["oldest_observed_at_unix_millis"],
                (CHECKPOINT_TEST_NOW_UNIX_SECS - 1_800) * 1_000
            );
        }
        // Different seconds retain the state transition in the snapshot key.
        clock.advance_secs(1);
        cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
            server.storage.as_ref(),
            &server.dynamic_view,
            provider,
            clock.as_ref(),
        )
        .await
        .unwrap();
        clock.advance_secs(1);
        let recovered = samples
            .iter()
            .cloned()
            .map(|mut sample| {
                sample.observed_at_unix_millis = CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000 + 2_001;
                sample.ingested_at_unix_millis = sample.observed_at_unix_millis;
                sample.sample_id = Uuid::new_v4();
                sample
            })
            .collect::<Vec<_>>();
        server
            .storage
            .record_subscription_quota_samples(&recovered)
            .await
            .unwrap();
        let checkpoints = server
            .storage
            .list_subscription_quota_slim_checkpoints(
                cc_lb_storage_api::SubscriptionQuotaCheckpointRangeQuery {
                    upstream_ids: vec![upstream],
                    windows: windows.clone(),
                    sources: vec![SubscriptionQuotaSource::Api],
                    since_unix_millis: 0,
                    until_unix_millis: (CHECKPOINT_TEST_NOW_UNIX_SECS + 10) * 1_000,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            checkpoints.len(),
            windows.len(),
            "unchanged recovery is deadbanded"
        );
        cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
            server.storage.as_ref(),
            &server.dynamic_view,
            provider,
            clock.as_ref(),
        )
        .await
        .unwrap();
        let (_, _, full) = server.client.get(&path).await;
        let (_, _, chart) = server
            .client
            .get(&format!("{path}&series_projection=chart"))
            .await;
        for (full, chart) in full["windows"]
            .as_array()
            .unwrap()
            .iter()
            .zip(chart["windows"].as_array().unwrap())
        {
            assert_eq!(full["latest"], chart["latest"]);
            assert_close(full["latest"]["utilization"].as_f64(), 0.50);
            assert_eq!(full["latest"]["stale_upstreams"], 0);
            let full_series = full["series"].as_array().unwrap();
            let chart_series = chart["series"].as_array().unwrap();
            assert_eq!(
                full_series.len(),
                3,
                "fresh/stale/recovered all persist immediately"
            );
            assert_eq!(full_series.len(), chart_series.len());
            for (full, chart) in full_series.iter().zip(chart_series) {
                assert_eq!(
                    full["snapshot_at_unix_secs"],
                    chart["snapshot_at_unix_secs"]
                );
                assert_eq!(full["utilization_percent"], chart["utilization_percent"]);
            }
            assert!(
                full_series[1]["utilization"].is_null(),
                "recovery retains old gap"
            );
        }
        let (_, _, bucketed) = server
            .client
            .get(&format!(
                "{path}&series_projection=chart&max_points_per_series=2"
            ))
            .await;
        for window in bucketed["windows"].as_array().unwrap() {
            assert!(
                window["series"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|point| point["utilization_percent"].is_null()),
                "bucket preserves stale gap"
            );
            assert_close(window["latest"]["utilization"].as_f64(), 0.50);
        }
    }
}

async fn provider_pool_history_members_windows_missing_and_absent_are_independent() {
    for (name, kind, first, second) in [
        (
            "openai",
            "openai_oauth",
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (
            "gemini",
            "gemini_oauth",
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ),
    ] {
        let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
        let stale = create_kind_oauth_upstream(&server, "stale", kind).await;
        let fresh = create_kind_oauth_upstream(&server, "fresh", kind).await;
        let missing = create_kind_oauth_upstream(&server, "missing", kind).await;
        let absent = create_kind_oauth_upstream(&server, "absent", kind).await;
        let disabled = create_kind_oauth_upstream(&server, "disabled", kind).await;
        let disabled_record =
            cc_lb_storage_api::UpstreamStore::get_by_id(server.storage.as_ref(), disabled)
                .await
                .unwrap()
                .unwrap();
        cc_lb_storage_api::UpstreamStore::set_enabled(
            server.storage.as_ref(),
            disabled,
            disabled_record.revision,
            false,
        )
        .await
        .unwrap();
        let mut old = quota_observation(
            stale,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 1_801,
            10_001,
            SubscriptionQuotaSource::Api,
            0.90,
            None,
        );
        old.window = first;
        let mut current = quota_observation(
            fresh,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
            10_002,
            SubscriptionQuotaSource::Api,
            0.20,
            None,
        );
        current.window = first;
        let mut other_window = old.clone();
        other_window.sample_id = Uuid::from_u128(10_003);
        other_window.window = second;
        other_window.observed_at_unix_millis = CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000;
        other_window.utilization = Some(0.40);
        let mut no_window = quota_absent_observation(
            absent,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 3_600,
            10_004,
            SubscriptionQuotaSource::Api,
        );
        no_window.window = first;
        let mut disabled_sample = current.clone();
        disabled_sample.upstream_id = disabled;
        disabled_sample.sample_id = Uuid::from_u128(10_005);
        disabled_sample.utilization = Some(1.0);
        // Equal-time API absence wins the header's numeric sample as a whole.
        let mut absent_header = no_window.clone();
        absent_header.sample_id = Uuid::from_u128(10_006);
        absent_header.source = SubscriptionQuotaSource::Header;
        absent_header.sample_kind = SubscriptionQuotaSampleKind::Sample;
        absent_header.utilization = Some(0.80);
        server
            .storage
            .record_subscription_quota_samples(&[
                old,
                current,
                other_window,
                no_window,
                disabled_sample,
                absent_header,
            ])
            .await
            .unwrap();
        let (_, _, body) = server
            .client
            .get(&format!(
                "/admin/v1/subscription-quotas/pool-history?provider={name}&windows={},{}",
                first.as_str(),
                second.as_str(),
            ))
            .await;
        let first_window = first;
        let first = &body["windows"][0]["latest"];
        assert_close(first["utilization"].as_f64(), 0.20);
        assert_eq!(first["eligible_upstreams"], 3);
        assert_eq!(first["stale_upstreams"], 1);
        assert_eq!(first["contributing_upstreams"], 1);
        assert_eq!(first["missing_observation_upstreams"], 1, "{missing}");
        assert_eq!(
            first["max_observed_at_unix_millis"],
            CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000
        );
        assert_eq!(
            first["oldest_observed_at_unix_millis"],
            (CHECKPOINT_TEST_NOW_UNIX_SECS - 1_801) * 1_000
        );
        let second = &body["windows"][1]["latest"];
        assert_close(second["utilization"].as_f64(), 0.40);
        assert_eq!(second["eligible_upstreams"], 4);
        assert_eq!(second["contributing_upstreams"], 1);
        assert_eq!(second["stale_upstreams"], 0);
        assert_eq!(second["missing_observation_upstreams"], 3);
        let mut returned_header = quota_observation(
            absent,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
            10_007,
            SubscriptionQuotaSource::Header,
            0.80,
            None,
        );
        returned_header.window = first_window;
        server
            .storage
            .record_subscription_quota_samples(&[returned_header.clone()])
            .await
            .unwrap();
        let path = format!(
            "/admin/v1/subscription-quotas/pool-history?provider={name}&windows={}",
            first_window.as_str(),
        );
        let (_, _, returned) = server.client.get(&path).await;
        assert_close(
            returned["windows"][0]["latest"]["utilization"].as_f64(),
            0.50,
        );
        assert_eq!(returned["windows"][0]["latest"]["eligible_upstreams"], 4);
        // A same-time API observation without utilization wins as a whole,
        // instead of giving the header value a successful API poll timestamp.
        returned_header.source = SubscriptionQuotaSource::Api;
        returned_header.sample_id = Uuid::from_u128(10_008);
        returned_header.utilization = None;
        server
            .storage
            .record_subscription_quota_samples(&[returned_header])
            .await
            .unwrap();
        let (_, _, missing_value) = server.client.get(&path).await;
        assert_close(
            missing_value["windows"][0]["latest"]["utilization"].as_f64(),
            0.20,
        );
        assert_eq!(
            missing_value["windows"][0]["latest"]["contributing_upstreams"],
            1
        );
        assert_eq!(
            missing_value["windows"][0]["latest"]["missing_observation_upstreams"],
            2
        );
    }
}

async fn provider_pool_history_unchanged_successes_outlive_checkpoint_deadband() {
    for (name, kind, window) in [
        ("openai", "openai_oauth", SubscriptionQuotaWindow::FiveHour),
        (
            "gemini",
            "gemini_oauth",
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ),
    ] {
        let clock = Arc::new(TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS));
        let server = admin_test_common::spawn_admin_server_with_clock(clock.clone()).await;
        let upstream = create_kind_oauth_upstream(&server, name, kind).await;
        let path = format!(
            "/admin/v1/subscription-quotas/pool-history?provider={name}&windows={}",
            window.as_str(),
        );
        let mut sample = quota_observation(
            upstream,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
            11_000,
            SubscriptionQuotaSource::Api,
            0.50,
            None,
        );
        sample.window = window;
        for elapsed in [0, 900, 1_800, 2_700, 3_600] {
            clock.set_unix_secs(CHECKPOINT_TEST_NOW_UNIX_SECS + elapsed);
            sample.sample_id = Uuid::new_v4();
            sample.observed_at_unix_millis = (CHECKPOINT_TEST_NOW_UNIX_SECS + elapsed) * 1_000;
            sample.ingested_at_unix_millis = sample.observed_at_unix_millis;
            server
                .storage
                .record_subscription_quota_samples(&[sample.clone()])
                .await
                .unwrap();
            let (status, _, body) = server.client.get(&path).await;
            assert_eq!(status, StatusCode::OK);
            let latest = &body["windows"][0]["latest"];
            assert_close(latest["utilization"].as_f64(), 0.50);
            assert_eq!(latest["stale_upstreams"], 0);
            assert_eq!(
                latest["max_observed_at_unix_millis"],
                sample.observed_at_unix_millis
            );
            assert!(body["windows"][0]["series"].as_array().unwrap().is_empty());
        }
        let checkpoints = server
            .storage
            .list_subscription_quota_slim_checkpoints(
                cc_lb_storage_api::SubscriptionQuotaCheckpointRangeQuery {
                    upstream_ids: vec![upstream],
                    windows: vec![window],
                    sources: vec![SubscriptionQuotaSource::Api],
                    since_unix_millis: CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000,
                    until_unix_millis: (CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600) * 1_000,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            checkpoints.len(),
            1,
            "freshness advances without semantic changes"
        );
    }
}

async fn provider_pool_history_relative_range_uses_server_clock() {
    let base = CHECKPOINT_TEST_NOW_UNIX_SECS as i64;
    let server = admin_test_common::spawn_admin_server_with_clock(Arc::new(
        TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS + 4_500),
    ))
    .await;
    for (provider, name, window) in [
        (
            Provider::Openai,
            "openai",
            SubscriptionQuotaWindow::FiveHour,
        ),
        (
            Provider::Gemini,
            "gemini",
            SubscriptionQuotaWindow::GeminiFiveHour,
        ),
    ] {
        let records = [
            (-86_400, 0.10),
            (-5_400, 0.20),
            (1_800, 0.30),
            (3_600, 0.40),
        ]
        .into_iter()
        .map(|(offset, utilization)| {
            let mut record = pool_history_record(base + offset, utilization);
            record.provider = provider;
            record.window = window;
            record
        })
        .collect::<Vec<_>>();
        server
            .storage
            .record_pool_quota_snapshots(&records)
            .await
            .unwrap();
        let root = format!(
            "/admin/v1/subscription-quotas/pool-history?provider={name}&windows={}",
            window.as_str(),
        );
        // Browser time deliberately does not enter the request. Query padding
        // and the response clock must share the server's captured instant.
        for projection in ["full", "chart"] {
            let (status, _, body) = server.client.get(&format!(
                "{root}&relative_range_secs=3600&window_quantum_secs=1800&series_projection={projection}",
            )).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["now_unix_secs"], base + 4_500);
            let series = body["windows"][0]["series"].as_array().unwrap();
            assert_eq!(
                series
                    .iter()
                    .map(|point| { point["snapshot_at_unix_secs"].as_i64().unwrap() })
                    .collect::<Vec<_>>(),
                vec![base - 3_900, base + 1_800, base + 3_600],
                "relative range must resolve from server clock: {body}"
            );
            assert_eq!(series[0]["utilization_percent"], 20.0);
        }
        for quantum in ["", "&window_quantum_secs=0"] {
            let (status, _, body) = server
                .client
                .get(&format!(
                    "{root}&relative_range_secs=3600{quantum}&series_projection=chart",
                ))
                .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["now_unix_secs"], base + 4_500);
            assert_eq!(
                body["windows"][0]["series"][0]["snapshot_at_unix_secs"],
                base - 3_000
            );
        }
        // Existing absolute and omitted-range queries preserve their contract.
        let (status, _, absolute) = server
            .client
            .get(&format!(
                "{root}&since_unix_secs={}&until_unix_secs={}&series_projection=chart",
                base - 2_100,
                base + 7_200,
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            absolute["windows"][0]["series"][0]["snapshot_at_unix_secs"],
            base - 2_100
        );
        let (status, _, default) = server
            .client
            .get(&format!("{root}&series_projection=chart"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            default["windows"][0]["series"][0]["snapshot_at_unix_secs"],
            base - 17_100
        );
    }
}

async fn pool_history_relative_range_rejects_invalid_or_ambiguous_bounds() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    for provider in ["anthropic", "openai", "gemini"] {
        for suffix in [
            "relative_range_secs=0",
            "relative_range_secs=-1",
            "relative_range_secs=9223372036854775808",
            "relative_range_secs=3600&window_quantum_secs=-1",
            "relative_range_secs=3600&window_quantum_secs=9223372036854775808",
            "relative_range_secs=9223372036854775807&window_quantum_secs=9223372036854775807",
            "relative_range_secs=3600&since_unix_secs=0",
            "relative_range_secs=3600&until_unix_secs=1800000000",
        ] {
            let (status, _, body) = server
                .client
                .get(&format!(
                    "/admin/v1/subscription-quotas/pool-history?provider={provider}&{suffix}",
                ))
                .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{provider}: {suffix}: {body}"
            );
        }
    }
}

#[tokio::test]
async fn subscription_quotas_relative_history_range_suite() {
    provider_pool_history_relative_range_uses_server_clock().await;
    pool_history_relative_range_rejects_invalid_or_ambiguous_bounds().await;
}

#[tokio::test]
async fn subscription_quotas_provider_freshness_suite() {
    provider_pool_history_freshness_boundary_recovery_and_projection().await;
    provider_pool_history_members_windows_missing_and_absent_are_independent().await;
    provider_pool_history_unchanged_successes_outlive_checkpoint_deadband().await;
}

#[tokio::test]
async fn subscription_quotas_suite_part_1() {
    subscription_quota_latest_lists_registered_upstreams().await;
    subscription_quota_series_defaults_missing_upstream_ids_to_all().await;
    subscription_quota_series_defaults_exclude_stored_fable_window().await;
    subscription_quota_series_explicit_fable_window_includes_stored_series().await;
    subscription_quota_pool_history_accepts_explicit_fable_window().await;
    subscription_quota_pool_history_caps_chart_points_and_preserves_peak().await;
    subscription_quota_aggregate_refreshes_observed_at_from_latest_when_checkpoint_deadbanded()
        .await;
    subscription_quota_checkpoint_series_returns_steps_without_fabricated_leading_zeroes().await;
    subscription_quota_checkpoint_aggregate_carries_unaligned_checkpoints_to_evaluation_time()
        .await;
}

#[tokio::test]
async fn subscription_quotas_suite_part_2() {
    subscription_quota_aggregate_matches_legacy_golden_for_multiple_windows_and_boundaries().await;
    provider_quotas_list_all_oauth_kinds_and_scope_series().await;
    subscription_quota_anthropic_absent_five_hour_is_excluded_from_pool_aggregate().await;
    subscription_quota_anthropic_all_absent_five_hour_drops_from_pool_outputs().await;
    subscription_quota_five_hour_absence_honors_requested_source().await;
    subscription_quota_openai_pool_uses_equal_mean_and_provider_isolation().await;
    subscription_quota_gemini_pool_uses_equal_mean().await;
    subscription_quota_pool_rejects_unknown_and_cross_provider_windows().await;
    subscription_quota_anthropic_five_hour_absence_is_not_unknown_and_is_reversible().await;
    provider_pool_history_does_not_refresh_stale_checkpoint_readings().await;
}

#[tokio::test]
async fn subscription_quotas_absence_suite() {
    absence_eligibility_matrix().await;
    absence_tombstone_and_recovery().await;
    absence_source_cache_staleness_matrix().await;
    absence_anthropic_pool_history_and_snapshots().await;
}

struct AbsenceStaticCache(Vec<(Uuid, cc_lb_domain::SubscriptionQuotaCandidateSnapshot)>);

impl cc_lb_control::SubscriptionQuotaCacheLike for AbsenceStaticCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _now_unix_millis: u64,
        max_staleness_secs: u64,
    ) -> Vec<cc_lb_domain::SubscriptionQuotaCandidateSnapshot> {
        SubscriptionQuotaWindow::all()
            .iter()
            .map(|window| {
                self.0
                    .iter()
                    .find(|(id, snapshot)| *id == upstream_id && snapshot.window == window.as_str())
                    .map(|(_, snapshot)| snapshot.clone())
                    .unwrap_or_else(|| {
                        serde_json::from_value(json!({
                            "window": window.as_str(),
                            "state": "unobserved",
                            "max_staleness_secs": max_staleness_secs,
                        }))
                        .unwrap()
                    })
            })
            .collect()
    }
}

fn absence_snapshot(
    sample: &SubscriptionQuotaSample,
) -> cc_lb_domain::SubscriptionQuotaCandidateSnapshot {
    serde_json::from_value(json!({
        "window": sample.window.as_str(),
        "state": if sample.sample_kind == SubscriptionQuotaSampleKind::Absent { "absent" } else { "fresh" },
        "source": sample.source.as_str(),
        "utilization": sample.utilization,
        "resets_at_unix_secs": sample.resets_at_unix_secs,
        "observed_at_unix_millis": sample.observed_at_unix_millis,
        "max_staleness_secs": 1_800,
    }))
    .unwrap()
}

fn absence_set_cache(
    server: &admin_test_common::SpawnedAdminServer,
    cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike>,
) {
    let next = cc_lb_control::DynamicViewBuilder::from_view(&server.dynamic_view.load())
        .subscription_quota_cache(cache)
        .build();
    server.dynamic_view.store(next);
}

fn absence_sample(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at: u64,
    source: SubscriptionQuotaSource,
    utilization: Option<f64>,
) -> SubscriptionQuotaSample {
    let mut sample = match utilization {
        Some(value) => quota_observation(
            upstream_id,
            observed_at,
            Uuid::new_v4().as_u128(),
            source,
            value,
            Some(SubscriptionQuotaStatus::Allowed),
        ),
        None => {
            quota_absent_observation(upstream_id, observed_at, Uuid::new_v4().as_u128(), source)
        }
    };
    sample.window = window;
    sample
}

async fn absence_aggregate(
    server: &admin_test_common::SpawnedAdminServer,
    provider: &str,
    windows: &str,
    source: &str,
    max_staleness_secs: u64,
) -> serde_json::Value {
    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?provider={provider}&windows={windows}&source={source}&max_staleness_secs={max_staleness_secs}"
        ))
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{provider}/{windows}/{source}: {body}"
    );
    body
}

async fn absence_create_upstream(
    server: &admin_test_common::SpawnedAdminServer,
    provider: &str,
    name: &str,
) -> Uuid {
    if provider == "anthropic" {
        create_oauth_upstream(server, name).await
    } else {
        create_kind_oauth_upstream(server, name, &format!("{provider}_oauth")).await
    }
}

// QA-03: Absent removes eligibility; Unobserved remains eligible and missing.
async fn absence_eligibility_matrix() {
    let empty = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    let oauth = cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response(
        empty.storage.as_ref(),
        &empty.dynamic_view,
        &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
    )
    .await
    .unwrap();
    assert!(oauth.five_hour.is_some());
    assert!(oauth.seven_day.is_some());
    for (provider, target, present_twin) in [
        (
            "openai",
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ),
        (
            "gemini",
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            SubscriptionQuotaWindow::GeminiSevenDay,
        ),
        (
            "anthropic",
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaWindow::SevenDay,
        ),
    ] {
        let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
        let (status, _, defaults) = server
            .client
            .get(&format!(
                "/admin/v1/subscription-quotas/aggregate?provider={provider}"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(!defaults["windows"].as_array().unwrap().is_empty());
        for window in defaults["windows"].as_array().unwrap() {
            assert_eq!(window["eligible_upstreams"], 0);
        }
        let first = absence_create_upstream(&server, provider, "absent-member").await;
        let absent = absence_sample(
            first,
            target,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            SubscriptionQuotaSource::Api,
            None,
        );
        let twin = absence_sample(
            first,
            present_twin,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            SubscriptionQuotaSource::Api,
            Some(0.6),
        );
        server
            .storage
            .record_subscription_quota_samples(&[absent.clone(), twin])
            .await
            .unwrap();
        for cached in [false, true] {
            let cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike> = if cached {
                Arc::new(AbsenceStaticCache(vec![(first, absence_snapshot(&absent))]))
            } else {
                Arc::new(cc_lb_control::NoopSubscriptionQuotaCache)
            };
            absence_set_cache(&server, cache);
            let body = absence_aggregate(
                &server,
                provider,
                &format!("{},{}", target.as_str(), present_twin.as_str()),
                "merged",
                1_800,
            )
            .await;
            let windows = body["windows"].as_array().unwrap();
            assert_eq!(windows.len(), 1, "{provider}, cached={cached}: {body}");
            assert_eq!(windows[0]["window"], present_twin.as_str());
            assert_eq!(windows[0]["eligible_upstreams"], 1);
            assert_close(windows[0]["utilization"].as_f64(), 0.6);
        }
        let second = absence_create_upstream(&server, provider, "present-member").await;
        let present = absence_sample(
            second,
            target,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 50,
            SubscriptionQuotaSource::Api,
            Some(0.4),
        );
        server
            .storage
            .record_subscription_quota_samples(std::slice::from_ref(&present))
            .await
            .unwrap();
        let body = absence_aggregate(&server, provider, target.as_str(), "merged", 1_800).await;
        assert_eq!(body["windows"][0]["eligible_upstreams"], 1);
        assert_eq!(body["windows"][0]["contributing_upstreams"], 1);
        assert_close(body["windows"][0]["utilization"].as_f64(), 0.4);
        let second_absent = absence_sample(
            second,
            target,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 40,
            SubscriptionQuotaSource::Api,
            None,
        );
        server
            .storage
            .record_subscription_quota_samples(&[second_absent])
            .await
            .unwrap();
        let body = absence_aggregate(&server, provider, target.as_str(), "merged", 1_800).await;
        assert!(
            body["windows"].as_array().unwrap().is_empty(),
            "{provider}: {body}"
        );
        let restored = absence_sample(
            second,
            target,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 30,
            SubscriptionQuotaSource::Api,
            Some(0.4),
        );
        server
            .storage
            .record_subscription_quota_samples(std::slice::from_ref(&restored))
            .await
            .unwrap();
        let third = absence_create_upstream(&server, provider, "unobserved-member").await;
        absence_set_cache(
            &server,
            Arc::new(AbsenceStaticCache(vec![
                (first, absence_snapshot(&absent)),
                (second, absence_snapshot(&restored)),
            ])),
        );
        let body = absence_aggregate(&server, provider, target.as_str(), "merged", 1_800).await;
        let window = &body["windows"][0];
        assert_eq!(window["eligible_upstreams"], 2, "{provider}: {body}");
        assert_eq!(window["contributing_upstreams"], 1);
        assert_eq!(window["missing_capacity_upstreams"], 1);
        assert!(
            window["provider_lots"]
                .as_array()
                .unwrap()
                .iter()
                .any(|lot| {
                    lot["upstream_id"] == third.to_string() && lot["state"] == "unobserved"
                })
        );
    }
}

// QA-04: tombstones hide every historical lot but never destroy the series.
async fn absence_tombstone_and_recovery() {
    for (provider, window) in [
        ("gemini", SubscriptionQuotaWindow::GeminiSevenDay),
        ("anthropic", SubscriptionQuotaWindow::SevenDay),
        ("anthropic", SubscriptionQuotaWindow::SevenDaySonnet),
        ("anthropic", SubscriptionQuotaWindow::SevenDayOpus),
        ("anthropic", SubscriptionQuotaWindow::SevenDayFable),
    ] {
        let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
        absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
        let first = absence_create_upstream(&server, provider, "tombstoned").await;
        let mut prior_cycle = absence_sample(
            first,
            window,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 300,
            SubscriptionQuotaSource::Api,
            Some(0.2),
        );
        prior_cycle.resets_at_unix_secs = Some(CHECKPOINT_TEST_NOW_UNIX_SECS - 200);
        let numeric = absence_sample(
            first,
            window,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
            SubscriptionQuotaSource::Api,
            Some(0.6),
        );
        server
            .storage
            .record_subscription_quota_samples(&[prior_cycle, numeric.clone()])
            .await
            .unwrap();
        server
            .storage
            .append_request_event(&usage_event(
                CHECKPOINT_TEST_NOW_UNIX_SECS - 90,
                "tombstoned-usage",
                first,
                "tombstoned",
                400,
            ))
            .await
            .unwrap();
        server.storage.rollup_usage_once().await.unwrap();
        let checkpoint_query = cc_lb_storage_api::SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: vec![first],
            windows: vec![window],
            sources: vec![SubscriptionQuotaSource::Api],
            since_unix_millis: (CHECKPOINT_TEST_NOW_UNIX_SECS - 600) * 1_000,
            until_unix_millis: CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000,
        };
        let checkpoints = server
            .storage
            .list_subscription_quota_slim_checkpoints(checkpoint_query.clone())
            .await
            .unwrap();
        assert_eq!(checkpoints.len(), 2);
        let lot_query = cc_lb_storage_api::SubscriptionQuotaProviderLotQuery {
            upstream_ids: vec![first],
            windows: vec![window],
            sources: vec![SubscriptionQuotaSource::Api],
            since_unix_millis: (CHECKPOINT_TEST_NOW_UNIX_SECS - 604_800) * 1_000,
            until_unix_millis: CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000,
            source_merge: cc_lb_storage_api::SubscriptionQuotaSourceMerge::Api,
            evaluation_unix_secs: CHECKPOINT_TEST_NOW_UNIX_SECS,
        };
        assert_eq!(
            server
                .storage
                .list_subscription_quota_provider_lots(lot_query.clone())
                .await
                .unwrap()
                .len(),
            2
        );
        let series_path = format!(
            "/admin/v1/subscription-quotas/series?upstream_ids={first}&windows={}&source=api&since_unix_secs={}&until_unix_secs={}&bucket_secs=60",
            window.as_str(),
            CHECKPOINT_TEST_NOW_UNIX_SECS - 600,
            CHECKPOINT_TEST_NOW_UNIX_SECS,
        );
        let (status, _, before_series) = server.client.get(&series_path).await;
        assert_eq!(status, StatusCode::OK);
        let before = absence_aggregate(&server, provider, window.as_str(), "api", 1_800).await;
        assert_eq!(before["windows"][0]["used_tokens"], 400);
        let absent = absence_sample(
            first,
            window,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            SubscriptionQuotaSource::Api,
            None,
        );
        server
            .storage
            .record_subscription_quota_samples(&[absent])
            .await
            .unwrap();
        let after = absence_aggregate(&server, provider, window.as_str(), "api", 1_800).await;
        assert!(
            after["windows"].as_array().unwrap().is_empty(),
            "{provider}/{}: {after}",
            window.as_str()
        );
        assert_eq!(
            server
                .storage
                .list_subscription_quota_slim_checkpoints(checkpoint_query.clone())
                .await
                .unwrap(),
            checkpoints
        );
        assert_eq!(
            server
                .storage
                .list_subscription_quota_provider_lots(lot_query)
                .await
                .unwrap()
                .len(),
            2
        );
        let (status, _, after_series) = server.client.get(&series_path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after_series, before_series);
        if provider == "anthropic" {
            let oauth = cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response(
                server.storage.as_ref(),
                &server.dynamic_view,
                &TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS),
            )
            .await
            .unwrap();
            match window {
                SubscriptionQuotaWindow::SevenDay => assert!(oauth.seven_day.is_none()),
                SubscriptionQuotaWindow::SevenDaySonnet => {
                    assert!(oauth.seven_day_sonnet.is_none())
                }
                SubscriptionQuotaWindow::SevenDayOpus => assert!(oauth.seven_day_opus.is_none()),
                SubscriptionQuotaWindow::SevenDayFable => assert!(oauth.limits.is_none()),
                _ => unreachable!(),
            }
        }
        let second = absence_create_upstream(&server, provider, "surviving").await;
        server
            .storage
            .record_subscription_quota_samples(&[absence_sample(
                second,
                window,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 50,
                SubscriptionQuotaSource::Api,
                Some(0.2),
            )])
            .await
            .unwrap();
        server
            .storage
            .append_request_event(&usage_event(
                CHECKPOINT_TEST_NOW_UNIX_SECS - 90,
                "surviving-usage",
                second,
                "surviving",
                100,
            ))
            .await
            .unwrap();
        server.storage.rollup_usage_once().await.unwrap();
        let partial = absence_aggregate(&server, provider, window.as_str(), "api", 1_800).await;
        let live = &partial["windows"][0];
        assert_eq!(live["eligible_upstreams"], 1);
        assert_eq!(live["used_tokens"], 100);
        assert_eq!(live["provider_lots"].as_array().unwrap().len(), 1);
        assert_eq!(live["provider_lots"][0]["upstream_id"], second.to_string());
        let mut recovered = numeric;
        recovered.sample_id = Uuid::new_v4();
        recovered.observed_at_unix_millis = (CHECKPOINT_TEST_NOW_UNIX_SECS - 10) * 1_000;
        recovered.ingested_at_unix_millis = recovered.observed_at_unix_millis;
        server
            .storage
            .record_subscription_quota_samples(&[recovered.clone()])
            .await
            .unwrap();
        let restored = absence_aggregate(&server, provider, window.as_str(), "api", 1_800).await;
        assert_eq!(restored["windows"][0]["eligible_upstreams"], 2);
        assert_eq!(restored["windows"][0]["used_tokens"], 500);
        let revived = restored["windows"][0]["provider_lots"]
            .as_array()
            .unwrap()
            .iter()
            .find(|lot| lot["upstream_id"] == first.to_string())
            .unwrap();
        assert_close(revived["utilization"].as_f64(), 0.6);
        assert_eq!(
            revived["observed_at_unix_millis"],
            recovered.observed_at_unix_millis
        );
        assert_eq!(
            server
                .storage
                .list_subscription_quota_slim_checkpoints(checkpoint_query)
                .await
                .unwrap(),
            checkpoints,
            "same numeric recovery stays deadbanded"
        );
    }
}

// QA-05: source filtering and stale absence are identical with and without cache.
async fn absence_source_cache_staleness_matrix() {
    for (provider, window) in [
        ("openai", SubscriptionQuotaWindow::SevenDay),
        ("gemini", SubscriptionQuotaWindow::GeminiSevenDay),
    ] {
        let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
        let upstream = absence_create_upstream(&server, provider, "source-matrix").await;
        let header = absence_sample(
            upstream,
            window,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
            SubscriptionQuotaSource::Header,
            Some(0.6),
        );
        let absent = absence_sample(
            upstream,
            window,
            CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
            SubscriptionQuotaSource::Api,
            None,
        );
        server
            .storage
            .record_subscription_quota_samples(&[header, absent.clone()])
            .await
            .unwrap();
        for phase in 0..3 {
            let latest = match phase {
                0 => absent.clone(),
                1 => absence_sample(
                    upstream,
                    window,
                    CHECKPOINT_TEST_NOW_UNIX_SECS - 30,
                    SubscriptionQuotaSource::Header,
                    Some(0.6),
                ),
                _ => absence_sample(
                    upstream,
                    window,
                    CHECKPOINT_TEST_NOW_UNIX_SECS - 10,
                    SubscriptionQuotaSource::Api,
                    Some(0.6),
                ),
            };
            if phase > 0 {
                server
                    .storage
                    .record_subscription_quota_samples(std::slice::from_ref(&latest))
                    .await
                    .unwrap();
            }
            for cached in [false, true] {
                let cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike> = if cached {
                    Arc::new(AbsenceStaticCache(vec![(
                        upstream,
                        absence_snapshot(&latest),
                    )]))
                } else {
                    Arc::new(cc_lb_control::NoopSubscriptionQuotaCache)
                };
                absence_set_cache(&server, cache);
                for source in ["header", "api", "merged"] {
                    for max_staleness_secs in [1_800, 0] {
                        let body = absence_aggregate(
                            &server,
                            provider,
                            window.as_str(),
                            source,
                            max_staleness_secs,
                        )
                        .await;
                        let omitted =
                            (source == "api" && phase < 2) || (source == "merged" && phase == 0);
                        let windows = body["windows"].as_array().unwrap();
                        assert_eq!(
                            windows.is_empty(),
                            omitted,
                            "{provider}, phase={phase}, cached={cached}, source={source}, max_staleness_secs={max_staleness_secs}: {body}"
                        );
                        if !omitted {
                            assert_eq!(windows[0]["eligible_upstreams"], 1);
                            assert_close(windows[0]["utilization"].as_f64(), 0.6);
                        }
                    }
                }
            }
        }
    }
}

// QA-06: Anthropic drops all-absent pooled windows without changing other providers.
async fn absence_anthropic_pool_history_and_snapshots() {
    let clock = Arc::new(TestClock::new_at_secs(CHECKPOINT_TEST_NOW_UNIX_SECS));
    let server = admin_test_common::spawn_admin_server_with_clock(clock.clone()).await;
    absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
    let first = create_oauth_upstream(&server, "history-first").await;
    let second = create_oauth_upstream(&server, "history-second").await;
    let mut samples = Vec::new();
    for id in [first, second] {
        for window in [
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDayFable,
        ] {
            samples.push(absence_sample(
                id,
                window,
                CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
                SubscriptionQuotaSource::Api,
                None,
            ));
        }
    }
    samples.push(absence_sample(
        first,
        SubscriptionQuotaWindow::SevenDay,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
        SubscriptionQuotaSource::Api,
        None,
    ));
    samples.push(absence_sample(
        second,
        SubscriptionQuotaWindow::SevenDay,
        CHECKPOINT_TEST_NOW_UNIX_SECS - 60,
        SubscriptionQuotaSource::Api,
        Some(0.4),
    ));
    server
        .storage
        .record_subscription_quota_samples(&samples)
        .await
        .unwrap();
    let path = format!(
        "/admin/v1/subscription-quotas/pool-history?provider=anthropic&windows=5h,7d,7d_fable&since_unix_secs={}&until_unix_secs={}",
        CHECKPOINT_TEST_NOW_UNIX_SECS - 120,
        CHECKPOINT_TEST_NOW_UNIX_SECS + 120,
    );
    cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &server.dynamic_view,
        Provider::Anthropic,
        clock.as_ref(),
    )
    .await
    .unwrap();
    let (status, _, history) = server.client.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    let windows = history["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 1, "{history}");
    assert_eq!(windows[0]["window"], "7d");
    assert_eq!(windows[0]["latest"]["eligible_upstreams"], 1);
    assert_close(windows[0]["latest"]["utilization"].as_f64(), 0.4);
    let stored = server
        .storage
        .list_latest_pool_quota_snapshot_summaries(
            Provider::Anthropic,
            &[
                SubscriptionQuotaWindow::FiveHour,
                SubscriptionQuotaWindow::SevenDay,
                SubscriptionQuotaWindow::SevenDayFable,
            ],
        )
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].window, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(stored[0].eligible_upstreams, 1);
    clock.advance_secs(60);
    server
        .storage
        .record_subscription_quota_samples(&[
            absence_sample(
                first,
                SubscriptionQuotaWindow::SevenDayFable,
                CHECKPOINT_TEST_NOW_UNIX_SECS + 60,
                SubscriptionQuotaSource::Api,
                Some(0.6),
            ),
            absence_sample(
                first,
                SubscriptionQuotaWindow::SevenDay,
                CHECKPOINT_TEST_NOW_UNIX_SECS + 60,
                SubscriptionQuotaSource::Api,
                Some(0.2),
            ),
        ])
        .await
        .unwrap();
    cc_lb_admin::subscription_quotas::record_provider_pool_quota_snapshots_now(
        server.storage.as_ref(),
        &server.dynamic_view,
        Provider::Anthropic,
        clock.as_ref(),
    )
    .await
    .unwrap();
    let (status, _, recovered) = server.client.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    let windows = recovered["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 2);
    let weekly = windows
        .iter()
        .find(|window| window["window"] == "7d")
        .unwrap();
    assert_eq!(weekly["latest"]["eligible_upstreams"], 2);
    assert_close(weekly["latest"]["utilization"].as_f64(), 0.3);
    assert_eq!(weekly["series"].as_array().unwrap().len(), 2);
    let fable = windows
        .iter()
        .find(|window| window["window"] == "7d_fable")
        .unwrap();
    assert_eq!(fable["latest"]["eligible_upstreams"], 1);
    assert_close(fable["latest"]["utilization"].as_f64(), 0.6);
    assert_eq!(fable["series"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn subscription_quotas_period_unknown_suite() {
    latest_period_unknown_meters_contract().await;
    latest_period_unknown_meters_follow_filters().await;
    latest_meters_survive_credential_rotation().await;
}

fn period_unknown_record(observed_at_unix_millis: u64) -> serde_json::Value {
    json!({
        "version": 1,
        "gemini": {
            "observed_at_unix_millis": observed_at_unix_millis,
            "meters": [{
                "group": "gemini",
                "model_id": "gemini-2.5-pro",
                "remaining_fraction": 0.4,
                "reset_time_unix_secs": CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600,
                "origin": "retrieve_user_quota",
            }],
        },
        "third_party": {
            "observed_at_unix_millis": observed_at_unix_millis - 60_000,
            "meters": [{
                "group": "third_party",
                "bucket_id": "unlabeled-0",
                "remaining_fraction": 0.25,
                "reset_time_unix_secs": null,
                "remaining_amount": "12",
                "origin": "retrieve_user_quota_summary",
            }],
        },
    })
}

async fn period_unknown_seed(
    server: &admin_test_common::SpawnedAdminServer,
    upstream_id: Uuid,
    value: &str,
) {
    use cc_lb_storage_api::MetaStore;

    server
        .storage
        .put_meta_value(
            &cc_lb_control::gemini_period_unknown_quota::gemini_period_unknown_quota_meta_key(
                upstream_id,
            ),
            value,
        )
        .await
        .unwrap();
}

async fn period_unknown_latest(
    server: &admin_test_common::SpawnedAdminServer,
    query: &str,
) -> serde_json::Value {
    let (status, _, body) = server
        .client
        .get(&format!("/admin/v1/subscription-quotas/latest?{query}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    body
}

fn period_unknown_upstream(body: &serde_json::Value, upstream_id: Uuid) -> &serde_json::Value {
    body["upstreams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|upstream| upstream["upstream_id"] == upstream_id.to_string())
        .unwrap()
}

// QA-12: raw API meters are display-only, credential-independent, and fenced per group.
async fn latest_period_unknown_meters_contract() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
    let gemini = create_kind_oauth_upstream(&server, "raw-gemini", "gemini_oauth").await;
    let anthropic = create_oauth_upstream(&server, "raw-anthropic").await;
    let empty = create_kind_oauth_upstream(&server, "raw-empty", "gemini_oauth").await;
    let aggregate_without_row =
        absence_aggregate(&server, "gemini", "gemini_5h,third_party_5h", "merged", 60).await;
    let observed_at = (CHECKPOINT_TEST_NOW_UNIX_SECS - 60) * 1_000;
    let record = period_unknown_record(observed_at).to_string();
    period_unknown_seed(&server, gemini, &record).await;
    period_unknown_seed(&server, anthropic, &record).await;
    let body = period_unknown_latest(&server, "max_staleness_secs=60").await;
    let meters = period_unknown_upstream(&body, gemini)["period_unknown_meters"]
        .as_array()
        .unwrap();
    assert_eq!(meters.len(), 2);
    assert_eq!(meters[0]["group"], "gemini");
    assert_eq!(meters[0]["model_id"], "gemini-2.5-pro");
    assert!(meters[0].get("bucket_id").is_none());
    assert!(meters[0].get("remaining_amount").is_none());
    assert_eq!(meters[0]["remaining_fraction"], 0.4);
    assert_close(meters[0]["utilization"].as_f64(), 0.6);
    assert_eq!(
        meters[0]["reset_time_unix_secs"],
        CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600
    );
    assert_eq!(meters[0]["origin"], "retrieve_user_quota");
    assert_eq!(meters[0]["source"], "api");
    assert_eq!(meters[0]["state"], "fresh");
    assert_eq!(meters[0]["observed_at_unix_millis"], observed_at);
    assert_eq!(meters[0]["age_secs"], 60);
    assert_eq!(meters[1]["group"], "third_party");
    assert_eq!(meters[1]["bucket_id"], "unlabeled-0");
    assert!(meters[1].get("model_id").is_none());
    assert_eq!(meters[1]["remaining_amount"], "12");
    assert_eq!(meters[1]["remaining_fraction"], 0.25);
    assert_eq!(meters[1]["utilization"], 0.75);
    assert!(meters[1]["reset_time_unix_secs"].is_null());
    assert_eq!(meters[1]["origin"], "retrieve_user_quota_summary");
    assert_eq!(meters[1]["source"], "api");
    assert_eq!(meters[1]["state"], "stale");
    assert_eq!(meters[1]["observed_at_unix_millis"], observed_at - 60_000);
    assert_eq!(meters[1]["age_secs"], 120);
    assert!(!body.to_string().contains("credential_fingerprint"));
    assert!(!body.to_string().contains("oauth_token_generation"));
    for upstream in [anthropic, empty] {
        assert!(
            period_unknown_upstream(&body, upstream)
                .get("period_unknown_meters")
                .is_none()
        );
    }
    assert_eq!(
        aggregate_without_row,
        absence_aggregate(&server, "gemini", "gemini_5h,third_party_5h", "merged", 60).await,
    );

    let mut same_poll_absent = absence_sample(
        gemini,
        SubscriptionQuotaWindow::GeminiFiveHour,
        observed_at / 1_000,
        SubscriptionQuotaSource::Api,
        None,
    );
    same_poll_absent.observed_at_unix_millis = observed_at;
    server
        .storage
        .record_subscription_quota_samples(std::slice::from_ref(&same_poll_absent))
        .await
        .unwrap();
    let body = period_unknown_latest(&server, "").await;
    assert_eq!(
        period_unknown_upstream(&body, gemini)["period_unknown_meters"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "same-poll durable absence must not suppress raw meters",
    );

    // Cache-only canonical data suppresses just its own group. Unobserved
    // placeholders never suppress, even when they carry a later timestamp.
    let mut third_party = same_poll_absent.clone();
    third_party.window = SubscriptionQuotaWindow::ThirdPartySevenDay;
    third_party.observed_at_unix_millis = observed_at + 1;
    let mut unobserved = absence_snapshot(&same_poll_absent);
    unobserved.state = cc_lb_domain::SubscriptionQuotaDataState::Unobserved;
    unobserved.observed_at_unix_millis = Some(observed_at + 1);
    absence_set_cache(
        &server,
        Arc::new(AbsenceStaticCache(vec![
            (gemini, absence_snapshot(&third_party)),
            (gemini, unobserved),
        ])),
    );
    let body = period_unknown_latest(&server, "").await;
    let meters = period_unknown_upstream(&body, gemini)["period_unknown_meters"]
        .as_array()
        .unwrap();
    assert_eq!(meters.len(), 1);
    assert_eq!(meters[0]["group"], "gemini");

    absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
    let mut newer_canonical = same_poll_absent;
    newer_canonical.sample_id = Uuid::new_v4();
    newer_canonical.observed_at_unix_millis = observed_at + 1;
    newer_canonical.sample_kind = SubscriptionQuotaSampleKind::Sample;
    newer_canonical.utilization = Some(0.5);
    server
        .storage
        .record_subscription_quota_samples(&[newer_canonical])
        .await
        .unwrap();
    let body = period_unknown_latest(&server, "").await;
    let meters = period_unknown_upstream(&body, gemini)["period_unknown_meters"]
        .as_array()
        .unwrap();
    assert_eq!(meters.len(), 1);
    assert_eq!(
        meters[0]["group"], "third_party",
        "durable rows suppress on a cold-cache peer"
    );

    for invalid in [
        "not-json",
        r#"{"version":99,"gemini":null,"third_party":null}"#,
    ] {
        period_unknown_seed(&server, empty, invalid).await;
        let body = period_unknown_latest(&server, "").await;
        assert!(
            period_unknown_upstream(&body, empty)
                .get("period_unknown_meters")
                .is_none()
        );
    }
}

// QA-13: raw meters obey source, canonical-window, and upstream selection filters.
async fn latest_period_unknown_meters_follow_filters() {
    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
    let gemini = create_kind_oauth_upstream(&server, "filter-gemini", "gemini_oauth").await;
    let other = create_kind_oauth_upstream(&server, "filter-other", "gemini_oauth").await;
    let record = period_unknown_record(CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000).to_string();
    period_unknown_seed(&server, gemini, &record).await;
    period_unknown_seed(&server, other, &record).await;
    for source in ["header", "api", "merged"] {
        for (windows, included) in [
            ("", true),
            ("&windows=gemini_5h", true),
            ("&windows=gemini_7d", true),
            ("&windows=third_party_5h", true),
            ("&windows=third_party_7d", true),
            ("&windows=5h,third_party_7d", true),
            ("&windows=5h,7d", false),
        ] {
            let query = format!("source={source}{windows}&upstream_ids={gemini}");
            let body = period_unknown_latest(&server, &query).await;
            assert_eq!(body["upstreams"].as_array().unwrap().len(), 1, "{query}");
            let upstream = period_unknown_upstream(&body, gemini);
            assert_eq!(
                upstream.get("period_unknown_meters").is_some(),
                included && source != "header",
                "{query}: {body}",
            );
        }
    }
    let body = period_unknown_latest(&server, &format!("upstream_ids={other}")).await;
    assert_eq!(body["upstreams"].as_array().unwrap().len(), 1);
    assert_eq!(
        period_unknown_upstream(&body, other)["period_unknown_meters"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

// QA-14: replacing OAuth credentials does not invalidate same-account quota state.
async fn latest_meters_survive_credential_rotation() {
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_storage_api::{OAuthTokensCreate, UpstreamStore, UpstreamUpdate};

    let server = admin_test_common::spawn_admin_server_with_clock(test_clock()).await;
    absence_set_cache(&server, Arc::new(cc_lb_control::NoopSubscriptionQuotaCache));
    let gemini = create_kind_oauth_upstream(&server, "rotation-gemini", "gemini_oauth").await;
    period_unknown_seed(
        &server,
        gemini,
        &period_unknown_record(CHECKPOINT_TEST_NOW_UNIX_SECS * 1_000).to_string(),
    )
    .await;
    let mut previous_meters = None;
    let mut previous_fingerprint = None;
    for access_token in ["initial-test-token", "rotated-test-token"] {
        let upstream = UpstreamStore::get_by_id(server.storage.as_ref(), gemini)
            .await
            .unwrap()
            .unwrap();
        let bundle = OAuthTokenBundle {
            access_token: access_token.to_owned(),
            refresh_token: "same-account-test-refresh-token".to_owned(),
            expires_at_unix_secs: CHECKPOINT_TEST_NOW_UNIX_SECS + 3_600,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("same-account".to_owned()),
            project_id: Some("same-project".to_owned()),
            oauth_profile: Default::default(),
            scopes: Vec::new(),
            never_refresh: false,
        };
        let tokens = EncryptedOAuthTokens::encrypt(
            &AeadService::from_master_key([0; 32]),
            &bundle,
            gemini.as_bytes(),
        )
        .unwrap();
        let updated = UpstreamStore::update(
            server.storage.as_ref(),
            gemini,
            upstream.revision,
            UpstreamUpdate {
                oauth_tokens: Some(OAuthTokensCreate {
                    tokens,
                    never_refresh: false,
                }),
                oauth_token_generation: Some(upstream.oauth_token_generation + 1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            updated.oauth_token_generation,
            upstream.oauth_token_generation + 1
        );
        if let Some(fingerprint) = previous_fingerprint {
            assert_ne!(updated.oauth_credential_fingerprint(), Some(fingerprint));
        }
        previous_fingerprint = updated.oauth_credential_fingerprint();
        let body = period_unknown_latest(&server, &format!("upstream_ids={gemini}")).await;
        let meters = &period_unknown_upstream(&body, gemini)["period_unknown_meters"];
        assert_eq!(meters.as_array().unwrap().len(), 2);
        if let Some(previous) = previous_meters {
            assert_eq!(*meters, previous);
        }
        previous_meters = Some(meters.clone());
    }
}
