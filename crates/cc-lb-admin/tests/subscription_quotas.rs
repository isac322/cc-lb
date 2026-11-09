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
            "provider-codex" => std::collections::BTreeSet::from(["5h", "7d"]),
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
}
