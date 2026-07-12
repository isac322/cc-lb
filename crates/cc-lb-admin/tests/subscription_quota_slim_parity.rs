mod admin_test_common;
#[path = "support/subscription_quota_fixture.rs"]
mod subscription_quota_fixture;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::{
    RequestEventStore, SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow,
    UpstreamSubscriptionQuotaStore, UsageRollupStore,
};
use serde_json::json;

use subscription_quota_fixture::{
    NOW_UNIX_SECS, assert_close, create_oauth_upstream, legacy_inclusive_tokens, quota_checkpoint,
    usage_event,
};

#[tokio::test]
async fn subscription_quota_aggregate_exact_boundary_tokens_match_legacy_inclusive_bytes() {
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let server = admin_test_common::spawn_admin_server_with_clock(clock).await;
    let upstream_id = create_oauth_upstream(&server, "quota-exact-boundary").await;
    let provider_sample_end = NOW_UNIX_SECS - 600;
    let provider_start = provider_sample_end - 5 * 3_600;
    let cc_start = NOW_UNIX_SECS - NOW_UNIX_SECS % (5 * 3_600);
    server
        .storage
        .put_subscription_quota_checkpoint(&quota_checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            NOW_UNIX_SECS - 1_200,
            1,
            0.5,
            provider_sample_end,
        ))
        .await
        .unwrap();
    let buckets = [
        (provider_start, 11_u64),
        (cc_start, 17_u64),
        (provider_sample_end, 13_u64),
        (NOW_UNIX_SECS, 19_u64),
    ];
    for (timestamp, tokens) in buckets {
        server
            .storage
            .append_request_event(&usage_event(
                timestamp,
                &format!("exact-boundary-{timestamp}"),
                upstream_id,
                "quota-exact-boundary",
                tokens,
            ))
            .await
            .unwrap();
    }
    server.storage.rollup_usage_once().await.unwrap();

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream_id}&windows=5h&source=header"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    let window = &body["windows"][0];
    let lot = &window["provider_lots"][0];
    let expected = json!({
        "capacity_estimate_tokens": legacy_inclusive_tokens(&buckets, provider_start, provider_sample_end) as f64 / 0.5,
        "used_before_cc_window_tokens": legacy_inclusive_tokens(&buckets, provider_start, cc_start),
        "used_tokens": legacy_inclusive_tokens(&buckets, cc_start, NOW_UNIX_SECS),
    });
    let actual = json!({
        "capacity_estimate_tokens": lot["capacity_estimate_tokens"],
        "used_before_cc_window_tokens": lot["used_before_cc_window_tokens"],
        "used_tokens": window["used_tokens"],
    });
    assert_eq!(
        serde_json::to_vec(&actual).unwrap(),
        serde_json::to_vec(&expected).unwrap(),
        "exact provider-start, provider-sample-end, cc-start, and now buckets must match the old inclusive result byte-for-byte"
    );
}

#[tokio::test]
async fn subscription_quota_slim_aggregate_matches_legacy_reference_for_seven_days_and_fifty_upstreams() {
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let server = admin_test_common::spawn_admin_server_with_clock(clock).await;
    let mut upstream_ids = Vec::with_capacity(50);
    let mut checkpoints = Vec::with_capacity(100);
    let mut buckets = Vec::with_capacity(50);
    for token_count in 1..=50_u64 {
        let index = usize::try_from(token_count - 1).unwrap();
        let name = format!("quota-parity-{index}");
        let upstream_id = create_oauth_upstream(&server, &name).await;
        upstream_ids.push(upstream_id);
        let sample_seed = u128::from(token_count) * 2;
        checkpoints.extend([
            quota_checkpoint(
                upstream_id,
                SubscriptionQuotaWindow::FiveHour,
                NOW_UNIX_SECS - 1_200,
                sample_seed,
                0.5,
                NOW_UNIX_SECS + 3_600,
            ),
            quota_checkpoint(
                upstream_id,
                SubscriptionQuotaWindow::SevenDay,
                NOW_UNIX_SECS - 1_800,
                sample_seed + 1,
                0.5,
                NOW_UNIX_SECS + 2 * 24 * 3_600,
            ),
        ]);
        server
            .storage
            .append_request_event(&usage_event(
                NOW_UNIX_SECS,
                &format!("quota-parity-event-{index}"),
                upstream_id,
                &name,
                token_count,
            ))
            .await
            .unwrap();
        buckets.push((NOW_UNIX_SECS, token_count));
    }
    server
        .storage
        .put_subscription_quota_checkpoints(&checkpoints)
        .await
        .unwrap();
    server.storage.rollup_usage_once().await.unwrap();
    let upstream_ids = upstream_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");

    let (status, _, body) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream_ids}&windows=5h,7d&source=header"
        ))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["upstream_count"], 50);
    let windows = body["windows"].as_array().expect("aggregate windows");
    assert_eq!(windows.len(), 2);
    for (window, window_secs) in windows.iter().zip([5 * 3_600, 7 * 24 * 3_600]) {
        let cc_start = NOW_UNIX_SECS - NOW_UNIX_SECS % window_secs;
        let used_tokens = legacy_inclusive_tokens(&buckets, cc_start, NOW_UNIX_SECS);
        let capacity = used_tokens as f64 / 0.5;
        assert_eq!(window["used_tokens"], used_tokens);
        assert_eq!(window["confidence"], "plan_weighted");
        assert_eq!(window["contributing_upstreams"], 50);
        assert_eq!(window["stale_upstreams"], 0);
        assert_eq!(window["missing_capacity_upstreams"], 0);
        assert_close(&window["utilization"], 0.5);
        assert_close(&window["utilization_percent"], 50.0);
        assert_close(&window["capacity_to_now_tokens_estimate"], capacity);
        assert_close(&window["projected_capacity_tokens_estimate"], capacity);
        assert_close(
            &window["remaining_to_now_tokens_estimate"],
            used_tokens as f64,
        );
        let lots = window["provider_lots"].as_array().expect("provider lots");
        assert_eq!(lots.len(), 50);
        for lot in lots {
            assert_eq!(lot["confidence"], "estimated");
            assert_eq!(lot["used_before_cc_window_tokens"], 0);
            assert_close(&lot["utilization"], 0.5);
        }
    }
}

#[tokio::test]
async fn subscription_quota_aggregate_is_identical_after_independent_storage_and_view_restart() {
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let server = admin_test_common::spawn_admin_server_with_clock(clock.clone()).await;
    let upstream_id = create_oauth_upstream(&server, "quota-restart").await;
    server
        .storage
        .put_subscription_quota_checkpoint(&quota_checkpoint(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            NOW_UNIX_SECS - 1_200,
            1,
            0.5,
            NOW_UNIX_SECS + 3_600,
        ))
        .await
        .unwrap();
    server
        .storage
        .append_request_event(&usage_event(
            NOW_UNIX_SECS,
            "quota-restart-event",
            upstream_id,
            "quota-restart",
            100,
        ))
        .await
        .unwrap();
    server.storage.rollup_usage_once().await.unwrap();
    let (status, _, first) = server
        .client
        .get(&format!(
            "/admin/v1/subscription-quotas/aggregate?upstream_ids={upstream_id}&windows=5h&source=header&max_staleness_secs=3600"
        ))
        .await;
    assert_eq!(status, StatusCode::OK);

    let restarted_storage = admin_test_common::sqlite_storage_with_clock(
        server._dir.path(),
        "admin.sqlite",
        clock.clone(),
    )
    .await;
    let restarted = cc_lb_admin::subscription_quotas::build_cc_lb_aggregate_response(
        restarted_storage.as_ref(),
        &admin_test_common::dynamic_view_holder(&Config::default()),
        Some(vec![upstream_id]),
        vec![SubscriptionQuotaWindow::FiveHour],
        SubscriptionQuotaSourceMerge::Header,
        3_600,
        clock.as_ref(),
    )
    .await
    .unwrap();
    let restarted = serde_json::to_value(restarted).unwrap();
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&restarted).unwrap(),
        "the same database must reproduce the aggregate with a fresh storage connection and dynamic view"
    );
}
