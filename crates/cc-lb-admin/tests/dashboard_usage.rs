use crate::config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, UsageRollupStore, normalize_usage_rollup_dimension,
};
use config_admin_common::{
    app, authed_bytes, authed_json, temp_storage, temp_storage_with_clock, test_state,
    test_state_with_clock,
};
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

#[tokio::test]
async fn usage_returns_200_grouped_by_model() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["range"], "1h");
    assert_eq!(body["group_by"], "model");
}

#[tokio::test]
async fn usage_legacy_dashboard_alias_matches_v1_body() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let admin_app = app(state);

    let (legacy_status, _, legacy_body, _) = authed_json(
        admin_app.clone(),
        "GET",
        "/admin/dashboard/usage?range=1h&group_by=model",
        None,
    )
    .await;
    let (v1_status, _, v1_body, _) = authed_json(
        admin_app,
        "GET",
        "/admin/v1/dashboard/usage?range=1h&group_by=model",
        None,
    )
    .await;

    assert_eq!(legacy_status, StatusCode::OK);
    assert_eq!(v1_status, StatusCode::OK);
    assert_eq!(legacy_body, v1_body);
}

#[tokio::test]
async fn usage_returns_200_grouped_by_principal() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["group_by"], "principal");
}

#[tokio::test]
async fn usage_totals_projection_preserves_full_series_totals() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(10);
    let now = current_unix_secs(clock.as_ref());

    let mut older = usage_event(
        now.saturating_sub(120),
        "req-totals-older",
        upstream_id,
        "target-upstream",
        3,
    );
    older.cost_usd_micros = Some(15);
    older.cost_input_micros = Some(1);
    older.cost_output_micros = Some(2);
    older.cost_cache_creation_5m_micros = Some(3);
    older.cost_cache_creation_1h_micros = Some(4);
    older.cost_cache_read_micros = Some(5);
    storage.append_request_event(&older).await.unwrap();

    let mut newer = usage_event(
        now.saturating_sub(60),
        "req-totals-newer",
        upstream_id,
        "target-upstream",
        7,
    );
    newer.status = 500;
    newer.cost_usd_micros = Some(10);
    newer.cost_input_micros = Some(2);
    newer.cost_output_micros = Some(3);
    newer.cost_cache_creation_5m_micros = Some(0);
    newer.cost_cache_creation_1h_micros = Some(0);
    newer.cost_cache_read_micros = Some(5);
    newer.duration_ms = 25;
    storage.append_request_event(&newer).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));
    let (full_status, _, full, _) = authed_json(
        admin_app.clone(),
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal",
        None,
    )
    .await;
    let (totals_status, _, totals, _) = authed_json(
        admin_app,
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal&projection=totals",
        None,
    )
    .await;

    assert_eq!(full_status, StatusCode::OK);
    assert_eq!(totals_status, StatusCode::OK);
    assert_eq!(
        full["window_start_unix_secs"],
        totals["window_start_unix_secs"]
    );
    assert_eq!(full["window_end_unix_secs"], totals["window_end_unix_secs"]);
    assert_eq!(
        full["truncated_series_count"],
        totals["truncated_series_count"]
    );
    assert_eq!(full["observed"], totals["observed"]);

    let full_series = full["series"].as_array().unwrap();
    let totals_series = totals["series"].as_array().unwrap();
    assert_eq!(full_series.len(), totals_series.len());
    assert!(
        full_series[0]["buckets"].as_array().unwrap().len() > 1,
        "the default response must retain its dense full-series contract"
    );
    let total_buckets = totals_series[0]["buckets"].as_array().unwrap();
    assert_eq!(total_buckets.len(), 1);
    assert_eq!(
        total_buckets[0]["bucket_start_unix_secs"],
        totals["window_start_unix_secs"]
    );

    let full_buckets = full_series[0]["buckets"].as_array().unwrap();
    for field in [
        "request_count",
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "error_count",
        "virtual_cost_micros",
        "latency_ms_sum",
        "latency_count",
        "proxy_setup_ms_sum",
        "proxy_setup_ms_count",
        "shape_ms_sum",
        "shape_ms_count",
        "sign_ms_sum",
        "sign_ms_count",
        "upstream_ttfb_ms_sum",
        "upstream_ttfb_ms_count",
        "upstream_body_ms_sum",
        "upstream_body_ms_count",
        "cost_input_micros",
        "cost_output_micros",
        "cost_cache_creation_5m_micros",
        "cost_cache_creation_1h_micros",
        "cost_cache_read_micros",
    ] {
        let full_sum = full_buckets
            .iter()
            .filter_map(|bucket| bucket[field].as_u64())
            .sum::<u64>();
        assert_eq!(
            total_buckets[0][field].as_u64(),
            Some(full_sum),
            "totals projection changed {field}"
        );
    }
    let full_latency_min = full_buckets
        .iter()
        .filter_map(|bucket| bucket["latency_ms_min"].as_u64())
        .min();
    let full_latency_max = full_buckets
        .iter()
        .filter_map(|bucket| bucket["latency_ms_max"].as_u64())
        .max();
    assert_eq!(
        total_buckets[0]["latency_ms_min"].as_u64(),
        full_latency_min
    );
    assert_eq!(
        total_buckets[0]["latency_ms_max"].as_u64(),
        full_latency_max
    );
}

#[tokio::test]
async fn usage_totals_projection_preserves_components_across_rollup_lag() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(11);
    let now = current_unix_secs(clock.as_ref());

    let mut rolled = usage_event(
        now.saturating_sub(120),
        "req-totals-lag-rolled",
        upstream_id,
        "target-upstream",
        1,
    );
    rolled.cost_usd_micros = Some(10);
    rolled.cost_input_micros = Some(1);
    rolled.cost_output_micros = Some(2);
    rolled.cost_cache_creation_5m_micros = Some(3);
    rolled.cost_cache_creation_1h_micros = Some(4);
    rolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&rolled).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let mut unrolled = rolled.clone();
    unrolled.request_id = "req-totals-lag-unrolled".to_owned();
    unrolled.event_id = None;
    unrolled.ts_ms = Some(now.saturating_sub(1) * 1_000);
    unrolled.cost_usd_micros = Some(5);
    unrolled.cost_input_micros = Some(5);
    unrolled.cost_output_micros = Some(0);
    unrolled.cost_cache_creation_5m_micros = Some(0);
    unrolled.cost_cache_creation_1h_micros = Some(0);
    unrolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&unrolled).await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));
    let (full_status, _, full, _) = authed_json(
        admin_app.clone(),
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal",
        None,
    )
    .await;
    let (totals_status, _, totals, _) = authed_json(
        admin_app,
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal&projection=totals",
        None,
    )
    .await;

    assert_eq!(full_status, StatusCode::OK);
    assert_eq!(totals_status, StatusCode::OK);
    let full_buckets = full["series"][0]["buckets"].as_array().unwrap();
    let totals_bucket = &totals["series"][0]["buckets"][0];
    for (field, expected) in [
        ("cost_input_micros", 1),
        ("cost_output_micros", 2),
        ("cost_cache_creation_5m_micros", 3),
        ("cost_cache_creation_1h_micros", 4),
        ("cost_cache_read_micros", 0),
    ] {
        let full_sum = full_buckets
            .iter()
            .filter_map(|bucket| bucket[field].as_u64())
            .sum::<u64>();
        assert_eq!(full_sum, expected, "unexpected full-series {field}");
        assert_eq!(
            totals_bucket[field].as_u64(),
            Some(expected),
            "totals projection changed {field} during rollup lag"
        );
    }
}

#[tokio::test]
async fn usage_rejects_invalid_projection() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal&projection=dense",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn usage_rejects_invalid_group_by() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=bogus",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn usage_filters_by_upstream_id() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(3_600);

    storage
        .append_request_event(&usage_event(
            bucket_ts,
            "req-target",
            upstream_id,
            "target-upstream",
            3,
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&usage_event(
            bucket_ts,
            "req-other",
            other_upstream_id,
            "other-upstream",
            7,
        ))
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/usage?range=24h&group_by=model&upstream_id={upstream_id}"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().expect("series array");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["key"], "claude-sonnet-4-5");
    let input_tokens = series[0]["buckets"]
        .as_array()
        .expect("buckets array")
        .iter()
        .map(|bucket| bucket["input_tokens"].as_u64().unwrap_or_default())
        .sum::<u64>();
    assert_eq!(input_tokens, 3);
}

#[tokio::test]
async fn usage_principal_enriches_mixed_legacy_and_component_costs() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(3);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);

    let mut modern = usage_event(
        bucket_ts,
        "req-principal-modern",
        upstream_id,
        "target-upstream",
        1,
    );
    modern.principal_id = Some("  principal/A  ".to_owned());
    modern.source_kind = Some("renewal".to_owned());
    modern.cost_usd_micros = Some(15);
    modern.cost_input_micros = Some(1);
    modern.cost_output_micros = Some(2);
    modern.cost_cache_creation_5m_micros = Some(3);
    modern.cost_cache_creation_1h_micros = Some(4);
    modern.cost_cache_read_micros = Some(5);
    storage.append_request_event(&modern).await.unwrap();

    let mut legacy = usage_event(
        bucket_ts + 1,
        "req-principal-legacy",
        upstream_id,
        "target-upstream",
        1,
    );
    legacy.principal_id = Some("principal A".to_owned());
    legacy.cost_usd_micros = Some(10);
    storage.append_request_event(&legacy).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let principal = normalize_usage_rollup_dimension(Some("principal A"));
    let series = body["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|series| series["key"].as_str() == Some(principal.as_str()))
        .expect("normalized principal series");
    let bucket = series["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["virtual_cost_micros"] == 25)
        .expect("cost-bearing bucket");
    assert_eq!(bucket["cost_input_micros"], 1);
    assert_eq!(bucket["cost_output_micros"], 2);
    assert_eq!(bucket["cost_cache_creation_5m_micros"], 3);
    assert_eq!(bucket["cost_cache_creation_1h_micros"], 4);
    assert_eq!(bucket["cost_cache_read_micros"], 5);
}

#[tokio::test]
async fn usage_principal_preserves_recorded_zero_component_costs() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(4);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut event = usage_event(
        bucket_ts,
        "req-principal-zero",
        upstream_id,
        "target-upstream",
        1,
    );
    event.cost_input_micros = Some(0);
    event.cost_output_micros = Some(0);
    event.cost_cache_creation_5m_micros = Some(0);
    event.cost_cache_creation_1h_micros = Some(0);
    event.cost_cache_read_micros = Some(0);
    storage.append_request_event(&event).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["request_count"] == 1)
        .expect("observed bucket");
    assert_eq!(bucket["cost_input_micros"], 0);
    assert_eq!(bucket["cost_output_micros"], 0);
    assert_eq!(bucket["cost_cache_creation_5m_micros"], 0);
    assert_eq!(bucket["cost_cache_creation_1h_micros"], 0);
    assert_eq!(bucket["cost_cache_read_micros"], 0);
}

#[tokio::test]
async fn usage_principal_omits_components_when_request_events_are_ahead() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(5);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut rolled = usage_event(
        bucket_ts,
        "req-principal-rolled",
        upstream_id,
        "target-upstream",
        1,
    );
    rolled.cost_usd_micros = Some(10);
    rolled.cost_input_micros = Some(10);
    rolled.cost_output_micros = Some(0);
    rolled.cost_cache_creation_5m_micros = Some(0);
    rolled.cost_cache_creation_1h_micros = Some(0);
    rolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&rolled).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let mut unrolled = rolled.clone();
    unrolled.request_id = "req-principal-unrolled".to_owned();
    unrolled.event_id = None;
    unrolled.ts_ms = Some((bucket_ts + 1) * 1_000);
    unrolled.cost_usd_micros = Some(5);
    unrolled.cost_input_micros = Some(5);
    storage.append_request_event(&unrolled).await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["virtual_cost_micros"] == 10)
        .expect("rolled bucket");
    let bucket = bucket.as_object().unwrap();
    assert!(!bucket.contains_key("cost_input_micros"));
    assert!(!bucket.contains_key("cost_output_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_5m_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_1h_micros"));
    assert!(!bucket.contains_key("cost_cache_read_micros"));
}

#[tokio::test]
async fn usage_non_principal_grouping_does_not_expose_component_costs() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(6);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut event = usage_event(
        bucket_ts,
        "req-model-components",
        upstream_id,
        "target-upstream",
        1,
    );
    event.cost_usd_micros = Some(1);
    event.cost_input_micros = Some(1);
    event.cost_output_micros = Some(0);
    event.cost_cache_creation_5m_micros = Some(0);
    event.cost_cache_creation_1h_micros = Some(0);
    event.cost_cache_read_micros = Some(0);
    storage.append_request_event(&event).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["request_count"] == 1)
        .expect("observed bucket")
        .as_object()
        .unwrap();
    assert!(!bucket.contains_key("cost_input_micros"));
    assert!(!bucket.contains_key("cost_output_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_5m_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_1h_micros"));
    assert!(!bucket.contains_key("cost_cache_read_micros"));
}

#[tokio::test]
async fn summary_error_rate_excludes_client_navigation_statuses() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);

    for (index, status) in [200, 401, 403, 404, 500].into_iter().enumerate() {
        let mut event = usage_event(
            bucket_ts,
            &format!("req-status-{status}"),
            upstream_id,
            "target-upstream",
            index as u64,
        );
        event.status = status;
        storage.append_request_event(&event).await.unwrap();
    }
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["totals"]["request_count"], 5);
    assert_eq!(body["totals"]["error_count"], 1);
    assert_eq!(body["totals"]["error_rate"], 0.2);
}

#[tokio::test]
async fn usage_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS))
}

fn current_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_clock::unix_secs(clock.now())
}

fn usage_event(
    ts: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts * 1000),
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
