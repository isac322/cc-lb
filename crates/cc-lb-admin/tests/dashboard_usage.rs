mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, UsageRollupStore};
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};
use uuid::Uuid;

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
    let (_dir, storage) = temp_storage().await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);
    let bucket_ts = current_unix_secs().saturating_sub(3_600);

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

    let state = test_state(Config::default(), Some(storage));
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

fn current_unix_secs() -> u64 {
    cc_lb_core::clock::unix_secs(cc_lb_core::Clock::now(&cc_lb_core::SystemClock))
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
