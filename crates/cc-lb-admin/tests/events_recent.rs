mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_core::Clock as _;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};
use uuid::Uuid;

#[tokio::test]
async fn events_recent_returns_empty_with_no_traffic() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 0);
    assert_eq!(body["count"], 0);
}

#[tokio::test]
async fn events_recent_accepts_limit_param() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/events/recent?limit=10", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["limit"], 10);
}

#[tokio::test]
async fn events_recent_rejects_invalid_limit() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (status, _, _) =
        authed_bytes(app(state), "GET", "/admin/events/recent?limit=abc", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn events_recent_filters_by_upstream_id() {
    let (_dir, storage) = temp_storage().await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);

    storage
        .append_request_event(&request_event(
            1,
            "req-target",
            upstream_id,
            "target-upstream",
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&request_event(
            2,
            "req-other",
            other_upstream_id,
            "other-upstream",
        ))
        .await
        .unwrap();

    let state = test_state(Config::default(), Some(storage));
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/events/recent?upstream_id={upstream_id}"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["request_id"], "req-target");
    assert_eq!(events[0]["upstream_id"], upstream_id.to_string());
    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn events_recent_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn request_event(
    index: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some((current_unix_secs().saturating_sub(60) + index) * 1000),
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some(format!("key-{index}")),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index),
        output_tokens: Some(0),
        duration_ms: 10,
        ..Default::default()
    }
}

fn current_unix_secs() -> u64 {
    let clock = cc_lb_core::SystemClock;
    cc_lb_core::clock::unix_secs(clock.now())
}
