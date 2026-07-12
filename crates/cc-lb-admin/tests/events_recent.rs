mod config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{
    app, authed_bytes, authed_json, temp_storage, temp_storage_with_clock, test_state,
    test_state_with_clock,
};
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

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
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);

    storage
        .append_request_event(&request_event(
            clock.as_ref(),
            1,
            "req-target",
            upstream_id,
            "target-upstream",
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&request_event(
            clock.as_ref(),
            2,
            "req-other",
            other_upstream_id,
            "other-upstream",
        ))
        .await
        .unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
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
async fn events_recent_uses_compound_cursor_for_same_timestamp_pages() {
    let (_dir, storage) = temp_storage().await;
    let ts_ms = 1_800_000_000_000;
    let upstream_id = Uuid::from_u128(1);
    storage
        .append_request_event(&request_event_with_cursor(
            ts_ms,
            "event-b",
            "req-newer",
            upstream_id,
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&request_event_with_cursor(
            ts_ms,
            "event-a",
            "req-older",
            upstream_id,
        ))
        .await
        .unwrap();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, first_page, _) = authed_json(
        app(state.clone()),
        "GET",
        "/admin/events/recent?limit=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_page["events"][0]["request_id"], "req-newer");

    let (status, _, second_page, _) = authed_json(
        app(state),
        "GET",
        "/admin/events/recent?limit=1&until_ts_ms=1800000000000&until_event_id=event-b",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second_page["events"].as_array().unwrap().len(), 1);
    assert_eq!(second_page["events"][0]["request_id"], "req-older");
}

#[tokio::test]
async fn events_recent_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn request_event(
    clock: &dyn cc_lb_clock::Clock,
    index: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some((current_unix_secs(clock).saturating_sub(60) + index) * 1000),
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some(format!("key-{index}")),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        event_id: Some(format!("event-{index:06}")),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index),
        output_tokens: Some(0),
        duration_ms: 10,
        ..Default::default()
    }
}

fn request_event_with_cursor(
    ts_ms: u64,
    event_id: &str,
    request_id: &str,
    upstream_id: Uuid,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts_ms),
        request_id: request_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("principal-a".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("target-upstream".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(0),
        duration_ms: 10,
        ..Default::default()
    }
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS))
}

fn current_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_clock::unix_secs(clock.now())
}
