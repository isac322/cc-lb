mod config_admin_common;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_config::Config;
use cc_lb_control::RequestEventBus;
use cc_lb_engine::InMemoryBus;
use cc_lb_request_log::RequestEventUpdate;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{TOKEN, app, authed_bytes, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::time::timeout;
use tower::ServiceExt;

#[tokio::test]
async fn events_stream_opens_with_sse_content_type() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .unwrap();
    assert!(
        content_type.starts_with("text/event-stream"),
        "expected SSE content-type, got {content_type}",
    );
}

#[tokio::test]
async fn events_stream_first_byte_is_connected_comment() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let mut body = response.into_body();
    let frame = body.frame().await.expect("first frame").expect("frame ok");
    let bytes = frame.into_data().expect("data frame");
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.starts_with(": connected"),
        "expected ': connected' initial SSE comment, got {text:?}",
    );
}

#[tokio::test]
async fn events_stream_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/stream", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn events_stream_backfills_initial_cursor_in_order_and_bookmarks() {
    let (_dir, storage) = temp_storage().await;
    append_events(storage.as_ref(), 1, 100).await;
    let state = test_state(Config::default(), Some(storage));

    let response = stream_response(state, Some("0")).await;
    let text = read_sse_frames(response.into_body(), 102).await;

    let request_ids = message_request_ids(&text);
    assert_eq!(request_ids.len(), 100);
    assert_eq!(request_ids.first().map(String::as_str), Some("req-1"));
    assert_eq!(request_ids.last().map(String::as_str), Some("req-100"));
    assert!(text.contains("event: cursor"));
    assert!(text.contains("id: 100"));
}

#[tokio::test]
async fn events_stream_reconnect_drains_after_last_event_id() {
    let (_dir, storage) = temp_storage().await;
    append_events(storage.as_ref(), 1, 5).await;
    let state = test_state(Config::default(), Some(storage));

    let response = stream_response(state, Some("2")).await;
    let text = read_sse_frames(response.into_body(), 5).await;

    assert_eq!(message_request_ids(&text), vec!["req-3", "req-4", "req-5"]);
    assert!(text.contains("event: cursor"));
    assert!(text.contains("id: 5"));
}

#[tokio::test]
async fn events_stream_writes_reset_frame_when_bus_lagged() {
    let (_dir, storage) = temp_storage().await;
    let bus = Arc::new(InMemoryBus::with_capacity(1));
    let mut state = test_state(Config::default(), Some(storage));
    state.event_bus = Some(bus.clone() as Arc<dyn RequestEventBus>);

    let response = stream_response(state, Some("0")).await;
    let mut body = response.into_body();

    let mut text = String::new();
    while !text.contains("event: cursor") {
        let frame = timeout(Duration::from_secs(2), body.frame())
            .await
            .expect("initial cursor frame timeout")
            .expect("stream ended before initial cursor")
            .expect("frame ok");
        if let Ok(data) = frame.into_data() {
            text.push_str(std::str::from_utf8(&data).unwrap());
        }
    }

    bus.publish(RequestEventUpdate::final_(request_event(1), 1));
    bus.publish(RequestEventUpdate::final_(request_event(2), 2));

    while !text.contains("event: reset") {
        let frame = timeout(Duration::from_secs(2), body.frame())
            .await
            .expect("reset frame timeout")
            .expect("stream ended before reset")
            .expect("frame ok");
        if let Ok(data) = frame.into_data() {
            text.push_str(std::str::from_utf8(&data).unwrap());
        }
    }

    assert!(text.contains(r#"reason":"bus_lagged"#));
}

#[tokio::test]
async fn events_stream_resets_after_one_backfill_page_when_over_cap() {
    let (_dir, storage) = temp_storage().await;
    append_events(storage.as_ref(), 1, 501).await;
    let state = test_state(Config::default(), Some(storage));

    let response = stream_response(state, Some("0")).await;
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = std::str::from_utf8(&bytes).unwrap();

    assert_eq!(text.matches("event: message").count(), 500);
    assert_eq!(text.matches("event: reset").count(), 1);
    assert!(text.contains(r#"reason":"backfill_cap"#));
}

#[tokio::test]
async fn events_stream_reconnect_with_last_event_id_still_requires_auth() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/events/stream")
                .header("Last-Event-ID", "1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

async fn stream_response(
    state: cc_lb_admin::AdminState,
    last_event_id: Option<&str>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method("GET")
        .uri("/admin/events/stream")
        .header("Authorization", format!("Bearer {TOKEN}"));
    if let Some(last_event_id) = last_event_id {
        request = request.header("Last-Event-ID", last_event_id);
    }
    let response = app(state)
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response
}

async fn read_sse_frames(mut body: Body, frames: usize) -> String {
    let mut text = String::new();
    for _ in 0..frames {
        let frame = timeout(Duration::from_secs(2), body.frame())
            .await
            .expect("sse frame timeout")
            .expect("sse frame")
            .expect("sse frame ok");
        if let Ok(data) = frame.into_data() {
            text.push_str(std::str::from_utf8(&data).unwrap());
        }
    }
    text
}

async fn append_events(storage: &cc_lb_storage_sqlite::SqliteStorage, start: u64, end: u64) {
    for index in start..=end {
        storage
            .append_request_event(&request_event(index))
            .await
            .unwrap();
    }
}

fn message_request_ids(text: &str) -> Vec<String> {
    text.split("\n\n")
        .filter(|frame| frame.lines().any(|line| line == "event: message"))
        .filter_map(message_request_id)
        .collect()
}

fn message_request_id(frame: &str) -> Option<String> {
    let data = frame.lines().find_map(|line| line.strip_prefix("data: "))?;
    let value: Value = serde_json::from_str(data).ok()?;
    value["payload"]["event"]["request_id"]
        .as_str()
        .map(ToOwned::to_owned)
}

fn request_event(index: u64) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000 + index,
        ts_ms: Some((1_800_000_000 + index) * 1_000),
        request_id: format!("req-{index}"),
        event_id: Some(format!("event-{index:06}")),
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 10,
        ..Default::default()
    }
}
