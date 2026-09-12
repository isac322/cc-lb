use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::AdminState;
use cc_lb_control::{InMemoryBus, RequestEventBus};
use cc_lb_request_log::{RequestEvent, RequestEventPartial, RequestEventUpdate};
use cc_lb_storage_api::RequestEventStore;
use cc_lb_storage_sqlite::SqliteStorage;
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::time::timeout;
use tower::ServiceExt;
use uuid::Uuid;

use crate::config_admin_common::{TOKEN, app};

pub const EVENT_ID: &str = "event-admin-enriched";
pub const MODEL: &str = "claude-admin-contract";
pub const THREAD_ID: &str = "thread-admin-contract";
pub const UPSTREAM_ID: Uuid = Uuid::from_u128(42);
pub const UPSTREAM_NAME: &str = "upstream-admin-contract";
pub const STRUCTURED_REQUEST_ID: &str = "req-structured-429";
pub const CANCELED_REQUEST_ID: &str = "req-client-closed";
pub const BROAD_REQUEST_ID: &str = "req-broad-only";
pub const DROPPED_REQUEST_ID: &str = "req-terminal-dropped";

pub fn event_bus() -> Arc<InMemoryBus> {
    Arc::new(InMemoryBus::new())
}

pub async fn publish_enriched(bus: &InMemoryBus, storage: &SqliteStorage) {
    for partial in enriched_partials() {
        bus.publish(RequestEventUpdate::partial(partial));
    }
    let event = RequestEvent {
        ts: 1_800_000_000,
        ts_ms: Some(1_800_000_000_000),
        request_id: "req-admin-enriched".to_owned(),
        principal_id: Some("principal-admin-contract".to_owned()),
        upstream_id: Some(UPSTREAM_ID),
        upstream_name: Some(UPSTREAM_NAME.to_owned()),
        model: Some(MODEL.to_owned()),
        status: 200,
        duration_ms: 12,
        event_id: Some(EVENT_ID.to_owned()),
        thread_id: Some(THREAD_ID.to_owned()),
        request_body_first_chunk_ms: Some(0.125),
        request_body_receive_ms: None,
        request_body_wait_ms: Some(0.0),
        request_body_process_ms: Some(0.25),
        request_body_chunk_count: Some(0),
        response_body_wait_ms: Some(0.5),
        response_body_process_ms: Some(0.0),
        response_body_downstream_poll_gap_ms: Some(0.75),
        retry_overhead_ms: Some(1.25),
        ..RequestEvent::default()
    };
    storage
        .append_request_event(&event)
        .await
        .expect("persist enriched event");
    bus.publish(RequestEventUpdate::final_(event, 1));
}

pub async fn publish_recent_contracts(storage: &SqliteStorage) {
    for event in [
        request_event(RequestEventFixture {
            request_id: STRUCTURED_REQUEST_ID,
            ts: 1_800_000_001,
            status: 429,
            error_code: "upstream_4xx",
            upstream_error_type: Some("rate_limit_error"),
            upstream_error_message: Some("bounded provider message"),
            source_kind: None,
        }),
        request_event(RequestEventFixture {
            request_id: CANCELED_REQUEST_ID,
            ts: 1_800_000_002,
            status: 499,
            error_code: "client_closed_request",
            upstream_error_type: None,
            upstream_error_message: None,
            source_kind: None,
        }),
        request_event(RequestEventFixture {
            request_id: BROAD_REQUEST_ID,
            ts: 1_800_000_003,
            status: 429,
            error_code: "upstream_4xx",
            upstream_error_type: None,
            upstream_error_message: None,
            source_kind: None,
        }),
        request_event(RequestEventFixture {
            request_id: DROPPED_REQUEST_ID,
            ts: 1_800_000_004,
            status: 504,
            error_code: "terminal_dropped",
            upstream_error_type: None,
            upstream_error_message: None,
            source_kind: None,
        }),
    ] {
        storage
            .append_request_event(&event)
            .await
            .expect("persist request event");
    }
}

pub async fn publish_source_kind_contracts(storage: &SqliteStorage) {
    for event in [
        request_event(RequestEventFixture {
            request_id: "req-normal",
            ts: 1_800_000_005,
            status: 200,
            error_code: "success",
            upstream_error_type: None,
            upstream_error_message: None,
            source_kind: None,
        }),
        request_event(RequestEventFixture {
            request_id: "req-renewal",
            ts: 1_800_000_006,
            status: 200,
            error_code: "success",
            upstream_error_type: None,
            upstream_error_message: None,
            source_kind: Some("renewal"),
        }),
    ] {
        storage
            .append_request_event(&event)
            .await
            .expect("persist source-kind request event");
    }
}

pub async fn stream_response(state: AdminState) -> axum::response::Response {
    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/admin/events/stream?model={MODEL}"))
                .header("Authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .expect("stream request builds"),
        )
        .await
        .expect("stream route responds");
    assert_eq!(response.status(), StatusCode::OK);
    response
}

pub async fn wait_for_initial_cursor(body: &mut Body) {
    for _ in 0..4 {
        let frame = next_frame_text(body).await;
        if frame.lines().any(|line| line == "event: cursor") {
            return;
        }
    }
    panic!("initial cursor frame not received");
}

pub async fn read_message_updates_through_final_window(body: &mut Body) -> Vec<Value> {
    let mut updates = Vec::new();
    let mut final_received = false;
    for _ in 0..12 {
        let Some(frame) = next_bounded_frame_text(body, final_received).await else {
            assert!(final_received, "final request event update not received");
            return updates;
        };
        if !frame.lines().any(|line| line == "event: message") {
            continue;
        }
        let data = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("message data");
        let update: Value = serde_json::from_str(data).expect("message update json");
        final_received |= update["phase"] == "final";
        updates.push(update);
    }
    assert!(final_received, "final request event update not received");
    updates
}

fn enriched_partials() -> [RequestEventPartial; 4] {
    [
        partial(None, None, None, false),
        partial(Some("principal-admin-contract"), None, None, false),
        partial(
            Some("principal-admin-contract"),
            Some(UPSTREAM_ID),
            Some(UPSTREAM_NAME),
            false,
        ),
        partial(
            Some("principal-admin-contract"),
            Some(UPSTREAM_ID),
            Some(UPSTREAM_NAME),
            true,
        ),
    ]
}

fn partial(
    principal_id: Option<&str>,
    upstream_id: Option<Uuid>,
    upstream_name: Option<&str>,
    include_io_timings: bool,
) -> RequestEventPartial {
    RequestEventPartial {
        event_id: EVENT_ID.to_owned(),
        request_id: "req-admin-enriched".to_owned(),
        ts: 1_800_000_000,
        ts_ms: 1_800_000_000_000,
        last_update_ms: 1_800_000_000_000,
        principal_id: principal_id.map(str::to_owned),
        upstream_id,
        upstream_name: upstream_name.map(str::to_owned),
        model: Some(MODEL.to_owned()),
        thread_id: Some(THREAD_ID.to_owned()),
        request_body_first_chunk_ms: include_io_timings.then_some(0.125),
        request_body_receive_ms: None,
        request_body_wait_ms: include_io_timings.then_some(0.0),
        request_body_process_ms: include_io_timings.then_some(0.25),
        request_body_chunk_count: include_io_timings.then_some(0),
        response_body_wait_ms: include_io_timings.then_some(0.5),
        response_body_process_ms: include_io_timings.then_some(0.0),
        response_body_downstream_poll_gap_ms: include_io_timings.then_some(0.75),
        retry_overhead_ms: include_io_timings.then_some(1.25),
        ..RequestEventPartial::default()
    }
}

struct RequestEventFixture {
    request_id: &'static str,
    ts: u64,
    status: u16,
    error_code: &'static str,
    upstream_error_type: Option<&'static str>,
    upstream_error_message: Option<&'static str>,
    source_kind: Option<&'static str>,
}

fn request_event(fixture: RequestEventFixture) -> RequestEvent {
    RequestEvent {
        ts: fixture.ts,
        ts_ms: Some(fixture.ts.saturating_mul(1_000)),
        request_id: fixture.request_id.to_owned(),
        status: fixture.status,
        duration_ms: 14,
        error_code: Some(fixture.error_code.to_owned()),
        upstream_error_type: fixture.upstream_error_type.map(str::to_owned),
        upstream_error_message: fixture.upstream_error_message.map(str::to_owned),
        source_kind: fixture.source_kind.map(str::to_owned),
        event_id: Some(format!("event-{}", fixture.request_id)),
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..RequestEvent::default()
    }
}

async fn next_bounded_frame_text(body: &mut Body, final_received: bool) -> Option<String> {
    if final_received {
        let frame = match timeout(Duration::from_millis(100), body.frame()).await {
            Ok(frame) => frame,
            Err(_) => return None,
        };
        let frame = frame
            .expect("SSE stream remains open")
            .expect("SSE frame succeeds");
        return Some(bytes_text(frame.into_data().expect("SSE data frame")));
    }
    Some(next_frame_text(body).await)
}

async fn next_frame_text(body: &mut Body) -> String {
    let frame = timeout(Duration::from_secs(2), body.frame())
        .await
        .expect("SSE frame timeout")
        .expect("SSE stream remains open")
        .expect("SSE frame succeeds");
    bytes_text(frame.into_data().expect("SSE data frame"))
}

fn bytes_text(bytes: bytes::Bytes) -> String {
    std::str::from_utf8(&bytes)
        .expect("SSE is UTF-8")
        .to_owned()
}
