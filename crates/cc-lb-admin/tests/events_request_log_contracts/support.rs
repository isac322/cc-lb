use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::AdminState;
use cc_lb_control::RequestEventBus;
use cc_lb_engine::{InMemoryBus, RequestEventAssemblerHandle, spawn_request_event_assembler};
use cc_lb_lifecycle::{AuthInfo, LifecycleEvent, ParseInfo, RouteInfo, TerminationReason};
use cc_lb_observability::NoopMetricsHook;
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

pub fn assembler(storage: Arc<SqliteStorage>) -> (Arc<InMemoryBus>, RequestEventAssemblerHandle) {
    let bus = Arc::new(InMemoryBus::new());
    let rx = bus.attach_lifecycle_assembler(cc_lb_engine::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY);
    let store = storage as Arc<dyn RequestEventStore>;
    let event_bus = Arc::clone(&bus) as Arc<dyn RequestEventBus>;
    let handle =
        spawn_request_event_assembler(rx, store, Some(event_bus), Arc::new(NoopMetricsHook));
    (bus, handle)
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
        .expect("SSE frame timeout");
    let frame = frame
        .expect("SSE stream remains open")
        .expect("SSE frame succeeds");
    bytes_text(frame.into_data().expect("SSE data frame"))
}

fn bytes_text(bytes: bytes::Bytes) -> String {
    std::str::from_utf8(&bytes)
        .expect("SSE is UTF-8")
        .to_owned()
}

pub fn publish_enriched(bus: &InMemoryBus) {
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: EVENT_ID.to_owned(),
        request_id: "req-admin-enriched".to_owned(),
        ts_ms: 1_800_000_000_000,
        stream: true,
    });
    bus.publish_lifecycle(LifecycleEvent::ParseCompleted {
        event_id: EVENT_ID.to_owned(),
        result: Ok(ParseInfo {
            path: "/v1/messages".to_owned(),
            method: "POST".to_owned(),
            model: Some(MODEL.to_owned()),
            stream: true,
            body_bytes: 128,
            thread_id: Some(THREAD_ID.to_owned()),
            ..ParseInfo::default()
        }),
    });
    bus.publish_lifecycle(LifecycleEvent::AuthCompleted {
        event_id: EVENT_ID.to_owned(),
        result: Ok(AuthInfo {
            principal_id: "principal-admin-contract".to_owned(),
            key_id: Some("key-admin-contract".to_owned()),
            principal_kind: Some("machine".to_owned()),
            auth_ms: Some(2),
        }),
    });
    bus.publish_lifecycle(LifecycleEvent::RouteCompleted {
        event_id: EVENT_ID.to_owned(),
        result: Ok(route_info()),
        routing_trace: None,
    });
    publish_termination(bus, EVENT_ID, TerminationReason::Success, 200, 12);
}

pub fn publish_recent_contracts(bus: &InMemoryBus) {
    publish_started(
        bus,
        "event-structured",
        STRUCTURED_REQUEST_ID,
        1_800_000_001_000,
    );
    bus.publish_lifecycle(LifecycleEvent::RequestLogUpstreamErrorObserved {
        event_id: "event-structured".to_owned(),
        error_type: "rate_limit_error".to_owned(),
        error_message: "bounded provider message".to_owned(),
    });
    publish_termination(
        bus,
        "event-structured",
        TerminationReason::ErrorCode("upstream_4xx".to_owned()),
        429,
        11,
    );

    publish_started(
        bus,
        "event-canceled",
        CANCELED_REQUEST_ID,
        1_800_000_002_000,
    );
    publish_termination(
        bus,
        "event-canceled",
        TerminationReason::ErrorCode("client_closed_request".to_owned()),
        499,
        12,
    );

    publish_started(bus, "event-broad", BROAD_REQUEST_ID, 1_800_000_003_000);
    bus.publish_lifecycle(LifecycleEvent::ProviderErrorObserved {
        event_id: "event-broad".to_owned(),
        code: "upstream_4xx".to_owned(),
        message: "429".to_owned(),
        source: "upstream".to_owned(),
    });
    publish_termination(
        bus,
        "event-broad",
        TerminationReason::ErrorCode("upstream_4xx".to_owned()),
        429,
        13,
    );

    publish_started(bus, "event-dropped", DROPPED_REQUEST_ID, 1_800_000_004_000);
    publish_termination(bus, "event-dropped", TerminationReason::Dropped, 504, 14);
}

fn publish_started(bus: &InMemoryBus, event_id: &str, request_id: &str, ts_ms: u64) {
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: event_id.to_owned(),
        request_id: request_id.to_owned(),
        ts_ms,
        stream: false,
    });
}

fn publish_termination(
    bus: &InMemoryBus,
    event_id: &str,
    reason: TerminationReason,
    client_status: u16,
    duration_ms: u64,
) {
    bus.publish_lifecycle(LifecycleEvent::RequestTerminated {
        event_id: event_id.to_owned(),
        reason,
        client_status,
        duration_ms,
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
    });
}

fn route_info() -> RouteInfo {
    RouteInfo {
        upstream_id: UPSTREAM_ID,
        upstream_name: UPSTREAM_NAME.to_owned(),
        model: Some(MODEL.to_owned()),
        upstream_kind: Some("anthropic_key".to_owned()),
        route_ms: Some(3),
        routing_trace: None,
        predicted_cache_read_tokens: None,
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        predicted_cache_creation_tokens_5m: None,
        predicted_cache_creation_tokens_1h: None,
        token_estimate_source: None,
        cache_value_micros: None,
        formula_winner_upstream_id: None,
        kept_upstream_id: None,
        quota_urgency_5h: None,
        quota_urgency_7d: None,
        quota_urgency_combined: None,
        quota_weight_factor: None,
        quota_cache_multiplier: None,
        quota_warning_multiplier: None,
        quota_effective_weight: None,
        quota_uniform_fallback: None,
        wrh_key_source: None,
        lineage_would_have_predicted_read_tokens: None,
        lineage_would_have_picked_upstream_id: None,
    }
}
