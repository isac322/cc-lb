mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use cc_lb_core::{DashboardBroadcaster, RequestEventSink};
use cc_lb_observability::dropped_events_total;
use cc_lb_storage_api::{RequestEvent, RequestEventUpstream};
use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn lifecycle_enqueues_metadata_only_request_event() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (sink, mut receiver) = RequestEventSink::with_capacity(8);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook,
    )
    .with_request_event_sink(Some(Arc::new(sink)));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test"}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);

    let event = receiver.try_recv().expect("request event is enqueued");
    assert!(event.request_id.starts_with("req_core_"));
    assert_eq!(event.principal_id.as_deref(), Some("principal-test"));
    assert_eq!(event.principal_kind.as_deref(), Some("api_key"));
    assert_eq!(
        event.upstream,
        Some(RequestEventUpstream::CustomAnthropicSpec)
    );
    assert_eq!(event.model.as_deref(), Some("claude-test"));
    assert_eq!(event.status, 200);
    assert_eq!(event.input_tokens, Some(1));
    assert_eq!(event.output_tokens, Some(1));
    assert_eq!(event.error_code, None);
    assert!(receiver.try_recv().is_err(), "one request emits one event");

    assert_forbidden_keys_absent(&serde_json::to_value(&event).expect("event serializes"));
}

#[tokio::test]
async fn lifecycle_publishes_request_event_to_sink_and_dashboard_broadcaster() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (sink, mut sink_receiver) = RequestEventSink::with_capacity(8);
    let broadcaster = Arc::new(DashboardBroadcaster::with_capacity(8));
    let mut dashboard_receiver = broadcaster.subscribe();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook,
    )
    .with_request_event_sink(Some(Arc::new(sink)))
    .with_dashboard_broadcaster(Some(broadcaster));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test"}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);

    let sink_event = sink_receiver.try_recv().expect("sink receives event");
    let dashboard_event = dashboard_receiver
        .try_recv()
        .expect("dashboard receives event");
    assert_eq!(sink_event, dashboard_event);
    assert!(sink_receiver.try_recv().is_err());
    assert!(dashboard_receiver.try_recv().is_err());
    assert_eq!(sink_event.principal_id.as_deref(), Some("principal-test"));
    assert_eq!(sink_event.model.as_deref(), Some("claude-test"));
    assert_eq!(sink_event.status, 200);
    assert_eq!(sink_event.input_tokens, Some(1));
    assert_eq!(sink_event.output_tokens, Some(1));
}

#[tokio::test]
async fn lifecycle_full_request_event_queue_drops_without_response_failure() {
    let before = dropped_events_total();
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (sink, mut receiver) = RequestEventSink::with_capacity(1);
    sink.enqueue(filler_event())
        .expect("first event fills queue");
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook,
    )
    .with_request_event_sink(Some(Arc::new(sink)));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test"}"#,
        )))
        .await
        .expect("lifecycle handles request even when event queue is full");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert_eq!(dropped_events_total() - before, 1);
    assert_eq!(
        receiver
            .try_recv()
            .expect("filler remains queued")
            .request_id,
        "filled"
    );
    assert!(receiver.try_recv().is_err());
}

fn filler_event() -> RequestEvent {
    RequestEvent {
        ts: 1,
        request_id: "filled".to_owned(),
        principal_id: None,
        principal_kind: None,
        upstream: None,
        model: None,
        status: 200,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 1,
        error_code: None,
    }
}

fn assert_forbidden_keys_absent(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                assert!(!is_forbidden_key(key), "forbidden key present: {key}");
                assert_forbidden_keys_absent(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                assert_forbidden_keys_absent(value);
            }
        }
        _ => {}
    }
}

fn is_forbidden_key(key: &str) -> bool {
    matches!(
        key,
        "messages" | "system" | "tools" | "tool_use" | "content"
    )
}
