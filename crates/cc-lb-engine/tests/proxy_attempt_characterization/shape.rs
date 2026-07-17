use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_contract::LifecycleEvent;
use cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber;
use cc_lb_plugin_api::{InternalErrorKind, InternalErrorStage};
use http::StatusCode;

use crate::common::{TestLifecycleBus, TestState, collect_body, messages_request};
use crate::support::{
    CapturingDispatch, assert_event_kinds, collect_events_until_terminal, lifecycle_receiver,
    lifecycle_with_failing_dialect, limit_engine_and_record, reservation_ids, tokens_remaining,
};

const SHAPE_FALLBACK_EVENT_KINDS: &[&str] = &[
    "request_started",
    "parse_completed",
    "auth_completed",
    "authentication_completed",
    "route_completed",
    "limit_decision",
    "upstream_attempt",
    "provider_error_observed",
    "upstream_response_started",
    "usage_observed",
    "stream_completed",
    "request_terminated",
];

#[derive(Debug)]
struct ShapeFallbackOutcome {
    response_body: Bytes,
    captured_bodies: Vec<Bytes>,
    event_kinds: Vec<&'static str>,
}

#[tokio::test]
async fn proxy_attempt_characterization_shape_failure_falls_back_to_raw_passthrough() {
    let downstream_body = Bytes::from_static(
        br#" { "model":"claude-test", "max_tokens":8, "messages":[], "opaque":"keep" } "#,
    );
    let upstream_body = Bytes::from_static(
        br#" {"type":"message","usage":{"input_tokens":2,"output_tokens":3},"opaque":[1,2]} "#,
    );

    let (status, events, outcome, limit_engine) =
        run_shape_fallback(&downstream_body, &upstream_body).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        outcome.captured_bodies.as_slice(),
        &[downstream_body.clone()]
    );
    assert_eq!(outcome.response_body, upstream_body);
    assert_eq!(outcome.event_kinds, SHAPE_FALLBACK_EVENT_KINDS);
    assert_event_kinds(&events, SHAPE_FALLBACK_EVENT_KINDS);
    assert!(matches!(
        events[7],
        LifecycleEvent::ProviderErrorObserved {
            ref code,
            ref source,
            ref message,
            ..
        } if code == "shape_error"
            && source == "dialect"
            && message == "unsupported request: forced shape failure"
    ));
    let LifecycleEvent::RequestTerminated {
        internal_errors, ..
    } = events.last().expect("request emits terminal event")
    else {
        panic!("expected terminal event");
    };
    assert!(matches!(
        internal_errors.as_slice(),
        [error]
            if error.stage == InternalErrorStage::Shape
                && error.kind == InternalErrorKind::Trap
                && error.message.as_deref() == Some("unsupported request: forced shape failure")
    ));
    assert_eq!(reservation_ids(&events).len(), 1);
    assert_eq!(tokens_remaining(&limit_engine), 4_003);
}

async fn run_shape_fallback(
    downstream_body: &Bytes,
    upstream_body: &Bytes,
) -> (
    StatusCode,
    Vec<LifecycleEvent>,
    ShapeFallbackOutcome,
    Arc<cc_lb_engine::api_keys::limit_engine::LimitEngine>,
) {
    let (limit_engine, record) = limit_engine_and_record();
    let test_bus = TestLifecycleBus::new();
    let mut event_rx = lifecycle_receiver(&test_bus);
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(
        test_bus.bus.attach_lifecycle_limit_reconcile(32),
        Arc::clone(&limit_engine),
    );
    let captured_bodies = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = lifecycle_with_failing_dialect(
        TestState::default(),
        Arc::new(CapturingDispatch {
            captured_bodies: Arc::clone(&captured_bodies),
            upstream_body: upstream_body.clone(),
        }),
    )
    .with_static_limit_subject(
        Arc::clone(&limit_engine),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        record,
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(downstream_body.clone()))
        .await
        .expect("raw fallback dispatches");
    let (status, _headers, response_body) = collect_body(response).await;
    let events = collect_events_until_terminal(&mut event_rx).await;
    reconcile.shutdown().await;
    let event_kinds = events.iter().map(LifecycleEvent::kind).collect();
    let captured_bodies = captured_bodies
        .lock()
        .expect("captured request lock")
        .clone();

    (
        status,
        events,
        ShapeFallbackOutcome {
            response_body,
            captured_bodies,
            event_kinds,
        },
        limit_engine,
    )
}
