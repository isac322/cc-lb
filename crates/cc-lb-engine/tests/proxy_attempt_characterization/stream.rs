use std::sync::Arc;

use cc_lb_contract::LifecycleEvent;
use cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, StatusCode};

use crate::common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestState,
    lifecycle_with, messages_request,
};
use crate::support::{
    SINGLE_ATTEMPT_RESERVATION, assert_event_kinds, collect_events_until_terminal,
    lifecycle_receiver, limit_engine_and_record, request_body, reservation_ids, tokens_remaining,
};

#[tokio::test]
async fn proxy_attempt_characterization_stream_drop_after_headers_refunds_without_reconcile() {
    let (limit_engine, record) = limit_engine_and_record();
    let test_bus = TestLifecycleBus::new();
    let mut event_rx = lifecycle_receiver(&test_bus);
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(
        test_bus.bus.attach_lifecycle_limit_reconcile(32),
        Arc::clone(&limit_engine),
    );
    let lifecycle = lifecycle_with(
        TestAuthn::new(TestState::default()),
        MockDispatch {
            state: TestState::default(),
            mode: DispatchMode::StreamingOk,
        },
        Arc::new(RecordingHook::default()),
    )
    .with_static_limit_subject(
        Arc::clone(&limit_engine),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        record,
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(request_body(true)))
        .await
        .expect("lifecycle returns streaming headers");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE),
        Some(&HeaderValue::from_static("text/event-stream"))
    );
    drop(response.into_body());

    let events = collect_events_until_terminal(&mut event_rx).await;
    reconcile.shutdown().await;

    assert_event_kinds(
        &events,
        &[
            "request_started",
            "parse_completed",
            "auth_completed",
            "authentication_completed",
            "route_completed",
            "limit_decision",
            "upstream_attempt",
            "upstream_response_started",
            "request_terminated",
        ],
    );
    assert!(matches!(
        events.last(),
        Some(LifecycleEvent::RequestTerminated {
            client_status: 499,
            ..
        })
    ));
    assert_eq!(reservation_ids(&events).len(), 1);
    assert_eq!(tokens_remaining(&limit_engine), SINGLE_ATTEMPT_RESERVATION);
}
