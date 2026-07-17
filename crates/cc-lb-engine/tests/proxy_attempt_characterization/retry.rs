use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use cc_lb_contract::LifecycleEvent;
use cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber;
use http::StatusCode;

use crate::common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestState,
    collect_body, lifecycle_with, messages_request,
};
use crate::support::{
    assert_event_kinds, collect_events_until_terminal, lifecycle_receiver, limit_engine_and_record,
    request_body, reservation_ids, tokens_remaining,
};

#[tokio::test]
async fn proxy_attempt_characterization_401_refresh_retries_with_one_reservation() {
    let (limit_engine, record) = limit_engine_and_record();
    let test_bus = TestLifecycleBus::new();
    let mut event_rx = lifecycle_receiver(&test_bus);
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(
        test_bus.bus.attach_lifecycle_limit_reconcile(32),
        Arc::clone(&limit_engine),
    );
    let state = TestState::default();
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(VecDeque::from([
                StatusCode::UNAUTHORIZED,
                StatusCode::OK,
            ])))),
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
        .handle(messages_request(request_body(false)))
        .await
        .expect("lifecycle handles refresh retry");
    let (status, _headers, _body) = collect_body(response).await;
    let events = collect_events_until_terminal(&mut event_rx).await;
    reconcile.shutdown().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 2);
    assert_eq!(state.refresh_count.load(Ordering::Relaxed), 1);
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
            "upstream_attempt",
            "upstream_response_started",
            "usage_observed",
            "stream_completed",
            "request_terminated",
        ],
    );
    assert!(matches!(
        events[7],
        LifecycleEvent::UpstreamResponseStarted { status: 401, .. }
    ));
    assert!(matches!(
        events[9],
        LifecycleEvent::UpstreamResponseStarted { status: 200, .. }
    ));
    assert_eq!(reservation_ids(&events).len(), 1);
    assert_eq!(tokens_remaining(&limit_engine), 4_006);
}
