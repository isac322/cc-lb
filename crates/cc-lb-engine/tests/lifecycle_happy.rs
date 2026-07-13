use crate::common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use http_body_util::BodyExt;
use tokio::time::{Duration, timeout};

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestState,
    lifecycle_with, messages_request,
};

#[tokio::test]
async fn happy_sse_relays_incrementally_and_observes_chunks() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new().with_hook_adapter(vec![
        hook.clone() as Arc<dyn cc_lb_observability::ObservabilityHook>
    ]);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("stream collects")
        .to_bytes();
    let data_lines = String::from_utf8_lossy(&body).matches("data:").count();
    println!("data_lines={data_lines}");
    assert!(
        data_lines >= 50,
        "expected at least 50 data lines, got {data_lines}"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert!(
        hook.events
            .lock()
            .expect("events lock")
            .iter()
            .any(|event| matches!(event, cc_lb_observability::ObserveEvent::Chunk { .. }))
    );
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(|event| {
            matches!(
                event,
                cc_lb_observability::ObserveEvent::RequestFinished {
                    input_tokens: Some(7),
                    output_tokens: Some(42),
                    ..
                }
            )
        }),
    )
    .await
    .expect("request-finished observation arrives");
}

#[tokio::test]
async fn happy_non_streaming_observes_usage_tokens() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new().with_hook_adapter(vec![
        hook.clone() as Arc<dyn cc_lb_observability::ObservabilityHook>
    ]);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let _body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();

    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(|event| {
            matches!(
                event,
                cc_lb_observability::ObserveEvent::RequestFinished {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                    ..
                }
            )
        }),
    )
    .await
    .expect("request-finished observation arrives");
}
