use crate::common;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use http::StatusCode;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, lifecycle_with,
    messages_request,
};

#[tokio::test]
async fn t2__oversized_messages_body_returns_413_before_upstream() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook,
    );

    let oversized = Bytes::from(vec![b'x'; 33 * 1024 * 1024]);
    let response = lifecycle
        .handle(messages_request(oversized))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(String::from_utf8_lossy(&body).contains("body_too_large"));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn t2__lifecycle__invalid_json_returns_400_before_auth() {
    let state = TestState::default();
    let signer_authn = TestAuthn::new(state.clone());
    let clock = cc_lb_testkit::fixed_clock(1_700_000_000);
    let authn = Arc::new(cc_lb_engine::api_keys::builtin_authn::BuiltinAuthn::new(
        cc_lb_config::DownstreamAuthMode::ApiKey,
        None,
        None,
        clock.clone(),
    ));
    let view = cc_lb_engine::DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(signer_authn.clone()))
        .global_router(Arc::new(common::TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
        .principal_view(signer_authn.principal_view.clone())
        .upstream_records(vec![common::default_upstream_record()])
        .build();
    let lifecycle = cc_lb_engine::Lifecycle::new_with_dynamic_view(
        authn,
        Arc::new(cc_lb_engine::DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        }),
        cc_lb_engine::LifecycleConfig::default(),
        clock,
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(b"{\"broken\"")))
        .await
        .expect("lifecycle handles invalid JSON");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).expect("error response is JSON")["error"]
            ["message"],
        "request body must be valid JSON"
    );
    assert_eq!(
        state.upstream_calls.load(Ordering::Relaxed),
        0,
        "invalid JSON must terminate before dispatch"
    );
}
