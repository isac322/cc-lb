mod common;

use std::sync::Arc;

use bytes::Bytes;
use cc_lb_core::PrincipalLimitStateSink;
use cc_lb_storage_redb::{PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState};
use http::{HeaderMap, HeaderValue};
use tokio::sync::mpsc::Receiver;

use common::{
    collect_body, lifecycle_with, messages_request, DispatchMode, MockDispatch, RecordingHook,
    TestAuthn, TestState,
};

#[tokio::test]
async fn lifecycle_persists_anthropic_rate_limit_headers() -> Result<(), Box<dyn std::error::Error>>
{
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let (sink, mut receiver) = PrincipalLimitStateSink::with_capacity(8);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(rate_limit_headers()),
        },
        hook,
    )
    .with_principal_limit_state_sink(Some(Arc::new(sink)));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (_status, _headers, _body) = collect_body(response).await;

    let states = drain_limit_states(&mut receiver);
    let request_state = states
        .iter()
        .find(|state| state.window == "5h" && state.kind == PrincipalLimitKind::Requests)
        .expect("request rate limit state is enqueued");
    let token_state = states
        .iter()
        .find(|state| state.window == "weekly" && state.kind == PrincipalLimitKind::Tokens)
        .expect("token rate limit state is enqueued");

    assert_eq!(states.len(), 2);
    assert_eq!(request_state.principal_id, "principal-test");
    assert_eq!(
        request_state.identity_kind,
        PrincipalLimitIdentityKind::Account
    );
    assert_eq!(request_state.identity_value.as_deref(), Some("org_test"));
    assert!(request_state.account_observed);
    assert_eq!(request_state.limit, Some(5000));
    assert_eq!(request_state.remaining, Some(4999));
    assert_eq!(token_state.reset.as_deref(), Some("2026-05-27T00:00:00Z"));
    Ok(())
}

fn drain_limit_states(receiver: &mut Receiver<PrincipalLimitState>) -> Vec<PrincipalLimitState> {
    let mut states = Vec::new();
    while let Ok(state) = receiver.try_recv() {
        states.push(state);
    }
    states
}

fn rate_limit_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org_test"),
    );
    headers.insert(
        "anthropic-ratelimit-requests-limit-5h",
        HeaderValue::from_static("5000"),
    );
    headers.insert(
        "anthropic-ratelimit-requests-remaining-5h",
        HeaderValue::from_static("4999"),
    );
    headers.insert(
        "anthropic-ratelimit-tokens-reset-weekly",
        HeaderValue::from_static("2026-05-27T00:00:00Z"),
    );
    headers
}
