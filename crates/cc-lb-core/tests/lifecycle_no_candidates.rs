mod common;
mod router_lifecycle_support;

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    RejectingEmptyRouter, RouterLifecycleState, lifecycle_with_records,
};

#[tokio::test]
async fn no_candidates_are_passed_to_router_and_return_route_error() {
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        Vec::new(),
        Arc::new(RejectingEmptyRouter {
            state: state.clone(),
        }),
        state.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(String::from_utf8_lossy(&body).contains("no upstream route is configured"));
    assert_eq!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .as_slice(),
        &[Vec::<uuid::Uuid>::new()]
    );
    assert_eq!(
        *state.dispatch_calls.lock().expect("dispatch calls lock"),
        0
    );
}
