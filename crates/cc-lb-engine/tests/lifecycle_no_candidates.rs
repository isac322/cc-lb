use crate::{common, router_lifecycle_support};

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    RejectingEmptyRouter, RouterLifecycleState, lifecycle_with_records,
};

#[tokio::test]
async fn t2__no_candidates_are_passed_to_router_and_return_route_error() {
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

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        String::from_utf8_lossy(&body)
            .contains("no upstream candidates remain after routing filters")
    );
    assert!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .is_empty(),
        "legacy global_router is no longer called when candidates are empty"
    );
    assert_eq!(
        *state.dispatch_calls.lock().expect("dispatch calls lock"),
        0
    );
}
