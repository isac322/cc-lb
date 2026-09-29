use crate::{common, router_lifecycle_support};

use bytes::Bytes;
use http::StatusCode;

use common::{collect_body, messages_request};
use router_lifecycle_support::{RouterLifecycleState, lifecycle_with_records};

#[tokio::test]
async fn no_candidates_return_route_error_without_dispatch() {
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(Vec::new(), state.clone());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles request");
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        String::from_utf8_lossy(&body)
            .contains("no upstream candidates remain after routing filters")
    );
    assert_eq!(
        *state.dispatch_calls.lock().expect("dispatch calls lock"),
        0
    );
}
