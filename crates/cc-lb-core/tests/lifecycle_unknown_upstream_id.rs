mod common;
mod router_lifecycle_support;

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use uuid::Uuid;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    RouterLifecycleState, SelectingRouter, custom_record, lifecycle_with_records, plugin_upstream,
};

#[tokio::test]
async fn unknown_router_upstream_id_is_rejected_before_signing_or_dispatch() {
    let known = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let unknown = Uuid::parse_str("00000000-0000-0000-0000-0000000000ff").unwrap();
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        vec![custom_record(known, "known", "http://known.local/")],
        Arc::new(SelectingRouter {
            selected_id: Some(unknown),
            state: state.clone(),
            plugin_upstream: plugin_upstream("http://plugin.local/"),
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
    assert!(
        String::from_utf8_lossy(&body)
            .contains("router selected an upstream outside the candidate set")
    );
    assert!(
        state
            .router_choice_names
            .lock()
            .expect("router choices lock")
            .is_empty()
    );
    assert_eq!(
        *state.dispatch_calls.lock().expect("dispatch calls lock"),
        0
    );
}
