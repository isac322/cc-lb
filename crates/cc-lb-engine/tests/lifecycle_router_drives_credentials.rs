use crate::{common, router_lifecycle_support};

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use uuid::Uuid;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    RouterLifecycleState, SelectingRouter, api_key_record, lifecycle_with_records, plugin_upstream,
};

#[tokio::test]
async fn terminal_upstream_id_drives_credentials_and_dispatch_upstream() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let second = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        vec![
            api_key_record(first, "first", "http://first.local/"),
            api_key_record(second, "second", "http://second.local/"),
        ],
        Arc::new(SelectingRouter {
            selected_id: Some(second),
            state: state.clone(),
            plugin_upstream: plugin_upstream("http://plugin.local/"),
        }),
        state.clone(),
    );

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
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .is_empty()
    );
    assert_eq!(
        state
            .router_choice_names
            .lock()
            .expect("router choices lock")
            .as_slice(),
        &["first".to_owned()]
    );
    assert_eq!(
        state
            .dispatched_urls
            .lock()
            .expect("dispatched URLs lock")
            .as_slice(),
        &["http://first.local/v1/messages".to_owned()]
    );
}
