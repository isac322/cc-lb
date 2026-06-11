mod common;
mod router_lifecycle_support;

use bytes::Bytes;
use http::StatusCode;
use uuid::Uuid;

use common::{collect_body, messages_request};
use router_lifecycle_support::{RouterLifecycleState, api_key_record, lifecycle_with_records};

#[tokio::test]
async fn router_upstream_id_drives_credentials_and_dispatch_upstream() {
    let first = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let second = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        vec![
            api_key_record(first, "first", "http://first.local/"),
            api_key_record(second, "second", "http://second.local/"),
        ],
        vec![],
        state.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .as_slice(),
        &[vec![first]]
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
        &["https://api.anthropic.com/v1/messages".to_owned()]
    );
}
