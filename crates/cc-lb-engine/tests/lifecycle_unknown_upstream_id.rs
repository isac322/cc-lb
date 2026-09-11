use crate::{common, router_lifecycle_support};

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use common::{collect_body, messages_request};
use router_lifecycle_support::{
    RouterLifecycleState, SelectingRouter, api_key_record, lifecycle_with_records, plugin_upstream,
};

#[tokio::test]
async fn t2__legacy_router_unknown_id_cannot_enter_pipeline_selection() {
    let known = Uuid::from_u128(1);
    let unknown = Uuid::from_u128(0xff);
    let state = RouterLifecycleState::default();
    let lifecycle = lifecycle_with_records(
        vec![api_key_record(known, "known", "http://known.local/")],
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

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).expect("response body is JSON"),
        json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}})
    );
    assert!(
        state
            .router_candidates
            .lock()
            .expect("router candidates lock")
            .is_empty(),
        "the legacy router cannot inject an ID into pipeline terminal selection"
    );
    assert_eq!(
        state
            .router_choice_names
            .lock()
            .expect("router choices lock")
            .as_slice(),
        &["known".to_owned()]
    );
    assert_eq!(
        state
            .dispatched_urls
            .lock()
            .expect("dispatched URLs lock")
            .as_slice(),
        &["http://known.local/v1/messages".to_owned()]
    );
    assert_eq!(
        *state.dispatch_calls.lock().expect("dispatch calls lock"),
        1
    );
}
