mod storage_support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::{BusReceiver, RequestEventBus};
use cc_lb_engine::{BreakerRegistry, DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use http_body_util::BodyExt;

use crate::common::{
    TestAuthn, TestLifecycleBus, TestRouter, TestState, lifecycle_with_parts, messages_request,
};
use storage_support::TestStorage;

#[test]
fn lifecycle_request_events_current_registry_smoke() {
    let _registry = BreakerRegistry::new();
}

#[tokio::test]
async fn hermes_root_identity_crosses_proxy_and_persists_without_body_rewrite()
-> Result<(), Box<dyn std::error::Error>> {
    let original_body = Bytes::from_static(
        br#"{"model":"claude-test","session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":[{"type":"text","text":"You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"}],"messages":[{"role":"user","content":"Review the auth change"}],"tools":[{"name":"terminal"}]}"#,
    );
    let storage = TestStorage::new();
    let test_bus =
        TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let BusReceiver::InMemory(mut update_rx) = test_bus.bus.subscribe() else {
        panic!("expected in-memory request-event receiver");
    };
    let dispatcher = CapturingDispatch::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(TestRouter {
            base_url: "http://upstream.local/"
                .parse()
                .expect("fixture URL parses"),
        }),
        Arc::new(dispatcher.clone()),
        Vec::new(),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(original_body.clone()))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                update_rx.recv().await.expect("request update delivered"),
                cc_lb_request_log::RequestEventUpdate::Final(_)
            ) {
                break;
            }
        }
    })
    .await
    .expect("final request event arrives");

    assert_eq!(
        dispatcher
            .bodies
            .lock()
            .expect("captured bodies lock")
            .as_slice(),
        &[original_body]
    );
    let events = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.thread_id.as_deref(), Some("hermes-root-session"));
    assert_eq!(
        event.observed_session_id.as_deref(),
        Some("hermes-root-session")
    );
    assert_eq!(event.session_id_source.as_deref(), Some("session_id"));
    assert_eq!(event.request_kind.as_deref(), Some("subagent"));
    assert_eq!(event.parent_session_id, None);
    assert_eq!(event.client_app, None);

    Ok(())
}

#[derive(Clone, Default)]
struct CapturingDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for CapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured bodies lock")
            .push(request.body().clone());

        let mut response = Response::new(Body::from(Bytes::from_static(
            br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
