use crate::common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_engine::LimitCostEstimator;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use http_body_util::BodyExt;
use tokio::time::{Duration, timeout};

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestState,
    lifecycle_with, messages_request,
};

#[derive(Default)]
struct RecordingLimitCostEstimator {
    service_tiers: Mutex<Vec<Option<String>>>,
}

impl LimitCostEstimator for RecordingLimitCostEstimator {
    fn estimate_max(
        &self,
        _model: &str,
        _max_input: u64,
        _max_output: u64,
        _upstream_kind: Option<&str>,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.service_tiers
            .lock()
            .expect("service tier lock")
            .push(service_tier.map(ToOwned::to_owned));
        Some(1)
    }
}

#[tokio::test]
async fn t2__happy_sse_relays_incrementally_and_observes_chunks() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new().with_hook_adapter(vec![
        hook.clone() as Arc<dyn cc_lb_observability::ObservabilityHook>
    ]);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"stream":true}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("stream collects")
        .to_bytes();
    let data_lines = String::from_utf8_lossy(&body).matches("data:").count();
    println!("data_lines={data_lines}");
    assert!(
        data_lines >= 50,
        "expected at least 50 data lines, got {data_lines}"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert!(
        hook.events
            .lock()
            .expect("events lock")
            .iter()
            .any(|event| matches!(event, cc_lb_observability::ObserveEvent::Chunk { .. }))
    );
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(|event| {
            matches!(
                event,
                cc_lb_observability::ObserveEvent::RequestFinished {
                    input_tokens: Some(7),
                    output_tokens: Some(42),
                    ..
                }
            )
        }),
    )
    .await
    .expect("request-finished observation arrives");
}

#[tokio::test]
async fn t2__happy_non_streaming_observes_usage_tokens() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let test_bus = TestLifecycleBus::new().with_hook_adapter(vec![
        hook.clone() as Arc<dyn cc_lb_observability::ObservabilityHook>
    ]);
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), http::StatusCode::OK);
    let _body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();

    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(|event| {
            matches!(
                event,
                cc_lb_observability::ObserveEvent::RequestFinished {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                    ..
                }
            )
        }),
    )
    .await
    .expect("request-finished observation arrives");
}

#[tokio::test]
async fn t2__requested_service_tier_reaches_limit_cost_estimator_before_dispatch() {
    // Given a priority request with limit reservation enabled.
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let estimator = Arc::new(RecordingLimitCostEstimator::default());
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        cc_lb_testkit::fixed_clock(1_700_000_000),
    );
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook,
    )
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_limit_cost_estimator(estimator.clone());

    // When the request crosses the proxy lifecycle.
    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":16,"service_tier":"Priority-Raw","messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");

    // Then reservation uses the exact raw requested tier.
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        *estimator.service_tiers.lock().expect("service tier lock"),
        vec![Some("Priority-Raw".to_owned())]
    );
}
