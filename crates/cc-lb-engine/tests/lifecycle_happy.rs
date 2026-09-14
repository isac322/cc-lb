use crate::common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, LimitCostEstimator,
};
use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::StatusCode;
use http_body_util::BodyExt;
use tokio::time::{Duration, timeout};

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestLifecycleBus, TestRouter, TestState,
    lifecycle_with, messages_request,
};

#[derive(Default)]
struct RecordingLimitCostEstimator {
    service_tiers: Mutex<Vec<Option<String>>>,
    upstream_kinds: Mutex<Vec<Option<String>>>,
}

impl LimitCostEstimator for RecordingLimitCostEstimator {
    fn estimate_max(
        &self,
        _model: &str,
        _max_input: u64,
        _max_output: u64,
        upstream_kind: Option<&str>,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.service_tiers
            .lock()
            .expect("service tier lock")
            .push(service_tier.map(ToOwned::to_owned));
        self.upstream_kinds
            .lock()
            .expect("upstream kind lock")
            .push(upstream_kind.map(ToOwned::to_owned));
        Some(1)
    }
}

#[tokio::test]
async fn happy_sse_relays_incrementally_and_observes_chunks() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        },
        hook.clone(),
    );

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
async fn happy_non_streaming_observes_usage_tokens() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook.clone(),
    );

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
async fn requested_service_tier_reaches_limit_cost_estimator_before_dispatch() {
    // Given a priority request with limit reservation enabled.
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let estimator = Arc::new(RecordingLimitCostEstimator::default());
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_engine::SystemClock),
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
    assert_eq!(
        *estimator.upstream_kinds.lock().expect("upstream kind lock"),
        vec![Some("anthropic_key".to_owned())]
    );
}

#[tokio::test]
async fn oauth_db_record_drives_route_pricing_and_rejection_audit_identity() {
    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        vec![Limit {
            kind: LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: 1,
        }],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view);
    let estimator = Arc::new(RecordingLimitCostEstimator::default());
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(32);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: "http://upstream.local/".parse().expect("test URL parses"),
        }))
        .global_observability_hooks(Vec::new())
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![oauth_upstream_record()])
        .build();
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_engine::SystemClock),
    );
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state: state.clone(),
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        }),
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
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
    .with_limit_cost_estimator(estimator.clone())
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","max_tokens":16,"messages":[]}"#,
        )))
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        *estimator.upstream_kinds.lock().expect("upstream kind lock"),
        vec![Some("anthropic_oauth".to_owned())]
    );

    let (route_kind, rejected_upstream_name) = timeout(Duration::from_secs(1), async {
        let mut route_kind = None;
        loop {
            match lifecycle_events
                .recv()
                .await
                .expect("lifecycle event channel remains open")
            {
                LifecycleEvent::RouteCompleted {
                    result: Ok(route), ..
                } => route_kind = route.upstream_kind,
                LifecycleEvent::LimitDecision {
                    decision:
                        LimitDecisionKind::Rejected {
                            route_summary: Some(route_summary),
                            ..
                        },
                    ..
                } => {
                    break (
                        route_kind.expect("successful route event precedes limit rejection"),
                        route_summary.upstream_name,
                    );
                }
                _ => {}
            }
        }
    })
    .await
    .expect("limit rejection lifecycle event arrives");

    assert_eq!(route_kind, "anthropic_oauth");
    assert_eq!(rejected_upstream_name, "oauth-db-primary");
}

#[tokio::test]
async fn global_hook_observes_authentication_error_without_event_bus() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = lifecycle_with(
        TestAuthn::new(state.clone()),
        MockDispatch {
            state,
            mode: DispatchMode::HeadersOk(http::HeaderMap::new()),
        },
        hook.clone(),
    );
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    request.headers_mut().insert(
        "x-api-key",
        http::HeaderValue::from_static("invalid-managed-key"),
    );

    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles invalid authentication");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    timeout(
        Duration::from_secs(1),
        hook.wait_for_event(|event| {
            matches!(
                event,
                cc_lb_observability::ObserveEvent::Error { code, source, .. }
                    if code == "authentication_error" && source == "authn"
            )
        }),
    )
    .await
    .expect("global hook authentication error arrives without event bus");
    assert!(
        hook.events
            .lock()
            .expect("events lock")
            .iter()
            .any(|event| matches!(
                event,
                cc_lb_observability::ObserveEvent::RequestFinished {
                    status: StatusCode::UNAUTHORIZED,
                    ..
                }
            )),
        "global hook receives terminal observation for authentication rejection"
    );
}

fn oauth_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::from_u128(1),
        name: "oauth-db-primary".to_owned(),
        kind: StorageUpstreamKind::AnthropicOauth,
        base_url: Some("http://upstream.local/".parse().expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}
