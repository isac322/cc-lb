use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_contract::{LifecycleBusReceiver, LifecycleEvent, RequestEventBus};
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, ShapePluginCache,
};
use cc_lb_engine::{DispatchError, UpstreamDispatch};
use cc_lb_plugin_api::{
    DialectError, Principal, RequestContext, ShapedRequest, ShapedRequestBuilder, SignedRequest,
    Upstream, UpstreamDialect,
};
use cc_lb_storage_api::types::{KeyStatus, Limit as StoredLimit, LimitKind, StoredApiKeyRecord};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use tokio::sync::broadcast;

use crate::common::{TestAuthn, TestLifecycleBus, TestRouter, TestState, lifecycle_with_parts};

pub const SINGLE_ATTEMPT_RESERVATION: i64 = 4_008;

pub fn limit_engine_and_record() -> (Arc<LimitEngine>, StoredApiKeyRecord) {
    (
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_engine::SystemClock),
        ),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            limit_overrides: vec![StoredLimit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: SINGLE_ATTEMPT_RESERVATION,
            }],
            ..StoredApiKeyRecord::default()
        },
    )
}

pub fn lifecycle_receiver(test_bus: &TestLifecycleBus) -> broadcast::Receiver<LifecycleEvent> {
    let LifecycleBusReceiver::InMemory(receiver) = test_bus.bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    receiver
}

pub fn lifecycle_with_failing_dialect(
    state: TestState,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> cc_lb_engine::Lifecycle {
    let mut chains = std::collections::HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            None,
            ObservabilityHooksCache::Inherit,
            DialectCache::Explicit(ShapePluginCache {
                dialect: Arc::new(ShapeFailureDialect),
            }),
        ),
    );
    let authn = TestAuthn::with_principal_view(
        state,
        Arc::new(
            cc_lb_engine::api_keys::principal_view::PrincipalView::for_tests(
                "principal-test",
                true,
                vec!["*".to_owned()],
                Vec::new(),
                chains,
            ),
        ),
    );
    lifecycle_with_parts(
        authn,
        Arc::new(TestRouter {
            base_url: url::Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        dispatcher,
        Vec::new(),
        cc_lb_engine::LifecycleConfig::default(),
    )
}

pub async fn collect_events_until_terminal(
    receiver: &mut broadcast::Receiver<LifecycleEvent>,
) -> Vec<LifecycleEvent> {
    let mut events = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("lifecycle event arrives")
            .expect("lifecycle event channel remains open");
        let terminal = matches!(event, LifecycleEvent::RequestTerminated { .. });
        events.push(event);
        if terminal {
            return events;
        }
    }
}

pub fn assert_event_kinds(events: &[LifecycleEvent], expected: &[&str]) {
    assert_eq!(
        events.iter().map(LifecycleEvent::kind).collect::<Vec<_>>(),
        expected
    );
}

pub fn reservation_ids(events: &[LifecycleEvent]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::LimitDecision {
                decision: cc_lb_contract::LimitDecisionKind::Reserved { reservation_id, .. },
                ..
            } => Some(reservation_id.as_str()),
            _ => None,
        })
        .collect()
}

pub fn request_body(stream: bool) -> Bytes {
    let stream = if stream { "true" } else { "false" };
    Bytes::from(format!(
        r#"{{"model":"claude-test","max_tokens":8,"messages":[],"stream":{stream}}}"#
    ))
}

pub fn tokens_remaining(engine: &LimitEngine) -> i64 {
    engine
        .headers_for("key-test", "principal-test")
        .into_iter()
        .find_map(|(name, value)| (name == "anthropic-ratelimit-tokens-remaining").then_some(value))
        .expect("token limit header exists")
        .parse()
        .expect("token limit is numeric")
}

pub struct CapturingDispatch {
    pub captured_bodies: Arc<Mutex<Vec<Bytes>>>,
    pub upstream_body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for CapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.captured_bodies
            .lock()
            .expect("captured request lock")
            .push(request.body().clone());
        let mut response = Response::new(Body::from(self.upstream_body.clone()));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}

struct ShapeFailureDialect;

impl UpstreamDialect for ShapeFailureDialect {
    fn shape(
        &self,
        _ctx: &RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        _builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Err(DialectError::UnsupportedRequest {
            reason: "forced shape failure".to_owned(),
        })
    }
}
