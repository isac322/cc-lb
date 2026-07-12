mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_domain::{Principal, UpstreamCandidate};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, PerCandidateReason, RoutingContext};
use cc_lb_upstream::SignedRequest;
use http::{Method, Response, StatusCode};
use serde_json::json;
use uuid::Uuid;

use common::{TestAuthn, TestRouter, TestState, lifecycle_with_parts, messages_request};

type FilterOutcome = (String, Vec<PerCandidateReason>);

#[tokio::test]
async fn fixed_request_preserves_routing_shape_and_signing_observables() {
    let state = ParityState::default();
    let lifecycle = lifecycle(state.clone());
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[{"role":"user","content":"hi"}]}"#,
    ));

    let response = lifecycle
        .handle(request)
        .await
        .expect("lifecycle handles request");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        state.filter_outcomes(),
        vec![("keep-fixture".to_owned(), Vec::new())]
    );
    assert_eq!(
        state.dispatched(),
        vec![DispatchedRequest {
            url: "http://upstream.local/v1/messages".to_owned(),
            method: Method::POST,
            body: Bytes::from_static(
                br#"{"model":"claude-test","messages":[{"role":"user","content":"hi"}]}"#,
            ),
            api_key: "sk-ant-initial".to_owned(),
            anthropic_version: "2023-06-01".to_owned(),
        }],
    );
}

fn lifecycle(state: ParityState) -> Lifecycle {
    let filter: Arc<dyn FilterPlugin> = Arc::new(RecordingFilter {
        state: state.clone(),
    });
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![filter],
        terminal: cc_lb_domain::TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view);

    lifecycle_with_parts(
        authn,
        Arc::new(TestRouter {
            base_url: "http://upstream.local/"
                .parse()
                .expect("fixture URL parses"),
        }),
        Arc::new(CapturingDispatch { state }),
        Vec::new(),
        LifecycleConfig::default(),
    )
}

#[derive(Clone, Default)]
struct ParityState {
    filter_outcomes: Arc<Mutex<Vec<FilterOutcome>>>,
    dispatched: Arc<Mutex<Vec<DispatchedRequest>>>,
}

impl ParityState {
    fn filter_outcomes(&self) -> Vec<FilterOutcome> {
        self.filter_outcomes
            .lock()
            .expect("filter outcomes lock")
            .clone()
    }

    fn dispatched(&self) -> Vec<DispatchedRequest> {
        self.dispatched.lock().expect("dispatch lock").clone()
    }
}

struct RecordingFilter {
    state: ParityState,
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        _context: &RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let output = FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: "keep-fixture".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        };
        self.state
            .filter_outcomes
            .lock()
            .expect("filter outcomes lock")
            .push((output.reason.clone(), output.per_candidate_reasons.clone()));
        Ok(output)
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "recording-filter"
    }
}

struct CapturingDispatch {
    state: ParityState,
}

#[async_trait]
impl UpstreamDispatch for CapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let api_key = request
            .headers()
            .get("x-api-key")
            .expect("signer inserts API key")
            .to_str()
            .expect("API key is valid header text")
            .to_owned();
        let anthropic_version = request
            .headers()
            .get("anthropic-version")
            .expect("shaper preserves Anthropic version")
            .to_str()
            .expect("Anthropic version is valid header text")
            .to_owned();
        self.state
            .dispatched
            .lock()
            .expect("dispatch lock")
            .push(DispatchedRequest {
                url: request.url().to_string(),
                method: request.method().clone(),
                body: request.body().clone(),
                api_key,
                anthropic_version,
            });

        let mut response = Response::new(Body::from(Bytes::from(
            json!({"type":"message","usage":{"input_tokens":1,"output_tokens":1}}).to_string(),
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DispatchedRequest {
    url: String,
    method: Method,
    body: Bytes,
    api_key: String,
    anthropic_version: String,
}
