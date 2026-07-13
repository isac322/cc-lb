#![cfg(feature = "capture")]

mod common;
#[allow(dead_code)]
mod prompt_cache_routing_support;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_capture::hook::CaptureHandle;
use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, Principal, TerminalStrategy, UpstreamCandidate,
};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, TestClock};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::{HeaderValue, StatusCode};
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestRouter, TestState, collect_body,
    messages_request,
};
use prompt_cache_routing_support::TestPromptCacheObservationCache;

const FIRST_UPSTREAM_ID: Uuid = Uuid::from_u128(1);
const SECOND_UPSTREAM_ID: Uuid = Uuid::from_u128(2);
const CAPTURED_AT_UNIX_SECS: u64 = 1_700_000_000;

#[tokio::test]
async fn captures_full_routing_input_when_prior_filter_narrows_candidates() {
    // Given
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![
        Arc::new(KeepFilter::new(
            Uuid::nil(),
            "prior-narrowing",
            vec![SECOND_UPSTREAM_ID],
        )),
        Arc::new(KeepFilter::new(
            BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
            "subscription-preference",
            vec![SECOND_UPSTREAM_ID],
        )),
    ];
    let (lifecycle, mut captured) = lifecycle_with_capture(filters);

    // When
    let response = lifecycle
        .handle(request_with_cache_breakpoint())
        .await
        .expect("lifecycle handles request");
    let (status, _, _) = collect_body(response).await;

    // Then
    assert_eq!(status, StatusCode::OK);
    let input = captured.try_recv().expect("one captured routing input");
    assert!(
        captured.try_recv().is_err(),
        "capture must emit exactly once"
    );
    assert_eq!(input.request_id, "capture-request");
    assert!(!input.event_id.is_empty());
    assert_eq!(
        input
            .candidates
            .iter()
            .map(|candidate| candidate.upstream_id)
            .collect::<Vec<_>>(),
        vec![FIRST_UPSTREAM_ID, SECOND_UPSTREAM_ID],
    );
    assert_eq!(
        input.subscription_preference_input_upstream_ids,
        vec![SECOND_UPSTREAM_ID],
    );
    assert_eq!(input.breakpoints.len(), 1);
    assert!(!input.breakpoints[0].lookback_prefixes.is_empty());
    assert_eq!(input.captured_at_unix_ms, CAPTURED_AT_UNIX_SECS * 1_000,);
    assert_eq!(
        input
            .routing_trace
            .terminal_decision
            .and_then(|decision| decision.upstream_id),
        Some(SECOND_UPSTREAM_ID),
    );
}

#[tokio::test]
async fn captures_routing_input_when_filters_remove_all_candidates() {
    // Given
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![
        Arc::new(KeepFilter::new(Uuid::nil(), "remove-all", Vec::new())),
        Arc::new(KeepFilter::new(
            BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
            "subscription-preference",
            Vec::new(),
        )),
    ];
    let (lifecycle, mut captured) = lifecycle_with_capture(filters);

    // When
    let response = lifecycle
        .handle(request_with_cache_breakpoint())
        .await
        .expect("lifecycle handles request");
    let (status, _, _) = collect_body(response).await;

    // Then
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let input = captured.try_recv().expect("failure routing input captured");
    assert!(
        captured.try_recv().is_err(),
        "capture must emit exactly once"
    );
    assert_eq!(input.candidates.len(), 2);
    assert!(input.subscription_preference_input_upstream_ids.is_empty(),);
    assert_eq!(
        input
            .routing_trace
            .terminal_decision
            .and_then(|decision| decision.upstream_id),
        None,
    );
}

fn lifecycle_with_capture(
    filters: Vec<Arc<dyn FilterPlugin>>,
) -> (
    Lifecycle,
    mpsc::Receiver<cc_lb_capture::schema::CapturedRequestInput>,
) {
    let state = TestState::default();
    let principal_view = principal_view(filters);
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(vec![
            upstream_record(FIRST_UPSTREAM_ID),
            upstream_record(SECOND_UPSTREAM_ID),
        ])
        .prompt_cache_observation_cache(Arc::new(TestPromptCacheObservationCache::new(
            HashMap::new(),
        )))
        .build();
    let mut config = LifecycleConfig::default();
    config.prompt_cache_shadow.enabled = true;
    let event_bus = TestLifecycleBus::new();
    let (sender, receiver) = mpsc::channel(4);
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Default::default()),
        }),
        config,
        Arc::new(TestClock::new_at_secs(CAPTURED_AT_UNIX_SECS)),
    )
    .with_event_bus(event_bus.bus_arc())
    .with_capture_handle(CaptureHandle::new(sender));
    (lifecycle, receiver)
}

fn principal_view(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let chains = HashMap::from([(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    )]);
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn upstream_record(id: Uuid) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: format!("upstream-{id}"),
        kind: UpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        revision: 1,
        ..UpstreamRecord::default()
    }
}

fn request_with_cache_breakpoint() -> http::Request<Bytes> {
    let mut request = messages_request(Bytes::from_static(
        br#"{
            "model":"claude-sonnet-4-5",
            "system":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}],
            "messages":[{"role":"user","content":"hello"}],
            "max_tokens":16
        }"#,
    ));
    request
        .headers_mut()
        .insert("request-id", HeaderValue::from_static("capture-request"));
    request
}

struct KeepFilter {
    plugin_id: Uuid,
    name: &'static str,
    kept_upstream_ids: Vec<Uuid>,
}

impl KeepFilter {
    fn new(plugin_id: Uuid, name: &'static str, kept_upstream_ids: Vec<Uuid>) -> Self {
        Self {
            plugin_id,
            name,
            kept_upstream_ids,
        }
    }
}

impl FilterPlugin for KeepFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: self.name.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        self.plugin_id
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}
