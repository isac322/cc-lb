use crate::common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_domain::{Principal, TerminalStrategy, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_routing::{
    FilterError, FilterOutput, FilterPlugin, RouteDecision, RouteError, RouterPlugin,
};
use cc_lb_storage_api::Storage as StorageTrait;
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use http::StatusCode;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    messages_request,
};

#[tokio::test]
async fn t2__no_candidates_after_filters_returns_503_and_logs_request_event()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = upstream_id(1);
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let storage = InMemoryStorage::new();
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_pipeline(
        vec![Arc::new(KeepFilter {
            kept_upstream_ids: Vec::new(),
            calls: Arc::clone(&filter_calls),
        })],
        vec![upstream_record(upstream_id, "first")],
        Arc::new(RecordingRouter {
            calls: Arc::clone(&router_calls),
            selected_id: Some(upstream_id),
        }),
        state.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        )))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["error"]["type"], "route_no_upstream_after_filter");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        filter_calls.lock().expect("filter calls lock").as_slice(),
        &[vec![upstream_id]]
    );
    assert!(router_calls.lock().expect("router calls lock").is_empty());

    let events = storage.wait_for_request_events(1).await;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.status, StatusCode::SERVICE_UNAVAILABLE.as_u16());
    assert_eq!(
        event.error_code.as_deref(),
        Some("route_no_upstream_after_filter")
    );
    assert_eq!(event.principal_id.as_deref(), Some("principal-test"));
    assert_eq!(event.key_id.as_deref(), Some("none-mode"));
    assert_eq!(event.model.as_deref(), Some("claude-test"));
    let trace = event.routing_trace.as_ref().expect("routing trace logged");
    assert_eq!(trace.stages.len(), 1);
    assert_eq!(trace.stages[0].stage_name, "drop-all");
    assert_eq!(trace.stages[0].upstream_id, None);
    assert_eq!(
        trace
            .terminal_decision
            .as_ref()
            .and_then(|decision| decision.upstream_id),
        None
    );
    assert!(event.internal_errors.iter().any(|error| {
        error.message.as_deref() == Some("no upstream candidates remain after routing filters")
    }));
    Ok(())
}

#[tokio::test]
async fn t2__lifecycle__principal_missing_from_view_returns_500()
-> Result<(), Box<dyn std::error::Error>> {
    let state = TestState::default();
    let storage = InMemoryStorage::new();
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
    let lifecycle = lifecycle_with_principal_view(
        principal_view,
        vec![upstream_record(upstream_id(1), "first")],
        state.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body["error"]["message"],
        "authenticated principal is unavailable"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    let events = storage.wait_for_request_events(1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::INTERNAL_SERVER_ERROR.as_u16());
    assert_eq!(events[0].error_code.as_deref(), Some("principal_missing"));
    Ok(())
}

#[tokio::test]
async fn t2__lifecycle__router_pipeline_unavailable_returns_502()
-> Result<(), Box<dyn std::error::Error>> {
    let state = TestState::default();
    let storage = InMemoryStorage::new();
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let principal_view = principal_view_with_error("plugin load error");
    let lifecycle = lifecycle_with_principal_view(
        principal_view,
        vec![upstream_record(upstream_id(1), "first")],
        state.clone(),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body["error"]["message"],
        "router pipeline is unavailable for this request"
    );
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    let events = storage.wait_for_request_events(1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::BAD_GATEWAY.as_u16());
    assert_eq!(
        events[0].error_code.as_deref(),
        Some("router_pipeline_unavailable")
    );
    Ok(())
}

fn lifecycle_with_pipeline(
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    router: Arc<dyn RouterPlugin>,
    state: TestState,
) -> Lifecycle {
    let principal_view = principal_view(filters);
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(router)
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        fixed_clock(1_700_000_000),
    )
}

fn lifecycle_with_principal_view(
    principal_view: Arc<PrincipalView>,
    records: Vec<UpstreamRecord>,
    state: TestState,
) -> Lifecycle {
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(common::TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }))
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        fixed_clock(1_700_000_000),
    )
}

fn principal_view_with_error(error: &str) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: Some(Arc::from(error)),
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
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}
fn principal_view(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
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
    Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn upstream_record(id: Uuid, name: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        api_key_ciphertext: Some(Vec::new()),
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

fn upstream_id(index: u128) -> Uuid {
    Uuid::from_u128(index)
}

struct RecordingRouter {
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    selected_id: Option<Uuid>,
}

impl RouterPlugin for RecordingRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.calls.lock().expect("router calls lock").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(RouteDecision {
            upstream_id: self.selected_id,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(common::PassthroughDialect {
                base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
            }),
        })
    }
}

struct KeepFilter {
    kept_upstream_ids: Vec<Uuid>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for KeepFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().expect("filter calls lock").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "drop-all removed every candidate".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "drop-all"
    }
}
