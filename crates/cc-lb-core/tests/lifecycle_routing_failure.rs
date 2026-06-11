mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::{
    DynamicViewBuilder, DynamicViewHolder, ErrorNormalizer, Lifecycle, LifecycleConfig,
};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, Principal, RequestContext, RouteDecision, RouteError,
    RouterPlugin, TerminalStrategy, Upstream, UpstreamCandidate,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_redb::Storage as RedbStorage;
use http::StatusCode;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use common::{DispatchMode, MockDispatch, TestAuthn, TestState, collect_body, messages_request};

#[tokio::test]
async fn no_candidates_after_filters_returns_503_and_logs_request_event()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = upstream_id(1);
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let _dir = tempfile::tempdir()?;
    let storage = Arc::new(RedbStorage::open(
        &_dir.path().join("lifecycle-routing-failure.redb"),
        [31; 32],
    )?);
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
    .with_request_event_storage(Arc::clone(&storage) as Arc<dyn StorageTrait>);

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

    let events = RequestEventStore::query_request_events(storage.as_ref(), 0, u64::MAX, 10).await?;
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
async fn router_none_decision_does_not_fall_back_to_first_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = upstream_id(1);
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let lifecycle = lifecycle_with_pipeline(
        Vec::new(),
        vec![upstream_record(upstream_id, "first")],
        Arc::new(RecordingRouter {
            calls: Arc::clone(&router_calls),
            selected_id: None,
        }),
        state.clone(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        )))
        .await?;
    let (status, _headers, body) = collect_body(response).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(body["error"]["type"], "route_not_configured");
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        router_calls.lock().expect("router calls lock").as_slice(),
        &[vec![upstream_id]]
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
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(router)
        .dispatcher(Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
        }))
        .global_observability_hooks(Vec::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
    )
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
        refresh_lease_holder: None,
        refresh_lease_until_unix_secs: None,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
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
        _ctx: &RequestContext,
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
            upstream: Upstream::AnthropicDirect,
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
        _ctx: &RequestContext,
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
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "drop-all"
    }
}
