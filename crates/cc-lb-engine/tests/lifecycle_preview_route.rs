mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::lifecycle::{PreviewRouteError, PreviewRouteInput};
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, ErrorNormalizer, Lifecycle, LifecycleConfig,
};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, Principal, RequestContext, RouteDecision, RouteError,
    RouterPlugin, TerminalStrategy, UpstreamCandidate,
};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::HeaderMap;
use url::Url;
use uuid::Uuid;

use common::{DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState};

const PRINCIPAL: &str = "principal-test";

#[tokio::test]
async fn happy_path_returns_trace_with_stages_and_winner() {
    let (lifecycle, upstream_ids) = build_lifecycle(vec![kept_filter("keep-first", vec![0])]);

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .expect("preview succeeds");

    assert_eq!(outcome.trace.stages.len(), 1);
    assert_eq!(outcome.trace.stages[0].stage_name, "keep-first");
    assert_eq!(
        outcome
            .trace
            .terminal_decision
            .as_ref()
            .unwrap()
            .upstream_id,
        Some(upstream_ids[0])
    );
    assert_eq!(outcome.winner_upstream_id, Some(upstream_ids[0]));
    assert_eq!(outcome.winner_upstream_name.as_deref(), Some("upstream-0"));
}

#[tokio::test]
async fn unknown_principal_returns_error() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("keep-first", vec![0])]);

    let err = lifecycle
        .preview_route(input("nonexistent-principal", None))
        .expect_err("must fail");

    match err {
        PreviewRouteError::PrincipalNotFound(id) => assert_eq!(id, "nonexistent-principal"),
        other => panic!("expected PrincipalNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn same_request_id_produces_deterministic_winner() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("keep-all", vec![0, 1, 2])]);

    let first = lifecycle
        .preview_route(input(PRINCIPAL, Some("fixed-req-id")))
        .expect("first preview succeeds");
    let second = lifecycle
        .preview_route(input(PRINCIPAL, Some("fixed-req-id")))
        .expect("second preview succeeds");

    assert_eq!(first.winner_upstream_id, second.winner_upstream_id);
    assert_eq!(first.winner_upstream_name, second.winner_upstream_name);
}

#[tokio::test]
async fn pipeline_instantiation_error_returns_error() {
    let upstream_records = default_upstream_records();
    let principal_view = principal_view_with_broken_pipeline();
    let lifecycle = build_lifecycle_from(principal_view, upstream_records);

    let err = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .expect_err("must fail");

    match err {
        PreviewRouteError::PipelineInstantiationError(detail) => {
            assert!(
                detail.contains("broken pipeline fixture"),
                "detail was: {detail}"
            );
        }
        other => panic!("expected PipelineInstantiationError, got {other:?}"),
    }
}

#[tokio::test]
async fn empty_candidate_pool_returns_null_winner() {
    let (lifecycle, _) = build_lifecycle(vec![kept_filter("drop-all", vec![])]);

    let outcome = lifecycle
        .preview_route(input(PRINCIPAL, None))
        .expect("preview succeeds");

    assert!(outcome.winner_upstream_id.is_none());
    assert!(outcome.winner_upstream_name.is_none());
    assert_eq!(
        outcome
            .trace
            .terminal_decision
            .as_ref()
            .unwrap()
            .upstream_id,
        None
    );
}

fn input(principal_id: &str, request_id: Option<&str>) -> PreviewRouteInput {
    PreviewRouteInput {
        principal_id: principal_id.to_owned(),
        request_id: request_id.map(str::to_owned),
        headers: HeaderMap::new(),
        body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
    }
}

fn kept_filter(name: &'static str, kept_indices: Vec<usize>) -> Arc<dyn FilterPlugin> {
    Arc::new(IndexKeepingFilter {
        name,
        kept_indices,
        calls: Arc::new(Mutex::new(Vec::new())),
    })
}

fn build_lifecycle(filters: Vec<Arc<dyn FilterPlugin>>) -> (Lifecycle, Vec<Uuid>) {
    let upstream_records = default_upstream_records();
    let upstream_ids: Vec<Uuid> = upstream_records.iter().map(|u| u.id).collect();
    let principal_view = principal_view_with_pipeline(filters);
    let lifecycle = build_lifecycle_from(principal_view, upstream_records);
    (lifecycle, upstream_ids)
}

fn build_lifecycle_from(
    principal_view: Arc<PrincipalView>,
    upstream_records: Vec<UpstreamRecord>,
) -> Lifecycle {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(NoopRouter))
        .dispatcher(Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![http::StatusCode::OK].into()))),
        }))
        .global_observability_hooks(vec![hook])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_records(upstream_records)
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
}

fn principal_view_with_pipeline(filters: Vec<Arc<dyn FilterPlugin>>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        PRINCIPAL.to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn principal_view_with_broken_pipeline() -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: Vec::new(),
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: Some(Arc::from("broken pipeline fixture")),
    });
    let mut chains = HashMap::new();
    chains.insert(
        PRINCIPAL.to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn default_upstream_records() -> Vec<UpstreamRecord> {
    (0..3).map(upstream_record).collect()
}

fn upstream_record(index: u8) -> UpstreamRecord {
    let mut bytes = [0u8; 16];
    bytes[15] = index + 1;
    UpstreamRecord {
        id: Uuid::from_bytes(bytes),
        name: format!("upstream-{index}"),
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

struct IndexKeepingFilter {
    name: &'static str,
    kept_indices: Vec<usize>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for IndexKeepingFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        let kept_upstream_ids = self
            .kept_indices
            .iter()
            .filter_map(|index| {
                candidates
                    .get(*index)
                    .map(|candidate| candidate.upstream_id)
            })
            .collect::<Vec<_>>();
        let reason = format!("{} kept {}", self.name, kept_upstream_ids.len());
        Ok(FilterOutput {
            kept_upstream_ids,
            reason,
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "test router should not be called by preview_route".to_owned(),
        })
    }
}
