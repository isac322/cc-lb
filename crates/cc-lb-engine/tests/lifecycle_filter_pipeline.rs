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
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::StatusCode;
use parking_lot::Mutex as ParkingMutex;
use tokio::time::{Duration, timeout};
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState, collect_body, messages_request,
};

#[tokio::test]
async fn pipeline_filters_candidates_before_terminal_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![Arc::new(RecordingFilter {
        name: "keep-default",
        calls: filter_calls.clone(),
        kept_upstream_ids: vec![upstream_id],
    })];
    let lifecycle = lifecycle_with_pipeline(filters, router_calls.clone(), state.clone(), hook);

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        filter_calls.lock().unwrap().as_slice(),
        &[vec![upstream_id]]
    );
    assert!(router_calls.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn request_model_reaches_filters_without_prompt_cache_shadow()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let canonical_models = Arc::new(ParkingMutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![Arc::new(RecordingModelFilter {
        canonical_models: canonical_models.clone(),
        kept_upstream_ids: vec![upstream_id],
    })];
    let lifecycle = lifecycle_with_pipeline(filters, router_calls, state, hook);

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-fable-5","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(canonical_models.lock().as_slice(), &["claude-fable-5"]);
    Ok(())
}

#[tokio::test]
async fn trap_and_runtime_errors_pass_candidates_through() -> Result<(), Box<dyn std::error::Error>>
{
    for error_kind in [FilterErrorKind::Trap, FilterErrorKind::Runtime] {
        let upstream_id = default_upstream_id();
        let error_filter_calls = Arc::new(Mutex::new(Vec::new()));
        let keep_filter_calls = Arc::new(Mutex::new(Vec::new()));
        let router_calls = Arc::new(Mutex::new(Vec::new()));
        let state = TestState::default();
        let hook = Arc::new(RecordingHook::default());
        let filters: Vec<Arc<dyn FilterPlugin>> = vec![
            Arc::new(ErrorFilter {
                name: error_kind.stage_name(),
                calls: error_filter_calls.clone(),
                error_kind,
            }),
            Arc::new(RecordingFilter {
                name: "after-error",
                calls: keep_filter_calls.clone(),
                kept_upstream_ids: vec![upstream_id],
            }),
        ];
        let lifecycle =
            lifecycle_with_pipeline(filters, router_calls.clone(), state.clone(), hook.clone());

        let request = messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        ));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("test request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        let (status, _headers, _body) = collect_body(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            error_filter_calls.lock().unwrap().as_slice(),
            &[vec![upstream_id]]
        );
        assert_eq!(
            keep_filter_calls.lock().unwrap().as_slice(),
            &[vec![upstream_id]]
        );
        assert!(router_calls.lock().unwrap().is_empty());
        timeout(
            Duration::from_secs(1),
            hook.wait_for_event(|event| {
                matches!(
                    event,
                    cc_lb_observability::ObserveEvent::Error { code, source, .. }
                        if code == "router_filter_passthrough" && source == "router"
                )
            }),
        )
        .await
        .expect("router filter error observation arrives");
    }
    Ok(())
}

#[tokio::test]
async fn empty_stage_output_propagates_to_later_stages_and_terminal_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let upstream_id = default_upstream_id();
    let empty_filter_calls = Arc::new(Mutex::new(Vec::new()));
    let later_filter_calls = Arc::new(Mutex::new(Vec::new()));
    let router_calls = Arc::new(Mutex::new(Vec::new()));
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let filters: Vec<Arc<dyn FilterPlugin>> = vec![
        Arc::new(RecordingFilter {
            name: "empty",
            calls: empty_filter_calls.clone(),
            kept_upstream_ids: Vec::new(),
        }),
        Arc::new(RecordingFilter {
            name: "later",
            calls: later_filter_calls.clone(),
            kept_upstream_ids: Vec::new(),
        }),
    ];
    let lifecycle = lifecycle_with_pipeline(filters, router_calls.clone(), state.clone(), hook);

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        empty_filter_calls.lock().unwrap().as_slice(),
        &[vec![upstream_id]]
    );
    assert_eq!(
        later_filter_calls.lock().unwrap().as_slice(),
        &[Vec::<Uuid>::new()]
    );
    assert!(router_calls.lock().unwrap().is_empty());
    Ok(())
}

fn lifecycle_with_pipeline(
    filters: Vec<Arc<dyn FilterPlugin>>,
    router_calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    state: TestState,
    hook: Arc<RecordingHook>,
) -> Lifecycle {
    let principal_view = principal_view(filters);
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(RecordingTerminalRouter {
            calls: router_calls,
        }))
        .global_observability_hooks(vec![hook])
        .principal_view(principal_view)
        .upstream_records(vec![test_upstream_record()])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
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

fn test_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: default_upstream_id(),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
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

fn default_upstream_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("default upstream id parses")
}

struct RecordingTerminalRouter {
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl RouterPlugin for RecordingTerminalRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        let candidate = candidates.first().ok_or_else(|| RouteError::NoRoute {
            reason: "no candidates after filters".to_owned(),
        })?;
        Ok(RouteDecision {
            upstream_id: Some(candidate.upstream_id),
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(common::PassthroughDialect {
                base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
            }),
        })
    }
}

struct RecordingFilter {
    name: &'static str,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: format!("{} kept {}", self.name, self.kept_upstream_ids.len()),
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
struct RecordingModelFilter {
    canonical_models: Arc<ParkingMutex<Vec<String>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for RecordingModelFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.canonical_models
            .lock()
            .push(ctx.canonical_model_id.clone());
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "record:model".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "record-model"
    }
}

struct ErrorFilter {
    name: &'static str,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
    error_kind: FilterErrorKind,
}

impl FilterPlugin for ErrorFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().unwrap().push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        match self.error_kind {
            FilterErrorKind::Trap => Err(FilterError::Trap {
                reason: "trap fixture".to_owned(),
            }),
            FilterErrorKind::Runtime => Err(FilterError::Runtime {
                reason: "runtime fixture".to_owned(),
            }),
        }
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

#[derive(Clone, Copy)]
enum FilterErrorKind {
    Trap,
    Runtime,
}

impl FilterErrorKind {
    fn stage_name(self) -> &'static str {
        match self {
            Self::Trap => "trap-filter",
            Self::Runtime => "runtime-filter",
        }
    }
}
