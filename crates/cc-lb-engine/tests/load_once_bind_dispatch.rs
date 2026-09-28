use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_domain::{Principal, TerminalStrategy, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::builtin_authn::BuiltinAuthError;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::{DialectCache, PrincipalView, RouterPipelineCache};
use cc_lb_engine::{DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig};
use cc_lb_routing::{
    FilterError, FilterOutput, FilterPlugin, RouteDecision, RouteError, RouterPlugin,
};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::{HeaderMap, StatusCode};
use url::Url;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestState, collect_body, managed_authn, messages_request,
};

#[test]
fn builtin_authn_accepts_bound_principal_view() -> Result<(), Box<dyn std::error::Error>> {
    let view = principal_view("principal-a", None);
    let authn = managed_authn("principal-a");

    let error = tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(authn.authenticate(&HeaderMap::new(), &view))
        .unwrap_err();

    assert_eq!(error, BuiltinAuthError::MissingHeader);
    Ok(())
}

#[test]
fn limit_engine_reserve_accepts_bound_principal_view() {
    let view = principal_view("principal-a", None);
    let engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_engine::SystemClock),
    );
    let record = StoredApiKeyRecord {
        key_hash_b64: "key-a".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    };

    let reservation = engine.reserve(&view, &record, "principal-a", "claude", 0, 0, None);

    assert!(reservation.is_ok());
}

#[tokio::test]
async fn lifecycle_explicit_pipeline_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let global_router_hits = Arc::new(Mutex::new(Vec::new()));

    let global_router: Arc<dyn RouterPlugin> = Arc::new(RecordingRouter {
        name: "global",
        hits: global_router_hits.clone(),
    });
    let explicit_pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![Arc::new(RecordingFilter { name: "explicit" })],
        terminal: TerminalStrategy::Random,
        instantiation_error: None,
    });
    let view = principal_view("principal-a", Some(explicit_pipeline));
    let state = TestState::default();
    let authn = managed_test_authn("principal-a", view.clone(), state.clone());
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let dynamic_view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(global_router)
        .principal_view(view)
        .upstream_records(vec![test_upstream_record()])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(dynamic_view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    );

    let request = messages_request(Bytes::from_static(br#"{"model":"claude","messages":[]}"#));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        global_router_hits.lock().unwrap().as_slice(),
        &[] as &[String]
    );
    Ok(())
}

// TODO(Task-35-followup): replace TOML config consumption with DB store read
fn principal_view(
    principal_id: &str,
    chain: Option<Arc<RouterPipelineCache>>,
) -> Arc<PrincipalView> {
    let mut chains = HashMap::new();
    if let Some(router) = chain {
        chains.insert(
            principal_id.to_owned(),
            (Some(router), DialectCache::Inherit),
        );
    }
    Arc::new(PrincipalView::for_tests(
        principal_id,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn managed_test_authn(principal_id: &str, view: Arc<PrincipalView>, state: TestState) -> TestAuthn {
    TestAuthn {
        authn: managed_authn(principal_id),
        principal_view: view,
        state,
        refresh_allowed: true,
    }
}

fn test_upstream_record() -> UpstreamRecord {
    UpstreamRecord {
        id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
            .expect("test upstream id parses"),
        name: "test-upstream".to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
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

struct RecordingRouter {
    name: &'static str,
    hits: Arc<Mutex<Vec<String>>>,
}

impl RouterPlugin for RecordingRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        self.hits
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, principal.id));
        Ok(RouteDecision {
            upstream_id: None,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(common::PassthroughDialect {
                base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
            }),
        })
    }
}

struct RecordingFilter {
    name: &'static str,
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(FilterOutput {
            kept_upstream_ids: vec![],
            reason: format!("{} rejected all candidates", self.name),
            per_candidate_reasons: vec![],
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> uuid::Uuid {
        uuid::Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}
