mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_core::api_keys::builtin_authn::{BuiltinAuthError, BuiltinAuthn};
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::key_store::KeyStore;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::{
    ObservabilityHooksCache, PrincipalView, RouterPluginCache,
};
use cc_lb_core::{Lifecycle, LifecycleConfig};
use cc_lb_plugin_api::{
    ObservabilityError, ObservabilityHook, ObserveEvent, Principal, RequestContext, RouteDecision,
    RouteError, RouterPlugin, Upstream,
};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};
use http::{HeaderMap, StatusCode};
use url::Url;

use common::{DispatchMode, MockDispatch, TestAuthn, TestState, collect_body, messages_request};

#[test]
fn builtin_authn_accepts_bound_principal_view() -> Result<(), Box<dyn std::error::Error>> {
    let (_storage_dir, storage) = storage("authn-bound-view")?;
    let view = principal_view("principal-a", None);
    let authn = BuiltinAuthn::new(
        DownstreamAuthMode::ApiKey,
        None,
        Some(Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(
            storage,
        ))))),
    );

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
    let engine = LimitEngine::new(Arc::new(KeyConcurrencyManager::new()));
    let record = StoredApiKeyRecord {
        key_hash_b64: "key-a".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    };

    let reservation = engine.reserve(&view, &record, "principal-a", "claude", 0, 0, None);

    assert!(reservation.is_ok());
}

#[tokio::test]
async fn lifecycle_per_principal_dispatch_hits_correct_router_and_hook()
-> Result<(), Box<dyn std::error::Error>> {
    let explicit_router_hits = Arc::new(Mutex::new(Vec::new()));
    let global_router_hits = Arc::new(Mutex::new(Vec::new()));
    let explicit_hook_events = Arc::new(Mutex::new(Vec::new()));
    let global_hook_events = Arc::new(Mutex::new(Vec::new()));

    let explicit_router: Arc<dyn RouterPlugin> = Arc::new(RecordingRouter {
        name: "explicit",
        hits: explicit_router_hits.clone(),
    });
    let global_router: Arc<dyn RouterPlugin> = Arc::new(RecordingRouter {
        name: "global",
        hits: global_router_hits.clone(),
    });
    let explicit_hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingNamedHook {
        name: "explicit",
        events: explicit_hook_events.clone(),
    });
    let global_hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingNamedHook {
        name: "global",
        events: global_hook_events.clone(),
    });
    let view = principal_view(
        "principal-a",
        Some((
            RouterPluginCache::Explicit(explicit_router),
            ObservabilityHooksCache::Explicit(vec![explicit_hook]),
        )),
    );
    let state = TestState::default();
    let authn = none_mode_authn("principal-a", view.clone(), state.clone())?;
    let lifecycle = Lifecycle::new(
        authn.authn.clone(),
        view,
        Arc::new(authn),
        global_router,
        Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
        }),
        vec![global_hook],
        LifecycleConfig::default(),
    );

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude","messages":[]}"#,
        )))
        .await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        global_router_hits.lock().unwrap().as_slice(),
        &[] as &[String]
    );
    assert_eq!(
        explicit_router_hits.lock().unwrap().as_slice(),
        &["explicit:principal-a".to_owned()]
    );
    assert!(
        explicit_hook_events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "explicit:AuthnComplete")
    );
    assert!(
        global_hook_events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "global:RequestStarted")
    );
    assert!(
        !global_hook_events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "global:AuthnComplete")
    );
    Ok(())
}

// TODO(Task-35-followup): replace TOML config consumption with DB store read
fn principal_view(
    principal_id: &str,
    chain: Option<(RouterPluginCache, ObservabilityHooksCache)>,
) -> Arc<PrincipalView> {
    let mut chains = HashMap::new();
    if let Some(chain) = chain {
        chains.insert(principal_id.to_owned(), chain);
    }
    Arc::new(PrincipalView::for_tests(
        principal_id,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ))
}

fn none_mode_authn(
    principal_id: &str,
    view: Arc<PrincipalView>,
    state: TestState,
) -> Result<TestAuthn, Box<dyn std::error::Error>> {
    let (_storage_dir, storage) = storage("none-mode-bound-view")?;
    Ok(TestAuthn {
        authn: Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: principal_id.to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
                upstream_credential_ref: "test-upstream".to_owned(),
            }),
            Some(Arc::new(KeyStore::new(Arc::new(RedbManagedKeyStore::new(
                storage,
            ))))),
        )),
        principal_view: view,
        state,
        refresh_allowed: true,
    })
}

fn storage(
    name: &str,
) -> Result<(&'static tempfile::TempDir, Arc<Storage>), Box<dyn std::error::Error>> {
    let dir = Box::leak(Box::new(tempfile::tempdir()?));
    let storage = Arc::new(Storage::open(
        &dir.path().join(format!("{name}.redb")),
        [9; 32],
    )?);
    Ok((dir, storage))
}

struct RecordingRouter {
    name: &'static str,
    hits: Arc<Mutex<Vec<String>>>,
}

impl RouterPlugin for RecordingRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        principal: &Principal,
    ) -> Result<RouteDecision, RouteError> {
        self.hits
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, principal.id));
        Ok(RouteDecision {
            upstream: Upstream::CustomAnthropicSpec {
                base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
            },
            dialect: Arc::new(common::PassthroughDialect),
        })
    }
}

struct RecordingNamedHook {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl ObservabilityHook for RecordingNamedHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, event_name(&event)));
        Ok(())
    }
}

fn event_name(event: &ObserveEvent) -> &'static str {
    match event {
        ObserveEvent::RequestStarted { .. } => "RequestStarted",
        ObserveEvent::AuthnComplete { .. } => "AuthnComplete",
        ObserveEvent::UpstreamChosen { .. } => "UpstreamChosen",
        ObserveEvent::Chunk { .. } => "Chunk",
        ObserveEvent::RequestFinished { .. } => "RequestFinished",
        ObserveEvent::Error { .. } => "Error",
    }
}
