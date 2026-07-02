mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
use cc_lb_core::api_keys::builtin_authn::{BuiltinAuthError, BuiltinAuthn};
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_core::{
    DynamicViewBuilder, DynamicViewHolder, ErrorNormalizer, Lifecycle, LifecycleConfig,
};
use cc_lb_plugin_api::{
    FilterError, FilterOutput, FilterPlugin, ObservabilityError, ObservabilityHook, ObserveEvent,
    Principal, RequestContext, RouteDecision, RouteError, RouterPlugin, TerminalStrategy, Upstream,
    UpstreamCandidate,
};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use http::{HeaderMap, StatusCode};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use url::Url;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    messages_request,
};

#[test]
fn builtin_authn_accepts_bound_principal_view() -> Result<(), Box<dyn std::error::Error>> {
    let view = principal_view("principal-a", None);
    let authn = BuiltinAuthn::new(
        DownstreamAuthMode::ApiKey,
        None,
        None,
        Arc::new(cc_lb_core::SystemClock),
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
    let engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(cc_lb_core::SystemClock),
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
async fn lifecycle_explicit_pipeline_fails_closed_and_uses_explicit_hook()
-> Result<(), Box<dyn std::error::Error>> {
    let global_router_hits = Arc::new(Mutex::new(Vec::new()));
    let explicit_hook_events = Arc::new(Mutex::new(Vec::new()));
    let explicit_hook_notify = Arc::new(Notify::new());
    let global_hook_events = Arc::new(Mutex::new(Vec::new()));
    let global_hook_notify = Arc::new(Notify::new());

    let global_router: Arc<dyn RouterPlugin> = Arc::new(RecordingRouter {
        name: "global",
        hits: global_router_hits.clone(),
    });
    let explicit_pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![Arc::new(RecordingFilter { name: "explicit" })],
        terminal: TerminalStrategy::Random,
        instantiation_error: None,
    });
    let explicit_hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingNamedHook {
        name: "explicit",
        events: explicit_hook_events.clone(),
        notify: explicit_hook_notify.clone(),
    });
    let test_bus = TestLifecycleBus::new().with_hook_adapter(vec![Arc::clone(&explicit_hook)]);
    let global_hook: Arc<dyn ObservabilityHook> = Arc::new(RecordingNamedHook {
        name: "global",
        events: global_hook_events.clone(),
        notify: global_hook_notify,
    });
    let view = principal_view(
        "principal-a",
        Some((
            explicit_pipeline,
            ObservabilityHooksCache::Explicit(vec![explicit_hook]),
        )),
    );
    let state = TestState::default();
    let authn = none_mode_authn("principal-a", view.clone(), state.clone())?;
    let dynamic_view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(global_router)
        .dispatcher(Arc::new(MockDispatch {
            state,
            mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
        }))
        .global_observability_hooks(vec![global_hook])
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(view)
        .upstream_records(vec![test_upstream_record()])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(dynamic_view)),
        LifecycleConfig::default(),
        Arc::new(cc_lb_core::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude","messages":[]}"#,
        )))
        .await?;
    let (status, _headers, _body) = collect_body(response).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        global_router_hits.lock().unwrap().as_slice(),
        &[] as &[String]
    );
    timeout(
        Duration::from_secs(1),
        wait_named_event(
            &explicit_hook_events,
            &explicit_hook_notify,
            "explicit:Error",
        ),
    )
    .await
    .expect("explicit hook error observation arrives");
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
    chain: Option<(Arc<RouterPipelineCache>, ObservabilityHooksCache)>,
) -> Arc<PrincipalView> {
    let mut chains = HashMap::new();
    if let Some((router, obs)) = chain {
        chains.insert(
            principal_id.to_owned(),
            (Some(router), obs, DialectCache::Inherit),
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

fn none_mode_authn(
    principal_id: &str,
    view: Arc<PrincipalView>,
    state: TestState,
) -> Result<TestAuthn, Box<dyn std::error::Error>> {
    Ok(TestAuthn {
        authn: Arc::new(BuiltinAuthn::new(
            DownstreamAuthMode::None,
            Some(NoneModeConfig {
                principal_id: principal_id.to_owned(),
                upstream_kind: NoneModeUpstreamKind::AnthropicKey,
            }),
            None,
            Arc::new(cc_lb_core::SystemClock),
        )),
        principal_view: view,
        state,
        refresh_allowed: true,
    })
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
        _ctx: &RequestContext,
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

struct RecordingNamedHook {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
}

async fn wait_named_event(events: &Arc<Mutex<Vec<String>>>, notify: &Arc<Notify>, expected: &str) {
    loop {
        if events.lock().unwrap().iter().any(|event| event == expected) {
            return;
        }
        notify.notified().await;
    }
}

struct RecordingFilter {
    name: &'static str,
}

impl FilterPlugin for RecordingFilter {
    fn filter(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        Ok(FilterOutput {
            kept_upstream_ids: vec![],
            reason: format!("{} rejected all candidates", self.name),
            per_candidate_reasons: vec![],
        })
    }

    fn plugin_id(&self) -> uuid::Uuid {
        uuid::Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        self.name
    }
}

impl ObservabilityHook for RecordingNamedHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:{}", self.name, event_name(&event)));
        self.notify.notify_waiters();
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
