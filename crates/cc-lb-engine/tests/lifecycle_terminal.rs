use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache,
};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
};
use cc_lb_plugin_api::{
    DialectError, FilterError, FilterOutput, FilterPlugin, Principal, RequestContext,
    RetryDecision, RouteDecision, RouteError, RouterPlugin, ShapedRequest, ShapedRequestBuilder,
    SignedRequest, Signer, SignerError, SignerFactory, SigningCapability, TerminalStrategy,
    Upstream, UpstreamCandidate, UpstreamDialect,
};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::types::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{BackendKind, MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use http::StatusCode;
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    messages_request,
};

#[test]
fn terminal_strategy_exhaustive_match_covers_supported_variants() {
    let labels = [TerminalStrategy::FirstPick, TerminalStrategy::Random]
        .map(terminal_strategy_label_for_exhaustive_test);

    assert_eq!(labels, ["first-pick", "random"]);
}

fn terminal_strategy_label_for_exhaustive_test(strategy: TerminalStrategy) -> &'static str {
    match strategy {
        TerminalStrategy::FirstPick => "first-pick",
        TerminalStrategy::Random => "random",
    }
}

#[tokio::test]
async fn first_pick_selects_first_candidate_after_filters() -> Result<(), Box<dyn std::error::Error>>
{
    let first = upstream_id(1);
    let second = upstream_id(2);
    let third = upstream_id(3);
    let choices = Arc::new(Mutex::new(Vec::new()));
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let _dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&_dir, "lifecycle-terminal.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_terminal(
        TerminalStrategy::FirstPick,
        vec![Arc::new(KeepFilter {
            kept_upstream_ids: vec![second, third],
            calls: Arc::clone(&filter_calls),
        })],
        vec![
            upstream_record(first, "first"),
            upstream_record(second, "second"),
            upstream_record(third, "third"),
        ],
        Arc::clone(&choices),
    )
    .with_event_bus(test_bus.bus_arc())
    .with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_engine::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    send_message(&lifecycle).await?;

    assert_eq!(
        filter_calls.lock().expect("filter calls lock").as_slice(),
        &[vec![first, second, third]]
    );
    assert_eq!(
        choices.lock().expect("choices lock").as_slice(),
        &["second".to_owned()]
    );
    let events = wait_for_events(storage.as_ref(), 1).await?;
    let trace = events
        .first()
        .and_then(|event| event.routing_trace.as_ref())
        .expect("routing trace is recorded");
    assert_eq!(trace.stages.len(), 1);
    assert_eq!(trace.stages[0].stage_name, "keep-terminal-candidates");
    assert_eq!(
        trace
            .terminal_decision
            .as_ref()
            .map(|decision| (decision.upstream_id, decision.strategy.clone(),)),
        Some((Some(second), TerminalStrategy::FirstPick))
    );
    Ok(())
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

async fn wait_for_events(
    storage: &dyn RequestEventStore,
    expected: usize,
) -> Result<Vec<cc_lb_storage_api::types::RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let events = RequestEventStore::query_request_events(storage, 0, u64::MAX, 10).await?;
        if events.len() >= expected {
            return Ok(events);
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected {expected} request event(s), got {}", events.len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn random_terminal_uses_seeded_rng_and_distributes_choices()
-> Result<(), Box<dyn std::error::Error>> {
    let first = upstream_id(1);
    let second = upstream_id(2);
    let third = upstream_id(3);
    let records = vec![
        upstream_record(first, "first"),
        upstream_record(second, "second"),
        upstream_record(third, "third"),
    ];

    let first_run = random_sequence([7; 32], records.clone()).await?;
    let second_run = random_sequence([7; 32], records).await?;

    assert_eq!(first_run, second_run);
    assert!(
        first_run.contains(&"first".to_owned()),
        "choices: {first_run:?}"
    );
    assert!(
        first_run.contains(&"second".to_owned()),
        "choices: {first_run:?}"
    );
    assert!(
        first_run.contains(&"third".to_owned()),
        "choices: {first_run:?}"
    );
    Ok(())
}

async fn random_sequence(
    seed: [u8; 32],
    records: Vec<UpstreamRecord>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let choices = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = lifecycle_with_terminal(
        TerminalStrategy::Random,
        Vec::new(),
        records,
        Arc::clone(&choices),
    )
    .with_terminal_rng_seed(seed);

    for _ in 0..24 {
        send_message(&lifecycle).await?;
    }

    Ok(choices.lock().expect("choices lock").clone())
}

async fn send_message(lifecycle: &Lifecycle) -> Result<(), Box<dyn std::error::Error>> {
    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
        )))
        .await?;
    let (status, _headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

fn lifecycle_with_terminal(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    choices: Arc<Mutex<Vec<String>>>,
) -> Lifecycle {
    let principal_view = principal_view(terminal, filters);
    let state = TestState::default();
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory { choices }))
        .global_router(Arc::new(NullRouter))
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_records(records)
        .build();

    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_engine::SystemClock),
    )
}

fn principal_view(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal,
        instantiation_error: None,
    });
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(pipeline),
            ObservabilityHooksCache::Inherit,
            DialectCache::Inherit,
        ),
    );
    Arc::new(PrincipalView::from_db(&[principal()], chains))
}

fn principal() -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: TerminalStrategy::FirstPick,
        cache_keepalive: None,
    }
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
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

struct NullRouter;

impl RouterPlugin for NullRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Ok(RouteDecision {
            upstream_id: None,
            upstream: Upstream::AnthropicDirect { base_url: None },
            dialect: Arc::new(NullDialect),
        })
    }
}

struct NullDialect;

impl UpstreamDialect for NullDialect {
    fn shape(
        &self,
        ctx: &RequestContext,
        upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        let _ = upstream;
        let mut url = Url::parse("http://upstream.local/")?;
        url.set_path(ctx.path.trim_start_matches('/'));
        url.set_query(ctx.query.as_deref());
        Ok(builder.shaped_request(
            url,
            ctx.method.clone(),
            ctx.downstream_headers.clone(),
            ctx.body_bytes.clone(),
        ))
    }
}

struct RecordingSignerFactory {
    choices: Arc<Mutex<Vec<String>>>,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        self.choices
            .lock()
            .expect("choices lock")
            .push(router_chosen_upstream_name);
        Arc::new(RecordingSignerFactoryInner)
    }
}

struct RecordingSignerFactoryInner;

#[async_trait]
impl SignerFactory for RecordingSignerFactoryInner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(RecordingSigner))
    }
}

struct RecordingSigner;

#[async_trait]
impl Signer for RecordingSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_plugin_api::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
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
            reason: "kept terminal candidates".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "keep-terminal-candidates"
    }
}
