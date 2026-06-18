#![allow(deprecated)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use cc_lb_admin::AdminState;
use cc_lb_aead::AeadService;
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_core::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_core::api_keys::key_store::KeyStore;
use cc_lb_core::api_keys::limit_engine::LimitEngine;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, UpstreamDispatch, UpstreamStatusSnapshot,
};
use cc_lb_plugin_api::{
    ObservabilityHook, Principal, RequestContext, RouteDecision, RouteError, RouterPlugin,
    SignedRequest, SignerFactory, Upstream, UpstreamCandidate,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_server::SubscriptionQuotaCache;
use cc_lb_server::bootstrap::{BootstrapError, apply_bootstrap};
use cc_lb_server::dynamic_view_builder::{Stores, build_dynamic_view};
use cc_lb_server::preflight;
use cc_lb_server::refresh::LazyRefresher;
use cc_lb_server::scheduler_factory::{SchedulerBackend, SqliteSchedulerStorage};
use cc_lb_server::warmup::dialect::{WarmupDispatchError, dispatch_warmup_with_dialect};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginChainEntry, PluginChainEntryInput, PluginRegistryStore,
    PluginSlot, PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamStore,
    UpstreamWarmupDialectPlugin, WasmBlob, WasmRegistryEntry, WasmRegistryEntryInput,
};
use cc_lb_storage_sqlite::SqliteStorage as Storage;
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const ADMIN_TOKEN: &str = "test-token";
const NOW: u64 = 1_800_000_000;

type WarmupClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

struct Fixture {
    _db_dir: TempDir,
    data_dir: TempDir,
    storage: Arc<Storage>,
    stores: Arc<Stores>,
}

impl Fixture {
    async fn new(_seed: u8) -> Self {
        let db_dir = tempfile::tempdir().expect("db tempdir");
        let data_dir = tempfile::tempdir().expect("data tempdir");
        let database_url = format!(
            "sqlite://{}",
            db_dir.path().join("supported-slots-guard.sqlite").display()
        );
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url)
                .await
                .expect("storage opens"),
        );
        storage.initialize(BackendKind::Sqlite).await.unwrap();
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage.clone()),
            plugin_registry_repo: None,
        });
        Self {
            _db_dir: db_dir,
            data_dir,
            storage,
            stores,
        }
    }
}

#[tokio::test]
async fn dynamic_view_hooks_skip_registry_entry_not_supporting_observability_slot() {
    let fixture = Fixture::new(91).await;
    let principal = seed_principal(&fixture.storage, "dynamic-principal").await;
    let plugin = seed_registry_with_slots(
        &fixture.storage,
        91,
        "router-only-dynamic",
        vec![PluginSlot::Router],
    )
    .await;
    seed_chain_with_slot(
        &fixture.storage,
        principal.id,
        PluginSlot::ObservabilityHook,
        plugin.id,
        1000,
    )
    .await;
    let runtime = ExtismRuntime::new();

    build_dynamic_view_for_test(&fixture, &runtime).await;

    assert!(
        !wasm_cache_path(fixture.data_dir.path(), plugin.sha256).exists(),
        "unsupported observability-hook entry must be skipped before wasm materialization"
    );
    assert!(
        runtime.registered_slot_keys().is_empty(),
        "unsupported observability-hook entry must not register an Extism slot"
    );
}

#[tokio::test]
async fn warmup_dialect_returns_registry_unsupported_slot_for_non_shape_plugin() {
    let fixture = Fixture::new(92).await;
    let plugin = seed_registry_with_slots(
        &fixture.storage,
        92,
        "router-only-warmup",
        vec![PluginSlot::Router],
    )
    .await;
    let upstream = seed_warmup_upstream(&fixture.storage, plugin.id).await;
    let runtime = ExtismRuntime::new();
    let aead = Arc::new(AeadService::from_master_key([92; 32]));
    let lazy_refresher = Arc::new(LazyRefresher::new(
        fixture.stores.clone(),
        aead.clone(),
        Arc::new(AnthropicOAuthConfig::default()),
        Uuid::new_v4(),
        None,
        CancellationToken::new(),
        sqlite_scheduler_backend().await,
    ));
    let client = warmup_client();

    let error = match dispatch_warmup_with_dialect(
        &runtime,
        &fixture.stores,
        fixture.data_dir.path(),
        aead,
        lazy_refresher,
        &upstream,
        &client,
    )
    .await
    {
        Ok(_) => panic!("unsupported warmup dialect slot should fail before instantiation"),
        Err(error) => error,
    };

    assert!(
        matches!(
            error,
            WarmupDispatchError::RegistryUnsupportedSlot {
                wasm_registry_id,
                slot: PluginSlot::Shape,
                ..
            } if wasm_registry_id == plugin.id
        ),
        "unexpected warmup error: {error:?}"
    );
    assert!(
        !wasm_cache_path(fixture.data_dir.path(), plugin.sha256).exists(),
        "unsupported warmup dialect entry must be rejected before wasm materialization"
    );
}

#[tokio::test]
async fn preflight_warns_when_chain_entry_registry_does_not_support_slot() {
    let fixture = Fixture::new(93).await;
    let principal = seed_principal(&fixture.storage, "preflight-principal").await;
    let plugin = seed_registry_with_slots(
        &fixture.storage,
        93,
        "shape-only-preflight",
        vec![PluginSlot::Shape],
    )
    .await;
    seed_chain_with_slot(
        &fixture.storage,
        principal.id,
        PluginSlot::Router,
        plugin.id,
        1000,
    )
    .await;

    let report = preflight::run_preflight(
        &fixture.stores,
        &cc_lb_core::LifecycleConfig::default(),
        fixture.data_dir.path(),
    )
    .await
    .expect("preflight completes");

    assert_eq!(report.plugin_blob_missing_count, 0);
    assert!(
        report.warnings.iter().any(|warning| {
            warning.contains("unsupported slot")
                && warning.contains("shape-only-preflight")
                && warning.contains("router")
        }),
        "expected unsupported-slot warning, got {:?}",
        report.warnings
    );
}

#[tokio::test]
async fn bootstrap_hard_errors_when_registry_entry_does_not_support_chain_slot() {
    let fixture = Fixture::new(94).await;
    seed_principal(&fixture.storage, "bootstrap-principal").await;
    seed_registry_with_slots(
        &fixture.storage,
        94,
        "shape-only-bootstrap",
        vec![PluginSlot::Shape],
    )
    .await;
    std::fs::write(
        fixture.data_dir.path().join("bootstrap.toml"),
        r#"
[[plugin_chains]]
principal_id = "bootstrap-principal"
slot = "router"
plugins = ["shape-only-bootstrap"]
"#,
    )
    .expect("bootstrap spec written");

    let error = apply_bootstrap(
        &Config::default(),
        fixture.storage.as_ref(),
        fixture.storage.as_ref(),
        fixture.storage.as_ref(),
        None,
        fixture.data_dir.path(),
    )
    .await
    .expect_err("unsupported bootstrap plugin slot should be a hard error");

    assert!(
        matches!(error, BootstrapError::InvalidSpec(ref message) if message.contains("unsupported slot") && message.contains("shape-only-bootstrap")),
        "unexpected bootstrap error: {error:?}"
    );
    let principal = PrincipalStore::get_by_name(fixture.storage.as_ref(), "bootstrap-principal")
        .await
        .unwrap()
        .unwrap();
    let chain = fixture
        .storage
        .list_chain_for_principal(principal.id, PluginSlot::Router)
        .await
        .unwrap();
    assert!(chain.is_empty());
}

#[tokio::test]
async fn admin_update_chain_rejects_registry_entry_not_supporting_existing_slot() {
    let fixture = Fixture::new(95).await;
    let principal = seed_principal(&fixture.storage, "admin-unsupported-principal").await;
    let plugin = seed_registry_with_slots(
        &fixture.storage,
        95,
        "shape-only-admin",
        vec![PluginSlot::Shape],
    )
    .await;
    let chain = seed_chain_with_slot(
        &fixture.storage,
        principal.id,
        PluginSlot::Router,
        plugin.id,
        1000,
    )
    .await;
    let app = cc_lb_admin::router(admin_state(fixture.storage.clone()));

    let (status, _, body) = admin_request_json(
        app,
        "PUT",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        Some(json!({ "sse_per_event": true })),
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unsupported_slot");
    assert_eq!(body["plugin_name"], "shape-only-admin");
    assert_eq!(body["slot"], "router");
    assert_chain_unchanged(&fixture.storage, principal.id, chain.id).await;
}

#[tokio::test]
async fn admin_update_chain_rejects_empty_supported_slots_as_metadata_unknown() {
    let fixture = Fixture::new(96).await;
    let principal = seed_principal(&fixture.storage, "admin-empty-principal").await;
    let plugin = seed_registry_raw(&fixture.storage, 96, "legacy-admin-empty-slots").await;
    let chain = seed_chain_with_slot(
        &fixture.storage,
        principal.id,
        PluginSlot::Router,
        plugin.id,
        1000,
    )
    .await;
    let app = cc_lb_admin::router(admin_state(fixture.storage.clone()));

    let (status, _, body) = admin_request_json(
        app,
        "PUT",
        &format!("/admin/v1/plugin-chain-entries/{}", chain.id),
        Some(json!({ "sse_per_event": true })),
        Some("W/\"0\""),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "slot_metadata_unknown");
    assert_eq!(body["plugin_name"], "legacy-admin-empty-slots");
    assert_chain_unchanged(&fixture.storage, principal.id, chain.id).await;
}

async fn build_dynamic_view_for_test(fixture: &Fixture, runtime: &ExtismRuntime) {
    build_dynamic_view(
        &fixture.stores,
        &AnthropicOAuthConfig::default(),
        Arc::new(AeadService::from_master_key([91; 32])),
        None,
        0,
        runtime,
        fixture.data_dir.path(),
        Arc::new(SubscriptionQuotaCache::new()),
        1800,
        &Config::default(),
    )
    .await
    .expect("dynamic view builds");
}

async fn seed_principal(storage: &Storage, name: &str) -> cc_lb_storage_api::PrincipalRecord {
    PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
        },
        NOW,
    )
    .await
    .expect("principal created")
}

async fn seed_warmup_upstream(
    storage: &Storage,
    wasm_registry_id: Uuid,
) -> cc_lb_storage_api::UpstreamRecord {
    let mut upstream = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: "warmup-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some(Url::parse("http://127.0.0.1:9/").expect("base url")),
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: true,
            next_warmup_at: None,
            last_warmup_cycle_key: None,
            warmup_lease_holder: None,
            warmup_lease_until_unix_secs: None,
            warmup_dialect_plugin: None,
        },
    )
    .await
    .expect("upstream created");
    upstream.warmup_dialect_plugin = Some(UpstreamWarmupDialectPlugin {
        wasm_registry_id,
        config: json!({}),
        wire_version: None,
    });
    upstream
}

async fn seed_registry_with_slots(
    storage: &Storage,
    seed: u8,
    name: &str,
    supported_slots: Vec<PluginSlot>,
) -> WasmRegistryEntry {
    seed_registry(storage, seed, name, supported_slots).await
}

async fn seed_registry_raw(storage: &Storage, seed: u8, name: &str) -> WasmRegistryEntry {
    seed_registry(storage, seed, name, Vec::new()).await
}

async fn seed_registry(
    storage: &Storage,
    seed: u8,
    name: &str,
    supported_slots: Vec<PluginSlot>,
) -> WasmRegistryEntry {
    let bytes = vec![seed; seed as usize];
    let (entry, _) = storage
        .persist_wasm_upload(
            WasmBlob {
                sha256: [seed; 32],
                size_bytes: bytes.len() as u64,
                bytes,
                parse_validated_at_unix_secs: NOW,
            },
            WasmRegistryEntryInput {
                name: name.to_owned(),
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: NOW,
                uploaded_by_admin_id: Uuid::new_v4(),
                wire_version: 1,
                supported_slots,
            },
        )
        .await
        .expect("registry entry persisted");
    entry
}

async fn seed_chain_with_slot(
    storage: &Storage,
    principal_id: Uuid,
    slot: PluginSlot,
    wasm_registry_id: Uuid,
    order: i64,
) -> PluginChainEntry {
    storage
        .insert_chain_entry(PluginChainEntryInput {
            principal_id,
            slot,
            order,
            wasm_registry_id,
            config: json!({}),
            sse_per_event: false,
            batched_events_per_flush: 1,
            batched_flush_ms: 100,
            wire_version: None,
        })
        .await
        .expect("chain entry inserted")
}

async fn assert_chain_unchanged(storage: &Storage, principal_id: Uuid, chain_id: Uuid) {
    let chain = storage
        .list_chain_for_principal(principal_id, PluginSlot::Router)
        .await
        .expect("chain listed")
        .into_iter()
        .find(|entry| entry.id == chain_id)
        .expect("chain entry still exists");
    assert_eq!(chain.revision, 0);
    assert!(!chain.sse_per_event);
}

fn warmup_client() -> WarmupClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

async fn sqlite_scheduler_backend() -> SchedulerBackend {
    let pool = scheduler_sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("scheduler sqlite opens");
    apalis_sqlite::SqliteStorage::setup(&pool)
        .await
        .expect("scheduler sqlite initializes");
    SchedulerBackend::Sqlite(SqliteSchedulerStorage {
        pool: pool.clone(),
        storage: apalis_sqlite::SqliteStorage::new_in_queue(
            &pool,
            cc_lb_scheduler::worker::ENTITY_QUEUE,
        ),
    })
}

fn wasm_cache_path(data_dir: &Path, sha256: [u8; 32]) -> PathBuf {
    data_dir
        .join("plugins")
        .join("wasm")
        .join("cache")
        .join(format!("{}.wasm", hex_sha256(sha256)))
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn admin_request_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"));
    if let Some(if_match) = if_match {
        builder = builder.header("If-Match", if_match);
    }
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&value).expect("request body serializes"))
        }
        None => Body::empty(),
    };
    let response = app
        .oneshot(builder.body(body).expect("request builds"))
        .await
        .expect("admin request succeeds");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body collects")
        .to_bytes();
    let body = serde_json::from_slice(&bytes).expect("response body is json");
    (status, headers, body)
}

fn admin_state(storage: Arc<Storage>) -> AdminState {
    let principal_view = Arc::new(PrincipalView::from_db(&[], HashMap::new()));
    let dynamic_view = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .dispatcher(Arc::new(NoopDispatch))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build(),
    ));
    AdminState {
        storage: Some(storage.clone() as Arc<dyn cc_lb_storage_api::Storage>),
        key_store: Some(Arc::new(KeyStore::new(storage))),
        aead: Arc::new(AeadService::from_master_key([0; 32])),
        limit_engine: LimitEngine::new(Arc::new(KeyConcurrencyManager::new())),
        lifecycle: None,
        subscription_metadata_hook: None,
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        audit_sink: None,
        dynamic_view,
        config: Arc::new(Config::default()),
        admin_token: Some(ADMIN_TOKEN.to_owned()),
        start_time: std::time::Instant::now(),
    }
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, cc_lb_plugin_api::SignerError> {
        Err(cc_lb_plugin_api::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
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
            reason: "noop test router".to_owned(),
        })
    }
}

struct NoopDispatch;

#[async_trait]
impl UpstreamDispatch for NoopDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        Ok(http::Response::new(Body::empty()))
    }
}
