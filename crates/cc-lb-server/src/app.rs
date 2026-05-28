use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::error_handling::HandleErrorLayer;
use axum::extract::State;
use axum::http::header::HeaderValue;
use axum::http::{HeaderName, Request, Response, StatusCode};
use axum::middleware::{self, Next};
use axum::routing::{any, get, post};
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DownstreamAuthMode, PluginRef, TlsConfig};
use cc_lb_core::BreakerState;
use cc_lb_core::{
    BreakerConfig, BreakerRegistry, BulkheadConfig, BulkheadDispatch, BulkheadRegistry,
    CircuitBreakerDispatch, ErrorNormalizer, HopByHopStripLayer, Lifecycle, LifecycleConfig,
    UpstreamDispatch, UpstreamKind,
    api_keys::{
        builtin_authn::BuiltinAuthn, concurrent_guard::KeyConcurrencyManager, key_store::KeyStore,
        limit_engine::LimitEngine, principal_view::PrincipalView,
    },
    make_default_dispatcher, spawn_audit_writer,
    usage_pruner::UsagePruner,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_plugin_api::{ObservabilityHook, PluginManifest, PluginRuntime};
use cc_lb_pricing::LiteLlmLoader;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_api::ManagedKeyStore;
use cc_lb_storage_redb::Storage;
use http_body_util::BodyExt;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tower::ServiceBuilder;

use crate::build_meta::BuildMeta;
use crate::builtins::{self, BuiltinRouter, CompositeSignerFactory, NoopObservabilityHook};
use crate::drain::DrainController;
use crate::preflight::{self, PreflightOptions};
use crate::reload::ConfigWatcher;
use crate::signal;
use crate::tls::{ReloadableListener, TlsState};
use cc_lb_admin::{AdminState, ConfigDraftError, CurrentConfig};

pub const PROXY_FILES_ROUTE_COLLECTION: &str = "/v1/files";
pub const PROXY_FILES_ROUTE_ITEM: &str = "/v1/files/{id}";
pub const PROXY_FILES_ROUTE_ITEM_CONTENT: &str = "/v1/files/{id}/content";
pub const PROXY_FILES_ROUTE_PATHS: &[&str] = &[
    PROXY_FILES_ROUTE_COLLECTION,
    PROXY_FILES_ROUTE_ITEM,
    PROXY_FILES_ROUTE_ITEM_CONTENT,
];

pub struct App {
    pub router: Router,
    pub admin_router: Router,
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub reload_task: Option<JoinHandle<()>>,
    audit_writer_task: Option<JoinHandle<()>>,
    signals: signal::SignalHandle,
    drain_controller: DrainController,
    tls_state: Option<Arc<TlsState>>,
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Preflight(#[from] crate::preflight::PreflightError),
    #[error(transparent)]
    Observability(#[from] cc_lb_observability::InitError),
    #[error(transparent)]
    Build(#[from] BuildError),
}

impl App {
    pub fn set_draining(&self, draining: bool) {
        self.drain_controller.set_draining(draining);
    }

    pub fn drain_controller(&self) -> DrainController {
        self.drain_controller.clone()
    }

    pub fn signal_handle(&self) -> signal::SignalHandle {
        self.signals.clone()
    }

    pub async fn start(self) -> Result<(), BuildError> {
        let App {
            router,
            admin_router,
            proxy_addr,
            admin_addr,
            reload_task,
            audit_writer_task,
            signals,
            drain_controller: _,
            tls_state,
        } = self;
        let proxy_listener = TcpListener::bind(proxy_addr).await?;
        let admin_listener = TcpListener::bind(admin_addr).await?;

        let (admin_stop_tx, admin_shutdown) = watch::channel(false);
        let admin = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(signal::wait_for_shutdown(admin_shutdown))
                .await
        });

        let proxy_shutdown = signals.subscribe();
        let mut proxy = if let Some(tls_state) = tls_state {
            tokio::spawn(async move {
                let proxy_listener = ReloadableListener::new(proxy_listener, tls_state);
                axum::serve(proxy_listener, router)
                    .with_graceful_shutdown(signal::wait_for_shutdown(proxy_shutdown))
                    .await
            })
        } else {
            tokio::spawn(async move {
                axum::serve(proxy_listener, router)
                    .with_graceful_shutdown(signal::wait_for_shutdown(proxy_shutdown))
                    .await
            })
        };

        let drain_complete = signals.subscribe_drain_complete();
        let proxy_result = tokio::select! {
            result = &mut proxy => server_join_result(result),
            _ = signal::wait_for_shutdown(drain_complete) => {
                if !proxy.is_finished() {
                    proxy.abort();
                }
                server_join_result(proxy.await)
            }
        };

        if let Some(task) = reload_task {
            task.abort();
        }
        let _ = admin_stop_tx.send(true);
        let _ = admin.await;
        if let Some(task) = audit_writer_task {
            let _ = task.await;
        }
        proxy_result?;
        Ok(())
    }
}

pub async fn run_serve(config_path: &Path) -> Result<(), ServeError> {
    let mut config = Config::load(config_path)?;
    let report = preflight::run(&config, PreflightOptions { skip_bind: false }).await?;
    print_preflight_report(&report);
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let _guard = init_observability(&mut config)?;
    let app = build_app_with_path(config, Some(config_path)).await?;
    app.start().await?;
    Ok(())
}

fn print_preflight_report(report: &preflight::PreflightReport) {
    println!("preflight: ok");
    for warning in &report.warnings {
        println!("preflight: warning: {warning}");
    }
}

pub async fn build_app(config: Config) -> Result<App, BuildError> {
    build_app_with_path(config, None).await
}

pub async fn build_app_for_testing(mut config: Config) -> Result<App, BuildError> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("storage.redb");
    let key = [0u8; 32];
    let aead = Arc::new(AeadService::from_master_key(key));
    let storage_arc = Arc::new(cc_lb_storage_redb::Storage::open(&path, key)?);
    let managed_store: Arc<dyn ManagedKeyStore> = Arc::new(
        cc_lb_storage_redb::RedbManagedKeyStore::new(storage_arc.clone()),
    );
    let storage = Some(storage_arc);
    config.storage = cc_lb_config::StorageConfig::Redb { path };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(cc_lb_config::NoneModeConfig {
        principal_id: "test-principal".to_owned(),
        upstream_kind: cc_lb_config::NoneModeUpstreamKind::AnthropicKey,
        upstream_credential_ref: "test-cred".to_owned(),
    });
    std::mem::forget(dir);
    build_app_with_storage(config, None, managed_store, storage, aead)
}

#[cfg(feature = "postgres")]
pub async fn build_app_for_testing_postgres(database_url: &str) -> Result<App, BuildError> {
    use cc_lb_storage_api::{BackendKind, Storage as StorageTrait};
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    let init_storage: Arc<dyn StorageTrait> =
        Arc::new(cc_lb_storage_postgres::PostgresStorage::new(pool.clone()));
    init_storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    sqlx::query("TRUNCATE managed_api_key_index_v1, managed_api_keys_v1")
        .execute(&pool)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    let managed_store: Arc<dyn ManagedKeyStore> =
        Arc::new(cc_lb_storage_postgres::PostgresManagedKeyStore::new(
            pool,
            Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
        ));
    let key = [0u8; 32];
    let aead = Arc::new(AeadService::from_master_key(key));
    let mut config = Config::default();
    config.storage = cc_lb_config::StorageConfig::Postgres {
        url: database_url.to_owned(),
        pool: cc_lb_config::PostgresPoolConfig::default(),
    };
    config.upstreams.insert(
        "test-upstream".to_owned(),
        cc_lb_config::UpstreamSpec {
            kind: cc_lb_config::UpstreamKind::AnthropicDirect,
            base_url: None,
            region: None,
            project: None,
            auth_strategy: cc_lb_config::AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(cc_lb_config::NoneModeConfig {
        principal_id: "test-principal".to_owned(),
        upstream_kind: cc_lb_config::NoneModeUpstreamKind::AnthropicKey,
        upstream_credential_ref: "test-cred".to_owned(),
    });
    build_app_with_storage(config, None, managed_store, None, aead)
}

pub async fn build_app_with_path(
    config: Config,
    config_path: Option<&Path>,
) -> Result<App, BuildError> {
    config.validate()?;
    let (managed_store, storage, aead) = open_storage(&config).await?;
    build_app_with_storage(config, config_path, managed_store, storage, aead)
}

pub fn build_app_with_storage(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Option<Arc<cc_lb_storage_redb::Storage>>,
    aead: Arc<AeadService>,
) -> Result<App, BuildError> {
    let principal_view = Arc::new(ArcSwap::from(PrincipalView::from_config(&config)));
    let key_store = Arc::new(KeyStore::new(managed_store));
    let concurrent_mgr = Arc::new(KeyConcurrencyManager::new());
    let price_catalog = cc_lb_pricing::global_catalog().clone();
    spawn_price_catalog_loader(&config, storage.clone(), price_catalog.clone());
    if let Some(storage) = storage.clone() {
        let pruner = UsagePruner::new(storage, config.api_keys.usage_retention_days);
        let _usage_pruner_task = tokio::spawn(pruner.start_daemon());
    }
    let (audit_sink, audit_writer_task) = match storage.clone() {
        Some(storage) => {
            let (sink, task) = spawn_audit_writer(storage, 1024);
            (Some(Arc::new(sink)), Some(task))
        }
        None => (None, None),
    };
    let limit_engine = LimitEngine::new(concurrent_mgr, principal_view.clone());
    if let Some(storage) = storage.clone() {
        limit_engine.startup_replay(storage);
    }
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        config.downstream_auth.mode.clone(),
        config.downstream_auth.none_mode.clone(),
        key_store.clone(),
        principal_view.clone(),
    ));

    let runtime = ExtismRuntime::new();
    let signer_factory_for_lifecycle = Arc::new(CompositeSignerFactory::new(
        &config,
        storage.clone(),
        aead.clone(),
    ));
    let router_plugin = match &config.plugins.router_plugin {
        Some(plugin) => runtime.instantiate_router(&manifest_from_plugin(plugin)?)?,
        None => Arc::new(BuiltinRouter::new(&config)?),
    };
    let mut observability_hooks: Vec<Arc<dyn ObservabilityHook>> = Vec::new();
    for plugin in &config.plugins.observability_hooks {
        observability_hooks
            .push(runtime.instantiate_observability(&manifest_from_plugin(plugin)?)?);
    }

    let error_normalizer = Arc::new(error_normalizer(&config)?);
    let (dispatcher, breaker_registry) = dispatcher(&config);
    let mut lifecycle = Lifecycle::new(
        builtin_authn.clone(),
        signer_factory_for_lifecycle,
        router_plugin,
        dispatcher,
        observability_hooks,
        LifecycleConfig {
            messages_body_cap_bytes: cap_to_usize(config.body.messages_cap_bytes),
            files_body_cap_bytes: cap_to_usize(config.body.files_cap_bytes),
        },
    )
    .with_error_normalizer(error_normalizer);
    if let Some(audit_sink) = audit_sink.clone() {
        lifecycle = lifecycle.with_audit_sink(audit_sink);
    }
    lifecycle = lifecycle.with_limit_engine(limit_engine.clone(), builtin_authn.clone());
    if let Some(storage) = storage.clone() {
        lifecycle = lifecycle.with_request_event_storage(storage);
    }
    let lifecycle = Arc::new(lifecycle);

    let start_time = std::time::Instant::now();
    let drain_controller = DrainController::new();
    let tls_state = build_tls_state(&config)?;
    let reload_tls_state = match active_tls_config(&config) {
        Some((_, tls)) if tls.reload_on_sighup => tls_state.clone(),
        _ => None,
    };
    let config_watcher = config_path.map(|path| {
        Arc::new(ConfigWatcher::new_with_principal_view(
            path,
            config.clone(),
            Some(principal_view.clone()),
        ))
    });
    let signals = signal::install(
        drain_controller.clone(),
        Duration::from_secs(config.timeouts.drain_secs),
        sighup_handler(reload_tls_state, config_watcher.clone()),
    );
    let state = ProxyState {
        lifecycle: lifecycle.clone(),
        breaker_registry,
        start_time,
        drain_controller: drain_controller.clone(),
        key_store: Some(key_store.clone()),
        builtin_authn: Some(builtin_authn.clone()),
    };
    let admin_config: Arc<dyn CurrentConfig> = match &config_watcher {
        Some(watcher) => watcher.clone(),
        None => Arc::new(InMemoryCurrentConfig::new(
            config.clone(),
            principal_view.clone(),
        )),
    };
    let admin_state = AdminState {
        storage: storage.clone(),
        key_store: Some(key_store),
        aead: aead.clone(),
        limit_engine: limit_engine.clone(),
        lifecycle: Some(lifecycle.clone()),
        audit_sink: audit_sink.clone(),
        principal_view: principal_view.clone(),
        config: admin_config,
        admin_token: config
            .admin
            .token
            .clone()
            .or_else(|| std::env::var(&config.admin.token_env).ok()),
        start_time,
    };
    let reload_task = config_watcher.clone().map(spawn_reload_watcher);

    Ok(App {
        router: app_router(state, config.timeouts.upstream_total_secs),
        admin_router: cc_lb_admin::router(admin_state),
        proxy_addr: config.listener.proxy_addr,
        admin_addr: config.listener.admin_addr,
        reload_task,
        audit_writer_task,
        signals,
        drain_controller,
        tls_state,
    })
}

struct InMemoryCurrentConfig {
    current: ArcSwap<Config>,
    draft: Mutex<Option<Config>>,
    principal_view: Arc<ArcSwap<PrincipalView>>,
}

impl InMemoryCurrentConfig {
    fn new(config: Config, principal_view: Arc<ArcSwap<PrincipalView>>) -> Self {
        Self {
            current: ArcSwap::from_pointee(config),
            draft: Mutex::new(None),
            principal_view,
        }
    }
}

impl CurrentConfig for InMemoryCurrentConfig {
    fn current_config(&self) -> Arc<Config> {
        self.current.load_full()
    }

    fn put_draft_config(&self, config: Config) -> Result<(), ConfigDraftError> {
        config
            .validate()
            .map_err(|error| ConfigDraftError::Invalid(error.to_string()))?;
        let mut draft = self
            .draft
            .lock()
            .map_err(|_| ConfigDraftError::Invalid("config draft lock poisoned".to_owned()))?;
        *draft = Some(config);
        Ok(())
    }

    fn apply_draft_config(&self) -> Result<Arc<Config>, ConfigDraftError> {
        let config = self
            .draft
            .lock()
            .map_err(|_| ConfigDraftError::Invalid("config draft lock poisoned".to_owned()))?
            .take()
            .ok_or(ConfigDraftError::MissingDraft)?;
        config
            .validate()
            .map_err(|error| ConfigDraftError::Invalid(error.to_string()))?;
        self.principal_view
            .store(PrincipalView::from_config(&config));
        let config = Arc::new(config);
        self.current.store(config.clone());
        Ok(config)
    }
}

#[derive(Clone)]
struct ProxyState {
    lifecycle: Arc<Lifecycle>,
    breaker_registry: Arc<BreakerRegistry>,
    start_time: std::time::Instant,
    drain_controller: DrainController,
    #[allow(dead_code)]
    key_store: Option<Arc<KeyStore>>,
    #[allow(dead_code)]
    builtin_authn: Option<Arc<BuiltinAuthn>>,
}

fn spawn_price_catalog_loader(
    config: &Config,
    storage: Option<Arc<Storage>>,
    price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
) {
    let Some(storage) = storage else {
        return;
    };
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!("tokio runtime unavailable; litellm price catalog loader not started");
        return;
    }

    let price_catalog_config = &config.api_keys.price_catalog;
    let loader = LiteLlmLoader::new(
        price_catalog.clone(),
        storage,
        price_catalog_config.url.clone(),
        price_catalog_config.refresh_interval,
        price_catalog_config.cache_path.clone(),
    );
    let _loader_task = loader.start_daemon();
    tokio::spawn(async move {
        if !LiteLlmLoader::wait_for_first_snapshot(&price_catalog, Duration::from_secs(10)).await {
            tracing::warn!("litellm price catalog first snapshot unavailable; startup continuing");
        }
    });
}

#[derive(Clone, Serialize)]
struct HealthBody {
    status: &'static str,
    uptime_seconds: u64,
    build: BuildMeta,
    #[serde(skip_serializing_if = "Option::is_none")]
    ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

fn app_router(state: ProxyState, timeout_secs: u64) -> Router {
    health_router(state.clone()).merge(proxy_router(state, timeout_secs))
}

fn health_router(state: ProxyState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state)
}

fn proxy_router(state: ProxyState, timeout_secs: u64) -> Router {
    let request_ids = RequestIdState::default();
    let drain_controller = state.drain_controller.clone();
    let service_builder = ServiceBuilder::new()
        .layer(middleware::from_fn_with_state(
            drain_controller,
            crate::drain::proxy_drain_middleware,
        ))
        .layer(HopByHopStripLayer::new())
        .layer(cc_lb_observability::trace_layer(NoopObservabilityHook))
        .layer(middleware::from_fn_with_state(
            request_ids,
            request_id_middleware,
        ));
    let service_builder = service_builder.layer(crate::chaos::ChaosLayer::from_env());
    let service_builder = service_builder
        .layer(HandleErrorLayer::new(timeout_error))
        .timeout(Duration::from_secs(timeout_secs.max(1)));

    Router::new()
        .route("/v1/messages", post(lifecycle_handler))
        .route("/v1/messages/count_tokens", post(lifecycle_handler))
        .route("/v1/models", get(lifecycle_handler))
        .route("/v1/models/{id}", get(lifecycle_handler))
        .route("/v1/files", post(lifecycle_handler).get(lifecycle_handler))
        .route(
            "/v1/files/{id}",
            get(lifecycle_handler).delete(lifecycle_handler),
        )
        .route("/v1/files/{id}/content", get(lifecycle_handler))
        .route("/api/{*path}", any(lifecycle_handler))
        .route("/v1/{*path}", any(lifecycle_handler))
        .with_state(state)
        .layer(service_builder)
}

async fn healthz(State(state): State<ProxyState>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        uptime_seconds: state.start_time.elapsed().as_secs(),
        build: BuildMeta::current(),
        ready: None,
        reason: None,
    })
}

async fn readyz(State(state): State<ProxyState>) -> (StatusCode, Json<HealthBody>) {
    let draining = state.drain_controller.is_draining();
    let breaker_ready = state
        .breaker_registry
        .map
        .iter()
        .any(|entry| entry.value().current_state() != BreakerState::Open);
    let ready = !draining && breaker_ready;
    let reason = if draining {
        Some("draining".to_owned())
    } else if breaker_ready {
        None
    } else {
        Some("no_ready_upstream".to_owned())
    };

    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthBody {
            status: if ready { "ok" } else { "not_ready" },
            uptime_seconds: state.start_time.elapsed().as_secs(),
            build: BuildMeta::current(),
            ready: Some(ready),
            reason,
        }),
    )
}

async fn timeout_error(error: tower::BoxError) -> Response<Body> {
    let status = if error.is::<tower::timeout::error::Elapsed>() {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let mut response = Response::new(Body::from("request timed out"));
    *response.status_mut() = status;
    response
}

async fn lifecycle_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(source) => {
            let mut response = Response::new(Body::from(format!("body read failed: {source}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return response;
        }
    };
    let request = Request::from_parts(parts, body);
    match state.lifecycle.handle(request).await {
        Ok(response) => response,
        Err(source) => {
            let mut response = Response::new(Body::from(source.to_string()));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

#[derive(Clone, Default)]
struct RequestIdState {
    counter: Arc<AtomicU64>,
}

async fn request_id_middleware(
    State(state): State<RequestIdState>,
    mut request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let request_id = request
        .headers()
        .get("request-id")
        .cloned()
        .or_else(|| request.headers().get("x-request-id").cloned())
        .unwrap_or_else(|| {
            let id = state.counter.fetch_add(1, Ordering::Relaxed);
            match HeaderValue::from_str(&server_request_id(id)) {
                Ok(value) => value,
                Err(_) => HeaderValue::from_static("req_server"),
            }
        });
    request
        .headers_mut()
        .insert(HeaderName::from_static("request-id"), request_id.clone());
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(HeaderName::from_static("request-id"), request_id);
    response
}

fn server_request_id(id: u64) -> String {
    let mut request_id = String::with_capacity("req_server_".len() + 20);
    request_id.push_str("req_server_");
    let _ = write!(&mut request_id, "{id}");
    request_id
}

fn server_join_result(result: Result<Result<(), io::Error>, JoinError>) -> Result<(), BuildError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BuildError::from(source)),
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(BuildError::from(io::Error::other(source.to_string()))),
    }
}

fn init_observability(config: &mut Config) -> Result<TracingGuard, BuildError> {
    config.observability.prometheus_endpoint = Some(config.listener.metrics_addr.to_string());
    cc_lb_observability::init(&ObservabilityConfig {
        tracing_level: config.observability.tracing_level.clone(),
        otlp_endpoint: config.observability.otlp_endpoint.clone(),
        prometheus_endpoint: config.observability.prometheus_endpoint.clone(),
        log_redaction: config.observability.log_redaction,
        user_prompt_redaction: config.observability.user_prompt_redaction,
        hook_channel_capacity: cc_lb_observability::DEFAULT_HOOK_CHANNEL_CAPACITY,
    })
    .map_err(BuildError::from)
}

pub async fn open_storage(
    config: &Config,
) -> Result<
    (
        Arc<dyn ManagedKeyStore>,
        Option<Arc<cc_lb_storage_redb::Storage>>,
        Arc<AeadService>,
    ),
    BuildError,
> {
    let key_hex =
        std::env::var(&config.aead.key_env).map_err(|_| BuildError::StorageKeyMissing {
            env: config.aead.key_env.clone(),
        })?;
    let key = decode_hex_key(&key_hex)?;
    let aead = Arc::new(AeadService::from_master_key(key));
    match &config.storage {
        cc_lb_config::StorageConfig::Redb { path } => {
            let storage = Arc::new(Storage::open(path, key)?);
            let managed = cc_lb_storage_redb::RedbManagedKeyStore::new(storage.clone());
            Ok((Arc::new(managed), Some(storage), aead))
        }
        cc_lb_config::StorageConfig::Postgres {
            url,
            pool: pool_config,
        } => open_postgres_managed(url, pool_config, aead).await,
    }
}

#[cfg(feature = "postgres")]
async fn open_postgres_managed(
    url: &str,
    pool_config: &cc_lb_config::PostgresPoolConfig,
    aead: Arc<AeadService>,
) -> Result<
    (
        Arc<dyn ManagedKeyStore>,
        Option<Arc<cc_lb_storage_redb::Storage>>,
        Arc<AeadService>,
    ),
    BuildError,
> {
    use cc_lb_storage_api::{BackendKind, Storage as StorageTrait};
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    let pool = PgPoolOptions::new()
        .max_connections(pool_config.max_connections)
        .min_connections(pool_config.min_connections)
        .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(pool_config.idle_timeout_secs))
        .connect(url)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    let init_storage: Arc<dyn StorageTrait> =
        Arc::new(cc_lb_storage_postgres::PostgresStorage::new(pool.clone()));
    init_storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    let managed = cc_lb_storage_postgres::PostgresManagedKeyStore::new(
        pool,
        Arc::new(cc_lb_storage_postgres::adapter::retry::RetryPolicy::default()),
    );
    Ok((Arc::new(managed), None, aead))
}

#[cfg(not(feature = "postgres"))]
async fn open_postgres_managed(
    _url: &str,
    _pool_config: &cc_lb_config::PostgresPoolConfig,
    _aead: Arc<AeadService>,
) -> Result<
    (
        Arc<dyn ManagedKeyStore>,
        Option<Arc<cc_lb_storage_redb::Storage>>,
        Arc<AeadService>,
    ),
    BuildError,
> {
    Err(BuildError::StorageConnect {
        message: "postgres feature is not enabled".to_owned(),
    })
}

fn decode_hex_key(value: &str) -> Result<[u8; 32], BuildError> {
    if value.len() != 64 {
        return Err(BuildError::InvalidStorageKey);
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn hex_nibble(byte: u8) -> Result<u8, BuildError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(BuildError::InvalidStorageKey),
    }
}

fn dispatcher(config: &Config) -> (Arc<dyn UpstreamDispatch>, Arc<BreakerRegistry>) {
    let bulkhead_config = BulkheadConfig {
        max_conns_per_upstream: config.bulkhead.max_conns_per_upstream,
        semaphore_permits: config.bulkhead.semaphore_per_upstream,
        acquire_timeout: Duration::from_secs(1),
    };
    let breaker_config = BreakerConfig {
        failures_to_open: config.circuit_breaker.failures_to_open,
        failure_window: Duration::from_secs(config.circuit_breaker.window_secs.max(1)),
        half_open_after: Duration::from_secs(config.circuit_breaker.half_open_after_secs.max(1)),
        half_open_max_in_flight: 1,
    };
    let upstream_name = Arc::new(|request: &cc_lb_plugin_api::SignedRequest| {
        request
            .url()
            .host_str()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "unknown".to_owned())
    });
    let breaker_registry = Arc::new(BreakerRegistry::new());
    let breaker_upstream_name = upstream_name.clone();
    let breaker_registry_for_dispatcher = breaker_registry.clone();
    let dispatcher_factory = Arc::new(move |max_idle_per_host| {
        let base = make_default_dispatcher(max_idle_per_host);
        Arc::new(CircuitBreakerDispatch::new(
            base,
            breaker_registry_for_dispatcher.clone(),
            breaker_config,
            breaker_upstream_name.clone(),
        )) as Arc<dyn UpstreamDispatch>
    });
    (
        Arc::new(BulkheadDispatch::with_dispatcher_factory(
            Arc::new(BulkheadRegistry::new()),
            bulkhead_config,
            upstream_name.clone(),
            dispatcher_factory,
        )),
        breaker_registry,
    )
}

fn error_normalizer(config: &Config) -> Result<ErrorNormalizer, BuildError> {
    let mut normalizer = ErrorNormalizer::new();
    for spec in config.upstreams.values() {
        let upstream = builtins::upstream_from_spec(spec)?;
        let kind = UpstreamKind::from(&upstream);
        normalizer.register_dialect(kind, builtins::dialect_for_spec(spec)?);
    }
    Ok(normalizer)
}

fn manifest_from_plugin(plugin: &PluginRef) -> Result<PluginManifest, BuildError> {
    let artifact = plugin
        .wasm_path
        .as_ref()
        .ok_or_else(|| BuildError::InvalidPlugin {
            name: plugin.name.clone(),
            reason: "missing wasm_path".to_owned(),
        })?
        .display()
        .to_string();
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "observe_batch_count".to_owned(),
        serde_json::Value::from(plugin.batched_events_per_flush),
    );
    metadata.insert(
        "observe_flush_ms".to_owned(),
        serde_json::Value::from(plugin.batched_flush_ms),
    );
    Ok(PluginManifest {
        name: plugin.name.clone(),
        artifact,
        config: plugin.config.clone(),
        metadata,
    })
}

fn build_tls_state(config: &Config) -> Result<Option<Arc<TlsState>>, BuildError> {
    let Some((prefix, tls)) = active_tls_config(config) else {
        return Ok(None);
    };

    let cert_path = tls
        .cert_path
        .as_deref()
        .ok_or_else(|| BuildError::InvalidTlsConfig {
            field: format!("{prefix}.cert_path"),
            message: "missing TLS certificate path".to_owned(),
        })?;
    let key_path = tls
        .key_path
        .as_deref()
        .ok_or_else(|| BuildError::InvalidTlsConfig {
            field: format!("{prefix}.key_path"),
            message: "missing TLS key path".to_owned(),
        })?;

    Ok(Some(Arc::new(TlsState::from_paths(cert_path, key_path)?)))
}

fn active_tls_config(config: &Config) -> Option<(&'static str, &TlsConfig)> {
    if let Some(tls) = &config.listener.tls {
        Some(("listener.tls", tls))
    } else {
        config.tls.as_ref().map(|tls| ("tls", tls))
    }
}

fn spawn_reload_watcher(config_watcher: Arc<ConfigWatcher>) -> JoinHandle<()> {
    tokio::spawn(async move {
        config_watcher.watch_file_changes().await;
    })
}

fn sighup_handler(
    tls_state: Option<Arc<TlsState>>,
    config_watcher: Option<Arc<ConfigWatcher>>,
) -> Option<signal::SighupHandler> {
    if tls_state.is_none() && config_watcher.is_none() {
        return None;
    }

    Some(Arc::new(move || {
        if let Some(tls_state) = &tls_state {
            match tls_state.reload() {
                Ok(()) => tracing::info!("TLS certificate reload accepted"),
                Err(source) => tracing::warn!(error = %source, "TLS certificate reload failed"),
            }
        }
        if let Some(config_watcher) = &config_watcher {
            let _result = config_watcher.reload_now();
        }
    }))
}

fn cap_to_usize(value: u64) -> usize {
    match usize::try_from(value) {
        Ok(value) => value,
        Err(_) => usize::MAX,
    }
}

#[derive(Debug, Error)]
pub enum BuildError {
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Builtin(#[from] crate::builtins::BuiltinError),
    #[error(transparent)]
    Runtime(#[from] cc_lb_plugin_api::RuntimeError),
    #[error(transparent)]
    Observability(#[from] cc_lb_observability::InitError),
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_redb::StorageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error("missing storage key env {env}")]
    StorageKeyMissing { env: String },
    #[error("storage key must be 64 hexadecimal characters")]
    InvalidStorageKey,
    #[error("invalid plugin {name}: {reason}")]
    InvalidPlugin { name: String, reason: String },
    #[error("{field}: {message}")]
    InvalidTlsConfig { field: String, message: String },
    #[error("storage connection failed: {message}")]
    StorageConnect { message: String },
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::build_app_for_testing_postgres;

    #[tokio::test]
    async fn build_app_for_testing_postgres_smoke() -> Result<(), Box<dyn std::error::Error>> {
        let url = match std::env::var("CI_POSTGRES_URL") {
            Ok(u) => u,
            Err(_) => return Ok(()),
        };
        let _app = build_app_for_testing_postgres(&url).await?;
        Ok(())
    }
}
