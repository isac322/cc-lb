use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use cc_lb_config::{Config, PluginRef, StorageConfig, TlsConfig};
use cc_lb_core::BreakerState;
use cc_lb_core::{
    BreakerConfig, BreakerRegistry, BulkheadConfig, BulkheadDispatch, BulkheadRegistry,
    CircuitBreakerDispatch, DashboardBroadcaster, ErrorNormalizer, HopByHopStripLayer, Lifecycle,
    LifecycleConfig, PrincipalLimitStateSink, QuotaManager, QuotaPolicy, RequestEventSink,
    UpstreamDispatch, UpstreamKind, make_default_dispatcher, start_principal_limit_state_writer,
    start_request_event_writer, start_sweep,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_plugin_api::{ObservabilityHook, PluginManifest, PluginRuntime};
use cc_lb_runtime_extism::{ExtismRuntime, SignerFactoryResolver};
use cc_lb_storage_api::Storage;
use http_body_util::BodyExt;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::runtime::RuntimeFlavor;
use tokio::sync::{broadcast, watch};
use tokio::task::{JoinError, JoinHandle};
use tower::ServiceBuilder;

use crate::build_meta::BuildMeta;
use crate::builtins::{self, BuiltinAuthn, BuiltinRouter, NoopObservabilityHook};
use crate::drain::DrainController;
use crate::preflight::{self, PreflightOptions};
use crate::reload::ConfigWatcher;
use crate::signal;
use crate::storage_factory;
use crate::tls::{ReloadableListener, TlsState};
use cc_lb_admin::{AdminState, CurrentConfig};

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
    pub sweep_task: Option<JoinHandle<()>>,
    pub request_event_task: Option<JoinHandle<()>>,
    pub principal_limit_state_task: Option<JoinHandle<()>>,
    pub usage_rollup_task: Option<JoinHandle<()>>,
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
            sweep_task,
            request_event_task,
            principal_limit_state_task,
            usage_rollup_task,
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
        if let Some(task) = sweep_task {
            task.abort();
        }
        if let Some(task) = request_event_task {
            task.abort();
        }
        if let Some(task) = principal_limit_state_task {
            task.abort();
        }
        if let Some(task) = usage_rollup_task {
            task.abort();
        }

        let _ = admin_stop_tx.send(true);
        let _ = admin.await;
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
    let app = build_app_with_path_async(config, Some(config_path)).await?;
    app.start().await?;
    Ok(())
}

fn print_preflight_report(report: &preflight::PreflightReport) {
    println!("preflight: ok");
    for warning in &report.warnings {
        println!("preflight: warning: {warning}");
    }
}

pub fn build_app(config: Config) -> Result<App, BuildError> {
    build_app_with_path(config, None)
}

/// Test-only variant: auto-creates a temp redb storage with all-zero key.
/// The tempdir is intentionally leaked so the redb file stays accessible.
pub fn build_app_for_testing(mut config: Config) -> Result<App, BuildError> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("storage.redb");
    let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
    config.storage = StorageConfig::Redb { path };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    let storage_config = config.storage.clone();
    let storage = block_on_storage_open({
        let aead = aead.clone();
        async move {
            storage_factory::open_storage(&storage_config, aead)
                .await
                .map_err(BuildError::from)
        }
    })?;
    std::mem::forget(dir);
    build_app_with_storage(config, None, storage, aead)
}

pub fn build_app_with_path(config: Config, config_path: Option<&Path>) -> Result<App, BuildError> {
    let (storage, aead) = open_storage(&config)?;
    build_app_with_storage(config, config_path, storage, aead)
}

async fn build_app_with_path_async(
    config: Config,
    config_path: Option<&Path>,
) -> Result<App, BuildError> {
    let (storage, aead) = open_storage_async(&config).await?;
    build_app_with_storage(config, config_path, storage, aead)
}

fn build_app_with_storage(
    config: Config,
    config_path: Option<&Path>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
) -> Result<App, BuildError> {
    let runtime = ExtismRuntime::with_signer_factory_resolver(
        host_signer_resolver(&config, storage.clone(), aead.clone())
            .expect("host signer resolver is always available when storage is configured"),
    );
    let authn = match &config.plugins.authn_plugin {
        Some(plugin) => runtime.instantiate(&manifest_from_plugin(plugin)?)?,
        None => Arc::new(BuiltinAuthn::new(&config, storage.clone(), aead.clone())),
    };
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
    let (dispatcher, breaker_registry, bulkhead_registry) = dispatcher(&config);
    let (request_event_sink, request_event_task) = {
        let (sink, receiver) = RequestEventSink::new();
        (
            Some(Arc::new(sink)),
            Some(start_request_event_writer(storage.clone(), receiver)),
        )
    };
    let (principal_limit_state_sink, principal_limit_state_task) = {
        let (sink, receiver) = PrincipalLimitStateSink::new();
        (
            Some(Arc::new(sink)),
            Some(start_principal_limit_state_writer(
                storage.clone(),
                receiver,
            )),
        )
    };
    let dashboard_broadcaster = Arc::new(DashboardBroadcaster::new());
    let lifecycle = Arc::new(
        Lifecycle::new(
            authn,
            router_plugin,
            dispatcher,
            observability_hooks,
            LifecycleConfig {
                messages_body_cap_bytes: cap_to_usize(config.body.messages_cap_bytes),
                files_body_cap_bytes: cap_to_usize(config.body.files_cap_bytes),
            },
        )
        .with_error_normalizer(error_normalizer)
        .with_principal_limit_state_sink(principal_limit_state_sink)
        .with_request_event_sink(request_event_sink)
        .with_dashboard_broadcaster(Some(dashboard_broadcaster.clone())),
    );

    let quota_manager = Arc::new(QuotaManager::new(storage.clone(), quota_policy(&config)));
    let sweep_task = Some(start_sweep(quota_manager.clone(), Duration::from_secs(60)));
    let usage_rollup_task = Some(start_usage_rollup_worker(
        storage.clone(),
        Duration::from_secs(60),
    ));
    let start_time = std::time::Instant::now();
    let drain_controller = DrainController::new();
    let tls_state = build_tls_state(&config)?;
    let reload_tls_state = match active_tls_config(&config) {
        Some((_, tls)) if tls.reload_on_sighup => tls_state.clone(),
        _ => None,
    };
    let config_watcher = config_path.map(|path| Arc::new(ConfigWatcher::new(path, config.clone())));
    let config_path_buf = config_path.map(Path::to_path_buf);
    let admin_config_watcher: Option<Arc<dyn cc_lb_admin::ConfigReloader>> = config_watcher
        .clone()
        .map(|watcher| watcher as Arc<dyn cc_lb_admin::ConfigReloader>);
    let signals = signal::install(
        drain_controller.clone(),
        Duration::from_secs(config.timeouts.drain_secs),
        sighup_handler(reload_tls_state, config_watcher.clone()),
    );
    let state = ProxyState {
        lifecycle: lifecycle.clone(),
        breaker_registry: breaker_registry.clone(),
        start_time,
        drain_controller: drain_controller.clone(),
    };
    let admin_config: Arc<dyn CurrentConfig> = match &config_watcher {
        Some(watcher) => watcher.clone(),
        None => Arc::new(config.clone()),
    };

    let admin_state = AdminState {
        storage: storage.clone(),
        aead: aead.clone(),
        quota_manager: Some(quota_manager.clone()),
        lifecycle: Some(lifecycle.clone()),
        breaker_registry: Some(breaker_registry.clone()),
        drain_controller: Some(drain_controller.clone()),
        bulkhead_registry: Some(bulkhead_registry),
        plugin_runtime_status: None,
        dashboard_broadcaster,
        config: admin_config,
        config_path: config_path_buf,
        config_watcher: admin_config_watcher,
        config_started_at_unix_secs: unix_now_secs(),
        admin_token: config
            .admin
            .token
            .clone()
            .or_else(|| std::env::var(&config.admin.token_env).ok()),
        start_time,
    };
    let reload_task = config_watcher
        .clone()
        .map(|watcher| spawn_reload_watcher(watcher, quota_manager));

    Ok(App {
        router: app_router(state, config.timeouts.upstream_total_secs),
        admin_router: cc_lb_admin::router(admin_state),
        proxy_addr: config.listener.proxy_addr,
        admin_addr: config.listener.admin_addr,
        reload_task,
        sweep_task,
        request_event_task,
        principal_limit_state_task,
        usage_rollup_task,
        signals,
        drain_controller,
        tls_state,
    })
}

#[derive(Clone)]
struct ProxyState {
    lifecycle: Arc<Lifecycle>,
    breaker_registry: Arc<BreakerRegistry>,
    start_time: std::time::Instant,
    drain_controller: DrainController,
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
        .route(
            PROXY_FILES_ROUTE_COLLECTION,
            post(lifecycle_handler).get(lifecycle_handler),
        )
        .route(
            PROXY_FILES_ROUTE_ITEM,
            get(lifecycle_handler).delete(lifecycle_handler),
        )
        .route(PROXY_FILES_ROUTE_ITEM_CONTENT, get(lifecycle_handler))
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

fn host_signer_resolver(
    config: &Config,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
) -> Option<SignerFactoryResolver> {
    let config = config.clone();
    Some(Arc::new(
        move |factory_ref, principal, _signer_state| match factory_ref {
            "anthropic-key" => principal
                .claims
                .get("real_credential_storage_key")
                .and_then(serde_json::Value::as_str)
                .filter(|storage_key| !storage_key.trim().is_empty())
                .map(|storage_key| {
                    Arc::new(
                        cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::from_storage(
                            storage_key.to_owned(),
                            storage.clone(),
                            aead.clone(),
                        ),
                    ) as Arc<dyn cc_lb_plugin_api::SignerFactory>
                }),
            "anthropic-oauth" => builtins::anthropic_oauth_factory(
                &config,
                storage.clone(),
                aead.clone(),
                &principal.id,
                "anthropic_oauth",
            )
            .map(|factory| factory as Arc<dyn cc_lb_plugin_api::SignerFactory>),
            _ => None,
        },
    ))
}

fn open_storage(config: &Config) -> Result<(Arc<dyn Storage>, Arc<AeadService>), BuildError> {
    let config = config.clone();
    block_on_storage_open(async move { open_storage_async(&config).await })
}

async fn open_storage_async(
    config: &Config,
) -> Result<(Arc<dyn Storage>, Arc<AeadService>), BuildError> {
    let key_hex =
        std::env::var(&config.aead.key_env).map_err(|_| BuildError::StorageKeyMissing {
            env: config.aead.key_env.clone(),
        })?;
    let key = decode_hex_key(&key_hex)?;
    let aead = Arc::new(AeadService::from_master_key(key));
    let storage = storage_factory::open_storage(&config.storage, aead.clone()).await?;
    Ok((storage, aead))
}

fn block_on_storage_open<F, T>(future: F) -> Result<T, BuildError>
where
    F: Future<Output = Result<T, BuildError>> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(BuildError::from)?
                .block_on(future)
        })
        .join()
        .map_err(|_| {
            BuildError::StorageTask("storage initialization thread panicked".to_owned())
        })?,
        Err(_) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(BuildError::from)?
            .block_on(future),
    }
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

fn dispatcher(
    config: &Config,
) -> (
    Arc<dyn UpstreamDispatch>,
    Arc<BreakerRegistry>,
    Arc<BulkheadRegistry>,
) {
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
    let bulkhead_registry = Arc::new(BulkheadRegistry::new());
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
            bulkhead_registry.clone(),
            bulkhead_config,
            upstream_name.clone(),
            dispatcher_factory,
        )),
        breaker_registry,
        bulkhead_registry,
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

fn quota_policy(config: &Config) -> QuotaPolicy {
    QuotaPolicy {
        window_secs: config.quotas.default_window_secs.max(1),
        capacity_requests: config.quotas.default_requests_per_window,
        capacity_input_tokens: config.quotas.default_input_tokens,
        capacity_output_tokens: config.quotas.default_output_tokens,
    }
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

fn spawn_reload_watcher(
    config_watcher: Arc<ConfigWatcher>,
    quota_manager: Arc<QuotaManager>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let file_watcher = config_watcher.clone();
        let quota_updates = config_watcher.subscribe();
        tokio::select! {
            _ = file_watcher.watch_file_changes() => {}
            _ = quota_reload_loop(quota_updates, quota_manager) => {}
        }
    })
}

fn start_usage_rollup_worker(storage: Arc<dyn Storage>, period: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        loop {
            interval.tick().await;
            match storage.rollup_usage_once().await {
                Ok(run) => {
                    if run.processed_events > 0 {
                        tracing::debug!(
                            processed_events = run.processed_events,
                            updated_rollups = run.updated_rollups,
                            "usage rollup completed"
                        );
                    }
                }
                Err(source) => {
                    tracing::warn!(error = %source, "usage rollup failed");
                }
            }
        }
    })
}

async fn quota_reload_loop(
    mut reloads: broadcast::Receiver<Arc<Config>>,
    quota_manager: Arc<QuotaManager>,
) {
    loop {
        match reloads.recv().await {
            Ok(config) => {
                quota_manager.set_defaults(quota_policy(&config)).await;
                tracing::info!("quota defaults reloaded from configuration");
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(skipped, "quota reload subscriber lagged");
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
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

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
    StorageFactory(#[from] crate::storage_factory::StorageFactoryError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error("missing storage key env {env}")]
    StorageKeyMissing { env: String },
    #[error("storage key must be 64 hexadecimal characters")]
    InvalidStorageKey,
    #[error("storage initialization failed: {0}")]
    StorageTask(String),
    #[error("invalid plugin {name}: {reason}")]
    InvalidPlugin { name: String, reason: String },
    #[error("{field}: {message}")]
    InvalidTlsConfig { field: String, message: String },
}
