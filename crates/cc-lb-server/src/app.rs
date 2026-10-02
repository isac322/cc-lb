use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::error_handling::HandleErrorLayer;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, HeaderValue};
use axum::http::{HeaderMap, HeaderName, Request, Response, StatusCode};
use axum::middleware::{self, Next};
use axum::routing::{any, get, post};
use bytes::{Bytes, BytesMut};
use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_config::{Config, TlsConfig};
use cc_lb_control::PromptCacheObservationSinkLike;
use cc_lb_control::{
    DynamicView, DynamicViewHolder,
    api_keys::{
        builtin_authn::BuiltinAuthn, concurrent_guard::KeyConcurrencyManager, key_store::KeyStore,
        limit_engine::LimitEngine,
    },
    spawn_audit_writer,
};
use cc_lb_engine::{
    BreakerRegistry, BreakerRuntimeConfig, BulkheadDispatch, BulkheadRegistry,
    BulkheadRuntimeConfig, CircuitBreakerDispatch, DrainController, HopByHopStripLayer, Lifecycle,
    LifecycleConfig, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig, UpstreamDispatch,
    UpstreamRateLimitSink, anthropic_error_response, cache_keepalive::AnthropicKeepaliveDispatcher,
    make_default_dispatcher, start_subscription_quota_writer, start_upstream_rate_limit_writer,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    ManagedKeyStore, MetaStore, RuntimeChangeNotifier, Storage, UpstreamRecord,
};
use cc_lb_upstream::SignedRequest;
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tracing::Instrument as _;

#[cfg(feature = "postgres")]
use crate::admin_ports::ServerRetainedPartialPort;
use crate::admin_ports::{
    ServerRoutePreviewPort, ServerSubscriptionQuotaIngestionPort, ServerWarmupPort,
};
use crate::build_meta::BuildMeta;
use crate::dynamic_view_builder::{
    Stores as DynamicStores, build_dynamic_view, ensure_wasm_cache_dirs,
};
use crate::notify_listener::{NotifyListener, NotifyListenerParams};
use crate::preflight;
use crate::prompt_cache_observation_sink::{
    DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
};
use crate::reconcile::Reconciler;
use crate::refresh::{LazyRefreshClaimGuard, LazyRefresher};
use crate::replica;
use crate::signal;
use crate::state_machine::{ServerState, ServerStateHandle};
use crate::storage_factory;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use crate::tls::{ReloadableListener, TlsState};
use cc_lb_admin::auth::{AdminAuthenticator, build_providers};
use cc_lb_admin::{
    AdminPorts, AdminState, DynamicViewRebinder, StartupConfigOverrides,
    WarmupDialectDispatchError, WarmupDialectDispatchErrorKind, WarmupDialectDispatchOutcome,
    WarmupDialectDispatcher,
};

const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_millis(500);
const PRICE_CATALOG_LOCAL_INSTALL_INTERVAL: Duration = Duration::from_secs(60);
const WASMTIME_POOL_METRICS_INTERVAL: Duration = Duration::from_secs(10);

/// Response extension carrying the terminal classification for a response
/// produced outside the lifecycle handler. `lifecycle_middleware` reads it
/// and finalizes the request's `LifecycleContext`; unmarked responses are
/// left to the context's `Drop` backstop. Responses that terminate on a
/// known internal failure carry `cc_lb_engine::InternalFailure` instead,
/// which the middleware consumes first.
type TerminalClassification = cc_lb_engine::TerminalClassification;

#[derive(Clone, Copy)]
struct RequestBodyCaps {
    messages: usize,
    files: usize,
}

impl RequestBodyCaps {
    fn for_path(self, path: &str) -> usize {
        if path.starts_with("/v1/files") {
            self.files
        } else {
            self.messages
        }
    }
}

#[derive(Debug)]
enum RequestBodyReadError {
    TooLarge,
    Read(tower::BoxError),
}

struct RequestBodyTimingGuard {
    observer: Option<cc_lb_engine::LifecycleContext>,
    started: Instant,
    first_chunk_received: Option<Instant>,
    accumulated_wait: Duration,
    active_wait_started: Option<Instant>,
    process_started: Instant,
    process: Duration,
    chunk_count: u64,
    published: bool,
}

impl RequestBodyTimingGuard {
    fn new(observer: Option<cc_lb_engine::LifecycleContext>) -> Self {
        let started = Instant::now();
        Self {
            observer,
            started,
            first_chunk_received: None,
            accumulated_wait: Duration::ZERO,
            active_wait_started: None,
            process_started: started,
            process: Duration::ZERO,
            chunk_count: 0,
            published: false,
        }
    }

    fn begin_wait(&mut self) {
        debug_assert!(self.active_wait_started.is_none());
        let started = Instant::now();
        self.process = self
            .process
            .saturating_add(started.saturating_duration_since(self.process_started));
        self.active_wait_started = Some(started);
    }

    fn end_wait(&mut self, ended: Instant) {
        let Some(started) = self.active_wait_started.take() else {
            debug_assert!(false, "request body wait ended without a start");
            return;
        };
        self.accumulated_wait = self
            .accumulated_wait
            .saturating_add(ended.saturating_duration_since(started));
        self.process_started = ended;
    }

    fn observe_data(&mut self, received: Instant) {
        self.first_chunk_received.get_or_insert(received);
        self.chunk_count = self.chunk_count.saturating_add(1);
    }

    fn snapshot(&self, ended: Instant) -> cc_lb_lifecycle::RequestIoTimings {
        let (wait, process) = if let Some(started) = self.active_wait_started {
            (
                self.accumulated_wait
                    .saturating_add(ended.saturating_duration_since(started)),
                self.process,
            )
        } else {
            (
                self.accumulated_wait,
                self.process
                    .saturating_add(ended.saturating_duration_since(self.process_started)),
            )
        };
        cc_lb_lifecycle::RequestIoTimings {
            request_body_first_chunk_ms: self
                .first_chunk_received
                .map(|received| duration_ms(received.saturating_duration_since(self.started))),
            request_body_receive_ms: self
                .first_chunk_received
                .map(|received| duration_ms(ended.saturating_duration_since(received))),
            request_body_wait_ms: Some(duration_ms(wait)),
            request_body_process_ms: Some(duration_ms(process)),
            request_body_chunk_count: Some(self.chunk_count),
            ..Default::default()
        }
    }

    fn finish(mut self) {
        let timings = self.snapshot(Instant::now());
        self.published = true;
        self.publish(timings);
    }

    fn publish(&self, timings: cc_lb_lifecycle::RequestIoTimings) {
        if let Some(observer) = self.observer.as_ref() {
            observer.set_io_timings(timings);
        }
    }
}

impl Drop for RequestBodyTimingGuard {
    fn drop(&mut self) {
        if !self.published {
            self.publish(self.snapshot(Instant::now()));
        }
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

struct PricingLimitCostEstimator {
    catalog: Arc<cc_lb_pricing::PriceCatalog>,
}

impl cc_lb_engine::LimitCostEstimator for PricingLimitCostEstimator {
    fn estimate_max(
        &self,
        model: &str,
        max_input: u64,
        max_output: u64,
        service_tier: Option<&str>,
    ) -> Option<i64> {
        self.catalog
            .estimate_max(model, max_input, max_output, service_tier)
            .map(|cost| cost.try_into().unwrap_or(i64::MAX))
    }
}

pub struct App {
    pub router: Router,
    pub admin_router: Router,
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    lifecycle: Arc<Lifecycle>,
    notify_cancel: Option<CancellationToken>,
    notifier_task: Option<JoinHandle<()>>,
    notify_listener_task: Option<JoinHandle<()>>,
    price_catalog_install_cancel: Option<CancellationToken>,
    price_catalog_install_task: Option<JoinHandle<()>>,
    wasmtime_pool_metrics_cancel: Option<CancellationToken>,
    wasmtime_pool_metrics_task: Option<JoinHandle<()>>,
    scheduler_cancel: Option<CancellationToken>,
    scheduler_tasks: Vec<JoinHandle<()>>,
    audit_writer_task: Option<JoinHandle<()>>,
    upstream_rate_limit_writer_task: Option<JoinHandle<()>>,
    subscription_quota_writer_task: Option<JoinHandle<()>>,
    prompt_cache_observation_writer_task: Option<JoinHandle<()>>,
    event_fanout_shutdown_tx: Option<watch::Sender<bool>>,
    event_fanout_tasks: Vec<JoinHandle<()>>,
    signals: signal::SignalHandle,
    drain_controller: DrainController,
    tls_state: Option<Arc<TlsState>>,
    server_state: Arc<ServerStateHandle>,
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

#[derive(Debug, Error)]
pub enum BuildError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Preflight(#[from] crate::preflight::PreflightError),
    #[error(transparent)]
    Observability(#[from] cc_lb_observability::InitError),
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_api::StorageError),
    #[error(transparent)]
    StorageFactory(#[from] crate::storage_factory::StorageFactoryError),
    #[error(transparent)]
    SchedulerFactory(#[from] crate::scheduler_factory::SchedulerFactoryError),
    #[error("admin authentication configuration failed: {message}")]
    AdminAuth { message: String },
    #[cfg(feature = "postgres")]
    #[error("storage connection failed: {message}")]
    StorageConnect { message: String },
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error(transparent)]
    Rebind(#[from] crate::dynamic_view_builder::RebindError),
    #[error("wasmtime plugin runtime init failed: {0}")]
    WasmtimeRuntimeInit(#[source] cc_lb_runtime_wasmtime::WasmtimeRuntimeError),
    #[error("storage is required")]
    StorageRequired,
    #[error("storage master key env {env} is missing")]
    StorageKeyMissing { env: String },
    #[error("storage master key must be 32 bytes encoded as 64 hex characters")]
    InvalidStorageKey,
    #[error("cluster token env {env} is missing")]
    ClusterTokenMissing { env: String },
    #[error("postgres storage pool unavailable for pg_notify fanout")]
    PgNotifyPoolUnavailable,
    #[error("cluster.instance_url is required when storage.kind=postgres")]
    ClusterInstanceUrlMissing,
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
            lifecycle,
            notify_cancel,
            notifier_task,
            notify_listener_task,
            price_catalog_install_cancel,
            price_catalog_install_task,
            wasmtime_pool_metrics_cancel,
            wasmtime_pool_metrics_task,
            scheduler_cancel,
            scheduler_tasks,
            audit_writer_task,
            upstream_rate_limit_writer_task,
            subscription_quota_writer_task,
            prompt_cache_observation_writer_task,
            event_fanout_shutdown_tx,
            event_fanout_tasks,
            signals,
            drain_controller: _,
            tls_state,
            server_state,
        } = self;
        server_state.wait_for_ready().await;
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

        if let Some(cancel) = notify_cancel {
            cancel.cancel();
        }
        if let Some(task) = notifier_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(task) = notify_listener_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(cancel) = price_catalog_install_cancel {
            cancel.cancel();
        }
        if let Some(task) = price_catalog_install_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(cancel) = wasmtime_pool_metrics_cancel {
            cancel.cancel();
        }
        if let Some(task) = wasmtime_pool_metrics_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(cancel) = scheduler_cancel {
            cancel.cancel();
        }
        for task in scheduler_tasks {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        let _ = admin_stop_tx.send(true);
        await_or_abort(admin, SHUTDOWN_TASK_TIMEOUT).await;
        if let Some(task) = audit_writer_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(task) = upstream_rate_limit_writer_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(task) = subscription_quota_writer_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(task) = prompt_cache_observation_writer_task {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        if let Some(tx) = event_fanout_shutdown_tx {
            let _ = tx.send(true);
        }
        for task in event_fanout_tasks {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        // Optional observations must not consume the durable writers' flush budget.
        // The worker has drained concurrently during the mandatory cleanup above.
        if tokio::time::timeout(SHUTDOWN_TASK_TIMEOUT, lifecycle.shutdown())
            .await
            .is_err()
        {
            cc_lb_observability::increment_dropped_events_by(
                "stream_completion_observer_shutdown_timeout",
                1,
            );
            tracing::warn!(
                "completion observer drain exceeded the cleanup budget; worker remains detached"
            );
        }
        proxy_result?;
        Ok(())
    }
}

pub async fn run_serve(
    config_path: &Path,
    data_dir: Option<&Path>,
    strict_preflight: bool,
    clock: ClockHandle,
) -> Result<(), ServeError> {
    let mut config = Config::load(config_path)?;
    let startup_config_overrides = StartupConfigOverrides {
        runtime_data_dir: data_dir.map(Path::to_path_buf),
    };
    if let Some(data_dir) = startup_config_overrides.runtime_data_dir.as_ref() {
        config.runtime.data_dir = Some(data_dir.clone());
    }
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let _guard = init_observability(&config)?;
    let app = match build_app_with_path_inner(
        config,
        Some(config_path),
        startup_config_overrides,
        Some(StartupPreflight { strict_preflight }),
        clock,
    )
    .await
    {
        Ok(app) => app,
        Err(error @ BuildError::SchedulerFactory(_)) => {
            cc_lb_scheduler::scheduler_metrics::set_scheduler_init_failure(true);
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    };
    app.start().await?;
    Ok(())
}

fn print_preflight_report(report: &preflight::PreflightReport) {
    println!("preflight: ok");
    println!("preflight: upstream_count: {}", report.upstream_count);
    println!("preflight: upstream_warnings: {}", report.upstream_warnings);
    println!("preflight: principal_count: {}", report.principal_count);
    println!(
        "preflight: principal_disabled_count: {}",
        report.principal_disabled_count
    );
    println!(
        "preflight: plugin_chain_entry_count: {}",
        report.plugin_chain_entry_count
    );
    println!(
        "preflight: plugin_blob_missing_count: {}",
        report.plugin_blob_missing_count
    );
    for warning in &report.warnings {
        println!("preflight: warning: {warning}");
    }
}

#[derive(Clone, Copy)]
struct StartupPreflight {
    strict_preflight: bool,
}

pub async fn build_app(config: Config, clock: ClockHandle) -> Result<App, BuildError> {
    build_app_with_path(config, None, clock).await
}

pub async fn build_app_for_testing(
    mut config: Config,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("storage.sqlite");
    let key = [0u8; 32];
    let aead = Arc::new(AeadService::from_master_key(key));
    let database_url = format!("sqlite://{}", path.display());
    let storage_arc =
        Arc::new(cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone()).await?);
    storage_arc.initialize().await?;
    let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
    let storage: Arc<dyn Storage> = storage_arc;
    config.storage = cc_lb_config::StorageConfig::Sqlite { path };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    std::mem::forget(dir);
    let opened_scheduler = crate::scheduler_factory::open_scheduler_storage(
        &config.storage,
        &config.scheduler,
        clock.clone(),
    )
    .await?;
    build_app_with_storage_inner(
        config,
        None,
        StartupConfigOverrides::default(),
        managed_store,
        storage,
        aead,
        None,
        opened_scheduler,
        None,
        clock,
    )
    .await
}

#[cfg(feature = "postgres")]
const POSTGRES_TEST_SCHEMA: &str = "cc_lb_app_test";

#[cfg(feature = "postgres")]
async fn reset_app_testing_postgres_schema(database_url: &str) -> Result<(), BuildError> {
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;
    sqlx::query("DROP SCHEMA IF EXISTS cc_lb_app_test CASCADE")
        .execute(&pool)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;
    sqlx::query("CREATE SCHEMA cc_lb_app_test")
        .execute(&pool)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;
    pool.close().await;
    Ok(())
}

#[cfg(feature = "postgres")]
pub async fn build_app_for_testing_postgres(
    database_url: &str,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    use cc_lb_storage_api::Storage as StorageTrait;
    use sqlx::postgres::PgPoolOptions;

    reset_app_testing_postgres_schema(database_url).await?;
    let search_path = format!("{POSTGRES_TEST_SCHEMA}, public");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |connection, _metadata| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path', $1, false)")
                    .bind(&search_path)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;

    let init_storage: Arc<dyn StorageTrait> = Arc::new(
        cc_lb_storage_postgres::PostgresStorage::new(pool.clone(), clock.clone()),
    );
    init_storage
        .initialize()
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;
    seed_app_testing_storage(init_storage.as_ref(), None, &*clock).await?;

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
            clock.clone(),
        ));
    let key = [0u8; 32];
    let aead = Arc::new(AeadService::from_master_key(key));
    let config = build_app_for_testing_postgres_config(database_url);
    build_app_with_storage(config, None, managed_store, init_storage, aead, clock).await
}

#[cfg(feature = "postgres")]
pub async fn seed_app_testing_storage(
    storage: &dyn Storage,
    upstream_base_url: Option<url::Url>,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<(), BuildError> {
    use cc_lb_clock::unix_secs;
    use cc_lb_storage_api::principal::{Limit, LimitKind, PrincipalCreate, PrincipalKind};
    use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
    use cc_lb_storage_api::{PrincipalStore, StorageError, UpstreamStore};

    let now = unix_secs(clock.now());
    // NOTE [Priority-3 footgun]: postgres-conformance's managed_key_multi_instance
    // spawns two cc-lb instances against the same database; both call this seed
    // helper, race on get_by_name, and one then loses the create with a Conflict
    // on principals_v1_name_active_uniq. Treat Conflict as success (same pattern
    // the upstream block below uses) so the test's `concurrent_cross_instance_issue`
    // scenario stops flaking.
    match PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: "test-principal".to_owned(),
            kind: PrincipalKind::Machine,
            default_limits: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 1_000_000,
            }],
            allowed_models: vec!["*".to_owned()],
            allowed_upstreams: vec![],
            cache_keepalive: None,
        },
        now,
    )
    .await
    {
        Ok(_) | Err(StorageError::Conflict { .. }) => {}
        Err(error) => return Err(error.into()),
    }

    if UpstreamStore::get_by_name(storage, "test-upstream")
        .await?
        .is_none()
    {
        match UpstreamStore::create(
            storage,
            UpstreamCreate {
                name: "test-upstream".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: upstream_base_url,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        {
            Ok(record) => {
                // Same master key as build_app_for_testing_postgres; the
                // ciphertext is bound to the upstream id like OAuth secrets.
                let ciphertext = AeadService::from_master_key([0u8; 32])
                    .encrypt(b"sk-ant-fixture-secret", record.id.as_bytes())
                    .map_err(|error| BuildError::StorageConnect {
                        message: format!("test upstream api-key encryption failed: {error}"),
                    })?;
                UpstreamStore::update_api_key_secret(storage, record.id, Some(ciphertext)).await?;
            }
            Err(StorageError::Conflict { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn build_app_for_testing_postgres_config(database_url: &str) -> Config {
    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Postgres {
            url: database_url.to_owned(),
            pool: cc_lb_config::PostgresPoolConfig::default(),
        },
        ..Config::default()
    };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    // The postgres backend always runs pg_notify fanout, which needs a peer
    // identity and a shared cluster token. Postgres tests only run with
    // CI_POSTGRES_URL set, so borrow it as the token env instead of mutating
    // process-wide state from a library helper.
    config.cluster.instance_url = Some("http://127.0.0.1:0".to_owned());
    config.cluster.token_env = "CI_POSTGRES_URL".to_owned();
    config
}

pub async fn build_app_with_path(
    config: Config,
    config_path: Option<&Path>,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    build_app_with_path_inner(
        config,
        config_path,
        StartupConfigOverrides::default(),
        None,
        clock,
    )
    .await
}

async fn build_app_with_path_inner(
    config: Config,
    config_path: Option<&Path>,
    startup_config_overrides: StartupConfigOverrides,
    startup_preflight: Option<StartupPreflight>,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    config.validate()?;
    validate_cluster_token_before_storage(&config)?;

    let (managed_store, storage, aead, lazy_refresh_claim_guard, opened_scheduler) =
        open_storage(&config, clock.clone()).await?;
    build_app_with_storage_inner(
        config,
        config_path,
        startup_config_overrides,
        managed_store,
        storage,
        aead,
        startup_preflight,
        opened_scheduler,
        Some(lazy_refresh_claim_guard),
        clock,
    )
    .await
}

pub fn resolve_data_dir(
    cli: Option<&Path>,
    config_value: Option<&Path>,
    env_var: &str,
) -> Result<PathBuf, BuildError> {
    let data_dir = if let Ok(env_path) = std::env::var(env_var) {
        PathBuf::from(env_path)
    } else if let Some(cli_path) = cli {
        cli_path.to_path_buf()
    } else if let Some(config_path) = config_value {
        config_path.to_path_buf()
    } else {
        PathBuf::from("./data")
    };

    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)?;
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&data_dir, Permissions::from_mode(0o700))?;
        }
    }

    Ok(data_dir)
}

pub async fn build_app_with_storage(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    validate_cluster_token_before_storage(&config)?;

    let opened_scheduler = crate::scheduler_factory::open_scheduler_storage(
        &config.storage,
        &config.scheduler,
        clock.clone(),
    )
    .await?;
    build_app_with_storage_inner(
        config,
        config_path,
        StartupConfigOverrides::default(),
        managed_store,
        storage,
        aead,
        None,
        opened_scheduler,
        None,
        clock,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_app_with_storage_inner(
    config: Config,
    config_path: Option<&Path>,
    startup_config_overrides: StartupConfigOverrides,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    startup_preflight: Option<StartupPreflight>,
    opened_scheduler: crate::scheduler_factory::OpenedScheduler,
    lazy_refresh_claim_guard: Option<Arc<dyn LazyRefreshClaimGuard>>,
    clock: ClockHandle,
) -> Result<App, BuildError> {
    let scheduler_lazy_handle = opened_scheduler.lazy_handle();
    let server_state = Arc::new(ServerStateHandle::new_starting());
    let metrics_hook: Arc<dyn cc_lb_observability::EngineMetricsHook> =
        Arc::new(cc_lb_observability::MetricsCrateHook);
    let key_store = Arc::new(KeyStore::new(managed_store));
    let price_catalog = cc_lb_pricing::global_catalog().clone();
    let (price_catalog_install_cancel, price_catalog_install_task) =
        spawn_price_catalog_local_installer(
            &config,
            storage.clone(),
            price_catalog.clone(),
            clock.clone(),
        );
    let (sink, audit_writer_task) = spawn_audit_writer(storage.clone(), 1024);
    let audit_sink = Arc::new(sink);
    let (upstream_rate_limit_sink, upstream_rate_limit_receiver) =
        UpstreamRateLimitSink::with_metrics(Arc::clone(&metrics_hook));
    let upstream_rate_limit_writer_task = start_upstream_rate_limit_writer(
        storage.clone(),
        upstream_rate_limit_receiver,
        Arc::clone(&metrics_hook),
    );
    let (subscription_quota_sink, subscription_quota_receiver) =
        SubscriptionQuotaSink::with_capacity(
            config.subscription_quota.writer_channel_capacity as usize,
        );
    let subscription_quota_cache = Arc::new(SubscriptionQuotaCache::new());
    let subscription_quota_writer_cancel = CancellationToken::new();
    let subscription_quota_writer_task = start_subscription_quota_writer(
        storage.clone(),
        subscription_quota_receiver,
        SubscriptionQuotaWriterConfig {
            batch_max_records: config.subscription_quota.writer_batch_max_records as usize,
            flush_max_ms: config.subscription_quota.writer_flush_ms,
        },
        subscription_quota_writer_cancel.clone(),
    );
    let subscription_metadata_hook = Some(cc_lb_control::start_subscription_metadata_hook(
        Arc::new(ServerMetadataRefreshEnqueue {
            scheduler_backend: scheduler_lazy_handle.clone(),
        }),
        clock.clone(),
    ));
    let mut hot_engine_cfg = hot_engine_config_from_config(&config);
    hot_engine_cfg.shape_origin_policy = match config.runtime.wasmtime.shape_origin_policy {
        cc_lb_config::ShapeOriginPolicy::Unrestricted => {
            cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy::Unrestricted
        }
        cc_lb_config::ShapeOriginPolicy::SelectedUpstreamOrigin => {
            cc_lb_runtime_wasmtime::policy::ShapeOriginPolicy::SelectedUpstreamOrigin
        }
    };
    let bounds = &config.runtime.wasmtime.wire_bounds;
    hot_engine_cfg.wire_bounds = cc_lb_runtime_wasmtime::policy::PluginWireBounds {
        output_body_bytes: bounds.output_body_bytes,
        max_headers: bounds.max_headers,
        max_header_value_bytes: bounds.max_header_value_bytes,
        reason_bytes: bounds.reason_bytes,
    };
    hot_engine_cfg.cookie_redaction = config.runtime.wasmtime.cookie_redaction;
    let runtime =
        Arc::new(WasmtimeRuntime::new(hot_engine_cfg).map_err(BuildError::WasmtimeRuntimeInit)?);
    let (wasmtime_pool_metrics_cancel, wasmtime_pool_metrics_task) =
        spawn_wasmtime_pool_metrics_publisher(runtime.clone());
    let data_dir = resolve_data_dir(None, config.runtime.data_dir.as_deref(), "CC_LB_DATA_DIR")?;
    let storage_for_dynamic = storage.clone();
    ensure_wasm_cache_dirs(&data_dir)?;

    let concurrent_mgr = Arc::new(KeyConcurrencyManager::new());
    let limit_engine = LimitEngine::new(concurrent_mgr, clock.clone());
    let api_key_usage_handle = limit_engine
        .start_durable_usage_sync(storage.clone())
        .await?;
    let api_key_usage_slot = Arc::new(tokio::sync::Mutex::new(Some(api_key_usage_handle)));
    let limit_reservation_ttl_handle = Some(
        cc_lb_control::api_keys::limit_engine::spawn_reservation_ttl_sweeper(
            limit_engine.clone(),
            std::time::Duration::from_secs(config.limit_reservation_ttl.ttl_secs.max(1)),
            std::time::Duration::from_secs(config.limit_reservation_ttl.tick_secs.max(1)),
        ),
    );
    let limit_reservation_ttl_slot: Arc<
        tokio::sync::Mutex<
            Option<cc_lb_control::api_keys::limit_engine::ReservationTtlSweeperHandle>,
        >,
    > = Arc::new(tokio::sync::Mutex::new(limit_reservation_ttl_handle));
    let builtin_authn = Arc::new(BuiltinAuthn::new(key_store.clone(), clock.clone()));

    let (dispatcher, _breaker_registry) = dispatcher(&config, clock.clone());

    let replica_identity = {
        match replica::load_or_create_replica_id(&data_dir) {
            Ok(id) => Some(cc_lb_domain::ReplicaIdentity { id }),
            Err(e) => {
                tracing::warn!(error = %e, "failed to load or create replica ID; proceeding without it");
                None
            }
        }
    };

    let stores = Arc::new(DynamicStores {
        upstreams: storage_for_dynamic.clone(),
        principals: storage_for_dynamic.clone(),
        plugin_registry: storage_for_dynamic.clone(),
        upstream_rate_limits: storage_for_dynamic.clone(),
        upstream_subscription_quotas: storage_for_dynamic.clone(),
        upstream_subscription_metadata: storage_for_dynamic.clone(),
        organization_metadata: storage_for_dynamic.clone(),
        plan_tiers: storage_for_dynamic.clone(),
        prompt_cache_observations: storage_for_dynamic.clone(),
        anthropic_compatibility_kv: storage_for_dynamic.clone(),
        audit: Some(storage_for_dynamic.clone()),
    });
    let prompt_cache_grace_margin_secs = config.prompt_cache_shadow.grace_margin_secs;
    let observation_store_kind = match &config.storage {
        cc_lb_config::StorageConfig::Sqlite { .. } => {
            cc_lb_observability::cache_observation_store_kind::SQLITE
        }
        cc_lb_config::StorageConfig::Postgres { .. } => {
            cc_lb_observability::cache_observation_store_kind::POSTGRES
        }
    };
    let (sink, writer) = PromptCacheObservationSink::new(
        stores.prompt_cache_observations.clone(),
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
        observation_store_kind,
    );
    let prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
    let prompt_cache_observation_writer_task = Some(writer);
    let body_caps = RequestBodyCaps {
        messages: cap_to_usize(config.body.messages_cap_bytes),
        files: cap_to_usize(config.body.files_cap_bytes),
    };
    let lifecycle_config =
        lifecycle_config_from_config(&config, body_caps, replica_identity.clone());
    let scheduler_replica_id = lifecycle_config
        .replica_identity
        .as_ref()
        .map(|identity| identity.id);
    if let Some(startup_preflight) = startup_preflight {
        let report = preflight::run_preflight(&stores, &data_dir).await?;
        print_preflight_report(&report);
        if startup_preflight.strict_preflight && !report.warnings.is_empty() {
            eprintln!(
                "preflight: strict mode failed with {} warning(s)",
                report.warnings.len()
            );
            std::process::exit(1);
        }
    }

    let oauth_cfg = Arc::new(config.oauth.anthropic.clone().unwrap_or_default());
    let refresh_cancel = CancellationToken::new();
    let lazy_refresher_concrete: Option<Arc<LazyRefresher>> = lifecycle_config
        .replica_identity
        .as_ref()
        .map(|identity| match lazy_refresh_claim_guard.clone() {
            Some(claim_guard) => Arc::new(LazyRefresher::new_with_claim_guard(
                crate::refresh::LazyRefresherDeps {
                    stores: stores.clone(),
                    aead: aead.clone(),
                    oauth_cfg: oauth_cfg.clone(),
                    clock: clock.clone(),
                },
                identity.id,
                subscription_metadata_hook.clone(),
                refresh_cancel.clone(),
                claim_guard,
                scheduler_lazy_handle.clone(),
            )),
            None => Arc::new(LazyRefresher::new(crate::refresh::LazyRefresherParams {
                deps: crate::refresh::LazyRefresherDeps {
                    stores: stores.clone(),
                    aead: aead.clone(),
                    oauth_cfg: oauth_cfg.clone(),
                    clock: clock.clone(),
                },
                replica_id: identity.id,
                metadata_hook: subscription_metadata_hook.clone(),
                cancel: refresh_cancel.clone(),
                apalis_handle: scheduler_lazy_handle.clone(),
            })),
        });
    let lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>> =
        lazy_refresher_concrete.as_ref().map(|refresher| {
            refresher.clone() as Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>
        });
    let initial_view = build_dynamic_view(
        &stores,
        aead.clone(),
        lazy_refresher.clone(),
        0,
        &runtime,
        &data_dir,
        subscription_quota_cache.clone(),
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink.clone(),
        config.subscription_quota.routing_max_staleness_secs,
        clock.clone(),
    )
    .await?;
    let dynamic_view_holder = Arc::new(DynamicViewHolder::new(initial_view.clone()));
    let notify_cancel = CancellationToken::new();
    let notifier: Arc<dyn RuntimeChangeNotifier> = storage_for_dynamic.clone();
    let notify_listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier: Arc::clone(&notifier),
        cancel: notify_cancel.clone(),
        holder: Arc::clone(&dynamic_view_holder),
        stores: Arc::clone(&stores),
        runtime: Arc::clone(&runtime),
        aead: aead.clone(),
        data_dir: data_dir.clone(),
        lazy_refresher: lazy_refresher.clone(),
        subscription_quota_cache: subscription_quota_cache.clone(),
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink: prompt_cache_observation_sink.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        clock: clock.clone(),
    }));
    let notify_rx = notify_listener.subscribe_with_retry().await;
    let notifier_task = {
        let notifier = Arc::clone(&notifier);
        let cancel = notify_cancel.clone();
        Some(tokio::spawn(async move {
            if let Err(error) = notifier.run(cancel).await {
                tracing::error!(error = %error, "runtime change notifier task failed");
            }
        }))
    };
    let notify_listener_task = notify_rx.map(|rx| {
        tokio::spawn(async move {
            notify_listener.run_subscribed(rx).await;
        })
    });
    let in_memory_bus =
        cc_lb_control::InMemoryBus::with_capacity(config.event_bus.broadcast_capacity);
    let lifecycle_event_logger_rx = in_memory_bus
        .attach_lifecycle_writer(cc_lb_control::event_bus::DEFAULT_LIFECYCLE_WRITER_CAPACITY);
    let lifecycle_assembler_rx = in_memory_bus
        .attach_lifecycle_assembler(cc_lb_control::event_bus::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY);
    let lifecycle_pricing_rx = in_memory_bus
        .attach_lifecycle_pricing(cc_lb_control::event_bus::DEFAULT_LIFECYCLE_PRICING_CAPACITY);
    let lifecycle_limit_reconcile_rx = in_memory_bus.attach_lifecycle_limit_reconcile(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_LIMIT_RECONCILE_CAPACITY,
    );
    let lifecycle_cache_obs_rx = in_memory_bus.attach_lifecycle_cache_observation(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_CACHE_OBS_CAPACITY,
    );
    let lifecycle_rate_limit_header_rx = in_memory_bus.attach_lifecycle_rate_limit_header(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_RATE_LIMIT_HEADER_CAPACITY,
    );
    let lifecycle_subscription_quota_rx = in_memory_bus.attach_lifecycle_subscription_quota(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_SUBSCRIPTION_QUOTA_CAPACITY,
    );
    let lifecycle_limit_rejection_audit_rx = in_memory_bus.attach_lifecycle_limit_rejection_audit(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_LIMIT_REJECTION_AUDIT_CAPACITY,
    );
    let lifecycle_api_key_metrics_rx = in_memory_bus.attach_lifecycle_api_key_metrics(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_API_KEY_METRICS_CAPACITY,
    );
    let lifecycle_cache_hit_miss_rx = in_memory_bus.attach_lifecycle_cache_hit_miss(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_CACHE_HIT_MISS_CAPACITY,
    );
    let lifecycle_routing_tier_rx = in_memory_bus.attach_lifecycle_routing_tier(
        cc_lb_control::event_bus::DEFAULT_LIFECYCLE_ROUTING_TIER_CAPACITY,
    );
    let (event_fanout_shutdown_tx, event_fanout_shutdown_rx) = watch::channel(false);
    let mut event_fanout_tasks = Vec::new();
    let mut internal_partials_state = None;
    // Shared with AdminState so the SSE handler subscribes to the same broadcast
    // the StorageTailPoller feeds. Kept live for the InMemory transport too —
    // AdminState holds one Sender clone so the channel never closes, and no
    // poller produces on it (finals arrive via the local bus in that mode).
    let storage_tail_tx: tokio::sync::broadcast::Sender<cc_lb_request_log::StorageTailUpdate> =
        tokio::sync::broadcast::channel(cc_lb_admin::events::DEFAULT_STORAGE_TAIL_CAPACITY).0;
    #[cfg(not(feature = "postgres"))]
    {
        let _ = &event_fanout_shutdown_rx;
        let _ = &mut event_fanout_tasks;
        let _ = &mut internal_partials_state;
    }
    let event_bus: Arc<dyn cc_lb_control::RequestEventBus> = match &config.storage {
        cc_lb_config::StorageConfig::Sqlite { .. } => Arc::new(in_memory_bus.clone()),
        cc_lb_config::StorageConfig::Postgres { .. } => {
            #[cfg(not(feature = "postgres"))]
            {
                return Err(BuildError::StorageFactory(
                    crate::storage_factory::StorageFactoryError::FeatureDisabled {
                        backend: "postgres".to_owned(),
                    },
                ));
            }
            #[cfg(feature = "postgres")]
            {
                let pg_pool = storage_factory::open_pg_fanout_pool(&config.storage)
                    .await?
                    .ok_or(BuildError::PgNotifyPoolUnavailable)?;
                let cluster_token = load_cluster_token(&config)?;
                let retention = cc_lb_engine::PartialRetentionCache::new(
                    Duration::from_secs(config.event_bus.partial_retention_ttl_secs.max(1)),
                    config.event_bus.partial_retention_max_entries,
                );
                let (notify_tx, notify_rx) =
                    tokio::sync::mpsc::channel(cc_lb_engine::PARTIAL_NOTIFY_MPSC_CAPACITY);
                let instance_url = config
                    .cluster
                    .instance_url
                    .clone()
                    .ok_or(BuildError::ClusterInstanceUrlMissing)?;

                event_fanout_tasks.push(cc_lb_engine::PgNotifier::spawn_with_channel(
                    pg_pool.clone(),
                    notify_rx,
                    retention.clone(),
                    instance_url.clone(),
                    config.event_bus.pg_notify_channel.clone(),
                    event_fanout_shutdown_rx.clone(),
                ));

                let listener_bus: Arc<dyn cc_lb_control::RequestEventBus> =
                    Arc::new(in_memory_bus.clone());
                let http_client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(3))
                    .build()
                    .map_err(|error| io::Error::other(error.to_string()))?;
                event_fanout_tasks.push(cc_lb_engine::PgListener::spawn_with_channel(
                    pg_pool,
                    listener_bus,
                    http_client,
                    secrecy::SecretString::from(cluster_token.clone()),
                    config.event_bus.pg_notify_channel.clone(),
                    instance_url,
                    event_fanout_shutdown_rx.clone(),
                ));

                let initial_storage_tail_cursor = storage.current_request_event_cursor().await?;
                event_fanout_tasks.push(cc_lb_engine::StorageTailPoller::spawn(
                    storage.clone(),
                    storage_tail_tx.clone(),
                    Duration::from_millis(config.event_bus.storage_tail_poll_interval_ms.max(1)),
                    initial_storage_tail_cursor,
                    event_fanout_shutdown_rx.clone(),
                ));

                let retained_partial_port: Arc<dyn cc_lb_admin::ports::RetainedPartialPort> =
                    Arc::new(ServerRetainedPartialPort::new(retention.clone()));
                internal_partials_state =
                    Some(cc_lb_admin::internal_partials::InternalPartialsState {
                        retention: retained_partial_port,
                        cluster_token,
                    });

                Arc::new(cc_lb_engine::PgNotifyFanout::new(
                    in_memory_bus.clone(),
                    notify_tx,
                ))
            }
        }
    };
    let lifecycle_event_logger_handle =
        cc_lb_engine::spawn_lifecycle_event_logger(lifecycle_event_logger_rx);
    let lifecycle_event_assembler_handle = cc_lb_engine::spawn_request_event_assembler(
        lifecycle_assembler_rx,
        storage.clone(),
        Some(event_bus.clone()),
        Arc::clone(&metrics_hook),
    );
    let lifecycle_pricing_subscriber_handle =
        cc_lb_pricing::spawn_lifecycle_pricing_subscriber(lifecycle_pricing_rx, event_bus.clone());
    let lifecycle_limit_reconcile_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(
            lifecycle_limit_reconcile_rx,
            limit_engine.clone(),
        );
    let lifecycle_cache_observation_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_cache_observation_subscriber(
            lifecycle_cache_obs_rx,
            event_bus.clone(),
        );
    let lifecycle_rate_limit_header_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_rate_limit_header_subscriber(
            lifecycle_rate_limit_header_rx,
            Arc::clone(&initial_view.upstream_rate_limit_cache),
            Some(upstream_rate_limit_sink.clone()),
        );
    let cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike> =
        subscription_quota_cache.clone();
    let lifecycle_subscription_quota_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_subscription_quota_subscriber(
            lifecycle_subscription_quota_rx,
            Some(cache),
            Some(subscription_quota_sink.clone()),
        );
    let lifecycle_limit_rejection_audit_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_limit_rejection_audit_subscriber(
            lifecycle_limit_rejection_audit_rx,
            audit_sink.clone(),
        );
    let lifecycle_api_key_metrics_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_api_key_metrics_subscriber(
            lifecycle_api_key_metrics_rx,
            event_bus.clone(),
        );
    let lifecycle_cache_hit_miss_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_cache_hit_miss_subscriber(
            lifecycle_cache_hit_miss_rx,
            Arc::clone(&metrics_hook),
        );
    let lifecycle_routing_tier_subscriber_handle =
        cc_lb_engine::spawn_lifecycle_routing_tier_subscriber(
            lifecycle_routing_tier_rx,
            Arc::clone(&metrics_hook),
        );
    let lifecycle_event_logger_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::LifecycleEventLoggerHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(lifecycle_event_logger_handle)));
    let lifecycle_event_assembler_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::RequestEventAssemblerHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_event_assembler_handle,
    )));
    let lifecycle_pricing_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_pricing::PricingSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_pricing_subscriber_handle,
    )));
    let lifecycle_limit_reconcile_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::LimitReconcileSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_limit_reconcile_subscriber_handle,
    )));
    let lifecycle_cache_observation_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::CacheObservationSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_cache_observation_subscriber_handle,
    )));
    let lifecycle_rate_limit_header_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::RateLimitHeaderSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_rate_limit_header_subscriber_handle,
    )));
    let lifecycle_subscription_quota_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::SubscriptionQuotaSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_subscription_quota_subscriber_handle,
    )));
    let lifecycle_limit_rejection_audit_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::LimitRejectionAuditSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_limit_rejection_audit_subscriber_handle,
    )));
    let lifecycle_api_key_metrics_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::ApiKeyMetricsSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_api_key_metrics_subscriber_handle,
    )));
    let lifecycle_cache_hit_miss_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::CacheHitMissSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_cache_hit_miss_subscriber_handle,
    )));
    let lifecycle_routing_tier_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_engine::RoutingTierSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_routing_tier_subscriber_handle,
    )));
    let keepalive_dispatcher = Arc::new(AnthropicKeepaliveDispatcher::new(
        Arc::clone(&dynamic_view_holder),
        Arc::clone(&stores.upstreams),
        Arc::clone(&dispatcher),
    ));
    let cache_keepalive_enqueuer = Arc::new(
        crate::cache_keepalive_enqueuer::ServerCacheKeepaliveEnqueuer::new(
            crate::cache_keepalive_enqueuer::ServerCacheKeepaliveEnqueuerDeps {
                storage: Arc::clone(&storage),
                pusher: Arc::new(opened_scheduler.backend.clone()),
                aead: Arc::clone(&aead),
                clock: clock.clone(),
            },
        ),
    );
    let mut lifecycle = Lifecycle::new_with_dynamic_view(
        builtin_authn.clone(),
        dynamic_view_holder.clone(),
        dispatcher,
        lifecycle_config,
        clock.clone(),
    );
    lifecycle = lifecycle.with_upstream_affinity_store(
        storage.clone() as Arc<dyn cc_lb_storage_api::UpstreamAffinityStore>
    );
    lifecycle = lifecycle.with_cache_keepalive_enqueuer(cache_keepalive_enqueuer);
    lifecycle = lifecycle.with_limit_engine(limit_engine.clone(), builtin_authn.clone());
    lifecycle = lifecycle.with_limit_cost_estimator(Arc::new(PricingLimitCostEstimator {
        catalog: price_catalog.clone(),
    }));
    lifecycle = lifecycle.with_event_bus(event_bus.clone());
    lifecycle = lifecycle.with_subscription_quota_sink(subscription_quota_sink.clone());
    if let Some(subscription_metadata_hook) = subscription_metadata_hook.clone() {
        lifecycle = lifecycle.with_subscription_metadata_hook(subscription_metadata_hook);
    }
    lifecycle = lifecycle.with_subscription_quota_cache(subscription_quota_cache.clone());
    let lifecycle = Arc::new(lifecycle);
    let dynamic_view = lifecycle.dynamic_view();

    let start_time = std::time::Instant::now();
    let drain_controller = DrainController::new();
    let tls_state = build_tls_state(&config)?;
    let reload_tls_state = match active_tls_config(&config) {
        Some(tls) if tls.reload_on_sighup => tls_state.clone(),
        _ => None,
    };
    let admin_rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(ServerDynamicViewRebinder {
        stores: stores.clone(),
        runtime: runtime.clone(),
        aead: aead.clone(),
        lazy_refresher: lazy_refresher.clone(),
        data_dir: data_dir.clone(),
        subscription_quota_cache: subscription_quota_cache.clone(),
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink: prompt_cache_observation_sink.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        clock: clock.clone(),
    });
    let signals = signal::install(
        drain_controller.clone(),
        Duration::from_secs(config.timeouts.drain_secs),
        sighup_handler(reload_tls_state),
    );
    {
        let logger_slot = lifecycle_event_logger_slot.clone();
        signals.add_shutdown_hook(move || {
            let logger_slot = logger_slot.clone();
            async move {
                let mut guard = logger_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let assembler_slot = lifecycle_event_assembler_slot.clone();
        signals.add_shutdown_hook(move || {
            let assembler_slot = assembler_slot.clone();
            async move {
                let mut guard = assembler_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let pricing_slot = lifecycle_pricing_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let pricing_slot = pricing_slot.clone();
            async move {
                let mut guard = pricing_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let ttl_slot = limit_reservation_ttl_slot.clone();
        signals.add_shutdown_hook(move || {
            let ttl_slot = ttl_slot.clone();
            async move {
                let mut guard = ttl_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let reconcile_slot = lifecycle_limit_reconcile_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let reconcile_slot = reconcile_slot.clone();
            async move {
                let mut guard = reconcile_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let cache_obs_slot = lifecycle_cache_observation_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let cache_obs_slot = cache_obs_slot.clone();
            async move {
                let mut guard = cache_obs_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let rate_limit_header_slot = lifecycle_rate_limit_header_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let rate_limit_header_slot = rate_limit_header_slot.clone();
            async move {
                let mut guard = rate_limit_header_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let subscription_quota_slot = lifecycle_subscription_quota_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let subscription_quota_slot = subscription_quota_slot.clone();
            async move {
                let mut guard = subscription_quota_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let limit_rejection_audit_slot = lifecycle_limit_rejection_audit_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let limit_rejection_audit_slot = limit_rejection_audit_slot.clone();
            async move {
                let mut guard = limit_rejection_audit_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let api_key_metrics_slot = lifecycle_api_key_metrics_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let api_key_metrics_slot = api_key_metrics_slot.clone();
            async move {
                let mut guard = api_key_metrics_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let cache_hit_miss_slot = lifecycle_cache_hit_miss_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let cache_hit_miss_slot = cache_hit_miss_slot.clone();
            async move {
                let mut guard = cache_hit_miss_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let routing_tier_slot = lifecycle_routing_tier_subscriber_slot.clone();
        signals.add_shutdown_hook(move || {
            let routing_tier_slot = routing_tier_slot.clone();
            async move {
                let mut guard = routing_tier_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    {
        let api_key_usage_slot = api_key_usage_slot.clone();
        signals.add_shutdown_hook(move || {
            let api_key_usage_slot = api_key_usage_slot.clone();
            async move {
                let mut guard = api_key_usage_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
    let scheduler_cancel = CancellationToken::new();
    let scheduler_ctx = crate::scheduler_dispatch::build_scheduler_ctx(
        crate::scheduler_dispatch::SchedulerDispatchDeps {
            backend: opened_scheduler.backend.clone(),
            cache_keepalive_pusher: Arc::new(opened_scheduler.backend.clone()),
            config: config.clone(),
            storage: storage.clone(),
            stores: stores.clone(),
            aead: aead.clone(),
            oauth_cfg: oauth_cfg.clone(),
            runtime: runtime.clone(),
            data_dir: data_dir.clone(),
            lazy_refresher: lazy_refresher_concrete.clone(),
            subscription_quota_sink: subscription_quota_sink.clone(),
            subscription_quota_cache: subscription_quota_cache.clone(),
            cancel: scheduler_cancel.clone(),
            replica_id: scheduler_replica_id,
            price_catalog: price_catalog.clone(),
            key_store: key_store.clone(),
            limit_engine: limit_engine.clone(),
            dynamic_view: dynamic_view_holder.clone(),
            keepalive_dispatcher,
            event_bus: event_bus.clone(),
            clock: clock.clone(),
        },
    );
    let scheduler_tasks = opened_scheduler
        .spawn(scheduler_ctx, scheduler_cancel.clone())
        .await?;
    spawn_reconcile_shutdown(signals.subscribe(), scheduler_cancel.clone());
    spawn_reconcile_shutdown(signals.subscribe(), subscription_quota_writer_cancel);
    spawn_reconcile_shutdown(signals.subscribe(), refresh_cancel);
    let reconcile_cancel = CancellationToken::new();
    spawn_reconciler(ReconcilerParams {
        stores: stores.clone(),
        holder: dynamic_view.clone(),
        runtime: runtime.clone(),
        aead: aead.clone(),
        lazy_refresher: lazy_refresher.clone(),
        data_dir: data_dir.clone(),
        cancel: reconcile_cancel.clone(),
        prompt_cache_grace_margin_secs,
        subscription_quota_cache: subscription_quota_cache.clone(),
        prompt_cache_observation_sink: prompt_cache_observation_sink.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        clock: clock.clone(),
    });
    spawn_reconcile_shutdown(signals.subscribe(), reconcile_cancel);
    let state = ProxyState {
        lifecycle: lifecycle.clone(),
        body_caps,
        server_state: server_state.clone(),
        start_time,
        drain_controller: drain_controller.clone(),
        aead: aead.clone(),
        storage: storage.clone(),
        dynamic_view: dynamic_view.clone(),
        builtin_authn: builtin_authn.clone(),
        clock: clock.clone(),
    };

    let admin_config = Arc::new(config.clone());
    let admin_state = AdminState {
        storage: Some(storage.clone()),
        key_store: Some(key_store),
        aead: aead.clone(),
        limit_engine: limit_engine.clone(),
        lifecycle: Some(AdminPorts {
            route_preview: Some(Arc::new(ServerRoutePreviewPort::new(lifecycle.clone()))),
            warmup: Some(Arc::new(ServerWarmupPort)),
            subscription_quota_ingestion: Some(Arc::new(
                ServerSubscriptionQuotaIngestionPort::new(lifecycle.clone()),
            )),
            replica_identity,
        }),
        lazy_refresher: lazy_refresher.clone(),
        runtime: Some(runtime.clone()),
        config_path: config_path.map(Path::to_path_buf),
        startup_config_overrides,
        data_dir: Some(data_dir.clone()),
        warmup_dialect_dispatcher: lazy_refresher_concrete.clone().map(|lazy_refresher| {
            Arc::new(ServerWarmupDialectDispatcher {
                stores: stores.clone(),
                aead: aead.clone(),
                lazy_refresher,
                clock: clock.clone(),
            }) as Arc<dyn WarmupDialectDispatcher>
        }),
        dynamic_view: dynamic_view.clone(),
        config: admin_config,
        dynamic_view_rebinder: Some(admin_rebinder),
        scheduler: Some(cc_lb_scheduler::admin::SchedulerAdminHandle::new(
            scheduler_lazy_handle.clone(),
        )),
        admin_auth: Arc::new(AdminAuthenticator::new(
            build_providers(&config.admin.auth).map_err(|error| BuildError::AdminAuth {
                message: error.to_string(),
            })?,
        )),
        start_time,
        event_bus: Some(event_bus.clone()),
        storage_tail: storage_tail_tx,
        clock: clock.clone(),
    };

    server_state.transition_to_ready();

    Ok(App {
        router: app_router(state, config.timeouts.upstream_total_secs),
        admin_router: admin_router(admin_state, server_state.clone(), internal_partials_state),
        proxy_addr: config.listener.proxy_addr,
        admin_addr: config.listener.admin_addr,
        lifecycle,
        notify_cancel: Some(notify_cancel),
        notifier_task,
        notify_listener_task,
        price_catalog_install_cancel: Some(price_catalog_install_cancel),
        price_catalog_install_task: Some(price_catalog_install_task),
        wasmtime_pool_metrics_cancel: Some(wasmtime_pool_metrics_cancel),
        wasmtime_pool_metrics_task: Some(wasmtime_pool_metrics_task),
        scheduler_cancel: Some(scheduler_cancel),
        scheduler_tasks,
        audit_writer_task: Some(audit_writer_task),
        upstream_rate_limit_writer_task: Some(upstream_rate_limit_writer_task),
        subscription_quota_writer_task: Some(subscription_quota_writer_task),
        prompt_cache_observation_writer_task,
        event_fanout_shutdown_tx: Some(event_fanout_shutdown_tx),
        event_fanout_tasks,
        signals,
        drain_controller,
        tls_state,
        server_state,
    })
}

fn spawn_wasmtime_pool_metrics_publisher(
    runtime: Arc<WasmtimeRuntime>,
) -> (CancellationToken, JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        runtime.publish_pool_metrics();
        let mut interval = tokio::time::interval(WASMTIME_POOL_METRICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => return,
                _ = interval.tick() => runtime.publish_pool_metrics(),
            }
        }
    });
    (cancel, task)
}

fn hot_engine_config_from_config(config: &Config) -> cc_lb_runtime_wasmtime::HotEngineConfig {
    let defaults = cc_lb_runtime_wasmtime::HotEngineConfig::default();
    let wasmtime = &config.runtime.wasmtime;
    cc_lb_runtime_wasmtime::HotEngineConfig {
        allocation_strategy: match wasmtime.allocation_strategy {
            cc_lb_config::WasmtimeAllocationStrategy::OnDemand => {
                cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::OnDemand
            }
            cc_lb_config::WasmtimeAllocationStrategy::Pooling => {
                cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::Pooling
            }
        },
        memory_max_pages: wasmtime
            .memory_max_pages
            .unwrap_or(defaults.memory_max_pages),
        memory_reservation_bytes: wasmtime
            .memory_reservation_bytes
            .unwrap_or(defaults.memory_reservation_bytes),
        memory_guard_bytes: wasmtime
            .memory_guard_bytes
            .unwrap_or(defaults.memory_guard_bytes),
        pool_total_memories: wasmtime
            .pool_total_memories
            .unwrap_or(defaults.pool_total_memories),
        pool_total_core_instances: wasmtime
            .pool_total_core_instances
            .unwrap_or(defaults.pool_total_core_instances),
        ..defaults
    }
}

struct ServerWarmupDialectDispatcher {
    stores: Arc<DynamicStores>,
    aead: Arc<AeadService>,
    lazy_refresher: Arc<LazyRefresher>,
    clock: ClockHandle,
}

struct ServerMetadataRefreshEnqueue {
    scheduler_backend: crate::scheduler_factory::SchedulerBackend,
}

#[async_trait]
impl cc_lb_control::MetadataRefreshEnqueue for ServerMetadataRefreshEnqueue {
    async fn push_metadata_refresh(
        &self,
        request: cc_lb_control::MetadataHookRequest,
    ) -> Result<(), cc_lb_control::MetadataHookEnqueueError> {
        let job = cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob {
            upstream_id: request.upstream_id,
            credential_generation: request.credential_generation,
            traceparent: request.traceparent,
        };
        let idempotency_key = job.idempotency_key();
        let task = cc_lb_scheduler::worker::SchedulerPushTask {
            args: cc_lb_scheduler::worker::AdaptiveJob::MetadataRefresh(job),
            idempotency_key: Some(idempotency_key),
            run_at_unix_secs: None,
            max_attempts: None,
        };
        match self.scheduler_backend.push_adaptive_task(task).await {
            Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => Ok(()),
            Err(error) => Err(cc_lb_control::MetadataHookEnqueueError::Enqueue(
                error.to_string(),
            )),
        }
    }
}

#[async_trait]
impl WarmupDialectDispatcher for ServerWarmupDialectDispatcher {
    async fn dispatch_warmup_with_dialect(
        &self,
        runtime: &Arc<WasmtimeRuntime>,
        data_dir: &Path,
        upstream: &cc_lb_storage_api::UpstreamRecord,
    ) -> Result<WarmupDialectDispatchOutcome, WarmupDialectDispatchError> {
        let http = warmup_dialect_http_client();
        let outcome = crate::warmup::dialect::dispatch_warmup_with_dialect(
            crate::warmup::dialect::WarmupDialectDispatchParams {
                runtime,
                stores: self.stores.as_ref(),
                data_dir,
                aead: self.aead.clone(),
                lazy_refresher: self.lazy_refresher.clone(),
                upstream,
                http: &http,
                clock: self.clock.clone(),
            },
        )
        .await
        .map_err(map_warmup_dialect_error)?;

        Ok(WarmupDialectDispatchOutcome {
            status: outcome.status,
            headers: outcome.headers,
        })
    }
}

fn warmup_dialect_http_client() -> crate::warmup::request::WarmupHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    hyper_util::client::legacy::Client::builder(TokioExecutor::new()).build(connector)
}

fn map_warmup_dialect_error(
    error: crate::warmup::dialect::WarmupDispatchError,
) -> WarmupDialectDispatchError {
    let kind = match &error {
        crate::warmup::dialect::WarmupDispatchError::Storage(_)
        | crate::warmup::dialect::WarmupDispatchError::Http(_) => {
            WarmupDialectDispatchErrorKind::Transient
        }
        crate::warmup::dialect::WarmupDispatchError::MissingPlugin
        | crate::warmup::dialect::WarmupDispatchError::RegistryNotFound(_)
        | crate::warmup::dialect::WarmupDispatchError::RegistryUnsupportedSlot { .. }
        | crate::warmup::dialect::WarmupDispatchError::Materialize(_)
        | crate::warmup::dialect::WarmupDispatchError::Register(_)
        | crate::warmup::dialect::WarmupDispatchError::BodySerialize(_)
        | crate::warmup::dialect::WarmupDispatchError::Shape(_)
        | crate::warmup::dialect::WarmupDispatchError::Signer(_)
        | crate::warmup::dialect::WarmupDispatchError::RequestBuild(_) => {
            WarmupDialectDispatchErrorKind::Permanent
        }
    };
    WarmupDialectDispatchError::new(kind, error.to_string())
}

#[allow(clippy::too_many_arguments)]
struct ServerDynamicViewRebinder {
    stores: Arc<DynamicStores>,
    runtime: Arc<WasmtimeRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    data_dir: PathBuf,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_grace_margin_secs: u64,
    prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: ClockHandle,
}

#[async_trait]
impl DynamicViewRebinder for ServerDynamicViewRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        Ok(build_dynamic_view(
            &self.stores,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            current_generation,
            &self.runtime,
            &self.data_dir,
            self.subscription_quota_cache.clone(),
            self.prompt_cache_grace_margin_secs,
            self.prompt_cache_observation_sink.clone(),
            self.subscription_quota_routing_max_staleness_secs,
            self.clock.clone(),
        )
        .await?)
    }
}

struct ReconcilerParams {
    stores: Arc<DynamicStores>,
    holder: Arc<DynamicViewHolder>,
    runtime: Arc<WasmtimeRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    cancel: CancellationToken,
    data_dir: PathBuf,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_grace_margin_secs: u64,
    prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: ClockHandle,
}

fn spawn_reconciler(params: ReconcilerParams) {
    let reconciler = Arc::new(Reconciler::new(
        params.stores,
        params.holder,
        params.runtime,
        params.aead,
        params.lazy_refresher,
        params.cancel,
        params.data_dir,
        params.subscription_quota_cache,
        params.prompt_cache_grace_margin_secs,
        params.prompt_cache_observation_sink,
        params.subscription_quota_routing_max_staleness_secs,
        params.clock,
    ));
    tokio::spawn(reconciler.run());
}

fn spawn_reconcile_shutdown(shutdown: watch::Receiver<bool>, cancel: CancellationToken) {
    tokio::spawn(async move {
        signal::wait_for_shutdown(shutdown).await;
        cancel.cancel();
    });
}
fn lifecycle_config_from_config(
    config: &Config,
    body_caps: RequestBodyCaps,
    replica_identity: Option<cc_lb_domain::ReplicaIdentity>,
) -> LifecycleConfig {
    LifecycleConfig {
        messages_body_cap_bytes: body_caps.messages,
        files_body_cap_bytes: body_caps.files,
        replica_identity,
        prompt_cache_shadow: config.prompt_cache_shadow.clone(),
        upstream_affinity_ttl: std::time::Duration::from_secs(config.upstream_affinity.ttl_secs()),
    }
}

fn cap_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn build_tls_state(config: &Config) -> Result<Option<Arc<TlsState>>, BuildError> {
    let Some(tls_config) = active_tls_config(config) else {
        return Ok(None);
    };
    let Some(cert_path) = tls_config.cert_path.as_ref() else {
        return Ok(None);
    };
    let Some(key_path) = tls_config.key_path.as_ref() else {
        return Ok(None);
    };
    Ok(Some(Arc::new(TlsState::from_paths(cert_path, key_path)?)))
}

fn active_tls_config(config: &Config) -> Option<&TlsConfig> {
    config.listener.tls.as_ref()
}

fn sighup_handler(tls_state: Option<Arc<TlsState>>) -> Option<signal::SighupHandler> {
    tls_state.map(|tls_state| {
        Arc::new(move || {
            if let Err(error) = tls_state.reload() {
                tracing::warn!(error = %error, "TLS reload failed");
            }
        }) as signal::SighupHandler
    })
}

#[cfg(feature = "postgres")]
fn validate_cluster_token_before_storage(config: &Config) -> Result<(), BuildError> {
    if matches!(config.storage, cc_lb_config::StorageConfig::Postgres { .. }) {
        load_cluster_token(config)?;
    }
    Ok(())
}

#[cfg(not(feature = "postgres"))]
fn validate_cluster_token_before_storage(_config: &Config) -> Result<(), BuildError> {
    Ok(())
}

#[cfg(feature = "postgres")]
fn load_cluster_token(config: &Config) -> Result<String, BuildError> {
    std::env::var(&config.cluster.token_env)
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| BuildError::ClusterTokenMissing {
            env: config.cluster.token_env.clone(),
        })
}

#[derive(Clone)]
struct ProxyState {
    lifecycle: Arc<Lifecycle>,
    body_caps: RequestBodyCaps,
    server_state: Arc<ServerStateHandle>,
    start_time: std::time::Instant,
    drain_controller: DrainController,
    aead: Arc<AeadService>,
    storage: Arc<dyn Storage>,
    dynamic_view: Arc<DynamicViewHolder>,
    builtin_authn: Arc<BuiltinAuthn>,
    clock: ClockHandle,
}

fn spawn_price_catalog_local_installer(
    config: &Config,
    storage: Arc<dyn Storage>,
    price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
    clock: ClockHandle,
) -> (CancellationToken, JoinHandle<()>) {
    install_default_fallback_if_uninitialized(&price_catalog, &*clock);

    let cfg = &config.price_catalog;
    let storage_cache: Arc<dyn cc_lb_storage_api::PriceCatalogCache> = storage;
    let loader = cc_lb_pricing::LiteLlmLoader::new(
        price_catalog,
        storage_cache,
        cfg.url.clone(),
        cfg.cache_path.clone(),
        clock.clone(),
    );
    tracing::info!(
        local_install_interval_secs = PRICE_CATALOG_LOCAL_INSTALL_INTERVAL.as_secs(),
        cache_path = %cfg.cache_path.display(),
        "starting LiteLLM price catalog local installer",
    );
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        run_price_catalog_local_install(&loader).await;
        let mut interval = tokio::time::interval(PRICE_CATALOG_LOCAL_INSTALL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            run_price_catalog_local_install(&loader).await;
        }
    });
    (cancel, task)
}

async fn run_price_catalog_local_install(loader: &cc_lb_pricing::LiteLlmLoader) {
    match loader.install_latest_local().await {
        Ok(true) => tracing::info!("installed latest local LiteLLM price catalog"),
        Ok(false) => tracing::debug!("LiteLLM price catalog local snapshot already current"),
        Err(error) => tracing::warn!(error = %error, "LiteLLM price catalog local install failed"),
    }
}

fn install_default_fallback_if_uninitialized(
    price_catalog: &cc_lb_pricing::PriceCatalog,
    clock: &dyn cc_lb_clock::Clock,
) {
    if !matches!(price_catalog.status(), cc_lb_pricing::CatalogStatus::Ok) {
        price_catalog.install_snapshot(claude_default_snapshot(clock));
    }
}

fn claude_default_snapshot(clock: &dyn cc_lb_clock::Clock) -> cc_lb_pricing::CatalogSnapshot {
    use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
    use std::collections::HashMap;

    // Per-million USD in micros (1 USD = 1_000_000 micros).
    // Source: Anthropic pricing (July 2026 snapshot).
    #[allow(clippy::type_complexity)]
    let raw: &[(&str, u64, u64, u64, u64)] = &[
        // model name, input, output, cache_creation_5m, cache_read
        (
            "claude-fable-5",
            10_000_000,
            50_000_000,
            12_500_000,
            1_000_000,
        ),
        (
            "claude-mythos-5",
            10_000_000,
            50_000_000,
            12_500_000,
            1_000_000,
        ),
        ("claude-opus-5", 5_000_000, 25_000_000, 6_250_000, 500_000),
        ("claude-opus-4-8", 5_000_000, 25_000_000, 6_250_000, 500_000),
        ("claude-opus-4-7", 5_000_000, 25_000_000, 6_250_000, 500_000),
        ("claude-opus-4-6", 5_000_000, 25_000_000, 6_250_000, 500_000),
        // Pre-change, both Opus 4.5 fallbacks were $15/$75 and overpriced every
        // component by 3x.
        ("claude-opus-4-5", 5_000_000, 25_000_000, 6_250_000, 500_000),
        (
            "claude-opus-4-5-20251101",
            5_000_000,
            25_000_000,
            6_250_000,
            500_000,
        ),
        ("claude-sonnet-5", 3_000_000, 15_000_000, 3_750_000, 300_000),
        (
            "claude-sonnet-4-6",
            3_000_000,
            15_000_000,
            3_750_000,
            300_000,
        ),
        (
            "claude-sonnet-4-5",
            3_000_000,
            15_000_000,
            3_750_000,
            300_000,
        ),
        (
            "claude-sonnet-4-5-20250929",
            3_000_000,
            15_000_000,
            3_750_000,
            300_000,
        ),
        ("claude-haiku-4-5", 1_000_000, 5_000_000, 1_250_000, 100_000),
        (
            "claude-haiku-4-5-20251001",
            1_000_000,
            5_000_000,
            1_250_000,
            100_000,
        ),
    ];

    let mut models = HashMap::new();
    let mut cache_creation = HashMap::new();
    let mut cache_read = HashMap::new();
    for (name, input, output, creation, read) in raw {
        models.insert(
            (*name).to_owned(),
            Pricing {
                model: (*name).to_owned(),
                input_per_million_usd: UsdPerMillion::from_micros_usd(*input),
                output_per_million_usd: UsdPerMillion::from_micros_usd(*output),
                by_tier: Default::default(),
            },
        );
        cache_creation.insert(
            (*name).to_owned(),
            UsdPerMillion::from_micros_usd(*creation),
        );
        cache_read.insert((*name).to_owned(), UsdPerMillion::from_micros_usd(*read));
    }

    CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: cc_lb_clock::unix_millis(clock.now())
            .try_into()
            .unwrap_or(u64::MAX),
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: cache_creation,
        cache_read_per_million_usd: cache_read,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    }
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

#[derive(Clone, Serialize)]
struct AdminServerStateBody {
    state: ServerState,
}

fn admin_router(
    admin_state: AdminState,
    server_state: Arc<ServerStateHandle>,
    internal_partials_state: Option<cc_lb_admin::internal_partials::InternalPartialsState>,
) -> Router {
    let mut admin_router =
        cc_lb_admin::router(admin_state).merge(server_state_router(server_state));
    if let Some(state) = internal_partials_state {
        admin_router = admin_router.merge(cc_lb_admin::internal_partials::router(state));
    }
    let admin_router = cc_lb_admin::routes::with_request_tracing(admin_router)
        // Admin surface only — proxy_router stays uncompressed to keep SSE
        // bodies streaming and skip CPU on the hot data plane. ETagged
        // static assets skip dynamic compression to keep strong ETags valid.
        // Defaults (gzip 6, brotli 4) are deliberate; avoid Best/level 11.
        .layer(crate::admin_compression::layer())
        // Reuse the existing request-id seam on the separate admin listener so
        // admin spans and responses carry the same correlation identifier.
        .layer(middleware::from_fn_with_state(
            RequestIdState::with_prefix("req_admin_"),
            request_id_middleware,
        ));

    crate::admin_security::with_browser_security_headers(admin_router)
}

fn server_state_router(server_state: Arc<ServerStateHandle>) -> Router {
    Router::new()
        .route("/admin/health/state", get(admin_server_state))
        .with_state(server_state)
}

async fn admin_server_state(
    State(server_state): State<Arc<ServerStateHandle>>,
) -> Json<AdminServerStateBody> {
    Json(AdminServerStateBody {
        state: server_state.current(),
    })
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

fn proxy_route_template(path: &str) -> Option<&'static str> {
    match path {
        "/v1/messages" => Some("/v1/messages"),
        "/v1/messages/count_tokens" => Some("/v1/messages/count_tokens"),
        "/v1/models" => Some("/v1/models"),
        "/v1/files" => Some("/v1/files"),
        _ if path.starts_with("/v1/models/") => Some("/v1/models/{id}"),
        _ if path.starts_with("/v1/files/") && path.ends_with("/content") => {
            Some("/v1/files/{id}/content")
        }
        _ if path.starts_with("/v1/files/") => Some("/v1/files/{id}"),
        "/api/oauth/usage" => Some("/api/oauth/usage"),
        _ if path.starts_with("/api/") => Some("/api/{*path}"),
        _ if path.starts_with("/v1/") => Some("/v1/{*path}"),
        _ => None,
    }
}

fn proxy_router(state: ProxyState, timeout_secs: u64) -> Router {
    let request_ids = RequestIdState::default();
    let drain_controller = state.drain_controller.clone();
    // request_id runs first so lifecycle_middleware can read the assigned id;
    // lifecycle runs before the drain gate so drain rejections can hand their
    // terminal classification back through the response extensions.
    let outer_sb = ServiceBuilder::new()
        .layer(middleware::from_fn_with_state(
            request_ids,
            request_id_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            lifecycle_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            drain_controller,
            cc_lb_engine::proxy_drain_middleware,
        ))
        .layer(HopByHopStripLayer::new())
        .layer(cc_lb_observability::trace_layer().make_span_with(
            cc_lb_observability::ProxyMakeSpan::with_route_template(proxy_route_template),
        ));

    let inner_sb = ServiceBuilder::new()
        .layer(crate::chaos::ChaosLayer::from_env())
        .layer(HandleErrorLayer::new(timeout_error))
        .timeout(Duration::from_secs(timeout_secs.max(1)));

    // Every route on the proxy listener — including the router fallback and
    // the method-not-allowed fallback below — runs under lifecycle_middleware
    // so each accepted request produces exactly one request-log row.
    let routes = Router::new()
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
        .route("/api/oauth/usage", get(oauth_usage_handler))
        .route("/api/{*path}", any(lifecycle_handler))
        .route("/v1/{*path}", any(lifecycle_handler))
        .with_state(state)
        .layer(inner_sb);

    Router::new()
        .merge(routes)
        .fallback(proxy_not_found)
        .method_not_allowed_fallback(proxy_method_not_allowed)
        .layer(outer_sb)
}

async fn proxy_not_found(_request: Request<Body>) -> Response<Body> {
    let mut response = anthropic_error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        "requested proxy path was not found",
    );
    response
        .extensions_mut()
        .insert(TerminalClassification::ROUTE_NOT_FOUND);
    response
}

async fn proxy_method_not_allowed(_request: Request<Body>) -> Response<Body> {
    let mut response = anthropic_error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "not_found",
        "method is not allowed for this proxy path",
    );
    response
        .extensions_mut()
        .insert(TerminalClassification::METHOD_NOT_ALLOWED);
    response
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
    let reason = readiness_blocker(&state);
    let ready = reason.is_none();

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
            reason: reason.map(str::to_owned),
        }),
    )
}

fn readiness_blocker(state: &ProxyState) -> Option<&'static str> {
    if state.drain_controller.is_draining() {
        return Some("draining");
    }
    if state.server_state.current() != ServerState::Ready {
        return Some("startup_not_ready");
    }

    let view = state.dynamic_view.load();
    if !view.principal_view.has_any_active_principal() {
        return Some("no_ready_principal");
    }
    if !has_declared_ready_upstream(&view) {
        return Some("no_ready_upstream");
    }
    if !oauth_credentials_decryptable(state.aead.as_ref(), view.upstreams_snapshot()) {
        return Some("oauth_credentials_not_decryptable");
    }

    None
}

fn has_declared_ready_upstream(view: &DynamicView) -> bool {
    view.upstreams_snapshot().iter().any(|upstream| {
        upstream.enabled
            && upstream.deleted_at_unix_secs.is_none()
            && view
                .upstream_status_snapshot
                .entries
                .get(&upstream.name)
                .is_some_and(|entry| entry.status == cc_lb_control::ApplyStatus::Active)
    })
}

fn oauth_credentials_decryptable(aead: &AeadService, upstreams: &[UpstreamRecord]) -> bool {
    upstreams
        .iter()
        .filter(|upstream| {
            upstream.enabled
                && upstream.deleted_at_unix_secs.is_none()
                && upstream.kind == UpstreamKind::AnthropicOauth
        })
        .all(|upstream| {
            upstream
                .oauth_credentials
                .as_ref()
                .is_some_and(|credentials| {
                    credentials.decrypt(aead, upstream.id.as_bytes()).is_ok()
                })
        })
}

async fn timeout_error(error: tower::BoxError) -> Response<Body> {
    let is_elapsed = error.is::<tower::timeout::error::Elapsed>();
    let status = if is_elapsed {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let mut response = Response::new(Body::from("request timed out"));
    *response.status_mut() = status;
    if is_elapsed {
        response
            .extensions_mut()
            .insert(cc_lb_engine::InternalFailure::tower_timeout(
                cc_lb_domain::InternalError {
                    stage: cc_lb_domain::InternalErrorStage::Relay,
                    kind: cc_lb_domain::InternalErrorKind::Timeout,
                    message: Some(error.to_string()),
                },
            ));
    }
    response
}

async fn lifecycle_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let observer = parts
        .extensions
        .get::<cc_lb_engine::LifecycleContext>()
        .cloned();
    // Authentication comes before any request work: an unauthenticated
    // caller must not be able to make this process buffer its body.
    if let Some(observer) = observer.as_ref() {
        observer.mark_authn_reached();
    }
    let auth = match state.lifecycle.authenticate(&parts.headers).await {
        Ok(auth) => auth,
        Err(error) => {
            return cc_lb_engine::reject_unauthenticated(&error, observer.as_ref());
        }
    };
    let cap = state.body_caps.for_path(parts.uri.path());
    let body_read_started = Instant::now();
    let body_read_span = tracing::info_span!(
        "proxy.read_request_body",
        "cc_lb.request_body_read_ms" = tracing::field::Empty,
        "http.request.body.size" = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let body_result = read_request_body(&parts.headers, body, cap, observer.clone(), &auth)
        .instrument(body_read_span.clone())
        .await;
    let body_read_ms = body_read_started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    body_read_span.record("cc_lb.request_body_read_ms", body_read_ms);

    let body = match body_result {
        Ok(body) => {
            let body_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
            body_read_span.record("http.request.body.size", body_bytes);
            body_read_span.record("outcome", "success");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, Some(body_bytes));
            }
            body
        }
        Err(RequestBodyReadError::TooLarge) => {
            body_read_span.record("outcome", "too_large");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, None);
                observer.terminate_body_too_large(cap as u64);
            }
            return body_too_large_response();
        }
        Err(RequestBodyReadError::Read(source)) => {
            body_read_span.record("outcome", "read_error");
            drop(body_read_span);
            if let Some(observer) = observer.as_ref() {
                observer.set_request_body_timing(body_read_ms, None);
            }
            let mut response = Response::new(Body::from(format!("body read failed: {source}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
                .extensions_mut()
                .insert(cc_lb_engine::InternalFailure::body_read_failed(
                    cc_lb_domain::InternalError {
                        stage: cc_lb_domain::InternalErrorStage::Ingress,
                        kind: cc_lb_domain::InternalErrorKind::InvalidInput,
                        message: Some(source.to_string()),
                    },
                ));
            return response;
        }
    };
    let request = Request::from_parts(parts, body);
    match state.lifecycle.handle(request, &auth).await {
        Ok(response) => response,
        Err(source) => {
            let mut response = Response::new(Body::from(source.to_string()));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

/// Buffers the request body under `cap`.
///
/// `_auth` is the proof that the request was already authenticated: the body
/// must never be buffered for a caller that has not authenticated. The
/// parameter is unused here on purpose — it exists so that moving this call
/// ahead of authentication fails to compile instead of silently reopening
/// the pre-auth buffering hole.
async fn read_request_body(
    headers: &HeaderMap,
    body: Body,
    cap: usize,
    observer: Option<cc_lb_engine::LifecycleContext>,
    _auth: &cc_lb_engine::Authenticated,
) -> Result<Bytes, RequestBodyReadError> {
    let mut timings = RequestBodyTimingGuard::new(observer);
    let content_length_too_large = content_length_exceeds_cap(headers, cap);
    if content_length_too_large {
        timings.finish();
        return Err(RequestBodyReadError::TooLarge);
    }

    let result = collect_limited_body(body, cap, &mut timings)
        .await
        .map_err(|source| {
            if source.is::<LengthLimitError>() {
                RequestBodyReadError::TooLarge
            } else {
                RequestBodyReadError::Read(source)
            }
        });
    timings.finish();
    result
}

fn content_length_exceeds_cap(headers: &HeaderMap, cap: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > cap))
}

async fn collect_limited_body(
    body: Body,
    cap: usize,
    timings: &mut RequestBodyTimingGuard,
) -> Result<Bytes, tower::BoxError> {
    let mut body = Limited::new(body, cap);
    let mut first: Option<Bytes> = None;
    let mut combined: Option<BytesMut> = None;

    loop {
        timings.begin_wait();
        let frame = body.frame().await;
        let received = Instant::now();
        timings.end_wait(received);
        let Some(frame) = frame else {
            break;
        };

        let frame = frame?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.is_empty() {
            continue;
        }

        timings.observe_data(received);
        if let Some(buffer) = combined.as_mut() {
            buffer.extend_from_slice(&data);
        } else if let Some(initial) = first.take() {
            let mut buffer = BytesMut::with_capacity(initial.len().saturating_add(data.len()));
            buffer.extend_from_slice(&initial);
            buffer.extend_from_slice(&data);
            combined = Some(buffer);
        } else {
            first = Some(data);
        }
    }

    Ok(combined.map(BytesMut::freeze).or(first).unwrap_or_default())
}

fn body_too_large_response() -> Response<Body> {
    anthropic_error_response(
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
        "request body exceeds configured cap",
    )
}

async fn oauth_usage_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let authn = &state.builtin_authn;
    let dynamic_view = state.dynamic_view.load();
    // The request reached the authentication attempt: replay buffered
    // pre-auth lifecycle events so the request-log row is complete.
    if let Some(ctx) = request.extensions().get::<cc_lb_engine::LifecycleContext>() {
        ctx.mark_authn_reached();
    }
    if let Err(error) =
        cc_lb_engine::authenticate_first(authn, request.headers(), &dynamic_view.principal_view)
            .await
    {
        let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
        let mut response = json_response(
            status,
            serde_json::json!({ "error": "authentication_error", "message": error.to_string() }),
        );
        response
            .extensions_mut()
            .insert(TerminalClassification::local(status));
        return response;
    }

    let mut response = match cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response(
        state.storage.as_ref(),
        &state.dynamic_view,
        &*state.clock,
    )
    .await
    {
        Ok(response) => json_response(StatusCode::OK, response),
        Err(error) => {
            tracing::error!(%error, "cc-lb oauth usage response build failed");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "usage_unavailable" }),
            )
        }
    };
    let status = response.status();
    response
        .extensions_mut()
        .insert(TerminalClassification::local(status));
    response
}

fn json_response(status: StatusCode, value: impl Serialize) -> Response<Body> {
    match serde_json::to_vec(&value) {
        Ok(bytes) => Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .unwrap_or_else(|_| Response::new(Body::empty())),
        Err(error) => {
            tracing::error!(%error, "json response serialization failed");
            let mut response = Response::new(Body::from("json serialization failed"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

#[derive(Clone)]
struct RequestIdState {
    counter: Arc<AtomicU64>,
    prefix: &'static str,
}

impl RequestIdState {
    fn with_prefix(prefix: &'static str) -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
            prefix,
        }
    }
}

impl Default for RequestIdState {
    fn default() -> Self {
        Self::with_prefix("req_server_")
    }
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
            match HeaderValue::from_str(&server_request_id(state.prefix, id)) {
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

fn server_request_id(prefix: &str, id: u64) -> String {
    let mut request_id = String::with_capacity(prefix.len() + 20);
    request_id.push_str(prefix);
    let _ = write!(&mut request_id, "{id}");
    request_id
}

async fn lifecycle_middleware(
    State(state): State<ProxyState>,
    mut request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let ctx: Option<cc_lb_engine::LifecycleContext> = state.lifecycle.event_bus().map(|bus| {
        let request_id = request
            .headers()
            .get("request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("req_server_unknown")
            .to_owned();
        let context = cc_lb_engine::LifecycleContext::new(request_id, bus, &state.clock);
        context.set_event_kind(cc_lb_request_log::RequestEventKind::from_path(
            request.uri().path(),
        ));
        context
    });
    if let Some(c) = ctx.as_ref() {
        request.extensions_mut().insert(c.clone());
    }
    let mut response = next.run(request).await;
    // Only a response carrying an explicit terminal marker is finalized here.
    // A streaming proxy response returns headers long before its body
    // finishes, so terminating unconditionally would finalize every stream at
    // header time; unmarked responses fall through to the context's Drop
    // backstop instead. `InternalFailure` (typed internal error) is checked
    // before `TerminalClassification` (local status-only outcome).
    if let Some(c) = ctx.as_ref() {
        if let Some(failure) = response
            .extensions_mut()
            .remove::<cc_lb_engine::InternalFailure>()
        {
            c.terminate_failure(failure);
        } else if let Some(marker) = response.extensions().get::<TerminalClassification>() {
            c.terminate_local(*marker);
        }
    }
    response
}

fn server_join_result(result: Result<Result<(), io::Error>, JoinError>) -> Result<(), BuildError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BuildError::from(source)),
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(BuildError::from(io::Error::other(source.to_string()))),
    }
}

async fn await_or_abort<T>(mut task: JoinHandle<T>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

fn init_observability(config: &Config) -> Result<TracingGuard, BuildError> {
    cc_lb_observability::init(&ObservabilityConfig {
        tracing_level: config.observability.tracing_level.clone(),
        otlp_endpoint: config.observability.otlp_endpoint.clone(),
        metrics_addr: config.listener.metrics_addr,
        log_redaction: config.observability.log_redaction,
        user_prompt_redaction: config.observability.user_prompt_redaction,
    })
    .map_err(BuildError::from)
}

type OpenStorageParts = (
    Arc<dyn ManagedKeyStore>,
    Arc<dyn Storage>,
    Arc<AeadService>,
    Arc<dyn LazyRefreshClaimGuard>,
    crate::scheduler_factory::OpenedScheduler,
);

pub async fn open_storage(
    config: &Config,
    clock: ClockHandle,
) -> Result<OpenStorageParts, BuildError> {
    let key_hex =
        std::env::var(&config.aead.key_env).map_err(|_| BuildError::StorageKeyMissing {
            env: config.aead.key_env.clone(),
        })?;
    let key = decode_hex_key(&key_hex)?;
    let aead = Arc::new(AeadService::from_master_key(key));
    let opened = storage_factory::open_storage(&config.storage, clock.clone()).await?;
    let opened_scheduler =
        crate::scheduler_factory::open_scheduler_storage(&config.storage, &config.scheduler, clock)
            .await?;
    let lazy_refresh_claim_guard =
        crate::refresh::lazy_refresh_claim_guard_from_scheduler(&opened_scheduler.backend);
    Ok((
        opened.managed_key_store,
        opened.storage,
        aead,
        lazy_refresh_claim_guard,
        opened_scheduler,
    ))
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
    clock: ClockHandle,
) -> (Arc<dyn UpstreamDispatch>, Arc<BreakerRegistry>) {
    let bulkhead_config = BulkheadRuntimeConfig::from(config.bulkhead.clone());
    let breaker_config = BreakerRuntimeConfig::from(config.circuit_breaker.clone());
    let upstream_name = Arc::new(|request: &SignedRequest| {
        request
            .url()
            .host_str()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "unknown".to_owned())
    });
    let breaker_registry = Arc::new(BreakerRegistry::new());
    let breaker_upstream_name = upstream_name.clone();
    let breaker_registry_for_dispatcher = breaker_registry.clone();
    let breaker_clock = clock;
    let dispatcher_factory = Arc::new(move |max_idle_per_host| {
        let base = make_default_dispatcher(max_idle_per_host);
        Arc::new(CircuitBreakerDispatch::new(
            base,
            breaker_registry_for_dispatcher.clone(),
            breaker_config,
            breaker_upstream_name.clone(),
            breaker_clock.clone(),
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::convert::Infallible;
    use std::io;
    use std::sync::{Arc, LazyLock};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use cc_lb_control::RequestEventBus;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_control::api_keys::secret;
    use cc_lb_lifecycle::LifecycleEvent;
    use cc_lb_storage_api::{
        ApiKeyMutation, IssueParams, KeyStatus, StorageResult, StoredApiKeyRecord,
    };
    use futures_util::stream;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    const TIER_PRICING_TEST_MODEL: &str = "tier-pricing-test-model";

    fn pricing_limit_cost_estimator(
        by_tier: BTreeMap<String, cc_lb_pricing::TierRate>,
    ) -> PricingLimitCostEstimator {
        let catalog = cc_lb_pricing::PriceCatalog::new_empty();
        let mut snapshot = cc_lb_pricing::CatalogSnapshot::empty_cost_disabled();
        snapshot.status = cc_lb_pricing::CatalogStatus::Ok;
        snapshot.models.insert(
            TIER_PRICING_TEST_MODEL.to_owned(),
            cc_lb_pricing::Pricing {
                model: TIER_PRICING_TEST_MODEL.to_owned(),
                input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(8),
                by_tier,
            },
        );
        catalog.install_snapshot(snapshot);
        PricingLimitCostEstimator { catalog }
    }

    async fn panic_when_polled() -> Result<Bytes, Infallible> {
        panic!("request body must not be polled")
    }

    fn controlled_body() -> (
        tokio::sync::mpsc::UnboundedSender<Result<Bytes, io::Error>>,
        tokio::sync::mpsc::UnboundedReceiver<()>,
        Body,
    ) {
        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::unbounded_channel();
        let (poll_tx, poll_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream = stream::unfold((chunk_rx, poll_tx), |(mut chunk_rx, poll_tx)| async move {
            let _ = poll_tx.send(());
            chunk_rx
                .recv()
                .await
                .map(|chunk| (chunk, (chunk_rx, poll_tx)))
        });
        (chunk_tx, poll_rx, Body::from_stream(stream))
    }

    fn assert_measured(value: Option<f64>, field: &str) -> f64 {
        let value = value.unwrap_or_else(|| panic!("{field} should be measured"));
        assert!(value.is_finite(), "{field} should be finite: {value}");
        assert!(value >= 0.0, "{field} should be nonnegative: {value}");
        value
    }

    const TEST_PRINCIPAL: &str = "principal-test";

    struct TestKey {
        plaintext: String,
        key_id: String,
        record: StoredApiKeyRecord,
    }

    static TEST_KEY: LazyLock<TestKey> = LazyLock::new(|| {
        let generated = secret::generate_new();
        TestKey {
            plaintext: generated.plaintext.expose().to_owned(),
            key_id: generated.key_id,
            record: StoredApiKeyRecord {
                label: "body-read test key".to_owned(),
                verify_hash: generated.verify_hash,
                secret_salt: generated.secret_salt,
                status: KeyStatus::Active,
                last_4: generated.last_4,
                index_hash: generated.index_hash,
                ..StoredApiKeyRecord::default()
            },
        }
    });

    /// In-memory `ManagedKeyStore` resolving exactly the fixture key for
    /// `TEST_PRINCIPAL` (mirrors the engine `InMemoryManagedKeyStore`).
    struct FixtureKeyStore;

    #[async_trait]
    impl ManagedKeyStore for FixtureKeyStore {
        async fn issue(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _params: IssueParams,
        ) -> StorageResult<StoredApiKeyRecord> {
            Ok(TEST_KEY.record.clone())
        }

        async fn get(
            &self,
            principal_id: &str,
            key_id: &str,
        ) -> StorageResult<Option<StoredApiKeyRecord>> {
            Ok(
                (principal_id == TEST_PRINCIPAL && key_id == TEST_KEY.key_id)
                    .then(|| TEST_KEY.record.clone()),
            )
        }

        async fn lookup_by_index_hash(
            &self,
            index_hash: &[u8; 32],
        ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
            Ok((index_hash == &TEST_KEY.record.index_hash).then(|| {
                (
                    TEST_PRINCIPAL.to_owned(),
                    TEST_KEY.key_id.clone(),
                    TEST_KEY.record.clone(),
                )
            }))
        }

        async fn list_by_principal(
            &self,
            principal_id: &str,
        ) -> StorageResult<Vec<StoredApiKeyRecord>> {
            Ok((principal_id == TEST_PRINCIPAL)
                .then(|| TEST_KEY.record.clone())
                .into_iter()
                .collect())
        }

        async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
            Ok(vec![(
                TEST_PRINCIPAL.to_owned(),
                TEST_KEY.key_id.clone(),
                TEST_KEY.record.clone(),
            )])
        }

        async fn update(
            &self,
            _principal_id: &str,
            _key_id: &str,
            _mutation: ApiKeyMutation,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn revoke_zero_secrets(
            &self,
            _principal_id: &str,
            _key_id: &str,
        ) -> StorageResult<()> {
            Ok(())
        }
    }

    /// Runs the real authentication path so `read_request_body` receives a
    /// genuine `Authenticated` proof token.
    async fn authenticate_test_request(headers: &HeaderMap) -> cc_lb_engine::Authenticated {
        let storage: Arc<dyn ManagedKeyStore> = Arc::new(FixtureKeyStore);
        let authn = BuiltinAuthn::new(
            Arc::new(KeyStore::new(storage)),
            Arc::new(cc_lb_clock::SystemClock),
        );
        let view = PrincipalView::for_tests(
            TEST_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            Vec::new(),
            HashMap::new(),
        );
        let mut headers = headers.clone();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(TEST_KEY.plaintext.as_str())
                .expect("fixture key is a valid header value"),
        );
        cc_lb_engine::authenticate_first(&authn, &headers, &view)
            .await
            .expect("fixture key should authenticate")
    }

    async fn read_with_timings(
        headers: HeaderMap,
        body: Body,
        cap: usize,
    ) -> (
        Result<Bytes, RequestBodyReadError>,
        cc_lb_lifecycle::RequestIoTimings,
    ) {
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = cc_lb_engine::LifecycleContext::new(
            "measured-ingress".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        let auth = authenticate_test_request(&headers).await;
        let result = read_request_body(&headers, body, cap, Some(observer.clone()), &auth).await;
        drop(observer);
        let LifecycleEvent::RequestTerminated { io_timings, .. } = events
            .recv()
            .await
            .expect("terminal event should be emitted")
        else {
            panic!("expected terminal event");
        };
        (result, io_timings)
    }

    fn assert_ms(actual: Option<f64>, expected: f64, field: &str) {
        let actual = actual.unwrap_or_else(|| panic!("{field} should be measured"));
        assert!(
            (actual - expected).abs() < 1e-9,
            "{field}: expected {expected}, got {actual}"
        );
    }

    #[test]
    fn timing_snapshot_partitions_completed_elapsed_time_without_overlap() {
        let ended = Instant::now();
        let started = ended.checked_sub(Duration::from_millis(30)).unwrap();
        let first_chunk_received = ended.checked_sub(Duration::from_millis(20));
        let process_started = ended.checked_sub(Duration::from_millis(6)).unwrap();
        let timings = RequestBodyTimingGuard {
            observer: None,
            started,
            first_chunk_received,
            accumulated_wait: Duration::from_millis(12),
            active_wait_started: None,
            process_started,
            process: Duration::from_millis(12),
            chunk_count: 2,
            published: false,
        }
        .snapshot(ended);

        assert_ms(timings.request_body_first_chunk_ms, 10.0, "first chunk");
        assert_ms(timings.request_body_receive_ms, 20.0, "receive");
        assert_ms(timings.request_body_wait_ms, 12.0, "wait");
        assert_ms(timings.request_body_process_ms, 18.0, "process");
        assert_eq!(timings.request_body_chunk_count, Some(2));
    }

    #[test]
    fn timing_snapshot_counts_an_in_flight_cancelled_await_as_wait_only() {
        let ended = Instant::now();
        let started = ended.checked_sub(Duration::from_millis(30)).unwrap();
        let first_chunk_received = ended.checked_sub(Duration::from_millis(20));
        let active_wait_started = ended.checked_sub(Duration::from_millis(4));
        let timings = RequestBodyTimingGuard {
            observer: None,
            started,
            first_chunk_received,
            accumulated_wait: Duration::from_millis(8),
            active_wait_started,
            process_started: active_wait_started.unwrap(),
            process: Duration::from_millis(18),
            chunk_count: 1,
            published: false,
        }
        .snapshot(ended);

        assert_ms(timings.request_body_first_chunk_ms, 10.0, "first chunk");
        assert_ms(timings.request_body_receive_ms, 20.0, "receive");
        assert_ms(timings.request_body_wait_ms, 12.0, "wait");
        assert_ms(timings.request_body_process_ms, 18.0, "process");
        assert_eq!(timings.request_body_chunk_count, Some(1));
    }

    #[tokio::test]
    async fn oversized_content_length_rejects_without_polling_body() {
        // Given
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("6"));
        let body = Body::from_stream(stream::once(panic_when_polled()));

        // When
        let (result, io_timings) = read_with_timings(headers, body, 5).await;

        // Then
        assert!(matches!(result, Err(RequestBodyReadError::TooLarge)));
        assert_eq!(io_timings.request_body_first_chunk_ms, None);
        assert_eq!(io_timings.request_body_receive_ms, None);
        assert_eq!(io_timings.request_body_wait_ms, Some(0.0));
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(0));
    }

    #[tokio::test]
    async fn single_frame_body_remains_zero_copy_at_cap() {
        // Given
        let input = Bytes::from_static(b"single frame");
        let input_ptr = input.as_ptr();

        // When
        let (output, io_timings) =
            read_with_timings(HeaderMap::new(), Body::from(input.clone()), input.len()).await;
        let output = output.expect("body at cap should collect");

        // Then
        assert_eq!(output, input);
        assert_eq!(output.as_ptr(), input_ptr);
        assert_eq!(io_timings.request_body_chunk_count, Some(1));
    }

    #[tokio::test]
    async fn delayed_first_and_subsequent_frames_preserve_bytes_and_split_waits() {
        // Given
        let (chunk_tx, mut poll_rx, body) = controlled_body();
        let read = tokio::spawn(async move {
            let (output, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;
            (output.expect("controlled body should collect"), io_timings)
        });

        poll_rx
            .recv()
            .await
            .expect("first frame await should begin");
        assert!(!read.is_finished(), "first frame should remain pending");
        chunk_tx
            .send(Ok(Bytes::from_static(b"abc")))
            .expect("send first frame");

        poll_rx
            .recv()
            .await
            .expect("subsequent frame await should begin");
        assert!(
            !read.is_finished(),
            "subsequent frame should remain pending"
        );
        chunk_tx
            .send(Ok(Bytes::from_static(b"def")))
            .expect("send subsequent frame");
        drop(chunk_tx);

        // When
        let (output, io_timings) = read.await.expect("body reader task should complete");

        // Then
        assert_eq!(output, Bytes::from_static(b"abcdef"));
        assert!(
            assert_measured(
                io_timings.request_body_first_chunk_ms,
                "request_body_first_chunk_ms",
            ) > 0.0
        );
        assert!(
            assert_measured(
                io_timings.request_body_receive_ms,
                "request_body_receive_ms",
            ) > 0.0
        );
        assert!(assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms") > 0.0);
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(2));
    }

    #[tokio::test]
    async fn empty_data_frames_do_not_start_data_markers_or_increment_count() {
        let (chunk_tx, mut poll_rx, body) = controlled_body();
        let read = tokio::spawn(async move { read_with_timings(HeaderMap::new(), body, 6).await });
        poll_rx
            .recv()
            .await
            .expect("first frame await should begin");
        chunk_tx
            .send(Ok(Bytes::new()))
            .expect("send empty DATA frame");
        poll_rx
            .recv()
            .await
            .expect("reader should await after empty DATA");
        assert!(!read.is_finished(), "empty DATA must not finish collection");
        drop(chunk_tx);

        // When
        let (output, io_timings) = read.await.expect("body reader task should complete");
        let output = output.expect("empty body should collect");

        // Then
        assert!(output.is_empty());
        assert_eq!(io_timings.request_body_first_chunk_ms, None);
        assert_eq!(io_timings.request_body_receive_ms, None);
        assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(0));
    }

    #[tokio::test]
    async fn multi_frame_body_collects_into_contiguous_bytes_at_cap() {
        // Given
        let body = Body::from_stream(stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"abc")),
            Ok::<_, Infallible>(Bytes::from_static(b"def")),
        ]));

        // When
        let (output, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;
        let output = output.expect("body at cap should collect");

        // Then
        assert_eq!(output, Bytes::from_static(b"abcdef"));
        assert_eq!(io_timings.request_body_chunk_count, Some(2));
    }

    #[tokio::test]
    async fn streamed_cap_error_preserves_completed_ingress_measurements() {
        // Given
        let headers = HeaderMap::new();
        let body = Body::from_stream(stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"abc")),
            Ok::<_, Infallible>(Bytes::from_static(b"def")),
        ]));

        // When
        let (result, io_timings) = read_with_timings(headers, body, 5).await;

        // Then
        assert!(matches!(result, Err(RequestBodyReadError::TooLarge)));
        assert_measured(
            io_timings.request_body_first_chunk_ms,
            "request_body_first_chunk_ms",
        );
        assert_measured(
            io_timings.request_body_receive_ms,
            "request_body_receive_ms",
        );
        assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(1));
    }

    #[tokio::test]
    async fn transport_error_preserves_completed_ingress_measurements() {
        // Given
        let body = Body::from_stream(stream::iter([
            Ok(Bytes::from_static(b"abc")),
            Err(io::Error::other("transport failed")),
        ]));

        // When
        let (result, io_timings) = read_with_timings(HeaderMap::new(), body, 6).await;

        // Then
        let Err(RequestBodyReadError::Read(source)) = result else {
            panic!("transport failure should remain a read error");
        };
        assert!(source.to_string().contains("transport failed"));
        assert_measured(
            io_timings.request_body_first_chunk_ms,
            "request_body_first_chunk_ms",
        );
        assert_measured(
            io_timings.request_body_receive_ms,
            "request_body_receive_ms",
        );
        assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms");
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(1));
    }

    #[tokio::test]
    async fn cancelled_body_read_publishes_active_wait_before_terminal_finish() {
        // Given
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let observer = cc_lb_engine::LifecycleContext::new(
            "cancelled-ingress".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.mark_authn_reached();
        let (chunk_tx, mut poll_rx, body) = controlled_body();
        let read_observer = observer.clone();
        let auth = authenticate_test_request(&HeaderMap::new()).await;
        let read = tokio::spawn(async move {
            read_request_body(&HeaderMap::new(), body, 6, Some(read_observer), &auth).await
        });
        poll_rx
            .recv()
            .await
            .expect("first frame await should begin");
        chunk_tx
            .send(Ok(Bytes::from_static(b"abc")))
            .expect("send first frame");
        poll_rx
            .recv()
            .await
            .expect("subsequent frame await should begin");

        // When
        read.abort();
        let error = read.await.expect_err("body reader should be cancelled");
        assert!(error.is_cancelled());
        drop(observer);

        // Then
        let LifecycleEvent::RequestTerminated { io_timings, .. } = events
            .recv()
            .await
            .expect("terminal event should be emitted")
        else {
            panic!("expected terminal event");
        };
        assert_measured(
            io_timings.request_body_first_chunk_ms,
            "request_body_first_chunk_ms",
        );
        assert!(
            assert_measured(
                io_timings.request_body_receive_ms,
                "request_body_receive_ms",
            ) > 0.0
        );
        assert!(assert_measured(io_timings.request_body_wait_ms, "request_body_wait_ms",) > 0.0);
        assert_measured(
            io_timings.request_body_process_ms,
            "request_body_process_ms",
        );
        assert_eq!(io_timings.request_body_chunk_count, Some(1));
    }

    #[test]
    fn request_body_caps_select_files_and_messages_independently() {
        // Given
        let caps = RequestBodyCaps {
            messages: 3,
            files: 7,
        };

        // When / Then
        assert_eq!(caps.for_path("/v1/messages"), 3);
        assert_eq!(caps.for_path("/v1/files"), 7);
        assert_eq!(caps.for_path("/v1/files/id/content"), 7);
        assert_eq!(caps.for_path("/v1/models"), 3);
    }

    #[tokio::test]
    async fn body_too_large_response_reuses_anthropic_error_shape() {
        // Given / When
        let response = body_too_large_response();
        let status = response.status();
        let content_type = response.headers()[axum::http::header::CONTENT_TYPE].clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("error body should collect")
            .to_bytes();

        // Then
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(content_type, "application/json; charset=utf-8");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("error body should be json"),
            serde_json::json!({
                "type": "error",
                "error": {
                    "type": "body_too_large",
                    "message": "request body exceeds configured cap",
                },
            })
        );
    }

    #[tokio::test]
    async fn admin_health_state_reports_current_state() {
        let state = Arc::new(ServerStateHandle::new_starting());
        let router = server_state_router(state.clone());

        assert_admin_state(router.clone(), "starting").await;
        state.transition_to_ready();
        assert_admin_state(router, "ready").await;
    }

    #[test]
    fn default_fallback_populates_empty_catalog() {
        let catalog = cc_lb_pricing::PriceCatalog::new_empty();
        assert!(matches!(
            catalog.status(),
            cc_lb_pricing::CatalogStatus::CostDisabled
        ));

        let clock = cc_lb_clock::SystemClock;
        install_default_fallback_if_uninitialized(&catalog, &clock);

        assert!(matches!(catalog.status(), cc_lb_pricing::CatalogStatus::Ok));
        assert!(catalog.lookup("claude-opus-4-5", None).is_some());
    }

    #[test]
    fn pricing_limit_cost_estimator_uses_requested_priority_tier() {
        // Given an estimator with a distinct priority price.
        let estimator = pricing_limit_cost_estimator(BTreeMap::from([(
            "priority".to_owned(),
            cc_lb_pricing::TierRate {
                input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(12),
            },
        )]));

        // When maximum cost is estimated for a priority request.
        let estimate = cc_lb_engine::LimitCostEstimator::estimate_max(
            &estimator,
            TIER_PRICING_TEST_MODEL,
            1_000_000,
            1_000_000,
            Some("priority"),
        );

        // Then the explicit priority rates are reserved.
        assert_eq!(estimate, Some(15_000_000));
    }

    #[test]
    fn pricing_limit_cost_estimator_uses_batch_fallback() {
        // Given an estimator whose catalog has only base prices.
        let estimator = pricing_limit_cost_estimator(BTreeMap::new());

        // When maximum cost is estimated for a batch request.
        let estimate = cc_lb_engine::LimitCostEstimator::estimate_max(
            &estimator,
            TIER_PRICING_TEST_MODEL,
            1_000_000,
            1_000_000,
            Some("batch"),
        );

        // Then the catalog's exact-half batch fallback is reserved.
        assert_eq!(estimate, Some(5_000_000));
    }

    #[test]
    fn default_fallback_contains_current_anthropic_pricing() {
        let catalog = cc_lb_pricing::PriceCatalog::new_empty();
        let clock = cc_lb_clock::SystemClock;

        install_default_fallback_if_uninitialized(&catalog, &clock);

        let expected = [
            (
                "claude-fable-5",
                10_000_000,
                12_500_000,
                20_000_000,
                1_000_000,
                50_000_000,
            ),
            (
                "claude-mythos-5",
                10_000_000,
                12_500_000,
                20_000_000,
                1_000_000,
                50_000_000,
            ),
            (
                "claude-opus-5",
                5_000_000,
                6_250_000,
                10_000_000,
                500_000,
                25_000_000,
            ),
            (
                "claude-opus-4-8",
                5_000_000,
                6_250_000,
                10_000_000,
                500_000,
                25_000_000,
            ),
            (
                "claude-opus-4-7",
                5_000_000,
                6_250_000,
                10_000_000,
                500_000,
                25_000_000,
            ),
            (
                "claude-opus-4-6",
                5_000_000,
                6_250_000,
                10_000_000,
                500_000,
                25_000_000,
            ),
            (
                "claude-opus-4-5",
                5_000_000,
                6_250_000,
                10_000_000,
                500_000,
                25_000_000,
            ),
            (
                "claude-sonnet-5",
                3_000_000,
                3_750_000,
                6_000_000,
                300_000,
                15_000_000,
            ),
            (
                "claude-sonnet-4-6",
                3_000_000,
                3_750_000,
                6_000_000,
                300_000,
                15_000_000,
            ),
            (
                "claude-sonnet-4-5",
                3_000_000,
                3_750_000,
                6_000_000,
                300_000,
                15_000_000,
            ),
            (
                "claude-haiku-4-5",
                1_000_000,
                1_250_000,
                2_000_000,
                100_000,
                5_000_000,
            ),
        ];

        for (model, input, cache_5m, cache_1h, cache_read, output) in expected {
            let pricing = catalog
                .lookup(model, None)
                .unwrap_or_else(|| panic!("{model} fallback pricing exists"));
            assert_eq!(pricing.input_per_million_usd.as_micros_usd(), input);
            assert_eq!(pricing.output_per_million_usd.as_micros_usd(), output);

            let cache_pricing = catalog
                .routing_cache_pricing(model, None)
                .unwrap_or_else(|| panic!("{model} fallback cache pricing exists"));
            assert_eq!(
                cache_pricing
                    .cache_creation_5m_per_million_usd
                    .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
                Some(cache_5m)
            );
            assert_eq!(
                cache_pricing
                    .cache_creation_1h_per_million_usd
                    .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
                Some(cache_1h)
            );
            assert_eq!(
                cache_pricing
                    .cache_read_per_million_usd
                    .map(cc_lb_pricing::UsdPerMillion::as_micros_usd),
                Some(cache_read)
            );
        }

        let snapshot = catalog.current();
        assert_eq!(
            snapshot
                .models
                .keys()
                .filter(|model| model.starts_with("claude-fable"))
                .count(),
            1
        );
    }

    #[test]
    fn default_fallback_preserves_existing_ok_snapshot() {
        use std::collections::HashMap;

        let catalog = cc_lb_pricing::PriceCatalog::new_empty();
        let mut models = HashMap::new();
        models.insert(
            "operator-model-a".to_owned(),
            cc_lb_pricing::Pricing {
                model: "operator-model-a".to_owned(),
                input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(2),
                output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(8),
                by_tier: Default::default(),
            },
        );
        catalog.install_snapshot(cc_lb_pricing::CatalogSnapshot {
            payload_hash: String::new(),
            fetched_at_ms: 0,
            models,
            raw_json: Vec::new(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            cache_creation_per_million_usd_by_tier: HashMap::new(),
            cache_read_per_million_usd_by_tier: HashMap::new(),
            status: cc_lb_pricing::CatalogStatus::Ok,
        });

        let clock = cc_lb_clock::SystemClock;
        install_default_fallback_if_uninitialized(&catalog, &clock);

        assert!(catalog.lookup("operator-model-a", None).is_some());
        assert!(catalog.lookup("claude-opus-4-5", None).is_none());
    }

    #[test]
    fn lifecycle_config_uses_startup_upstream_affinity_ttl() {
        let mut config = Config::default();
        config.upstream_affinity.ttl_days = 14;

        let lifecycle_config = super::lifecycle_config_from_config(
            &config,
            super::RequestBodyCaps {
                messages: 1024,
                files: 2048,
            },
            None,
        );

        assert_eq!(
            lifecycle_config.upstream_affinity_ttl,
            std::time::Duration::from_secs(14 * 86_400)
        );
    }

    #[test]
    fn wasmtime_config_overrides_hot_engine_memory_profile() {
        let mut config = Config::default();
        config.runtime.wasmtime.allocation_strategy =
            cc_lb_config::WasmtimeAllocationStrategy::Pooling;
        config.runtime.wasmtime.memory_max_pages = Some(4096);
        config.runtime.wasmtime.memory_reservation_bytes = Some(512 * 1024 * 1024);
        config.runtime.wasmtime.memory_guard_bytes = Some(128 * 1024 * 1024);
        config.runtime.wasmtime.pool_total_memories = Some(8);
        config.runtime.wasmtime.pool_total_core_instances = Some(12);

        let hot_engine_cfg = super::hot_engine_config_from_config(&config);

        assert_eq!(
            hot_engine_cfg.allocation_strategy,
            cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::Pooling,
        );
        assert_eq!(hot_engine_cfg.memory_max_pages, 4096);
        assert_eq!(hot_engine_cfg.memory_reservation_bytes, 512 * 1024 * 1024);
        assert_eq!(hot_engine_cfg.memory_guard_bytes, 128 * 1024 * 1024);
        assert_eq!(hot_engine_cfg.pool_total_memories, 8);
        assert_eq!(hot_engine_cfg.pool_total_core_instances, 12);
    }

    #[test]
    fn wasmtime_config_defaults_to_on_demand_hot_engine() {
        let hot_engine_cfg = super::hot_engine_config_from_config(&Config::default());

        assert_eq!(
            hot_engine_cfg.allocation_strategy,
            cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::OnDemand,
        );
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_startup_rejects_missing_cluster_token_before_storage_connect() {
        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Postgres {
                url: "postgres://127.0.0.1:1/cc_lb".to_owned(),
                pool: cc_lb_config::PostgresPoolConfig::default(),
            },
            ..Config::default()
        };
        config.cluster.instance_url = Some("http://127.0.0.1:9091".to_owned());
        config.cluster.token_env = format!(
            "CC_LB_TEST_MISSING_CLUSTER_TOKEN_{}",
            uuid::Uuid::now_v7().simple()
        );

        let preflight_result = preflight::run_offline(
            &config,
            preflight::PreflightOptions { skip_bind: true },
            Arc::new(cc_lb_clock::SystemClock),
        )
        .await;
        assert!(matches!(
            preflight_result,
            Err(preflight::PreflightError::ClusterTokenMissing(env))
                if env == config.cluster.token_env
        ));

        let result = build_app(config.clone(), Arc::new(cc_lb_clock::SystemClock)).await;

        assert!(matches!(
            result,
            Err(BuildError::ClusterTokenMissing { env }) if env == config.cluster.token_env
        ));
    }

    async fn assert_admin_state(router: Router, expected: &str) {
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/admin/health/state")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("admin health state request should succeed");
        assert_eq!(response.status(), StatusCode::OK);

        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();
        let payload: Value = serde_json::from_slice(&body).expect("body should be json");
        assert_eq!(payload.get("state").and_then(Value::as_str), Some(expected));
    }

    /// QA-CTRL-14: a tower timeout that fires before the handler runs (and
    /// therefore before `mark_authn_reached`) must produce no request-log row
    /// and no `RequestTerminated` event — the pre-authentication silent
    /// invariant. The handler and the request body must never be polled.
    #[tokio::test]
    async fn pre_authn_timeout_emits_no_lifecycle_events() {
        struct PendingSignerFactory;

        impl cc_lb_upstream::ApiKeyAwareSignerFactory for PendingSignerFactory {
            fn with_router_choice(
                &self,
                _router_chosen_upstream_name: String,
            ) -> Arc<dyn cc_lb_upstream::SignerFactory> {
                unreachable!("pre-authn timeout never signs")
            }
        }

        struct PendingDispatch;

        #[async_trait]
        impl UpstreamDispatch for PendingDispatch {
            async fn dispatch(
                &self,
                _request: SignedRequest,
            ) -> Result<Response<Body>, cc_lb_engine::DispatchError> {
                unreachable!("pre-authn timeout never dispatches")
            }
        }

        async fn pending_handler() -> Response<Body> {
            std::future::pending::<()>().await;
            unreachable!("pre-authn timeout never reaches the handler")
        }

        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
            .await
            .expect("in-memory storage opens");
        let builtin_authn = Arc::new(BuiltinAuthn::new(
            Arc::new(KeyStore::new(
                Arc::new(FixtureKeyStore) as Arc<dyn ManagedKeyStore>
            )),
            clock.clone(),
        ));
        let view = cc_lb_control::DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(PendingSignerFactory))
            .principal_view(Arc::new(PrincipalView::for_tests(
                TEST_PRINCIPAL,
                true,
                vec!["*".to_owned()],
                Vec::new(),
                HashMap::new(),
            )))
            .upstream_records(Vec::new())
            .build();
        let dynamic_view = Arc::new(DynamicViewHolder::new(view));
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                builtin_authn.clone(),
                dynamic_view.clone(),
                Arc::new(PendingDispatch),
                LifecycleConfig::default(),
                clock.clone(),
            )
            .with_event_bus(bus as Arc<dyn RequestEventBus>),
        );
        let state = ProxyState {
            lifecycle,
            body_caps: RequestBodyCaps {
                messages: 1024,
                files: 1024,
            },
            server_state: Arc::new(ServerStateHandle::new_starting()),
            start_time: Instant::now(),
            drain_controller: DrainController::new(),
            aead: Arc::new(AeadService::from_master_key([0; 32])),
            storage: Arc::new(storage),
            dynamic_view,
            builtin_authn,
            clock,
        };

        // Same middleware order as `proxy_router`: request_id → lifecycle →
        // drain → (HandleError(timeout_error) → timeout) → routes.
        let router = Router::new()
            .route("/v1/messages", post(pending_handler))
            .with_state(state.clone())
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(timeout_error))
                    .timeout(Duration::from_secs(1)),
            )
            .layer(middleware::from_fn_with_state(
                state.drain_controller.clone(),
                cc_lb_engine::proxy_drain_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                lifecycle_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                RequestIdState::default(),
                request_id_middleware,
            ));

        let request = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .body(Body::from_stream(stream::once(panic_when_polled())))
            .expect("request builds");
        // Pause only after all real-clock setup (sqlite open, router build)
        // completed: the pending handler then lets the 1s tower timeout fire
        // deterministically without racing external worker threads.
        tokio::time::pause();
        let response = router.oneshot(request).await.expect("router responds");

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("timeout body collects")
            .to_bytes();
        assert_eq!(&body[..], b"request timed out");
        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "pre-authn timeout must emit no lifecycle events"
        );
    }
}
