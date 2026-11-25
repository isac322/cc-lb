use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_config::{Config, TlsConfig};
use cc_lb_control::PromptCacheObservationSinkLike;
use cc_lb_control::anthropic_api_key::AnthropicOrgApi;
use cc_lb_control::{
    DynamicViewHolder,
    api_keys::{
        builtin_authn::BuiltinAuthn, concurrent_guard::KeyConcurrencyManager, key_store::KeyStore,
        limit_engine::LimitEngine,
    },
    spawn_audit_writer,
};
use cc_lb_engine::{
    BreakerRegistry, BreakerRuntimeConfig, BulkheadDispatch, BulkheadRegistry,
    BulkheadRuntimeConfig, CircuitBreakerDispatch, DrainController, Lifecycle, LifecycleConfig,
    SubscriptionQuotaSink, UpstreamDispatch, cache_keepalive::AnthropicKeepaliveDispatcher,
    make_default_dispatcher,
};
use cc_lb_event_pipeline::{
    SubscriptionQuotaWriterConfig, UpstreamRateLimitSink, start_subscription_quota_writer,
    start_upstream_rate_limit_writer,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{ManagedKeyStore, RuntimeChangeNotifier, Storage};
use cc_lb_upstream::SignedRequest;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::dynamic_view_builder::{
    Stores as DynamicStores, VertexState, build_dynamic_view, ensure_wasm_cache_dirs,
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
use crate::state_machine::ServerStateHandle;
use crate::storage_factory;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use crate::tls::{ReloadableListener, TlsState};

use super::*;

pub(super) const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_millis(500);
pub(super) const PRICE_CATALOG_LOCAL_INSTALL_INTERVAL: Duration = Duration::from_secs(60);
pub(super) const WASMTIME_POOL_METRICS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProxyRole {
    #[default]
    ProxyWorker,
    ProxyOnly,
    WorkerOnly,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProxyServeOptions {
    pub role: ProxyRole,
    pub strict_preflight: bool,
}

#[derive(Clone, Copy)]
pub(super) struct StartupPreflight {
    pub(super) strict_preflight: bool,
}

pub(super) struct ShutdownRuntime {
    pub(super) lifecycle: Arc<Lifecycle>,
    pub(super) notify_cancel: Option<CancellationToken>,
    pub(super) notifier_task: Option<JoinHandle<()>>,
    pub(super) notify_listener_task: Option<JoinHandle<()>>,
    pub(super) price_catalog_install_cancel: Option<CancellationToken>,
    pub(super) price_catalog_install_task: Option<JoinHandle<()>>,
    pub(super) wasmtime_pool_metrics_cancel: Option<CancellationToken>,
    pub(super) wasmtime_pool_metrics_task: Option<JoinHandle<()>>,
    pub(super) scheduler_cancel: Option<CancellationToken>,
    pub(super) scheduler_tasks: Vec<JoinHandle<()>>,
    pub(super) audit_writer_task: Option<JoinHandle<()>>,
    pub(super) upstream_rate_limit_writer_task: Option<JoinHandle<()>>,
    pub(super) subscription_quota_writer_task: Option<JoinHandle<()>>,
    pub(super) prompt_cache_observation_writer_task: Option<JoinHandle<()>>,
    pub(super) event_fanout_shutdown_tx: Option<watch::Sender<bool>>,
    pub(super) event_fanout_tasks: Vec<JoinHandle<()>>,
    pub(super) signals: signal::SignalHandle,
    pub(super) drain_controller: DrainController,
    pub(super) tls_state: Option<Arc<TlsState>>,
    pub(super) server_state: Arc<ServerStateHandle>,
}

pub(super) struct RuntimeCore {
    pub(super) router: Router,
    pub(super) proxy_addr: SocketAddr,
    #[cfg(feature = "admin")]
    pub(super) admin_addr: SocketAddr,
    pub(super) shutdown: ShutdownRuntime,
    #[cfg(feature = "admin")]
    pub(super) stores: Arc<DynamicStores>,
    #[cfg(feature = "admin")]
    pub(super) runtime: Arc<WasmtimeRuntime>,
    #[cfg(feature = "admin")]
    pub(super) aead: Arc<AeadService>,
    #[cfg(feature = "admin")]
    pub(super) storage: Arc<dyn Storage>,
    #[cfg(feature = "admin")]
    pub(super) key_store: Arc<KeyStore>,
    #[cfg(feature = "admin")]
    pub(super) lazy_refresher_concrete: Option<Arc<LazyRefresher>>,
    #[cfg(feature = "admin")]
    pub(super) lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    #[cfg(feature = "admin")]
    pub(super) warmup_window_verifier: Arc<dyn cc_lb_control::WarmupWindowVerifier>,
    #[cfg(feature = "admin")]
    pub(super) limit_reset_usage_refresh: Arc<dyn cc_lb_control::LimitResetUsageRefreshPort>,
    #[cfg(feature = "admin")]
    pub(super) dynamic_view: Arc<DynamicViewHolder>,
    #[cfg(feature = "admin")]
    pub(super) scheduler_lazy_handle: crate::scheduler_factory::SchedulerBackend,
    #[cfg(feature = "admin")]
    pub(super) data_dir: PathBuf,
    #[cfg(feature = "admin")]
    pub(super) config: Config,
    #[cfg(feature = "admin")]
    pub(super) replica_identity: Option<cc_lb_domain::ReplicaIdentity>,
    #[cfg(feature = "admin")]
    pub(super) subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    #[cfg(feature = "admin")]
    pub(super) prompt_cache_grace_margin_secs: u64,
    #[cfg(feature = "admin")]
    pub(super) prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    #[cfg(feature = "admin")]
    pub(super) start_time: std::time::Instant,
    #[cfg(feature = "admin")]
    pub(super) event_bus: Arc<dyn cc_lb_control::RequestEventBus>,
    #[cfg(feature = "admin")]
    pub(super) anthropic_org_api: AnthropicOrgApi,
}

pub struct ProxyApp {
    pub router: Router,
    pub proxy_addr: SocketAddr,
    role: ProxyRole,
    shutdown: ShutdownRuntime,
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
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error(transparent)]
    Rebind(#[from] crate::dynamic_view_builder::RebindError),
    #[error("wasmtime plugin runtime init failed: {0}")]
    WasmtimeRuntimeInit(#[source] cc_lb_runtime_wasmtime::WasmtimeRuntimeError),
    #[error("storage master key env {env} is missing")]
    StorageKeyMissing { env: String },
    #[error("storage master key must be 32 bytes encoded as 64 hex characters")]
    InvalidStorageKey,
    #[error("postgres storage pool unavailable for pg_notify fanout")]
    PgNotifyPoolUnavailable,
    #[error("anthropic org api origin is invalid: {0}")]
    AnthropicOrgOrigin(#[source] url::ParseError),
    #[error("request diagnostics sink failed to start: {0}")]
    RequestDiagnosticsSink(#[source] io::Error),
}

impl ProxyApp {
    pub fn set_draining(&self, draining: bool) {
        self.shutdown.drain_controller.set_draining(draining);
    }

    pub fn drain_controller(&self) -> DrainController {
        self.shutdown.drain_controller.clone()
    }

    pub fn signal_handle(&self) -> signal::SignalHandle {
        self.shutdown.signals.clone()
    }

    pub async fn start(self) -> Result<(), BuildError> {
        tracing::info!(
            role = ?self.role,
            scheduler_workers = self.shutdown.scheduler_tasks.len(),
            proxy_addr = %self.proxy_addr,
            "starting cc-lb proxy",
        );
        serve_runtime(self.shutdown, self.router, self.proxy_addr, None).await
    }
}

pub(super) async fn serve_runtime(
    shutdown: ShutdownRuntime,
    router: Router,
    proxy_addr: SocketAddr,
    admin: Option<(Router, SocketAddr)>,
) -> Result<(), BuildError> {
    let ShutdownRuntime {
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
    } = shutdown;
    server_state.wait_for_ready().await;
    let proxy_listener = TcpListener::bind(proxy_addr).await?;
    let admin_handle = if let Some((admin_router, admin_addr)) = admin {
        let admin_listener = TcpListener::bind(admin_addr).await?;
        let (admin_stop_tx, admin_shutdown) = watch::channel(false);
        let admin_task = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router)
                .with_graceful_shutdown(signal::wait_for_shutdown(admin_shutdown))
                .await
        });
        Some((admin_stop_tx, admin_task))
    } else {
        None
    };

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

    // The proxy task ended. When a shutdown run started it, the hooks
    // (assembler flush, diagnostics drain, subscriber shutdowns) are still
    // running inside the detached shutdown task; returning now would let
    // `main` drop the runtime and cut them off mid-write (#948). Wait for
    // `drain_complete`, which the shutdown task sends only after every hook
    // finished, bounded by SHUTDOWN_HOOKS_TIMEOUT so a wedged hook cannot
    // hold the process forever. A proxy that stopped on its own with no
    // shutdown started keeps the old behavior: no hooks are running, so
    // there is nothing to wait for.
    if signals.is_shutdown_started()
        && tokio::time::timeout(
            signal::SHUTDOWN_HOOKS_TIMEOUT,
            signal::wait_for_shutdown(signals.subscribe_drain_complete()),
        )
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_secs = signal::SHUTDOWN_HOOKS_TIMEOUT.as_secs(),
            "shutdown hooks did not finish before the bound; they continue detached"
        );
    }

    if let Some(cancel) = notify_cancel {
        cancel.cancel();
    }
    if let Some(task) = notifier_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = notify_listener_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(cancel) = price_catalog_install_cancel {
        cancel.cancel();
    }
    if let Some(task) = price_catalog_install_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(cancel) = wasmtime_pool_metrics_cancel {
        cancel.cancel();
    }
    if let Some(task) = wasmtime_pool_metrics_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(cancel) = scheduler_cancel {
        cancel.cancel();
    }
    for task in scheduler_tasks {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some((admin_stop_tx, admin_task)) = admin_handle {
        let _ = admin_stop_tx.send(true);
        cc_lb_runtime_sync::await_or_abort(admin_task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = audit_writer_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = upstream_rate_limit_writer_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = subscription_quota_writer_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = prompt_cache_observation_writer_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(tx) = event_fanout_shutdown_tx {
        let _ = tx.send(true);
    }
    for task in event_fanout_tasks {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
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

pub async fn run_proxy_serve(
    config_path: &Path,
    data_dir: Option<&Path>,
    options: ProxyServeOptions,
    clock: ClockHandle,
) -> Result<(), ServeError> {
    let mut config = Config::load(config_path)?;
    if let Some(data_dir) = data_dir {
        config.runtime.data_dir = Some(data_dir.to_path_buf());
    }
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let _guard = init_observability(&config)?;
    let app = match build_proxy_app_with_path_inner(
        config,
        options.role,
        Some(StartupPreflight {
            strict_preflight: options.strict_preflight,
        }),
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

pub(super) fn print_preflight_report(report: &preflight::PreflightReport) {
    println!("preflight: ok");
    println!("preflight: upstream_count: {}", report.upstream_count);
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

async fn build_proxy_app_with_path_inner(
    config: Config,
    role: ProxyRole,
    startup_preflight: Option<StartupPreflight>,
    clock: ClockHandle,
) -> Result<ProxyApp, BuildError> {
    config.validate()?;
    let (managed_store, storage, aead, lazy_refresh_claim_guard, opened_scheduler) =
        open_storage_for_role(&config, role, clock.clone()).await?;
    let core = build_runtime_with_storage_core(
        config,
        role,
        None,
        managed_store,
        storage,
        aead,
        startup_preflight,
        opened_scheduler,
        Some(lazy_refresh_claim_guard),
        clock,
    )
    .await?;
    core.shutdown.server_state.transition_to_ready();
    Ok(ProxyApp {
        router: core.router,
        proxy_addr: core.proxy_addr,
        role,
        shutdown: core.shutdown,
    })
}

pub async fn build_proxy_app_with_storage(
    config: Config,
    role: ProxyRole,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
) -> Result<ProxyApp, BuildError> {
    let scheduler_role = match role {
        ProxyRole::ProxyOnly => crate::scheduler_factory::SchedulerPoolRole::EnqueueOnly,
        ProxyRole::ProxyWorker | ProxyRole::WorkerOnly => {
            crate::scheduler_factory::SchedulerPoolRole::Worker
        }
    };
    let opened_scheduler = crate::scheduler_factory::open_scheduler_storage_with_role(
        &config.storage,
        &config.scheduler,
        scheduler_role,
        clock.clone(),
    )
    .await?;
    let core = build_runtime_with_storage_core(
        config,
        role,
        None,
        managed_store,
        storage,
        aead,
        None,
        opened_scheduler,
        None,
        clock,
    )
    .await?;
    core.shutdown.server_state.transition_to_ready();
    Ok(ProxyApp {
        router: core.router,
        proxy_addr: core.proxy_addr,
        role,
        shutdown: core.shutdown,
    })
}

pub fn resolve_data_dir(
    cli: Option<&Path>,
    config_value: Option<&Path>,
    env_var: &str,
) -> Result<PathBuf, BuildError> {
    Ok(cc_lb_runtime_sync::resolve_data_dir(
        cli,
        config_value,
        env_var,
    )?)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_runtime_with_storage_core(
    config: Config,
    role: ProxyRole,
    storage_tail_tx: Option<tokio::sync::broadcast::Sender<cc_lb_request_log::StorageTailUpdate>>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    startup_preflight: Option<StartupPreflight>,
    opened_scheduler: crate::scheduler_factory::OpenedScheduler,
    lazy_refresh_claim_guard: Option<Arc<dyn LazyRefreshClaimGuard>>,
    clock: ClockHandle,
) -> Result<RuntimeCore, BuildError> {
    let _ = cc_lb_providers::builtin_provider_registry();
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
    let reservation_idle_ttl =
        std::time::Duration::from_secs(config.limit_reservation_ttl.ttl_secs.max(1));
    let lifecycle_max_ttl = cc_lb_lifecycle::DEFAULT_LIFECYCLE_MAX_TTL
        .max(std::time::Duration::from_secs(
            config.timeouts.upstream_total_secs.max(1),
        ))
        .max(reservation_idle_ttl);
    let limit_reservation_ttl_handle = Some(
        cc_lb_control::api_keys::limit_engine::spawn_reservation_ttl_sweeper_with_max_ttl(
            limit_engine.clone(),
            reservation_idle_ttl,
            lifecycle_max_ttl,
            std::time::Duration::from_secs(config.limit_reservation_ttl.tick_secs.max(1)),
        ),
    );
    let limit_reservation_ttl_slot: Arc<
        tokio::sync::Mutex<
            Option<cc_lb_control::api_keys::limit_engine::ReservationTtlSweeperHandle>,
        >,
    > = Arc::new(tokio::sync::Mutex::new(limit_reservation_ttl_handle));
    let builtin_authn = Arc::new(BuiltinAuthn::new(key_store.clone(), clock.clone()));
    let (
        dispatcher,
        breaker_registry,
        breaker_config,
        websocket_bulkheads,
        websocket_bulkhead_config,
    ) = dispatcher(&config, clock.clone());
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
        vertex_state: Arc::new(VertexState::new(storage_for_dynamic.clone())),
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
        let report = preflight::run_preflight(&stores, &data_dir, &config, aead.as_ref()).await?;
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
                    google_config: Arc::new(config.clone()),
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
                    google_config: Arc::new(config.clone()),
                    clock: clock.clone(),
                },
                replica_id: identity.id,
                metadata_hook: subscription_metadata_hook.clone(),
                cancel: refresh_cancel.clone(),
                apalis_handle: scheduler_lazy_handle.clone(),
                meta: storage.clone(),
            })),
        });
    let lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>> =
        lazy_refresher_concrete.as_ref().map(|refresher| {
            refresher.clone() as Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>
        });
    let initial_view = build_dynamic_view(
        &stores,
        config.oauth.gemini.as_ref(),
        config.oauth.antigravity.as_ref(),
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
        gemini_config: config.oauth.gemini.clone(),
        antigravity_config: config.oauth.antigravity.clone(),
    }));
    let initial_notify_rx = notify_listener.try_subscribe_once().await;
    let notifier_task = {
        let notifier = Arc::clone(&notifier);
        let cancel = notify_cancel.clone();
        Some(tokio::spawn(async move {
            if let Err(error) = notifier.run(cancel).await {
                tracing::error!(error = %error, "runtime change notifier task failed");
            }
        }))
    };
    let notify_listener_task = Some(tokio::spawn(async move {
        match initial_notify_rx {
            Some(rx) => notify_listener.run_with_initial(rx).await,
            None => notify_listener.run().await,
        }
    }));
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
    let lifecycle_dispatcher_handle = in_memory_bus
        .spawn_lifecycle_dispatcher(cc_lb_control::event_bus::DEFAULT_LIFECYCLE_ASSEMBLER_CAPACITY);
    let (event_fanout_shutdown_tx, event_fanout_shutdown_rx) = watch::channel(false);
    let mut event_fanout_tasks = Vec::new();
    #[cfg(not(feature = "storage-postgres"))]
    {
        let _ = &event_fanout_shutdown_rx;
        let _ = &mut event_fanout_tasks;
        let _ = &storage_tail_tx;
    }
    let event_bus: Arc<dyn cc_lb_control::RequestEventBus> = match &config.storage {
        cc_lb_config::StorageConfig::Sqlite { .. } => {
            let _ = &storage_tail_tx;
            Arc::new(in_memory_bus.clone())
        }
        cc_lb_config::StorageConfig::Postgres { .. } => {
            #[cfg(not(feature = "storage-postgres"))]
            {
                return Err(BuildError::StorageFactory(
                    crate::storage_factory::StorageFactoryError::FeatureDisabled {
                        backend: "postgres".to_owned(),
                    },
                ));
            }
            #[cfg(feature = "storage-postgres")]
            {
                let pg_pool = storage_factory::open_pg_fanout_pool(&config.storage)
                    .await?
                    .ok_or(BuildError::PgNotifyPoolUnavailable)?;
                let (notify_tx, notify_rx) =
                    tokio::sync::mpsc::channel(cc_lb_event_pipeline::PARTIAL_NOTIFY_MPSC_CAPACITY);
                let notify_producer_id = uuid::Uuid::new_v4().to_string();

                event_fanout_tasks.push(cc_lb_event_pipeline::PgNotifier::spawn_with_channel(
                    pg_pool.clone(),
                    notify_rx,
                    notify_producer_id.clone(),
                    config.event_bus.pg_notify_channel.clone(),
                    event_fanout_shutdown_rx.clone(),
                ));

                if let Some(storage_tail_tx) = storage_tail_tx {
                    let listener_bus: Arc<dyn cc_lb_control::RequestEventBus> =
                        Arc::new(in_memory_bus.clone());
                    event_fanout_tasks.push(cc_lb_event_pipeline::PgListener::spawn_with_channel(
                        pg_pool,
                        listener_bus,
                        config.event_bus.pg_notify_channel.clone(),
                        notify_producer_id,
                        event_fanout_shutdown_rx.clone(),
                    ));

                    let initial_storage_tail_cursor =
                        storage.current_request_event_cursor().await?;
                    event_fanout_tasks.push(cc_lb_event_pipeline::StorageTailPoller::spawn(
                        storage.clone(),
                        storage_tail_tx,
                        Duration::from_millis(
                            config.event_bus.storage_tail_poll_interval_ms.max(1),
                        ),
                        initial_storage_tail_cursor,
                        event_fanout_shutdown_rx.clone(),
                    ));
                } else {
                    tracing::debug!(
                        "split proxy mode: skipping local PgListener and StorageTailPoller"
                    );
                }

                Arc::new(cc_lb_event_pipeline::PgNotifyFanout::new(
                    in_memory_bus.clone(),
                    notify_tx,
                ))
            }
        }
    };
    let lifecycle_event_logger_handle =
        cc_lb_event_pipeline::spawn_lifecycle_event_logger(lifecycle_event_logger_rx);
    let diagnostics = if config.observability.request_diagnostics_log {
        cc_lb_event_pipeline::RequestDiagnosticsOptions {
            sink: Some(Arc::new(
                cc_lb_event_pipeline::StdoutRequestDiagnosticsSink::new()
                    .map_err(BuildError::RequestDiagnosticsSink)?,
            )),
        }
    } else {
        cc_lb_event_pipeline::RequestDiagnosticsOptions::default()
    };
    let lifecycle_event_assembler_handle =
        cc_lb_event_pipeline::lifecycle_event_assembler::spawn_with_ttls(
            lifecycle_assembler_rx,
            storage.clone(),
            Some(event_bus.clone()),
            Arc::clone(&metrics_hook),
            cc_lb_event_pipeline::lifecycle_event_assembler::DEFAULT_ASSEMBLER_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_event_assembler::DEFAULT_ASSEMBLER_TTL,
            lifecycle_max_ttl,
            diagnostics,
        );
    let lifecycle_pricing_subscriber_handle =
        cc_lb_pricing::lifecycle_pricing_subscriber::spawn_with_ttls(
            lifecycle_pricing_rx,
            event_bus.clone(),
            cc_lb_pricing::lifecycle_pricing_subscriber::DEFAULT_PRICING_MAP_CAP,
            cc_lb_pricing::lifecycle_pricing_subscriber::DEFAULT_PRICING_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_limit_reconcile_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_limit_reconcile_subscriber::spawn_with_ttls(
            lifecycle_limit_reconcile_rx,
            limit_engine.clone(),
            cc_lb_event_pipeline::lifecycle_limit_reconcile_subscriber::DEFAULT_LIMIT_RECONCILE_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_limit_reconcile_subscriber::DEFAULT_LIMIT_RECONCILE_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_rate_limit_header_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_rate_limit_header_subscriber::spawn_with_ttls(
            lifecycle_rate_limit_header_rx,
            Arc::clone(&initial_view.upstream_rate_limit_cache),
            Some(upstream_rate_limit_sink.clone()),
            cc_lb_event_pipeline::lifecycle_rate_limit_header_subscriber::DEFAULT_RATE_LIMIT_HEADER_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_rate_limit_header_subscriber::DEFAULT_RATE_LIMIT_HEADER_TTL,
            lifecycle_max_ttl,
        );
    let cache: Arc<dyn cc_lb_control::SubscriptionQuotaCacheLike> =
        subscription_quota_cache.clone();
    let lifecycle_subscription_quota_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_subscription_quota_subscriber::spawn_with_ttls(
            lifecycle_subscription_quota_rx,
            Some(cache),
            Some(subscription_quota_sink.clone()),
            cc_lb_event_pipeline::lifecycle_subscription_quota_subscriber::DEFAULT_SUBSCRIPTION_QUOTA_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_subscription_quota_subscriber::DEFAULT_SUBSCRIPTION_QUOTA_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_limit_rejection_audit_subscriber_handle =
        cc_lb_event_pipeline::spawn_lifecycle_limit_rejection_audit_subscriber(
            lifecycle_limit_rejection_audit_rx,
            audit_sink.clone(),
        );
    let lifecycle_api_key_metrics_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_api_key_metrics_subscriber::spawn_with_ttls(
            lifecycle_api_key_metrics_rx,
            cc_lb_event_pipeline::lifecycle_api_key_metrics_subscriber::DEFAULT_API_KEY_METRICS_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_api_key_metrics_subscriber::DEFAULT_API_KEY_METRICS_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_cache_hit_miss_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_cache_hit_miss_subscriber::spawn_with_ttls(
            lifecycle_cache_hit_miss_rx,
            Arc::clone(&metrics_hook),
            cc_lb_event_pipeline::lifecycle_cache_hit_miss_subscriber::DEFAULT_CACHE_HIT_MISS_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_cache_hit_miss_subscriber::DEFAULT_CACHE_HIT_MISS_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_routing_tier_subscriber_handle =
        cc_lb_event_pipeline::lifecycle_routing_tier_subscriber::spawn_with_ttls(
            lifecycle_routing_tier_rx,
            Arc::clone(&metrics_hook),
            cc_lb_event_pipeline::lifecycle_routing_tier_subscriber::DEFAULT_ROUTING_TIER_MAP_CAP,
            cc_lb_event_pipeline::lifecycle_routing_tier_subscriber::DEFAULT_ROUTING_TIER_TTL,
            lifecycle_max_ttl,
        );
    let lifecycle_dispatcher_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_control::LifecycleDispatcherHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(lifecycle_dispatcher_handle)));
    let lifecycle_event_logger_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::LifecycleEventLoggerHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(lifecycle_event_logger_handle)));
    let lifecycle_event_assembler_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::RequestEventAssemblerHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_event_assembler_handle,
    )));
    let lifecycle_pricing_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_pricing::PricingSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_pricing_subscriber_handle,
    )));
    let lifecycle_limit_reconcile_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::LimitReconcileSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_limit_reconcile_subscriber_handle,
    )));
    let lifecycle_rate_limit_header_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::RateLimitHeaderSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_rate_limit_header_subscriber_handle,
    )));
    let lifecycle_subscription_quota_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::SubscriptionQuotaSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_subscription_quota_subscriber_handle,
    )));
    let lifecycle_limit_rejection_audit_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::LimitRejectionAuditSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_limit_rejection_audit_subscriber_handle,
    )));
    let lifecycle_api_key_metrics_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::ApiKeyMetricsSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_api_key_metrics_subscriber_handle,
    )));
    let lifecycle_cache_hit_miss_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::CacheHitMissSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_cache_hit_miss_subscriber_handle,
    )));
    let lifecycle_routing_tier_subscriber_slot: Arc<
        tokio::sync::Mutex<Option<cc_lb_event_pipeline::RoutingTierSubscriberHandle>>,
    > = Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_routing_tier_subscriber_handle,
    )));
    let keepalive_dispatcher = Arc::new(
        AnthropicKeepaliveDispatcher::new(
            Arc::clone(&dynamic_view_holder),
            Arc::clone(&stores.upstreams),
            Arc::clone(&dispatcher),
            clock.clone(),
        )
        .with_timeout(Duration::from_secs(
            config.scheduler.keepalive_dispatch_timeout_secs,
        )),
    );
    let cache_keepalive_enqueuer = Arc::new(
        crate::cache_keepalive_enqueuer::ServerCacheKeepaliveEnqueuer::new(
            crate::cache_keepalive_enqueuer::ServerCacheKeepaliveEnqueuerDeps {
                storage: Arc::clone(&storage),
                pusher: Arc::new(opened_scheduler.backend.clone()),
                aead: Arc::clone(&aead),
                clock: clock.clone(),
                request_timeout: Duration::from_secs(config.timeouts.upstream_total_secs.max(1)),
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
    let signals = signal::install(
        drain_controller.clone(),
        Duration::from_secs(config.timeouts.drain_secs),
        sighup_handler(reload_tls_state),
    );
    {
        // Drain the single-channel bus dispatcher before downstream subscribers/assembler shut down.
        let dispatcher_slot = lifecycle_dispatcher_slot.clone();
        signals.add_shutdown_hook(move || {
            let dispatcher_slot = dispatcher_slot.clone();
            async move {
                let mut guard = dispatcher_slot.lock().await;
                if let Some(handle) = guard.take() {
                    handle.shutdown().await;
                }
            }
        });
    }
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
    let anthropic_org_api =
        AnthropicOrgApi::production().map_err(BuildError::AnthropicOrgOrigin)?;
    let scheduler_cancel = CancellationToken::new();
    let (scheduler_ctx, limit_reset_usage_refresh, warmup_window_verifier) =
        crate::scheduler_dispatch::build_scheduler_ctx(
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
                anthropic_org_api: anthropic_org_api.clone(),
                openai_org_api: cc_lb_control::openai_org_api::OpenAiOrgApi::production()
                    .expect("OpenAI production organization origin parses"),
            },
        );
    #[cfg(not(feature = "admin"))]
    let _ = (&limit_reset_usage_refresh, &warmup_window_verifier);
    let scheduler_tasks = if role == ProxyRole::ProxyOnly {
        Vec::new()
    } else {
        opened_scheduler
            .spawn(scheduler_ctx, scheduler_cancel.clone())
            .await?
    };
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
        gemini_config: config.oauth.gemini.clone(),
        antigravity_config: config.oauth.antigravity.clone(),
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
        breaker_registry,
        breaker_config,
        websocket_bulkheads,
        websocket_bulkhead_config,
    };

    let router = match role {
        ProxyRole::WorkerOnly => health_router(state),
        ProxyRole::ProxyWorker | ProxyRole::ProxyOnly => {
            app_router(state, config.timeouts.upstream_total_secs)
        }
    };

    Ok(RuntimeCore {
        router,
        proxy_addr: config.listener.proxy_addr,
        #[cfg(feature = "admin")]
        admin_addr: config.listener.admin_addr,
        shutdown: ShutdownRuntime {
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
        },
        #[cfg(feature = "admin")]
        stores,
        #[cfg(feature = "admin")]
        runtime,
        #[cfg(feature = "admin")]
        aead,
        #[cfg(feature = "admin")]
        storage,
        #[cfg(feature = "admin")]
        key_store,
        #[cfg(feature = "admin")]
        lazy_refresher_concrete,
        #[cfg(feature = "admin")]
        lazy_refresher,
        #[cfg(feature = "admin")]
        warmup_window_verifier,
        #[cfg(feature = "admin")]
        limit_reset_usage_refresh,
        #[cfg(feature = "admin")]
        dynamic_view,
        #[cfg(feature = "admin")]
        scheduler_lazy_handle,
        #[cfg(feature = "admin")]
        data_dir,
        #[cfg(feature = "admin")]
        config,
        #[cfg(feature = "admin")]
        replica_identity,
        #[cfg(feature = "admin")]
        subscription_quota_cache,
        #[cfg(feature = "admin")]
        prompt_cache_grace_margin_secs,
        #[cfg(feature = "admin")]
        prompt_cache_observation_sink,
        #[cfg(feature = "admin")]
        start_time,
        #[cfg(feature = "admin")]
        event_bus,
        #[cfg(feature = "admin")]
        anthropic_org_api,
    })
}

pub(super) fn spawn_wasmtime_pool_metrics_publisher(
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

pub(super) fn hot_engine_config_from_config(
    config: &Config,
) -> cc_lb_runtime_wasmtime::HotEngineConfig {
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

pub(super) struct ServerMetadataRefreshEnqueue {
    pub(super) scheduler_backend: crate::scheduler_factory::SchedulerBackend,
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

pub(super) struct ReconcilerParams {
    pub(super) stores: Arc<DynamicStores>,
    pub(super) holder: Arc<DynamicViewHolder>,
    pub(super) runtime: Arc<WasmtimeRuntime>,
    pub(super) aead: Arc<AeadService>,
    pub(super) lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub(super) cancel: CancellationToken,
    pub(super) data_dir: PathBuf,
    pub(super) subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub(super) prompt_cache_grace_margin_secs: u64,
    pub(super) prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    pub(super) subscription_quota_routing_max_staleness_secs: u64,
    pub(super) clock: ClockHandle,
    pub(super) gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
    pub(super) antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
}

pub(super) fn spawn_reconciler(params: ReconcilerParams) {
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
        params.gemini_config,
        params.antigravity_config,
    ));
    tokio::spawn(reconciler.run());
}

pub(super) fn spawn_reconcile_shutdown(shutdown: watch::Receiver<bool>, cancel: CancellationToken) {
    tokio::spawn(async move {
        signal::wait_for_shutdown(shutdown).await;
        cancel.cancel();
    });
}

pub(super) fn lifecycle_config_from_config(
    config: &Config,
    body_caps: RequestBodyCaps,
    replica_identity: Option<cc_lb_domain::ReplicaIdentity>,
) -> LifecycleConfig {
    LifecycleConfig {
        messages_body_cap_bytes: body_caps.messages,
        files_body_cap_bytes: body_caps.files,
        replica_identity,
        upstream_affinity_ttl: std::time::Duration::from_secs(config.upstream_affinity.ttl_secs()),
        unknown_openai_reasoning: config.upstream_affinity.unknown_openai_reasoning,
        openai_oauth_compat: config.openai_oauth_compat,
        openai_realtime_origin: config.openai_public_api.realtime_origin.clone(),
    }
}

pub(super) fn cap_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(super) fn build_tls_state(config: &Config) -> Result<Option<Arc<TlsState>>, BuildError> {
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

pub(super) fn active_tls_config(config: &Config) -> Option<&TlsConfig> {
    config.listener.tls.as_ref()
}

pub(super) fn sighup_handler(tls_state: Option<Arc<TlsState>>) -> Option<signal::SighupHandler> {
    tls_state.map(|tls_state| {
        Arc::new(move || {
            if let Err(error) = tls_state.reload() {
                tracing::warn!(error = %error, "TLS reload failed");
            }
        }) as signal::SighupHandler
    })
}

#[derive(Clone)]
pub(super) struct ProxyState {
    pub(super) lifecycle: Arc<Lifecycle>,
    pub(super) body_caps: RequestBodyCaps,
    pub(super) server_state: Arc<ServerStateHandle>,
    pub(super) start_time: std::time::Instant,
    pub(super) drain_controller: DrainController,
    pub(super) aead: Arc<AeadService>,
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dynamic_view: Arc<DynamicViewHolder>,
    pub(super) builtin_authn: Arc<BuiltinAuthn>,
    pub(super) clock: ClockHandle,
    pub(super) breaker_registry: Arc<BreakerRegistry>,
    pub(super) breaker_config: BreakerRuntimeConfig,
    pub(super) websocket_bulkheads: Arc<BulkheadRegistry>,
    pub(super) websocket_bulkhead_config: BulkheadRuntimeConfig,
}

pub(super) fn spawn_price_catalog_local_installer(
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

pub(super) fn install_default_fallback_if_uninitialized(
    price_catalog: &cc_lb_pricing::PriceCatalog,
    clock: &dyn cc_lb_clock::Clock,
) {
    if !matches!(price_catalog.status(), cc_lb_pricing::CatalogStatus::Ok) {
        price_catalog.install_snapshot(claude_default_snapshot(clock));
    }
}

pub(super) fn claude_default_snapshot(
    clock: &dyn cc_lb_clock::Clock,
) -> cc_lb_pricing::CatalogSnapshot {
    use cc_lb_pricing::{
        CatalogSnapshot, CatalogStatus, Pricing, ThresholdBand, ThresholdRate, UsdPerMillion,
    };
    use std::collections::{BTreeMap, HashMap};

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
        ("claude-opus-5-5", 4_000_000, 20_000_000, 5_000_000, 200_000),
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
        (
            "claude-sonnet-5-5",
            2_000_000,
            10_000_000,
            2_500_000,
            100_000,
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
        ("claude-haiku-5-5", 100_000, 500_000, 125_000, 10_000),
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
                native: None,
                model: (*name).to_owned(),
                input_per_million_usd: UsdPerMillion::from_micros_usd(*input),
                output_per_million_usd: UsdPerMillion::from_micros_usd(*output),
                input_image_per_million_usd: None,
                output_image_per_million_usd: None,
                by_tier: Default::default(),
            },
        );
        cache_creation.insert(
            (*name).to_owned(),
            UsdPerMillion::from_micros_usd(*creation),
        );
        cache_read.insert((*name).to_owned(), UsdPerMillion::from_micros_usd(*read));
    }

    // Anthropic charges 5x base rates strictly above 100,000 gross input tokens
    // for Claude Haiku 5.5 (`above_tokens_rates` triggers strictly above threshold).
    let mut above_tokens_rates = HashMap::new();
    above_tokens_rates.insert(
        "claude-haiku-5-5".to_owned(),
        BTreeMap::from([(
            100_000,
            ThresholdBand {
                default: ThresholdRate {
                    input_per_million_usd: Some(UsdPerMillion::from_micros_usd(500_000)),
                    output_per_million_usd: Some(UsdPerMillion::from_micros_usd(2_500_000)),
                    cache_creation_per_million_usd: Some(UsdPerMillion::from_micros_usd(625_000)),
                    cache_read_per_million_usd: Some(UsdPerMillion::from_micros_usd(50_000)),
                },
                by_tier: BTreeMap::new(),
            },
        )]),
    );

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
        above_tokens_rates,
        status: CatalogStatus::Ok,
    }
}

pub(super) fn server_join_result(
    result: Result<Result<(), io::Error>, JoinError>,
) -> Result<(), BuildError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(BuildError::from(source)),
        Err(source) if source.is_cancelled() => Ok(()),
        Err(source) => Err(BuildError::from(io::Error::other(source.to_string()))),
    }
}

pub(super) fn init_observability(config: &Config) -> Result<TracingGuard, BuildError> {
    let guard = cc_lb_observability::init(&ObservabilityConfig {
        tracing_level: config.observability.tracing_level.clone(),
        otlp_endpoint: config.observability.otlp_endpoint.clone(),
        metrics_addr: config.listener.metrics_addr,
        log_redaction: config.observability.log_redaction,
        user_prompt_redaction: config.observability.user_prompt_redaction,
    })
    .map_err(BuildError::from)?;
    #[cfg(any(feature = "storage-sqlite", feature = "storage-postgres"))]
    cc_lb_scheduler::scheduler_metrics::touch_scheduler_metric_handles();
    Ok(guard)
}

pub(super) type OpenStorageParts = (
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
    open_storage_for_role(config, ProxyRole::ProxyWorker, clock).await
}

pub async fn open_storage_for_role(
    config: &Config,
    role: ProxyRole,
    clock: ClockHandle,
) -> Result<OpenStorageParts, BuildError> {
    let aead = cc_lb_runtime_sync::aead_key::load_aead_from_env(&config.aead.key_env).map_err(
        |error| match error {
            cc_lb_runtime_sync::aead_key::AeadKeyError::Missing { env } => {
                BuildError::StorageKeyMissing { env }
            }
            cc_lb_runtime_sync::aead_key::AeadKeyError::Invalid => BuildError::InvalidStorageKey,
        },
    )?;
    let opened = storage_factory::open_storage(&config.storage, clock.clone()).await?;
    let scheduler_role = match role {
        ProxyRole::ProxyOnly => crate::scheduler_factory::SchedulerPoolRole::EnqueueOnly,
        ProxyRole::ProxyWorker | ProxyRole::WorkerOnly => {
            crate::scheduler_factory::SchedulerPoolRole::Worker
        }
    };
    let opened_scheduler = crate::scheduler_factory::open_scheduler_storage_with_role(
        &config.storage,
        &config.scheduler,
        scheduler_role,
        clock,
    )
    .await?;
    let lazy_refresh_claim_guard = crate::refresh::lazy_refresh_claim_guard_from_scheduler(
        &opened_scheduler.backend,
        opened.storage.clone(),
    );
    Ok((
        opened.managed_key_store,
        opened.storage,
        aead,
        lazy_refresh_claim_guard,
        opened_scheduler,
    ))
}

pub(super) fn dispatcher(
    config: &Config,
    clock: ClockHandle,
) -> (
    Arc<dyn UpstreamDispatch>,
    Arc<BreakerRegistry>,
    BreakerRuntimeConfig,
    Arc<BulkheadRegistry>,
    BulkheadRuntimeConfig,
) {
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
    let bulkhead_registry = Arc::new(BulkheadRegistry::new());
    let breaker_upstream_name = upstream_name.clone();
    let breaker_registry_for_dispatcher = breaker_registry.clone();
    let breaker_clock = clock;
    let dispatcher_factory = Arc::new(move |max_idle_per_host| {
        let base = make_default_dispatcher(max_idle_per_host);
        let base = Arc::new(input_tokens_ws::InputTokensWebsocketDispatch::new(base));
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
            bulkhead_registry.clone(),
            bulkhead_config,
            upstream_name.clone(),
            dispatcher_factory,
        )),
        breaker_registry,
        breaker_config,
        bulkhead_registry,
        bulkhead_config,
    )
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use cc_lb_storage_api::MetaStore as _;
    use tower::ServiceExt as _;

    async fn build_test_proxy_app(role: ProxyRole) -> ProxyApp {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let key = [0u8; 32];
        let aead = Arc::new(AeadService::from_master_key(key));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");
        let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
        let storage: Arc<dyn Storage> = storage_arc;
        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Sqlite { path: sqlite_path },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        std::mem::forget(dir);
        build_proxy_app_with_storage(config, role, managed_store, storage, aead, clock)
            .await
            .expect("build_proxy_app_with_storage")
    }

    #[tokio::test]
    async fn proxy_only_spawns_zero_scheduler_tasks_while_worker_roles_spawn_them() {
        let proxy_only = build_test_proxy_app(ProxyRole::ProxyOnly).await;
        assert!(
            proxy_only.shutdown.scheduler_tasks.is_empty(),
            "ProxyOnly must not spawn apalis scheduler worker tasks",
        );

        let proxy_worker = build_test_proxy_app(ProxyRole::ProxyWorker).await;
        assert!(
            !proxy_worker.shutdown.scheduler_tasks.is_empty(),
            "ProxyWorker must spawn scheduler worker tasks",
        );

        let worker_only = build_test_proxy_app(ProxyRole::WorkerOnly).await;
        assert!(
            !worker_only.shutdown.scheduler_tasks.is_empty(),
            "WorkerOnly must spawn scheduler worker tasks",
        );
    }

    #[tokio::test]
    async fn worker_only_serves_health_only_and_rejects_proxy_and_admin_routes() {
        let app = build_test_proxy_app(ProxyRole::WorkerOnly).await;

        let health = app
            .router
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("healthz request"),
            )
            .await
            .expect("healthz response");
        assert_eq!(health.status(), StatusCode::OK);

        let messages = app
            .router
            .clone()
            .oneshot(
                Request::post("/v1/messages")
                    .body(Body::empty())
                    .expect("messages request"),
            )
            .await
            .expect("messages response");
        assert_eq!(messages.status(), StatusCode::NOT_FOUND);

        let admin = app
            .router
            .oneshot(
                Request::get("/admin/health")
                    .body(Body::empty())
                    .expect("admin request"),
            )
            .await
            .expect("admin response");
        assert_eq!(admin.status(), StatusCode::NOT_FOUND);
    }
}
