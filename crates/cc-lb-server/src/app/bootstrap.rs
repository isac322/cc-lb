use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::Request as HttpRequest;
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
    MetricDnsResolver, SubscriptionQuotaSink, UpstreamDispatch,
    cache_keepalive::AnthropicKeepaliveDispatcher, dns_cache::DnsResolverConfig,
    make_shared_dispatcher,
};
use cc_lb_event_pipeline::{
    SubscriptionQuotaWriterConfig, UpstreamRateLimitSink, start_subscription_quota_writer,
    start_upstream_rate_limit_writer,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{ManagedKeyStore, RuntimeChangeNotifier, Storage};
use cc_lb_upstream::SignedRequest;
use futures_util::stream::FuturesUnordered;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperAutoBuilder;
use hyper_util::service::TowerToHyperService;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

use crate::dynamic_view_builder::{
    DynamicViewPluginScope, Stores as DynamicStores, VertexState, build_dynamic_view_with_scope,
    ensure_wasm_cache_dirs,
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
pub(super) const DURABLE_WRITER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const SCHEDULER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
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
    pub(super) reconcile_cancel: Option<CancellationToken>,
    pub(super) refresh_cancel: Option<CancellationToken>,
    pub(super) price_catalog_install_cancel: Option<CancellationToken>,
    pub(super) price_catalog_install_task: Option<JoinHandle<()>>,
    pub(super) wasmtime_pool_metrics_cancel: Option<CancellationToken>,
    pub(super) wasmtime_pool_metrics_task: Option<JoinHandle<()>>,
    pub(super) scheduler_cancel: Option<CancellationToken>,
    pub(super) scheduler_tasks_slot: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>>,
    pub(super) audit_writer_task: Option<JoinHandle<()>>,
    pub(super) upstream_rate_limit_writer_task: Option<JoinHandle<()>>,
    pub(super) subscription_quota_writer_cancel: Option<CancellationToken>,
    pub(super) subscription_quota_writer_task: Option<JoinHandle<()>>,
    pub(super) prompt_cache_observation_writer_cancel: Option<CancellationToken>,
    pub(super) prompt_cache_observation_writer_task: Option<JoinHandle<()>>,
    pub(super) event_fanout_shutdown_tx: Option<watch::Sender<bool>>,
    pub(super) event_fanout_tasks: Vec<JoinHandle<()>>,
    pub(super) signals: signal::SignalHandle,
    pub(super) drain_controller: DrainController,
    pub(super) tls_state: Option<Arc<TlsState>>,
    pub(super) tls_handshake_timeout: std::time::Duration,
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

pub fn serve_error_exit_code(error: &ServeError) -> std::process::ExitCode {
    match error {
        ServeError::Config(_)
        | ServeError::Preflight(_)
        | ServeError::Build(
            BuildError::Config(_)
            | BuildError::Preflight(_)
            | BuildError::StorageKeyMissing { .. }
            | BuildError::InvalidStorageKey,
        ) => std::process::ExitCode::from(2),
        _ => std::process::ExitCode::FAILURE,
    }
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
        let scheduler_workers = self
            .shutdown
            .scheduler_tasks_slot
            .try_lock()
            .map_or(0, |tasks| tasks.len());
        tracing::info!(
            role = ?self.role,
            scheduler_workers,
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
    admin: Option<(Router, SocketAddr, cc_lb_control::PostCommitTracker)>,
) -> Result<(), BuildError> {
    let ShutdownRuntime {
        lifecycle,
        notify_cancel,
        notifier_task,
        notify_listener_task,
        reconcile_cancel,
        refresh_cancel,
        price_catalog_install_cancel,
        price_catalog_install_task,
        wasmtime_pool_metrics_cancel,
        wasmtime_pool_metrics_task,
        scheduler_cancel,
        scheduler_tasks_slot,
        audit_writer_task,
        upstream_rate_limit_writer_task,
        subscription_quota_writer_cancel,
        subscription_quota_writer_task,
        prompt_cache_observation_writer_cancel,
        prompt_cache_observation_writer_task,
        event_fanout_shutdown_tx,
        event_fanout_tasks,
        signals,
        drain_controller: _,
        tls_state,
        tls_handshake_timeout,
        server_state,
    } = shutdown;
    server_state.wait_for_ready().await;
    let proxy_listener = TcpListener::bind(proxy_addr).await?;
    cc_lb_runtime_sync::warn_if_container_loopback_bind("proxy", proxy_addr);
    let admin_handle = if let Some((admin_router, admin_addr, admin_post_commit_tracker)) = admin {
        let admin_listener = TcpListener::bind(admin_addr).await?;
        cc_lb_runtime_sync::warn_if_container_loopback_bind("admin", admin_addr);
        let (admin_stop_tx, admin_shutdown) = watch::channel(false);
        let admin_task = tokio::spawn(serve_admin_until_shutdown(
            admin_listener,
            admin_router,
            admin_shutdown,
            SHUTDOWN_TASK_TIMEOUT,
        ));
        Some((admin_stop_tx, admin_task, admin_post_commit_tracker))
    } else {
        None
    };

    let proxy_shutdown = signals.subscribe();
    let mut proxy = if let Some(tls_state) = tls_state {
        tokio::spawn(async move {
            let proxy_listener = ReloadableListener::with_handshake_timeout(
                proxy_listener,
                tls_state,
                tls_handshake_timeout,
            );
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

    // The proxy task ended. When a shutdown run started it, wait for request
    // drain to finish (`hooks_started`) before applying `SHUTDOWN_HOOKS_TIMEOUT`
    // so long graceful drains do not steal budget from shutdown hooks. Then wait
    // for `drain_complete`, bounded by SHUTDOWN_HOOKS_TIMEOUT so a wedged hook
    // cannot hold the process forever. A proxy that stopped on its own with no
    // shutdown started keeps the old behavior: no hooks are running, so there is
    // nothing to wait for.
    if signals.is_shutdown_started() {
        signal::wait_for_shutdown(signals.subscribe_hooks_started()).await;
        if tokio::time::timeout(
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
    }

    // 1. Stop polling loops and scheduler workers before shutting down durable writers.
    if let Some(cancel) = notify_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = reconcile_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = refresh_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = price_catalog_install_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = wasmtime_pool_metrics_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = scheduler_cancel {
        cancel.cancel();
    }

    // 2. Await non-durable polling and admin HTTP tasks.
    if let Some(task) = notifier_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = notify_listener_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = price_catalog_install_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some(task) = wasmtime_pool_metrics_task {
        cc_lb_runtime_sync::await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
    }
    if let Some((admin_stop_tx, admin_task, admin_post_commit_tracker)) = admin_handle {
        admin_post_commit_tracker.close();
        let _ = admin_stop_tx.send(true);
        // `serve_admin_until_shutdown` internally bounds connection draining to
        // `SHUTDOWN_TASK_TIMEOUT` and then aborts + joins all live connections; give the outer
        // await enough headroom to let that abort-and-join loop complete before waiting on
        // post-commit tasks spawned by active handlers.
        cc_lb_runtime_sync::await_or_abort(
            admin_task,
            SHUTDOWN_TASK_TIMEOUT + Duration::from_secs(2),
        )
        .await;
        if tokio::time::timeout(
            DURABLE_WRITER_SHUTDOWN_TIMEOUT,
            admin_post_commit_tracker.wait_idle(),
        )
        .await
        .is_err()
        {
            tracing::warn!(
                in_flight = admin_post_commit_tracker.in_flight(),
                timeout_secs = DURABLE_WRITER_SHUTDOWN_TIMEOUT.as_secs(),
                "timed out waiting for detached admin post-commit tasks to drain",
            );
        }
    }

    // 3. Await any remaining scheduler tasks concurrently under one shared 10s deadline.
    let scheduler_tasks = std::mem::take(&mut *scheduler_tasks_slot.lock().await);
    await_tasks_with_shared_timeout(scheduler_tasks, SCHEDULER_SHUTDOWN_TIMEOUT, "scheduler").await;

    // 4. Signal durable writers to drain once producers, hooks, and scheduler stopped.
    if let Some(cancel) = subscription_quota_writer_cancel {
        cancel.cancel();
    }
    if let Some(cancel) = prompt_cache_observation_writer_cancel {
        cancel.cancel();
    }

    // 5. Await durable writer tasks concurrently under a 5s shared deadline.
    let durable_writer_tasks: Vec<_> = [
        audit_writer_task,
        upstream_rate_limit_writer_task,
        subscription_quota_writer_task,
        prompt_cache_observation_writer_task,
    ]
    .into_iter()
    .flatten()
    .collect();
    await_tasks_with_shared_timeout(
        durable_writer_tasks,
        DURABLE_WRITER_SHUTDOWN_TIMEOUT,
        "durable_writers",
    )
    .await;

    // 6. Shut down cross-replica event fanout tasks concurrently under a 5s shared deadline.
    if let Some(tx) = event_fanout_shutdown_tx {
        let _ = tx.send(true);
    }
    await_tasks_with_shared_timeout(
        event_fanout_tasks,
        DURABLE_WRITER_SHUTDOWN_TIMEOUT,
        "event_fanout",
    )
    .await;

    // 7. Flush remaining lifecycle observer tasks.
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

async fn await_tasks_with_shared_timeout(
    tasks: Vec<JoinHandle<()>>,
    budget: Duration,
    label: &'static str,
) {
    if tasks.is_empty() {
        return;
    }
    let abort_handles: Vec<_> = tasks.iter().map(JoinHandle::abort_handle).collect();
    let mut pending: FuturesUnordered<_> = tasks.into_iter().collect();
    if tokio::time::timeout(budget, async { while pending.next().await.is_some() {} })
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_secs = budget.as_secs(),
            task_group = label,
            "shutdown task group exceeded shared deadline; aborting remaining tasks"
        );
        for handle in abort_handles {
            handle.abort();
        }
        while pending.next().await.is_some() {}
    }
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
    let data_dir = resolve_data_dir(None, config.runtime.data_dir.as_deref(), "CC_LB_DATA_DIR")?;
    let storage_for_dynamic = storage.clone();
    ensure_wasm_cache_dirs(&data_dir)?;
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
    if let Some(startup_preflight) = startup_preflight {
        let report = preflight::run_preflight(&stores, &data_dir, &config, aead.as_ref()).await?;
        print_preflight_report(&report);
        if startup_preflight.strict_preflight && !report.warnings.is_empty() {
            return Err(BuildError::Preflight(
                preflight::PreflightError::StrictWarnings {
                    warning_count: report.warnings.len(),
                },
            ));
        }
    }
    let key_store = match &config.storage {
        cc_lb_config::StorageConfig::Postgres { .. } => {
            if let Some(conn_state) = storage.connection_state() {
                Arc::new(KeyStore::new(managed_store).with_positive_cache(conn_state))
            } else {
                Arc::new(KeyStore::new(managed_store))
            }
        }
        cc_lb_config::StorageConfig::Sqlite { .. } => {
            Arc::new(KeyStore::new(managed_store).with_positive_cache(Arc::new(
                cc_lb_storage_api::NotifierConnectionState::new_always_connected(),
            )))
        }
    };
    let thread_sticky_subagent_latch = Arc::new(cc_lb_engine::ThreadStickySubagentLatch::new());
    let price_catalog = cc_lb_pricing::global_catalog().clone();
    let (price_catalog_install_cancel, price_catalog_install_task) =
        spawn_price_catalog_local_installer(
            &config,
            storage.clone(),
            price_catalog.clone(),
            clock.clone(),
        )
        .await;
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
    let runtime =
        cc_lb_runtime_sync::wasmtime_builder::build_wasmtime_runtime(&config.runtime.wasmtime)
            .map_err(BuildError::WasmtimeRuntimeInit)?;
    let (wasmtime_pool_metrics_cancel, wasmtime_pool_metrics_task) =
        spawn_wasmtime_pool_metrics_publisher(runtime.clone());

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
    let DispatcherParts {
        dispatcher,
        dns_resolver,
        upstream_connect_timeout,
        breaker_registry,
        breaker_config,
        http_bulkheads,
        websocket_bulkheads,
        websocket_bulkhead_config,
    } = dispatcher(&config, clock.clone());
    let replica_identity = Some(cc_lb_domain::ReplicaIdentity {
        id: replica::load_or_create_replica_id_or_ephemeral(&data_dir),
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
    let prompt_cache_observation_writer_cancel = CancellationToken::new();
    let (sink, writer) = PromptCacheObservationSink::new_with_cancel(
        stores.prompt_cache_observations.clone(),
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
        observation_store_kind,
        prompt_cache_observation_writer_cancel.clone(),
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
    let plugin_scope = if role == ProxyRole::WorkerOnly {
        DynamicViewPluginScope::WarmupOnly
    } else {
        DynamicViewPluginScope::All
    };
    let initial_view = build_dynamic_view_with_scope(
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
        plugin_scope,
    )
    .await?;
    let dynamic_view_holder = Arc::new(wire_dynamic_view_eviction(
        DynamicViewHolder::new(initial_view.clone()),
        Arc::clone(&breaker_registry),
        Arc::clone(&http_bulkheads),
        Arc::clone(&websocket_bulkheads),
    ));
    let notify_cancel = CancellationToken::new();
    let notifier: Arc<dyn RuntimeChangeNotifier> = storage_for_dynamic.clone();
    let notify_listener = Arc::new(
        NotifyListener::new(NotifyListenerParams {
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
            key_store: Some(Arc::clone(&key_store)),
            thread_sticky_subagent_latch: Some(Arc::clone(&thread_sticky_subagent_latch)),
            clock: clock.clone(),
            gemini_config: config.oauth.gemini.clone(),
            antigravity_config: config.oauth.antigravity.clone(),
        })
        .with_plugin_scope(plugin_scope),
    );
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
    lifecycle = lifecycle
        .with_thread_sticky_subagent_latch(thread_sticky_subagent_latch)
        .with_thread_sticky_subagent_store(
            storage.clone() as Arc<dyn cc_lb_storage_api::ThreadStickySubagentStore>
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
    let scheduler_cancel = CancellationToken::new();
    let scheduler_tasks_slot: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    {
        // Stop claiming new background scheduler jobs as soon as graceful drain begins.
        signals.add_drain_start_cancel_token(scheduler_cancel.clone());
    }
    {
        // Join active scheduler workers BEFORE draining/shutting down downstream
        // subscribers (`api_key_usage_slot`, `lifecycle_dispatcher_slot`, etc.) so
        // in-flight cache-keepalive finalizers flush into `LimitEngine` first.
        let scheduler_cancel_for_hook = scheduler_cancel.clone();
        let scheduler_tasks_slot = scheduler_tasks_slot.clone();
        signals.add_shutdown_hook(move || {
            let scheduler_cancel_for_hook = scheduler_cancel_for_hook.clone();
            let scheduler_tasks_slot = scheduler_tasks_slot.clone();
            async move {
                scheduler_cancel_for_hook.cancel();
                let tasks = std::mem::take(&mut *scheduler_tasks_slot.lock().await);
                await_tasks_with_shared_timeout(tasks, SCHEDULER_SHUTDOWN_TIMEOUT, "scheduler")
                    .await;
            }
        });
    }
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
        // Drain pricing BEFORE shutting down assembler & logger so `Priced` events
        // emitted during shutdown are merged onto finalized request rows.
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
    *scheduler_tasks_slot.lock().await = scheduler_tasks;
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
        plugin_scope,
    });
    let state = ProxyState {
        lifecycle: lifecycle.clone(),
        body_caps,
        server_state: server_state.clone(),
        start_time,
        drain_controller: drain_controller.clone(),
        aead: aead.clone(),
        storage: storage.clone(),
        dynamic_view: dynamic_view.clone(),
        clock: clock.clone(),
        breaker_registry,
        breaker_config,
        websocket_bulkheads,
        websocket_bulkhead_config,
        dns_resolver,
        upstream_connect_timeout,
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
            reconcile_cancel: Some(reconcile_cancel),
            refresh_cancel: Some(refresh_cancel),
            price_catalog_install_cancel: Some(price_catalog_install_cancel),
            price_catalog_install_task: Some(price_catalog_install_task),
            wasmtime_pool_metrics_cancel: Some(wasmtime_pool_metrics_cancel),
            wasmtime_pool_metrics_task: Some(wasmtime_pool_metrics_task),
            scheduler_cancel: Some(scheduler_cancel),
            scheduler_tasks_slot,
            audit_writer_task: Some(audit_writer_task),
            upstream_rate_limit_writer_task: Some(upstream_rate_limit_writer_task),
            subscription_quota_writer_cancel: Some(subscription_quota_writer_cancel),
            subscription_quota_writer_task: Some(subscription_quota_writer_task),
            prompt_cache_observation_writer_cancel: Some(prompt_cache_observation_writer_cancel),
            prompt_cache_observation_writer_task,
            event_fanout_shutdown_tx: Some(event_fanout_shutdown_tx),
            event_fanout_tasks,
            signals,
            drain_controller,
            tls_state,
            tls_handshake_timeout: active_tls_config(&config)
                .map(|tls| std::time::Duration::from_secs(tls.handshake_timeout_secs))
                .unwrap_or(crate::tls::DEFAULT_TLS_HANDSHAKE_TIMEOUT),
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

#[cfg(test)]
pub(super) fn hot_engine_config_from_config(
    config: &Config,
) -> cc_lb_runtime_wasmtime::HotEngineConfig {
    cc_lb_runtime_sync::wasmtime_builder::hot_engine_config_from_wasmtime_config(
        &config.runtime.wasmtime,
    )
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
    pub(super) plugin_scope: DynamicViewPluginScope,
}

pub(super) fn spawn_reconciler(params: ReconcilerParams) {
    let reconciler = Arc::new(
        Reconciler::new(
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
        )
        .with_plugin_scope(params.plugin_scope),
    );
    tokio::spawn(reconciler.run());
}

pub(super) fn lifecycle_config_from_config(
    config: &Config,
    body_caps: RequestBodyCaps,
    replica_identity: Option<cc_lb_domain::ReplicaIdentity>,
) -> LifecycleConfig {
    LifecycleConfig {
        messages_body_cap_bytes: body_caps.messages,
        files_body_cap_bytes: body_caps.files,
        max_buffered_response_bytes: cap_to_usize(config.body.max_buffered_response_bytes),
        replica_identity,
        upstream_affinity_ttl: std::time::Duration::from_secs(config.upstream_affinity.ttl_secs()),
        unknown_openai_reasoning: config.upstream_affinity.unknown_openai_reasoning,
        unavailable_openai_reasoning_owner: config
            .upstream_affinity
            .unavailable_openai_reasoning_owner,
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
    pub(super) clock: ClockHandle,
    pub(super) breaker_registry: Arc<BreakerRegistry>,
    pub(super) breaker_config: BreakerRuntimeConfig,
    pub(super) websocket_bulkheads: Arc<BulkheadRegistry>,
    pub(super) websocket_bulkhead_config: BulkheadRuntimeConfig,
    pub(super) dns_resolver: MetricDnsResolver,
    pub(super) upstream_connect_timeout: Duration,
}

pub(super) async fn spawn_price_catalog_local_installer(
    config: &Config,
    storage: Arc<dyn Storage>,
    price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
    clock: ClockHandle,
) -> (CancellationToken, JoinHandle<()>) {
    let cfg = &config.price_catalog;
    let storage_cache: Arc<dyn cc_lb_storage_api::PriceCatalogCache> = storage;
    let loader = cc_lb_pricing::LiteLlmLoader::new(
        price_catalog.clone(),
        storage_cache,
        cfg.url.clone(),
        cfg.cache_path.clone(),
        clock.clone(),
    );
    if tokio::time::timeout(
        Duration::from_secs(5),
        run_price_catalog_local_install(&loader),
    )
    .await
    .is_err()
    {
        tracing::warn!("timed out waiting for initial LiteLLM price catalog local install");
    }
    install_default_fallback_if_uninitialized(&price_catalog, &*clock);

    tracing::info!(
        local_install_interval_secs = PRICE_CATALOG_LOCAL_INSTALL_INTERVAL.as_secs(),
        cache_path = %cfg.cache_path.display(),
        "starting LiteLLM price catalog local installer",
    );
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PRICE_CATALOG_LOCAL_INSTALL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::select! {
            _ = task_cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
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

pub(super) async fn serve_admin_until_shutdown(
    mut listener: TcpListener,
    router: Router,
    shutdown_rx: watch::Receiver<bool>,
    drain_timeout: Duration,
) -> io::Result<()> {
    let (conn_stop_tx, conn_stop_rx) = watch::channel(false);
    let mut shutdown = std::pin::pin!(signal::wait_for_shutdown(shutdown_rx));
    let mut conn_tasks = JoinSet::new();

    loop {
        tokio::select! {
            Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => {}
            (stream, remote_addr) = axum::serve::Listener::accept(&mut listener) => {
                spawn_admin_connection(
                    &mut conn_tasks,
                    &router,
                    conn_stop_rx.clone(),
                    stream,
                    remote_addr,
                );
            }
            _ = &mut shutdown => {
                let _ = conn_stop_tx.send(true);
                break;
            }
        }
    }

    drop(listener);
    drop(router);

    if !conn_tasks.is_empty()
        && tokio::time::timeout(drain_timeout, async {
            while conn_tasks.join_next().await.is_some() {}
        })
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_ms = drain_timeout.as_millis() as u64,
            active_connections = conn_tasks.len(),
            "combined admin graceful drain elapsed with active connections; aborting connections",
        );
        conn_tasks.abort_all();
        while conn_tasks.join_next().await.is_some() {}
    }

    Ok(())
}

fn spawn_admin_connection(
    conn_tasks: &mut JoinSet<()>,
    router: &Router,
    mut stop_rx: watch::Receiver<bool>,
    stream: TcpStream,
    remote_addr: SocketAddr,
) {
    let tower_service = router
        .clone()
        .map_request(|req: HttpRequest<Incoming>| req.map(Body::new));
    let hyper_service = TowerToHyperService::new(tower_service);
    let io = TokioIo::new(stream);

    conn_tasks.spawn(async move {
        let mut builder = HyperAutoBuilder::new(TokioExecutor::new());
        builder.http2().enable_connect_protocol();
        let mut conn = std::pin::pin!(builder.serve_connection_with_upgrades(io, hyper_service));
        let mut graceful_sent = false;

        loop {
            tokio::select! {
                result = conn.as_mut() => {
                    if let Err(err) = result {
                        tracing::trace!(
                            peer = %remote_addr,
                            error = %err,
                            "combined admin connection closed with error",
                        );
                    }
                    break;
                }
                wait_res = stop_rx.wait_for(|stopped| *stopped), if !graceful_sent => {
                    graceful_sent = true;
                    let _ = wait_res;
                    conn.as_mut().graceful_shutdown();
                }
            }
        }
    });
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

pub(super) fn wire_dynamic_view_eviction(
    holder: DynamicViewHolder,
    breaker_registry: Arc<BreakerRegistry>,
    http_bulkheads: Arc<BulkheadRegistry>,
    websocket_bulkheads: Arc<BulkheadRegistry>,
) -> DynamicViewHolder {
    holder.with_on_store(Arc::new(move |view| {
        let snapshot = view.upstreams_snapshot();
        let live_http: std::collections::HashSet<String> =
            snapshot.iter().map(|u| u.id.to_string()).collect();
        let live_ws: std::collections::HashSet<String> = snapshot
            .iter()
            .map(|u| websocket_breaker_key(u.id))
            .collect();
        breaker_registry.retain_keys(|key| live_http.contains(key) || live_ws.contains(key));
        http_bulkheads.retain_keys(|key| live_http.contains(key));
        websocket_bulkheads.retain_keys(|key| live_ws.contains(key));
    }))
}

pub(super) struct DispatcherParts {
    pub(super) dispatcher: Arc<dyn UpstreamDispatch>,
    pub(super) dns_resolver: MetricDnsResolver,
    pub(super) upstream_connect_timeout: Duration,
    pub(super) breaker_registry: Arc<BreakerRegistry>,
    pub(super) breaker_config: BreakerRuntimeConfig,
    pub(super) http_bulkheads: Arc<BulkheadRegistry>,
    pub(super) websocket_bulkheads: Arc<BulkheadRegistry>,
    pub(super) websocket_bulkhead_config: BulkheadRuntimeConfig,
}

pub(super) fn dispatcher(config: &Config, clock: ClockHandle) -> DispatcherParts {
    let bulkhead_config = BulkheadRuntimeConfig::from(config.bulkhead.clone());
    let breaker_config = BreakerRuntimeConfig::from(config.circuit_breaker.clone());
    let upstream_connect_timeout =
        Duration::from_secs(config.timeouts.upstream_connect_secs.max(1));
    let dns_resolver =
        MetricDnsResolver::new(&DnsResolverConfig::default()).expect("default DNS resolver builds");
    let upstream_name = Arc::new(|request: &SignedRequest| {
        request
            .upstream_id()
            .map(|id| id.to_string())
            .or_else(|| request.url().host_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| cc_lb_observability::UNRESOLVED_ATTRIBUTION_LABEL.to_owned())
    });
    let breaker_registry = Arc::new(BreakerRegistry::new());
    let http_bulkheads = Arc::new(BulkheadRegistry::new());
    let websocket_bulkheads = Arc::new(BulkheadRegistry::new());
    let base = make_shared_dispatcher(
        dns_resolver.clone(),
        Some(upstream_connect_timeout),
        bulkhead_config.max_conns_per_upstream as usize,
    );
    let base = Arc::new(input_tokens_ws::InputTokensWebsocketDispatch::new(
        base,
        dns_resolver.clone(),
        upstream_connect_timeout,
    ));
    let shared_inner: Arc<dyn UpstreamDispatch> = Arc::new(CircuitBreakerDispatch::new(
        base,
        breaker_registry.clone(),
        breaker_config,
        upstream_name.clone(),
        clock,
    ));
    let dispatcher_factory = Arc::new(move |_| Arc::clone(&shared_inner));
    DispatcherParts {
        dispatcher: Arc::new(BulkheadDispatch::with_dispatcher_factory(
            http_bulkheads.clone(),
            bulkhead_config,
            upstream_name,
            dispatcher_factory,
        )),
        dns_resolver,
        upstream_connect_timeout,
        breaker_registry,
        breaker_config,
        http_bulkheads,
        websocket_bulkheads,
        websocket_bulkhead_config: bulkhead_config,
    }
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use cc_lb_storage_api::MetaStore as _;

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
            proxy_only
                .shutdown
                .scheduler_tasks_slot
                .lock()
                .await
                .is_empty(),
            "ProxyOnly must not spawn apalis scheduler worker tasks",
        );

        let proxy_worker = build_test_proxy_app(ProxyRole::ProxyWorker).await;
        assert!(
            !proxy_worker
                .shutdown
                .scheduler_tasks_slot
                .lock()
                .await
                .is_empty(),
            "ProxyWorker must spawn scheduler worker tasks",
        );

        let worker_only = build_test_proxy_app(ProxyRole::WorkerOnly).await;
        assert!(
            !worker_only
                .shutdown
                .scheduler_tasks_slot
                .lock()
                .await
                .is_empty(),
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

    #[test]
    fn serve_error_exit_code_maps_build_preflight_to_exit_code_2() {
        let err = ServeError::Build(BuildError::Preflight(
            crate::preflight::PreflightError::StrictWarnings { warning_count: 1 },
        ));
        assert_eq!(serve_error_exit_code(&err), std::process::ExitCode::from(2));
    }

    #[tokio::test]
    async fn strict_preflight_returns_typed_error_without_exiting_process() {
        use cc_lb_storage_api::{
            PluginChainEntryInput, PluginRegistryStore as _, PluginSlotKind, PrincipalCreate,
            PrincipalKind, PrincipalStore as _, WasmBlob, WasmRegistryEntryInput,
        };

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");

        let principal = storage_arc
            .create(
                PrincipalCreate {
                    name: "strict-preflight-principal".to_owned(),
                    kind: PrincipalKind::Machine,
                    allowed_models: vec!["*".to_owned()],
                    allowed_upstreams: Vec::new(),
                    default_limits: Vec::new(),
                    cache_keepalive: None,
                    openai_oauth_compat: None,
                },
                1_800_000_000,
            )
            .await
            .expect("create principal");
        let (entry, _) = storage_arc
            .persist_wasm_upload(
                WasmBlob {
                    sha256: [7u8; 32],
                    bytes: b"wasm-stub".to_vec(),
                    size_bytes: 9,
                },
                WasmRegistryEntryInput {
                    name: "shape-only-plugin".to_owned(),
                    version: None,
                    original_filename: "shape.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1_800_000_000,
                    uploaded_by_admin_id: uuid::Uuid::from_u128(1),
                    description: String::new(),
                    usage: String::new(),
                    hook_metadata: Default::default(),
                    supported_slots: vec![PluginSlotKind::Shape],
                },
            )
            .await
            .expect("persist wasm");
        storage_arc
            .insert_chain_entry(PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Router,
                order: 0,
                wasm_registry_id: entry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("insert unsupported slot chain entry");

        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Sqlite { path: sqlite_path },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        let opened_scheduler = crate::scheduler_factory::open_scheduler_storage_with_role(
            &config.storage,
            &config.scheduler,
            crate::scheduler_factory::SchedulerPoolRole::EnqueueOnly,
            clock.clone(),
        )
        .await
        .expect("open scheduler");

        let err = match build_runtime_with_storage_core(
            config,
            ProxyRole::ProxyOnly,
            None,
            storage_arc.clone() as Arc<dyn ManagedKeyStore>,
            storage_arc as Arc<dyn Storage>,
            aead,
            Some(StartupPreflight {
                strict_preflight: true,
            }),
            opened_scheduler,
            None,
            clock,
        )
        .await
        {
            Ok(_) => {
                panic!("strict preflight must return BuildError::Preflight when warnings exist")
            }
            Err(err) => err,
        };

        assert!(
            matches!(
                err,
                BuildError::Preflight(crate::preflight::PreflightError::StrictWarnings {
                    warning_count
                }) if warning_count >= 1
            ),
            "unexpected error: {err:?}"
        );
    }

    #[cfg(feature = "admin")]
    #[tokio::test]
    async fn pricing_subscriber_flushes_before_assembler_during_app_shutdown() {
        use cc_lb_storage_api::RequestEventStore as _;

        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("storage.sqlite");
        let aead = Arc::new(AeadService::from_master_key([0u8; 32]));
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let storage_arc = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage_arc.initialize().await.expect("init sqlite");
        let mut config = Config {
            storage: cc_lb_config::StorageConfig::Sqlite { path: sqlite_path },
            ..Config::default()
        };
        config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        let opened_scheduler = crate::scheduler_factory::open_scheduler_storage_with_role(
            &config.storage,
            &config.scheduler,
            crate::scheduler_factory::SchedulerPoolRole::EnqueueOnly,
            clock.clone(),
        )
        .await
        .expect("open scheduler");

        let core = build_runtime_with_storage_core(
            config,
            ProxyRole::ProxyOnly,
            None,
            storage_arc.clone() as Arc<dyn ManagedKeyStore>,
            storage_arc.clone() as Arc<dyn Storage>,
            aead,
            None,
            opened_scheduler,
            None,
            clock,
        )
        .await
        .expect("runtime builds");

        // Hold an in-flight request across shutdown trigger, publish lifecycle events
        // that require `PricingSubscriber` -> `Priced` before assembler finalizes,
        // then drop the guard so shutdown hooks execute in sequence.
        let guard = core
            .shutdown
            .drain_controller
            .try_track()
            .expect("drain guard acquired");
        core.event_bus
            .publish_lifecycle(cc_lb_lifecycle::LifecycleEvent::RequestStarted {
                event_id: "evt-shutdown-price".to_owned(),
                request_id: "req-shutdown-price".to_owned(),
                ts_ms: 1_700_000_000_000,
                stream: false,
                source_kind: None,
                source_ref_id: None,
                event_kind: None,
                protocol: None,
            });
        core.event_bus
            .publish_lifecycle(cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                event_id: "evt-shutdown-price".to_owned(),
                result: Ok(cc_lb_lifecycle::ParseInfo {
                    path: "/v1/messages".to_owned(),
                    method: "POST".to_owned(),
                    model: Some("claude-sonnet-4-5-20250929".to_owned()),
                    ..Default::default()
                }),
            });
        core.event_bus
            .publish_lifecycle(cc_lb_lifecycle::LifecycleEvent::UsageObserved {
                event_id: "evt-shutdown-price".to_owned(),
                usage: cc_lb_lifecycle::UsageSnapshot {
                    input_tokens: 1_000_000,
                    output_tokens: 1_000_000,
                    ..Default::default()
                },
                source: cc_lb_lifecycle::UsageSource::Final,
            });
        core.event_bus
            .publish_lifecycle(cc_lb_lifecycle::LifecycleEvent::RequestTerminated {
                event_id: "evt-shutdown-price".to_owned(),
                reason: cc_lb_lifecycle::TerminationReason::Success,
                client_status: 200,
                duration_ms: 15,
                request_body_read_ms: None,
                request_body_bytes: None,
                proxy_setup_ms: None,
                setup_timings: cc_lb_lifecycle::RequestSetupTimings::default(),
                io_timings: cc_lb_lifecycle::RequestIoTimings::default(),
                upstream_ttfb_ms: None,
                upstream_body_ms: None,
                dns_ms: None,
                connect_ms: None,
                connection_reused: None,
                first_content_delta_ms: None,
                first_body_chunk_ms: None,
                finalize_ms: None,
                internal_errors: Vec::new(),
                event_kind: None,
                funding_ambiguous: false,
                protocol: None,
            });

        let drain_complete = core.shutdown.signals.subscribe_drain_complete();
        core.shutdown.signals.start_shutdown();
        drop(guard);

        tokio::time::timeout(
            Duration::from_secs(10),
            signal::wait_for_shutdown(drain_complete),
        )
        .await
        .expect("shutdown hooks complete cleanly");

        let row = storage_arc
            .get_request_event("evt-shutdown-price")
            .await
            .expect("query persisted row")
            .expect("shutdown-flushed request event row exists");
        assert!(
            row.cost_usd_micros.is_some_and(|micros| micros > 0),
            "pricing subscriber must flush `Priced` before assembler shuts down; got {:?}",
            row.cost_usd_micros
        );
        drop(core);
    }

    #[tokio::test]
    async fn scheduler_cancels_at_drain_start_and_joins_before_post_usage_hooks() {
        let app = build_test_proxy_app(ProxyRole::ProxyWorker).await;
        let signals = app.signal_handle();
        let drain = app.drain_controller();
        let scheduler_cancel = app
            .shutdown
            .scheduler_cancel
            .clone()
            .expect("scheduler_cancel set");
        let guard = drain.try_track().expect("acquire drain guard");

        let (sched_cancelled_tx, sched_cancelled_rx) = tokio::sync::oneshot::channel();
        let (sched_release_tx, sched_release_rx) = tokio::sync::oneshot::channel::<()>();
        let sched_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sched_finished_task = Arc::clone(&sched_finished);
        {
            let mut tasks = app.shutdown.scheduler_tasks_slot.lock().await;
            tasks.push(tokio::spawn(async move {
                scheduler_cancel.cancelled().await;
                let _ = sched_cancelled_tx.send(());
                let _ = sched_release_rx.await;
                sched_finished_task.store(true, std::sync::atomic::Ordering::Release);
            }));
        }

        let (after_usage_tx, after_usage_rx) = tokio::sync::oneshot::channel();
        let after_usage_tx = Arc::new(std::sync::Mutex::new(Some(after_usage_tx)));
        let sched_finished_hook = Arc::clone(&sched_finished);
        // Registered after `api_key_usage_slot` (and every built-in shutdown hook) at startup.
        signals.add_shutdown_hook(move || {
            let after_usage_tx = Arc::clone(&after_usage_tx);
            let sched_finished_hook = Arc::clone(&sched_finished_hook);
            async move {
                let observed = sched_finished_hook.load(std::sync::atomic::Ordering::Acquire);
                if let Ok(mut slot) = after_usage_tx.lock()
                    && let Some(tx) = slot.take()
                {
                    let _ = tx.send(observed);
                }
            }
        });

        let hooks_started = signals.subscribe_hooks_started();
        let drain_complete = signals.subscribe_drain_complete();
        signals.start_shutdown();

        tokio::time::timeout(Duration::from_secs(2), sched_cancelled_rx)
            .await
            .expect("scheduler_cancel fires on drain start while request guard is held")
            .expect("scheduler cancelled channel sends");
        assert!(
            !*hooks_started.borrow(),
            "hooks_started must stay false while drain guard is active"
        );

        drop(guard);
        tokio::time::timeout(
            Duration::from_secs(2),
            signal::wait_for_shutdown(hooks_started),
        )
        .await
        .expect("hooks_started fires after drain guard drops");

        assert!(
            !sched_finished.load(std::sync::atomic::Ordering::Acquire),
            "gated scheduler task has not been released yet"
        );
        let _ = sched_release_tx.send(());

        let saw_scheduler_joined = tokio::time::timeout(Duration::from_secs(5), after_usage_rx)
            .await
            .expect("post-usage shutdown hook runs")
            .expect("post-usage hook sends");
        assert!(
            saw_scheduler_joined,
            "scheduler tasks must join before api_key_usage_slot and downstream shutdown hooks run"
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            signal::wait_for_shutdown(drain_complete),
        )
        .await
        .expect("drain_complete signals");
        drop(app);
    }

    #[tokio::test]
    async fn await_tasks_with_shared_timeout_awaits_aborted_original_workers() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let completed = tokio::spawn(async {});
        let worker_dropped = Arc::new(AtomicBool::new(false));
        let worker_dropped_flag = DropFlag(Arc::clone(&worker_dropped));
        let worker_started = Arc::new(tokio::sync::Notify::new());
        let worker_started_task = Arc::clone(&worker_started);
        let stalled = tokio::spawn(async move {
            let _guard = worker_dropped_flag;
            worker_started_task.notify_one();
            std::future::pending::<()>().await;
        });
        worker_started.notified().await;
        await_tasks_with_shared_timeout(
            vec![completed, stalled],
            Duration::from_millis(25),
            "test-workers",
        )
        .await;
        assert!(
            worker_dropped.load(Ordering::Acquire),
            "original worker Drop guard must complete before await_tasks_with_shared_timeout returns"
        );
    }

    #[tokio::test]
    async fn serve_admin_until_shutdown_aborts_and_joins_hung_connections_before_returning() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::AsyncWriteExt as _;

        struct DropGuard(Arc<AtomicBool>);
        impl Drop for DropGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_handler = Arc::clone(&entered);
        let dropped_handler = Arc::clone(&dropped);

        let router = Router::new().route(
            "/hung",
            axum::routing::get(move || {
                let entered = Arc::clone(&entered_handler);
                let dropped = Arc::clone(&dropped_handler);
                async move {
                    let _guard = DropGuard(dropped);
                    entered.notify_one();
                    std::future::pending::<StatusCode>().await
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind admin listener");
        let addr = listener.local_addr().expect("local addr");
        let (stop_tx, stop_rx) = watch::channel(false);
        let server_task = tokio::spawn(serve_admin_until_shutdown(
            listener,
            router,
            stop_rx,
            Duration::from_millis(100),
        ));

        let mut client = TcpStream::connect(addr).await.expect("connect admin");
        client
            .write_all(b"GET /hung HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write request");

        entered.notified().await;
        assert!(!dropped.load(Ordering::Acquire));

        let _ = stop_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("admin server shuts down cleanly")
            .expect("server task joins")
            .expect("server returns Ok");

        assert!(
            dropped.load(Ordering::Acquire),
            "hung admin connection handler must be aborted and joined before serve_admin_until_shutdown returns"
        );
    }

    #[tokio::test]
    async fn serve_admin_until_shutdown_tracks_http2_mutations_and_rejects_after_close() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let tracker = cc_lb_control::PostCommitTracker::default();
        let handler_entered = Arc::new(tokio::sync::Notify::new());
        let (release_handler_tx, release_handler_rx) = tokio::sync::oneshot::channel::<()>();
        let release_handler_rx = Arc::new(tokio::sync::Mutex::new(Some(release_handler_rx)));
        let (release_post_commit_tx, release_post_commit_rx) =
            tokio::sync::oneshot::channel::<()>();
        let release_post_commit_rx =
            Arc::new(tokio::sync::Mutex::new(Some(release_post_commit_rx)));
        let post_commit_done = Arc::new(AtomicBool::new(false));
        let post_commit_blocked_entered = Arc::new(AtomicBool::new(false));

        let tracker_route = tracker.clone();
        let entered_route = Arc::clone(&handler_entered);
        let post_commit_done_route = Arc::clone(&post_commit_done);
        let router = Router::new().route(
            "/mutate",
            axum::routing::post(move || {
                let tracker = tracker_route.clone();
                let entered = Arc::clone(&entered_route);
                let release_handler_rx = Arc::clone(&release_handler_rx);
                let release_post_commit_rx = Arc::clone(&release_post_commit_rx);
                let post_commit_done = Arc::clone(&post_commit_done_route);
                async move {
                    let Some(_permit) = tracker.try_begin_request() else {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            r#"{"error":"server_shutting_down"}"#,
                        );
                    };
                    entered.notify_one();
                    if let Some(rx) = release_handler_rx.lock().await.take() {
                        let _ = rx.await;
                    }
                    let rx_pc = release_post_commit_rx.lock().await.take();
                    tracker.spawn(async move {
                        if let Some(rx) = rx_pc {
                            let _ = rx.await;
                        }
                        post_commit_done.store(true, Ordering::Release);
                    });
                    (StatusCode::OK, "ok")
                }
            }),
        );
        let tracker_closed_route = tracker.clone();
        let blocked_flag = Arc::clone(&post_commit_blocked_entered);
        let router = router.route(
            "/mutate-second",
            axum::routing::post(move || {
                let tracker = tracker_closed_route.clone();
                let blocked_flag = Arc::clone(&blocked_flag);
                async move {
                    let Some(_permit) = tracker.try_begin_request() else {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            r#"{"error":"server_shutting_down"}"#,
                        );
                    };
                    blocked_flag.store(true, Ordering::Release);
                    (StatusCode::OK, "unexpected")
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind h2c admin listener");
        let addr = listener.local_addr().expect("local addr");
        let (stop_tx, stop_rx) = watch::channel(false);
        let server_task = tokio::spawn(serve_admin_until_shutdown(
            listener,
            router,
            stop_rx,
            Duration::from_secs(5),
        ));

        let client: hyper_util::client::legacy::Client<
            hyper_util::client::legacy::connect::HttpConnector,
            Body,
        > = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .http2_only(true)
            .build_http();

        let uri1: axum::http::Uri = format!("http://{addr}/mutate").parse().expect("uri1");
        let req1 = Request::post(uri1).body(Body::empty()).expect("h2 req1");
        let resp1_task = tokio::spawn({
            let client = client.clone();
            async move { client.request(req1).await }
        });

        handler_entered.notified().await;
        assert_eq!(tracker.in_flight(), 1);

        // Close gate before sending second HTTP/2 request on the multiplexed connection:
        // must immediately return 503 server_shutting_down without running handler body.
        tracker.close();
        let uri2: axum::http::Uri = format!("http://{addr}/mutate-second")
            .parse()
            .expect("uri2");
        let req2 = Request::post(uri2).body(Body::empty()).expect("h2 req2");
        let resp2 = client.request(req2).await.expect("h2 closed gate response");
        assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!post_commit_blocked_entered.load(Ordering::Acquire));

        let _ = stop_tx.send(true);
        let waiter = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.wait_idle().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "wait_idle must block while HTTP/2 stream still holds request permit"
        );

        let _ = release_handler_tx.send(());
        let resp1 = resp1_task
            .await
            .expect("resp1 task join")
            .expect("resp1 ok");
        assert_eq!(resp1.status(), StatusCode::OK);
        drop(resp1);
        drop(resp2);
        drop(client);

        server_task
            .await
            .expect("admin server task joins")
            .expect("admin server Ok");
        assert!(
            !waiter.is_finished(),
            "wait_idle must remain blocked for post-commit task even after HTTP/2 server finishes"
        );
        assert!(!post_commit_done.load(Ordering::Acquire));

        let _ = release_post_commit_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("wait_idle finishes once post-commit completes")
            .expect("waiter joins");
        assert!(post_commit_done.load(Ordering::Acquire));
    }
}
