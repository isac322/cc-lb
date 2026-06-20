use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
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
use cc_lb_config::{Config, DownstreamAuthMode, TlsConfig};
use cc_lb_core::{
    BreakerConfig, BreakerRegistry, BulkheadConfig, BulkheadDispatch, BulkheadRegistry,
    CircuitBreakerDispatch, DynamicView, DynamicViewBuilder, DynamicViewHolder, HopByHopStripLayer,
    Lifecycle, LifecycleConfig, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig,
    UpstreamDispatch, UpstreamRateLimitSink, anthropic_error_response,
    api_keys::{
        builtin_authn::BuiltinAuthn, concurrent_guard::KeyConcurrencyManager, key_store::KeyStore,
        limit_engine::LimitEngine, principal_view::PrincipalView,
    },
    make_default_dispatcher, spawn_audit_writer, start_subscription_quota_writer,
    start_upstream_rate_limit_writer,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_runtime_extism::registry::PluginRegistry;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    BackendKind, ManagedKeyStore, MetaStore, PluginBlobRepo, PluginRegistryRepo,
    RuntimeChangeNotifier, Storage, UpstreamRecord,
};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::rt::TokioExecutor;
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;

use crate::bootstrap;
use crate::build_meta::BuildMeta;
use crate::builtins::NoopObservabilityHook;
use crate::drain::DrainController;
use crate::dynamic_view_builder::{
    Stores as DynamicStores, build_dynamic_view, ensure_wasm_cache_dirs,
};
use crate::notify_listener::{NotifyListener, NotifyListenerParams};
use crate::preflight;
use crate::reconcile::Reconciler;
use crate::refresh::{LazyRefreshClaimGuard, LazyRefresher};
use crate::reload::{ConfigWatcher, summarize_restart_required};
use crate::replica;
use crate::signal;
use crate::startup_handshake::{
    LegacyBridgeReport, StartupHandshakeOpts, StartupHandshakeReport, bridge_legacy_wasm_registry,
    run_startup_handshake_with_slot_store,
};
use crate::state_machine::{ServerState, ServerStateHandle};
use crate::storage_factory;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use crate::tls::{ReloadableListener, TlsState};
use cc_lb_admin::{
    AdminState, ConfigDraftError, CurrentConfig, DynamicViewRebinder, WarmupDialectDispatchError,
    WarmupDialectDispatchErrorKind, WarmupDialectDispatchOutcome, WarmupDialectDispatcher,
};

const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_millis(500);
const PRICE_CATALOG_LOCAL_INSTALL_INTERVAL: Duration = Duration::from_secs(60);

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
    notify_cancel: Option<CancellationToken>,
    notifier_task: Option<JoinHandle<()>>,
    notify_listener_task: Option<JoinHandle<()>>,
    price_catalog_install_cancel: Option<CancellationToken>,
    price_catalog_install_task: Option<JoinHandle<()>>,
    scheduler_cancel: Option<CancellationToken>,
    scheduler_tasks: Vec<JoinHandle<()>>,
    audit_writer_task: Option<JoinHandle<()>>,
    upstream_rate_limit_writer_task: Option<JoinHandle<()>>,
    subscription_quota_writer_task: Option<JoinHandle<()>>,
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
    #[cfg(feature = "postgres")]
    #[error("storage connection failed: {message}")]
    StorageConnect { message: String },
    #[error(transparent)]
    Bootstrap(#[from] crate::bootstrap::BootstrapError),
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error(transparent)]
    Rebind(#[from] crate::dynamic_view_builder::RebindError),
    #[error("startup plugin re-handshake failed: {0}")]
    StartupHandshake(String),
    #[error("storage is required")]
    StorageRequired,
    #[error("storage master key env {env} is missing")]
    StorageKeyMissing { env: String },
    #[error("storage master key must be 32 bytes encoded as 64 hex characters")]
    InvalidStorageKey,
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
            notify_cancel,
            notifier_task,
            notify_listener_task,
            price_catalog_install_cancel,
            price_catalog_install_task,
            scheduler_cancel,
            scheduler_tasks,
            audit_writer_task,
            upstream_rate_limit_writer_task,
            subscription_quota_writer_task,
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

        if let Some(task) = reload_task {
            task.abort();
        }
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
        proxy_result?;
        Ok(())
    }
}

pub async fn run_serve(
    config_path: &Path,
    data_dir: Option<&Path>,
    strict_preflight: bool,
    skip_handshake_if_fresh: Option<bool>,
    force_handshake: Option<bool>,
) -> Result<(), ServeError> {
    let mut config = Config::load(config_path)?;
    if let Some(data_dir) = data_dir {
        config.runtime.data_dir = Some(data_dir.to_path_buf());
    }
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let guard = init_observability(&mut config)?;
    let startup_opts = startup_handshake_opts_from_flags(
        skip_handshake_if_fresh,
        force_handshake,
        &config.runtime.startup_handshake,
    );
    let app = match build_app_with_path_inner(
        config,
        Some(config_path),
        Some(StartupPreflight { strict_preflight }),
        startup_opts,
    )
    .await
    {
        Ok(app) => app,
        Err(error @ BuildError::SchedulerFactory(_)) => {
            cc_lb_scheduler::scheduler_metrics::set_scheduler_init_failure(true);
            guard.flush_metrics();
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

fn startup_handshake_opts_from_flags(
    skip_handshake_if_fresh: Option<bool>,
    force_handshake: Option<bool>,
    config: &cc_lb_config::StartupHandshakeConfig,
) -> StartupHandshakeOpts {
    StartupHandshakeOpts {
        skip_if_fresh: skip_handshake_if_fresh.unwrap_or(config.skip_if_fresh),
        force: force_handshake.unwrap_or(config.force),
        ..StartupHandshakeOpts::default()
    }
}

pub async fn build_app(config: Config) -> Result<App, BuildError> {
    build_app_with_path(config, None).await
}

pub async fn build_app_for_testing(mut config: Config) -> Result<App, BuildError> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("storage.sqlite");
    let key = [0u8; 32];
    let aead = Arc::new(AeadService::from_master_key(key));
    let database_url = format!("sqlite://{}", path.display());
    let storage_arc = Arc::new(cc_lb_storage_sqlite::open_sqlite(&database_url).await?);
    storage_arc.initialize(BackendKind::Sqlite).await?;
    let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
    let storage: Arc<dyn Storage> = storage_arc.clone();
    config.storage = cc_lb_config::StorageConfig::Sqlite { path };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(cc_lb_config::NoneModeConfig {
        principal_id: "test-principal".to_owned(),
        upstream_kind: cc_lb_config::NoneModeUpstreamKind::AnthropicKey,
    });
    std::mem::forget(dir);
    let plugin_registry_repo = storage_arc.clone() as Arc<dyn PluginRegistryRepo>;
    let plugin_blob_repo = storage_arc.clone() as Arc<dyn PluginBlobRepo>;
    let opened_scheduler =
        crate::scheduler_factory::open_scheduler_storage(&config.storage, &config.scheduler)
            .await?;
    build_app_with_storage_inner(
        config,
        None,
        managed_store,
        storage,
        aead,
        Some((plugin_registry_repo, plugin_blob_repo)),
        None,
        StartupHandshakeOpts::default(),
        opened_scheduler,
        None,
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
pub async fn build_app_for_testing_postgres(database_url: &str) -> Result<App, BuildError> {
    use cc_lb_storage_api::{BackendKind, Storage as StorageTrait};
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

    let init_storage: Arc<dyn StorageTrait> =
        Arc::new(cc_lb_storage_postgres::PostgresStorage::new(pool.clone()));
    init_storage
        .initialize(BackendKind::Postgres)
        .await
        .map_err(|e| BuildError::StorageConnect {
            message: e.to_string(),
        })?;
    seed_app_testing_storage(init_storage.as_ref(), None).await?;

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
    let config = build_app_for_testing_postgres_config(database_url);
    build_app_with_storage(config, None, managed_store, init_storage, aead).await
}

#[cfg(feature = "postgres")]
pub async fn seed_app_testing_storage(
    storage: &dyn Storage,
    upstream_base_url: Option<url::Url>,
) -> Result<(), BuildError> {
    use cc_lb_storage_api::principal::{Limit, LimitKind, PrincipalCreate, PrincipalKind};
    use cc_lb_storage_api::upstream::{UpstreamCreate, UpstreamKind};
    use cc_lb_storage_api::{PrincipalStore, StorageError, UpstreamStore};

    let now = unix_now_secs();
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
                api_key_ciphertext: Some(Vec::new()),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                ..UpstreamCreate::default()
            },
        )
        .await
        {
            Ok(_) | Err(StorageError::Conflict { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn build_app_for_testing_postgres_config(database_url: &str) -> Config {
    const TEST_ADMIN_TOKEN: &str = "test-token";

    let mut config = Config {
        storage: cc_lb_config::StorageConfig::Postgres {
            url: database_url.to_owned(),
            pool: cc_lb_config::PostgresPoolConfig::default(),
        },
        ..Config::default()
    };
    config.admin.token = Some(TEST_ADMIN_TOKEN.to_owned());
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::ApiKey;
    config.downstream_auth.none_mode = None;
    config
}

pub async fn build_app_with_path(
    config: Config,
    config_path: Option<&Path>,
) -> Result<App, BuildError> {
    build_app_with_path_inner(config, config_path, None, StartupHandshakeOpts::default()).await
}

async fn build_app_with_path_inner(
    config: Config,
    config_path: Option<&Path>,
    startup_preflight: Option<StartupPreflight>,
    startup_handshake_opts: StartupHandshakeOpts,
) -> Result<App, BuildError> {
    config.validate()?;
    let (
        managed_store,
        storage,
        aead,
        plugin_registry_repo,
        plugin_blob_repo,
        lazy_refresh_claim_guard,
        opened_scheduler,
    ) = open_storage(&config).await?;
    build_app_with_storage_inner(
        config,
        config_path,
        managed_store,
        storage,
        aead,
        Some((plugin_registry_repo, plugin_blob_repo)),
        startup_preflight,
        startup_handshake_opts,
        opened_scheduler,
        Some(lazy_refresh_claim_guard),
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
) -> Result<App, BuildError> {
    let opened_scheduler =
        crate::scheduler_factory::open_scheduler_storage(&config.storage, &config.scheduler)
            .await?;
    build_app_with_storage_inner(
        config,
        config_path,
        managed_store,
        storage,
        aead,
        None,
        None,
        StartupHandshakeOpts::default(),
        opened_scheduler,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_app_with_storage_inner(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    plugin_repos: Option<(Arc<dyn PluginRegistryRepo>, Arc<dyn PluginBlobRepo>)>,
    startup_preflight: Option<StartupPreflight>,
    startup_handshake_opts: StartupHandshakeOpts,
    opened_scheduler: crate::scheduler_factory::OpenedScheduler,
    lazy_refresh_claim_guard: Option<Arc<dyn LazyRefreshClaimGuard>>,
) -> Result<App, BuildError> {
    opened_scheduler.probe_leader().await?;
    let scheduler_lazy_handle = opened_scheduler.lazy_handle();
    let server_state = Arc::new(ServerStateHandle::new_starting());
    let key_store = Arc::new(KeyStore::new(managed_store));
    let price_catalog = cc_lb_pricing::global_catalog().clone();
    let (price_catalog_install_cancel, price_catalog_install_task) =
        spawn_price_catalog_local_installer(&config, storage.clone(), price_catalog.clone());
    let (sink, audit_writer_task) = spawn_audit_writer(storage.clone(), 1024);
    let audit_sink = Some(Arc::new(sink));
    let (upstream_rate_limit_sink, upstream_rate_limit_receiver) = UpstreamRateLimitSink::new();
    let upstream_rate_limit_writer_task =
        start_upstream_rate_limit_writer(storage.clone(), upstream_rate_limit_receiver);
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
            dedup_elapsed_override_secs: config.subscription_quota.dedup_elapsed_override_secs,
        },
        subscription_quota_writer_cancel.clone(),
    );
    let subscription_metadata_hook = Some(cc_lb_core::start_subscription_metadata_hook(Arc::new(
        ServerMetadataRefreshEnqueue {
            handle: scheduler_lazy_handle.clone(),
        },
    )));
    let runtime = Arc::new(ExtismRuntime::new());
    let data_dir = resolve_data_dir(None, config.runtime.data_dir.as_deref(), "CC_LB_DATA_DIR")?;
    let storage_for_dynamic = storage.clone();
    let env_token = std::env::var("CC_LB_BOOTSTRAP_ADMIN_TOKEN").ok();
    bootstrap::apply_bootstrap(
        &config,
        storage.as_ref(),
        storage.as_ref(),
        storage.as_ref(),
        env_token,
        &data_dir,
    )
    .await?;
    ensure_wasm_cache_dirs(&data_dir)?;

    let concurrent_mgr = Arc::new(KeyConcurrencyManager::new());
    let limit_engine = LimitEngine::new(concurrent_mgr);
    limit_engine.startup_replay(storage.clone()).await;
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        config.downstream_auth.mode.clone(),
        config.downstream_auth.none_mode.clone(),
        Some(key_store.clone()),
    ));

    let (_dispatcher, _breaker_registry) = dispatcher(&config);

    let replica_identity = {
        match replica::load_or_create_replica_id(&data_dir) {
            Ok(id) => {
                let started_at_unix_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Some(cc_lb_core::ReplicaIdentity {
                    id,
                    started_at_unix_secs,
                })
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to load or create replica ID; proceeding without it");
                None
            }
        }
    };

    let (plugin_registry_repo, plugin_blob_repo) = match plugin_repos {
        Some(repos) => repos,
        None => (
            storage.clone() as Arc<dyn PluginRegistryRepo>,
            storage.clone() as Arc<dyn PluginBlobRepo>,
        ),
    };
    let stores = Arc::new(DynamicStores {
        upstreams: storage_for_dynamic.clone(),
        principals: storage_for_dynamic.clone(),
        plugin_registry: storage_for_dynamic.clone(),
        upstream_rate_limits: storage_for_dynamic.clone(),
        upstream_subscription_quotas: storage_for_dynamic.clone(),
        prompt_cache_observations: storage_for_dynamic.clone(),
        anthropic_compatibility_kv: storage_for_dynamic.clone(),
        audit: Some(storage_for_dynamic.clone()),
        plugin_registry_repo: Some(plugin_registry_repo.clone()),
    });
    let lifecycle_config = LifecycleConfig {
        messages_body_cap_bytes: cap_to_usize(config.body.messages_cap_bytes),
        files_body_cap_bytes: cap_to_usize(config.body.files_cap_bytes),
        replica_identity,
        prompt_cache_shadow: config.prompt_cache_shadow.clone(),
    };
    let scheduler_replica_id = lifecycle_config
        .replica_identity
        .as_ref()
        .map(|identity| identity.id);
    if let Some(startup_preflight) = startup_preflight {
        let report = preflight::run_preflight(&stores, &lifecycle_config, &data_dir).await?;
        print_preflight_report(&report);
        if startup_preflight.strict_preflight && !report.warnings.is_empty() {
            eprintln!(
                "preflight: strict mode failed with {} warning(s)",
                report.warnings.len()
            );
            std::process::exit(1);
        }
    }
    let (startup_shutdown, startup_shutdown_triggered, startup_shutdown_task) =
        spawn_startup_shutdown_signal();
    let plugin_registry = PluginRegistry::new(
        plugin_registry_repo.clone(),
        plugin_blob_repo.clone(),
        cc_lb_runtime_extism::handshake::build_offer(&BTreeSet::new()),
    )
    .map_err(|error| BuildError::StartupHandshake(error.to_string()))?;
    let startup_report = run_startup_handshake_with_slot_store(
        &plugin_registry,
        plugin_registry_repo.as_ref(),
        storage.clone(),
        startup_handshake_opts,
        startup_shutdown,
    )
    .await;
    startup_shutdown_task.abort();
    let _ = startup_shutdown_task.await;
    log_startup_handshake_report(&startup_report);
    if startup_shutdown_triggered.load(Ordering::SeqCst) {
        return Err(BuildError::StartupHandshake(
            "startup interrupted by shutdown signal".to_owned(),
        ));
    }
    backfill_supported_slots(storage.as_ref()).await;
    backfill_wire_version(storage.as_ref()).await;
    if let Some((_, error)) = startup_report
        .errors
        .iter()
        .find(|(sha256, _)| *sha256 == [0; 32])
    {
        return Err(BuildError::StartupHandshake(error.to_string()));
    }
    let legacy_bridge_report =
        bridge_legacy_wasm_registry(&plugin_registry, storage_for_dynamic.as_ref()).await;
    log_legacy_bridge_report(&legacy_bridge_report);
    let oauth_anthropic = config.oauth.anthropic.clone().unwrap_or_default();
    let oauth_cfg = Arc::new(oauth_anthropic.clone());
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
                },
                identity.id,
                subscription_metadata_hook.clone(),
                refresh_cancel.clone(),
                claim_guard,
                scheduler_lazy_handle.clone(),
            )),
            None => Arc::new(LazyRefresher::new(
                stores.clone(),
                aead.clone(),
                oauth_cfg.clone(),
                identity.id,
                subscription_metadata_hook.clone(),
                refresh_cancel.clone(),
                scheduler_lazy_handle.clone(),
            )),
        });
    let lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>> =
        lazy_refresher_concrete.as_ref().map(|refresher| {
            refresher.clone() as Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>
        });
    let initial_view = build_dynamic_view(
        &stores,
        &oauth_anthropic,
        aead.clone(),
        lazy_refresher.clone(),
        0,
        &runtime,
        &data_dir,
        subscription_quota_cache.clone(),
        config.subscription_quota.routing_max_staleness_secs,
        &config,
    )
    .await?;
    let dynamic_view_holder = Arc::new(DynamicViewHolder::new(initial_view));
    let notify_cancel = CancellationToken::new();
    let notifier: Arc<dyn RuntimeChangeNotifier> = storage_for_dynamic.clone();
    let notifier_task = {
        let notifier = Arc::clone(&notifier);
        let cancel = notify_cancel.clone();
        Some(tokio::spawn(async move {
            if let Err(error) = notifier.run(cancel).await {
                tracing::error!(error = %error, "runtime change notifier task failed");
            }
        }))
    };
    let notify_listener = Arc::new(NotifyListener::new(NotifyListenerParams {
        notifier,
        cancel: notify_cancel.clone(),
        holder: Arc::clone(&dynamic_view_holder),
        stores: Arc::clone(&stores),
        oauth_cfg: Arc::new(oauth_anthropic.clone()),
        runtime: Arc::clone(&runtime),
        aead: aead.clone(),
        data_dir: data_dir.clone(),
        lazy_refresher: lazy_refresher.clone(),
        subscription_quota_cache: subscription_quota_cache.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        config: Arc::new(config.clone()),
    }));
    let notify_listener_task = Some(tokio::spawn(async move {
        notify_listener.run().await;
    }));
    let mut lifecycle = Lifecycle::new_with_dynamic_view(
        builtin_authn.clone(),
        dynamic_view_holder,
        lifecycle_config,
    );
    if let Some(audit_sink) = audit_sink.clone() {
        lifecycle = lifecycle.with_audit_sink(audit_sink);
    }
    lifecycle = lifecycle.with_limit_engine(limit_engine.clone(), builtin_authn.clone());
    lifecycle = lifecycle.with_request_event_storage(storage.clone());
    lifecycle = lifecycle.with_upstream_rate_limit_sink(upstream_rate_limit_sink);
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
        Some((_, tls)) if tls.reload_on_sighup => tls_state.clone(),
        _ => None,
    };
    let admin_rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(ServerDynamicViewRebinder {
        stores: stores.clone(),
        oauth_cfg: Arc::new(oauth_anthropic.clone()),
        runtime: runtime.clone(),
        aead: aead.clone(),
        lazy_refresher: lazy_refresher.clone(),
        data_dir: data_dir.clone(),
        subscription_quota_cache: subscription_quota_cache.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        config: Arc::new(config.clone()),
    });
    let config_watcher = config_path.map(|path| {
        let watcher = Arc::new(ConfigWatcher::new_with_principal_view(
            path,
            config.clone(),
            Arc::clone(&runtime),
            Some(dynamic_view.clone()),
        ));
        watcher.set_dynamic_view_rebinder(admin_rebinder.clone());
        watcher
    });
    let signals = signal::install(
        drain_controller.clone(),
        Duration::from_secs(config.timeouts.drain_secs),
        sighup_handler(reload_tls_state, config_watcher.clone()),
    );
    if let Some(leader_election) = opened_scheduler.leader_shutdown_election() {
        signals.add_shutdown_hook(move || {
            let leader_election = leader_election.clone();
            async move {
                if let Err(error) = leader_election.close().await {
                    tracing::warn!(error = %error, "scheduler leader connection close failed");
                }
            }
        });
    }
    let scheduler_cancel = CancellationToken::new();
    let scheduler_ctx = crate::scheduler_dispatch::build_scheduler_ctx(
        crate::scheduler_dispatch::SchedulerDispatchDeps {
            backend: opened_scheduler.backend.clone(),
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
        },
    );
    let scheduler_tasks =
        opened_scheduler.spawn(config.clone(), scheduler_ctx, scheduler_cancel.clone())?;
    spawn_reconcile_shutdown(signals.subscribe(), scheduler_cancel.clone());
    spawn_reconcile_shutdown(signals.subscribe(), subscription_quota_writer_cancel);
    spawn_reconcile_shutdown(signals.subscribe(), refresh_cancel);
    let reconcile_cancel = CancellationToken::new();
    spawn_reconciler(ReconcilerParams {
        stores: stores.clone(),
        holder: dynamic_view.clone(),
        oauth_cfg: Arc::new(oauth_anthropic),
        runtime: runtime.clone(),
        aead: aead.clone(),
        lazy_refresher: lazy_refresher.clone(),
        cancel: reconcile_cancel.clone(),
        data_dir: data_dir.clone(),
        subscription_quota_cache: subscription_quota_cache.clone(),
        subscription_quota_routing_max_staleness_secs: config
            .subscription_quota
            .routing_max_staleness_secs,
        config: Arc::new(config.clone()),
    });
    spawn_reconcile_shutdown(signals.subscribe(), reconcile_cancel);
    let state = ProxyState {
        lifecycle: lifecycle.clone(),
        server_state: server_state.clone(),
        start_time,
        drain_controller: drain_controller.clone(),
        aead: aead.clone(),
        storage: storage.clone(),
        dynamic_view: dynamic_view.clone(),
        key_store: Some(key_store.clone()),
        builtin_authn: Some(builtin_authn.clone()),
    };

    let admin_config: Arc<dyn CurrentConfig> = match &config_watcher {
        Some(watcher) => watcher.clone(),
        None => Arc::new(InMemoryCurrentConfig::new(
            config.clone(),
            dynamic_view.clone(),
            Some(admin_rebinder.clone()),
        )),
    };
    let admin_state = AdminState {
        storage: Some(storage.clone()),
        key_store: Some(key_store),
        aead: aead.clone(),
        limit_engine: limit_engine.clone(),
        lifecycle: Some(lifecycle.clone()),
        subscription_metadata_hook,
        lazy_refresher: lazy_refresher.clone(),
        runtime: Some(runtime.clone()),
        data_dir: Some(data_dir.clone()),
        warmup_dialect_dispatcher: lazy_refresher_concrete.clone().map(|lazy_refresher| {
            Arc::new(ServerWarmupDialectDispatcher {
                stores: stores.clone(),
                aead: aead.clone(),
                lazy_refresher,
            }) as Arc<dyn WarmupDialectDispatcher>
        }),
        audit_sink: audit_sink.clone(),
        dynamic_view: dynamic_view.clone(),
        config: admin_config,
        scheduler: Some(cc_lb_scheduler::admin::SchedulerAdminHandle::new(
            scheduler_lazy_handle.clone(),
            opened_scheduler.leader_election(),
        )),
        admin_token: config
            .admin
            .token
            .clone()
            .or_else(|| std::env::var(&config.admin.token_env).ok()),
        start_time,
    };
    let reload_task = config_watcher.clone().map(spawn_reload_watcher);

    server_state.transition_to_ready();

    Ok(App {
        router: app_router(state, config.timeouts.upstream_total_secs),
        admin_router: admin_router(admin_state, server_state.clone(), plugin_registry),
        proxy_addr: config.listener.proxy_addr,
        admin_addr: config.listener.admin_addr,
        reload_task,
        notify_cancel: Some(notify_cancel),
        notifier_task,
        notify_listener_task,
        price_catalog_install_cancel: Some(price_catalog_install_cancel),
        price_catalog_install_task: Some(price_catalog_install_task),
        scheduler_cancel: Some(scheduler_cancel),
        scheduler_tasks,
        audit_writer_task: Some(audit_writer_task),
        upstream_rate_limit_writer_task: Some(upstream_rate_limit_writer_task),
        subscription_quota_writer_task: Some(subscription_quota_writer_task),
        signals,
        drain_controller,
        tls_state,
        server_state,
    })
}

struct ServerWarmupDialectDispatcher {
    stores: Arc<DynamicStores>,
    aead: Arc<AeadService>,
    lazy_refresher: Arc<LazyRefresher>,
}

struct ServerMetadataRefreshEnqueue {
    handle: crate::scheduler_factory::SchedulerBackend,
}

#[async_trait]
impl cc_lb_core::MetadataRefreshEnqueue for ServerMetadataRefreshEnqueue {
    async fn push_metadata_refresh(
        &self,
        request: cc_lb_core::MetadataHookRequest,
    ) -> Result<(), cc_lb_core::MetadataHookEnqueueError> {
        let job = cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob {
            upstream_id: request.upstream_id,
            credential_generation: request.credential_generation,
            traceparent: request.traceparent,
        };
        self.handle
            .push_job(cc_lb_scheduler::worker::EntityJob::MetadataRefresh(job))
            .await
            .map_err(|error| cc_lb_core::MetadataHookEnqueueError::Enqueue(error.to_string()))
    }
}

#[async_trait]
impl WarmupDialectDispatcher for ServerWarmupDialectDispatcher {
    async fn dispatch_warmup_with_dialect(
        &self,
        runtime: &ExtismRuntime,
        data_dir: &Path,
        upstream: &cc_lb_storage_api::UpstreamRecord,
    ) -> Result<WarmupDialectDispatchOutcome, WarmupDialectDispatchError> {
        let http = warmup_dialect_http_client();
        let outcome = crate::warmup::dialect::dispatch_warmup_with_dialect(
            runtime,
            self.stores.as_ref(),
            data_dir,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            upstream,
            &http,
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
        | crate::warmup::dialect::WarmupDispatchError::Instantiate(_)
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
    oauth_cfg: Arc<cc_lb_config::AnthropicOAuthConfig>,
    runtime: Arc<ExtismRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    data_dir: PathBuf,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    subscription_quota_routing_max_staleness_secs: u64,
    config: Arc<cc_lb_config::Config>,
}

#[async_trait]
impl DynamicViewRebinder for ServerDynamicViewRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        Ok(build_dynamic_view(
            &self.stores,
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            current_generation,
            &self.runtime,
            &self.data_dir,
            self.subscription_quota_cache.clone(),
            self.subscription_quota_routing_max_staleness_secs,
            &self.config,
        )
        .await?)
    }
}

fn log_startup_handshake_report(report: &StartupHandshakeReport) {
    tracing::info!(
        processed = report.processed,
        skipped_fresh = report.skipped_fresh,
        re_handshaked = report.re_handshaked,
        disabled = report.disabled,
        errors = report.errors.len(),
        "startup plugin re-handshake completed",
    );
    for (sha256, error) in &report.errors {
        if *sha256 == [0; 32] {
            continue;
        }
        tracing::warn!(
            sha256 = %hex_sha256_bytes(sha256),
            error = %error,
            "startup plugin re-handshake disabled or skipped a plugin",
        );
    }
}

pub async fn backfill_supported_slots(storage: &dyn Storage) {
    use cc_lb_runtime_extism::handshake::{
        build_offer, execute_handshake, slot_set_from_handshake,
    };
    use cc_lb_storage_api::{BUILTIN_CACHE_AFFINITY_ID, PluginSlot};

    let entries = match storage.list_registry(None, usize::MAX).await {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(%error, "supported_slots backfill: list_registry failed");
            return;
        }
    };
    let host_caps = std::collections::BTreeSet::new();
    let offer = build_offer(&host_caps);
    let mut updated = 0_usize;
    let mut failed = 0_usize;
    for entry in entries {
        if !entry.supported_slots.is_empty() {
            continue;
        }
        if entry.id == BUILTIN_CACHE_AFFINITY_ID {
            if let Err(error) = storage
                .update_supported_slots(entry.id, vec![PluginSlot::Router])
                .await
            {
                tracing::warn!(%error, "supported_slots backfill: cache-affinity builtin update failed");
                failed += 1;
            } else {
                updated += 1;
            }
            continue;
        }
        if entry.is_builtin {
            continue;
        }
        let bytes = match storage.get_blob_bytes(entry.sha256).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                tracing::warn!(
                    sha256 = %hex_sha256_bytes(&entry.sha256),
                    "supported_slots backfill: blob missing",
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "supported_slots backfill: get_blob failed");
                failed += 1;
                continue;
            }
        };
        let offer_for_task = offer.clone();
        let bytes_for_task = bytes;
        let outcome = tokio::task::spawn_blocking(move || {
            execute_handshake(&bytes_for_task, &offer_for_task)
                .map(|accept| slot_set_from_handshake(&accept.implemented_functions))
        })
        .await;
        let slots = match outcome {
            Ok(Ok(slots)) => slots,
            Ok(Err(error)) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "supported_slots backfill: handshake failed");
                failed += 1;
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "supported_slots backfill: handshake worker panicked");
                failed += 1;
                continue;
            }
        };
        if slots.is_empty() {
            continue;
        }
        if let Err(error) = storage.update_supported_slots(entry.id, slots).await {
            tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "supported_slots backfill: update failed");
            failed += 1;
            continue;
        }
        updated += 1;
    }
    if updated > 0 || failed > 0 {
        tracing::info!(updated, failed, "supported_slots backfill completed",);
    }
}

pub async fn backfill_wire_version(storage: &dyn Storage) {
    use cc_lb_runtime_extism::handshake::{build_offer, execute_handshake};
    use cc_lb_storage_api::{BUILTIN_CACHE_AFFINITY_ID, default_wire_version};

    let entries = match storage.list_registry(None, usize::MAX).await {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(%error, "wire_version backfill: list_registry failed");
            return;
        }
    };
    let host_caps = BTreeSet::new();
    let mut offer = build_offer(&host_caps);
    extend_offer_versions(&mut offer.function_versions, "filter", &[1, 2, 3]);
    extend_offer_versions(&mut offer.function_versions, "shape", &[1, 2]);
    let mut updated = 0_usize;
    let mut failed = 0_usize;
    for entry in entries {
        if entry.id == BUILTIN_CACHE_AFFINITY_ID {
            continue;
        }
        if entry.is_builtin {
            continue;
        }
        if entry.wire_version != default_wire_version() {
            continue;
        }

        let bytes = match storage.get_blob_bytes(entry.sha256).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                tracing::warn!(
                    sha256 = %hex_sha256_bytes(&entry.sha256),
                    "wire_version backfill: blob missing",
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "wire_version backfill: get_blob failed");
                failed += 1;
                continue;
            }
        };
        let offer_for_task = offer.clone();
        let bytes_for_task = bytes.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            execute_handshake(&bytes_for_task, &offer_for_task)
        })
        .await;
        let fresh = match outcome {
            Ok(Ok(accept)) => match accept.chosen_versions.values().copied().max() {
                Some(fresh) => match u8::try_from(fresh) {
                    Ok(fresh) => fresh,
                    Err(error) => {
                        tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), fresh, "wire_version backfill: chosen wire version out of range");
                        failed += 1;
                        continue;
                    }
                },
                None => {
                    tracing::warn!(sha256 = %hex_sha256_bytes(&entry.sha256), "wire_version backfill: handshake returned no chosen wire versions");
                    failed += 1;
                    continue;
                }
            },
            Ok(Err(error)) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "wire_version backfill: handshake failed");
                failed += 1;
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "wire_version backfill: handshake worker panicked");
                failed += 1;
                continue;
            }
        };

        if let Err(error) = storage.update_wire_version(entry.id, fresh).await {
            tracing::warn!(%error, sha256 = %hex_sha256_bytes(&entry.sha256), "wire_version backfill: update failed");
            failed += 1;
            continue;
        }
        updated += 1;
    }
    if updated > 0 || failed > 0 {
        tracing::info!(updated, failed, "wire_version backfill completed",);
    }
}

fn extend_offer_versions(
    function_versions: &mut std::collections::BTreeMap<String, Vec<u32>>,
    function: &str,
    versions: &[u32],
) {
    let entry = function_versions.entry(function.to_owned()).or_default();
    for version in versions {
        if !entry.contains(version) {
            entry.push(*version);
        }
    }
    entry.sort_unstable();
}

fn log_legacy_bridge_report(report: &LegacyBridgeReport) {
    tracing::info!(
        scanned = report.scanned,
        already_present = report.already_present,
        bridged = report.bridged,
        orphan = report.orphan,
        failed = report.failed.len(),
        "startup legacy wasm registry bridge completed",
    );
    for (sha256, error) in &report.failed {
        tracing::warn!(
            sha256 = %hex_sha256_bytes(sha256),
            error = %error,
            "legacy wasm bridge skipped a plugin",
        );
    }
}

fn spawn_startup_shutdown_signal() -> (watch::Receiver<bool>, Arc<AtomicBool>, JoinHandle<()>) {
    let (tx, rx) = watch::channel(false);
    let triggered = Arc::new(AtomicBool::new(false));
    let task_triggered = triggered.clone();
    let task = tokio::spawn(async move {
        wait_for_startup_shutdown_signal().await;
        task_triggered.store(true, Ordering::SeqCst);
        let _ = tx.send(true);
    });
    (rx, triggered, task)
}

#[cfg(unix)]
async fn wait_for_startup_shutdown_signal() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_startup_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn hex_sha256_bytes(sha256: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

struct ReconcilerParams {
    stores: Arc<DynamicStores>,
    holder: Arc<DynamicViewHolder>,
    oauth_cfg: Arc<cc_lb_config::AnthropicOAuthConfig>,
    runtime: Arc<ExtismRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    cancel: CancellationToken,
    data_dir: PathBuf,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    subscription_quota_routing_max_staleness_secs: u64,
    config: Arc<cc_lb_config::Config>,
}

fn spawn_reconciler(params: ReconcilerParams) {
    let reconciler = Arc::new(Reconciler::new(
        params.stores,
        params.holder,
        params.oauth_cfg,
        params.runtime,
        params.aead,
        params.lazy_refresher,
        params.cancel,
        params.data_dir,
        params.subscription_quota_cache,
        params.subscription_quota_routing_max_staleness_secs,
        params.config,
    ));
    tokio::spawn(reconciler.run());
}

fn spawn_reconcile_shutdown(shutdown: watch::Receiver<bool>, cancel: CancellationToken) {
    tokio::spawn(async move {
        signal::wait_for_shutdown(shutdown).await;
        cancel.cancel();
    });
}

fn cap_to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(feature = "postgres")]
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn build_tls_state(config: &Config) -> Result<Option<Arc<TlsState>>, BuildError> {
    let Some((_label, tls_config)) = active_tls_config(config) else {
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

fn active_tls_config(config: &Config) -> Option<(&'static str, &TlsConfig)> {
    config
        .listener
        .tls
        .as_ref()
        .map(|tls| ("listener", tls))
        .or_else(|| config.tls.as_ref().map(|tls| ("legacy", tls)))
}

fn sighup_handler(
    tls_state: Option<Arc<TlsState>>,
    config_watcher: Option<Arc<ConfigWatcher>>,
) -> Option<signal::SighupHandler> {
    if tls_state.is_none() && config_watcher.is_none() {
        return None;
    }
    Some(Arc::new(move || {
        if let Some(tls_state) = &tls_state
            && let Err(error) = tls_state.reload()
        {
            tracing::warn!(error = %error, "TLS reload failed");
        }
        if let Some(config_watcher) = &config_watcher
            && let Err(error) = config_watcher.reload_now()
        {
            tracing::warn!(error = %error, "configuration reload failed");
        }
    }))
}

fn spawn_reload_watcher(config_watcher: Arc<ConfigWatcher>) -> JoinHandle<()> {
    config_watcher.spawn_file_watcher()
}

struct InMemoryCurrentConfig {
    process_start_config: Arc<Config>,
    current: ArcSwap<Config>,
    draft: Mutex<Option<Config>>,
    dynamic_view: Arc<DynamicViewHolder>,
    dynamic_view_rebinder: Option<Arc<dyn DynamicViewRebinder>>,
}

impl InMemoryCurrentConfig {
    fn new(
        config: Config,
        dynamic_view: Arc<DynamicViewHolder>,
        dynamic_view_rebinder: Option<Arc<dyn DynamicViewRebinder>>,
    ) -> Self {
        let process_start_config = Arc::new(config.clone());
        Self {
            process_start_config,
            current: ArcSwap::from_pointee(config),
            draft: Mutex::new(None),
            dynamic_view,
            dynamic_view_rebinder,
        }
    }
}

impl CurrentConfig for InMemoryCurrentConfig {
    fn current_config(&self) -> Arc<Config> {
        self.current.load_full()
    }

    fn restart_required_changes(&self) -> Vec<cc_lb_config::RestartRequiredField> {
        summarize_restart_required(&self.process_start_config, &self.current_config())
    }

    fn dynamic_view_rebinder(&self) -> Option<Arc<dyn DynamicViewRebinder>> {
        self.dynamic_view_rebinder.clone()
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
        let principal_view = Arc::new(PrincipalView::from_db(
            &[],
            std::collections::HashMap::new(),
        ));
        let current_view = self.dynamic_view.load();
        self.dynamic_view.store(
            DynamicViewBuilder::from_view(&current_view)
                .principal_view(principal_view)
                .build(),
        );
        let config = Arc::new(config);
        self.current.store(config.clone());
        Ok(config)
    }
}

#[derive(Clone)]
struct ProxyState {
    lifecycle: Arc<Lifecycle>,
    server_state: Arc<ServerStateHandle>,
    start_time: std::time::Instant,
    drain_controller: DrainController,
    aead: Arc<AeadService>,
    storage: Arc<dyn Storage>,
    dynamic_view: Arc<DynamicViewHolder>,
    #[allow(dead_code)]
    key_store: Option<Arc<KeyStore>>,
    builtin_authn: Option<Arc<BuiltinAuthn>>,
}

fn spawn_price_catalog_local_installer(
    config: &Config,
    storage: Arc<dyn Storage>,
    price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
) -> (CancellationToken, JoinHandle<()>) {
    install_default_fallback_if_uninitialized(&price_catalog);

    let cfg = &config.api_keys.price_catalog;
    let storage_cache: Arc<dyn cc_lb_storage_api::PriceCatalogCache> = storage;
    let loader = cc_lb_pricing::LiteLlmLoader::new(
        price_catalog,
        storage_cache,
        cfg.url.clone(),
        cfg.refresh_interval,
        cfg.cache_path.clone(),
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

fn install_default_fallback_if_uninitialized(price_catalog: &cc_lb_pricing::PriceCatalog) {
    if !matches!(price_catalog.status(), cc_lb_pricing::CatalogStatus::Ok) {
        price_catalog.install_snapshot(claude_default_snapshot());
    }
}

fn claude_default_snapshot() -> cc_lb_pricing::CatalogSnapshot {
    use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion};
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Per-million USD in micros (1 USD = 1_000_000 micros).
    // Sources: anthropic.com/pricing (Nov 2026 snapshot).
    #[allow(clippy::type_complexity)]
    let raw: &[(&str, u64, u64, u64, u64)] = &[
        // model name, input, output, cache_creation_5m, cache_read
        (
            "claude-opus-4-5",
            15_000_000,
            75_000_000,
            18_750_000,
            1_500_000,
        ),
        (
            "claude-opus-4-5-20251101",
            15_000_000,
            75_000_000,
            18_750_000,
            1_500_000,
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
            },
        );
        cache_creation.insert(
            (*name).to_owned(),
            UsdPerMillion::from_micros_usd(*creation),
        );
        cache_read.insert((*name).to_owned(), UsdPerMillion::from_micros_usd(*read));
    }

    CatalogSnapshot {
        fetched_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: cache_creation,
        cache_read_per_million_usd: cache_read,
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
    plugin_registry: PluginRegistry,
) -> Router {
    let admin_token = admin_state.admin_token.clone();
    cc_lb_admin::router(admin_state)
        .merge(crate::admin_plugins::router(admin_token, plugin_registry))
        .merge(server_state_router(server_state))
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
        .route("/api/oauth/usage", get(oauth_usage_handler))
        .route("/api/{*path}", any(lifecycle_handler))
        .route("/v1/{*path}", any(lifecycle_handler))
        .fallback(proxy_not_found)
        .method_not_allowed_fallback(proxy_method_not_allowed)
        .with_state(state)
        .layer(service_builder)
}

async fn proxy_not_found() -> Response<Body> {
    anthropic_error_response(
        StatusCode::NOT_FOUND,
        "not_found",
        "requested proxy path was not found",
    )
}

async fn proxy_method_not_allowed() -> Response<Body> {
    anthropic_error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "not_found",
        "method is not allowed for this proxy path",
    )
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
                .is_some_and(|entry| entry.status == cc_lb_core::ApplyStatus::Active)
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

async fn oauth_usage_handler(
    State(state): State<ProxyState>,
    request: Request<Body>,
) -> Response<Body> {
    let Some(authn) = state.builtin_authn.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({ "error": "auth_unavailable" }),
        );
    };
    if authn
        .authenticate_none_mode(request.headers())
        .await
        .is_none()
    {
        let dynamic_view = state.dynamic_view.load();
        if let Err(error) = authn
            .authenticate(request.headers(), &dynamic_view.principal_view)
            .await
        {
            let status =
                StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
            return json_response(
                status,
                serde_json::json!({ "error": "authentication_error", "message": error.to_string() }),
            );
        }
    }

    match cc_lb_admin::subscription_quotas::build_cc_lb_oauth_usage_response(
        state.storage.as_ref(),
        &state.dynamic_view,
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
    }
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

async fn await_or_abort<T>(mut task: JoinHandle<T>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut task).await.is_err() {
        task.abort();
        let _ = task.await;
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

type OpenStorageParts = (
    Arc<dyn ManagedKeyStore>,
    Arc<dyn Storage>,
    Arc<AeadService>,
    Arc<dyn PluginRegistryRepo>,
    Arc<dyn PluginBlobRepo>,
    Arc<dyn LazyRefreshClaimGuard>,
    crate::scheduler_factory::OpenedScheduler,
);

pub async fn open_storage(config: &Config) -> Result<OpenStorageParts, BuildError> {
    let key_hex =
        std::env::var(&config.aead.key_env).map_err(|_| BuildError::StorageKeyMissing {
            env: config.aead.key_env.clone(),
        })?;
    let key = decode_hex_key(&key_hex)?;
    let aead = Arc::new(AeadService::from_master_key(key));
    let opened = storage_factory::open_storage(&config.storage, aead.clone(), key).await?;
    let opened_scheduler =
        crate::scheduler_factory::open_scheduler_storage(&config.storage, &config.scheduler)
            .await?;
    let lazy_refresh_claim_guard =
        lazy_refresh_claim_guard_from_scheduler(&opened_scheduler.backend);
    Ok((
        opened.managed_key_store,
        opened.storage,
        aead,
        opened.plugin_registry_repo,
        opened.plugin_blob_repo,
        lazy_refresh_claim_guard,
        opened_scheduler,
    ))
}

fn lazy_refresh_claim_guard_from_scheduler(
    scheduler_backend: &crate::scheduler_factory::SchedulerBackend,
) -> Arc<dyn LazyRefreshClaimGuard> {
    match scheduler_backend {
        #[cfg(feature = "sqlite")]
        crate::scheduler_factory::SchedulerBackend::Sqlite(sqlite) => Arc::new(
            cc_lb_scheduler::idempotency::OAuthRefreshClaimsStore::new(sqlite.pool.clone()),
        ),
        #[cfg(feature = "postgres")]
        crate::scheduler_factory::SchedulerBackend::Postgres(postgres) => Arc::new(
            cc_lb_scheduler::idempotency::OAuthRefreshClaimsStore::new(postgres.pool.clone()),
        ),
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn admin_health_state_reports_current_state() {
        let state = Arc::new(ServerStateHandle::new_starting());
        let router = server_state_router(state.clone());

        assert_admin_state(router.clone(), "starting").await;
        state.transition_to_ready();
        assert_admin_state(router, "ready").await;
    }

    #[test]
    fn omitted_cli_uses_config_defaults_for_startup_handshake() {
        let cfg = cc_lb_config::StartupHandshakeConfig::default();
        let opts = startup_handshake_opts_from_flags(None, None, &cfg);
        assert!(opts.skip_if_fresh);
        assert!(!opts.force);
    }

    #[test]
    fn config_can_disable_skip_if_fresh() {
        let cfg = cc_lb_config::StartupHandshakeConfig {
            skip_if_fresh: false,
            force: true,
        };
        let opts = startup_handshake_opts_from_flags(None, None, &cfg);
        assert!(!opts.skip_if_fresh);
        assert!(opts.force);
    }

    #[test]
    fn cli_overrides_config_for_startup_handshake() {
        let cfg = cc_lb_config::StartupHandshakeConfig {
            skip_if_fresh: false,
            force: true,
        };
        let opts = startup_handshake_opts_from_flags(Some(true), Some(false), &cfg);
        assert!(opts.skip_if_fresh);
        assert!(!opts.force);
    }

    #[test]
    fn default_fallback_populates_empty_catalog() {
        let catalog = cc_lb_pricing::PriceCatalog::new_empty();
        assert!(matches!(
            catalog.status(),
            cc_lb_pricing::CatalogStatus::CostDisabled
        ));

        install_default_fallback_if_uninitialized(&catalog);

        assert!(matches!(catalog.status(), cc_lb_pricing::CatalogStatus::Ok));
        assert!(catalog.lookup("claude-opus-4-5", None).is_some());
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
            },
        );
        catalog.install_snapshot(cc_lb_pricing::CatalogSnapshot {
            fetched_at_ms: 0,
            models,
            raw_json: Vec::new(),
            cache_creation_per_million_usd: HashMap::new(),
            cache_read_per_million_usd: HashMap::new(),
            status: cc_lb_pricing::CatalogStatus::Ok,
        });

        install_default_fallback_if_uninitialized(&catalog);

        assert!(catalog.lookup("operator-model-a", None).is_some());
        assert!(catalog.lookup("claude-opus-4-5", None).is_none());
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
}
