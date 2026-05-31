use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
use cc_lb_core::BreakerState;
use cc_lb_core::{
    BreakerConfig, BreakerRegistry, BulkheadConfig, BulkheadDispatch, BulkheadRegistry,
    CircuitBreakerDispatch, DynamicView, DynamicViewBuilder, DynamicViewHolder, HopByHopStripLayer,
    Lifecycle, LifecycleConfig, UpstreamDispatch, UpstreamRateLimitSink,
    api_keys::{
        builtin_authn::BuiltinAuthn, concurrent_guard::KeyConcurrencyManager, key_store::KeyStore,
        limit_engine::LimitEngine, principal_view::PrincipalView,
    },
    make_default_dispatcher, spawn_audit_writer, start_upstream_rate_limit_writer,
    usage_pruner::UsagePruner,
};
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_api::{ManagedKeyStore, RuntimeChangeNotifier, Storage};
use http_body_util::BodyExt;
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
use crate::refresh::{LazyRefresher, OAuthRefresher};
use crate::reload::{ConfigWatcher, summarize_restart_required};
use crate::replica;
use crate::signal;
use crate::storage_factory;
use crate::tls::{ReloadableListener, TlsState};
use cc_lb_admin::{AdminState, ConfigDraftError, CurrentConfig, DynamicViewRebinder};

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
    audit_writer_task: Option<JoinHandle<()>>,
    upstream_rate_limit_writer_task: Option<JoinHandle<()>>,
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
    Storage(#[from] cc_lb_storage_redb::StorageError),
    #[error(transparent)]
    StorageApi(#[from] cc_lb_storage_api::StorageError),
    #[error(transparent)]
    StorageFactory(#[from] crate::storage_factory::StorageFactoryError),
    #[cfg(feature = "postgres")]
    #[error("storage connection failed: {message}")]
    StorageConnect { message: String },
    #[error(transparent)]
    Bootstrap(#[from] crate::bootstrap::BootstrapError),
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    #[error(transparent)]
    Rebind(#[from] crate::dynamic_view_builder::RebindError),
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
            audit_writer_task,
            upstream_rate_limit_writer_task,
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
        if let Some(cancel) = notify_cancel {
            cancel.cancel();
        }
        if let Some(task) = notifier_task {
            let _ = task.await;
        }
        if let Some(task) = notify_listener_task {
            let _ = task.await;
        }
        let _ = admin_stop_tx.send(true);
        let _ = admin.await;
        if let Some(task) = audit_writer_task {
            let _ = task.await;
        }
        if let Some(task) = upstream_rate_limit_writer_task {
            let _ = task.await;
        }
        proxy_result?;
        Ok(())
    }
}

pub async fn run_serve(
    config_path: &Path,
    data_dir: Option<&Path>,
    strict_preflight: bool,
) -> Result<(), ServeError> {
    let mut config = Config::load(config_path)?;
    if let Some(data_dir) = data_dir {
        config.runtime.data_dir = Some(data_dir.to_path_buf());
    }
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let _guard = init_observability(&mut config)?;
    let app = build_app_with_path_inner(
        config,
        Some(config_path),
        Some(StartupPreflight { strict_preflight }),
    )
    .await?;
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
    let storage: Arc<dyn Storage> = storage_arc.clone();
    config.storage = cc_lb_config::StorageConfig::Redb { path };
    config.aead.key_env = "__CC_LB_TEST_KEY__".to_owned();
    config.downstream_auth.mode = DownstreamAuthMode::None;
    config.downstream_auth.none_mode = Some(cc_lb_config::NoneModeConfig {
        principal_id: "test-principal".to_owned(),
        upstream_kind: cc_lb_config::NoneModeUpstreamKind::AnthropicKey,
    });
    std::mem::forget(dir);
    build_app_with_storage(config, None, managed_store, storage, aead).await
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
                kind: UpstreamKind::Custom,
                base_url: upstream_base_url,
                api_key_ciphertext: None,
                shape_plugin: None,
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
    build_app_with_path_inner(config, config_path, None).await
}

async fn build_app_with_path_inner(
    config: Config,
    config_path: Option<&Path>,
    startup_preflight: Option<StartupPreflight>,
) -> Result<App, BuildError> {
    config.validate()?;
    let (managed_store, storage, aead) = open_storage(&config).await?;
    build_app_with_storage_inner(
        config,
        config_path,
        managed_store,
        storage,
        aead,
        startup_preflight,
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
    build_app_with_storage_inner(config, config_path, managed_store, storage, aead, None).await
}

async fn build_app_with_storage_inner(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    startup_preflight: Option<StartupPreflight>,
) -> Result<App, BuildError> {
    let key_store = Arc::new(KeyStore::new(managed_store));
    let price_catalog = cc_lb_pricing::global_catalog().clone();
    spawn_price_catalog_loader(&config, storage.clone(), price_catalog.clone());
    let pruner = UsagePruner::new(storage.clone(), config.api_keys.usage_retention_days);
    let _usage_pruner_task = tokio::spawn(pruner.start_daemon());
    let (sink, audit_writer_task) = spawn_audit_writer(storage.clone(), 1024);
    let audit_sink = Some(Arc::new(sink));
    let (upstream_rate_limit_sink, upstream_rate_limit_receiver) = UpstreamRateLimitSink::new();
    let upstream_rate_limit_writer_task =
        start_upstream_rate_limit_writer(storage.clone(), upstream_rate_limit_receiver);
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
    limit_engine.startup_replay(storage.clone());
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        config.downstream_auth.mode.clone(),
        config.downstream_auth.none_mode.clone(),
        Some(key_store.clone()),
    ));

    let (_dispatcher, breaker_registry) = dispatcher(&config);

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

    let stores = Arc::new(DynamicStores {
        upstreams: storage_for_dynamic.clone(),
        principals: storage_for_dynamic.clone(),
        plugin_registry: storage_for_dynamic.clone(),
        upstream_rate_limits: storage_for_dynamic.clone(),
        audit: Some(storage_for_dynamic.clone()),
    });
    let lifecycle_config = LifecycleConfig {
        messages_body_cap_bytes: cap_to_usize(config.body.messages_cap_bytes),
        files_body_cap_bytes: cap_to_usize(config.body.files_cap_bytes),
        replica_identity,
    };
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
    let oauth_anthropic = config.oauth.anthropic.clone().unwrap_or_default();
    let oauth_cfg = Arc::new(oauth_anthropic.clone());
    let refresh_cancel = CancellationToken::new();
    let lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>> =
        lifecycle_config.replica_identity.as_ref().map(|identity| {
            let refresher = LazyRefresher::new(
                stores.clone(),
                aead.clone(),
                oauth_cfg.clone(),
                identity.id,
                refresh_cancel.clone(),
            );
            Arc::new(refresher) as Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>
        });
    let initial_view = build_dynamic_view(
        &stores,
        &oauth_anthropic,
        aead.clone(),
        lazy_refresher.clone(),
        0,
        &runtime,
        &data_dir,
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
    }));
    let notify_listener_task = Some(tokio::spawn(async move {
        notify_listener.run().await;
    }));
    let replica_id = lifecycle_config
        .replica_identity
        .as_ref()
        .map(|identity| identity.id);

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
    });
    spawn_reconcile_shutdown(signals.subscribe(), reconcile_cancel);
    if let Some(replica_id) = replica_id {
        spawn_oauth_refresher(
            stores.clone(),
            aead.clone(),
            oauth_cfg,
            replica_id,
            refresh_cancel.clone(),
        );
        spawn_reconcile_shutdown(signals.subscribe(), refresh_cancel);
    }
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
        audit_sink: audit_sink.clone(),
        dynamic_view: dynamic_view.clone(),
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
        notify_cancel: Some(notify_cancel),
        notifier_task,
        notify_listener_task,
        audit_writer_task: Some(audit_writer_task),
        upstream_rate_limit_writer_task: Some(upstream_rate_limit_writer_task),
        signals,
        drain_controller,
        tls_state,
    })
}

#[allow(clippy::too_many_arguments)]
struct ServerDynamicViewRebinder {
    stores: Arc<DynamicStores>,
    oauth_cfg: Arc<cc_lb_config::AnthropicOAuthConfig>,
    runtime: Arc<ExtismRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    data_dir: PathBuf,
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
        )
        .await?)
    }
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
    ));
    tokio::spawn(reconciler.run());
}

fn spawn_oauth_refresher(
    stores: Arc<DynamicStores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<cc_lb_config::AnthropicOAuthConfig>,
    replica_id: uuid::Uuid,
    cancel: CancellationToken,
) {
    let refresher = OAuthRefresher::new(stores, aead, oauth_cfg, replica_id, cancel);
    tokio::spawn(Arc::new(refresher).run());
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
    storage: Arc<dyn Storage>,
    price_catalog: Arc<cc_lb_pricing::PriceCatalog>,
) {
    let _storage = storage;
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::warn!("tokio runtime unavailable; litellm price catalog loader not started");
        return;
    }

    let _price_catalog_config = &config.api_keys.price_catalog;
    let _ = price_catalog;
    tracing::warn!(
        "litellm price catalog loader requires a storage-agnostic cache adapter before it can run with unified storage"
    );
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
) -> Result<(Arc<dyn ManagedKeyStore>, Arc<dyn Storage>, Arc<AeadService>), BuildError> {
    let key_hex =
        std::env::var(&config.aead.key_env).map_err(|_| BuildError::StorageKeyMissing {
            env: config.aead.key_env.clone(),
        })?;
    let key = decode_hex_key(&key_hex)?;
    let aead = Arc::new(AeadService::from_master_key(key));
    let opened = storage_factory::open_storage(&config.storage, aead.clone(), key).await?;
    Ok((opened.managed_key_store, opened.storage, aead))
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
