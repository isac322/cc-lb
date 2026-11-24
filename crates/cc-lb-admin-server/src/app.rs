use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::Request as HttpRequest;
use axum::middleware;
use cc_lb_admin::auth::{AdminAuthenticator, build_providers};
use cc_lb_admin::{AdminBuildInfo, AdminPorts, AdminState, StartupConfigOverrides};
use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_config::{Config, StorageConfig};
use cc_lb_control::anthropic_api_key::AnthropicOrgApi;
use cc_lb_control::api_keys::{key_store::KeyStore, principal_view::PrincipalView};
use cc_lb_control::{
    DynamicViewBuilder, DynamicViewHolder, InMemoryBus, RequestEventBus, UpstreamStatusSnapshot,
};
use cc_lb_domain::Upstream;
use cc_lb_observability::{self, ObservabilityConfig, TracingGuard};
use cc_lb_runtime_sync::admin_http::{
    RequestIdState, compression_layer as admin_compression_layer, request_id_middleware,
    with_browser_security_headers,
};
use cc_lb_runtime_sync::aead_key::{AeadKeyError, load_aead_from_env};
use cc_lb_runtime_sync::build_meta::BuildMeta;
use cc_lb_runtime_sync::replica::load_or_create_replica_id;
use cc_lb_runtime_sync::storage_factory::{StorageFactoryError, open_storage};
use cc_lb_scheduler::admin::SchedulerAdminHandle;
use cc_lb_scheduler::factory::{OpenedScheduler, SchedulerFactoryError, open_scheduler_storage};
use cc_lb_storage_api::{ManagedKeyStore, Storage};
use cc_lb_upstream::{ApiKeyAwareSignerFactory, Signer, SignerError, SignerFactory};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperAutoBuilder;
use hyper_util::service::TowerToHyperService;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tower::ServiceExt as _;

const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_millis(500);

pub struct AdminApp {
    pub admin_router: Router,
    pub admin_addr: SocketAddr,
    event_fanout_shutdown_tx: Option<watch::Sender<bool>>,
    event_fanout_tasks: Vec<JoinHandle<()>>,
}

#[derive(Debug, Error)]
pub enum AdminServeError {
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Observability(#[from] cc_lb_observability::InitError),
    #[error(transparent)]
    Build(#[from] AdminBuildError),
}

#[derive(Debug, Error)]
pub enum AdminBuildError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
    #[error(transparent)]
    Observability(#[from] cc_lb_observability::InitError),
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_api::StorageError),
    #[error(transparent)]
    StorageFactory(#[from] StorageFactoryError),
    #[error(transparent)]
    SchedulerFactory(#[from] SchedulerFactoryError),
    #[error("admin authentication configuration failed: {message}")]
    AdminAuth { message: String },
    #[error("storage master key env {env} is missing")]
    StorageKeyMissing { env: String },
    #[error("storage master key must be 32 bytes encoded as 64 hex characters")]
    InvalidStorageKey,
    #[error("postgres storage pool unavailable for pg_notify fanout")]
    PgNotifyPoolUnavailable,
    #[error("anthropic org api origin is invalid: {0}")]
    AnthropicOrgOrigin(#[source] url::ParseError),
}

impl AdminApp {
    pub async fn start(self) -> Result<(), AdminBuildError> {
        let listener = TcpListener::bind(self.admin_addr).await?;
        tracing::info!(
            admin_addr = %self.admin_addr,
            "starting split cc-lb admin control-plane server",
        );
        self.serve_with_listener(listener, shutdown_signal(), SHUTDOWN_TASK_TIMEOUT)
            .await
    }

    async fn serve_with_listener<F>(
        self,
        mut listener: TcpListener,
        shutdown: F,
        drain_timeout: Duration,
    ) -> Result<(), AdminBuildError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut shutdown = std::pin::pin!(shutdown);
        let mut conn_tasks = JoinSet::new();
        let router = self.admin_router;

        loop {
            tokio::select! {
                Some(_) = conn_tasks.join_next(), if !conn_tasks.is_empty() => {}
                (stream, remote_addr) = axum::serve::Listener::accept(&mut listener) => {
                    spawn_connection(
                        &mut conn_tasks,
                        &router,
                        stop_rx.clone(),
                        stream,
                        remote_addr,
                    );
                }
                _ = &mut shutdown => {
                    let _ = stop_tx.send(true);
                    break;
                }
            }
        }

        // Stop accepting new peers and drop our router handle so only live connections
        // still hold clones of shared state and broadcast channels during drain.
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
                "split admin graceful drain elapsed with active connections (e.g. SSE stream); aborting connections",
            );
            conn_tasks.abort_all();
            while conn_tasks.join_next().await.is_some() {}
        }

        if let Some(tx) = self.event_fanout_shutdown_tx {
            let _ = tx.send(true);
        }
        for task in self.event_fanout_tasks {
            await_or_abort(task, SHUTDOWN_TASK_TIMEOUT).await;
        }
        Ok(())
    }
}

fn spawn_connection(
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
                            "split admin connection closed with error",
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

pub async fn run_admin_serve(
    config_path: &Path,
    data_dir: Option<&Path>,
    clock: ClockHandle,
) -> Result<(), AdminServeError> {
    let mut config = Config::load(config_path)?;
    let startup_config_overrides = StartupConfigOverrides {
        runtime_data_dir: data_dir.map(Path::to_path_buf),
        split_mode_read_only_config: true,
    };
    if let Some(data_dir) = startup_config_overrides.runtime_data_dir.as_ref() {
        config.runtime.data_dir = Some(data_dir.clone());
    }
    cc_lb_observability::install_panic_hook(cc_lb_observability::RedactionPolicy::new(
        config.observability.user_prompt_redaction,
    ));
    let _guard = init_observability(&config)?;
    let app = match build_admin_app_with_path_inner(
        config,
        Some(config_path),
        startup_config_overrides,
        clock,
    )
    .await
    {
        Ok(app) => app,
        Err(error @ AdminBuildError::SchedulerFactory(_)) => {
            cc_lb_scheduler::scheduler_metrics::set_scheduler_init_failure(true);
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    };
    app.start().await?;
    Ok(())
}

pub async fn build_admin_app(
    config: Config,
    clock: ClockHandle,
) -> Result<AdminApp, AdminBuildError> {
    build_admin_app_with_path(config, None, clock).await
}

pub async fn build_admin_app_with_path(
    config: Config,
    config_path: Option<&Path>,
    clock: ClockHandle,
) -> Result<AdminApp, AdminBuildError> {
    build_admin_app_with_path_inner(
        config,
        config_path,
        StartupConfigOverrides {
            split_mode_read_only_config: true,
            ..Default::default()
        },
        clock,
    )
    .await
}

async fn build_admin_app_with_path_inner(
    config: Config,
    config_path: Option<&Path>,
    startup_config_overrides: StartupConfigOverrides,
    clock: ClockHandle,
) -> Result<AdminApp, AdminBuildError> {
    config.validate()?;
    let aead = load_aead_from_env(&config.aead.key_env).map_err(|error| match error {
        AeadKeyError::Missing { env } => AdminBuildError::StorageKeyMissing { env },
        AeadKeyError::Invalid => AdminBuildError::InvalidStorageKey,
    })?;
    let opened = open_storage(&config.storage, clock.clone()).await?;
    let opened_scheduler =
        open_scheduler_storage(&config.storage, &config.scheduler, clock.clone()).await?;
    build_admin_app_with_storage_inner(
        config,
        config_path,
        startup_config_overrides,
        opened.managed_key_store,
        opened.storage,
        aead,
        opened_scheduler,
        clock,
        None,
    )
    .await
}

pub async fn build_admin_app_with_storage(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
) -> Result<AdminApp, AdminBuildError> {
    let opened_scheduler =
        open_scheduler_storage(&config.storage, &config.scheduler, clock.clone()).await?;
    build_admin_app_with_storage_inner(
        config,
        config_path,
        StartupConfigOverrides {
            split_mode_read_only_config: true,
            ..Default::default()
        },
        managed_store,
        storage,
        aead,
        opened_scheduler,
        clock,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn build_admin_app_with_storage_inner(
    config: Config,
    config_path: Option<&Path>,
    mut startup_config_overrides: StartupConfigOverrides,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    opened_scheduler: OpenedScheduler,
    clock: ClockHandle,
    admin_auth_override: Option<Arc<AdminAuthenticator>>,
) -> Result<AdminApp, AdminBuildError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let _ = cc_lb_providers::builtin_provider_registry();
    startup_config_overrides.split_mode_read_only_config = true;

    let data_dir = resolve_data_dir(None, config.runtime.data_dir.as_deref(), "CC_LB_DATA_DIR")?;
    let replica_identity = match load_or_create_replica_id(&data_dir) {
        Ok(id) => Some(cc_lb_domain::ReplicaIdentity { id }),
        Err(error) => {
            tracing::warn!(error = %error, "failed to load or create replica ID; proceeding without it");
            None
        }
    };

    let key_store = Arc::new(KeyStore::new(managed_store));
    let in_memory_bus = InMemoryBus::with_capacity(config.event_bus.broadcast_capacity);
    let storage_tail_tx =
        tokio::sync::broadcast::channel(cc_lb_admin::events::DEFAULT_STORAGE_TAIL_CAPACITY).0;

    let (event_fanout_shutdown_tx, event_fanout_shutdown_rx) = watch::channel(false);
    #[allow(unused_mut)]
    let mut event_fanout_tasks = Vec::new();
    #[cfg(not(feature = "postgres"))]
    {
        let _ = &event_fanout_shutdown_rx;
    }

    let event_bus: Arc<dyn RequestEventBus> = match &config.storage {
        StorageConfig::Sqlite { .. } => {
            let initial_cursor = storage.current_request_event_cursor().await?;
            event_fanout_tasks.push(
                cc_lb_runtime_sync::live_tail::storage_tail_poller::StorageTailPoller::spawn(
                    storage.clone(),
                    storage_tail_tx.clone(),
                    Duration::from_millis(config.event_bus.storage_tail_poll_interval_ms.max(1)),
                    initial_cursor,
                    event_fanout_shutdown_rx,
                ),
            );
            Arc::new(in_memory_bus)
        }
        StorageConfig::Postgres { .. } => {
            #[cfg(not(feature = "postgres"))]
            {
                return Err(AdminBuildError::StorageFactory(
                    StorageFactoryError::FeatureDisabled {
                        backend: "postgres".to_owned(),
                    },
                ));
            }
            #[cfg(feature = "postgres")]
            {
                let pg_pool =
                    cc_lb_runtime_sync::storage_factory::open_pg_fanout_pool(&config.storage)
                        .await?
                        .ok_or(AdminBuildError::PgNotifyPoolUnavailable)?;
                let listener_bus: Arc<dyn RequestEventBus> = Arc::new(in_memory_bus);
                let listener_id = uuid::Uuid::new_v4().to_string();
                event_fanout_tasks.push(
                    cc_lb_runtime_sync::live_tail::pg_notify_fanout::PgListener::spawn_with_channel(
                        pg_pool,
                        listener_bus.clone(),
                        config.event_bus.pg_notify_channel.clone(),
                        listener_id,
                        event_fanout_shutdown_rx.clone(),
                    ),
                );
                let initial_cursor = storage.current_request_event_cursor().await?;
                event_fanout_tasks.push(
                    cc_lb_runtime_sync::live_tail::storage_tail_poller::StorageTailPoller::spawn(
                        storage.clone(),
                        storage_tail_tx.clone(),
                        Duration::from_millis(
                            config.event_bus.storage_tail_poll_interval_ms.max(1),
                        ),
                        initial_cursor,
                        event_fanout_shutdown_rx,
                    ),
                );
                listener_bus
            }
        }
    };

    let dynamic_view = Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopAdminSignerFactory))
            .principal_view(Arc::new(PrincipalView::from_db(&[], HashMap::new())))
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .subscription_quota_routing_max_staleness_secs(
                config.subscription_quota.routing_max_staleness_secs,
            )
            .build(),
    ));

    let admin_addr = config.listener.admin_addr;
    let admin_config = Arc::new(config);
    let admin_auth = match admin_auth_override {
        Some(auth) => auth,
        None => Arc::new(AdminAuthenticator::new(
            build_providers(&admin_config.admin.auth).map_err(|error| {
                AdminBuildError::AdminAuth {
                    message: error.to_string(),
                }
            })?,
        )),
    };
    let meta = BuildMeta::current();
    let admin_state = AdminState {
        build: AdminBuildInfo {
            git_sha: meta.git_sha,
            rustc: meta.rustc,
            profile: meta.profile,
            target: meta.target,
        },
        config_path: config_path.map(Path::to_path_buf),
        startup_config_overrides,
        storage: Some(storage),
        key_store: Some(key_store),
        aead,
        lifecycle: Some(AdminPorts {
            warmup: None,
            subscription_quota_ingestion: None,
            replica_identity,
        }),
        lazy_refresher: None,
        runtime: None,
        data_dir: Some(data_dir),
        warmup_dialect_dispatcher: None,
        warmup_window_verifier: None,
        limit_reset_usage_refresh: None,
        dynamic_view,
        config: admin_config,
        dynamic_view_rebinder: None,
        scheduler: Some(SchedulerAdminHandle::new(opened_scheduler.lazy_handle())),
        admin_auth,
        start_time: Instant::now(),
        event_bus: Some(event_bus),
        storage_tail: storage_tail_tx,
        clock,
        anthropic_org_api: AnthropicOrgApi::production()
            .map_err(AdminBuildError::AnthropicOrgOrigin)?,
    };

    Ok(AdminApp {
        admin_router: wrap_admin_router(admin_state),
        admin_addr,
        event_fanout_shutdown_tx: Some(event_fanout_shutdown_tx),
        event_fanout_tasks,
    })
}

fn wrap_admin_router(admin_state: AdminState) -> Router {
    let router = cc_lb_admin::router(admin_state);
    let router = cc_lb_admin::routes::with_request_tracing(router)
        .layer(admin_compression_layer())
        .layer(middleware::from_fn_with_state(
            RequestIdState::counter("req_admin_"),
            request_id_middleware,
        ));
    with_browser_security_headers(router)
}

fn resolve_data_dir(
    cli: Option<&Path>,
    config_value: Option<&Path>,
    env_var: &str,
) -> Result<PathBuf, AdminBuildError> {
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

fn init_observability(config: &Config) -> Result<TracingGuard, AdminBuildError> {
    let guard = cc_lb_observability::init(&ObservabilityConfig {
        tracing_level: config.observability.tracing_level.clone(),
        otlp_endpoint: config.observability.otlp_endpoint.clone(),
        metrics_addr: config.listener.metrics_addr,
        log_redaction: config.observability.log_redaction,
        user_prompt_redaction: config.observability.user_prompt_redaction,
    })?;
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    cc_lb_scheduler::scheduler_metrics::touch_scheduler_metric_handles();
    Ok(guard)
}

async fn await_or_abort<T>(mut task: JoinHandle<T>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).ok();
        match sigterm.as_mut() {
            Some(term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            None => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct NoopAdminSignerFactory;

impl ApiKeyAwareSignerFactory for NoopAdminSignerFactory {
    fn with_router_choice(&self, _router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(NoopAdminSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopAdminSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        Err(SignerError::MissingCredentials {
            reason: "split admin server does not sign proxy requests".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn write_raw_request(stream: &TcpStream, req: &str) {
        stream.writable().await.expect("writable");
        let mut written = 0;
        while written < req.len() {
            match stream.try_write(&req.as_bytes()[written..]) {
                Ok(n) => written += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    stream.writable().await.expect("writable retry");
                }
                Err(e) => panic!("write http request failed: {e}"),
            }
        }
    }

    async fn read_until_contains(stream: &TcpStream, needle: &str) -> String {
        let mut buf = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            assert!(
                !remaining.is_zero(),
                "timed out waiting for `{needle}`; got: {}",
                String::from_utf8_lossy(&buf)
            );
            let mut tmp = [0u8; 1024];
            tokio::time::timeout(remaining, stream.readable())
                .await
                .expect("readable timeout")
                .expect("readable");
            match stream.try_read(&mut tmp) {
                Ok(0) => {
                    let text = String::from_utf8_lossy(&buf).into_owned();
                    assert!(
                        text.contains(needle),
                        "unexpected EOF before `{needle}`; got: {text}"
                    );
                    return text;
                }
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if String::from_utf8_lossy(&buf).contains(needle) {
                        return String::from_utf8_lossy(&buf).into_owned();
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("read http response failed: {e}"),
            }
        }
    }

    async fn assert_peer_closed(stream: &TcpStream) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut buf = [0u8; 1024];
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or(Duration::ZERO);
            assert!(
                !remaining.is_zero(),
                "timed out waiting for peer TCP connection to close after server shutdown",
            );
            tokio::time::timeout(remaining, stream.readable())
                .await
                .expect("timed out waiting for peer socket readiness on close")
                .expect("peer socket readable failed");
            match stream.try_read(&mut buf) {
                Ok(0) => return,
                Ok(_) => continue,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(ref e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return;
                }
                Err(e) => panic!("unexpected peer read error after shutdown: {e}"),
            }
        }
    }

    #[tokio::test]
    async fn serve_with_listener_gracefully_drains_in_flight_request_before_timeout() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
        let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));

        let router = Router::new().route(
            "/slow",
            axum::routing::get(move || {
                let started_tx = started_tx.clone();
                let release_rx = release_rx.clone();
                async move {
                    if let Some(tx) = started_tx.lock().expect("lock").take() {
                        let _ = tx.send(());
                    }
                    let rx = release_rx.lock().await.take();
                    if let Some(rx) = rx {
                        let _ = rx.await;
                    }
                    "done"
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let app = AdminApp {
            admin_router: router,
            admin_addr: addr,
            event_fanout_shutdown_tx: None,
            event_fanout_tasks: Vec::new(),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            app.serve_with_listener(
                listener,
                async move {
                    let _ = shutdown_rx.await;
                },
                Duration::from_secs(2),
            )
            .await
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        write_raw_request(
            &stream,
            &format!("GET /slow HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        )
        .await;
        started_rx.await.expect("handler started");

        let _ = shutdown_tx.send(());
        let _ = release_tx.send(());

        let body = read_until_contains(&stream, "done").await;
        assert!(
            body.starts_with("HTTP/1.1 200 OK"),
            "unexpected response: {body}"
        );
        assert_peer_closed(&stream).await;

        let serve_res = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("graceful drain timed out")
            .expect("server task panicked");
        serve_res.expect("serve_with_listener must succeed");
    }

    #[cfg(feature = "sqlite")]
    struct FixedTokenAuthProvider(&'static str);

    #[cfg(feature = "sqlite")]
    #[async_trait]
    impl cc_lb_admin::auth::AdminAuthProvider for FixedTokenAuthProvider {
        fn id(&self) -> &str {
            "fixed-test-token"
        }

        fn is_static_token(&self) -> bool {
            true
        }

        async fn authenticate(
            &self,
            headers: &axum::http::HeaderMap,
        ) -> cc_lb_admin::auth::ProviderOutcome {
            let expected = format!("Bearer {}", self.0);
            let provided = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok());
            if provided == Some(expected.as_str()) {
                cc_lb_admin::auth::ProviderOutcome::Verified(cc_lb_admin::auth::AdminIdentity {
                    authority: "test".to_owned(),
                    subject: "shutdown-test".to_owned(),
                    kind: cc_lb_admin::auth::AdminActorKind::Service,
                    provider_id: self.id().to_owned(),
                    email: None,
                    display_name: None,
                    groups: Vec::new(),
                    expires_at_unix_secs: None,
                })
            } else {
                cc_lb_admin::auth::ProviderOutcome::NotPresent
            }
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn admin_app_shutdown_is_bounded_with_active_sse_stream() {
        use cc_lb_storage_api::MetaStore as _;
        const SYNTHETIC_TOKEN: &str = "cc-lb-synthetic-shutdown-test-token";

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

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");

        let mut config = Config {
            storage: StorageConfig::Sqlite {
                path: sqlite_path.clone(),
            },
            ..Config::default()
        };
        config.listener.admin_addr = addr;
        config.aead.key_env = "__CC_LB_TEST_SHUTDOWN_KEY__".to_owned();
        config.runtime.data_dir = Some(dir.path().to_path_buf());
        config.event_bus.storage_tail_poll_interval_ms = 25;

        let opened_scheduler =
            open_scheduler_storage(&config.storage, &config.scheduler, clock.clone())
                .await
                .expect("open scheduler storage");
        let synthetic_auth = Arc::new(AdminAuthenticator::new(vec![Arc::new(
            FixedTokenAuthProvider(SYNTHETIC_TOKEN),
        )]));
        let managed_store: Arc<dyn ManagedKeyStore> = storage_arc.clone();
        let storage: Arc<dyn Storage> = storage_arc.clone();
        let app = build_admin_app_with_storage_inner(
            config,
            None,
            StartupConfigOverrides {
                split_mode_read_only_config: true,
                ..Default::default()
            },
            managed_store,
            storage,
            aead,
            opened_scheduler,
            clock,
            Some(synthetic_auth),
        )
        .await
        .expect("build_admin_app_with_storage_inner");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            app.serve_with_listener(
                listener,
                async move {
                    let _ = shutdown_rx.await;
                },
                Duration::from_millis(100),
            )
            .await
        });

        let stream = TcpStream::connect(addr).await.expect("connect to admin");
        let req = format!(
            "GET /admin/v1/events/stream HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {SYNTHETIC_TOKEN}\r\nAccept: text/event-stream\r\n\r\n"
        );
        write_raw_request(&stream, &req).await;
        let _ = read_until_contains(&stream, ": connected").await;

        // Trigger shutdown while keeping `stream` open and keeping this runtime alive.
        let _ = shutdown_tx.send(());
        let serve_res = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("split admin server must finish bounded shutdown despite active SSE stream")
            .expect("server task panicked");
        serve_res.expect("serve_with_listener must succeed");

        // Prove both the active connection task and the listening socket closed
        // before `serve_with_listener` returned, without dropping the caller runtime.
        assert_peer_closed(&stream).await;
        assert!(
            TcpStream::connect(addr).await.is_err(),
            "admin listener must stop accepting new connections after shutdown"
        );
    }
}
