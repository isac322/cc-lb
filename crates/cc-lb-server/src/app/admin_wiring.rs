use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use cc_lb_admin::auth::{AdminAuthenticator, build_providers};
use cc_lb_admin::{
    AdminPorts, AdminState, DynamicViewRebinder, StartupConfigOverrides,
    WarmupDialectDispatchError, WarmupDialectDispatchErrorKind, WarmupDialectDispatchOutcome,
    WarmupDialectDispatcher, WarmupNativeDispatchOutcome,
};
use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_config::Config;
use cc_lb_control::{DynamicView, PromptCacheObservationSinkLike};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle as _;
use cc_lb_storage_api::{ManagedKeyStore, Storage};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::rt::TokioExecutor;

use crate::admin_ports::{ServerSubscriptionQuotaIngestionPort, ServerWarmupPort};
use crate::build_meta::BuildMeta;
use crate::dynamic_view_builder::{Stores as DynamicStores, build_dynamic_view};
use crate::refresh::{LazyRefreshClaimGuard, LazyRefresher};
use crate::subscription_quota_cache::SubscriptionQuotaCache;

use super::bootstrap::{
    ProxyRole, ShutdownRuntime, StartupPreflight, build_runtime_with_storage_core,
    init_observability, serve_runtime,
};
use super::*;

pub struct App {
    pub router: Router,
    pub admin_router: Router,
    pub proxy_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    shutdown: ShutdownRuntime,
}

impl App {
    pub fn set_draining(&self, draining: bool) {
        self.shutdown.drain_controller.set_draining(draining);
    }

    pub fn drain_controller(&self) -> cc_lb_engine::DrainController {
        self.shutdown.drain_controller.clone()
    }

    pub fn signal_handle(&self) -> crate::signal::SignalHandle {
        self.shutdown.signals.clone()
    }

    pub async fn start(self) -> Result<(), BuildError> {
        serve_runtime(
            self.shutdown,
            self.router,
            self.proxy_addr,
            Some((self.admin_router, self.admin_addr)),
        )
        .await
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
        ..Default::default()
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

pub async fn build_app(config: Config, clock: ClockHandle) -> Result<App, BuildError> {
    build_app_with_path(config, None, clock).await
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

pub async fn build_app_with_storage(
    config: Config,
    config_path: Option<&Path>,
    managed_store: Arc<dyn ManagedKeyStore>,
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
) -> Result<App, BuildError> {
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
    // Shared with AdminState so the SSE handler subscribes to the same broadcast
    // the StorageTailPoller feeds. Kept live for the InMemory transport too —
    // AdminState holds one Sender clone so the channel never closes, and no
    // poller produces on it (finals arrive via the local bus in that mode).
    let storage_tail_tx: tokio::sync::broadcast::Sender<cc_lb_request_log::StorageTailUpdate> =
        tokio::sync::broadcast::channel(cc_lb_admin::events::DEFAULT_STORAGE_TAIL_CAPACITY).0;

    let core = build_runtime_with_storage_core(
        config,
        ProxyRole::ProxyWorker,
        Some(storage_tail_tx.clone()),
        managed_store,
        storage,
        aead,
        startup_preflight,
        opened_scheduler,
        lazy_refresh_claim_guard,
        clock.clone(),
    )
    .await?;

    let admin_rebinder: Arc<dyn DynamicViewRebinder> = Arc::new(ServerDynamicViewRebinder {
        stores: core.stores.clone(),
        runtime: core.runtime.clone(),
        aead: core.aead.clone(),
        lazy_refresher: core.lazy_refresher.clone(),
        data_dir: core.data_dir.clone(),
        subscription_quota_cache: core.subscription_quota_cache.clone(),
        prompt_cache_grace_margin_secs: core.prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink: core.prompt_cache_observation_sink.clone(),
        subscription_quota_routing_max_staleness_secs: core
            .config
            .subscription_quota
            .routing_max_staleness_secs,
        clock: clock.clone(),
        gemini_config: core.config.oauth.gemini.clone(),
        antigravity_config: core.config.oauth.antigravity.clone(),
    });

    let admin_config = Arc::new(core.config);
    let admin_auth = Arc::new(AdminAuthenticator::new(
        build_providers(&admin_config.admin.auth).map_err(|error| BuildError::AdminAuth {
            message: error.to_string(),
        })?,
    ));
    let admin_state = AdminState {
        build: BuildMeta::current().admin_build_info(),
        storage: Some(core.storage),
        key_store: Some(core.key_store),
        aead: core.aead.clone(),
        lifecycle: Some(AdminPorts {
            warmup: Some(Arc::new(ServerWarmupPort)),
            subscription_quota_ingestion: Some(Arc::new(
                ServerSubscriptionQuotaIngestionPort::new(core.shutdown.lifecycle.clone()),
            )),
            replica_identity: core.replica_identity,
        }),
        lazy_refresher: core.lazy_refresher,
        runtime: Some(core.runtime),
        config_path: config_path.map(Path::to_path_buf),
        startup_config_overrides,
        data_dir: Some(core.data_dir),
        warmup_dialect_dispatcher: core.lazy_refresher_concrete.map(|lazy_refresher| {
            Arc::new(ServerWarmupDialectDispatcher {
                stores: core.stores,
                aead: core.aead,
                lazy_refresher,
                http: warmup_dialect_http_client(),
                clock: clock.clone(),
                config: admin_config.clone(),
            }) as Arc<dyn WarmupDialectDispatcher>
        }),
        warmup_window_verifier: Some(core.warmup_window_verifier),
        limit_reset_usage_refresh: Some(core.limit_reset_usage_refresh),
        dynamic_view: core.dynamic_view,
        config: admin_config,
        dynamic_view_rebinder: Some(admin_rebinder),
        scheduler: Some(cc_lb_scheduler::admin::SchedulerAdminHandle::new(
            core.scheduler_lazy_handle,
        )),
        admin_auth,
        start_time: core.start_time,
        event_bus: Some(core.event_bus),
        storage_tail: storage_tail_tx,
        clock,
        anthropic_org_api: core.anthropic_org_api,
    };

    core.shutdown.server_state.transition_to_ready();

    Ok(App {
        router: core.router,
        admin_router: super::admin_router(admin_state),
        proxy_addr: core.proxy_addr,
        admin_addr: core.admin_addr,
        shutdown: core.shutdown,
    })
}

pub(super) struct ServerWarmupDialectDispatcher {
    stores: Arc<DynamicStores>,
    aead: Arc<AeadService>,
    lazy_refresher: Arc<LazyRefresher>,
    http: crate::warmup::request::WarmupHttpClient,
    clock: ClockHandle,
    config: Arc<Config>,
}

#[async_trait]
impl WarmupDialectDispatcher for ServerWarmupDialectDispatcher {
    async fn dispatch_warmup_with_dialect(
        &self,
        runtime: &Arc<WasmtimeRuntime>,
        data_dir: &Path,
        upstream: &cc_lb_storage_api::UpstreamRecord,
        lane: cc_lb_storage_api::WarmupLane,
    ) -> Result<WarmupDialectDispatchOutcome, WarmupDialectDispatchError> {
        let outcome = crate::warmup::dialect::dispatch_warmup_with_dialect(
            crate::warmup::dialect::WarmupDialectDispatchParams {
                runtime,
                stores: self.stores.as_ref(),
                data_dir,
                aead: self.aead.clone(),
                lazy_refresher: Some(self.lazy_refresher.clone()),
                upstream,
                http: &self.http,
                clock: self.clock.clone(),
                config: self.config.as_ref(),
                lane,
            },
        )
        .await
        .map_err(map_warmup_dialect_error)?;

        Ok(WarmupDialectDispatchOutcome {
            status: outcome.status,
            headers: outcome.headers,
        })
    }

    async fn dispatch_warmup_native(
        &self,
        upstream: &cc_lb_storage_api::UpstreamRecord,
        lane: cc_lb_storage_api::WarmupLane,
    ) -> Result<WarmupNativeDispatchOutcome, WarmupDialectDispatchError> {
        use crate::warmup::request::{
            ProviderWarmupDispatchParams, WarmupRequestAttempt, dispatch_provider_warmup_attempt,
        };
        use cc_lb_storage_api::{
            WarmupDispatchKind, WarmupPermanentFailureReason, WarmupTransientFailureReason,
        };

        let needs_refresh = !upstream.oauth_never_refresh
            && upstream
                .oauth_credentials
                .as_ref()
                .is_some_and(|credentials| {
                    credentials
                        .decrypt(self.aead.as_ref(), upstream.id.as_bytes())
                        .is_ok_and(|bundle| {
                            bundle.expires_at_unix_secs.saturating_sub(60)
                                <= cc_lb_clock::unix_secs(self.clock.now())
                        })
                });
        let refreshed_upstream = if needs_refresh {
            if self.lazy_refresher.refresh_one(upstream.id).await.is_err() {
                return Ok(WarmupNativeDispatchOutcome::PermanentFailure {
                    dispatch_kind: WarmupDispatchKind::NotDispatched,
                    reason: WarmupPermanentFailureReason::OauthRefreshFailed,
                    status: StatusCode::BAD_GATEWAY,
                    error_detail: "warmup OAuth refresh failed".to_owned(),
                });
            }
            Some(
                self.stores
                    .upstreams
                    .get_by_id(upstream.id)
                    .await
                    .map_err(|error| {
                        WarmupDialectDispatchError::new(
                            WarmupDialectDispatchErrorKind::Transient,
                            error.to_string(),
                        )
                    })?
                    .ok_or_else(|| {
                        WarmupDialectDispatchError::new(
                            WarmupDialectDispatchErrorKind::Permanent,
                            "upstream missing after warmup OAuth refresh",
                        )
                    })?,
            )
        } else {
            None
        };
        let upstream = refreshed_upstream.as_ref().unwrap_or(upstream);
        Ok(
            match dispatch_provider_warmup_attempt(ProviderWarmupDispatchParams {
                http: &self.http,
                upstream,
                aead: self.aead.as_ref(),
                config: self.config.as_ref(),
                clock: self.clock.clone(),
                lane,
            })
            .await
            {
                WarmupRequestAttempt::Response {
                    status,
                    observations,
                    error_detail,
                } => WarmupNativeDispatchOutcome::Response {
                    status,
                    dispatch_kind: WarmupDispatchKind::Http,
                    observations,
                    error_detail,
                },
                WarmupRequestAttempt::CredentialDecryptFailed { error } => {
                    WarmupNativeDispatchOutcome::PermanentFailure {
                        dispatch_kind: WarmupDispatchKind::NotDispatched,
                        reason: WarmupPermanentFailureReason::CredentialDecryptFailed,
                        status: StatusCode::BAD_GATEWAY,
                        error_detail: error,
                    }
                }
                WarmupRequestAttempt::RequestBuildFailed { error } => {
                    WarmupNativeDispatchOutcome::PermanentFailure {
                        dispatch_kind: WarmupDispatchKind::NotDispatched,
                        reason: WarmupPermanentFailureReason::RequestBuildFailed,
                        status: StatusCode::BAD_GATEWAY,
                        error_detail: error,
                    }
                }
                WarmupRequestAttempt::NetworkError { error } => {
                    WarmupNativeDispatchOutcome::TransientFailure {
                        dispatch_kind: WarmupDispatchKind::Http,
                        reason: WarmupTransientFailureReason::NetworkError,
                        status: StatusCode::BAD_GATEWAY,
                        error_detail: error,
                    }
                }
            },
        )
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
        | crate::warmup::dialect::WarmupDispatchError::RegistryUnsupportedProvider(_)
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
    gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
    antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
}

#[async_trait]
impl DynamicViewRebinder for ServerDynamicViewRebinder {
    async fn rebuild_dynamic_view(
        &self,
        current_generation: u64,
    ) -> anyhow::Result<Arc<DynamicView>> {
        Ok(build_dynamic_view(
            &self.stores,
            self.gemini_config.as_ref(),
            self.antigravity_config.as_ref(),
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
