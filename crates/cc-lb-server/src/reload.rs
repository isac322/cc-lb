use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use cc_lb_admin::{DynamicViewRebinder, LastReloadStatus, ReloadOutcome};
use cc_lb_config::{BootEnv, Config, ConfigError, RestartRequiredField, StorageConfig};
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::clock::{ClockHandle, unix_secs};
use cc_lb_storage_api::Storage;
use thiserror::Error;
use tokio::sync::broadcast;

const BROADCAST_CAPACITY: usize = 16;

pub struct ConfigWatcher {
    boot: Arc<BootEnv>,
    storage: Arc<dyn Storage>,
    process_start_config: Arc<Config>,
    current: ArcSwap<Config>,
    reload_tx: broadcast::Sender<Arc<Config>>,
    reloads_attempted: AtomicUsize,
    last_reload_status: Arc<ArcSwap<Option<LastReloadStatus>>>,
    dynamic_view: Option<Arc<DynamicViewHolder>>,
    dynamic_view_rebinder: Mutex<Option<Arc<dyn DynamicViewRebinder>>>,
    clock: ClockHandle,
}

impl ConfigWatcher {
    pub fn new(
        boot: Arc<BootEnv>,
        storage: Arc<dyn Storage>,
        initial_config: Config,
        _runtime: Arc<WasmtimeRuntime>,
        clock: ClockHandle,
    ) -> Self {
        Self::new_with_principal_view(boot, storage, initial_config, _runtime, None, clock)
    }

    pub fn new_with_principal_view(
        boot: Arc<BootEnv>,
        storage: Arc<dyn Storage>,
        initial_config: Config,
        _runtime: Arc<WasmtimeRuntime>,
        dynamic_view: Option<Arc<DynamicViewHolder>>,
        clock: ClockHandle,
    ) -> Self {
        let (reload_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let process_start_config = Arc::new(initial_config.clone());
        Self {
            boot,
            storage,
            process_start_config,
            current: ArcSwap::from_pointee(initial_config),
            reload_tx,
            reloads_attempted: AtomicUsize::new(0),
            last_reload_status: Arc::new(ArcSwap::from(Arc::new(None))),
            dynamic_view,
            dynamic_view_rebinder: Mutex::new(None),
            clock,
        }
    }

    pub fn current_config(&self) -> Arc<Config> {
        self.current.load_full()
    }

    pub fn current(&self) -> Arc<Config> {
        self.current_config()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Config>> {
        self.reload_tx.subscribe()
    }

    pub fn set_dynamic_view_rebinder(&self, rebinder: Arc<dyn DynamicViewRebinder>) {
        if let Ok(mut current) = self.dynamic_view_rebinder.lock() {
            *current = Some(rebinder);
        }
    }

    pub fn reload_attempts(&self) -> usize {
        self.reloads_attempted.load(Ordering::Acquire)
    }

    pub fn reload_now(&self) -> Result<Arc<Config>, ReloadError> {
        let overlay = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.storage.get_effective_config())
        })
        .map_err(|source| {
            self.record_attempt();
            self.record_failure(source.to_string(), None, None);
            ReloadError::Storage(source.to_string())
        })?;

        let overlay_value = overlay.map(|effective| effective.config);
        let new_config = match Config::compose(&self.boot, overlay_value) {
            Ok(config) => config,
            Err(source) => {
                self.record_attempt();
                self.record_failure(source.to_string(), None, None);
                return Err(ReloadError::Config(source));
            }
        };

        let current_config = self.current_config();
        if *current_config == new_config {
            return Ok(current_config);
        }

        self.record_attempt();
        for change in summarize_restart_required(&current_config, &new_config) {
            tracing::warn!(
                field = %change.field,
                current = %change.current,
                new = %change.new,
                reason = %change.reason,
                "restart required to apply"
            );
        }

        let new_config = Arc::new(new_config);
        if self.dynamic_view.is_some() {
            tracing::debug!(
                "dynamic runtime view reload is storage-driven; static config reload skips principal/plugin rebuild"
            );
        }
        self.current.store(Arc::clone(&new_config));
        metrics::counter!("cc_lb_config_reload_total", "outcome" => "success").increment(1);
        self.record_success();
        let _receivers = self.reload_tx.send(Arc::clone(&new_config));
        tracing::info!("configuration reload accepted");
        Ok(new_config)
    }

    fn record_attempt(&self) {
        self.reloads_attempted.fetch_add(1, Ordering::AcqRel);
    }

    fn record_success(&self) {
        self.last_reload_status
            .store(Arc::new(Some(LastReloadStatus {
                timestamp_unix_secs: unix_secs(self.clock.now()),
                outcome: ReloadOutcome::Success,
            })));
    }

    fn record_failure(&self, reason: String, principal: Option<String>, plugin: Option<String>) {
        metrics::counter!("cc_lb_config_reload_total", "outcome" => "failure").increment(1);
        metrics::counter!("cc_lb_config_reload_failed_total").increment(1);
        tracing::warn!(
            error = %reason,
            principal = principal.as_deref(),
            plugin = plugin.as_deref(),
            "configuration reload failed"
        );
        self.last_reload_status
            .store(Arc::new(Some(LastReloadStatus {
                timestamp_unix_secs: unix_secs(self.clock.now()),
                outcome: ReloadOutcome::Failure {
                    reason,
                    principal,
                    plugin,
                },
            })));
    }
}

impl cc_lb_admin::CurrentConfig for ConfigWatcher {
    fn current_config(&self) -> Arc<Config> {
        ConfigWatcher::current_config(self)
    }

    fn restart_required_changes(&self) -> Vec<RestartRequiredField> {
        summarize_restart_required(&self.process_start_config, &self.current_config())
    }

    fn last_reload_status(&self) -> Option<LastReloadStatus> {
        let guard = self.last_reload_status.load();
        guard.as_ref().clone()
    }
}

impl cc_lb_admin::ConfigReloader for ConfigWatcher {
    fn reload_now(&self) -> Result<(), String> {
        ConfigWatcher::reload_now(self)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Error)]
pub enum ReloadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("storage read failed: {0}")]
    Storage(String),
}

/// Diff a current vs a candidate `Config` and return every field that is
/// captured at process startup and therefore requires a restart to take
/// effect. The list is intentionally conservative: any field that is read
/// once at `run_serve()` boot and never re-read through `current_config()`
/// must appear here so the dashboard can warn operators honestly.
pub fn summarize_restart_required(current: &Config, new: &Config) -> Vec<RestartRequiredField> {
    let mut changes = Vec::new();

    // ----- Boot-only (env-bound; should never differ via dashboard, but keep
    // for defence-in-depth in case BootEnv reconstruction differs.) -----
    push_if_changed(
        &mut changes,
        "listener.proxy_addr",
        &current.listener.proxy_addr,
        &new.listener.proxy_addr,
        "rebinding the proxy socket requires a process restart",
    );
    push_if_changed(
        &mut changes,
        "listener.admin_addr",
        &current.listener.admin_addr,
        &new.listener.admin_addr,
        "rebinding the admin socket requires a process restart",
    );
    push_if_changed(
        &mut changes,
        "listener.metrics_addr",
        &current.listener.metrics_addr,
        &new.listener.metrics_addr,
        "rebinding the metrics socket requires a process restart",
    );
    push_if_changed_str(
        &mut changes,
        "storage.kind",
        storage_kind(&current.storage),
        storage_kind(&new.storage),
        "switching storage backends requires a process restart",
    );
    push_if_changed(
        &mut changes,
        "aead.key_env",
        &current.aead.key_env,
        &new.aead.key_env,
        "rotating the AEAD master key env requires a restart",
    );
    let current_tls_cert = current
        .listener
        .tls
        .as_ref()
        .and_then(|tls| tls.cert_path.as_ref());
    let new_tls_cert = new
        .listener
        .tls
        .as_ref()
        .and_then(|tls| tls.cert_path.as_ref());
    push_if_changed_opt_path(
        &mut changes,
        "listener.tls.cert_path",
        current_tls_cert,
        new_tls_cert,
        "TLS cert path is loaded at startup",
    );

    // ----- Runtime fields captured at startup (snapshot semantics). -----
    // Body caps are baked into `LifecycleConfig` at `build_app_with_path_inner`.
    push_if_changed(
        &mut changes,
        "body.messages_cap_bytes",
        &current.body.messages_cap_bytes,
        &new.body.messages_cap_bytes,
        "body cap is baked into the lifecycle pipeline at startup",
    );
    push_if_changed(
        &mut changes,
        "body.files_cap_bytes",
        &current.body.files_cap_bytes,
        &new.body.files_cap_bytes,
        "body cap is baked into the lifecycle pipeline at startup",
    );

    // Downstream auth: BuiltinAuthn is constructed once at startup.
    push_if_changed_debug(
        &mut changes,
        "downstream_auth.mode",
        &current.downstream_auth.mode,
        &new.downstream_auth.mode,
        "BuiltinAuthn is constructed once at startup",
    );
    push_if_changed_debug(
        &mut changes,
        "downstream_auth.none_mode",
        &current.downstream_auth.none_mode,
        &new.downstream_auth.none_mode,
        "BuiltinAuthn is constructed once at startup",
    );

    // OAuth: LazyRefresher and NotifyListener snapshot oauth.anthropic at boot.
    push_if_changed_debug(
        &mut changes,
        "oauth.anthropic",
        &current.oauth.anthropic,
        &new.oauth.anthropic,
        "OAuth refresher captures these at startup",
    );

    // Timeouts: drain and upstream_total are wired into shutdown/router at boot.
    push_if_changed(
        &mut changes,
        "timeouts.drain_secs",
        &current.timeouts.drain_secs,
        &new.timeouts.drain_secs,
        "drain timeout is wired into the shutdown signal at startup",
    );
    push_if_changed(
        &mut changes,
        "timeouts.upstream_total_secs",
        &current.timeouts.upstream_total_secs,
        &new.timeouts.upstream_total_secs,
        "upstream total timeout is wired into the proxy router at startup",
    );

    // Observability: tracing subscriber and metrics exporters bound at startup.
    push_if_changed(
        &mut changes,
        "observability.tracing_level",
        &current.observability.tracing_level,
        &new.observability.tracing_level,
        "tracing subscriber is installed at startup",
    );
    push_if_changed_opt(
        &mut changes,
        "observability.otlp_endpoint",
        current.observability.otlp_endpoint.as_ref(),
        new.observability.otlp_endpoint.as_ref(),
        "OTLP exporter is installed at startup",
    );
    push_if_changed(
        &mut changes,
        "observability.log_redaction",
        &current.observability.log_redaction,
        &new.observability.log_redaction,
        "log redaction policy is installed at startup",
    );
    push_if_changed(
        &mut changes,
        "observability.user_prompt_redaction",
        &current.observability.user_prompt_redaction,
        &new.observability.user_prompt_redaction,
        "prompt redaction policy is installed at startup",
    );

    // Circuit breaker / bulkhead: configured at dispatcher build time.
    push_if_changed(
        &mut changes,
        "circuit_breaker.failures_to_open",
        &current.circuit_breaker.failures_to_open,
        &new.circuit_breaker.failures_to_open,
        "circuit breaker is built at startup",
    );
    push_if_changed(
        &mut changes,
        "circuit_breaker.window_secs",
        &current.circuit_breaker.window_secs,
        &new.circuit_breaker.window_secs,
        "circuit breaker is built at startup",
    );
    push_if_changed(
        &mut changes,
        "circuit_breaker.half_open_after_secs",
        &current.circuit_breaker.half_open_after_secs,
        &new.circuit_breaker.half_open_after_secs,
        "circuit breaker is built at startup",
    );
    push_if_changed(
        &mut changes,
        "bulkhead.max_conns_per_upstream",
        &current.bulkhead.max_conns_per_upstream,
        &new.bulkhead.max_conns_per_upstream,
        "bulkhead limits are built at startup",
    );
    push_if_changed(
        &mut changes,
        "bulkhead.semaphore_per_upstream",
        &current.bulkhead.semaphore_per_upstream,
        &new.bulkhead.semaphore_per_upstream,
        "bulkhead limits are built at startup",
    );

    // Subscription quota: writer + cache are built at startup.
    push_if_changed(
        &mut changes,
        "subscription_quota.writer_channel_capacity",
        &current.subscription_quota.writer_channel_capacity,
        &new.subscription_quota.writer_channel_capacity,
        "subscription quota writer channel is sized at startup",
    );
    push_if_changed(
        &mut changes,
        "subscription_quota.writer_batch_max_records",
        &current.subscription_quota.writer_batch_max_records,
        &new.subscription_quota.writer_batch_max_records,
        "subscription quota writer batch is configured at startup",
    );
    push_if_changed(
        &mut changes,
        "subscription_quota.writer_flush_ms",
        &current.subscription_quota.writer_flush_ms,
        &new.subscription_quota.writer_flush_ms,
        "subscription quota writer flush is configured at startup",
    );
    push_if_changed(
        &mut changes,
        "subscription_quota.routing_max_staleness_secs",
        &current.subscription_quota.routing_max_staleness_secs,
        &new.subscription_quota.routing_max_staleness_secs,
        "subscription quota cache staleness is configured at startup",
    );
    // Prompt-cache shadow: built into LifecycleConfig.
    push_if_changed_debug(
        &mut changes,
        "prompt_cache_shadow",
        &current.prompt_cache_shadow,
        &new.prompt_cache_shadow,
        "prompt cache shadow pipeline is built at startup",
    );

    // Scheduler: built once at startup.
    push_if_changed_debug(
        &mut changes,
        "scheduler",
        &current.scheduler,
        &new.scheduler,
        "scheduler workers are built at startup",
    );

    // API keys / price catalog: refresher task started at startup.
    push_if_changed_debug(
        &mut changes,
        "api_keys",
        &current.api_keys,
        &new.api_keys,
        "API key catalog refresher is started at startup",
    );

    macro_rules! push_startup_snapshot {
        ($field:ident) => {
            push_if_changed_debug(
                &mut changes,
                stringify!($field),
                &current.$field,
                &new.$field,
                "component wiring is captured at startup",
            );
        };
    }
    push_startup_snapshot!(event_bus);
    push_startup_snapshot!(cluster);
    push_startup_snapshot!(limit_reservation_ttl);

    changes
}

fn push_if_changed<T>(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: &T,
    new: &T,
    reason: &str,
) where
    T: PartialEq + std::fmt::Display,
{
    if current != new {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current: current.to_string(),
            new: new.to_string(),
            reason: reason.to_owned(),
        });
    }
}

fn push_if_changed_debug<T>(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: &T,
    new: &T,
    reason: &str,
) where
    T: PartialEq + std::fmt::Debug,
{
    if current != new {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current: format!("{current:?}"),
            new: format!("{new:?}"),
            reason: reason.to_owned(),
        });
    }
}

fn push_if_changed_str(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: &str,
    new: &str,
    reason: &str,
) {
    if current != new {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current: current.to_owned(),
            new: new.to_owned(),
            reason: reason.to_owned(),
        });
    }
}

fn push_if_changed_opt<T>(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: Option<&T>,
    new: Option<&T>,
    reason: &str,
) where
    T: PartialEq + std::fmt::Display,
{
    if current != new {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current: option_display(current),
            new: option_display(new),
            reason: reason.to_owned(),
        });
    }
}

fn push_if_changed_opt_path(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: Option<&PathBuf>,
    new: Option<&PathBuf>,
    reason: &str,
) {
    if current.map(|p| p.as_path()) != new.map(|p| p.as_path()) {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current: path_option_string(current),
            new: path_option_string(new),
            reason: reason.to_owned(),
        });
    }
}

fn option_display<T: std::fmt::Display>(value: Option<&T>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn path_option_string(path: Option<&PathBuf>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_default()
}

fn storage_kind(storage: &StorageConfig) -> &'static str {
    match storage {
        StorageConfig::Postgres { .. } => "postgres",
        StorageConfig::Sqlite { .. } => "sqlite",
    }
}
