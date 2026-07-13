use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use cc_lb_admin::{DynamicViewRebinder, LastReloadStatus, ReloadOutcome};
use cc_lb_config::{
    Config, ConfigError, RestartRequiredField, StorageConfig, WasmtimeAllocationStrategy,
};
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::clock::{ClockHandle, unix_secs};
use notify::{Event, RecursiveMode, Watcher};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const DEBOUNCE: Duration = Duration::from_millis(500);
const BROADCAST_CAPACITY: usize = 16;

pub struct ConfigWatcher {
    path: PathBuf,
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
        path: impl AsRef<Path>,
        initial_config: Config,
        _runtime: Arc<WasmtimeRuntime>,
        clock: ClockHandle,
    ) -> Self {
        Self::new_with_principal_view(path, initial_config, _runtime, None, clock)
    }

    pub fn new_with_principal_view(
        path: impl AsRef<Path>,
        initial_config: Config,
        _runtime: Arc<WasmtimeRuntime>,
        dynamic_view: Option<Arc<DynamicViewHolder>>,
        clock: ClockHandle,
    ) -> Self {
        let (reload_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let process_start_config = Arc::new(initial_config.clone());
        Self {
            path: path.as_ref().to_path_buf(),
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

    pub fn spawn_file_watcher(self: &Arc<Self>) -> JoinHandle<()> {
        let watcher = Arc::clone(self);
        tokio::spawn(async move {
            watcher.watch_file_changes().await;
        })
    }

    pub async fn watch_file_changes(self: Arc<Self>) {
        if let Err(source) = self.run_file_watch().await {
            tracing::warn!(
                path = %self.path.display(),
                error = %source,
                "configuration file watch stopped"
            );
        }
    }

    pub fn reload_now(&self) -> Result<Arc<Config>, ReloadError> {
        self.reload_from_path(false)
    }

    fn reload_from_path(&self, skip_unchanged: bool) -> Result<Arc<Config>, ReloadError> {
        let config_path = self.config_path_string();
        let new_config = match Config::load(&self.path) {
            Ok(config) => config,
            Err(source) => {
                self.record_attempt();
                self.record_failure(source.to_string(), None, None, config_path);
                return Err(ReloadError::Config(source));
            }
        };

        let current_config = self.current_config();
        if skip_unchanged && *current_config == new_config {
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
        self.record_success(config_path);
        let _receivers = self.reload_tx.send(Arc::clone(&new_config));
        tracing::info!(path = %self.path.display(), "configuration reload accepted");
        Ok(new_config)
    }

    async fn run_file_watch(&self) -> Result<(), FileWatchError> {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _sent = event_tx.send(event);
        })?;
        let watch_dir = watch_dir_for(&self.path);
        watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

        while let Some(event) = event_rx.recv().await {
            if event_matches_config(&self.path, event) {
                self.debounce_and_reload(&mut event_rx).await;
            }
        }

        Ok(())
    }

    async fn debounce_and_reload(
        &self,
        event_rx: &mut mpsc::UnboundedReceiver<notify::Result<Event>>,
    ) {
        let deadline = tokio::time::sleep_until(tokio::time::Instant::now() + DEBOUNCE);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => break,
                event = event_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };
                    if event_matches_config(&self.path, event) {
                        deadline.as_mut().reset(tokio::time::Instant::now() + DEBOUNCE);
                    }
                }
            }
        }

        let _result = self.reload_from_path(true);
    }

    fn record_attempt(&self) {
        self.reloads_attempted.fetch_add(1, Ordering::AcqRel);
    }

    fn record_success(&self, config_path: Option<String>) {
        self.last_reload_status
            .store(Arc::new(Some(LastReloadStatus {
                timestamp_unix_secs: unix_secs(self.clock.now()),
                outcome: ReloadOutcome::Success,
                config_path,
            })));
    }

    fn record_failure(
        &self,
        reason: String,
        principal: Option<String>,
        plugin: Option<String>,
        config_path: Option<String>,
    ) {
        metrics::counter!("cc_lb_config_reload_total", "outcome" => "failure").increment(1);
        metrics::counter!("cc_lb_config_reload_failed_total").increment(1);
        tracing::warn!(
            path = %self.path.display(),
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
                config_path,
            })));
    }

    fn config_path_string(&self) -> Option<String> {
        Some(self.path.display().to_string())
    }
}

impl cc_lb_admin::CurrentConfig for ConfigWatcher {
    fn current_config(&self) -> Arc<Config> {
        ConfigWatcher::current_config(self)
    }

    fn last_reload_status(&self) -> Option<LastReloadStatus> {
        self.last_reload_status.load().as_ref().clone()
    }

    fn restart_required_changes(&self) -> Vec<RestartRequiredField> {
        summarize_restart_required(&self.process_start_config, &self.current_config())
    }

    fn dynamic_view_rebinder(&self) -> Option<Arc<dyn DynamicViewRebinder>> {
        self.dynamic_view_rebinder.lock().ok()?.clone()
    }
}

#[derive(Debug, Error)]
pub enum ReloadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
}

#[derive(Debug, Error)]
enum FileWatchError {
    #[error(transparent)]
    Notify(#[from] notify::Error),
}

pub fn summarize_restart_required(
    current: &Config,
    new_config: &Config,
) -> Vec<RestartRequiredField> {
    let mut changes = Vec::new();
    push_changed(
        &mut changes,
        "listener.proxy_addr",
        current.listener.proxy_addr.to_string(),
        new_config.listener.proxy_addr.to_string(),
        "socket binding changes require a process restart",
    );
    push_changed(
        &mut changes,
        "listener.admin_addr",
        current.listener.admin_addr.to_string(),
        new_config.listener.admin_addr.to_string(),
        "socket binding changes require a process restart",
    );
    push_changed(
        &mut changes,
        "listener.metrics_addr",
        current.listener.metrics_addr.to_string(),
        new_config.listener.metrics_addr.to_string(),
        "socket binding changes require a process restart",
    );
    push_changed(
        &mut changes,
        "listener.tls.cert_path",
        path_option_string(
            current
                .listener
                .tls
                .as_ref()
                .and_then(|tls| tls.cert_path.as_ref()),
        ),
        path_option_string(
            new_config
                .listener
                .tls
                .as_ref()
                .and_then(|tls| tls.cert_path.as_ref()),
        ),
        "listener TLS certificate changes require a process restart",
    );
    push_changed(
        &mut changes,
        "listener.tls.key_path",
        path_option_string(
            current
                .listener
                .tls
                .as_ref()
                .and_then(|tls| tls.key_path.as_ref()),
        ),
        path_option_string(
            new_config
                .listener
                .tls
                .as_ref()
                .and_then(|tls| tls.key_path.as_ref()),
        ),
        "listener TLS key changes require a process restart",
    );
    push_changed(
        &mut changes,
        "tls.cert_path",
        path_option_string(current.tls.as_ref().and_then(|tls| tls.cert_path.as_ref())),
        path_option_string(
            new_config
                .tls
                .as_ref()
                .and_then(|tls| tls.cert_path.as_ref()),
        ),
        "TLS certificate changes require a process restart",
    );
    push_changed(
        &mut changes,
        "tls.key_path",
        path_option_string(current.tls.as_ref().and_then(|tls| tls.key_path.as_ref())),
        path_option_string(
            new_config
                .tls
                .as_ref()
                .and_then(|tls| tls.key_path.as_ref()),
        ),
        "TLS key changes require a process restart",
    );
    summarize_storage_restart_required(&mut changes, &current.storage, &new_config.storage);
    push_changed(
        &mut changes,
        "aead.key_env",
        current.aead.key_env.clone(),
        new_config.aead.key_env.clone(),
        "storage encryption key environment changes require a process restart",
    );
    summarize_wasmtime_restart_required(&mut changes, current, new_config);
    summarize_oauth_restart_required(&mut changes, current, new_config);
    summarize_lifecycle_subscriber_restart_required(&mut changes, current, new_config);
    summarize_capture_restart_required(&mut changes, current, new_config);
    changes
}

fn summarize_capture_restart_required(
    changes: &mut Vec<RestartRequiredField>,
    current: &Config,
    new_config: &Config,
) {
    let current = &current.capture;
    let new_config = &new_config.capture;
    push_changed(
        changes,
        "capture.enabled",
        current.enabled.to_string(),
        new_config.enabled.to_string(),
        "capture wiring is bound at startup; toggling requires a process restart",
    );
    push_changed(
        changes,
        "capture.path",
        current.path.display().to_string(),
        new_config.path.display().to_string(),
        "capture store path changes require a process restart",
    );
    push_changed(
        changes,
        "capture.channel_capacity",
        current.channel_capacity.to_string(),
        new_config.channel_capacity.to_string(),
        "capture channel capacity changes require a process restart",
    );
    push_changed(
        changes,
        "capture.retention_max_rows",
        current.retention_max_rows.to_string(),
        new_config.retention_max_rows.to_string(),
        "capture retention changes require a process restart",
    );
}

fn summarize_wasmtime_restart_required(
    changes: &mut Vec<RestartRequiredField>,
    current: &Config,
    new_config: &Config,
) {
    let current = &current.runtime.wasmtime;
    let new_config = &new_config.runtime.wasmtime;
    push_changed(
        changes,
        "runtime.wasmtime.allocation_strategy",
        allocation_strategy_string(current.allocation_strategy).to_owned(),
        allocation_strategy_string(new_config.allocation_strategy).to_owned(),
        "wasmtime engine allocation strategy changes require a process restart",
    );
    push_changed(
        changes,
        "runtime.wasmtime.memory_max_pages",
        option_u32_string(current.memory_max_pages),
        option_u32_string(new_config.memory_max_pages),
        "wasmtime engine memory changes require a process restart",
    );
    push_changed(
        changes,
        "runtime.wasmtime.memory_reservation_bytes",
        option_u64_string(current.memory_reservation_bytes),
        option_u64_string(new_config.memory_reservation_bytes),
        "wasmtime engine memory changes require a process restart",
    );
    push_changed(
        changes,
        "runtime.wasmtime.memory_guard_bytes",
        option_u64_string(current.memory_guard_bytes),
        option_u64_string(new_config.memory_guard_bytes),
        "wasmtime engine memory changes require a process restart",
    );
    push_changed(
        changes,
        "runtime.wasmtime.pool_total_memories",
        option_u32_string(current.pool_total_memories),
        option_u32_string(new_config.pool_total_memories),
        "wasmtime engine pool changes require a process restart",
    );
    push_changed(
        changes,
        "runtime.wasmtime.pool_total_core_instances",
        option_u32_string(current.pool_total_core_instances),
        option_u32_string(new_config.pool_total_core_instances),
        "wasmtime engine pool changes require a process restart",
    );
}

fn summarize_lifecycle_subscriber_restart_required(
    changes: &mut Vec<RestartRequiredField>,
    current: &Config,
    new_config: &Config,
) {
    const REASON: &str =
        "lifecycle subscriber wiring is bound at startup; toggling requires a process restart";

    let entries: &[(&str, bool, bool)] = &[
        (
            "prompt_cache_shadow.enabled",
            current.prompt_cache_shadow.enabled,
            new_config.prompt_cache_shadow.enabled,
        ),
        (
            "lifecycle_hook_adapter.enabled",
            current.lifecycle_hook_adapter.enabled,
            new_config.lifecycle_hook_adapter.enabled,
        ),
        (
            "lifecycle_pricing_subscriber.enabled",
            current.lifecycle_pricing_subscriber.enabled,
            new_config.lifecycle_pricing_subscriber.enabled,
        ),
        (
            "lifecycle_cache_observation_subscriber.enabled",
            current.lifecycle_cache_observation_subscriber.enabled,
            new_config.lifecycle_cache_observation_subscriber.enabled,
        ),
        (
            "lifecycle_rate_limit_header_subscriber.enabled",
            current.lifecycle_rate_limit_header_subscriber.enabled,
            new_config.lifecycle_rate_limit_header_subscriber.enabled,
        ),
        (
            "lifecycle_subscription_quota_subscriber.enabled",
            current.lifecycle_subscription_quota_subscriber.enabled,
            new_config.lifecycle_subscription_quota_subscriber.enabled,
        ),
        (
            "lifecycle_limit_rejection_audit_subscriber.enabled",
            current.lifecycle_limit_rejection_audit_subscriber.enabled,
            new_config
                .lifecycle_limit_rejection_audit_subscriber
                .enabled,
        ),
        (
            "lifecycle_api_key_metrics_subscriber.enabled",
            current.lifecycle_api_key_metrics_subscriber.enabled,
            new_config.lifecycle_api_key_metrics_subscriber.enabled,
        ),
        (
            "lifecycle_cache_hit_miss_subscriber.enabled",
            current.lifecycle_cache_hit_miss_subscriber.enabled,
            new_config.lifecycle_cache_hit_miss_subscriber.enabled,
        ),
        (
            "lifecycle_routing_tier_subscriber.enabled",
            current.lifecycle_routing_tier_subscriber.enabled,
            new_config.lifecycle_routing_tier_subscriber.enabled,
        ),
        (
            "lifecycle_prompt_cache_drift_subscriber.enabled",
            current.lifecycle_prompt_cache_drift_subscriber.enabled,
            new_config.lifecycle_prompt_cache_drift_subscriber.enabled,
        ),
        (
            "lifecycle_prompt_cache_observation_subscriber.enabled",
            current
                .lifecycle_prompt_cache_observation_subscriber
                .enabled,
            new_config
                .lifecycle_prompt_cache_observation_subscriber
                .enabled,
        ),
        (
            "lifecycle_limit_reconcile_subscriber.enabled",
            current.lifecycle_limit_reconcile_subscriber.enabled,
            new_config.lifecycle_limit_reconcile_subscriber.enabled,
        ),
    ];

    for (field, current_value, new_value) in entries {
        push_changed(
            changes,
            field,
            current_value.to_string(),
            new_value.to_string(),
            REASON,
        );
    }
}

fn summarize_storage_restart_required(
    changes: &mut Vec<RestartRequiredField>,
    current: &StorageConfig,
    new_config: &StorageConfig,
) {
    match (current, new_config) {
        (
            StorageConfig::Postgres {
                url: current_url,
                pool: current_pool,
            },
            StorageConfig::Postgres {
                url: new_url,
                pool: new_pool,
            },
        ) => {
            push_changed(
                changes,
                "storage.url",
                current_url.clone(),
                new_url.clone(),
                "storage backend changes require a process restart",
            );
            push_changed(
                changes,
                "storage.pool",
                format!("{current_pool:?}"),
                format!("{new_pool:?}"),
                "storage pool changes require a process restart",
            );
        }
        (StorageConfig::Sqlite { path: current }, StorageConfig::Sqlite { path: new_config }) => {
            push_changed(
                changes,
                "storage.path",
                current.display().to_string(),
                new_config.display().to_string(),
                "storage backend changes require a process restart",
            );
        }
        _ => push_changed(
            changes,
            "storage.kind",
            storage_kind(current).to_owned(),
            storage_kind(new_config).to_owned(),
            "storage backend changes require a process restart",
        ),
    }
}

fn summarize_oauth_restart_required(
    changes: &mut Vec<RestartRequiredField>,
    current: &Config,
    new_config: &Config,
) {
    let current = current.oauth.anthropic.as_ref();
    let new_config = new_config.oauth.anthropic.as_ref();
    push_changed(
        changes,
        "oauth.anthropic.client_id",
        current
            .map(|oauth| oauth.client_id.clone())
            .unwrap_or_default(),
        new_config
            .map(|oauth| oauth.client_id.clone())
            .unwrap_or_default(),
        "Anthropic OAuth client changes require a process restart",
    );
    push_changed(
        changes,
        "oauth.anthropic.auth_url",
        current
            .map(|oauth| oauth.auth_url.to_string())
            .unwrap_or_default(),
        new_config
            .map(|oauth| oauth.auth_url.to_string())
            .unwrap_or_default(),
        "Anthropic OAuth endpoint changes require a process restart",
    );
    push_changed(
        changes,
        "oauth.anthropic.token_url",
        current
            .map(|oauth| oauth.token_url.to_string())
            .unwrap_or_default(),
        new_config
            .map(|oauth| oauth.token_url.to_string())
            .unwrap_or_default(),
        "Anthropic OAuth endpoint changes require a process restart",
    );
    push_changed(
        changes,
        "oauth.anthropic.redirect_uri",
        current
            .map(|oauth| oauth.redirect_uri.to_string())
            .unwrap_or_default(),
        new_config
            .map(|oauth| oauth.redirect_uri.to_string())
            .unwrap_or_default(),
        "Anthropic OAuth redirect changes require a process restart",
    );
    push_changed(
        changes,
        "oauth.anthropic.scopes",
        current
            .map(|oauth| oauth.scopes.join(","))
            .unwrap_or_default(),
        new_config
            .map(|oauth| oauth.scopes.join(","))
            .unwrap_or_default(),
        "Anthropic OAuth scope changes require a process restart",
    );
}

fn push_changed(
    changes: &mut Vec<RestartRequiredField>,
    field: &str,
    current: String,
    new: String,
    reason: &str,
) {
    if current != new {
        changes.push(RestartRequiredField {
            field: field.to_owned(),
            current,
            new,
            reason: reason.to_owned(),
        });
    }
}

fn path_option_string(path: Option<&PathBuf>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_default()
}

fn option_u32_string(value: Option<u32>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn option_u64_string(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn allocation_strategy_string(value: WasmtimeAllocationStrategy) -> &'static str {
    match value {
        WasmtimeAllocationStrategy::OnDemand => "ondemand",
        WasmtimeAllocationStrategy::Pooling => "pooling",
    }
}

fn storage_kind(storage: &StorageConfig) -> &'static str {
    match storage {
        StorageConfig::Postgres { .. } => "postgres",
        StorageConfig::Sqlite { .. } => "sqlite",
    }
}

fn event_matches_config(path: &Path, event: notify::Result<Event>) -> bool {
    let Ok(event) = event else {
        return false;
    };

    if event.paths.is_empty() {
        return false;
    }

    let target_file_name = path.file_name();
    event.paths.iter().any(|event_path| {
        event_path == path
            || event_path
                .file_name()
                .is_some_and(|name| Some(name) == target_file_name)
    })
}

fn watch_dir_for(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}
