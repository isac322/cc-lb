use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use cc_lb_admin::{LastReloadStatus, ReloadOutcome};
use cc_lb_config::{Config, ConfigError, PluginRef, StorageConfig};
use cc_lb_core::api_keys::principal_view::{
    ObservabilityHooksCache, PrincipalView, RouterPluginCache,
};
use cc_lb_plugin_api::RuntimeError;
use cc_lb_runtime_extism::{ExtismRuntime, StagedSlot};
use notify::{Event, RecursiveMode, Watcher};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::app::{GlobalChainError, build_global_chain, manifest_from_plugin};

const DEBOUNCE: Duration = Duration::from_millis(500);
const BROADCAST_CAPACITY: usize = 16;

pub struct ConfigWatcher {
    path: PathBuf,
    current: ArcSwap<Config>,
    reload_tx: broadcast::Sender<Arc<Config>>,
    reloads_attempted: AtomicUsize,
    runtime: Arc<ExtismRuntime>,
    last_reload_status: Arc<ArcSwap<Option<LastReloadStatus>>>,
    principal_view: Option<Arc<ArcSwap<PrincipalView>>>,
}

impl ConfigWatcher {
    pub fn new(
        path: impl AsRef<Path>,
        initial_config: Config,
        runtime: Arc<ExtismRuntime>,
    ) -> Self {
        Self::new_with_principal_view(path, initial_config, runtime, None)
    }

    pub fn new_with_principal_view(
        path: impl AsRef<Path>,
        initial_config: Config,
        runtime: Arc<ExtismRuntime>,
        principal_view: Option<Arc<ArcSwap<PrincipalView>>>,
    ) -> Self {
        let (reload_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            path: path.as_ref().to_path_buf(),
            current: ArcSwap::from_pointee(initial_config),
            reload_tx,
            reloads_attempted: AtomicUsize::new(0),
            runtime,
            last_reload_status: Arc::new(ArcSwap::from(Arc::new(None))),
            principal_view,
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
        warn_restart_required_changes(&current_config, &new_config);

        let new_config = Arc::new(new_config);
        if let Some(principal_view) = &self.principal_view {
            let mut all_staged: Vec<StagedSlot> = Vec::new();
            if new_config.plugins.router_plugin.is_some()
                || !new_config.plugins.observability_hooks.is_empty()
            {
                let (_new_global_router, _new_global_observability_hooks) =
                    match build_global_chain(&new_config, &self.runtime, &mut all_staged) {
                        Ok(global_chain) => global_chain,
                        Err(source) => {
                            self.record_failure(source.to_string(), None, None, config_path);
                            return Err(ReloadError::GlobalChain(source));
                        }
                    };
            }

            let mut principal_chains = HashMap::new();
            let mut per_principal_staged = Vec::new();
            for (principal_id, spec) in &new_config.principals {
                let router_cache = match &spec.router_plugin {
                    Some(plugin) => {
                        let manifest = match manifest_from_plugin(plugin) {
                            Ok(manifest) => manifest,
                            Err(source) => {
                                self.record_failure(
                                    source.to_string(),
                                    Some(principal_id.clone()),
                                    Some(plugin.name.clone()),
                                    config_path,
                                );
                                return Err(ReloadError::Config(source));
                            }
                        };
                        let (handle, slot) = match self.runtime.instantiate_router_for(
                            principal_id,
                            &plugin.name,
                            &manifest,
                        ) {
                            Ok(value) => value,
                            Err(source) => {
                                self.record_failure(
                                    source.to_string(),
                                    Some(principal_id.clone()),
                                    Some(plugin.name.clone()),
                                    config_path,
                                );
                                return Err(ReloadError::Runtime(source));
                            }
                        };
                        per_principal_staged.push(slot);
                        RouterPluginCache::Explicit(handle)
                    }
                    None => RouterPluginCache::Inherit,
                };
                let hooks_cache = match &spec.observability_hooks {
                    Some(plugins) => {
                        let mut handles = Vec::with_capacity(plugins.len());
                        for plugin in plugins {
                            let manifest = match manifest_from_plugin(plugin) {
                                Ok(manifest) => manifest,
                                Err(source) => {
                                    self.record_failure(
                                        source.to_string(),
                                        Some(principal_id.clone()),
                                        Some(plugin.name.clone()),
                                        config_path,
                                    );
                                    return Err(ReloadError::Config(source));
                                }
                            };
                            let (handle, slot) = match self.runtime.instantiate_observability_for(
                                principal_id,
                                &plugin.name,
                                &manifest,
                            ) {
                                Ok(value) => value,
                                Err(source) => {
                                    self.record_failure(
                                        source.to_string(),
                                        Some(principal_id.clone()),
                                        Some(plugin.name.clone()),
                                        config_path,
                                    );
                                    return Err(ReloadError::Runtime(source));
                                }
                            };
                            per_principal_staged.push(slot);
                            handles.push(handle);
                        }
                        ObservabilityHooksCache::Explicit(handles)
                    }
                    None => ObservabilityHooksCache::Inherit,
                };
                principal_chains.insert(principal_id.clone(), (router_cache, hooks_cache));
            }
            all_staged.extend(per_principal_staged);

            let new_principal_view = match PrincipalView::from_config(&new_config, principal_chains)
            {
                Ok(view) => view,
                Err(source) => {
                    self.record_failure(source.to_string(), None, None, config_path);
                    return Err(ReloadError::Config(source));
                }
            };
            if let Err(source) = self.runtime.commit_staged(all_staged) {
                self.record_failure(source.to_string(), None, None, config_path);
                return Err(ReloadError::Runtime(source));
            }
            principal_view.store(new_principal_view);
            let referenced: HashSet<(String, String)> = referenced_slot_keys(&new_config);
            for (principal, plugin) in self.runtime.registered_slot_keys() {
                if !referenced.contains(&(principal.clone(), plugin.clone())) {
                    self.runtime.evict_slot(&principal, &plugin);
                }
            }
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
                timestamp_unix_secs: unix_timestamp_secs(),
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
                timestamp_unix_secs: unix_timestamp_secs(),
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
}

pub(crate) fn referenced_slot_keys(config: &Config) -> HashSet<(String, String)> {
    let mut keys = HashSet::new();
    for plugin in config
        .plugins
        .router_plugin
        .iter()
        .chain(config.plugins.observability_hooks.iter())
    {
        keys.insert(("__global__".to_owned(), plugin.name.clone()));
    }

    for (principal_name, spec) in &config.principals {
        if let Some(plugin) = &spec.router_plugin {
            keys.insert((principal_name.clone(), plugin.name.clone()));
        }
        for plugin in spec
            .observability_hooks
            .as_ref()
            .map(|hooks| hooks.iter())
            .into_iter()
            .flatten()
        {
            keys.insert((principal_name.clone(), plugin.name.clone()));
        }
    }

    keys
}

#[derive(Debug, Error)]
pub enum ReloadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    GlobalChain(#[from] GlobalChainError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

#[derive(Debug, Error)]
enum FileWatchError {
    #[error(transparent)]
    Notify(#[from] notify::Error),
}

fn warn_restart_required_changes(current: &Config, new_config: &Config) {
    warn_if_changed(
        &current.listener.proxy_addr,
        &new_config.listener.proxy_addr,
        "listener.proxy_addr",
    );
    warn_if_changed(
        &current.listener.admin_addr,
        &new_config.listener.admin_addr,
        "listener.admin_addr",
    );
    warn_if_changed(
        &current.listener.metrics_addr,
        &new_config.listener.metrics_addr,
        "listener.metrics_addr",
    );
    warn_if_changed(
        &current
            .listener
            .tls
            .as_ref()
            .and_then(|tls| tls.cert_path.as_ref()),
        &new_config
            .listener
            .tls
            .as_ref()
            .and_then(|tls| tls.cert_path.as_ref()),
        "listener.tls.cert_path",
    );
    warn_if_changed(
        &current
            .listener
            .tls
            .as_ref()
            .and_then(|tls| tls.key_path.as_ref()),
        &new_config
            .listener
            .tls
            .as_ref()
            .and_then(|tls| tls.key_path.as_ref()),
        "listener.tls.key_path",
    );
    warn_if_changed(
        &current.tls.as_ref().and_then(|tls| tls.cert_path.as_ref()),
        &new_config
            .tls
            .as_ref()
            .and_then(|tls| tls.cert_path.as_ref()),
        "tls.cert_path",
    );
    warn_if_changed(
        &current.tls.as_ref().and_then(|tls| tls.key_path.as_ref()),
        &new_config
            .tls
            .as_ref()
            .and_then(|tls| tls.key_path.as_ref()),
        "tls.key_path",
    );
    warn_plugin_path_change(
        "plugins.router_plugin.wasm_path",
        current.plugins.router_plugin.as_ref(),
        new_config.plugins.router_plugin.as_ref(),
    );
    warn_observability_hook_path_changes(
        "plugins.observability_hooks",
        &current.plugins.observability_hooks,
        &new_config.plugins.observability_hooks,
    );
    warn_principal_plugin_path_changes(current, new_config);
    warn_storage_restart_required(&current.storage, &new_config.storage);
    warn_aead_restart_required(&current.aead.key_env, &new_config.aead.key_env);
}

fn warn_storage_restart_required(current: &StorageConfig, new_config: &StorageConfig) {
    match (current, new_config) {
        (StorageConfig::Redb { path: current }, StorageConfig::Redb { path: new_config }) => {
            if current != new_config {
                tracing::warn!("storage backend changed; restart required to apply");
            }
        }
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
            if current_url != new_url {
                tracing::warn!("storage backend changed; restart required to apply");
            } else if current_pool != new_pool {
                tracing::warn!(field = "storage.pool", "restart required to apply");
            }
        }
        _ => tracing::warn!("storage backend changed; restart required to apply"),
    }
}

fn warn_aead_restart_required(current: &str, new_config: &str) {
    if current != new_config {
        tracing::warn!("aead key env changed; restart required to apply");
    }
}

fn warn_plugin_path_change(
    field: &str,
    current: Option<&PluginRef>,
    new_config: Option<&PluginRef>,
) {
    warn_if_changed(
        &current.and_then(|plugin| plugin.wasm_path.as_ref()),
        &new_config.and_then(|plugin| plugin.wasm_path.as_ref()),
        field,
    );
}

fn warn_observability_hook_path_changes(
    field_prefix: &str,
    current: &[PluginRef],
    new_config: &[PluginRef],
) {
    let max_len = current.len().max(new_config.len());
    for index in 0..max_len {
        let field = format!("{field_prefix}.{index}.wasm_path");
        warn_if_changed(
            &current
                .get(index)
                .and_then(|plugin| plugin.wasm_path.as_ref()),
            &new_config
                .get(index)
                .and_then(|plugin| plugin.wasm_path.as_ref()),
            &field,
        );
    }
}

fn warn_principal_plugin_path_changes(current: &Config, new_config: &Config) {
    let mut principal_ids = BTreeSet::new();
    principal_ids.extend(current.principals.keys());
    principal_ids.extend(new_config.principals.keys());

    for principal_id in principal_ids {
        let current_principal = current.principals.get(principal_id);
        let new_principal = new_config.principals.get(principal_id);
        warn_plugin_path_change(
            &format!("principals.{principal_id}.router_plugin.wasm_path"),
            current_principal.and_then(|spec| spec.router_plugin.as_ref()),
            new_principal.and_then(|spec| spec.router_plugin.as_ref()),
        );

        let current_hooks = current_principal
            .and_then(|spec| spec.observability_hooks.as_deref())
            .unwrap_or(&[]);
        let new_hooks = new_principal
            .and_then(|spec| spec.observability_hooks.as_deref())
            .unwrap_or(&[]);
        warn_observability_hook_path_changes(
            &format!("principals.{principal_id}.observability_hooks"),
            current_hooks,
            new_hooks,
        );
    }
}

fn warn_if_changed<T: PartialEq>(current: &T, new_config: &T, field: &str) {
    if current != new_config {
        tracing::warn!(field = field, "restart required to apply");
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

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
