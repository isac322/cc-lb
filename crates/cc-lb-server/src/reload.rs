use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use cc_lb_admin::{LastReloadStatus, ReloadOutcome};
use cc_lb_config::{Config, ConfigError, StorageConfig};
use cc_lb_core::DynamicViewHolder;
use notify::{Event, RecursiveMode, Watcher};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const DEBOUNCE: Duration = Duration::from_millis(500);
const BROADCAST_CAPACITY: usize = 16;

pub struct ConfigWatcher {
    path: PathBuf,
    current: ArcSwap<Config>,
    reload_tx: broadcast::Sender<Arc<Config>>,
    reloads_attempted: AtomicUsize,
    last_reload_status: Arc<ArcSwap<Option<LastReloadStatus>>>,
    dynamic_view: Option<Arc<DynamicViewHolder>>,
}

impl ConfigWatcher {
    pub fn new(
        path: impl AsRef<Path>,
        initial_config: Config,
        _runtime: Arc<cc_lb_runtime_extism::ExtismRuntime>,
    ) -> Self {
        Self::new_with_principal_view(path, initial_config, _runtime, None)
    }

    pub fn new_with_principal_view(
        path: impl AsRef<Path>,
        initial_config: Config,
        _runtime: Arc<cc_lb_runtime_extism::ExtismRuntime>,
        dynamic_view: Option<Arc<DynamicViewHolder>>,
    ) -> Self {
        let (reload_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            path: path.as_ref().to_path_buf(),
            current: ArcSwap::from_pointee(initial_config),
            reload_tx,
            reloads_attempted: AtomicUsize::new(0),
            last_reload_status: Arc::new(ArcSwap::from(Arc::new(None))),
            dynamic_view,
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
