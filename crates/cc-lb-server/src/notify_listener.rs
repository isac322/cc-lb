use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::DynamicViewHolder;
use cc_lb_observability::record_notify_received;
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_api::{ChangeChannel, ChangeEvent, RuntimeChangeNotifier};
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::dynamic_view_builder::{self, Stores};

pub struct NotifyListener {
    notifier: Arc<dyn RuntimeChangeNotifier>,
    cancel: CancellationToken,
    holder: Arc<DynamicViewHolder>,
    stores: Arc<Stores>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<ExtismRuntime>,
    aead: Arc<AeadService>,
    data_dir: PathBuf,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
}

pub struct NotifyListenerParams {
    pub notifier: Arc<dyn RuntimeChangeNotifier>,
    pub cancel: CancellationToken,
    pub holder: Arc<DynamicViewHolder>,
    pub stores: Arc<Stores>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<ExtismRuntime>,
    pub aead: Arc<AeadService>,
    pub data_dir: PathBuf,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
}

impl NotifyListener {
    pub fn new(params: NotifyListenerParams) -> Self {
        Self {
            notifier: params.notifier,
            cancel: params.cancel,
            holder: params.holder,
            stores: params.stores,
            oauth_cfg: params.oauth_cfg,
            runtime: params.runtime,
            aead: params.aead,
            data_dir: params.data_dir,
            lazy_refresher: params.lazy_refresher,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut rx = match self.subscribe_with_retry().await {
            Some(rx) => rx,
            None => return,
        };

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                event = rx.recv() => {
                    match event {
                        Ok(event) if is_rebind_channel(event.channel) => {
                            record_notify_received(channel_label(event.channel));
                            self.debounce_and_rebuild(&mut rx).await;
                        },
                        Ok(event) => record_notify_received(channel_label(event.channel)),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "runtime change listener lagged; rebuilding from latest storage state");
                            self.debounce_and_rebuild(&mut rx).await;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    }

    async fn subscribe_with_retry(&self) -> Option<broadcast::Receiver<ChangeEvent>> {
        match self.notifier.subscribe().await {
            Ok(rx) => Some(rx),
            Err(error) => {
                tracing::warn!(error = %error, "runtime change notifier subscribe failed; retrying once");
                tokio::select! {
                    _ = self.cancel.cancelled() => None,
                    _ = sleep(Duration::from_secs(1)) => match self.notifier.subscribe().await {
                        Ok(rx) => Some(rx),
                        Err(error) => {
                            tracing::error!(error = %error, "runtime change notifier subscribe retry failed");
                            None
                        }
                    },
                }
            }
        }
    }

    async fn debounce_and_rebuild(&self, rx: &mut broadcast::Receiver<ChangeEvent>) {
        drain_pending_rebind_events(rx);
        tokio::select! {
            _ = self.cancel.cancelled() => return,
            _ = sleep(Duration::from_millis(250)) => {}
        }
        drain_pending_rebind_events(rx);

        if self.cancel.is_cancelled() {
            return;
        }

        let current_generation = self.holder.load().generation;
        match dynamic_view_builder::build_dynamic_view(
            &self.stores,
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            current_generation,
            &self.runtime,
            &self.data_dir,
        )
        .await
        {
            Ok(view) => {
                if self.cancel.is_cancelled() {
                    return;
                }
                let generation = view.generation;
                self.holder.store(view);
                tracing::info!(
                    generation,
                    "dynamic view rebound after runtime change notification"
                );
            }
            Err(error) => {
                tracing::error!(
                    error = %error,
                    current_generation,
                    "dynamic view rebind failed after runtime change notification"
                );
            }
        }
    }
}

fn drain_pending_rebind_events(rx: &mut broadcast::Receiver<ChangeEvent>) {
    loop {
        match rx.try_recv() {
            Ok(event) => record_notify_received(channel_label(event.channel)),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                break;
            }
        }
    }
}

fn is_rebind_channel(channel: ChangeChannel) -> bool {
    matches!(
        channel,
        ChangeChannel::Upstream
            | ChangeChannel::Principal
            | ChangeChannel::PluginRegistry
            | ChangeChannel::PluginChain
    )
}

fn channel_label(channel: ChangeChannel) -> &'static str {
    match channel {
        ChangeChannel::Upstream => "upstream",
        ChangeChannel::Principal => "principal",
        ChangeChannel::PluginRegistry => "plugin_registry",
        ChangeChannel::PluginChain => "plugin_chain",
    }
}
