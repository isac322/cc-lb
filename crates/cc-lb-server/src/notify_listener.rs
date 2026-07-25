use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::PromptCacheObservationSinkLike;
use cc_lb_engine::clock::ClockHandle;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{ChangeChannel, ChangeEvent, RuntimeChangeNotifier};
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::dynamic_view_builder::{self, Stores};
use crate::prompt_cache_observation_cache::PromptCacheObservationCache;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

pub struct NotifyListener {
    notifier: Arc<dyn RuntimeChangeNotifier>,
    cancel: CancellationToken,
    holder: Arc<DynamicViewHolder>,
    stores: Arc<Stores>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<WasmtimeRuntime>,
    aead: Arc<AeadService>,
    data_dir: PathBuf,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: ClockHandle,
}

pub struct NotifyListenerParams {
    pub notifier: Arc<dyn RuntimeChangeNotifier>,
    pub cancel: CancellationToken,
    pub holder: Arc<DynamicViewHolder>,
    pub stores: Arc<Stores>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<WasmtimeRuntime>,
    pub aead: Arc<AeadService>,
    pub data_dir: PathBuf,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    pub prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    pub subscription_quota_routing_max_staleness_secs: u64,
    pub clock: ClockHandle,
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
            subscription_quota_cache: params.subscription_quota_cache,
            prompt_cache_observation_cache: params.prompt_cache_observation_cache,
            prompt_cache_observation_sink: params.prompt_cache_observation_sink,
            subscription_quota_routing_max_staleness_secs: params
                .subscription_quota_routing_max_staleness_secs,
            clock: params.clock,
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
                        Ok(event) if is_rebind_channel(event.channel) => self.debounce_and_rebuild(&mut rx).await,
                        Ok(_) => {}
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
        let started = Instant::now();
        match dynamic_view_builder::build_dynamic_view(
            &self.stores,
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            current_generation,
            &self.runtime,
            &self.data_dir,
            self.subscription_quota_cache.clone(),
            self.prompt_cache_observation_cache.clone(),
            self.prompt_cache_observation_sink.clone(),
            self.subscription_quota_routing_max_staleness_secs,
            self.clock.clone(),
        )
        .await
        {
            Ok(view) => {
                let elapsed = started.elapsed();
                if self.cancel.is_cancelled() {
                    return;
                }
                let generation = view.generation;
                if self.holder.try_store_if_newer(view) {
                    metrics::counter!("cclb_rebind_total", "outcome" => "success").increment(1);
                    metrics::histogram!("cclb_rebind_duration_seconds")
                        .record(elapsed.as_secs_f64());
                    tracing::info!(
                        generation,
                        duration_ms = elapsed.as_millis(),
                        "dynamic view rebound after runtime change notification"
                    );
                } else {
                    metrics::counter!("cclb_rebind_total", "outcome" => "stale").increment(1);
                    tracing::debug!(
                        generation,
                        duration_ms = elapsed.as_millis(),
                        "notify-triggered view rejected: newer generation already resident"
                    );
                }
            }
            Err(error) => {
                let elapsed = started.elapsed();
                metrics::counter!("cclb_rebind_total", "outcome" => "error").increment(1);
                metrics::histogram!("cclb_rebind_duration_seconds").record(elapsed.as_secs_f64());
                tracing::error!(
                    error = %error,
                    current_generation,
                    duration_ms = elapsed.as_millis(),
                    "dynamic view rebind failed after runtime change notification"
                );
            }
        }
    }
}

fn drain_pending_rebind_events(rx: &mut broadcast::Receiver<ChangeEvent>) {
    loop {
        match rx.try_recv() {
            Ok(event) if is_rebind_channel(event.channel) => {}
            Ok(_) => {}
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
