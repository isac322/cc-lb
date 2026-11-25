use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backon::{BackoffBuilder as _, ExponentialBuilder};
use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_control::api_keys::key_store::KeyStore;
use cc_lb_control::{DynamicViewHolder, PromptCacheObservationSinkLike, UpstreamRateLimitCache};
use cc_lb_engine::ThreadStickySubagentLatch;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{ChangeChannel, ChangeEvent, RuntimeChangeNotifier};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::{self, DynamicViewPluginScope, Stores};
use crate::subscription_quota_cache::SubscriptionQuotaCache;

pub struct NotifyListener {
    notifier: Arc<dyn RuntimeChangeNotifier>,
    cancel: CancellationToken,
    holder: Arc<DynamicViewHolder>,
    stores: Arc<Stores>,
    runtime: Arc<WasmtimeRuntime>,
    aead: Arc<AeadService>,
    data_dir: PathBuf,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_grace_margin_secs: u64,
    prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    subscription_quota_routing_max_staleness_secs: u64,
    upstream_rate_limit_cache: Arc<RwLock<UpstreamRateLimitCache>>,
    key_store: Option<Arc<KeyStore>>,
    thread_sticky_subagent_latch: Option<Arc<ThreadStickySubagentLatch>>,
    clock: ClockHandle,
    gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
    antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
    plugin_scope: DynamicViewPluginScope,
}

pub struct NotifyListenerParams {
    pub notifier: Arc<dyn RuntimeChangeNotifier>,
    pub cancel: CancellationToken,
    pub holder: Arc<DynamicViewHolder>,
    pub stores: Arc<Stores>,
    pub runtime: Arc<WasmtimeRuntime>,
    pub aead: Arc<AeadService>,
    pub data_dir: PathBuf,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub prompt_cache_grace_margin_secs: u64,
    pub prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    pub subscription_quota_routing_max_staleness_secs: u64,
    pub key_store: Option<Arc<KeyStore>>,
    pub thread_sticky_subagent_latch: Option<Arc<ThreadStickySubagentLatch>>,
    pub clock: ClockHandle,
    pub gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
    pub antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
}

impl NotifyListener {
    pub fn new(params: NotifyListenerParams) -> Self {
        let upstream_rate_limit_cache = Arc::clone(&params.holder.load().upstream_rate_limit_cache);
        Self {
            notifier: params.notifier,
            cancel: params.cancel,
            holder: params.holder,
            stores: params.stores,
            runtime: params.runtime,
            aead: params.aead,
            data_dir: params.data_dir,
            lazy_refresher: params.lazy_refresher,
            subscription_quota_cache: params.subscription_quota_cache,
            prompt_cache_grace_margin_secs: params.prompt_cache_grace_margin_secs,
            prompt_cache_observation_sink: params.prompt_cache_observation_sink,
            subscription_quota_routing_max_staleness_secs: params
                .subscription_quota_routing_max_staleness_secs,
            upstream_rate_limit_cache,
            key_store: params.key_store,
            thread_sticky_subagent_latch: params.thread_sticky_subagent_latch,
            gemini_config: params.gemini_config,
            antigravity_config: params.antigravity_config,
            clock: params.clock,
            plugin_scope: DynamicViewPluginScope::All,
        }
    }

    pub fn with_plugin_scope(mut self, plugin_scope: DynamicViewPluginScope) -> Self {
        self.plugin_scope = plugin_scope;
        self
    }

    fn accepts_rebind_channel(&self, channel: ChangeChannel) -> bool {
        match self.plugin_scope {
            DynamicViewPluginScope::All => is_rebind_channel(channel),
            DynamicViewPluginScope::WarmupOnly => {
                is_rebind_channel(channel) && channel != ChangeChannel::PluginChain
            }
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut is_reconnect = false;
        while !self.cancel.is_cancelled() {
            let Some(rx) = self.subscribe_with_retry().await else {
                return;
            };
            if is_reconnect {
                let mut rx = rx;
                self.debounce_and_rebuild(&mut rx).await;
                self.clone().run_subscribed(rx).await;
            } else {
                self.clone().run_subscribed(rx).await;
            }
            is_reconnect = true;
        }
    }

    pub(crate) async fn run_with_initial(
        self: Arc<Self>,
        initial: broadcast::Receiver<ChangeEvent>,
    ) {
        self.clone().run_subscribed(initial).await;
        if !self.cancel.is_cancelled() {
            self.run_reconnect_loop().await;
        }
    }

    async fn run_reconnect_loop(self: Arc<Self>) {
        while !self.cancel.is_cancelled() {
            let Some(mut rx) = self.subscribe_with_retry().await else {
                return;
            };
            self.debounce_and_rebuild(&mut rx).await;
            self.clone().run_subscribed(rx).await;
        }
    }

    pub(crate) async fn try_subscribe_once(&self) -> Option<broadcast::Receiver<ChangeEvent>> {
        self.notifier.subscribe().await.ok()
    }

    pub(crate) async fn run_subscribed(self: Arc<Self>, mut rx: broadcast::Receiver<ChangeEvent>) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                event = rx.recv() => {
                    match event {
                        Ok(event) if is_immediate_cache_channel(event.channel) => {
                            self.handle_immediate_cache_event(&event);
                        }
                        Ok(event) if self.accepts_rebind_channel(event.channel) => {
                            if event.channel == ChangeChannel::Principal {
                                self.invalidate_all_caches_on_reset();
                            }
                            self.debounce_and_rebuild(&mut rx).await;
                        }
                        Ok(event) if is_hydrate_channel(event.channel) => {
                            self.debounce_and_hydrate(&mut rx, event).await
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "runtime change listener lagged; rebuilding from latest storage state");
                            self.invalidate_all_caches_on_reset();
                            self.debounce_and_rebuild(&mut rx).await;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            self.invalidate_all_caches_on_reset();
                            break;
                        }
                    }
                }
            }
        }
    }

    fn invalidate_all_caches_on_reset(&self) {
        if let Some(key_store) = &self.key_store {
            key_store.invalidate_all();
        }
        if let Some(latch) = &self.thread_sticky_subagent_latch {
            latch.clear_negative_entries();
        }
    }

    fn handle_immediate_cache_event(&self, event: &ChangeEvent) {
        match event.channel {
            ChangeChannel::ManagedKey => {
                if let Some(key_store) = &self.key_store {
                    let payload = event.payload.trim();
                    if payload.is_empty() {
                        key_store.invalidate_all();
                    } else if let Some((principal_id, key_id)) = payload.split_once('\t') {
                        key_store.invalidate_key(principal_id, key_id);
                    } else {
                        key_store.invalidate_principal(payload);
                    }
                }
                tracing::info!(
                    payload = %event.payload,
                    "managed key cache invalidated after runtime change notification"
                );
            }
            ChangeChannel::ThreadStickySubagent => {
                if let Some(latch) = &self.thread_sticky_subagent_latch {
                    let payload = event.payload.trim();
                    let mut parts = payload.split('\t');
                    match (parts.next(), parts.next(), parts.next(), parts.next()) {
                        (
                            Some(principal_id),
                            Some(observed_session_id),
                            Some(expires_str),
                            None,
                        ) if !principal_id.is_empty() && !observed_session_id.is_empty() => {
                            if let Ok(expires_at) = expires_str.parse::<u64>() {
                                latch.seed_local(principal_id, observed_session_id, expires_at);
                            } else {
                                latch.invalidate_local(principal_id, observed_session_id);
                            }
                        }
                        (Some(principal_id), Some(observed_session_id), None, None)
                            if !principal_id.is_empty() && !observed_session_id.is_empty() =>
                        {
                            latch.invalidate_local(principal_id, observed_session_id);
                        }
                        _ => {
                            latch.clear_negative_entries();
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub(crate) async fn subscribe_with_retry(&self) -> Option<broadcast::Receiver<ChangeEvent>> {
        let mut backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(5))
            .with_jitter()
            .without_max_times()
            .build();
        loop {
            if self.cancel.is_cancelled() {
                return None;
            }
            match self.notifier.subscribe().await {
                Ok(rx) => return Some(rx),
                Err(error) => {
                    let delay = backoff.next().unwrap_or_else(|| Duration::from_secs(5));
                    tracing::warn!(
                        error = %error,
                        backoff_ms = delay.as_millis(),
                        "runtime change notifier subscribe failed; retrying"
                    );
                    tokio::select! {
                        _ = self.cancel.cancelled() => return None,
                        _ = sleep(delay) => {}
                    }
                }
            }
        }
    }

    async fn debounce_and_rebuild(&self, rx: &mut broadcast::Receiver<ChangeEvent>) {
        self.drain_pending_rebind_events(rx);
        tokio::select! {
            _ = self.cancel.cancelled() => return,
            _ = sleep(Duration::from_millis(250)) => {}
        }
        self.drain_pending_rebind_events(rx);

        if self.cancel.is_cancelled() {
            return;
        }

        // Rebuild protocol: hold the holder's rebuild lock from the
        // resident-generation read through installation. Waiting for the
        // lock stays cancellation-aware so shutdown never waits behind
        // another publisher's slow build.
        let _rebuild_guard = tokio::select! {
            _ = self.cancel.cancelled() => return,
            guard = self.holder.lock_rebuild() => guard,
        };
        let current_generation = self.holder.load().generation;
        let started = Instant::now();
        match dynamic_view_builder::build_dynamic_view_with_scope_and_rate_limit_cache(
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
            Arc::clone(&self.upstream_rate_limit_cache),
            self.plugin_scope,
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
    /// Collapse a burst of hydrate-only change events, then refresh the
    /// affected in-process caches in place. These channels never trigger a
    /// dynamic-view rebuild; a rebind event observed while debouncing takes
    /// precedence and routes through `debounce_and_rebuild`, which rebuilds
    /// every cache from storage anyway.
    async fn debounce_and_hydrate(
        &self,
        rx: &mut broadcast::Receiver<ChangeEvent>,
        first: ChangeEvent,
    ) {
        let mut pending: HashMap<ChangeChannel, HydrateScope> = HashMap::new();
        let mut saw_rebind = false;
        collect_pending_event_for_scope(&first, &mut pending, &mut saw_rebind, self.plugin_scope);
        tokio::select! {
            _ = self.cancel.cancelled() => return,
            _ = sleep(Duration::from_millis(250)) => {}
        }
        self.drain_pending_events(rx, &mut pending, &mut saw_rebind);

        if self.cancel.is_cancelled() {
            return;
        }
        if saw_rebind {
            self.invalidate_all_caches_on_reset();
            self.debounce_and_rebuild(rx).await;
            return;
        }

        for (channel, scope) in pending {
            self.hydrate_channel(channel, scope).await;
        }
    }

    async fn hydrate_channel(&self, channel: ChangeChannel, scope: HydrateScope) {
        let upstream_ids = self.resolve_hydrate_upstream_ids(&scope);
        if upstream_ids.is_empty() {
            return;
        }
        match channel {
            ChangeChannel::UpstreamRateLimit => {
                match self
                    .stores
                    .upstream_rate_limits
                    .list_for_upstream_ids(&upstream_ids)
                    .await
                {
                    Ok(records) => {
                        let mut cache = self.upstream_rate_limit_cache.write();
                        for record in records {
                            cache.upsert_record(record);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "upstream rate limit cache hydrate failed");
                    }
                }
            }
            ChangeChannel::SubscriptionQuota => {
                if let Err(error) = self
                    .subscription_quota_cache
                    .hydrate_from_store(&self.stores, &upstream_ids)
                    .await
                {
                    tracing::warn!(%error, "subscription quota cache hydrate failed");
                }
            }
            _ => {}
        }
    }

    fn resolve_hydrate_upstream_ids(&self, scope: &HydrateScope) -> Vec<Uuid> {
        match scope {
            HydrateScope::All => self
                .holder
                .load()
                .upstreams_snapshot()
                .iter()
                .map(|upstream| upstream.id)
                .collect(),
            HydrateScope::Ids(ids) => ids.iter().copied().collect(),
        }
    }

    fn drain_pending_rebind_events(&self, rx: &mut broadcast::Receiver<ChangeEvent>) {
        loop {
            match rx.try_recv() {
                Ok(event) if is_immediate_cache_channel(event.channel) => {
                    self.handle_immediate_cache_event(&event);
                }
                Ok(event) if self.accepts_rebind_channel(event.channel) => {
                    if event.channel == ChangeChannel::Principal {
                        self.invalidate_all_caches_on_reset();
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    self.invalidate_all_caches_on_reset();
                    continue;
                }
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => {
                    break;
                }
            }
        }
    }

    fn drain_pending_events(
        &self,
        rx: &mut broadcast::Receiver<ChangeEvent>,
        pending: &mut HashMap<ChangeChannel, HydrateScope>,
        saw_rebind: &mut bool,
    ) {
        loop {
            match rx.try_recv() {
                Ok(event) if is_immediate_cache_channel(event.channel) => {
                    self.handle_immediate_cache_event(&event);
                }
                Ok(event) => {
                    collect_pending_event_for_scope(&event, pending, saw_rebind, self.plugin_scope)
                }
                // Missed events may include rebinds; rebuild to stay safe.
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    *saw_rebind = true;
                    self.invalidate_all_caches_on_reset();
                }
                Err(
                    broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed,
                ) => {
                    break;
                }
            }
        }
    }
}

fn is_immediate_cache_channel(channel: ChangeChannel) -> bool {
    matches!(
        channel,
        ChangeChannel::ManagedKey | ChangeChannel::ThreadStickySubagent
    )
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

/// Which upstreams a hydrate event should refresh. `All` covers events whose
/// payload does not name a single upstream.
enum HydrateScope {
    All,
    Ids(HashSet<Uuid>),
}

impl HydrateScope {
    fn merge(&mut self, upstream_id: Option<Uuid>) {
        match upstream_id {
            Some(id) => {
                if let Self::Ids(ids) = self {
                    ids.insert(id);
                }
            }
            None => *self = Self::All,
        }
    }
}

fn collect_pending_event_for_scope(
    event: &ChangeEvent,
    pending: &mut HashMap<ChangeChannel, HydrateScope>,
    saw_rebind: &mut bool,
    plugin_scope: DynamicViewPluginScope,
) {
    if is_rebind_channel(event.channel) {
        if plugin_scope == DynamicViewPluginScope::All
            || event.channel != ChangeChannel::PluginChain
        {
            *saw_rebind = true;
        }
        return;
    }
    if !is_hydrate_channel(event.channel) {
        return;
    }
    let upstream_id = Uuid::parse_str(event.payload.trim()).ok();
    pending
        .entry(event.channel)
        .or_insert_with(|| HydrateScope::Ids(HashSet::new()))
        .merge(upstream_id);
}

fn is_hydrate_channel(channel: ChangeChannel) -> bool {
    matches!(
        channel,
        ChangeChannel::UpstreamRateLimit | ChangeChannel::SubscriptionQuota
    )
}
