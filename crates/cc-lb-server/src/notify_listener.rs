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

use crate::dynamic_view_builder::{self, DynamicViewPluginRuntime, Stores};
use crate::prompt_cache_observation_cache::PromptCacheObservationCache;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

pub struct NotifyListener {
    notifier: Arc<dyn RuntimeChangeNotifier>,
    cancel: CancellationToken,
    holder: Arc<DynamicViewHolder>,
    stores: Arc<Stores>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<dyn DynamicViewPluginRuntime>,
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

struct NotifyListenerPluginParams {
    notifier: Arc<dyn RuntimeChangeNotifier>,
    cancel: CancellationToken,
    holder: Arc<DynamicViewHolder>,
    stores: Arc<Stores>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<dyn DynamicViewPluginRuntime>,
    aead: Arc<AeadService>,
    data_dir: PathBuf,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: ClockHandle,
}

impl NotifyListener {
    pub fn new(params: NotifyListenerParams) -> Self {
        Self::new_with_plugin_runtime(NotifyListenerPluginParams {
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
        })
    }

    fn new_with_plugin_runtime(params: NotifyListenerPluginParams) -> Self {
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
        let Some(rx) = self.subscribe_with_retry().await else {
            return;
        };
        self.run_subscribed(rx).await;
    }

    pub(crate) async fn run_subscribed(self: Arc<Self>, mut rx: broadcast::Receiver<ChangeEvent>) {
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

    pub(crate) async fn subscribe_with_retry(&self) -> Option<broadcast::Receiver<ChangeEvent>> {
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
        match dynamic_view_builder::build_dynamic_view_with_plugin_runtime(
            &self.stores,
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            current_generation,
            self.runtime.as_ref(),
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

#[cfg(test)]
#[allow(non_snake_case)]
mod t2__notify_listener {
    // tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{NotifyListener, NotifyListenerPluginParams};
    use crate::dynamic_view_builder::{
        DynamicViewPluginRuntime, PluginRuntimeSlotKey, RebindError, Stores,
        build_dynamic_view_with_plugin_runtime,
    };
    use async_trait::async_trait;
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens};
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_engine::DynamicViewHolder;
    use cc_lb_storage_api::upstream::UpstreamStatusUpdate;
    use cc_lb_storage_api::{
        BackfillApplyOutcome, ChangeChannel, ChangeEvent, MetadataTierMappingOverrideRecord,
        OrganizationMetadataRecord, OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore,
        RuntimeChangeNotifier, StorageError, StorageResult, UpstreamCreate, UpstreamPlanTierRecord,
        UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore, UpstreamRecord,
        UpstreamStore, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
        UpstreamUpdate,
    };
    use cc_lb_testkit::InMemoryStorage as Storage;
    use metrics_util::debugging::Snapshotter;
    use tokio::sync::{Notify, broadcast, oneshot};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingPluginRuntime {
        retained: Mutex<Vec<HashSet<PluginRuntimeSlotKey>>>,
    }

    #[async_trait]
    impl DynamicViewPluginRuntime for RecordingPluginRuntime {
        async fn instantiate_filter(
            &self,
            _slot_key: &PluginRuntimeSlotKey,
            _manifest: &crate::PluginManifest,
            _chain_entry_id: Uuid,
        ) -> Result<Arc<dyn cc_lb_routing::FilterPlugin>, RebindError> {
            Err(RebindError::Io(std::io::Error::other(
                "unexpected filter plugin instantiation",
            )))
        }

        async fn instantiate_shape(
            &self,
            _slot_key: &PluginRuntimeSlotKey,
            _manifest: &crate::PluginManifest,
        ) -> Result<Arc<dyn cc_lb_upstream::UpstreamDialect>, RebindError> {
            Err(RebindError::Io(std::io::Error::other(
                "unexpected shape plugin instantiation",
            )))
        }

        async fn instantiate_observability_hook(
            &self,
            _slot_key: &PluginRuntimeSlotKey,
            _manifest: &crate::PluginManifest,
        ) -> Result<Arc<dyn cc_lb_observability::ObservabilityHook>, RebindError> {
            Err(RebindError::Io(std::io::Error::other(
                "unexpected observability plugin instantiation",
            )))
        }

        fn retain_slots(&self, slot_keys: &HashSet<PluginRuntimeSlotKey>) -> usize {
            self.retained
                .lock()
                .expect("plugin runtime retain lock")
                .push(slot_keys.clone());
            0
        }
    }

    const INITIAL_GENERATION: u64 = 1;

    struct MockNotifier {
        tx: Mutex<Option<broadcast::Sender<ChangeEvent>>>,
        subscribed: Notify,
    }

    impl MockNotifier {
        fn new() -> Self {
            let (tx, _) = broadcast::channel(64);
            Self {
                tx: Mutex::new(Some(tx)),
                subscribed: Notify::new(),
            }
        }

        fn send(&self, channel: ChangeChannel) {
            let _ = self
                .tx
                .lock()
                .expect("notifier sender lock")
                .as_ref()
                .expect("notifier sender remains open")
                .send(ChangeEvent::new(channel, "test", std::time::UNIX_EPOCH));
        }

        fn close(&self) {
            self.tx.lock().expect("notifier sender lock").take();
        }

        async fn wait_for_subscriber(&self) {
            self.subscribed.notified().await;
        }
    }

    #[async_trait]
    impl RuntimeChangeNotifier for MockNotifier {
        async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
            let receiver = self
                .tx
                .lock()
                .expect("notifier sender lock")
                .as_ref()
                .expect("notifier sender remains open")
                .subscribe();
            self.subscribed.notify_one();
            Ok(receiver)
        }

        async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
            cancel.cancelled().await;
            Ok(())
        }
    }

    struct ControlledUpstreamStore {
        inner: Arc<Storage>,
        fail_list: bool,
        list_started: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
        list_release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl ControlledUpstreamStore {
        fn failing(inner: Arc<Storage>, list_started: oneshot::Sender<()>) -> Self {
            Self {
                inner,
                fail_list: true,
                list_started: tokio::sync::Mutex::new(Some(list_started)),
                list_release: tokio::sync::Mutex::new(None),
            }
        }

        fn blocking(
            inner: Arc<Storage>,
            list_started: oneshot::Sender<()>,
            list_release: oneshot::Receiver<()>,
        ) -> Self {
            Self {
                inner,
                fail_list: false,
                list_started: tokio::sync::Mutex::new(Some(list_started)),
                list_release: tokio::sync::Mutex::new(Some(list_release)),
            }
        }
    }

    #[async_trait]
    impl UpstreamStore for ControlledUpstreamStore {
        async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            UpstreamStore::create(&*self.inner, create).await
        }

        async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
            UpstreamStore::get_by_name(&*self.inner, name).await
        }

        async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
            UpstreamStore::get_by_id(&*self.inner, id).await
        }

        async fn list(
            &self,
            after: Option<Uuid>,
            limit: usize,
        ) -> StorageResult<Vec<UpstreamRecord>> {
            if let Some(sender) = self.list_started.lock().await.take() {
                let _ = sender.send(());
            }
            if self.fail_list {
                return Err(StorageError::Unavailable {
                    message: "injected list failure".to_owned(),
                });
            }
            if let Some(receiver) = self.list_release.lock().await.take() {
                let _ = receiver.await;
            }
            UpstreamStore::list(&*self.inner, after, limit).await
        }

        async fn update(
            &self,
            id: Uuid,
            expected_revision: u64,
            update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::update(&*self.inner, id, expected_revision, update).await
        }

        async fn set_enabled(
            &self,
            id: Uuid,
            expected_revision: u64,
            enabled: bool,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::set_enabled(&*self.inner, id, expected_revision, enabled).await
        }

        async fn update_spec(
            &self,
            id: Uuid,
            expected_revision: u64,
            update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::update_spec(&*self.inner, id, expected_revision, update).await
        }

        async fn update_api_key_secret(
            &self,
            id: Uuid,
            api_key_ciphertext: Option<Vec<u8>>,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::update_api_key_secret(&*self.inner, id, api_key_ciphertext).await
        }

        async fn update_oauth_token(
            &self,
            id: Uuid,
            tokens: EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::update_oauth_token(&*self.inner, id, tokens).await
        }

        async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
            UpstreamStore::set_status(&*self.inner, id, status).await
        }

        async fn store_oauth_tokens(
            &self,
            id: Uuid,
            expected_revision: u64,
            tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::store_oauth_tokens(&*self.inner, id, expected_revision, tokens).await
        }

        async fn complete_refresh(
            &self,
            id: Uuid,
            holder: Uuid,
            tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::complete_refresh(&*self.inner, id, holder, tokens).await
        }

        async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
            UpstreamStore::set_last_apply_error(&*self.inner, id, error).await
        }

        async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
            UpstreamStore::soft_delete(&*self.inner, id, expected_revision).await
        }

        async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
            UpstreamStore::hard_delete(&*self.inner, id).await
        }

        async fn clear_warmup_dialect_plugin(
            &self,
            id: Uuid,
            expected_revision: u64,
        ) -> StorageResult<Option<UpstreamRecord>> {
            UpstreamStore::clear_warmup_dialect_plugin(&*self.inner, id, expected_revision).await
        }
    }

    struct EmptyDynamicViewStore;

    #[async_trait]
    impl UpstreamRateLimitStateStore for EmptyDynamicViewStore {
        async fn put_observation(
            &self,
            _record: &UpstreamRateLimitObservationRecord,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn list_for_upstream_ids(
            &self,
            _upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl UpstreamSubscriptionMetadataStore for EmptyDynamicViewStore {
        async fn put_upstream_subscription_metadata(
            &self,
            _record: &UpstreamSubscriptionMetadataRecord,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn get_upstream_subscription_metadata(
            &self,
            _upstream_id: Uuid,
        ) -> StorageResult<Option<UpstreamSubscriptionMetadataRecord>> {
            Ok(None)
        }

        async fn list_upstream_subscription_metadata(
            &self,
        ) -> StorageResult<Vec<UpstreamSubscriptionMetadataRecord>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl OrganizationMetadataStore for EmptyDynamicViewStore {
        async fn put_organization_metadata(
            &self,
            _record: &OrganizationMetadataRecord,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn get_organization_metadata(
            &self,
            _organization_uuid: &str,
        ) -> StorageResult<Option<OrganizationMetadataRecord>> {
            Ok(None)
        }

        async fn list_organization_metadata(
            &self,
        ) -> StorageResult<Vec<OrganizationMetadataRecord>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl PlanTierStore for EmptyDynamicViewStore {
        async fn upsert_plan_tier_ratio(&self, _record: &PlanTierRatioRecord) -> StorageResult<()> {
            Ok(())
        }

        async fn list_current_plan_tier_ratios(&self) -> StorageResult<Vec<PlanTierRatioRecord>> {
            Ok(Vec::new())
        }

        async fn list_plan_tier_ratios_as_of(
            &self,
            _as_of_unix_millis: i64,
        ) -> StorageResult<Vec<PlanTierRatioRecord>> {
            Ok(Vec::new())
        }

        async fn upsert_metadata_tier_override(
            &self,
            _record: &MetadataTierMappingOverrideRecord,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn list_current_metadata_tier_overrides(
            &self,
        ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
            Ok(Vec::new())
        }

        async fn list_metadata_tier_overrides_as_of(
            &self,
            _as_of_unix_millis: i64,
        ) -> StorageResult<Vec<MetadataTierMappingOverrideRecord>> {
            Ok(Vec::new())
        }

        async fn append_upstream_plan_tier(
            &self,
            _record: &UpstreamPlanTierRecord,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn backfill_upstream_plan_tier_intervals(
            &self,
            _upstream_id: Uuid,
            _intervals: &[UpstreamPlanTierRecord],
            _terminal_cap_unix_millis: i64,
            _provenance: &str,
        ) -> StorageResult<BackfillApplyOutcome> {
            Ok(BackfillApplyOutcome::Skipped)
        }

        async fn list_current_upstream_plan_tiers(
            &self,
        ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
            Ok(Vec::new())
        }

        async fn list_upstream_plan_tiers_as_of(
            &self,
            _as_of_unix_millis: i64,
        ) -> StorageResult<Vec<UpstreamPlanTierRecord>> {
            Ok(Vec::new())
        }
    }

    struct Fixture {
        storage: Arc<Storage>,
        stores: Arc<Stores>,
        oauth: Arc<AnthropicOAuthConfig>,
        aead: Arc<AeadService>,
        runtime: Arc<RecordingPluginRuntime>,
        holder: Arc<DynamicViewHolder>,
    }

    async fn fixture() -> Fixture {
        let storage = Arc::new(Storage::with_clock(cc_lb_testkit::fixed_clock(
            1_800_000_000,
        )));
        let empty = Arc::new(EmptyDynamicViewStore);
        let stores = Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: empty.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: empty.clone(),
            organization_metadata: empty.clone(),
            plan_tiers: empty,
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage.clone(),
            audit: None,
        });
        let oauth = Arc::new(AnthropicOAuthConfig::default());
        let aead = Arc::new(AeadService::from_master_key([24; 32]));
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let initial_generation = INITIAL_GENERATION;
        let initial = build_dynamic_view_with_plugin_runtime(
            &stores,
            &oauth,
            aead.clone(),
            None,
            initial_generation - 1,
            runtime.as_ref(),
            Path::new("."),
            Arc::new(crate::SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            cc_lb_testkit::fixed_clock(1_800_000_000),
        )
        .await
        .expect("initial dynamic view builds");
        assert_eq!(initial.generation, initial_generation);
        let holder = Arc::new(DynamicViewHolder::new(initial));
        Fixture {
            storage,
            stores,
            oauth,
            aead,
            runtime,
            holder,
        }
    }

    async fn spawn_listener(
        fixture: &Fixture,
        notifier: Arc<MockNotifier>,
        cancel: CancellationToken,
        stores: Arc<Stores>,
    ) -> tokio::task::JoinHandle<()> {
        let listener = Arc::new(NotifyListener::new_with_plugin_runtime(
            NotifyListenerPluginParams {
                notifier: notifier.clone(),
                cancel,
                holder: fixture.holder.clone(),
                stores,
                oauth_cfg: fixture.oauth.clone(),
                runtime: fixture.runtime.clone(),
                aead: fixture.aead.clone(),
                data_dir: Path::new(".").to_path_buf(),
                lazy_refresher: None,
                subscription_quota_cache: Arc::new(crate::SubscriptionQuotaCache::new()),
                prompt_cache_observation_cache: None,
                prompt_cache_observation_sink: None,
                subscription_quota_routing_max_staleness_secs: 1800,
                clock: cc_lb_testkit::fixed_clock(1_800_000_000),
            },
        ));
        let task = tokio::spawn(async move {
            listener.run().await;
        });
        notifier.wait_for_subscriber().await;
        task
    }

    fn stores_with_upstream(fixture: &Fixture, upstreams: Arc<dyn UpstreamStore>) -> Arc<Stores> {
        Arc::new(Stores {
            upstreams,
            principals: fixture.stores.principals.clone(),
            plugin_registry: fixture.stores.plugin_registry.clone(),
            upstream_rate_limits: fixture.stores.upstream_rate_limits.clone(),
            upstream_subscription_quotas: fixture.stores.upstream_subscription_quotas.clone(),
            upstream_subscription_metadata: fixture.stores.upstream_subscription_metadata.clone(),
            organization_metadata: fixture.stores.organization_metadata.clone(),
            plan_tiers: fixture.stores.plan_tiers.clone(),
            prompt_cache_observations: fixture.stores.prompt_cache_observations.clone(),
            anthropic_compatibility_kv: fixture.stores.anthropic_compatibility_kv.clone(),
            audit: None,
        })
    }

    #[tokio::test]
    async fn t2__single_notify_triggers_single_rebuild() {
        tokio::time::pause();
        let fixture = fixture().await;
        let start = fixture.holder.generation();
        let notifier = Arc::new(MockNotifier::new());
        assert_eq!(start, INITIAL_GENERATION);
        let cancel = CancellationToken::new();
        let task = spawn_listener(
            &fixture,
            notifier.clone(),
            cancel.clone(),
            fixture.stores.clone(),
        )
        .await;

        notifier.send(ChangeChannel::Upstream);
        notifier.close();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(250)).await;
        task.await
            .expect("listener exits after completing notified rebuild");
        assert_eq!(fixture.holder.generation(), start + 1);
    }

    #[tokio::test]
    async fn t2__burst_of_10_notifies_in_100ms_triggers_1_rebuild() {
        tokio::time::pause();
        let fixture = fixture().await;
        let start = fixture.holder.generation();
        let notifier = Arc::new(MockNotifier::new());
        assert_eq!(start, INITIAL_GENERATION);
        let cancel = CancellationToken::new();
        let task = spawn_listener(
            &fixture,
            notifier.clone(),
            cancel.clone(),
            fixture.stores.clone(),
        )
        .await;

        for _ in 0..10 {
            notifier.send(ChangeChannel::Upstream);
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        notifier.close();
        tokio::time::advance(Duration::from_millis(250)).await;
        task.await
            .expect("listener exits after completing debounced rebuild");
        assert_eq!(fixture.holder.generation(), start + 1);
    }

    #[tokio::test]
    async fn t2__cancel_during_rebuild_graceful() {
        tokio::time::pause();
        let fixture = fixture().await;
        let start = fixture.holder.generation();
        let notifier = Arc::new(MockNotifier::new());
        let cancel = CancellationToken::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let stores = stores_with_upstream(
            &fixture,
            Arc::new(ControlledUpstreamStore::blocking(
                fixture.storage.clone(),
                started_tx,
                release_rx,
            )),
        );
        let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

        notifier.send(ChangeChannel::Upstream);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(250)).await;
        started_rx.await.expect("rebuild starts");
        cancel.cancel();
        let _ = release_tx.send(());
        task.await.expect("listener exits without panic");

        assert_eq!(fixture.holder.generation(), start);
    }

    #[tokio::test]
    async fn t2__rebuild_failure_does_not_swap_view() {
        let (recorder, metrics) = cc_lb_testkit::local_recorder();
        let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
        tokio::time::pause();
        let fixture = fixture().await;
        let start = fixture.holder.generation();
        let before = labeled_counter_value(&metrics, "cclb_rebind_total", "outcome", "error");
        let notifier = Arc::new(MockNotifier::new());
        let cancel = CancellationToken::new();
        let (started_tx, started_rx) = oneshot::channel();
        let stores = stores_with_upstream(
            &fixture,
            Arc::new(ControlledUpstreamStore::failing(
                fixture.storage.clone(),
                started_tx,
            )),
        );
        let task = spawn_listener(&fixture, notifier.clone(), cancel.clone(), stores).await;

        notifier.send(ChangeChannel::Upstream);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(250)).await;
        started_rx.await.expect("failed rebuild starts");
        tokio::task::yield_now().await;

        assert_eq!(fixture.holder.generation(), start);
        assert_eq!(
            labeled_counter_value(&metrics, "cclb_rebind_total", "outcome", "error"),
            before + 1.0
        );
        cancel.cancel();
        task.await.expect("listener exits");
    }

    fn labeled_counter_value(
        snapshotter: &Snapshotter,
        name: &str,
        label: &str,
        value: &str,
    ) -> f64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _, _, metric)| {
                (key.key().name() == name
                    && key
                        .key()
                        .labels()
                        .any(|item| item.key() == label && item.value() == value))
                .then(|| format!("{metric:?}"))
                .and_then(|rendered| {
                    rendered
                        .strip_prefix("Counter(")?
                        .strip_suffix(')')?
                        .parse::<u64>()
                        .ok()
                })
            })
            .unwrap_or(0) as f64
    }
}
