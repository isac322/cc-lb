use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::clock::ClockHandle;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{PluginSlotKind, StorageResult};
#[cfg(test)]
use rand::SeedableRng;
use rand::{RngExt, rngs::StdRng};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::{
    DynamicViewPluginRuntime, Stores, build_dynamic_view_with_plugin_runtime,
};
use crate::prompt_cache_observation_cache::PromptCacheObservationCache;
use crate::revision_hash::compute_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use cc_lb_engine::PromptCacheObservationSinkLike;

pub struct Reconciler {
    pub stores: Arc<Stores>,
    pub holder: Arc<DynamicViewHolder>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<WasmtimeRuntime>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub cancel: CancellationToken,
    pub data_dir: PathBuf,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    pub prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    pub subscription_quota_routing_max_staleness_secs: u64,
    pub clock: ClockHandle,
    rng: Arc<Mutex<StdRng>>,
}

impl Reconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stores: Arc<Stores>,
        holder: Arc<DynamicViewHolder>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        runtime: Arc<WasmtimeRuntime>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        cancel: CancellationToken,
        data_dir: PathBuf,
        subscription_quota_cache: Arc<SubscriptionQuotaCache>,
        prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
        prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
        subscription_quota_routing_max_staleness_secs: u64,
        clock: ClockHandle,
    ) -> Self {
        Self {
            stores,
            holder,
            oauth_cfg,
            runtime,
            aead,
            lazy_refresher,
            cancel,
            data_dir,
            subscription_quota_cache,
            prompt_cache_observation_cache,
            prompt_cache_observation_sink,
            subscription_quota_routing_max_staleness_secs,
            clock,
            rng: Arc::new(Mutex::new(rand::make_rng())),
        }
    }

    pub async fn run(self: Arc<Self>) {
        Arc::new(ReconcilerDriver::from_reconciler(self.as_ref()))
            .run()
            .await;
    }
}

struct ReconcilerDriver {
    stores: Arc<Stores>,
    holder: Arc<DynamicViewHolder>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    runtime: Arc<dyn DynamicViewPluginRuntime>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    cancel: CancellationToken,
    data_dir: PathBuf,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: ClockHandle,
    rng: Arc<Mutex<StdRng>>,
}

impl ReconcilerDriver {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stores: Arc<Stores>,
        holder: Arc<DynamicViewHolder>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        runtime: Arc<dyn DynamicViewPluginRuntime>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        cancel: CancellationToken,
        data_dir: PathBuf,
        subscription_quota_cache: Arc<SubscriptionQuotaCache>,
        prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
        prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
        subscription_quota_routing_max_staleness_secs: u64,
        clock: ClockHandle,
        rng: Arc<Mutex<StdRng>>,
    ) -> Self {
        Self {
            stores,
            holder,
            oauth_cfg,
            runtime,
            aead,
            lazy_refresher,
            cancel,
            data_dir,
            subscription_quota_cache,
            prompt_cache_observation_cache,
            prompt_cache_observation_sink,
            subscription_quota_routing_max_staleness_secs,
            clock,
            rng,
        }
    }

    fn from_reconciler(reconciler: &Reconciler) -> Self {
        Self::new(
            reconciler.stores.clone(),
            reconciler.holder.clone(),
            reconciler.oauth_cfg.clone(),
            reconciler.runtime.clone(),
            reconciler.aead.clone(),
            reconciler.lazy_refresher.clone(),
            reconciler.cancel.clone(),
            reconciler.data_dir.clone(),
            reconciler.subscription_quota_cache.clone(),
            reconciler.prompt_cache_observation_cache.clone(),
            reconciler.prompt_cache_observation_sink.clone(),
            reconciler.subscription_quota_routing_max_staleness_secs,
            reconciler.clock.clone(),
            reconciler.rng.clone(),
        )
    }

    async fn run(self: Arc<Self>) {
        let mut interval =
            tokio::time::interval(Duration::from_millis(60_000 + jitter_millis(&self.rng)));
        interval.tick().await;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = interval.tick() => {}
            }

            let jitter = tokio::time::sleep(Duration::from_millis(jitter_millis(&self.rng)));
            tokio::pin!(jitter);
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = &mut jitter => {}
            }

            let result = tokio::select! {
                _ = self.cancel.cancelled() => return,
                result = self.reconcile_once() => result,
            };
            if let Err(error) = result {
                tracing::warn!(error = %error, "dynamic reconciliation tick failed");
            }
        }
    }

    async fn reconcile_once(&self) -> StorageResult<()> {
        let hash = collect_revision_hash(&self.stores).await?;
        let current = self.holder.load();
        if hash == current.upstream_status_snapshot.revision_hash {
            metrics::counter!("cclb_reconcile_total", "outcome" => "unchanged").increment(1);
            return Ok(());
        }

        let generation = current.generation;
        drop(current);
        match build_dynamic_view_with_plugin_runtime(
            &self.stores,
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            generation,
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
                // Reconcile is a background rebuilder that can race with
                // admin-triggered rebinds. If our view is stale relative
                // to whatever the admin path just published, drop it —
                // resurrecting a deleted slot via an old generation is
                // the concrete failure mode Sprint 2 fixes.
                if self.holder.try_store_if_newer(view) {
                    metrics::counter!("cclb_reconcile_total", "outcome" => "changed").increment(1);
                } else {
                    metrics::counter!("cclb_reconcile_total", "outcome" => "stale").increment(1);
                    tracing::debug!("reconcile view rejected: newer generation already resident");
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "dynamic reconciliation rebuild failed");
                metrics::counter!("cclb_reconcile_total", "outcome" => "error").increment(1);
            }
        }
        Ok(())
    }
}

pub(crate) async fn collect_revision_hash(stores: &Stores) -> StorageResult<u64> {
    let mut upstreams = Vec::new();
    let mut after_upstream = None;
    loop {
        let page = stores.upstreams.list(after_upstream, 100).await?;
        if page.is_empty() {
            break;
        }
        after_upstream = page.last().map(|record| record.id);
        upstreams.extend(page.into_iter().map(|record| (record.id, record.revision)));
    }

    let mut registry_entries = Vec::new();
    let mut after_registry = None;
    loop {
        let page = stores
            .plugin_registry
            .list_registry(after_registry, 100)
            .await?;
        if page.is_empty() {
            break;
        }
        after_registry = page.last().map(|entry| entry.id);
        registry_entries.extend(
            page.into_iter()
                .map(|entry| (entry.id, entry.revision, entry.sha256)),
        );
    }

    let mut principals = Vec::new();
    let mut chains = Vec::new();
    let mut offset = 0;
    loop {
        let page = stores.principals.list(offset, 100, false).await?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        for principal in page {
            chains.extend(chain_revisions(stores, principal.id, PluginSlotKind::Router).await?);
            chains.extend(
                chain_revisions(stores, principal.id, PluginSlotKind::ObservabilityHook).await?,
            );
            chains.extend(chain_revisions(stores, principal.id, PluginSlotKind::Shape).await?);
            principals.push((principal.id, principal.revision));
        }
    }

    Ok(compute_revision_hash(
        &upstreams,
        &principals,
        &chains,
        &registry_entries,
    ))
}

async fn chain_revisions(
    stores: &Stores,
    principal_id: Uuid,
    slot: PluginSlotKind,
) -> StorageResult<Vec<(Uuid, u64)>> {
    let entries = stores
        .plugin_registry
        .list_chain_for_principal(principal_id, slot)
        .await?;
    Ok(entries
        .into_iter()
        .map(|entry| (entry.id, entry.revision))
        .collect())
}

fn jitter_millis(rng: &Mutex<StdRng>) -> u64 {
    rng.lock()
        .expect("reconciler RNG mutex poisoned")
        .random_range(0..=10_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn seeded_rng(seed: [u8; 32]) -> Mutex<StdRng> {
        Mutex::new(StdRng::from_seed(seed))
    }

    #[test]
    fn equal_seeds_produce_equal_jitter_sequences() {
        let left = seeded_rng([31; 32]);
        let right = seeded_rng([31; 32]);

        let left_jitter = (0..256).map(|_| jitter_millis(&left)).collect::<Vec<_>>();
        let right_jitter = (0..256).map(|_| jitter_millis(&right)).collect::<Vec<_>>();

        assert_eq!(left_jitter, right_jitter);
    }

    #[test]
    fn jitter_stays_within_inclusive_bounds() {
        let rng = seeded_rng([37; 32]);

        assert!((0..10_000).all(|_| (0..=10_000).contains(&jitter_millis(&rng))));
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod t2__reconciliation {
    // tier-allow(silent-skip): storage trait optional-return signatures never skip assertions until=2027-03-31
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::ReconcilerDriver;
    use crate::dynamic_view_builder::{
        DynamicViewPluginRuntime, PluginRuntimeSlotKey, RebindError, Stores,
        build_dynamic_view_with_plugin_runtime,
    };
    use async_trait::async_trait;
    use cc_lb_aead::AeadService;
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_engine::DynamicViewHolder;
    use cc_lb_storage_api::{
        AnthropicCompatibilityKvStore, BackfillApplyOutcome, CompatibilityKvRecord,
        MetadataTierMappingOverrideRecord, OrganizationMetadataRecord, OrganizationMetadataStore,
        PlanTierRatioRecord, PlanTierStore, PluginChainEntry, PluginChainEntryInput,
        PluginChainEntryUpdate, PluginRegistryStore, PluginSlotKind, PrincipalCreate,
        PrincipalKind, PrincipalRecord, PrincipalStore, PrincipalUpdate,
        PromptCacheObservationStore, StorageResult, SubscriptionQuotaCheckpointRange,
        SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaCheckpointRecord,
        SubscriptionQuotaSample, SubscriptionQuotaSeries, SubscriptionQuotaSeriesQuery,
        UpstreamCreate, UpstreamPlanTierRecord, UpstreamRateLimitObservationRecord,
        UpstreamRateLimitStateStore, UpstreamRecord, UpstreamStore,
        UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
        UpstreamSubscriptionQuotaStore, UpstreamUpdate, WasmBlob, WasmRegistryEntry,
        WasmRegistryEntryInput,
    };

    use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
    use cc_lb_testkit::InMemoryStorage as Storage;
    use metrics_util::debugging::Snapshotter;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[derive(Default)]
    struct RecordingPluginRuntime {
        retained: std::sync::Mutex<Vec<HashSet<PluginRuntimeSlotKey>>>,
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

    fn labeled_counter_value(
        snapshotter: &Snapshotter,
        name: &str,
        label: &str,
        value: &str,
    ) -> u64 {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _, _, metric)| {
                if key.key().name() != name
                    || !key
                        .key()
                        .labels()
                        .any(|item| item.key() == label && item.value() == value)
                {
                    return None;
                }
                debug_counter_value(&metric)
            })
            .unwrap_or(0)
    }

    fn debug_counter_value(value: &impl Debug) -> Option<u64> {
        let rendered = format!("{value:?}");
        rendered
            .strip_prefix("Counter(")?
            .strip_suffix(')')?
            .parse()
            .ok()
    }

    fn storage_fixture() -> Arc<Storage> {
        Arc::new(Storage::with_clock(cc_lb_testkit::fixed_clock(
            1_800_000_000,
        )))
    }

    fn stores(storage: Arc<Storage>) -> Arc<Stores> {
        Arc::new(Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage,
            upstream_rate_limits: Arc::new(EmptyRateLimitStore),
            upstream_subscription_quotas: Arc::new(EmptySubscriptionQuotaStore),
            upstream_subscription_metadata: Arc::new(EmptyUpstreamSubscriptionMetadataStore),
            organization_metadata: Arc::new(EmptyOrganizationMetadataStore),
            plan_tiers: Arc::new(EmptyPlanTierStore),
            prompt_cache_observations: Arc::new(EmptyPromptCacheObservationStore),
            anthropic_compatibility_kv: Arc::new(EmptyCompatibilityKvStore),
            audit: None,
        })
    }

    async fn create_principal(storage: &Storage, name: &str) -> PrincipalRecord {
        PrincipalStore::create(
            storage,
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await
        .expect("principal created")
    }

    async fn create_upstream(storage: &Storage, name: &str) -> UpstreamRecord {
        UpstreamStore::create(
            storage,
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(vec![1, 2, 3]),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created")
    }

    async fn seed_registry(storage: &Storage, seed: u8, name: &str) -> WasmRegistryEntry {
        let (entry, _) = storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256: [seed; 32],
                    bytes: vec![seed; seed as usize],
                    size_bytes: seed as u64,
                    parse_validated_at_unix_secs: 1_800_000_000,
                },
                WasmRegistryEntryInput {
                    schema_hash: None,
                    name: name.to_owned(),
                    version: None,
                    original_filename: format!("{name}.wasm"),
                    label: None,
                    uploaded_at_unix_secs: 1_800_000_000,
                    uploaded_by_admin_id: Uuid::from_u128(u128::from(seed)),
                    description: format!("{name} description"),
                    usage: "test fixture".to_owned(),
                    hook_metadata: Default::default(),
                    supported_slots: Vec::new(),
                },
            )
            .await
            .expect("registry entry created");
        entry
    }

    async fn initial_holder(
        stores: &Stores,
        runtime: &Arc<RecordingPluginRuntime>,
        data_dir: &std::path::Path,
    ) -> Arc<DynamicViewHolder> {
        let view = build_dynamic_view_with_plugin_runtime(
            stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([25; 32])),
            None,
            0,
            runtime.as_ref(),
            data_dir,
            Arc::new(crate::SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            cc_lb_testkit::fixed_clock(1_800_000_000),
        )
        .await
        .expect("initial dynamic view");
        Arc::new(DynamicViewHolder::new(view))
    }

    fn reconciler(
        stores: Arc<Stores>,
        holder: Arc<DynamicViewHolder>,
        runtime: Arc<RecordingPluginRuntime>,
        cancel: CancellationToken,
        data_dir: &std::path::Path,
    ) -> Arc<ReconcilerDriver> {
        Arc::new(ReconcilerDriver::new(
            stores,
            holder,
            Arc::new(AnthropicOAuthConfig::default()),
            runtime,
            Arc::new(AeadService::from_master_key([25; 32])),
            None,
            cancel,
            data_dir.to_path_buf(),
            Arc::new(crate::SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            cc_lb_testkit::fixed_clock(1_800_000_000),
            Arc::new(super::tests::seeded_rng([43; 32])),
        ))
    }

    async fn advance_until(mut done: impl FnMut() -> bool) {
        tokio::task::yield_now().await;
        for _ in 0..200 {
            if done() {
                return;
            }
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn t2__unchanged_tick_emits_unchanged_metric_and_does_not_rebuild() {
        let (recorder, metrics) = cc_lb_testkit::local_recorder();
        let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
        let storage = storage_fixture();
        create_principal(&storage, "principal-a").await;
        create_upstream(&storage, "upstream-a").await;
        let stores = stores(storage);
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let holder = initial_holder(&stores, &runtime, Path::new(".")).await;
        let generation = holder.load().generation;
        let before =
            labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "unchanged");
        let cancel = CancellationToken::new();
        let reconciler = reconciler(stores, holder.clone(), runtime, cancel, Path::new("."));
        reconciler.reconcile_once().await.expect("reconcile tick");

        assert_eq!(holder.load().generation, generation);
        let after = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "unchanged");
        assert!(after > before);
    }

    #[tokio::test]
    async fn t2__notify_dropped_then_reconcile_catches_up_within_one_tick() {
        let (recorder, metrics) = cc_lb_testkit::local_recorder();
        let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);
        let storage = storage_fixture();
        create_principal(&storage, "principal-a").await;
        create_upstream(&storage, "upstream-a").await;
        let stores = stores(storage.clone());
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let holder = initial_holder(&stores, &runtime, Path::new(".")).await;
        let generation = holder.load().generation;
        let before = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "changed");
        create_upstream(&storage, "upstream-b").await;
        let cancel = CancellationToken::new();
        let reconciler = reconciler(stores, holder.clone(), runtime, cancel, Path::new("."));
        reconciler.reconcile_once().await.expect("reconcile tick");

        assert!(holder.load().generation > generation);
        let after = labeled_counter_value(&metrics, "cclb_reconcile_total", "outcome", "changed");
        assert!(after > before);
    }

    #[tokio::test]
    async fn t2__reconciler_rebuilds_after_registry_entry_insert() {
        let storage = storage_fixture();
        create_principal(&storage, "principal-registry-insert").await;
        create_upstream(&storage, "upstream-registry-insert").await;
        let stores = stores(storage.clone());
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let holder = initial_holder(&stores, &runtime, Path::new(".")).await;
        let generation = holder.load().generation;

        seed_registry(&storage, 31, "plugin-registry-insert").await;
        let cancel = CancellationToken::new();
        let reconciler = reconciler(stores, holder.clone(), runtime, cancel, Path::new("."));
        reconciler.reconcile_once().await.expect("reconcile tick");

        assert!(holder.load().generation > generation);
    }

    #[tokio::test]
    async fn t2__reconciler_rebuilds_after_registry_entry_delete() {
        let storage = storage_fixture();
        create_principal(&storage, "principal-registry-delete").await;
        create_upstream(&storage, "upstream-registry-delete").await;
        let entry = seed_registry(&storage, 32, "plugin-registry-delete").await;
        let stores = stores(storage.clone());
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let holder = initial_holder(&stores, &runtime, Path::new(".")).await;
        let generation = holder.load().generation;

        storage
            .delete_registry_entry(entry.id, entry.revision)
            .await
            .expect("registry delete")
            .expect("registry entry deleted");
        let cancel = CancellationToken::new();
        let reconciler = reconciler(stores, holder.clone(), runtime, cancel, Path::new("."));
        reconciler.reconcile_once().await.expect("reconcile tick");

        assert!(holder.load().generation > generation);
    }

    #[tokio::test]
    async fn t2__reconciler_rebuilds_after_registry_entry_update() {
        let storage = storage_fixture();
        create_principal(&storage, "principal-registry-update").await;
        create_upstream(&storage, "upstream-registry-update").await;
        let entry = seed_registry(&storage, 33, "plugin-registry-update").await;
        let stores = stores(storage.clone());
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let holder = initial_holder(&stores, &runtime, Path::new(".")).await;
        let generation = holder.load().generation;

        storage
            .update_registry_label(entry.id, entry.revision, Some("Updated".to_owned()))
            .await
            .expect("registry label update");
        let cancel = CancellationToken::new();
        let reconciler = reconciler(stores, holder.clone(), runtime, cancel, Path::new("."));
        reconciler.reconcile_once().await.expect("reconcile tick");

        assert!(holder.load().generation > generation);
    }

    #[tokio::test(start_paused = true)]
    async fn t2__cancel_during_tick_is_graceful() {
        let blocking_store = Arc::new(BlockingUpstreamStore::default());
        let blocking_stores = Arc::new(Stores {
            upstreams: blocking_store.clone(),
            principals: Arc::new(EmptyPrincipalStore),
            plugin_registry: Arc::new(EmptyPluginRegistryStore),
            upstream_rate_limits: Arc::new(EmptyRateLimitStore),
            upstream_subscription_quotas: Arc::new(EmptySubscriptionQuotaStore),
            upstream_subscription_metadata: Arc::new(EmptyUpstreamSubscriptionMetadataStore),
            organization_metadata: Arc::new(EmptyOrganizationMetadataStore),
            plan_tiers: Arc::new(EmptyPlanTierStore),
            prompt_cache_observations: Arc::new(EmptyPromptCacheObservationStore),
            anthropic_compatibility_kv: Arc::new(EmptyCompatibilityKvStore),
            audit: None,
        });
        let storage = storage_fixture();
        let runtime = Arc::new(RecordingPluginRuntime::default());
        let real_stores = stores(storage);
        let holder = initial_holder(&real_stores, &runtime, Path::new(".")).await;
        let cancel = CancellationToken::new();
        let reconciler = reconciler(
            blocking_stores.clone(),
            holder,
            runtime,
            cancel.clone(),
            Path::new("."),
        );
        let release = blocking_store.release.clone();
        let task = tokio::spawn(reconciler.run());

        advance_until(|| blocking_store.entered()).await;
        assert!(blocking_store.entered());
        cancel.cancel();
        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("task exits")
            .expect("task join");
    }

    #[derive(Default)]
    struct BlockingUpstreamStore {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        blocked: AtomicBool,
        entered_flag: AtomicBool,
    }

    impl BlockingUpstreamStore {
        fn entered(&self) -> bool {
            self.entered_flag.load(Ordering::Acquire)
        }
    }

    #[async_trait]
    impl UpstreamStore for BlockingUpstreamStore {
        async fn create(&self, _create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn get_by_name(&self, _name: &str) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }

        async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }

        async fn list(
            &self,
            _after: Option<Uuid>,
            _limit: usize,
        ) -> StorageResult<Vec<UpstreamRecord>> {
            if !self.blocked.swap(true, Ordering::AcqRel) {
                self.entered_flag.store(true, Ordering::Release);
                self.entered.notify_waiters();
                self.release.notified().await;
            }
            Ok(Vec::new())
        }

        async fn update(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn set_enabled(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _enabled: bool,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn update_spec(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn update_api_key_secret(
            &self,
            _id: Uuid,
            _api_key_ciphertext: Option<Vec<u8>>,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn update_oauth_token(
            &self,
            _id: Uuid,
            _tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn set_status(&self, _id: Uuid, _status: UpstreamStatusUpdate) -> StorageResult<()> {
            unimplemented!()
        }

        async fn store_oauth_tokens(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn complete_refresh(
            &self,
            _id: Uuid,
            _holder: Uuid,
            _tokens: cc_lb_aead::EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            unimplemented!()
        }

        async fn set_last_apply_error(
            &self,
            _id: Uuid,
            _error: Option<String>,
        ) -> StorageResult<()> {
            unimplemented!()
        }

        async fn soft_delete(&self, _id: Uuid, _expected_revision: u64) -> StorageResult<()> {
            unimplemented!()
        }

        async fn hard_delete(&self, _id: Uuid) -> StorageResult<()> {
            unimplemented!()
        }

        async fn clear_warmup_dialect_plugin(
            &self,
            _id: Uuid,
            _expected_revision: u64,
        ) -> StorageResult<Option<UpstreamRecord>> {
            unimplemented!()
        }
    }

    struct EmptyRateLimitStore;

    #[async_trait]
    impl UpstreamRateLimitStateStore for EmptyRateLimitStore {
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

    struct EmptySubscriptionQuotaStore;

    #[async_trait]
    impl UpstreamSubscriptionQuotaStore for EmptySubscriptionQuotaStore {
        async fn record_subscription_quota_samples(
            &self,
            _records: &[SubscriptionQuotaSample],
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn list_latest_subscription_quota_for_upstreams(
            &self,
            _upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<SubscriptionQuotaSample>> {
            Ok(Vec::new())
        }

        async fn list_subscription_quota_series(
            &self,
            _query: SubscriptionQuotaSeriesQuery,
        ) -> StorageResult<Vec<SubscriptionQuotaSeries>> {
            Ok(Vec::new())
        }

        async fn put_subscription_quota_checkpoints(
            &self,
            _records: &[SubscriptionQuotaCheckpointRecord],
        ) -> StorageResult<usize> {
            Ok(0)
        }

        async fn list_latest_subscription_quota_checkpoints_for_upstreams(
            &self,
            _upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRecord>> {
            Ok(Vec::new())
        }

        async fn list_subscription_quota_checkpoint_ranges(
            &self,
            _query: SubscriptionQuotaCheckpointRangeQuery,
        ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRange>> {
            Ok(Vec::new())
        }
    }

    struct EmptyPromptCacheObservationStore;

    impl PromptCacheObservationStore for EmptyPromptCacheObservationStore {}

    struct EmptyPlanTierStore;

    #[async_trait]
    impl PlanTierStore for EmptyPlanTierStore {
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

    struct EmptyUpstreamSubscriptionMetadataStore;

    #[async_trait]
    impl UpstreamSubscriptionMetadataStore for EmptyUpstreamSubscriptionMetadataStore {
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

    struct EmptyOrganizationMetadataStore;

    #[async_trait]
    impl OrganizationMetadataStore for EmptyOrganizationMetadataStore {
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

    struct EmptyCompatibilityKvStore;

    #[async_trait]
    impl AnthropicCompatibilityKvStore for EmptyCompatibilityKvStore {
        async fn put_compatibility_kv_value(
            &self,
            _key: &str,
            _value: &str,
            _observed_at_unix_secs: u64,
            _source_url: Option<&str>,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn put_compatibility_kv_failure(
            &self,
            _key: &str,
            _attempted_at_unix_secs: u64,
            _error: &str,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn get_compatibility_kv(
            &self,
            _key: &str,
        ) -> StorageResult<Option<CompatibilityKvRecord>> {
            Ok(None)
        }

        async fn list_compatibility_kv(&self) -> StorageResult<Vec<CompatibilityKvRecord>> {
            Ok(Vec::new())
        }
    }

    struct EmptyPrincipalStore;

    #[async_trait]
    impl PrincipalStore for EmptyPrincipalStore {
        async fn create(
            &self,
            _input: PrincipalCreate,
            _now_unix_secs: u64,
        ) -> StorageResult<PrincipalRecord> {
            unimplemented!()
        }
        async fn get_by_id(&self, _id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
        async fn get_by_name(&self, _name: &str) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
        async fn list(
            &self,
            _offset: usize,
            _limit: usize,
            _include_deleted: bool,
        ) -> StorageResult<Vec<PrincipalRecord>> {
            Ok(Vec::new())
        }
        async fn update(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _update: PrincipalUpdate,
            _now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
        async fn set_enabled(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _enabled: bool,
            _now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
        async fn soft_delete(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
        async fn hard_delete(&self, _id: Uuid) -> StorageResult<bool> {
            unimplemented!()
        }
        async fn set_last_apply_error(
            &self,
            _id: Uuid,
            _error: Option<String>,
            _applied_at_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            unimplemented!()
        }
    }

    struct EmptyPluginRegistryStore;

    #[async_trait]
    impl PluginRegistryStore for EmptyPluginRegistryStore {
        async fn persist_wasm_upload(
            &self,
            _blob: WasmBlob,
            _entry: WasmRegistryEntryInput,
        ) -> StorageResult<(WasmRegistryEntry, bool)> {
            unimplemented!()
        }
        async fn get_blob_bytes(&self, _sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
            unimplemented!()
        }
        async fn get_blob(
            &self,
            _sha256: [u8; 32],
        ) -> StorageResult<Option<cc_lb_storage_api::WasmBlobRecord>> {
            unimplemented!()
        }
        async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
            unimplemented!()
        }
        async fn list_registry(
            &self,
            _after: Option<Uuid>,
            _limit: usize,
        ) -> StorageResult<Vec<WasmRegistryEntry>> {
            Ok(Vec::new())
        }
        async fn get_registry_entry_by_sha(
            &self,
            _sha256: [u8; 32],
        ) -> StorageResult<Option<WasmRegistryEntry>> {
            unimplemented!()
        }
        async fn get_registry_entry_by_id(
            &self,
            _id: Uuid,
        ) -> StorageResult<Option<WasmRegistryEntry>> {
            unimplemented!()
        }
        async fn get_registry_entry_by_name(
            &self,
            _name: &str,
        ) -> StorageResult<Option<WasmRegistryEntry>> {
            unimplemented!()
        }
        async fn replace_wasm_entry(
            &self,
            _blob: WasmBlob,
            _entry: WasmRegistryEntryInput,
            _expected_revision: u64,
        ) -> StorageResult<WasmRegistryEntry> {
            unimplemented!()
        }
        async fn list_registry_references(
            &self,
            _id: Uuid,
        ) -> StorageResult<cc_lb_storage_api::WasmRegistryReferences> {
            unimplemented!()
        }
        async fn cascade_delete_registry_entry(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _expected_references: cc_lb_storage_api::WasmRegistryReferenceFingerprint,
        ) -> StorageResult<Option<cc_lb_storage_api::WasmRegistryCascadeDelete>> {
            unimplemented!()
        }
        async fn update_registry_label(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _label: Option<String>,
        ) -> StorageResult<WasmRegistryEntry> {
            unimplemented!()
        }
        async fn update_supported_slots(
            &self,
            _id: Uuid,
            _supported_slots: Vec<PluginSlotKind>,
        ) -> StorageResult<()> {
            Ok(())
        }
        async fn delete_registry_entry(
            &self,
            _id: Uuid,
            _expected_revision: u64,
        ) -> StorageResult<Option<WasmRegistryEntry>> {
            unimplemented!()
        }
        async fn decrement_blob_refcount_or_delete(
            &self,
            _sha256: [u8; 32],
        ) -> StorageResult<bool> {
            unimplemented!()
        }
        async fn insert_chain_entry(
            &self,
            _entry: PluginChainEntryInput,
        ) -> StorageResult<PluginChainEntry> {
            unimplemented!()
        }
        async fn list_chain_for_principal(
            &self,
            _principal_id: Uuid,
            _slot: PluginSlotKind,
        ) -> StorageResult<Vec<PluginChainEntry>> {
            Ok(Vec::new())
        }
        async fn update_chain_entry(
            &self,
            _id: Uuid,
            _expected_revision: u64,
            _update: PluginChainEntryUpdate,
        ) -> StorageResult<Option<PluginChainEntry>> {
            unimplemented!()
        }
        async fn reorder_chain(
            &self,
            _principal_id: Uuid,
            _slot: PluginSlotKind,
            _new_orders: Vec<(Uuid, i64, u64)>,
        ) -> StorageResult<Vec<PluginChainEntry>> {
            unimplemented!()
        }
        async fn delete_chain_entry(
            &self,
            _id: Uuid,
            _expected_revision: u64,
        ) -> StorageResult<Option<PluginChainEntry>> {
            unimplemented!()
        }
        async fn rebalance_chain(
            &self,
            _principal_id: Uuid,
            _slot: PluginSlotKind,
        ) -> StorageResult<Vec<PluginChainEntry>> {
            unimplemented!()
        }
    }
}
