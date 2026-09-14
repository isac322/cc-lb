use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::{AnthropicOAuthConfig, PromptCacheShadowConfig};
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, Principal, RateLimitObservation, Upstream,
    UpstreamCandidate,
};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache, ShapePluginCache,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::clock::{unix_millis, unix_secs};
use cc_lb_engine::plan_capacity::{
    PRO_CAPACITY_RATIO, PlanInfo, PlanTierClassification, TierKey, classify_plan_tier,
};
use cc_lb_engine::{
    ApplyStatus, DynamicView, DynamicViewBuilder, UpstreamRateLimitCache, UpstreamStatusEntry,
    UpstreamStatusSnapshot,
};
use cc_lb_routing::{FilterPlugin, RouteDecision, RouteError, RouterPlugin};
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntime};

use crate::wasm_host::{
    WasmtimeFilterPlugin, WasmtimeObservabilityHookPlugin, WasmtimeUpstreamDialect,
};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, MetadataTierMappingOverrideRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, PluginChainEntry,
    PluginRegistryStore, PluginSlotKind, PrincipalRecord, PrincipalStore,
    PromptCacheObservationStore, RateLimitKind, StorageError, StorageResult, TierResolutionSource,
    UpstreamPlanTierRecord, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamRecord, UpstreamStore, UpstreamSubscriptionMetadataStore,
    UpstreamSubscriptionQuotaStore, WasmRegistryEntry,
};
use cc_lb_upstream::{Signer, SignerError, SignerFactory};
use parking_lot::RwLock;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use cc_lb_engine::PromptCacheObservationSinkLike;

use crate::PluginManifest;
use crate::prompt_cache_observation_cache::PromptCacheObservationCache;
use crate::reconcile::collect_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

/// Maximum number of user-supplied router filters. The built-in
/// `subscription-preference` entry is structural router-chain state.
const MAX_ROUTER_CHAIN_DEPTH: usize = 16;

pub struct Stores {
    pub upstreams: Arc<dyn UpstreamStore>,
    pub principals: Arc<dyn PrincipalStore>,
    pub plugin_registry: Arc<dyn PluginRegistryStore>,
    pub upstream_rate_limits: Arc<dyn UpstreamRateLimitStateStore>,
    pub upstream_subscription_quotas: Arc<dyn UpstreamSubscriptionQuotaStore>,
    pub upstream_subscription_metadata: Arc<dyn UpstreamSubscriptionMetadataStore>,
    pub organization_metadata: Arc<dyn OrganizationMetadataStore>,
    pub plan_tiers: Arc<dyn PlanTierStore>,
    pub prompt_cache_observations: Arc<dyn PromptCacheObservationStore>,
    pub anthropic_compatibility_kv: Arc<dyn AnthropicCompatibilityKvStore>,
    pub audit: Option<Arc<dyn AuditStore>>,
}

#[derive(Debug, Error)]
pub enum RebindError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    PluginRuntime(#[from] cc_lb_runtime_wasmtime::WasmtimeRuntimeError),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PluginRuntimeSlotKey {
    principal: String,
    plugin: String,
}

impl PluginRuntimeSlotKey {
    fn new(principal: impl Into<String>, plugin: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            plugin: plugin.into(),
        }
    }
}

#[async_trait]
pub(crate) trait DynamicViewPluginRuntime: Send + Sync {
    async fn instantiate_filter(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
        chain_entry_id: Uuid,
    ) -> Result<Arc<dyn FilterPlugin>, RebindError>;

    async fn instantiate_shape(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn cc_lb_upstream::UpstreamDialect>, RebindError>;

    async fn instantiate_observability_hook(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn cc_lb_observability::ObservabilityHook>, RebindError>;

    fn retain_slots(&self, slot_keys: &HashSet<PluginRuntimeSlotKey>) -> usize;
}

#[async_trait]
impl DynamicViewPluginRuntime for WasmtimeRuntime {
    async fn instantiate_filter(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
        chain_entry_id: Uuid,
    ) -> Result<Arc<dyn FilterPlugin>, RebindError> {
        let wasm = read_wasm_for_manifest(manifest).await?;
        let slot = self.register_filter(
            cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                slot_key.principal.clone(),
                slot_key.plugin.clone(),
            ),
            manifest.name.clone(),
            &wasm,
        )?;
        let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(slot, self.config_arc()));
        Ok(Arc::new(WasmtimeFilterPlugin::new(
            dispatch,
            chain_entry_id,
            manifest.name.clone(),
        )))
    }

    async fn instantiate_shape(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn cc_lb_upstream::UpstreamDialect>, RebindError> {
        let wasm = read_wasm_for_manifest(manifest).await?;
        let slot = self.register_shape(
            cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                slot_key.principal.clone(),
                slot_key.plugin.clone(),
            ),
            manifest.name.clone(),
            &wasm,
        )?;
        let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(slot, self.config_arc()));
        Ok(Arc::new(WasmtimeUpstreamDialect::new(dispatch)))
    }

    async fn instantiate_observability_hook(
        &self,
        slot_key: &PluginRuntimeSlotKey,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn cc_lb_observability::ObservabilityHook>, RebindError> {
        let wasm = read_wasm_for_manifest(manifest).await?;
        let slot = self.register_observe(
            cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                slot_key.principal.clone(),
                slot_key.plugin.clone(),
            ),
            manifest.name.clone(),
            &wasm,
        )?;
        let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(slot, self.config_arc()));
        Ok(Arc::new(WasmtimeObservabilityHookPlugin::new(dispatch)))
    }

    fn retain_slots(&self, slot_keys: &HashSet<PluginRuntimeSlotKey>) -> usize {
        let slot_keys: HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey> = slot_keys
            .iter()
            .map(|key| {
                cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                    key.principal.clone(),
                    key.plugin.clone(),
                )
            })
            .collect();
        WasmtimeRuntime::retain_slots(self, &slot_keys).len()
    }
}

async fn read_wasm_for_manifest(
    manifest: &PluginManifest,
) -> Result<Vec<u8>, cc_lb_runtime_wasmtime::WasmtimeRuntimeError> {
    tokio::fs::read(&manifest.artifact).await.map_err(|source| {
        cc_lb_runtime_wasmtime::WasmtimeRuntimeError::ModuleRejected {
            reason: format!(
                "failed to read plugin wasm at `{}`: {source}",
                manifest.artifact
            ),
        }
    })
}

pub fn ensure_wasm_cache_dirs(data_dir: &Path) -> io::Result<()> {
    let cache_dir = data_dir.join("plugins").join("wasm").join("cache");
    let tmp_dir = cache_dir.join(".tmp");
    fs::create_dir_all(&tmp_dir)?;
    #[cfg(unix)]
    {
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&cache_dir, Permissions::from_mode(0o700))?;
        fs::set_permissions(&tmp_dir, Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn ensure_wasm_cached(
    data_dir: &Path,
    sha256: [u8; 32],
    fetch: impl FnOnce() -> StorageResult<Option<Vec<u8>>>,
) -> io::Result<PathBuf> {
    ensure_wasm_cache_dirs(data_dir)?;
    let cache_dir = data_dir.join("plugins").join("wasm").join("cache");
    let tmp_dir = cache_dir.join(".tmp");
    let target = cache_dir.join(format!("{}.wasm", hex_sha256(sha256)));
    if target.exists() {
        return Ok(target);
    }

    let bytes = fetch().map_err(io::Error::other)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("wasm blob {} not found", hex_sha256(sha256)),
        )
    })?;
    let tmp = tmp_dir.join(format!("{}.wasm", uuid::Uuid::new_v4()));
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    match fs::rename(&tmp, &target) {
        Ok(()) => Ok(target),
        Err(_) if target.exists() => {
            let _ = fs::remove_file(&tmp);
            Ok(target)
        }
        Err(error) => {
            let _ = fs::remove_file(&tmp);
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn build_dynamic_view(
    stores: &Stores,
    oauth_anthropic: &AnthropicOAuthConfig,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    current_generation: u64,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: cc_lb_engine::ClockHandle,
) -> Result<Arc<DynamicView>, RebindError> {
    build_dynamic_view_with_plugin_runtime(
        stores,
        oauth_anthropic,
        aead,
        lazy_refresher,
        current_generation,
        runtime.as_ref(),
        data_dir,
        subscription_quota_cache,
        prompt_cache_observation_cache,
        prompt_cache_observation_sink,
        subscription_quota_routing_max_staleness_secs,
        clock,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_dynamic_view_with_plugin_runtime(
    stores: &Stores,
    oauth_anthropic: &AnthropicOAuthConfig,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    current_generation: u64,
    runtime: &dyn DynamicViewPluginRuntime,
    data_dir: &Path,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_observation_cache: Option<Arc<PromptCacheObservationCache>>,
    prompt_cache_observation_sink: Option<Arc<dyn PromptCacheObservationSinkLike>>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: cc_lb_engine::ClockHandle,
) -> Result<Arc<DynamicView>, RebindError> {
    let upstreams = list_upstreams(stores).await?;
    let all_upstream_ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    subscription_quota_cache
        .hydrate_from_store(stores, &all_upstream_ids)
        .await?;
    let prompt_cache_observation_handles = match prompt_cache_observation_cache {
        Some(cache) => {
            match tokio::time::timeout(
                Duration::from_secs(5),
                cache.hydrate_from_store(
                    stores.prompt_cache_observations.as_ref(),
                    &all_upstream_ids,
                ),
            )
            .await
            {
                Ok(Ok(record_count)) => {
                    tracing::info!(
                        record_count,
                        upstream_count = all_upstream_ids.len(),
                        "hydrated prompt cache observation cache from store"
                    );
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        %error,
                        "failed to hydrate prompt cache observation cache from store; continuing with shared cache"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = 5,
                        "timed out hydrating prompt cache observation cache from store; continuing with shared cache"
                    );
                }
            }
            Some((cache, prompt_cache_observation_sink))
        }
        None => None,
    };
    let upstream_rate_limit_records = stores
        .upstream_rate_limits
        .list_for_upstream_ids(&all_upstream_ids)
        .await?;
    let upstream_rate_limit_cache = Arc::new(RwLock::new(UpstreamRateLimitCache {
        snapshots: group_rate_limit_observations(upstream_rate_limit_records),
        updated_at_unix_secs: unix_secs(clock.now()),
    }));
    let principals = list_principals(stores).await?;
    let (principal_chains, registered_slot_keys) =
        build_principal_chains(stores, runtime, data_dir, &principals).await?;
    // RFC-0001 gap-analysis #1: sweep any runtime slot whose key is
    // no longer referenced by the freshly-built view. Must run AFTER
    // build_principal_chains succeeds so we don't tear down slots the
    // still-active view is dispatching against; already-cloned
    // `Arc<PluginSlotKind>` handles keep the evicted cells alive for the
    // duration of any in-flight call (see `WasmtimeRuntime::evict_slot`
    // docs).
    let evicted_count = runtime.retain_slots(&registered_slot_keys);
    if evicted_count > 0 {
        tracing::info!(evicted_count, "reconcile swept orphan plugin slots",);
    }
    let principal_view = Arc::new(PrincipalView::from_db(&principals, principal_chains));
    let now = unix_secs(clock.now());
    let statuses = apply_upstreams(stores, &upstreams, oauth_anthropic, now).await?;
    let revision_hash = collect_revision_hash(stores).await?;
    let global_router = Arc::new(FirstCandidateRouter);
    let signer_factory = Arc::new(DbCompositeSignerFactory::new(
        upstreams.clone(),
        stores.upstreams.clone(),
        aead,
        lazy_refresher,
        clock.clone(),
    ));
    let snapshot = Arc::new(UpstreamStatusSnapshot {
        entries: statuses,
        applied_at_unix_secs: unix_secs(clock.now()),
        revision_hash,
    });

    let plan_info_by_upstream = load_plan_info_by_upstream(stores).await?;
    let now_unix_millis = i64::try_from(unix_millis(clock.now())).unwrap_or(i64::MAX);
    reconcile_upstream_plan_tiers(stores, now_unix_millis).await;
    let mut builder = DynamicViewBuilder::new(current_generation)
        .signer_factory(signer_factory)
        .global_router(global_router)
        .global_observability_hooks(Vec::new())
        .principal_view(principal_view)
        .upstream_status_snapshot(snapshot)
        .upstream_rate_limit_cache(upstream_rate_limit_cache)
        .subscription_quota_cache(subscription_quota_cache)
        .subscription_quota_routing_max_staleness_secs(
            subscription_quota_routing_max_staleness_secs,
        )
        .plan_info_by_upstream(plan_info_by_upstream)
        .upstream_records(upstreams.clone());
    if let Some((cache, sink)) = prompt_cache_observation_handles {
        builder = builder.prompt_cache_observation_cache(cache);
        if let Some(sink) = sink {
            builder = builder.prompt_cache_observation_sink(sink);
        }
    }
    Ok(builder.build())
}

pub(crate) fn new_prompt_cache_observation_cache(
    config: &PromptCacheShadowConfig,
    clock: cc_lb_engine::ClockHandle,
) -> Arc<PromptCacheObservationCache> {
    let cache = Arc::new(
        PromptCacheObservationCache::new_with_debounce(
            clock,
            config.grace_margin_secs,
            config.refresh_debounce_secs,
        )
        .with_max_entries_per_partition(config.max_live_entries_per_partition),
    );
    cache.spawn_expiry_sweeper();
    cache
}

fn group_rate_limit_observations(
    records: Vec<UpstreamRateLimitObservationRecord>,
) -> HashMap<Uuid, Vec<RateLimitObservation>> {
    let mut snapshots: HashMap<Uuid, Vec<RateLimitObservation>> = HashMap::new();
    for record in records {
        snapshots
            .entry(record.upstream_id)
            .or_default()
            .push(rate_limit_observation(record));
    }
    snapshots
}

fn rate_limit_observation(record: UpstreamRateLimitObservationRecord) -> RateLimitObservation {
    RateLimitObservation {
        kind: rate_limit_kind(record.kind),
        window: record.window,
        limit: record.limit,
        remaining: record.remaining,
        reset: record.reset,
    }
}

fn rate_limit_kind(kind: RateLimitKind) -> cc_lb_domain::RateLimitKind {
    match kind {
        RateLimitKind::Requests => cc_lb_domain::RateLimitKind::Requests,
        RateLimitKind::Tokens => cc_lb_domain::RateLimitKind::Tokens,
        RateLimitKind::InputTokens => cc_lb_domain::RateLimitKind::InputTokens,
        RateLimitKind::OutputTokens => cc_lb_domain::RateLimitKind::OutputTokens,
    }
}

async fn load_plan_info_by_upstream(stores: &Stores) -> StorageResult<HashMap<Uuid, PlanInfo>> {
    let ratio_by_tier = ratio_by_tier(stores.plan_tiers.list_current_plan_tier_ratios().await?);
    let overrides = stores
        .plan_tiers
        .list_current_metadata_tier_overrides()
        .await?;
    let subscription_metadata = stores
        .upstream_subscription_metadata
        .list_upstream_subscription_metadata()
        .await?;
    let organization_metadata = stores
        .organization_metadata
        .list_organization_metadata()
        .await?;
    let organizations = organization_metadata
        .iter()
        .map(|record| (record.organization_uuid.as_str(), record))
        .collect::<HashMap<_, _>>();
    let mut plan_info = HashMap::new();
    for record in subscription_metadata {
        let Some(organization_uuid) = record.organization_uuid.as_deref() else {
            continue;
        };
        let Some(organization) = organizations.get(organization_uuid).copied() else {
            continue;
        };
        let (tier, _source) = resolve_tier(
            &overrides,
            organization.organization_type.as_deref(),
            organization.rate_limit_tier.as_deref(),
            organization.seat_tier.as_deref(),
        );
        let capacity_ratio = match tier {
            Some(tier) => match ratio_by_tier.get(&tier) {
                Some(ratio) => *ratio,
                None => {
                    tracing::error!(
                        upstream_id = %record.upstream_id,
                        tier = tier.as_str(),
                        "plan tier missing a current ratio row in plan_tier_ratio_history_v1; using built-in seed ratio"
                    );
                    metrics::counter!("cc_lb_plan_tier_ratio_missing_total").increment(1);
                    tier.seed_pro_relative_ratio()
                }
            },
            None => {
                tracing::warn!(
                    upstream_id = %record.upstream_id,
                    organization_type = ?organization.organization_type,
                    rate_limit_tier = ?organization.rate_limit_tier,
                    seat_tier = ?organization.seat_tier,
                    "unknown plan tier; routing at Pro ratio"
                );
                metrics::counter!("cc_lb_plan_tier_unknown_total").increment(1);
                PRO_CAPACITY_RATIO
            }
        };
        plan_info.insert(
            record.upstream_id,
            PlanInfo {
                organization_type: organization.organization_type.clone(),
                rate_limit_tier: organization.rate_limit_tier.clone(),
                seat_tier: organization.seat_tier.clone(),
                capacity_ratio,
            },
        );
    }
    Ok(plan_info)
}

fn resolve_tier(
    overrides: &[MetadataTierMappingOverrideRecord],
    organization_type: Option<&str>,
    rate_limit_tier: Option<&str>,
    seat_tier: Option<&str>,
) -> (Option<TierKey>, TierResolutionSource) {
    let norm = |s: Option<&str>| s.unwrap_or_default().to_ascii_lowercase();
    let (ot, rlt, st) = (
        norm(organization_type),
        norm(rate_limit_tier),
        norm(seat_tier),
    );
    for override_record in overrides {
        if norm(override_record.organization_type.as_deref()) == ot
            && norm(override_record.rate_limit_tier.as_deref()) == rlt
            && norm(override_record.seat_tier.as_deref()) == st
            && let Ok(tier) = override_record.tier_key.parse::<TierKey>()
        {
            return (Some(tier), TierResolutionSource::Override);
        }
    }
    match classify_plan_tier(organization_type, rate_limit_tier, seat_tier) {
        PlanTierClassification::Known(tier) => (Some(tier), TierResolutionSource::Builtin),
        PlanTierClassification::Unknown => (None, TierResolutionSource::Unknown),
    }
}

async fn reconcile_upstream_plan_tiers(stores: &Stores, now_unix_millis: i64) {
    if let Err(error) = reconcile_upstream_plan_tiers_inner(stores, now_unix_millis).await {
        tracing::error!(error = %error, "plan tier history reconcile failed");
        metrics::counter!("cc_lb_plan_tier_reconcile_failed_total").increment(1);
    }
}

async fn reconcile_upstream_plan_tiers_inner(
    stores: &Stores,
    now_unix_millis: i64,
) -> StorageResult<()> {
    let ratio_by_tier = ratio_by_tier(stores.plan_tiers.list_current_plan_tier_ratios().await?);
    let overrides = stores
        .plan_tiers
        .list_current_metadata_tier_overrides()
        .await?;
    let current_by_upstream = stores
        .plan_tiers
        .list_current_upstream_plan_tiers()
        .await?
        .into_iter()
        .map(|record| (record.upstream_id, record))
        .collect::<HashMap<_, _>>();
    let subscription_metadata = stores
        .upstream_subscription_metadata
        .list_upstream_subscription_metadata()
        .await?;
    let organization_metadata = stores
        .organization_metadata
        .list_organization_metadata()
        .await?;
    let organizations = organization_metadata
        .iter()
        .map(|record| (record.organization_uuid.as_str(), record))
        .collect::<HashMap<_, _>>();

    for record in subscription_metadata {
        let Some(organization_uuid) = record.organization_uuid.as_deref() else {
            continue;
        };
        let Some(organization) = organizations.get(organization_uuid).copied() else {
            continue;
        };
        let (tier, resolution_source) = resolve_tier(
            &overrides,
            organization.organization_type.as_deref(),
            organization.rate_limit_tier.as_deref(),
            organization.seat_tier.as_deref(),
        );
        let desired = UpstreamPlanTierRecord {
            upstream_id: record.upstream_id,
            organization_uuid: record.organization_uuid.clone(),
            organization_type: organization.organization_type.clone(),
            rate_limit_tier: organization.rate_limit_tier.clone(),
            seat_tier: organization.seat_tier.clone(),
            tier_key: tier.map(|tier| tier.as_str().to_owned()),
            resolution_source,
            resolved_ratio_snapshot: tier.and_then(|tier| ratio_by_tier.get(&tier).copied()),
            observed_at_unix_millis: now_unix_millis,
            effective_from_unix_millis: now_unix_millis,
            effective_to_unix_millis: None,
            provenance: "dynamic_view_reconcile".to_owned(),
            created_at_unix_millis: now_unix_millis,
        };
        if current_by_upstream
            .get(&record.upstream_id)
            .is_none_or(|current| upstream_plan_tier_changed(current, &desired))
        {
            stores
                .plan_tiers
                .append_upstream_plan_tier(&desired)
                .await?;
        }
    }
    Ok(())
}

fn ratio_by_tier(records: Vec<PlanTierRatioRecord>) -> HashMap<TierKey, f64> {
    records
        .into_iter()
        .filter_map(|record| {
            record
                .tier_key
                .parse::<TierKey>()
                .ok()
                .map(|tier| (tier, record.pro_relative_ratio))
        })
        .collect()
}

fn upstream_plan_tier_changed(
    current: &UpstreamPlanTierRecord,
    desired: &UpstreamPlanTierRecord,
) -> bool {
    current.tier_key != desired.tier_key
        || current.resolution_source != desired.resolution_source
        || !normalized_option_eq(
            current.organization_type.as_deref(),
            desired.organization_type.as_deref(),
        )
        || !normalized_option_eq(
            current.rate_limit_tier.as_deref(),
            desired.rate_limit_tier.as_deref(),
        )
        || !normalized_option_eq(current.seat_tier.as_deref(), desired.seat_tier.as_deref())
        || current.organization_uuid != desired.organization_uuid
}

fn normalized_option_eq(left: Option<&str>, right: Option<&str>) -> bool {
    left.unwrap_or_default()
        .eq_ignore_ascii_case(right.unwrap_or_default())
}

async fn list_upstreams(stores: &Stores) -> StorageResult<Vec<UpstreamRecord>> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = stores.upstreams.list(after, 100).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        all.extend(
            page.into_iter()
                .filter(|record| record.deleted_at_unix_secs.is_none()),
        );
    }
    Ok(all)
}

async fn list_principals(stores: &Stores) -> StorageResult<Vec<PrincipalRecord>> {
    let mut all = Vec::new();
    let mut offset = 0;
    loop {
        let page = stores.principals.list(offset, 100, false).await?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        all.extend(page);
    }
    Ok(all)
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin
        && !registry_entry.supported_slots.is_empty()
        && !registry_entry.supported_slots.contains(&slot)
}

async fn build_principal_chains(
    stores: &Stores,
    runtime: &dyn DynamicViewPluginRuntime,
    data_dir: &Path,
    principals: &[PrincipalRecord],
) -> Result<
    (
        HashMap<String, PrincipalRoutingArtifacts>,
        HashSet<PluginRuntimeSlotKey>,
    ),
    RebindError,
> {
    let registry = list_registry_by_id(stores).await?;
    let principal_ids = principals
        .iter()
        .map(|principal| principal.id)
        .collect::<Vec<_>>();
    let slots = [
        PluginSlotKind::Router,
        PluginSlotKind::ObservabilityHook,
        PluginSlotKind::Shape,
    ];
    let mut chain_entries: HashMap<(Uuid, PluginSlotKind), Vec<PluginChainEntry>> = stores
        .plugin_registry
        .list_chains_for_principals(&principal_ids, &slots)
        .await?
        .into_iter()
        .fold(HashMap::new(), |mut entries, entry| {
            entries
                .entry((entry.principal_id, entry.slot))
                .or_insert_with(Vec::new)
                .push(entry);
            entries
        });
    let mut chains = HashMap::new();
    let mut registered_slot_keys: HashSet<PluginRuntimeSlotKey> = HashSet::new();
    for principal in principals {
        let router_entries =
            take_chain_entries(&mut chain_entries, principal.id, PluginSlotKind::Router);
        let hook_entries = take_chain_entries(
            &mut chain_entries,
            principal.id,
            PluginSlotKind::ObservabilityHook,
        );
        let shape_entries =
            take_chain_entries(&mut chain_entries, principal.id, PluginSlotKind::Shape);

        let router = build_router_pipeline(
            stores,
            runtime,
            data_dir,
            principal,
            router_entries,
            &registry,
            &mut registered_slot_keys,
        )
        .await?;

        let mut hooks = Vec::new();
        for entry in hook_entries {
            let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
            })?;
            if registry_entry_unsupported_slot(registry_entry, PluginSlotKind::ObservabilityHook) {
                tracing::warn!(
                    target: "cc_lb_server::drift",
                    principal = %principal.name,
                    plugin = registry_entry.name.as_str(),
                    chain_entry_id = %entry.id,
                    wasm_registry_id = %registry_entry.id,
                    requested_slot = PluginSlotKind::ObservabilityHook.as_str(),
                    supported_slots = ?registry_entry.supported_slots,
                    "skipping observability_hook chain entry: registry entry does not support requested slot",
                );
                continue;
            }
            let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
            let manifest = PluginManifest {
                pure: true,
                name: registry_entry.name.clone(),
                artifact: wasm_path.to_string_lossy().into_owned(),
                wire_version: None,
                config: entry.config,
                metadata: std::collections::BTreeMap::new(),
            };
            let slot_key = PluginRuntimeSlotKey::new(principal.name.clone(), manifest.name.clone());
            registered_slot_keys.insert(slot_key.clone());
            match runtime
                .instantiate_observability_hook(&slot_key, &manifest)
                .await
            {
                Ok(handle) => hooks.push(handle),
                Err(error) => {
                    tracing::error!(
                        principal = %principal.name,
                        plugin = %manifest.name,
                        chain_entry_id = %entry.id,
                        %error,
                        "skipping observability_hook chain entry: instantiation failed",
                    );
                }
            }
        }
        let hooks = if hooks.is_empty() {
            ObservabilityHooksCache::Inherit
        } else {
            ObservabilityHooksCache::Explicit(hooks)
        };

        let dialect = if let Some(entry) = shape_entries.into_iter().next() {
            let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
            })?;
            if registry_entry_unsupported_slot(registry_entry, PluginSlotKind::Shape) {
                tracing::warn!(
                    target: "cc_lb_server::drift",
                    principal = %principal.name,
                    plugin = registry_entry.name.as_str(),
                    chain_entry_id = %entry.id,
                    wasm_registry_id = %registry_entry.id,
                    requested_slot = PluginSlotKind::Shape.as_str(),
                    supported_slots = ?registry_entry.supported_slots,
                    "skipping shape chain entry: registry entry does not support requested slot",
                );
                DialectCache::Inherit
            } else {
                let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
                let manifest = PluginManifest {
                    pure: true,
                    name: registry_entry.name.clone(),
                    artifact: wasm_path.to_string_lossy().into_owned(),
                    wire_version: None,
                    config: entry.config,
                    metadata: std::collections::BTreeMap::new(),
                };
                let slot_key =
                    PluginRuntimeSlotKey::new(principal.name.clone(), manifest.name.clone());
                registered_slot_keys.insert(slot_key.clone());
                match runtime.instantiate_shape(&slot_key, &manifest).await {
                    Ok(handle) => DialectCache::Explicit(ShapePluginCache { dialect: handle }),
                    Err(error) => {
                        tracing::error!(
                            principal = %principal.name,
                            plugin = %manifest.name,
                            chain_entry_id = %entry.id,
                            %error,
                            "skipping shape chain entry: instantiation failed; principal falls back to route dialect",
                        );
                        DialectCache::Inherit
                    }
                }
            }
        } else {
            DialectCache::Inherit
        };

        chains.insert(principal.name.clone(), (router, hooks, dialect));
    }
    Ok((chains, registered_slot_keys))
}

fn take_chain_entries(
    entries: &mut HashMap<(Uuid, PluginSlotKind), Vec<PluginChainEntry>>,
    principal_id: Uuid,
    slot: PluginSlotKind,
) -> Vec<PluginChainEntry> {
    let mut chain = entries.remove(&(principal_id, slot)).unwrap_or_default();
    chain.sort_by_key(|entry| (entry.order, entry.id));
    chain
}

async fn list_registry_by_id(
    stores: &Stores,
) -> StorageResult<HashMap<uuid::Uuid, cc_lb_storage_api::WasmRegistryEntry>> {
    let mut all = HashMap::new();
    let mut after = None;
    loop {
        let page = stores.plugin_registry.list_registry(after, 100).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|entry| entry.id);
        all.extend(page.into_iter().map(|entry| (entry.id, entry)));
    }
    Ok(all)
}

pub(crate) async fn materialize_wasm(
    stores: &Stores,
    data_dir: &Path,
    sha256: [u8; 32],
) -> Result<PathBuf, RebindError> {
    let cache_path = data_dir
        .join("plugins")
        .join("wasm")
        .join("cache")
        .join(format!("{}.wasm", hex_sha256(sha256)));
    if cache_path.exists() {
        return Ok(cache_path);
    }
    let bytes = stores.plugin_registry.get_blob_bytes(sha256).await?;
    ensure_wasm_cached(data_dir, sha256, || Ok(bytes)).map_err(RebindError::Io)
}

async fn manifest_for_chain_entry(
    stores: &Stores,
    data_dir: &Path,
    registry: &HashMap<Uuid, cc_lb_storage_api::WasmRegistryEntry>,
    entry: &cc_lb_storage_api::PluginChainEntry,
) -> Result<PluginManifest, RebindError> {
    let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
    })?;
    let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
    Ok(PluginManifest {
        pure: true,
        name: registry_entry.name.clone(),
        artifact: wasm_path.to_string_lossy().into_owned(),
        wire_version: None,
        config: entry.config.clone(),
        metadata: std::collections::BTreeMap::new(),
    })
}

async fn build_router_pipeline(
    stores: &Stores,
    runtime: &dyn DynamicViewPluginRuntime,
    data_dir: &Path,
    principal: &PrincipalRecord,
    mut router_entries: Vec<cc_lb_storage_api::PluginChainEntry>,
    registry: &HashMap<Uuid, cc_lb_storage_api::WasmRegistryEntry>,
    registered_slot_keys: &mut HashSet<PluginRuntimeSlotKey>,
) -> Result<Option<Arc<RouterPipelineCache>>, RebindError> {
    if router_entries.is_empty() {
        return Ok(None);
    }
    let user_router_filter_count = router_entries
        .iter()
        .filter(|entry| entry.wasm_registry_id != BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .count();
    if user_router_filter_count > MAX_ROUTER_CHAIN_DEPTH {
        return Ok(Some(Arc::new(RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: principal.router_terminal_strategy.clone(),
            instantiation_error: Some(Arc::<str>::from(format!(
                "router user filter depth {} exceeds maximum {}",
                user_router_filter_count, MAX_ROUTER_CHAIN_DEPTH
            ))),
        })));
    }

    router_entries.sort_by_key(|entry| entry.order);
    let mut filters: Vec<Arc<dyn FilterPlugin>> = Vec::with_capacity(router_entries.len());
    for entry in router_entries {
        let Some(registry_entry) = registry.get(&entry.wasm_registry_id) else {
            let error = io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found");
            tracing::error!(
                principal = %principal.name,
                chain_entry_id = %entry.id,
                %error,
                "router chain entry failed to materialize; principal fails closed",
            );
            return Ok(Some(Arc::new(RouterPipelineCache {
                user_filters: Vec::new(),
                terminal: principal.router_terminal_strategy.clone(),
                instantiation_error: Some(Arc::<str>::from(format!(
                    "router pipeline instantiation failed: {error}"
                ))),
            })));
        };
        if registry_entry.is_builtin {
            if registry_entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
                filters.push(Arc::new(SubscriptionPreferenceFilter::new()));
            }
            continue;
        }
        let manifest = match manifest_for_chain_entry(stores, data_dir, registry, &entry).await {
            Ok(manifest) => manifest,
            Err(error) => {
                tracing::error!(
                    principal = %principal.name,
                    chain_entry_id = %entry.id,
                    %error,
                    "router chain entry failed to materialize; principal fails closed",
                );
                return Ok(Some(Arc::new(RouterPipelineCache {
                    user_filters: Vec::new(),
                    terminal: principal.router_terminal_strategy.clone(),
                    instantiation_error: Some(Arc::<str>::from(format!(
                        "router pipeline instantiation failed: {error}"
                    ))),
                })));
            }
        };
        let slot_key = PluginRuntimeSlotKey::new(principal.name.clone(), manifest.name.clone());
        registered_slot_keys.insert(slot_key.clone());
        match runtime
            .instantiate_filter(&slot_key, &manifest, entry.id)
            .await
        {
            Ok(handle) => {
                filters.push(handle);
            }
            Err(error) => {
                tracing::error!(
                    principal = %principal.name,
                    plugin = %manifest.name,
                    chain_entry_id = %entry.id,
                    %error,
                    "router filter chain entry instantiation failed; principal fails closed",
                );
                return Ok(Some(Arc::new(RouterPipelineCache {
                    user_filters: Vec::new(),
                    terminal: principal.router_terminal_strategy.clone(),
                    instantiation_error: Some(Arc::<str>::from(format!(
                        "router pipeline instantiation failed: {error}"
                    ))),
                })));
            }
        }
    }

    Ok(Some(Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal: principal.router_terminal_strategy.clone(),
        instantiation_error: None,
    })))
}

async fn apply_upstreams(
    stores: &Stores,
    upstreams: &[UpstreamRecord],
    _oauth_anthropic: &AnthropicOAuthConfig,
    now: u64,
) -> StorageResult<HashMap<String, UpstreamStatusEntry>> {
    let mut statuses = HashMap::new();
    for upstream in upstreams {
        if !upstream.enabled {
            stores
                .upstreams
                .set_last_apply_error(upstream.id, None)
                .await?;
            statuses.insert(
                upstream.name.clone(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Disabled,
                    last_apply_error: None,
                    last_apply_at_unix_secs: now,
                },
            );
            continue;
        }

        if let Err(message) = validate_upstream(upstream) {
            stores
                .upstreams
                .set_last_apply_error(upstream.id, Some(message.clone()))
                .await?;
            statuses.insert(
                upstream.name.clone(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Error,
                    last_apply_error: Some(message),
                    last_apply_at_unix_secs: now,
                },
            );
            continue;
        }

        stores
            .upstreams
            .set_last_apply_error(upstream.id, None)
            .await?;
        statuses.insert(
            upstream.name.clone(),
            UpstreamStatusEntry {
                status: ApplyStatus::Active,
                last_apply_error: None,
                last_apply_at_unix_secs: now,
            },
        );
    }
    Ok(statuses)
}

fn validate_upstream(upstream: &UpstreamRecord) -> Result<(), String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey if upstream.api_key_ciphertext.is_none() => {
            return Err("anthropic api-key upstream missing api_key_ciphertext".to_owned());
        }
        UpstreamKind::AnthropicOauth => {
            let ciphertext = upstream
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| "anthropic oauth upstream missing oauth credentials".to_owned())?
                .ciphertext();
            if ciphertext.len() < 29 {
                return Err("anthropic oauth credentials ciphertext is corrupt".to_owned());
            }
        }
        _ => {}
    }
    Ok(())
}

struct FirstCandidateRouter;

impl RouterPlugin for FirstCandidateRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        let candidate = candidates.first().ok_or_else(|| RouteError::NoRoute {
            reason: "no eligible upstream candidates for principal".to_owned(),
        })?;
        let base_url = candidate
            .base_url
            .as_deref()
            .map(Url::parse)
            .transpose()
            .map_err(|source| RouteError::NoRoute {
                reason: format!("candidate upstream has invalid base_url: {source}"),
            })?;
        tracing::debug!(
            upstream = candidate.name.as_str(),
            upstream_id = %candidate.upstream_id,
            "first candidate route selected",
        );
        Ok(RouteDecision {
            upstream_id: Some(candidate.upstream_id),
            upstream: Upstream::AnthropicDirect {
                base_url: base_url.clone(),
            },
            dialect: Arc::new(AnthropicDirectDialect::with_base_url(base_url)),
        })
    }
}

#[derive(Clone)]
struct DbCompositeSignerFactory {
    upstreams: Vec<UpstreamRecord>,
    upstream_store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    downstream_api_key: Option<String>,
    router_chosen_upstream_name: Option<String>,
    clock: cc_lb_engine::ClockHandle,
}

impl DbCompositeSignerFactory {
    fn new(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        clock: cc_lb_engine::ClockHandle,
    ) -> Self {
        Self {
            upstreams,
            upstream_store,
            aead,
            lazy_refresher,
            downstream_api_key: None,
            router_chosen_upstream_name: None,
            clock,
        }
    }

    fn with_router_choice_state(
        &self,
        api_key: String,
        router_chosen_upstream_name: String,
    ) -> Self {
        Self {
            upstreams: self.upstreams.clone(),
            upstream_store: self.upstream_store.clone(),
            aead: self.aead.clone(),
            lazy_refresher: self.lazy_refresher.clone(),
            downstream_api_key: Some(api_key),
            router_chosen_upstream_name: Some(router_chosen_upstream_name),
            clock: self.clock.clone(),
        }
    }
}

impl cc_lb_engine::ApiKeyAwareSignerFactory for DbCompositeSignerFactory {
    fn with_router_choice(
        &self,
        api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(self.with_router_choice_state(api_key, router_chosen_upstream_name))
    }
}

#[async_trait]
impl SignerFactory for DbCompositeSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let record = self
            .upstreams
            .iter()
            .find(|candidate| self.record_matches(candidate, upstream))
            .ok_or_else(|| SignerError::MissingCredentials {
                reason: "upstream not present in dynamic signer view".to_owned(),
            })?;
        match record.kind {
            UpstreamKind::AnthropicApiKey => {
                let api_key = self.downstream_api_key.clone().ok_or_else(|| {
                    SignerError::MissingCredentials {
                        reason: "dynamic api-key signer requires downstream api key until Task 22 storage signer lands".to_owned(),
                    }
                })?;
                cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::new(api_key)
                    .build(upstream)
                    .await
            }
            UpstreamKind::AnthropicOauth => {
                let factory =
                    cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory::for_upstream_name(
                        self.upstream_store.clone(),
                        self.aead.clone(),
                        record.name.clone(),
                        self.clock.clone(),
                    );
                if let Some(handle) = &self.lazy_refresher {
                    cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactoryWithLazyRefresh::new(
                        factory,
                        handle.clone(),
                        record.id,
                    )
                    .build(upstream)
                    .await
                } else {
                    factory.build(upstream).await
                }
            }
        }
    }
}

impl DbCompositeSignerFactory {
    fn record_matches(&self, candidate: &UpstreamRecord, upstream: &Upstream) -> bool {
        if !upstream_matches(candidate, upstream) {
            return false;
        }

        self.router_chosen_upstream_name
            .as_deref()
            .is_some_and(|router_choice| candidate.name == router_choice)
    }
}

fn upstream_matches(record: &UpstreamRecord, upstream: &Upstream) -> bool {
    matches!(
        (&record.kind, upstream),
        (
            UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth,
            Upstream::AnthropicDirect { .. }
        )
    )
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
#[allow(non_snake_case)]
mod t2__tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use axum::body::Bytes;
    use axum::http::{Method, StatusCode};
    use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_config::{DownstreamAuthMode, NoneModeConfig, NoneModeUpstreamKind};
    use cc_lb_engine::api_keys::builtin_authn::BuiltinAuthn;
    use cc_lb_engine::{
        Body, DispatchError, DynamicViewHolder, Lifecycle, LifecycleConfig, UpstreamDispatch,
    };
    use cc_lb_upstream::SignedRequest;
    use http::{Request, Response};
    use http_body_util::{BodyExt, Full};

    use cc_lb_domain::{TtlClass as PluginTtlClass, TtlClass as StorageTtlClass};
    use cc_lb_engine::lifecycle::PromptCacheObservationCacheLike;
    use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    use cc_lb_storage_api::{
        OrganizationMetadataRecord, PlanTierRatioRecord, PromptCacheObservationRecord,
        TierResolutionSource, UpstreamCreate, UpstreamSubscriptionMetadataRecord,
    };
    use cc_lb_testkit::{InMemoryStorage as Storage, fixed_clock};

    use super::*;
    use crate::prompt_cache_observation_cache::HASH_SCHEMA_VERSION;
    use crate::prompt_cache_observation_sink::{
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
    };

    const MODEL: &str = "claude-sonnet-4-5-20250929";

    /// Stand-in for a user-uploaded router filter so ordering and dedupe can be
    /// observed without a wasm runtime.
    fn plugin_ids(filters: &[Arc<dyn FilterPlugin>]) -> Vec<Uuid> {
        filters.iter().map(|filter| filter.plugin_id()).collect()
    }

    #[derive(Default)]
    struct RecordingPluginRuntime {
        instantiations: Mutex<Vec<(&'static str, PluginRuntimeSlotKey)>>,
        retained: Mutex<Vec<HashSet<PluginRuntimeSlotKey>>>,
    }

    impl RecordingPluginRuntime {
        fn record_unexpected(
            &self,
            kind: &'static str,
            slot_key: &PluginRuntimeSlotKey,
        ) -> RebindError {
            self.instantiations
                .lock()
                .expect("plugin runtime instantiation lock")
                .push((kind, slot_key.clone()));
            RebindError::Io(io::Error::other(format!(
                "unexpected {kind} plugin instantiation"
            )))
        }

        fn assert_no_instantiations(&self) {
            assert!(
                self.instantiations
                    .lock()
                    .expect("plugin runtime instantiation lock")
                    .is_empty(),
                "built-in/default composition must not instantiate a user plugin"
            );
        }
    }

    #[async_trait]
    impl DynamicViewPluginRuntime for RecordingPluginRuntime {
        async fn instantiate_filter(
            &self,
            slot_key: &PluginRuntimeSlotKey,
            _manifest: &PluginManifest,
            _chain_entry_id: Uuid,
        ) -> Result<Arc<dyn FilterPlugin>, RebindError> {
            Err(self.record_unexpected("filter", slot_key))
        }

        async fn instantiate_shape(
            &self,
            slot_key: &PluginRuntimeSlotKey,
            _manifest: &PluginManifest,
        ) -> Result<Arc<dyn cc_lb_upstream::UpstreamDialect>, RebindError> {
            Err(self.record_unexpected("shape", slot_key))
        }

        async fn instantiate_observability_hook(
            &self,
            slot_key: &PluginRuntimeSlotKey,
            _manifest: &PluginManifest,
        ) -> Result<Arc<dyn cc_lb_observability::ObservabilityHook>, RebindError> {
            Err(self.record_unexpected("observability-hook", slot_key))
        }

        fn retain_slots(&self, slot_keys: &HashSet<PluginRuntimeSlotKey>) -> usize {
            self.retained
                .lock()
                .expect("plugin runtime retain lock")
                .push(slot_keys.clone());
            0
        }
    }

    async fn router_pipeline_for_seeded_principal(
        runtime: &RecordingPluginRuntime,
        include_router_entries: bool,
    ) -> Option<Arc<RouterPipelineCache>> {
        let storage = storage_fixture();
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let principal = PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::PrincipalCreate {
                name: "pipeline-wiring".to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await
        .expect("principal created");
        let router_entries = if include_router_entries {
            PluginRegistryStore::list_chain_for_principal(
                storage.as_ref(),
                principal.id,
                cc_lb_storage_api::PluginSlotKind::Router,
            )
            .await
            .expect("seeded router chain")
        } else {
            Vec::new()
        };
        let registry = PluginRegistryStore::list_registry(storage.as_ref(), None, 100)
            .await
            .expect("plugin registry")
            .into_iter()
            .map(|entry| (entry.id, entry))
            .collect();

        let mut slot_keys = HashSet::new();
        build_router_pipeline(
            &stores,
            runtime,
            Path::new("."),
            &principal,
            router_entries,
            &registry,
            &mut slot_keys,
        )
        .await
        .expect("pipeline builds")
    }

    #[tokio::test]
    async fn t2__seeded_subscription_preference_chain_yields_the_builtin_filter() {
        let runtime = RecordingPluginRuntime::default();
        let pipeline = router_pipeline_for_seeded_principal(&runtime, true)
            .await
            .expect("seeded entry produces a router pipeline");

        assert!(pipeline.instantiation_error.is_none());
        assert_eq!(
            plugin_ids(&pipeline.user_filters),
            vec![BUILTIN_SUBSCRIPTION_PREFERENCE_ID]
        );
        runtime.assert_no_instantiations();
    }

    #[tokio::test]
    async fn t2__empty_router_chain_yields_no_pipeline() {
        let runtime = RecordingPluginRuntime::default();
        assert!(
            router_pipeline_for_seeded_principal(&runtime, false)
                .await
                .is_none(),
            "an absent chain means no router filter pipeline"
        );
        runtime.assert_no_instantiations();
    }

    #[derive(Clone)]
    struct FakePromptCacheObservationStore {
        records: Arc<Vec<PromptCacheObservationRecord>>,
        list_delay: Option<Duration>,
        upserts: Arc<tokio::sync::Mutex<Vec<PromptCacheObservationRecord>>>,
        upserted: Arc<tokio::sync::Notify>,
    }

    impl FakePromptCacheObservationStore {
        fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
            Self {
                records: Arc::new(records),
                list_delay: None,
                upserts: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                upserted: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn sleeping(records: Vec<PromptCacheObservationRecord>, delay: Duration) -> Self {
            Self {
                records: Arc::new(records),
                list_delay: Some(delay),
                upserts: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                upserted: Arc::new(tokio::sync::Notify::new()),
            }
        }

        async fn list_count(&self) -> usize {
            self.upserts.lock().await.len()
        }

        async fn wait_for_upserts(&self, expected: usize) {
            while self.list_count().await < expected {
                self.upserted.notified().await;
            }
        }

        async fn list_all(&self) -> Vec<PromptCacheObservationRecord> {
            self.upserts.lock().await.clone()
        }
    }

    #[async_trait]
    impl PromptCacheObservationStore for FakePromptCacheObservationStore {
        async fn list_active_for_upstream(
            &self,
            upstream_id: Uuid,
            not_expired_at_unix_secs: u64,
        ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
            if let Some(delay) = self.list_delay {
                tokio::time::sleep(delay).await;
            }
            Ok(self
                .records
                .iter()
                .filter(|record| {
                    record.upstream_id == upstream_id
                        && record.expires_at_unix_secs > not_expired_at_unix_secs
                })
                .cloned()
                .collect())
        }

        async fn upsert_observation(
            &self,
            record: &PromptCacheObservationRecord,
        ) -> StorageResult<()> {
            self.upserts.lock().await.push(record.clone());
            self.upserted.notify_one();
            Ok(())
        }
    }

    fn storage_fixture() -> Arc<Storage> {
        Arc::new(Storage::with_clock(fixed_clock(1_700_000_000)))
    }

    fn stores(
        storage: Arc<Storage>,
        prompt_cache_observations: Arc<dyn PromptCacheObservationStore>,
    ) -> Stores {
        Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: storage.clone(),
            organization_metadata: storage.clone(),
            plan_tiers: storage.clone(),
            prompt_cache_observations,
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage),
        }
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

    async fn build_view(
        stores: &Stores,
        runtime: &RecordingPluginRuntime,
        data_dir: &Path,
    ) -> Arc<DynamicView> {
        let config = cc_lb_config::Config::default();
        build_view_with_config(stores, runtime, data_dir, config).await
    }

    async fn build_view_with_config(
        stores: &Stores,
        runtime: &RecordingPluginRuntime,
        data_dir: &Path,
        config: cc_lb_config::Config,
    ) -> Arc<DynamicView> {
        let clock = fixed_clock(1_700_000_000);
        let cache = new_prompt_cache_observation_cache(&config.prompt_cache_shadow, clock.clone());
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
        let prompt_cache_observation_cache = Some(cache);
        let prompt_cache_observation_sink = Some(sink);
        build_dynamic_view_with_plugin_runtime(
            stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            0,
            runtime,
            data_dir,
            Arc::new(SubscriptionQuotaCache::new()),
            prompt_cache_observation_cache,
            prompt_cache_observation_sink,
            1800,
            clock,
        )
        .await
        .expect("dynamic view builds")
    }

    fn prompt_record(
        upstream_id: Uuid,
        prefix_hash: &str,
        last_observed_at_unix_secs: u64,
    ) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id,
            canonical_model_id: MODEL.to_owned(),
            v3_prefix_key: prefix_hash.to_owned(),
            ttl_class: StorageTtlClass::Ephemeral5m,
            expires_at_unix_secs: 4_100_000_000,
            last_observed_at_unix_secs,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        }
    }

    #[tokio::test]
    async fn t2__includes_prompt_cache() {
        let storage = storage_fixture();
        let upstream = create_upstream(&storage, "prompt-cache-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new(vec![
            prompt_record(upstream.id, "hash-a", 1_700_000_001),
            prompt_record(upstream.id, "hash-b", 1_700_000_002),
            prompt_record(upstream.id, "hash-c", 1_700_000_003),
        ]));
        let stores = stores(storage, prompt_store);
        let runtime = RecordingPluginRuntime::default();

        let dynamic_view = build_view(&stores, &runtime, Path::new(".")).await;
        let clock = fixed_clock(1_700_000_000);

        let snapshot = dynamic_view
            .prompt_cache_observation_cache_opt()
            .expect("prompt cache enabled")
            .snapshot_for_upstream(
                upstream.id,
                MODEL,
                &[
                    ("hash-a".to_owned(), PluginTtlClass::Ephemeral5m),
                    ("hash-b".to_owned(), PluginTtlClass::Ephemeral5m),
                    ("hash-c".to_owned(), PluginTtlClass::Ephemeral5m),
                ],
                unix_secs(clock.now()),
            );
        let prefix_hashes = snapshot
            .into_iter()
            .map(|entry| entry.prefix_hash)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            prefix_hashes,
            BTreeSet::from_iter(["hash-a", "hash-b", "hash-c"].map(str::to_owned))
        );

        assert!(
            dynamic_view.prompt_cache_observation_sink_opt().is_some(),
            "prompt cache observation sink must be wired into DynamicView when prompt_cache_shadow is enabled"
        );
    }

    #[tokio::test]
    async fn t2__observation_sink_routes_records_to_store() {
        let storage = storage_fixture();
        let upstream = create_upstream(&storage, "sink-wiring-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new(Vec::new()));
        let stores = stores(storage, prompt_store.clone());
        let runtime = RecordingPluginRuntime::default();

        let dynamic_view = build_view(&stores, &runtime, Path::new(".")).await;
        let sink = dynamic_view
            .prompt_cache_observation_sink_opt()
            .expect("sink wired when prompt_cache_shadow enabled")
            .clone();
        let record = PromptCacheObservationRecord {
            upstream_id: upstream.id,
            canonical_model_id: MODEL.to_owned(),
            v3_prefix_key: "sink-wiring-prefix".to_owned(),
            ttl_class: cc_lb_domain::TtlClass::Ephemeral5m,
            expires_at_unix_secs: 4_100_000_300,
            last_observed_at_unix_secs: 1_700_000_000,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
        };
        sink.enqueue(record.clone())
            .expect("enqueue succeeds while writer is alive");
        prompt_store.wait_for_upserts(1).await;
        let stored = prompt_store.list_all().await;
        assert_eq!(
            stored.len(),
            1,
            "observation enqueued through DynamicView sink must reach the production store"
        );
        assert_eq!(stored[0].v3_prefix_key, "sink-wiring-prefix");
        assert_eq!(stored[0].upstream_id, upstream.id);
    }

    #[tokio::test]
    async fn t2__shared_prompt_cache_observation_cache_sees_live_upsert_after_rebind() {
        // Given: two DynamicView builds share the process-level observation cache handle.
        let storage = storage_fixture();
        let upstream = create_upstream(&storage, "shared-cache-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new(Vec::new()));
        let stores = stores(storage, prompt_store);
        let runtime = RecordingPluginRuntime::default();
        let config = cc_lb_config::Config::default();
        let clock = fixed_clock(1_700_000_000);
        let shared_cache =
            new_prompt_cache_observation_cache(&config.prompt_cache_shadow, clock.clone());
        let shared_cache_trait: Arc<dyn PromptCacheObservationCacheLike> = shared_cache.clone();

        let view_a = build_dynamic_view_with_plugin_runtime(
            &stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            0,
            &runtime,
            Path::new("."),
            Arc::new(SubscriptionQuotaCache::new()),
            Some(shared_cache.clone()),
            None,
            1800,
            clock.clone(),
        )
        .await
        .expect("first dynamic view builds");
        let view_b = build_dynamic_view_with_plugin_runtime(
            &stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            view_a.generation,
            &runtime,
            Path::new("."),
            Arc::new(SubscriptionQuotaCache::new()),
            Some(shared_cache.clone()),
            None,
            1800,
            clock.clone(),
        )
        .await
        .expect("second dynamic view builds");

        // When: the long-lived subscriber upserts into the shared cache after rebind.
        shared_cache.upsert_observation(
            crate::prompt_cache_observation_cache::PromptCacheObservationUpsert {
                upstream_id: upstream.id,
                canonical_model: MODEL.to_owned(),
                prefix_hash: "live-prefix".to_owned(),
                ttl_class: PluginTtlClass::Ephemeral5m,
                expires_at_unix_secs: 4_100_000_000,
                last_observed_at_unix_secs: 1_700_000_010,
                prefix_content_block_index: 0,
                estimated_prefix_tokens: 0,
                token_estimate_source: V3_TOKEN_ESTIMATE_SOURCE.to_owned(),
            },
        );

        // Then: the rebuilt DynamicView routes against the same cache and sees the live write.
        assert!(Arc::ptr_eq(
            view_a
                .prompt_cache_observation_cache_opt()
                .expect("first view has shared cache"),
            &shared_cache_trait,
        ));
        let view_b_cache = view_b
            .prompt_cache_observation_cache_opt()
            .expect("second view has shared cache");
        assert!(Arc::ptr_eq(view_b_cache, &shared_cache_trait));
        let snapshot = view_b_cache.snapshot_for_upstream(
            upstream.id,
            MODEL,
            &[("live-prefix".to_owned(), PluginTtlClass::Ephemeral5m)],
            unix_secs(clock.now()),
        );
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].prefix_hash, "live-prefix");
    }

    #[tokio::test(start_paused = true)]
    async fn t2__hydrate_timeout_logs_warn_and_continues() {
        let storage = storage_fixture();
        let upstream = create_upstream(&storage, "timeout-upstream").await;
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::sleeping(
                vec![prompt_record(upstream.id, "hash-a", 1_700_000_001)],
                Duration::from_secs(30),
            )),
        );
        let runtime = RecordingPluginRuntime::default();

        let dynamic_view = build_view(&stores, &runtime, Path::new(".")).await;
        let clock = fixed_clock(1_700_000_000);
        let snapshot = dynamic_view
            .prompt_cache_observation_cache_opt()
            .expect("prompt cache enabled")
            .snapshot_for_upstream(
                upstream.id,
                MODEL,
                &[("hash-a".to_owned(), PluginTtlClass::Ephemeral5m)],
                unix_secs(clock.now()),
            );
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn t2__default_config_constructs_cache() {
        let storage = storage_fixture();
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let runtime = RecordingPluginRuntime::default();

        let dynamic_view = build_view_with_config(
            &stores,
            &runtime,
            Path::new("."),
            cc_lb_config::Config::default(),
        )
        .await;

        assert!(dynamic_view.prompt_cache_observation_cache_opt().is_some());
        assert!(
            dynamic_view.prompt_cache_observation_sink_opt().is_some(),
            "prompt cache observation sink must always be wired"
        );
    }

    #[tokio::test]
    async fn t2__tunables_propagate_to_cache() {
        let storage = storage_fixture();
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let runtime = RecordingPluginRuntime::default();
        let mut config = cc_lb_config::Config::default();
        config.prompt_cache_shadow.grace_margin_secs = 99;
        config.prompt_cache_shadow.refresh_debounce_secs = 123;

        let dynamic_view = build_view_with_config(&stores, &runtime, Path::new("."), config).await;

        let cache = dynamic_view
            .prompt_cache_observation_cache_opt()
            .expect("prompt cache enabled");
        assert_eq!(cache.grace_margin_secs(), 99);
    }

    struct ChosenUpstreamRouter {
        target_id: Uuid,
        reported_base_url: Option<Url>,
    }

    impl RouterPlugin for ChosenUpstreamRouter {
        fn route(
            &self,
            _ctx: &cc_lb_routing::RoutingContext,
            _principal: &Principal,
            candidates: &[UpstreamCandidate],
        ) -> Result<RouteDecision, RouteError> {
            if !candidates
                .iter()
                .any(|candidate| candidate.upstream_id == self.target_id)
            {
                return Err(RouteError::NoRoute {
                    reason: format!("target upstream {} is not eligible", self.target_id),
                });
            }
            Ok(RouteDecision {
                upstream_id: Some(self.target_id),
                upstream: Upstream::AnthropicDirect {
                    base_url: self.reported_base_url.clone(),
                },
                dialect: Arc::new(AnthropicDirectDialect::with_base_url(
                    self.reported_base_url.clone(),
                )),
            })
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ObservedDispatch {
        url: Url,
        authorization: Option<String>,
    }

    struct RecordingDispatcher {
        captured: Arc<Mutex<Vec<ObservedDispatch>>>,
    }

    #[async_trait]
    impl UpstreamDispatch for RecordingDispatcher {
        async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            self.captured
                .lock()
                .expect("captured dispatch lock")
                .push(ObservedDispatch {
                    url: request.url().clone(),
                    authorization: request
                        .headers()
                        .get(http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned),
                });
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::new(Full::from(Bytes::from_static(
                    br#"{"type":"message","content":[]}"#,
                ))))
                .map_err(|error| DispatchError::RequestBuild {
                    reason: error.to_string(),
                })
        }
    }

    fn message_request() -> Request<Bytes> {
        Request::builder()
            .method(Method::POST)
            .uri("/v1/messages")
            .header("x-api-key", "sk-ant-downstream")
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .body(Bytes::from_static(
                br#"{"model":"claude-3-5-sonnet-20241022","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .expect("request builds")
    }

    async fn build_dispatch_view(
        storage: Arc<Storage>,
        aead: Arc<AeadService>,
        clock: cc_lb_engine::ClockHandle,
        target_id: Uuid,
        reported_base_url: Option<Url>,
    ) -> Arc<DynamicView> {
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let runtime = RecordingPluginRuntime::default();
        let view = build_dynamic_view_with_plugin_runtime(
            &stores,
            &AnthropicOAuthConfig::default(),
            aead,
            None,
            0,
            &runtime,
            Path::new("."),
            Arc::new(SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            clock,
        )
        .await
        .expect("dynamic view builds");
        runtime.assert_no_instantiations();
        DynamicViewBuilder::from_view(&view)
            .global_router(Arc::new(ChosenUpstreamRouter {
                target_id,
                reported_base_url,
            }))
            .build()
    }

    #[tokio::test]
    async fn t2__router_choice_dispatches_to_matching_oauth_upstream_not_first_upstream() {
        let clock = fixed_clock(1_800_000_000);
        let storage = Arc::new(Storage::with_clock(clock.clone()));
        let aead = Arc::new(AeadService::from_master_key([33; 32]));
        let missing = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "missing-before-target".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse("http://missing.invalid").expect("missing base URL")),
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("missing upstream created");
        let target = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "oauth-target".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(Url::parse("http://oauth-target.invalid").expect("target base URL")),
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("target upstream created");
        let tokens = EncryptedOAuthTokens::encrypt(
            &aead,
            &OAuthTokenBundle {
                access_token: "sk-ant-oat01-oauth-target-access-token".to_owned(),
                refresh_token: "sk-ant-ort01-oauth-target-refresh-token".to_owned(),
                expires_at_unix_secs: 1_800_003_600,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["messages".to_owned()],
            },
            target.id.as_bytes(),
        )
        .expect("tokens encrypt");
        UpstreamStore::store_oauth_tokens(storage.as_ref(), target.id, target.revision, tokens)
            .await
            .expect("tokens stored");
        PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::PrincipalCreate {
                name: "oauth-principal".to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: vec![target.id],
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1_800_000_000,
        )
        .await
        .expect("principal created");

        let view = build_dispatch_view(storage, aead, clock.clone(), target.id, None).await;
        let captured = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Lifecycle::new_with_dynamic_view(
            Arc::new(BuiltinAuthn::new(
                DownstreamAuthMode::None,
                Some(NoneModeConfig {
                    principal_id: "oauth-principal".to_owned(),
                    upstream_kind: NoneModeUpstreamKind::AnthropicOAuth,
                }),
                None,
                clock.clone(),
            )),
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(RecordingDispatcher {
                captured: captured.clone(),
            }),
            LifecycleConfig::default(),
            clock,
        );

        let response = lifecycle
            .handle(message_request())
            .await
            .expect("lifecycle response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        let captured = captured.lock().expect("captured dispatch lock");
        assert_eq!(captured.len(), 1, "expected one upstream dispatch");
        assert_eq!(captured[0].url.host_str(), Some("oauth-target.invalid"));
        assert_eq!(
            captured[0].authorization.as_deref(),
            Some("Bearer sk-ant-oat01-oauth-target-access-token")
        );
        assert_ne!(missing.id, target.id);
    }

    #[tokio::test]
    async fn t2__dispatch_uses_resolved_upstream_base_url_not_reported_route_dialect() {
        let clock = fixed_clock(1_800_000_000);
        let storage = Arc::new(Storage::with_clock(clock.clone()));
        let aead = Arc::new(AeadService::from_master_key([33; 32]));
        let _primary = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "aaa-primary".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: Some(vec![1, 2, 3]),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("primary upstream created");
        let target = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "bbb-target".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: Some(Url::parse("http://target.invalid").expect("target base URL")),
                api_key_ciphertext: Some(vec![1, 2, 3]),
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("target upstream created");
        PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::PrincipalCreate {
                name: "test-principal".to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: vec![target.id],
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1_800_000_000,
        )
        .await
        .expect("principal created");

        let view = build_dispatch_view(
            storage,
            aead,
            clock.clone(),
            target.id,
            Some(Url::parse("https://api.anthropic.com").expect("reported base URL")),
        )
        .await;
        let captured = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Lifecycle::new_with_dynamic_view(
            Arc::new(BuiltinAuthn::new(
                DownstreamAuthMode::None,
                Some(NoneModeConfig {
                    principal_id: "test-principal".to_owned(),
                    upstream_kind: NoneModeUpstreamKind::AnthropicKey,
                }),
                None,
                clock.clone(),
            )),
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(RecordingDispatcher {
                captured: captured.clone(),
            }),
            LifecycleConfig::default(),
            clock,
        );

        let response = lifecycle
            .handle(message_request())
            .await
            .expect("lifecycle response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let captured = captured.lock().expect("captured dispatch lock");
        assert_eq!(
            captured.len(),
            1,
            "expected exactly one upstream dispatch; got {captured:?} status={status} body={body:?}"
        );
        assert_eq!(captured[0].url.host_str(), Some("target.invalid"));
        assert_eq!(captured[0].url.path(), "/v1/messages");
    }

    fn rebind_stores(storage: Arc<Storage>) -> Stores {
        Stores {
            upstreams: storage.clone(),
            principals: storage.clone(),
            plugin_registry: storage.clone(),
            upstream_rate_limits: storage.clone(),
            upstream_subscription_quotas: storage.clone(),
            upstream_subscription_metadata: storage.clone(),
            organization_metadata: storage.clone(),
            plan_tiers: storage.clone(),
            prompt_cache_observations: storage.clone(),
            anthropic_compatibility_kv: storage,
            audit: None,
        }
    }

    async fn create_rebind_principal(storage: &Storage, name: &str) -> PrincipalRecord {
        PrincipalStore::create(
            storage,
            cc_lb_storage_api::PrincipalCreate {
                name: name.to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
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

    async fn build_rebind_view(
        stores: &Stores,
        current_generation: u64,
        runtime: &RecordingPluginRuntime,
    ) -> Arc<DynamicView> {
        build_dynamic_view_with_plugin_runtime(
            stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([1; 32])),
            None,
            current_generation,
            runtime,
            Path::new("."),
            Arc::new(SubscriptionQuotaCache::new()),
            None,
            None,
            1800,
            fixed_clock(1_800_000_000),
        )
        .await
        .expect("dynamic view builds")
    }

    async fn attach_rebind_plan_metadata(storage: &Storage, upstream_id: Uuid) {
        UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata(
            storage,
            &UpstreamSubscriptionMetadataRecord {
                upstream_id,
                organization_uuid: Some("org-max-5x".to_owned()),
                organization_role: None,
                workspace_role: None,
                observed_at_unix_millis: 1_800_000_000_000,
                last_error: None,
                raw_roles: None,
                raw_bootstrap: None,
            },
        )
        .await
        .expect("subscription metadata stored");
        OrganizationMetadataStore::put_organization_metadata(
            storage,
            &OrganizationMetadataRecord {
                organization_uuid: "org-max-5x".to_owned(),
                organization_name: Some("Max 5x Org".to_owned()),
                organization_type: Some("claude_max".to_owned()),
                rate_limit_tier: Some("default_claude_max_5x".to_owned()),
                seat_tier: None,
                has_extra_usage_enabled: None,
                billing_type: None,
                subscription_created_at_unix_secs: None,
                account_email: None,
                account_display_name: None,
                account_uuid: None,
                overage_credit_amount_minor_units: None,
                overage_credit_currency: None,
                overage_credit_granted: None,
                overage_credit_eligible: None,
                observed_at_unix_millis: 1_800_000_000_000,
                last_error: None,
                raw_profile: None,
                raw_overage_grant: None,
            },
        )
        .await
        .expect("organization metadata stored");
    }

    async fn seed_rebind_plan_ratio_catalog(storage: &Storage) {
        PlanTierStore::upsert_plan_tier_ratio(
            storage,
            &PlanTierRatioRecord {
                tier_key: "max_5x".to_owned(),
                pro_relative_ratio: 5.0,
                effective_from_unix_millis: 1_800_000_000_000,
                effective_to_unix_millis: None,
                provenance: "test_catalog".to_owned(),
                created_at_unix_millis: 1_800_000_000_000,
            },
        )
        .await
        .expect("plan ratio catalog seeded");
    }

    #[tokio::test]
    async fn principals_delete_rebuild_removes_deleted_and_increments_generation() {
        let storage = Arc::new(Storage::with_clock(fixed_clock(1_800_000_000)));
        let stores = rebind_stores(storage.clone());
        let runtime = RecordingPluginRuntime::default();
        let principal_a = create_rebind_principal(&storage, "principal-a").await;
        create_rebind_principal(&storage, "principal-b").await;

        let view = build_rebind_view(&stores, 0, &runtime).await;
        assert_eq!(view.generation, 1);
        assert_eq!(
            view.principal_view.principal_status("principal-a"),
            cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
        );
        assert_eq!(
            view.principal_view.principal_status("principal-b"),
            cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
        );

        PrincipalStore::soft_delete(storage.as_ref(), principal_a.id, principal_a.revision, 2)
            .await
            .expect("soft delete");
        let view = build_rebind_view(&stores, view.generation, &runtime).await;

        assert_eq!(view.generation, 2);
        assert_eq!(
            view.principal_view.principal_status("principal-a"),
            cc_lb_engine::api_keys::principal_view::PrincipalStatus::Missing
        );
        assert_eq!(
            view.principal_view.principal_status("principal-b"),
            cc_lb_engine::api_keys::principal_view::PrincipalStatus::Active
        );
        runtime.assert_no_instantiations();
    }

    #[tokio::test]
    async fn corrupt_oauth_upstream_is_error_while_other_upstreams_stay_active() {
        let storage = Arc::new(Storage::with_clock(fixed_clock(1_800_000_000)));
        let stores = rebind_stores(storage.clone());
        let runtime = RecordingPluginRuntime::default();
        create_rebind_principal(&storage, "principal-a").await;
        create_upstream(&storage, "healthy").await;
        let corrupt = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "corrupt".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("oauth upstream created");
        storage
            .store_oauth_tokens(
                corrupt.id,
                corrupt.revision,
                EncryptedOAuthTokens::from_ciphertext(vec![9]),
            )
            .await
            .expect("corrupt oauth stored");

        let view = build_rebind_view(&stores, 10, &runtime).await;
        assert_eq!(view.generation, 11);
        let mut upstream_names = view
            .upstreams_snapshot()
            .iter()
            .map(|record| record.name.as_str())
            .collect::<Vec<_>>();
        upstream_names.sort_unstable();
        assert_eq!(upstream_names, ["corrupt", "healthy"]);

        let healthy = view
            .upstream_status_snapshot
            .entries
            .get("healthy")
            .expect("healthy status");
        assert_eq!(healthy.status, ApplyStatus::Active);
        assert!(healthy.last_apply_error.is_none());

        let corrupt = view
            .upstream_status_snapshot
            .entries
            .get("corrupt")
            .expect("corrupt status");
        assert_eq!(corrupt.status, ApplyStatus::Error);
        assert!(
            corrupt
                .last_apply_error
                .as_ref()
                .is_some_and(|message| !message.is_empty())
        );

        let persisted = UpstreamStore::get_by_name(storage.as_ref(), "corrupt")
            .await
            .expect("load corrupt")
            .expect("corrupt exists");
        assert!(persisted.last_apply_error.is_some());
        runtime.assert_no_instantiations();
    }

    #[tokio::test]
    async fn plan_info_uses_catalog_ratio_and_reconciles_history() {
        let storage = Arc::new(Storage::with_clock(fixed_clock(1_800_000_000)));
        let stores = rebind_stores(storage.clone());
        let runtime = RecordingPluginRuntime::default();
        let upstream = create_upstream(&storage, "max-5x").await;
        attach_rebind_plan_metadata(&storage, upstream.id).await;
        seed_rebind_plan_ratio_catalog(&storage).await;

        let view = build_rebind_view(&stores, 0, &runtime).await;

        let plan_info = view
            .plan_info_by_upstream
            .get(&upstream.id)
            .expect("upstream plan info");
        assert_eq!(plan_info.organization_type.as_deref(), Some("claude_max"));
        assert_eq!(
            plan_info.rate_limit_tier.as_deref(),
            Some("default_claude_max_5x")
        );
        assert_eq!(plan_info.capacity_ratio, 5.0);

        let history = PlanTierStore::list_current_upstream_plan_tiers(storage.as_ref())
            .await
            .expect("plan tier history listed");
        let resolved = history
            .iter()
            .find(|record| record.upstream_id == upstream.id)
            .expect("upstream tier history");
        assert_eq!(resolved.organization_uuid.as_deref(), Some("org-max-5x"));
        assert_eq!(resolved.organization_type.as_deref(), Some("claude_max"));
        assert_eq!(
            resolved.rate_limit_tier.as_deref(),
            Some("default_claude_max_5x")
        );
        assert_eq!(resolved.seat_tier, None);
        assert_eq!(resolved.tier_key.as_deref(), Some("max_5x"));
        assert_eq!(resolved.resolution_source, TierResolutionSource::Builtin);
        assert_eq!(resolved.resolved_ratio_snapshot, Some(5.0));
        assert_eq!(resolved.effective_to_unix_millis, None);
        assert_eq!(resolved.provenance, "dynamic_view_reconcile");
        runtime.assert_no_instantiations();
    }
}
