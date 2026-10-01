use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_clock::{unix_millis, unix_secs};
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache, ShapePluginCache,
};
use cc_lb_control::{
    ApplyStatus, DynamicView, DynamicViewBuilder, UpstreamRateLimitCache, UpstreamStatusEntry,
    UpstreamStatusSnapshot,
};
use cc_lb_domain::{BUILTIN_SUBSCRIPTION_PREFERENCE_ID, RateLimitObservation, Upstream};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_oauth_protocol::refresh_requires_reconnect;
use cc_lb_quota::plan_capacity::{
    PRO_CAPACITY_RATIO, PlanInfo, PlanTierClassification, TierKey, classify_plan_tier,
};
use cc_lb_routing::FilterPlugin;
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntime};

use crate::wasm_host::{WasmtimeFilterPlugin, WasmtimeUpstreamDialect};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, MetadataTierMappingOverrideRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, PluginChainEntry,
    PluginRegistryStore, PluginSlotKind, PrincipalRecord, PrincipalStore,
    PromptCacheObservationStore, RateLimitKind, StorageError, StorageResult, TierResolutionSource,
    UpstreamPlanTierRecord, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamRecord, UpstreamStatusUpdate, UpstreamStore, UpstreamSubscriptionMetadataStore,
    UpstreamSubscriptionQuotaStore, WasmRegistryEntry,
};
use cc_lb_upstream::{Signer, SignerError, SignerFactory};
use parking_lot::RwLock;
use thiserror::Error;
use uuid::Uuid;

use crate::PluginManifest;
use crate::reconcile::collect_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use cc_lb_control::PromptCacheObservationSinkLike;

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

async fn register_filter_slot(
    runtime: &Arc<WasmtimeRuntime>,
    slot_key: &cc_lb_runtime_wasmtime::RuntimeSlotKey,
    manifest: &PluginManifest,
) -> Result<
    Arc<cc_lb_runtime_wasmtime::LoadedPluginSlot>,
    cc_lb_runtime_wasmtime::WasmtimeRuntimeError,
> {
    let wasm = read_wasm_for_manifest(manifest).await?;
    runtime.register_filter(slot_key.clone(), manifest.name.clone(), &wasm)
}

async fn register_shape_slot(
    runtime: &Arc<WasmtimeRuntime>,
    slot_key: &cc_lb_runtime_wasmtime::RuntimeSlotKey,
    manifest: &PluginManifest,
) -> Result<
    Arc<cc_lb_runtime_wasmtime::LoadedPluginSlot>,
    cc_lb_runtime_wasmtime::WasmtimeRuntimeError,
> {
    let wasm = read_wasm_for_manifest(manifest).await?;
    runtime.register_shape(slot_key.clone(), manifest.name.clone(), &wasm)
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
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    current_generation: u64,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    prompt_cache_grace_margin_secs: u64,
    prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    subscription_quota_routing_max_staleness_secs: u64,
    clock: cc_lb_clock::ClockHandle,
) -> Result<Arc<DynamicView>, RebindError> {
    let upstreams = list_upstreams(stores).await?;
    let all_upstream_ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    subscription_quota_cache
        .hydrate_from_store(stores, &all_upstream_ids)
        .await?;
    // Prompt-cache observations are read per request from the shared store
    // (`stores.prompt_cache_observations`); there is no pod-local warmth map
    // to hydrate at build/rebind time.
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
    let evicted = runtime.retain_slots(&registered_slot_keys);
    if !evicted.is_empty() {
        tracing::info!(
            evicted_count = evicted.len(),
            "reconcile swept orphan plugin slots",
        );
    }
    let principal_view = Arc::new(PrincipalView::from_db(&principals, principal_chains));
    let now = unix_secs(clock.now());
    let statuses = apply_upstreams(stores, &upstreams, &aead, now).await?;
    let revision_hash = collect_revision_hash(stores).await?;
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
    let builder = DynamicViewBuilder::new(current_generation)
        .signer_factory(signer_factory)
        .principal_view(principal_view)
        .upstream_status_snapshot(snapshot)
        .upstream_rate_limit_cache(upstream_rate_limit_cache)
        .subscription_quota_cache(subscription_quota_cache)
        .subscription_quota_routing_max_staleness_secs(
            subscription_quota_routing_max_staleness_secs,
        )
        .plan_info_by_upstream(plan_info_by_upstream)
        .upstream_records(upstreams.clone())
        .prompt_cache_observation_store(stores.prompt_cache_observations.clone())
        .prompt_cache_grace_margin_secs(prompt_cache_grace_margin_secs)
        .prompt_cache_observation_sink(prompt_cache_observation_sink);
    Ok(builder.build())
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
    !registry_entry.is_builtin && !registry_entry.supported_slots.contains(&slot)
}

async fn build_principal_chains(
    stores: &Stores,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    principals: &[PrincipalRecord],
) -> Result<
    (
        HashMap<String, PrincipalRoutingArtifacts>,
        HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
    ),
    RebindError,
> {
    let registry = list_registry_by_id(stores).await?;
    let principal_ids = principals
        .iter()
        .map(|principal| principal.id)
        .collect::<Vec<_>>();
    let slots = [PluginSlotKind::Router, PluginSlotKind::Shape];
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
    let mut registered_slot_keys: HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey> = HashSet::new();
    for principal in principals {
        let router_entries =
            take_chain_entries(&mut chain_entries, principal.id, PluginSlotKind::Router);
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
                    name: registry_entry.name.clone(),
                    artifact: wasm_path.to_string_lossy().into_owned(),
                };
                let slot_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                    principal.name.clone(),
                    manifest.name.clone(),
                );
                registered_slot_keys.insert(slot_key.clone());
                match register_shape_slot(runtime, &slot_key, &manifest).await {
                    Ok(slot) => {
                        let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
                            slot,
                            runtime.config_arc(),
                        ));
                        let handle: Arc<dyn cc_lb_upstream::UpstreamDialect> =
                            Arc::new(WasmtimeUpstreamDialect::new(dispatch));
                        DialectCache::Explicit(ShapePluginCache { dialect: handle })
                    }
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

        chains.insert(principal.name.clone(), (router, dialect));
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
        name: registry_entry.name.clone(),
        artifact: wasm_path.to_string_lossy().into_owned(),
    })
}

async fn build_router_pipeline(
    stores: &Stores,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    principal: &PrincipalRecord,
    mut router_entries: Vec<cc_lb_storage_api::PluginChainEntry>,
    registry: &HashMap<Uuid, cc_lb_storage_api::WasmRegistryEntry>,
    registered_slot_keys: &mut HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
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
        let slot_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
            principal.name.clone(),
            manifest.name.clone(),
        );
        registered_slot_keys.insert(slot_key.clone());
        match register_filter_slot(runtime, &slot_key, &manifest).await {
            Ok(slot) => {
                let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
                    slot,
                    runtime.config_arc(),
                ));
                let handle: Arc<dyn FilterPlugin> = Arc::new(WasmtimeFilterPlugin::new(
                    dispatch,
                    entry.id,
                    manifest.name.clone(),
                ));
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
    aead: &AeadService,
    now: u64,
) -> StorageResult<HashMap<String, UpstreamStatusEntry>> {
    let mut statuses = HashMap::new();
    for upstream in upstreams {
        let is_oauth = upstream.kind == UpstreamKind::AnthropicOauth;
        let last_apply_error = if is_oauth {
            upstream.last_apply_error.clone()
        } else {
            None
        };
        if !upstream.enabled {
            if !is_oauth {
                stores
                    .upstreams
                    .set_last_apply_error(upstream.id, None)
                    .await?;
            }
            statuses.insert(
                upstream.name.clone(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Disabled,
                    last_apply_error,
                    last_apply_at_unix_secs: now,
                },
            );
            continue;
        }

        if let Err(message) = validate_upstream(upstream, aead) {
            let reconnect_required =
                is_oauth && refresh_requires_reconnect(last_apply_error.as_deref());
            if !reconnect_required {
                if is_oauth {
                    match stores
                        .upstreams
                        .set_status(
                            upstream.id,
                            UpstreamStatusUpdate {
                                last_apply_error: Some(Some(message.clone())),
                                last_apply_at_unix_secs: Some(Some(now)),
                                expected_oauth_token_generation: Some(
                                    upstream.oauth_token_generation,
                                ),
                                ..UpstreamStatusUpdate::default()
                            },
                        )
                        .await
                    {
                        Ok(()) | Err(StorageError::Conflict { .. }) => {}
                        Err(error) => return Err(error),
                    }
                } else {
                    stores
                        .upstreams
                        .set_last_apply_error(upstream.id, Some(message.clone()))
                        .await?;
                }
            }
            statuses.insert(
                upstream.name.clone(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Error,
                    last_apply_error: if reconnect_required {
                        last_apply_error
                    } else {
                        Some(message)
                    },
                    last_apply_at_unix_secs: now,
                },
            );
            continue;
        }

        // Applying a view cannot prove the refresh token usable. Only a real
        // OAuth token write clears its renewal error; avoiding a status write
        // here also preserves a terminal failure arriving during this build.
        if !is_oauth {
            stores
                .upstreams
                .set_last_apply_error(upstream.id, None)
                .await?;
        }
        statuses.insert(
            upstream.name.clone(),
            UpstreamStatusEntry {
                status: ApplyStatus::Active,
                last_apply_error,
                last_apply_at_unix_secs: now,
            },
        );
    }
    Ok(statuses)
}

/// Why an anthropic api-key credential could not be resolved. Callers map each
/// reason onto their own error type; the variants carry no key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiKeyResolutionError {
    Missing,
    NotDecryptable,
    NotUtf8,
    InvalidHeaderValue,
}

/// Resolve the operator-configured anthropic api key for an upstream record.
///
/// `api_key_ciphertext` is sealed with AAD `upstream.id` over the raw UTF-8
/// key bytes. Resolution fails closed: any error means no credential, never a
/// fallback to the downstream caller's key.
fn resolve_api_key(
    aead: &AeadService,
    record: &UpstreamRecord,
) -> Result<zeroize::Zeroizing<String>, ApiKeyResolutionError> {
    let ciphertext = record
        .api_key_ciphertext
        .as_deref()
        .ok_or(ApiKeyResolutionError::Missing)?;
    let plaintext = aead
        .decrypt(ciphertext, record.id.as_bytes())
        .map(zeroize::Zeroizing::new)
        .map_err(|_| ApiKeyResolutionError::NotDecryptable)?;
    // The key must reach the upstream byte-identical to what the operator
    // stored, so it is neither trimmed nor rewritten.
    let key = std::str::from_utf8(&plaintext)
        .map_err(|_| ApiKeyResolutionError::NotUtf8)?
        .to_owned();
    let key = zeroize::Zeroizing::new(key);
    // The signer applies `HeaderValue::from_str` to this value at request time;
    // reject here so a malformed key surfaces as a view-build error instead of
    // a runtime signing failure.
    if key.trim().is_empty() || http::HeaderValue::from_str(&key).is_err() {
        return Err(ApiKeyResolutionError::InvalidHeaderValue);
    }
    Ok(key)
}

fn validate_upstream(upstream: &UpstreamRecord, aead: &AeadService) -> Result<(), String> {
    match upstream.kind {
        UpstreamKind::AnthropicApiKey => {
            // The resolved probe is only used to prove the credential is
            // usable; `Zeroizing` scrubs it when this arm returns.
            if let Err(reason) = resolve_api_key(aead, upstream) {
                return Err(match reason {
                    ApiKeyResolutionError::Missing => {
                        "anthropic api-key upstream missing api_key_ciphertext".to_owned()
                    }
                    ApiKeyResolutionError::NotDecryptable => {
                        "anthropic api-key upstream credential is not decryptable".to_owned()
                    }
                    ApiKeyResolutionError::NotUtf8 => {
                        "anthropic api-key upstream credential is not valid utf-8".to_owned()
                    }
                    ApiKeyResolutionError::InvalidHeaderValue => {
                        "anthropic api-key upstream credential is not a valid header value"
                            .to_owned()
                    }
                });
            }
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
    }
    Ok(())
}

#[derive(Clone)]
struct DbCompositeSignerFactory {
    upstreams: Vec<UpstreamRecord>,
    upstream_store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    router_chosen_upstream_name: Option<String>,
    clock: cc_lb_clock::ClockHandle,
}

impl DbCompositeSignerFactory {
    fn new(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        clock: cc_lb_clock::ClockHandle,
    ) -> Self {
        Self {
            upstreams,
            upstream_store,
            aead,
            lazy_refresher,
            router_chosen_upstream_name: None,
            clock,
        }
    }

    fn with_router_choice_state(&self, router_chosen_upstream_name: String) -> Self {
        Self {
            upstreams: self.upstreams.clone(),
            upstream_store: self.upstream_store.clone(),
            aead: self.aead.clone(),
            lazy_refresher: self.lazy_refresher.clone(),
            router_chosen_upstream_name: Some(router_chosen_upstream_name),
            clock: self.clock.clone(),
        }
    }
}

impl cc_lb_engine::ApiKeyAwareSignerFactory for DbCompositeSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        // The downstream caller's key is deliberately not forwarded upstream.
        Arc::new(self.with_router_choice_state(router_chosen_upstream_name))
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
                let api_key = resolve_api_key(&self.aead, record).map_err(|reason| {
                    SignerError::MissingCredentials {
                        reason: match reason {
                            ApiKeyResolutionError::Missing => {
                                "anthropic api-key upstream has no stored credential".to_owned()
                            }
                            ApiKeyResolutionError::NotDecryptable => "anthropic api-key upstream credential could not be decrypted; re-set api_key_value for this upstream".to_owned(),
                            ApiKeyResolutionError::NotUtf8 => {
                                "anthropic api-key upstream credential is not valid utf-8"
                                    .to_owned()
                            }
                            ApiKeyResolutionError::InvalidHeaderValue => "anthropic api-key upstream credential is not a valid header value; re-set api_key_value for this upstream".to_owned(),
                        },
                    }
                })?;
                cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::new(api_key.as_str())
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
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use cc_lb_clock::TestClock;
    use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
    use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    use cc_lb_storage_api::{MetaStore, PromptCacheObservationRecord, UpstreamCreate};
    use cc_lb_storage_sqlite::SqliteStorage as Storage;

    use super::*;
    use crate::prompt_cache_observation_sink::{
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
    };

    const MODEL: &str = "claude-sonnet-4-5-20250929";

    #[tokio::test]
    async fn stale_oauth_validation_failure_cannot_poison_replacement_long_lived_credential() {
        let (_dir, storage) = storage_fixture(19).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let aead = AeadService::from_master_key([1; 32]);
        let created = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                name: "stale-missing-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                ..UpstreamCreate::default()
            },
        )
        .await
        .expect("oauth upstream created");
        let snapshot =
            UpstreamStore::set_enabled(storage.as_ref(), created.id, created.revision, true)
                .await
                .expect("capture enabled state before credential installation");
        let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
            &aead,
            &cc_lb_aead::OAuthTokenBundle {
                access_token: "long-lived-access".to_owned(),
                refresh_token: "long-lived-refresh".to_owned(),
                expires_at_unix_secs: 1_900_000_000,
                refresh_token_expires_at_unix_secs: None,
                scopes: vec!["messages".to_owned()],
                never_refresh: true,
            },
            snapshot.id.as_bytes(),
        )
        .expect("encrypt actual long-lived credential");
        let replaced = UpstreamStore::store_oauth_tokens(
            storage.as_ref(),
            snapshot.id,
            snapshot.revision,
            encrypted,
            true,
        )
        .await
        .expect("actual credential installation after snapshot");
        assert!(replaced.oauth_token_generation > snapshot.oauth_token_generation);
        let stale_statuses = apply_upstreams(&stores, std::slice::from_ref(&snapshot), &aead, 1)
            .await
            .expect("stale validation conflict is skipped");
        assert_eq!(
            stale_statuses
                .get(&snapshot.name)
                .expect("stale apply status")
                .status,
            ApplyStatus::Error
        );
        let current = UpstreamStore::get_by_id(storage.as_ref(), snapshot.id)
            .await
            .expect("load installed credential")
            .expect("upstream exists");
        assert_eq!(current.last_apply_error, None);
        assert_eq!(
            current.oauth_token_generation,
            replaced.oauth_token_generation
        );
        assert!(current.oauth_never_refresh);
        let fresh_statuses = apply_upstreams(&stores, std::slice::from_ref(&current), &aead, 2)
            .await
            .expect("fresh credential validates");
        assert_eq!(
            fresh_statuses
                .get(&snapshot.name)
                .expect("fresh apply status")
                .status,
            ApplyStatus::Active
        );
    }

    #[tokio::test]
    async fn applying_stale_oauth_snapshot_cannot_clear_later_terminal_refresh_failure() {
        let (_dir, storage) = storage_fixture(18).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let aead = AeadService::from_master_key([1; 32]);
        for enabled in [true, false] {
            let created = UpstreamStore::create(
                storage.as_ref(),
                UpstreamCreate {
                    name: format!("stale-oauth-{enabled}"),
                    kind: UpstreamKind::AnthropicOauth,
                    ..UpstreamCreate::default()
                },
            )
            .await
            .expect("oauth upstream created");
            let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
                &aead,
                &cc_lb_aead::OAuthTokenBundle {
                    access_token: "valid-access".to_owned(),
                    refresh_token: "valid-refresh".to_owned(),
                    expires_at_unix_secs: 1_900_000_000,
                    refresh_token_expires_at_unix_secs: None,
                    scopes: vec!["messages".to_owned()],
                    never_refresh: false,
                },
                created.id.as_bytes(),
            )
            .expect("encrypt credential");
            let stored = UpstreamStore::store_oauth_tokens(
                storage.as_ref(),
                created.id,
                created.revision,
                encrypted,
                false,
            )
            .await
            .expect("oauth credential stored");
            let snapshot =
                UpstreamStore::set_enabled(storage.as_ref(), stored.id, stored.revision, enabled)
                    .await
                    .expect("capture pre-failure upstream state");
            UpstreamStore::set_status(
                storage.as_ref(),
                snapshot.id,
                cc_lb_storage_api::UpstreamStatusUpdate {
                    last_apply_error: Some(Some("status_401".to_owned())),
                    expected_oauth_token_generation: Some(snapshot.oauth_token_generation),
                    ..cc_lb_storage_api::UpstreamStatusUpdate::default()
                },
            )
            .await
            .expect("terminal failure arrives after snapshot");
            let statuses = apply_upstreams(&stores, std::slice::from_ref(&snapshot), &aead, 1)
                .await
                .expect("stale view apply succeeds");
            assert_eq!(
                statuses.get(&snapshot.name).expect("apply status").status,
                if enabled {
                    ApplyStatus::Active
                } else {
                    ApplyStatus::Disabled
                }
            );
            let current = UpstreamStore::get_by_id(storage.as_ref(), snapshot.id)
                .await
                .expect("load current terminal state")
                .expect("upstream exists");
            assert_eq!(current.last_apply_error.as_deref(), Some("status_401"));
            assert_eq!(
                current.oauth_token_generation,
                snapshot.oauth_token_generation
            );
        }
    }

    /// Stand-in for a user-uploaded router filter so ordering and dedupe can be
    /// observed without a wasm runtime.
    fn plugin_ids(filters: &[Arc<dyn FilterPlugin>]) -> Vec<Uuid> {
        filters.iter().map(|filter| filter.plugin_id()).collect()
    }

    async fn router_pipeline_for_seeded_principal(
        include_router_entries: bool,
    ) -> Option<Arc<RouterPipelineCache>> {
        let (dir, storage) = storage_fixture(9).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(
            cc_lb_runtime_wasmtime::WasmtimeRuntime::new(Default::default()).expect("runtime"),
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
        let pipeline = build_router_pipeline(
            &stores,
            &runtime,
            dir.path(),
            &principal,
            router_entries,
            &registry,
            &mut slot_keys,
        )
        .await
        .expect("pipeline builds");
        drop(dir);
        pipeline
    }

    #[tokio::test]
    async fn seeded_subscription_preference_chain_yields_the_builtin_filter() {
        let pipeline = router_pipeline_for_seeded_principal(true)
            .await
            .expect("seeded entry produces a router pipeline");

        assert!(pipeline.instantiation_error.is_none());
        assert_eq!(
            plugin_ids(&pipeline.user_filters),
            vec![BUILTIN_SUBSCRIPTION_PREFERENCE_ID]
        );
    }

    #[tokio::test]
    async fn empty_router_chain_yields_no_pipeline() {
        assert!(
            router_pipeline_for_seeded_principal(false).await.is_none(),
            "an absent chain means no router filter pipeline"
        );
    }

    /// Observation store test double. `list_active_for_candidates` counts
    /// calls so tests can prove the view build never reads the store —
    /// routing reads happen per request, not at build/rebind time.
    #[derive(Clone, Default)]
    struct FakePromptCacheObservationStore {
        upserts: Arc<tokio::sync::Mutex<Vec<PromptCacheObservationRecord>>>,
        list_calls: Arc<AtomicU64>,
    }

    impl FakePromptCacheObservationStore {
        fn new() -> Self {
            Self::default()
        }

        async fn upsert_count(&self) -> usize {
            self.upserts.lock().await.len()
        }

        async fn upserted(&self) -> Vec<PromptCacheObservationRecord> {
            self.upserts.lock().await.clone()
        }

        fn list_call_count(&self) -> u64 {
            self.list_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PromptCacheObservationStore for FakePromptCacheObservationStore {
        async fn list_active_for_candidates(
            &self,
            _upstream_ids: &[Uuid],
            _canonical_model_id: &str,
            _v3_prefix_keys: &[String],
            _not_expired_at_unix_secs: u64,
        ) -> StorageResult<Vec<PromptCacheObservationRecord>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }

        async fn upsert_observation(
            &self,
            record: &PromptCacheObservationRecord,
        ) -> StorageResult<()> {
            self.upserts.lock().await.push(record.clone());
            Ok(())
        }
    }

    async fn storage_fixture(seed: u8) -> (tempfile::TempDir, Arc<Storage>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            dir.path()
                .join(format!("dynamic-view-builder-{seed}.sqlite"))
                .display()
        );
        let storage =
            cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
                .await
                .expect("storage");
        storage.initialize().await.unwrap();
        let storage = Arc::new(storage);
        (dir, storage)
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
        let created = UpstreamStore::create(
            storage,
            UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created");
        // Mirrors the admin two-phase flow: the ciphertext is bound to the
        // upstream id, which only exists after create.
        let aead = AeadService::from_master_key([19; 32]);
        let ciphertext = aead
            .encrypt(b"sk-ant-fixture-secret", created.id.as_bytes())
            .expect("api-key ciphertext encrypts");
        UpstreamStore::update_api_key_secret(storage, created.id, Some(ciphertext))
            .await
            .expect("api-key secret stored")
    }

    async fn build_view(
        stores: &Stores,
        runtime: &Arc<WasmtimeRuntime>,
        data_dir: &Path,
    ) -> Arc<DynamicView> {
        let config = cc_lb_config::Config::default();
        build_view_with_config(stores, runtime, data_dir, config).await
    }

    async fn build_view_with_config(
        stores: &Stores,
        runtime: &Arc<WasmtimeRuntime>,
        data_dir: &Path,
        config: cc_lb_config::Config,
    ) -> Arc<DynamicView> {
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
        build_dynamic_view(
            stores,
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            0,
            runtime,
            data_dir,
            Arc::new(SubscriptionQuotaCache::new()),
            config.prompt_cache_shadow.grace_margin_secs,
            sink,
            1800,
            clock,
        )
        .await
        .expect("dynamic view builds")
    }

    #[tokio::test]
    async fn wires_shared_observation_store_without_hydrating() {
        let (dir, storage) = storage_fixture(19).await;
        create_upstream(&storage, "prompt-cache-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new());
        let stores = stores(storage, prompt_store.clone());
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;

        assert!(
            dynamic_view.prompt_cache_observation_store_opt().is_some(),
            "shared observation store must be wired into DynamicView"
        );
        assert_eq!(
            prompt_store.list_call_count(),
            0,
            "view build must not read the observation store; routing reads per request"
        );
    }

    #[tokio::test]
    async fn observation_sink_routes_records_to_store() {
        let (dir, storage) = storage_fixture(22).await;
        let upstream = create_upstream(&storage, "sink-wiring-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new());
        let stores = stores(storage, prompt_store.clone());
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;
        let sink = dynamic_view.prompt_cache_observation_sink.clone();
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
        for _ in 0..50 {
            if prompt_store.upsert_count().await >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let stored = prompt_store.upserted().await;
        assert_eq!(
            stored.len(),
            1,
            "observation enqueued through DynamicView sink must reach the production store"
        );
        assert_eq!(stored[0].v3_prefix_key, "sink-wiring-prefix");
        assert_eq!(stored[0].upstream_id, upstream.id);
    }

    #[tokio::test]
    async fn build_never_reads_observation_store() {
        let (dir, storage) = storage_fixture(20).await;
        create_upstream(&storage, "no-read-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new());
        let stores = stores(storage, prompt_store.clone());
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;

        assert!(dynamic_view.prompt_cache_observation_store_opt().is_some());
        assert_eq!(
            prompt_store.list_call_count(),
            0,
            "build/rebind must not hydrate observations; reads are per request"
        );
    }

    #[tokio::test]
    async fn grace_margin_propagates_to_view() {
        let (dir, storage) = storage_fixture(22).await;
        let stores = stores(storage, Arc::new(FakePromptCacheObservationStore::new()));
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
        let mut config = cc_lb_config::Config::default();
        config.prompt_cache_shadow.grace_margin_secs = 99;

        let dynamic_view = build_view_with_config(&stores, &runtime, dir.path(), config).await;

        assert_eq!(dynamic_view.prompt_cache_grace_margin_secs, 99);
    }

    fn api_key_record(name: &str, ciphertext: Option<Vec<u8>>) -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            api_key_ciphertext: ciphertext,
            ..UpstreamRecord::default()
        }
    }

    #[test]
    fn resolve_api_key_reads_canonical_raw_id_aad_shape() {
        let aead = AeadService::from_master_key([19; 32]);
        let mut record = api_key_record("canonical", None);
        record.api_key_ciphertext = Some(
            aead.encrypt(b"sk-ant-canonical", record.id.as_bytes())
                .expect("encrypt"),
        );

        let resolved = resolve_api_key(&aead, &record).expect("canonical shape resolves");
        assert_eq!(resolved.as_str(), "sk-ant-canonical");
    }

    #[test]
    fn resolve_api_key_rejects_ciphertext_under_unrelated_aad() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("bound-elsewhere", None);
        let ciphertext = aead
            .encrypt(b"sk-ant-bound-elsewhere", b"some-other-aad")
            .expect("encrypt");
        let record = UpstreamRecord {
            api_key_ciphertext: Some(ciphertext),
            ..record
        };

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::NotDecryptable)
        ));
    }

    #[test]
    fn resolve_api_key_rejects_key_with_invalid_header_byte() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("bad-key", None);
        let ciphertext = aead
            .encrypt(b"sk-ant-\nnewline", record.id.as_bytes())
            .expect("encrypt");
        let record = UpstreamRecord {
            api_key_ciphertext: Some(ciphertext),
            ..record
        };

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::InvalidHeaderValue)
        ));
    }

    #[test]
    fn resolve_api_key_reports_missing_ciphertext() {
        let aead = AeadService::from_master_key([19; 32]);
        let record = api_key_record("no-credential", None);

        assert!(matches!(
            resolve_api_key(&aead, &record),
            Err(ApiKeyResolutionError::Missing)
        ));
    }
}
