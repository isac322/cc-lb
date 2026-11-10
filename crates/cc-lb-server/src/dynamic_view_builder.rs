use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_clock::{unix_millis, unix_secs};
use cc_lb_control::anthropic_api_key::credential::{ApiKeyResolutionError, resolve_api_key};
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, ProviderFilterChains,
    RouterPipelineCache,
};
use cc_lb_control::{
    ApplyStatus, DynamicView, DynamicViewBuilder, UpstreamRateLimitCache, UpstreamStatusEntry,
    UpstreamStatusSnapshot,
};
use cc_lb_domain::{BUILTIN_SUBSCRIPTION_PREFERENCE_ID, Provider, RateLimitObservation, Upstream};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_oauth_protocol::refresh_requires_reconnect;
use cc_lb_quota::plan_capacity::{
    PRO_CAPACITY_RATIO, PlanInfo, PlanTierClassification, TierKey, classify_plan_tier,
};
use cc_lb_routing::FilterPlugin;
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntime};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::wasm_host::{WasmtimeFilterPlugin, WasmtimeUpstreamDialect};
use cc_lb_storage_api::upstream::UpstreamKind;
use secrecy::ExposeSecret;

use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, MetadataTierMappingOverrideRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, PluginChainEntry,
    PluginRegistryStore, PluginSlotKind, PrincipalRecord, PrincipalStore,
    PromptCacheObservationStore, RateLimitKind, StorageError, StorageResult, TierResolutionSource,
    UpstreamPlanTierRecord, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamRecord, UpstreamStatusUpdate, UpstreamStore, UpstreamSubscriptionMetadataStore,
    UpstreamSubscriptionQuotaStore, WasmRegistryEntry,
};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError, scrub_secret,
};
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
    antigravity_config: Option<&cc_lb_config::AntigravityOAuthConfig>,
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
    // Collect the change signature before any content read. A mutation that
    // commits while this build is reading contents then changes the stored
    // signature after this early sample, so a later reconcile detects the
    // drift instead of trusting a signature newer than the contents.
    let revision_hash = collect_revision_hash(stores).await?;
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
        build_principal_chains(stores, runtime, data_dir, &principals, &upstreams, &aead).await?;
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

    let signer_factory = Arc::new(DbCompositeSignerFactory::new(
        upstreams.clone(),
        stores.upstreams.clone(),
        aead,
        lazy_refresher,
        clock.clone(),
        antigravity_config.cloned().unwrap_or_default(),
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

pub(crate) async fn list_upstreams(stores: &Stores) -> StorageResult<Vec<UpstreamRecord>> {
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
    upstreams: &[UpstreamRecord],
    aead: &AeadService,
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
    // Identity is bound only for records used by a plugin, once per
    // reconciliation rather than once per principal or request.
    let mut upstream_identities = HashMap::new();
    let mut chains = HashMap::new();
    let mut registered_slot_keys: HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey> = HashSet::new();
    for principal in principals {
        let allowed_upstreams: Vec<&UpstreamRecord> = upstreams
            .iter()
            .filter(|upstream| {
                upstream.enabled
                    && upstream.deleted_at_unix_secs.is_none()
                    && (principal.allowed_upstreams.is_empty()
                        || principal.allowed_upstreams.contains(&upstream.id))
            })
            .collect();
        let providers: Vec<Provider> = [Provider::Anthropic, Provider::Openai, Provider::Gemini]
            .into_iter()
            .filter(|provider| {
                allowed_upstreams
                    .iter()
                    .any(|record| record.kind.provider() == *provider)
            })
            .collect();
        let router_entries =
            take_chain_entries(&mut chain_entries, principal.id, PluginSlotKind::Router);
        let shape_entries =
            take_chain_entries(&mut chain_entries, principal.id, PluginSlotKind::Shape);

        let router = build_router_pipeline(
            RouterPipelineBuildContext {
                stores,
                runtime,
                data_dir,
                registry: &registry,
            },
            principal,
            router_entries,
            &mut registered_slot_keys,
            &providers,
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
                        let dispatch =
                            WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
                        let mut dialects = HashMap::new();
                        for provider in &providers {
                            let Some(selected) = dispatch.for_provider(wire_provider(*provider))
                            else {
                                continue;
                            };
                            let selected = Arc::new(selected);
                            for record in allowed_upstreams
                                .iter()
                                .filter(|record| record.kind.provider() == *provider)
                            {
                                let identity =
                                    upstream_identities.entry(record.id).or_insert_with(|| {
                                        let tokens = if record.kind == UpstreamKind::GeminiOauth {
                                            record.oauth_credentials.as_ref().and_then(
                                                |credentials| {
                                                    credentials
                                                        .decrypt(aead, record.id.as_bytes())
                                                        .ok()
                                                },
                                            )
                                        } else {
                                            None
                                        };
                                        upstream_identity(record, tokens.as_ref())
                                    });
                                let mut dialect =
                                    WasmtimeUpstreamDialect::new(Arc::clone(&selected))
                                        .with_upstream_identity(identity.clone());
                                if selected.shape_wire_version().is_none() {
                                    dialect = dialect.with_native_request(
                                        cc_lb_engine::lifecycle::native_route_dialect(record),
                                    );
                                }
                                let dialect: Arc<dyn cc_lb_upstream::UpstreamDialect> =
                                    Arc::new(dialect);
                                dialects.insert(record.id, dialect);
                            }
                        }
                        DialectCache::PerUpstream(dialects)
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

/// Copies stored upstream identity without decrypting or inferring metadata.
pub(crate) fn upstream_identity(
    record: &UpstreamRecord,
    tokens: Option<&cc_lb_aead::OAuthTokenBundle>,
) -> cc_lb_domain::UpstreamIdentity {
    let gemini_tokens = tokens.filter(|_| record.kind == UpstreamKind::GeminiOauth);
    cc_lb_domain::UpstreamIdentity {
        provider: record.kind.provider(),
        credential_kind: record.kind.as_str().to_owned(),
        upstream_id: record.id.to_string(),
        name: record.name.clone(),
        base_url: record.base_url.as_ref().map(ToString::to_string),
        gemini_oauth_profile: gemini_tokens.map(|tokens| match tokens.oauth_profile {
            cc_lb_aead::GeminiOAuthProfile::GeminiCli => "gemini_cli".to_owned(),
            cc_lb_aead::GeminiOAuthProfile::Antigravity => "antigravity".to_owned(),
        }),
        code_assist_project_id: gemini_tokens.and_then(|tokens| tokens.project_id.clone()),
    }
}

fn wire_provider(provider: Provider) -> cc_lb_plugin_wire::schema::ProviderFamily {
    match provider {
        Provider::Anthropic => cc_lb_plugin_wire::schema::ProviderFamily::Anthropic,
        Provider::Openai => cc_lb_plugin_wire::schema::ProviderFamily::Openai,
        Provider::Gemini => cc_lb_plugin_wire::schema::ProviderFamily::Gemini,
    }
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

struct RouterPipelineBuildContext<'a> {
    stores: &'a Stores,
    runtime: &'a Arc<WasmtimeRuntime>,
    data_dir: &'a Path,
    registry: &'a HashMap<Uuid, WasmRegistryEntry>,
}

async fn build_router_pipeline(
    context: RouterPipelineBuildContext<'_>,
    principal: &PrincipalRecord,
    mut router_entries: Vec<cc_lb_storage_api::PluginChainEntry>,
    registered_slot_keys: &mut HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
    providers: &[Provider],
) -> Result<Option<Arc<RouterPipelineCache>>, RebindError> {
    let RouterPipelineBuildContext {
        stores,
        runtime,
        data_dir,
        registry,
    } = context;
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
            provider_filters: None,
        })));
    }

    router_entries.sort_by_key(|entry| entry.order);
    let mut provider_filters = ProviderFilterChains::default();
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
                provider_filters: None,
            })));
        };
        if registry_entry.is_builtin {
            if registry_entry.id == BUILTIN_SUBSCRIPTION_PREFERENCE_ID {
                let filter: Arc<dyn FilterPlugin> = Arc::new(SubscriptionPreferenceFilter::new());
                for provider in providers {
                    provider_filters
                        .get_mut(*provider)
                        .push(Arc::clone(&filter));
                }
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
                    provider_filters: None,
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
                let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
                for provider in providers {
                    if let Some(filter) = WasmtimeFilterPlugin::for_provider(
                        &dispatch,
                        wire_provider(*provider),
                        entry.id,
                        manifest.name.clone(),
                    ) {
                        provider_filters.get_mut(*provider).push(Arc::new(filter));
                    }
                }
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
                    provider_filters: None,
                })));
            }
        }
    }

    Ok(Some(Arc::new(RouterPipelineCache {
        user_filters: provider_filters.anthropic.clone(),
        terminal: principal.router_terminal_strategy.clone(),
        instantiation_error: None,
        provider_filters: Some(provider_filters),
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
        let is_oauth = matches!(
            upstream.kind,
            UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
        );
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
                    ApiKeyResolutionError::LegacyEncoding => {
                        "anthropic api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream"
                            .to_owned()
                    }
                });
            }
        }
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
            if let Err(reason) = resolve_api_key(aead, upstream) {
                let provider = match upstream.kind {
                    UpstreamKind::OpenaiApiKey => "openai",
                    UpstreamKind::GeminiApiKey => "gemini",
                    _ => unreachable!("provider arm restricts upstream kind"),
                };
                return Err(match reason {
                    ApiKeyResolutionError::Missing => {
                        format!("{provider} api-key upstream missing api_key_ciphertext")
                    }
                    ApiKeyResolutionError::NotDecryptable => {
                        format!("{provider} api-key upstream credential is not decryptable")
                    }
                    ApiKeyResolutionError::NotUtf8 => {
                        format!("{provider} api-key upstream credential is not valid utf-8")
                    }
                    ApiKeyResolutionError::InvalidHeaderValue => {
                        format!(
                            "{provider} api-key upstream credential is not a valid header value"
                        )
                    }
                    ApiKeyResolutionError::LegacyEncoding => {
                        format!(
                            "{provider} api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream"
                        )
                    }
                });
            }
        }
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            let provider = match upstream.kind {
                UpstreamKind::AnthropicOauth => "anthropic",
                UpstreamKind::OpenaiOauth => "openai",
                UpstreamKind::GeminiOauth => "gemini",
                _ => unreachable!("oauth arm restricts upstream kind"),
            };
            let ciphertext = upstream
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| format!("{provider} oauth upstream missing oauth credentials"))?
                .ciphertext();
            if ciphertext.len() < 29 {
                return Err(format!(
                    "{provider} oauth credentials ciphertext is corrupt"
                ));
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
    refresh_locks: cc_lb_signer_anthropic_oauth::RefreshLocks,
    router_chosen_upstream_name: Option<String>,
    clock: cc_lb_clock::ClockHandle,
    antigravity_config: Arc<cc_lb_config::AntigravityOAuthConfig>,
}

impl DbCompositeSignerFactory {
    fn new(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        clock: cc_lb_clock::ClockHandle,
        antigravity_config: cc_lb_config::AntigravityOAuthConfig,
    ) -> Self {
        Self {
            upstreams,
            upstream_store,
            aead,
            lazy_refresher,
            refresh_locks: cc_lb_signer_anthropic_oauth::new_refresh_locks(),
            router_chosen_upstream_name: None,
            clock,
            antigravity_config: Arc::new(antigravity_config),
        }
    }

    fn with_router_choice_state(&self, router_chosen_upstream_name: String) -> Self {
        Self {
            upstreams: self.upstreams.clone(),
            upstream_store: self.upstream_store.clone(),
            aead: self.aead.clone(),
            lazy_refresher: self.lazy_refresher.clone(),
            refresh_locks: self.refresh_locks.clone(),
            router_chosen_upstream_name: Some(router_chosen_upstream_name),
            clock: self.clock.clone(),
            antigravity_config: self.antigravity_config.clone(),
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
                            ApiKeyResolutionError::LegacyEncoding => "anthropic api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream".to_owned(),
                        },
                    }
                })?;
                cc_lb_signer_anthropic_key::AnthropicKeySignerFactory::new(
                    api_key.as_str(),
                    record.workspace_id.as_deref(),
                )?
                .build(upstream)
                .await
            }
            UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
                let api_key = resolve_api_key(&self.aead, record).map_err(|reason| {
                    let provider = match record.kind {
                        UpstreamKind::OpenaiApiKey => "openai",
                        UpstreamKind::GeminiApiKey => "gemini",
                        _ => unreachable!("provider arm restricts upstream kind"),
                    };
                    SignerError::MissingCredentials {
                        reason: match reason {
                            ApiKeyResolutionError::Missing => {
                                format!("{provider} api-key upstream has no stored credential")
                            }
                            ApiKeyResolutionError::NotDecryptable => {
                                format!("{provider} api-key upstream credential could not be decrypted; re-set api_key_value for this upstream")
                            }
                            ApiKeyResolutionError::NotUtf8 => {
                                format!("{provider} api-key upstream credential is not valid utf-8")
                            }
                            ApiKeyResolutionError::InvalidHeaderValue => {
                                format!("{provider} api-key upstream credential is not a valid header value; re-set api_key_value for this upstream")
                            }
                            ApiKeyResolutionError::LegacyEncoding => {
                                format!("{provider} api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream")
                            }
                        },
                    }
                })?;
                let kind = match record.kind {
                    UpstreamKind::OpenaiApiKey => {
                        cc_lb_signer_anthropic_key::ProviderKeyKind::Openai
                    }
                    UpstreamKind::GeminiApiKey => {
                        cc_lb_signer_anthropic_key::ProviderKeyKind::Gemini
                    }
                    _ => unreachable!("provider arm restricts upstream kind"),
                };
                cc_lb_signer_anthropic_key::ProviderKeySignerFactory::new(kind, api_key.as_str())?
                    .build(upstream)
                    .await
            }
            UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
                let encrypted = record.oauth_credentials.as_ref().ok_or_else(|| {
                    SignerError::MissingCredentials {
                        reason: "subscription oauth upstream has no stored credential".to_owned(),
                    }
                })?;
                let tokens = encrypted
                    .decrypt(&self.aead, record.id.as_bytes())
                    .map_err(|source| SignerError::SigningFailed {
                        reason: source.to_string(),
                    })?;
                let signer = build_subscription_signer_with_policy(
                    record,
                    &tokens,
                    self.clock.clone(),
                    Some(&self.antigravity_config),
                    true,
                )?;
                if !tokens.never_refresh
                    && let Some(refresh_handle) = self.lazy_refresher.clone()
                {
                    Ok(Arc::new(RefreshingSubscriptionSigner {
                        state: Arc::new(RwLock::new(SubscriptionSignerState {
                            signer,
                            access_token: secrecy::SecretString::new(
                                tokens.access_token.clone().into_boxed_str(),
                            ),
                            historical_access_tokens: Vec::new(),
                            expires_at_unix_secs: tokens.expires_at_unix_secs,
                        })),
                        record: record.clone(),
                        store: self.upstream_store.clone(),
                        aead: self.aead.clone(),
                        clock: self.clock.clone(),
                        antigravity_config: self.antigravity_config.clone(),
                        refresh_handle,
                        refresh_locks: self.refresh_locks.clone(),
                    }))
                } else if tokens.never_refresh {
                    Ok(Arc::new(PersistedAccessOnlySubscriptionSigner {
                        signer,
                        store: self.upstream_store.clone(),
                        upstream_id: record.id,
                        oauth_token_generation: record.oauth_token_generation,
                    }))
                } else {
                    Ok(signer)
                }
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

/// Shared proxy and background-request signer construction for subscription OAuth.
pub(crate) fn build_subscription_signer(
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    clock: cc_lb_clock::ClockHandle,
    antigravity_config: Option<&cc_lb_config::AntigravityOAuthConfig>,
) -> Result<Arc<dyn Signer>, SignerError> {
    build_subscription_signer_with_policy(record, tokens, clock, antigravity_config, false)
}

fn build_subscription_signer_with_policy(
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    clock: cc_lb_clock::ClockHandle,
    antigravity_config: Option<&cc_lb_config::AntigravityOAuthConfig>,
    refreshing: bool,
) -> Result<Arc<dyn Signer>, SignerError> {
    match record.kind {
        UpstreamKind::OpenaiOauth => {
            let signer = if tokens.never_refresh || !refreshing {
                cc_lb_signer_anthropic_key::OpenaiSubscriptionSigner::new(
                    tokens.access_token.as_str(),
                    tokens.account_id.as_deref(),
                    tokens.never_refresh.then_some(tokens.expires_at_unix_secs),
                    clock,
                )?
            } else {
                cc_lb_signer_anthropic_key::OpenaiSubscriptionSigner::new_refreshing(
                    tokens.access_token.as_str(),
                    tokens.account_id.as_deref(),
                    tokens.expires_at_unix_secs,
                    clock,
                )?
            };
            Ok(Arc::new(signer))
        }
        UpstreamKind::GeminiOauth => {
            let signer = match tokens.oauth_profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                    if tokens.never_refresh || !refreshing {
                        cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new(
                            tokens.access_token.as_str(),
                            tokens.project_id.as_deref(),
                            tokens.never_refresh.then_some(tokens.expires_at_unix_secs),
                            clock,
                        )?
                    } else {
                        cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new_refreshing(
                            tokens.access_token.as_str(),
                            tokens.project_id.as_deref(),
                            tokens.expires_at_unix_secs,
                            clock,
                        )?
                    }
                }
                cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                    let default_config;
                    let antigravity_config = match antigravity_config {
                        Some(config) => config,
                        None => {
                            default_config = cc_lb_config::AntigravityOAuthConfig::default();
                            &default_config
                        }
                    };
                    let base_url = record
                        .base_url
                        .as_ref()
                        .filter(|url| url.host_str() != Some("cloudcode-pa.googleapis.com"))
                        .unwrap_or(&antigravity_config.code_assist_url);
                    if tokens.never_refresh || !refreshing {
                        cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new_antigravity(
                            tokens.access_token.as_str(),
                            tokens.project_id.as_deref(),
                            tokens.never_refresh.then_some(tokens.expires_at_unix_secs),
                            clock,
                            base_url.as_str(),
                            &antigravity_config.user_agent,
                        )?
                    } else {
                        cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new_antigravity_refreshing(
                            tokens.access_token.as_str(),
                            tokens.project_id.as_deref(),
                            tokens.expires_at_unix_secs,
                            clock,
                            base_url.as_str(),
                            &antigravity_config.user_agent,
                        )?
                    }
                }
            };
            Ok(Arc::new(signer))
        }
        _ => Err(SignerError::SigningFailed {
            reason: "subscription signer requires OpenAI or Gemini OAuth".to_owned(),
        }),
    }
}
/// Persists a terminal provider rejection for an access-only subscription
/// credential without retrying or changing the provider signer behavior.
struct PersistedAccessOnlySubscriptionSigner {
    signer: Arc<dyn Signer>,
    store: Arc<dyn UpstreamStore>,
    upstream_id: Uuid,
    oauth_token_generation: u64,
}

#[async_trait]
impl Signer for PersistedAccessOnlySubscriptionSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        self.signer.sign(shaped, capability).await
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        let status = UpstreamStatusUpdate {
            last_apply_error: Some(Some("status_401".to_owned())),
            expected_oauth_token_generation: Some(self.oauth_token_generation),
            ..UpstreamStatusUpdate::default()
        };
        match self.store.set_status(self.upstream_id, status).await {
            Ok(()) | Err(StorageError::Conflict { .. }) => {}
            Err(error) => tracing::warn!(
                upstream_id = %self.upstream_id,
                %error,
                "failed to record status_401 for rejected access-only oauth credential",
            ),
        }
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        self.signer.scrub_secrets(text)
    }
}

struct SubscriptionSignerState {
    signer: Arc<dyn Signer>,
    access_token: secrecy::SecretString,
    historical_access_tokens: Vec<secrecy::SecretString>,
    expires_at_unix_secs: u64,
}

fn scrub_secret_owned(secret: &str, text: &str) -> Option<String> {
    match scrub_secret(secret, text) {
        std::borrow::Cow::Borrowed(_) => None,
        std::borrow::Cow::Owned(value) => Some(value),
    }
}

struct RefreshingSubscriptionSigner {
    state: Arc<RwLock<SubscriptionSignerState>>,
    record: UpstreamRecord,
    store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    clock: cc_lb_clock::ClockHandle,
    antigravity_config: Arc<cc_lb_config::AntigravityOAuthConfig>,
    refresh_handle: Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>,
    refresh_locks: cc_lb_signer_anthropic_oauth::RefreshLocks,
}

impl RefreshingSubscriptionSigner {
    async fn load(&self) -> Result<(UpstreamRecord, cc_lb_aead::OAuthTokenBundle), SignerError> {
        let record = self
            .store
            .get_by_id(self.record.id)
            .await
            .map_err(|_| SignerError::StorageUnavailable {
                reason: "storage unavailable".to_owned(),
            })?
            .ok_or_else(|| SignerError::MissingCredentials {
                reason: "oauth upstream not found".to_owned(),
            })?;
        if !matches!(
            record.kind,
            UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
        ) || record.deleted_at_unix_secs.is_some()
        {
            return Err(SignerError::MissingCredentials {
                reason: "oauth upstream not found".to_owned(),
            });
        }
        if !record.enabled {
            return Err(SignerError::MissingCredentials {
                reason: "oauth upstream disabled".to_owned(),
            });
        }
        if refresh_requires_reconnect(record.last_apply_error.as_deref()) {
            return Err(SignerError::MissingCredentials {
                reason: "oauth upstream requires reauthorization".to_owned(),
            });
        }
        let encrypted =
            record
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "subscription oauth upstream has no stored credential".to_owned(),
                })?;
        let tokens = encrypted
            .decrypt(&self.aead, record.id.as_bytes())
            .map_err(|_| SignerError::SigningFailed {
                reason: "oauth credentials could not be decrypted".to_owned(),
            })?;
        Ok((record, tokens))
    }

    fn update_state(
        &self,
        signer: Arc<dyn Signer>,
        tokens: &cc_lb_aead::OAuthTokenBundle,
    ) -> Arc<dyn Signer> {
        let mut state = self.state.write();
        state.signer = signer.clone();
        let previous_access_token = std::mem::replace(
            &mut state.access_token,
            secrecy::SecretString::new(tokens.access_token.clone().into_boxed_str()),
        );
        state.historical_access_tokens.push(previous_access_token);
        state.expires_at_unix_secs = tokens.expires_at_unix_secs;
        signer
    }

    async fn refresh(&self, force: bool) -> Result<Arc<dyn Signer>, SignerError> {
        let lock = cc_lb_signer_anthropic_oauth::lock_for(
            &self.refresh_locks,
            "upstream",
            &self.record.id.to_string(),
        );
        let _guard = lock.lock().await;
        let (record, tokens) = self.load().await?;
        if tokens.never_refresh {
            return Err(SignerError::ExpiredToken {
                reason: "long-lived oauth access token rejected; reauthorization required"
                    .to_owned(),
            });
        }
        let usable = tokens.expires_at_unix_secs > unix_secs(self.clock.now()).saturating_add(30);
        if usable {
            let state = self.state.read();
            let stored_token_is_newer =
                tokens.access_token.as_str() != state.access_token.expose_secret();
            let state_is_usable =
                state.expires_at_unix_secs > unix_secs(self.clock.now()).saturating_add(30);
            let current_signer = state.signer.clone();
            drop(state);
            if stored_token_is_newer {
                let signer = build_subscription_signer_with_policy(
                    &record,
                    &tokens,
                    self.clock.clone(),
                    Some(&self.antigravity_config),
                    true,
                )?;
                return Ok(self.update_state(signer, &tokens));
            }
            if !force && state_is_usable {
                return Ok(current_signer);
            }
        }
        self.refresh_handle
            .refresh_one(self.record.id)
            .await
            .map_err(|_| SignerError::SigningFailed {
                reason: "oauth refresh failed".to_owned(),
            })?;
        let (record, tokens) = self.load().await?;
        if tokens.expires_at_unix_secs <= unix_secs(self.clock.now()).saturating_add(30) {
            return Err(SignerError::ExpiredToken {
                reason: "oauth access token expired".to_owned(),
            });
        }
        let signer = build_subscription_signer_with_policy(
            &record,
            &tokens,
            self.clock.clone(),
            Some(&self.antigravity_config),
            true,
        )?;
        Ok(self.update_state(signer, &tokens))
    }
}

#[async_trait]
impl Signer for RefreshingSubscriptionSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let retry_shape = shaped.clone();
        let signer = self.state.read().signer.clone();
        match signer.sign(shaped, capability).await {
            Ok(signed) => Ok(signed),
            Err(SignerError::ExpiredToken { .. }) => {
                let refreshed = self.refresh(false).await?;
                refreshed.sign(retry_shape, capability).await
            }
            Err(error) => Err(error),
        }
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        match self.refresh(true).await {
            Ok(signer) => RetryDecision::Refresh { new_signer: signer },
            Err(_) => RetryDecision::Fail,
        }
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        let state = self.state.read();
        let signer_scrubbed = state.signer.scrub_secrets(text);
        let mut owned = match signer_scrubbed {
            std::borrow::Cow::Borrowed(_) => None,
            std::borrow::Cow::Owned(value) => Some(value),
        };
        for access_token in
            std::iter::once(&state.access_token).chain(state.historical_access_tokens.iter())
        {
            let input = owned.as_deref().unwrap_or(text);
            if let Some(value) = scrub_secret_owned(access_token.expose_secret(), input) {
                owned = Some(value);
            }
        }
        owned
            .map(std::borrow::Cow::Owned)
            .unwrap_or(std::borrow::Cow::Borrowed(text))
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
        ) | (
            UpstreamKind::OpenaiApiKey | UpstreamKind::OpenaiOauth,
            Upstream::OpenaiDirect { .. }
        ) | (
            UpstreamKind::GeminiApiKey | UpstreamKind::GeminiOauth,
            Upstream::GeminiDirect { .. }
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use cc_lb_clock::TestClock;
    use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
    use cc_lb_engine::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    use cc_lb_storage_api::{MetaStore, PromptCacheObservationRecord, UpstreamCreate};
    use cc_lb_storage_sqlite::SqliteStorage as Storage;
    use tokio::sync::Notify;

    use super::*;
    use crate::prompt_cache_observation_sink::{
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
    };

    const MODEL: &str = "claude-sonnet-4-5-20250929";

    fn provider_test_section(bytes: &mut Vec<u8>, name: &str, data: &[u8]) {
        fn leb(bytes: &mut Vec<u8>, mut value: usize) {
            loop {
                let byte = (value & 0x7f) as u8;
                value >>= 7;
                bytes.push(if value == 0 { byte } else { byte | 0x80 });
                if value == 0 {
                    break;
                }
            }
        }
        let mut payload = Vec::new();
        leb(&mut payload, name.len());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(data);
        bytes.push(0);
        leb(bytes, payload.len());
        bytes.extend_from_slice(&payload);
    }

    fn legacy_filter_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;
        let output = rkyv::to_bytes::<rkyv::rancor::Error>(&cc_lb_plugin_wire::FilterResponse {
            results: Box::new([cc_lb_plugin_wire::PerCandidateReason {
                upstream_id: Box::from("00000000-0000-0000-0000-000000000001"),
                decision: Box::from("reject"),
                reason: Box::from("legacy-ran"),
            }]),
        })
        .expect("filter response");
        let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
        let mut wasm = wat::parse_str(format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{data}\") (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) (func (export \"cc_lb_free\") (param i32 i32 i32)) (func (export \"cc_lb_filter\") (param i32 i32) (result i64) i64.const {}))",
            packed as i64,
        )).expect("filter module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.filter.v1",
            &<cc_lb_plugin_wire::FilterRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(&mut wasm, "cc_lb.plugin.v1", br#"{
            "name":"legacy-provider-filter","version":"1.0.0","description":"legacy filter","usage":"test",
            "hooks":{"filter":{"wire_version":1,"description":"legacy","usage":"test","mode":"active"}}
        }"#);
        wasm
    }

    fn openai_response_only_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;
        let output = rkyv::to_bytes::<rkyv::rancor::Error>(
            &cc_lb_plugin_wire::TransformResponseResult::Unchanged,
        )
        .expect("response transform");
        let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
        let mut wasm = wat::parse_str(format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{data}\") (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) (func (export \"cc_lb_free\") (param i32 i32 i32)) (func (export \"cc_lb_transform_response_openai\") (param i32 i32) (result i64) i64.const {}))",
            packed as i64,
        )).expect("response module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.transform_response.v1",
            &<cc_lb_plugin_wire::TransformResponseRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(&mut wasm, "cc_lb.plugin.v2", br#"{
            "name":"openai-response-only","version":"1.0.0","description":"OpenAI response","usage":"test",
            "hooks":{"transform_response_openai":{"wire_version":1,"description":"response","usage":"test","mode":"active"}}
        }"#);
        wasm
    }
    fn frame_only_provider_fixture(
        provider: cc_lb_plugin_wire::schema::ProviderFamily,
        noop: bool,
    ) -> Vec<u8> {
        use cc_lb_plugin_wire::schema::{HookKind, WireSchema};

        let output = rkyv::to_bytes::<rkyv::rancor::Error>(
            &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        )
        .expect("realtime frame transform");
        let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
        let export = HookKind::TransformRealtimeFrame.qualified_export_name(provider);
        let provider_name = match provider {
            cc_lb_plugin_wire::schema::ProviderFamily::Openai => "openai",
            cc_lb_plugin_wire::schema::ProviderFamily::Gemini => "gemini",
            cc_lb_plugin_wire::schema::ProviderFamily::Anthropic => "anthropic",
        };
        let plugin_name = if noop {
            format!("{provider_name}-frame-only-noop")
        } else {
            format!("{provider_name}-frame-only")
        };
        let mut wasm = wat::parse_str(format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{data}\") (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) (func (export \"cc_lb_free\") (param i32 i32 i32)) (func (export \"{export}\") (param i32 i32) (result i64) i64.const {packed}))",
        ))
        .expect("realtime frame module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.transform_realtime_frame.v2",
            &<cc_lb_plugin_wire::v2::TransformRealtimeFrameRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut wasm,
            "cc_lb.plugin.v2",
            format!(
                r#"{{
            "name":"{plugin_name}","version":"1.0.0","description":"{plugin_name}","usage":"test",
            "hooks":{{"transform_realtime_frame_{provider_name}":{{"wire_version":2,"description":"frame","usage":"test","mode":"{}"}}}}
        }}"#,
                if noop { "noop" } else { "active" }
            )
            .as_bytes(),
        );
        wasm
    }

    async fn install_shape_plugin(
        storage: &Storage,
        principal_id: Uuid,
        wasm: Vec<u8>,
        sha256: [u8; 32],
        filename: &str,
    ) {
        use cc_lb_storage_api::{PluginChainEntryInput, WasmBlob, WasmRegistryEntryInput};

        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("frame inspection")
            .metadata;
        let (registry, _) = storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256,
                    size_bytes: wasm.len() as u64,
                    bytes: wasm,
                },
                WasmRegistryEntryInput {
                    name: metadata.name,
                    version: Some(metadata.version),
                    original_filename: filename.to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: metadata.description,
                    usage: metadata.usage,
                    hook_metadata: metadata.hooks,
                    supported_slots: vec![PluginSlotKind::Shape],
                },
            )
            .await
            .expect("frame registry");
        storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id,
                slot: PluginSlotKind::Shape,
                order: 1000,
                wasm_registry_id: registry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("frame chain entry");
    }

    async fn create_machine_principal(storage: &Storage, name: &str) -> PrincipalRecord {
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
        .expect("principal")
    }

    #[tokio::test]
    async fn real_principal_chains_select_provider_hooks_and_preserve_native_response_only_shaping()
    {
        use cc_lb_storage_api::{PluginChainEntryInput, WasmBlob, WasmRegistryEntryInput};
        let (dir, storage) = storage_fixture(40).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
        let principal = PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::PrincipalCreate {
                name: "provider-chain".to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await
        .expect("principal");
        let anthropic = create_upstream(storage.as_ref(), "anthropic-provider-chain").await;
        let openai = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "openai-provider-chain".to_owned(),
                kind: UpstreamKind::OpenaiOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("OpenAI upstream");
        let openai = UpstreamStore::set_enabled(storage.as_ref(), openai.id, openai.revision, true)
            .await
            .expect("OpenAI enabled");
        let anthropic =
            UpstreamStore::set_enabled(storage.as_ref(), anthropic.id, anthropic.revision, true)
                .await
                .expect("Anthropic enabled");
        let wasm = legacy_filter_provider_fixture();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("inspection")
            .metadata;
        let (registry, _) = storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256: [240; 32],
                    size_bytes: wasm.len() as u64,
                    bytes: wasm,
                },
                WasmRegistryEntryInput {
                    name: metadata.name,
                    version: Some(metadata.version),
                    original_filename: "legacy-provider-filter.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: metadata.description,
                    usage: metadata.usage,
                    hook_metadata: metadata.hooks,
                    supported_slots: vec![PluginSlotKind::Router],
                },
            )
            .await
            .expect("registry");
        let entry = storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Router,
                order: 2000,
                wasm_registry_id: registry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("chain entry");
        let wasm = openai_response_only_provider_fixture();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("response inspection")
            .metadata;
        let (registry, _) = storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256: [241; 32],
                    size_bytes: wasm.len() as u64,
                    bytes: wasm,
                },
                WasmRegistryEntryInput {
                    name: metadata.name,
                    version: Some(metadata.version),
                    original_filename: "openai-response-only.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: metadata.description,
                    usage: metadata.usage,
                    hook_metadata: metadata.hooks,
                    supported_slots: vec![PluginSlotKind::Shape],
                },
            )
            .await
            .expect("response registry");
        storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Shape,
                order: 1000,
                wasm_registry_id: registry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("response chain entry");
        let upstreams = [anthropic, openai];
        let (chains, _) = build_principal_chains(
            &stores,
            &runtime,
            dir.path(),
            std::slice::from_ref(&principal),
            &upstreams,
            &AeadService::from_master_key([19; 32]),
        )
        .await
        .expect("real principal chains");
        let principal_view = PrincipalView::from_db(std::slice::from_ref(&principal), chains);
        let pipeline = principal_view
            .get(&principal.name)
            .expect("principal view")
            .resolved_pipeline(None);
        assert!(pipeline.instantiation_error.is_none());
        let identity = cc_lb_domain::Principal {
            id: principal.name.clone(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
        };
        for (provider, path, body, expected) in [
            (
                Provider::Anthropic,
                "/v1/messages",
                &br#"{"model":"claude-test","messages":[]}"#[..],
                true,
            ),
            (
                Provider::Openai,
                "/v1/responses",
                &br#"{"model":"gpt-test","input":"hi"}"#[..],
                false,
            ),
        ] {
            let context = cc_lb_routing::RoutingContext {
                request_id: "provider-chain-request".to_owned(),
                thread_id: None,
                requested_service_tier: None,
                downstream_headers: http::HeaderMap::new(),
                method: http::Method::POST,
                path: path.to_owned(),
                query: None,
                body_bytes: bytes::Bytes::copy_from_slice(body),
                canonical_model_id: "test-model".to_owned(),
                cache_pricing: cc_lb_domain::CachePricingSummary::default(),
            };
            let mut candidates = vec![cc_lb_domain::UpstreamCandidate {
                upstream_id: Uuid::from_u128(1),
                name: "provider-candidate".to_owned(),
                kind: match provider {
                    Provider::Anthropic => cc_lb_domain::UpstreamKind::AnthropicApiKey,
                    Provider::Openai => cc_lb_domain::UpstreamKind::OpenaiOauth,
                    Provider::Gemini => cc_lb_domain::UpstreamKind::GeminiApiKey,
                },
                subscription_quotas: Vec::new(),
                observed_at_unix_secs: 1,
                cache_score: None,
                base_url: None,
                plan_capacity_ratio: None,
                organization_type: None,
                rate_limit_tier: None,
                seat_tier: None,
            }];
            let mut ran = false;
            for filter in pipeline.filters_for(provider) {
                let output = filter
                    .filter(&context, &identity, &candidates)
                    .expect("provider filter executes");
                candidates
                    .retain(|candidate| output.kept_upstream_ids.contains(&candidate.upstream_id));
                if filter.plugin_id() == entry.id {
                    assert!(output.reason.contains("legacy-ran"));
                    ran = true;
                }
            }
            assert_eq!(ran, expected, "provider {provider:?}");
            assert_eq!(
                candidates.is_empty(),
                expected,
                "only the Anthropic chain applies the legacy rejection"
            );
        }
        let openai = upstreams
            .iter()
            .find(|record| record.kind == UpstreamKind::OpenaiOauth)
            .expect("OpenAI record");
        let spec = principal_view.get(&principal.name).expect("principal");
        let dialect = spec
            .resolved_dialect(openai.id)
            .expect("prebuilt response-only dialect");
        let context = cc_lb_upstream::DialectShapeContext {
            request_id: "response-only-request".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: http::Method::POST,
            path: "/v1/responses".to_owned(),
            query: Some("key=secret&stream=false".to_owned()),
            body_bytes: bytes::Bytes::from_static(br#"{"model":"gpt-test","input":"hi"}"#),
        };
        let upstream = cc_lb_domain::Upstream::OpenaiDirect { base_url: None };
        let shaped =
            cc_lb_upstream::shape_request(dialect.as_ref(), &context, &upstream, &identity).expect(
                "response-only plugin uses native request shaping without a guest request hook",
            );
        assert_eq!(
            shaped.url().as_str(),
            "https://chatgpt.com/backend-api/codex/responses?stream=false"
        );
        assert_eq!(shaped.body(), &context.body_bytes);
        assert!(dialect.sse_event_transform_hook().is_none());
        let response = dialect
            .response_transform_hook()
            .expect("buffered response hook")
            .transform_response(cc_lb_upstream::TransformResponseRequest {
                request_id: context.request_id.clone(),
                principal: identity.clone(),
                upstream,
                request_method: context.method.clone(),
                request_path: context.path.clone(),
                canonical_model_id: "gpt-test".to_owned(),
                response_status: http::StatusCode::OK,
                response_headers: http::HeaderMap::new(),
                body: bytes::Bytes::from_static(br#"{"output":[]}"#),
            })
            .expect("independent qualified response export");
        assert_eq!(response, cc_lb_upstream::TransformResponseResult::Unchanged);
        let anthropic = upstreams
            .iter()
            .find(|record| record.kind == UpstreamKind::AnthropicApiKey)
            .expect("Anthropic record");
        assert!(
            spec.resolved_dialect(anthropic.id).is_none(),
            "unsupported Shape provider selects no plugin dialect"
        );
    }
    #[tokio::test]
    async fn frame_only_shape_builds_provider_dialects_with_native_request_fallback() {
        let (dir, storage) = storage_fixture(41).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
        let openai_principal = create_machine_principal(&storage, "openai-frame-chain").await;
        let gemini_principal = create_machine_principal(&storage, "gemini-frame-chain").await;
        let noop_principal = create_machine_principal(&storage, "noop-frame-chain").await;

        let anthropic = create_upstream(storage.as_ref(), "anthropic-frame-chain").await;
        let openai = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "openai-frame-upstream".to_owned(),
                kind: UpstreamKind::OpenaiOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("OpenAI upstream");
        let openai = UpstreamStore::set_enabled(storage.as_ref(), openai.id, openai.revision, true)
            .await
            .expect("OpenAI enabled");
        let gemini = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "gemini-frame-upstream".to_owned(),
                kind: UpstreamKind::GeminiApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("Gemini upstream");
        let gemini = UpstreamStore::set_enabled(storage.as_ref(), gemini.id, gemini.revision, true)
            .await
            .expect("Gemini enabled");
        let anthropic =
            UpstreamStore::set_enabled(storage.as_ref(), anthropic.id, anthropic.revision, true)
                .await
                .expect("Anthropic enabled");

        install_shape_plugin(
            &storage,
            openai_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Openai, false),
            [242; 32],
            "openai-frame-only.wasm",
        )
        .await;
        install_shape_plugin(
            &storage,
            gemini_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Gemini, false),
            [243; 32],
            "gemini-frame-only.wasm",
        )
        .await;
        install_shape_plugin(
            &storage,
            noop_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Openai, true),
            [244; 32],
            "openai-frame-only-noop.wasm",
        )
        .await;

        let anthropic_id = anthropic.id;
        let openai_id = openai.id;
        let gemini_id = gemini.id;
        let principals = vec![openai_principal, gemini_principal, noop_principal];
        let upstreams = vec![anthropic, openai, gemini];
        let (chains, _) = build_principal_chains(
            &stores,
            &runtime,
            dir.path(),
            &principals,
            &upstreams,
            &AeadService::from_master_key([19; 32]),
        )
        .await
        .expect("frame principal chains");
        let principal_view = PrincipalView::from_db(&principals, chains);

        let identity = cc_lb_domain::Principal {
            id: "frame-principal".to_owned(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
        };
        let openai_spec = principal_view
            .get("openai-frame-chain")
            .expect("OpenAI principal");
        let openai_dialect = openai_spec
            .resolved_dialect(openai_id)
            .expect("frame-only OpenAI dialect");
        assert!(openai_dialect.realtime_frame_transform_hook().is_some());
        assert!(openai_dialect.response_transform_hook().is_none());
        assert!(openai_dialect.sse_event_transform_hook().is_none());
        assert!(openai_spec.resolved_dialect(gemini_id).is_none());
        assert!(openai_spec.resolved_dialect(anthropic_id).is_none());
        let openai_context = cc_lb_upstream::DialectShapeContext {
            request_id: "openai-frame-only-request".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: http::Method::POST,
            path: "/v1/responses".to_owned(),
            query: Some("stream=false".to_owned()),
            body_bytes: bytes::Bytes::from_static(br#"{"model":"gpt-test","input":"hi"}"#),
        };
        let openai_shaped = cc_lb_upstream::shape_request(
            openai_dialect.as_ref(),
            &openai_context,
            &cc_lb_domain::Upstream::OpenaiDirect { base_url: None },
            &identity,
        )
        .expect("OpenAI frame-only plugin uses native request shaping");
        assert_eq!(
            openai_shaped.url().as_str(),
            "https://chatgpt.com/backend-api/codex/responses?stream=false"
        );
        assert_eq!(openai_shaped.body(), &openai_context.body_bytes);

        let gemini_spec = principal_view
            .get("gemini-frame-chain")
            .expect("Gemini principal");
        let gemini_dialect = gemini_spec
            .resolved_dialect(gemini_id)
            .expect("frame-only Gemini dialect");
        assert!(gemini_dialect.realtime_frame_transform_hook().is_some());
        assert!(gemini_dialect.response_transform_hook().is_none());
        assert!(gemini_dialect.sse_event_transform_hook().is_none());
        assert!(gemini_spec.resolved_dialect(openai_id).is_none());
        assert!(gemini_spec.resolved_dialect(anthropic_id).is_none());
        let gemini_context = cc_lb_upstream::DialectShapeContext {
            request_id: "gemini-frame-only-request".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: http::Method::POST,
            path: "/v1beta/models/gemini-2.5-flash:generateContent".to_owned(),
            query: None,
            body_bytes: bytes::Bytes::from_static(br#"{"contents":[]}"#),
        };
        let gemini_shaped = cc_lb_upstream::shape_request(
            gemini_dialect.as_ref(),
            &gemini_context,
            &cc_lb_domain::Upstream::GeminiDirect { base_url: None },
            &identity,
        )
        .expect("Gemini frame-only plugin uses native request shaping");
        assert_eq!(
            gemini_shaped.url().as_str(),
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        );
        assert_eq!(gemini_shaped.body(), &gemini_context.body_bytes);

        let noop_spec = principal_view
            .get("noop-frame-chain")
            .expect("noop principal");
        assert!(noop_spec.resolved_dialect(openai_id).is_none());
        assert!(noop_spec.resolved_dialect(gemini_id).is_none());
        assert!(noop_spec.resolved_dialect(anthropic_id).is_none());
    }

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
                id: Uuid::new_v4(),
                name: "stale-missing-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
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
                account_id: None,
                project_id: None,
                oauth_profile: Default::default(),
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
                    id: Uuid::new_v4(),
                    name: format!("stale-oauth-{enabled}"),
                    kind: UpstreamKind::AnthropicOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
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
                    account_id: None,
                    project_id: None,
                    oauth_profile: Default::default(),
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
            RouterPipelineBuildContext {
                stores: &stores,
                runtime: &runtime,
                data_dir: dir.path(),
                registry: &registry,
            },
            &principal,
            router_entries,
            &mut slot_keys,
            &[Provider::Anthropic, Provider::Openai, Provider::Gemini],
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
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
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
            config.oauth.antigravity.as_ref(),
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

    #[test]
    fn upstream_identity_preserves_record_fields_for_all_six_credential_kinds() {
        let mut tokens = oauth_bundle("not-exported-access", "not-exported-refresh");
        tokens.oauth_profile = cc_lb_aead::GeminiOAuthProfile::Antigravity;
        tokens.project_id = Some("stored-project".to_owned());
        for kind in [
            UpstreamKind::AnthropicApiKey,
            UpstreamKind::AnthropicOauth,
            UpstreamKind::OpenaiApiKey,
            UpstreamKind::OpenaiOauth,
            UpstreamKind::GeminiApiKey,
            UpstreamKind::GeminiOauth,
        ] {
            let record = UpstreamRecord {
                id: Uuid::from_u128(42),
                name: "operator-selected-name".to_owned(),
                kind,
                base_url: Some(
                    "https://operator.example/custom-prefix"
                        .parse()
                        .expect("URL"),
                ),
                ..UpstreamRecord::default()
            };
            let identity = upstream_identity(&record, Some(&tokens));
            assert_eq!(identity.provider, kind.provider());
            assert_eq!(identity.credential_kind, kind.as_str());
            assert_eq!(identity.upstream_id, record.id.to_string());
            assert_eq!(identity.name, record.name);
            assert_eq!(
                identity.base_url.as_deref(),
                Some("https://operator.example/custom-prefix")
            );
            if kind == UpstreamKind::GeminiOauth {
                assert_eq!(
                    identity.gemini_oauth_profile.as_deref(),
                    Some("antigravity")
                );
                assert_eq!(
                    identity.code_assist_project_id.as_deref(),
                    Some("stored-project")
                );
            } else {
                assert!(identity.gemini_oauth_profile.is_none());
                assert!(identity.code_assist_project_id.is_none());
            }
        }
    }

    #[test]
    fn upstream_identity_does_not_infer_missing_gemini_metadata() {
        let record = UpstreamRecord {
            kind: UpstreamKind::GeminiOauth,
            name: "antigravity-is-only-a-name".to_owned(),
            base_url: Some(
                "https://daily-cloudcode-pa.googleapis.com"
                    .parse()
                    .expect("URL"),
            ),
            ..UpstreamRecord::default()
        };
        let missing = upstream_identity(&record, None);
        assert!(missing.gemini_oauth_profile.is_none());
        assert!(missing.code_assist_project_id.is_none());
        let tokens = oauth_bundle("access", "refresh");
        let present = upstream_identity(&record, Some(&tokens));
        assert_eq!(present.gemini_oauth_profile.as_deref(), Some("gemini_cli"));
        assert!(present.code_assist_project_id.is_none());
    }

    struct DirectDialect;

    impl cc_lb_upstream::UpstreamDialect for DirectDialect {
        fn shape(
            &self,
            _context: &cc_lb_upstream::DialectShapeContext,
            _upstream: &Upstream,
            _principal: &cc_lb_domain::Principal,
            builder: &mut cc_lb_upstream::ShapedRequestBuilder,
        ) -> Result<cc_lb_upstream::ShapedRequest, cc_lb_upstream::DialectError> {
            Ok(builder.shaped_request(
                "https://api.anthropic.com/v1/messages"
                    .parse()
                    .expect("url"),
                http::Method::POST,
                http::HeaderMap::new(),
                bytes::Bytes::from_static(b"{}"),
            ))
        }
    }

    fn shaped_request() -> cc_lb_upstream::ShapedRequest {
        let context = cc_lb_upstream::DialectShapeContext {
            request_id: "req-1".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: http::Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: bytes::Bytes::from_static(b"{}"),
        };
        let principal = cc_lb_domain::Principal {
            id: "principal".to_owned(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
        };
        cc_lb_upstream::shape_request(
            &DirectDialect,
            &context,
            &Upstream::AnthropicDirect { base_url: None },
            &principal,
        )
        .expect("shape request")
    }

    #[tokio::test]
    async fn gemini_signer_profile_comes_from_encrypted_credentials() {
        let (_dir, storage) = storage_fixture(27).await;
        let stored = create_upstream(&storage, "gemini-profile").await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        for (profile, configured_endpoint, override_endpoint, expected_host) in [
            (
                cc_lb_aead::GeminiOAuthProfile::GeminiCli,
                None,
                None,
                "cloudcode-pa.googleapis.com",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                None,
                None,
                "daily-cloudcode-pa.googleapis.com",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                None,
                Some("https://cloudcode-pa.googleapis.com/v1internal"),
                "daily-cloudcode-pa.googleapis.com",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                Some("https://configured.invalid/"),
                None,
                "configured.invalid",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                Some("https://configured.invalid/"),
                Some("http://override.invalid/"),
                "override.invalid",
            ),
        ] {
            let mut record = stored.clone();
            record.kind = UpstreamKind::GeminiOauth;
            record.base_url = override_endpoint.map(|url| url.parse().expect("override URL"));
            let mut bundle = oauth_bundle("gemini-profile-access", "gemini-profile-refresh");
            bundle.oauth_profile = profile;
            bundle.project_id = Some("project-123".to_owned());
            record.oauth_credentials = Some(
                cc_lb_aead::EncryptedOAuthTokens::encrypt(
                    aead.as_ref(),
                    &bundle,
                    record.id.as_bytes(),
                )
                .expect("encrypted credential"),
            );
            let mut config = cc_lb_config::AntigravityOAuthConfig {
                user_agent: "antigravity/custom-config".to_owned(),
                ..Default::default()
            };
            if let Some(endpoint) = configured_endpoint {
                config.code_assist_url = endpoint.parse().expect("configured URL");
            }
            let factory = DbCompositeSignerFactory::new(
                vec![record],
                storage.clone(),
                aead.clone(),
                None,
                Arc::new(TestClock::new_at_secs(1_700_000_000)),
                config,
            )
            .with_router_choice_state(stored.name.clone());
            let signer = factory
                .build(&Upstream::GeminiDirect { base_url: None })
                .await
                .expect("Gemini signer");
            let mut shaped = shaped_request();
            shaped.set_url(
                "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                    .parse()
                    .expect("request URL"),
            );
            shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
            shaped.headers_mut().insert(
                http::header::USER_AGENT,
                http::HeaderValue::from_static("GeminiCLI/existing"),
            );
            let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped)
                .await
                .expect("signed Gemini request");
            assert_eq!(signed.url().host_str(), Some(expected_host));
            let body: serde_json::Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["project"], "project-123");
            match profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                    assert!(body.get("userAgent").is_none());
                    assert!(body.get("requestId").is_none());
                    assert_eq!(
                        signed.headers().get(http::header::USER_AGENT),
                        Some(&http::HeaderValue::from_static("GeminiCLI/existing"))
                    );
                }
                cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                    assert_eq!(body["userAgent"], "antigravity");
                    assert_eq!(body["requestType"], "agent");
                    assert_eq!(
                        signed.headers().get(http::header::USER_AGENT),
                        Some(&http::HeaderValue::from_static("antigravity/custom-config"))
                    );
                }
            }
        }
    }
    #[tokio::test]
    async fn access_only_oauth_signer_rechecks_expiry_before_mutating_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_dir, storage) = storage_fixture(26).await;
        let stored = create_upstream(&storage, "access-only-expiry").await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
            let mut record = stored.clone();
            record.kind = kind;
            record.oauth_never_refresh = true;
            let mut bundle = oauth_bundle("access-only-test-token", "");
            bundle.never_refresh = true;
            bundle.expires_at_unix_secs = 1_700_000_001;
            bundle.project_id =
                (kind == UpstreamKind::GeminiOauth).then(|| "test-project".to_owned());
            record.oauth_credentials = Some(cc_lb_aead::EncryptedOAuthTokens::encrypt(
                aead.as_ref(),
                &bundle,
                record.id.as_bytes(),
            )?);
            let factory = DbCompositeSignerFactory::new(
                vec![record],
                storage.clone(),
                aead.clone(),
                None,
                clock.clone(),
                cc_lb_config::AntigravityOAuthConfig::default(),
            )
            .with_router_choice_state(stored.name.clone());
            let upstream = match kind {
                UpstreamKind::OpenaiOauth => Upstream::OpenaiDirect { base_url: None },
                UpstreamKind::GeminiOauth => Upstream::GeminiDirect { base_url: None },
                _ => return Err("unexpected provider kind".into()),
            };
            let signer = factory.build(&upstream).await?;
            let mut fresh_request = shaped_request();
            fresh_request.headers_mut().insert(
                "x-request-id",
                http::HeaderValue::from_static("access-only-expiry-request"),
            );
            let fresh = cc_lb_upstream::sign_request(signer.as_ref(), fresh_request).await?;
            assert_eq!(fresh.body(), &bytes::Bytes::from_static(b"{}"));
            assert_eq!(
                fresh.headers().get(http::header::AUTHORIZATION),
                Some(&http::HeaderValue::from_static(
                    "Bearer access-only-test-token"
                ))
            );
            assert!(
                fresh
                    .headers()
                    .get(http::header::AUTHORIZATION)
                    .is_some_and(http::HeaderValue::is_sensitive)
            );
            assert_eq!(
                fresh.headers().get("x-request-id"),
                Some(&http::HeaderValue::from_static(
                    "access-only-expiry-request"
                ))
            );

            let generate_url = "https://cloudcode-pa.googleapis.com/v1internal:generateContent";
            let mut malformed_request = shaped_request();
            malformed_request.set_url(generate_url.parse()?);
            malformed_request.set_body(bytes::Bytes::from_static(b"not-json"));
            let parse_failure = if kind == UpstreamKind::GeminiOauth {
                match cc_lb_upstream::sign_request(signer.as_ref(), malformed_request).await {
                    Err(SignerError::SigningFailed { reason }) => Some(reason),
                    result => {
                        return Err(format!(
                            "fresh malformed Gemini body must fail parsing: {result:?}"
                        )
                        .into());
                    }
                }
            } else {
                None
            };

            clock.set_unix_secs(bundle.expires_at_unix_secs);
            let mut valid_expired_request = shaped_request();
            valid_expired_request.set_url(generate_url.parse()?);
            valid_expired_request.set_body(bytes::Bytes::from_static(
                br#"{"model":"gemini-test","contents":[]}"#,
            ));
            let valid_expired =
                cc_lb_upstream::sign_request(signer.as_ref(), valid_expired_request).await;
            let mut malformed_expired_request = shaped_request();
            malformed_expired_request.set_url(generate_url.parse()?);
            malformed_expired_request.set_body(bytes::Bytes::from_static(b"not-json"));
            let malformed_expired =
                cc_lb_upstream::sign_request(signer.as_ref(), malformed_expired_request).await;
            match (valid_expired, malformed_expired) {
                (
                    Err(SignerError::SigningFailed { reason: valid_reason }),
                    Err(SignerError::SigningFailed { reason: malformed_reason }),
                ) => {
                    assert_eq!(valid_reason, malformed_reason);
                    if let Some(parse_reason) = parse_failure {
                        assert_ne!(malformed_reason, parse_reason);
                    }
                }
                results => return Err(format!(
                    "the cached signer must reject both expired requests before body parsing: {results:?}"
                ).into()),
            }
        }
        Ok(())
    }

    /// SGN-03: the composite factory builds the API-key signer from the
    /// record's own `workspace_id`.
    #[tokio::test]
    async fn sgn_03_api_key_signer_carries_record_workspace() {
        let (_dir, storage) = storage_fixture(23).await;
        let stored = create_upstream(&storage, "workspace-key").await;
        for workspace_id in [Some("wrkspc_P"), None] {
            let mut record = stored.clone();
            record.workspace_id = workspace_id.map(str::to_owned);
            let factory = DbCompositeSignerFactory::new(
                vec![record],
                storage.clone(),
                Arc::new(AeadService::from_master_key([19; 32])),
                None,
                Arc::new(TestClock::new_at_secs(1_700_000_000)),
                cc_lb_config::AntigravityOAuthConfig::default(),
            )
            .with_router_choice_state("workspace-key".to_owned());

            let signer = factory
                .build(&Upstream::AnthropicDirect { base_url: None })
                .await
                .expect("signer builds");
            let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
                .await
                .expect("request signs");

            let header = |name: &str| {
                signed
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
            };
            assert_eq!(
                header(cc_lb_domain::ANTHROPIC_WORKSPACE_ID_HEADER).as_deref(),
                workspace_id
            );
            assert_eq!(
                header("x-api-key").as_deref(),
                Some("sk-ant-fixture-secret")
            );
        }
    }

    /// Rate-limit store wrapper whose first `list_for_upstream_ids` call,
    /// made by `build_dynamic_view` after it sampled the revision hash and
    /// listed upstreams, first persists a reconnect-required refresh failure.
    /// This deterministically lands a runtime 401 after the build's hash
    /// sample and upstream read.
    struct ReconnectMidBuildRateLimits {
        storage: Arc<Storage>,
        upstream_id: Uuid,
        expected_oauth_token_generation: u64,
        injected: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl UpstreamRateLimitStateStore for ReconnectMidBuildRateLimits {
        async fn put_observation(
            &self,
            record: &UpstreamRateLimitObservationRecord,
        ) -> StorageResult<()> {
            UpstreamRateLimitStateStore::put_observation(self.storage.as_ref(), record).await
        }

        async fn list_for_upstream_ids(
            &self,
            upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
            if !self.injected.swap(true, Ordering::SeqCst) {
                UpstreamStore::set_status(
                    self.storage.as_ref(),
                    self.upstream_id,
                    UpstreamStatusUpdate {
                        last_apply_error: Some(Some("status_401".to_owned())),
                        expected_oauth_token_generation: Some(self.expected_oauth_token_generation),
                        ..UpstreamStatusUpdate::default()
                    },
                )
                .await?;
            }
            UpstreamRateLimitStateStore::list_for_upstream_ids(self.storage.as_ref(), upstream_ids)
                .await
        }
    }

    #[tokio::test]
    async fn reconnect_required_write_during_build_triggers_next_reconcile_rebuild() {
        let (dir, storage) = storage_fixture(24).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let created = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "mid-build-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
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
                account_id: None,
                project_id: None,
                oauth_profile: Default::default(),
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
        let installed =
            UpstreamStore::set_enabled(storage.as_ref(), stored.id, stored.revision, true)
                .await
                .expect("oauth upstream enabled");
        let mut stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        stores.upstream_rate_limits = Arc::new(ReconnectMidBuildRateLimits {
            storage: storage.clone(),
            upstream_id: installed.id,
            expected_oauth_token_generation: installed.oauth_token_generation,
            injected: std::sync::atomic::AtomicBool::new(false),
        });
        let stores = Arc::new(stores);
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

        let raced = build_view(&stores, &runtime, dir.path()).await;
        let raced_status = raced
            .upstream_status_snapshot
            .entries
            .get(&installed.name)
            .expect("raced status");
        assert_eq!(
            raced_status.last_apply_error, None,
            "the build must have installed the pre-failure record"
        );
        let persisted = UpstreamStore::get_by_id(storage.as_ref(), installed.id)
            .await
            .expect("load upstream")
            .expect("upstream exists");
        assert_eq!(persisted.last_apply_error.as_deref(), Some("status_401"));
        assert_ne!(
            raced.upstream_status_snapshot.revision_hash,
            crate::reconcile::collect_revision_hash(&stores)
                .await
                .expect("current hash"),
            "the installed hash must be the pre-content sample, not include the mid-build write"
        );

        let holder = Arc::new(cc_lb_control::DynamicViewHolder::new(Arc::clone(&raced)));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let reconciler = crate::reconcile::Reconciler::new(
            stores.clone(),
            holder.clone(),
            runtime,
            aead,
            None,
            tokio_util::sync::CancellationToken::new(),
            dir.path().to_path_buf(),
            Arc::new(SubscriptionQuotaCache::new()),
            0,
            Arc::new(sink),
            1800,
            clock,
            None,
        );

        reconciler.reconcile_once().await.expect("reconcile runs");
        let rebuilt = holder.load();
        assert_eq!(
            rebuilt.generation,
            raced.generation + 1,
            "reconcile must rebuild the view that missed the mid-build write"
        );
        assert_eq!(
            rebuilt
                .upstream_status_snapshot
                .entries
                .get(&installed.name)
                .expect("rebuilt status")
                .last_apply_error
                .as_deref(),
            Some("status_401")
        );

        reconciler.reconcile_once().await.expect("reconcile reruns");
        assert_eq!(
            holder.generation(),
            rebuilt.generation,
            "a view built from current records must not rebuild again"
        );
    }

    const PRINCIPAL_LIST_PASSTHROUGH: u8 = 0;
    const PRINCIPAL_LIST_INJECT_RECONNECT_REQUIRED: u8 = 1;
    const PRINCIPAL_LIST_FAIL: u8 = 2;

    /// Principal store wrapper that scripts its next `list` call. The
    /// revision hash lists principals right after its upstream read, so an
    /// armed injection persists a reconnect-required refresh failure between
    /// a build's hash sample and its upstream content read, and an armed
    /// failure aborts the next build with one transient storage error.
    struct ScriptedPrincipals {
        storage: Arc<Storage>,
        upstream_id: Uuid,
        expected_oauth_token_generation: u64,
        next_list: std::sync::atomic::AtomicU8,
    }

    #[async_trait]
    impl PrincipalStore for ScriptedPrincipals {
        async fn create(
            &self,
            input: cc_lb_storage_api::PrincipalCreate,
            now_unix_secs: u64,
        ) -> StorageResult<PrincipalRecord> {
            PrincipalStore::create(self.storage.as_ref(), input, now_unix_secs).await
        }

        async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
            PrincipalStore::get_by_id(self.storage.as_ref(), id).await
        }

        async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
            PrincipalStore::get_by_name(self.storage.as_ref(), name).await
        }

        async fn list(
            &self,
            offset: usize,
            limit: usize,
            include_deleted: bool,
        ) -> StorageResult<Vec<PrincipalRecord>> {
            match self
                .next_list
                .swap(PRINCIPAL_LIST_PASSTHROUGH, Ordering::SeqCst)
            {
                PRINCIPAL_LIST_INJECT_RECONNECT_REQUIRED => {
                    UpstreamStore::set_status(
                        self.storage.as_ref(),
                        self.upstream_id,
                        UpstreamStatusUpdate {
                            last_apply_error: Some(Some("status_401".to_owned())),
                            expected_oauth_token_generation: Some(
                                self.expected_oauth_token_generation,
                            ),
                            ..UpstreamStatusUpdate::default()
                        },
                    )
                    .await?;
                }
                PRINCIPAL_LIST_FAIL => {
                    return Err(StorageError::Unavailable {
                        message: "injected transient principal read failure".to_owned(),
                    });
                }
                _ => {}
            }
            PrincipalStore::list(self.storage.as_ref(), offset, limit, include_deleted).await
        }

        async fn update(
            &self,
            id: Uuid,
            expected_revision: u64,
            update: cc_lb_storage_api::PrincipalUpdate,
            now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            PrincipalStore::update(
                self.storage.as_ref(),
                id,
                expected_revision,
                update,
                now_unix_secs,
            )
            .await
        }

        async fn set_enabled(
            &self,
            id: Uuid,
            expected_revision: u64,
            enabled: bool,
            now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            PrincipalStore::set_enabled(
                self.storage.as_ref(),
                id,
                expected_revision,
                enabled,
                now_unix_secs,
            )
            .await
        }

        async fn soft_delete(
            &self,
            id: Uuid,
            expected_revision: u64,
            now_unix_secs: u64,
        ) -> StorageResult<Option<PrincipalRecord>> {
            PrincipalStore::soft_delete(self.storage.as_ref(), id, expected_revision, now_unix_secs)
                .await
        }

        async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
            PrincipalStore::hard_delete(self.storage.as_ref(), id).await
        }
    }

    fn oauth_bundle(access_token: &str, refresh_token: &str) -> cc_lb_aead::OAuthTokenBundle {
        cc_lb_aead::OAuthTokenBundle {
            access_token: access_token.to_owned(),
            refresh_token: refresh_token.to_owned(),
            expires_at_unix_secs: 1_900_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: None,
            project_id: None,
            oauth_profile: Default::default(),
            scopes: vec!["messages".to_owned()],
            never_refresh: false,
        }
    }

    /// Replica B samples a clear reconnect marker at OAuth generation G,
    /// captures the reconnect-required record a terminal 401 then persists,
    /// and installs it after an operator reconnect on another replica
    /// advanced the generation and cleared the marker. B's
    /// notification-triggered rebuild fails once; the next healthy periodic
    /// reconcile must still detect the drift and rebuild.
    #[tokio::test]
    async fn reconnect_after_captured_failure_is_repaired_by_periodic_reconcile() {
        let (dir, storage) = storage_fixture(25).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let created = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "reconnect-aba-oauth".to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("oauth upstream created");
        let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
            &aead,
            &oauth_bundle("valid-access", "valid-refresh"),
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
        let installed =
            UpstreamStore::set_enabled(storage.as_ref(), stored.id, stored.revision, true)
                .await
                .expect("oauth upstream enabled");
        let principals = Arc::new(ScriptedPrincipals {
            storage: storage.clone(),
            upstream_id: installed.id,
            expected_oauth_token_generation: installed.oauth_token_generation,
            next_list: std::sync::atomic::AtomicU8::new(PRINCIPAL_LIST_PASSTHROUGH),
        });
        let mut stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        stores.principals = principals.clone();
        let stores = Arc::new(stores);
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

        let clear_hash = crate::reconcile::collect_revision_hash(&stores)
            .await
            .expect("clear-marker hash");
        principals
            .next_list
            .store(PRINCIPAL_LIST_INJECT_RECONNECT_REQUIRED, Ordering::SeqCst);
        let raced = build_view(&stores, &runtime, dir.path()).await;
        assert_eq!(
            raced.upstream_status_snapshot.revision_hash, clear_hash,
            "the build must have sampled the clear marker at the original generation"
        );
        assert_eq!(
            raced
                .upstream_status_snapshot
                .entries
                .get(&installed.name)
                .expect("raced status")
                .last_apply_error
                .as_deref(),
            Some("status_401"),
            "the build must have captured the reconnect-required record"
        );

        let replacement = cc_lb_aead::EncryptedOAuthTokens::encrypt(
            &aead,
            &oauth_bundle("reconnected-access", "reconnected-refresh"),
            installed.id.as_bytes(),
        )
        .expect("encrypt replacement credential");
        let reconnected = UpstreamStore::store_oauth_tokens(
            storage.as_ref(),
            installed.id,
            installed.revision,
            replacement,
            false,
        )
        .await
        .expect("operator reconnect");
        assert_eq!(reconnected.revision, installed.revision);
        assert!(reconnected.oauth_token_generation > installed.oauth_token_generation);
        assert_eq!(reconnected.last_apply_error, None);

        let holder = Arc::new(cc_lb_control::DynamicViewHolder::new(Arc::clone(&raced)));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);

        principals
            .next_list
            .store(PRINCIPAL_LIST_FAIL, Ordering::SeqCst);
        let forced = build_dynamic_view(
            &stores,
            None,
            aead.clone(),
            None,
            holder.generation(),
            &runtime,
            dir.path(),
            Arc::new(SubscriptionQuotaCache::new()),
            0,
            sink.clone(),
            1800,
            clock.clone(),
        )
        .await;
        assert!(
            matches!(
                forced,
                Err(RebindError::Storage(StorageError::Unavailable { .. }))
            ),
            "the notification-triggered rebuild must fail once"
        );
        assert_eq!(holder.generation(), raced.generation);

        let reconciler = crate::reconcile::Reconciler::new(
            stores.clone(),
            holder.clone(),
            runtime,
            aead,
            None,
            tokio_util::sync::CancellationToken::new(),
            dir.path().to_path_buf(),
            Arc::new(SubscriptionQuotaCache::new()),
            0,
            sink,
            1800,
            clock,
            None,
        );
        reconciler.reconcile_once().await.expect("reconcile runs");
        let rebuilt = holder.load();
        assert_eq!(
            rebuilt.generation,
            raced.generation + 1,
            "a healthy periodic reconcile must replace the captured reconnect-required view"
        );
        assert_eq!(
            rebuilt
                .upstream_status_snapshot
                .entries
                .get(&installed.name)
                .expect("rebuilt status")
                .last_apply_error,
            None,
            "the rebuilt view must route the reconnected credential"
        );

        reconciler.reconcile_once().await.expect("reconcile reruns");
        assert_eq!(
            holder.generation(),
            rebuilt.generation,
            "a view built from current records must not rebuild again"
        );
    }

    struct RefreshGate {
        entered: Notify,
        released: std::sync::atomic::AtomicBool,
        release: Notify,
    }

    impl RefreshGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: Notify::new(),
                released: std::sync::atomic::AtomicBool::new(false),
                release: Notify::new(),
            })
        }
    }

    struct DynamicRefreshHandle {
        storage: Arc<Storage>,
        aead: Arc<AeadService>,
        refreshed_access_token: String,
        calls: AtomicUsize,
        fail: bool,
        gate: Option<Arc<RefreshGate>>,
    }

    #[async_trait]
    impl cc_lb_signer_anthropic_oauth::LazyRefreshHandle for DynamicRefreshHandle {
        async fn refresh_one(
            &self,
            upstream_id: Uuid,
        ) -> Result<(), cc_lb_signer_anthropic_oauth::LazyRefreshError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.entered.notify_waiters();
                while !gate.released.load(Ordering::SeqCst) {
                    gate.release.notified().await;
                }
            }
            if self.fail {
                return Err(cc_lb_signer_anthropic_oauth::LazyRefreshError::Failed {
                    reason: "scripted refresh failure".to_owned(),
                });
            }
            let record = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
                .await
                .map_err(
                    |error| cc_lb_signer_anthropic_oauth::LazyRefreshError::Failed {
                        reason: error.to_string(),
                    },
                )?
                .expect("refresh upstream");
            let encrypted = record
                .oauth_credentials
                .as_ref()
                .expect("oauth credentials");
            let mut tokens = encrypted
                .decrypt(&self.aead, record.id.as_bytes())
                .expect("decrypt oauth credentials");
            tokens.access_token = self.refreshed_access_token.clone();
            tokens.expires_at_unix_secs = 1_900_000_000;
            let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
                &self.aead,
                &tokens,
                record.id.as_bytes(),
            )
            .expect("encrypt refreshed oauth credentials");
            UpstreamStore::store_oauth_tokens(
                self.storage.as_ref(),
                record.id,
                record.revision,
                encrypted,
                false,
            )
            .await
            .map_err(|error| {
                cc_lb_signer_anthropic_oauth::LazyRefreshError::Failed {
                    reason: error.to_string(),
                }
            })?;
            Ok(())
        }
    }

    async fn oauth_record(
        storage: &Storage,
        aead: &AeadService,
        name: &str,
        kind: UpstreamKind,
        mut tokens: cc_lb_aead::OAuthTokenBundle,
        base_url: Option<&str>,
    ) -> UpstreamRecord {
        let record = UpstreamStore::create(
            storage,
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: name.to_owned(),
                kind,
                base_url: base_url.map(|url| url.parse().expect("base URL")),
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await
        .expect("oauth upstream");
        tokens.scopes = vec!["openid".to_owned()];
        let encrypted =
            cc_lb_aead::EncryptedOAuthTokens::encrypt(aead, &tokens, record.id.as_bytes())
                .expect("encrypt oauth credentials");
        UpstreamStore::store_oauth_tokens(storage, record.id, record.revision, encrypted, false)
            .await
            .expect("store oauth credentials")
    }

    fn refreshing_factory(
        record: UpstreamRecord,
        storage: Arc<Storage>,
        aead: Arc<AeadService>,
        handle: Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>,
        clock: cc_lb_clock::ClockHandle,
    ) -> DbCompositeSignerFactory {
        DbCompositeSignerFactory::new(
            vec![record.clone()],
            storage,
            aead,
            Some(handle),
            clock,
            cc_lb_config::AntigravityOAuthConfig {
                user_agent: "antigravity/test".to_owned(),
                ..Default::default()
            },
        )
        .with_router_choice_state(record.name)
    }

    fn expired_tokens() -> cc_lb_aead::OAuthTokenBundle {
        cc_lb_aead::OAuthTokenBundle {
            access_token: "old-access".to_owned(),
            refresh_token: "refresh".to_owned(),
            expires_at_unix_secs: 1_700_000_000,
            refresh_token_expires_at_unix_secs: None,
            account_id: Some("account".to_owned()),
            project_id: Some("project".to_owned()),
            oauth_profile: Default::default(),
            scopes: vec!["openid".to_owned()],
            never_refresh: false,
        }
    }

    #[tokio::test]
    async fn dynamic_factory_refreshes_openai_and_reuses_storage_newer_token() {
        let (_dir, storage) = storage_fixture(41).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let record = oauth_record(
            &storage,
            &aead,
            "refresh-openai",
            UpstreamKind::OpenaiOauth,
            expired_tokens(),
            None,
        )
        .await;
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "new-openai".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: None,
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("OpenAI signer");

        let first = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect("refresh and sign");
        assert_eq!(
            first.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer new-openai"))
        );
        let second = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect("reuse stored token and sign");
        assert_eq!(
            second.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer new-openai"))
        );
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dynamic_factory_refreshes_gemini_cli_and_antigravity_without_losing_shape() {
        for (index, profile) in [
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ]
        .into_iter()
        .enumerate()
        {
            let (_dir, storage) = storage_fixture(42 + index as u8).await;
            let aead = Arc::new(AeadService::from_master_key([19; 32]));
            let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
            let mut tokens = expired_tokens();
            tokens.oauth_profile = profile;
            let record = oauth_record(
                &storage,
                &aead,
                "refresh-gemini",
                UpstreamKind::GeminiOauth,
                tokens,
                None,
            )
            .await;
            let handle = Arc::new(DynamicRefreshHandle {
                storage: storage.clone(),
                aead: aead.clone(),
                refreshed_access_token: "new-gemini".to_owned(),
                calls: AtomicUsize::new(0),
                fail: false,
                gate: None,
            });
            let signer = refreshing_factory(record, storage, aead, handle, clock)
                .build(&Upstream::GeminiDirect { base_url: None })
                .await
                .expect("Gemini signer");
            let mut shaped = shaped_request();
            shaped.set_url(
                "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                    .parse()
                    .expect("Gemini URL"),
            );
            shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
            shaped.headers_mut().insert(
                http::header::USER_AGENT,
                http::HeaderValue::from_static("GeminiCLI/existing"),
            );
            let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped)
                .await
                .expect("refresh and sign Gemini");
            assert_eq!(
                signed.headers().get(http::header::AUTHORIZATION),
                Some(&http::HeaderValue::from_static("Bearer new-gemini"))
            );
            let body: serde_json::Value =
                serde_json::from_slice(signed.body()).expect("Gemini envelope");
            assert_eq!(body["project"], "project");
            match profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                    assert!(body.get("userAgent").is_none());
                    assert_eq!(
                        signed.headers().get(http::header::USER_AGENT),
                        Some(&http::HeaderValue::from_static("GeminiCLI/existing"))
                    );
                }
                cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                    assert_eq!(body["userAgent"], "antigravity");
                    assert_eq!(body["requestType"], "agent");
                    assert_eq!(
                        signed.headers().get(http::header::USER_AGENT),
                        Some(&http::HeaderValue::from_static("antigravity/test"))
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn dynamic_factory_pre_send_refresh_then_unauthorized_forces_second_refresh() {
        let (_dir, storage) = storage_fixture(48).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let record = oauth_record(
            &storage,
            &aead,
            "pre-send-then-unauthorized",
            UpstreamKind::OpenaiOauth,
            expired_tokens(),
            None,
        )
        .await;
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "pre-send-token".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: None,
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("OpenAI signer");

        let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect("pre-send refresh and sign");
        assert_eq!(
            signed.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer pre-send-token"))
        );
        let scrubbed = signer.scrub_secrets("old-access pre-send-token");
        assert!(!scrubbed.as_ref().contains("old-access"));
        assert!(!scrubbed.as_ref().contains("pre-send-token"));
        let decision = signer
            .on_unauthorized(&UpstreamError::Unauthorized {
                status: http::StatusCode::UNAUTHORIZED,
                body: None,
            })
            .await;
        assert!(
            matches!(decision, RetryDecision::Refresh { .. }),
            "401 after a pre-send refresh must request another refreshed signer"
        );
        assert_eq!(handle.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dynamic_factory_unauthorized_refresh_returns_new_signer_for_one_retry() {
        let (_dir, storage) = storage_fixture(44).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.expires_at_unix_secs = 1_900_000_000;
        let record = oauth_record(
            &storage,
            &aead,
            "unauthorized-refresh",
            UpstreamKind::OpenaiOauth,
            tokens,
            None,
        )
        .await;
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "retry-token".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: None,
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("OpenAI signer");
        let decision = signer
            .on_unauthorized(&UpstreamError::Unauthorized {
                status: http::StatusCode::UNAUTHORIZED,
                body: None,
            })
            .await;
        let RetryDecision::Refresh { new_signer } = decision else {
            panic!("401 must request one refreshed signer");
        };
        let signed = cc_lb_upstream::sign_request(new_signer.as_ref(), shaped_request())
            .await
            .expect("retry signs");
        assert_eq!(
            signed.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer retry-token"))
        );
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dynamic_factory_concurrent_expired_requests_share_one_refresh() {
        let (_dir, storage) = storage_fixture(45).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let record = oauth_record(
            &storage,
            &aead,
            "concurrent-refresh",
            UpstreamKind::OpenaiOauth,
            expired_tokens(),
            None,
        )
        .await;
        let gate = RefreshGate::new();
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "concurrent-token".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: Some(gate.clone()),
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("OpenAI signer");
        let first_signer = signer.clone();
        let entered = gate.entered.notified();
        let first = tokio::spawn(async move {
            cc_lb_upstream::sign_request(first_signer.as_ref(), shaped_request()).await
        });
        entered.await;
        let second_signer = signer.clone();
        let second = tokio::spawn(async move {
            cc_lb_upstream::sign_request(second_signer.as_ref(), shaped_request()).await
        });
        gate.released.store(true, Ordering::SeqCst);
        gate.release.notify_waiters();
        assert!(first.await.expect("first task").is_ok());
        assert!(second.await.expect("second task").is_ok());
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dynamic_factory_never_refresh_access_only_skips_refresh_handle() {
        let (_dir, storage) = storage_fixture(47).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.never_refresh = true;
        tokens.expires_at_unix_secs = 1_700_000_001;
        let record = oauth_record(
            &storage,
            &aead,
            "access-only-refresh",
            UpstreamKind::OpenaiOauth,
            tokens,
            None,
        )
        .await;
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "must-not-refresh".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: None,
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("access-only signer");
        let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect("access-only request signs");
        assert_eq!(
            signed.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer old-access"))
        );
        assert_eq!(handle.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dynamic_factory_refresh_failure_is_terminal_for_request() {
        let (_dir, storage) = storage_fixture(46).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let record = oauth_record(
            &storage,
            &aead,
            "failed-refresh",
            UpstreamKind::OpenaiOauth,
            expired_tokens(),
            None,
        )
        .await;
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "unused".to_owned(),
            calls: AtomicUsize::new(0),
            fail: true,
            gate: None,
        });
        let signer = refreshing_factory(record, storage, aead, handle.clone(), clock)
            .build(&Upstream::OpenaiDirect { base_url: None })
            .await
            .expect("OpenAI signer");
        let error = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect_err("refresh failure must terminate signing");
        assert!(matches!(error, SignerError::SigningFailed { .. }));
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
    }
}
