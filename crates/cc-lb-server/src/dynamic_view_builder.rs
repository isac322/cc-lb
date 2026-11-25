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
use cc_lb_domain::{BUILTIN_SUBSCRIPTION_PREFERENCE_ID, Provider, Upstream};
use cc_lb_oauth_protocol::refresh_requires_reconnect;
use cc_lb_quota::plan_capacity::{
    PRO_CAPACITY_RATIO, PlanInfo, PlanTierClassification, TierKey, classify_plan_tier,
};
use cc_lb_routing::FilterPlugin;
use cc_lb_routing_eval::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntime};
use cc_lb_vertex_auth::{
    BearerSource, TokenRegistry, TokenSource, TokenTransport, parse_credential,
    vertex_credential_aad,
};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use crate::vertex_status::{ReportingBearerSource, VertexStatusSink};
use crate::vertex_transport::production_transport;
use crate::wasm_host::{WasmtimeFilterPlugin, WasmtimeUpstreamDialect};

use cc_lb_signer_vertex::{VertexCredentialSource, VertexSigner};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, MetadataTierMappingOverrideRecord,
    OrganizationMetadataStore, PlanTierRatioRecord, PlanTierStore, PluginChainEntry,
    PluginRegistryStore, PluginSlotKind, PrincipalRecord, PrincipalStore,
    PromptCacheObservationStore, StorageError, StorageResult, TierResolutionSource,
    UpstreamPlanTierRecord, UpstreamRateLimitStateStore, UpstreamRecord, UpstreamStatusUpdate,
    UpstreamStore, UpstreamSubscriptionMetadataStore, UpstreamSubscriptionQuotaStore,
    WasmRegistryEntry,
};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError, scrub_secret,
};
use http::HeaderValue;
use parking_lot::RwLock;
use secrecy::ExposeSecret;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::PluginManifest;
use crate::reconcile::collect_revision_hash_for_scope;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use cc_lb_control::PromptCacheObservationSinkLike;

/// Maximum number of user-supplied router filters. The built-in
/// `subscription-preference` entry is structural router-chain state.
const MAX_ROUTER_CHAIN_DEPTH: usize = 16;
static VERTEX_TOKEN_TRANSPORT: LazyLock<Arc<dyn TokenTransport>> =
    LazyLock::new(production_transport);

fn vertex_token_transport() -> Arc<dyn TokenTransport> {
    VERTEX_TOKEN_TRANSPORT.clone()
}

#[derive(Clone)]
pub struct SharedSignerState {
    pub(crate) refresh_locks: cc_lb_signer_anthropic_oauth::RefreshLocks,
    pub(crate) anthropic_forced_generations: cc_lb_signer_anthropic_oauth::ForcedRefreshGenerations,
    pub(crate) subscription_signer_states:
        Arc<RwLock<HashMap<Uuid, Arc<RwLock<SubscriptionSignerState>>>>>,
    pub(crate) anthropic_oauth_states:
        Arc<RwLock<HashMap<Uuid, cc_lb_signer_anthropic_oauth::SharedAnthropicOAuthState>>>,
}

impl Default for SharedSignerState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedSignerState {
    pub fn new() -> Self {
        Self {
            refresh_locks: cc_lb_signer_anthropic_oauth::new_refresh_locks(),
            anthropic_forced_generations:
                cc_lb_signer_anthropic_oauth::new_forced_refresh_generations(),
            subscription_signer_states: Arc::new(RwLock::new(HashMap::new())),
            anthropic_oauth_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
pub struct VertexState {
    pub(crate) token_registry: Arc<TokenRegistry>,
    pub(crate) token_transport: Arc<dyn TokenTransport>,
    pub(crate) status_sink: Arc<VertexStatusSink>,
    pub(crate) shared_signer_state: Arc<SharedSignerState>,
}

impl VertexState {
    pub fn new(store: Arc<dyn UpstreamStore>) -> Self {
        Self {
            token_registry: Arc::new(TokenRegistry::new()),
            token_transport: vertex_token_transport(),
            status_sink: Arc::new(VertexStatusSink::new(store)),
            shared_signer_state: Arc::new(SharedSignerState::new()),
        }
    }
}

pub struct Stores {
    pub upstreams: Arc<dyn UpstreamStore>,
    pub vertex_state: Arc<VertexState>,
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

/// Controls which Wasmtime plugin chains a runtime instance compiles and retains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DynamicViewPluginScope {
    /// Proxy processes compile and retain principal router/shape chains plus warmup dialect slots.
    #[default]
    All,
    /// Dedicated scheduler/worker processes skip principal router/shape compilation
    /// and retain only warmup dialect slots.
    WarmupOnly,
}

async fn register_filter_slot(
    runtime: &Arc<WasmtimeRuntime>,
    slot_key: &cc_lb_runtime_wasmtime::RuntimeSlotKey,
    manifest: &PluginManifest,
    wasm: &[u8],
) -> Result<
    Arc<cc_lb_runtime_wasmtime::LoadedPluginSlot>,
    cc_lb_runtime_wasmtime::WasmtimeRuntimeError,
> {
    runtime.register_filter(slot_key.clone(), manifest.name.clone(), wasm)
}

async fn register_shape_slot(
    runtime: &Arc<WasmtimeRuntime>,
    slot_key: &cc_lb_runtime_wasmtime::RuntimeSlotKey,
    manifest: &PluginManifest,
    wasm: &[u8],
) -> Result<
    Arc<cc_lb_runtime_wasmtime::LoadedPluginSlot>,
    cc_lb_runtime_wasmtime::WasmtimeRuntimeError,
> {
    runtime.register_shape(slot_key.clone(), manifest.name.clone(), wasm)
}

fn verify_wasm_sha256(bytes: &[u8], expected_sha256: [u8; 32]) -> io::Result<()> {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    if digest != expected_sha256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "wasm sha256 mismatch: expected {}, got {}",
                crate::revision_hash::hex_sha256(&expected_sha256),
                crate::revision_hash::hex_sha256(&digest),
            ),
        ));
    }
    Ok(())
}

/// Materializes a plugin `.wasm` file, verifies its on-disk SHA-256 against
/// `expected_sha256`, and self-heals from storage if the disk cache was
/// corrupted. Returns `(cache_path, verified_bytes)`.
pub(crate) async fn read_verified_wasm(
    stores: &Stores,
    data_dir: &Path,
    expected_sha256: [u8; 32],
) -> Result<(PathBuf, Vec<u8>), RebindError> {
    let wasm_path = materialize_wasm(stores, data_dir, expected_sha256).await?;
    let bytes = tokio::fs::read(&wasm_path).await?;
    if verify_wasm_sha256(&bytes, expected_sha256).is_ok() {
        return Ok((wasm_path, bytes));
    }
    tracing::warn!(
        path = %wasm_path.display(),
        expected_sha256 = %crate::revision_hash::hex_sha256(&expected_sha256),
        "cached wasm file failed sha256 verification; deleting and re-fetching from storage",
    );
    let _ = tokio::fs::remove_file(&wasm_path).await;
    let db_bytes = stores
        .plugin_registry
        .get_blob_bytes(expected_sha256)
        .await?
        .ok_or_else(|| {
            RebindError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "wasm blob {} not found in storage",
                    crate::revision_hash::hex_sha256(&expected_sha256),
                ),
            ))
        })?;
    verify_wasm_sha256(&db_bytes, expected_sha256).map_err(RebindError::Io)?;
    let rewritten = ensure_wasm_cached(data_dir, expected_sha256, || Ok(Some(db_bytes.clone())))
        .map_err(RebindError::Io)?;
    Ok((rewritten, db_bytes))
}

/// Removes `.wasm` files from the local cache directory whose SHA-256 hex stem
/// is no longer in `active_shas`.
pub(crate) fn sweep_stale_wasm_disk_cache(data_dir: &Path, active_shas: &HashSet<[u8; 32]>) {
    let cache_dir = data_dir.join("plugins").join("wasm").join("cache");
    let Ok(entries) = fs::read_dir(&cache_dir) else {
        return;
    };
    let active_stems: HashSet<String> = active_shas
        .iter()
        .map(crate::revision_hash::hex_sha256)
        .collect();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.file_name().is_some_and(|name| name == ".tmp") {
            continue;
        }
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("wasm") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if !active_stems.contains(stem) {
            match fs::remove_file(&path) {
                Ok(()) => tracing::info!(path = %path.display(), "swept stale wasm cache file"),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    %error,
                    "failed to remove stale wasm cache file",
                ),
            }
        }
    }
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
    let target = cache_dir.join(format!(
        "{}.wasm",
        crate::revision_hash::hex_sha256(&sha256)
    ));
    if target.exists() {
        return Ok(target);
    }

    let bytes = fetch().map_err(io::Error::other)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "wasm blob {} not found",
                crate::revision_hash::hex_sha256(&sha256)
            ),
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
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
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
    build_dynamic_view_with_scope(
        stores,
        gemini_config,
        antigravity_config,
        aead,
        lazy_refresher,
        current_generation,
        runtime,
        data_dir,
        subscription_quota_cache,
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink,
        subscription_quota_routing_max_staleness_secs,
        clock,
        DynamicViewPluginScope::All,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn build_dynamic_view_with_scope(
    stores: &Stores,
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
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
    plugin_scope: DynamicViewPluginScope,
) -> Result<Arc<DynamicView>, RebindError> {
    build_dynamic_view_with_scope_and_rate_limit_cache(
        stores,
        gemini_config,
        antigravity_config,
        aead,
        lazy_refresher,
        current_generation,
        runtime,
        data_dir,
        subscription_quota_cache,
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink,
        subscription_quota_routing_max_staleness_secs,
        clock,
        Arc::new(RwLock::new(UpstreamRateLimitCache::default())),
        plugin_scope,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn build_dynamic_view_with_rate_limit_cache(
    stores: &Stores,
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
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
    upstream_rate_limit_cache: Arc<RwLock<UpstreamRateLimitCache>>,
) -> Result<Arc<DynamicView>, RebindError> {
    build_dynamic_view_with_scope_and_rate_limit_cache(
        stores,
        gemini_config,
        antigravity_config,
        aead,
        lazy_refresher,
        current_generation,
        runtime,
        data_dir,
        subscription_quota_cache,
        prompt_cache_grace_margin_secs,
        prompt_cache_observation_sink,
        subscription_quota_routing_max_staleness_secs,
        clock,
        upstream_rate_limit_cache,
        DynamicViewPluginScope::All,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn build_dynamic_view_with_scope_and_rate_limit_cache(
    stores: &Stores,
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
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
    upstream_rate_limit_cache: Arc<RwLock<UpstreamRateLimitCache>>,
    plugin_scope: DynamicViewPluginScope,
) -> Result<Arc<DynamicView>, RebindError> {
    // Collect the change signature before any content read. A mutation that
    // commits while this build is reading contents then changes the stored
    // signature after this early sample, so a later reconcile detects the
    // drift instead of trusting a signature newer than the contents.
    let revision_hash = collect_revision_hash_for_scope(stores, plugin_scope).await?;
    let upstreams = list_upstreams(stores).await?;
    let all_upstream_ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    stores
        .vertex_state
        .token_registry
        .retain_live(&all_upstream_ids.iter().map(ToString::to_string).collect());
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
    {
        let mut cache = upstream_rate_limit_cache.write();
        cache.hydrate_from_records(upstream_rate_limit_records, &all_upstream_ids);
    }
    let principals = list_principals(stores).await?;
    let (principal_chains, registered_slot_keys, active_shas, has_retryable_plugin_failure) =
        build_principal_chains_with_scope(
            stores,
            runtime,
            data_dir,
            &principals,
            &upstreams,
            &aead,
            &clock,
            plugin_scope,
        )
        .await?;
    // RFC-0001 gap-analysis #1: sweep any runtime slot whose key is
    // no longer referenced by the freshly-built view. Must run AFTER
    // build_principal_chains_with_scope succeeds so we don't tear down slots the
    // still-active view is dispatching against; already-cloned
    // `Arc<PluginSlotKind>` handles keep the evicted cells alive for the
    // duration of any in-flight call (see `WasmtimeRuntime::retain_slots`
    // docs).
    let evicted = runtime.retain_slots(&registered_slot_keys);
    if !evicted.is_empty() {
        tracing::info!(
            evicted_count = evicted.len(),
            "reconcile swept orphan plugin slots",
        );
    }
    if plugin_scope == DynamicViewPluginScope::All {
        sweep_stale_wasm_disk_cache(data_dir, &active_shas);
    }
    let principal_view = Arc::new(PrincipalView::from_db(&principals, principal_chains));
    let now = unix_secs(clock.now());
    let statuses = apply_upstreams(stores, &upstreams, &aead, now).await?;
    let vertex_sources = Arc::new(build_vertex_sources(
        &upstreams,
        &aead,
        &stores.vertex_state.token_registry,
        &stores.vertex_state.token_transport,
        &clock,
    ));

    let signer_factory = Arc::new(DbCompositeSignerFactory::new_with_vertex_state(
        upstreams.clone(),
        stores.upstreams.clone(),
        aead.clone(),
        lazy_refresher,
        clock.clone(),
        (
            gemini_config.cloned().unwrap_or_default(),
            antigravity_config.cloned().unwrap_or_default(),
        ),
        (stores.vertex_state.clone(), vertex_sources),
    ));
    let snapshot = Arc::new(UpstreamStatusSnapshot {
        entries: statuses,
        applied_at_unix_secs: unix_secs(clock.now()),
        revision_hash,
    });
    let readiness_summary =
        crate::app::proxy_router::compute_upstream_readiness_summary(&upstreams, &snapshot, &aead);

    let plan_info_by_upstream = load_plan_info_by_upstream(stores).await?;
    let now_unix_millis = i64::try_from(unix_millis(clock.now())).unwrap_or(i64::MAX);
    reconcile_upstream_plan_tiers(stores, now_unix_millis).await;
    let builder = DynamicViewBuilder::new(current_generation)
        .signer_factory(signer_factory)
        .principal_view(principal_view)
        .upstream_status_snapshot(snapshot)
        .readiness_summary(readiness_summary)
        .upstream_rate_limit_cache(upstream_rate_limit_cache)
        .subscription_quota_cache(subscription_quota_cache)
        .subscription_quota_routing_max_staleness_secs(
            subscription_quota_routing_max_staleness_secs,
        )
        .plan_info_by_upstream(plan_info_by_upstream)
        .upstream_records(upstreams.clone())
        .prompt_cache_observation_store(stores.prompt_cache_observations.clone())
        .prompt_cache_grace_margin_secs(prompt_cache_grace_margin_secs)
        .prompt_cache_observation_sink(prompt_cache_observation_sink)
        .has_retryable_plugin_failure(has_retryable_plugin_failure);
    Ok(builder.build())
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

#[cfg(test)]
async fn build_principal_chains(
    stores: &Stores,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    principals: &[PrincipalRecord],
    upstreams: &[UpstreamRecord],
    aead: &AeadService,
    clock: &cc_lb_clock::ClockHandle,
) -> Result<
    (
        HashMap<String, PrincipalRoutingArtifacts>,
        HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
    ),
    RebindError,
> {
    let (chains, slot_keys, _, _) = build_principal_chains_with_scope(
        stores,
        runtime,
        data_dir,
        principals,
        upstreams,
        aead,
        clock,
        DynamicViewPluginScope::All,
    )
    .await?;
    Ok((chains, slot_keys))
}

#[allow(clippy::too_many_arguments)]
async fn build_principal_chains_with_scope(
    stores: &Stores,
    runtime: &Arc<WasmtimeRuntime>,
    data_dir: &Path,
    principals: &[PrincipalRecord],
    upstreams: &[UpstreamRecord],
    aead: &AeadService,
    clock: &cc_lb_clock::ClockHandle,
    plugin_scope: DynamicViewPluginScope,
) -> Result<
    (
        HashMap<String, PrincipalRoutingArtifacts>,
        HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
        HashSet<[u8; 32]>,
        bool,
    ),
    RebindError,
> {
    let registry = list_registry_by_id(stores).await?;
    let active_shas: HashSet<[u8; 32]> = registry
        .values()
        .filter(|entry| !entry.is_builtin)
        .map(|entry| entry.sha256)
        .collect();
    let mut upstream_identities = HashMap::new();
    let mut chains = HashMap::new();
    let mut registered_slot_keys: HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey> = HashSet::new();
    let mut has_retryable_plugin_failure = false;

    if plugin_scope == DynamicViewPluginScope::All {
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
            let providers: Vec<Provider> =
                [Provider::Anthropic, Provider::Openai, Provider::Gemini]
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
                    clock,
                },
                principal,
                router_entries,
                &mut registered_slot_keys,
                &providers,
                &mut has_retryable_plugin_failure,
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
                    let (wasm_path, wasm_bytes) =
                        read_verified_wasm(stores, data_dir, registry_entry.sha256).await?;
                    let manifest = PluginManifest {
                        name: registry_entry.name.clone(),
                        artifact: wasm_path.to_string_lossy().into_owned(),
                    };
                    let slot_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
                        principal.name.clone(),
                        manifest.name.clone(),
                        cc_lb_runtime_wasmtime::RuntimeSlotRole::Shape,
                    );
                    registered_slot_keys.insert(slot_key.clone());
                    match register_shape_slot(runtime, &slot_key, &manifest, &wasm_bytes).await {
                        Ok(slot) => {
                            let dispatch =
                                WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
                            let mut dialects = HashMap::new();
                            for provider in &providers {
                                let Some(selected) =
                                    dispatch.for_provider(wire_provider(*provider))
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
                                            let tokens = if record.kind == UpstreamKind::GeminiOauth
                                            {
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
                                            .with_upstream_identity(identity.clone())
                                            .with_claims(crate::wasm_host::project_config_claims(
                                                &entry.config,
                                            ));
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
                            let retryable = error.is_pool_saturated() || error.is_epoch_interrupt();
                            has_retryable_plugin_failure |= retryable;
                            let affected_providers =
                                declared_shape_providers(registry_entry, &wasm_bytes);
                            let message = format!("shape plugin instantiation failed: {error}");
                            tracing::error!(
                                principal = %principal.name,
                                plugin = %manifest.name,
                                artifact = %manifest.artifact,
                                chain_entry_id = %entry.id,
                                affected_providers = ?affected_providers,
                                retryable,
                                %error,
                                "shape chain entry failed to instantiate; affected provider requests will fail closed",
                            );
                            DialectCache::InstantiationFailed {
                                reason: message,
                                providers: affected_providers,
                            }
                        }
                    }
                }
            } else {
                DialectCache::Inherit
            };

            chains.insert(principal.name.clone(), (router, dialect));
        }
    }
    for upstream in upstreams {
        if !upstream.enabled || upstream.deleted_at_unix_secs.is_some() {
            continue;
        }
        let Some(plugin_ref) = upstream.warmup_dialect_plugin.as_ref() else {
            continue;
        };
        let Some(registry_entry) = registry.get(&plugin_ref.wasm_registry_id) else {
            continue;
        };
        if !registry_entry_unsupported_slot(registry_entry, PluginSlotKind::Shape) {
            registered_slot_keys.insert(crate::warmup::dialect::warmup_dialect_slot_key(
                registry_entry.sha256,
                &registry_entry.name,
            ));
        }
    }
    Ok((
        chains,
        registered_slot_keys,
        active_shas,
        has_retryable_plugin_failure,
    ))
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

fn domain_provider(family: cc_lb_plugin_wire::schema::ProviderFamily) -> Provider {
    match family {
        cc_lb_plugin_wire::schema::ProviderFamily::Anthropic => Provider::Anthropic,
        cc_lb_plugin_wire::schema::ProviderFamily::Openai => Provider::Openai,
        cc_lb_plugin_wire::schema::ProviderFamily::Gemini => Provider::Gemini,
    }
}

fn shape_providers_from_hook_metadata<'a>(
    hooks: impl IntoIterator<Item = (&'a String, &'a cc_lb_plugin_wire::metadata::HookMetadata)>,
) -> Vec<Provider> {
    use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily};

    let mut providers = Vec::with_capacity(3);
    for (hook_name, meta) in hooks {
        let Some((hook, family)) = HookKind::parse_entrypoint(hook_name) else {
            continue;
        };
        let is_active_shape_hook = match hook {
            HookKind::Shape => true,
            HookKind::ShapeEnvelopeGemini => family == ProviderFamily::Gemini,
            HookKind::TransformResponse
            | HookKind::TransformSseEvent
            | HookKind::TransformRealtimeFrame => !meta.mode.is_noop(),
            HookKind::Filter => false,
        };
        if is_active_shape_hook {
            let provider = domain_provider(family);
            if !providers.contains(&provider) {
                providers.push(provider);
            }
        }
    }
    providers
}

fn declared_shape_providers(
    registry_entry: &WasmRegistryEntry,
    wasm_bytes: &[u8],
) -> Vec<Provider> {
    if !registry_entry.hook_metadata.is_empty() {
        return shape_providers_from_hook_metadata(&registry_entry.hook_metadata);
    }
    if let Ok(inspection) =
        cc_lb_runtime_wasmtime::inspect_wasm(cc_lb_runtime_wasmtime::HookKind::Shape, wasm_bytes)
    {
        return shape_providers_from_hook_metadata(&inspection.metadata.hooks);
    }
    vec![Provider::Anthropic, Provider::Openai, Provider::Gemini]
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
        .join(format!(
            "{}.wasm",
            crate::revision_hash::hex_sha256(&sha256)
        ));
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
) -> Result<(PluginManifest, Vec<u8>), RebindError> {
    let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
    })?;
    let (wasm_path, wasm_bytes) =
        read_verified_wasm(stores, data_dir, registry_entry.sha256).await?;
    Ok((
        PluginManifest {
            name: registry_entry.name.clone(),
            artifact: wasm_path.to_string_lossy().into_owned(),
        },
        wasm_bytes,
    ))
}

struct RouterPipelineBuildContext<'a> {
    stores: &'a Stores,
    runtime: &'a Arc<WasmtimeRuntime>,
    data_dir: &'a Path,
    registry: &'a HashMap<Uuid, WasmRegistryEntry>,
    clock: &'a cc_lb_clock::ClockHandle,
}

async fn build_router_pipeline(
    context: RouterPipelineBuildContext<'_>,
    principal: &PrincipalRecord,
    mut router_entries: Vec<cc_lb_storage_api::PluginChainEntry>,
    registered_slot_keys: &mut HashSet<cc_lb_runtime_wasmtime::RuntimeSlotKey>,
    providers: &[Provider],
    has_retryable_plugin_failure: &mut bool,
) -> Result<Option<Arc<RouterPipelineCache>>, RebindError> {
    let RouterPipelineBuildContext {
        stores,
        runtime,
        data_dir,
        registry,
        clock,
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
                let filter: Arc<dyn FilterPlugin> =
                    Arc::new(SubscriptionPreferenceFilter::with_clock(clock.clone()));
                for provider in providers {
                    provider_filters
                        .get_mut(*provider)
                        .push(Arc::clone(&filter));
                }
            }
            continue;
        }
        let (manifest, wasm_bytes) =
            match manifest_for_chain_entry(stores, data_dir, registry, &entry).await {
                Ok(pair) => pair,
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
            cc_lb_runtime_wasmtime::RuntimeSlotRole::Filter,
        );
        registered_slot_keys.insert(slot_key.clone());
        match register_filter_slot(runtime, &slot_key, &manifest, &wasm_bytes).await {
            Ok(slot) => {
                let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
                let claims = crate::wasm_host::project_config_claims(&entry.config);
                for provider in providers {
                    if let Some(filter) = WasmtimeFilterPlugin::for_provider(
                        &dispatch,
                        wire_provider(*provider),
                        entry.id,
                        manifest.name.clone(),
                    ) {
                        provider_filters
                            .get_mut(*provider)
                            .push(Arc::new(filter.with_claims(Arc::clone(&claims))));
                    }
                }
            }
            Err(error) => {
                let retryable = error.is_pool_saturated() || error.is_epoch_interrupt();
                *has_retryable_plugin_failure |= retryable;
                tracing::error!(
                    principal = %principal.name,
                    plugin = %manifest.name,
                    artifact = %manifest.artifact,
                    chain_entry_id = %entry.id,
                    retryable,
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
            UpstreamKind::AnthropicOauth
                | UpstreamKind::OpenaiOauth
                | UpstreamKind::GeminiOauth
                | UpstreamKind::VertexCredential
        );
        let mut last_apply_error = if is_oauth {
            upstream.last_apply_error.clone()
        } else {
            None
        };
        if !upstream.enabled {
            if !is_oauth && upstream.last_apply_error.is_some() {
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

        if upstream.kind == UpstreamKind::VertexCredential
            && cc_lb_domain::vertex::vertex_credential_requires_replacement(
                upstream.last_apply_error.as_deref(),
            )
        {
            statuses.insert(
                upstream.name.clone(),
                UpstreamStatusEntry {
                    status: ApplyStatus::Error,
                    last_apply_error: upstream.last_apply_error.clone(),
                    last_apply_at_unix_secs: now,
                },
            );
            continue;
        }
        if let Err(message) = cc_lb_control::validate_upstream(upstream, aead) {
            let reconnect_required = if upstream.kind == UpstreamKind::VertexCredential {
                cc_lb_domain::vertex::vertex_credential_requires_replacement(
                    last_apply_error.as_deref(),
                )
            } else {
                is_oauth && refresh_requires_reconnect(last_apply_error.as_deref())
            };
            if !reconnect_required && upstream.last_apply_error.as_deref() != Some(message.as_str())
            {
                if is_oauth {
                    match stores
                        .upstreams
                        .set_status(
                            upstream.id,
                            UpstreamStatusUpdate {
                                last_apply_error: Some(Some(message.clone())),
                                last_apply_at_unix_secs: Some(Some(now)),
                                expected_oauth_token_generation: matches!(
                                    upstream.kind,
                                    UpstreamKind::AnthropicOauth
                                        | UpstreamKind::OpenaiOauth
                                        | UpstreamKind::GeminiOauth
                                )
                                .then_some(upstream.oauth_token_generation),
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

        // A successful validation clears stale non-terminal Vertex credential
        // errors, while terminal replacement markers remain authoritative.
        if (!is_oauth && upstream.last_apply_error.is_some())
            || (upstream.kind == UpstreamKind::VertexCredential
                && last_apply_error.is_some()
                && !cc_lb_domain::vertex::vertex_credential_requires_replacement(
                    last_apply_error.as_deref(),
                ))
        {
            stores
                .upstreams
                .set_last_apply_error(upstream.id, None)
                .await?;
            last_apply_error = None;
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

fn build_vertex_sources(
    upstreams: &[UpstreamRecord],
    aead: &AeadService,
    registry: &TokenRegistry,
    transport: &Arc<dyn TokenTransport>,
    clock: &cc_lb_clock::ClockHandle,
) -> HashMap<Uuid, Arc<TokenSource>> {
    upstreams
        .iter()
        .filter(|record| record.kind == UpstreamKind::VertexCredential)
        .filter_map(|record| {
            let ciphertext = record.api_key_ciphertext.as_deref()?;
            let upstream_id = record.id.to_string();
            let source = registry.get(&upstream_id, ciphertext).or_else(|| {
                let plaintext = aead
                    .decrypt(ciphertext, &vertex_credential_aad(&upstream_id))
                    .ok()?;
                let credential = parse_credential(&plaintext).ok()?;
                Some(registry.source_for(
                    upstream_id,
                    ciphertext,
                    credential,
                    transport.clone(),
                    clock.clone(),
                ))
            })?;
            Some((record.id, source))
        })
        .collect()
}

struct DbCompositeSignerFactoryInner {
    upstreams: Vec<UpstreamRecord>,
    upstreams_by_name: HashMap<String, usize>,
    prebuilt_static_signers: HashMap<Uuid, Arc<dyn Signer>>,
    upstream_store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    refresh_locks: cc_lb_signer_anthropic_oauth::RefreshLocks,
    anthropic_forced_generations: cc_lb_signer_anthropic_oauth::ForcedRefreshGenerations,
    subscription_signer_states: Arc<RwLock<HashMap<Uuid, Arc<RwLock<SubscriptionSignerState>>>>>,
    anthropic_oauth_states:
        Arc<RwLock<HashMap<Uuid, cc_lb_signer_anthropic_oauth::SharedAnthropicOAuthState>>>,
    clock: cc_lb_clock::ClockHandle,
    gemini_config: Arc<cc_lb_config::GeminiOAuthConfig>,
    antigravity_config: Arc<cc_lb_config::AntigravityOAuthConfig>,
    token_registry: Arc<TokenRegistry>,
    vertex_sources: Arc<HashMap<Uuid, Arc<TokenSource>>>,
    vertex_status_sink: Arc<VertexStatusSink>,
}

#[derive(Clone)]
struct DbCompositeSignerFactory {
    inner: Arc<DbCompositeSignerFactoryInner>,
    router_chosen_upstream_name: Option<String>,
}

impl DbCompositeSignerFactory {
    #[cfg(test)]
    fn new(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        clock: cc_lb_clock::ClockHandle,
        oauth_configs: (
            cc_lb_config::GeminiOAuthConfig,
            cc_lb_config::AntigravityOAuthConfig,
        ),
    ) -> Self {
        let vertex_state = Arc::new(VertexState::new(upstream_store.clone()));
        Self::new_with_vertex_state(
            upstreams,
            upstream_store,
            aead,
            lazy_refresher,
            clock,
            oauth_configs,
            (vertex_state, Arc::new(HashMap::new())),
        )
    }

    fn new_with_vertex_state(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        clock: cc_lb_clock::ClockHandle,
        oauth_configs: (
            cc_lb_config::GeminiOAuthConfig,
            cc_lb_config::AntigravityOAuthConfig,
        ),
        (vertex_state, vertex_sources): (Arc<VertexState>, Arc<HashMap<Uuid, Arc<TokenSource>>>),
    ) -> Self {
        let gemini_config = Arc::new(oauth_configs.0);
        let antigravity_config = Arc::new(oauth_configs.1);
        let shared = &vertex_state.shared_signer_state;
        let mut upstreams_by_name = HashMap::with_capacity(upstreams.len());
        let mut prebuilt_static_signers = HashMap::new();
        for (idx, record) in upstreams.iter().enumerate() {
            upstreams_by_name.entry(record.name.clone()).or_insert(idx);
            match record.kind {
                UpstreamKind::AnthropicApiKey
                | UpstreamKind::OpenaiApiKey
                | UpstreamKind::GeminiApiKey
                | UpstreamKind::VertexApiKey
                | UpstreamKind::VertexCredential => {
                    if let Ok(signer) = build_static_signer_for_record(
                        record,
                        &aead,
                        &vertex_sources,
                        &vertex_state.token_registry,
                        &vertex_state.status_sink,
                    ) {
                        prebuilt_static_signers.insert(record.id, signer);
                    }
                }
                UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
                    if let Some(encrypted) = record.oauth_credentials.as_ref()
                        && let Ok(tokens) = encrypted.decrypt(&aead, record.id.as_bytes())
                        && let Ok(signer) = build_subscription_signer_with_policy(
                            record,
                            &tokens,
                            clock.clone(),
                            Some(&gemini_config),
                            Some(&antigravity_config),
                            true,
                        )
                    {
                        if !tokens.never_refresh && lazy_refresher.is_some() {
                            let _ = reconcile_subscription_signer_state(
                                &shared.subscription_signer_states,
                                record,
                                &tokens,
                                signer,
                                &clock,
                                &gemini_config,
                                &antigravity_config,
                            );
                        } else if tokens.never_refresh {
                            prebuilt_static_signers.insert(
                                record.id,
                                Arc::new(PersistedAccessOnlySubscriptionSigner {
                                    signer,
                                    store: upstream_store.clone(),
                                    upstream_id: record.id,
                                    oauth_token_generation: record.oauth_token_generation,
                                }),
                            );
                        } else {
                            prebuilt_static_signers.insert(record.id, signer);
                        }
                    }
                }
                UpstreamKind::AnthropicOauth => {
                    if let Some(encrypted) = record.oauth_credentials.as_ref()
                        && let Ok(tokens) = encrypted.decrypt(&aead, record.id.as_bytes())
                    {
                        let requires_reconnect =
                            refresh_requires_reconnect(record.last_apply_error.as_deref());
                        let deleted = record.deleted_at_unix_secs.is_some();
                        let existing = shared
                            .anthropic_oauth_states
                            .read()
                            .get(&record.id)
                            .cloned();
                        if let Some(state) = existing {
                            let mut guard = state
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let prev_rev = guard.spec_revision;
                            guard.enabled = record.enabled;
                            guard.deleted = deleted;
                            guard.spec_revision = record.revision;
                            if record.oauth_token_generation > guard.oauth_token_generation {
                                guard.access_token = secrecy::SecretString::new(
                                    tokens.access_token.into_boxed_str(),
                                );
                                guard.expires_at_unix_secs = tokens.expires_at_unix_secs;
                                guard.never_refresh = tokens.never_refresh;
                                guard.oauth_token_generation = record.oauth_token_generation;
                                guard.requires_reconnect = requires_reconnect;
                            } else if record.oauth_token_generation == guard.oauth_token_generation
                                && (requires_reconnect || record.revision > prev_rev)
                            {
                                guard.requires_reconnect = requires_reconnect;
                            }
                        } else {
                            let mut states = shared.anthropic_oauth_states.write();
                            states.entry(record.id).or_insert_with(|| {
                                Arc::new(std::sync::RwLock::new(
                                    cc_lb_signer_anthropic_oauth::AnthropicOAuthCachedState {
                                        upstream_id: record.id,
                                        access_token: secrecy::SecretString::new(
                                            tokens.access_token.into_boxed_str(),
                                        ),
                                        expires_at_unix_secs: tokens.expires_at_unix_secs,
                                        never_refresh: tokens.never_refresh,
                                        oauth_token_generation: record.oauth_token_generation,
                                        spec_revision: record.revision,
                                        enabled: record.enabled,
                                        deleted,
                                        requires_reconnect,
                                    },
                                ))
                            });
                        }
                    } else {
                        shared.anthropic_oauth_states.write().remove(&record.id);
                    }
                }
            }
        }
        Self {
            inner: Arc::new(DbCompositeSignerFactoryInner {
                upstreams,
                upstreams_by_name,
                prebuilt_static_signers,
                upstream_store,
                aead,
                lazy_refresher,
                refresh_locks: shared.refresh_locks.clone(),
                anthropic_forced_generations: shared.anthropic_forced_generations.clone(),
                subscription_signer_states: shared.subscription_signer_states.clone(),
                anthropic_oauth_states: shared.anthropic_oauth_states.clone(),
                clock,
                gemini_config,
                antigravity_config,
                token_registry: vertex_state.token_registry.clone(),
                vertex_sources,
                vertex_status_sink: vertex_state.status_sink.clone(),
            }),
            router_chosen_upstream_name: None,
        }
    }

    fn with_router_choice_state(&self, router_chosen_upstream_name: String) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            router_chosen_upstream_name: Some(router_chosen_upstream_name),
        }
    }
    fn lookup_record(&self, upstream: &Upstream) -> Option<&UpstreamRecord> {
        let name = self.router_chosen_upstream_name.as_deref()?;
        if let Some(&idx) = self.inner.upstreams_by_name.get(name)
            && let Some(candidate) = self.inner.upstreams.get(idx)
            && upstream_matches(candidate, upstream)
        {
            return Some(candidate);
        }
        self.inner
            .upstreams
            .iter()
            .find(|candidate| self.record_matches(candidate, upstream))
    }

    #[cfg(test)]
    fn insert_or_reconcile_subscription_signer_state(
        &self,
        record: &UpstreamRecord,
        tokens: &cc_lb_aead::OAuthTokenBundle,
        signer: Arc<dyn Signer>,
    ) -> Result<Arc<RwLock<SubscriptionSignerState>>, SignerError> {
        insert_or_reconcile_subscription_signer_state(
            &self.inner.subscription_signer_states,
            record,
            tokens,
            signer,
            &self.inner.clock,
            &self.inner.gemini_config,
            &self.inner.antigravity_config,
        )
    }
}
impl cc_lb_engine::ApiKeyAwareSignerFactory for DbCompositeSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        // The downstream caller's key is deliberately not forwarded upstream.
        Arc::new(self.with_router_choice_state(router_chosen_upstream_name))
    }
}

fn build_static_signer_for_record(
    record: &UpstreamRecord,
    aead: &AeadService,
    vertex_sources: &HashMap<Uuid, Arc<TokenSource>>,
    token_registry: &TokenRegistry,
    vertex_status_sink: &VertexStatusSink,
) -> Result<Arc<dyn Signer>, SignerError> {
    match record.kind {
        UpstreamKind::AnthropicApiKey => {
            let api_key = resolve_api_key(aead, record).map_err(|reason| {
                SignerError::MissingCredentials {
                    reason: match reason {
                        ApiKeyResolutionError::Missing => {
                            "anthropic api-key upstream has no stored credential".to_owned()
                        }
                        ApiKeyResolutionError::NotDecryptable => "anthropic api-key upstream credential could not be decrypted; re-set api_key_value for this upstream".to_owned(),
                        ApiKeyResolutionError::NotUtf8 => {
                            "anthropic api-key upstream credential is not valid utf-8".to_owned()
                        }
                        ApiKeyResolutionError::InvalidHeaderValue => "anthropic api-key upstream credential is not a valid header value; re-set api_key_value for this upstream".to_owned(),
                        ApiKeyResolutionError::LegacyEncoding => "anthropic api-key upstream credential uses a retired encoding; re-set api_key_value for this upstream".to_owned(),
                    },
                }
            })?;
            Ok(Arc::new(
                cc_lb_signer_anthropic_key::AnthropicKeySigner::new(
                    api_key.as_str(),
                    record.workspace_id.as_deref(),
                )?,
            ))
        }
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey => {
            let api_key = resolve_api_key(aead, record).map_err(|reason| {
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
                UpstreamKind::OpenaiApiKey => cc_lb_signer_anthropic_key::ProviderKeyKind::Openai,
                UpstreamKind::GeminiApiKey => cc_lb_signer_anthropic_key::ProviderKeyKind::Gemini,
                _ => unreachable!("provider arm restricts upstream kind"),
            };
            Ok(Arc::new(
                cc_lb_signer_anthropic_key::ProviderKeySigner::new(kind, api_key.as_str())?,
            ))
        }
        UpstreamKind::VertexApiKey => {
            let api_key =
                resolve_api_key(aead, record).map_err(|error| SignerError::MissingCredentials {
                    reason: format!("vertex api-key credential unavailable: {error:?}"),
                })?;
            let header = HeaderValue::from_str(api_key.as_str()).map_err(|_| {
                SignerError::MissingCredentials {
                    reason: "vertex api-key credential is not a valid header".to_owned(),
                }
            })?;
            let cfg = record
                .vertex
                .clone()
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "vertex api-key upstream missing Vertex configuration".to_owned(),
                })?;
            Ok(Arc::new(VertexSigner::new(
                &record.id.to_string(),
                record.kind,
                cfg,
                record.base_url.clone(),
                record.vertex_quota_project_id.clone(),
                VertexCredentialSource::ApiKey(header),
            )))
        }
        UpstreamKind::VertexCredential => {
            if cc_lb_domain::vertex::vertex_credential_requires_replacement(
                record.last_apply_error.as_deref(),
            ) {
                return Err(SignerError::MissingCredentials {
                    reason: "vertex credential requires replacement".to_owned(),
                });
            }
            let ciphertext = record.api_key_ciphertext.as_deref().ok_or_else(|| {
                SignerError::MissingCredentials {
                    reason: "vertex credential upstream has no stored credential".to_owned(),
                }
            })?;
            // Installed views keep a strong source reference. The weak
            // registry is only consulted as a cross-view reuse path; no
            // decryption or credential parsing occurs on request builds.
            let source = vertex_sources
                .get(&record.id)
                .cloned()
                .or_else(|| token_registry.get(&record.id.to_string(), ciphertext))
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "vertex credential source unavailable".to_owned(),
                })?;
            let source: Arc<dyn BearerSource> = Arc::new(source);
            let source = ReportingBearerSource::new(
                source,
                vertex_status_sink.clone(),
                record.id,
                record.revision,
            );
            let cfg = record
                .vertex
                .clone()
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "vertex credential upstream missing Vertex configuration".to_owned(),
                })?;
            Ok(Arc::new(VertexSigner::new(
                &record.id.to_string(),
                record.kind,
                cfg,
                record.base_url.clone(),
                record.vertex_quota_project_id.clone(),
                VertexCredentialSource::Bearer(source),
            )))
        }
        UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth | UpstreamKind::AnthropicOauth => {
            unreachable!("static signer builder called for oauth upstream")
        }
    }
}

fn reconcile_existing_subscription_signer_state(
    shared: &Arc<RwLock<SubscriptionSignerState>>,
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    signer: Arc<dyn Signer>,
    clock: &cc_lb_clock::ClockHandle,
    gemini_config: &cc_lb_config::GeminiOAuthConfig,
    antigravity_config: &cc_lb_config::AntigravityOAuthConfig,
) -> Result<(), SignerError> {
    let mut guard = shared.write();
    let token_is_newer = record.oauth_token_generation > guard.oauth_token_generation;
    let target_is_newer = record.revision > guard.spec_revision
        || (record.revision == guard.spec_revision && record.base_url != guard.base_url);
    if record.revision < guard.spec_revision {
        if token_is_newer {
            let mut monotonic_record = record.clone();
            monotonic_record.revision = guard.spec_revision;
            monotonic_record.base_url = guard.base_url.clone();
            guard.signer = build_subscription_signer_with_policy(
                &monotonic_record,
                tokens,
                clock.clone(),
                Some(gemini_config),
                Some(antigravity_config),
                true,
            )?;
            let previous = std::mem::replace(
                &mut guard.access_token,
                secrecy::SecretString::new(tokens.access_token.clone().into_boxed_str()),
            );
            if previous.expose_secret() != tokens.access_token.as_str()
                && !guard
                    .historical_access_tokens
                    .iter()
                    .any(|item| item.expose_secret() == previous.expose_secret())
            {
                guard.historical_access_tokens.push(previous);
            }
            guard.expires_at_unix_secs = tokens.expires_at_unix_secs;
            guard.forced_refresh_generation = None;
            guard.oauth_token_generation = record.oauth_token_generation;
            guard.oauth_profile = tokens.oauth_profile;
            guard.project_id = tokens.project_id.clone();
            guard.account_id = tokens.account_id.clone();
            guard.never_refresh = tokens.never_refresh;
        }
    } else if token_is_newer {
        guard.signer = signer;
        let previous = std::mem::replace(
            &mut guard.access_token,
            secrecy::SecretString::new(tokens.access_token.clone().into_boxed_str()),
        );
        if previous.expose_secret() != tokens.access_token.as_str()
            && !guard
                .historical_access_tokens
                .iter()
                .any(|item| item.expose_secret() == previous.expose_secret())
        {
            guard.historical_access_tokens.push(previous);
        }
        guard.expires_at_unix_secs = tokens.expires_at_unix_secs;
        guard.forced_refresh_generation = None;
        guard.oauth_token_generation = record.oauth_token_generation;
        guard.spec_revision = record.revision;
        guard.base_url = record.base_url.clone();
        guard.oauth_profile = tokens.oauth_profile;
        guard.project_id = tokens.project_id.clone();
        guard.account_id = tokens.account_id.clone();
        guard.never_refresh = tokens.never_refresh;
    } else if target_is_newer {
        if record.oauth_token_generation == guard.oauth_token_generation {
            guard.signer = signer;
        } else {
            let newer_tokens = cc_lb_aead::OAuthTokenBundle {
                oauth_profile: guard.oauth_profile,
                access_token: guard.access_token.expose_secret().to_owned(),
                refresh_token: String::new(),
                expires_at_unix_secs: guard.expires_at_unix_secs,
                refresh_token_expires_at_unix_secs: None,
                account_id: guard.account_id.clone(),
                project_id: guard.project_id.clone(),
                scopes: Vec::new(),
                never_refresh: guard.never_refresh,
            };
            guard.signer = build_subscription_signer_with_policy(
                record,
                &newer_tokens,
                clock.clone(),
                Some(gemini_config),
                Some(antigravity_config),
                true,
            )?;
        }
        guard.spec_revision = record.revision;
        guard.base_url = record.base_url.clone();
    }
    Ok(())
}

fn insert_or_reconcile_subscription_signer_state(
    subscription_signer_states: &RwLock<HashMap<Uuid, Arc<RwLock<SubscriptionSignerState>>>>,
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    signer: Arc<dyn Signer>,
    clock: &cc_lb_clock::ClockHandle,
    gemini_config: &cc_lb_config::GeminiOAuthConfig,
    antigravity_config: &cc_lb_config::AntigravityOAuthConfig,
) -> Result<Arc<RwLock<SubscriptionSignerState>>, SignerError> {
    let mut states = subscription_signer_states.write();
    match states.entry(record.id) {
        Entry::Vacant(entry) => Ok(entry
            .insert(Arc::new(RwLock::new(SubscriptionSignerState {
                signer,
                access_token: secrecy::SecretString::new(
                    tokens.access_token.clone().into_boxed_str(),
                ),
                historical_access_tokens: Vec::new(),
                expires_at_unix_secs: tokens.expires_at_unix_secs,
                forced_refresh_generation: None,
                oauth_token_generation: record.oauth_token_generation,
                spec_revision: record.revision,
                base_url: record.base_url.clone(),
                oauth_profile: tokens.oauth_profile,
                project_id: tokens.project_id.clone(),
                account_id: tokens.account_id.clone(),
                never_refresh: tokens.never_refresh,
            })))
            .clone()),
        Entry::Occupied(entry) => {
            let shared = entry.get().clone();
            drop(states);
            reconcile_existing_subscription_signer_state(
                &shared,
                record,
                tokens,
                signer,
                clock,
                gemini_config,
                antigravity_config,
            )?;
            Ok(shared)
        }
    }
}

fn reconcile_subscription_signer_state(
    subscription_signer_states: &RwLock<HashMap<Uuid, Arc<RwLock<SubscriptionSignerState>>>>,
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    signer: Arc<dyn Signer>,
    clock: &cc_lb_clock::ClockHandle,
    gemini_config: &cc_lb_config::GeminiOAuthConfig,
    antigravity_config: &cc_lb_config::AntigravityOAuthConfig,
) -> Result<Arc<RwLock<SubscriptionSignerState>>, SignerError> {
    let existing = subscription_signer_states.read().get(&record.id).cloned();
    if let Some(shared) = existing {
        reconcile_existing_subscription_signer_state(
            &shared,
            record,
            tokens,
            signer,
            clock,
            gemini_config,
            antigravity_config,
        )?;
        Ok(shared)
    } else {
        insert_or_reconcile_subscription_signer_state(
            subscription_signer_states,
            record,
            tokens,
            signer,
            clock,
            gemini_config,
            antigravity_config,
        )
    }
}

#[async_trait]
impl SignerFactory for DbCompositeSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let record =
            self.lookup_record(upstream)
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "upstream not present in dynamic signer view".to_owned(),
                })?;
        if let Some(prebuilt) = self.inner.prebuilt_static_signers.get(&record.id) {
            return Ok(Arc::clone(prebuilt));
        }
        match record.kind {
            UpstreamKind::AnthropicApiKey
            | UpstreamKind::OpenaiApiKey
            | UpstreamKind::GeminiApiKey
            | UpstreamKind::VertexApiKey
            | UpstreamKind::VertexCredential => build_static_signer_for_record(
                record,
                &self.inner.aead,
                &self.inner.vertex_sources,
                &self.inner.token_registry,
                &self.inner.vertex_status_sink,
            ),
            UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
                if record.oauth_credentials.is_some()
                    && let Some(refresh_handle) = self.inner.lazy_refresher.clone()
                    && let Some(state) = self
                        .inner
                        .subscription_signer_states
                        .read()
                        .get(&record.id)
                        .cloned()
                {
                    let guard = state.read();
                    if !guard.never_refresh
                        && record.revision == guard.spec_revision
                        && record.base_url == guard.base_url
                        && record.oauth_token_generation <= guard.oauth_token_generation
                    {
                        let attempted_access_token = RwLock::new(secrecy::SecretString::new(
                            guard
                                .access_token
                                .expose_secret()
                                .to_owned()
                                .into_boxed_str(),
                        ));
                        drop(guard);
                        return Ok(Arc::new(RefreshingSubscriptionSigner {
                            state,
                            attempted_access_token,
                            record: record.clone(),
                            store: self.inner.upstream_store.clone(),
                            aead: self.inner.aead.clone(),
                            clock: self.inner.clock.clone(),
                            gemini_config: self.inner.gemini_config.clone(),
                            antigravity_config: self.inner.antigravity_config.clone(),
                            refresh_handle,
                            refresh_locks: self.inner.refresh_locks.clone(),
                        }));
                    }
                }
                let encrypted = record.oauth_credentials.as_ref().ok_or_else(|| {
                    SignerError::MissingCredentials {
                        reason: "subscription oauth upstream has no stored credential".to_owned(),
                    }
                })?;
                let tokens = encrypted
                    .decrypt(&self.inner.aead, record.id.as_bytes())
                    .map_err(|source| SignerError::SigningFailed {
                        reason: source.to_string(),
                    })?;
                let signer = build_subscription_signer_with_policy(
                    record,
                    &tokens,
                    self.inner.clock.clone(),
                    Some(&self.inner.gemini_config),
                    Some(&self.inner.antigravity_config),
                    true,
                )?;
                if !tokens.never_refresh
                    && let Some(refresh_handle) = self.inner.lazy_refresher.clone()
                {
                    let state = reconcile_subscription_signer_state(
                        &self.inner.subscription_signer_states,
                        record,
                        &tokens,
                        signer,
                        &self.inner.clock,
                        &self.inner.gemini_config,
                        &self.inner.antigravity_config,
                    )?;
                    let attempted_access_token = RwLock::new(secrecy::SecretString::new(
                        state
                            .read()
                            .access_token
                            .expose_secret()
                            .to_owned()
                            .into_boxed_str(),
                    ));
                    Ok(Arc::new(RefreshingSubscriptionSigner {
                        state,
                        attempted_access_token,
                        record: record.clone(),
                        store: self.inner.upstream_store.clone(),
                        aead: self.inner.aead.clone(),
                        clock: self.inner.clock.clone(),
                        gemini_config: self.inner.gemini_config.clone(),
                        antigravity_config: self.inner.antigravity_config.clone(),
                        refresh_handle,
                        refresh_locks: self.inner.refresh_locks.clone(),
                    }))
                } else if tokens.never_refresh {
                    Ok(Arc::new(PersistedAccessOnlySubscriptionSigner {
                        signer,
                        store: self.inner.upstream_store.clone(),
                        upstream_id: record.id,
                        oauth_token_generation: record.oauth_token_generation,
                    }))
                } else {
                    Ok(signer)
                }
            }
            UpstreamKind::AnthropicOauth => {
                let mut factory =
                    cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactory::for_upstream_name(
                        self.inner.upstream_store.clone(),
                        self.inner.aead.clone(),
                        record.name.clone(),
                        self.inner.clock.clone(),
                    );
                if let Some(cached) = self
                    .inner
                    .anthropic_oauth_states
                    .read()
                    .get(&record.id)
                    .cloned()
                {
                    factory = factory.with_cached_state(cached);
                }
                if let Some(handle) = &self.inner.lazy_refresher {
                    cc_lb_signer_anthropic_oauth::AnthropicOAuthSignerFactoryWithLazyRefresh::new(
                        factory,
                        handle.clone(),
                        record.id,
                    )
                    .with_shared_refresh_state(
                        self.inner.refresh_locks.clone(),
                        self.inner.anthropic_forced_generations.clone(),
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
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
    antigravity_config: Option<&cc_lb_config::AntigravityOAuthConfig>,
) -> Result<Arc<dyn Signer>, SignerError> {
    build_subscription_signer_with_policy(
        record,
        tokens,
        clock,
        gemini_config,
        antigravity_config,
        false,
    )
}

fn build_subscription_signer_with_policy(
    record: &UpstreamRecord,
    tokens: &cc_lb_aead::OAuthTokenBundle,
    clock: cc_lb_clock::ClockHandle,
    gemini_config: Option<&cc_lb_config::GeminiOAuthConfig>,
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
            let profile = match tokens.oauth_profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                    cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli
                }
                cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                    cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity
                }
            };
            let target = cc_lb_oauth_protocol::resolve_gemini_oauth_target(
                profile,
                record.base_url.as_ref(),
                antigravity_config.map(|config| &config.code_assist_url),
                None,
                cc_lb_oauth_protocol::GeminiOAuthOperation::GenerateContent,
            )
            .map_err(|_| SignerError::SigningFailed {
                reason: "Gemini Code Assist URL is invalid".to_owned(),
            })?;
            let signer = if tokens.never_refresh || !refreshing {
                cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new_for_target(
                    tokens.access_token.as_str(),
                    tokens.project_id.as_deref(),
                    tokens.never_refresh.then_some(tokens.expires_at_unix_secs),
                    clock,
                    target,
                )?
            } else {
                cc_lb_signer_anthropic_key::GeminiSubscriptionSigner::new_refreshing_for_target(
                    tokens.access_token.as_str(),
                    tokens.project_id.as_deref(),
                    tokens.expires_at_unix_secs,
                    clock,
                    target,
                )?
            };
            let signer = match tokens.oauth_profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => signer,
                cc_lb_aead::GeminiOAuthProfile::Antigravity => signer.with_antigravity_user_agent(
                    antigravity_config
                        .map(|config| config.user_agent.as_str())
                        .unwrap_or(cc_lb_oauth_protocol::ANTIGRAVITY_OAUTH_USER_AGENT),
                )?,
            };
            let metadata_default = match profile {
                cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli => {
                    gemini_config.map(|config| &config.code_assist_url)
                }
                cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity => {
                    antigravity_config.map(|config| &config.code_assist_url)
                }
            };
            let catalog_target = cc_lb_oauth_protocol::resolve_gemini_oauth_target(
                profile,
                record.base_url.as_ref(),
                None,
                metadata_default,
                cc_lb_oauth_protocol::GeminiOAuthOperation::FetchAvailableModels,
            )
            .map_err(|_| SignerError::SigningFailed {
                reason: "Gemini OAuth catalog endpoint could not be resolved".to_owned(),
            })?;
            let signer = signer.with_catalog_endpoint(catalog_target.endpoint());
            Ok(Arc::new(signer))
        }
        _ => Err(SignerError::SigningFailed {
            reason: "subscription signer requires OpenAI or Gemini OAuth".to_owned(),
        }),
    }
}
/// Records `status_401` when a provider rejects an OAuth credential, guarded by
/// the expected OAuth token generation so a concurrent reconnect is never marked.
async fn persist_subscription_unauthorized(
    store: &dyn UpstreamStore,
    upstream_id: Uuid,
    oauth_token_generation: u64,
    context: &str,
) {
    let status = UpstreamStatusUpdate {
        last_apply_error: Some(Some("status_401".to_owned())),
        expected_oauth_token_generation: Some(oauth_token_generation),
        ..UpstreamStatusUpdate::default()
    };
    match store.set_status(upstream_id, status).await {
        Ok(()) | Err(StorageError::Conflict { .. }) => {}
        Err(error) => tracing::warn!(
            upstream_id = %upstream_id,
            %error,
            context,
            "failed to record status_401 for rejected oauth credential",
        ),
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
        persist_subscription_unauthorized(
            self.store.as_ref(),
            self.upstream_id,
            self.oauth_token_generation,
            "access_only",
        )
        .await;
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        self.signer.scrub_secrets(text)
    }
}

/// Persists `status_401` if a subscription credential obtained via a forced
/// refresh is still rejected with 401 on the single retry attempt, while
/// retaining access to the shared token history for error redaction.
struct PersistedRefreshedSubscriptionSigner {
    signer: Arc<dyn Signer>,
    state: Arc<RwLock<SubscriptionSignerState>>,
    store: Arc<dyn UpstreamStore>,
    upstream_id: Uuid,
    oauth_token_generation: Option<u64>,
}

#[async_trait]
impl Signer for PersistedRefreshedSubscriptionSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        self.signer.sign(shaped, capability).await
    }

    async fn on_unauthorized(&self, _error: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }

    async fn on_retry_unauthorized(&self, _error: &UpstreamError) {
        if let Some(oauth_token_generation) = self.oauth_token_generation {
            persist_subscription_unauthorized(
                self.store.as_ref(),
                self.upstream_id,
                oauth_token_generation,
                "post_refresh_retry",
            )
            .await;
        }
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        let state = self.state.read();
        scrub_subscription_secrets(&state, self.signer.as_ref(), None, text)
    }
}

pub(crate) struct SubscriptionSignerState {
    signer: Arc<dyn Signer>,
    access_token: secrecy::SecretString,
    historical_access_tokens: Vec<secrecy::SecretString>,
    expires_at_unix_secs: u64,
    forced_refresh_generation: Option<u64>,
    oauth_token_generation: u64,
    spec_revision: u64,
    base_url: Option<url::Url>,
    oauth_profile: cc_lb_aead::GeminiOAuthProfile,
    project_id: Option<String>,
    account_id: Option<String>,
    never_refresh: bool,
}

fn scrub_secret_owned(secret: &str, text: &str) -> Option<String> {
    match scrub_secret(secret, text) {
        std::borrow::Cow::Borrowed(_) => None,
        std::borrow::Cow::Owned(value) => Some(value),
    }
}

fn scrub_subscription_secrets<'a>(
    state: &SubscriptionSignerState,
    signer: &dyn Signer,
    extra_secret: Option<&str>,
    text: &'a str,
) -> std::borrow::Cow<'a, str> {
    let mut owned = match signer.scrub_secrets(text) {
        std::borrow::Cow::Borrowed(_) => None,
        std::borrow::Cow::Owned(value) => Some(value),
    };
    for secret in extra_secret
        .into_iter()
        .chain(std::iter::once(state.access_token.expose_secret()))
        .chain(
            state
                .historical_access_tokens
                .iter()
                .map(ExposeSecret::expose_secret),
        )
    {
        let input = owned.as_deref().unwrap_or(text);
        if let Some(value) = scrub_secret_owned(secret, input) {
            owned = Some(value);
        }
    }
    owned
        .map(std::borrow::Cow::Owned)
        .unwrap_or(std::borrow::Cow::Borrowed(text))
}

struct RefreshingSubscriptionSigner {
    state: Arc<RwLock<SubscriptionSignerState>>,
    attempted_access_token: RwLock<secrecy::SecretString>,
    record: UpstreamRecord,
    store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    clock: cc_lb_clock::ClockHandle,
    gemini_config: Arc<cc_lb_config::GeminiOAuthConfig>,
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
        record: &UpstreamRecord,
        tokens: &cc_lb_aead::OAuthTokenBundle,
        forced_refresh_generation: Option<u64>,
        oauth_token_generation: u64,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        let mut state = self.state.write();
        let target_is_older = record.revision < state.spec_revision;
        let target_is_newer = record.revision > state.spec_revision
            || (record.revision == state.spec_revision && record.base_url != state.base_url);
        let token_is_older = oauth_token_generation < state.oauth_token_generation;
        if target_is_older
            && (token_is_older
                || (oauth_token_generation == state.oauth_token_generation
                    && tokens.access_token.as_str() == state.access_token.expose_secret()))
        {
            return Ok(state.signer.clone());
        }

        if target_is_older {
            // Preserve the newer installed target endpoint while adopting newer tokens.
            let mut monotonic_record = record.clone();
            monotonic_record.revision = state.spec_revision;
            monotonic_record.base_url = state.base_url.clone();
            let rebuilt = build_subscription_signer_with_policy(
                &monotonic_record,
                tokens,
                self.clock.clone(),
                Some(&self.gemini_config),
                Some(&self.antigravity_config),
                true,
            )?;
            state.signer = rebuilt.clone();
            let previous_access_token = std::mem::replace(
                &mut state.access_token,
                secrecy::SecretString::new(tokens.access_token.clone().into_boxed_str()),
            );
            if previous_access_token.expose_secret() != tokens.access_token.as_str()
                && !state.historical_access_tokens.iter().any(|existing| {
                    existing.expose_secret() == previous_access_token.expose_secret()
                })
            {
                state.historical_access_tokens.push(previous_access_token);
            }
            state.expires_at_unix_secs = tokens.expires_at_unix_secs;
            state.forced_refresh_generation = forced_refresh_generation;
            state.oauth_token_generation = oauth_token_generation;
            state.oauth_profile = tokens.oauth_profile;
            state.project_id = tokens.project_id.clone();
            state.account_id = tokens.account_id.clone();
            state.never_refresh = tokens.never_refresh;
            return Ok(rebuilt);
        }

        if token_is_older {
            if !target_is_newer {
                return Ok(state.signer.clone());
            }
            // Newer target revision arrived with an older token generation: preserve newer token
            // material while rebuilding the signer against the newer target endpoint.
            let newer_tokens = cc_lb_aead::OAuthTokenBundle {
                oauth_profile: state.oauth_profile,
                access_token: state.access_token.expose_secret().to_owned(),
                refresh_token: String::new(),
                expires_at_unix_secs: state.expires_at_unix_secs,
                refresh_token_expires_at_unix_secs: None,
                account_id: state.account_id.clone(),
                project_id: state.project_id.clone(),
                scopes: Vec::new(),
                never_refresh: state.never_refresh,
            };
            let rebuilt = build_subscription_signer_with_policy(
                record,
                &newer_tokens,
                self.clock.clone(),
                Some(&self.gemini_config),
                Some(&self.antigravity_config),
                true,
            )?;
            state.signer = rebuilt.clone();
            state.spec_revision = record.revision;
            state.base_url = record.base_url.clone();
            return Ok(rebuilt);
        }

        state.signer = signer.clone();
        let previous_access_token = std::mem::replace(
            &mut state.access_token,
            secrecy::SecretString::new(tokens.access_token.clone().into_boxed_str()),
        );
        if previous_access_token.expose_secret() != tokens.access_token.as_str()
            && !state
                .historical_access_tokens
                .iter()
                .any(|existing| existing.expose_secret() == previous_access_token.expose_secret())
        {
            state.historical_access_tokens.push(previous_access_token);
        }
        state.expires_at_unix_secs = tokens.expires_at_unix_secs;
        state.forced_refresh_generation = forced_refresh_generation;
        state.oauth_token_generation = oauth_token_generation;
        state.spec_revision = record.revision;
        state.base_url = record.base_url.clone();
        state.oauth_profile = tokens.oauth_profile;
        state.project_id = tokens.project_id.clone();
        state.account_id = tokens.account_id.clone();
        state.never_refresh = tokens.never_refresh;
        Ok(signer)
    }

    fn set_attempted_access_token(&self, access_token: &str) {
        *self.attempted_access_token.write() =
            secrecy::SecretString::new(access_token.to_owned().into_boxed_str());
    }

    async fn refresh(&self, force: bool) -> Result<(Arc<dyn Signer>, Option<u64>), SignerError> {
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
            let stored_token_is_newer =
                tokens.access_token.as_str() != self.attempted_access_token.read().expose_secret();
            let (
                state_matches_stored,
                state_is_usable,
                current_signer,
                state_forced_generation,
                state_target_matches,
            ) = {
                let state = self.state.read();
                (
                    tokens.access_token.as_str() == state.access_token.expose_secret(),
                    state.expires_at_unix_secs > unix_secs(self.clock.now()).saturating_add(30),
                    state.signer.clone(),
                    state.forced_refresh_generation,
                    record.revision < state.spec_revision
                        || (record.revision == state.spec_revision
                            && record.base_url == state.base_url),
                )
            };
            if stored_token_is_newer {
                if state_matches_stored && state_target_matches {
                    self.set_attempted_access_token(&tokens.access_token);
                    let forced = state_forced_generation
                        .filter(|generation| *generation == record.oauth_token_generation);
                    return Ok((current_signer, forced));
                }
                let signer = build_subscription_signer_with_policy(
                    &record,
                    &tokens,
                    self.clock.clone(),
                    Some(&self.gemini_config),
                    Some(&self.antigravity_config),
                    true,
                )?;
                self.set_attempted_access_token(&tokens.access_token);
                return Ok((
                    self.update_state(
                        signer,
                        &record,
                        &tokens,
                        None,
                        record.oauth_token_generation,
                    )?,
                    None,
                ));
            }
            if !force && state_is_usable {
                if state_target_matches {
                    self.set_attempted_access_token(&tokens.access_token);
                    return Ok((current_signer, None));
                }
                let signer = build_subscription_signer_with_policy(
                    &record,
                    &tokens,
                    self.clock.clone(),
                    Some(&self.gemini_config),
                    Some(&self.antigravity_config),
                    true,
                )?;
                self.set_attempted_access_token(&tokens.access_token);
                return Ok((
                    self.update_state(
                        signer,
                        &record,
                        &tokens,
                        None,
                        record.oauth_token_generation,
                    )?,
                    None,
                ));
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
            Some(&self.gemini_config),
            Some(&self.antigravity_config),
            true,
        )?;
        let forced_generation = force.then_some(record.oauth_token_generation);
        self.set_attempted_access_token(&tokens.access_token);
        Ok((
            self.update_state(
                signer,
                &record,
                &tokens,
                forced_generation,
                record.oauth_token_generation,
            )?,
            forced_generation,
        ))
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
        let (signer, token) = {
            let state = self.state.read();
            (
                state.signer.clone(),
                state.access_token.expose_secret().to_owned(),
            )
        };
        self.set_attempted_access_token(&token);
        match signer.sign(shaped, capability).await {
            Ok(signed) => Ok(signed),
            Err(SignerError::ExpiredToken { .. }) => {
                let (refreshed, _) = self.refresh(false).await?;
                refreshed.sign(retry_shape, capability).await
            }
            Err(error) => Err(error),
        }
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        match self.refresh(true).await {
            Ok((signer, oauth_token_generation)) => RetryDecision::Refresh {
                new_signer: Arc::new(PersistedRefreshedSubscriptionSigner {
                    signer,
                    state: self.state.clone(),
                    store: self.store.clone(),
                    upstream_id: self.record.id,
                    oauth_token_generation,
                }),
            },
            Err(_) => RetryDecision::Fail,
        }
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> std::borrow::Cow<'a, str> {
        let attempted = self
            .attempted_access_token
            .read()
            .expose_secret()
            .to_owned();
        let state = self.state.read();
        scrub_subscription_secrets(
            &state,
            state.signer.as_ref(),
            Some(attempted.as_str()),
            text,
        )
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
            UpstreamKind::GeminiApiKey
                | UpstreamKind::GeminiOauth
                | UpstreamKind::VertexApiKey
                | UpstreamKind::VertexCredential,
            Upstream::GeminiDirect { .. }
        )
    )
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cc_lb_clock::TestClock;
    use cc_lb_engine::lifecycle::HASH_SCHEMA_VERSION;
    use cc_lb_routing_eval::prompt_cache_simulator::V3_TOKEN_ESTIMATE_SOURCE;
    use cc_lb_storage_api::{
        MetaStore, PromptCacheObservationRecord, UpstreamCreate, UpstreamUpdate,
    };
    use cc_lb_storage_sqlite::SqliteStorage as Storage;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    use super::*;
    use crate::prompt_cache_observation_sink::{
        DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
    };
    use cc_lb_upstream::sign_request;
    use cc_lb_vertex_auth::{TokenEndpoint, TokenRequest, TokenResponse, TransportError};

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

    fn anthropic_filter_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;
        let output =
            rkyv::to_bytes::<rkyv::rancor::Error>(&cc_lb_plugin_wire::v2::FilterResponse {
                results: Box::new([cc_lb_plugin_wire::v2::PerCandidateReason {
                    upstream_id: Box::from("00000000-0000-0000-0000-000000000001"),
                    decision: Box::from("reject"),
                    reason: Box::from("anthropic-filter-ran"),
                }]),
            })
            .expect("filter response");
        let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
        let mut wasm = wat::parse_str(format!(
            "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{data}\") (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) (func (export \"cc_lb_free\") (param i32 i32 i32)) (func (export \"cc_lb_filter_anthropic\") (param i32 i32) (result i64) i64.const {}))",
            packed as i64,
        )).expect("filter module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.filter.v3",
            &<cc_lb_plugin_wire::v3::FilterRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(&mut wasm, "cc_lb.plugin.v2", br#"{
            "name":"anthropic-provider-filter","version":"1.0.0","description":"anthropic filter","usage":"test",
            "hooks":{"filter_anthropic":{"wire_version":3,"description":"anthropic filter","usage":"test","mode":"active"}}
        }"#);
        wasm
    }

    fn openai_response_only_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;
        let output = rkyv::to_bytes::<rkyv::rancor::Error>(
            &cc_lb_plugin_wire::v2::TransformResponseResult::Unchanged,
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
            "cc_lb.schema.transform_response.v2",
            &<cc_lb_plugin_wire::v2::TransformResponseRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(&mut wasm, "cc_lb.plugin.v2", br#"{
            "name":"openai-response-only","version":"1.0.0","description":"OpenAI response","usage":"test",
            "hooks":{"transform_response_openai":{"wire_version":2,"description":"response","usage":"test","mode":"active"}}
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
        filename: &str,
    ) {
        use cc_lb_storage_api::{PluginChainEntryInput, WasmBlob, WasmRegistryEntryInput};

        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
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
                openai_oauth_compat: None,
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

    fn dual_role_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;

        let filter =
            rkyv::to_bytes::<rkyv::rancor::Error>(&cc_lb_plugin_wire::v2::FilterResponse {
                results: Box::new([]),
            })
            .expect("filter response");
        let shape = rkyv::to_bytes::<rkyv::rancor::Error>(&cc_lb_plugin_wire::v2::ShapeResponse {
            url: Box::from("https://dual-role.example.test/v1/messages"),
            method: Box::from("POST"),
            headers: Box::new([]),
            body: Box::from(&b"dual-role-shaped"[..]),
        })
        .expect("shape response");
        let filter_data: String = filter.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let shape_data: String = shape.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let filter_packed = cc_lb_plugin_wire::schema::pack_ret(4096, filter.len() as u32);
        let shape_packed = cc_lb_plugin_wire::schema::pack_ret(8192, shape.len() as u32);
        let mut wasm = wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 4096) "{filter_data}")
                (data (i32.const 8192) "{shape_data}")
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 16384)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64) i64.const {filter_packed})
                (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64) i64.const {shape_packed})
            )"#
        ))
        .expect("dual-role module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.filter.v3",
            &<cc_lb_plugin_wire::v3::FilterRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.shape.v2",
            &<cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut wasm,
            "cc_lb.plugin.v2",
            br#"{
            "name":"dual-role-provider","version":"1.0.0","description":"dual role","usage":"test",
            "hooks":{
                "filter_anthropic":{"wire_version":3,"description":"filter","usage":"test","mode":"active"},
                "shape_anthropic":{"wire_version":2,"description":"shape","usage":"test","mode":"active"}
            }
        }"#,
        );
        wasm
    }

    async fn install_dual_role_provider(storage: &Storage) -> Uuid {
        use cc_lb_storage_api::{WasmBlob, WasmRegistryEntryInput};

        let wasm = dual_role_provider_fixture();
        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("dual-role inspection")
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
                    original_filename: "dual-role-provider.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: metadata.description,
                    usage: metadata.usage,
                    hook_metadata: metadata.hooks,
                    supported_slots: vec![PluginSlotKind::Router, PluginSlotKind::Shape],
                },
            )
            .await
            .expect("dual-role registry");
        registry.id
    }

    async fn attach_dual_role_provider(
        storage: &Storage,
        principal_id: Uuid,
        registry_id: Uuid,
        slot: PluginSlotKind,
    ) -> PluginChainEntry {
        storage
            .insert_chain_entry(cc_lb_storage_api::PluginChainEntryInput {
                principal_id,
                slot,
                order: 1000,
                wasm_registry_id: registry_id,
                config: serde_json::json!({}),
            })
            .await
            .expect("dual-role chain entry")
    }

    fn assert_dual_role_artifacts(
        chains: &HashMap<String, PrincipalRoutingArtifacts>,
        principal: &PrincipalRecord,
        upstream: &UpstreamRecord,
        router_entry: Option<Uuid>,
        has_shape: bool,
    ) {
        let (router, dialect) = chains.get(&principal.name).expect("principal artifacts");
        if let Some(entry_id) = router_entry {
            let router = router.as_ref().expect("router pipeline");
            assert!(
                router.instantiation_error.is_none(),
                "router failed to instantiate: {:?}",
                router.instantiation_error
            );
            assert!(
                router
                    .filters_for(Provider::Anthropic)
                    .iter()
                    .any(|filter| filter.plugin_id() == entry_id),
                "attached router must remain installed"
            );
            let filter = router
                .filters_for(Provider::Anthropic)
                .iter()
                .find(|filter| filter.plugin_id() == entry_id)
                .expect("attached filter");
            let context = cc_lb_routing::RoutingContext {
                request_id: "dual-role-filter-request".to_owned(),
                thread_id: None,
                requested_service_tier: None,
                downstream_headers: http::HeaderMap::new(),
                method: http::Method::POST,
                path: "/v1/messages".to_owned(),
                query: None,
                body_bytes: bytes::Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
                canonical_model_id: "claude-test".to_owned(),
                cache_pricing: cc_lb_domain::CachePricingSummary::default(),
            };
            let identity = cc_lb_domain::Principal {
                id: principal.name.clone(),
                kind: cc_lb_domain::PrincipalKind::ApiKey,
            };
            let output = filter
                .filter(&context, &identity, &[])
                .expect("retained filter executes");
            assert!(output.kept_upstream_ids.is_empty());
        } else if let Some(router) = router {
            assert!(router.instantiation_error.is_none());
            assert!(
                router
                    .filters_for(Provider::Anthropic)
                    .iter()
                    .all(|filter| filter.plugin_name() != "dual-role-provider"),
                "detached plugin must not remain in the router"
            );
        }
        if has_shape {
            let DialectCache::PerUpstream(dialects) = dialect else {
                panic!("attached shape must install a dialect rather than inherit");
            };
            let dialect = dialects.get(&upstream.id).expect("shape dialect");
            let context = cc_lb_upstream::DialectShapeContext {
                request_id: "dual-role-request".to_owned(),
                downstream_headers: http::HeaderMap::new(),
                method: http::Method::POST,
                path: "/v1/messages".to_owned(),
                query: None,
                body_bytes: bytes::Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
            };
            let identity = cc_lb_domain::Principal {
                id: principal.name.clone(),
                kind: cc_lb_domain::PrincipalKind::ApiKey,
            };
            let shaped = cc_lb_upstream::shape_request(
                dialect.as_ref(),
                &context,
                &cc_lb_domain::Upstream::AnthropicDirect {
                    base_url: Some("https://dual-role.example.test".parse().expect("valid URL")),
                },
                &identity,
            )
            .expect("shape executes");
            assert_eq!(
                shaped.url().as_str(),
                "https://dual-role.example.test/v1/messages"
            );
            assert_eq!(shaped.body().as_ref(), b"dual-role-shaped");
        } else {
            assert!(matches!(dialect, DialectCache::Inherit));
        }
    }

    #[tokio::test]
    async fn dual_role_principal_build_installs_both_roles_and_unchanged_rebuild() {
        let (dir, storage) = storage_fixture(53).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
        let principal = create_machine_principal(&storage, "dual-role-both").await;
        let upstream = create_upstream(&storage, "dual-role-both-upstream").await;
        let upstream = UpstreamStore::update(
            &*storage,
            upstream.id,
            upstream.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enabled upstream");
        let registry = install_dual_role_provider(&storage).await;
        let router =
            attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Router)
                .await;
        attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Shape).await;
        let aead = AeadService::from_master_key([19; 32]);
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let mut previous_keys = None;
        for _ in 0..2 {
            let (chains, keys) = build_principal_chains(
                &stores,
                &runtime,
                dir.path(),
                std::slice::from_ref(&principal),
                std::slice::from_ref(&upstream),
                &aead,
                &clock,
            )
            .await
            .expect("dual-role build");
            assert_dual_role_artifacts(&chains, &principal, &upstream, Some(router.id), true);
            assert_eq!(keys.len(), 2);
            for key in &keys {
                assert!(runtime.get_slot(key).is_some());
            }
            assert!(runtime.retain_slots(&keys).is_empty());
            if let Some(previous) = &previous_keys {
                assert_eq!(
                    &keys, previous,
                    "unchanged rebuild must preserve both role keys"
                );
            }
            previous_keys = Some(keys);
        }
    }

    #[tokio::test]
    async fn dual_role_principal_filter_then_shape_does_not_silently_inherit() {
        let (dir, storage) = storage_fixture(54).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
        let principal = create_machine_principal(&storage, "dual-role-filter-first").await;
        let upstream = create_upstream(&storage, "dual-role-filter-first-upstream").await;
        let upstream = UpstreamStore::update(
            &*storage,
            upstream.id,
            upstream.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enabled upstream");
        let registry = install_dual_role_provider(&storage).await;
        let router =
            attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Router)
                .await;
        let aead = AeadService::from_master_key([19; 32]);
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let (chains, filter_keys) = build_principal_chains(
            &stores,
            &runtime,
            dir.path(),
            std::slice::from_ref(&principal),
            std::slice::from_ref(&upstream),
            &aead,
            &clock,
        )
        .await
        .expect("filter-only build");
        assert_dual_role_artifacts(&chains, &principal, &upstream, Some(router.id), false);
        assert_eq!(filter_keys.len(), 1);
        for key in &filter_keys {
            assert!(runtime.get_slot(key).is_some());
        }
        assert!(runtime.retain_slots(&filter_keys).is_empty());
        attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Shape).await;
        for _ in 0..2 {
            let (chains, keys) = build_principal_chains(
                &stores,
                &runtime,
                dir.path(),
                std::slice::from_ref(&principal),
                std::slice::from_ref(&upstream),
                &aead,
                &clock,
            )
            .await
            .expect("filter then shape build");
            assert_dual_role_artifacts(&chains, &principal, &upstream, Some(router.id), true);
            assert_eq!(keys.len(), 2);
            assert!(filter_keys.is_subset(&keys));
            for key in &keys {
                assert!(runtime.get_slot(key).is_some());
            }
            assert!(runtime.retain_slots(&keys).is_empty());
        }
    }

    #[tokio::test]
    async fn dual_role_principal_detach_retains_other_role_and_sibling() {
        for (seed, removed_role) in [(55, PluginSlotKind::Router), (56, PluginSlotKind::Shape)] {
            let (dir, storage) = storage_fixture(seed).await;
            let stores = stores(
                storage.clone(),
                Arc::new(FakePromptCacheObservationStore::new()),
            );
            let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
            let principal = create_machine_principal(&storage, "dual-role-detach").await;
            let sibling = create_machine_principal(&storage, "dual-role-sibling").await;
            let upstream = create_upstream(&storage, "dual-role-detach-upstream").await;
            let upstream = UpstreamStore::update(
                &*storage,
                upstream.id,
                upstream.revision,
                UpstreamUpdate {
                    enabled: Some(true),
                    ..UpstreamUpdate::default()
                },
            )
            .await
            .expect("enabled upstream");
            let registry = install_dual_role_provider(&storage).await;
            let router =
                attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Router)
                    .await;
            let shape =
                attach_dual_role_provider(&storage, principal.id, registry, PluginSlotKind::Shape)
                    .await;
            let sibling_router =
                attach_dual_role_provider(&storage, sibling.id, registry, PluginSlotKind::Router)
                    .await;
            let principals = [principal, sibling];
            let aead = AeadService::from_master_key([19; 32]);
            let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
            let (chains, original_keys) = build_principal_chains(
                &stores,
                &runtime,
                dir.path(),
                &principals,
                std::slice::from_ref(&upstream),
                &aead,
                &clock,
            )
            .await
            .expect("initial detach build");
            assert_dual_role_artifacts(&chains, &principals[0], &upstream, Some(router.id), true);
            assert_dual_role_artifacts(
                &chains,
                &principals[1],
                &upstream,
                Some(sibling_router.id),
                false,
            );
            assert_eq!(original_keys.len(), 3);
            for key in &original_keys {
                assert!(runtime.get_slot(key).is_some());
            }
            assert!(runtime.retain_slots(&original_keys).is_empty());
            let removed = if removed_role == PluginSlotKind::Router {
                &router
            } else {
                &shape
            };
            storage
                .delete_chain_entry(removed.id, removed.revision)
                .await
                .expect("detach role")
                .expect("existing role");
            let (chains, retained_keys) = build_principal_chains(
                &stores,
                &runtime,
                dir.path(),
                &principals,
                std::slice::from_ref(&upstream),
                &aead,
                &clock,
            )
            .await
            .expect("post-detach build");
            assert_dual_role_artifacts(
                &chains,
                &principals[0],
                &upstream,
                (removed_role != PluginSlotKind::Router).then_some(router.id),
                removed_role != PluginSlotKind::Shape,
            );
            assert_dual_role_artifacts(
                &chains,
                &principals[1],
                &upstream,
                Some(sibling_router.id),
                false,
            );
            assert_eq!(retained_keys.len(), 2);
            assert!(retained_keys.is_subset(&original_keys));
            for key in &original_keys {
                assert!(
                    runtime.get_slot(key).is_some(),
                    "building alone must not evict active-view slots"
                );
            }
            let evicted = runtime.retain_slots(&retained_keys);
            assert_eq!(evicted.len(), 1);
            assert!(original_keys.contains(&evicted[0]));
            assert!(!retained_keys.contains(&evicted[0]));
            assert!(runtime.get_slot(&evicted[0]).is_none());
            for key in &retained_keys {
                assert!(runtime.get_slot(key).is_some());
            }
            assert!(runtime.retain_slots(&retained_keys).is_empty());
            assert_dual_role_artifacts(
                &chains,
                &principals[0],
                &upstream,
                (removed_role != PluginSlotKind::Router).then_some(router.id),
                removed_role != PluginSlotKind::Shape,
            );
            assert_dual_role_artifacts(
                &chains,
                &principals[1],
                &upstream,
                Some(sibling_router.id),
                false,
            );
        }
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
                openai_oauth_compat: None,
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let openai = UpstreamStore::update(
            storage.as_ref(),
            openai.id,
            openai.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("OpenAI enabled");
        let anthropic = UpstreamStore::update(
            storage.as_ref(),
            anthropic.id,
            anthropic.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("Anthropic enabled");
        let wasm = anthropic_filter_provider_fixture();
        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("inspection")
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
                    original_filename: "anthropic-provider-filter.wasm".to_owned(),
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
        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("response inspection")
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
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let (chains, _) = build_principal_chains(
            &stores,
            &runtime,
            dir.path(),
            std::slice::from_ref(&principal),
            &upstreams,
            &AeadService::from_master_key([19; 32]),
            &clock,
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
                    assert!(output.reason.contains("anthropic-filter-ran"));
                    ran = true;
                }
            }
            assert_eq!(ran, expected, "provider {provider:?}");
            assert_eq!(
                candidates.is_empty(),
                expected,
                "only the Anthropic chain applies the Anthropic filter rejection"
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let openai = UpstreamStore::update(
            storage.as_ref(),
            openai.id,
            openai.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("OpenAI enabled");
        let gemini = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let gemini = UpstreamStore::update(
            storage.as_ref(),
            gemini.id,
            gemini.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("Gemini enabled");
        let anthropic = UpstreamStore::update(
            storage.as_ref(),
            anthropic.id,
            anthropic.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("Anthropic enabled");

        install_shape_plugin(
            &storage,
            openai_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Openai, false),
            "openai-frame-only.wasm",
        )
        .await;
        install_shape_plugin(
            &storage,
            gemini_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Gemini, false),
            "gemini-frame-only.wasm",
        )
        .await;
        install_shape_plugin(
            &storage,
            noop_principal.id,
            frame_only_provider_fixture(cc_lb_plugin_wire::schema::ProviderFamily::Openai, true),
            "openai-frame-only-noop.wasm",
        )
        .await;

        let anthropic_id = anthropic.id;
        let openai_id = openai.id;
        let gemini_id = gemini.id;
        let principals = vec![openai_principal, gemini_principal, noop_principal];
        let upstreams = vec![anthropic, openai, gemini];
        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let (chains, _) = build_principal_chains(
            &stores,
            &runtime,
            dir.path(),
            &principals,
            &upstreams,
            &AeadService::from_master_key([19; 32]),
            &clock,
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let snapshot = UpstreamStore::update(
            storage.as_ref(),
            created.id,
            created.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
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
    async fn vertex_apply_clears_nonterminal_error_but_keeps_terminal_marker() {
        let (_dir, storage) = storage_fixture(63).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let aead = AeadService::from_master_key([19; 32]);
        let plaintext =
            br#"{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh"}"#;
        let mut upstream = UpstreamStore::create(
            storage.as_ref(),
            UpstreamCreate {
                vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                    target: cc_lb_domain::VertexTarget::Regional {
                        project_id: "project-123".into(),
                        location: "us-central1".into(),
                    },
                    api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
                }),
                vertex_credential_type: Some(cc_lb_domain::VertexCredentialType::AuthorizedUser),
                vertex_quota_project_id: None,
                id: Uuid::new_v4(),
                name: "vertex-apply-qa".to_owned(),
                kind: UpstreamKind::VertexCredential,
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
        .expect("vertex upstream created");
        let ciphertext = aead
            .encrypt(plaintext, &vertex_credential_aad(&upstream.id.to_string()))
            .expect("credential encrypts");
        upstream = UpstreamStore::update(
            storage.as_ref(),
            upstream.id,
            upstream.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(ciphertext),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("credential stored");

        for marker in [
            "stale_validation_error",
            cc_lb_domain::VERTEX_CREDENTIAL_INVALID_GRANT,
        ] {
            UpstreamStore::set_status(
                storage.as_ref(),
                upstream.id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some(marker.to_owned())),
                    ..UpstreamStatusUpdate::default()
                },
            )
            .await
            .expect("apply marker stored");
            let current = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
                .await
                .expect("load upstream")
                .expect("upstream exists");
            let statuses = apply_upstreams(&stores, &[current], &aead, 9)
                .await
                .expect("apply completes");
            let stored = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
                .await
                .expect("reload upstream")
                .expect("upstream exists");
            if marker == "stale_validation_error" {
                assert_eq!(statuses["vertex-apply-qa"].status, ApplyStatus::Active);
                assert_eq!(stored.last_apply_error, None);
            } else {
                assert_eq!(statuses["vertex-apply-qa"].status, ApplyStatus::Error);
                assert_eq!(stored.last_apply_error.as_deref(), Some(marker));
            }
        }
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
                    vertex: None,
                    vertex_credential_type: None,
                    vertex_quota_project_id: None,
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
            let snapshot = UpstreamStore::update(
                storage.as_ref(),
                stored.id,
                stored.revision,
                UpstreamUpdate {
                    enabled: Some(enabled),
                    ..UpstreamUpdate::default()
                },
            )
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
                openai_oauth_compat: None,
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

        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let mut slot_keys = HashSet::new();
        let mut has_retryable_plugin_failure = false;
        let pipeline = build_router_pipeline(
            RouterPipelineBuildContext {
                stores: &stores,
                runtime: &runtime,
                data_dir: dir.path(),
                registry: &registry,
                clock: &clock,
            },
            &principal,
            router_entries,
            &mut slot_keys,
            &[Provider::Anthropic, Provider::Openai, Provider::Gemini],
            &mut has_retryable_plugin_failure,
        )
        .await
        .expect("pipeline builds");
        assert!(!has_retryable_plugin_failure);
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
            vertex_state: Arc::new(VertexState::new(storage.clone())),
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        UpstreamStore::update(
            storage,
            created.id,
            created.revision,
            UpstreamUpdate {
                api_key_ciphertext: Some(ciphertext),
                ..UpstreamUpdate::default()
            },
        )
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
            config.oauth.gemini.as_ref(),
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
    struct VertexMintTransport {
        calls: AtomicUsize,
        responses: Mutex<VecDeque<TokenResponse>>,
    }

    #[async_trait]
    impl TokenTransport for VertexMintTransport {
        async fn post(
            &self,
            _endpoint: TokenEndpoint,
            _request: TokenRequest,
        ) -> Result<TokenResponse, TransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .responses
                .lock()
                .expect("responses")
                .pop_front()
                .expect("test response"))
        }
    }

    fn vertex_credential_record(id: Uuid, ciphertext: Vec<u8>) -> UpstreamRecord {
        UpstreamRecord {
            id,
            name: "vertex-credential-test".to_owned(),
            kind: UpstreamKind::VertexCredential,
            enabled: true,
            api_key_ciphertext: Some(ciphertext),
            vertex: Some(cc_lb_domain::VertexUpstreamConfig {
                target: cc_lb_domain::VertexTarget::Regional {
                    project_id: "project-123".into(),
                    location: "us-central1".into(),
                },
                api_version: cc_lb_domain::VertexApiVersion::V1Beta1,
            }),
            ..UpstreamRecord::default()
        }
    }

    fn vertex_signed_request() -> cc_lb_upstream::ShapedRequest {
        let mut request = shaped_request();
        request.set_url(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
                .parse()
                .expect("Vertex URL"),
        );
        request.set_body(bytes::Bytes::from_static(
            br#"{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}"#,
        ));
        request
    }

    fn vertex_token_response(token: &'static str) -> TokenResponse {
        TokenResponse {
            status: 200,
            headers: vec![],
            body: format!(
                r#"{{"access_token":"{token}","token_type":"Bearer","expires_in":3600}}"#
            )
            .into_bytes(),
        }
    }

    fn vertex_factory(
        record: UpstreamRecord,
        storage: Arc<Storage>,
        aead: Arc<AeadService>,
        state: Arc<VertexState>,
        clock: cc_lb_clock::ClockHandle,
    ) -> DbCompositeSignerFactory {
        let sources = Arc::new(build_vertex_sources(
            std::slice::from_ref(&record),
            &aead,
            &state.token_registry,
            &state.token_transport,
            &clock,
        ));
        DbCompositeSignerFactory::new_with_vertex_state(
            vec![record],
            storage,
            aead,
            None,
            clock,
            (
                cc_lb_config::GeminiOAuthConfig::default(),
                cc_lb_config::AntigravityOAuthConfig::default(),
            ),
            (state, sources),
        )
        .with_router_choice_state("vertex-credential-test".to_owned())
    }

    #[tokio::test]
    async fn qa_tok_4_factory_reuses_mint_and_replaces_rotated_ciphertext() {
        let (_dir, storage) = storage_fixture(61).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let transport = Arc::new(VertexMintTransport {
            calls: AtomicUsize::new(0),
            responses: Mutex::new(VecDeque::from([
                vertex_token_response("first"),
                vertex_token_response("second"),
            ])),
        });
        let mut state = VertexState::new(storage.clone());
        state.token_transport = transport.clone();
        let state = Arc::new(state);
        let id = Uuid::new_v4();
        let plaintext = br#"{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh"}"#;
        let first_ciphertext = aead
            .encrypt(plaintext, &vertex_credential_aad(&id.to_string()))
            .expect("first ciphertext");
        let first_record = vertex_credential_record(id, first_ciphertext);
        let factory = vertex_factory(
            first_record,
            storage.clone(),
            aead.clone(),
            state.clone(),
            clock.clone(),
        );
        let upstream = Upstream::GeminiDirect { base_url: None };
        let signer = factory.build(&upstream).await.expect("first signer");
        sign_request(signer.as_ref(), vertex_signed_request())
            .await
            .expect("first request");
        drop(signer);
        let signer = factory.build(&upstream).await.expect("cached signer");
        sign_request(signer.as_ref(), vertex_signed_request())
            .await
            .expect("cached request");
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

        let rotated_ciphertext = aead
            .encrypt(plaintext, &vertex_credential_aad(&id.to_string()))
            .expect("rotated ciphertext");
        let rotated_factory = vertex_factory(
            vertex_credential_record(id, rotated_ciphertext),
            storage,
            aead,
            state,
            clock,
        );
        let signer = rotated_factory
            .build(&upstream)
            .await
            .expect("rotated signer");
        sign_request(signer.as_ref(), vertex_signed_request())
            .await
            .expect("rotated request");
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn qa_tok_5_factory_401_refreshes_once_with_fresh_token() {
        let (_dir, storage) = storage_fixture(62).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let transport = Arc::new(VertexMintTransport {
            calls: AtomicUsize::new(0),
            responses: Mutex::new(VecDeque::from([
                vertex_token_response("stale"),
                vertex_token_response("fresh"),
            ])),
        });
        let mut state = VertexState::new(storage.clone());
        state.token_transport = transport.clone();
        let state = Arc::new(state);
        let id = Uuid::new_v4();
        let ciphertext = aead
            .encrypt(
                br#"{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh"}"#,
                &vertex_credential_aad(&id.to_string()),
            )
            .expect("ciphertext");
        let factory = vertex_factory(
            vertex_credential_record(id, ciphertext),
            storage,
            aead,
            state,
            clock,
        );
        let signer = factory
            .build(&Upstream::GeminiDirect { base_url: None })
            .await
            .expect("signer");
        let first = sign_request(signer.as_ref(), vertex_signed_request())
            .await
            .expect("stale request");
        assert_eq!(
            first.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer stale"))
        );
        let decision = signer
            .on_unauthorized(&UpstreamError::Unauthorized {
                status: http::StatusCode::UNAUTHORIZED,
                body: None,
            })
            .await;
        let RetryDecision::Refresh { new_signer } = decision else {
            panic!("401 must trigger one refresh retry");
        };
        let second = sign_request(new_signer.as_ref(), vertex_signed_request())
            .await
            .expect("fresh retry");
        assert_eq!(
            second.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer fresh"))
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn gemini_signer_profile_comes_from_encrypted_credentials() {
        let (_dir, storage) = storage_fixture(27).await;
        let stored = create_upstream(&storage, "gemini-profile").await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let mut cases = vec![
            (
                cc_lb_aead::GeminiOAuthProfile::GeminiCli,
                None,
                None,
                "cloudcode-pa.googleapis.com",
                "/v1internal",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                None,
                None,
                "daily-cloudcode-pa.googleapis.com",
                "/v1internal",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                None,
                Some("https://cloudcode-pa.googleapis.com/v1internal"),
                "cloudcode-pa.googleapis.com",
                "/v1internal",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                Some("https://configured.invalid/"),
                None,
                "configured.invalid",
                "/v1internal",
            ),
            (
                cc_lb_aead::GeminiOAuthProfile::Antigravity,
                Some("https://configured.invalid/"),
                Some("http://override.invalid/"),
                "override.invalid",
                "/v1internal",
            ),
        ];
        for profile in [
            cc_lb_aead::GeminiOAuthProfile::GeminiCli,
            cc_lb_aead::GeminiOAuthProfile::Antigravity,
        ] {
            let metadata_endpoint =
                "https://configured.invalid/metadata?tenant=one#metadata-fragment";
            cases.push((
                profile,
                Some(metadata_endpoint),
                None,
                match profile {
                    cc_lb_aead::GeminiOAuthProfile::GeminiCli => "cloudcode-pa.googleapis.com",
                    cc_lb_aead::GeminiOAuthProfile::Antigravity => "configured.invalid",
                },
                "/v1internal",
            ));
            for configured_endpoint in [None, Some(metadata_endpoint)] {
                for (endpoint, host, prefix) in [
                    (
                        "http://override.invalid/",
                        "override.invalid",
                        "/v1internal",
                    ),
                    (
                        "http://override.invalid/v1internal",
                        "override.invalid",
                        "/v1internal",
                    ),
                    (
                        "http://override.invalid/v1internal:generateContent",
                        "override.invalid",
                        "/v1internal",
                    ),
                    (
                        "http://override.invalid/gateway",
                        "override.invalid",
                        "/gateway",
                    ),
                    (
                        "http://override.invalid/gateway/v1internal",
                        "override.invalid",
                        "/gateway/v1internal",
                    ),
                    (
                        "http://override.invalid/gateway/v1internal:streamGenerateContent",
                        "override.invalid",
                        "/gateway/v1internal",
                    ),
                    (
                        "http://override.invalid/gateway/v1internal:countTokens",
                        "override.invalid",
                        "/gateway/v1internal",
                    ),
                    (
                        "http://override.invalid/gateway?tenant=one#override-fragment",
                        "override.invalid",
                        "/gateway",
                    ),
                    (
                        "https://cloudcode-pa.googleapis.com/tenant/v1internal:fetchAvailableModels?tenant=one#override-fragment",
                        "cloudcode-pa.googleapis.com",
                        "/tenant/v1internal",
                    ),
                ] {
                    cases.push((profile, configured_endpoint, Some(endpoint), host, prefix));
                }
            }
        }
        for (profile, configured_endpoint, override_endpoint, expected_host, expected_prefix) in
            cases
        {
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
            let mut gemini_config = cc_lb_config::GeminiOAuthConfig::default();
            let mut config = cc_lb_config::AntigravityOAuthConfig {
                user_agent: "antigravity/custom-config".to_owned(),
                ..Default::default()
            };
            if let Some(endpoint) = configured_endpoint {
                config.code_assist_url = endpoint.parse().expect("configured URL");
                gemini_config.code_assist_url = endpoint.parse().expect("configured Gemini URL");
            }
            let factory = DbCompositeSignerFactory::new(
                vec![record],
                storage.clone(),
                aead.clone(),
                None,
                Arc::new(TestClock::new_at_secs(1_700_000_000)),
                (gemini_config, config),
            )
            .with_router_choice_state(stored.name.clone());
            let signer = factory
                .build(&Upstream::GeminiDirect { base_url: None })
                .await
                .expect("Gemini signer");
            for action in ["generateContent", "streamGenerateContent", "countTokens"] {
                let mut shaped = shaped_request();
                shaped.set_url(
                    format!(
                        "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:{action}?tenant=client&key=synthetic"
                    )
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
                assert_eq!(
                    signed.url().scheme(),
                    if expected_host == "override.invalid" {
                        "http"
                    } else {
                        "https"
                    }
                );
                assert_eq!(
                    signed.url().path(),
                    format!("{expected_prefix}:{action}"),
                    "profile={profile:?}, override={override_endpoint:?}"
                );
                assert_eq!(
                    signed
                        .url()
                        .query_pairs()
                        .find(|(key, _)| key == "tenant")
                        .map(|(_, value)| value.into_owned()),
                    Some("client".to_owned())
                );
                assert_eq!(
                    signed
                        .url()
                        .query_pairs()
                        .find(|(key, _)| key == "alt")
                        .map(|(_, value)| value.into_owned()),
                    (action == "streamGenerateContent").then(|| "sse".to_owned())
                );
                assert_eq!(
                    signed.url().query_pairs().count(),
                    1 + usize::from(action == "streamGenerateContent")
                );
                assert!(!signed.url().query_pairs().any(|(key, _)| key == "key"));
                assert!(signed.url().fragment().is_none());
                let body: serde_json::Value =
                    serde_json::from_slice(signed.body()).expect("envelope");
                assert_eq!(body["request"]["contents"], serde_json::json!([]));
                if action == "countTokens" {
                    assert_eq!(
                        body,
                        serde_json::json!({"request": {
                            "model": "models/gemini-3-pro", "contents": []
                        }})
                    );
                } else {
                    assert_eq!(body["project"], "project-123");
                    assert_eq!(body["model"], "gemini-3-pro");
                    match profile {
                        cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                            assert!(body.get("userAgent").is_none());
                            assert!(body.get("requestId").is_none());
                        }
                        cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                            assert_eq!(body["userAgent"], "antigravity");
                            assert!(body.get("requestType").is_none());
                            assert!(
                                body["requestId"]
                                    .as_str()
                                    .is_some_and(|id| id.starts_with("agent-"))
                            );
                        }
                    }
                }
                let expected_ua = match profile {
                    cc_lb_aead::GeminiOAuthProfile::GeminiCli => "GeminiCLI/existing",
                    cc_lb_aead::GeminiOAuthProfile::Antigravity => "antigravity/custom-config",
                };
                assert_eq!(
                    signed.headers().get(http::header::USER_AGENT),
                    Some(&http::HeaderValue::from_static(expected_ua))
                );
            }
            let metadata_source: url::Url = override_endpoint
                .or(configured_endpoint)
                .unwrap_or(match profile {
                    cc_lb_aead::GeminiOAuthProfile::GeminiCli => {
                        "https://cloudcode-pa.googleapis.com"
                    }
                    cc_lb_aead::GeminiOAuthProfile::Antigravity => {
                        "https://daily-cloudcode-pa.googleapis.com"
                    }
                })
                .parse()
                .expect("metadata source");
            let metadata_prefix = if override_endpoint.is_none()
                && configured_endpoint.is_some_and(|endpoint| endpoint.contains("/metadata"))
            {
                "/metadata/v1internal"
            } else {
                expected_prefix
            };
            for path in ["/v1/models", "/v1beta/models"] {
                let mut shaped = shaped_request();
                shaped.set_method(http::Method::GET);
                shaped.set_url(
                    format!("https://generativelanguage.googleapis.com{path}")
                        .parse()
                        .expect("catalog URL"),
                );
                shaped.set_body(bytes::Bytes::new());
                let signed = cc_lb_upstream::sign_request(signer.as_ref(), shaped)
                    .await
                    .expect("signed catalog request");
                assert_eq!(signed.method(), http::Method::POST);
                assert_eq!(signed.url().origin(), metadata_source.origin());
                assert_eq!(
                    signed.url().path(),
                    format!("{metadata_prefix}:fetchAvailableModels")
                );
                assert_eq!(signed.url().query(), metadata_source.query());
                assert!(signed.url().fragment().is_none());
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(signed.body())
                        .expect("catalog body"),
                    serde_json::json!({"project": "project-123"})
                );
                assert_eq!(
                    signed.headers().get(http::header::AUTHORIZATION),
                    Some(&http::HeaderValue::from_static(
                        "Bearer gemini-profile-access"
                    ))
                );
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
                (
                    cc_lb_config::GeminiOAuthConfig::default(),
                    cc_lb_config::AntigravityOAuthConfig::default(),
                ),
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
                (
                    cc_lb_config::GeminiOAuthConfig::default(),
                    cc_lb_config::AntigravityOAuthConfig::default(),
                ),
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
            record: &cc_lb_storage_api::UpstreamRateLimitObservationRecord,
        ) -> StorageResult<()> {
            UpstreamRateLimitStateStore::put_observation(self.storage.as_ref(), record).await
        }

        async fn list_for_upstream_ids(
            &self,
            upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<cc_lb_storage_api::UpstreamRateLimitObservationRecord>> {
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let installed = UpstreamStore::update(
            storage.as_ref(),
            stored.id,
            stored.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        let installed = UpstreamStore::update(
            storage.as_ref(),
            stored.id,
            stored.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
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
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
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
        config: &cc_lb_config::Config,
    ) -> DbCompositeSignerFactory {
        DbCompositeSignerFactory::new(
            vec![record.clone()],
            storage,
            aead,
            Some(handle),
            clock,
            (
                config.oauth.gemini.clone().unwrap_or_default(),
                config.oauth.antigravity.clone().unwrap_or_default(),
            ),
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
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
            let mut config = cc_lb_config::Config::default();
            config.oauth.gemini = Some(cc_lb_config::GeminiOAuthConfig {
                code_assist_url: "https://cli-metadata.invalid/cli-metadata?tenant=cli#fragment"
                    .parse()
                    .expect("CLI metadata URL"),
                ..Default::default()
            });
            config.oauth.antigravity = Some(cc_lb_config::AntigravityOAuthConfig {
                code_assist_url:
                    "https://antigravity-metadata.invalid/antigravity-metadata?tenant=antigravity#fragment"
                        .parse().expect("Antigravity metadata URL"),
                user_agent: "antigravity/test".to_owned(),
                ..Default::default()
            });
            let (default_content, default_catalog) = match profile {
                cc_lb_aead::GeminiOAuthProfile::GeminiCli => (
                    "https://cloudcode-pa.googleapis.com/v1internal:generateContent?tenant=client",
                    "https://cli-metadata.invalid/cli-metadata/v1internal:fetchAvailableModels?tenant=cli",
                ),
                cc_lb_aead::GeminiOAuthProfile::Antigravity => (
                    "https://antigravity-metadata.invalid/v1internal:generateContent?tenant=client",
                    "https://antigravity-metadata.invalid/antigravity-metadata/v1internal:fetchAvailableModels?tenant=antigravity",
                ),
            };
            for (base_url, content_endpoint, catalog_endpoint) in [
                (None, default_content, default_catalog),
                (
                    Some("https://cloudcode-pa.googleapis.com/stored/prefix?tenant=one#fragment"),
                    "https://cloudcode-pa.googleapis.com/stored/prefix:generateContent?tenant=client",
                    "https://cloudcode-pa.googleapis.com/stored/prefix:fetchAvailableModels?tenant=one",
                ),
                (
                    Some(
                        "http://override.invalid/gateway/v1internal:loadCodeAssist?tenant=one#fragment",
                    ),
                    "http://override.invalid/gateway/v1internal:generateContent?tenant=client",
                    "http://override.invalid/gateway/v1internal:fetchAvailableModels?tenant=one",
                ),
            ] {
                for reuse_stored_token in [false, true] {
                    let (_dir, storage) = storage_fixture(42 + index as u8).await;
                    let aead = Arc::new(AeadService::from_master_key([19; 32]));
                    let clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
                    let mut tokens = expired_tokens();
                    tokens.oauth_profile = profile;
                    tokens.expires_at_unix_secs = 1_700_000_060;
                    let record = oauth_record(
                        &storage,
                        &aead,
                        "refresh-gemini",
                        UpstreamKind::GeminiOauth,
                        tokens,
                        base_url,
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
                    let signer = refreshing_factory(
                        record.clone(),
                        storage.clone(),
                        aead.clone(),
                        handle.clone(),
                        clock.clone(),
                        &config,
                    )
                    .build(&Upstream::GeminiDirect { base_url: None })
                    .await
                    .expect("Gemini signer");
                    let request = |catalog: bool| {
                        let mut shaped = shaped_request();
                        shaped.set_url(if catalog {
                            "https://generativelanguage.googleapis.com/v1beta/models"
                        } else {
                            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent?tenant=client&key=synthetic"
                        }.parse().expect("Gemini request URL"));
                        if catalog {
                            shaped.set_method(http::Method::GET);
                            shaped.set_body(bytes::Bytes::new());
                        } else {
                            shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
                        }
                        shaped.headers_mut().insert(
                            http::header::USER_AGENT,
                            http::HeaderValue::from_static("GeminiCLI/existing"),
                        );
                        shaped
                    };
                    let assert_signed = |signed: &SignedRequest, catalog: bool, token: &str| {
                        assert_eq!(
                            signed.url().as_str(),
                            if catalog {
                                catalog_endpoint
                            } else {
                                content_endpoint
                            },
                            "profile={profile:?}, stored={base_url:?}, reuse={reuse_stored_token}"
                        );
                        assert!(!signed.url().query_pairs().any(|(key, _)| key == "key"));
                        assert_eq!(signed.method(), http::Method::POST);
                        assert_eq!(
                            signed
                                .headers()
                                .get(http::header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok()),
                            Some(format!("Bearer {token}").as_str())
                        );
                        let body: serde_json::Value =
                            serde_json::from_slice(signed.body()).expect("Gemini envelope");
                        if catalog {
                            assert_eq!(body, serde_json::json!({"project": "project"}));
                            return;
                        }
                        assert_eq!(body["project"], "project");
                        assert_eq!(body["model"], "gemini-3-pro");
                        assert_eq!(body["request"]["contents"], serde_json::json!([]));
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
                                assert!(body.get("requestType").is_none());
                                assert!(
                                    body["requestId"]
                                        .as_str()
                                        .is_some_and(|id| id.starts_with("agent-"))
                                );
                                assert_eq!(
                                    signed.headers().get(http::header::USER_AGENT),
                                    Some(&http::HeaderValue::from_static("antigravity/test"))
                                );
                            }
                        }
                    };
                    for refreshed in [false, true] {
                        if refreshed {
                            clock.set_unix_secs(1_700_000_061);
                            if reuse_stored_token {
                                let mut tokens = expired_tokens();
                                tokens.oauth_profile = profile;
                                tokens.access_token = "new-gemini".to_owned();
                                tokens.expires_at_unix_secs = 1_900_000_000;
                                let encrypted = cc_lb_aead::EncryptedOAuthTokens::encrypt(
                                    &aead,
                                    &tokens,
                                    record.id.as_bytes(),
                                )
                                .expect("encrypt newer stored token");
                                UpstreamStore::store_oauth_tokens(
                                    storage.as_ref(),
                                    record.id,
                                    record.revision,
                                    encrypted,
                                    false,
                                )
                                .await
                                .expect("persist newer token");
                            }
                        }
                        for catalog in [false, true] {
                            let signed =
                                cc_lb_upstream::sign_request(signer.as_ref(), request(catalog))
                                    .await
                                    .expect("sign before and after token reconstruction");
                            assert_signed(
                                &signed,
                                catalog,
                                if refreshed {
                                    "new-gemini"
                                } else {
                                    "old-access"
                                },
                            );
                        }
                        assert_eq!(
                            handle.calls.load(Ordering::SeqCst),
                            usize::from(refreshed && !reuse_stored_token)
                        );
                    }
                    let decision = signer
                        .on_unauthorized(&UpstreamError::Unauthorized {
                            status: http::StatusCode::UNAUTHORIZED,
                            body: None,
                        })
                        .await;
                    let RetryDecision::Refresh { new_signer } = decision else {
                        panic!("Gemini 401 must rebuild a signer for one retry");
                    };
                    for catalog in [false, true] {
                        let signed =
                            cc_lb_upstream::sign_request(new_signer.as_ref(), request(catalog))
                                .await
                                .expect("sign forced-refresh retry");
                        assert_signed(&signed, catalog, "new-gemini");
                    }
                    assert_eq!(
                        handle.calls.load(Ordering::SeqCst),
                        1 + usize::from(!reuse_stored_token)
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
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
        let signer = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        )
        .build(&Upstream::OpenaiDirect { base_url: None })
        .await
        .expect("OpenAI signer");
        let error = cc_lb_upstream::sign_request(signer.as_ref(), shaped_request())
            .await
            .expect_err("refresh failure must terminate signing");
        assert!(matches!(error, SignerError::SigningFailed { .. }));
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dynamic_factory_concurrent_401_loser_persists_status_401_when_winner_drops() {
        let (_dir, storage) = storage_fixture(49).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.expires_at_unix_secs = 1_900_000_000;
        let record = oauth_record(
            &storage,
            &aead,
            "concurrent-401-loser",
            UpstreamKind::OpenaiOauth,
            tokens,
            None,
        )
        .await;
        let gate = RefreshGate::new();
        let handle = Arc::new(DynamicRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            refreshed_access_token: "winner-refreshed-token".to_owned(),
            calls: AtomicUsize::new(0),
            fail: false,
            gate: Some(gate.clone()),
        });
        let factory = refreshing_factory(
            record.clone(),
            storage.clone(),
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        );
        let upstream = Upstream::OpenaiDirect { base_url: None };
        let winner_signer = factory.build(&upstream).await.expect("winner signer");
        let loser_signer = factory.build(&upstream).await.expect("loser signer");

        let entered = gate.entered.notified();
        let winner_task = tokio::spawn({
            let winner_signer = winner_signer.clone();
            async move {
                winner_signer
                    .on_unauthorized(&UpstreamError::Unauthorized {
                        status: http::StatusCode::UNAUTHORIZED,
                        body: None,
                    })
                    .await
            }
        });
        entered.await;
        let loser_task = tokio::spawn({
            let loser_signer = loser_signer.clone();
            async move {
                loser_signer
                    .on_unauthorized(&UpstreamError::Unauthorized {
                        status: http::StatusCode::UNAUTHORIZED,
                        body: None,
                    })
                    .await
            }
        });
        gate.released.store(true, Ordering::SeqCst);
        gate.release.notify_waiters();

        let winner_decision = winner_task.await.expect("winner task joins");
        let RetryDecision::Refresh {
            new_signer: winner_retry,
        } = winner_decision
        else {
            panic!("winner must return refreshed retry signer");
        };
        drop(winner_retry);
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);

        let loser_decision = loser_task.await.expect("loser task joins");
        let RetryDecision::Refresh {
            new_signer: loser_retry,
        } = loser_decision
        else {
            panic!("loser must reuse winner's refreshed retry signer");
        };
        assert_eq!(handle.calls.load(Ordering::SeqCst), 1);
        loser_retry
            .on_retry_unauthorized(&UpstreamError::Unauthorized {
                status: http::StatusCode::UNAUTHORIZED,
                body: None,
            })
            .await;

        let stored = UpstreamStore::get_by_id(storage.as_ref(), record.id)
            .await
            .expect("upstream fetch")
            .expect("upstream exists");
        assert_eq!(stored.last_apply_error.as_deref(), Some("status_401"));
    }

    #[tokio::test]
    async fn dynamic_factory_scrub_secrets_retains_in_flight_tokens_across_many_rotations() {
        struct RotatingRefreshHandle {
            storage: Arc<Storage>,
            aead: Arc<AeadService>,
            calls: AtomicUsize,
        }

        #[async_trait]
        impl cc_lb_signer_anthropic_oauth::LazyRefreshHandle for RotatingRefreshHandle {
            async fn refresh_one(
                &self,
                upstream_id: Uuid,
            ) -> Result<(), cc_lb_signer_anthropic_oauth::LazyRefreshError> {
                let next = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
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
                tokens.access_token = format!("tok{next:02}abcdefg{next:02}");
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

        let (_dir, storage) = storage_fixture(50).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.access_token = "tok00abcdefg00".to_owned();
        tokens.expires_at_unix_secs = 1_900_000_000;
        let record = oauth_record(
            &storage,
            &aead,
            "scrub-retention",
            UpstreamKind::OpenaiOauth,
            tokens,
            None,
        )
        .await;
        let handle = Arc::new(RotatingRefreshHandle {
            storage: storage.clone(),
            aead: aead.clone(),
            calls: AtomicUsize::new(0),
        });
        let factory = refreshing_factory(
            record,
            storage,
            aead,
            handle.clone(),
            clock,
            &cc_lb_config::Config::default(),
        );
        let upstream = Upstream::OpenaiDirect { base_url: None };
        let initial_signer = factory.build(&upstream).await.expect("initial signer");
        let signed_initial =
            cc_lb_upstream::sign_request(initial_signer.as_ref(), shaped_request())
                .await
                .expect("initial sign");
        assert_eq!(
            signed_initial.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer tok00abcdefg00"))
        );

        let mut first_retry_signer: Option<Arc<dyn cc_lb_upstream::Signer>> = None;
        let mut latest_retry_signer: Option<Arc<dyn cc_lb_upstream::Signer>> = None;
        for rotation in 1..=12 {
            let current_signer = factory.build(&upstream).await.expect("signer before 401");
            let RetryDecision::Refresh { new_signer } = current_signer
                .on_unauthorized(&UpstreamError::Unauthorized {
                    status: http::StatusCode::UNAUTHORIZED,
                    body: None,
                })
                .await
            else {
                panic!("rotation {rotation} must yield retry signer");
            };
            if rotation == 1 {
                first_retry_signer = Some(new_signer.clone());
            }
            latest_retry_signer = Some(new_signer);
        }
        assert_eq!(handle.calls.load(Ordering::SeqCst), 12);

        let leaked =
            "echo tok00abcdefg00 tok01abcdefg01 tok08abcdefg08 tok11abcdefg11 tok12abcdefg12";
        for candidate in [
            initial_signer,
            first_retry_signer.expect("first retry signer"),
            latest_retry_signer.expect("latest retry signer"),
        ] {
            let scrubbed = candidate.scrub_secrets(leaked);
            assert_eq!(
                scrubbed.as_ref(),
                "echo [REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED]"
            );
            assert!(matches!(
                candidate.scrub_secrets("clean upstream error"),
                std::borrow::Cow::Borrowed("clean upstream error")
            ));
        }
    }

    #[tokio::test]
    async fn shared_subscription_signer_state_invalidates_on_base_url_change_without_generation_bump()
     {
        let (_dir, storage) = storage_fixture(49).await;
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.access_token = "gemini-tok-gen1".to_owned();
        tokens.expires_at_unix_secs = 1_900_000_000;
        let record_v1 = oauth_record(
            &storage,
            &aead,
            "gemini-target-change",
            UpstreamKind::GeminiOauth,
            tokens,
            Some("http://old-target.invalid/gateway/v1internal"),
        )
        .await;
        let handle: Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle> =
            Arc::new(DynamicRefreshHandle {
                storage: storage.clone(),
                aead: aead.clone(),
                refreshed_access_token: "gemini-tok-gen2".to_owned(),
                calls: AtomicUsize::new(0),
                fail: false,
                gate: None,
            });
        let vertex_state = Arc::new(VertexState::new(storage.clone()));
        let make_factory = |rec: UpstreamRecord| {
            DbCompositeSignerFactory::new_with_vertex_state(
                vec![rec.clone()],
                storage.clone(),
                aead.clone(),
                Some(handle.clone()),
                clock.clone(),
                (Default::default(), Default::default()),
                (vertex_state.clone(), Arc::new(HashMap::new())),
            )
            .with_router_choice_state(rec.name)
        };
        let gemini_req = |catalog: bool| {
            let mut shaped = shaped_request();
            shaped.set_url(
                if catalog {
                    "https://generativelanguage.googleapis.com/v1beta/models"
                } else {
                    "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                }
                .parse()
                .expect("url"),
            );
            if catalog {
                shaped.set_method(http::Method::GET);
                shaped.set_body(bytes::Bytes::new());
            } else {
                shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
            }
            shaped
        };

        let upstream = Upstream::GeminiDirect { base_url: None };
        let factory_v1 = make_factory(record_v1.clone());
        let signer_v1 = factory_v1.build(&upstream).await.expect("v1 signer");
        let signed_v1_content = cc_lb_upstream::sign_request(signer_v1.as_ref(), gemini_req(false))
            .await
            .expect("sign v1 content");
        let signed_v1_catalog = cc_lb_upstream::sign_request(signer_v1.as_ref(), gemini_req(true))
            .await
            .expect("sign v1 catalog");
        assert!(
            signed_v1_content
                .url()
                .as_str()
                .starts_with("http://old-target.invalid/gateway/v1internal:generateContent")
        );
        assert!(
            signed_v1_catalog
                .url()
                .as_str()
                .starts_with("http://old-target.invalid/gateway/v1internal:fetchAvailableModels")
        );

        // Spec update changes base_url (advancing revision) while oauth_token_generation is unchanged.
        let record_v2 = UpstreamStore::update(
            storage.as_ref(),
            record_v1.id,
            record_v1.revision,
            UpstreamUpdate {
                base_url: Some(Some(
                    "http://new-target.invalid/custom/v1internal"
                        .parse()
                        .expect("new url"),
                )),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("update base_url");
        assert_eq!(
            record_v2.oauth_token_generation,
            record_v1.oauth_token_generation
        );
        assert!(record_v2.revision > record_v1.revision);

        let factory_v2 = make_factory(record_v2.clone());
        let signer_v2 = factory_v2.build(&upstream).await.expect("v2 signer");
        let signed_v2_content = cc_lb_upstream::sign_request(signer_v2.as_ref(), gemini_req(false))
            .await
            .expect("sign v2 content");
        let signed_v2_catalog = cc_lb_upstream::sign_request(signer_v2.as_ref(), gemini_req(true))
            .await
            .expect("sign v2 catalog");
        assert!(
            signed_v2_content
                .url()
                .as_str()
                .starts_with("http://new-target.invalid/custom/v1internal:generateContent")
        );
        assert!(
            signed_v2_catalog
                .url()
                .as_str()
                .starts_with("http://new-target.invalid/custom/v1internal:fetchAvailableModels")
        );

        // Even when shared state already holds a newer rotated token, a stale-generation spec
        // rebuild with a newer base_url preserves the newer token AND switches the target endpoints.
        if let Some(shared) = vertex_state
            .shared_signer_state
            .subscription_signer_states
            .read()
            .get(&record_v2.id)
            .cloned()
        {
            let mut guard = shared.write();
            guard.oauth_token_generation = record_v2.oauth_token_generation + 1;
            guard.access_token = secrecy::SecretString::new("gemini-tok-gen2".to_owned().into());
        }
        let mut record_v3 = record_v2.clone();
        record_v3.revision += 1;
        record_v3.base_url = Some(
            "http://third-target.invalid/edge/v1internal"
                .parse()
                .expect("third url"),
        );
        let factory_v3 = make_factory(record_v3.clone());
        let signer_v3 = factory_v3.build(&upstream).await.expect("v3 signer");
        let signed_v3_content = cc_lb_upstream::sign_request(signer_v3.as_ref(), gemini_req(false))
            .await
            .expect("sign v3 content");
        assert!(
            signed_v3_content
                .url()
                .as_str()
                .starts_with("http://third-target.invalid/edge/v1internal:generateContent")
        );
        assert_eq!(
            signed_v3_content.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer gemini-tok-gen2"))
        );

        // An overlapping request holding the older factory_v1 must never downgrade shared state
        // back to v1's base_url or spec_revision once revision 3 has been installed.
        let signer_v1_late = factory_v1
            .build(&upstream)
            .await
            .expect("stale v1 signer build");
        let signed_v1_late =
            cc_lb_upstream::sign_request(signer_v1_late.as_ref(), gemini_req(false))
                .await
                .expect("sign with late v1 signer");
        assert!(
            signed_v1_late
                .url()
                .as_str()
                .starts_with("http://third-target.invalid/edge/v1internal:generateContent"),
            "late v1 build must not regress URL: {}",
            signed_v1_late.url()
        );
        let signed_v3_after = cc_lb_upstream::sign_request(signer_v3.as_ref(), gemini_req(false))
            .await
            .expect("re-sign v3 content after stale v1 build");
        assert!(
            signed_v3_after
                .url()
                .as_str()
                .starts_with("http://third-target.invalid/edge/v1internal:generateContent"),
            "v3 signer must retain v3 endpoint after stale v1 build: {}",
            signed_v3_after.url()
        );
        assert_eq!(
            signed_v3_after.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer gemini-tok-gen2"))
        );

        // Likewise, RefreshingSubscriptionSigner::update_state must keep target revision
        // monotonic when called with an older record revision.
        let (_, record_v1_tokens) =
            cc_lb_storage_api::UpstreamStore::get_by_id(&*storage, record_v1.id)
                .await
                .expect("load record")
                .expect("record exists")
                .oauth_credentials
                .expect("creds")
                .decrypt(&aead, record_v1.id.as_bytes())
                .map(|t| (record_v1.id, t))
                .expect("decrypt");
        let stale_refresh_handle = RefreshingSubscriptionSigner {
            state: vertex_state
                .shared_signer_state
                .subscription_signer_states
                .read()
                .get(&record_v3.id)
                .expect("shared state exists")
                .clone(),
            store: storage.clone(),
            aead: aead.clone(),
            record: record_v1.clone(),
            clock: clock.clone(),
            gemini_config: Default::default(),
            antigravity_config: Default::default(),
            refresh_handle: handle.clone(),
            refresh_locks: Arc::new(Default::default()),
            attempted_access_token: RwLock::new(secrecy::SecretString::new(
                "gemini-tok-gen1".to_owned().into_boxed_str(),
            )),
        };
        let returned_signer = stale_refresh_handle
            .update_state(
                signer_v1,
                &record_v1,
                &record_v1_tokens,
                None,
                record_v1.oauth_token_generation,
            )
            .expect("monotonic update_state succeeds");
        let signed_after_stale_refresh =
            cc_lb_upstream::sign_request(returned_signer.as_ref(), gemini_req(false))
                .await
                .expect("sign after stale refresh update_state");
        assert!(
            signed_after_stale_refresh
                .url()
                .as_str()
                .starts_with("http://third-target.invalid/edge/v1internal:generateContent"),
            "stale update_state must not regress v3 target: {}",
            signed_after_stale_refresh.url()
        );
    }

    #[tokio::test]
    async fn concurrent_first_insert_reconciles_newer_spec_revision_when_older_wins_entry() {
        let (_dir, storage) = storage_fixture(50).await;
        let aead = Arc::new(AeadService::from_master_key([23; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.access_token = "gemini-concurrent-tok".to_owned();
        tokens.expires_at_unix_secs = 1_900_000_000;
        let record_v1 = oauth_record(
            &storage,
            &aead,
            "gemini-concurrent-insert",
            UpstreamKind::GeminiOauth,
            tokens,
            Some("http://old-race.invalid/gateway/v1internal"),
        )
        .await;
        let handle: Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle> =
            Arc::new(DynamicRefreshHandle {
                storage: storage.clone(),
                aead: aead.clone(),
                refreshed_access_token: "gemini-concurrent-tok-2".to_owned(),
                calls: AtomicUsize::new(0),
                fail: false,
                gate: None,
            });
        let vertex_state = Arc::new(VertexState::new(storage.clone()));
        let make_factory = |rec: UpstreamRecord| {
            DbCompositeSignerFactory::new_with_vertex_state(
                vec![rec.clone()],
                storage.clone(),
                aead.clone(),
                Some(handle.clone()),
                clock.clone(),
                (Default::default(), Default::default()),
                (vertex_state.clone(), Arc::new(HashMap::new())),
            )
            .with_router_choice_state(rec.name)
        };
        let upstream = Upstream::GeminiDirect { base_url: None };

        // Older view R wins the initial Entry::Vacant insert first.
        let factory_r1 = make_factory(record_v1.clone());
        let signer_r1 = factory_r1.build(&upstream).await.expect("r1 signer");

        // Newer view R+1 also observed the initial read miss and now hits Entry::Occupied.
        let record_v2 = UpstreamStore::update(
            storage.as_ref(),
            record_v1.id,
            record_v1.revision,
            UpstreamUpdate {
                base_url: Some(Some(
                    "http://new-race.invalid/custom/v1internal"
                        .parse()
                        .expect("new url"),
                )),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("update base_url");
        let tokens_v1 = record_v1
            .oauth_credentials
            .as_ref()
            .expect("v1 creds")
            .decrypt(&aead, record_v1.id.as_bytes())
            .expect("decrypt v1");
        let tokens_v2 = record_v2
            .oauth_credentials
            .as_ref()
            .expect("v2 creds")
            .decrypt(&aead, record_v2.id.as_bytes())
            .expect("decrypt v2");
        let raw_signer_v2 = build_subscription_signer_with_policy(
            &record_v2,
            &tokens_v2,
            clock.clone(),
            Some(&Default::default()),
            Some(&Default::default()),
            true,
        )
        .expect("raw v2 signer");
        let raw_signer_v1 = build_subscription_signer_with_policy(
            &record_v1,
            &tokens_v1,
            clock.clone(),
            Some(&Default::default()),
            Some(&Default::default()),
            true,
        )
        .expect("raw v1 signer");

        let factory_r2 = make_factory(record_v2.clone());
        let state = factory_r2
            .insert_or_reconcile_subscription_signer_state(&record_v2, &tokens_v2, raw_signer_v2)
            .expect("occupied miss path reconciles v2");
        assert_eq!(state.read().spec_revision, record_v2.revision);
        assert_eq!(state.read().base_url, record_v2.base_url);

        // Even the R1 handle reading the shared state must now sign toward R+1's endpoint.
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                .parse()
                .expect("url"),
        );
        shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
        let signed = cc_lb_upstream::sign_request(signer_r1.as_ref(), shaped)
            .await
            .expect("sign after concurrent occupied reconciliation");
        assert!(
            signed
                .url()
                .as_str()
                .starts_with("http://new-race.invalid/custom/v1internal:generateContent"),
            "concurrent R+1 occupied insert must upgrade shared target URL: {}",
            signed.url()
        );

        // A subsequent slower R1 hitting Entry::Occupied must not downgrade R+1.
        let state_after = factory_r1
            .insert_or_reconcile_subscription_signer_state(&record_v1, &tokens_v1, raw_signer_v1)
            .expect("stale occupied miss path stays monotonic");
        assert_eq!(state_after.read().spec_revision, record_v2.revision);
        assert_eq!(state_after.read().base_url, record_v2.base_url);
    }

    struct CountingUpstreamStore {
        inner: Arc<Storage>,
        get_by_name_calls: AtomicUsize,
        get_by_id_calls: AtomicUsize,
    }

    #[async_trait]
    impl UpstreamStore for CountingUpstreamStore {
        async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            UpstreamStore::create(&*self.inner, create).await
        }

        async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
            self.get_by_name_calls.fetch_add(1, Ordering::SeqCst);
            UpstreamStore::get_by_name(&*self.inner, name).await
        }

        async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
            self.get_by_id_calls.fetch_add(1, Ordering::SeqCst);
            UpstreamStore::get_by_id(&*self.inner, id).await
        }

        async fn list(
            &self,
            after: Option<Uuid>,
            limit: usize,
        ) -> StorageResult<Vec<UpstreamRecord>> {
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

        async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
            UpstreamStore::set_status(&*self.inner, id, status).await
        }

        async fn store_oauth_tokens(
            &self,
            id: Uuid,
            expected_revision: u64,
            tokens: cc_lb_aead::EncryptedOAuthTokens,
            never_refresh: bool,
        ) -> StorageResult<UpstreamRecord> {
            UpstreamStore::store_oauth_tokens(
                &*self.inner,
                id,
                expected_revision,
                tokens,
                never_refresh,
            )
            .await
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

    #[tokio::test]
    async fn anthropic_oauth_signer_uses_shared_cache_with_zero_db_lookups_until_expired() {
        let (_dir, storage) = storage_fixture(71).await;
        let aead = Arc::new(AeadService::from_master_key([21; 32]));
        let clock = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let mut tokens = expired_tokens();
        tokens.access_token = "anthropic-tok-initial".to_owned();
        tokens.expires_at_unix_secs = 1_700_001_000;
        let record = oauth_record(
            &storage,
            &aead,
            "anthropic-cached",
            UpstreamKind::AnthropicOauth,
            tokens,
            None,
        )
        .await;
        let counting_store = Arc::new(CountingUpstreamStore {
            inner: storage.clone(),
            get_by_name_calls: AtomicUsize::new(0),
            get_by_id_calls: AtomicUsize::new(0),
        });
        let vertex_state = Arc::new(VertexState::new(counting_store.clone()));
        let factory = DbCompositeSignerFactory::new_with_vertex_state(
            vec![record.clone()],
            counting_store.clone(),
            aead.clone(),
            None,
            clock.clone(),
            (Default::default(), Default::default()),
            (vertex_state.clone(), Arc::new(HashMap::new())),
        )
        .with_router_choice_state(record.name.clone());
        let upstream = Upstream::AnthropicDirect { base_url: None };

        for _ in 0..2 {
            let signer = factory.build(&upstream).await.expect("cached signer");
            let signed = sign_request(signer.as_ref(), shaped_request())
                .await
                .expect("sign cached");
            assert_eq!(
                signed.headers().get(http::header::AUTHORIZATION),
                Some(&http::HeaderValue::from_static(
                    "Bearer anthropic-tok-initial"
                ))
            );
        }
        assert_eq!(counting_store.get_by_name_calls.load(Ordering::SeqCst), 0);
        assert_eq!(counting_store.get_by_id_calls.load(Ordering::SeqCst), 0);

        // Store refreshed token in DB and advance clock past cached expiry.
        let mut refreshed = expired_tokens();
        refreshed.access_token = "anthropic-tok-refreshed".to_owned();
        refreshed.expires_at_unix_secs = 1_700_010_000;
        let encrypted =
            cc_lb_aead::EncryptedOAuthTokens::encrypt(&aead, &refreshed, record.id.as_bytes())
                .expect("encrypt refreshed");
        UpstreamStore::store_oauth_tokens(
            storage.as_ref(),
            record.id,
            record.revision,
            encrypted,
            false,
        )
        .await
        .expect("store refreshed tokens");
        clock.advance_secs(1_100);

        let signer_after_expiry = factory.build(&upstream).await.expect("fallback signer");
        let signed_after_expiry = sign_request(signer_after_expiry.as_ref(), shaped_request())
            .await
            .expect("sign refreshed");
        assert_eq!(
            signed_after_expiry
                .headers()
                .get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static(
                "Bearer anthropic-tok-refreshed"
            ))
        );
        assert_eq!(counting_store.get_by_name_calls.load(Ordering::SeqCst), 1);

        // Next build reuses the newly warmed cache without touching DB.
        let signer_re_cached = factory.build(&upstream).await.expect("re-warmed signer");
        let signed_re_cached = sign_request(signer_re_cached.as_ref(), shaped_request())
            .await
            .expect("sign re-warmed");
        assert_eq!(
            signed_re_cached.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static(
                "Bearer anthropic-tok-refreshed"
            ))
        );
        assert_eq!(counting_store.get_by_name_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn composite_signer_factory_reuses_prebuilt_static_signers_and_avoids_redecrypting_subscription_oauth()
     {
        let (_dir, storage) = storage_fixture(72).await;
        let aead = Arc::new(AeadService::from_master_key([22; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));

        let anthropic_id = Uuid::new_v4();
        let anthropic_ciphertext = aead
            .encrypt(b"sk-ant-static", anthropic_id.as_bytes())
            .expect("encrypt anthropic");
        let anthropic_rec = UpstreamRecord {
            id: anthropic_id,
            name: "ant-static".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            api_key_ciphertext: Some(anthropic_ciphertext),
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        };

        let openai_id = Uuid::new_v4();
        let openai_ciphertext = aead
            .encrypt(b"sk-oai-static", openai_id.as_bytes())
            .expect("encrypt openai");
        let openai_rec = UpstreamRecord {
            id: openai_id,
            name: "oai-static".to_owned(),
            kind: UpstreamKind::OpenaiApiKey,
            api_key_ciphertext: Some(openai_ciphertext),
            enabled: true,
            revision: 1,
            ..UpstreamRecord::default()
        };

        let mut gemini_tokens = expired_tokens();
        gemini_tokens.access_token = "gemini-tok-fast".to_owned();
        gemini_tokens.expires_at_unix_secs = 1_900_000_000;
        let gemini_rec = oauth_record(
            &storage,
            &aead,
            "gemini-fast",
            UpstreamKind::GeminiOauth,
            gemini_tokens,
            None,
        )
        .await;
        let handle: Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle> =
            Arc::new(DynamicRefreshHandle {
                storage: storage.clone(),
                aead: aead.clone(),
                refreshed_access_token: "unused".to_owned(),
                calls: AtomicUsize::new(0),
                fail: false,
                gate: None,
            });
        let vertex_state = Arc::new(VertexState::new(storage.clone()));
        let factory = DbCompositeSignerFactory::new_with_vertex_state(
            vec![
                anthropic_rec.clone(),
                openai_rec.clone(),
                gemini_rec.clone(),
            ],
            storage.clone(),
            aead.clone(),
            Some(handle.clone()),
            clock.clone(),
            (Default::default(), Default::default()),
            (vertex_state.clone(), Arc::new(HashMap::new())),
        );

        let ant_factory = cc_lb_engine::ApiKeyAwareSignerFactory::with_router_choice(
            &factory,
            "ant-static".to_owned(),
        );
        let ant_upstream = Upstream::AnthropicDirect { base_url: None };
        let ant1 = ant_factory.build(&ant_upstream).await.expect("ant1");
        let ant2 = ant_factory.build(&ant_upstream).await.expect("ant2");
        assert!(Arc::ptr_eq(&ant1, &ant2));

        let oai_factory = cc_lb_engine::ApiKeyAwareSignerFactory::with_router_choice(
            &factory,
            "oai-static".to_owned(),
        );
        let oai_upstream = Upstream::OpenaiDirect { base_url: None };
        let oai1 = oai_factory.build(&oai_upstream).await.expect("oai1");
        let oai2 = oai_factory.build(&oai_upstream).await.expect("oai2");
        assert!(Arc::ptr_eq(&oai1, &oai2));

        // Rebuild view with the same shared VertexState, revision, base_url, and generation
        // while corrupting ciphertext bytes: build() must hit the shared fast path without AEAD decrypt.
        let mut gemini_corrupt_ciphertext = gemini_rec.clone();
        gemini_corrupt_ciphertext.oauth_credentials =
            Some(cc_lb_aead::EncryptedOAuthTokens::from_ciphertext(vec![
                1, 2, 3, 4,
            ]));
        let fast_factory = DbCompositeSignerFactory::new_with_vertex_state(
            vec![gemini_corrupt_ciphertext],
            storage,
            aead,
            Some(handle),
            clock,
            (Default::default(), Default::default()),
            (vertex_state, Arc::new(HashMap::new())),
        )
        .with_router_choice_state("gemini-fast".to_owned());
        let gemini_upstream = Upstream::GeminiDirect { base_url: None };
        let gemini_signer = fast_factory
            .build(&gemini_upstream)
            .await
            .expect("gemini fast path skips AEAD decrypt");
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                .parse()
                .expect("url"),
        );
        shaped.set_body(bytes::Bytes::from_static(br#"{"contents":[]}"#));
        let signed = sign_request(gemini_signer.as_ref(), shaped)
            .await
            .expect("sign fast path");
        assert_eq!(
            signed.headers().get(http::header::AUTHORIZATION),
            Some(&http::HeaderValue::from_static("Bearer gemini-tok-fast"))
        );
    }

    #[tokio::test]
    async fn read_verified_wasm_self_heals_corrupt_disk_cache_and_rejects_corrupt_blob() {
        let (dir, storage) = storage_fixture(71).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let wasm = anthropic_filter_provider_fixture();
        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("inspection")
            .metadata;
        storage
            .persist_wasm_upload(
                cc_lb_storage_api::WasmBlob {
                    sha256,
                    size_bytes: wasm.len() as u64,
                    bytes: wasm.clone(),
                },
                cc_lb_storage_api::WasmRegistryEntryInput {
                    name: metadata.name,
                    version: Some(metadata.version),
                    original_filename: "self-heal.wasm".to_owned(),
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
            .expect("persist valid plugin");

        ensure_wasm_cache_dirs(dir.path()).expect("cache dirs");
        let cache_path = dir
            .path()
            .join("plugins")
            .join("wasm")
            .join("cache")
            .join(format!(
                "{}.wasm",
                crate::revision_hash::hex_sha256(&sha256)
            ));
        std::fs::write(&cache_path, b"corrupted on-disk bytes").expect("seed corrupt cache file");

        let (resolved_path, loaded_bytes) = read_verified_wasm(&stores, dir.path(), sha256)
            .await
            .expect("corrupt cache must self-heal from verified storage blob");
        assert_eq!(resolved_path, cache_path);
        assert_eq!(loaded_bytes, wasm);
        assert_eq!(
            std::fs::read(&cache_path).expect("re-read repaired cache"),
            wasm
        );

        let mismatched_sha = [0x99; 32];
        let bad_blob = b"storage bytes not matching mismatched_sha".to_vec();
        storage
            .persist_wasm_upload(
                cc_lb_storage_api::WasmBlob {
                    sha256: mismatched_sha,
                    size_bytes: bad_blob.len() as u64,
                    bytes: bad_blob,
                },
                cc_lb_storage_api::WasmRegistryEntryInput {
                    name: "corrupt-storage-blob".to_owned(),
                    version: Some("1.0.0".to_owned()),
                    original_filename: "corrupt-db.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: "corrupt".to_owned(),
                    usage: "test".to_owned(),
                    hook_metadata: Default::default(),
                    supported_slots: vec![PluginSlotKind::Router],
                },
            )
            .await
            .expect("persist corrupt sha row");
        let err = read_verified_wasm(&stores, dir.path(), mismatched_sha)
            .await
            .expect_err("corrupt storage blob must fail closed");
        assert!(
            matches!(err, RebindError::Io(ref io_err) if io_err.kind() == io::ErrorKind::InvalidData),
            "expected InvalidData on mismatched storage blob, got {err:?}"
        );
    }

    #[tokio::test]
    async fn sweep_stale_wasm_disk_cache_removes_unreferenced_wasm_files_and_preserves_tmp() {
        let (dir, storage) = storage_fixture(72).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));
        let wasm = anthropic_filter_provider_fixture();
        let active_sha: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("inspection")
            .metadata;
        storage
            .persist_wasm_upload(
                cc_lb_storage_api::WasmBlob {
                    sha256: active_sha,
                    size_bytes: wasm.len() as u64,
                    bytes: wasm.clone(),
                },
                cc_lb_storage_api::WasmRegistryEntryInput {
                    name: metadata.name,
                    version: Some(metadata.version),
                    original_filename: "active.wasm".to_owned(),
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
            .expect("persist active plugin");

        ensure_wasm_cache_dirs(dir.path()).expect("cache dirs");
        let cache_dir = dir.path().join("plugins").join("wasm").join("cache");
        let active_path = cache_dir.join(format!(
            "{}.wasm",
            crate::revision_hash::hex_sha256(&active_sha)
        ));
        let stale_path = cache_dir.join(format!("{}.wasm", "aa".repeat(32)));
        let tmp_path = cache_dir.join(".tmp").join("inflight.wasm");
        std::fs::write(&active_path, &wasm).expect("write active");
        std::fs::write(&stale_path, b"stale orphan wasm").expect("write stale");
        std::fs::write(&tmp_path, b"in-flight temp").expect("write tmp");

        let _view = build_view(&stores, &runtime, dir.path()).await;

        assert!(active_path.exists(), "active registry wasm must remain");
        assert!(
            !stale_path.exists(),
            "unreferenced stale wasm cache file must be swept"
        );
        assert!(tmp_path.exists(), ".tmp directory files must be preserved");
    }

    #[tokio::test]
    async fn warmup_only_scope_skips_principal_plugin_chains_and_compiles_no_principal_slots() {
        let (dir, storage) = storage_fixture(73).await;
        let stores = stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        );
        let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("runtime"));

        let wasm = dual_role_provider_fixture();
        let sha256: [u8; 32] = Sha256::digest(&wasm).into();
        let metadata = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&wasm)
            .expect("dual-role inspection")
            .metadata;
        let (registry, _) = storage
            .persist_wasm_upload(
                cc_lb_storage_api::WasmBlob {
                    sha256,
                    size_bytes: wasm.len() as u64,
                    bytes: wasm.clone(),
                },
                cc_lb_storage_api::WasmRegistryEntryInput {
                    name: metadata.name.clone(),
                    version: Some(metadata.version),
                    original_filename: "dual-role-warmup.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: metadata.description,
                    usage: metadata.usage,
                    hook_metadata: metadata.hooks,
                    supported_slots: vec![PluginSlotKind::Router, PluginSlotKind::Shape],
                },
            )
            .await
            .expect("persist dual-role");

        let principal = PrincipalStore::create(
            storage.as_ref(),
            cc_lb_storage_api::PrincipalCreate {
                openai_oauth_compat: None,
                name: "warmup-only-principal".to_owned(),
                kind: cc_lb_storage_api::PrincipalKind::Machine,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await
        .expect("create principal");
        storage
            .insert_chain_entry(cc_lb_storage_api::PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Router,
                order: 1000,
                wasm_registry_id: registry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("insert router chain");
        storage
            .insert_chain_entry(cc_lb_storage_api::PluginChainEntryInput {
                principal_id: principal.id,
                slot: PluginSlotKind::Shape,
                order: 1000,
                wasm_registry_id: registry.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("insert shape chain");

        let upstream_rec = UpstreamStore::create(
            storage.as_ref(),
            cc_lb_storage_api::upstream::UpstreamCreate {
                id: Uuid::new_v4(),
                name: "warmup-upstream".to_owned(),
                kind: UpstreamKind::AnthropicApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: true,
                warmup_dialect_plugin: Some(
                    cc_lb_storage_api::upstream::UpstreamWarmupDialectPlugin {
                        wasm_registry_id: registry.id,
                        config: serde_json::json!({}),
                        wire_version: None,
                    },
                ),
                workspace_id: None,
                api_key_access: None,
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
            },
        )
        .await
        .expect("create upstream with warmup dialect");

        let warmup_key = crate::warmup::dialect::warmup_dialect_slot_key(sha256, &registry.name);
        runtime
            .register_shape(warmup_key.clone(), registry.name.clone(), &wasm)
            .expect("pre-register warmup slot");

        let clock: cc_lb_clock::ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let view = build_dynamic_view_with_scope(
            &stores,
            None,
            None,
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            0,
            &runtime,
            dir.path(),
            Arc::new(SubscriptionQuotaCache::new()),
            30,
            Arc::new(cc_lb_control::NoopPromptCacheObservationSink),
            1800,
            clock,
            DynamicViewPluginScope::WarmupOnly,
        )
        .await
        .expect("WarmupOnly dynamic view builds");

        let artifacts = view
            .principal_view
            .get("warmup-only-principal")
            .expect("principal present");
        let resolved_pipeline = artifacts.resolved_pipeline(None);
        assert!(
            resolved_pipeline
                .filters_for(Provider::Anthropic)
                .is_empty()
                && resolved_pipeline.provider_filters.is_none(),
            "WarmupOnly must not build principal router pipelines"
        );
        assert!(
            artifacts.resolved_dialect(upstream_rec.id).is_none(),
            "WarmupOnly must not build principal shape dialect plugins"
        );

        let principal_filter_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
            "warmup-only-principal".to_owned(),
            registry.name.clone(),
            cc_lb_runtime_wasmtime::RuntimeSlotRole::Filter,
        );
        let principal_shape_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::new(
            "warmup-only-principal".to_owned(),
            registry.name.clone(),
            cc_lb_runtime_wasmtime::RuntimeSlotRole::Shape,
        );
        assert!(runtime.get_slot(&principal_filter_key).is_none());
        assert!(runtime.get_slot(&principal_shape_key).is_none());
        assert!(
            runtime.get_slot(&warmup_key).is_some(),
            "WarmupOnly must retain warmup dialect slots across retain_slots"
        );
    }

    fn anthropic_shape_provider_fixture() -> Vec<u8> {
        use cc_lb_plugin_wire::schema::WireSchema;

        let shape = rkyv::to_bytes::<rkyv::rancor::Error>(&cc_lb_plugin_wire::v2::ShapeResponse {
            url: Box::from("https://anthropic-shape.example.test/v1/messages"),
            method: Box::from("POST"),
            headers: Box::new([]),
            body: Box::from(&b"anthropic-shaped"[..]),
        })
        .expect("shape response");
        let shape_data: String = shape.iter().map(|byte| format!("\\{byte:02x}")).collect();
        let shape_packed = cc_lb_plugin_wire::schema::pack_ret(16, shape.len() as u32);
        let mut wasm = wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 16) "{shape_data}")
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 8192)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64) i64.const {shape_packed})
            )"#
        ))
        .expect("anthropic shape module");
        provider_test_section(
            &mut wasm,
            "cc_lb.schema.shape.v2",
            &<cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut wasm,
            "cc_lb.plugin.v2",
            br#"{
            "name":"anthropic-shape-only","version":"1.0.0","description":"anthropic shape","usage":"test",
            "hooks":{"shape_anthropic":{"wire_version":2,"description":"shape","usage":"test","mode":"active"}}
        }"#,
        );
        wasm
    }

    fn hold_sole_pooling_slot(
        runtime: &Arc<WasmtimeRuntime>,
    ) -> (std::sync::mpsc::SyncSender<()>, std::thread::JoinHandle<()>) {
        let blocker_key = cc_lb_runtime_wasmtime::RuntimeSlotKey::global(
            "pool-blocker",
            cc_lb_runtime_wasmtime::RuntimeSlotRole::Filter,
        );
        let blocker_slot = runtime
            .register_filter(
                blocker_key,
                "pool-blocker",
                &anthropic_filter_provider_fixture(),
            )
            .expect("blocker filter registers before saturation");
        let blocker_dispatch =
            WasmPluginWireDispatch::from_slot(blocker_slot, runtime.config_arc());
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let handle = std::thread::spawn(move || {
            let result = blocker_dispatch.call_filter_scoped(&[], |_| {
                entered_tx.send(()).expect("signal blocker holds slot");
                let _ = release_rx.recv();
            });
            result.expect("blocker invocation completes");
        });
        entered_rx.recv().expect("blocker holds sole pooling slot");
        (release_tx, handle)
    }

    #[tokio::test]
    async fn shape_admission_failure_scopes_fail_closed_to_declared_providers() {
        let (dir, storage) = storage_fixture(74).await;
        let stores = Arc::new(stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        ));
        let principal = create_machine_principal(&storage, "scoped-shape-fail").await;
        let anthropic_upstream = create_upstream(&storage, "anthropic-upstream").await;
        let _ = UpstreamStore::update(
            &*storage,
            anthropic_upstream.id,
            anthropic_upstream.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enable anthropic");
        let openai_created = UpstreamStore::create(
            &*storage,
            UpstreamCreate {
                vertex: None,
                vertex_credential_type: None,
                vertex_quota_project_id: None,
                id: Uuid::new_v4(),
                name: "openai-upstream".to_owned(),
                kind: UpstreamKind::OpenaiApiKey,
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
        .expect("create openai");
        let _ = UpstreamStore::update(
            &*storage,
            openai_created.id,
            openai_created.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enable openai");

        install_shape_plugin(
            &storage,
            principal.id,
            anthropic_shape_provider_fixture(),
            "anthropic-shape-only.wasm",
        )
        .await;

        let runtime = Arc::new(
            WasmtimeRuntime::new(cc_lb_runtime_wasmtime::HotEngineConfig {
                allocation_strategy: cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::Pooling,
                pool_total_memories: 1,
                pool_total_core_instances: 1,
                ..cc_lb_runtime_wasmtime::HotEngineConfig::default()
            })
            .expect("pooling runtime"),
        );
        let (release_tx, blocker_handle) = hold_sole_pooling_slot(&runtime);
        let view = build_view(&stores, &runtime, dir.path()).await;
        let _ = release_tx.send(());
        blocker_handle.join().expect("blocker thread joins");

        assert!(
            view.has_retryable_plugin_failure(),
            "pool saturation during shape admission must flag view as retryable"
        );
        let cached = view
            .principal_view
            .get(&principal.name)
            .expect("principal cached");
        assert!(
            cached
                .dialect_instantiation_error(Provider::Anthropic)
                .is_some(),
            "declared Anthropic provider must fail closed"
        );
        assert!(
            cached
                .dialect_instantiation_error(Provider::Openai)
                .is_none(),
            "undeclared OpenAI provider must keep native routing"
        );
        assert!(
            cached
                .dialect_instantiation_error(Provider::Gemini)
                .is_none(),
            "undeclared Gemini provider must keep native routing"
        );
    }

    #[tokio::test]
    async fn reconcile_retries_transient_pool_saturation_bounded_and_recovers() {
        let (dir, storage) = storage_fixture(75).await;
        let stores = Arc::new(stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        ));
        let principal = create_machine_principal(&storage, "transient-retry-principal").await;
        let upstream = create_upstream(&storage, "transient-retry-anthropic").await;
        let _ = UpstreamStore::update(
            &*storage,
            upstream.id,
            upstream.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enable upstream");
        install_shape_plugin(
            &storage,
            principal.id,
            anthropic_shape_provider_fixture(),
            "anthropic-shape-only.wasm",
        )
        .await;

        let runtime = Arc::new(
            WasmtimeRuntime::new(cc_lb_runtime_wasmtime::HotEngineConfig {
                allocation_strategy: cc_lb_runtime_wasmtime::HotEngineAllocationStrategy::Pooling,
                pool_total_memories: 1,
                pool_total_core_instances: 1,
                ..cc_lb_runtime_wasmtime::HotEngineConfig::default()
            })
            .expect("pooling runtime"),
        );

        let (release_tx, blocker_handle) = hold_sole_pooling_slot(&runtime);
        let initial = build_view(&stores, &runtime, dir.path()).await;
        assert!(initial.has_retryable_plugin_failure());
        let initial_gen = initial.generation;
        let initial_hash = initial.upstream_status_snapshot.revision_hash;

        let holder = Arc::new(cc_lb_control::DynamicViewHolder::new(initial));
        let aead = Arc::new(AeadService::from_master_key([19; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
        let reconciler = crate::reconcile::Reconciler::new(
            stores.clone(),
            holder.clone(),
            runtime.clone(),
            aead.clone(),
            None,
            tokio_util::sync::CancellationToken::new(),
            dir.path().to_path_buf(),
            Arc::new(SubscriptionQuotaCache::new()),
            0,
            sink.clone(),
            1800,
            clock.clone(),
            None,
            None,
        );

        // Fast-retry ticks rebuild despite unchanged hash up to TRANSIENT_PLUGIN_RETRY_MAX_ATTEMPTS,
        // after which fast-retry stops while ordinary periodic reconciliation can still recover.
        for expected_step in 1..=crate::reconcile::TRANSIENT_PLUGIN_RETRY_MAX_ATTEMPTS {
            reconciler
                .reconcile_fast_retry_once()
                .await
                .expect("fast retry tick runs");
            let current = holder.load();
            assert_eq!(current.generation, initial_gen + u64::from(expected_step));
            assert_eq!(current.upstream_status_snapshot.revision_hash, initial_hash);
            assert!(current.has_retryable_plugin_failure());
        }
        reconciler
            .reconcile_fast_retry_once()
            .await
            .expect("post-budget fast retry tick runs");
        assert_eq!(
            holder.generation(),
            initial_gen + u64::from(crate::reconcile::TRANSIENT_PLUGIN_RETRY_MAX_ATTEMPTS),
            "fast retry must stop retrying unchanged revision after bounded attempt budget"
        );

        // Once pooling capacity returns, ordinary periodic reconciliation on the SAME reconciler
        // (with the fast-retry budget already exhausted) recovers without needing a storage hash change.
        let _ = release_tx.send(());
        blocker_handle.join().expect("blocker thread joins");

        reconciler
            .reconcile_once()
            .await
            .expect("ordinary periodic recovery reconcile runs");
        let recovered = holder.load();
        assert!(!recovered.has_retryable_plugin_failure());
        assert_eq!(
            recovered.upstream_status_snapshot.revision_hash,
            initial_hash
        );
        let cached = recovered
            .principal_view
            .get(&principal.name)
            .expect("principal cached");
        assert!(
            cached
                .dialect_instantiation_error(Provider::Anthropic)
                .is_none()
        );
        assert!(
            cached.resolved_dialect(upstream.id).is_some(),
            "shape dialect must recover once pooling capacity returns"
        );

        let recovered_gen = recovered.generation;
        reconciler
            .reconcile_once()
            .await
            .expect("steady-state reconcile");
        assert_eq!(
            holder.generation(),
            recovered_gen,
            "clean view with unchanged hash must not rebuild again"
        );
    }

    #[test]
    fn declared_shape_providers_preserves_empty_set_for_noop_only_plugin() {
        use cc_lb_plugin_wire::metadata::{HookMetadata, HookMode};
        use cc_lb_storage_api::WasmRegistryEntry;
        use std::collections::BTreeMap;

        let mut hooks = BTreeMap::new();
        hooks.insert(
            "transform_response_anthropic".to_owned(),
            HookMetadata {
                wire_version: 2,
                description: "noop".to_owned(),
                usage: "test".to_owned(),
                mode: HookMode::Noop,
            },
        );
        let entry = WasmRegistryEntry {
            id: Uuid::new_v4(),
            sha256: [0; 32],
            name: "noop-only".to_owned(),
            version: Some("1.0.0".to_owned()),
            original_filename: "noop-only.wasm".to_owned(),
            label: None,
            uploaded_at_unix_secs: 1,
            uploaded_by_admin_id: Uuid::nil(),
            refcount: 1,
            revision: 1,
            kind: "shape".to_owned(),
            description: "noop".to_owned(),
            usage: "test".to_owned(),
            hook_metadata: hooks,
            is_builtin: false,
            metadata: None,
            supported_slots: vec![PluginSlotKind::Shape],
        };
        assert!(
            declared_shape_providers(&entry, b"not-even-inspected").is_empty(),
            "known no-op-only shape plugin must not fail-close native providers"
        );

        let empty_meta = WasmRegistryEntry {
            hook_metadata: BTreeMap::new(),
            ..entry.clone()
        };
        let wasm_noop = {
            use cc_lb_plugin_wire::schema::{HookKind, WireSchema};

            let mut wasm = wat::parse_str(
                r#"(module
                    (memory (export "memory") 1)
                    (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 8192)
                    (func (export "cc_lb_free") (param i32 i32 i32))
                    (func (export "cc_lb_transform_response_openai") (param i32 i32) (result i64) i64.const 0)
                )"#,
            )
            .expect("minimal module");
            provider_test_section(
                &mut wasm,
                &format!("{}.v2", HookKind::TransformResponse.section_prefix()),
                &<cc_lb_plugin_wire::v2::TransformResponseRequest as WireSchema>::FINGERPRINT,
            );
            provider_test_section(
                &mut wasm,
                "cc_lb.plugin.v2",
                br#"{"name":"noop-wasm","version":"1.0.0","description":"noop","usage":"test","hooks":{"transform_response_openai":{"wire_version":2,"description":"noop","usage":"test","mode":"noop"}}}"#,
            );
            wasm
        };
        assert!(
            declared_shape_providers(&empty_meta, &wasm_noop).is_empty(),
            "wasm inspection showing only noop hooks must not fail-close native providers"
        );
        assert_eq!(
            declared_shape_providers(&empty_meta, b"corrupt-wasm"),
            vec![Provider::Anthropic, Provider::Openai, Provider::Gemini],
            "unknown metadata plus uninspectable bytes must still fail closed across all providers"
        );
    }

    #[tokio::test]
    async fn epoch_interrupted_shape_and_filter_probes_fail_closed_and_schedule_reconcile_retry() {
        use cc_lb_plugin_wire::schema::{HookKind, WireSchema};
        use cc_lb_storage_api::{PluginChainEntryInput, WasmBlob, WasmRegistryEntryInput};

        // Shape plugin whose Anthropic probe spins indefinitely to trigger epoch interruption.
        let mut looping_shape_wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 8192)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64)
                    (loop $hang br $hang)
                    i64.const 0
                )
            )"#,
        )
        .expect("looping shape module");
        provider_test_section(
            &mut looping_shape_wasm,
            &format!("{}.v2", HookKind::Shape.section_prefix()),
            &<cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut looping_shape_wasm,
            "cc_lb.plugin.v2",
            br#"{
                "name":"epoch-loop-shape","version":"1.0.0","description":"loop shape","usage":"test",
                "hooks":{"shape_anthropic":{"wire_version":2,"description":"shape","usage":"test","mode":"active"}}
            }"#,
        );

        let (dir, storage) = storage_fixture(76).await;
        let stores = Arc::new(stores(
            storage.clone(),
            Arc::new(FakePromptCacheObservationStore::new()),
        ));
        let principal = create_machine_principal(&storage, "epoch-shape-principal").await;
        let upstream = create_upstream(&storage, "epoch-shape-anthropic").await;
        let _ = UpstreamStore::update(
            &*storage,
            upstream.id,
            upstream.revision,
            UpstreamUpdate {
                enabled: Some(true),
                ..UpstreamUpdate::default()
            },
        )
        .await
        .expect("enable anthropic");
        install_shape_plugin(
            &storage,
            principal.id,
            looping_shape_wasm,
            "epoch-loop-shape.wasm",
        )
        .await;

        let runtime = Arc::new(
            WasmtimeRuntime::new_with_execution_timeout(
                cc_lb_runtime_wasmtime::HotEngineConfig::default(),
                std::time::Duration::from_millis(10),
            )
            .expect("short-epoch runtime"),
        );
        let initial = build_view(&stores, &runtime, dir.path()).await;
        assert!(
            initial.has_retryable_plugin_failure(),
            "epoch-interrupted shape probe must mark view retryable"
        );
        assert!(
            initial
                .principal_view
                .get(&principal.name)
                .expect("principal cached")
                .dialect_instantiation_error(Provider::Anthropic)
                .is_some(),
            "affected Anthropic provider must stay fail-closed"
        );
        assert!(
            initial
                .principal_view
                .get(&principal.name)
                .expect("principal cached")
                .dialect_instantiation_error(Provider::Openai)
                .is_none(),
            "undeclared OpenAI provider must keep native routing"
        );

        let holder = Arc::new(cc_lb_control::DynamicViewHolder::new(initial));
        let aead = Arc::new(AeadService::from_master_key([21; 32]));
        let clock: cc_lb_clock::ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000));
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
        let reconciler = crate::reconcile::Reconciler::new(
            stores.clone(),
            holder.clone(),
            runtime.clone(),
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
            None,
        );
        let before_gen = holder.generation();
        reconciler
            .reconcile_once()
            .await
            .expect("reconcile_once retries epoch-interrupted shape probe");
        assert!(
            holder.generation() > before_gen,
            "reconcile_once must retry unchanged revision hash after epoch-interrupted shape probe"
        );

        // Now verify an epoch-interrupted router filter probe also flags retryable failure
        // and keeps the principal fail-closed.
        let mut looping_filter_wasm = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 8192)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
                    (loop $hang br $hang)
                    i64.const 0
                )
            )"#,
        )
        .expect("looping filter module");
        provider_test_section(
            &mut looping_filter_wasm,
            &format!("{}.v3", HookKind::Filter.section_prefix()),
            &<cc_lb_plugin_wire::v3::FilterRequest as WireSchema>::FINGERPRINT,
        );
        provider_test_section(
            &mut looping_filter_wasm,
            "cc_lb.plugin.v2",
            br#"{
                "name":"epoch-loop-filter","version":"1.0.0","description":"loop filter","usage":"test",
                "hooks":{"filter_anthropic":{"wire_version":3,"description":"filter","usage":"test","mode":"active"}}
            }"#,
        );
        let filter_principal = create_machine_principal(&storage, "epoch-filter-principal").await;
        let filter_sha: [u8; 32] = Sha256::digest(&looping_filter_wasm).into();
        let filter_meta = cc_lb_runtime_wasmtime::inspect_wasm_agnostic(&looping_filter_wasm)
            .expect("inspect filter")
            .metadata;
        let (filter_reg, _) = storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256: filter_sha,
                    size_bytes: looping_filter_wasm.len() as u64,
                    bytes: looping_filter_wasm,
                },
                WasmRegistryEntryInput {
                    name: filter_meta.name,
                    version: Some(filter_meta.version),
                    original_filename: "epoch-loop-filter.wasm".to_owned(),
                    label: None,
                    uploaded_at_unix_secs: 1,
                    uploaded_by_admin_id: Uuid::nil(),
                    description: filter_meta.description,
                    usage: filter_meta.usage,
                    hook_metadata: filter_meta.hooks,
                    supported_slots: vec![PluginSlotKind::Router],
                },
            )
            .await
            .expect("persist filter");
        storage
            .insert_chain_entry(PluginChainEntryInput {
                principal_id: filter_principal.id,
                slot: PluginSlotKind::Router,
                order: 1000,
                wasm_registry_id: filter_reg.id,
                config: serde_json::json!({}),
            })
            .await
            .expect("insert filter chain");

        let with_filter = build_view(&stores, &runtime, dir.path()).await;
        assert!(
            with_filter.has_retryable_plugin_failure(),
            "epoch-interrupted filter probe must mark view retryable"
        );
        let filter_cached = with_filter
            .principal_view
            .get(&filter_principal.name)
            .expect("filter principal cached");
        assert!(
            filter_cached
                .resolved_pipeline(None)
                .instantiation_error
                .is_some(),
            "epoch-interrupted filter probe must keep principal fail-closed"
        );
    }
}
