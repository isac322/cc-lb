#![allow(deprecated)]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::{AnthropicOAuthConfig, PromptCacheShadowConfig};
use cc_lb_core::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView,
    RouterPipelineCache,
};
use cc_lb_core::builtin_filters::cache_affinity::CacheAffinityFilter;
use cc_lb_core::clock::SystemClock;
use cc_lb_core::{
    ApplyStatus, DynamicView, DynamicViewBuilder, ErrorNormalizer, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot, make_default_dispatcher,
};
use cc_lb_dialect_anthropic::AnthropicDirectDialect;
use cc_lb_plugin_api::{
    FilterPlugin, PluginManifest, Principal, RateLimitObservation, RequestContext, RouteDecision,
    RouteError, RouterPlugin, Signer, SignerError, SignerFactory, Upstream, UpstreamCandidate,
};
use cc_lb_runtime_extism::{ExtismRuntime, StagedSlot};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, AuditStore, PluginRegistryRepo, PluginRegistryStore, PluginSlot,
    PrincipalRecord, PrincipalStore, PromptCacheObservationStore, RateLimitKind, StorageError,
    StorageResult, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore, UpstreamRecord,
    UpstreamStore, UpstreamSubscriptionQuotaStore, WasmRegistryEntry,
};
use parking_lot::RwLock;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use cc_lb_core::lifecycle::PromptCacheObservationSinkLike;

use crate::prompt_cache_observation_cache::PromptCacheObservationCache;
use crate::prompt_cache_observation_sink::{
    DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY, PromptCacheObservationSink,
};
use crate::reconcile::collect_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

const MAX_ROUTER_CHAIN_DEPTH: usize = 16;

pub struct Stores {
    pub upstreams: Arc<dyn UpstreamStore>,
    pub principals: Arc<dyn PrincipalStore>,
    pub plugin_registry: Arc<dyn PluginRegistryStore>,
    pub upstream_rate_limits: Arc<dyn UpstreamRateLimitStateStore>,
    pub upstream_subscription_quotas: Arc<dyn UpstreamSubscriptionQuotaStore>,
    pub prompt_cache_observations: Arc<dyn PromptCacheObservationStore>,
    pub anthropic_compatibility_kv: Arc<dyn AnthropicCompatibilityKvStore>,
    pub audit: Option<Arc<dyn AuditStore>>,
    /// T43 bridge: when `Some` and a `PluginRegistryRecord` exists for the
    /// same SHA-256 as the `WasmRegistryEntry`, the bridge injects
    /// `augmented_metadata` into `PluginManifest.metadata["augmented_metadata"]`.
    /// Otherwise dispatch falls back to `legacy_dispatch_metadata` in `plugin_wrap`.
    pub plugin_registry_repo: Option<Arc<dyn PluginRegistryRepo>>,
}

#[derive(Debug, Error)]
pub enum RebindError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Plugin(#[from] cc_lb_plugin_api::RuntimeError),
}

pub async fn bridged_metadata(
    repo: Option<&Arc<dyn PluginRegistryRepo>>,
    sha256: [u8; 32],
) -> std::collections::BTreeMap<String, serde_json::Value> {
    let mut metadata = std::collections::BTreeMap::new();
    let Some(repo) = repo else {
        return metadata;
    };
    let record = match repo.get_by_sha256(&sha256).await {
        Ok(Some(record)) => record,
        Ok(None) => return metadata,
        Err(error) => {
            tracing::warn!(
                sha256 = %hex_sha256(sha256),
                %error,
                "PluginRegistry lookup failed; falling back to legacy dispatch metadata",
            );
            return metadata;
        }
    };
    match serde_json::to_value(&record.augmented_metadata) {
        Ok(value) => {
            metadata.insert("augmented_metadata".to_owned(), value);
        }
        Err(error) => {
            tracing::warn!(
                sha256 = %hex_sha256(sha256),
                %error,
                "augmented_metadata serialization failed; falling back to legacy dispatch metadata",
            );
        }
    }
    metadata
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
    runtime: &ExtismRuntime,
    data_dir: &Path,
    subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    subscription_quota_routing_max_staleness_secs: u64,
    config: &cc_lb_config::Config,
) -> Result<Arc<DynamicView>, RebindError> {
    let upstreams = list_upstreams(stores).await?;
    let all_upstream_ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    subscription_quota_cache
        .hydrate_from_store(stores, &all_upstream_ids)
        .await?;
    let prompt_cache_shadow = &config.prompt_cache_shadow;
    let (prompt_cache_observation_cache, prompt_cache_observation_sink) = if prompt_cache_shadow
        .enabled
    {
        let cache = new_prompt_cache_observation_cache(prompt_cache_shadow);
        let cache = match tokio::time::timeout(
            Duration::from_secs(5),
            cache.hydrate_from_store(stores.prompt_cache_observations.as_ref(), &all_upstream_ids),
        )
        .await
        {
            Ok(Ok(record_count)) => {
                tracing::info!(
                    record_count,
                    upstream_count = all_upstream_ids.len(),
                    "hydrated prompt cache observation cache from store"
                );
                cache
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    "failed to hydrate prompt cache observation cache from store; continuing with empty cache"
                );
                new_prompt_cache_observation_cache(prompt_cache_shadow)
            }
            Err(_) => {
                tracing::warn!(
                    timeout_secs = 5,
                    "timed out hydrating prompt cache observation cache from store; continuing with empty cache"
                );
                new_prompt_cache_observation_cache(prompt_cache_shadow)
            }
        };
        // Spawn the async observation sink writer. The JoinHandle is intentionally
        // dropped: when the sender is dropped on the next rebind the mpsc channel
        // closes and the writer task exits naturally.
        //
        let (sink, _writer) = PromptCacheObservationSink::new(
            stores.prompt_cache_observations.clone(),
            DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY,
            cc_lb_observability::cache_observation_store_kind::SQLITE,
        );
        let sink_arc: Arc<dyn PromptCacheObservationSinkLike> = Arc::new(sink);
        (Some(cache), Some(sink_arc))
    } else {
        (None, None)
    };
    let upstream_rate_limit_records = stores
        .upstream_rate_limits
        .list_for_upstream_ids(&all_upstream_ids)
        .await?;
    let upstream_rate_limit_cache = Arc::new(RwLock::new(UpstreamRateLimitCache {
        snapshots: group_rate_limit_observations(upstream_rate_limit_records),
        updated_at_unix_secs: unix_now_secs(),
    }));
    let principals = list_principals(stores).await?;
    let mut staged = Vec::new();
    let principal_chains =
        build_principal_chains(stores, runtime, data_dir, &principals, &mut staged).await?;
    let principal_view = Arc::new(PrincipalView::from_db(&principals, principal_chains));
    let now = unix_now_secs();
    let statuses = apply_upstreams(stores, &upstreams, oauth_anthropic, now).await?;
    let revision_hash = collect_revision_hash(stores).await?;
    let global_router = Arc::new(FirstCandidateRouter);
    let signer_factory = Arc::new(DbCompositeSignerFactory::new(
        upstreams.clone(),
        stores.upstreams.clone(),
        aead,
        lazy_refresher,
    ));
    let dispatcher = make_default_dispatcher(50);
    let snapshot = Arc::new(UpstreamStatusSnapshot {
        entries: statuses,
        applied_at_unix_secs: unix_now_secs(),
        revision_hash,
    });

    runtime.commit_staged(staged)?;

    let mut builder = DynamicViewBuilder::new(current_generation)
        .signer_factory(signer_factory)
        .global_router(global_router)
        .dispatcher(dispatcher)
        .global_observability_hooks(Vec::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_status_snapshot(snapshot)
        .upstream_rate_limit_cache(upstream_rate_limit_cache)
        .subscription_quota_cache(subscription_quota_cache)
        .subscription_quota_routing_max_staleness_secs(
            subscription_quota_routing_max_staleness_secs,
        )
        .upstream_records(upstreams.clone());
    if let Some(cache) = prompt_cache_observation_cache {
        builder = builder.prompt_cache_observation_cache(cache);
    }
    if let Some(sink) = prompt_cache_observation_sink {
        builder = builder.prompt_cache_observation_sink(sink);
    }
    Ok(builder.build())
}

fn new_prompt_cache_observation_cache(
    config: &PromptCacheShadowConfig,
) -> Arc<PromptCacheObservationCache> {
    Arc::new(PromptCacheObservationCache::new_with_debounce(
        Arc::new(SystemClock),
        config.grace_margin_secs,
        config.warm_set_cap,
        config.refresh_debounce_secs,
    ))
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

fn rate_limit_kind(kind: RateLimitKind) -> cc_lb_plugin_api::RateLimitKind {
    match kind {
        RateLimitKind::Requests => cc_lb_plugin_api::RateLimitKind::Requests,
        RateLimitKind::Tokens => cc_lb_plugin_api::RateLimitKind::Tokens,
        RateLimitKind::InputTokens => cc_lb_plugin_api::RateLimitKind::InputTokens,
        RateLimitKind::OutputTokens => cc_lb_plugin_api::RateLimitKind::OutputTokens,
    }
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

fn registry_entry_unsupported_slot(registry_entry: &WasmRegistryEntry, slot: PluginSlot) -> bool {
    !registry_entry.is_builtin
        && !registry_entry.supported_slots.is_empty()
        && !registry_entry.supported_slots.contains(&slot)
}

async fn build_principal_chains(
    stores: &Stores,
    runtime: &ExtismRuntime,
    data_dir: &Path,
    principals: &[PrincipalRecord],
    staged: &mut Vec<StagedSlot>,
) -> Result<HashMap<String, PrincipalRoutingArtifacts>, RebindError> {
    let registry = list_registry_by_id(stores).await?;
    let mut chains = HashMap::new();
    for principal in principals {
        let router_entries = stores
            .plugin_registry
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await?;
        let hook_entries = stores
            .plugin_registry
            .list_chain_for_principal(principal.id, PluginSlot::ObservabilityHook)
            .await?;
        let shape_entries = stores
            .plugin_registry
            .list_chain_for_principal(principal.id, PluginSlot::Shape)
            .await?;

        let router = build_router_pipeline(
            stores,
            runtime,
            data_dir,
            principal,
            router_entries,
            &registry,
            staged,
        )
        .await?;

        let mut hooks = Vec::new();
        let mut hook_entries = hook_entries;
        hook_entries.sort_by_key(|entry| entry.order);
        for entry in hook_entries {
            let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
            })?;
            let registry_entry = stores
                .plugin_registry
                .get_registry_entry_by_sha(registry_entry.sha256)
                .await?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "plugin registry sha not found")
                })?;
            if registry_entry_unsupported_slot(&registry_entry, PluginSlot::ObservabilityHook) {
                tracing::warn!(
                    target: "cc_lb_server::drift",
                    principal = %principal.name,
                    plugin = registry_entry.name.as_str(),
                    chain_entry_id = %entry.id,
                    wasm_registry_id = %registry_entry.id,
                    requested_slot = PluginSlot::ObservabilityHook.as_str(),
                    supported_slots = ?registry_entry.supported_slots,
                    "skipping observability_hook chain entry: registry entry does not support requested slot",
                );
                continue;
            }
            let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
            let manifest = PluginManifest {
                name: registry_entry.name,
                artifact: wasm_path.to_string_lossy().into_owned(),
                wire_version: entry.wire_version,
                config: entry.config,
                metadata: bridged_metadata(
                    stores.plugin_registry_repo.as_ref(),
                    registry_entry.sha256,
                )
                .await,
            };
            match runtime.instantiate_observability_for(&principal.name, &manifest.name, &manifest)
            {
                Ok((handle, slot)) => {
                    staged.push(slot);
                    hooks.push(handle);
                }
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

        let dialect = if let Some(entry) = shape_entries.into_iter().min_by_key(|entry| entry.order)
        {
            let registry_entry = registry.get(&entry.wasm_registry_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "plugin registry entry not found")
            })?;
            let registry_entry = stores
                .plugin_registry
                .get_registry_entry_by_sha(registry_entry.sha256)
                .await?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "plugin registry sha not found")
                })?;
            let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
            let manifest = PluginManifest {
                name: registry_entry.name,
                artifact: wasm_path.to_string_lossy().into_owned(),
                wire_version: entry.wire_version,
                config: entry.config,
                metadata: bridged_metadata(
                    stores.plugin_registry_repo.as_ref(),
                    registry_entry.sha256,
                )
                .await,
            };
            match runtime.instantiate_dialect_for_principal(
                &principal.name,
                &manifest.name,
                &manifest,
            ) {
                Ok((handle, slot)) => {
                    staged.push(slot);
                    DialectCache::Explicit(handle)
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
        } else {
            DialectCache::Inherit
        };

        chains.insert(principal.name.clone(), (router, hooks, dialect));
    }
    Ok(chains)
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
    let registry_entry = stores
        .plugin_registry
        .get_registry_entry_by_sha(registry_entry.sha256)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "plugin registry sha not found"))?;
    let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
    Ok(PluginManifest {
        name: registry_entry.name,
        artifact: wasm_path.to_string_lossy().into_owned(),
        wire_version: entry.wire_version,
        config: entry.config.clone(),
        metadata: bridged_metadata(stores.plugin_registry_repo.as_ref(), registry_entry.sha256)
            .await,
    })
}

async fn build_router_pipeline(
    stores: &Stores,
    runtime: &ExtismRuntime,
    data_dir: &Path,
    principal: &PrincipalRecord,
    mut router_entries: Vec<cc_lb_storage_api::PluginChainEntry>,
    registry: &HashMap<Uuid, cc_lb_storage_api::WasmRegistryEntry>,
    staged: &mut Vec<StagedSlot>,
) -> Result<Option<Arc<RouterPipelineCache>>, RebindError> {
    if router_entries.is_empty() {
        return Ok(None);
    }
    if router_entries.len() > MAX_ROUTER_CHAIN_DEPTH {
        return Ok(Some(Arc::new(RouterPipelineCache {
            user_filters: Vec::new(),
            terminal: principal.router_terminal_strategy.clone(),
            instantiation_error: Some(Arc::<str>::from(format!(
                "router chain depth {} exceeds maximum {}",
                router_entries.len(),
                MAX_ROUTER_CHAIN_DEPTH
            ))),
        })));
    }

    router_entries.sort_by_key(|entry| entry.order);
    let mut filters: Vec<Arc<dyn FilterPlugin>> = Vec::with_capacity(router_entries.len());
    let mut router_staged = Vec::with_capacity(router_entries.len());
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
            filters.push(Arc::new(CacheAffinityFilter::new()));
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
        match runtime.instantiate_filter_for(&principal.name, entry.id, &manifest.name, &manifest) {
            Ok((handle, slot)) => {
                filters.push(handle);
                router_staged.push(slot);
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

    staged.extend(router_staged);
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
        _ctx: &RequestContext,
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
}

impl DbCompositeSignerFactory {
    fn new(
        upstreams: Vec<UpstreamRecord>,
        upstream_store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    ) -> Self {
        Self {
            upstreams,
            upstream_store,
            aead,
            lazy_refresher,
            downstream_api_key: None,
            router_chosen_upstream_name: None,
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
        }
    }
}

impl cc_lb_core::ApiKeyAwareSignerFactory for DbCompositeSignerFactory {
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

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
    use std::collections::BTreeSet;

    use cc_lb_plugin_api::types::TtlClass as PluginTtlClass;
    use cc_lb_storage_api::{
        BackendKind, MetaStore, PromptCacheObservationRecord, TtlClass as StorageTtlClass,
        UpstreamCreate,
    };
    use cc_lb_storage_sqlite::SqliteStorage as Storage;

    use super::*;
    use crate::prompt_cache_observation_cache::HASH_SCHEMA_VERSION;

    const MODEL: &str = "claude-sonnet-4-5-20250929";

    #[derive(Clone)]
    struct FakePromptCacheObservationStore {
        records: Arc<Vec<PromptCacheObservationRecord>>,
        list_delay: Option<Duration>,
        upserts: Arc<tokio::sync::Mutex<Vec<PromptCacheObservationRecord>>>,
    }

    impl FakePromptCacheObservationStore {
        fn new(records: Vec<PromptCacheObservationRecord>) -> Self {
            Self {
                records: Arc::new(records),
                list_delay: None,
                upserts: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }

        fn sleeping(delay: Duration) -> Self {
            Self {
                records: Arc::new(Vec::new()),
                list_delay: Some(delay),
                upserts: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }

        async fn list_count(&self) -> usize {
            self.upserts.lock().await.len()
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
        let storage = cc_lb_storage_sqlite::open_sqlite(&database_url)
            .await
            .expect("storage");
        storage.initialize(BackendKind::Sqlite).await.unwrap();
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
            prompt_cache_observations,
            anthropic_compatibility_kv: storage.clone(),
            audit: Some(storage),
            plugin_registry_repo: None,
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
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await
        .expect("upstream created")
    }

    async fn build_view(
        stores: &Stores,
        runtime: &ExtismRuntime,
        data_dir: &Path,
    ) -> Arc<DynamicView> {
        let mut config = cc_lb_config::Config::default();
        config.prompt_cache_shadow.enabled = true;
        build_view_with_config(stores, runtime, data_dir, config).await
    }

    async fn build_view_with_config(
        stores: &Stores,
        runtime: &ExtismRuntime,
        data_dir: &Path,
        config: cc_lb_config::Config,
    ) -> Arc<DynamicView> {
        build_dynamic_view(
            stores,
            &AnthropicOAuthConfig::default(),
            Arc::new(AeadService::from_master_key([19; 32])),
            None,
            0,
            runtime,
            data_dir,
            Arc::new(SubscriptionQuotaCache::new()),
            1800,
            &config,
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
            prefix_hash: prefix_hash.to_owned(),
            ttl_class: StorageTtlClass::Ephemeral5m,
            expires_at_unix_secs: 4_100_000_000,
            last_observed_at_unix_secs,
            hash_schema_version: HASH_SCHEMA_VERSION,
        }
    }

    #[tokio::test]
    async fn includes_prompt_cache() {
        let (dir, storage) = storage_fixture(19).await;
        let upstream = create_upstream(&storage, "prompt-cache-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new(vec![
            prompt_record(upstream.id, "hash-a", 1_700_000_001),
            prompt_record(upstream.id, "hash-b", 1_700_000_002),
            prompt_record(upstream.id, "hash-c", 1_700_000_003),
        ]));
        let stores = stores(storage, prompt_store);
        let runtime = ExtismRuntime::new();

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;

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
                unix_now_secs(),
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
    async fn observation_sink_routes_records_to_store() {
        let (dir, storage) = storage_fixture(22).await;
        let upstream = create_upstream(&storage, "sink-wiring-upstream").await;
        let prompt_store = Arc::new(FakePromptCacheObservationStore::new(Vec::new()));
        let stores = stores(storage, prompt_store.clone());
        let runtime = ExtismRuntime::new();

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;

        let sink = dynamic_view
            .prompt_cache_observation_sink_opt()
            .expect("sink wired when prompt_cache_shadow enabled")
            .clone();
        let record = PromptCacheObservationRecord {
            upstream_id: upstream.id,
            canonical_model_id: MODEL.to_owned(),
            prefix_hash: "sink-wiring-prefix".to_owned(),
            ttl_class: cc_lb_storage_api::TtlClass::Ephemeral5m,
            expires_at_unix_secs: 4_100_000_300,
            last_observed_at_unix_secs: 1_700_000_000,
            hash_schema_version: HASH_SCHEMA_VERSION,
        };
        sink.enqueue(record.clone())
            .expect("enqueue succeeds while writer is alive");
        for _ in 0..50 {
            if prompt_store.list_count().await >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let stored = prompt_store.list_all().await;
        assert_eq!(
            stored.len(),
            1,
            "observation enqueued through DynamicView sink must reach the production store"
        );
        assert_eq!(stored[0].prefix_hash, "sink-wiring-prefix");
        assert_eq!(stored[0].upstream_id, upstream.id);
    }

    #[tokio::test]
    async fn hydrate_timeout_logs_warn_and_continues() {
        let (dir, storage) = storage_fixture(20).await;
        let upstream = create_upstream(&storage, "timeout-upstream").await;
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::sleeping(
                Duration::from_secs(30),
            )),
        );
        let runtime = ExtismRuntime::new();
        let started = tokio::time::Instant::now();

        let dynamic_view = build_view(&stores, &runtime, dir.path()).await;

        assert!(started.elapsed() <= Duration::from_secs(6));
        let snapshot = dynamic_view
            .prompt_cache_observation_cache_opt()
            .expect("prompt cache enabled")
            .snapshot_for_upstream(
                upstream.id,
                MODEL,
                &[("hash-a".to_owned(), PluginTtlClass::Ephemeral5m)],
                unix_now_secs(),
            );
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn disabled_flag_skips_cache_construction() {
        let (dir, storage) = storage_fixture(21).await;
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let runtime = ExtismRuntime::new();
        let mut config = cc_lb_config::Config::default();
        config.prompt_cache_shadow.enabled = false;

        let dynamic_view = build_view_with_config(&stores, &runtime, dir.path(), config).await;

        assert!(dynamic_view.prompt_cache_observation_cache_opt().is_none());
        assert!(
            dynamic_view.prompt_cache_observation_sink_opt().is_none(),
            "prompt cache observation sink must NOT be wired when prompt_cache_shadow is disabled"
        );
    }

    #[tokio::test]
    async fn tunables_propagate_to_cache() {
        let (dir, storage) = storage_fixture(22).await;
        let stores = stores(
            storage,
            Arc::new(FakePromptCacheObservationStore::new(Vec::new())),
        );
        let runtime = ExtismRuntime::new();
        let mut config = cc_lb_config::Config::default();
        config.prompt_cache_shadow.enabled = true;
        config.prompt_cache_shadow.grace_margin_secs = 99;
        config.prompt_cache_shadow.warm_set_cap = 7;
        config.prompt_cache_shadow.refresh_debounce_secs = 123;
        config.prompt_cache_shadow.sweeper_interval_secs = 456;

        let dynamic_view = build_view_with_config(&stores, &runtime, dir.path(), config).await;

        let cache = dynamic_view
            .prompt_cache_observation_cache_opt()
            .expect("prompt cache enabled");
        assert_eq!(cache.grace_margin_secs(), 99);
    }
}
