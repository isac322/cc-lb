use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::api_keys::principal_view::{
    ObservabilityHooksCache, PrincipalRoutingArtifacts, PrincipalView, RouterPluginCache,
};
use cc_lb_core::{
    ApplyStatus, DynamicView, DynamicViewBuilder, ErrorNormalizer, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot, make_default_dispatcher,
};
use cc_lb_dialect_anthropic::{AnthropicDirectDialect, CustomAnthropicSpecDialect};
use cc_lb_plugin_api::{
    PluginManifest, Principal, RateLimitObservation, RequestContext, RouteDecision, RouteError,
    RouterPlugin, Signer, SignerError, SignerFactory, Upstream, UpstreamCandidate, UpstreamDialect,
};
use cc_lb_runtime_extism::{ExtismRuntime, StagedSlot};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditStore, PluginRegistryStore, PluginSlot, PrincipalRecord, PrincipalStore, RateLimitKind,
    StorageError, StorageResult, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    UpstreamRecord, UpstreamStore,
};
use parking_lot::RwLock;
use thiserror::Error;
use uuid::Uuid;

use crate::reconcile::collect_revision_hash;

pub struct Stores {
    pub upstreams: Arc<dyn UpstreamStore>,
    pub principals: Arc<dyn PrincipalStore>,
    pub plugin_registry: Arc<dyn PluginRegistryStore>,
    pub upstream_rate_limits: Arc<dyn UpstreamRateLimitStateStore>,
    pub audit: Option<Arc<dyn AuditStore>>,
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

pub async fn build_dynamic_view(
    stores: &Stores,
    oauth_anthropic: &AnthropicOAuthConfig,
    aead: Arc<AeadService>,
    lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    current_generation: u64,
    runtime: &ExtismRuntime,
    data_dir: &Path,
) -> Result<Arc<DynamicView>, RebindError> {
    let upstreams = list_upstreams(stores).await?;
    let all_upstream_ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
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
    let (routes, statuses) = apply_upstreams(stores, &upstreams, oauth_anthropic, now).await?;
    let revision_hash = collect_revision_hash(stores).await?;
    let global_router = Arc::new(DbRouter::new(routes));
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

    Ok(DynamicViewBuilder::new(current_generation)
        .signer_factory(signer_factory)
        .global_router(global_router)
        .dispatcher(dispatcher)
        .global_observability_hooks(Vec::new())
        .error_normalizer(Arc::new(ErrorNormalizer::new()))
        .principal_view(principal_view)
        .upstream_status_snapshot(snapshot)
        .upstream_rate_limit_cache(upstream_rate_limit_cache)
        .upstream_records(upstreams.clone())
        .build())
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

        let router = if let Some(entry) = router_entries.into_iter().min_by_key(|entry| entry.order)
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
                config: entry.config,
                metadata: Default::default(),
            };
            let (handle, slot) =
                runtime.instantiate_router_for(&principal.name, &manifest.name, &manifest)?;
            staged.push(slot);
            RouterPluginCache::Explicit(handle)
        } else {
            RouterPluginCache::Inherit
        };

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
            let wasm_path = materialize_wasm(stores, data_dir, registry_entry.sha256).await?;
            let manifest = PluginManifest {
                name: registry_entry.name,
                artifact: wasm_path.to_string_lossy().into_owned(),
                config: entry.config,
                metadata: Default::default(),
            };
            let (handle, slot) = runtime.instantiate_observability_for(
                &principal.name,
                &manifest.name,
                &manifest,
            )?;
            staged.push(slot);
            hooks.push(handle);
        }
        let hooks = if hooks.is_empty() {
            ObservabilityHooksCache::Inherit
        } else {
            ObservabilityHooksCache::Explicit(hooks)
        };
        chains.insert(principal.name.clone(), (router, hooks));
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

async fn materialize_wasm(
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

async fn apply_upstreams(
    stores: &Stores,
    upstreams: &[UpstreamRecord],
    _oauth_anthropic: &AnthropicOAuthConfig,
    now: u64,
) -> StorageResult<(Vec<DbRoute>, HashMap<String, UpstreamStatusEntry>)> {
    let mut routes = Vec::new();
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

        match validate_upstream(upstream) {
            Ok(route) => {
                stores
                    .upstreams
                    .set_last_apply_error(upstream.id, None)
                    .await?;
                routes.push(route);
                statuses.insert(
                    upstream.name.clone(),
                    UpstreamStatusEntry {
                        status: ApplyStatus::Active,
                        last_apply_error: None,
                        last_apply_at_unix_secs: now,
                    },
                );
            }
            Err(message) => {
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
            }
        }
    }
    Ok((routes, statuses))
}

fn validate_upstream(upstream: &UpstreamRecord) -> Result<DbRoute, String> {
    let upstream_target = match upstream.kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth => Upstream::AnthropicDirect,
        UpstreamKind::Custom => Upstream::CustomAnthropicSpec {
            base_url: upstream
                .base_url
                .clone()
                .ok_or_else(|| "custom upstream missing base_url".to_owned())?,
        },
    };
    let dialect: Arc<dyn UpstreamDialect> = match upstream.kind {
        UpstreamKind::AnthropicApiKey | UpstreamKind::AnthropicOauth => Arc::new(
            AnthropicDirectDialect::with_base_url(upstream.base_url.clone()),
        ),
        UpstreamKind::Custom => Arc::new(CustomAnthropicSpecDialect),
    };
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
    Ok(DbRoute {
        name: upstream.name.clone(),
        upstream: upstream_target,
        dialect,
    })
}

#[derive(Clone)]
struct DbRoute {
    name: String,
    upstream: Upstream,
    dialect: Arc<dyn UpstreamDialect>,
}

struct DbRouter {
    routes: Vec<DbRoute>,
}

impl DbRouter {
    fn new(routes: Vec<DbRoute>) -> Self {
        Self { routes }
    }
}

impl RouterPlugin for DbRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        let route = self.routes.first().ok_or_else(|| RouteError::NoRoute {
            reason: "no active upstreams in dynamic view".to_owned(),
        })?;
        tracing::debug!(upstream = route.name.as_str(), "dynamic route selected");
        Ok(RouteDecision {
            upstream_id: None,
            upstream: route.upstream.clone(),
            dialect: route.dialect.clone(),
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
            UpstreamKind::AnthropicApiKey | UpstreamKind::Custom => {
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
            Upstream::AnthropicDirect
        ) | (UpstreamKind::Custom, Upstream::CustomAnthropicSpec { .. })
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
