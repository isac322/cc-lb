use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_control::{DynamicViewHolder, PromptCacheObservationSinkLike, UpstreamRateLimitCache};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{PluginSlotKind, StorageResult, UpstreamRecord, upstream::UpstreamKind};
use parking_lot::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::dynamic_view_builder::{
    DynamicViewPluginScope, Stores, build_dynamic_view_with_scope_and_rate_limit_cache,
    list_upstreams,
};
use crate::revision_hash::compute_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

const RECONNECT_REQUIRED_SEED: u64 = 0x7265_636f_6e6e_6563;
pub(crate) const TRANSIENT_PLUGIN_RETRY_MAX_ATTEMPTS: u32 = 5;
const TRANSIENT_PLUGIN_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const TRANSIENT_PLUGIN_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
struct TransientPluginRetryState {
    revision_hash: Option<u64>,
    attempts: u32,
}

pub struct Reconciler {
    pub stores: Arc<Stores>,
    pub holder: Arc<DynamicViewHolder>,
    pub runtime: Arc<WasmtimeRuntime>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub cancel: CancellationToken,
    pub data_dir: PathBuf,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub prompt_cache_grace_margin_secs: u64,
    pub prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
    pub subscription_quota_routing_max_staleness_secs: u64,
    pub upstream_rate_limit_cache: Arc<RwLock<UpstreamRateLimitCache>>,
    pub clock: ClockHandle,
    pub gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
    pub antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
    pub plugin_scope: DynamicViewPluginScope,
    transient_plugin_retry: Mutex<TransientPluginRetryState>,
}

impl Reconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stores: Arc<Stores>,
        holder: Arc<DynamicViewHolder>,
        runtime: Arc<WasmtimeRuntime>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        cancel: CancellationToken,
        data_dir: PathBuf,
        subscription_quota_cache: Arc<SubscriptionQuotaCache>,
        prompt_cache_grace_margin_secs: u64,
        prompt_cache_observation_sink: Arc<dyn PromptCacheObservationSinkLike>,
        subscription_quota_routing_max_staleness_secs: u64,
        clock: ClockHandle,
        gemini_config: Option<cc_lb_config::GeminiOAuthConfig>,
        antigravity_config: Option<cc_lb_config::AntigravityOAuthConfig>,
    ) -> Self {
        let upstream_rate_limit_cache = Arc::clone(&holder.load().upstream_rate_limit_cache);
        Self {
            stores,
            holder,
            runtime,
            aead,
            lazy_refresher,
            cancel,
            data_dir,
            subscription_quota_cache,
            prompt_cache_grace_margin_secs,
            prompt_cache_observation_sink,
            subscription_quota_routing_max_staleness_secs,
            upstream_rate_limit_cache,
            clock,
            gemini_config,
            antigravity_config,
            plugin_scope: DynamicViewPluginScope::All,
            transient_plugin_retry: Mutex::new(TransientPluginRetryState::default()),
        }
    }

    pub fn with_plugin_scope(mut self, plugin_scope: DynamicViewPluginScope) -> Self {
        self.plugin_scope = plugin_scope;
        self
    }

    fn clear_transient_plugin_retry(&self) {
        let mut state = self.transient_plugin_retry.lock();
        state.revision_hash = None;
        state.attempts = 0;
    }

    fn transient_retry_attempts_for(&self, hash: u64) -> Option<u32> {
        let mut state = self.transient_plugin_retry.lock();
        if state.revision_hash != Some(hash) {
            state.revision_hash = Some(hash);
            state.attempts = 0;
        }
        (state.attempts < TRANSIENT_PLUGIN_RETRY_MAX_ATTEMPTS).then_some(state.attempts)
    }

    fn record_transient_retry_attempt(&self, hash: u64) {
        let mut state = self.transient_plugin_retry.lock();
        if state.revision_hash != Some(hash) {
            state.revision_hash = Some(hash);
            state.attempts = 1;
        } else {
            state.attempts = state.attempts.saturating_add(1);
        }
    }

    fn seed_transient_retry_for(&self, hash: u64) {
        let mut state = self.transient_plugin_retry.lock();
        state.revision_hash = Some(hash);
        state.attempts = 0;
    }

    fn next_transient_retry_delay(&self) -> Option<Duration> {
        let current = self.holder.load();
        if !current.has_retryable_plugin_failure() {
            self.clear_transient_plugin_retry();
            return None;
        }
        let hash = current.upstream_status_snapshot.revision_hash;
        let attempts = self.transient_retry_attempts_for(hash)?;
        let shift = attempts.min(5);
        Some(
            TRANSIENT_PLUGIN_RETRY_INITIAL_DELAY
                .saturating_mul(1u32 << shift)
                .min(TRANSIENT_PLUGIN_RETRY_MAX_DELAY),
        )
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(60_000 + jitter_millis()));
        interval.tick().await;

        loop {
            if let Some(retry_delay) = self.next_transient_retry_delay() {
                tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    _ = tokio::time::sleep(retry_delay) => {}
                    _ = interval.tick() => {}
                }

                let result = tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    result = self.reconcile_fast_retry_once() => result,
                };
                if let Err(error) = result {
                    tracing::warn!(error = %error, "dynamic reconciliation transient retry failed");
                }
                continue;
            }

            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = self.holder.wait_for_retryable_plugin_failure() => continue,
                _ = interval.tick() => {}
            }

            let jitter = tokio::time::sleep(Duration::from_millis(jitter_millis()));
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

    pub async fn reconcile_once(&self) -> StorageResult<()> {
        self.reconcile_once_inner(false).await
    }

    pub(crate) async fn reconcile_fast_retry_once(&self) -> StorageResult<()> {
        self.reconcile_once_inner(true).await
    }

    async fn reconcile_once_inner(&self, is_fast_retry: bool) -> StorageResult<()> {
        self.hydrate_subscription_quota_cache().await;

        // Lock-free fast path: an unchanged signature needs no rebuild unless
        // a transient plugin admission failure requires recovery. Ordinary periodic
        // reconciliation continues retrying even after the fast retry budget
        // expires so temporary saturation eventually heals once capacity returns.
        let hash = collect_revision_hash_for_scope(&self.stores, self.plugin_scope).await?;
        let resident = self.holder.load();
        let hash_changed = hash != resident.upstream_status_snapshot.revision_hash;
        if !resident.has_retryable_plugin_failure() {
            self.clear_transient_plugin_retry();
        }
        let retry_transient = !hash_changed
            && resident.has_retryable_plugin_failure()
            && (!is_fast_retry || self.transient_retry_attempts_for(hash).is_some());
        if !hash_changed && !retry_transient {
            self.hydrate_upstream_rate_limit_cache().await;
            metrics::counter!("cclb_reconcile_total", "outcome" => "unchanged").increment(1);
            return Ok(());
        }

        // Rebuild protocol: hold the holder's rebuild lock from the
        // resident-generation read through installation. Another publisher
        // may have installed a fresh view while this pass waited, so reread
        // the resident view under the guard instead of reusing the earlier
        // sample.
        let _rebuild_guard = self.holder.lock_rebuild().await;
        let current = self.holder.load();
        let hash = collect_revision_hash_for_scope(&self.stores, self.plugin_scope).await?;
        let hash_changed = hash != current.upstream_status_snapshot.revision_hash;
        if !current.has_retryable_plugin_failure() {
            self.clear_transient_plugin_retry();
        }
        let retry_transient = !hash_changed
            && current.has_retryable_plugin_failure()
            && (!is_fast_retry || self.transient_retry_attempts_for(hash).is_some());
        if !hash_changed && !retry_transient {
            drop(current);
            drop(_rebuild_guard);
            self.hydrate_upstream_rate_limit_cache().await;
            metrics::counter!("cclb_reconcile_total", "outcome" => "unchanged").increment(1);
            return Ok(());
        }
        if hash_changed {
            self.clear_transient_plugin_retry();
        } else if is_fast_retry && retry_transient {
            self.record_transient_retry_attempt(hash);
        }
        let generation = current.generation;
        drop(current);
        match build_dynamic_view_with_scope_and_rate_limit_cache(
            &self.stores,
            self.gemini_config.as_ref(),
            self.antigravity_config.as_ref(),
            self.aead.clone(),
            self.lazy_refresher.clone(),
            generation,
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
                let retryable = view.has_retryable_plugin_failure();
                let rebuilt_hash = view.upstream_status_snapshot.revision_hash;
                // Defensive CAS: under the rebuild lock every live
                // publisher builds from the resident generation, so a
                // rejection only follows saturation or an out-of-protocol
                // publication.
                if self.holder.try_store_if_newer(view) {
                    if !retryable {
                        self.clear_transient_plugin_retry();
                    } else if hash_changed {
                        self.seed_transient_retry_for(rebuilt_hash);
                    }
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

    /// Refresh the subscription-quota cache from committed storage on every
    /// tick. Quota observations are outside the revision hash and their
    /// change notifications can be lost, so this periodic read is the
    /// backstop. A stored observation replaces the cached one when strictly
    /// newer, or on an equal timestamp when no other path replaced that slot
    /// during the read, so a read that raced a local or notified observation
    /// cannot restore older content. It never changes generation or hash, and
    /// a failure only logs so the dynamic-view reconcile still runs.
    async fn hydrate_subscription_quota_cache(&self) {
        let upstream_ids = self
            .holder
            .load()
            .upstreams_snapshot()
            .iter()
            .map(|upstream| upstream.id)
            .collect::<Vec<Uuid>>();
        if upstream_ids.is_empty() {
            return;
        }
        if let Err(error) = self
            .subscription_quota_cache
            .hydrate_from_store_unless_raced(&self.stores, &upstream_ids)
            .await
        {
            tracing::warn!(%error, "periodic subscription quota cache hydrate failed");
        }
    }

    /// Periodic backstop for `upstream_rate_limit_cache` when NOTIFY delivery
    /// lags or drops on an idle proxy replica. Hydrates monotonically per slot.
    async fn hydrate_upstream_rate_limit_cache(&self) {
        let upstream_ids = self
            .holder
            .load()
            .upstreams_snapshot()
            .iter()
            .map(|upstream| upstream.id)
            .collect::<Vec<Uuid>>();
        if upstream_ids.is_empty() {
            return;
        }
        match self
            .stores
            .upstream_rate_limits
            .list_for_upstream_ids(&upstream_ids)
            .await
        {
            Ok(records) => {
                let mut cache = self.upstream_rate_limit_cache.write();
                cache.merge_records(records);
            }
            Err(error) => {
                tracing::warn!(%error, "periodic upstream rate limit cache hydrate failed");
            }
        }
    }
}

/// Hashes the current storage state; `reconcile_once` compares this with the
/// resident view's hash, and `build_dynamic_view` samples it before reading
/// any contents. Reconnect-required status writes bump no revision and emit no
/// NOTIFY, so the upstream material folds that status in: a write that lands
/// after a build's early sample leaves the resident hash stale and the next
/// reconcile rebuilds. The material also folds the monotonic OAuth token
/// generation, so a reconnect that clears the marker again cannot return the
/// hash to a sample taken before the captured failure.
#[cfg(test)]
pub(crate) async fn collect_revision_hash(stores: &Stores) -> StorageResult<u64> {
    collect_revision_hash_for_scope(stores, DynamicViewPluginScope::All).await
}

pub(crate) async fn collect_revision_hash_for_scope(
    stores: &Stores,
    plugin_scope: DynamicViewPluginScope,
) -> StorageResult<u64> {
    let upstreams = list_upstreams(stores)
        .await?
        .iter()
        .map(upstream_revision_material)
        .collect::<Vec<_>>();

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
        if plugin_scope == DynamicViewPluginScope::All {
            let page_ids = page
                .iter()
                .map(|principal| principal.id)
                .collect::<Vec<_>>();
            let page_chains = stores
                .plugin_registry
                .list_chains_for_principals(
                    &page_ids,
                    &[PluginSlotKind::Router, PluginSlotKind::Shape],
                )
                .await?;
            chains.extend(
                page_chains
                    .into_iter()
                    .map(|entry| (entry.id, entry.revision)),
            );
        }
        principals.extend(
            page.into_iter()
                .map(|principal| (principal.id, principal.revision)),
        );
    }

    let plan_metadata_hash = collect_plan_metadata_hash(stores).await?;

    Ok(compute_revision_hash(
        &upstreams,
        &principals,
        &chains,
        &registry_entries,
        plan_metadata_hash,
    ))
}

async fn collect_plan_metadata_hash(stores: &Stores) -> StorageResult<u64> {
    let (mut ratios, mut overrides, mut subscription_meta, mut organization_meta) = tokio::try_join!(
        stores.plan_tiers.list_current_plan_tier_ratios(),
        stores.plan_tiers.list_current_metadata_tier_overrides(),
        stores
            .upstream_subscription_metadata
            .list_upstream_subscription_metadata(),
        stores.organization_metadata.list_organization_metadata(),
    )?;

    ratios.sort_by(|a, b| a.tier_key.cmp(&b.tier_key));
    overrides.sort_by(|a, b| {
        (
            a.organization_type.as_deref(),
            a.rate_limit_tier.as_deref(),
            a.seat_tier.as_deref(),
            a.tier_key.as_str(),
        )
            .cmp(&(
                b.organization_type.as_deref(),
                b.rate_limit_tier.as_deref(),
                b.seat_tier.as_deref(),
                b.tier_key.as_str(),
            ))
    });
    subscription_meta.sort_by_key(|record| record.upstream_id);
    organization_meta.sort_by(|a, b| a.organization_uuid.cmp(&b.organization_uuid));

    let mut bytes = Vec::new();
    let push_str = |buf: &mut Vec<u8>, s: &str| {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    };
    let push_opt_str = |buf: &mut Vec<u8>, s: Option<&str>| match s {
        Some(v) => {
            buf.push(1);
            push_str(buf, v);
        }
        None => buf.push(0),
    };

    bytes.extend_from_slice(&(ratios.len() as u32).to_le_bytes());
    for ratio in ratios {
        push_str(&mut bytes, &ratio.tier_key);
        bytes.extend_from_slice(&ratio.pro_relative_ratio.to_bits().to_le_bytes());
        bytes.extend_from_slice(&ratio.effective_from_unix_millis.to_le_bytes());
        bytes.extend_from_slice(&ratio.created_at_unix_millis.to_le_bytes());
    }

    bytes.extend_from_slice(&(overrides.len() as u32).to_le_bytes());
    for entry in overrides {
        push_opt_str(&mut bytes, entry.organization_type.as_deref());
        push_opt_str(&mut bytes, entry.rate_limit_tier.as_deref());
        push_opt_str(&mut bytes, entry.seat_tier.as_deref());
        push_str(&mut bytes, &entry.tier_key);
        bytes.extend_from_slice(&entry.effective_from_unix_millis.to_le_bytes());
        bytes.extend_from_slice(&entry.created_at_unix_millis.to_le_bytes());
    }

    bytes.extend_from_slice(&(subscription_meta.len() as u32).to_le_bytes());
    for entry in subscription_meta {
        bytes.extend_from_slice(&entry.upstream_id.as_u128().to_le_bytes());
        push_opt_str(&mut bytes, entry.organization_uuid.as_deref());
        push_opt_str(&mut bytes, entry.organization_role.as_deref());
        push_opt_str(&mut bytes, entry.workspace_role.as_deref());
        bytes.extend_from_slice(&entry.observed_at_unix_millis.to_le_bytes());
    }

    bytes.extend_from_slice(&(organization_meta.len() as u32).to_le_bytes());
    for entry in organization_meta {
        push_str(&mut bytes, &entry.organization_uuid);
        push_opt_str(&mut bytes, entry.organization_type.as_deref());
        push_opt_str(&mut bytes, entry.rate_limit_tier.as_deref());
        push_opt_str(&mut bytes, entry.seat_tier.as_deref());
        bytes.extend_from_slice(&entry.observed_at_unix_millis.to_le_bytes());
    }

    Ok(xxh3_64(&bytes))
}

fn upstream_revision_material(record: &UpstreamRecord) -> (Uuid, u64) {
    // Rotating the api-key credential does not bump `record.revision`; fold
    // the ciphertext (fresh nonce per encryption) into the hash so a
    // secret-only rotation still converges on peers that missed the NOTIFY.
    let material = xxh3_64_with_seed(
        record.api_key_ciphertext.as_deref().unwrap_or_default(),
        record.revision,
    );
    // OAuth reconnects and refreshes bump no spec revision but always advance
    // this monotonic generation. Folding it in keeps a reconnect that clears
    // the reconnect-required marker distinguishable from the pre-failure
    // state, so a view built from contents captured between those writes
    // never matches the post-reconnect hash.
    let material = xxh3_64_with_seed(&record.oauth_token_generation.to_le_bytes(), material);
    // A runtime refresh failure that requires an operator reconnect is a
    // status write without NOTIFY; fold it in so the periodic reconcile
    // rebuilds the view and routing stops picking the dead OAuth account.
    let reconnect_required = match record.kind {
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth => {
            cc_lb_oauth_protocol::refresh_requires_reconnect(record.last_apply_error.as_deref())
        }
        UpstreamKind::VertexCredential => {
            cc_lb_domain::vertex::vertex_credential_requires_replacement(
                record.last_apply_error.as_deref(),
            )
        }
        _ => false,
    };
    let material = if reconnect_required {
        xxh3_64_with_seed(&material.to_le_bytes(), RECONNECT_REQUIRED_SEED)
    } else {
        material
    };
    (record.id, material)
}

fn jitter_millis() -> u64 {
    rand::random_range(0..=10_000)
}
