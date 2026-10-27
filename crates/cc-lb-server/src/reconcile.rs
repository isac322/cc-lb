use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_control::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{PluginSlotKind, StorageResult, UpstreamRecord, upstream::UpstreamKind};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::dynamic_view_builder::{Stores, build_dynamic_view, list_upstreams};
use crate::revision_hash::compute_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use cc_lb_control::PromptCacheObservationSinkLike;

const RECONNECT_REQUIRED_SEED: u64 = 0x7265_636f_6e6e_6563;

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
    pub clock: ClockHandle,
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
    ) -> Self {
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
            clock,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(60_000 + jitter_millis()));
        interval.tick().await;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
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
        self.hydrate_subscription_quota_cache().await;

        // Lock-free fast path: an unchanged signature needs no rebuild.
        let hash = collect_revision_hash(&self.stores).await?;
        if hash == self.holder.load().upstream_status_snapshot.revision_hash {
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
        let hash = collect_revision_hash(&self.stores).await?;
        if hash == current.upstream_status_snapshot.revision_hash {
            metrics::counter!("cclb_reconcile_total", "outcome" => "unchanged").increment(1);
            return Ok(());
        }
        let generation = current.generation;
        drop(current);
        match build_dynamic_view(
            &self.stores,
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
        )
        .await
        {
            Ok(view) => {
                // Defensive CAS: under the rebuild lock every live
                // publisher builds from the resident generation, so a
                // rejection only follows saturation or an out-of-protocol
                // publication.
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
}

/// Hashes the current storage state; `reconcile_once` compares this with the
/// resident view's hash, and `build_dynamic_view` samples it before reading
/// any contents. Reconnect-required status writes bump no revision and emit no
/// NOTIFY, so the upstream material folds that status in: a write that lands
/// after a build's early sample leaves the resident hash stale and the next
/// reconcile rebuilds. The material also folds the monotonic OAuth token
/// generation, so a reconnect that clears the marker again cannot return the
/// hash to a sample taken before the captured failure.
pub(crate) async fn collect_revision_hash(stores: &Stores) -> StorageResult<u64> {
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
        for principal in page {
            chains.extend(chain_revisions(stores, principal.id, PluginSlotKind::Router).await?);
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
    let reconnect_required =
        matches!(
            record.kind,
            UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
        ) && cc_lb_oauth_protocol::refresh_requires_reconnect(record.last_apply_error.as_deref());
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
