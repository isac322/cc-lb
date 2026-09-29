use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_clock::ClockHandle;
use cc_lb_control::DynamicViewHolder;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{PluginSlotKind, StorageResult};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::dynamic_view_builder::{Stores, build_dynamic_view};
use crate::revision_hash::compute_revision_hash;
use crate::subscription_quota_cache::SubscriptionQuotaCache;
use cc_lb_control::PromptCacheObservationSinkLike;

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
        let hash = collect_revision_hash(&self.stores).await?;
        let current = self.holder.load();
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
        upstreams.extend(page.into_iter().map(|record| {
            // Rotating the api-key credential does not bump
            // `record.revision`; fold the ciphertext (fresh nonce per
            // encryption) into the hash so a secret-only rotation still
            // converges on peers that missed the NOTIFY.
            let material = xxh3_64_with_seed(
                record.api_key_ciphertext.as_deref().unwrap_or_default(),
                record.revision,
            );
            (record.id, material)
        }));
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

fn jitter_millis() -> u64 {
    rand::random_range(0..=10_000)
}
