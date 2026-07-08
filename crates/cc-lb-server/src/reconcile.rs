use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_config::{AnthropicOAuthConfig, Config};
use cc_lb_engine::DynamicViewHolder;
use cc_lb_engine::clock::ClockHandle;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_storage_api::{PluginSlot, StorageResult};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::{Stores, build_dynamic_view};
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
    pub config: Arc<Config>,
    pub clock: ClockHandle,
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
        config: Arc<Config>,
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
            config,
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
            &self.oauth_cfg,
            self.aead.clone(),
            self.lazy_refresher.clone(),
            generation,
            &self.runtime,
            &self.data_dir,
            self.subscription_quota_cache.clone(),
            self.prompt_cache_observation_cache.clone(),
            self.prompt_cache_observation_sink.clone(),
            self.subscription_quota_routing_max_staleness_secs,
            &self.config,
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
            chains.extend(chain_revisions(stores, principal.id, PluginSlot::Router).await?);
            chains.extend(
                chain_revisions(stores, principal.id, PluginSlot::ObservabilityHook).await?,
            );
            chains.extend(chain_revisions(stores, principal.id, PluginSlot::Shape).await?);
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
    slot: PluginSlot,
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
