use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_aead::AeadService;
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_core::DynamicViewHolder;
use cc_lb_observability::{ReconcileOutcome, record_reconcile};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_api::{PluginSlot, StorageResult};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::{Stores, build_dynamic_view};
use crate::revision_hash::compute_revision_hash;

pub struct Reconciler {
    pub stores: Arc<Stores>,
    pub holder: Arc<DynamicViewHolder>,
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub runtime: Arc<ExtismRuntime>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
    pub cancel: CancellationToken,
    pub data_dir: PathBuf,
}

impl Reconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stores: Arc<Stores>,
        holder: Arc<DynamicViewHolder>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        runtime: Arc<ExtismRuntime>,
        aead: Arc<AeadService>,
        lazy_refresher: Option<Arc<dyn cc_lb_signer_anthropic_oauth::LazyRefreshHandle>>,
        cancel: CancellationToken,
        data_dir: PathBuf,
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
            record_reconcile(ReconcileOutcome::Unchanged);
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
        )
        .await
        {
            Ok(view) => {
                self.holder.store(view);
                record_reconcile(ReconcileOutcome::Changed);
            }
            Err(error) => {
                tracing::warn!(error = %error, "dynamic reconciliation rebuild failed");
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
            principals.push((principal.id, principal.revision));
        }
    }

    Ok(compute_revision_hash(&upstreams, &principals, &chains))
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
