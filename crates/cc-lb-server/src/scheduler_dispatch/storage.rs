use std::sync::Arc;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshUpstreams;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeStore;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneRunner;
use cc_lb_storage_api::{
    PromptCacheObservationStore, Storage, StorageError, StorageResult, UpstreamRecord,
    UpstreamStore,
};
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct StorageHandle {
    storage: Arc<dyn Storage>,
}

impl StorageHandle {
    pub(super) fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

impl OAuthRefreshUpstreams for StorageHandle {
    async fn get_by_id(&self, id: Uuid) -> SchedulerResult<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self.storage.as_ref(), id)
            .await
            .map_err(storage_scheduler_error)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> SchedulerResult<UpstreamRecord> {
        UpstreamStore::complete_refresh(self.storage.as_ref(), id, holder, tokens)
            .await
            .map_err(storage_scheduler_error)
    }

    async fn read_oauth_token_generation(&self, id: Uuid) -> SchedulerResult<Option<u64>> {
        UpstreamStore::read_oauth_token_generation(self.storage.as_ref(), id)
            .await
            .map_err(storage_scheduler_error)
    }
}

impl PromptCacheObservationPurgeStore for StorageHandle {
    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        PromptCacheObservationStore::purge_expired_before(self.storage.as_ref(), ts_unix_secs).await
    }
}

impl UsagePruneRunner for StorageHandle {
    async fn prune_once_for_retention(
        &self,
        retention_days: u64,
        clock: cc_lb_engine::ClockHandle,
    ) -> cc_lb_engine::usage_pruner::PruneResult {
        cc_lb_engine::usage_pruner::UsagePruner::new(self.storage.clone(), retention_days, clock)
            .prune_once()
            .await
    }
}

pub(super) fn storage_scheduler_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}
