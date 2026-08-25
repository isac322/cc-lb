use std::sync::Arc;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshUpstreams;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeStore;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneRunner;
use cc_lb_storage_api::{
    ApiKeyUsageBucketStore, ApiKeyUsageCompactionRun, PromptCacheObservationStore, Storage,
    StorageError, StorageResult, UpstreamRecord, UpstreamStore,
    usage_pruner::{PruneResult, UsagePruner},
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

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        expected_generation: u64,
        ttl_secs: u64,
    ) -> SchedulerResult<bool> {
        UpstreamStore::claim_refresh_lease(
            self.storage.as_ref(),
            id,
            holder,
            expected_generation,
            ttl_secs,
        )
        .await
        .map_err(storage_scheduler_error)
    }

    async fn read_oauth_refresh_terminal_failure(
        &self,
        id: Uuid,
    ) -> SchedulerResult<Option<cc_lb_storage_api::OAuthRefreshTerminalFailure>> {
        UpstreamStore::read_oauth_refresh_terminal_failure(self.storage.as_ref(), id)
            .await
            .map_err(storage_scheduler_error)
    }

    async fn fail_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        terminal_error: Option<String>,
    ) -> SchedulerResult<bool> {
        UpstreamStore::fail_refresh(self.storage.as_ref(), id, holder, terminal_error)
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
        clock: cc_lb_clock::ClockHandle,
    ) -> PruneResult {
        UsagePruner::new(self.storage.clone(), retention_days, clock)
            .prune_once()
            .await
    }

    async fn compact_api_key_usage_buckets(
        &self,
        writer_inactive_after_secs: u64,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<ApiKeyUsageCompactionRun> {
        ApiKeyUsageBucketStore::compact_api_key_usage_buckets(
            self.storage.as_ref(),
            writer_inactive_after_secs,
            retain_for_secs,
            batch_size,
        )
        .await
    }
}

pub(super) fn storage_scheduler_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}
