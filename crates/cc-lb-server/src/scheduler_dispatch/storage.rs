use std::sync::Arc;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshUpstreams;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeStore;
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeStore;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneRunner;
use cc_lb_storage_api::{
    ApiKeyUsageBucketStore, ApiKeyUsageCompactionRun, PromptCacheObservationStore, Storage,
    StorageError, StorageResult, ThreadStickySubagentStore, UpstreamAffinityStore, UpstreamRecord,
    UpstreamStore,
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

impl UpstreamAffinityPurgeStore for StorageHandle {
    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        UpstreamAffinityStore::purge_expired_upstream_affinities(
            self.storage.as_ref(),
            now_unix_secs,
            ttl_secs,
            batch_size,
        )
        .await
    }

    async fn purge_expired_thread_sticky_subagents(
        &self,
        now_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        ThreadStickySubagentStore::purge_expired_thread_sticky_subagents(
            self.storage.as_ref(),
            now_unix_secs,
            batch_size,
        )
        .await
    }
}

impl UsagePruneRunner for StorageHandle {
    async fn prune_once_for_retention(
        &self,
        retention_days: u64,
        clock: cc_lb_clock::ClockHandle,
    ) -> PruneResult {
        let now_unix_secs = cc_lb_clock::unix_secs(clock.now());
        if let Err(error) = cc_lb_control::prune_expired_split_bridge_results(
            self.storage.as_ref(),
            now_unix_secs,
            256,
        )
        .await
        {
            tracing::warn!(%error, "split bridge expired result prune failed");
        }
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

    async fn prune_expired_api_key_usage_flush_markers(
        &self,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        ApiKeyUsageBucketStore::prune_expired_api_key_usage_flush_markers(
            self.storage.as_ref(),
            retain_for_secs,
            batch_size,
        )
        .await
    }
}

pub(super) fn storage_scheduler_error(error: StorageError) -> SchedulerError {
    match error {
        StorageError::Conflict { message } => SchedulerError::Conflict(message),
        other => SchedulerError::Job(other.to_string()),
    }
}
