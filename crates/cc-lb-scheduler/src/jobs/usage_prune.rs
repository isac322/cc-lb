use std::future::Future;
use std::sync::Arc;

use cc_lb_clock::ClockHandle;
use cc_lb_storage_api::{
    ApiKeyUsageBucketStore, ApiKeyUsageCompactionRun, StorageResult,
    usage_pruner::{IntoUsagePrunerStorage, PruneResult, UsagePruner},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsagePruneJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsagePruneJobResult {
    Done {
        result: PruneResult,
        api_key_usage_compaction: ApiKeyUsageCompactionRun,
    },
}

pub trait UsagePruneRunner {
    fn prune_once_for_retention(
        &self,
        retention_days: u64,
        clock: ClockHandle,
    ) -> impl Future<Output = PruneResult> + Send + '_;

    fn compact_api_key_usage_buckets(
        &self,
        writer_inactive_after_secs: u64,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> impl Future<Output = StorageResult<ApiKeyUsageCompactionRun>> + Send + '_;
}

impl<Store> UsagePruneRunner for Arc<Store>
where
    Store: ApiKeyUsageBucketStore + ?Sized,
    Arc<Store>: IntoUsagePrunerStorage + Clone + Send + Sync + 'static,
{
    async fn prune_once_for_retention(
        &self,
        retention_days: u64,
        clock: ClockHandle,
    ) -> PruneResult {
        let storage = Arc::clone(self);
        UsagePruner::new(storage, retention_days, clock)
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
            self.as_ref(),
            writer_inactive_after_secs,
            retain_for_secs,
            batch_size,
        )
        .await
    }
}

pub async fn handle_usage_prune_job<Runner>(
    job: UsagePruneJob,
    runner: &Runner,
    usage_retention_days: u64,
    clock: ClockHandle,
    api_key_usage_writer_inactive_after_secs: u64,
    api_key_usage_retain_for_secs: u64,
    api_key_usage_compaction_batch_size: usize,
) -> StorageResult<UsagePruneJobResult>
where
    Runner: UsagePruneRunner + ?Sized,
{
    if let Some(traceparent) = job.traceparent.as_deref() {
        tracing::debug!(traceparent, "handling usage prune job");
    }
    let result = if usage_retention_days == 0 {
        tracing::info!("general usage pruning skipped because retention is disabled");
        PruneResult::default()
    } else {
        runner
            .prune_once_for_retention(usage_retention_days, clock)
            .await
    };
    ::metrics::counter!(crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL, "table" => "request_events")
        .increment(result.request_events_removed);
    ::metrics::counter!(crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL, "table" => "audit_log")
        .increment(result.audit_log_removed);
    let api_key_usage_compaction = runner
        .compact_api_key_usage_buckets(
            api_key_usage_writer_inactive_after_secs,
            api_key_usage_retain_for_secs,
            api_key_usage_compaction_batch_size,
        )
        .await?;
    ::metrics::counter!(crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL, "table" => "api_key_usage_buckets")
        .increment(api_key_usage_compaction.pruned_rows);
    ::metrics::counter!(crate::scheduler_metrics::API_KEY_USAGE_BUCKETS_FOLDED_TOTAL)
        .increment(api_key_usage_compaction.folded_rows);
    tracing::info!(
        request_events_removed = result.request_events_removed,
        usage_rollups_removed = result.usage_rollups_removed,
        principal_limit_states_removed = result.principal_limit_states_removed,
        audit_log_removed = result.audit_log_removed,
        api_key_usage_buckets_folded = api_key_usage_compaction.folded_rows,
        api_key_usage_buckets_pruned = api_key_usage_compaction.pruned_rows,
        "usage prune job complete",
    );
    Ok(UsagePruneJobResult::Done {
        result,
        api_key_usage_compaction,
    })
}

#[cfg(test)]
mod tests;
