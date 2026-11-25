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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub api_key_usage_continuation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsagePruneJobResult {
    Done {
        result: PruneResult,
        api_key_usage_compaction: ApiKeyUsageCompactionRun,
        has_more_api_key_usage_work: bool,
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

    fn prune_expired_api_key_usage_flush_markers(
        &self,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> impl Future<Output = StorageResult<u64>> + Send + '_ {
        let _ = (retain_for_secs, batch_size);
        std::future::ready(Ok(0))
    }
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

    async fn prune_expired_api_key_usage_flush_markers(
        &self,
        retain_for_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        ApiKeyUsageBucketStore::prune_expired_api_key_usage_flush_markers(
            self.as_ref(),
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
    let result = if job.api_key_usage_continuation {
        tracing::debug!("running continuation pass for API-key usage compaction/cleanup");
        PruneResult::default()
    } else if usage_retention_days == 0 {
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
    ::metrics::counter!(crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL, "table" => "upstream_api_usage")
        .increment(result.upstream_api_usage_removed);
    let api_key_usage_compaction = runner
        .compact_api_key_usage_buckets(
            api_key_usage_writer_inactive_after_secs,
            api_key_usage_retain_for_secs,
            api_key_usage_compaction_batch_size,
        )
        .await?;
    let batch_cap = u64::try_from(api_key_usage_compaction_batch_size).unwrap_or(u64::MAX);
    let more_markers = if api_key_usage_compaction_batch_size == 0 {
        false
    } else {
        runner
            .prune_expired_api_key_usage_flush_markers(api_key_usage_retain_for_secs, 1)
            .await?
            > 0
    };
    let has_more_api_key_usage_work = more_markers
        || (api_key_usage_compaction_batch_size > 0
            && (api_key_usage_compaction.folded_rows >= batch_cap
                || api_key_usage_compaction.pruned_rows >= batch_cap));
    ::metrics::counter!(crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL, "table" => "api_key_usage_buckets")
        .increment(api_key_usage_compaction.pruned_rows);
    ::metrics::counter!(crate::scheduler_metrics::API_KEY_USAGE_BUCKETS_FOLDED_TOTAL)
        .increment(api_key_usage_compaction.folded_rows);
    tracing::info!(
        request_events_removed = result.request_events_removed,
        audit_log_removed = result.audit_log_removed,
        upstream_api_usage_removed = result.upstream_api_usage_removed,
        api_key_usage_buckets_folded = api_key_usage_compaction.folded_rows,
        api_key_usage_buckets_pruned = api_key_usage_compaction.pruned_rows,
        has_more_api_key_usage_work,
        continuation = job.api_key_usage_continuation,
        "usage prune job complete",
    );
    Ok(UsagePruneJobResult::Done {
        result,
        api_key_usage_compaction,
        has_more_api_key_usage_work,
    })
}

#[cfg(test)]
mod tests;
