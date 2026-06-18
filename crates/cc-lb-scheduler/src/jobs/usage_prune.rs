use std::future::Future;
use std::sync::Arc;

use cc_lb_core::usage_pruner::{IntoUsagePrunerStorage, PruneResult, UsagePruner};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsagePruneJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsagePruneJobResult {
    Done { result: PruneResult },
    Skip,
}

pub trait UsagePruneRunner {
    fn prune_once_for_retention(
        &self,
        retention_days: u64,
    ) -> impl Future<Output = PruneResult> + Send + '_;
}

impl<Store> UsagePruneRunner for Arc<Store>
where
    Store: ?Sized,
    Arc<Store>: IntoUsagePrunerStorage + Clone + Send + Sync + 'static,
{
    fn prune_once_for_retention(
        &self,
        retention_days: u64,
    ) -> impl Future<Output = PruneResult> + Send + '_ {
        let storage = Arc::clone(self);
        async move { UsagePruner::new(storage, retention_days).prune_once().await }
    }
}

pub async fn handle_usage_prune_job<Runner>(
    job: UsagePruneJob,
    runner: &Runner,
    usage_retention_days: u64,
) -> UsagePruneJobResult
where
    Runner: UsagePruneRunner + ?Sized,
{
    if let Some(traceparent) = job.traceparent.as_deref() {
        tracing::debug!(traceparent, "handling usage prune job");
    }
    if usage_retention_days == 0 {
        tracing::info!("usage prune job skipped because retention is disabled");
        return UsagePruneJobResult::Skip;
    }

    let result = runner.prune_once_for_retention(usage_retention_days).await;
    metrics::counter!("cclb_scheduler_prune_rows_removed_total", "table" => "request_events")
        .increment(result.request_events_removed);
    metrics::counter!("cclb_scheduler_prune_rows_removed_total", "table" => "audit_log")
        .increment(result.audit_log_removed);
    tracing::info!(
        request_events_removed = result.request_events_removed,
        usage_rollups_removed = result.usage_rollups_removed,
        principal_limit_states_removed = result.principal_limit_states_removed,
        audit_log_removed = result.audit_log_removed,
        "usage prune job complete",
    );
    UsagePruneJobResult::Done { result }
}

#[cfg(test)]
mod tests;
