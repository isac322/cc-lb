//! Usage rollup singleton job.

use std::time::Duration;

use cc_lb_storage_api::{UsageRollupRun, UsageRollupStore};
use serde::{Deserialize, Serialize};

pub const USAGE_ROLLUP_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollupJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageRollupJobResult {
    Done { run: UsageRollupRun },
    Retry { delay: Duration, error: String },
}

pub async fn handle_usage_rollup_job<S>(job: UsageRollupJob, storage: &S) -> UsageRollupJobResult
where
    S: UsageRollupStore + ?Sized,
{
    if let Some(traceparent) = job.traceparent.as_deref() {
        tracing::debug!(traceparent, "handling usage rollup job");
    }

    match storage.rollup_usage_once().await {
        Ok(run) => {
            metrics::counter!("cclb_scheduler_rollup_events_processed")
                .increment(run.processed_events);
            metrics::counter!("cclb_scheduler_rollup_rows_updated").increment(run.updated_rollups);
            tracing::info!(
                processed_events = run.processed_events,
                updated_rollups = run.updated_rollups,
                checkpoint = ?run.checkpoint,
                "usage rollup job complete",
            );
            UsageRollupJobResult::Done { run }
        }
        Err(error) => {
            tracing::warn!(%error, "usage rollup job failed");
            UsageRollupJobResult::Retry {
                delay: USAGE_ROLLUP_RETRY_DELAY,
                error: error.to_string(),
            }
        }
    }
}
