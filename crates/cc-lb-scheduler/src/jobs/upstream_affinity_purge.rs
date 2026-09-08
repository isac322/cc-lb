use std::future::Future;
use std::time::Duration;

use cc_lb_storage_api::{StorageResult, UpstreamAffinityStore};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::middleware::TraceparentCarrier;

pub const UPSTREAM_AFFINITY_PURGE_BATCH_SIZE: usize = 1_000;
pub const UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB: usize = 1_024;
pub const UPSTREAM_AFFINITY_PURGE_RETRY_DELAY: Duration = Duration::from_secs(60);

pub trait UpstreamAffinityPurgeStore: Send + Sync {
    fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> impl Future<Output = StorageResult<u64>> + Send + '_;
}

impl<Store> UpstreamAffinityPurgeStore for Store
where
    Store: UpstreamAffinityStore + ?Sized,
{
    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        UpstreamAffinityStore::purge_expired_upstream_affinities(
            self,
            now_unix_secs,
            ttl_secs,
            batch_size,
        )
        .await
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpstreamAffinityPurgeJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl TraceparentCarrier for UpstreamAffinityPurgeJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpstreamAffinityPurgeJobResult {
    Done {
        rows_removed: u64,
        batches: usize,
        limit_reached: bool,
    },
    Cancelled {
        rows_removed: u64,
        batches: usize,
    },
    Retry {
        delay: Duration,
        error: String,
        rows_removed: u64,
        batches: usize,
    },
}

#[derive(Clone, Debug)]
pub struct UpstreamAffinityPurgeJobHandler<Store> {
    store: Store,
}

impl<Store> UpstreamAffinityPurgeJobHandler<Store> {
    pub const fn new(store: Store) -> Self {
        Self { store }
    }
}

impl<Store> UpstreamAffinityPurgeJobHandler<Store>
where
    Store: UpstreamAffinityPurgeStore,
{
    pub async fn handle(
        &self,
        job: UpstreamAffinityPurgeJob,
        now_unix_secs: u64,
        ttl_secs: u64,
        cancel: &CancellationToken,
    ) -> UpstreamAffinityPurgeJobResult {
        if let Some(traceparent) = job.traceparent.as_deref() {
            tracing::debug!(traceparent, "handling upstream affinity purge job");
        }

        let mut rows_removed = 0_u64;
        let mut batches = 0_usize;
        loop {
            if cancel.is_cancelled() {
                tracing::debug!(
                    rows_removed,
                    batches,
                    "upstream affinity purge job cancelled"
                );
                return UpstreamAffinityPurgeJobResult::Cancelled {
                    rows_removed,
                    batches,
                };
            }
            if batches == UPSTREAM_AFFINITY_PURGE_MAX_BATCHES_PER_JOB {
                tracing::info!(
                    rows_removed,
                    batches,
                    ttl_secs,
                    "upstream affinity purge job reached its batch limit",
                );
                return UpstreamAffinityPurgeJobResult::Done {
                    rows_removed,
                    batches,
                    limit_reached: true,
                };
            }

            match self
                .store
                .purge_expired_upstream_affinities(
                    now_unix_secs,
                    ttl_secs,
                    UPSTREAM_AFFINITY_PURGE_BATCH_SIZE,
                )
                .await
            {
                Ok(batch_rows_removed) => {
                    rows_removed = rows_removed.saturating_add(batch_rows_removed);
                    batches += 1;
                    ::metrics::counter!(
                        crate::scheduler_metrics::PRUNE_ROWS_REMOVED_TOTAL,
                        "table" => "upstream_affinity",
                    )
                    .increment(batch_rows_removed);
                    if batch_rows_removed < UPSTREAM_AFFINITY_PURGE_BATCH_SIZE as u64 {
                        tracing::info!(
                            rows_removed,
                            batches,
                            ttl_secs,
                            "upstream affinity purge job complete",
                        );
                        return UpstreamAffinityPurgeJobResult::Done {
                            rows_removed,
                            batches,
                            limit_reached: false,
                        };
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        rows_removed,
                        batches,
                        ttl_secs,
                        "upstream affinity purge job failed",
                    );
                    return UpstreamAffinityPurgeJobResult::Retry {
                        delay: UPSTREAM_AFFINITY_PURGE_RETRY_DELAY,
                        error: error.to_string(),
                        rows_removed,
                        batches,
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
