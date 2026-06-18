use std::future::Future;
use std::time::Duration;

use cc_lb_storage_api::{PromptCacheObservationStore, StorageResult};
use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

pub const PROMPT_CACHE_OBSERVATION_PURGE_RETRY_DELAY: Duration = Duration::from_secs(60);

pub trait PromptCacheObservationPurgeStore: Send + Sync {
    fn purge_expired_before(
        &self,
        ts_unix_secs: u64,
    ) -> impl Future<Output = StorageResult<u64>> + Send + '_;
}

impl<Store> PromptCacheObservationPurgeStore for Store
where
    Store: PromptCacheObservationStore,
{
    async fn purge_expired_before(&self, ts_unix_secs: u64) -> StorageResult<u64> {
        PromptCacheObservationStore::purge_expired_before(self, ts_unix_secs).await
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromptCacheObservationPurgeJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl TraceparentCarrier for PromptCacheObservationPurgeJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptCacheObservationPurgeJobResult {
    Done {
        rows_removed: u64,
        cutoff_unix_secs: u64,
    },
    Retry {
        delay: Duration,
        error: String,
    },
}

#[derive(Clone, Debug)]
pub struct PromptCacheObservationPurgeJobHandler<Store> {
    store: Store,
}

impl<Store> PromptCacheObservationPurgeJobHandler<Store> {
    pub const fn new(store: Store) -> Self {
        Self { store }
    }
}

impl<Store> PromptCacheObservationPurgeJobHandler<Store>
where
    Store: PromptCacheObservationPurgeStore,
{
    pub async fn handle(
        &self,
        job: PromptCacheObservationPurgeJob,
        now_unix_secs: u64,
    ) -> PromptCacheObservationPurgeJobResult {
        if let Some(traceparent) = job.traceparent.as_deref() {
            tracing::debug!(traceparent, "handling prompt cache observation purge job");
        }

        match self.store.purge_expired_before(now_unix_secs).await {
            Ok(rows_removed) => {
                ::metrics::counter!(
                    crate::scheduler_metrics::PROMPT_CACHE_PURGE_ROWS_REMOVED_TOTAL
                )
                .increment(rows_removed);
                tracing::info!(
                    rows_removed,
                    cutoff_unix_secs = now_unix_secs,
                    "prompt cache observation purge job complete",
                );
                PromptCacheObservationPurgeJobResult::Done {
                    rows_removed,
                    cutoff_unix_secs: now_unix_secs,
                }
            }
            Err(error) => {
                tracing::warn!(%error, "prompt cache observation purge job failed");
                PromptCacheObservationPurgeJobResult::Retry {
                    delay: PROMPT_CACHE_OBSERVATION_PURGE_RETRY_DELAY,
                    error: error.to_string(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
