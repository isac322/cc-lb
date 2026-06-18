use std::future::Future;
use std::time::Duration;

use cc_lb_storage_api::{StorageResult, UpstreamSubscriptionQuotaStore};
use serde::{Deserialize, Serialize};

use crate::middleware::TraceparentCarrier;

pub const SECONDS_PER_DAY: u64 = 86_400;
pub const SUBSCRIPTION_QUOTA_MIN_RETENTION_DAYS: u64 = 8;
pub const SUBSCRIPTION_QUOTA_GC_RETRY_DELAY: Duration = Duration::from_secs(60);

pub trait SubscriptionQuotaGcStore: Send + Sync {
    fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> impl Future<Output = StorageResult<u64>> + Send + '_;
}

impl<Store> SubscriptionQuotaGcStore for Store
where
    Store: UpstreamSubscriptionQuotaStore + ?Sized,
{
    fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> impl Future<Output = StorageResult<u64>> + Send + '_ {
        async move {
            UpstreamSubscriptionQuotaStore::delete_subscription_quota_before(
                self,
                cutoff_unix_millis,
                batch_size,
            )
            .await
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaGcJob {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl TraceparentCarrier for SubscriptionQuotaGcJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionQuotaGcConfig {
    pub retention_days: u64,
    pub batch_size: u32,
}

impl SubscriptionQuotaGcConfig {
    pub const fn new(retention_days: u64, batch_size: u32) -> Self {
        Self {
            retention_days,
            batch_size,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubscriptionQuotaGcJobResult {
    Done {
        rows_removed: u64,
        cutoff_unix_millis: u64,
    },
    Retry {
        delay: Duration,
        error: String,
    },
}

#[derive(Clone, Debug)]
pub struct SubscriptionQuotaGcJobHandler<Quotas> {
    quotas: Quotas,
    config: SubscriptionQuotaGcConfig,
}

impl<Quotas> SubscriptionQuotaGcJobHandler<Quotas> {
    pub const fn new(quotas: Quotas, config: SubscriptionQuotaGcConfig) -> Self {
        Self { quotas, config }
    }
}

impl<Quotas> SubscriptionQuotaGcJobHandler<Quotas>
where
    Quotas: SubscriptionQuotaGcStore,
{
    pub async fn handle(
        &self,
        job: SubscriptionQuotaGcJob,
        now_unix_secs: u64,
    ) -> SubscriptionQuotaGcJobResult {
        if let Some(traceparent) = job.traceparent.as_deref() {
            tracing::debug!(traceparent, "handling subscription quota GC job");
        }
        let cutoff_unix_millis = cutoff_unix_millis(now_unix_secs, self.config.retention_days);
        match self
            .quotas
            .delete_subscription_quota_before(cutoff_unix_millis, self.config.batch_size)
            .await
        {
            Ok(rows_removed) => {
                metrics::counter!("cclb_scheduler_quota_gc_rows_removed_total")
                    .increment(rows_removed);
                tracing::info!(
                    rows_removed,
                    cutoff_unix_millis,
                    batch_size = self.config.batch_size,
                    "subscription quota GC job complete",
                );
                SubscriptionQuotaGcJobResult::Done {
                    rows_removed,
                    cutoff_unix_millis,
                }
            }
            Err(error) => {
                tracing::warn!(%error, "subscription quota GC job failed");
                SubscriptionQuotaGcJobResult::Retry {
                    delay: SUBSCRIPTION_QUOTA_GC_RETRY_DELAY,
                    error: error.to_string(),
                }
            }
        }
    }
}

fn cutoff_unix_millis(now_unix_secs: u64, retention_days: u64) -> u64 {
    now_unix_secs
        .saturating_sub(
            retention_days
                .max(SUBSCRIPTION_QUOTA_MIN_RETENTION_DAYS)
                .saturating_mul(SECONDS_PER_DAY),
        )
        .saturating_mul(1_000)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Mutex;

    use cc_lb_storage_api::{StorageError, StorageResult};

    use super::{
        SECONDS_PER_DAY, SUBSCRIPTION_QUOTA_GC_RETRY_DELAY, SubscriptionQuotaGcConfig,
        SubscriptionQuotaGcJob, SubscriptionQuotaGcJobHandler, SubscriptionQuotaGcJobResult,
        SubscriptionQuotaGcStore,
    };

    #[tokio::test]
    async fn retention_gc_deletes_one_batch() {
        let cutoff = 12 * SECONDS_PER_DAY * 1_000;
        let handler = SubscriptionQuotaGcJobHandler::new(
            FakeQuotaStore::seeded(&[cutoff - 3, cutoff - 2, cutoff - 1, cutoff, cutoff + 1]),
            SubscriptionQuotaGcConfig::new(1, 2),
        );

        let result = handler
            .handle(SubscriptionQuotaGcJob::default(), 20 * SECONDS_PER_DAY)
            .await;

        assert_eq!(
            result,
            SubscriptionQuotaGcJobResult::Done {
                rows_removed: 2,
                cutoff_unix_millis: cutoff,
            }
        );
        assert_eq!(handler.quotas.calls(), vec![(cutoff, 2)]);
        assert_eq!(
            handler.quotas.remaining(),
            vec![cutoff - 1, cutoff, cutoff + 1]
        );
    }

    #[tokio::test]
    async fn storage_error_returns_retry_without_deleting() {
        let handler = SubscriptionQuotaGcJobHandler::new(
            FakeQuotaStore::failing(vec![10, 20]),
            SubscriptionQuotaGcConfig::new(8, 10),
        );

        let result = handler
            .handle(SubscriptionQuotaGcJob::default(), 20 * SECONDS_PER_DAY)
            .await;

        assert_eq!(
            result,
            SubscriptionQuotaGcJobResult::Retry {
                delay: SUBSCRIPTION_QUOTA_GC_RETRY_DELAY,
                error: "unavailable: quota store unavailable".to_owned(),
            }
        );
        assert_eq!(handler.quotas.remaining(), vec![10, 20]);
    }

    struct FakeQuotaStore {
        observations: Mutex<Vec<u64>>,
        calls: Mutex<Vec<(u64, u32)>>,
        fail_delete: bool,
    }

    impl FakeQuotaStore {
        fn seeded(observations: &[u64]) -> Self {
            Self {
                observations: Mutex::new(observations.to_vec()),
                calls: Mutex::new(Vec::new()),
                fail_delete: false,
            }
        }

        fn failing(observations: Vec<u64>) -> Self {
            Self {
                observations: Mutex::new(observations),
                calls: Mutex::new(Vec::new()),
                fail_delete: true,
            }
        }

        fn remaining(&self) -> Vec<u64> {
            self.observations.lock().expect("observations lock").clone()
        }

        fn calls(&self) -> Vec<(u64, u32)> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    impl SubscriptionQuotaGcStore for FakeQuotaStore {
        fn delete_subscription_quota_before(
            &self,
            cutoff_unix_millis: u64,
            batch_size: u32,
        ) -> impl Future<Output = StorageResult<u64>> + Send + '_ {
            async move {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push((cutoff_unix_millis, batch_size));
                if self.fail_delete {
                    return Err(StorageError::Unavailable {
                        message: "quota store unavailable".to_owned(),
                    });
                }
                let mut removed = 0_u32;
                self.observations
                    .lock()
                    .expect("observations lock")
                    .retain(|observed_at| {
                        if *observed_at < cutoff_unix_millis && removed < batch_size {
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                Ok(u64::from(removed))
            }
        }
    }
}
