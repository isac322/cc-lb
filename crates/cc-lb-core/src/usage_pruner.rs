use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_storage_api::Storage;

const DAY_MS: u64 = 86_400_000;
const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
const PRUNE_BATCH_SIZE: usize = 1_000;
const PRUNE_BATCH_SLEEP: Duration = Duration::from_millis(50);

pub struct UsagePruner {
    storage: Arc<dyn Storage>,
    retention_days: u64,
}

pub trait IntoUsagePrunerStorage {
    fn into_usage_pruner_storage(self) -> Arc<dyn Storage>;
}

impl IntoUsagePrunerStorage for Arc<dyn Storage> {
    fn into_usage_pruner_storage(self) -> Arc<dyn Storage> {
        self
    }
}

impl<Store> IntoUsagePrunerStorage for Arc<Store>
where
    Store: Storage,
{
    fn into_usage_pruner_storage(self) -> Arc<dyn Storage> {
        self
    }
}

impl UsagePruner {
    pub fn new(storage: impl IntoUsagePrunerStorage, retention_days: u64) -> Self {
        Self {
            storage: storage.into_usage_pruner_storage(),
            retention_days,
        }
    }

    pub async fn prune_once(&self) -> PruneResult {
        if self.retention_days == 0 {
            return PruneResult::default();
        }

        let now_ms = now_unix_ms();
        let retention_ms = self.retention_days.saturating_mul(DAY_MS);
        let cutoff_ms = now_ms.saturating_sub(retention_ms);
        let cutoff_secs = cutoff_ms / 1_000;

        let request_events_removed = self
            .prune_request_events(cutoff_ms.saturating_mul(KEY_SEQUENCE_SCALE))
            .await;
        let audit_log_removed = self
            .prune_audit_log(cutoff_secs.saturating_mul(KEY_SEQUENCE_SCALE))
            .await;

        PruneResult {
            request_events_removed,
            usage_rollups_removed: 0,
            principal_limit_states_removed: 0,
            audit_log_removed,
        }
    }

    async fn prune_request_events(&self, cutoff_ms_x_1m: u64) -> u64 {
        let mut total_removed = 0;
        loop {
            let removed = match self
                .storage
                .prune_request_events_before(cutoff_ms_x_1m, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune request events");
                    break;
                }
            };

            if removed == 0 {
                break;
            }
            // TODO(T26): cclb_usage_pruned_rows_total{table="request_events"} counter increment
            total_removed += removed;

            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
        total_removed
    }

    async fn prune_audit_log(&self, cutoff_ts_x_1m: u64) -> u64 {
        let mut total_removed = 0;
        loop {
            let removed = match self
                .storage
                .prune_audit_before(cutoff_ts_x_1m, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune audit log");
                    break;
                }
            };

            if removed == 0 {
                break;
            }
            // TODO(T26): cclb_usage_pruned_rows_total{table="audit_log"} counter increment
            total_removed += removed;

            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
        total_removed
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneResult {
    pub request_events_removed: u64,
    pub usage_rollups_removed: u64,
    pub principal_limit_states_removed: u64,
    pub audit_log_removed: u64,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}
