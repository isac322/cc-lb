use std::sync::Arc;
use std::time::Duration;

use cc_lb_clock::{Clock, ClockHandle, unix_millis};

use crate::Storage;

const DAY_MS: u64 = 86_400_000;
const DAY_SECS: u64 = 86_400;
const KEY_SEQUENCE_SCALE: u64 = 1_000_000;
const PRUNE_BATCH_SIZE: usize = 1_000;
const PRUNE_BATCH_SLEEP: Duration = Duration::from_millis(50);

pub struct UsagePruner {
    storage: Arc<dyn Storage>,
    retention_days: u64,
    clock: ClockHandle,
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
    pub fn new(
        storage: impl IntoUsagePrunerStorage,
        retention_days: u64,
        clock: ClockHandle,
    ) -> Self {
        Self {
            storage: storage.into_usage_pruner_storage(),
            retention_days,
            clock,
        }
    }

    pub async fn prune_once(&self) -> PruneResult {
        if self.retention_days == 0 {
            return PruneResult::default();
        }

        let now_ms = now_unix_ms(&*self.clock);
        let retention_ms = self.retention_days.saturating_mul(DAY_MS);
        let cutoff_ms = now_ms.saturating_sub(retention_ms);
        let cutoff_secs = cutoff_ms / 1_000;
        let request_events_removed = self
            .prune_request_events(cutoff_ms.saturating_mul(KEY_SEQUENCE_SCALE))
            .await;
        let audit_log_removed = self
            .prune_audit_log(cutoff_secs.saturating_mul(KEY_SEQUENCE_SCALE))
            .await;
        // Reported usage of the current UTC month is never pruned, so the
        // month summary always starts on the 1st and is never re-fetched.
        let upstream_api_usage_removed = self
            .prune_upstream_api_usage(cutoff_secs.min(utc_month_start_unix_secs(now_ms / 1_000)))
            .await;

        let minute_cutoff_secs =
            now_ms.saturating_sub(self.retention_days.min(14).saturating_mul(DAY_MS)) / 1_000;
        let quota_cutoff_ms =
            now_ms.saturating_sub(self.retention_days.min(30).saturating_mul(DAY_MS));
        self.prune_usage_rollups(minute_cutoff_secs, cutoff_secs)
            .await;
        self.prune_subscription_quota_checkpoints(quota_cutoff_ms)
            .await;
        self.prune_pool_quota_history((quota_cutoff_ms / 1_000) as i64)
            .await;
        self.prune_cache_keepalive_history(minute_cutoff_secs).await;

        PruneResult {
            request_events_removed,
            audit_log_removed,
            upstream_api_usage_removed,
        }
    }

    async fn prune_usage_rollups(&self, minute_cutoff_secs: u64, hour_cutoff_secs: u64) {
        loop {
            let removed = match self
                .storage
                .prune_usage_rollups_before(minute_cutoff_secs, hour_cutoff_secs, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune usage rollups");
                    break;
                }
            };
            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
    }

    async fn prune_subscription_quota_checkpoints(&self, cutoff_ms: u64) {
        loop {
            let removed = match self
                .storage
                .prune_subscription_quota_checkpoints_before(cutoff_ms, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune subscription quota checkpoints");
                    break;
                }
            };
            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
    }

    async fn prune_pool_quota_history(&self, cutoff_secs: i64) {
        loop {
            let removed = match self
                .storage
                .prune_pool_quota_history_before(cutoff_secs, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune pool quota history");
                    break;
                }
            };
            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
    }

    async fn prune_cache_keepalive_history(&self, cutoff_secs: u64) {
        loop {
            let removed = match self
                .storage
                .prune_cache_keepalive_history_before(cutoff_secs, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune cache keepalive history");
                    break;
                }
            };
            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
    }

    async fn prune_upstream_api_usage(&self, before_unix_secs: u64) -> u64 {
        let mut total_removed = 0;
        loop {
            let removed = match self
                .storage
                .prune_upstream_api_usage_buckets(before_unix_secs, PRUNE_BATCH_SIZE)
                .await
            {
                Ok(removed) => removed,
                Err(error) => {
                    tracing::warn!(%error, "usage pruner failed to prune upstream API usage");
                    break;
                }
            };
            total_removed += removed;
            if removed < PRUNE_BATCH_SIZE as u64 {
                break;
            }
            tokio::time::sleep(PRUNE_BATCH_SLEEP).await;
        }
        total_removed
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
    pub audit_log_removed: u64,
    /// Provider-reported Anthropic API-key usage buckets removed.
    pub upstream_api_usage_removed: u64,
}

fn now_unix_ms(clock: &dyn Clock) -> u64 {
    unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

/// Start (00:00 UTC on the 1st) of the UTC calendar month containing
/// `unix_secs`.
pub fn utc_month_start_unix_secs(unix_secs: u64) -> u64 {
    let days = unix_secs / DAY_SECS;
    days.saturating_sub(day_of_month(days).saturating_sub(1))
        .saturating_mul(DAY_SECS)
}

/// Day of month (1-based) of `days` since 1970-01-01, using the proleptic
/// Gregorian civil-from-days algorithm.
fn day_of_month(days: u64) -> u64 {
    let z = days + 719_468;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    doy - (153 * mp + 2) / 5 + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_start_is_the_first_at_midnight_utc() {
        // 2026-10-20T13:45:00Z -> 2026-10-01T00:00:00Z
        assert_eq!(utc_month_start_unix_secs(1_792_503_900), 1_790_812_800);
        // 2026-10-01T00:00:00Z is its own month start.
        assert_eq!(utc_month_start_unix_secs(1_790_812_800), 1_790_812_800);
        // 2024-02-29T23:59:59Z (leap day) -> 2024-02-01T00:00:00Z
        assert_eq!(utc_month_start_unix_secs(1_709_251_199), 1_706_745_600);
        // 2024-03-01T00:00:00Z
        assert_eq!(utc_month_start_unix_secs(1_709_251_200), 1_709_251_200);
        // 2025-12-31T23:59:59Z -> 2025-12-01T00:00:00Z
        assert_eq!(utc_month_start_unix_secs(1_767_225_599), 1_764_547_200);
        assert_eq!(utc_month_start_unix_secs(0), 0);
    }
}
