use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cc_lb_config::SubscriptionQuotaConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::dynamic_view_builder::Stores;

pub fn spawn_subscription_quota_gc(
    stores: Arc<Stores>,
    config: SubscriptionQuotaConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let tick_secs = config.gc_tick_interval_secs.max(30);
        let mut interval = tokio::time::interval(Duration::from_secs(tick_secs));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            let retention_days = config.retention_days.max(8);
            let cutoff_unix_secs =
                now_unix_secs().saturating_sub(retention_days.saturating_mul(86_400));
            match stores
                .upstream_subscription_quotas
                .delete_subscription_quota_before(
                    cutoff_unix_secs.saturating_mul(1_000),
                    config.gc_batch_size,
                )
                .await
            {
                Ok(rows_deleted) => {
                    tracing::info!(rows_deleted, "subscription quota GC tick completed");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "subscription quota GC tick failed");
                }
            }
        }
    })
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
