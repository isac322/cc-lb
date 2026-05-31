use std::sync::Arc;
use std::time::Duration;

use cc_lb_storage_api::Storage;

const ROLLUP_TICK: Duration = Duration::from_secs(30);

pub struct UsageRollupJob {
    storage: Arc<dyn Storage>,
}

impl UsageRollupJob {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub fn start_daemon(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(ROLLUP_TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match self.storage.rollup_usage_once().await {
                    Ok(run) => {
                        if run.processed_events > 0 || run.updated_rollups > 0 {
                            tracing::info!(
                                processed_events = run.processed_events,
                                updated_rollups = run.updated_rollups,
                                checkpoint = ?run.checkpoint,
                                "usage rollup tick complete",
                            );
                        } else {
                            tracing::debug!(
                                checkpoint = ?run.checkpoint,
                                "usage rollup tick complete (no new events)",
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "usage rollup tick failed");
                    }
                }
            }
        })
    }
}
