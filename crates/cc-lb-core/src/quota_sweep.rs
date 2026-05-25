use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{QuotaManager, QuotaPolicy};

pub fn start_sweep(manager: Arc<QuotaManager>, period: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        loop {
            interval.tick().await;
            let storage = Arc::clone(&manager.storage);
            let older_than = older_than_window_start(manager.default_policy().await);
            let _result = storage.sweep_old_quotas(older_than).await;
        }
    })
}

fn older_than_window_start(defaults: QuotaPolicy) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    now.saturating_sub(24 * defaults.window_secs.max(1))
}
