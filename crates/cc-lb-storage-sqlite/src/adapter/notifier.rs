use std::sync::LazyLock;
use std::time::SystemTime;

use async_trait::async_trait;
use cc_lb_storage_api::{ChangeChannel, ChangeEvent, RuntimeChangeNotifier, StorageResult};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::SqliteStorage;

const BROADCAST_CAPACITY: usize = 1024;

static CHANGE_TX: LazyLock<broadcast::Sender<ChangeEvent>> = LazyLock::new(|| {
    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
    sender
});

/// Publish a runtime change event so the in-process NotifyListener rebuilds the
/// dynamic view immediately. Postgres uses `pg_notify` SQL triggers; SQLite has
/// no equivalent so the adapter explicitly calls this after successful writes
/// to principals/upstreams/plugins. Send errors mean no subscribers yet, which
/// is fine: the next subscriber will pick up later writes.
pub(crate) fn publish_change(
    channel: ChangeChannel,
    payload: impl AsRef<str>,
    observed_at: SystemTime,
) {
    let _ = CHANGE_TX.send(ChangeEvent::new(channel, payload, observed_at));
}

#[async_trait]
impl RuntimeChangeNotifier for SqliteStorage {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
        Ok(CHANGE_TX.subscribe())
    }

    async fn run(&self, cancel: CancellationToken) -> StorageResult<()> {
        cancel.cancelled().await;
        Ok(())
    }
}
