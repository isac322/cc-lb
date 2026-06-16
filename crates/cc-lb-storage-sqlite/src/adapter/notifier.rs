use std::sync::LazyLock;

use async_trait::async_trait;
use cc_lb_storage_api::{ChangeEvent, RuntimeChangeNotifier, StorageResult};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::SqliteStorage;

const BROADCAST_CAPACITY: usize = 1024;

static CHANGE_TX: LazyLock<broadcast::Sender<ChangeEvent>> = LazyLock::new(|| {
    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
    sender
});

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
