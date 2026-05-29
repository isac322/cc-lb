use async_trait::async_trait;
use cc_lb_storage_api::{ChangeEvent, RuntimeChangeNotifier, StorageResult};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::RedbStorage;

const BROADCAST_CAPACITY: usize = 1;

#[async_trait]
impl RuntimeChangeNotifier for RedbStorage {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
        Ok(self.noop_change_tx.subscribe())
    }

    async fn run(&self, _cancel: CancellationToken) -> StorageResult<()> {
        Ok(())
    }
}

pub(crate) fn noop_change_sender() -> broadcast::Sender<ChangeEvent> {
    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
    sender
}
