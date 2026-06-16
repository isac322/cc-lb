use async_trait::async_trait;
use cc_lb_storage_api::{ChangeEvent, RuntimeChangeNotifier, StorageResult};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::SqliteStorage;

#[async_trait]
impl RuntimeChangeNotifier for SqliteStorage {
    async fn subscribe(&self) -> StorageResult<broadcast::Receiver<ChangeEvent>> {
        unimplemented!()
    }

    async fn run(&self, _cancel: CancellationToken) -> StorageResult<()> {
        unimplemented!()
    }
}
