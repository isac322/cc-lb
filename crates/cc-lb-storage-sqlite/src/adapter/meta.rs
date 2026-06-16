use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, MetaStore, StorageResult};

use crate::SqliteStorage;

#[async_trait]
impl MetaStore for SqliteStorage {
    async fn initialize(&self, _requested: BackendKind) -> StorageResult<()> {
        unimplemented!()
    }

    async fn contract_version(&self) -> StorageResult<u32> {
        unimplemented!()
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        unimplemented!()
    }

    async fn killswitch_enabled(&self) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn set_killswitch_enabled(&self, _enabled: bool) -> StorageResult<()> {
        unimplemented!()
    }
}
