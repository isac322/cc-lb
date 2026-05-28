use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, CURRENT_CONTRACT_VERSION, MetaStore, StorageResult};

use crate::RedbStorage;

use crate::adapter_error_map::{map_join_err, map_redb_err};

#[async_trait]
impl MetaStore for RedbStorage {
    async fn initialize(&self, requested: BackendKind) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::initialize(&storage, requested))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn contract_version(&self) -> StorageResult<u32> {
        Ok(CURRENT_CONTRACT_VERSION)
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::backend_kind(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn killswitch_enabled(&self) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::killswitch_enabled(&storage))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_killswitch_enabled(&self, enabled: bool) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || RedbStorage::set_killswitch_enabled(&storage, enabled))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }
}
