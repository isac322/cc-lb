use async_trait::async_trait;
use cc_lb_storage_api::PluginSlot;
use cc_lb_storage_api::{
    PluginBlobRepo, PluginChainEntry, PluginChainEntryInput, PluginChainEntryUpdate,
    PluginRegistryRecord, PluginRegistryRepo, PluginRegistryStatus, PluginRegistryStore, RepoError,
    StorageResult, WasmBlob, WasmBlobRecord, WasmRegistryEntry, WasmRegistryEntryInput,
};
use uuid::Uuid;

use crate::SqliteStorage;

#[async_trait]
impl PluginRegistryStore for SqliteStorage {
    async fn persist_wasm_upload(
        &self,
        _blob: WasmBlob,
        _entry: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)> {
        unimplemented!()
    }

    async fn get_blob(&self, _sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>> {
        unimplemented!()
    }

    async fn get_blob_bytes(&self, _sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>> {
        unimplemented!()
    }

    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>> {
        unimplemented!()
    }

    async fn list_registry(
        &self,
        _after: Option<Uuid>,
        _limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>> {
        unimplemented!()
    }

    async fn get_registry_entry_by_sha(
        &self,
        _sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }

    async fn get_registry_entry_by_id(
        &self,
        _id: Uuid,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }

    async fn update_registry_label(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry> {
        unimplemented!()
    }

    async fn update_supported_slots(
        &self,
        _id: Uuid,
        _supported_slots: Vec<PluginSlot>,
    ) -> StorageResult<()> {
        unimplemented!()
    }

    async fn update_wire_version(&self, _id: Uuid, _wire_version: u8) -> StorageResult<()> {
        unimplemented!()
    }

    async fn delete_registry_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>> {
        unimplemented!()
    }

    async fn decrement_blob_refcount_or_delete(&self, _sha256: [u8; 32]) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn insert_chain_entry(
        &self,
        _entry: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry> {
        unimplemented!()
    }

    async fn list_chain_for_principal(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        unimplemented!()
    }

    async fn update_chain_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
        _update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>> {
        unimplemented!()
    }

    async fn reorder_chain(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
        _new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        unimplemented!()
    }

    async fn delete_chain_entry(
        &self,
        _id: Uuid,
        _expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>> {
        unimplemented!()
    }

    async fn rebalance_chain(
        &self,
        _principal_id: Uuid,
        _slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>> {
        unimplemented!()
    }
}

#[async_trait]
impl PluginRegistryRepo for SqliteStorage {
    async fn upsert_record(&self, _record: &PluginRegistryRecord) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn get_by_sha256(
        &self,
        _sha256: &[u8; 32],
    ) -> Result<Option<PluginRegistryRecord>, RepoError> {
        unimplemented!()
    }

    async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
        unimplemented!()
    }

    async fn set_status(
        &self,
        _sha256: &[u8; 32],
        _status: PluginRegistryStatus,
    ) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn delete_by_sha256(&self, _sha256: &[u8; 32]) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn count(&self) -> Result<usize, RepoError> {
        unimplemented!()
    }

    async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
        unimplemented!()
    }

    async fn set_shutdown_marker(&self, _unix_secs: i64) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
        unimplemented!()
    }
}

#[async_trait]
impl PluginBlobRepo for SqliteStorage {
    async fn put_blob(&self, _sha256: &[u8; 32], _bytes: &[u8]) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn get_blob(&self, _sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
        unimplemented!()
    }

    async fn delete_blob(&self, _sha256: &[u8; 32]) -> Result<(), RepoError> {
        unimplemented!()
    }

    async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
        unimplemented!()
    }
}
