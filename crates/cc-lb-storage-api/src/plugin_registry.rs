use async_trait::async_trait;
use uuid::Uuid;

use crate::StorageResult;

pub use crate::PluginSlotKind;
pub use crate::plugin_registry_models::*;

const fn default_pure() -> bool {
    true
}

const _: () = assert!(default_pure());

#[async_trait]
pub trait PluginRegistryStore: Send + Sync {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
    ) -> StorageResult<(WasmRegistryEntry, bool)>;

    async fn get_blob(&self, sha256: [u8; 32]) -> StorageResult<Option<WasmBlobRecord>>;

    async fn get_blob_bytes(&self, sha256: [u8; 32]) -> StorageResult<Option<Vec<u8>>>;

    async fn list_orphan_blobs(&self) -> StorageResult<Vec<[u8; 32]>>;

    async fn list_registry(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<WasmRegistryEntry>>;

    async fn get_registry_entry_by_sha(
        &self,
        sha256: [u8; 32],
    ) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn get_registry_entry_by_name(
        &self,
        name: &str,
    ) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn replace_wasm_entry(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
        expected_revision: u64,
    ) -> StorageResult<WasmRegistryEntry>;

    async fn list_registry_references(&self, id: Uuid) -> StorageResult<WasmRegistryReferences>;

    async fn cascade_delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        expected_references: WasmRegistryReferenceFingerprint,
    ) -> StorageResult<Option<WasmRegistryCascadeDelete>>;

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry>;

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlotKind>,
    ) -> StorageResult<()>;

    async fn delete_registry_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn decrement_blob_refcount_or_delete(&self, sha256: [u8; 32]) -> StorageResult<bool>;

    async fn insert_chain_entry(
        &self,
        entry: PluginChainEntryInput,
    ) -> StorageResult<PluginChainEntry>;

    async fn list_chain_for_principal(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>>;

    async fn list_chains_for_principals(
        &self,
        principal_ids: &[Uuid],
        slots: &[PluginSlotKind],
    ) -> StorageResult<Vec<PluginChainEntry>> {
        let mut entries = Vec::new();
        for principal_id in principal_ids {
            for slot in slots {
                entries.extend(self.list_chain_for_principal(*principal_id, *slot).await?);
            }
        }
        Ok(entries)
    }

    async fn update_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>>;

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>>;

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>>;

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlotKind,
    ) -> StorageResult<Vec<PluginChainEntry>>;
}
