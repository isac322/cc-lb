use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::StorageResult;

pub const MAX_WASM_BLOB_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmBlob {
    pub sha256: [u8; 32],
    pub bytes: Vec<u8>,
    pub size_bytes: u64,
    pub parse_validated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmBlobRecord {
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmRegistryEntryInput {
    pub name: String,
    pub original_filename: String,
    pub label: Option<String>,
    pub uploaded_at_unix_secs: u64,
    pub uploaded_by_admin_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmRegistryEntry {
    pub id: Uuid,
    pub sha256: [u8; 32],
    pub name: String,
    pub original_filename: String,
    pub label: Option<String>,
    pub uploaded_at_unix_secs: u64,
    pub uploaded_by_admin_id: Uuid,
    pub refcount: i64,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginSlot {
    Router,
    ObservabilityHook,
    Shape,
}

impl PluginSlot {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Router => "router",
            Self::ObservabilityHook => "observability_hook",
            Self::Shape => "shape",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "router" => Some(Self::Router),
            "observability_hook" => Some(Self::ObservabilityHook),
            "shape" => Some(Self::Shape),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginChainEntryInput {
    pub principal_id: Uuid,
    pub slot: PluginSlot,
    pub order: i64,
    pub wasm_registry_id: Uuid,
    pub config: Value,
    pub sse_per_event: bool,
    pub batched_events_per_flush: u32,
    pub batched_flush_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginChainEntry {
    pub id: Uuid,
    pub principal_id: Uuid,
    pub slot: PluginSlot,
    pub order: i64,
    pub wasm_registry_id: Uuid,
    pub config: Value,
    pub sse_per_event: bool,
    pub batched_events_per_flush: u32,
    pub batched_flush_ms: u64,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PluginChainEntryUpdate {
    pub config: Option<Value>,
    pub sse_per_event: Option<bool>,
    pub batched_events_per_flush: Option<u32>,
    pub batched_flush_ms: Option<u64>,
}

#[async_trait]
pub trait PluginRegistryStore: Send + Sync {
    async fn persist_wasm_upload(
        &self,
        blob: WasmBlob,
        entry: WasmRegistryEntryInput,
    ) -> StorageResult<WasmRegistryEntry>;

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

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry>;

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
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>>;

    async fn update_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PluginChainEntryUpdate,
    ) -> StorageResult<Option<PluginChainEntry>>;

    async fn reorder_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
        new_orders: Vec<(Uuid, i64, u64)>,
    ) -> StorageResult<Vec<PluginChainEntry>>;

    async fn delete_chain_entry(&self, id: Uuid) -> StorageResult<bool>;

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>>;
}
