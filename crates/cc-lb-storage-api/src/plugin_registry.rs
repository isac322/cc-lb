use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::StorageResult;

pub use cc_lb_plugin_api::{
    BUILTIN_CACHE_AFFINITY_ID, BUILTIN_CACHE_AFFINITY_NAME, BUILTIN_CACHE_AFFINITY_WIRE_VERSION,
};

pub const BUILTIN_PLUGIN_KIND_FILTER: &str = "filter";
pub const BUILTIN_CACHE_AFFINITY_SHA256: [u8; 32] = [0; 32];

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
    /// Slots the plugin exports a wire function for, derived from
    /// `HandshakeAccept.implemented_functions`. Empty preserves legacy uploads
    /// that did not supply this metadata.
    #[serde(default)]
    pub supported_slots: Vec<PluginSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub purpose: String,
    pub keeps: String,
    pub drops: String,
    pub empty_behavior: String,
    #[serde(default)]
    pub examples: Vec<String>,
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
    #[serde(default = "default_plugin_kind")]
    pub kind: String,
    #[serde(default = "default_wire_version")]
    pub wire_version: u8,
    #[serde(default)]
    pub is_builtin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<PluginMetadata>,
    /// Slots the plugin exports a wire function for. Empty when a legacy
    /// entry has not yet been backfilled by `run_startup_handshake`.
    #[serde(default)]
    pub supported_slots: Vec<PluginSlot>,
}

impl WasmRegistryEntry {
    pub fn builtin_cache_affinity(refcount: i64) -> Self {
        Self {
            id: BUILTIN_CACHE_AFFINITY_ID,
            sha256: BUILTIN_CACHE_AFFINITY_SHA256,
            name: BUILTIN_CACHE_AFFINITY_NAME.to_owned(),
            original_filename: "builtin://cache-affinity".to_owned(),
            label: Some("Built-in cache affinity filter".to_owned()),
            uploaded_at_unix_secs: 0,
            uploaded_by_admin_id: Uuid::nil(),
            refcount,
            revision: 0,
            kind: BUILTIN_PLUGIN_KIND_FILTER.to_owned(),
            wire_version: BUILTIN_CACHE_AFFINITY_WIRE_VERSION,
            is_builtin: true,
            metadata: Some(builtin_metadata_for_cache_affinity()),
            supported_slots: vec![PluginSlot::Router],
        }
    }

    pub fn is_cache_affinity_builtin(&self) -> bool {
        self.id == BUILTIN_CACHE_AFFINITY_ID
    }
}

fn default_plugin_kind() -> String {
    BUILTIN_PLUGIN_KIND_FILTER.to_owned()
}

fn default_wire_version() -> u8 {
    1
}

fn builtin_metadata_for_cache_affinity() -> PluginMetadata {
    PluginMetadata {
        purpose: "Prefer upstreams whose prompt cache is already warm for this request.".to_owned(),
        keeps: "Candidates with a positive prefill_cache_score (the upstream has already cached the prefix).".to_owned(),
        drops: "Candidates with zero cache score — only when at least one candidate is a cache hit; otherwise nothing is dropped.".to_owned(),
        empty_behavior: "Never drops everything. Falls back to passing all candidates through when no cache hit exists.".to_owned(),
        examples: vec![
            "5 candidates, 2 with positive cache score → keep the 2 hits.".to_owned(),
            "5 candidates, all with zero cache score → pass all 5 through.".to_owned(),
            "Exactly 1 candidate → no change.".to_owned(),
        ],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    /// Host-side wire version to negotiate when instantiating this plugin.
    /// `None` (the default) preserves the historical behaviour of falling back
    /// to wire v1 inside the runtime; admins must opt v2 plugins in explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
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
    /// Negotiated wire version requested for this chain entry. Forward-compatible
    /// `None` for records persisted before this field existed; reads back from
    /// redb/postgres after a round-trip through `insert_chain_entry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
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

    async fn get_registry_entry_by_id(&self, id: Uuid) -> StorageResult<Option<WasmRegistryEntry>>;

    async fn update_registry_label(
        &self,
        id: Uuid,
        expected_revision: u64,
        label: Option<String>,
    ) -> StorageResult<WasmRegistryEntry>;

    async fn update_supported_slots(
        &self,
        id: Uuid,
        supported_slots: Vec<PluginSlot>,
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

    async fn delete_chain_entry(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<PluginChainEntry>>;

    async fn rebalance_chain(
        &self,
        principal_id: Uuid,
        slot: PluginSlot,
    ) -> StorageResult<Vec<PluginChainEntry>>;
}
