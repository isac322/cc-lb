use std::collections::BTreeMap;

use cc_lb_plugin_wire::metadata::HookMetadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::PluginSlotKind;

pub use cc_lb_domain::{BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME};

pub const BUILTIN_PLUGIN_KIND_FILTER: &str = "filter";
/// Pseudo-SHA for the built-in `subscription-preference` filter.
///
/// Encoded as ASCII `b"subscription-preference"` followed by NUL padding so it
/// never collides with the `[seed; 32]` uniform patterns that conformance and
/// fixture tests use.
pub const BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256: [u8; 32] = [
    0x73, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6F, 0x6E, 0x2D, 0x70, 0x72, 0x65,
    0x66, 0x65, 0x72, 0x65, 0x6E, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub original_filename: String,
    pub label: Option<String>,
    pub uploaded_at_unix_secs: u64,
    pub uploaded_by_admin_id: Uuid,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub usage: String,
    #[serde(default)]
    pub hook_metadata: BTreeMap<String, HookMetadata>,
    /// Slots the plugin exports a wire function for, derived from
    /// `HandshakeAccept.implemented_functions`. Empty preserves legacy uploads
    /// that did not supply this metadata.
    #[serde(default)]
    pub supported_slots: Vec<PluginSlotKind>,
    /// 32-byte BLAKE3 schema hash from `cc_lb.schema.<kind>.v1` custom
    /// section (set by the admin upload after `inspect_wasm`). `None`
    /// preserves legacy uploads that pre-date the wasmtime ABI.
    #[serde(default)]
    pub schema_hash: Option<[u8; 32]>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub original_filename: String,
    pub label: Option<String>,
    pub uploaded_at_unix_secs: u64,
    pub uploaded_by_admin_id: Uuid,
    pub refcount: i64,
    pub revision: u64,
    #[serde(default = "default_plugin_kind")]
    pub kind: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub usage: String,
    #[serde(default)]
    pub hook_metadata: BTreeMap<String, HookMetadata>,
    #[serde(default)]
    pub is_builtin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<PluginMetadata>,
    /// Slots the plugin exports a wire function for. Empty when a legacy
    /// entry has not yet been backfilled by `run_startup_handshake`.
    #[serde(default)]
    pub supported_slots: Vec<PluginSlotKind>,
    /// See [`WasmRegistryEntryInput::schema_hash`].
    #[serde(default)]
    pub schema_hash: Option<[u8; 32]>,
}

impl WasmRegistryEntry {
    pub fn builtin_subscription_preference(refcount: i64) -> Self {
        Self {
            id: BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
            sha256: BUILTIN_SUBSCRIPTION_PREFERENCE_SHA256,
            name: BUILTIN_SUBSCRIPTION_PREFERENCE_NAME.to_owned(),
            version: None,
            original_filename: "builtin://subscription-preference".to_owned(),
            label: Some("Built-in subscription preference filter".to_owned()),
            uploaded_at_unix_secs: 0,
            uploaded_by_admin_id: Uuid::nil(),
            refcount,
            revision: 0,
            kind: BUILTIN_PLUGIN_KIND_FILTER.to_owned(),
            description: "Prefer subscription/OAuth upstreams while quota appears alive."
                .to_owned(),
            usage: "Attach to router chains before API-key fallback filters.".to_owned(),
            hook_metadata: builtin_filter_hook_metadata(),
            is_builtin: true,
            metadata: Some(builtin_metadata_for_subscription_preference()),
            supported_slots: vec![PluginSlotKind::Router],
            schema_hash: None,
        }
    }
}

fn default_plugin_kind() -> String {
    BUILTIN_PLUGIN_KIND_FILTER.to_owned()
}

pub fn default_wire_version() -> u8 {
    1
}

fn builtin_filter_hook_metadata() -> BTreeMap<String, HookMetadata> {
    BTreeMap::from([(
        "filter".to_owned(),
        HookMetadata {
            wire_version: default_wire_version(),
            description: "Built-in filter hook".to_owned(),
            usage: "Called by the router filter pipeline.".to_owned(),
            mode: Default::default(),
        },
    )])
}

fn builtin_metadata_for_subscription_preference() -> PluginMetadata {
    PluginMetadata {
        purpose: "Prefer subscription/OAuth upstreams while quota appears alive; use API-key upstreams only when subscription candidates are exhausted.".to_owned(),
        keeps: "OAuth candidates whose subscription quota is not clearly exhausted, or API-key candidates when every OAuth candidate is exhausted.".to_owned(),
        drops: "API-key candidates while at least one OAuth candidate appears alive; exhausted OAuth candidates when API-key fallback is available.".to_owned(),
        empty_behavior: "Never drops everything. If no API-key fallback exists, exhausted OAuth candidates pass through so the upstream/provider returns the authoritative result.".to_owned(),
        examples: vec![
            "OAuth and API-key candidates, OAuth quota alive → keep OAuth candidates only.".to_owned(),
            "OAuth and API-key candidates, every OAuth quota exhausted → keep API-key candidates only.".to_owned(),
            "Only API-key candidates → keep API-key candidates.".to_owned(),
        ],
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginChainEntryInput {
    pub principal_id: Uuid,
    pub slot: PluginSlotKind,
    pub order: i64,
    pub wasm_registry_id: Uuid,
    pub config: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginChainEntry {
    pub id: Uuid,
    pub principal_id: Uuid,
    pub slot: PluginSlotKind,
    pub order: i64,
    pub wasm_registry_id: Uuid,
    pub config: Value,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PluginChainEntryUpdate {
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WasmRegistryReferenceFingerprint([u8; 32]);

impl WasmRegistryReferenceFingerprint {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_references(references: &[WasmRegistryReference]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cc-lb-wasm-registry-references-v1");
        for reference in references {
            match reference {
                WasmRegistryReference::PluginChain {
                    chain_entry_id,
                    principal_id,
                    principal_name,
                    slot,
                    revision,
                } => {
                    hasher.update(&[0]);
                    hasher.update(chain_entry_id.as_bytes());
                    hasher.update(principal_id.as_bytes());
                    update_fingerprint_bytes(&mut hasher, principal_name.as_bytes());
                    update_fingerprint_bytes(&mut hasher, slot.as_str().as_bytes());
                    hasher.update(&revision.to_be_bytes());
                }
                WasmRegistryReference::UpstreamWarmupDialect {
                    upstream_id,
                    upstream_name,
                    revision,
                } => {
                    hasher.update(&[1]);
                    hasher.update(upstream_id.as_bytes());
                    update_fingerprint_bytes(&mut hasher, upstream_name.as_bytes());
                    hasher.update(&revision.to_be_bytes());
                }
            }
        }
        Self(*hasher.finalize().as_bytes())
    }
}

fn update_fingerprint_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&value.len().to_be_bytes());
    hasher.update(value);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WasmRegistryReference {
    PluginChain {
        chain_entry_id: Uuid,
        principal_id: Uuid,
        principal_name: String,
        slot: PluginSlotKind,
        revision: u64,
    },
    UpstreamWarmupDialect {
        upstream_id: Uuid,
        upstream_name: String,
        revision: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmRegistryReferences {
    pub references: Vec<WasmRegistryReference>,
    pub fingerprint: WasmRegistryReferenceFingerprint,
}

impl WasmRegistryReferences {
    pub fn from_references(references: Vec<WasmRegistryReference>) -> Self {
        let fingerprint = WasmRegistryReferenceFingerprint::from_references(&references);
        Self {
            references,
            fingerprint,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmRegistryCascadeDelete {
    pub entry: WasmRegistryEntry,
    pub references: Vec<WasmRegistryReference>,
}
