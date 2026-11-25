use std::collections::BTreeMap;

use cc_lb_plugin_wire::metadata::{HookMetadata, HookMode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{PluginSlotKind, StorageError, StorageResult};

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
    /// `HandshakeAccept.implemented_functions`.
    pub supported_slots: Vec<PluginSlotKind>,
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
    pub kind: String,
    pub description: String,
    pub usage: String,
    pub hook_metadata: BTreeMap<String, HookMetadata>,
    pub is_builtin: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<PluginMetadata>,
    /// Slots the plugin exports a wire function for.
    pub supported_slots: Vec<PluginSlotKind>,
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
        }
    }
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
            mode: HookMode::Active,
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

/// Maximum number of top-level keys forwarded from a plugin chain entry's
/// `config` object into per-request WASM `principal.claims`.
pub const MAX_PLUGIN_CHAIN_CONFIG_CLAIMS: usize = 64;
/// Maximum byte length of a single plugin chain config claim key.
pub const MAX_PLUGIN_CHAIN_CLAIM_KEY_BYTES: usize = 256;
/// Maximum projected byte length of a single plugin chain config claim value.
pub const MAX_PLUGIN_CHAIN_CLAIM_VALUE_BYTES: usize = 16 * 1024;
/// Maximum aggregate byte length (keys + projected values) for all claims in
/// one plugin chain entry config.
pub const MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES: usize = 32 * 1024;

/// Serializes one JSON claim value into deterministic claim bytes.
///
/// Canonical sorted-key serialization MUST stay on `serde_json` (ADR 0002).
fn encode_plugin_claim_value(value: &Value) -> Vec<u8> {
    match value {
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Number(n) => n.to_string().into_bytes(),
        Value::Bool(b) => {
            if *b {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        Value::Null => b"null".to_vec(),
        Value::Array(_) | Value::Object(_) => serde_json::to_vec(value).unwrap_or_default(),
    }
}

/// Validates that a plugin chain entry's `config` stays within bounded claim
/// count and byte budgets before persisting into storage.
pub fn validate_plugin_chain_config(config: &Value) -> StorageResult<()> {
    let invalid = |reason: String| StorageError::InvalidInput {
        field: "config".to_owned(),
        reason,
    };
    let Some(object) = config.as_object() else {
        let encoded_len = serde_json::to_vec(config).map_or(0, |bytes| bytes.len());
        if encoded_len > MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES {
            return Err(invalid(format!(
                "plugin chain config exceeds maximum {MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES} bytes"
            )));
        }
        return Ok(());
    };
    if object.len() > MAX_PLUGIN_CHAIN_CONFIG_CLAIMS {
        return Err(invalid(format!(
            "plugin chain config has {} keys, exceeding maximum {MAX_PLUGIN_CHAIN_CONFIG_CLAIMS}",
            object.len()
        )));
    }
    let mut total_bytes = 0usize;
    for (key, value) in object {
        if key.len() > MAX_PLUGIN_CHAIN_CLAIM_KEY_BYTES {
            return Err(invalid(format!(
                "plugin chain claim key exceeds maximum {MAX_PLUGIN_CHAIN_CLAIM_KEY_BYTES} bytes"
            )));
        }
        let encoded = encode_plugin_claim_value(value);
        if encoded.len() > MAX_PLUGIN_CHAIN_CLAIM_VALUE_BYTES {
            return Err(invalid(format!(
                "plugin chain claim value for '{key}' is {} bytes, exceeding maximum {MAX_PLUGIN_CHAIN_CLAIM_VALUE_BYTES}",
                encoded.len()
            )));
        }
        total_bytes = total_bytes
            .saturating_add(key.len())
            .saturating_add(encoded.len());
        if total_bytes > MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES {
            return Err(invalid(format!(
                "plugin chain config projected claims exceed maximum {MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

/// Deterministically projects a `PluginChainEntry.config` JSON object into
/// bounded `(key, value_bytes)` pairs for WASM `principal.claims`.
pub fn project_plugin_config_claims_bounded(config: &Value) -> Vec<(String, Vec<u8>)> {
    let Some(object) = config.as_object() else {
        return Vec::new();
    };
    let mut claims = Vec::with_capacity(object.len().min(MAX_PLUGIN_CHAIN_CONFIG_CLAIMS));
    let mut total_bytes = 0usize;
    for (key, value) in object {
        if claims.len() >= MAX_PLUGIN_CHAIN_CONFIG_CLAIMS {
            tracing::warn!(
                max_claims = MAX_PLUGIN_CHAIN_CONFIG_CLAIMS,
                "truncating plugin chain config claims beyond claim count bound"
            );
            break;
        }
        if key.len() > MAX_PLUGIN_CHAIN_CLAIM_KEY_BYTES {
            tracing::warn!(
                key_bytes = key.len(),
                max_key_bytes = MAX_PLUGIN_CHAIN_CLAIM_KEY_BYTES,
                "skipping oversized plugin chain claim key"
            );
            continue;
        }
        let bytes = encode_plugin_claim_value(value);
        if bytes.len() > MAX_PLUGIN_CHAIN_CLAIM_VALUE_BYTES {
            tracing::warn!(
                key = key.as_str(),
                value_bytes = bytes.len(),
                max_value_bytes = MAX_PLUGIN_CHAIN_CLAIM_VALUE_BYTES,
                "skipping oversized plugin chain claim value"
            );
            continue;
        }
        let next_total = total_bytes
            .saturating_add(key.len())
            .saturating_add(bytes.len());
        if next_total > MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES {
            tracing::warn!(
                max_total_bytes = MAX_PLUGIN_CHAIN_CONFIG_TOTAL_BYTES,
                "truncating plugin chain config claims beyond aggregate byte bound"
            );
            break;
        }
        total_bytes = next_total;
        claims.push((key.clone(), bytes));
    }
    claims
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
