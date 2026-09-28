use serde::{Deserialize, Serialize};

use crate::{KeyStatus, Limit};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token_expires_at_unix_secs: Option<u64>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredApiKeyRecord {
    pub label: String,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
    pub key_hash_b64: String,
    pub verify_hash: [u8; 32],
    pub secret_salt: [u8; 16],
    pub limit_overrides: Vec<Limit>,
    pub status: KeyStatus,
    pub expires_at_unix_secs: Option<u64>,
    pub last_4: String,
    pub description: Option<String>,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueParams {
    pub label: String,
    pub description: Option<String>,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
    pub secret_salt: [u8; 16],
    pub verify_hash: [u8; 32],
    pub last_4: String,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApiKeyMutation {
    pub label: Option<String>,
    pub description: Option<Option<String>>,
    pub expires_at_unix_secs: Option<Option<u64>>,
    pub limit_overrides: Option<Vec<Limit>>,
    pub status: Option<KeyStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCatalogSnapshotRecord {
    pub json_bytes: Vec<u8>,
    pub fetched_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCatalogSnapshotMetadata {
    pub payload_hash: String,
    pub fetched_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriceCatalogSnapshotFetch {
    Missing,
    Unchanged(PriceCatalogSnapshotMetadata),
    Changed(PriceCatalogSnapshotRecord),
}
