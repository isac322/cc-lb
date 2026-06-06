use cc_lb_aead::EncryptedOAuthTokens;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamShapePluginRef {
    pub registry_id: Uuid,
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    AnthropicApiKey,
    AnthropicOauth,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    pub enabled: bool,
    pub oauth_credentials: Option<EncryptedOAuthTokens>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub refresh_lease_holder: Option<Uuid>,
    pub refresh_lease_until_unix_secs: Option<u64>,
    pub last_apply_error: Option<String>,
    pub last_apply_at_unix_secs: Option<u64>,
    pub deleted_at_unix_secs: Option<u64>,
    #[serde(default)]
    pub shape_plugin: Option<UpstreamShapePluginRef>,
    pub revision: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
}
