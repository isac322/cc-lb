use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::StorageResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    AnthropicApiKey,
    AnthropicOauth,
}

impl UpstreamKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamStatus {
    Active,
    Disabled,
    Error,
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
    pub revision: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
}

impl UpstreamRecord {
    pub fn status(&self) -> UpstreamStatus {
        if !self.enabled {
            UpstreamStatus::Disabled
        } else if self.last_apply_error.is_some() {
            UpstreamStatus::Error
        } else {
            UpstreamStatus::Active
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamCreate {
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    pub api_key_ciphertext: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamUpdate {
    pub name: Option<String>,
    pub base_url: Option<Url>,
    pub api_key_ciphertext: Option<Vec<u8>>,
}

#[async_trait]
pub trait UpstreamStore: Send + Sync {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord>;
    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>>;
    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>>;
    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>>;
    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord>;
    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord>;
    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool>;
    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()>;
    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()>;
    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()>;
    async fn hard_delete(&self, id: Uuid) -> StorageResult<()>;
}
