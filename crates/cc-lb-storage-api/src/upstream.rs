use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::StorageResult;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    #[default]
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
pub struct UpstreamWarmupDialectPlugin {
    pub wasm_registry_id: Uuid,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    pub enabled: bool,
    pub oauth_credentials: Option<EncryptedOAuthTokens>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub last_apply_error: Option<String>,
    pub last_apply_at_unix_secs: Option<u64>,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
    pub oauth_token_generation: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    #[serde(default)]
    pub warmup_enabled: bool,
    #[serde(default)]
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// Wall-clock unix seconds when the most recent successful warmup HTTP call
    /// completed. Used by the admin UI to render "Last cycle ran <X> ago".
    #[serde(default)]
    pub last_warmup_at_unix_secs: Option<u64>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpstreamCreate {
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub oauth_token_generation: Option<u64>,
    pub warmup_enabled: bool,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamUpdate {
    pub name: Option<String>,
    /// `None` preserves the override; `Some(None)` clears it;
    /// `Some(Some(url))` sets a new override.
    pub base_url: Option<Option<Url>>,
    pub enabled: Option<bool>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub oauth_token_generation: Option<u64>,
    pub warmup_enabled: Option<bool>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamStatusUpdate {
    pub last_apply_error: Option<Option<String>>,
    pub last_apply_at_unix_secs: Option<Option<u64>>,
    pub observed_spec_revision: Option<Option<u64>>,
    pub observed_api_key_secret_revision: Option<Option<u64>>,
    pub observed_oauth_token_revision: Option<Option<u64>>,
    /// When `Some(value)`, write `value` to `upstream_status_v1.last_warmup_at`.
    /// `Some(None)` clears it; `None` leaves it untouched.
    pub last_warmup_at_unix_secs: Option<Option<u64>>,
}

#[async_trait]
pub trait UpstreamStore: Send + Sync {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord>;
    /// Returns the live (non-soft-deleted) record with this name.
    ///
    /// Soft-deleted rows are never returned; look them up by id with [`Self::get_by_id`].
    /// Names are unique only among live rows, so a soft-deleted name is reusable.
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
    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord>;
    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord>;
    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()>;
    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        Ok(self
            .get_by_id(id)
            .await?
            .map(|record| record.oauth_token_generation))
    }
    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()>;
    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()>;
    async fn hard_delete(&self, id: Uuid) -> StorageResult<()>;
    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>>;
}
