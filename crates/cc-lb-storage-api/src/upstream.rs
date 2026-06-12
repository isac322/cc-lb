use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{StorageError, StorageResult};

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
pub struct UpstreamWarmupDialectPlugin {
    pub wasm_registry_id: Uuid,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
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
    #[serde(default)]
    pub warmup_enabled: bool,
    #[serde(default)]
    pub next_warmup_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_warmup_cycle_key: Option<i64>,
    #[serde(default)]
    pub warmup_lease_holder: Option<String>,
    #[serde(default)]
    pub warmup_lease_until_unix_secs: Option<i64>,
    #[serde(default)]
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
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
    pub warmup_enabled: bool,
    pub next_warmup_at: Option<DateTime<Utc>>,
    pub last_warmup_cycle_key: Option<i64>,
    pub warmup_lease_holder: Option<String>,
    pub warmup_lease_until_unix_secs: Option<i64>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamUpdate {
    pub name: Option<String>,
    pub base_url: Option<Url>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub warmup_enabled: Option<bool>,
    pub next_warmup_at: Option<DateTime<Utc>>,
    pub last_warmup_cycle_key: Option<i64>,
    pub warmup_lease_holder: Option<String>,
    pub warmup_lease_until_unix_secs: Option<i64>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
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
    // Mirrors claim_refresh_lease verbatim; do NOT refactor into a generic claim_lease(kind).
    async fn claim_warmup_lease(
        &self,
        upstream_id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool>;
    /// Conditional cycle-key write. Returns Ok(true) if the WHERE clause matched and the row was updated.
    /// On success, implementors atomically write the cycle key and clear the warmup lease.
    /// WHERE matches iff: id == upstream_id AND warmup_lease_holder == holder
    ///   AND warmup_lease_until_unix_secs > db_now() AND last_warmup_cycle_key IS DISTINCT FROM new_cycle_key
    ///   AND deleted_at_unix_secs IS NULL.
    async fn write_warmup_cycle_key(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool>;
    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool>;
    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>>;
    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        Ok(Utc::now().timestamp())
    }
    async fn write_warmup_next_at(
        &self,
        upstream_id: Uuid,
        holder: &str,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        let Some(record) = self.get_by_id(upstream_id).await? else {
            return Ok(false);
        };
        let now = self.warmup_now_unix_secs().await?;
        if record.deleted_at_unix_secs.is_some()
            || record.warmup_lease_holder.as_deref() != Some(holder)
            || record
                .warmup_lease_until_unix_secs
                .is_none_or(|lease_until| lease_until <= now)
        {
            return Ok(false);
        }
        match self
            .update(
                upstream_id,
                record.revision,
                UpstreamUpdate {
                    next_warmup_at: Some(next_warmup_at),
                    ..UpstreamUpdate::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::Conflict { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
