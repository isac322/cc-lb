use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{StorageError, StorageResult};

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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub refresh_lease_holder: Option<Uuid>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub refresh_lease_until_unix_secs: Option<u64>,
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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub next_warmup_at: Option<DateTime<Utc>>,
    #[serde(default)]
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub last_warmup_cycle_key: Option<i64>,
    #[serde(default)]
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_holder: Option<String>,
    #[serde(default)]
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_until_unix_secs: Option<i64>,
    #[serde(default)]
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// Wall-clock unix seconds when the most recent successful warmup HTTP call
    /// completed. Distinct from `last_warmup_cycle_key`, which encodes the
    /// 5h-reset boundary used for idempotency. Used by the admin UI to render
    /// "Last cycle ran <X> ago".
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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub next_warmup_at: Option<DateTime<Utc>>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub last_warmup_cycle_key: Option<i64>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_holder: Option<String>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_until_unix_secs: Option<i64>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamUpdate {
    pub name: Option<String>,
    pub base_url: Option<Url>,
    pub enabled: Option<bool>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub oauth_token_generation: Option<u64>,
    pub warmup_enabled: Option<bool>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub next_warmup_at: Option<DateTime<Utc>>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub last_warmup_cycle_key: Option<i64>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_holder: Option<String>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub warmup_lease_until_unix_secs: Option<i64>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamLeaseKind {
    Refresh,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    Warmup,
}

impl UpstreamLeaseKind {
    pub fn as_str(self) -> &'static str {
        if matches!(self, Self::Refresh) {
            "refresh"
        } else {
            "warmup"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamStatusUpdate {
    pub last_apply_error: Option<Option<String>>,
    pub last_apply_at_unix_secs: Option<Option<u64>>,
    pub observed_spec_revision: Option<Option<u64>>,
    pub observed_api_key_secret_revision: Option<Option<u64>>,
    pub observed_oauth_token_revision: Option<Option<u64>>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub next_warmup_at: Option<Option<DateTime<Utc>>>,
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    pub last_warmup_cycle_key: Option<Option<i64>>,
    /// When `Some(value)`, write `value` to `upstream_status_v1.last_warmup_at`.
    /// `Some(None)` clears it; `None` leaves it untouched.
    pub last_warmup_at_unix_secs: Option<Option<u64>>,
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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool>;
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool>;
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool>;
    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
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
    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        Ok(self
            .get_by_id(id)
            .await?
            .map(|record| record.oauth_token_generation))
    }
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
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
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn write_warmup_cycle_key(
        &self,
        upstream_id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool>;
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool>;
    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>>;
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        Ok(Utc::now().timestamp())
    }
    #[deprecated(
        note = "removed in scheduler-migration follow-up PR; see .omo/plans/cc-lb-apalis-scheduler-migration.md Wave 8"
    )]
    #[allow(deprecated)]
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
