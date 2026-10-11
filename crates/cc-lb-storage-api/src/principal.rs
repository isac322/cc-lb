use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CacheKeepaliveConfig;
use crate::StorageResult;
pub use crate::limits::{Limit, LimitKind};
pub use crate::prompt_cache_mode::PromptCacheMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Machine,
    Human,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrincipalRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: PrincipalKind,
    pub allowed_models: Vec<String>,
    #[serde(default)]
    pub allowed_upstreams: Vec<Uuid>,
    pub default_limits: Vec<Limit>,
    pub enabled: bool,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    pub router_terminal_strategy: cc_lb_domain::TerminalStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_keepalive: Option<CacheKeepaliveConfig>,
    /// Effective prompt-cache policy. Stored non-null; rows written before the
    /// column existed read back as [`PromptCacheMode::PreserveClient`].
    #[serde(default)]
    pub prompt_cache_mode: PromptCacheMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrincipalCreate {
    pub name: String,
    pub kind: PrincipalKind,
    pub allowed_models: Vec<String>,
    #[serde(default)]
    pub allowed_upstreams: Vec<Uuid>,
    pub default_limits: Vec<Limit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_keepalive: Option<CacheKeepaliveConfig>,
    /// Omitted creates default to [`PromptCacheMode::PreserveClient`].
    #[serde(default)]
    pub prompt_cache_mode: PromptCacheMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PrincipalUpdate {
    pub name: Option<String>,
    pub allowed_models: Option<Vec<String>>,
    pub allowed_upstreams: Option<Vec<Uuid>>,
    pub default_limits: Option<Vec<Limit>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router_terminal_strategy: Option<cc_lb_domain::TerminalStrategy>,
    /// When `Some`, replaces the principal's cache_keepalive config
    /// (including `Some(None)` to clear it). When `None`, the existing
    /// value is preserved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_keepalive: Option<Option<CacheKeepaliveConfig>>,
    /// When `Some`, replaces the principal's prompt-cache policy. When `None`,
    /// the existing mode is preserved — including on updates that only touch
    /// `cache_keepalive`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_mode: Option<PromptCacheMode>,
}

#[async_trait]
pub trait PrincipalStore: Send + Sync {
    async fn create(
        &self,
        input: PrincipalCreate,
        now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord>;

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>>;
    /// Returns the live (non-soft-deleted) record with this name.
    ///
    /// Soft-deleted rows are never returned; look them up by id with [`Self::get_by_id`].
    /// Names are unique only among live rows, so a soft-deleted name is reusable.
    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>>;

    async fn list(
        &self,
        offset: usize,
        limit: usize,
        include_deleted: bool,
    ) -> StorageResult<Vec<PrincipalRecord>>;

    /// Applies `update` when `expected_revision` matches, returning `None` for
    /// an unknown id and `Conflict` for a stale revision.
    ///
    /// Setting [`PromptCacheMode::OneHourNoKeepalive`] takes effect in one
    /// transaction with the mode write: every active keep-alive session of that
    /// principal is retired with
    /// [`CacheKeepaliveTerminalReason::Cancelled`](crate::CacheKeepaliveTerminalReason::Cancelled),
    /// so a request admitted before the update cannot recreate a session
    /// afterwards. Retirement matches every identity an admitted request could
    /// have recorded — the immutable UUID, the name in force before the update,
    /// and the new name — because a rename and a mode switch can arrive in one
    /// update (see [`crate::principal_identity_refs`]). Already-enqueued
    /// scheduler jobs become no-ops through their existing generation and status
    /// fences; no job payload is rewritten.
    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PrincipalUpdate,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>>;

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>>;

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>>;

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool>;
}
