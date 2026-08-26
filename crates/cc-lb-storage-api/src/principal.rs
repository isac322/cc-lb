use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CacheKeepaliveConfig;
use crate::StorageResult;
pub use crate::limits::{Limit, LimitKind};

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
    pub last_apply_error: Option<String>,
    pub last_apply_at_unix_secs: Option<u64>,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    #[serde(default)]
    pub router_terminal_strategy: cc_lb_domain::TerminalStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_keepalive: Option<CacheKeepaliveConfig>,
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

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>>;
}
