#![forbid(unsafe_code)]

mod adapter;
mod audit;
mod config_store;
mod limit_state;
mod migration;
mod oauth;
mod quota;
mod request_events;
mod usage_rollups;

use std::path::Path;
use std::sync::Arc;

use redb::{Database, TableDefinition};
use thiserror::Error;

pub use audit::AuditEntry;
pub use config_store::{ConfigDraftState, HistoryEntry, HistorySummary};
pub use limit_state::{
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, principal_limit_state_key,
};
pub use oauth::{ApiKeyRecord, IssuedKey, OAuthCredentials, oauth_key};
pub use quota::{BucketKind, quota_key};
pub use request_events::{RequestEvent, RequestEventUpstream};
pub use usage_rollups::{
    UsageRollup, UsageRollupKey, UsageRollupResolution, UsageRollupRun, usage_rollup_key,
};

pub const CURRENT_SCHEMA_VERSION: u32 = 1;

pub const OAUTH_CREDENTIALS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("OAUTH_CREDENTIALS_V1");
pub const API_KEYS_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("API_KEYS_V1");
pub const QUOTAS_BY_PRINCIPAL_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("QUOTAS_BY_PRINCIPAL_V1");
pub const AUDIT_LOG_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("AUDIT_LOG_V1");
pub const PRINCIPAL_LIMIT_STATES_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("PRINCIPAL_LIMIT_STATES_V1");
pub const REQUEST_EVENTS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("REQUEST_EVENTS_V1");
pub const USAGE_ROLLUPS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("USAGE_ROLLUPS_V1");
pub const USAGE_ROLLUP_CHECKPOINTS_V1: TableDefinition<&str, u64> =
    TableDefinition::new("USAGE_ROLLUP_CHECKPOINTS_V1");
pub const CONFIG_DRAFT_V1: TableDefinition<&str, &[u8]> = TableDefinition::new("CONFIG_DRAFT_V1");
pub const CONFIG_HISTORY_V1: TableDefinition<u64, &[u8]> =
    TableDefinition::new("CONFIG_HISTORY_V1");
pub const SCHEMA_VERSION_V1: TableDefinition<&str, u32> = TableDefinition::new("SCHEMA_VERSION_V1");
pub const KILLSWITCH_V1: TableDefinition<&str, bool> = TableDefinition::new("KILLSWITCH_V1");

pub(crate) const SCHEMA_VERSION_KEY: &str = "version";
pub(crate) const KILLSWITCH_KEY: &str = "enabled";

#[derive(Clone)]
pub struct Storage {
    pub(crate) db: Arc<Database>,
    pub(crate) master_key: [u8; 32],
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("redb database error: {0}")]
    Database(#[source] Box<redb::DatabaseError>),
    #[error("redb transaction error: {0}")]
    Transaction(#[source] Box<redb::TransactionError>),
    #[error("redb table error: {0}")]
    Table(#[source] Box<redb::TableError>),
    #[error("redb storage error: {0}")]
    RedbStorage(#[source] Box<redb::StorageError>),
    #[error("redb commit error: {0}")]
    Commit(#[source] Box<redb::CommitError>),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("aead authentication failure")]
    AeadAuthenticationFailed,
    #[error("oauth ciphertext is too short: {0} bytes")]
    CiphertextTooShort(usize),
    #[error("unsupported schema version {found}; current supported version is {current}")]
    UnsupportedSchemaVersion { found: u32, current: u32 },
    #[error("invalid schema version {0}")]
    InvalidSchemaVersion(u32),
    #[error("quota counter overflow for key {0}")]
    QuotaCounterOverflow(String),
    #[error("quota counter underflow for key {0}")]
    QuotaCounterUnderflow(String),
    #[error("invalid quota counter value for key {0}")]
    InvalidQuotaCounter(String),
    #[error("invalid quota key {0}")]
    InvalidQuotaKey(String),
    #[error("invalid audit key")]
    InvalidAuditKey,
    #[error("audit key overflow")]
    AuditKeyOverflow,
    #[error("stale draft revision; current revision is {current}")]
    StaleDraftRevision { current: u64 },
    #[error("config draft revision overflow")]
    ConfigRevisionOverflow,
    #[error("invalid request event key")]
    InvalidRequestEventKey,
    #[error("request event key overflow")]
    RequestEventKeyOverflow,
    #[error("utf-8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("random API key generation failed")]
    Random,
    #[error("unknown API key {key_id} for principal {principal_id}")]
    UnknownApiKey {
        principal_id: String,
        key_id: String,
    },
}

impl From<redb::DatabaseError> for StorageError {
    fn from(error: redb::DatabaseError) -> Self {
        Self::Database(Box::new(error))
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(error: redb::TransactionError) -> Self {
        Self::Transaction(Box::new(error))
    }
}

impl From<redb::TableError> for StorageError {
    fn from(error: redb::TableError) -> Self {
        Self::Table(Box::new(error))
    }
}

impl From<redb::StorageError> for StorageError {
    fn from(error: redb::StorageError) -> Self {
        Self::RedbStorage(Box::new(error))
    }
}

impl From<redb::CommitError> for StorageError {
    fn from(error: redb::CommitError) -> Self {
        Self::Commit(Box::new(error))
    }
}

impl Storage {
    pub fn open(path: &Path, master_key: [u8; 32]) -> Result<Self, StorageError> {
        let db = Arc::new(Database::create(path)?);
        migration::initialize_schema(&db)?;

        Ok(Self { db, master_key })
    }

    pub fn schema_version(&self) -> Result<u32, StorageError> {
        migration::schema_version(&self.db)
    }

    pub fn killswitch_enabled(&self) -> Result<bool, StorageError> {
        migration::killswitch_enabled(&self.db)
    }

    pub fn set_killswitch_enabled(&self, enabled: bool) -> Result<(), StorageError> {
        migration::set_killswitch_enabled(&self.db, enabled)
    }
}
