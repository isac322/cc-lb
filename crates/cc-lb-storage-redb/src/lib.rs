#![forbid(unsafe_code)]

mod audit;
mod migration;
mod oauth;
mod quota;

use std::path::Path;
use std::sync::Arc;

use redb::{Database, TableDefinition};
use thiserror::Error;

pub use audit::AuditEntry;
pub use oauth::{oauth_key, OAuthCredentials};
pub use quota::{quota_key, BucketKind};

pub const CURRENT_SCHEMA_VERSION: u32 = 1;

pub const OAUTH_CREDENTIALS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("OAUTH_CREDENTIALS_V1");
pub const QUOTAS_BY_PRINCIPAL_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("QUOTAS_BY_PRINCIPAL_V1");
pub const AUDIT_LOG_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("AUDIT_LOG_V1");
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
    #[error("utf-8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
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
