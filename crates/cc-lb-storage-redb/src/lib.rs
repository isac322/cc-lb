#![forbid(unsafe_code)]

mod adapter;
mod audit;
mod config_store;
use adapter::error_map;
mod key_index;
pub mod managed_keys;
mod migration;
mod oauth;
pub mod plugin_registry;
pub mod price_catalog;
mod request_events;
mod usage_rollups;

use std::path::Path;
use std::sync::Arc;

use cc_lb_storage_api::ChangeEvent;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use thiserror::Error;
use tokio::sync::broadcast;

pub use cc_lb_storage_api::RuntimeChangeNotifier;
pub use cc_lb_storage_api::types::{
    AuditEntry, ConfigDraftState, HistoryEntry, HistorySummary, OAuthCredentials, RequestEvent,
    RequestEventUpstream, UsageRollup, UsageRollupResolution, UsageRollupRun,
};
pub use oauth::{api_key_storage_key, oauth_key};
pub use plugin_registry::{RedbPluginBlobRepo, RedbPluginRegistryRepo};
pub use price_catalog::PriceSnapshot;

pub const CURRENT_SCHEMA_VERSION: u32 = 4;

pub const OAUTH_CREDENTIALS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("OAUTH_CREDENTIALS_V1");
pub const API_KEYS_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("API_KEYS_V1");
pub const KEY_INDEX_BY_HASH_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("KEY_INDEX_BY_HASH_V1");
pub const PRICE_CATALOG_V1: TableDefinition<&str, &[u8]> = TableDefinition::new("price_catalog_v1");
pub const AUDIT_LOG_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("AUDIT_LOG_V1");
pub const REQUEST_EVENTS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("REQUEST_EVENTS_V1");
pub const USAGE_ROLLUPS_V2: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("USAGE_ROLLUPS_V2");
pub const USAGE_ROLLUP_CHECKPOINTS_V1: TableDefinition<&str, u64> =
    TableDefinition::new("USAGE_ROLLUP_CHECKPOINTS_V1");
pub const SCHEMA_VERSION_V1: TableDefinition<&str, u32> = TableDefinition::new("SCHEMA_VERSION_V1");
pub const META_BACKEND_KIND_V1: TableDefinition<&str, &str> =
    TableDefinition::new("META_BACKEND_KIND_V1");
pub const KILLSWITCH_V1: TableDefinition<&str, bool> = TableDefinition::new("KILLSWITCH_V1");
pub const CONFIG_DRAFT_V1: TableDefinition<&str, &[u8]> = TableDefinition::new("CONFIG_DRAFT_V1");
pub const CONFIG_HISTORY_V1: TableDefinition<u64, &[u8]> =
    TableDefinition::new("CONFIG_HISTORY_V1");
pub const PRINCIPALS_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("principals_v2");
pub const PRINCIPALS_V2_BY_NAME: TableDefinition<&str, &[u8]> =
    TableDefinition::new("principals_v2_by_name");
pub const PRINCIPAL_ALLOWED_UPSTREAMS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("principal_allowed_upstreams_v1");
pub const UPSTREAMS_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("upstreams_v2");
pub const UPSTREAMS_V2_BY_NAME: TableDefinition<&str, &[u8]> =
    TableDefinition::new("upstreams_v2_by_name");
pub const UPSTREAM_RATE_LIMIT_STATE_V1: TableDefinition<&str, &[u8]> =
    TableDefinition::new("upstream_rate_limit_state_v1");
pub const ANTHROPIC_COMPATIBILITY_KV_V1: TableDefinition<&str, &[u8]> =
    TableDefinition::new("anthropic_compatibility_kv_v1");
pub const UPSTREAM_SUBSCRIPTION_METADATA_V1: TableDefinition<&str, &[u8]> =
    TableDefinition::new("upstream_subscription_metadata_v1");
pub const ORGANIZATION_METADATA_V1: TableDefinition<&str, &[u8]> =
    TableDefinition::new("organization_metadata_v1");
pub const UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("upstream_subscription_quota_observations_v1");
pub const UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("upstream_subscription_quota_observations_by_time_v1");
pub const UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("upstream_subscription_quota_latest_v1");
pub const PROMPT_CACHE_OBSERVATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("prompt_cache_observations_v1");
pub const WASM_BLOBS_V2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("wasm_blobs_v2");
pub const WASM_REGISTRY_V2: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("wasm_registry_v2");
pub const PLUGIN_CHAINS_V2: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("plugin_chains_v2");
pub const PLUGIN_REGISTRY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("plugin_registry");
pub const PLUGIN_REGISTRY_MARKER: TableDefinition<&str, i64> =
    TableDefinition::new("plugin_registry_marker");
pub const PLUGIN_BLOBS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("plugin_blobs");

pub(crate) const SCHEMA_VERSION_KEY: &str = "version";
pub(crate) const KILLSWITCH_KEY: &str = "enabled";
pub(crate) const BACKEND_KIND_KEY: &str = "backend_kind";

#[derive(Clone)]
pub struct Storage {
    pub(crate) db: Arc<Database>,
    pub(crate) master_key: [u8; 32],
    pub(crate) noop_change_tx: broadcast::Sender<ChangeEvent>,
}

pub type RedbStorage = Storage;
pub use managed_keys::RedbManagedKeyStore;

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
    #[error("bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("aead authentication failure")]
    AeadAuthenticationFailed,
    #[error("oauth ciphertext is too short: {0} bytes")]
    CiphertextTooShort(usize),
    #[error("unsupported schema version {found}; current supported version is {current}")]
    UnsupportedSchemaVersion { found: u32, current: u32 },
    #[error("invalid schema version {0}")]
    InvalidSchemaVersion(u32),
    #[error("invalid audit key")]
    InvalidAuditKey,
    #[error("audit key overflow")]
    AuditKeyOverflow,
    #[error("request event key overflow")]
    RequestEventKeyOverflow,
    #[error("invalid request event key")]
    InvalidRequestEventKey,
    #[error("utf-8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("random generation failed")]
    Random,
    #[error("unknown api key {principal_id}/{key_id}")]
    UnknownApiKey {
        principal_id: String,
        key_id: String,
    },
    #[error("stale config draft revision; current revision is {current}")]
    StaleDraftRevision { current: u64 },
    #[error("upstream conflict: {0}")]
    UpstreamConflict(String),
    #[error("upstream not found")]
    UpstreamNotFound,
    #[error("config revision overflow")]
    ConfigRevisionOverflow,
    #[error("stale principal revision; current revision is {current}")]
    StalePrincipalRevision { current: u64 },
    #[error("principal revision overflow")]
    PrincipalRevisionOverflow,
    #[error("principal name already exists: {name}")]
    PrincipalNameConflict { name: String },
    #[error("principal not found: {id}")]
    PrincipalNotFound { id: String },
    #[error("principal is referenced by audit entries: {id}")]
    PrincipalReferencedByAudit { id: String },
    #[error("plugin registry conflict: {message}")]
    PluginRegistryConflict { message: String },
    #[error("plugin chain conflict: {reason}")]
    PluginChainConflict {
        reason: cc_lb_storage_api::PluginChainConflictReason,
    },
    #[error("stale plugin registry revision; current revision is {current}")]
    StalePluginRegistryRevision { current: u64 },
    #[error("plugin registry revision overflow")]
    PluginRegistryRevisionOverflow,
    #[error("stale plugin chain revision; current revision is {current}")]
    StalePluginChainRevision { current: u64 },
    #[error("plugin chain revision overflow")]
    PluginChainRevisionOverflow,
    #[error("plugin registry row is referenced by plugin chain: {id}")]
    PluginRegistryReferenced { id: String },
    #[error("backend kind mismatch: stored={stored:?}, configured={configured:?}")]
    BackendKindMismatch {
        stored: cc_lb_storage_api::BackendKind,
        configured: cc_lb_storage_api::BackendKind,
    },
    #[error("invalid backend kind {0}")]
    InvalidBackendKind(String),
    #[error("invalid input: {field} {reason}")]
    InvalidInput { field: String, reason: String },
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

        Ok(Self {
            db,
            master_key,
            noop_change_tx: adapter::notifier::noop_change_sender(),
        })
    }

    pub fn open_in_memory(master_key: [u8; 32]) -> Result<Self, StorageError> {
        let db = Arc::new(
            Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?,
        );
        migration::initialize_schema(&db)?;

        Ok(Self {
            db,
            master_key,
            noop_change_tx: adapter::notifier::noop_change_sender(),
        })
    }

    pub fn begin_read(&self) -> Result<redb::ReadTransaction, StorageError> {
        Ok(self.db.begin_read()?)
    }

    pub fn begin_write(&self) -> Result<redb::WriteTransaction, StorageError> {
        Ok(self.db.begin_write()?)
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

    pub fn initialize(
        &self,
        requested: cc_lb_storage_api::BackendKind,
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(META_BACKEND_KIND_V1)?;
            let stored = table
                .get(BACKEND_KIND_KEY)?
                .map(|value| value.value().to_owned());
            match stored {
                Some(value) => {
                    let stored_kind = parse_backend_kind(&value)?;
                    if stored_kind != requested {
                        return Err(StorageError::BackendKindMismatch {
                            stored: stored_kind,
                            configured: requested,
                        });
                    }
                }
                None => {
                    table.insert(BACKEND_KIND_KEY, requested.as_str())?;
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn backend_kind(&self) -> Result<cc_lb_storage_api::BackendKind, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META_BACKEND_KIND_V1)?;
        match table.get(BACKEND_KIND_KEY)? {
            Some(value) => parse_backend_kind(value.value()),
            None => Ok(cc_lb_storage_api::BackendKind::Redb),
        }
    }
}

fn parse_backend_kind(value: &str) -> Result<cc_lb_storage_api::BackendKind, StorageError> {
    match value {
        "redb" => Ok(cc_lb_storage_api::BackendKind::Redb),
        "postgres" => Ok(cc_lb_storage_api::BackendKind::Postgres),
        other => Err(StorageError::InvalidBackendKind(other.to_owned())),
    }
}

#[cfg(any(test, feature = "crash-test-hooks"))]
pub(crate) fn crash_test_sentinel_sleep(env_name: &str) {
    if std::env::var_os(env_name).is_none() {
        return;
    }

    if let Some(control_dir) = std::env::var_os("CC_LB_CRASH_CONTROL_DIR") {
        let _ = std::fs::write(
            std::path::PathBuf::from(control_dir).join("pending_tx_started"),
            b"started",
        );
    }
    std::thread::sleep(std::time::Duration::from_secs(60));
}

#[cfg(not(any(test, feature = "crash-test-hooks")))]
#[inline]
pub(crate) fn crash_test_sentinel_sleep(_: &str) {}
