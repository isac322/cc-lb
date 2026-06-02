use std::{error::Error, fmt};

use thiserror::Error;
use uuid::Uuid;

use crate::BackendKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginChainConflictReason {
    InvalidOrderGap,
    SlotIsSingleton { existing_entry_id: Uuid },
}

impl fmt::Display for PluginChainConflictReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOrderGap => formatter.write_str("invalid_order_gap"),
            Self::SlotIsSingleton { .. } => formatter.write_str("slot_is_singleton"),
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("transient (retryable={retryable}): {source}")]
    Transient {
        retryable: bool,
        #[source]
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    #[error("conflict: {message}")]
    Conflict { message: String },
    #[error("schema mismatch: found={found}, expected={expected}")]
    SchemaMismatch { found: u32, expected: u32 },
    #[error("unavailable: {message}")]
    Unavailable { message: String },
    #[error("corrupted: {message}")]
    Corrupted { message: String },
    #[error("fatal: {message}")]
    Fatal { message: String },
    #[error("backend kind mismatch: stored={stored:?}, configured={configured:?}")]
    BackendKindMismatch {
        stored: BackendKind,
        configured: BackendKind,
    },
    #[error("serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("aead: {0}")]
    Aead(String),
    #[error("invalid input: {field} {reason}")]
    InvalidInput { field: String, reason: String },
    #[error("plugin registry conflict: {message}")]
    PluginRegistryConflict { message: String },
    #[error("plugin registry row is referenced by plugin chain: {id}")]
    PluginRegistryReferenced { id: String },
    #[error("stale plugin registry revision; current revision is {current}")]
    StalePluginRegistryRevision { current: u64 },
    #[error("stale plugin chain revision; current revision is {current}")]
    StalePluginChainRevision { current: u64 },
    #[error("plugin chain conflict: {reason}")]
    PluginChainConflict { reason: PluginChainConflictReason },
    #[error("principal not found: {id}")]
    PrincipalNotFound { id: String },
}

impl StorageError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transient {
                retryable: true,
                ..
            } | Self::Unavailable { .. }
        )
    }
}

pub type StorageResult<T> = Result<T, StorageError>;
