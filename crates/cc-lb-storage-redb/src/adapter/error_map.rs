use std::error::Error;

use cc_lb_storage_api::StorageError as ApiStorageError;

use crate::{CURRENT_SCHEMA_VERSION, StorageError};

pub(crate) fn map_redb_err(error: StorageError) -> ApiStorageError {
    match error {
        StorageError::Database(source) => redb_transient(source),
        StorageError::Transaction(source) => redb_transient(source),
        StorageError::Table(source) => redb_transient(source),
        StorageError::RedbStorage(source) => redb_transient(source),
        StorageError::Commit(source) => redb_transient(source),
        StorageError::Json(error) => ApiStorageError::Serialization(error),
        StorageError::BincodeEncode(error) => ApiStorageError::Corrupted {
            message: format!("redb bincode encode error: {error}"),
        },
        StorageError::BincodeDecode(error) => ApiStorageError::Corrupted {
            message: format!("redb bincode decode error: {error}"),
        },
        StorageError::AeadAuthenticationFailed => {
            ApiStorageError::Aead("redb AEAD authentication failed".to_owned())
        }
        StorageError::CiphertextTooShort(len) => {
            ApiStorageError::Aead(format!("redb ciphertext is too short: {len} bytes"))
        }
        StorageError::UnsupportedSchemaVersion { found, current } => {
            ApiStorageError::SchemaMismatch {
                found,
                expected: current,
            }
        }
        StorageError::InvalidSchemaVersion(found) => ApiStorageError::SchemaMismatch {
            found,
            expected: CURRENT_SCHEMA_VERSION,
        },
        StorageError::InvalidAuditKey => ApiStorageError::Corrupted {
            message: "redb invalid audit key".to_owned(),
        },
        StorageError::AuditKeyOverflow => ApiStorageError::Fatal {
            message: "redb audit key overflow".to_owned(),
        },
        StorageError::StaleDraftRevision { current } => ApiStorageError::Conflict {
            message: format!("stale redb config draft revision; current revision is {current}"),
        },
        StorageError::UpstreamConflict(message) => ApiStorageError::Conflict { message },
        StorageError::UpstreamNotFound => ApiStorageError::Conflict {
            message: "redb upstream not found".to_owned(),
        },
        StorageError::ConfigRevisionOverflow => ApiStorageError::Fatal {
            message: "redb config draft revision overflow".to_owned(),
        },
        StorageError::StalePrincipalRevision { current } => ApiStorageError::Conflict {
            message: format!("stale redb principal revision; current revision is {current}"),
        },
        StorageError::PrincipalRevisionOverflow => ApiStorageError::Fatal {
            message: "redb principal revision overflow".to_owned(),
        },
        StorageError::PrincipalNameConflict { name } => ApiStorageError::Conflict {
            message: format!("redb principal name already exists: {name}"),
        },
        StorageError::PrincipalReferencedByAudit { id } => ApiStorageError::Conflict {
            message: format!("redb principal {id} is referenced by audit entries"),
        },
        StorageError::PluginRegistryConflict { message } => ApiStorageError::Conflict { message },
        StorageError::StalePluginRegistryRevision { current } => ApiStorageError::Conflict {
            message: format!("stale redb plugin registry revision; current revision is {current}"),
        },
        StorageError::PluginRegistryRevisionOverflow => ApiStorageError::Fatal {
            message: "redb plugin registry revision overflow".to_owned(),
        },
        StorageError::StalePluginChainRevision { current } => ApiStorageError::Conflict {
            message: format!("stale redb plugin chain revision; current revision is {current}"),
        },
        StorageError::PluginChainRevisionOverflow => ApiStorageError::Fatal {
            message: "redb plugin chain revision overflow".to_owned(),
        },
        StorageError::PluginRegistryReferenced { id } => ApiStorageError::Conflict {
            message: format!("redb plugin registry row {id} is referenced by plugin chain"),
        },
        StorageError::InvalidRequestEventKey => ApiStorageError::Corrupted {
            message: "redb invalid request event key".to_owned(),
        },
        StorageError::RequestEventKeyOverflow => ApiStorageError::Fatal {
            message: "redb request event key overflow".to_owned(),
        },
        StorageError::Utf8(error) => ApiStorageError::Corrupted {
            message: format!("redb utf-8 decoding error: {error}"),
        },
        StorageError::Random => ApiStorageError::Fatal {
            message: "redb random API key generation failed".to_owned(),
        },
        StorageError::UnknownApiKey {
            principal_id,
            key_id,
        } => ApiStorageError::Conflict {
            message: format!("unknown redb API key {key_id} for principal {principal_id}"),
        },
        StorageError::BackendKindMismatch { stored, configured } => {
            ApiStorageError::BackendKindMismatch { stored, configured }
        }
        StorageError::InvalidBackendKind(kind) => ApiStorageError::Corrupted {
            message: format!("redb invalid backend kind {kind}"),
        },
    }
}

pub(crate) fn map_join_err(error: tokio::task::JoinError) -> ApiStorageError {
    ApiStorageError::Fatal {
        message: format!("redb blocking task failed: {error}"),
    }
}

fn redb_transient<E>(source: Box<E>) -> ApiStorageError
where
    E: Error + Send + Sync + 'static,
{
    let source: Box<dyn Error + Send + Sync + 'static> = source;
    ApiStorageError::Transient {
        retryable: true,
        source,
    }
}
