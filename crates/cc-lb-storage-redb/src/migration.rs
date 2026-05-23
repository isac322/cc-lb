use std::sync::Arc;

use cc_lb_storage_api::BackendKind;
use redb::{Database, ReadableDatabase, ReadableTable};

use crate::{
    API_KEYS_V1, AUDIT_LOG_V1, BACKEND_KIND_KEY, CONFIG_DRAFT_V1, CONFIG_HISTORY_V1,
    CURRENT_SCHEMA_VERSION, KILLSWITCH_KEY, KILLSWITCH_V1, META_BACKEND_KIND_V1,
    OAUTH_CREDENTIALS_V1, PRINCIPAL_LIMIT_STATES_V1, QUOTAS_BY_PRINCIPAL_V1, REQUEST_EVENTS_V1,
    SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1, StorageError, USAGE_ROLLUP_CHECKPOINTS_V1,
    USAGE_ROLLUPS_V1,
};

pub(crate) fn initialize_schema(db: &Arc<Database>) -> Result<(), StorageError> {
    let write_txn = db.begin_write()?;
    {
        let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        let version = schema.get(SCHEMA_VERSION_KEY)?.map(|stored| stored.value());

        match version {
            Some(found) if found > CURRENT_SCHEMA_VERSION => {
                return Err(StorageError::UnsupportedSchemaVersion {
                    found,
                    current: CURRENT_SCHEMA_VERSION,
                });
            }
            Some(0) => return Err(StorageError::InvalidSchemaVersion(0)),
            Some(_) => {}
            None => {
                schema.insert(SCHEMA_VERSION_KEY, &CURRENT_SCHEMA_VERSION)?;
            }
        }
    }

    {
        let mut killswitch = write_txn.open_table(KILLSWITCH_V1)?;
        if killswitch.get(KILLSWITCH_KEY)?.is_none() {
            killswitch.insert(KILLSWITCH_KEY, &false)?;
        }
    }

    {
        let mut backend = write_txn.open_table(META_BACKEND_KIND_V1)?;
        let stored = backend
            .get(BACKEND_KIND_KEY)?
            .map(|stored| stored.value().to_owned());
        match stored {
            Some(stored) => ensure_backend_kind(&stored, BackendKind::Redb)?,
            None => {
                backend.insert(BACKEND_KIND_KEY, BackendKind::Redb.as_str())?;
            }
        }
    }

    {
        write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
    }
    {
        write_txn.open_table(API_KEYS_V1)?;
    }
    {
        write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
    }
    {
        write_txn.open_table(AUDIT_LOG_V1)?;
    }
    {
        write_txn.open_table(PRINCIPAL_LIMIT_STATES_V1)?;
    }
    {
        write_txn.open_table(REQUEST_EVENTS_V1)?;
    }
    {
        write_txn.open_table(USAGE_ROLLUPS_V1)?;
    }
    {
        write_txn.open_table(USAGE_ROLLUP_CHECKPOINTS_V1)?;
    }
    {
        write_txn.open_table(CONFIG_DRAFT_V1)?;
    }
    {
        write_txn.open_table(CONFIG_HISTORY_V1)?;
    }

    write_txn.commit()?;
    Ok(())
}

pub(crate) fn schema_version(db: &Arc<Database>) -> Result<u32, StorageError> {
    let read_txn = db.begin_read()?;
    let schema = read_txn.open_table(SCHEMA_VERSION_V1)?;
    let version = schema
        .get(SCHEMA_VERSION_KEY)?
        .map(|stored| stored.value())
        .unwrap_or(0);
    Ok(version)
}

pub(crate) fn killswitch_enabled(db: &Arc<Database>) -> Result<bool, StorageError> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(KILLSWITCH_V1)?;
    let enabled = table
        .get(KILLSWITCH_KEY)?
        .map(|stored| stored.value())
        .unwrap_or(false);
    Ok(enabled)
}

pub(crate) fn set_killswitch_enabled(
    db: &Arc<Database>,
    enabled: bool,
) -> Result<(), StorageError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(KILLSWITCH_V1)?;
        table.insert(KILLSWITCH_KEY, &enabled)?;
    }
    write_txn.commit()?;
    Ok(())
}

pub(crate) fn backend_kind(db: &Arc<Database>) -> Result<BackendKind, StorageError> {
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(META_BACKEND_KIND_V1)?;
    match table
        .get(BACKEND_KIND_KEY)?
        .map(|stored| stored.value().to_owned())
    {
        Some(stored) => parse_backend_kind(&stored),
        None => {
            drop(table);
            drop(read_txn);
            stamp_legacy_backend_kind(db)
        }
    }
}

pub(crate) fn initialize_backend(
    db: &Arc<Database>,
    requested: BackendKind,
) -> Result<(), StorageError> {
    let stored = backend_kind(db)?;
    if stored != requested {
        return Err(StorageError::BackendKindMismatch {
            stored,
            configured: requested,
        });
    }
    Ok(())
}

fn stamp_legacy_backend_kind(db: &Arc<Database>) -> Result<BackendKind, StorageError> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(META_BACKEND_KIND_V1)?;
        let stored = table
            .get(BACKEND_KIND_KEY)?
            .map(|stored| stored.value().to_owned());
        match stored {
            Some(stored) => ensure_backend_kind(&stored, BackendKind::Redb)?,
            None => {
                table.insert(BACKEND_KIND_KEY, BackendKind::Redb.as_str())?;
            }
        }
    }
    write_txn.commit()?;
    Ok(BackendKind::Redb)
}

fn ensure_backend_kind(stored: &str, configured: BackendKind) -> Result<(), StorageError> {
    let stored = parse_backend_kind(stored)?;
    if stored == configured {
        return Ok(());
    }
    Err(StorageError::BackendKindMismatch { stored, configured })
}

fn parse_backend_kind(stored: &str) -> Result<BackendKind, StorageError> {
    match stored {
        "redb" => Ok(BackendKind::Redb),
        "postgres" => Ok(BackendKind::Postgres),
        other => Err(StorageError::InvalidBackendKind(other.to_owned())),
    }
}
