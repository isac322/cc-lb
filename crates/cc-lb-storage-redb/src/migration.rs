use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable};

use crate::{
    ANTHROPIC_COMPATIBILITY_KV_V1, API_KEYS_V1, AUDIT_LOG_V1, CONFIG_DRAFT_V1, CONFIG_HISTORY_V1,
    CURRENT_SCHEMA_VERSION, KEY_INDEX_BY_HASH_V1, KILLSWITCH_KEY, KILLSWITCH_V1,
    OAUTH_CREDENTIALS_V1, PLUGIN_CHAINS_V2, PRICE_CATALOG_V1, PRINCIPAL_ALLOWED_UPSTREAMS_V1,
    PRINCIPALS_V2, PRINCIPALS_V2_BY_NAME, REQUEST_EVENTS_V1, SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1,
    StorageError, UPSTREAM_RATE_LIMIT_STATE_V1, UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1, UPSTREAMS_V2, UPSTREAMS_V2_BY_NAME, WASM_BLOBS_V2,
    WASM_REGISTRY_V2,
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
            Some(found) if found < CURRENT_SCHEMA_VERSION => {
                schema.insert(SCHEMA_VERSION_KEY, &CURRENT_SCHEMA_VERSION)?;
            }
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
        write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
    }
    {
        write_txn.open_table(API_KEYS_V1)?;
    }
    {
        write_txn.open_table(KEY_INDEX_BY_HASH_V1)?;
    }
    {
        write_txn.open_table(PRICE_CATALOG_V1)?;
    }
    {
        write_txn.open_table(AUDIT_LOG_V1)?;
    }
    {
        write_txn.open_table(REQUEST_EVENTS_V1)?;
    }
    {
        write_txn.open_table(CONFIG_DRAFT_V1)?;
    }
    {
        write_txn.open_table(CONFIG_HISTORY_V1)?;
    }
    {
        write_txn.open_table(PRINCIPALS_V2)?;
    }
    {
        write_txn.open_table(PRINCIPALS_V2_BY_NAME)?;
    }
    {
        write_txn.open_table(PRINCIPAL_ALLOWED_UPSTREAMS_V1)?;
    }
    {
        write_txn.open_table(UPSTREAMS_V2)?;
    }
    {
        write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
    }
    {
        write_txn.open_table(UPSTREAM_RATE_LIMIT_STATE_V1)?;
    }
    {
        write_txn.open_table(ANTHROPIC_COMPATIBILITY_KV_V1)?;
    }
    {
        write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1)?;
    }
    {
        write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1)?;
    }
    {
        write_txn.open_table(UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1)?;
    }
    {
        write_txn.open_table(WASM_BLOBS_V2)?;
    }
    {
        write_txn.open_table(WASM_REGISTRY_V2)?;
    }
    {
        write_txn.open_table(PLUGIN_CHAINS_V2)?;
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
