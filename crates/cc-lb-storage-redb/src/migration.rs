use std::sync::Arc;

use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

mod legacy_upstreams;

use crate::{
    ANTHROPIC_COMPATIBILITY_KV_V1, API_KEYS_V1, AUDIT_LOG_V1, CONFIG_DRAFT_V1, CONFIG_HISTORY_V1,
    CURRENT_SCHEMA_VERSION, KEY_INDEX_BY_HASH_V1, KILLSWITCH_KEY, KILLSWITCH_V1,
    OAUTH_CREDENTIALS_V1, ORGANIZATION_METADATA_V1, PLUGIN_CHAINS_V2, PRICE_CATALOG_V1,
    PRINCIPAL_ALLOWED_UPSTREAMS_V1, PRINCIPALS_V2, PRINCIPALS_V2_BY_NAME,
    PROMPT_CACHE_OBSERVATIONS, REQUEST_EVENTS_V1, SCHEMA_VERSION_KEY, SCHEMA_VERSION_V1,
    StorageError, UPSTREAM_RATE_LIMIT_STATE_V1, UPSTREAM_SUBSCRIPTION_METADATA_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_LATEST_V1, UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_BY_TIME_V1,
    UPSTREAM_SUBSCRIPTION_QUOTA_OBSERVATIONS_V1, UPSTREAMS_V2, UPSTREAMS_V2_BY_NAME,
    USAGE_ROLLUP_CHECKPOINTS_V1, USAGE_ROLLUPS_V2, WASM_BLOBS_V2, WASM_REGISTRY_V2,
};

const USAGE_ROLLUPS_V1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("USAGE_ROLLUPS_V1");
const REQUEST_EVENT_CHECKPOINT_KEY: &str = "request_events_v1_high_water";

pub(crate) fn initialize_schema(db: &Arc<Database>) -> Result<(), StorageError> {
    let write_txn = db.begin_write()?;
    let stored_version;
    let reset_usage_rollups;
    let old_version;
    {
        let schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        stored_version = schema.get(SCHEMA_VERSION_KEY)?.map(|stored| stored.value());
        old_version = stored_version;

        reset_usage_rollups = stored_version.is_none_or(|found| found < CURRENT_SCHEMA_VERSION);
        match stored_version {
            Some(found) if found > CURRENT_SCHEMA_VERSION => {
                return Err(StorageError::UnsupportedSchemaVersion {
                    found,
                    current: CURRENT_SCHEMA_VERSION,
                });
            }
            Some(0) => return Err(StorageError::InvalidSchemaVersion(0)),
            Some(_) => {}
            None => {}
        }
    }
    if reset_usage_rollups {
        reset_usage_rollups_v1(&write_txn)?;
    }
    // Schema v4 -> v5: prompt_cache_observations key changed to include canonical_model_id.
    // Ephemeral data (5min/1h TTL) is acceptable to lose on upgrade.
    if let Some(version) = old_version {
        if version < 5 && version >= 4 {
            reset_prompt_cache_observations_v4_to_v5(&write_txn)?;
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
        write_txn.open_table(USAGE_ROLLUPS_V2)?;
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
        write_txn.open_table(UPSTREAM_SUBSCRIPTION_METADATA_V1)?;
    }
    {
        write_txn.open_table(ORGANIZATION_METADATA_V1)?;
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
        write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
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
    if stored_version.is_some_and(|version| version < 4) {
        migrate_upstreams_v3_to_v4(&write_txn)?;
    }
    {
        let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        schema.insert(SCHEMA_VERSION_KEY, &CURRENT_SCHEMA_VERSION)?;
    }

    write_txn.commit()?;
    Ok(())
}

fn migrate_upstreams_v3_to_v4(write_txn: &redb::WriteTransaction) -> Result<(), StorageError> {
    let rows = {
        let table = write_txn.open_table(UPSTREAMS_V2)?;
        table
            .iter()?
            .map(|row| {
                let (key, value) = row?;
                Ok((key.value().to_vec(), value.value().to_vec()))
            })
            .collect::<Result<Vec<_>, StorageError>>()?
    };

    let mut rewritten = Vec::with_capacity(rows.len());
    let mut dropped_keys = Vec::new();
    let mut dropped_names = Vec::new();

    for (key, value) in rows {
        let legacy: legacy_upstreams::UpstreamRecord = serde_json::from_slice(&value)?;
        if legacy.kind == legacy_upstreams::UpstreamKind::Custom {
            if legacy.deleted_at_unix_secs.is_some() {
                dropped_keys.push(key);
                dropped_names.push(legacy.name);
                continue;
            } else {
                let record = UpstreamRecord {
                    id: legacy.id,
                    name: legacy.name,
                    kind: UpstreamKind::AnthropicApiKey,
                    base_url: legacy.base_url,
                    enabled: legacy.enabled,
                    oauth_credentials: legacy.oauth_credentials,
                    api_key_ciphertext: legacy.api_key_ciphertext,
                    refresh_lease_holder: legacy.refresh_lease_holder,
                    refresh_lease_until_unix_secs: legacy.refresh_lease_until_unix_secs,
                    last_apply_error: legacy.last_apply_error,
                    last_apply_at_unix_secs: legacy.last_apply_at_unix_secs,
                    deleted_at_unix_secs: legacy.deleted_at_unix_secs,
                    revision: legacy.revision,
                    created_at_unix_secs: legacy.created_at_unix_secs,
                    updated_at_unix_secs: legacy.updated_at_unix_secs,
                };
                rewritten.push((key, serde_json::to_vec(&record)?));
            }
        } else {
            let record = UpstreamRecord {
                id: legacy.id,
                name: legacy.name,
                kind: match legacy.kind {
                    legacy_upstreams::UpstreamKind::AnthropicApiKey => {
                        UpstreamKind::AnthropicApiKey
                    }
                    legacy_upstreams::UpstreamKind::AnthropicOauth => UpstreamKind::AnthropicOauth,
                    legacy_upstreams::UpstreamKind::Custom => unreachable!(),
                },
                base_url: legacy.base_url,
                enabled: legacy.enabled,
                oauth_credentials: legacy.oauth_credentials,
                api_key_ciphertext: legacy.api_key_ciphertext,
                refresh_lease_holder: legacy.refresh_lease_holder,
                refresh_lease_until_unix_secs: legacy.refresh_lease_until_unix_secs,
                last_apply_error: legacy.last_apply_error,
                last_apply_at_unix_secs: legacy.last_apply_at_unix_secs,
                deleted_at_unix_secs: legacy.deleted_at_unix_secs,
                revision: legacy.revision,
                created_at_unix_secs: legacy.created_at_unix_secs,
                updated_at_unix_secs: legacy.updated_at_unix_secs,
            };
            rewritten.push((key, serde_json::to_vec(&record)?));
        }
    }

    let mut table = write_txn.open_table(UPSTREAMS_V2)?;
    for key in &dropped_keys {
        let _ = table.remove(key.as_slice());
    }
    for (key, value) in rewritten {
        table.insert(key.as_slice(), value.as_slice())?;
    }

    let mut by_name_table = write_txn.open_table(UPSTREAMS_V2_BY_NAME)?;
    for name in dropped_names {
        let _ = by_name_table.remove(name.as_str());
    }

    Ok(())
}

fn reset_usage_rollups_v1(write_txn: &redb::WriteTransaction) -> Result<(), StorageError> {
    let keys = {
        let table = write_txn.open_table(USAGE_ROLLUPS_V1)?;
        table
            .iter()?
            .map(|row| row.map(|(key, _)| key.value().to_vec()))
            .collect::<Result<Vec<_>, _>>()?
    };
    {
        let mut table = write_txn.open_table(USAGE_ROLLUPS_V1)?;
        for key in keys {
            table.remove(key.as_slice())?;
        }
    }
    {
        let mut checkpoints = write_txn.open_table(USAGE_ROLLUP_CHECKPOINTS_V1)?;
        checkpoints.remove(REQUEST_EVENT_CHECKPOINT_KEY)?;
    }
    Ok(())
}

fn reset_prompt_cache_observations_v4_to_v5(
    write_txn: &redb::WriteTransaction,
) -> Result<(), StorageError> {
    let keys = {
        let table = write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
        table
            .iter()?
            .map(|row| row.map(|(key, _)| key.value().to_vec()))
            .collect::<Result<Vec<_>, _>>()?
    };
    {
        let mut table = write_txn.open_table(PROMPT_CACHE_OBSERVATIONS)?;
        for key in keys {
            table.remove(key.as_slice())?;
        }
    }
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
