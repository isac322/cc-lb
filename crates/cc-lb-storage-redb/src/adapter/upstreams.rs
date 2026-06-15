use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::upstream::{UpstreamLeaseKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    StorageError as ApiStorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore,
    UpstreamUpdate, validate_identifier,
};
use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::{
    RedbStorage, UPSTREAM_API_KEY_SECRET_V1, UPSTREAM_LEASE_V1, UPSTREAM_OAUTH_TOKEN_V1,
    UPSTREAM_SPEC_V1, UPSTREAM_SPEC_V1_BY_NAME, UPSTREAM_STATUS_V1,
};

use crate::error_map::{map_join_err, map_redb_err};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UpstreamSpecRecord {
    id: Uuid,
    name: String,
    kind: cc_lb_storage_api::upstream::UpstreamKind,
    base_url: Option<url::Url>,
    enabled: bool,
    warmup_enabled: bool,
    warmup_dialect_plugin: Option<cc_lb_storage_api::upstream::UpstreamWarmupDialectPlugin>,
    spec_revision: u64,
    created_at_unix_secs: u64,
    updated_at_unix_secs: u64,
    deleted_at_unix_secs: Option<u64>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UpstreamApiKeySecretRecord {
    api_key_ciphertext: Option<Vec<u8>>,
    secret_revision: u64,
    created_at_unix_secs: u64,
    updated_at_unix_secs: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UpstreamOauthTokenRecord {
    oauth_credentials: EncryptedOAuthTokens,
    token_revision: u64,
    refreshed_at_unix_secs: Option<u64>,
    created_at_unix_secs: u64,
    updated_at_unix_secs: u64,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct UpstreamStatusRecord {
    last_apply_error: Option<String>,
    last_apply_at_unix_secs: Option<u64>,
    observed_spec_revision: Option<u64>,
    observed_api_key_secret_revision: Option<u64>,
    observed_oauth_token_revision: Option<u64>,
    next_warmup_at: Option<DateTime<Utc>>,
    last_warmup_cycle_key: Option<i64>,
    updated_at_unix_secs: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UpstreamLeaseRecord {
    holder: String,
    until_unix_secs: i64,
    updated_at_unix_secs: u64,
}

#[async_trait]
impl UpstreamStore for RedbStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("name", &create.name)?;
        let storage = self.clone();
        tokio::task::spawn_blocking(move || create_split_sync(&storage, create))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        validate_identifier("name", name)?;
        let storage = self.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || get_split_by_name_sync(&storage, &name))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || get_split_by_id_sync(&storage, id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || list_split_sync(&storage, after, limit))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        if let Some(name) = &update.name {
            validate_identifier("name", name)?;
        }
        let storage = self.clone();
        tokio::task::spawn_blocking(move || update_split_sync(&storage, id, expected_revision, update))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            update_split_spec_sync(
                &storage,
                id,
                expected_revision,
                UpstreamUpdate {
                    enabled: Some(enabled),
                    ..UpstreamUpdate::default()
                },
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || update_split_spec_sync(&storage, id, expected_revision, update))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || update_split_api_key_secret_sync(&storage, id, api_key_ciphertext))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || update_split_oauth_token_sync(&storage, id, tokens))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || set_split_status_sync(&storage, id, status))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || claim_split_lease_sync(&storage, id, lease_kind, &holder, ttl_secs))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || renew_split_lease_sync(&storage, id, lease_kind, &holder, ttl_secs))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || release_split_lease_sync(&storage, id, lease_kind, &holder))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            ensure_split_spec_revision_sync(&storage, id, expected_revision)?;
            update_split_oauth_token_sync(&storage, id, tokens)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            let ttl_secs =
                i64::try_from(ttl_secs).map_err(|_| crate::StorageError::InvalidInput {
                    field: "ttl_secs".to_owned(),
                    reason: "refresh lease ttl cannot be represented as i64".to_owned(),
                })?;
            claim_split_lease_sync(
                &storage,
                id,
                UpstreamLeaseKind::Refresh,
                &holder.to_string(),
                ttl_secs,
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            if !release_split_lease_sync(
                &storage,
                id,
                UpstreamLeaseKind::Refresh,
                &holder.to_string(),
            )? {
                return Err(crate::StorageError::UpstreamConflict(
                    "refresh lease holder mismatch".to_owned(),
                ));
            }
            update_split_oauth_token_sync(&storage, id, tokens)?;
            set_split_status_sync(
                &storage,
                id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(None),
                    ..UpstreamStatusUpdate::default()
                },
            )?;
            get_split_by_id_sync(&storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            if !release_split_lease_sync(
                &storage,
                id,
                UpstreamLeaseKind::Refresh,
                &holder.to_string(),
            )? {
                return Err(crate::StorageError::UpstreamConflict(
                    "refresh lease holder mismatch".to_owned(),
                ));
            }
            set_split_status_sync(
                &storage,
                id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(Some(reason)),
                    last_apply_at_unix_secs: Some(Some(now_unix_secs())),
                    ..UpstreamStatusUpdate::default()
                },
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            set_split_status_sync(
                &storage,
                id,
                UpstreamStatusUpdate {
                    last_apply_error: Some(error),
                    last_apply_at_unix_secs: Some(Some(now_unix_secs())),
                    ..UpstreamStatusUpdate::default()
                },
            )
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || soft_delete_split_sync(&storage, id, expected_revision))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || hard_delete_split_sync(&storage, id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn claim_warmup_lease(
        &self,
        id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            claim_split_lease_sync(&storage, id, UpstreamLeaseKind::Warmup, &holder, ttl_secs)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn write_warmup_cycle_key(
        &self,
        id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            write_split_warmup_cycle_key_sync(&storage, id, &holder, new_cycle_key, next_warmup_at)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            release_split_lease_sync(&storage, id, UpstreamLeaseKind::Warmup, &holder)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        i64::try_from(now_unix_secs()).map_err(|_| ApiStorageError::Fatal {
            message: "current unix timestamp cannot be represented as i64".to_owned(),
        })
    }

    async fn write_warmup_next_at(
        &self,
        id: Uuid,
        holder: &str,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        let storage = self.clone();
        let holder = holder.to_owned();
        tokio::task::spawn_blocking(move || {
            write_split_warmup_next_at_sync(&storage, id, &holder, next_warmup_at)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let storage = self.clone();
        tokio::task::spawn_blocking(move || {
            clear_split_warmup_dialect_plugin_sync(&storage, id, expected_revision)
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

fn create_split_sync(
    storage: &RedbStorage,
    create: UpstreamCreate,
) -> Result<UpstreamRecord, crate::StorageError> {
    let now = now_unix_secs();
    let spec = UpstreamSpecRecord {
        id: Uuid::new_v4(),
        name: create.name,
        kind: create.kind,
        base_url: create.base_url,
        enabled: true,
        warmup_enabled: create.warmup_enabled,
        warmup_dialect_plugin: create.warmup_dialect_plugin,
        spec_revision: 1,
        created_at_unix_secs: now,
        updated_at_unix_secs: now,
        deleted_at_unix_secs: None,
    };
    let id = spec.id;
    let write_txn = storage.db.begin_write()?;
    {
        let mut by_name = write_txn.open_table(UPSTREAM_SPEC_V1_BY_NAME)?;
        if by_name.get(spec.name.as_str())?.is_some() {
            return Err(crate::StorageError::UpstreamConflict(
                "upstream name already exists".to_owned(),
            ));
        }
        let mut specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        specs.insert(id.as_bytes().as_slice(), serde_json::to_vec(&spec)?.as_slice())?;
        by_name.insert(spec.name.as_str(), id.as_bytes().as_slice())?;
    }
    if let Some(api_key_ciphertext) = create.api_key_ciphertext {
        let secret = UpstreamApiKeySecretRecord {
            api_key_ciphertext: Some(api_key_ciphertext),
            secret_revision: 1,
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
        };
        let mut table = write_txn.open_table(UPSTREAM_API_KEY_SECRET_V1)?;
        table.insert(id.as_bytes().as_slice(), serde_json::to_vec(&secret)?.as_slice())?;
    }
    if create.next_warmup_at.is_some() || create.last_warmup_cycle_key.is_some() {
        let status = UpstreamStatusRecord {
            next_warmup_at: create.next_warmup_at,
            last_warmup_cycle_key: create.last_warmup_cycle_key,
            updated_at_unix_secs: now,
            ..UpstreamStatusRecord::default()
        };
        let mut table = write_txn.open_table(UPSTREAM_STATUS_V1)?;
        table.insert(id.as_bytes().as_slice(), serde_json::to_vec(&status)?.as_slice())?;
    }
    if let (Some(holder), Some(until_unix_secs)) = (
        create.warmup_lease_holder,
        create.warmup_lease_until_unix_secs,
    ) {
        let lease = UpstreamLeaseRecord {
            holder,
            until_unix_secs,
            updated_at_unix_secs: now,
        };
        let key = lease_key(id, UpstreamLeaseKind::Warmup);
        let mut table = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        table.insert(key.as_str(), serde_json::to_vec(&lease)?.as_slice())?;
    }
    write_txn.commit()?;
    get_split_by_id_sync(storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
}

fn get_split_by_name_sync(
    storage: &RedbStorage,
    name: &str,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let read_txn = storage.db.begin_read()?;
    let by_name = read_txn.open_table(UPSTREAM_SPEC_V1_BY_NAME)?;
    let Some(id) = by_name.get(name)?.map(|value| value.value().to_vec()) else {
        return Ok(None);
    };
    compose_split_from_txn(&read_txn, id.as_slice())
}

fn get_split_by_id_sync(
    storage: &RedbStorage,
    id: Uuid,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let read_txn = storage.db.begin_read()?;
    compose_split_from_txn(&read_txn, id.as_bytes().as_slice())
}

fn list_split_sync(
    storage: &RedbStorage,
    after: Option<Uuid>,
    limit: usize,
) -> Result<Vec<UpstreamRecord>, crate::StorageError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let read_txn = storage.db.begin_read()?;
    let specs = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let mut ids = Vec::new();
    for row in specs.iter()? {
        let (key, value) = row?;
        let spec: UpstreamSpecRecord = serde_json::from_slice(value.value())?;
        if spec.deleted_at_unix_secs.is_none()
            && after.is_none_or(|after| spec.id.as_bytes() > after.as_bytes())
        {
            ids.push(key.value().to_vec());
        }
    }
    ids.sort();
    ids.truncate(limit);
    ids.into_iter()
        .map(|id| compose_split_from_txn(&read_txn, id.as_slice()))
        .filter_map(|result| match result {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn compose_split_from_txn(
    read_txn: &redb::ReadTransaction,
    id: &[u8],
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let specs = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let Some(spec_guard) = specs.get(id)? else {
        return Ok(None);
    };
    let spec: UpstreamSpecRecord = serde_json::from_slice(spec_guard.value())?;
    let secret = read_optional_json::<UpstreamApiKeySecretRecord>(read_txn, UPSTREAM_API_KEY_SECRET_V1, id)?;
    let token = read_optional_json::<UpstreamOauthTokenRecord>(read_txn, UPSTREAM_OAUTH_TOKEN_V1, id)?;
    let status = read_optional_json::<UpstreamStatusRecord>(read_txn, UPSTREAM_STATUS_V1, id)?;
    let refresh_lease = read_lease(read_txn, spec.id, UpstreamLeaseKind::Refresh)?;
    let warmup_lease = read_lease(read_txn, spec.id, UpstreamLeaseKind::Warmup)?;
    let refresh_holder = refresh_lease
        .as_ref()
        .map(|lease| Uuid::parse_str(&lease.holder))
        .transpose()
        .map_err(|error| crate::StorageError::UpstreamConflict(error.to_string()))?;
    Ok(Some(UpstreamRecord {
        id: spec.id,
        name: spec.name,
        kind: spec.kind,
        base_url: spec.base_url,
        enabled: spec.enabled,
        oauth_credentials: token.map(|token| token.oauth_credentials),
        api_key_ciphertext: secret.and_then(|secret| secret.api_key_ciphertext),
        refresh_lease_holder: refresh_holder,
        refresh_lease_until_unix_secs: refresh_lease
            .as_ref()
            .and_then(|lease| u64::try_from(lease.until_unix_secs).ok()),
        last_apply_error: status.as_ref().and_then(|status| status.last_apply_error.clone()),
        last_apply_at_unix_secs: status.as_ref().and_then(|status| status.last_apply_at_unix_secs),
        deleted_at_unix_secs: spec.deleted_at_unix_secs,
        revision: spec.spec_revision,
        created_at_unix_secs: spec.created_at_unix_secs,
        updated_at_unix_secs: spec.updated_at_unix_secs,
        warmup_enabled: spec.warmup_enabled,
        next_warmup_at: status.as_ref().and_then(|status| status.next_warmup_at),
        last_warmup_cycle_key: status.as_ref().and_then(|status| status.last_warmup_cycle_key),
        warmup_lease_holder: warmup_lease.as_ref().map(|lease| lease.holder.clone()),
        warmup_lease_until_unix_secs: warmup_lease.as_ref().map(|lease| lease.until_unix_secs),
        warmup_dialect_plugin: spec.warmup_dialect_plugin,
    }))
}

fn read_optional_json<T: serde::de::DeserializeOwned>(
    read_txn: &redb::ReadTransaction,
    definition: redb::TableDefinition<&[u8], &[u8]>,
    key: &[u8],
) -> Result<Option<T>, crate::StorageError> {
    let table = read_txn.open_table(definition)?;
    table
        .get(key)?
        .map(|value| serde_json::from_slice(value.value()).map_err(Into::into))
        .transpose()
}

fn read_lease(
    read_txn: &redb::ReadTransaction,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
) -> Result<Option<UpstreamLeaseRecord>, crate::StorageError> {
    let key = lease_key(id, lease_kind);
    let table = read_txn.open_table(UPSTREAM_LEASE_V1)?;
    table
        .get(key.as_str())?
        .map(|value| serde_json::from_slice(value.value()).map_err(Into::into))
        .transpose()
}

fn update_split_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
    mut update: UpstreamUpdate,
) -> Result<UpstreamRecord, crate::StorageError> {
    let has_spec_update = update.name.is_some()
        || update.base_url.is_some()
        || update.enabled.is_some()
        || update.warmup_enabled.is_some()
        || update.warmup_dialect_plugin.is_some();
    let api_key_ciphertext = update.api_key_ciphertext.take();
    let status = UpstreamStatusUpdate {
        next_warmup_at: update.next_warmup_at.take().map(Some),
        last_warmup_cycle_key: update.last_warmup_cycle_key.take().map(Some),
        ..UpstreamStatusUpdate::default()
    };
    let has_status_update = status.next_warmup_at.is_some() || status.last_warmup_cycle_key.is_some();
    if has_spec_update {
        update_split_spec_sync(storage, id, expected_revision, update)?;
    } else {
        ensure_split_spec_revision_sync(storage, id, expected_revision)?;
    }
    if let Some(ciphertext) = api_key_ciphertext {
        update_split_api_key_secret_sync(storage, id, Some(ciphertext))?;
    }
    if has_status_update {
        set_split_status_sync(storage, id, status)?;
    }
    get_split_by_id_sync(storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
}

fn update_split_spec_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> Result<UpstreamRecord, crate::StorageError> {
    if let Some(name) = &update.name {
        validate_identifier("name", name).map_err(api_error_as_redb)?;
    }
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    {
        let mut specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        let stored = specs
            .get(id.as_bytes().as_slice())?
            .ok_or(crate::StorageError::UpstreamNotFound)?
            .value()
            .to_vec();
        let mut spec: UpstreamSpecRecord = serde_json::from_slice(&stored)?;
        if spec.deleted_at_unix_secs.is_some() {
            return Err(crate::StorageError::UpstreamNotFound);
        }
        if spec.spec_revision != expected_revision {
            return Err(crate::StorageError::UpstreamConflict(
                "stale upstream revision".to_owned(),
            ));
        }
        let old_name = spec.name.clone();
        if let Some(name) = update.name {
            spec.name = name;
        }
        if update.base_url.is_some() {
            spec.base_url = update.base_url;
        }
        if let Some(enabled) = update.enabled {
            spec.enabled = enabled;
        }
        if let Some(warmup_enabled) = update.warmup_enabled {
            spec.warmup_enabled = warmup_enabled;
        }
        if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
            spec.warmup_dialect_plugin = Some(warmup_dialect_plugin);
        }
        spec.spec_revision = spec.spec_revision.saturating_add(1);
        spec.updated_at_unix_secs = now;
        if old_name != spec.name {
            let mut by_name = write_txn.open_table(UPSTREAM_SPEC_V1_BY_NAME)?;
            if by_name.get(spec.name.as_str())?.is_some() {
                return Err(crate::StorageError::UpstreamConflict(
                    "upstream name already exists".to_owned(),
                ));
            }
            by_name.remove(old_name.as_str())?;
            by_name.insert(spec.name.as_str(), id.as_bytes().as_slice())?;
        }
        specs.insert(id.as_bytes().as_slice(), serde_json::to_vec(&spec)?.as_slice())?;
    }
    write_txn.commit()?;
    get_split_by_id_sync(storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
}

fn update_split_api_key_secret_sync(
    storage: &RedbStorage,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> Result<UpstreamRecord, crate::StorageError> {
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    {
        let mut table = write_txn.open_table(UPSTREAM_API_KEY_SECRET_V1)?;
        let revision = table
            .get(id.as_bytes().as_slice())?
            .map(|value| serde_json::from_slice::<UpstreamApiKeySecretRecord>(value.value()))
            .transpose()?
            .map(|record| record.secret_revision.saturating_add(1))
            .unwrap_or(1);
        let secret = UpstreamApiKeySecretRecord {
            api_key_ciphertext,
            secret_revision: revision,
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
        };
        table.insert(id.as_bytes().as_slice(), serde_json::to_vec(&secret)?.as_slice())?;
    }
    write_txn.commit()?;
    get_split_by_id_sync(storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
}

fn update_split_oauth_token_sync(
    storage: &RedbStorage,
    id: Uuid,
    tokens: EncryptedOAuthTokens,
) -> Result<UpstreamRecord, crate::StorageError> {
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    {
        let mut table = write_txn.open_table(UPSTREAM_OAUTH_TOKEN_V1)?;
        let revision = table
            .get(id.as_bytes().as_slice())?
            .map(|value| serde_json::from_slice::<UpstreamOauthTokenRecord>(value.value()))
            .transpose()?
            .map(|record| record.token_revision.saturating_add(1))
            .unwrap_or(1);
        let token = UpstreamOauthTokenRecord {
            oauth_credentials: tokens,
            token_revision: revision,
            refreshed_at_unix_secs: Some(now),
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
        };
        table.insert(id.as_bytes().as_slice(), serde_json::to_vec(&token)?.as_slice())?;
    }
    write_txn.commit()?;
    get_split_by_id_sync(storage, id)?.ok_or(crate::StorageError::UpstreamNotFound)
}

fn set_split_status_sync(
    storage: &RedbStorage,
    id: Uuid,
    patch: UpstreamStatusUpdate,
) -> Result<(), crate::StorageError> {
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    {
        let mut table = write_txn.open_table(UPSTREAM_STATUS_V1)?;
        let mut status = table
            .get(id.as_bytes().as_slice())?
            .map(|value| serde_json::from_slice::<UpstreamStatusRecord>(value.value()))
            .transpose()?
            .unwrap_or_default();
        if let Some(value) = patch.last_apply_error {
            status.last_apply_error = value;
        }
        if let Some(value) = patch.last_apply_at_unix_secs {
            status.last_apply_at_unix_secs = value;
        }
        if let Some(value) = patch.observed_spec_revision {
            status.observed_spec_revision = value;
        }
        if let Some(value) = patch.observed_api_key_secret_revision {
            status.observed_api_key_secret_revision = value;
        }
        if let Some(value) = patch.observed_oauth_token_revision {
            status.observed_oauth_token_revision = value;
        }
        if let Some(value) = patch.next_warmup_at {
            status.next_warmup_at = value;
        }
        if let Some(value) = patch.last_warmup_cycle_key {
            status.last_warmup_cycle_key = value;
        }
        status.updated_at_unix_secs = now;
        table.insert(id.as_bytes().as_slice(), serde_json::to_vec(&status)?.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn ensure_split_spec_revision_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
) -> Result<(), crate::StorageError> {
    let read_txn = storage.db.begin_read()?;
    let specs = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let stored = specs
        .get(id.as_bytes().as_slice())?
        .ok_or(crate::StorageError::UpstreamNotFound)?
        .value()
        .to_vec();
    let spec: UpstreamSpecRecord = serde_json::from_slice(&stored)?;
    if spec.deleted_at_unix_secs.is_some() {
        return Err(crate::StorageError::UpstreamNotFound);
    }
    if spec.spec_revision != expected_revision {
        return Err(crate::StorageError::UpstreamConflict(
            "stale upstream revision".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_split_spec_exists_in_txn(
    write_txn: &redb::WriteTransaction,
    id: Uuid,
) -> Result<(), crate::StorageError> {
    let specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
    let Some(stored) = specs.get(id.as_bytes().as_slice())? else {
        return Err(crate::StorageError::UpstreamNotFound);
    };
    let spec: UpstreamSpecRecord = serde_json::from_slice(stored.value())?;
    if spec.deleted_at_unix_secs.is_some() {
        return Err(crate::StorageError::UpstreamNotFound);
    }
    Ok(())
}

fn now_unix_secs_i64() -> Result<i64, crate::StorageError> {
    i64::try_from(now_unix_secs()).map_err(|_| crate::StorageError::InvalidInput {
        field: "now".to_owned(),
        reason: "current unix timestamp cannot be represented as i64".to_owned(),
    })
}

fn claim_split_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
    ttl_secs: i64,
) -> Result<bool, crate::StorageError> {
    let now = now_unix_secs_i64()?;
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    let key = lease_key(id, lease_kind);
    let claimed = {
        let mut table = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        let existing = table
            .get(key.as_str())?
            .map(|value| serde_json::from_slice::<UpstreamLeaseRecord>(value.value()))
            .transpose()?;
        if existing
            .as_ref()
            .is_some_and(|lease| lease.until_unix_secs > now && lease.holder != holder)
        {
            false
        } else {
            let lease = UpstreamLeaseRecord {
                holder: holder.to_owned(),
                until_unix_secs: now.saturating_add(ttl_secs),
                updated_at_unix_secs: now_unix_secs(),
            };
            table.insert(key.as_str(), serde_json::to_vec(&lease)?.as_slice())?;
            true
        }
    };
    write_txn.commit()?;
    Ok(claimed)
}

fn renew_split_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
    ttl_secs: i64,
) -> Result<bool, crate::StorageError> {
    let now = now_unix_secs_i64()?;
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    let key = lease_key(id, lease_kind);
    let renewed = {
        let mut table = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        let stored = {
            let Some(stored) = table.get(key.as_str())? else {
                return Ok(false);
            };
            stored.value().to_vec()
        };
        let mut lease: UpstreamLeaseRecord = serde_json::from_slice(&stored)?;
        if lease.holder != holder || lease.until_unix_secs <= now {
            false
        } else {
            lease.until_unix_secs = now.saturating_add(ttl_secs);
            lease.updated_at_unix_secs = now_unix_secs();
            table.insert(key.as_str(), serde_json::to_vec(&lease)?.as_slice())?;
            true
        }
    };
    write_txn.commit()?;
    Ok(renewed)
}

fn release_split_lease_sync(
    storage: &RedbStorage,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
) -> Result<bool, crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let key = lease_key(id, lease_kind);
    let released = {
        let mut table = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        let stored = {
            let Some(stored) = table.get(key.as_str())? else {
                return Ok(false);
            };
            stored.value().to_vec()
        };
        let lease: UpstreamLeaseRecord = serde_json::from_slice(&stored)?;
        if lease.holder == holder {
            table.remove(key.as_str())?;
            true
        } else {
            false
        }
    };
    write_txn.commit()?;
    Ok(released)
}

fn write_split_warmup_cycle_key_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
    new_cycle_key: i64,
    next_warmup_at: Option<DateTime<Utc>>,
) -> Result<bool, crate::StorageError> {
    let now = now_unix_secs_i64()?;
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    let lease_key = lease_key(id, UpstreamLeaseKind::Warmup);
    let updated = {
        let mut leases = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        let stored = {
            let Some(stored) = leases.get(lease_key.as_str())? else {
                return Ok(false);
            };
            stored.value().to_vec()
        };
        let lease: UpstreamLeaseRecord = serde_json::from_slice(&stored)?;
        if lease.holder != holder || lease.until_unix_secs <= now {
            false
        } else {
            let mut status_table = write_txn.open_table(UPSTREAM_STATUS_V1)?;
            let mut status = status_table
                .get(id.as_bytes().as_slice())?
                .map(|value| serde_json::from_slice::<UpstreamStatusRecord>(value.value()))
                .transpose()?
                .unwrap_or_default();
            if status.last_warmup_cycle_key == Some(new_cycle_key) {
                false
            } else {
                status.last_warmup_cycle_key = Some(new_cycle_key);
                status.next_warmup_at = next_warmup_at;
                status.updated_at_unix_secs = now_unix_secs();
                status_table.insert(
                    id.as_bytes().as_slice(),
                    serde_json::to_vec(&status)?.as_slice(),
                )?;
                leases.remove(lease_key.as_str())?;
                true
            }
        }
    };
    write_txn.commit()?;
    Ok(updated)
}

fn write_split_warmup_next_at_sync(
    storage: &RedbStorage,
    id: Uuid,
    holder: &str,
    next_warmup_at: DateTime<Utc>,
) -> Result<bool, crate::StorageError> {
    let now = now_unix_secs_i64()?;
    let write_txn = storage.db.begin_write()?;
    ensure_split_spec_exists_in_txn(&write_txn, id)?;
    let lease_key = lease_key(id, UpstreamLeaseKind::Warmup);
    let updated = {
        let leases = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        let Some(stored) = leases.get(lease_key.as_str())? else {
            return Ok(false);
        };
        let stored = stored.value().to_vec();
        let lease: UpstreamLeaseRecord = serde_json::from_slice(&stored)?;
        if lease.holder != holder || lease.until_unix_secs <= now {
            false
        } else {
            let mut status_table = write_txn.open_table(UPSTREAM_STATUS_V1)?;
            let mut status = status_table
                .get(id.as_bytes().as_slice())?
                .map(|value| serde_json::from_slice::<UpstreamStatusRecord>(value.value()))
                .transpose()?
                .unwrap_or_default();
            status.next_warmup_at = Some(next_warmup_at);
            status.updated_at_unix_secs = now_unix_secs();
            status_table.insert(
                id.as_bytes().as_slice(),
                serde_json::to_vec(&status)?.as_slice(),
            )?;
            true
        }
    };
    write_txn.commit()?;
    Ok(updated)
}

fn soft_delete_split_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
) -> Result<(), crate::StorageError> {
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    {
        let mut specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        let stored = specs
            .get(id.as_bytes().as_slice())?
            .ok_or(crate::StorageError::UpstreamNotFound)?
            .value()
            .to_vec();
        let mut spec: UpstreamSpecRecord = serde_json::from_slice(&stored)?;
        if spec.deleted_at_unix_secs.is_some() {
            return Err(crate::StorageError::UpstreamNotFound);
        }
        if spec.spec_revision != expected_revision {
            return Err(crate::StorageError::UpstreamConflict(
                "stale upstream revision".to_owned(),
            ));
        }
        spec.deleted_at_unix_secs = Some(now);
        spec.spec_revision = spec.spec_revision.saturating_add(1);
        spec.updated_at_unix_secs = now;
        specs.insert(id.as_bytes().as_slice(), serde_json::to_vec(&spec)?.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn hard_delete_split_sync(storage: &RedbStorage, id: Uuid) -> Result<(), crate::StorageError> {
    let write_txn = storage.db.begin_write()?;
    let record_to_delete = {
        let specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        specs
            .get(id.as_bytes().as_slice())?
            .map(|value| serde_json::from_slice::<UpstreamSpecRecord>(value.value()))
            .transpose()?
    };
    if let Some(spec) = record_to_delete {
        let mut specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        specs.remove(id.as_bytes().as_slice())?;
        let mut by_name = write_txn.open_table(UPSTREAM_SPEC_V1_BY_NAME)?;
        by_name.remove(spec.name.as_str())?;
        let mut secrets = write_txn.open_table(UPSTREAM_API_KEY_SECRET_V1)?;
        secrets.remove(id.as_bytes().as_slice())?;
        let mut tokens = write_txn.open_table(UPSTREAM_OAUTH_TOKEN_V1)?;
        tokens.remove(id.as_bytes().as_slice())?;
        let mut statuses = write_txn.open_table(UPSTREAM_STATUS_V1)?;
        statuses.remove(id.as_bytes().as_slice())?;
        let mut leases = write_txn.open_table(UPSTREAM_LEASE_V1)?;
        leases.remove(lease_key(id, UpstreamLeaseKind::Refresh).as_str())?;
        leases.remove(lease_key(id, UpstreamLeaseKind::Warmup).as_str())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn clear_split_warmup_dialect_plugin_sync(
    storage: &RedbStorage,
    id: Uuid,
    expected_revision: u64,
) -> Result<Option<UpstreamRecord>, crate::StorageError> {
    let now = now_unix_secs();
    let write_txn = storage.db.begin_write()?;
    let updated = {
        let mut specs = write_txn.open_table(UPSTREAM_SPEC_V1)?;
        let stored = {
            let Some(stored) = specs.get(id.as_bytes().as_slice())? else {
                return Ok(None);
            };
            stored.value().to_vec()
        };
        let mut spec: UpstreamSpecRecord = serde_json::from_slice(&stored)?;
        if spec.deleted_at_unix_secs.is_some() || spec.spec_revision != expected_revision {
            None
        } else {
            spec.warmup_dialect_plugin = None;
            spec.spec_revision = spec.spec_revision.saturating_add(1);
            spec.updated_at_unix_secs = now;
            specs.insert(id.as_bytes().as_slice(), serde_json::to_vec(&spec)?.as_slice())?;
            Some(())
        }
    };
    write_txn.commit()?;
    if updated.is_none() {
        return Ok(None);
    }
    get_split_by_id_sync(storage, id)
}

fn lease_key(id: Uuid, lease_kind: UpstreamLeaseKind) -> String {
    format!("{}:{}", id, lease_kind.as_str())
}

fn api_error_as_redb(error: ApiStorageError) -> crate::StorageError {
    crate::StorageError::UpstreamConflict(error.to_string())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use chrono::TimeZone;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn run_async(future: impl Future<Output = TestResult>) -> TestResult {
        tokio::runtime::Runtime::new()?.block_on(future)
    }

    fn temp_storage() -> TestResult<(tempfile::TempDir, RedbStorage)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("upstreams.redb");
        let storage = RedbStorage::open(&path, [29; 32])?;
        Ok((dir, storage))
    }

    async fn create_warmup_upstream(storage: &RedbStorage) -> TestResult<UpstreamRecord> {
        let record = storage
            .create(UpstreamCreate {
                name: format!("upstream-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                warmup_enabled: true,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            })
            .await?;
        Ok(record)
    }

    #[test]
    fn claim_warmup_lease_succeeds_when_free() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            let now = now_unix_secs_i64()?;
            assert!(stored
                .warmup_lease_until_unix_secs
                .is_some_and(|until| until > now));
            Ok(())
        })
    }

    #[test]
    fn claim_warmup_lease_fails_when_held_by_other() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .claim_warmup_lease(record.id, "replica-b", 60)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_with_wrong_holder() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(record.id, "replica-b", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, None);
            assert_eq!(stored.next_warmup_at, None);
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_when_lease_expired() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 0)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(record.id, "replica-a", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, None);
            assert_eq!(stored.next_warmup_at, None);
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_fails_when_cycle_key_unchanged() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let first_next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();
            let second_next_warmup_at = Utc.timestamp_opt(1_700_018_060, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                storage
                    .write_warmup_cycle_key(
                        record.id,
                        "replica-a",
                        1_700_000_000,
                        first_next_warmup_at,
                    )
                    .await?
            );
            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.warmup_lease_holder, None);
            assert_eq!(stored.warmup_lease_until_unix_secs, None);
            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                !storage
                    .write_warmup_cycle_key(
                        record.id,
                        "replica-a",
                        1_700_000_000,
                        second_next_warmup_at,
                    )
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, Some(1_700_000_000));
            assert_eq!(stored.next_warmup_at, first_next_warmup_at);
            assert_eq!(stored.warmup_lease_holder.as_deref(), Some("replica-a"));
            Ok(())
        })
    }

    #[test]
    fn write_warmup_cycle_key_succeeds_when_all_predicates_hold() -> TestResult {
        run_async(async {
            let (_dir, storage) = temp_storage()?;
            let record = create_warmup_upstream(&storage).await?;
            let next_warmup_at = Utc.timestamp_opt(1_700_018_030, 0).single();

            assert!(
                storage
                    .claim_warmup_lease(record.id, "replica-a", 60)
                    .await?
            );
            assert!(
                storage
                    .write_warmup_cycle_key(record.id, "replica-a", 1_700_000_000, next_warmup_at)
                    .await?
            );

            let stored = storage.get_by_id(record.id).await?.expect("upstream");
            assert_eq!(stored.last_warmup_cycle_key, Some(1_700_000_000));
            assert_eq!(stored.next_warmup_at, next_warmup_at);
            assert_eq!(stored.warmup_lease_holder, None);
            assert_eq!(stored.warmup_lease_until_unix_secs, None);
            Ok(())
        })
    }
}
