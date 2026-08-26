use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_clock::{Clock, unix_secs};
use cc_lb_storage_api::upstream::{
    UpstreamKind, UpstreamStatusUpdate, UpstreamWarmupDialectPlugin,
};
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore, UpstreamUpdate,
    validate_identifier,
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

macro_rules! split_upstream_columns {
    () => {
        "spec.id, spec.name, spec.kind, spec.base_url, spec.enabled, token.oauth_credentials_ciphertext AS oauth_credentials, secret.api_key_ciphertext, status.last_apply_error, status.last_apply_at, spec.deleted_at, spec.spec_revision AS revision, COALESCE(token.oauth_token_generation, 0) AS oauth_token_generation, spec.created_at, spec.updated_at, spec.warmup_enabled, status.last_warmup_at, spec.warmup_dialect_plugin"
    };
}

macro_rules! split_upstream_joins {
    () => {
        " FROM upstream_spec_v1 spec LEFT JOIN upstream_api_key_secret_v1 secret ON secret.upstream_id = spec.id LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id "
    };
}

#[async_trait]
impl UpstreamStore for SqliteStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        create_split(self, create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        get_split_by_name(self.pool(), name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        get_split_by_id(self.pool(), id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        list_split(self.pool(), after, limit).await
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        if let Some(name) = update.name.as_deref() {
            validate_identifier("upstream.name", name)?;
        }
        update_split(self, id, expected_revision, update).await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        self.update_spec(
            id,
            expected_revision,
            UpstreamUpdate {
                enabled: Some(enabled),
                ..UpstreamUpdate::default()
            },
        )
        .await
    }

    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord> {
        update_split_spec(self, id, expected_revision, update).await
    }

    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord> {
        update_split_api_key_secret(self, id, api_key_ciphertext).await
    }

    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        update_split_oauth_token(self, id, tokens, None).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        ensure_split_spec_active_in_tx(&mut tx, id).await?;
        set_split_status_in_tx(&mut tx, id, status).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        update_split_oauth_token(self, id, tokens, Some(expected_revision)).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        complete_split_refresh(self, id, holder, tokens).await?;
        get_split_by_id(self.pool(), id)
            .await?
            .ok_or_else(|| conflict("upstream not found"))
    }

    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        read_split_oauth_token_generation(self.pool(), id).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(now_unix_secs_u64(self.clock())?)),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        let row = sqlx::query(
            "UPDATE upstream_spec_v1 SET deleted_at = unixepoch(), spec_revision = spec_revision + 1, updated_at = unixepoch() WHERE id = ? AND spec_revision = ? AND deleted_at IS NULL RETURNING id",
        )
        .bind(id.to_string())
        .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        if row.is_none() {
            return Err(conflict("stale upstream revision"));
        }
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        sqlx::query("DELETE FROM upstream_spec_v1 WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let mut tx = self.begin_immediate().await?;
        let row = sqlx::query(
            "UPDATE upstream_spec_v1 SET warmup_dialect_plugin = NULL, spec_revision = spec_revision + 1, updated_at = unixepoch() WHERE id = ? AND spec_revision = ? AND deleted_at IS NULL RETURNING id",
        )
        .bind(id.to_string())
        .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        if row.is_none() {
            return Ok(None);
        }
        get_split_by_id(self.pool(), id).await
    }
}

async fn create_split(
    storage: &SqliteStorage,
    create: UpstreamCreate,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.begin_immediate().await?;
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin, spec_revision, created_at, updated_at) VALUES (?, ?, ?, ?, 1, ?, ?, 1, unixepoch(), unixepoch())",
    )
    .bind(id.to_string())
    .bind(&create.name)
    .bind(create.kind.as_str())
    .bind(create.base_url.as_ref().map(ToString::to_string))
    .bind(create.warmup_enabled)
    .bind(json_string(create.warmup_dialect_plugin.as_ref())?)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlite_error)?;

    if let Some(api_key_ciphertext) = create.api_key_ciphertext {
        sqlx::query(
            "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at) VALUES (?, ?, 1, unixepoch(), unixepoch())",
        )
        .bind(id.to_string())
        .bind(api_key_ciphertext)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    if let Some(oauth_token_generation) = create.oauth_token_generation {
        sqlx::query(
            "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, token_revision, oauth_token_generation, refreshed_at, created_at, updated_at) VALUES (?, NULL, 1, ?, NULL, unixepoch(), unixepoch())",
        )
        .bind(id.to_string())
        .bind(u64_to_i64(oauth_token_generation, "oauth token generation")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(storage.pool(), id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn get_split_by_name(pool: &SqlitePool, name: &str) -> StorageResult<Option<UpstreamRecord>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        split_upstream_columns!(),
        split_upstream_joins!(),
        "WHERE spec.name = ? AND spec.deleted_at IS NULL"
    ))
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    row.map(split_row_to_record).transpose()
}

async fn get_split_by_id(pool: &SqlitePool, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        split_upstream_columns!(),
        split_upstream_joins!(),
        "WHERE spec.id = ?"
    ))
    .bind(id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    row.map(split_row_to_record).transpose()
}

async fn list_split(
    pool: &SqlitePool,
    after: Option<Uuid>,
    limit: usize,
) -> StorageResult<Vec<UpstreamRecord>> {
    let rows = match after {
        Some(after) => sqlx::query(concat!(
            "SELECT ",
            split_upstream_columns!(),
            split_upstream_joins!(),
            "WHERE spec.deleted_at IS NULL AND spec.id > ? ORDER BY spec.id LIMIT ?"
        ))
        .bind(after.to_string())
        .bind(usize_to_i64(limit, "upstream list limit")?)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?,
        None => sqlx::query(concat!(
            "SELECT ",
            split_upstream_columns!(),
            split_upstream_joins!(),
            "WHERE spec.deleted_at IS NULL ORDER BY spec.id LIMIT ?"
        ))
        .bind(usize_to_i64(limit, "upstream list limit")?)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?,
    };
    rows.into_iter().map(split_row_to_record).collect()
}

async fn update_split(
    storage: &SqliteStorage,
    id: Uuid,
    expected_revision: u64,
    mut update: UpstreamUpdate,
) -> StorageResult<UpstreamRecord> {
    let has_spec_update = update.name.is_some()
        || update.base_url.is_some()
        || update.enabled.is_some()
        || update.warmup_enabled.is_some()
        || update.warmup_dialect_plugin.is_some();
    let api_key_ciphertext = update.api_key_ciphertext.take();
    let oauth_token_generation = update.oauth_token_generation.take();
    // Spec, api-key, and generation updates share one transaction so a partial
    // update can never commit independently.
    let mut tx = storage.begin_immediate().await?;
    if has_spec_update {
        update_split_spec_in_tx(&mut tx, id, expected_revision, update).await?;
    } else {
        ensure_split_spec_revision_in_tx(&mut tx, id, expected_revision).await?;
    }
    if let Some(ciphertext) = api_key_ciphertext {
        update_split_api_key_secret_in_tx(&mut tx, id, Some(ciphertext)).await?;
    }
    if let Some(generation) = oauth_token_generation {
        update_split_oauth_token_generation_in_tx(&mut tx, id, generation).await?;
    }
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(storage.pool(), id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_spec(
    storage: &SqliteStorage,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> StorageResult<UpstreamRecord> {
    if let Some(name) = update.name.as_deref() {
        validate_identifier("upstream.name", name)?;
    }
    let mut tx = storage.begin_immediate().await?;
    update_split_spec_in_tx(&mut tx, id, expected_revision, update).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(storage.pool(), id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_spec_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> StorageResult<()> {
    let warmup_dialect_plugin = json_string(update.warmup_dialect_plugin.as_ref())?;
    let row = sqlx::query(
        "UPDATE upstream_spec_v1
           SET name = COALESCE(?, name),
               base_url = COALESCE(?, base_url),
               enabled = COALESCE(?, enabled),
               warmup_enabled = COALESCE(?, warmup_enabled),
               warmup_dialect_plugin = COALESCE(?, warmup_dialect_plugin),
               spec_revision = spec_revision + 1,
               updated_at = unixepoch()
         WHERE id = ? AND spec_revision = ? AND deleted_at IS NULL
         RETURNING id",
    )
    .bind(update.name)
    .bind(update.base_url.as_ref().map(ToString::to_string))
    .bind(update.enabled)
    .bind(update.warmup_enabled)
    .bind(warmup_dialect_plugin)
    .bind(id.to_string())
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlite_error)?;
    if row.is_none() {
        return Err(conflict("stale upstream revision"));
    }
    Ok(())
}

async fn update_split_api_key_secret(
    storage: &SqliteStorage,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.begin_immediate().await?;
    update_split_api_key_secret_in_tx(&mut tx, id, api_key_ciphertext).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(storage.pool(), id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_api_key_secret_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> StorageResult<()> {
    ensure_split_spec_active_in_tx(tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at)
         VALUES (?, ?, 1, unixepoch(), unixepoch())
         ON CONFLICT (upstream_id) DO UPDATE
         SET api_key_ciphertext = excluded.api_key_ciphertext,
             secret_revision = upstream_api_key_secret_v1.secret_revision + 1,
             updated_at = unixepoch()",
    )
    .bind(id.to_string())
    .bind(api_key_ciphertext)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn update_split_oauth_token(
    storage: &SqliteStorage,
    id: Uuid,
    tokens: EncryptedOAuthTokens,
    expected_revision: Option<u64>,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.begin_immediate().await?;
    if let Some(expected_revision) = expected_revision {
        ensure_split_spec_revision_in_tx(&mut tx, id, expected_revision).await?;
    } else {
        ensure_split_spec_active_in_tx(&mut tx, id).await?;
    }
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, token_revision, refreshed_at, created_at, updated_at)
         VALUES (?, ?, 1, unixepoch(), unixepoch(), unixepoch())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_credentials_ciphertext = excluded.oauth_credentials_ciphertext,
             token_revision = upstream_oauth_token_v1.token_revision + 1,
             refreshed_at = unixepoch(),
             updated_at = unixepoch()",
    )
    .bind(id.to_string())
    .bind(tokens.ciphertext())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(storage.pool(), id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_oauth_token_generation_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    id: Uuid,
    generation: u64,
) -> StorageResult<()> {
    ensure_split_spec_active_in_tx(tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, token_revision, oauth_token_generation, refreshed_at, created_at, updated_at)
         VALUES (?, NULL, 1, ?, NULL, unixepoch(), unixepoch())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_token_generation = excluded.oauth_token_generation,
             updated_at = unixepoch()",
    )
    .bind(id.to_string())
    .bind(u64_to_i64(generation, "oauth token generation")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn complete_split_refresh(
    storage: &SqliteStorage,
    id: Uuid,
    _holder: Uuid,
    tokens: EncryptedOAuthTokens,
) -> StorageResult<()> {
    let mut tx = storage.begin_immediate().await?;
    ensure_split_spec_active_in_tx(&mut tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, token_revision, oauth_token_generation, refreshed_at, created_at, updated_at)
         VALUES (?, ?, 1, 1, unixepoch(), unixepoch(), unixepoch())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_credentials_ciphertext = excluded.oauth_credentials_ciphertext,
             token_revision = upstream_oauth_token_v1.token_revision + 1,
             oauth_token_generation = upstream_oauth_token_v1.oauth_token_generation + 1,
             refreshed_at = unixepoch(),
             updated_at = unixepoch()",
    )
    .bind(id.to_string())
    .bind(tokens.ciphertext())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    set_split_status_in_tx(
        &mut tx,
        id,
        UpstreamStatusUpdate {
            last_apply_error: Some(None),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await?;
    tx.commit().await.map_err(map_sqlx_error)
}

async fn read_split_oauth_token_generation(
    pool: &SqlitePool,
    id: Uuid,
) -> StorageResult<Option<u64>> {
    let generation: Option<i64> = sqlx::query_scalar(
        "SELECT COALESCE(token.oauth_token_generation, 0)
           FROM upstream_spec_v1 spec
           LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id
          WHERE spec.id = ? AND spec.deleted_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    generation
        .map(|value| i64_to_u64(value, "oauth token generation"))
        .transpose()
}

async fn ensure_split_spec_revision_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT spec_revision FROM upstream_spec_v1 WHERE id = ? AND deleted_at IS NULL",
    )
    .bind(id.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    match current {
        Some(current) if i64_to_u64(current, "upstream spec revision")? == expected_revision => {
            Ok(())
        }
        Some(_) => Err(conflict("stale upstream revision")),
        None => Err(conflict("upstream not found")),
    }
}

async fn ensure_split_spec_active_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    id: Uuid,
) -> StorageResult<()> {
    let row: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM upstream_spec_v1 WHERE id = ? AND deleted_at IS NULL")
            .bind(id.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("upstream not found"));
    }
    Ok(())
}

async fn set_split_status_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    patch: Uuid,
    status: UpstreamStatusUpdate,
) -> StorageResult<()> {
    let id = patch;
    let row = sqlx::query(
        "SELECT last_apply_error, last_apply_at, observed_spec_revision, observed_api_key_secret_revision, observed_oauth_token_revision, last_warmup_at FROM upstream_status_v1 WHERE upstream_id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    let mut current = StatusFields::from_row(row.as_ref())?;
    if let Some(value) = status.last_apply_error {
        current.last_apply_error = value;
    }
    if let Some(value) = status.last_apply_at_unix_secs {
        current.last_apply_at = value
            .map(|unix_secs| u64_to_i64(unix_secs, "last_apply_at"))
            .transpose()?;
    }
    if let Some(value) = status.observed_spec_revision {
        current.observed_spec_revision = value
            .map(|value| u64_to_i64(value, "observed_spec_revision"))
            .transpose()?;
    }
    if let Some(value) = status.observed_api_key_secret_revision {
        current.observed_api_key_secret_revision = value
            .map(|value| u64_to_i64(value, "observed_api_key_secret_revision"))
            .transpose()?;
    }
    if let Some(value) = status.observed_oauth_token_revision {
        current.observed_oauth_token_revision = value
            .map(|value| u64_to_i64(value, "observed_oauth_token_revision"))
            .transpose()?;
    }
    if let Some(value) = status.last_warmup_at_unix_secs {
        current.last_warmup_at = value
            .map(|unix_secs| u64_to_i64(unix_secs, "last_warmup_at"))
            .transpose()?;
    }

    sqlx::query(
        "INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, last_apply_at, observed_spec_revision, observed_api_key_secret_revision, observed_oauth_token_revision, last_warmup_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, unixepoch())
         ON CONFLICT (upstream_id) DO UPDATE
         SET last_apply_error = excluded.last_apply_error,
             last_apply_at = excluded.last_apply_at,
             observed_spec_revision = excluded.observed_spec_revision,
             observed_api_key_secret_revision = excluded.observed_api_key_secret_revision,
             observed_oauth_token_revision = excluded.observed_oauth_token_revision,
             last_warmup_at = excluded.last_warmup_at,
             updated_at = unixepoch()",
    )
    .bind(id.to_string())
    .bind(current.last_apply_error)
    .bind(current.last_apply_at)
    .bind(current.observed_spec_revision)
    .bind(current.observed_api_key_secret_revision)
    .bind(current.observed_oauth_token_revision)
    .bind(current.last_warmup_at)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

fn split_row_to_record(row: SqliteRow) -> StorageResult<UpstreamRecord> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let kind = parse_kind(row.try_get::<String, _>("kind").map_err(map_sqlx_error)?)?;
    let base_url = row
        .try_get::<Option<String>, _>("base_url")
        .map_err(map_sqlx_error)?
        .map(|value| value.parse())
        .transpose()
        .map_err(|error| StorageError::Corrupted {
            message: format!("invalid upstream base_url: {error}"),
        })?;
    let warmup_dialect_plugin = row
        .try_get::<Option<String>, _>("warmup_dialect_plugin")
        .map_err(map_sqlx_error)?
        .map(|value| serde_json::from_str::<UpstreamWarmupDialectPlugin>(&value))
        .transpose()?;
    Ok(UpstreamRecord {
        id: Uuid::parse_str(&id).map_err(|error| StorageError::Corrupted {
            message: format!("invalid upstream id {id}: {error}"),
        })?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind,
        base_url,
        enabled: row.try_get::<i64, _>("enabled").map_err(map_sqlx_error)? != 0,
        oauth_credentials: row
            .try_get::<Option<Vec<u8>>, _>("oauth_credentials")
            .map_err(map_sqlx_error)?
            .map(EncryptedOAuthTokens::from_ciphertext),
        api_key_ciphertext: row.try_get("api_key_ciphertext").map_err(map_sqlx_error)?,
        last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
        last_apply_at_unix_secs: optional_i64_to_u64(
            row.try_get("last_apply_at").map_err(map_sqlx_error)?,
            "last_apply_at",
        )?,
        deleted_at_unix_secs: optional_i64_to_u64(
            row.try_get("deleted_at").map_err(map_sqlx_error)?,
            "deleted_at",
        )?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "upstream spec revision",
        )?,
        oauth_token_generation: i64_to_u64(
            row.try_get("oauth_token_generation")
                .map_err(map_sqlx_error)?,
            "oauth token generation",
        )?,
        created_at_unix_secs: i64_to_u64(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "created_at",
        )?,
        updated_at_unix_secs: i64_to_u64(
            row.try_get("updated_at").map_err(map_sqlx_error)?,
            "updated_at",
        )?,
        warmup_enabled: row
            .try_get::<i64, _>("warmup_enabled")
            .map_err(map_sqlx_error)?
            != 0,
        warmup_dialect_plugin,
        last_warmup_at_unix_secs: optional_i64_to_u64(
            row.try_get("last_warmup_at").map_err(map_sqlx_error)?,
            "last_warmup_at",
        )?,
    })
}

#[derive(Default)]
struct StatusFields {
    last_apply_error: Option<String>,
    last_apply_at: Option<i64>,
    observed_spec_revision: Option<i64>,
    observed_api_key_secret_revision: Option<i64>,
    observed_oauth_token_revision: Option<i64>,
    last_warmup_at: Option<i64>,
}

impl StatusFields {
    fn from_row(row: Option<&SqliteRow>) -> StorageResult<Self> {
        let Some(row) = row else {
            return Ok(Self::default());
        };
        Ok(Self {
            last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
            last_apply_at: row.try_get("last_apply_at").map_err(map_sqlx_error)?,
            observed_spec_revision: row
                .try_get("observed_spec_revision")
                .map_err(map_sqlx_error)?,
            observed_api_key_secret_revision: row
                .try_get("observed_api_key_secret_revision")
                .map_err(map_sqlx_error)?,
            observed_oauth_token_revision: row
                .try_get("observed_oauth_token_revision")
                .map_err(map_sqlx_error)?,
            last_warmup_at: row.try_get("last_warmup_at").map_err(map_sqlx_error)?,
        })
    }
}

fn json_string<T: serde::Serialize>(value: Option<&T>) -> StorageResult<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn parse_kind(value: String) -> StorageResult<UpstreamKind> {
    match value.as_str() {
        "anthropic_api_key" => Ok(UpstreamKind::AnthropicApiKey),
        "anthropic_oauth" => Ok(UpstreamKind::AnthropicOauth),
        other => Err(StorageError::Corrupted {
            message: format!("invalid upstream kind {other}"),
        }),
    }
}

fn optional_i64_to_u64(value: Option<i64>, field: &str) -> StorageResult<Option<u64>> {
    value.map(|value| i64_to_u64(value, field)).transpose()
}

fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite integer"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite integer"),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

fn now_unix_secs_u64(clock: &dyn Clock) -> StorageResult<u64> {
    Ok(unix_secs(clock.now()))
}

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn map_sqlite_error(error: sqlx::Error) -> StorageError {
    if error
        .as_database_error()
        .is_some_and(|database_error| database_error.is_unique_violation())
    {
        return conflict(error.to_string());
    }
    map_sqlx_error(error)
}
