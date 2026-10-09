use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_clock::unix_secs;
use cc_lb_oauth_protocol::refresh_requires_reconnect;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore, UpstreamUpdate,
    validate_identifier,
};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Transaction};
use url::Url;
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage, conflict, datetime_to_unix_secs, i64_to_u64, u64_to_i64,
        unix_secs_to_datetime,
    },
    error_map::map_sqlx_error,
};

const CHANNEL: &str = "cclb_upstream_changed";

macro_rules! split_upstream_columns {
    () => {
        "spec.id, spec.name, spec.kind, spec.base_url, spec.enabled, token.oauth_credentials_ciphertext AS oauth_credentials, secret.api_key_ciphertext, status.last_apply_error, status.last_apply_at, spec.deleted_at, spec.spec_revision AS revision, COALESCE(token.oauth_token_generation, 0) AS oauth_token_generation, COALESCE(token.never_refresh, FALSE) AS oauth_never_refresh, spec.created_at, spec.updated_at, spec.warmup_enabled, status.last_warmup_at, spec.warmup_dialect_plugin"
    };
}

macro_rules! split_upstream_joins {
    () => {
        " FROM upstream_spec_v1 spec LEFT JOIN upstream_api_key_secret_v1 secret ON secret.upstream_id = spec.id LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id "
    };
}

#[async_trait]
impl UpstreamStore for PostgresStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        create_split(self, create).await
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        get_split_by_name(&self.pool, name).await
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        get_split_by_id(&self.pool, id).await
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        list_split(&self.pool, after, limit).await
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
        update_split_oauth_token(self, id, tokens, false, None).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        set_split_status(&self.pool, id, status).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
        never_refresh: bool,
    ) -> StorageResult<UpstreamRecord> {
        update_split_oauth_token(self, id, tokens, never_refresh, Some(expected_revision)).await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        complete_split_refresh(self, id, holder, tokens).await?;
        get_split_by_id(&self.pool, id)
            .await?
            .ok_or_else(|| conflict("upstream not found"))
    }

    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        read_split_oauth_token_generation(&self.pool, id).await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(unix_secs(self.clock.now()))),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        soft_delete_split(&self.pool, id, expected_revision).await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM upstream_spec_v1 WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        notify(&mut tx, id).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        clear_split_warmup_dialect_plugin(&self.pool, id, expected_revision).await
    }
}

async fn create_split(
    storage: &PostgresStorage,
    create: UpstreamCreate,
) -> StorageResult<UpstreamRecord> {
    // The spec row and its initial credential commit together, with a single
    // change notification, so no reader ever observes the upstream without
    // its credential.
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    let id = create.id;
    sqlx::query(
        "INSERT INTO upstream_spec_v1 (id, name, kind, base_url, enabled, warmup_enabled, warmup_dialect_plugin, spec_revision, created_at, updated_at) VALUES ($1, $2, $3, $4, TRUE, $5, $6, 1, NOW(), NOW())",
    )
    .bind(id)
    .bind(&create.name)
    .bind(create.kind.as_str())
    .bind(create.base_url.as_ref().map(ToString::to_string))
    .bind(create.warmup_enabled)
    .bind(
        create
            .warmup_dialect_plugin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
    )
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    if let Some(api_key_ciphertext) = create.api_key_ciphertext {
        sqlx::query(
            "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, created_at, updated_at) VALUES ($1, $2, NOW(), NOW())",
        )
        .bind(id)
        .bind(api_key_ciphertext)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    if let Some(oauth) = create.oauth_tokens {
        sqlx::query(
            "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, never_refresh, oauth_token_generation, created_at, updated_at) VALUES ($1, $2, $3, 1, NOW(), NOW())",
        )
        .bind(id)
        .bind(oauth.tokens.ciphertext())
        .bind(oauth.never_refresh)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        clear_split_apply_error_in_tx(&mut tx, id).await?;
    }

    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn get_split_by_name(
    pool: &sqlx::PgPool,
    name: &str,
) -> StorageResult<Option<UpstreamRecord>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        split_upstream_columns!(),
        split_upstream_joins!(),
        "WHERE spec.name = $1 AND spec.deleted_at IS NULL"
    ))
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    row.map(split_row_to_record).transpose()
}

async fn get_split_by_id(pool: &sqlx::PgPool, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        split_upstream_columns!(),
        split_upstream_joins!(),
        "WHERE spec.id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    row.map(split_row_to_record).transpose()
}

async fn list_split(
    pool: &sqlx::PgPool,
    after: Option<Uuid>,
    limit: usize,
) -> StorageResult<Vec<UpstreamRecord>> {
    let rows = match after {
        Some(after) => sqlx::query(concat!(
            "SELECT ",
            split_upstream_columns!(),
            split_upstream_joins!(),
            "WHERE spec.deleted_at IS NULL AND spec.id > $1 ORDER BY spec.id LIMIT $2"
        ))
        .bind(after)
        .bind(u64_to_i64(limit as u64, "upstream list limit")?)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?,
        None => sqlx::query(concat!(
            "SELECT ",
            split_upstream_columns!(),
            split_upstream_joins!(),
            "WHERE spec.deleted_at IS NULL ORDER BY spec.id LIMIT $1"
        ))
        .bind(u64_to_i64(limit as u64, "upstream list limit")?)
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?,
    };
    rows.into_iter().map(split_row_to_record).collect()
}

async fn update_split(
    storage: &PostgresStorage,
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
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    if has_spec_update {
        update_split_spec_in_tx(&mut tx, id, expected_revision, update).await?;
    } else {
        ensure_split_spec_revision_in_tx(&mut tx, id, expected_revision).await?;
    }
    let mut changed = has_spec_update;
    if let Some(ciphertext) = api_key_ciphertext {
        update_split_api_key_secret_in_tx(&mut tx, id, Some(ciphertext)).await?;
        changed = true;
    }
    if let Some(generation) = oauth_token_generation {
        update_split_oauth_token_generation_in_tx(&mut tx, id, generation).await?;
        changed = true;
    }
    if changed {
        notify(&mut tx, id).await?;
    }
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_spec(
    storage: &PostgresStorage,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> StorageResult<UpstreamRecord> {
    if let Some(name) = update.name.as_deref() {
        validate_identifier("upstream.name", name)?;
    }
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    update_split_spec_in_tx(&mut tx, id, expected_revision, update).await?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_spec_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    expected_revision: u64,
    update: UpstreamUpdate,
) -> StorageResult<()> {
    let base_url_present = update.base_url.is_some();
    let base_url = update
        .base_url
        .as_ref()
        .and_then(Option::as_ref)
        .map(Url::as_str);
    let row = sqlx::query(
        "UPDATE upstream_spec_v1
           SET name = COALESCE($3, name),
               base_url = CASE WHEN $4 THEN $5 ELSE base_url END,
               enabled = COALESCE($6, enabled),
               warmup_enabled = COALESCE($7, warmup_enabled),
               warmup_dialect_plugin = COALESCE($8, warmup_dialect_plugin),
               spec_revision = spec_revision + 1,
               updated_at = NOW()
         WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL
         RETURNING id",
    )
    .bind(id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .bind(update.name)
    .bind(base_url_present)
    .bind(base_url)
    .bind(update.enabled)
    .bind(update.warmup_enabled)
    .bind(
        update
            .warmup_dialect_plugin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale upstream revision"));
    }
    Ok(())
}

async fn update_split_api_key_secret(
    storage: &PostgresStorage,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    update_split_api_key_secret_in_tx(&mut tx, id, api_key_ciphertext).await?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_api_key_secret_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> StorageResult<()> {
    ensure_split_spec_active_in_tx(tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, created_at, updated_at)
         VALUES ($1, $2, NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET api_key_ciphertext = EXCLUDED.api_key_ciphertext,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(api_key_ciphertext)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn update_split_oauth_token(
    storage: &PostgresStorage,
    id: Uuid,
    tokens: EncryptedOAuthTokens,
    never_refresh: bool,
    expected_revision: Option<u64>,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    if let Some(expected_revision) = expected_revision {
        ensure_split_spec_revision_in_tx(&mut tx, id, expected_revision).await?;
    } else {
        ensure_split_spec_active_in_tx(&mut tx, id).await?;
    }
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, never_refresh, oauth_token_generation, created_at, updated_at)
         VALUES ($1, $2, $3, 1, NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_credentials_ciphertext = EXCLUDED.oauth_credentials_ciphertext,
             never_refresh = EXCLUDED.never_refresh,
             oauth_token_generation = upstream_oauth_token_v1.oauth_token_generation + 1,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(tokens.ciphertext())
    .bind(never_refresh)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    clear_split_apply_error_in_tx(&mut tx, id).await?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_oauth_token_generation_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    generation: u64,
) -> StorageResult<()> {
    ensure_split_spec_active_in_tx(tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, oauth_token_generation, created_at, updated_at)
         VALUES ($1, NULL, $2, NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_token_generation = EXCLUDED.oauth_token_generation,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(u64_to_i64(generation, "oauth token generation")?)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn complete_split_refresh(
    storage: &PostgresStorage,
    id: Uuid,
    _holder: Uuid,
    tokens: EncryptedOAuthTokens,
) -> StorageResult<()> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    ensure_split_spec_active_in_tx(&mut tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, oauth_token_generation, created_at, updated_at)
         VALUES ($1, $2, 1, NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_credentials_ciphertext = EXCLUDED.oauth_credentials_ciphertext,
             oauth_token_generation = upstream_oauth_token_v1.oauth_token_generation + 1,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(tokens.ciphertext())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    clear_split_apply_error_in_tx(&mut tx, id).await?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)
}

async fn read_split_oauth_token_generation(
    pool: &sqlx::PgPool,
    id: Uuid,
) -> StorageResult<Option<u64>> {
    let generation: Option<i64> = sqlx::query_scalar(
        "SELECT COALESCE(token.oauth_token_generation, 0)
           FROM upstream_spec_v1 spec
           LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id
          WHERE spec.id = $1 AND spec.deleted_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    generation
        .map(|value| i64_to_u64(value, "oauth token generation"))
        .transpose()
}

async fn clear_split_apply_error_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> StorageResult<()> {
    sqlx::query(
        "INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, updated_at)
         VALUES ($1, NULL, NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET last_apply_error = NULL,
             updated_at = NOW()",
    )
    .bind(id)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn ensure_split_spec_revision_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT spec_revision FROM upstream_spec_v1 WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(id)
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
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> StorageResult<()> {
    let row: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM upstream_spec_v1 WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("upstream not found"));
    }
    Ok(())
}

async fn set_split_status(
    pool: &sqlx::PgPool,
    id: Uuid,
    patch: UpstreamStatusUpdate,
) -> StorageResult<()> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    ensure_split_spec_active_in_tx(&mut tx, id).await?;
    // Every OAuth token writer also locks the spec row above, so generation
    // cannot advance between this predicate and the status write.
    if let Some(expected_generation) = patch.expected_oauth_token_generation {
        let generation: Option<i64> = sqlx::query_scalar(
            "SELECT oauth_token_generation FROM upstream_oauth_token_v1 WHERE upstream_id = $1",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if i64_to_u64(generation.unwrap_or_default(), "oauth token generation")?
            != expected_generation
        {
            return Err(conflict("stale oauth token generation"));
        }
    }
    let row = sqlx::query(
        "SELECT spec.kind, status.last_apply_error, status.last_apply_at, status.last_warmup_at
           FROM upstream_spec_v1 spec
           LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id
          WHERE spec.id = $1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

    let is_oauth = row
        .as_ref()
        .map(|row| row.try_get::<&str, _>("kind"))
        .transpose()
        .map_err(map_sqlx_error)?
        == Some(UpstreamKind::AnthropicOauth.as_str());
    let mut last_apply_error = row
        .as_ref()
        .map(|row| row.try_get::<Option<String>, _>("last_apply_error"))
        .transpose()
        .map_err(map_sqlx_error)?
        .flatten();
    let mut last_apply_at = row
        .as_ref()
        .map(|row| row.try_get::<Option<DateTime<Utc>>, _>("last_apply_at"))
        .transpose()
        .map_err(map_sqlx_error)?
        .flatten();
    let mut last_warmup_at = optional_i64_to_u64(
        row.as_ref()
            .map(|row| row.try_get::<Option<i64>, _>("last_warmup_at"))
            .transpose()
            .map_err(map_sqlx_error)?
            .flatten(),
        "last_warmup_at",
    )?;

    if let Some(value) = patch.last_apply_error
        && (!is_oauth
            || !refresh_requires_reconnect(last_apply_error.as_deref())
            || refresh_requires_reconnect(value.as_deref()))
    {
        last_apply_error = value;
    }
    if let Some(value) = patch.last_apply_at_unix_secs {
        last_apply_at = value
            .map(|unix_secs| unix_secs_to_datetime(unix_secs, "last_apply_at"))
            .transpose()?;
    }
    if let Some(value) = patch.last_warmup_at_unix_secs {
        last_warmup_at = value;
    }

    sqlx::query(
        "INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, last_apply_at, last_warmup_at, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET last_apply_error = EXCLUDED.last_apply_error,
             last_apply_at = EXCLUDED.last_apply_at,
             last_warmup_at = EXCLUDED.last_warmup_at,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(last_apply_error)
    .bind(last_apply_at)
    .bind(
        last_warmup_at
            .map(|value| u64_to_i64(value, "last_warmup_at"))
            .transpose()?,
    )
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn soft_delete_split(
    pool: &sqlx::PgPool,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let row = sqlx::query(
        "UPDATE upstream_spec_v1 SET deleted_at = NOW(), spec_revision = spec_revision + 1, updated_at = NOW() WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL RETURNING id",
    )
    .bind(id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale upstream revision"));
    }
    Ok(())
}

async fn clear_split_warmup_dialect_plugin(
    pool: &sqlx::PgPool,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<Option<UpstreamRecord>> {
    let row = sqlx::query(
        "UPDATE upstream_spec_v1 SET warmup_dialect_plugin = NULL, spec_revision = spec_revision + 1, updated_at = NOW() WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL RETURNING id",
    )
    .bind(id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Ok(None);
    }
    get_split_by_id(pool, id).await
}

async fn notify(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> StorageResult<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

fn split_row_to_record(row: sqlx::postgres::PgRow) -> StorageResult<UpstreamRecord> {
    let kind = parse_kind(row.try_get::<String, _>("kind").map_err(map_sqlx_error)?)?;
    let base_url = row
        .try_get::<Option<String>, _>("base_url")
        .map_err(map_sqlx_error)?
        .map(|value| Url::parse(&value))
        .transpose()
        .map_err(|error| StorageError::Corrupted {
            message: format!("invalid upstream base_url: {error}"),
        })?;
    Ok(UpstreamRecord {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind,
        base_url,
        enabled: row.try_get("enabled").map_err(map_sqlx_error)?,
        oauth_credentials: row
            .try_get::<Option<Vec<u8>>, _>("oauth_credentials")
            .map_err(map_sqlx_error)?
            .map(EncryptedOAuthTokens::from_ciphertext),
        oauth_never_refresh: row.try_get("oauth_never_refresh").map_err(map_sqlx_error)?,
        api_key_ciphertext: row.try_get("api_key_ciphertext").map_err(map_sqlx_error)?,
        last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
        last_apply_at_unix_secs: optional_ts(
            row.try_get("last_apply_at").map_err(map_sqlx_error)?,
            "last_apply_at",
        )?,
        deleted_at_unix_secs: optional_ts(
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
        created_at_unix_secs: datetime_to_unix_secs(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "created_at",
        )?,
        updated_at_unix_secs: datetime_to_unix_secs(
            row.try_get("updated_at").map_err(map_sqlx_error)?,
            "updated_at",
        )?,
        warmup_enabled: row.try_get("warmup_enabled").map_err(map_sqlx_error)?,
        warmup_dialect_plugin: row
            .try_get::<Option<serde_json::Value>, _>("warmup_dialect_plugin")
            .map_err(map_sqlx_error)?
            .map(serde_json::from_value)
            .transpose()
            .map_err(StorageError::Serialization)?,
        last_warmup_at_unix_secs: optional_i64_to_u64(
            row.try_get("last_warmup_at").map_err(map_sqlx_error)?,
            "last_warmup_at",
        )?,
    })
}

fn optional_i64_to_u64(value: Option<i64>, field: &str) -> StorageResult<Option<u64>> {
    value.map(|value| i64_to_u64(value, field)).transpose()
}

fn optional_ts(value: Option<DateTime<Utc>>, field: &str) -> StorageResult<Option<u64>> {
    value
        .map(|value| datetime_to_unix_secs(value, field))
        .transpose()
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
