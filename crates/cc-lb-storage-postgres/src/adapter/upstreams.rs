use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamLeaseKind, UpstreamStatusUpdate};
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
        "spec.id, spec.name, spec.kind, spec.base_url, spec.enabled, token.oauth_credentials_ciphertext AS oauth_credentials, secret.api_key_ciphertext, refresh_lease.holder AS refresh_lease_holder, refresh_lease.until_unix_secs AS refresh_lease_until_unix_secs, status.last_apply_error, status.last_apply_at, spec.deleted_at, spec.spec_revision AS revision, spec.created_at, spec.updated_at, spec.warmup_enabled, status.next_warmup_at, status.last_warmup_cycle_key, warmup_lease.holder AS warmup_lease_holder, warmup_lease.until_unix_secs AS warmup_lease_until_unix_secs, spec.warmup_dialect_plugin"
    };
}

macro_rules! split_upstream_joins {
    () => {
        " FROM upstream_spec_v1 spec LEFT JOIN upstream_api_key_secret_v1 secret ON secret.upstream_id = spec.id LEFT JOIN upstream_oauth_token_v1 token ON token.upstream_id = spec.id LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id LEFT JOIN upstream_lease_v1 refresh_lease ON refresh_lease.upstream_id = spec.id AND refresh_lease.lease_kind = 'refresh' LEFT JOIN upstream_lease_v1 warmup_lease ON warmup_lease.upstream_id = spec.id AND warmup_lease.lease_kind = 'warmup' "
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
        update_split_oauth_token(self, id, tokens).await
    }

    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()> {
        set_split_status(&self.pool, id, status).await
    }

    async fn claim_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        claim_split_lease(&self.pool, id, lease_kind, &holder, ttl_secs).await
    }

    async fn renew_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        renew_split_lease(&self.pool, id, lease_kind, &holder, ttl_secs).await
    }

    async fn release_lease(
        &self,
        id: Uuid,
        lease_kind: UpstreamLeaseKind,
        holder: String,
    ) -> StorageResult<bool> {
        release_split_lease(&self.pool, id, lease_kind, &holder).await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        ensure_split_spec_revision(&self.pool, id, expected_revision).await?;
        self.update_oauth_token(id, tokens).await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        self.claim_lease(
            id,
            UpstreamLeaseKind::Refresh,
            holder.to_string(),
            i64::try_from(ttl_secs).map_err(|_| StorageError::Fatal {
                message: "refresh lease ttl cannot be represented as bigint".to_owned(),
            })?,
        )
        .await
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        let released = self
            .release_lease(id, UpstreamLeaseKind::Refresh, holder.to_string())
            .await?;
        if !released {
            return Err(conflict("refresh lease holder mismatch"));
        }
        self.update_oauth_token(id, tokens).await?;
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(None),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await?;
        get_split_by_id(&self.pool, id)
            .await?
            .ok_or_else(|| conflict("upstream not found"))
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        let released = self
            .release_lease(id, UpstreamLeaseKind::Refresh, holder.to_string())
            .await?;
        if !released {
            return Err(conflict("refresh lease holder mismatch"));
        }
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason)),
                last_apply_at_unix_secs: Some(Some(now_unix_secs())),
                ..UpstreamStatusUpdate::default()
            },
        )
        .await
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        self.set_status(
            id,
            UpstreamStatusUpdate {
                last_apply_error: Some(error),
                last_apply_at_unix_secs: Some(Some(now_unix_secs())),
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

    async fn claim_warmup_lease(
        &self,
        id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        self.claim_lease(id, UpstreamLeaseKind::Warmup, holder.to_owned(), ttl_secs)
            .await
    }

    async fn write_warmup_cycle_key(
        &self,
        id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        write_split_warmup_cycle_key(&self.pool, id, holder, new_cycle_key, next_warmup_at).await
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        self.release_lease(id, UpstreamLeaseKind::Warmup, holder.to_owned())
            .await
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        clear_split_warmup_dialect_plugin(&self.pool, id, expected_revision).await
    }

    async fn warmup_now_unix_secs(&self) -> StorageResult<i64> {
        let row = sqlx::query("SELECT extract(epoch from now())::bigint AS now_unix_secs")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.try_get("now_unix_secs").map_err(map_sqlx_error)
    }

    async fn write_warmup_next_at(
        &self,
        id: Uuid,
        holder: &str,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        write_split_warmup_next_at(&self.pool, id, holder, next_warmup_at).await
    }
}

async fn create_split(
    storage: &PostgresStorage,
    create: UpstreamCreate,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    let id = Uuid::new_v4();
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
            "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at) VALUES ($1, $2, 1, NOW(), NOW())",
        )
        .bind(id)
        .bind(api_key_ciphertext)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    if create.next_warmup_at.is_some() || create.last_warmup_cycle_key.is_some() {
        sqlx::query(
            "INSERT INTO upstream_status_v1 (upstream_id, next_warmup_at, last_warmup_cycle_key, updated_at) VALUES ($1, $2, $3, NOW())",
        )
        .bind(id)
        .bind(create.next_warmup_at)
        .bind(create.last_warmup_cycle_key)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    if let (Some(holder), Some(until_unix_secs)) = (
        create.warmup_lease_holder,
        create.warmup_lease_until_unix_secs,
    ) {
        sqlx::query(
            "INSERT INTO upstream_lease_v1 (upstream_id, lease_kind, holder, until_unix_secs, updated_at) VALUES ($1, 'warmup', $2, $3, NOW())",
        )
        .bind(id)
        .bind(holder)
        .bind(until_unix_secs)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
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
        "WHERE spec.name = $1"
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
    let status = UpstreamStatusUpdate {
        next_warmup_at: update.next_warmup_at.take().map(Some),
        last_warmup_cycle_key: update.last_warmup_cycle_key.take().map(Some),
        ..UpstreamStatusUpdate::default()
    };
    let has_status_update =
        status.next_warmup_at.is_some() || status.last_warmup_cycle_key.is_some();

    if has_spec_update {
        update_split_spec(storage, id, expected_revision, update).await?;
    } else {
        ensure_split_spec_revision(&storage.pool, id, expected_revision).await?;
    }
    if let Some(ciphertext) = api_key_ciphertext {
        update_split_api_key_secret(storage, id, Some(ciphertext)).await?;
    }
    if has_status_update {
        set_split_status(&storage.pool, id, status).await?;
    }
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
    let row = sqlx::query(
        "UPDATE upstream_spec_v1
           SET name = COALESCE($3, name),
               base_url = COALESCE($4, base_url),
               enabled = COALESCE($5, enabled),
               warmup_enabled = COALESCE($6, warmup_enabled),
               warmup_dialect_plugin = COALESCE($7, warmup_dialect_plugin),
               spec_revision = spec_revision + 1,
               updated_at = NOW()
         WHERE id = $1 AND spec_revision = $2 AND deleted_at IS NULL
         RETURNING id",
    )
    .bind(id)
    .bind(u64_to_i64(expected_revision, "upstream spec revision")?)
    .bind(update.name)
    .bind(update.base_url.as_ref().map(ToString::to_string))
    .bind(update.enabled)
    .bind(update.warmup_enabled)
    .bind(
        update
            .warmup_dialect_plugin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Err(conflict("stale upstream revision"));
    }
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_api_key_secret(
    storage: &PostgresStorage,
    id: Uuid,
    api_key_ciphertext: Option<Vec<u8>>,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    ensure_split_spec_active_in_tx(&mut tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_api_key_secret_v1 (upstream_id, api_key_ciphertext, secret_revision, created_at, updated_at)
         VALUES ($1, $2, 1, NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET api_key_ciphertext = EXCLUDED.api_key_ciphertext,
             secret_revision = upstream_api_key_secret_v1.secret_revision + 1,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(api_key_ciphertext)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn update_split_oauth_token(
    storage: &PostgresStorage,
    id: Uuid,
    tokens: EncryptedOAuthTokens,
) -> StorageResult<UpstreamRecord> {
    let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
    ensure_split_spec_active_in_tx(&mut tx, id).await?;
    sqlx::query(
        "INSERT INTO upstream_oauth_token_v1 (upstream_id, oauth_credentials_ciphertext, token_revision, refreshed_at, created_at, updated_at)
         VALUES ($1, $2, 1, NOW(), NOW(), NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET oauth_credentials_ciphertext = EXCLUDED.oauth_credentials_ciphertext,
             token_revision = upstream_oauth_token_v1.token_revision + 1,
             refreshed_at = NOW(),
             updated_at = NOW()",
    )
    .bind(id)
    .bind(tokens.ciphertext())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    get_split_by_id(&storage.pool, id)
        .await?
        .ok_or_else(|| conflict("upstream not found"))
}

async fn ensure_split_spec_revision(
    pool: &sqlx::PgPool,
    id: Uuid,
    expected_revision: u64,
) -> StorageResult<()> {
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT spec_revision FROM upstream_spec_v1 WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
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
    let row = sqlx::query(
        "SELECT last_apply_error, last_apply_at, observed_spec_revision, observed_api_key_secret_revision, observed_oauth_token_revision, next_warmup_at, last_warmup_cycle_key FROM upstream_status_v1 WHERE upstream_id = $1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;

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
    let mut observed_spec_revision = optional_i64_to_u64(
        row.as_ref()
            .map(|row| row.try_get::<Option<i64>, _>("observed_spec_revision"))
            .transpose()
            .map_err(map_sqlx_error)?
            .flatten(),
        "observed_spec_revision",
    )?;
    let mut observed_api_key_secret_revision = optional_i64_to_u64(
        row.as_ref()
            .map(|row| row.try_get::<Option<i64>, _>("observed_api_key_secret_revision"))
            .transpose()
            .map_err(map_sqlx_error)?
            .flatten(),
        "observed_api_key_secret_revision",
    )?;
    let mut observed_oauth_token_revision = optional_i64_to_u64(
        row.as_ref()
            .map(|row| row.try_get::<Option<i64>, _>("observed_oauth_token_revision"))
            .transpose()
            .map_err(map_sqlx_error)?
            .flatten(),
        "observed_oauth_token_revision",
    )?;
    let mut next_warmup_at = row
        .as_ref()
        .map(|row| row.try_get::<Option<DateTime<Utc>>, _>("next_warmup_at"))
        .transpose()
        .map_err(map_sqlx_error)?
        .flatten();
    let mut last_warmup_cycle_key = row
        .as_ref()
        .map(|row| row.try_get::<Option<i64>, _>("last_warmup_cycle_key"))
        .transpose()
        .map_err(map_sqlx_error)?
        .flatten();

    if let Some(value) = patch.last_apply_error {
        last_apply_error = value;
    }
    if let Some(value) = patch.last_apply_at_unix_secs {
        last_apply_at = value
            .map(|unix_secs| unix_secs_to_datetime(unix_secs, "last_apply_at"))
            .transpose()?;
    }
    if let Some(value) = patch.observed_spec_revision {
        observed_spec_revision = value;
    }
    if let Some(value) = patch.observed_api_key_secret_revision {
        observed_api_key_secret_revision = value;
    }
    if let Some(value) = patch.observed_oauth_token_revision {
        observed_oauth_token_revision = value;
    }
    if let Some(value) = patch.next_warmup_at {
        next_warmup_at = value;
    }
    if let Some(value) = patch.last_warmup_cycle_key {
        last_warmup_cycle_key = value;
    }

    sqlx::query(
        "INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, last_apply_at, observed_spec_revision, observed_api_key_secret_revision, observed_oauth_token_revision, next_warmup_at, last_warmup_cycle_key, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (upstream_id) DO UPDATE
         SET last_apply_error = EXCLUDED.last_apply_error,
             last_apply_at = EXCLUDED.last_apply_at,
             observed_spec_revision = EXCLUDED.observed_spec_revision,
             observed_api_key_secret_revision = EXCLUDED.observed_api_key_secret_revision,
             observed_oauth_token_revision = EXCLUDED.observed_oauth_token_revision,
             next_warmup_at = EXCLUDED.next_warmup_at,
             last_warmup_cycle_key = EXCLUDED.last_warmup_cycle_key,
             updated_at = NOW()",
    )
    .bind(id)
    .bind(last_apply_error)
    .bind(last_apply_at)
    .bind(
        observed_spec_revision
            .map(|value| u64_to_i64(value, "observed_spec_revision"))
            .transpose()?,
    )
    .bind(
        observed_api_key_secret_revision
            .map(|value| u64_to_i64(value, "observed_api_key_secret_revision"))
            .transpose()?,
    )
    .bind(
        observed_oauth_token_revision
            .map(|value| u64_to_i64(value, "observed_oauth_token_revision"))
            .transpose()?,
    )
    .bind(next_warmup_at)
    .bind(last_warmup_cycle_key)
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn claim_split_lease(
    pool: &sqlx::PgPool,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
    ttl_secs: i64,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "INSERT INTO upstream_lease_v1 (upstream_id, lease_kind, holder, until_unix_secs, updated_at)
         SELECT $1, $2, $3, extract(epoch from now())::bigint + $4, NOW()
         WHERE EXISTS (SELECT 1 FROM upstream_spec_v1 WHERE id = $1 AND deleted_at IS NULL)
         ON CONFLICT (upstream_id, lease_kind) DO UPDATE
         SET holder = EXCLUDED.holder,
             until_unix_secs = EXCLUDED.until_unix_secs,
             updated_at = NOW()
         WHERE upstream_lease_v1.holder = EXCLUDED.holder
            OR upstream_lease_v1.until_unix_secs <= extract(epoch from now())::bigint
         RETURNING upstream_id",
    )
    .bind(id)
    .bind(lease_kind.as_str())
    .bind(holder)
    .bind(ttl_secs)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(row.is_some())
}

async fn renew_split_lease(
    pool: &sqlx::PgPool,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
    ttl_secs: i64,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "UPDATE upstream_lease_v1
            SET until_unix_secs = extract(epoch from now())::bigint + $4,
                updated_at = NOW()
          WHERE upstream_id = $1
            AND lease_kind = $2
            AND holder = $3
            AND until_unix_secs > extract(epoch from now())::bigint
            AND EXISTS (
                SELECT 1 FROM upstream_spec_v1
                WHERE id = $1 AND deleted_at IS NULL
            )
          RETURNING upstream_id",
    )
    .bind(id)
    .bind(lease_kind.as_str())
    .bind(holder)
    .bind(ttl_secs)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(row.is_some())
}

async fn release_split_lease(
    pool: &sqlx::PgPool,
    id: Uuid,
    lease_kind: UpstreamLeaseKind,
    holder: &str,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "DELETE FROM upstream_lease_v1 WHERE upstream_id = $1 AND lease_kind = $2 AND holder = $3 RETURNING upstream_id",
    )
    .bind(id)
    .bind(lease_kind.as_str())
    .bind(holder)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(row.is_some())
}

async fn write_split_warmup_cycle_key(
    pool: &sqlx::PgPool,
    id: Uuid,
    holder: &str,
    new_cycle_key: i64,
    next_warmup_at: Option<DateTime<Utc>>,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "DELETE FROM upstream_lease_v1
          WHERE upstream_id = $1
            AND lease_kind = 'warmup'
            AND holder = $2
            AND until_unix_secs > extract(epoch from now())::bigint
            AND EXISTS (
                SELECT 1 FROM upstream_spec_v1 spec
                LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id
                WHERE spec.id = $1
                  AND spec.deleted_at IS NULL
                  AND status.last_warmup_cycle_key IS DISTINCT FROM $3
            )
          RETURNING upstream_id",
    )
    .bind(id)
    .bind(holder)
    .bind(new_cycle_key)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Ok(false);
    }
    set_split_status(
        pool,
        id,
        UpstreamStatusUpdate {
            last_warmup_cycle_key: Some(Some(new_cycle_key)),
            next_warmup_at: Some(next_warmup_at),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await?;
    Ok(true)
}

async fn write_split_warmup_next_at(
    pool: &sqlx::PgPool,
    id: Uuid,
    holder: &str,
    next_warmup_at: DateTime<Utc>,
) -> StorageResult<bool> {
    let row = sqlx::query(
        "SELECT upstream_id FROM upstream_lease_v1
          WHERE upstream_id = $1
            AND lease_kind = 'warmup'
            AND holder = $2
            AND until_unix_secs > extract(epoch from now())::bigint
            AND EXISTS (SELECT 1 FROM upstream_spec_v1 WHERE id = $1 AND deleted_at IS NULL)
          LIMIT 1",
    )
    .bind(id)
    .bind(holder)
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?;
    if row.is_none() {
        return Ok(false);
    }
    set_split_status(
        pool,
        id,
        UpstreamStatusUpdate {
            next_warmup_at: Some(Some(next_warmup_at)),
            ..UpstreamStatusUpdate::default()
        },
    )
    .await?;
    Ok(true)
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
    let refresh_lease_holder = row
        .try_get::<Option<String>, _>("refresh_lease_holder")
        .map_err(map_sqlx_error)?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|error| StorageError::Corrupted {
            message: format!("invalid refresh lease holder: {error}"),
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
        api_key_ciphertext: row.try_get("api_key_ciphertext").map_err(map_sqlx_error)?,
        refresh_lease_holder,
        refresh_lease_until_unix_secs: optional_i64_to_u64(
            row.try_get("refresh_lease_until_unix_secs")
                .map_err(map_sqlx_error)?,
            "refresh_lease_until_unix_secs",
        )?,
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
        created_at_unix_secs: datetime_to_unix_secs(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "created_at",
        )?,
        updated_at_unix_secs: datetime_to_unix_secs(
            row.try_get("updated_at").map_err(map_sqlx_error)?,
            "updated_at",
        )?,
        warmup_enabled: row.try_get("warmup_enabled").map_err(map_sqlx_error)?,
        next_warmup_at: row.try_get("next_warmup_at").map_err(map_sqlx_error)?,
        last_warmup_cycle_key: row
            .try_get("last_warmup_cycle_key")
            .map_err(map_sqlx_error)?,
        warmup_lease_holder: row.try_get("warmup_lease_holder").map_err(map_sqlx_error)?,
        warmup_lease_until_unix_secs: row
            .try_get("warmup_lease_until_unix_secs")
            .map_err(map_sqlx_error)?,
        warmup_dialect_plugin: row
            .try_get::<Option<serde_json::Value>, _>("warmup_dialect_plugin")
            .map_err(map_sqlx_error)?
            .map(serde_json::from_value)
            .transpose()
            .map_err(StorageError::Serialization)?,
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

fn now_unix_secs() -> u64 {
    u64::try_from(Utc::now().timestamp()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr};

    use cc_lb_storage_api::{BackendKind, MetaStore};
    use chrono::TimeZone;
    use sqlx::{
        AssertSqlSafe, PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[tokio::test]
    async fn pg_claim_warmup_lease_blocks_second_caller() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        assert!(
            !store
                .claim_warmup_lease(upstream.id, "replica-b", 60)
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.warmup_lease_holder.as_deref(), Some("replica-a"));

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pg_claim_warmup_lease_unblocks_after_ttl() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        expire_warmup_lease(&fixture.pool, upstream.id).await?;
        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-b", 60)
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.warmup_lease_holder.as_deref(), Some("replica-b"));

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pg_write_warmup_cycle_key_rejects_expired_lease() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        expire_warmup_lease(&fixture.pool, upstream.id).await?;
        assert!(
            !store
                .write_warmup_cycle_key(upstream.id, "replica-a", 1_700_000_000, Some(next_at(1)))
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.last_warmup_cycle_key, None);
        assert_eq!(fetched.next_warmup_at, None);

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pg_write_warmup_cycle_key_rejects_wrong_holder() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        assert!(
            !store
                .write_warmup_cycle_key(upstream.id, "replica-b", 1_700_000_000, Some(next_at(1)))
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.last_warmup_cycle_key, None);
        assert_eq!(fetched.next_warmup_at, None);

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pg_write_warmup_cycle_key_rejects_unchanged_key() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;
        let first_next = next_at(1);
        let second_next = next_at(2);

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        assert!(
            store
                .write_warmup_cycle_key(upstream.id, "replica-a", 1_700_000_000, Some(first_next))
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.warmup_lease_holder, None);
        assert_eq!(fetched.warmup_lease_until_unix_secs, None);
        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        assert!(
            !store
                .write_warmup_cycle_key(upstream.id, "replica-a", 1_700_000_000, Some(second_next))
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert_eq!(fetched.last_warmup_cycle_key, Some(1_700_000_000));
        assert_eq!(fetched.next_warmup_at, Some(first_next));
        assert_eq!(fetched.warmup_lease_holder.as_deref(), Some("replica-a"));

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pg_write_warmup_cycle_key_rejects_soft_deleted_row() -> TestResult<()> {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let store = fixture.store();
        let upstream = create_warmup_upstream(&store).await?;

        assert!(
            store
                .claim_warmup_lease(upstream.id, "replica-a", 60)
                .await?
        );
        store.soft_delete(upstream.id, upstream.revision).await?;
        assert!(
            !store
                .write_warmup_cycle_key(upstream.id, "replica-a", 1_700_000_000, Some(next_at(1)))
                .await?
        );
        let fetched = store
            .get_by_id(upstream.id)
            .await?
            .ok_or("upstream should exist")?;
        assert!(fetched.deleted_at_unix_secs.is_some());
        assert_eq!(fetched.last_warmup_cycle_key, None);

        fixture.drop_schema().await?;
        Ok(())
    }

    struct Fixture {
        schema: String,
        admin_pool: PgPool,
        pool: PgPool,
    }

    impl Fixture {
        async fn create() -> TestResult<Option<Self>> {
            let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
                eprintln!("skip: CI_POSTGRES_URL not set");
                return Ok(None);
            };
            let schema = format!("test_upstream_warmup_{}", Uuid::new_v4().simple());
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(PgConnectOptions::from_str(&url)?)
                .await?;
            sqlx::query(AssertSqlSafe(format!(
                "CREATE SCHEMA {}",
                quote_ident(&schema)
            )))
            .execute(&admin_pool)
            .await?;

            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(
                    PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]),
                )
                .await?;
            let store = PostgresStorage::new(pool.clone());
            store.initialize(BackendKind::Postgres).await?;

            Ok(Some(Self {
                schema,
                admin_pool,
                pool,
            }))
        }

        fn store(&self) -> PostgresStorage {
            PostgresStorage::new(self.pool.clone())
        }

        async fn drop_schema(self) -> TestResult<()> {
            self.pool.close().await;
            sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_ident(&self.schema)
            )))
            .execute(&self.admin_pool)
            .await?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    async fn create_warmup_upstream(store: &PostgresStorage) -> StorageResult<UpstreamRecord> {
        store
            .create(UpstreamCreate {
                name: format!("warmup_{}", Uuid::new_v4().simple()),
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
            .await
    }

    async fn expire_warmup_lease(pool: &PgPool, upstream_id: Uuid) -> TestResult<()> {
        sqlx::query(
            "UPDATE upstream_lease_v1
                SET until_unix_secs = extract(epoch from now())::bigint - 1
              WHERE upstream_id = $1
                AND lease_kind = 'warmup'",
        )
        .bind(upstream_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    fn next_at(offset: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + offset, 0)
            .single()
            .expect("valid next_warmup_at")
    }

    fn quote_ident(identifier: &str) -> String {
        assert!(
            identifier
                .chars()
                .all(|character| character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || character == '_'),
            "unsafe postgres identifier: {identifier}"
        );
        format!("\"{identifier}\"")
    }
}
