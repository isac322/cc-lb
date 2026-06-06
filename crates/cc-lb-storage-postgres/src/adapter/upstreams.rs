use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore, UpstreamUpdate,
    validate_identifier,
};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Transaction};
use url::Url;
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, conflict, datetime_to_unix_secs, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

const CHANNEL: &str = "cclb_upstream_changed";

#[async_trait]
impl UpstreamStore for PostgresStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let id = Uuid::new_v4();
        let row = sqlx::query(
            "INSERT INTO upstreams_v1 (id, name, kind, base_url, enabled, oauth_credentials, api_key_ciphertext, revision, created_at, updated_at) VALUES ($1, $2, $3, $4, TRUE, NULL, $5, 1, NOW(), NOW()) RETURNING *",
        )
        .bind(id)
        .bind(&create.name)
        .bind(create.kind.as_str())
        .bind(create.base_url.as_ref().map(ToString::to_string))
        .bind(create.api_key_ciphertext)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        notify(&mut tx, id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        row_to_record(row)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        let row = sqlx::query("SELECT * FROM upstreams_v1 WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        let row = sqlx::query("SELECT * FROM upstreams_v1 WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = match after {
            Some(after) => {
                sqlx::query("SELECT * FROM upstreams_v1 WHERE deleted_at IS NULL AND id > $1 ORDER BY id LIMIT $2")
                    .bind(after)
                    .bind(u64_to_i64(limit as u64, "upstream list limit")?)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(map_sqlx_error)?
            }
            None => {
                sqlx::query("SELECT * FROM upstreams_v1 WHERE deleted_at IS NULL ORDER BY id LIMIT $1")
                    .bind(u64_to_i64(limit as u64, "upstream list limit")?)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(map_sqlx_error)?
            }
        };
        rows.into_iter().map(row_to_record).collect()
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
        mutate_with_revision(&self.pool, id, Some(expected_revision), |record| {
            if let Some(name) = update.name {
                record.name = name;
            }
            if let Some(base_url) = update.base_url {
                record.base_url = Some(base_url);
            }
            if let Some(api_key_ciphertext) = update.api_key_ciphertext {
                record.api_key_ciphertext = Some(api_key_ciphertext);
            }
            Ok(())
        })
        .await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord> {
        mutate_with_revision(&self.pool, id, Some(expected_revision), |record| {
            record.enabled = enabled;
            Ok(())
        })
        .await
    }

    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        mutate_with_revision(&self.pool, id, Some(expected_revision), |record| {
            record.oauth_credentials = Some(tokens);
            Ok(())
        })
        .await
    }

    async fn claim_refresh_lease(
        &self,
        id: Uuid,
        holder: Uuid,
        ttl_secs: u64,
    ) -> StorageResult<bool> {
        let now = now_unix_secs();
        let mut claimed = false;
        mutate_with_revision(&self.pool, id, None, |record| {
            let expired = record
                .refresh_lease_until_unix_secs
                .map(|lease_until| lease_until <= now)
                .unwrap_or(true);
            if expired || record.refresh_lease_holder == Some(holder) {
                record.refresh_lease_holder = Some(holder);
                record.refresh_lease_until_unix_secs = Some(now.saturating_add(ttl_secs));
                claimed = true;
            }
            Ok(())
        })
        .await?;
        Ok(claimed)
    }

    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord> {
        mutate_with_revision(&self.pool, id, None, |record| {
            if record.refresh_lease_holder != Some(holder) {
                return Err(conflict("refresh lease holder mismatch"));
            }
            record.oauth_credentials = Some(tokens);
            record.refresh_lease_holder = None;
            record.refresh_lease_until_unix_secs = None;
            record.last_apply_error = None;
            Ok(())
        })
        .await
    }

    async fn release_lease_on_failure(
        &self,
        id: Uuid,
        holder: Uuid,
        reason: String,
    ) -> StorageResult<()> {
        mutate_with_revision(&self.pool, id, None, |record| {
            if record.refresh_lease_holder != Some(holder) {
                return Err(conflict("refresh lease holder mismatch"));
            }
            record.last_apply_error = Some(reason);
            record.last_apply_at_unix_secs = Some(now_unix_secs());
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()> {
        mutate_with_revision(&self.pool, id, None, |record| {
            record.last_apply_error = error;
            record.last_apply_at_unix_secs = Some(now_unix_secs());
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()> {
        mutate_with_revision(&self.pool, id, Some(expected_revision), |record| {
            record.deleted_at_unix_secs = Some(now_unix_secs());
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("DELETE FROM upstreams_v1 WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        notify(&mut tx, id).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }
}

async fn mutate_with_revision<F>(
    pool: &sqlx::PgPool,
    id: Uuid,
    expected_revision: Option<u64>,
    mutate: F,
) -> StorageResult<UpstreamRecord>
where
    F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
{
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    let row = sqlx::query("SELECT * FROM upstreams_v1 WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| conflict("upstream not found"))?;
    let mut record = row_to_record(row)?;
    if let Some(expected_revision) = expected_revision
        && record.revision != expected_revision
    {
        return Err(conflict("stale upstream revision"));
    }
    mutate(&mut record)?;
    record.revision = record
        .revision
        .checked_add(1)
        .ok_or_else(|| StorageError::Fatal {
            message: "upstream revision overflow".to_owned(),
        })?;
    record.updated_at_unix_secs = now_unix_secs();
    let row = upsert_record(&mut tx, &record).await?;
    notify(&mut tx, id).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    row_to_record(row)
}

async fn upsert_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &UpstreamRecord,
) -> StorageResult<sqlx::postgres::PgRow> {
    sqlx::query(
        "UPDATE upstreams_v1 SET name = $2, kind = $3, base_url = $4, enabled = $5, oauth_credentials = $6, api_key_ciphertext = $7, refresh_lease_holder = $8, refresh_lease_until = to_timestamp($9), last_apply_error = $10, last_apply_at = to_timestamp($11), deleted_at = to_timestamp($12), revision = $13, updated_at = to_timestamp($14) WHERE id = $1 RETURNING *",
    )
    .bind(record.id)
    .bind(&record.name)
    .bind(record.kind.as_str())
    .bind(record.base_url.as_ref().map(ToString::to_string))
    .bind(record.enabled)
    .bind(record.oauth_credentials.as_ref().map(|tokens| tokens.ciphertext().to_vec()))
    .bind(&record.api_key_ciphertext)
    .bind(record.refresh_lease_holder)
    .bind(record.refresh_lease_until_unix_secs.map(|value| value as f64))
    .bind(&record.last_apply_error)
    .bind(record.last_apply_at_unix_secs.map(|value| value as f64))
    .bind(record.deleted_at_unix_secs.map(|value| value as f64))
    .bind(u64_to_i64(record.revision, "upstream revision")?)
    .bind(record.updated_at_unix_secs as f64)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx_error)
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

fn row_to_record(row: sqlx::postgres::PgRow) -> StorageResult<UpstreamRecord> {
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
        api_key_ciphertext: row.try_get("api_key_ciphertext").map_err(map_sqlx_error)?,
        refresh_lease_holder: row
            .try_get("refresh_lease_holder")
            .map_err(map_sqlx_error)?,
        refresh_lease_until_unix_secs: optional_ts(
            row.try_get("refresh_lease_until").map_err(map_sqlx_error)?,
            "refresh_lease_until",
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
            "upstream revision",
        )?,
        created_at_unix_secs: datetime_to_unix_secs(
            row.try_get("created_at").map_err(map_sqlx_error)?,
            "created_at",
        )?,
        updated_at_unix_secs: datetime_to_unix_secs(
            row.try_get("updated_at").map_err(map_sqlx_error)?,
            "updated_at",
        )?,
    })
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
