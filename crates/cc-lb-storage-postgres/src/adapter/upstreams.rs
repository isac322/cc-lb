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

macro_rules! upstream_columns {
    () => {
        "id, name, kind, base_url, enabled, oauth_credentials, api_key_ciphertext, refresh_lease_holder, refresh_lease_until, last_apply_error, last_apply_at, deleted_at, revision, created_at, updated_at, warmup_enabled, next_warmup_at, last_warmup_cycle_key, warmup_lease_holder, warmup_lease_until_unix_secs, warmup_dialect_plugin"
    };
}

#[async_trait]
impl UpstreamStore for PostgresStorage {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
        validate_identifier("upstream.name", &create.name)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let id = Uuid::new_v4();
        let row = sqlx::query(concat!(
            "INSERT INTO upstreams_v1 (id, name, kind, base_url, enabled, oauth_credentials, api_key_ciphertext, warmup_enabled, next_warmup_at, last_warmup_cycle_key, warmup_lease_holder, warmup_lease_until_unix_secs, warmup_dialect_plugin, revision, created_at, updated_at) VALUES ($1, $2, $3, $4, TRUE, NULL, $5, $6, $7, $8, $9, $10, $11, 1, NOW(), NOW()) RETURNING ",
            upstream_columns!()
        ))
        .bind(id)
        .bind(&create.name)
        .bind(create.kind.as_str())
        .bind(create.base_url.as_ref().map(ToString::to_string))
        .bind(create.api_key_ciphertext)
        .bind(create.warmup_enabled)
        .bind(create.next_warmup_at)
        .bind(create.last_warmup_cycle_key)
        .bind(create.warmup_lease_holder)
        .bind(create.warmup_lease_until_unix_secs)
        .bind(
            create
                .warmup_dialect_plugin
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        notify(&mut tx, id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        row_to_record(row)
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            upstream_columns!(),
            " FROM upstreams_v1 WHERE name = $1"
        ))
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            upstream_columns!(),
            " FROM upstreams_v1 WHERE id = $1"
        ))
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
            Some(after) => sqlx::query(concat!(
                "SELECT ",
                upstream_columns!(),
                " FROM upstreams_v1 WHERE deleted_at IS NULL AND id > $1 ORDER BY id LIMIT $2"
            ))
            .bind(after)
            .bind(u64_to_i64(limit as u64, "upstream list limit")?)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?,
            None => sqlx::query(concat!(
                "SELECT ",
                upstream_columns!(),
                " FROM upstreams_v1 WHERE deleted_at IS NULL ORDER BY id LIMIT $1"
            ))
            .bind(u64_to_i64(limit as u64, "upstream list limit")?)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?,
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
            if let Some(warmup_enabled) = update.warmup_enabled {
                record.warmup_enabled = warmup_enabled;
            }
            if let Some(next_warmup_at) = update.next_warmup_at {
                record.next_warmup_at = Some(next_warmup_at);
            }
            if let Some(last_warmup_cycle_key) = update.last_warmup_cycle_key {
                record.last_warmup_cycle_key = Some(last_warmup_cycle_key);
            }
            if let Some(warmup_lease_holder) = update.warmup_lease_holder {
                record.warmup_lease_holder = Some(warmup_lease_holder);
            }
            if let Some(warmup_lease_until_unix_secs) = update.warmup_lease_until_unix_secs {
                record.warmup_lease_until_unix_secs = Some(warmup_lease_until_unix_secs);
            }
            if let Some(warmup_dialect_plugin) = update.warmup_dialect_plugin {
                record.warmup_dialect_plugin = Some(warmup_dialect_plugin);
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
            record.refresh_lease_holder = None;
            record.refresh_lease_until_unix_secs = None;
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

    async fn claim_warmup_lease(
        &self,
        id: Uuid,
        holder: &str,
        ttl_secs: i64,
    ) -> StorageResult<bool> {
        // Lock-step with crates/cc-lb-server/src/upstream_warmup_loop.rs.
        // `next_warmup_at` is ADVISORY; `last_warmup_cycle_key` is the dedup source of truth.
        // Operators must not manually SET next_warmup_at = now() to force a fire (use fire-now endpoint).
        let row = sqlx::query(
            "UPDATE upstreams_v1
               SET warmup_lease_holder = $2,
                   warmup_lease_until_unix_secs = extract(epoch from now())::bigint + $3
             WHERE id = $1
               AND deleted_at IS NULL
               AND (warmup_lease_holder IS NULL
                    OR warmup_lease_holder = $2
                    OR warmup_lease_until_unix_secs <= extract(epoch from now())::bigint)
             RETURNING id",
        )
        .bind(id)
        .bind(holder)
        .bind(ttl_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(row.is_some())
    }

    async fn write_warmup_cycle_key(
        &self,
        id: Uuid,
        holder: &str,
        new_cycle_key: i64,
        next_warmup_at: Option<DateTime<Utc>>,
    ) -> StorageResult<bool> {
        // Lock-step with crates/cc-lb-server/src/upstream_warmup_loop.rs.
        // `next_warmup_at` is ADVISORY; `last_warmup_cycle_key` is the dedup source of truth.
        // Operators must not manually SET next_warmup_at = now() to force a fire (use fire-now endpoint).
        // Holder match is required; on success the cycle key and lease clear happen atomically.
        let row = sqlx::query(
            "UPDATE upstreams_v1
               SET last_warmup_cycle_key = $3,
                   next_warmup_at = $4,
                   warmup_lease_holder = NULL,
                   warmup_lease_until_unix_secs = NULL
              WHERE id = $1
                AND warmup_lease_holder = $2
                AND warmup_lease_until_unix_secs > extract(epoch from now())::bigint
               AND last_warmup_cycle_key IS DISTINCT FROM $3
               AND deleted_at IS NULL
             RETURNING id",
        )
        .bind(id)
        .bind(holder)
        .bind(new_cycle_key)
        .bind(next_warmup_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(row.is_some())
    }

    async fn release_warmup_lease(&self, id: Uuid, holder: &str) -> StorageResult<bool> {
        let row = sqlx::query(
            "UPDATE upstreams_v1
               SET warmup_lease_holder = NULL,
                   warmup_lease_until_unix_secs = NULL
             WHERE id = $1
               AND warmup_lease_holder = $2
               AND deleted_at IS NULL
             RETURNING id",
        )
        .bind(id)
        .bind(holder)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(row.is_some())
    }

    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let row = sqlx::query(concat!(
            "UPDATE upstreams_v1
               SET warmup_dialect_plugin = NULL,
                   revision = revision + 1,
                   updated_at = NOW()
             WHERE id = $1 AND revision = $2
             RETURNING ",
            upstream_columns!()
        ))
        .bind(id)
        .bind(u64_to_i64(expected_revision, "upstream revision")?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if row.is_some() {
            notify(&mut tx, id).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
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
        let row = sqlx::query(
            "UPDATE upstreams_v1
               SET next_warmup_at = $3
             WHERE id = $1
               AND warmup_lease_holder = $2
               AND warmup_lease_until_unix_secs > extract(epoch from now())::bigint
               AND deleted_at IS NULL
             RETURNING id",
        )
        .bind(id)
        .bind(holder)
        .bind(next_warmup_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(row.is_some())
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
    let row = sqlx::query(concat!(
        "SELECT ",
        upstream_columns!(),
        " FROM upstreams_v1 WHERE id = $1 FOR UPDATE"
    ))
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
    sqlx::query(concat!(
        "UPDATE upstreams_v1 SET name = $2, kind = $3, base_url = $4, enabled = $5, oauth_credentials = $6, api_key_ciphertext = $7, refresh_lease_holder = $8, refresh_lease_until = to_timestamp($9), last_apply_error = $10, last_apply_at = to_timestamp($11), deleted_at = to_timestamp($12), warmup_enabled = $13, next_warmup_at = $14, last_warmup_cycle_key = $15, warmup_lease_holder = $16, warmup_lease_until_unix_secs = $17, warmup_dialect_plugin = $18, revision = $19, updated_at = to_timestamp($20) WHERE id = $1 RETURNING ",
        upstream_columns!()
    ))
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
    .bind(record.warmup_enabled)
    .bind(record.next_warmup_at)
    .bind(record.last_warmup_cycle_key)
    .bind(&record.warmup_lease_holder)
    .bind(record.warmup_lease_until_unix_secs)
    .bind(
        record
            .warmup_dialect_plugin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?,
    )
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
            "UPDATE upstreams_v1
                SET warmup_lease_until_unix_secs = extract(epoch from now())::bigint - 1
              WHERE id = $1",
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
