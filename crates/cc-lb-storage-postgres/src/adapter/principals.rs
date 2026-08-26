use async_trait::async_trait;
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, CacheKeepaliveConfig, PrincipalCreate, PrincipalKind,
    PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageError, StorageResult,
    validate_identifier,
};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row};
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage, conflict, datetime_to_unix_secs, i64_to_u64, u64_to_i64,
        unix_secs_to_datetime,
    },
    error_map::map_sqlx_error,
};

#[async_trait]
impl PrincipalStore for PostgresStorage {
    async fn create(
        &self,
        input: PrincipalCreate,
        now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord> {
        validate_identifier("principal.name", &input.name)?;
        let id = Uuid::new_v4();
        let now = unix_secs_to_datetime(now_unix_secs, "principal.now_unix_secs")?;
        let allowed_models = serde_json::to_value(&input.allowed_models)?;
        let default_limits = serde_json::to_value(&input.default_limits)?;
        let cache_keepalive = input
            .cache_keepalive
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let row = sqlx::query(
            "INSERT INTO principals_v1 (id, name, kind, allowed_models, allowed_upstreams, default_limits, enabled, revision, created_at, updated_at, cache_keepalive) VALUES ($1, $2, $3, $4, $5, $6, TRUE, 0, $7, $7, $8) RETURNING *",
        )
        .bind(id)
        .bind(input.name)
        .bind(principal_kind_to_str(input.kind))
        .bind(allowed_models)
        .bind(&input.allowed_upstreams)
        .bind(default_limits)
        .bind(now)
        .bind(cache_keepalive)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query(
            "INSERT INTO plugin_chains_v2 (id, principal_id, slot, order_value, wasm_registry_id, config, sse_per_event, batched_events_per_flush, batched_flush_ms, revision) VALUES ($1, $2, 'router', $3, $4, '{}'::jsonb, FALSE, 1, 100, 0)",
        )
        .bind(Uuid::new_v4())
        .bind(id)
        .bind(0_i64)
        .bind(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let record = principal_from_row(row)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        self.notify_principal_changed(record.id).await?;
        Ok(record)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query("SELECT id, name, kind, allowed_models, allowed_upstreams, default_limits, enabled, last_apply_error, last_apply_at, deleted_at, revision, created_at, updated_at, router_terminal_strategy, cache_keepalive FROM principals_v1 WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query("SELECT id, name, kind, allowed_models, allowed_upstreams, default_limits, enabled, last_apply_error, last_apply_at, deleted_at, revision, created_at, updated_at, router_terminal_strategy, cache_keepalive FROM principals_v1 WHERE name = $1 AND deleted_at IS NULL")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn list(
        &self,
        offset: usize,
        limit: usize,
        include_deleted: bool,
    ) -> StorageResult<Vec<PrincipalRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT id, name, kind, allowed_models, allowed_upstreams, default_limits, enabled, last_apply_error, last_apply_at, deleted_at, revision, created_at, updated_at, router_terminal_strategy, cache_keepalive FROM principals_v1 WHERE ($1 OR deleted_at IS NULL) ORDER BY name ASC OFFSET $2 LIMIT $3",
        )
        .bind(include_deleted)
        .bind(u64_to_i64(offset as u64, "principal.offset")?)
        .bind(u64_to_i64(limit as u64, "principal.limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(principal_from_row).collect()
    }

    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: PrincipalUpdate,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        if let Some(name) = update.name.as_deref() {
            validate_identifier("principal.name", name)?;
        }
        let Some(current) = self.get_by_id(id).await? else {
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(conflict(format!(
                "stale postgres principal revision; current revision is {}",
                current.revision
            )));
        }
        let name = update.name.unwrap_or(current.name);
        let allowed_models = update.allowed_models.unwrap_or(current.allowed_models);
        let allowed_upstreams = update
            .allowed_upstreams
            .unwrap_or(current.allowed_upstreams);
        let default_limits = update.default_limits.unwrap_or(current.default_limits);
        let router_terminal_strategy = update
            .router_terminal_strategy
            .unwrap_or(current.router_terminal_strategy);
        let router_terminal_strategy = terminal_strategy_to_db_value(&router_terminal_strategy)?;
        let cache_keepalive = match update.cache_keepalive {
            Some(next) => next,
            None => current.cache_keepalive,
        };
        let cache_keepalive_value = cache_keepalive
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        let now = unix_secs_to_datetime(now_unix_secs, "principal.updated_at")?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET name = $2, allowed_models = $3, allowed_upstreams = $4, default_limits = $5, router_terminal_strategy = $6, cache_keepalive = $7, revision = revision + 1, updated_at = $8 WHERE id = $1 AND revision = $9 RETURNING *",
        )
        .bind(id)
        .bind(name)
        .bind(serde_json::to_value(allowed_models)?)
        .bind(&allowed_upstreams)
        .bind(serde_json::to_value(default_limits)?)
        .bind(router_terminal_strategy)
        .bind(cache_keepalive_value)
        .bind(now)
        .bind(u64_to_i64(expected_revision, "principal.revision")?)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let record = principal_from_row(row)?;
        self.notify_principal_changed(record.id).await?;
        Ok(Some(record))
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        self.update_scalar(id, expected_revision, now_unix_secs, "enabled", enabled)
            .await
    }

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let now = unix_secs_to_datetime(now_unix_secs, "principal.deleted_at")?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let state: Option<(i64, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT revision, deleted_at FROM principals_v1 WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let Some((current_revision, deleted_at)) = state else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(None);
        };
        if deleted_at.is_some() {
            return Err(conflict(
                "stale postgres principal revision; principal is already deleted",
            ));
        }
        let current = i64_to_u64(current_revision, "principal.revision")?;
        if current != expected_revision {
            return Err(conflict(format!(
                "stale postgres principal revision; current revision is {current}"
            )));
        }
        sqlx::query("DELETE FROM plugin_chains_v2 WHERE principal_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET deleted_at = $2, updated_at = $2, revision = revision + 1 WHERE id = $1 AND revision = $3 RETURNING *",
        )
        .bind(id)
        .bind(now)
        .bind(u64_to_i64(expected_revision, "principal.revision")?)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        sqlx::query(
            "SELECT pg_notify('cclb_plugin_chain_changed', $1), pg_notify('cclb_principal_changed', $1)",
        )
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        principal_from_row(row).map(Some)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let audit_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_log_v1 WHERE principal_id = $1")
                .bind(id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        if audit_count > 0 {
            return Err(conflict(format!(
                "postgres principal {id} is referenced by audit entries"
            )));
        }
        let principal_exists: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM principals_v1 WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        if principal_exists.is_none() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM plugin_chains_v2 WHERE principal_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let result = sqlx::query("DELETE FROM principals_v1 WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let deleted = result.rows_affected() == 1;
        if deleted {
            sqlx::query(
                "SELECT pg_notify('cclb_plugin_chain_changed', $1), pg_notify('cclb_principal_changed', $1)",
            )
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted)
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let applied_at = unix_secs_to_datetime(applied_at_unix_secs, "principal.last_apply_at")?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET last_apply_error = $2, last_apply_at = $3 WHERE id = $1 RETURNING *",
        )
        .bind(id)
        .bind(error)
        .bind(applied_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = principal_from_row(row)?;
        self.notify_principal_changed(record.id).await?;
        Ok(Some(record))
    }
}

impl PostgresStorage {
    async fn update_scalar(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
        field: &str,
        enabled: bool,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let Some(current) = self.get_by_id(id).await? else {
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(conflict(format!(
                "stale postgres principal revision; current revision is {}",
                current.revision
            )));
        }
        let now = unix_secs_to_datetime(now_unix_secs, "principal.updated_at")?;
        let sql = format!(
            "UPDATE principals_v1 SET {field} = $3, updated_at = $4, revision = revision + 1 WHERE id = $1 AND revision = $2 RETURNING *"
        );
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(id)
            .bind(u64_to_i64(expected_revision, "principal.revision")?)
            .bind(enabled)
            .bind(now)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        let record = principal_from_row(row)?;
        self.notify_principal_changed(record.id).await?;
        Ok(Some(record))
    }

    async fn notify_principal_changed(&self, id: Uuid) -> StorageResult<()> {
        sqlx::query("SELECT pg_notify('cclb_principal_changed', $1)")
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }
}

fn principal_from_row(row: sqlx::postgres::PgRow) -> StorageResult<PrincipalRecord> {
    let allowed_models: Value = row.try_get("allowed_models").map_err(map_sqlx_error)?;
    let allowed_upstreams: Vec<Uuid> = row.try_get("allowed_upstreams").map_err(map_sqlx_error)?;
    let default_limits: Value = row.try_get("default_limits").map_err(map_sqlx_error)?;
    let created_at: DateTime<Utc> = row.try_get("created_at").map_err(map_sqlx_error)?;
    let updated_at: DateTime<Utc> = row.try_get("updated_at").map_err(map_sqlx_error)?;
    let last_apply_at: Option<DateTime<Utc>> =
        row.try_get("last_apply_at").map_err(map_sqlx_error)?;
    let deleted_at: Option<DateTime<Utc>> = row.try_get("deleted_at").map_err(map_sqlx_error)?;
    let router_terminal_strategy: String = row
        .try_get("router_terminal_strategy")
        .map_err(map_sqlx_error)?;
    let cache_keepalive_value: Option<Value> =
        row.try_get("cache_keepalive").map_err(map_sqlx_error)?;
    let cache_keepalive = cache_keepalive_value
        .map(serde_json::from_value::<CacheKeepaliveConfig>)
        .transpose()?;
    Ok(PrincipalRecord {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind: principal_kind_from_str(
            row.try_get::<String, _>("kind")
                .map_err(map_sqlx_error)?
                .as_str(),
        )?,
        allowed_models: serde_json::from_value(allowed_models)?,
        allowed_upstreams,
        default_limits: serde_json::from_value(default_limits)?,
        enabled: row.try_get("enabled").map_err(map_sqlx_error)?,
        last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
        last_apply_at_unix_secs: last_apply_at
            .map(|value| datetime_to_unix_secs(value, "principal.last_apply_at"))
            .transpose()?,
        deleted_at_unix_secs: deleted_at
            .map(|value| datetime_to_unix_secs(value, "principal.deleted_at"))
            .transpose()?,
        revision: i64_to_u64(
            row.try_get("revision").map_err(map_sqlx_error)?,
            "principal.revision",
        )?,
        created_at_unix_secs: datetime_to_unix_secs(created_at, "principal.created_at")?,
        updated_at_unix_secs: datetime_to_unix_secs(updated_at, "principal.updated_at")?,
        router_terminal_strategy: terminal_strategy_from_db_value(&router_terminal_strategy),
        cache_keepalive,
    })
}

fn terminal_strategy_from_db_value<T>(value: &str) -> T
where
    T: DeserializeOwned + Default,
{
    match serde_json::from_value(Value::String(value.to_owned())) {
        Ok(strategy) => strategy,
        Err(error) => {
            tracing::warn!(
                storage_backend = "postgres",
                router_terminal_strategy = %value,
                %error,
                "unexpected principal router_terminal_strategy; defaulting to first-pick"
            );
            T::default()
        }
    }
}

fn terminal_strategy_to_db_value(strategy: &impl serde::Serialize) -> StorageResult<String> {
    match serde_json::to_value(strategy)? {
        Value::String(value) => Ok(value),
        value => Err(StorageError::Fatal {
            message: format!("router_terminal_strategy serialized to non-string value {value}"),
        }),
    }
}

fn principal_kind_to_str(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::Machine => "machine",
        PrincipalKind::Human => "human",
        PrincipalKind::Admin => "admin",
    }
}

fn principal_kind_from_str(value: &str) -> StorageResult<PrincipalKind> {
    match value {
        "machine" => Ok(PrincipalKind::Machine),
        "human" => Ok(PrincipalKind::Human),
        "admin" => Ok(PrincipalKind::Admin),
        value => Err(StorageError::Corrupted {
            message: format!("invalid principal kind {value}"),
        }),
    }
}
