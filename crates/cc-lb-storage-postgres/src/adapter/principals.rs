use async_trait::async_trait;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageError,
    StorageResult, validate_identifier,
};
use chrono::{DateTime, Utc};
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
        let allowed_upstreams = serde_json::to_value(&input.allowed_upstreams)?;
        let default_limits = serde_json::to_value(&input.default_limits)?;
        let row = sqlx::query(
            "INSERT INTO principals_v1 (id, name, kind, allowed_models, allowed_upstreams, default_limits, enabled, revision, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, TRUE, 0, $7, $7) RETURNING *",
        )
        .bind(id)
        .bind(input.name)
        .bind(principal_kind_to_str(input.kind))
        .bind(allowed_models)
        .bind(allowed_upstreams)
        .bind(default_limits)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let record = principal_from_row(row)?;
        self.notify_principal_changed(record.id).await?;
        Ok(record)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query("SELECT * FROM principals_v1 WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query("SELECT * FROM principals_v1 WHERE name = $1")
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
            "SELECT * FROM principals_v1 WHERE ($1 OR deleted_at IS NULL) ORDER BY name ASC OFFSET $2 LIMIT $3",
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
        let now = unix_secs_to_datetime(now_unix_secs, "principal.updated_at")?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET name = $2, allowed_models = $3, allowed_upstreams = $4, default_limits = $5, revision = revision + 1, updated_at = $6 WHERE id = $1 AND revision = $7 RETURNING *",
        )
        .bind(id)
        .bind(name)
        .bind(serde_json::to_value(allowed_models)?)
        .bind(serde_json::to_value(allowed_upstreams)?)
        .bind(serde_json::to_value(default_limits)?)
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
        self.update_with_query(
            id,
            expected_revision,
            "UPDATE principals_v1 SET deleted_at = $3, updated_at = $3, revision = revision + 1 WHERE id = $1 AND revision = $2 RETURNING *",
            now,
        )
        .await
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
        let audit_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_log_v1 WHERE principal_id = $1")
                .bind(id.to_string())
                .fetch_one(&self.pool)
                .await
                .map_err(map_sqlx_error)?;
        if audit_count > 0 {
            return Err(conflict(format!(
                "postgres principal {id} is referenced by audit entries"
            )));
        }
        let result = sqlx::query("DELETE FROM principals_v1 WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        if result.rows_affected() == 1 {
            self.notify_principal_changed(id).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        expected_revision: u64,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let applied_at = unix_secs_to_datetime(applied_at_unix_secs, "principal.last_apply_at")?;
        let Some(current) = self.get_by_id(id).await? else {
            return Ok(None);
        };
        if current.revision != expected_revision {
            return Err(conflict(format!(
                "stale postgres principal revision; current revision is {}",
                current.revision
            )));
        }
        let row = sqlx::query(
            "UPDATE principals_v1 SET last_apply_error = $3, last_apply_at = $4, updated_at = $4, revision = revision + 1 WHERE id = $1 AND revision = $2 RETURNING *",
        )
        .bind(id)
        .bind(u64_to_i64(expected_revision, "principal.revision")?)
        .bind(error)
        .bind(applied_at)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
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

    async fn update_with_query(
        &self,
        id: Uuid,
        expected_revision: u64,
        query: &str,
        value: DateTime<Utc>,
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
        let row = sqlx::query(AssertSqlSafe(query.to_owned()))
            .bind(id)
            .bind(u64_to_i64(expected_revision, "principal.revision")?)
            .bind(value)
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
    let allowed_upstreams: Value = row.try_get("allowed_upstreams").map_err(map_sqlx_error)?;
    let default_limits: Value = row.try_get("default_limits").map_err(map_sqlx_error)?;
    let created_at: DateTime<Utc> = row.try_get("created_at").map_err(map_sqlx_error)?;
    let updated_at: DateTime<Utc> = row.try_get("updated_at").map_err(map_sqlx_error)?;
    let last_apply_at: Option<DateTime<Utc>> =
        row.try_get("last_apply_at").map_err(map_sqlx_error)?;
    let deleted_at: Option<DateTime<Utc>> = row.try_get("deleted_at").map_err(map_sqlx_error)?;
    Ok(PrincipalRecord {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind: principal_kind_from_str(
            row.try_get::<String, _>("kind")
                .map_err(map_sqlx_error)?
                .as_str(),
        )?,
        allowed_models: serde_json::from_value(allowed_models)?,
        allowed_upstreams: serde_json::from_value(allowed_upstreams)?,
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
    })
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
