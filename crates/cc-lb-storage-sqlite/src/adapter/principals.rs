use async_trait::async_trait;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageError,
    StorageResult, principal::Limit, validate_identifier,
};
use sqlx::{Row, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl PrincipalStore for SqliteStorage {
    async fn create(
        &self,
        input: PrincipalCreate,
        now_unix_secs: u64,
    ) -> StorageResult<PrincipalRecord> {
        validate_identifier("principal.name", &input.name)?;
        let id = Uuid::new_v4();
        let now = u64_to_i64(now_unix_secs, "principal.created_at")?;
        let allowed_upstreams = serde_json::to_string(&input.allowed_upstreams)?;
        let row = sqlx::query(
            "INSERT INTO principals_v1 (id, name, enabled, allowed_upstreams, created_at, updated_at) VALUES (?, ?, 1, ?, ?, ?) RETURNING id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at",
        )
        .bind(id.to_string())
        .bind(input.name)
        .bind(allowed_upstreams)
        .bind(now)
        .bind(now)
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        principal_from_row(row)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query(
            "SELECT id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at FROM principals_v1 WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query(
            "SELECT id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at FROM principals_v1 WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(self.pool())
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
            "SELECT id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at FROM principals_v1 WHERE (? OR deleted_at IS NULL) ORDER BY name ASC LIMIT ? OFFSET ?",
        )
        .bind(include_deleted)
        .bind(usize_to_i64(limit, "principal.limit")?)
        .bind(usize_to_i64(offset, "principal.offset")?)
        .fetch_all(self.pool())
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
        let now = u64_to_i64(now_unix_secs, "principal.updated_at")?;
        let allowed_upstreams = update
            .allowed_upstreams
            .map(|allowed_upstreams| serde_json::to_string(&allowed_upstreams))
            .transpose()?;
        update_principal(
            self,
            id,
            expected_revision,
            now,
            update.name,
            None,
            allowed_upstreams,
        )
        .await
    }

    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        update_principal(
            self,
            id,
            expected_revision,
            u64_to_i64(now_unix_secs, "principal.updated_at")?,
            None,
            Some(enabled),
            None,
        )
        .await
    }

    async fn soft_delete(
        &self,
        id: Uuid,
        expected_revision: u64,
        now_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let now = u64_to_i64(now_unix_secs, "principal.deleted_at")?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET deleted_at = ?, updated_at = ? WHERE id = ? AND updated_at = ? RETURNING id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at",
        )
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .bind(u64_to_i64(expected_revision, "principal.revision")?)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
        let result = sqlx::query("DELETE FROM principals_v1 WHERE id = ?")
            .bind(id.to_string())
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        expected_revision: u64,
        _error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        update_principal(
            self,
            id,
            expected_revision,
            u64_to_i64(applied_at_unix_secs, "principal.last_apply_at")?,
            None,
            None,
            None,
        )
        .await
    }
}

async fn update_principal(
    storage: &SqliteStorage,
    id: Uuid,
    expected_revision: u64,
    updated_at: i64,
    name: Option<String>,
    enabled: Option<bool>,
    allowed_upstreams: Option<String>,
) -> StorageResult<Option<PrincipalRecord>> {
    let row = sqlx::query(
        "UPDATE principals_v1
            SET name = COALESCE(?, name),
                enabled = COALESCE(?, enabled),
                allowed_upstreams = COALESCE(?, allowed_upstreams),
                updated_at = ?
          WHERE id = ? AND updated_at = ?
          RETURNING id, name, enabled, allowed_upstreams, created_at, updated_at, deleted_at",
    )
    .bind(name)
    .bind(enabled)
    .bind(allowed_upstreams)
    .bind(updated_at)
    .bind(id.to_string())
    .bind(u64_to_i64(expected_revision, "principal.revision")?)
    .fetch_optional(storage.pool())
    .await
    .map_err(map_sqlite_error)?;
    row.map(principal_from_row).transpose()
}

fn principal_from_row(row: SqliteRow) -> StorageResult<PrincipalRecord> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let allowed_upstreams = row
        .try_get::<String, _>("allowed_upstreams")
        .map_err(map_sqlx_error)?;
    let created_at = row
        .try_get::<i64, _>("created_at")
        .map_err(map_sqlx_error)?;
    let updated_at = row
        .try_get::<i64, _>("updated_at")
        .map_err(map_sqlx_error)?;
    let deleted_at = row
        .try_get::<Option<i64>, _>("deleted_at")
        .map_err(map_sqlx_error)?;
    Ok(PrincipalRecord {
        id: Uuid::parse_str(&id).map_err(|error| StorageError::Corrupted {
            message: format!("invalid principal id {id}: {error}"),
        })?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: serde_json::from_str(&allowed_upstreams)?,
        default_limits: Vec::<Limit>::new(),
        enabled: row.try_get::<i64, _>("enabled").map_err(map_sqlx_error)? != 0,
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: deleted_at
            .map(|value| i64_to_u64(value, "principal.deleted_at"))
            .transpose()?,
        revision: i64_to_u64(updated_at, "principal.revision")?,
        created_at_unix_secs: i64_to_u64(created_at, "principal.created_at")?,
        updated_at_unix_secs: i64_to_u64(updated_at, "principal.updated_at")?,
        router_terminal_strategy: Default::default(),
    })
}

fn map_sqlite_error(error: sqlx::Error) -> StorageError {
    if error
        .as_database_error()
        .is_some_and(|database_error| database_error.is_unique_violation())
    {
        return StorageError::Conflict {
            message: error.to_string(),
        };
    }
    map_sqlx_error(error)
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
