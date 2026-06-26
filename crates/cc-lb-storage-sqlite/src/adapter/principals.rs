use async_trait::async_trait;
use cc_lb_storage_api::{
    PrincipalCreate, PrincipalKind, PrincipalRecord, PrincipalStore, PrincipalUpdate, StorageError,
    StorageResult, validate_identifier,
};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
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
        let allowed_models = serde_json::to_string(&input.allowed_models)?;
        let allowed_upstreams = serde_json::to_string(&input.allowed_upstreams)?;
        let default_limits = serde_json::to_string(&input.default_limits)?;
        let row = sqlx::query(
            "INSERT INTO principals_v1 (id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?, ?, 'first-pick', 0, ?, ?) RETURNING id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at",
        )
        .bind(id.to_string())
        .bind(input.name)
        .bind(principal_kind_to_str(input.kind))
        .bind(allowed_models)
        .bind(allowed_upstreams)
        .bind(default_limits)
        .bind(now)
        .bind(now)
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        principal_from_row(row)
    }

    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query(
            "SELECT id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at FROM principals_v1 WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        row.map(principal_from_row).transpose()
    }

    async fn get_by_name(&self, name: &str) -> StorageResult<Option<PrincipalRecord>> {
        let row = sqlx::query(
            "SELECT id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at FROM principals_v1 WHERE name = ?",
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
            "SELECT id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at FROM principals_v1 WHERE (? OR deleted_at IS NULL) ORDER BY name ASC LIMIT ? OFFSET ?",
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
        let allowed_models = update
            .allowed_models
            .map(|allowed_models| serde_json::to_string(&allowed_models))
            .transpose()?;
        let default_limits = update
            .default_limits
            .map(|default_limits| serde_json::to_string(&default_limits))
            .transpose()?;
        let router_terminal_strategy = update
            .router_terminal_strategy
            .map(|strategy| terminal_strategy_to_db_value(&strategy))
            .transpose()?;
        update_principal(
            self,
            id,
            expected_revision,
            now,
            update.name,
            None,
            allowed_models,
            allowed_upstreams,
            default_limits,
            router_terminal_strategy,
            false,
            None,
            None,
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
            None,
            None,
            None,
            false,
            None,
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
        let mut tx = self.begin_immediate().await?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET deleted_at = ?, updated_at = ?, revision = revision + 1 WHERE id = ? AND revision = ? AND deleted_at IS NULL RETURNING id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at",
        )
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .bind(u64_to_i64(expected_revision, "principal.revision")?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        if row.is_some() {
            cascade_plugin_chains_in_tx(&mut tx, id).await?;
        }
        let record = optional_updated_principal_in_tx(&mut tx, id, row).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(record)
    }

    async fn hard_delete(&self, id: Uuid) -> StorageResult<bool> {
        let mut tx = self.begin_immediate().await?;
        let audit_refs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM audit_log_v1 WHERE principal_id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if audit_refs > 0 {
            return Err(StorageError::Conflict {
                message: "principal is referenced by audit entries".to_owned(),
            });
        }
        cascade_plugin_chains_in_tx(&mut tx, id).await?;
        let result = sqlx::query("DELETE FROM principals_v1 WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let deleted = result.rows_affected() == 1;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted)
    }

    async fn set_last_apply_error(
        &self,
        id: Uuid,
        error: Option<String>,
        applied_at_unix_secs: u64,
    ) -> StorageResult<Option<PrincipalRecord>> {
        let applied_at = u64_to_i64(applied_at_unix_secs, "principal.last_apply_at")?;
        let row = sqlx::query(
            "UPDATE principals_v1 SET last_apply_error = ?, last_apply_at = ? WHERE id = ? RETURNING id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at",
        )
        .bind(error)
        .bind(applied_at)
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlite_error)?;
        row.map(principal_from_row).transpose()
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_principal(
    storage: &SqliteStorage,
    id: Uuid,
    expected_revision: u64,
    updated_at: i64,
    name: Option<String>,
    enabled: Option<bool>,
    allowed_models: Option<String>,
    allowed_upstreams: Option<String>,
    default_limits: Option<String>,
    router_terminal_strategy: Option<String>,
    update_last_apply_error: bool,
    last_apply_error: Option<String>,
    last_apply_at: Option<i64>,
) -> StorageResult<Option<PrincipalRecord>> {
    let row = sqlx::query(
        "UPDATE principals_v1
            SET name = COALESCE(?, name),
                enabled = COALESCE(?, enabled),
                allowed_models = COALESCE(?, allowed_models),
                allowed_upstreams = COALESCE(?, allowed_upstreams),
                default_limits = COALESCE(?, default_limits),
                router_terminal_strategy = COALESCE(?, router_terminal_strategy),
                last_apply_error = CASE WHEN ? THEN ? ELSE last_apply_error END,
                last_apply_at = COALESCE(?, last_apply_at),
                revision = revision + 1,
                updated_at = ?
          WHERE id = ? AND revision = ?
          RETURNING id, name, kind, enabled, allowed_models, allowed_upstreams, default_limits, router_terminal_strategy, revision, created_at, updated_at, last_apply_error, last_apply_at, deleted_at",
    )
    .bind(name)
    .bind(enabled)
    .bind(allowed_models)
    .bind(allowed_upstreams)
    .bind(default_limits)
    .bind(router_terminal_strategy)
    .bind(update_last_apply_error)
    .bind(last_apply_error)
    .bind(last_apply_at)
    .bind(updated_at)
    .bind(id.to_string())
    .bind(u64_to_i64(expected_revision, "principal.revision")?)
    .fetch_optional(storage.pool())
    .await
    .map_err(map_sqlite_error)?;
    optional_updated_principal(storage, id, row).await
}

async fn optional_updated_principal(
    storage: &SqliteStorage,
    id: Uuid,
    row: Option<SqliteRow>,
) -> StorageResult<Option<PrincipalRecord>> {
    if let Some(row) = row {
        return principal_from_row(row).map(Some);
    }
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(1) FROM principals_v1 WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(storage.pool())
        .await
        .map_err(map_sqlx_error)?
        > 0;
    if exists {
        return Err(StorageError::Conflict {
            message: "principal revision conflict".to_owned(),
        });
    }
    Ok(None)
}

async fn optional_updated_principal_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
    row: Option<SqliteRow>,
) -> StorageResult<Option<PrincipalRecord>> {
    if let Some(row) = row {
        return principal_from_row(row).map(Some);
    }
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(1) FROM principals_v1 WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        > 0;
    if exists {
        return Err(StorageError::Conflict {
            message: "principal revision conflict".to_owned(),
        });
    }
    Ok(None)
}

async fn cascade_plugin_chains_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    principal_id: Uuid,
) -> StorageResult<()> {
    let shas = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT DISTINCT wasm_registry_v2.sha256 FROM plugin_chains_v2 JOIN wasm_registry_v2 ON wasm_registry_v2.id = plugin_chains_v2.wasm_registry_id WHERE plugin_chains_v2.principal_id = ?",
    )
    .bind(principal_id.to_string())
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    sqlx::query("DELETE FROM plugin_chains_v2 WHERE principal_id = ?")
        .bind(principal_id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlite_error)?;

    for sha in shas {
        sqlx::query(
            "UPDATE wasm_blobs_v2 SET refcount = (SELECT COUNT(1) FROM plugin_chains_v2 JOIN wasm_registry_v2 ON wasm_registry_v2.id = plugin_chains_v2.wasm_registry_id WHERE wasm_registry_v2.sha256 = wasm_blobs_v2.sha256) WHERE sha256 = ?",
        )
        .bind(sha.as_slice())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    }
    Ok(())
}

fn principal_from_row(row: SqliteRow) -> StorageResult<PrincipalRecord> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let allowed_models = row
        .try_get::<String, _>("allowed_models")
        .map_err(map_sqlx_error)?;
    let allowed_upstreams = row
        .try_get::<String, _>("allowed_upstreams")
        .map_err(map_sqlx_error)?;
    let default_limits = row
        .try_get::<String, _>("default_limits")
        .map_err(map_sqlx_error)?;
    let created_at = row
        .try_get::<i64, _>("created_at")
        .map_err(map_sqlx_error)?;
    let updated_at = row
        .try_get::<i64, _>("updated_at")
        .map_err(map_sqlx_error)?;
    let revision = row.try_get::<i64, _>("revision").map_err(map_sqlx_error)?;
    let deleted_at = row
        .try_get::<Option<i64>, _>("deleted_at")
        .map_err(map_sqlx_error)?;
    let last_apply_at = row
        .try_get::<Option<i64>, _>("last_apply_at")
        .map_err(map_sqlx_error)?;
    Ok(PrincipalRecord {
        id: Uuid::parse_str(&id).map_err(|error| StorageError::Corrupted {
            message: format!("invalid principal id {id}: {error}"),
        })?,
        name: row.try_get("name").map_err(map_sqlx_error)?,
        kind: principal_kind_from_str(&row.try_get::<String, _>("kind").map_err(map_sqlx_error)?)?,
        allowed_models: serde_json::from_str(&allowed_models)?,
        allowed_upstreams: serde_json::from_str(&allowed_upstreams)?,
        default_limits: serde_json::from_str(&default_limits)?,
        enabled: row.try_get::<i64, _>("enabled").map_err(map_sqlx_error)? != 0,
        last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
        last_apply_at_unix_secs: last_apply_at
            .map(|value| i64_to_u64(value, "principal.last_apply_at"))
            .transpose()?,
        deleted_at_unix_secs: deleted_at
            .map(|value| i64_to_u64(value, "principal.deleted_at"))
            .transpose()?,
        revision: i64_to_u64(revision, "principal.revision")?,
        created_at_unix_secs: i64_to_u64(created_at, "principal.created_at")?,
        updated_at_unix_secs: i64_to_u64(updated_at, "principal.updated_at")?,
        router_terminal_strategy: serde_json::from_value(Value::String(
            row.try_get::<String, _>("router_terminal_strategy")
                .map_err(map_sqlx_error)?,
        ))
        .map_err(|error| StorageError::Corrupted {
            message: format!("invalid principal router_terminal_strategy: {error}"),
        })?,
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
    if value == usize::MAX {
        return Ok(i64::MAX);
    }
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite integer"),
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

fn terminal_strategy_to_db_value(strategy: &impl serde::Serialize) -> StorageResult<String> {
    match serde_json::to_value(strategy)? {
        Value::String(value) => Ok(value),
        value => Err(StorageError::Fatal {
            message: format!("router_terminal_strategy serialized to non-string value {value}"),
        }),
    }
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
