use async_trait::async_trait;
use cc_lb_storage_api::{
    AccountOwnerBinding, CodexFileResource, CodexFileState, CodexFileStore,
    OwnerBindingExpectation, StorageError, StorageResult,
};
use sqlx::{Row, sqlite::SqliteRow};
use uuid::Uuid;

use super::account_owner::validate_owner_binding_in_tx;
use crate::{SqliteStorage, map_sqlx_error};

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| conflict(format!("{field} exceeds storage range")))
}

fn decode(row: SqliteRow) -> StorageResult<CodexFileResource> {
    let upstream = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let upstream_id = Uuid::parse_str(&upstream).map_err(|error| StorageError::Corrupted {
        message: format!("invalid codex file upstream id: {error}"),
    })?;
    let state = match row
        .try_get::<String, _>("state")
        .map_err(map_sqlx_error)?
        .as_str()
    {
        "created" => CodexFileState::Created,
        "finalized" => CodexFileState::Finalized,
        value => {
            return Err(StorageError::Corrupted {
                message: format!("unknown codex file state: {value}"),
            });
        }
    };
    Ok(CodexFileResource {
        file_id: row.try_get("file_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        provider: row.try_get("provider").map_err(map_sqlx_error)?,
        kind: row.try_get("kind").map_err(map_sqlx_error)?,
        owner_binding_id: row.try_get("owner_binding_id").map_err(map_sqlx_error)?,
        owner_id: row.try_get("owner_id").map_err(map_sqlx_error)?,
        upstream_id,
        owner_generation: i64_to_u64(
            row.try_get("owner_generation").map_err(map_sqlx_error)?,
            "owner_generation",
        )?,
        binding_revision: i64_to_u64(
            row.try_get("binding_revision").map_err(map_sqlx_error)?,
            "binding_revision",
        )?,
        workspace_id: row.try_get("workspace_id").map_err(map_sqlx_error)?,
        state,
        created_at_unix_secs: i64_to_u64(
            row.try_get("created_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "created_at_unix_secs",
        )?,
        expires_at_unix_secs: row
            .try_get::<Option<i64>, _>("expires_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|v| i64_to_u64(v, "expires_at_unix_secs"))
            .transpose()?,
        reference: row.try_get("reference").map_err(map_sqlx_error)?,
    })
}

fn ensure_bound(resource: &CodexFileResource, binding: &AccountOwnerBinding) -> StorageResult<()> {
    if resource.principal_id != binding.principal_id
        || resource.provider != binding.provider
        || resource.owner_binding_id != binding.binding_id
        || resource.owner_id != binding.owner_id
        || resource.upstream_id != binding.upstream_id
        || resource.owner_generation != binding.owner_generation
    {
        return Err(conflict("codex file owner binding does not match"));
    }
    Ok(())
}

const SELECT: &str = "SELECT * FROM codex_files_v1 WHERE principal_id = ? AND provider = ? AND kind = ? AND owner_binding_id = ? AND owner_generation = ? AND file_id = ?";

#[async_trait]
impl CodexFileStore for SqliteStorage {
    async fn create_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        resource: CodexFileResource,
    ) -> StorageResult<()> {
        if resource.state != CodexFileState::Created || resource.reference.is_some() {
            return Err(conflict(
                "codex file create must be an unfinalized resource",
            ));
        }
        let mut tx = self.begin_immediate().await?;
        let binding = validate_owner_binding_in_tx(&mut tx, expected).await?;
        ensure_bound(&resource, &binding)?;
        if resource.binding_revision != binding.revision
            || resource.workspace_id != expected.workspace_id
        {
            return Err(conflict("codex file creation snapshot does not match"));
        }
        sqlx::query("INSERT INTO codex_files_v1 (file_id, principal_id, provider, kind, owner_binding_id, owner_id, upstream_id, owner_generation, binding_revision, workspace_id, state, created_at_unix_secs, expires_at_unix_secs, reference) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?, NULL)")
            .bind(&resource.file_id).bind(&resource.principal_id).bind(&resource.provider).bind(&resource.kind)
            .bind(&resource.owner_binding_id).bind(&resource.owner_id).bind(resource.upstream_id.to_string())
            .bind(u64_to_i64(resource.owner_generation, "owner_generation")?).bind(u64_to_i64(resource.binding_revision, "binding_revision")?)
            .bind(&resource.workspace_id).bind(u64_to_i64(resource.created_at_unix_secs, "created_at_unix_secs")?)
            .bind(resource.expires_at_unix_secs.map(|v| u64_to_i64(v, "expires_at_unix_secs")).transpose()?)
            .execute(&mut *tx).await.map_err(|error| {
                if error.as_database_error().is_some_and(|db| db.is_unique_violation()) { conflict("codex file id already exists") } else { map_sqlx_error(error) }
            })?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn finalize_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        kind: &str,
        file_id: &str,
        reference: String,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<Option<CodexFileResource>> {
        let mut tx = self.begin_immediate().await?;
        validate_owner_binding_in_tx(&mut tx, expected).await?;
        let row = sqlx::query("UPDATE codex_files_v1 SET state = 'finalized', expires_at_unix_secs = ?, reference = ? WHERE principal_id = ? AND provider = ? AND kind = ? AND owner_binding_id = ? AND owner_generation = ? AND file_id = ? RETURNING *")
            .bind(expires_at_unix_secs.map(|v| u64_to_i64(v, "expires_at_unix_secs")).transpose()?)
            .bind(reference).bind(&expected.principal_id).bind(&expected.provider).bind(kind)
            .bind(&expected.binding_id).bind(u64_to_i64(expected.owner_generation, "owner_generation")?)
            .bind(file_id).fetch_optional(&mut *tx).await.map_err(map_sqlx_error)?;
        let resource = row.map(decode).transpose()?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(resource)
    }

    async fn resolve_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        kind: &str,
        file_id: &str,
    ) -> StorageResult<Option<CodexFileResource>> {
        let mut tx = self.begin_immediate().await?;
        validate_owner_binding_in_tx(&mut tx, expected).await?;
        let row = sqlx::query(SELECT)
            .bind(&expected.principal_id)
            .bind(&expected.provider)
            .bind(kind)
            .bind(&expected.binding_id)
            .bind(u64_to_i64(expected.owner_generation, "owner_generation")?)
            .bind(file_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let resource = row.map(decode).transpose()?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(resource)
    }
}
