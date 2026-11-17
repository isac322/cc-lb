use async_trait::async_trait;
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, AccountOwnerWorkspaceTarget, OwnerBindingExpectation,
    StorageError, StorageResult, UpstreamRecordId, validate_owner_binding_for_create,
    validate_owner_binding_replacement,
};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const SELECT_BINDING: &str =
    "SELECT binding_id, principal_id, provider, owner_id, upstream_id, owner_generation,
            local_user_id, allowed_workspaces_json, provider_workspace_id, outbound_region,
            revision, revoked_at_unix_secs
     FROM account_owner_bindings_v1 WHERE binding_id = ?";
const SELECT_WORKSPACE_BINDING: &str =
    "SELECT binding_id, principal_id, provider, owner_id, upstream_id, owner_generation,
            local_user_id, allowed_workspaces_json, provider_workspace_id, outbound_region,
            revision, revoked_at_unix_secs
     FROM account_owner_bindings_v1
     WHERE binding_id = (
         SELECT binding_id FROM account_owner_workspaces_v1
         WHERE principal_id = ? AND provider = ? AND workspace_id = ?
     )";

#[async_trait]
impl AccountOwnerStore for SqliteStorage {
    async fn create_owner_binding(&self, binding: AccountOwnerBinding) -> StorageResult<()> {
        validate_owner_binding_for_create(&binding)?;

        let mut tx = self.begin_immediate().await?;
        let duplicate: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM account_owner_bindings_v1 WHERE binding_id = ?")
                .bind(&binding.binding_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
        if duplicate.is_some() {
            return Err(conflict("owner binding id already exists or is tombstoned"));
        }
        ensure_upstream_provider_in_tx(&mut tx, binding.upstream_id, &binding.provider, true)
            .await?;
        ensure_principal_exists_in_tx(&mut tx, &binding.principal_id).await?;
        insert_owner_binding_in_tx(&mut tx, &binding).await?;
        reserve_workspace_aliases_in_tx(&mut tx, &binding).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn get_owner_binding(
        &self,
        binding_id: &str,
    ) -> StorageResult<Option<AccountOwnerBinding>> {
        let row = sqlx::query(SELECT_BINDING)
            .bind(binding_id)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        row.map(binding_from_row).transpose()
    }

    async fn find_owner_binding_for_workspace(
        &self,
        target: &AccountOwnerWorkspaceTarget,
    ) -> StorageResult<AccountOwnerBinding> {
        let row = sqlx::query(SELECT_WORKSPACE_BINDING)
            .bind(&target.principal_id)
            .bind(&target.provider)
            .bind(&target.workspace_id)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?
            .ok_or_else(|| conflict("workspace owner binding does not exist"))?;
        let binding = binding_from_row(row)?;
        if binding.principal_id != target.principal_id
            || binding.provider != target.provider
            || binding.local_user_id != target.local_user_id
            || binding.revoked_at_unix_secs.is_some()
            || !binding.allowed_workspaces.contains(&target.workspace_id)
        {
            return Err(conflict("workspace owner binding is not authorized"));
        }
        Ok(binding)
    }

    async fn list_owner_workspace_targets(
        &self,
        principal_id: &str,
        provider: &str,
        local_user_id: &str,
    ) -> StorageResult<Vec<String>> {
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let aliases =
            list_owner_workspace_aliases_in_read_tx(&mut tx, principal_id, provider, local_user_id)
                .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(aliases)
    }

    async fn compare_and_replace_owner_binding(
        &self,
        binding: AccountOwnerBinding,
        expected_revision: u64,
    ) -> StorageResult<()> {
        let expected_revision_i64 = u64_to_i64(expected_revision, "owner binding revision")?;
        let mut tx = self.begin_immediate().await?;

        let row = sqlx::query(SELECT_BINDING)
            .bind(&binding.binding_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .ok_or_else(|| conflict("owner binding does not exist"))?;
        let existing = binding_from_row(row)?;

        validate_owner_binding_replacement(&existing, &binding, expected_revision)?;
        let identity_changed = existing.provider != binding.provider
            || existing.owner_id != binding.owner_id
            || existing.upstream_id != binding.upstream_id
            || existing.provider_workspace_id != binding.provider_workspace_id;
        let permission_withdrawal = !identity_changed
            && binding.outbound_region == existing.outbound_region
            && binding
                .allowed_workspaces
                .iter()
                .all(|workspace| existing.allowed_workspaces.contains(workspace));
        let require_live_upstream =
            identity_changed || (binding.revoked_at_unix_secs.is_none() && !permission_withdrawal);

        // A tombstone or a same-identity permission withdrawal remains
        // writable while its old upstream is dead. New grants and identity
        // moves require a live, enabled, healthy target.
        ensure_upstream_provider_in_tx(
            &mut tx,
            binding.upstream_id,
            &binding.provider,
            require_live_upstream,
        )
        .await?;
        let allowed_workspaces_json = serde_json::to_string(&binding.allowed_workspaces)?;
        let revision = u64_to_i64(binding.revision, "owner binding revision")?;
        let owner_generation = u64_to_i64(binding.owner_generation, "owner generation")?;
        let revoked_at = binding
            .revoked_at_unix_secs
            .map(|value| u64_to_i64(value, "revoked_at_unix_secs"))
            .transpose()?;

        let result = sqlx::query(
            "UPDATE account_owner_bindings_v1
             SET provider = ?, owner_id = ?, upstream_id = ?, owner_generation = ?,
                 allowed_workspaces_json = ?, provider_workspace_id = ?, outbound_region = ?,
                 revision = ?, revoked_at_unix_secs = ?
             WHERE binding_id = ? AND revision = ? AND revoked_at_unix_secs IS NULL",
        )
        .bind(&binding.provider)
        .bind(&binding.owner_id)
        .bind(binding.upstream_id.to_string())
        .bind(owner_generation)
        .bind(allowed_workspaces_json)
        .bind(&binding.provider_workspace_id)
        .bind(&binding.outbound_region)
        .bind(revision)
        .bind(revoked_at)
        .bind(&binding.binding_id)
        .bind(expected_revision_i64)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlite_error)?;
        if result.rows_affected() != 1 {
            return Err(conflict("owner binding revision is stale or terminal"));
        }
        reserve_workspace_aliases_in_tx(&mut tx, &binding).await?;

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn resolve_owner_binding(
        &self,
        expected: &OwnerBindingExpectation,
    ) -> StorageResult<AccountOwnerBinding> {
        let mut tx = self.begin_immediate().await?;
        let binding = validate_owner_binding_in_tx(&mut tx, expected).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(binding)
    }
}

/// Lists local aliases in the caller's read snapshot without granting authority.
/// This helper neither obtains a writer lock nor changes any account state.
pub(crate) async fn list_owner_workspace_aliases_in_read_tx(
    tx: &mut Transaction<'_, Sqlite>,
    principal_id: &str,
    provider: &str,
    local_user_id: &str,
) -> StorageResult<Vec<String>> {
    let principal = Uuid::parse_str(principal_id)
        .map_err(|_| conflict("principal id must be a canonical UUID"))?;
    let mut canonical = [0_u8; 36];
    if principal.hyphenated().encode_lower(&mut canonical) != principal_id {
        return Err(conflict("principal id must be a canonical UUID"));
    }
    sqlx::query_scalar(
        "SELECT alias.workspace_id COLLATE BINARY
         FROM account_owner_workspaces_v1 alias
         JOIN account_owner_bindings_v1 binding ON binding.binding_id = alias.binding_id
         JOIN principals_v1 principal ON principal.id = alias.principal_id
         JOIN upstream_spec_v1 upstream ON upstream.id = binding.upstream_id
         LEFT JOIN upstream_status_v1 status ON status.upstream_id = upstream.id
         WHERE alias.principal_id = ? AND alias.provider = ?
           AND binding.principal_id = alias.principal_id
           AND binding.provider = alias.provider AND binding.local_user_id = ?
           AND binding.revoked_at_unix_secs IS NULL
           AND principal.enabled = 1 AND principal.deleted_at IS NULL
           AND upstream.enabled = 1 AND upstream.deleted_at IS NULL
           AND status.last_apply_error IS NULL
           AND binding.provider IN ('anthropic', 'openai', 'gemini')
           AND upstream.kind IN (binding.provider || '_api_key', binding.provider || '_oauth')
           AND EXISTS (
               SELECT 1 FROM json_each(binding.allowed_workspaces_json) allowed
               WHERE allowed.type = 'text' AND allowed.value = alias.workspace_id
           )
           AND (
               json_array_length(principal.allowed_upstreams) = 0
               OR EXISTS (
                   SELECT 1 FROM json_each(principal.allowed_upstreams) allowed
                   WHERE allowed.type = 'text' AND allowed.value = upstream.id
               )
           )
         ORDER BY alias.workspace_id COLLATE BINARY",
    )
    .bind(principal_id)
    .bind(provider)
    .bind(local_user_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)
}

/// Validates a fresh tuple inside the transaction used for the protected write.
///
/// The caller MUST start this transaction with `BEGIN IMMEDIATE` (for example,
/// `SqliteStorage::begin_immediate`). Authorization precedes upstream health,
/// and the writer lock keeps the validated binding stable until commit.
pub(crate) async fn validate_owner_binding_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    expected: &OwnerBindingExpectation,
) -> StorageResult<AccountOwnerBinding> {
    let (source, _) = validate_owner_bindings_in_tx(tx, expected, None).await?;
    Ok(source)
}

/// Validates a binding and, optionally, both ends of a workspace handoff in the
/// protected-write transaction. The caller MUST hold a `BEGIN IMMEDIATE` writer
/// transaction. With no target, the source is returned without cloning it.
pub(crate) async fn validate_owner_bindings_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    source_expected: &OwnerBindingExpectation,
    target_expected: Option<&OwnerBindingExpectation>,
) -> StorageResult<(AccountOwnerBinding, Option<AccountOwnerBinding>)> {
    if let Some(target_expected) = target_expected
        && (source_expected.principal_id != target_expected.principal_id
            || source_expected.provider != target_expected.provider)
    {
        return Err(conflict(
            "owner binding handoff principal or provider differs",
        ));
    }
    // Validate both authorization predicates before consulting either upstream.
    // BEGIN IMMEDIATE holds the writer lock across these reads and health checks,
    // so neither binding can change between authorization and the caller's write.
    let source_row = sqlx::query(SELECT_BINDING)
        .bind(&source_expected.binding_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| conflict("source owner binding does not exist"))?;
    let source = binding_from_row(source_row)?;
    validate_expectation(&source, source_expected)?;
    let target = if let Some(target_expected) = target_expected {
        let target_row = sqlx::query(SELECT_BINDING)
            .bind(&target_expected.binding_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_error)?
            .ok_or_else(|| conflict("target owner binding does not exist"))?;
        let target = binding_from_row(target_row)?;
        validate_expectation(&target, target_expected)?;
        if source.principal_id != target.principal_id
            || source.provider != target.provider
            || source.local_user_id != target.local_user_id
        {
            return Err(conflict("owner binding handoff identity differs"));
        }
        Some(target)
    } else {
        None
    };
    ensure_upstream_provider_in_tx(tx, source.upstream_id, &source.provider, true).await?;
    if let Some(target) = &target {
        ensure_upstream_provider_in_tx(tx, target.upstream_id, &target.provider, true).await?;
    }
    Ok((source, target))
}

fn validate_expectation(
    binding: &AccountOwnerBinding,
    expected: &OwnerBindingExpectation,
) -> StorageResult<()> {
    if binding.principal_id != expected.principal_id
        || binding.provider != expected.provider
        || binding.owner_generation != expected.owner_generation
        || binding.revision != expected.binding_revision
        || binding.revoked_at_unix_secs.is_some()
        || !binding.allowed_workspaces.contains(&expected.workspace_id)
    {
        return Err(conflict("owner binding expectation does not match"));
    }
    Ok(())
}

async fn ensure_upstream_provider_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    upstream_id: UpstreamRecordId,
    provider: &str,
    require_live: bool,
) -> StorageResult<()> {
    let row = sqlx::query(
        "SELECT spec.kind, spec.enabled, spec.deleted_at, status.last_apply_error
         FROM upstream_spec_v1 spec
         LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id
         WHERE spec.id = ?",
    )
    .bind(upstream_id.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| upstream_unavailable("owner binding upstream does not exist"))?;
    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;
    if provider_for_upstream_kind(&kind)? != provider {
        return Err(conflict("owner binding provider does not match upstream"));
    }
    if require_live {
        let enabled: i64 = row.try_get("enabled").map_err(map_sqlx_error)?;
        let deleted_at: Option<i64> = row.try_get("deleted_at").map_err(map_sqlx_error)?;
        let last_apply_error: Option<String> =
            row.try_get("last_apply_error").map_err(map_sqlx_error)?;
        if enabled == 0 || deleted_at.is_some() || last_apply_error.is_some() {
            return Err(upstream_unavailable(
                "owner binding upstream is disabled, deleted, or unhealthy",
            ));
        }
    }
    Ok(())
}

async fn ensure_principal_exists_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    principal_id: &str,
) -> StorageResult<()> {
    let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM principals_v1 WHERE id = ?")
        .bind(principal_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    if exists.is_none() {
        return Err(conflict("owner binding principal does not exist"));
    }
    Ok(())
}

async fn reserve_workspace_aliases_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    binding: &AccountOwnerBinding,
) -> StorageResult<()> {
    for workspace_id in &binding.allowed_workspaces {
        sqlx::query(
            "INSERT INTO account_owner_workspaces_v1
             (principal_id, provider, workspace_id, binding_id)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (principal_id, provider, workspace_id) DO NOTHING",
        )
        .bind(&binding.principal_id)
        .bind(&binding.provider)
        .bind(workspace_id)
        .bind(&binding.binding_id)
        .execute(&mut **tx)
        .await
        .map_err(map_sqlite_error)?;
        let owner: String = sqlx::query_scalar(
            "SELECT binding_id FROM account_owner_workspaces_v1
             WHERE principal_id = ? AND provider = ? AND workspace_id = ?",
        )
        .bind(&binding.principal_id)
        .bind(&binding.provider)
        .bind(workspace_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
        if owner != binding.binding_id {
            return Err(conflict(
                "workspace alias is permanently bound to another binding",
            ));
        }
    }
    Ok(())
}

async fn insert_owner_binding_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    binding: &AccountOwnerBinding,
) -> StorageResult<()> {
    let allowed_workspaces_json = serde_json::to_string(&binding.allowed_workspaces)?;
    sqlx::query(
        "INSERT INTO account_owner_bindings_v1
         (binding_id, principal_id, provider, owner_id, upstream_id, owner_generation,
          local_user_id, allowed_workspaces_json, provider_workspace_id, outbound_region,
          revision, revoked_at_unix_secs)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&binding.binding_id)
    .bind(&binding.principal_id)
    .bind(&binding.provider)
    .bind(&binding.owner_id)
    .bind(binding.upstream_id.to_string())
    .bind(u64_to_i64(binding.owner_generation, "owner generation")?)
    .bind(&binding.local_user_id)
    .bind(allowed_workspaces_json)
    .bind(&binding.provider_workspace_id)
    .bind(&binding.outbound_region)
    .bind(u64_to_i64(binding.revision, "owner binding revision")?)
    .bind(
        binding
            .revoked_at_unix_secs
            .map(|value| u64_to_i64(value, "revoked_at_unix_secs"))
            .transpose()?,
    )
    .execute(&mut **tx)
    .await
    .map_err(map_sqlite_error)?;
    Ok(())
}

fn binding_from_row(row: SqliteRow) -> StorageResult<AccountOwnerBinding> {
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let upstream_id = Uuid::parse_str(&upstream_id).map_err(|error| StorageError::Corrupted {
        message: format!("invalid account owner upstream id: {error}"),
    })?;
    let owner_generation: i64 = row.try_get("owner_generation").map_err(map_sqlx_error)?;
    let revision: i64 = row.try_get("revision").map_err(map_sqlx_error)?;
    let revoked_at_unix_secs: Option<i64> = row
        .try_get("revoked_at_unix_secs")
        .map_err(map_sqlx_error)?;
    let allowed_workspaces_json: String = row
        .try_get("allowed_workspaces_json")
        .map_err(map_sqlx_error)?;

    Ok(AccountOwnerBinding {
        binding_id: row.try_get("binding_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        provider: row.try_get("provider").map_err(map_sqlx_error)?,
        owner_id: row.try_get("owner_id").map_err(map_sqlx_error)?,
        upstream_id,
        owner_generation: i64_to_u64(owner_generation, "owner_generation")?,
        local_user_id: row.try_get("local_user_id").map_err(map_sqlx_error)?,
        allowed_workspaces: serde_json::from_str(&allowed_workspaces_json)?,
        provider_workspace_id: row
            .try_get("provider_workspace_id")
            .map_err(map_sqlx_error)?,
        outbound_region: row.try_get("outbound_region").map_err(map_sqlx_error)?,
        revision: i64_to_u64(revision, "revision")?,
        revoked_at_unix_secs: revoked_at_unix_secs
            .map(|value| i64_to_u64(value, "revoked_at_unix_secs"))
            .transpose()?,
    })
}

fn provider_for_upstream_kind(kind: &str) -> StorageResult<&str> {
    let Some((provider, credential_kind)) = kind.split_once('_') else {
        return Err(conflict("upstream kind has no provider"));
    };
    if provider.is_empty() || credential_kind.is_empty() {
        return Err(conflict("upstream kind has no provider"));
    }
    Ok(provider)
}

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn upstream_unavailable(message: impl Into<String>) -> StorageError {
    StorageError::UpstreamUnavailable {
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

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| conflict(format!("{field} cannot be represented as sqlite integer")))
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cc_lb_storage_api::MetaStore;

    use super::*;

    async fn fixture() -> (
        tempfile::TempDir,
        SqliteStorage,
        AccountOwnerBinding,
        AccountOwnerBinding,
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            temp_dir.path().join("owner-pair.sqlite").display()
        );
        let storage = crate::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
        storage.initialize().await.expect("initialize sqlite");
        let principal_id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO principals_v1 (id, name, created_at, updated_at)
             VALUES (?, 'pair-principal', 1, 1)",
        )
        .bind(&principal_id)
        .execute(storage.pool())
        .await
        .expect("seed principal");
        let mut bindings = Vec::new();
        for suffix in ["a", "b"] {
            let upstream_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO upstream_spec_v1 (id, name, kind, created_at, updated_at)
                 VALUES (?, ?, 'openai_oauth', 1, 1)",
            )
            .bind(upstream_id.to_string())
            .bind(format!("pair-upstream-{suffix}"))
            .execute(storage.pool())
            .await
            .expect("seed upstream");
            let binding = AccountOwnerBinding {
                binding_id: format!("pair-binding-{suffix}"),
                principal_id: principal_id.clone(),
                provider: "openai".to_owned(),
                owner_id: format!("pair-owner-{suffix}"),
                upstream_id,
                owner_generation: 1,
                local_user_id: "pair-user".to_owned(),
                allowed_workspaces: vec![format!("workspace-{suffix}")],
                provider_workspace_id: format!("vendor-workspace-{suffix}"),
                outbound_region: None,
                revision: 1,
                revoked_at_unix_secs: None,
            };
            storage
                .create_owner_binding(binding.clone())
                .await
                .expect("seed binding");
            bindings.push(binding);
        }
        let target = bindings.pop().expect("target binding");
        let source = bindings.pop().expect("source binding");
        (temp_dir, storage, source, target)
    }

    #[tokio::test]
    async fn workspace_list_keeps_the_callers_read_snapshot_without_blocking_writers() {
        let (_temp_dir, storage, source, target) = fixture().await;
        let mut reader = storage.pool().begin().await.expect("deferred reader");
        let before = list_owner_workspace_aliases_in_read_tx(
            &mut reader,
            &source.principal_id,
            &source.provider,
            &source.local_user_id,
        )
        .await
        .expect("establish snapshot");
        assert_eq!(before, ["workspace-a", "workspace-b"]);

        let mut writer = storage.begin_immediate().await.expect("independent writer");
        sqlx::query("UPDATE account_owner_bindings_v1 SET revoked_at_unix_secs = 1, revision = revision + 1 WHERE binding_id = ?")
            .bind(&target.binding_id).execute(&mut *writer).await.expect("revoke target");
        assert_eq!(
            list_owner_workspace_aliases_in_read_tx(
                &mut reader,
                &source.principal_id,
                &source.provider,
                &source.local_user_id,
            )
            .await
            .expect("snapshot during writer"),
            before
        );
        writer
            .commit()
            .await
            .expect("writer commits while reader remains open");
        assert_eq!(
            list_owner_workspace_aliases_in_read_tx(
                &mut reader,
                &source.principal_id,
                &source.provider,
                &source.local_user_id,
            )
            .await
            .expect("snapshot after commit"),
            before
        );
        reader.commit().await.expect("close snapshot");
        assert_eq!(
            storage
                .list_owner_workspace_targets(
                    &source.principal_id,
                    &source.provider,
                    &source.local_user_id,
                )
                .await
                .expect("fresh snapshot"),
            ["workspace-a"]
        );
    }

    fn expected(binding: &AccountOwnerBinding) -> OwnerBindingExpectation {
        OwnerBindingExpectation {
            binding_id: binding.binding_id.clone(),
            principal_id: binding.principal_id.clone(),
            provider: binding.provider.clone(),
            owner_generation: binding.owner_generation,
            binding_revision: binding.revision,
            workspace_id: binding.allowed_workspaces[0].clone(),
        }
    }

    #[tokio::test]
    async fn pair_validation_uses_fresh_fences_and_rolls_back_consumer_write() {
        let (_temp_dir, storage, source, target) = fixture().await;
        let mut tx = storage
            .begin_immediate()
            .await
            .expect("begin pair transaction");
        let pair =
            validate_owner_bindings_in_tx(&mut tx, &expected(&source), Some(&expected(&target)))
                .await
                .expect("validate cross-owner workspace pair");
        assert_eq!(pair, (source.clone(), Some(target.clone())));
        tx.rollback().await.expect("rollback pair read");

        let mut replacement = target.clone();
        replacement.revision = 2;
        storage
            .compare_and_replace_owner_binding(replacement, 1)
            .await
            .expect("advance target fence");
        let mut tx = storage
            .begin_immediate()
            .await
            .expect("begin protected write");
        sqlx::query("INSERT INTO meta_v1 (key, value) VALUES ('pair-consumer-write', 'no')")
            .execute(&mut *tx)
            .await
            .expect("write consumer state");
        let stale =
            validate_owner_bindings_in_tx(&mut tx, &expected(&source), Some(&expected(&target)))
                .await;
        assert!(matches!(stale, Err(StorageError::Conflict { .. })));
        tx.rollback()
            .await
            .expect("rollback stale consumer transaction");
        assert_eq!(
            storage
                .get_meta_value("pair-consumer-write")
                .await
                .expect("read consumer state"),
            None,
        );
    }

    #[tokio::test]
    async fn pair_validation_rejects_cross_user_and_cross_principal_handoffs() {
        let (_temp_dir, storage, source, mut target) = fixture().await;
        target.binding_id = "other-user-binding".to_owned();
        target.local_user_id = "other-user".to_owned();
        target.allowed_workspaces = vec!["other-user-workspace".to_owned()];
        storage
            .create_owner_binding(target.clone())
            .await
            .expect("create other user binding");
        let mut tx = storage
            .begin_immediate()
            .await
            .expect("begin cross-user validation");
        let cross_user =
            validate_owner_bindings_in_tx(&mut tx, &expected(&source), Some(&expected(&target)))
                .await;
        assert!(matches!(cross_user, Err(StorageError::Conflict { .. })));
        let mut foreign = expected(&target);
        foreign.principal_id = Uuid::new_v4().to_string();
        let cross_principal =
            validate_owner_bindings_in_tx(&mut tx, &expected(&source), Some(&foreign)).await;
        assert!(matches!(
            cross_principal,
            Err(StorageError::Conflict { .. })
        ));
        tx.rollback().await.expect("rollback rejected handoffs");
    }

    #[tokio::test]
    async fn orphaned_upstream_is_unavailable_but_foreign_tuple_remains_conflict() {
        let (_temp_dir, storage, source, _) = fixture().await;
        // Normal deletes are fenced by the FK. Simulate a legacy/corrupted
        // orphan on one connection to cover the fail-closed resolution path.
        let mut connection = storage
            .pool()
            .acquire()
            .await
            .expect("acquire sqlite connection");
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *connection)
            .await
            .expect("disable fixture FK");
        sqlx::query("DELETE FROM upstream_spec_v1 WHERE id = ?")
            .bind(source.upstream_id.to_string())
            .execute(&mut *connection)
            .await
            .expect("create orphan fixture");
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *connection)
            .await
            .expect("restore fixture FK");
        drop(connection);
        let result = storage.resolve_owner_binding(&expected(&source)).await;
        assert!(matches!(
            result,
            Err(StorageError::UpstreamUnavailable { .. })
        ));
        let mut foreign = expected(&source);
        foreign.principal_id = Uuid::new_v4().to_string();
        let result = storage.resolve_owner_binding(&foreign).await;
        assert!(matches!(result, Err(StorageError::Conflict { .. })));
    }

    #[tokio::test]
    async fn authorization_conflicts_precede_source_health_and_rollback_consumer_write() {
        for scenario in ["stale-target", "revoked-target", "other-local-user"] {
            let (_temp_dir, storage, source, target) = fixture().await;
            sqlx::query("UPDATE upstream_spec_v1 SET enabled = 0 WHERE id = ?")
                .bind(source.upstream_id.to_string())
                .execute(storage.pool())
                .await
                .expect("disable source fixture");

            let mut tx = storage
                .begin_immediate()
                .await
                .expect("begin healthy-target control");
            let unavailable = validate_owner_bindings_in_tx(
                &mut tx,
                &expected(&source),
                Some(&expected(&target)),
            )
            .await;
            assert!(matches!(
                unavailable,
                Err(StorageError::UpstreamUnavailable { .. })
            ));
            tx.rollback().await.expect("rollback unavailable control");

            let mut target_expected = expected(&target);
            match scenario {
                "stale-target" => {
                    let mut updated = target.clone();
                    updated.revision = 2;
                    storage
                        .compare_and_replace_owner_binding(updated, 1)
                        .await
                        .expect("advance target revision");
                }
                "revoked-target" => {
                    let mut revoked = target.clone();
                    revoked.revision = 2;
                    revoked.revoked_at_unix_secs = Some(100);
                    storage
                        .compare_and_replace_owner_binding(revoked.clone(), 1)
                        .await
                        .expect("revoke target");
                    target_expected = expected(&revoked);
                }
                _ => {
                    let mut other_user = target.clone();
                    other_user.binding_id = "other-local-user-binding".to_owned();
                    other_user.local_user_id = "other-local-user".to_owned();
                    other_user.allowed_workspaces = vec!["other-local-user-workspace".to_owned()];
                    storage
                        .create_owner_binding(other_user.clone())
                        .await
                        .expect("create other local user's target");
                    target_expected = expected(&other_user);
                }
            }

            let mut tx = storage
                .begin_immediate()
                .await
                .expect("begin protected write");
            sqlx::query(
                "INSERT INTO meta_v1 (key, value) VALUES ('pair-authorization-write', 'rejected')",
            )
            .execute(&mut *tx)
            .await
            .expect("write consumer fixture");
            let result =
                validate_owner_bindings_in_tx(&mut tx, &expected(&source), Some(&target_expected))
                    .await;
            assert!(
                matches!(result, Err(StorageError::Conflict { .. })),
                "{scenario} must conflict before unhealthy source: {result:?}",
            );
            tx.rollback()
                .await
                .expect("rollback rejected consumer write");
            assert_eq!(
                storage
                    .get_meta_value("pair-authorization-write")
                    .await
                    .expect("read rejected consumer state"),
                None,
                "{scenario} consumer write must roll back",
            );
        }
    }

    #[tokio::test]
    async fn absent_target_returns_only_the_source() {
        let (_temp_dir, storage, source, _) = fixture().await;
        let mut tx = storage
            .begin_immediate()
            .await
            .expect("begin source validation");
        let (actual, target) = validate_owner_bindings_in_tx(&mut tx, &expected(&source), None)
            .await
            .expect("validate source without a target");
        assert_eq!(actual, source);
        assert!(target.is_none());
        let wrapped = validate_owner_binding_in_tx(&mut tx, &expected(&source))
            .await
            .expect("validate source through single wrapper");
        assert_eq!(wrapped, source);
        tx.rollback().await.expect("rollback source validation");
    }
}
