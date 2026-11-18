use async_trait::async_trait;
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, AccountOwnerWorkspaceTarget, OwnerBindingExpectation,
    StorageError, StorageResult, UpstreamRecordId, validate_owner_binding_for_create,
    validate_owner_binding_replacement,
};
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

struct UpstreamState {
    kind: String,
    enabled: bool,
    deleted_at: Option<DateTime<Utc>>,
    last_apply_error: Option<String>,
}

fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}

fn unavailable(message: impl Into<String>) -> StorageError {
    StorageError::Unavailable {
        message: message.into(),
    }
}

fn to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| conflict(format!("{field} exceeds the storage range")))
}

fn to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| conflict(format!("{field} is negative")))
}

fn parse_principal_id(value: &str) -> StorageResult<Uuid> {
    let parsed = Uuid::parse_str(value).map_err(|_| conflict("principal_id must be a UUID"))?;
    let mut buffer = [0_u8; 36];
    if parsed.hyphenated().encode_lower(&mut buffer) != value {
        return Err(conflict(
            "principal_id must use canonical lowercase UUID spelling",
        ));
    }
    Ok(parsed)
}

fn provider_matches_kind(provider: &str, kind: &str) -> bool {
    matches!(
        (provider, kind),
        ("anthropic", "anthropic_api_key" | "anthropic_oauth")
            | ("openai", "openai_api_key" | "openai_oauth")
            | ("gemini", "gemini_api_key" | "gemini_oauth")
    )
}

fn decode_binding(row: PgRow) -> StorageResult<AccountOwnerBinding> {
    let workspaces: String = row
        .try_get("allowed_workspaces_json")
        .map_err(map_sqlx_error)?;
    Ok(AccountOwnerBinding {
        binding_id: row.try_get("binding_id").map_err(map_sqlx_error)?,
        principal_id: row
            .try_get::<Uuid, _>("principal_id")
            .map_err(map_sqlx_error)?
            .to_string(),
        provider: row.try_get("provider").map_err(map_sqlx_error)?,
        owner_id: row.try_get("owner_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        owner_generation: to_u64(
            row.try_get("owner_generation").map_err(map_sqlx_error)?,
            "owner_generation",
        )?,
        local_user_id: row.try_get("local_user_id").map_err(map_sqlx_error)?,
        allowed_workspaces: serde_json::from_str(&workspaces)
            .map_err(|error| conflict(format!("allowed_workspaces_json is malformed: {error}")))?,
        provider_workspace_id: row
            .try_get("provider_workspace_id")
            .map_err(map_sqlx_error)?,
        outbound_region: row.try_get("outbound_region").map_err(map_sqlx_error)?,
        revision: to_u64(row.try_get("revision").map_err(map_sqlx_error)?, "revision")?,
        revoked_at_unix_secs: row
            .try_get::<Option<i64>, _>("revoked_at_unix_secs")
            .map_err(map_sqlx_error)?
            .map(|value| to_u64(value, "revoked_at_unix_secs"))
            .transpose()?,
    })
}

async fn read_binding(
    tx: &mut Transaction<'_, Postgres>,
    binding_id: &str,
    lock: bool,
) -> StorageResult<AccountOwnerBinding> {
    let query = if lock {
        "SELECT * FROM account_owner_bindings_v1 WHERE binding_id = $1 FOR UPDATE"
    } else {
        "SELECT * FROM account_owner_bindings_v1 WHERE binding_id = $1"
    };
    let row = sqlx::query(query)
        .bind(binding_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| conflict("owner binding does not exist"))?;
    decode_binding(row)
}

async fn lock_upstream(
    tx: &mut Transaction<'_, Postgres>,
    upstream_id: UpstreamRecordId,
) -> StorageResult<Option<UpstreamState>> {
    let exists = sqlx::query("SELECT id FROM upstream_spec_v1 WHERE id = $1 FOR UPDATE")
        .bind(upstream_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    if exists.is_none() {
        return Ok(None);
    }
    // The lock and health read must be separate statements: after waiting on
    // the spec lock, READ COMMITTED needs a fresh snapshot for status changes.
    let row = sqlx::query(
        "SELECT spec.kind, spec.enabled, spec.deleted_at, status.last_apply_error FROM upstream_spec_v1 spec LEFT JOIN upstream_status_v1 status ON status.upstream_id = spec.id WHERE spec.id = $1",
    )
    .bind(upstream_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(Some(UpstreamState {
        kind: row.try_get("kind").map_err(map_sqlx_error)?,
        enabled: row.try_get("enabled").map_err(map_sqlx_error)?,
        deleted_at: row.try_get("deleted_at").map_err(map_sqlx_error)?,
        last_apply_error: row.try_get("last_apply_error").map_err(map_sqlx_error)?,
    }))
}

fn validate_live_upstream(state: Option<&UpstreamState>, provider: &str) -> StorageResult<()> {
    let state = state.ok_or_else(|| unavailable("upstream does not exist"))?;
    if !state.enabled || state.deleted_at.is_some() || state.last_apply_error.is_some() {
        return Err(unavailable("upstream is not live, enabled, and healthy"));
    }
    if !provider_matches_kind(provider, &state.kind) {
        return Err(conflict(
            "binding provider does not match upstream provider",
        ));
    }
    Ok(())
}

fn validate_expectation_tuple(
    binding: &AccountOwnerBinding,
    expected: &OwnerBindingExpectation,
) -> StorageResult<()> {
    if binding.binding_id != expected.binding_id
        || binding.principal_id != expected.principal_id
        || binding.provider != expected.provider
        || binding.owner_generation != expected.owner_generation
        || binding.revision != expected.binding_revision
        || !binding.allowed_workspaces.contains(&expected.workspace_id)
        || binding.revoked_at_unix_secs.is_some()
    {
        return Err(conflict(
            "owner binding expectation is stale or unauthorized",
        ));
    }
    Ok(())
}

fn validate_pair_identity(
    source: &AccountOwnerBinding,
    target: &AccountOwnerBinding,
) -> StorageResult<()> {
    if source.principal_id != target.principal_id
        || source.provider != target.provider
        || source.local_user_id != target.local_user_id
    {
        return Err(conflict(
            "owner bindings do not share the same principal, provider, and local user",
        ));
    }
    Ok(())
}

/// Validate one binding in the transaction that will perform the protected write.
pub(crate) async fn validate_owner_binding_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    expected: &OwnerBindingExpectation,
) -> StorageResult<AccountOwnerBinding> {
    let (binding, _) = validate_owner_bindings_in_tx(tx, expected, None).await?;
    Ok(binding)
}

/// Upstreams precede binding locks; opposite switches share the same sorted order.
pub(crate) async fn validate_owner_bindings_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    source_expected: &OwnerBindingExpectation,
    target_expected: Option<&OwnerBindingExpectation>,
) -> StorageResult<(AccountOwnerBinding, Option<AccountOwnerBinding>)> {
    let source = read_binding(tx, &source_expected.binding_id, false).await?;
    validate_expectation_tuple(&source, source_expected)?;
    let Some(target_expected) = target_expected else {
        let upstream = lock_upstream(tx, source.upstream_id).await?;
        let binding = read_binding(tx, &source_expected.binding_id, true).await?;
        validate_expectation_tuple(&binding, source_expected)?;
        if binding != source {
            return Err(conflict("owner binding changed while resolving"));
        }
        validate_live_upstream(upstream.as_ref(), &binding.provider)?;
        return Ok((binding, None));
    };
    let target = read_binding(tx, &target_expected.binding_id, false).await?;
    validate_expectation_tuple(&target, target_expected)?;
    validate_pair_identity(&source, &target)?;
    let mut upstream_ids = [source.upstream_id, target.upstream_id];
    upstream_ids.sort_unstable();
    let first_state = lock_upstream(tx, upstream_ids[0]).await?;
    let second_state = if upstream_ids[0] != upstream_ids[1] {
        lock_upstream(tx, upstream_ids[1]).await?
    } else {
        None
    };
    let (fresh_source, fresh_target) = if source.binding_id == target.binding_id {
        let fresh = read_binding(tx, &source.binding_id, true).await?;
        (fresh.clone(), fresh)
    } else if source.binding_id < target.binding_id {
        let fresh_source = read_binding(tx, &source.binding_id, true).await?;
        let fresh_target = read_binding(tx, &target.binding_id, true).await?;
        (fresh_source, fresh_target)
    } else {
        let fresh_target = read_binding(tx, &target.binding_id, true).await?;
        let fresh_source = read_binding(tx, &source.binding_id, true).await?;
        (fresh_source, fresh_target)
    };
    validate_expectation_tuple(&fresh_source, source_expected)?;
    validate_expectation_tuple(&fresh_target, target_expected)?;
    validate_pair_identity(&fresh_source, &fresh_target)?;
    if source != fresh_source || target != fresh_target {
        return Err(conflict("owner binding changed while resolving the pair"));
    }
    for binding in [&fresh_source, &fresh_target] {
        let state = if binding.upstream_id == upstream_ids[0] {
            first_state.as_ref()
        } else {
            second_state.as_ref()
        };
        validate_live_upstream(state, &binding.provider)?;
    }
    Ok((fresh_source, Some(fresh_target)))
}

async fn reserve_workspace_aliases(
    tx: &mut Transaction<'_, Postgres>,
    binding: &AccountOwnerBinding,
) -> StorageResult<()> {
    let principal_id = parse_principal_id(&binding.principal_id)?;
    let aliases = &binding.allowed_workspaces;
    sqlx::query(
        "INSERT INTO account_owner_workspaces_v1 (principal_id, provider, workspace_id, binding_id) SELECT $1, $2, workspace_id, $3 FROM unnest($4::TEXT[]) AS aliases(workspace_id) ORDER BY workspace_id ON CONFLICT (principal_id, provider, workspace_id) DO NOTHING",
    ).bind(principal_id).bind(&binding.provider).bind(&binding.binding_id)
        .bind(aliases).execute(&mut **tx).await.map_err(map_sqlx_error)?;
    let conflicting: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM account_owner_workspaces_v1 WHERE principal_id = $1 AND provider = $2 AND workspace_id = ANY($3::TEXT[]) AND binding_id <> $4)",
    ).bind(principal_id).bind(&binding.provider).bind(aliases)
        .bind(&binding.binding_id).fetch_one(&mut **tx).await.map_err(map_sqlx_error)?;
    if conflicting {
        return Err(conflict(
            "workspace alias is already reserved by another owner binding",
        ));
    }
    Ok(())
}

#[async_trait]
impl AccountOwnerStore for PostgresStorage {
    async fn create_owner_binding(&self, binding: AccountOwnerBinding) -> StorageResult<()> {
        validate_owner_binding_for_create(&binding)?;
        let principal_id = parse_principal_id(&binding.principal_id)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM account_owner_bindings_v1 WHERE binding_id = $1)",
        )
        .bind(&binding.binding_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if exists {
            return Err(conflict("owner binding ID is already reserved"));
        }
        let upstream = lock_upstream(&mut tx, binding.upstream_id).await?;
        validate_live_upstream(upstream.as_ref(), &binding.provider)?;
        let workspaces = serde_json::to_string(&binding.allowed_workspaces)?;
        sqlx::query(
            "INSERT INTO account_owner_bindings_v1 (binding_id, principal_id, provider, owner_id, upstream_id, owner_generation, local_user_id, allowed_workspaces_json, provider_workspace_id, outbound_region, revision, revoked_at_unix_secs) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        ).bind(&binding.binding_id).bind(principal_id).bind(&binding.provider)
            .bind(&binding.owner_id).bind(binding.upstream_id)
            .bind(to_i64(binding.owner_generation, "owner_generation")?).bind(&binding.local_user_id)
            .bind(workspaces).bind(&binding.provider_workspace_id).bind(&binding.outbound_region)
            .bind(to_i64(binding.revision, "revision")?)
            .bind(binding.revoked_at_unix_secs.map(|value| to_i64(value, "revoked_at_unix_secs")).transpose()?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        reserve_workspace_aliases(&mut tx, &binding).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn get_owner_binding(
        &self,
        binding_id: &str,
    ) -> StorageResult<Option<AccountOwnerBinding>> {
        let row = sqlx::query("SELECT * FROM account_owner_bindings_v1 WHERE binding_id = $1")
            .bind(binding_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(decode_binding).transpose()
    }

    async fn find_owner_binding_for_workspace(
        &self,
        target: &AccountOwnerWorkspaceTarget,
    ) -> StorageResult<AccountOwnerBinding> {
        let principal_id = parse_principal_id(&target.principal_id)?;
        let row = sqlx::query(
            "SELECT binding.* FROM account_owner_workspaces_v1 workspace JOIN account_owner_bindings_v1 binding ON binding.binding_id = workspace.binding_id WHERE workspace.principal_id = $1 AND workspace.provider = $2 AND workspace.workspace_id = $3",
        ).bind(principal_id).bind(&target.provider).bind(&target.workspace_id)
            .fetch_optional(&self.pool).await.map_err(map_sqlx_error)?
            .ok_or_else(|| conflict("workspace owner binding does not exist"))?;
        let binding = decode_binding(row)?;
        if binding.principal_id != target.principal_id
            || binding.provider != target.provider
            || binding.local_user_id != target.local_user_id
            || !binding.allowed_workspaces.contains(&target.workspace_id)
            || binding.revoked_at_unix_secs.is_some()
        {
            return Err(conflict("workspace owner binding is unauthorized"));
        }
        Ok(binding)
    }

    async fn list_owner_workspace_targets(
        &self,
        principal_id: &str,
        provider: &str,
        local_user_id: &str,
    ) -> StorageResult<Vec<String>> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        let aliases =
            list_owner_workspace_aliases_in_read_tx(&mut tx, principal_id, provider, local_user_id)
                .await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(aliases)
    }

    async fn compare_and_replace_owner_binding(
        &self,
        replacement: AccountOwnerBinding,
        expected_revision: u64,
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let discovered = read_binding(&mut tx, &replacement.binding_id, false).await?;
        validate_owner_binding_replacement(&discovered, &replacement, expected_revision)?;
        let mut upstream_ids = [discovered.upstream_id, replacement.upstream_id];
        upstream_ids.sort_unstable();
        let first_state = lock_upstream(&mut tx, upstream_ids[0]).await?;
        let second_state = if upstream_ids[0] != upstream_ids[1] {
            lock_upstream(&mut tx, upstream_ids[1]).await?
        } else {
            None
        };
        let existing = read_binding(&mut tx, &replacement.binding_id, true).await?;
        if existing != discovered {
            return Err(conflict("owner binding changed while replacing"));
        }
        validate_owner_binding_replacement(&existing, &replacement, expected_revision)?;
        let target_state = if replacement.upstream_id == upstream_ids[0] {
            first_state.as_ref()
        } else {
            second_state.as_ref()
        };
        let identity_changed = existing.provider != replacement.provider
            || existing.owner_id != replacement.owner_id
            || existing.upstream_id != replacement.upstream_id
            || existing.provider_workspace_id != replacement.provider_workspace_id;
        let permission_withdrawal = !identity_changed
            && replacement.outbound_region == existing.outbound_region
            && replacement
                .allowed_workspaces
                .iter()
                .all(|workspace| existing.allowed_workspaces.contains(workspace));
        if identity_changed
            || (replacement.revoked_at_unix_secs.is_none() && !permission_withdrawal)
        {
            validate_live_upstream(target_state, &replacement.provider)?;
        } else if let Some(state) = target_state
            && !provider_matches_kind(&replacement.provider, &state.kind)
        {
            return Err(conflict(
                "binding provider does not match upstream provider",
            ));
        }
        let workspaces = serde_json::to_string(&replacement.allowed_workspaces)?;
        sqlx::query(
            "UPDATE account_owner_bindings_v1 SET provider = $2, owner_id = $3, upstream_id = $4, owner_generation = $5, allowed_workspaces_json = $6, provider_workspace_id = $7, outbound_region = $8, revision = $9, revoked_at_unix_secs = $10 WHERE binding_id = $1",
        ).bind(&replacement.binding_id).bind(&replacement.provider).bind(&replacement.owner_id)
            .bind(replacement.upstream_id).bind(to_i64(replacement.owner_generation, "owner_generation")?)
            .bind(workspaces).bind(&replacement.provider_workspace_id).bind(&replacement.outbound_region)
            .bind(to_i64(replacement.revision, "revision")?)
            .bind(replacement.revoked_at_unix_secs.map(|value| to_i64(value, "revoked_at_unix_secs")).transpose()?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        reserve_workspace_aliases(&mut tx, &replacement).await?;
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn resolve_owner_binding(
        &self,
        expected: &OwnerBindingExpectation,
    ) -> StorageResult<AccountOwnerBinding> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let binding = validate_owner_binding_in_tx(&mut tx, expected).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(binding)
    }
}

/// Lists local aliases in the caller's read snapshot without granting authority.
///
/// This helper performs only a SELECT and does not acquire writer locks, update
/// bindings or leases, or contact a provider. Callers that need a multi-query
/// snapshot may supply their own repeatable-read, read-only transaction.
pub(crate) async fn list_owner_workspace_aliases_in_read_tx(
    tx: &mut Transaction<'_, Postgres>,
    principal_id: &str,
    provider: &str,
    local_user_id: &str,
) -> StorageResult<Vec<String>> {
    let principal_id = parse_principal_id(principal_id)?;
    sqlx::query_scalar(
        r#"SELECT alias.workspace_id COLLATE "C"
           FROM account_owner_workspaces_v1 alias
           JOIN account_owner_bindings_v1 binding
             ON binding.binding_id = alias.binding_id
           JOIN principals_v1 principal
             ON principal.id = alias.principal_id
           JOIN upstream_spec_v1 upstream
             ON upstream.id = binding.upstream_id
           LEFT JOIN upstream_status_v1 status
             ON status.upstream_id = upstream.id
           WHERE alias.principal_id = $1
             AND alias.provider = $2
             AND binding.principal_id = alias.principal_id
             AND binding.provider = alias.provider
             AND binding.local_user_id = $3
             AND binding.revoked_at_unix_secs IS NULL
             AND principal.enabled = TRUE
             AND principal.deleted_at IS NULL
             AND upstream.enabled = TRUE
             AND upstream.deleted_at IS NULL
             AND status.last_apply_error IS NULL
             AND (
                   (binding.provider = 'anthropic'
                    AND upstream.kind IN ('anthropic_api_key', 'anthropic_oauth'))
                OR (binding.provider = 'openai'
                    AND upstream.kind IN ('openai_api_key', 'openai_oauth'))
                OR (binding.provider = 'gemini'
                    AND upstream.kind IN ('gemini_api_key', 'gemini_oauth'))
             )
             AND EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(binding.allowed_workspaces_json::jsonb) allowed(value)
                   WHERE jsonb_typeof(allowed.value) = 'string'
                     AND allowed.value = to_jsonb(alias.workspace_id)
             )
             AND (
                   cardinality(principal.allowed_upstreams) = 0
                   OR upstream.id = ANY(principal.allowed_upstreams)
             )
           ORDER BY alias.workspace_id COLLATE "C""#,
    )
    .bind(principal_id)
    .bind(provider)
    .bind(local_user_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_storage_api::{
        MetaStore, PrincipalCreate, PrincipalKind, PrincipalStore, UpstreamCreate, UpstreamStore,
        upstream::UpstreamKind,
    };
    use sqlx::{
        AssertSqlSafe,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use std::{str::FromStr, sync::Arc, time::Duration};
    use tokio::sync::oneshot;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
    async fn pair_validation_orders_opposite_switches_and_rechecks_tuples() -> anyhow::Result<()> {
        let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
            return Ok(());
        };
        let schema = format!("account_owner_pair_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        storage.initialize().await?;
        let principal_id = PrincipalStore::create(
            &storage,
            PrincipalCreate {
                name: format!("pair-principal-{}", Uuid::new_v4().simple()),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await?
        .id
        .to_string();
        let mut values = Vec::new();
        for index in 0..2 {
            let upstream = UpstreamStore::create(
                &storage,
                UpstreamCreate {
                    id: Uuid::new_v4(),
                    name: format!("pair-{index}"),
                    kind: UpstreamKind::OpenaiApiKey,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                },
            )
            .await?;
            let binding = AccountOwnerBinding {
                binding_id: format!("binding-{index}"),
                principal_id: principal_id.clone(),
                provider: "openai".into(),
                owner_id: format!("owner-{index}"),
                upstream_id: upstream.id,
                owner_generation: 1,
                local_user_id: "user".into(),
                allowed_workspaces: vec![format!("workspace-{index}")],
                provider_workspace_id: format!("vendor-workspace-{index}"),
                outbound_region: None,
                revision: 1,
                revoked_at_unix_secs: None,
            };
            storage.create_owner_binding(binding.clone()).await?;
            values.push(binding);
        }
        let expected = values
            .iter()
            .map(|binding| OwnerBindingExpectation {
                binding_id: binding.binding_id.clone(),
                principal_id: binding.principal_id.clone(),
                provider: binding.provider.clone(),
                owner_generation: 1,
                binding_revision: 1,
                workspace_id: binding.allowed_workspaces[0].clone(),
            })
            .collect::<Vec<_>>();
        let validate = |source: OwnerBindingExpectation, target: OwnerBindingExpectation| {
            let pool = pool.clone();
            async move {
                let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
                sqlx::query("SET LOCAL lock_timeout = '2s'")
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                let result = validate_owner_bindings_in_tx(&mut tx, &source, Some(&target)).await?;
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok::<_, StorageError>(result)
            }
        };
        let (forward, reverse) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                validate(expected[0].clone(), expected[1].clone()),
                validate(expected[1].clone(), expected[0].clone()),
            )
        })
        .await?;
        let forward = forward?;
        let reverse = reverse?;
        assert_eq!(forward, (values[0].clone(), Some(values[1].clone())));
        assert_eq!(reverse, (values[1].clone(), Some(values[0].clone())));
        let mut single_tx = pool.begin().await?;
        let single = validate_owner_bindings_in_tx(&mut single_tx, &expected[0], None).await?;
        assert_eq!(single, (values[0].clone(), None));
        single_tx.commit().await?;
        let mut stale = expected[1].clone();
        stale.owner_generation = 2;
        assert!(matches!(
            validate(expected[0].clone(), stale).await,
            Err(StorageError::Conflict { .. })
        ));
        let mut stale_revision = expected[1].clone();
        stale_revision.binding_revision = 2;
        assert!(matches!(
            validate(expected[0].clone(), stale_revision).await,
            Err(StorageError::Conflict { .. })
        ));
        sqlx::query(
            "UPDATE account_owner_bindings_v1 SET local_user_id = 'other' WHERE binding_id = $1",
        )
        .bind(&values[1].binding_id)
        .execute(&pool)
        .await?;
        assert!(matches!(
            validate(expected[0].clone(), expected[1].clone()).await,
            Err(StorageError::Conflict { .. })
        ));
        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .execute(&admin)
            .await?;
        admin.close().await;
        Ok(())
    }
    #[tokio::test]
    #[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
    async fn late_status_error_after_upstream_lock_is_not_stale() -> anyhow::Result<()> {
        let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
            return Ok(());
        };
        let schema = format!("account_owner_status_race_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        storage.initialize().await?;
        let principal_id = PrincipalStore::create(
            &storage,
            PrincipalCreate {
                name: format!("status-race-principal-{}", Uuid::new_v4().simple()),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await?
        .id
        .to_string();
        let upstream_id = UpstreamStore::create(
            &storage,
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "status-race-upstream".to_owned(),
                kind: UpstreamKind::OpenaiApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await?
        .id;
        let binding = AccountOwnerBinding {
            binding_id: "status-race-binding".to_owned(),
            principal_id,
            provider: "openai".to_owned(),
            owner_id: "status-race-owner".to_owned(),
            upstream_id,
            owner_generation: 1,
            local_user_id: "status-race-user".to_owned(),
            allowed_workspaces: vec!["status-race-workspace".to_owned()],
            provider_workspace_id: "status-race-vendor-workspace".to_owned(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        };
        storage.create_owner_binding(binding.clone()).await?;
        let expected = OwnerBindingExpectation {
            binding_id: binding.binding_id.clone(),
            principal_id: binding.principal_id.clone(),
            provider: binding.provider.clone(),
            owner_generation: 1,
            binding_revision: 1,
            workspace_id: binding.allowed_workspaces[0].clone(),
        };
        let mut healthy_tx = pool.begin().await?;
        assert_eq!(
            validate_owner_binding_in_tx(&mut healthy_tx, &expected).await?,
            binding,
        );
        healthy_tx.rollback().await?;

        let mut writer = pool.begin().await?;
        sqlx::query("SELECT id FROM upstream_spec_v1 WHERE id = $1 FOR UPDATE")
            .bind(upstream_id)
            .fetch_one(&mut *writer)
            .await?;
        sqlx::query(
            "INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, updated_at) VALUES ($1, 'late apply error', NOW()) ON CONFLICT (upstream_id) DO UPDATE SET last_apply_error = EXCLUDED.last_apply_error, updated_at = EXCLUDED.updated_at",
        )
        .bind(upstream_id)
        .execute(&mut *writer)
        .await?;

        let (pid_tx, pid_rx) = oneshot::channel();
        let reader_pool = pool.clone();
        let reader_expected = expected.clone();
        let reader = tokio::spawn(async move {
            let mut tx = reader_pool.begin().await.map_err(map_sqlx_error)?;
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            let _ = pid_tx.send(pid);
            let result = validate_owner_binding_in_tx(&mut tx, &reader_expected).await;
            tx.rollback().await.map_err(map_sqlx_error)?;
            Ok::<_, StorageError>((pid, result))
        });
        let reader_pid = pid_rx.await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: i64 =
                    sqlx::query_scalar("SELECT cardinality(pg_blocking_pids($1))::bigint")
                        .bind(reader_pid)
                        .fetch_one(&pool)
                        .await?;
                if blocked > 0 {
                    return Ok::<_, sqlx::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader never blocked on the writer's upstream lock")?;
        writer.commit().await?;
        let (_, result) = tokio::time::timeout(Duration::from_secs(5), reader).await???;
        assert!(matches!(result, Err(StorageError::Unavailable { .. })));
        let mut post_update_tx = pool.begin().await?;
        assert!(matches!(
            validate_owner_binding_in_tx(&mut post_update_tx, &expected).await,
            Err(StorageError::Unavailable { .. })
        ));
        post_update_tx.rollback().await?;
        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .execute(&admin)
            .await?;
        admin.close().await;
        Ok(())
    }
    #[tokio::test]
    #[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
    async fn workspace_alias_read_snapshot_stays_stable_until_fresh_list() -> anyhow::Result<()> {
        let Ok(url) = std::env::var("CI_POSTGRES_URL") else {
            return Ok(());
        };
        let schema = format!("account_owner_alias_snapshot_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        let storage = PostgresStorage::new(pool.clone(), Arc::new(cc_lb_clock::SystemClock));
        storage.initialize().await?;
        let principal = PrincipalStore::create(
            &storage,
            PrincipalCreate {
                name: format!("alias-snapshot-principal-{}", Uuid::new_v4().simple()),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            1,
        )
        .await?;
        let upstream = UpstreamStore::create(
            &storage,
            UpstreamCreate {
                id: Uuid::new_v4(),
                name: "alias-snapshot-upstream".to_owned(),
                kind: UpstreamKind::OpenaiApiKey,
                base_url: None,
                api_key_ciphertext: None,
                oauth_tokens: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
                workspace_id: None,
                api_key_access: None,
            },
        )
        .await?;
        let binding = AccountOwnerBinding {
            binding_id: "alias-snapshot-binding".to_owned(),
            principal_id: principal.id.to_string(),
            provider: "openai".to_owned(),
            owner_id: "alias-snapshot-owner".to_owned(),
            upstream_id: upstream.id,
            owner_generation: 1,
            local_user_id: "alias-snapshot-user".to_owned(),
            allowed_workspaces: vec!["before".to_owned(), "after".to_owned()],
            provider_workspace_id: "alias-snapshot-vendor-workspace".to_owned(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        };
        storage.create_owner_binding(binding.clone()).await?;

        let mut snapshot = pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *snapshot)
            .await?;
        let before = list_owner_workspace_aliases_in_read_tx(
            &mut snapshot,
            &principal.id.to_string(),
            "openai",
            "alias-snapshot-user",
        )
        .await?;
        assert_eq!(before, ["after", "before"]);

        let mut withdrawn = binding.clone();
        withdrawn.allowed_workspaces = vec!["after".to_owned()];
        withdrawn.revision = 2;
        storage
            .compare_and_replace_owner_binding(withdrawn, 1)
            .await?;

        let during = list_owner_workspace_aliases_in_read_tx(
            &mut snapshot,
            &principal.id.to_string(),
            "openai",
            "alias-snapshot-user",
        )
        .await?;
        assert_eq!(during, ["after", "before"]);
        snapshot.rollback().await?;

        assert_eq!(
            storage
                .list_owner_workspace_targets(
                    &principal.id.to_string(),
                    "openai",
                    "alias-snapshot-user",
                )
                .await?,
            ["after"]
        );

        pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .execute(&admin)
            .await?;
        admin.close().await;
        Ok(())
    }
}
