use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, AccountOwnerWorkspaceTarget, MetaStore,
    OwnerBindingExpectation, PrincipalCreate, PrincipalKind, PrincipalStore, StorageError,
    UpstreamCreate, UpstreamStore, upstream::UpstreamKind,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn owner_binding_lifecycle_and_cas_fence_stale_grants() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;
    let principal_id = create_principal(&storage).await?;
    let upstream_id = UpstreamStore::create(
        &storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "owner-binding-upstream".to_owned(),
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
    let mut initial = binding(upstream_id, 1, 1, None);
    initial.principal_id = principal_id.clone();
    storage.create_owner_binding(initial.clone()).await?;
    let mut unknown_principal = initial.clone();
    unknown_principal.binding_id = "unknown-principal-binding".to_owned();
    unknown_principal.principal_id = Uuid::new_v4().to_string();
    assert!(matches!(
        storage.create_owner_binding(unknown_principal).await,
        Err(StorageError::Conflict { .. })
    ));
    assert!(
        storage
            .get_owner_binding("unknown-principal-binding")
            .await?
            .is_none()
    );
    assert!(matches!(
        PrincipalStore::hard_delete(&storage, Uuid::parse_str(&principal_id)?).await,
        Err(StorageError::Conflict { .. })
    ));
    let located = storage
        .find_owner_binding_for_workspace(&AccountOwnerWorkspaceTarget {
            principal_id: principal_id.clone(),
            provider: "openai".to_owned(),
            local_user_id: "local-user-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
        })
        .await?;
    assert_eq!(located, initial);
    assert_eq!(
        storage.get_owner_binding("binding-1").await?.as_ref(),
        Some(&initial)
    );

    let mut replacement = initial.clone();
    replacement.allowed_workspaces = vec!["workspace-2".to_owned()];
    replacement.revision = 2;
    let left = storage.clone();
    let right = storage.clone();
    let left_value = replacement.clone();
    let right_value = replacement.clone();
    let (left_result, right_result) = tokio::join!(
        left.compare_and_replace_owner_binding(left_value, 1),
        right.compare_and_replace_owner_binding(right_value, 1),
    );
    assert_eq!(
        u8::from(left_result.is_ok()) + u8::from(right_result.is_ok()),
        1
    );
    let current = storage.get_owner_binding("binding-1").await?.unwrap();
    assert_eq!(current.revision, 2);
    assert_eq!(current.allowed_workspaces, vec!["workspace-2".to_owned()]);

    let stale = OwnerBindingExpectation {
        binding_id: "binding-1".to_owned(),
        principal_id: principal_id.clone(),
        provider: "openai".to_owned(),
        owner_generation: 1,
        binding_revision: 1,
        workspace_id: "workspace-1".to_owned(),
    };
    let current_expectation = OwnerBindingExpectation {
        binding_id: "binding-1".to_owned(),
        principal_id: principal_id.clone(),
        provider: "openai".to_owned(),
        owner_generation: 1,
        binding_revision: 2,
        workspace_id: "workspace-2".to_owned(),
    };
    assert_eq!(
        storage.resolve_owner_binding(&current_expectation).await?,
        current
    );
    assert!(matches!(
        storage
            .resolve_owner_binding(&OwnerBindingExpectation {
                workspace_id: "workspace-1".to_owned(),
                ..current_expectation.clone()
            })
            .await,
        Err(StorageError::Conflict { .. })
    ));
    assert!(matches!(
        storage.resolve_owner_binding(&stale).await,
        Err(StorageError::Conflict { .. })
    ));
    let mut revoked = current.clone();
    revoked.revision = 3;
    revoked.revoked_at_unix_secs = Some(1_700_000_000);
    storage
        .compare_and_replace_owner_binding(revoked, 2)
        .await?;
    assert!(matches!(
        storage
            .resolve_owner_binding(&OwnerBindingExpectation {
                binding_id: "binding-1".to_owned(),
                principal_id: principal_id.clone(),
                provider: "openai".to_owned(),
                owner_generation: 1,
                binding_revision: 2,
                workspace_id: "workspace-2".to_owned(),
            })
            .await,
        Err(StorageError::Conflict { .. })
    ));
    fixture.drop_schema().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn workspace_alias_reservations_rollback_and_identity_generation_are_strict() -> TestResult<()>
{
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;
    let principal_id = create_principal(&storage).await?;
    let upstream_id = create_upstream(&storage, "aliases").await?;
    let mut initial = binding(upstream_id, 1, 1, None);
    initial.principal_id = principal_id;
    storage.create_owner_binding(initial.clone()).await?;
    let mut other = initial.clone();
    other.binding_id = "binding-2".to_owned();
    assert!(matches!(
        storage.create_owner_binding(other.clone()).await,
        Err(StorageError::Conflict { .. })
    ));
    assert!(storage.get_owner_binding("binding-2").await?.is_none());
    other.allowed_workspaces = vec!["workspace-2".to_owned()];
    storage.create_owner_binding(other.clone()).await?;
    let target = AccountOwnerWorkspaceTarget {
        principal_id: initial.principal_id.clone(),
        provider: initial.provider.clone(),
        local_user_id: initial.local_user_id.clone(),
        workspace_id: "workspace-1".to_owned(),
    };
    assert!(matches!(
        storage
            .find_owner_binding_for_workspace(&AccountOwnerWorkspaceTarget {
                local_user_id: "wrong-user".to_owned(),
                ..target.clone()
            })
            .await,
        Err(StorageError::Conflict { .. })
    ));
    let mut collision = other.clone();
    collision.revision = 2;
    collision.allowed_workspaces = vec!["workspace-1".to_owned(), "new-alias".to_owned()];
    assert!(matches!(
        storage
            .compare_and_replace_owner_binding(collision, 1)
            .await,
        Err(StorageError::Conflict { .. })
    ));
    assert_eq!(storage.get_owner_binding("binding-2").await?, Some(other));
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM account_owner_workspaces_v1 WHERE workspace_id = 'new-alias'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(count, 0);
    let mut changed = initial.clone();
    changed.revision = 2;
    changed.provider_workspace_id = "vendor-workspace-2".to_owned();
    assert!(matches!(
        storage
            .compare_and_replace_owner_binding(changed.clone(), 1)
            .await,
        Err(StorageError::Conflict { .. })
    ));
    changed.owner_generation = 2;
    storage
        .compare_and_replace_owner_binding(changed.clone(), 1)
        .await?;
    assert_eq!(
        storage.get_owner_binding("binding-1").await?,
        Some(changed.clone())
    );
    assert!(matches!(
        storage
            .resolve_owner_binding(&expectation(&initial, "workspace-1"))
            .await,
        Err(StorageError::Conflict { .. })
    ));
    let mut retired = changed.clone();
    retired.revision = 3;
    retired.allowed_workspaces = vec!["new-workspace".to_owned()];
    storage
        .compare_and_replace_owner_binding(retired, 2)
        .await?;
    assert!(matches!(
        storage.find_owner_binding_for_workspace(&target).await,
        Err(StorageError::Conflict { .. })
    ));
    let mut reuse = binding(upstream_id, 1, 1, None);
    reuse.principal_id = initial.principal_id.clone();
    reuse.binding_id = "binding-3".to_owned();
    assert!(matches!(
        storage.create_owner_binding(reuse).await,
        Err(StorageError::Conflict { .. })
    ));
    fixture.drop_schema().await
}

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn unhealthy_upstream_blocks_grants_but_not_permission_withdrawal_or_revocation()
-> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;
    let principal_id = create_principal(&storage).await?;
    let upstream_id = create_upstream(&storage, "unhealthy").await?;
    let mut initial = binding(upstream_id, 1, 1, None);
    initial.principal_id = principal_id;
    initial.allowed_workspaces.push("workspace-2".to_owned());
    storage.create_owner_binding(initial.clone()).await?;
    sqlx::query("UPDATE upstream_spec_v1 SET enabled = FALSE WHERE id = $1")
        .bind(upstream_id)
        .execute(&fixture.pool)
        .await?;
    assert!(matches!(
        storage
            .resolve_owner_binding(&expectation(&initial, "workspace-1"))
            .await,
        Err(StorageError::Unavailable { .. })
    ));
    let mut wrong = expectation(&initial, "workspace-1");
    wrong.principal_id = Uuid::new_v4().to_string();
    assert!(matches!(
        storage.resolve_owner_binding(&wrong).await,
        Err(StorageError::Conflict { .. })
    ));
    let mut grant = initial.clone();
    grant.revision = 2;
    grant.allowed_workspaces.push("workspace-3".to_owned());
    assert!(matches!(
        storage.compare_and_replace_owner_binding(grant, 1).await,
        Err(StorageError::Unavailable { .. })
    ));
    sqlx::query("UPDATE upstream_spec_v1 SET enabled = TRUE WHERE id = $1")
        .bind(upstream_id)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("INSERT INTO upstream_status_v1 (upstream_id, last_apply_error, updated_at) VALUES ($1, 'test unhealthy upstream', NOW()) ON CONFLICT (upstream_id) DO UPDATE SET last_apply_error = EXCLUDED.last_apply_error")
        .bind(upstream_id).execute(&fixture.pool).await?;
    assert!(matches!(
        storage
            .resolve_owner_binding(&expectation(&initial, "workspace-1"))
            .await,
        Err(StorageError::Unavailable { .. })
    ));
    sqlx::query("UPDATE upstream_spec_v1 SET deleted_at = NOW() WHERE id = $1")
        .bind(upstream_id)
        .execute(&fixture.pool)
        .await?;
    assert!(matches!(
        storage
            .resolve_owner_binding(&expectation(&initial, "workspace-1"))
            .await,
        Err(StorageError::Unavailable { .. })
    ));
    let mut withdraw = initial.clone();
    withdraw.revision = 2;
    withdraw.allowed_workspaces = vec!["workspace-1".to_owned()];
    storage
        .compare_and_replace_owner_binding(withdraw.clone(), 1)
        .await?;
    let mut revoked = withdraw.clone();
    revoked.revision = 3;
    revoked.revoked_at_unix_secs = Some(1);
    storage
        .compare_and_replace_owner_binding(revoked.clone(), 2)
        .await?;
    assert!(matches!(
        storage
            .resolve_owner_binding(&expectation(&revoked, "workspace-1"))
            .await,
        Err(StorageError::Conflict { .. })
    ));
    let mut reactivate = revoked;
    reactivate.revision = 4;
    reactivate.revoked_at_unix_secs = None;
    assert!(matches!(
        storage
            .compare_and_replace_owner_binding(reactivate, 3)
            .await,
        Err(StorageError::Conflict { .. })
    ));
    fixture.drop_schema().await
}

async fn create_principal(storage: &PostgresStorage) -> TestResult<String> {
    Ok(PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: format!("owner-principal-{}", Uuid::new_v4().simple()),
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
    .to_string())
}

async fn create_upstream(storage: &PostgresStorage, name: &str) -> TestResult<Uuid> {
    Ok(UpstreamStore::create(
        storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: name.to_owned(),
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
    .id)
}

fn expectation(binding: &AccountOwnerBinding, workspace: &str) -> OwnerBindingExpectation {
    OwnerBindingExpectation {
        binding_id: binding.binding_id.clone(),
        principal_id: binding.principal_id.clone(),
        provider: binding.provider.clone(),
        owner_generation: binding.owner_generation,
        binding_revision: binding.revision,
        workspace_id: workspace.to_owned(),
    }
}

fn binding(
    upstream_id: Uuid,
    revision: u64,
    generation: u64,
    revoked_at_unix_secs: Option<u64>,
) -> AccountOwnerBinding {
    AccountOwnerBinding {
        binding_id: "binding-1".to_owned(),
        principal_id: Uuid::nil().to_string(),
        provider: "openai".to_owned(),
        owner_id: "owner-1".to_owned(),
        upstream_id,
        owner_generation: generation,
        local_user_id: "local-user-1".to_owned(),
        allowed_workspaces: vec!["workspace-1".to_owned()],
        provider_workspace_id: "vendor-workspace-1".to_owned(),
        outbound_region: Some("us-east".to_owned()),
        revision,
        revoked_at_unix_secs,
    }
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
        let schema = format!("account_owner_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        Ok(Some(Self {
            schema,
            admin_pool,
            pool,
        }))
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(self.pool.clone(), Arc::new(cc_lb_clock::SystemClock))
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

fn quote_ident(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    );
    format!("\"{identifier}\"")
}
