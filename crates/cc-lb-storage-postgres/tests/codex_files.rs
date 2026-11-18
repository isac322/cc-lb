use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_clock::SystemClock;
use cc_lb_storage_api::{
    AccountOwnerBinding, AccountOwnerStore, CodexFileResource, CodexFileState, CodexFileStore,
    MetaStore, OwnerBindingExpectation, PrincipalCreate, PrincipalKind, PrincipalStore,
    StorageError, UpstreamCreate, UpstreamKind, UpstreamStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::{PgConnectOptions, PgPoolOptions}};
use uuid::Uuid;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires CI_POSTGRES_URL; run with --ignored"]
async fn codex_files_create_finalize_scope_and_upstream_fence() -> TestResult<()> {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let storage = fixture.store();
    storage.initialize().await?;

    let principal_id = PrincipalStore::create(
        &storage,
        PrincipalCreate {
            name: format!("codex-files-principal-{}", Uuid::new_v4().simple()),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await?
    .id;
    let upstream_id = UpstreamStore::create(
        &storage,
        UpstreamCreate {
            id: Uuid::new_v4(),
            name: "codex-files-upstream".to_owned(),
            kind: UpstreamKind::OpenaiOauth,
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
        binding_id: format!("codex-files-binding-{}", Uuid::new_v4().simple()),
        principal_id: principal_id.to_string(),
        provider: "openai".to_owned(),
        owner_id: "codex-files-owner".to_owned(),
        upstream_id,
        owner_generation: 1,
        local_user_id: "codex-files-local-user".to_owned(),
        allowed_workspaces: vec!["codex-files-workspace".to_owned()],
        provider_workspace_id: "provider-workspace".to_owned(),
        outbound_region: None,
        revision: 1,
        revoked_at_unix_secs: None,
    };
    storage.create_owner_binding(binding.clone()).await?;
    let expected = OwnerBindingExpectation {
        binding_id: binding.binding_id.clone(),
        principal_id: binding.principal_id.clone(),
        provider: binding.provider.clone(),
        owner_generation: binding.owner_generation,
        binding_revision: binding.revision,
        workspace_id: binding.allowed_workspaces[0].clone(),
    };
    let resource = CodexFileResource::bind(
        "postgres-file".to_owned(),
        "input_file".to_owned(),
        &binding,
        expected.workspace_id.clone(),
        4_100_000_000,
        None,
    );
    storage.create_codex_file(&expected, resource.clone()).await?;
    assert_eq!(resource.state, CodexFileState::Created);

    let finalized = storage
        .finalize_codex_file(
            &expected,
            "input_file",
            "postgres-file",
            "https://files.example.test/reference/postgres".to_owned(),
            Some(4_100_007_200),
        )
        .await?
        .expect("finalized resource");
    assert_eq!(finalized.state, CodexFileState::Finalized);
    assert_eq!(
        finalized.reference.as_deref(),
        Some("https://files.example.test/reference/postgres")
    );
    assert_eq!(
        storage
            .resolve_codex_file(&expected, "input_file", "postgres-file")
            .await?,
        Some(finalized)
    );

    let upstream = UpstreamStore::get_by_id(&storage, upstream_id)
        .await?
        .expect("upstream");
    UpstreamStore::set_enabled(&storage, upstream.id, upstream.revision, false).await?;
    assert!(matches!(
        storage
            .resolve_codex_file(&expected, "input_file", "postgres-file")
            .await,
        Err(StorageError::UpstreamUnavailable { .. })
    ));

    fixture.drop_schema().await
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
        let schema = format!("codex_files_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(&url).await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {}", quote_ident(&schema))))
            .execute(&admin_pool)
            .await?;
        let options = PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new().max_connections(8).connect_with(options).await?;
        Ok(Some(Self { schema, admin_pool, pool }))
    }

    fn store(&self) -> PostgresStorage {
        PostgresStorage::new(self.pool.clone(), Arc::new(SystemClock))
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
    assert!(identifier.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
    format!("\"{identifier}\"")
}
