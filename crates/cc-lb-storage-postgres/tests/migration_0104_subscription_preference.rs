use std::{error::Error, str::FromStr, sync::Arc};

use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BackendKind, MetaStore, PluginRegistryStore,
    PrincipalCreate, PrincipalKind, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const MIGRATION: &str =
    include_str!("../migrations/0104_materialize_default_subscription_preference.sql");

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn migration_materializes_default_on_real_schema() -> TestResult {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL or DATABASE_URL_TEST not set");
        return Ok(());
    };

    let fixture = Fixture::create(&url).await?;
    let result: TestResult = async {
        let empty = create_principal(&fixture.storage, "empty").await?;
        let existing_builtin = create_principal(&fixture.storage, "builtin").await?;
        let custom = create_principal(&fixture.storage, "custom").await?;
        let custom_registry_id = persist_custom_registry(&fixture.storage).await?;
        replace_router_registry(&fixture.pool, custom, custom_registry_id).await?;
        let deleted_empty = create_principal(&fixture.storage, "deleted").await?;
        remove_router_chain(&fixture.pool, empty).await?;
        remove_router_chain(&fixture.pool, deleted_empty).await?;
        sqlx::query("UPDATE principals_v1 SET deleted_at = NOW() WHERE id = $1")
            .bind(deleted_empty)
            .execute(&fixture.pool)
            .await?;

        sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;

        assert_eq!(builtin_order(&fixture.pool, empty).await?, Some(0));
        assert_eq!(builtin_count(&fixture.pool, empty).await?, 1);
        assert_eq!(
            builtin_order(&fixture.pool, existing_builtin).await?,
            Some(0)
        );
        assert_eq!(builtin_count(&fixture.pool, existing_builtin).await?, 1);
        assert_eq!(builtin_count(&fixture.pool, custom).await?, 0);
        assert_eq!(router_entry_count(&fixture.pool, custom).await?, 1);
        assert_eq!(builtin_order(&fixture.pool, deleted_empty).await?, None);
        assert_eq!(builtin_count(&fixture.pool, deleted_empty).await?, 0);
        Ok(())
    }
    .await;
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

async fn create_principal(storage: &PostgresStorage, name: &str) -> TestResult<Uuid> {
    Ok(PrincipalStore::create(
        storage,
        PrincipalCreate {
            name: name.to_owned(),
            kind: PrincipalKind::Machine,
            allowed_models: Vec::new(),
            allowed_upstreams: Vec::new(),
            default_limits: Vec::new(),
            cache_keepalive: None,
        },
        1,
    )
    .await?
    .id)
}

async fn remove_router_chain(pool: &PgPool, principal_id: Uuid) -> TestResult {
    sqlx::query("DELETE FROM plugin_chains_v2 WHERE principal_id = $1 AND slot = 'router'")
        .bind(principal_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn persist_custom_registry(storage: &PostgresStorage) -> TestResult<Uuid> {
    let (entry, _) = PluginRegistryStore::persist_wasm_upload(
        storage,
        WasmBlob {
            sha256: [0xA1; 32],
            bytes: vec![0],
            size_bytes: 1,
            parse_validated_at_unix_secs: 1,
        },
        WasmRegistryEntryInput {
            name: "custom-router-filter".to_owned(),
            version: None,
            original_filename: "custom-router-filter.wasm".to_owned(),
            label: None,
            uploaded_at_unix_secs: 1,
            uploaded_by_admin_id: Uuid::nil(),
            description: String::new(),
            usage: String::new(),
            hook_metadata: Default::default(),
            supported_slots: Vec::new(),
            schema_hash: None,
        },
    )
    .await?;
    Ok(entry.id)
}

async fn replace_router_registry(
    pool: &PgPool,
    principal_id: Uuid,
    wasm_registry_id: Uuid,
) -> TestResult {
    sqlx::query(
        "UPDATE plugin_chains_v2
            SET wasm_registry_id = $1
          WHERE principal_id = $2
            AND slot = 'router'",
    )
    .bind(wasm_registry_id)
    .bind(principal_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn builtin_order(pool: &PgPool, principal_id: Uuid) -> TestResult<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT order_value
           FROM plugin_chains_v2
          WHERE principal_id = $1
            AND slot = 'router'
            AND wasm_registry_id = $2",
    )
    .bind(principal_id)
    .bind(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
    .fetch_optional(pool)
    .await?)
}

async fn builtin_count(pool: &PgPool, principal_id: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM plugin_chains_v2
          WHERE principal_id = $1
            AND slot = 'router'
            AND wasm_registry_id = $2",
    )
    .bind(principal_id)
    .bind(BUILTIN_SUBSCRIPTION_PREFERENCE_ID)
    .fetch_one(pool)
    .await?)
}

async fn router_entry_count(pool: &PgPool, principal_id: Uuid) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM plugin_chains_v2
          WHERE principal_id = $1
            AND slot = 'router'",
    )
    .bind(principal_id)
    .fetch_one(pool)
    .await?)
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
    storage: PostgresStorage,
}

impl Fixture {
    async fn create(url: &str) -> TestResult<Self> {
        let schema = format!("test_migration_0104_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(
                PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        let clock: ClockHandle = Arc::new(SystemClock);
        let storage = PostgresStorage::new(pool.clone(), clock);
        let fixture = Self {
            schema,
            admin_pool,
            pool,
            storage,
        };
        if let Err(error) = fixture.storage.initialize(BackendKind::Postgres).await {
            let _ = fixture.drop_schema().await;
            return Err(error.into());
        }
        Ok(fixture)
    }

    async fn drop_schema(self) -> TestResult {
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

fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL_TEST"))
        .ok()
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
