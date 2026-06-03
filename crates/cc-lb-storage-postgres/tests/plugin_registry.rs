use std::str::FromStr;

use anyhow::Result;
use cc_lb_storage_api::{BackendKind, MetaStore, PluginBlobRepo};
use cc_lb_storage_postgres::{PostgresPluginBlobRepo, PostgresPluginRegistryRepo, PostgresStorage};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

fn get_postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[test]
fn plugin_registry_postgres_conformance() {
    let url = match get_postgres_url() {
        Some(url) => url,
        None => {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return;
        }
    };

    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(async move { run_conformance(&url).await })
        .expect("plugin registry postgres conformance");
}

async fn run_conformance(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let registry = PostgresPluginRegistryRepo::new(fixture.pool.clone());
    let blobs = PostgresPluginBlobRepo::new(fixture.pool.clone());

    let result =
        cc_lb_storage_conformance::plugin_registry::plugin_registry_roundtrip(&registry, &blobs)
            .await;
    let result = match result {
        Ok(()) => blob_roundtrip(&blobs).await,
        Err(error) => Err(error),
    };
    let teardown = fixture.drop_schema().await;

    result?;
    teardown
}

async fn blob_roundtrip(blobs: &PostgresPluginBlobRepo) -> Result<()> {
    let sha256 = [90; 32];
    blobs.put_blob(&sha256, b"plugin wasm bytes").await?;
    assert_eq!(
        blobs.get_blob(&sha256).await?.as_deref(),
        Some(b"plugin wasm bytes".as_slice())
    );
    assert!(blobs.list_blob_keys().await?.contains(&sha256));
    blobs.delete_blob(&sha256).await?;
    assert!(blobs.get_blob(&sha256).await?.is_none());
    Ok(())
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("plugin_registry_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            quote_ident(&schema)
        )))
        .execute(&admin_pool)
        .await?;

        let pool = schema_pool(url, &schema).await?;
        let storage = PostgresStorage::new(pool.clone());
        storage.initialize(BackendKind::Postgres).await?;

        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
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

async fn schema_pool(url: &str, schema: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
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
