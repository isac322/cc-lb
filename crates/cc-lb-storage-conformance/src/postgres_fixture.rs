#![allow(
    dead_code,
    reason = "the shared fixture is path-included by sibling test crates with feature-specific consumers"
)]

use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use cc_lb_clock::TestClock;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const FIXED_CLOCK_UNIX_SECS: u64 = 1_700_000_000;
const REQUIRED_URL_ERROR: &str = "CI_POSTGRES_URL is required for t3_postgres__ tests";

#[allow(
    dead_code,
    reason = "this helper is consumed by path-included PostgreSQL test modules in sibling crates"
)]
pub(crate) fn required_postgres_url() -> String {
    postgres_url().unwrap_or_else(|error| panic!("{error:#}"))
}

fn postgres_url() -> Result<String> {
    std::env::var("CI_POSTGRES_URL").map_err(|_| anyhow::anyhow!(REQUIRED_URL_ERROR))
}

#[must_use = "PostgresFixture must be explicitly torn down with teardown() or drop_schema().await"]
pub struct PostgresFixture {
    database_url: String,
    schema_name: String,
    admin_pool: PgPool,
    pool: PgPool,
    storage: PostgresStorage,
    torn_down: bool,
}

impl PostgresFixture {
    #[must_use]
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub fn storage(&self) -> &PostgresStorage {
        &self.storage
    }

    #[must_use]
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub async fn teardown(mut self) -> Result<()> {
        self.drop_schema().await
    }

    pub async fn drop_schema(&mut self) -> Result<()> {
        if self.torn_down {
            return Ok(());
        }

        self.pool.close().await;
        drop_schema_with_pool(&self.admin_pool, &self.schema_name).await?;
        self.admin_pool.close().await;
        self.torn_down = true;
        Ok(())
    }
}

impl Drop for PostgresFixture {
    fn drop(&mut self) {
        if !self.torn_down {
            eprintln!(
                "PostgresFixture for schema {} was dropped without explicit teardown; call teardown().await or drop_schema().await",
                self.schema_name
            );
        }
    }
}

pub async fn postgres_fixture() -> Result<PostgresFixture> {
    let url = postgres_url()?;

    let schema_name = format!("cc_lb_test_{}", Uuid::new_v4().simple());
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&url).context("parse CI_POSTGRES_URL")?)
        .await
        .context("connect PostgreSQL fixture admin pool")?;

    create_schema(&admin_pool, &schema_name).await?;

    let setup = async {
        let search_path = format!("{}, public", quote_identifier(&schema_name));
        let options = PgConnectOptions::from_str(&url)
            .context("parse CI_POSTGRES_URL for schema pool")?
            .options([("search_path", search_path.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .context("connect PostgreSQL fixture schema pool")?;
        let storage = PostgresStorage::new(
            pool.clone(),
            Arc::new(TestClock::new_at_secs(FIXED_CLOCK_UNIX_SECS)),
        );
        MetaStore::initialize(&storage, BackendKind::Postgres)
            .await
            .context("initialize PostgreSQL fixture storage")?;
        Ok::<_, anyhow::Error>((pool, storage))
    }
    .await;

    match setup {
        Ok((pool, storage)) => Ok(PostgresFixture {
            database_url: url,
            schema_name,
            admin_pool,
            pool,
            storage,
            torn_down: false,
        }),
        Err(setup_error) => {
            let cleanup = drop_schema_with_pool(&admin_pool, &schema_name).await;
            admin_pool.close().await;
            match cleanup {
                Ok(()) => Err(setup_error),
                Err(cleanup_error) => Err(setup_error.context(format!(
                    "also failed to drop PostgreSQL fixture schema {schema_name}: {cleanup_error:#}"
                ))),
            }
        }
    }
}

async fn create_schema(pool: &PgPool, schema_name: &str) -> Result<()> {
    sqlx::query(AssertSqlSafe(format!(
        "CREATE SCHEMA {}",
        quote_identifier(schema_name)
    )))
    .execute(pool)
    .await
    .with_context(|| format!("create PostgreSQL fixture schema {schema_name}"))?;
    Ok(())
}

async fn drop_schema_with_pool(pool: &PgPool, schema_name: &str) -> Result<()> {
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        quote_identifier(schema_name)
    )))
    .execute(pool)
    .await
    .with_context(|| format!("drop PostgreSQL fixture schema {schema_name}"))?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'),
        "unsafe PostgreSQL identifier: {identifier}"
    );
    format!("\"{identifier}\"")
}
