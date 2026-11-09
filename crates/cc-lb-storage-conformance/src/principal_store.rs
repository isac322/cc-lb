use std::sync::Arc;

#[cfg(feature = "postgres")]
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::{BackendKind, MetaStore};
#[cfg(feature = "postgres")]
use cc_lb_storage_postgres::PostgresStorage;
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
#[cfg(feature = "postgres")]
use sqlx::AssertSqlSafe;
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
#[cfg(feature = "postgres")]
use uuid::Uuid;

use crate::harness::ConformanceBackend;
use crate::scenarios::principal_store::{run_all, stale_revision_conflict};

#[tokio::test]
async fn principal_store_sqlite() -> Result<()> {
    let clock: ClockHandle = Arc::new(SystemClock);
    run_all(Arc::new(SqlitePrincipalBackend { clock })).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn principal_store_postgres() -> Result<()> {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return Ok(());
    };
    let clock: ClockHandle = Arc::new(SystemClock);
    let backend = Arc::new(PostgresPrincipalBackend {
        url,
        clock,
        reused: tokio::sync::Mutex::new(None),
    });
    let result = run_all(Arc::clone(&backend)).await;
    backend.cleanup().await?;
    result
}

#[tokio::test]
async fn principal_store_stale_revision_conflict_sqlite() -> Result<()> {
    let clock: ClockHandle = Arc::new(SystemClock);
    stale_revision_conflict(Arc::new(SqlitePrincipalBackend { clock })).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn principal_store_stale_revision_conflict_postgres() -> Result<()> {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return Ok(());
    };
    let clock: ClockHandle = Arc::new(SystemClock);
    let backend = Arc::new(PostgresPrincipalBackend {
        url,
        clock,
        reused: tokio::sync::Mutex::new(None),
    });
    let result = stale_revision_conflict(Arc::clone(&backend)).await;
    backend.cleanup().await?;
    result
}

struct SqlitePrincipalBackend {
    clock: ClockHandle,
}

struct SqliteFixture {
    _dir: tempfile::TempDir,
    database_url: String,
}

#[async_trait]
impl ConformanceBackend for SqlitePrincipalBackend {
    type Storage = SqliteStorage;
    type Fixture = SqliteFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("principal_store.sqlite");
        let database_url = format!("sqlite://{}", path.display());
        Ok(SqliteFixture {
            _dir: dir,
            database_url,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        let storage = open_sqlite(&fixture.database_url, self.clock.clone()).await?;
        storage.initialize().await?;
        Ok(storage)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }
}

#[cfg(feature = "postgres")]
struct PostgresPrincipalBackend {
    url: String,
    clock: ClockHandle,
    reused: tokio::sync::Mutex<Option<PostgresFixture>>,
}

#[cfg(feature = "postgres")]
#[derive(Clone)]
struct PostgresFixture {
    url: String,
    schema: String,
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresPrincipalBackend {
    async fn cleanup(&self) -> Result<()> {
        let Some(fixture) = self.reused.lock().await.take() else {
            return Ok(());
        };
        fixture.pool.close().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&fixture.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            fixture.schema
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;
        Ok(())
    }
}
#[async_trait]
#[cfg(feature = "postgres")]
impl ConformanceBackend for PostgresPrincipalBackend {
    type Storage = PostgresStorage;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
        let mut reused = self.reused.lock().await;
        if let Some(existing) = reused.as_ref() {
            sqlx::query(
                "TRUNCATE TABLE audit_log_v1, principals_v1, plugin_chains_v2 \
                 RESTART IDENTITY CASCADE",
            )
            .execute(&existing.pool)
            .await?;
            sqlx::query(
                "DELETE FROM wasm_registry_v2 \
                 WHERE original_filename != 'builtin://subscription-preference'",
            )
            .execute(&existing.pool)
            .await?;
            sqlx::query(
                "DELETE FROM wasm_blobs_v2 \
                 WHERE NOT EXISTS (SELECT 1 FROM wasm_registry_v2 r \
                                   WHERE r.sha256 = wasm_blobs_v2.sha256)",
            )
            .execute(&existing.pool)
            .await?;
            return Ok(existing.clone());
        }
        let schema = format!("test_principal_store_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(PgConnectOptions::from_str(&self.url)?)
            .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin_pool)
            .await?;
        admin_pool.close().await;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(
                PgConnectOptions::from_str(&self.url)?.options([("search_path", schema.as_str())]),
            )
            .await?;
        let storage = PostgresStorage::new(pool.clone(), self.clock.clone());
        MetaStore::initialize(&storage).await?;
        let fixture = PostgresFixture {
            url: self.url.clone(),
            schema,
            pool,
        };
        *reused = Some(fixture.clone());
        Ok(fixture)
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        Ok(PostgresStorage::new(
            fixture.pool.clone(),
            self.clock.clone(),
        ))
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }
}

#[cfg(feature = "postgres")]
fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}
