use std::sync::Arc;

#[cfg(feature = "postgres")]
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
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
    run_all(Arc::new(SqlitePrincipalBackend)).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn principal_store_postgres() -> Result<()> {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return Ok(());
    };
    run_all(Arc::new(PostgresPrincipalBackend { url })).await
}

#[tokio::test]
async fn principal_store_stale_revision_conflict_sqlite() -> Result<()> {
    stale_revision_conflict(Arc::new(SqlitePrincipalBackend)).await
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn principal_store_stale_revision_conflict_postgres() -> Result<()> {
    let Some(url) = postgres_url() else {
        eprintln!("skip: CI_POSTGRES_URL not set");
        return Ok(());
    };
    stale_revision_conflict(Arc::new(PostgresPrincipalBackend { url })).await
}

struct SqlitePrincipalBackend;

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
        let storage = open_sqlite(&fixture.database_url).await?;
        storage.initialize(BackendKind::Sqlite).await?;
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
}

#[cfg(feature = "postgres")]
struct PostgresFixture {
    url: String,
    schema: String,
    pool: sqlx::PgPool,
}

#[async_trait]
#[cfg(feature = "postgres")]
impl ConformanceBackend for PostgresPrincipalBackend {
    type Storage = PostgresStorage;
    type Fixture = PostgresFixture;

    async fn create_fixture(&self) -> Result<Self::Fixture> {
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
        for migration in MIGRATIONS {
            sqlx::raw_sql(*migration).execute(&pool).await?;
        }
        Ok(PostgresFixture {
            url: self.url.clone(),
            schema,
            pool,
        })
    }

    async fn open(&self, fixture: &Self::Fixture) -> Result<Self::Storage> {
        Ok(PostgresStorage::new(fixture.pool.clone()))
    }

    async fn teardown(&self, fixture: Self::Fixture) -> Result<()> {
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

    fn kind(&self) -> BackendKind {
        BackendKind::Postgres
    }
}

#[cfg(feature = "postgres")]
fn postgres_url() -> Option<String> {
    std::env::var("CI_POSTGRES_URL").ok()
}

#[cfg(feature = "postgres")]
const MIGRATIONS: &[&str] = &[
    include_str!("../../cc-lb-storage-postgres/migrations/0001_meta.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0002_killswitch.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0003_audit_log.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0004_request_events.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0005_quotas.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0006_principal_limit_states.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0007_oauth_credentials.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0008_api_keys.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0009_anthropic_api_keys.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0010_usage_rollups.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0011_config_draft.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0012_config_history.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0016_principals.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0019_principal_allowed_upstreams.sql"),
    include_str!("../../cc-lb-storage-postgres/migrations/0033_router_pipeline.sql"),
];
