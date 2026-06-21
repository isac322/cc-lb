//! Storage backend bootstrap helpers used by `BddCtx`.
//!
//! - SQLite: tempfile-per-scenario, dropped on teardown via `TempDir`.
//! - Postgres: schema-per-scenario, owned pool, dropped on teardown via
//!   `DROP SCHEMA ... CASCADE`. Postgres path is gated behind the
//!   `postgres` cargo feature and the `CI_POSTGRES_URL` env var.

use std::sync::Arc;

use anyhow::Result;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::open_sqlite;

/// Trait-object handle to the storage backend underlying a scenario.
/// The harness drives every persona client through the `dyn` view so
/// that one set of helper code serves both SQLite and PostgreSQL
/// without per-backend monomorphisation.
///
/// The concrete type behind the trait object is whichever backend the
/// macro chose: `SqliteStorage` for `_sqlite` tests and
/// `PostgresStorage` for `_postgres` tests (behind the `postgres`
/// feature flag).
pub type StorageHandle = Arc<dyn cc_lb_storage_api::Storage>;

/// Opaque ownership of the on-disk fixture for a single scenario. Held
/// by `BddCtx` so the underlying tempdir / schema lives for the entire
/// scenario and is dropped on scope exit.
pub struct StorageFixture {
    #[allow(dead_code)]
    inner: FixtureInner,
}

enum FixtureInner {
    Sqlite {
        _dir: tempfile::TempDir,
    },
    #[cfg(feature = "postgres")]
    Postgres {
        url: String,
        schema: String,
        pool: sqlx::PgPool,
    },
}

impl StorageFixture {
    pub(crate) fn new_sqlite(dir: tempfile::TempDir) -> Self {
        Self {
            inner: FixtureInner::Sqlite { _dir: dir },
        }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn new_postgres(url: String, schema: String, pool: sqlx::PgPool) -> Self {
        Self {
            inner: FixtureInner::Postgres { url, schema, pool },
        }
    }
}

impl Drop for StorageFixture {
    fn drop(&mut self) {
        // Sqlite cleanup is automatic via TempDir's own Drop impl.
        // Postgres cleanup needs an async DROP SCHEMA CASCADE; if a
        // tokio runtime is available we kick it off there. Tests run
        // inside `#[tokio::test]` so this is always the case.
        #[cfg(feature = "postgres")]
        if let FixtureInner::Postgres { url, schema, pool } = &self.inner {
            let url = url.clone();
            let schema = schema.clone();
            let pool = pool.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    pool.close().await;
                    let opts: sqlx::postgres::PgConnectOptions =
                        match std::str::FromStr::from_str(&url) {
                            Ok(o) => o,
                            Err(_) => return,
                        };
                    let admin = match sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .connect_with(opts)
                        .await
                    {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP SCHEMA IF EXISTS {schema} CASCADE",
                    )))
                    .execute(&admin)
                    .await;
                    admin.close().await;
                });
            }
        }
    }
}

/// Build a fresh, isolated SQLite-backed storage for a single scenario.
/// The returned `StorageFixture` owns the tempdir and must outlive the
/// returned storage handle.
pub async fn bootstrap_sqlite() -> Result<(StorageHandle, StorageFixture)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("bdd_scenario.sqlite");
    let url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&url).await?;
    storage.initialize(BackendKind::Sqlite).await?;
    let handle: StorageHandle = Arc::new(storage) as Arc<dyn cc_lb_storage_api::Storage>;
    Ok((handle, StorageFixture::new_sqlite(dir)))
}

/// Build a fresh, isolated PostgreSQL-backed storage for a single
/// scenario. Returns `Ok(None)` when `CI_POSTGRES_URL` is unset so the
/// scenario can declare itself skipped without failing the test run;
/// the canonical env var name matches the existing postgres.yml CI
/// workflow as required by v3.2 plan §6.
#[cfg(feature = "postgres")]
pub async fn bootstrap_postgres() -> Result<Option<(StorageHandle, StorageFixture)>> {
    use std::str::FromStr;

    use cc_lb_storage_postgres::PostgresStorage;
    use sqlx::AssertSqlSafe;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
        return Ok(None);
    };

    let schema = format!("bdd_{}", uuid::Uuid::new_v4().simple());

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(PgConnectOptions::from_str(&url)?)
        .await?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin_pool)
        .await?;
    admin_pool.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]))
        .await?;

    let storage = PostgresStorage::new(pool.clone());
    storage.initialize(BackendKind::Postgres).await?;

    let handle: StorageHandle = Arc::new(storage) as Arc<dyn cc_lb_storage_api::Storage>;
    Ok(Some((
        handle,
        StorageFixture::new_postgres(url, schema, pool),
    )))
}
