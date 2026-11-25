//! Database migrations for the scheduler tables.

use std::future::Future;

use sqlx::{Database, Pool};

use crate::error::{Result, SchedulerError};

#[cfg(feature = "postgres")]
const SCHEDULER_POST_SETUP_LOCK_KEY: i64 = 0x_CC1B_5CDE_0002_i64;
#[cfg(feature = "postgres")]
const LATEST_POST_SETUP_VERSION: i64 = 11;
#[cfg(feature = "sqlite")]
const LATEST_SQLITE_POST_SETUP_VERSION: i64 = 11;

#[doc(hidden)]
pub trait ApalisPostSetupMigrationDatabase: Database {
    #[doc(hidden)]
    fn apply_post_setup(pool: &Pool<Self>) -> impl Future<Output = Result<()>> + Send;
}

#[cfg(feature = "sqlite")]
async fn seed_sqlite_legacy_ledger_atomically(
    conn: &mut sqlx::SqliteConnection,
    migrator: &sqlx::migrate::Migrator,
) -> Result<()> {
    use sqlx::Connection as _;

    let mut tx = conn.begin().await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _cc_lb_scheduler_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;
    for migration in migrator.iter().filter(|migration| {
        !migration.migration_type.is_down_migration()
            && migration.version <= LATEST_SQLITE_POST_SETUP_VERSION
    }) {
        sqlx::query(
            "INSERT INTO _cc_lb_scheduler_migrations
                (version, description, success, checksum, execution_time)
             VALUES (?1, ?2, TRUE, ?3, -1)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
impl ApalisPostSetupMigrationDatabase for sqlx::Sqlite {
    async fn apply_post_setup(pool: &Pool<Self>) -> Result<()> {
        let mut conn = pool.acquire().await?;
        // apalis-sqlite 1.0.0's SqliteStorage::setup sets synchronous = OFF;
        // restore NORMAL immediately after setup on every boot.
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(&mut *conn)
            .await?;

        let mut migrator = sqlx::migrate!("./migrations/sqlite");
        migrator.dangerous_set_table_name("_cc_lb_scheduler_migrations");
        migrator.set_locking(false);

        let ledger_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_cc_lb_scheduler_migrations')",
        )
        .fetch_one(&mut *conn)
        .await?;

        if !ledger_exists && pre_ledger_sqlite_migrations_applied(&mut conn).await? {
            seed_sqlite_legacy_ledger_atomically(&mut conn, &migrator).await?;
        }

        migrator
            .run_direct(None, &mut *conn, false)
            .await
            .map_err(|err| SchedulerError::Database(err.into()))?;
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
async fn pre_ledger_sqlite_migrations_applied(conn: &mut sqlx::SqliteConnection) -> Result<bool> {
    let price_catalog_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'price_catalog_versions')",
    )
    .fetch_one(&mut *conn)
    .await?;
    if !price_catalog_exists {
        return Ok(false);
    }

    let compat_etags_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'anthropic_compat_etags')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_etag_col: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('anthropic_compat_etags') WHERE name = 'etag')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let tidx_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'TIdx')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let idempotency_sql: Option<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_idempotency_key'",
    )
    .fetch_optional(&mut *conn)
    .await?;
    let legacy_cursor_col: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('oauth_usage_poll_cursors') WHERE name = 'last_throttle_count')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Jobs WHERE job_type IN ('entity', 'singleton', 'scheduler_reconcile')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_workers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM Workers WHERE worker_type IN ('entity', 'singleton', 'scheduler_reconcile')",
    )
    .fetch_one(&mut *conn)
    .await?;

    Ok(compat_etags_exists
        && !legacy_etag_col
        && !tidx_exists
        && idempotency_sql.is_some_and(|sql| !sql.contains("WHERE"))
        && !legacy_cursor_col
        && legacy_jobs == 0
        && legacy_workers == 0)
}

#[cfg(feature = "postgres")]
impl ApalisPostSetupMigrationDatabase for sqlx::Postgres {
    async fn apply_post_setup(pool: &Pool<Self>) -> Result<()> {
        let mut conn = pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(SCHEDULER_POST_SETUP_LOCK_KEY)
            .execute(&mut *conn)
            .await?;
        let outcome = apply_postgres_post_setup_migrations_on_conn(&mut conn).await;
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SCHEDULER_POST_SETUP_LOCK_KEY)
            .execute(&mut *conn)
            .await;
        outcome
    }
}

#[cfg(feature = "postgres")]
async fn seed_postgres_legacy_ledger_atomically(
    conn: &mut sqlx::PgConnection,
    migrator: &sqlx::migrate::Migrator,
) -> Result<()> {
    use sqlx::Connection as _;

    let mut tx = conn.begin().await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cc_lb_scheduler._sqlx_post_setup_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
            success BOOLEAN NOT NULL,
            checksum BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;
    for migration in migrator.iter().filter(|migration| {
        !migration.migration_type.is_down_migration()
            && migration.version <= LATEST_POST_SETUP_VERSION
    }) {
        sqlx::query(
            "INSERT INTO cc_lb_scheduler._sqlx_post_setup_migrations
                (version, description, success, checksum, execution_time)
             VALUES ($1, $2, TRUE, $3, -1)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(feature = "postgres")]
pub async fn apply_postgres_post_setup_migrations_on_conn(
    conn: &mut sqlx::PgConnection,
) -> Result<()> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS cc_lb_scheduler")
        .execute(&mut *conn)
        .await?;

    let mut migrator = sqlx::migrate!("./migrations/postgres");
    migrator.dangerous_set_table_name("cc_lb_scheduler._sqlx_post_setup_migrations");
    migrator.set_locking(false);

    let ledger_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('cc_lb_scheduler._sqlx_post_setup_migrations') IS NOT NULL",
    )
    .fetch_one(&mut *conn)
    .await?;

    if !ledger_exists && pre_ledger_postgres_migrations_applied(conn).await? {
        seed_postgres_legacy_ledger_atomically(conn, &migrator).await?;
    }

    migrator
        .run_direct(None, conn, false)
        .await
        .map_err(|err| SchedulerError::Database(err.into()))?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn pre_ledger_postgres_migrations_applied(conn: &mut sqlx::PgConnection) -> Result<bool> {
    let price_catalog_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('price_catalog_versions') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !price_catalog_exists {
        return Ok(false);
    }

    let compat_etags_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('anthropic_compat_etags') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    let legacy_etag_col: bool = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 \
             FROM pg_attribute \
             WHERE attrelid = to_regclass('anthropic_compat_etags') \
               AND attname = 'etag' \
               AND NOT attisdropped \
         )",
    )
    .fetch_one(&mut *conn)
    .await?;
    let tidx_exists: bool = sqlx::query_scalar("SELECT to_regclass('apalis.tidx') IS NOT NULL")
        .fetch_one(&mut *conn)
        .await?;
    let full_unique_present: bool = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 \
             FROM pg_index indexes \
             JOIN pg_class classes ON classes.oid = indexes.indexrelid \
             JOIN pg_namespace namespaces ON namespaces.oid = classes.relnamespace \
             WHERE namespaces.nspname = 'apalis' \
               AND classes.relname = 'idx_jobs_idempotency_key' \
               AND indexes.indisunique \
               AND indexes.indpred IS NULL \
         )",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_cursor_col: bool = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 \
             FROM pg_attribute \
             WHERE attrelid = to_regclass('oauth_usage_poll_cursors') \
               AND attname = 'last_throttle_count' \
               AND NOT attisdropped \
         )",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_jobs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM apalis.jobs WHERE job_type IN ('entity', 'singleton', 'scheduler_reconcile')",
    )
    .fetch_one(&mut *conn)
    .await?;
    let legacy_workers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM apalis.workers WHERE worker_type IN ('entity', 'singleton', 'scheduler_reconcile')",
    )
    .fetch_one(&mut *conn)
    .await?;

    Ok(compat_etags_exists
        && !legacy_etag_col
        && !tidx_exists
        && full_unique_present
        && !legacy_cursor_col
        && legacy_jobs == 0
        && legacy_workers == 0)
}

pub async fn apply_post_setup_migrations<Db>(pool: &Pool<Db>) -> Result<()>
where
    Db: ApalisPostSetupMigrationDatabase,
{
    Db::apply_post_setup(pool).await
}
