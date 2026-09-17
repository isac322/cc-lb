//! Bounded startup migration gate for the 0.4.9 schema-compat bridge.
//!
//! The bridge binary ships the full migration manifest through version 123 so
//! that databases already migrated by a newer binary are recognized, but it
//! only *applies* migrations through [`BRIDGE_MIGRATION_CAP`]. Every applied
//! row in the migrations table is validated against the embedded manifest —
//! including versions above the cap — so a tampered, dirty, or unknown
//! migration history fails startup instead of silently drifting.
//! Migrations run on a connection acquired through the pool and then detached:
//! acquiring runs `after_connect` hooks (the configured `statement_timeout`)
//! and `test_before_acquire`, while detaching guarantees the connection is
//! never returned to the pool. That matters because sqlx's migrator returns
//! early on validation/apply errors without releasing the session-level
//! `pg_advisory_lock`, and a pooled connection would carry that held lock back
//! into the pool. A detached connection releases every session-level lock when
//! it is closed or dropped, including on cancellation.

use std::collections::HashMap;

use cc_lb_storage_api::{StorageError, StorageResult};
use sqlx::migrate::{AppliedMigration, Migrate, MigrateError, Migrator};
use sqlx::{Connection, PgConnection, PgPool};

use crate::error_map::map_sqlx_error;

/// Highest migration version this binary is allowed to apply. Versions above
/// the cap are validated when already applied but are never executed.
const BRIDGE_MIGRATION_CAP: i64 = 114;

/// Full embedded manifest (through version 123). Kept as a static so the
/// compile-time `migrate!` checksums are shared by validation and `run_to`.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub(crate) async fn run_capped_migrations(pool: &PgPool) -> StorageResult<()> {
    // `acquire` applies `after_connect` hooks and `test_before_acquire`;
    // `detach` keeps the connection out of the pool so a leaked advisory lock
    // can never be handed to an unrelated caller, and lets the pool open a
    // replacement.
    let mut conn = pool.acquire().await.map_err(map_sqlx_error)?.detach();

    let result = migrate_locked(&mut conn).await;

    // `pg_advisory_lock` is session-scoped: closing the session releases the
    // lock even if `migrate_locked` returned before `unlock` ran. A close
    // failure only means the socket is already gone, which also releases it.
    if let Err(error) = conn.close().await {
        tracing::warn!(%error, "postgres migration connection close failed");
    }

    result
}

async fn migrate_locked(conn: &mut PgConnection) -> StorageResult<()> {
    // Hold the advisory lock across validation and application so the checked
    // state is the state migrations run against. `run_to` re-acquires the same
    // session lock internally (advisory locks are re-entrant), so the counts
    // stay balanced.
    conn.lock().await.map_err(migrate_error)?;

    let result = migrate_inner(conn).await;
    let unlock = conn.unlock().await.map_err(migrate_error);

    result.and(unlock)
}

async fn migrate_inner(conn: &mut PgConnection) -> StorageResult<()> {
    let table_name = &*MIGRATOR.table_name;

    conn.ensure_migrations_table(table_name)
        .await
        .map_err(migrate_error)?;

    if let Some(version) = conn
        .dirty_version(table_name)
        .await
        .map_err(migrate_error)?
    {
        return Err(migrate_error(MigrateError::Dirty(version)));
    }

    let applied = conn
        .list_applied_migrations(table_name)
        .await
        .map_err(migrate_error)?;
    validate_full_manifest(&applied)?;

    MIGRATOR
        .run_direct(Some(BRIDGE_MIGRATION_CAP), conn, false)
        .await
        .map_err(migrate_error)
}

/// Validates every applied migration against the complete embedded manifest,
/// including versions above [`BRIDGE_MIGRATION_CAP`]. `run_to` alone only
/// checksum-compares applied versions up to the cap, so this pass is what
/// makes a tampered future migration fatal.
fn validate_full_manifest(applied: &[AppliedMigration]) -> StorageResult<()> {
    let known: HashMap<i64, &[u8]> = MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| (migration.version, &*migration.checksum))
        .collect();

    for migration in applied {
        match known.get(&migration.version) {
            None => {
                return Err(migrate_error(MigrateError::VersionMissing(
                    migration.version,
                )));
            }
            Some(&checksum) if checksum != &*migration.checksum => {
                return Err(migrate_error(MigrateError::VersionMismatch(
                    migration.version,
                )));
            }
            Some(_) => {}
        }
    }

    Ok(())
}

fn migrate_error(error: MigrateError) -> StorageError {
    StorageError::Fatal {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

    use cc_lb_storage_api::{BackendKind, MetaStore};
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::{AssertSqlSafe, PgPool, Row};
    use uuid::Uuid;

    use super::*;
    use crate::adapter::PostgresStorage;

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[tokio::test]
    async fn initialize_applies_through_cap_and_accepts_newer_schema() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        let storage = fixture.storage();

        storage.initialize(BackendKind::Postgres).await?;
        assert_eq!(fixture.max_applied_version().await?, BRIDGE_MIGRATION_CAP);
        assert!(
            fixture
                .applied_versions_above(BRIDGE_MIGRATION_CAP)
                .await?
                .is_empty()
        );
        assert_migration_lock_free(&fixture).await?;

        // Simulate a newer binary that applied the full manifest, then prove
        // the capped gate accepts the database without further DDL.
        fixture.run_full_migrator().await?;
        assert_eq!(fixture.max_applied_version().await?, 123);

        storage.initialize(BackendKind::Postgres).await?;
        assert_eq!(fixture.max_applied_version().await?, 123);
        assert_migration_lock_free(&fixture).await?;

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn gate_connection_inherits_after_connect_settings() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };

        // A deliberately held advisory lock makes the configured timeout
        // deterministic; this does not assume migrations take a minimum time.
        let timed = Fixture::create_with_after_connect(&url, |schema| {
            format!("SET statement_timeout = '100ms'; SET search_path = {schema}")
        })
        .await?;
        let mut holder = timed.dedicated_connection().await?;
        holder.lock().await?;
        let error = tokio::time::timeout(Duration::from_secs(5), timed.run_capped())
            .await?
            .expect_err("configured statement timeout must abort the blocked lock");
        assert!(error.to_string().contains("statement timeout"), "{error}");
        holder.unlock().await?;
        holder.close().await?;
        assert_migration_lock_free(&timed).await?;
        timed.drop_schema().await?;

        // `search_path` applied only via `after_connect` must also reach the
        // migration connection: the migrations table lands in the test schema
        // only if the hook ran.
        let fixture = Fixture::create_with_after_connect(&url, |schema| {
            format!("SET search_path = {schema}")
        })
        .await?;
        fixture.run_capped().await?;
        assert_eq!(fixture.max_applied_version().await?, BRIDGE_MIGRATION_CAP);
        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn initialize_rejects_tampered_checksum() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        fixture.run_capped().await?;

        // A tampered checksum at or below the cap must fail.
        fixture.tamper_checksum(50).await?;
        let error = fixture
            .run_capped()
            .await
            .expect_err("tampered applied checksum should fail");
        assert!(matches!(error, StorageError::Fatal { .. }));
        assert!(error.to_string().contains("50"));
        fixture.restore_checksum(50).await?;

        // A tampered checksum above the cap must also fail; `run_to` alone
        // would never compare it.
        fixture.run_full_migrator().await?;
        fixture.tamper_checksum(120).await?;
        let error = fixture
            .run_capped()
            .await
            .expect_err("tampered future checksum should fail");
        assert!(matches!(error, StorageError::Fatal { .. }));
        assert!(error.to_string().contains("120"));

        assert_migration_lock_free(&fixture).await?;
        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn initialize_rejects_unknown_applied_version() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        fixture.run_capped().await?;

        fixture.insert_migration_row(999, true, &[0xAB; 48]).await?;
        let error = fixture
            .run_capped()
            .await
            .expect_err("unknown applied version should fail");
        assert!(matches!(error, StorageError::Fatal { .. }));
        assert!(error.to_string().contains("999"));

        assert_migration_lock_free(&fixture).await?;
        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn initialize_rejects_dirty_migration() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        fixture.run_capped().await?;

        fixture
            .insert_migration_row(500, false, &[0xCD; 48])
            .await?;
        let error = fixture
            .run_capped()
            .await
            .expect_err("dirty migration should fail");
        assert!(matches!(error, StorageError::Fatal { .. }));
        assert!(error.to_string().contains("500"));

        assert_migration_lock_free(&fixture).await?;
        fixture.drop_schema().await?;
        Ok(())
    }

    /// Mirrors sqlx-postgres `generate_lock_id`: the advisory lock key is
    /// `0x3d32ad9e * CRC-32/ISO-HDLC(current_database())`.
    async fn migration_lock_id(fixture: &Fixture) -> TestResult<i64> {
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&fixture.pool)
            .await?;
        Ok(0x3d32_ad9e_i64 * crc32_iso_hdlc(database.as_bytes()))
    }

    fn crc32_iso_hdlc(bytes: &[u8]) -> i64 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
        i64::from(!crc)
    }

    async fn assert_migration_lock_free(fixture: &Fixture) -> TestResult<()> {
        let lock_id = migration_lock_id(fixture).await?;
        let mut conn = fixture.dedicated_connection().await?;
        sqlx::query("SET statement_timeout = '5s'")
            .execute(&mut conn)
            .await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_id)
            .execute(&mut conn)
            .await?;
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_id)
            .execute(&mut conn)
            .await?;
        conn.close().await?;
        Ok(())
    }

    struct Fixture {
        url: String,
        schema: String,
        pool: PgPool,
    }

    impl Fixture {
        async fn create(url: &str) -> TestResult<Self> {
            Self::create_with_after_connect(url, |schema| format!("SET search_path = {schema}"))
                .await
        }

        /// Pool whose `search_path` (and any extra session settings) is applied
        /// by an `after_connect` hook instead of connect options, mirroring how
        /// `storage_factory` delivers `statement_timeout`.
        async fn create_with_after_connect(
            url: &str,
            session_sql: impl Fn(&str) -> String,
        ) -> TestResult<Self> {
            let schema = format!("test_migration_gate_{}", Uuid::new_v4().simple());
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(PgConnectOptions::from_str(url)?)
                .await?;
            sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {schema}")))
                .execute(&admin_pool)
                .await?;
            admin_pool.close().await;

            let session_sql = session_sql(&schema);
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .after_connect(move |conn, _meta| {
                    let session_sql = session_sql.clone();
                    Box::pin(async move {
                        sqlx::raw_sql(AssertSqlSafe(session_sql))
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect_with(PgConnectOptions::from_str(url)?.database(&schema))
                .await?;
            sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
                .execute(&pool)
                .await?;

            Ok(Self {
                url: url.to_owned(),
                schema,
                pool,
            })
        }

        fn storage(&self) -> PostgresStorage {
            PostgresStorage::new(self.pool.clone(), Arc::new(cc_lb_clock::SystemClock))
        }

        // Detaching retains the fixture's database and after_connect settings.
        async fn dedicated_connection(&self) -> TestResult<PgConnection> {
            Ok(self.pool.acquire().await?.detach())
        }

        async fn run_capped(&self) -> StorageResult<()> {
            run_capped_migrations(&self.pool).await
        }

        /// Applies the complete manifest like an uncapped (newer) binary.
        async fn run_full_migrator(&self) -> TestResult<()> {
            let mut conn = self.dedicated_connection().await?;
            MIGRATOR.run(&mut conn).await?;
            conn.close().await?;
            Ok(())
        }

        async fn applied_versions(&self) -> TestResult<Vec<i64>> {
            let rows = sqlx::query("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?;
            Ok(rows.iter().map(|row| row.get::<i64, _>(0)).collect())
        }

        async fn max_applied_version(&self) -> TestResult<i64> {
            Ok(self
                .applied_versions()
                .await?
                .into_iter()
                .max()
                .unwrap_or(0))
        }

        async fn applied_versions_above(&self, cap: i64) -> TestResult<Vec<i64>> {
            Ok(self
                .applied_versions()
                .await?
                .into_iter()
                .filter(|version| *version > cap)
                .collect())
        }

        async fn tamper_checksum(&self, version: i64) -> TestResult<()> {
            sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2")
                .bind(vec![0xEE_u8; 48])
                .bind(version)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn restore_checksum(&self, version: i64) -> TestResult<()> {
            let checksum = MIGRATOR
                .iter()
                .find(|migration| migration.version == version)
                .ok_or("version missing from manifest")?
                .checksum
                .to_vec();
            sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2")
                .bind(checksum)
                .bind(version)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn insert_migration_row(
            &self,
            version: i64,
            success: bool,
            checksum: &[u8],
        ) -> TestResult<()> {
            sqlx::query(
                "INSERT INTO _sqlx_migrations \
                 (version, description, success, checksum, execution_time) \
                 VALUES ($1, 'fixture', $2, $3, -1)",
            )
            .bind(version)
            .bind(success)
            .bind(checksum)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn drop_schema(self) -> TestResult<()> {
            self.pool.close().await;
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(PgConnectOptions::from_str(&self.url)?)
                .await?;
            sqlx::query(AssertSqlSafe(format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.schema
            )))
            .execute(&admin_pool)
            .await?;
            admin_pool.close().await;
            Ok(())
        }
    }
}
