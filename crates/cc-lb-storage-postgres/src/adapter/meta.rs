use std::fmt::Write as _;

use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, MetaStore, StorageError, StorageResult, verify_applied_migrations,
};

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
static FAST_INIT_DDL_SQL: &str =
    include_str!(concat!(env!("OUT_DIR"), "/postgres_fast_init_ddl.sql"));
static FAST_INIT_FULL_SQL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn get_fast_init_full_sql() -> &'static str {
    FAST_INIT_FULL_SQL
        .get_or_init(|| {
            let mut sql = String::from(
                "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
);
",
            );
            sql.push_str(FAST_INIT_DDL_SQL);
            sql.push_str("\n;\n");
            for migration in MIGRATOR
                .iter()
                .filter(|migration| !migration.migration_type.is_down_migration())
            {
                let escaped_description = migration.description.replace('\'', "''");
                let checksum = migration
                    .checksum
                    .as_ref()
                    .iter()
                    .fold(String::new(), |mut checksum, byte| {
                        write!(checksum, "{byte:02x}").expect("writing to String cannot fail");
                        checksum
                    });
                writeln!(
                    sql,
                    "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES ({}, '{}', now(), TRUE, decode('{}', 'hex'), 1);",
                    migration.version,
                    escaped_description,
                    checksum,
                )
                .expect("writing to String cannot fail");
            }
            sql.push_str(
                "INSERT INTO meta (key, value) VALUES ('backend_kind', 'postgres') ON CONFLICT (key) DO NOTHING;
",
            );
            sql
        })
        .as_str()
}

fn sqlx_migration_lock_id(database_name: &str) -> i64 {
    let mut crc: u32 = 0xffff_ffff;
    for &byte in database_name.as_bytes() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    0x3d32_ad9e_i64 * i64::from(!crc)
}

#[async_trait]
impl MetaStore for PostgresStorage {
    async fn initialize(&self) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

        let (is_non_public, database_name): (bool, String) =
            sqlx::query_as("SELECT current_schema() IS DISTINCT FROM 'public', current_database()")
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;

        if is_non_public {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(sqlx_migration_lock_id(&database_name))
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;

            let (existing_tables,): (i64,) = sqlx::query_as(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema()",
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            if existing_tables == 0 {
                sqlx::raw_sql(sqlx::AssertSqlSafe(get_fast_init_full_sql()))
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StorageError::Fatal {
                        message: error.to_string(),
                    })?;
                tx.commit().await.map_err(map_sqlx_error)?;
                return Ok(());
            }
        }

        tx.rollback().await.map_err(map_sqlx_error)?;

        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|error| StorageError::Fatal {
                message: error.to_string(),
            })?;

        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ('backend_kind', $1)              ON CONFLICT (key) DO NOTHING",
        )
        .bind(BackendKind::Postgres.as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM meta WHERE key = 'backend_kind'")
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx_error)?;

        parse_backend_kind(value.as_deref().ok_or_else(|| StorageError::Corrupted {
            message: "missing backend_kind meta value".to_owned(),
        })?)
    }

    async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
        sqlx::query_scalar("SELECT value FROM meta WHERE key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)
    }

    async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ($1, $2)              ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn compare_and_put_meta_value(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &str,
    ) -> StorageResult<bool> {
        let result =
            match expected {
                Some(expected) => {
                    sqlx::query("UPDATE meta SET value = $1 WHERE key = $2 AND value = $3")
                        .bind(value)
                        .bind(key)
                        .bind(expected)
                        .execute(&self.pool)
                        .await
                }
                None => sqlx::query(
                    "INSERT INTO meta (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                )
                .bind(key)
                .bind(value)
                .execute(&self.pool)
                .await,
            }
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() == 1)
    }

    /// Read-only: never migrates, so an operator CLI pointed at an older or
    /// tampered schema fails instead of rewriting it.
    async fn verify_schema_current(&self) -> StorageResult<()> {
        let applied: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT version, checksum FROM _sqlx_migrations WHERE success ORDER BY version",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Fatal {
            message: format!("cannot read applied migrations from _sqlx_migrations: {error}"),
        })?;
        verify_applied_migrations(
            MIGRATOR
                .iter()
                .filter(|migration| !migration.migration_type.is_down_migration())
                .map(|migration| (migration.version, migration.checksum.as_ref())),
            &applied,
        )
    }
}

fn parse_backend_kind(value: &str) -> StorageResult<BackendKind> {
    match value {
        "postgres" => Ok(BackendKind::Postgres),
        "sqlite" => Ok(BackendKind::Sqlite),
        value => Err(StorageError::Corrupted {
            message: format!("invalid backend_kind meta value {value}"),
        }),
    }
}
