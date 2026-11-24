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

fn sqlx_migration_lock_id(database_name: &str, schema_name: &str) -> i64 {
    let mut crc: u32 = 0xffff_ffff;
    for byte in database_name
        .as_bytes()
        .iter()
        .copied()
        .chain(std::iter::once(0))
        .chain(schema_name.as_bytes().iter().copied())
    {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    0x3d32_ad9e_i64 * i64::from(!crc)
}

// A row lock cannot protect an absent meta key. All meta writers and guarded
// readers use this per-key transaction lock, including first-insert CAS.
pub(super) async fn lock_meta_value(conn: &mut sqlx::PgConnection, key: &str) -> StorageResult<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(1667460194, hashtext($1))")
        .bind(key)
        .execute(conn)
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

#[async_trait]
impl MetaStore for PostgresStorage {
    async fn initialize(&self) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

        let (schema_name, database_name): (String, String) =
            sqlx::query_as("SELECT current_schema(), current_database()")
                .fetch_one(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;

        if schema_name != "public" {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(sqlx_migration_lock_id(&database_name, &schema_name))
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
                return finish_overview_error_attribution(self).await;
            }
        }

        tx.rollback().await.map_err(map_sqlx_error)?;

        MIGRATOR
            .run(&self.listener_pool)
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

        finish_overview_error_attribution(self).await
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        lock_meta_value(&mut tx, key).await?;
        sqlx::query(
            "INSERT INTO meta (key, value) VALUES ($1, $2)              ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn compare_and_put_meta_value(
        &self,
        key: &str,
        expected: Option<&str>,
        value: &str,
    ) -> StorageResult<bool> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        lock_meta_value(&mut tx, key).await?;
        let result =
            match expected {
                Some(expected) => {
                    sqlx::query("UPDATE meta SET value = $1 WHERE key = $2 AND value = $3")
                        .bind(value)
                        .bind(key)
                        .bind(expected)
                        .execute(&mut *tx)
                        .await
                }
                None => sqlx::query(
                    "INSERT INTO meta (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                )
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await,
            }
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
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

async fn finish_overview_error_attribution(storage: &PostgresStorage) -> StorageResult<()> {
    use super::usage_rollups::ROLLUP_BATCH_LIMIT;
    use std::collections::BTreeMap;

    loop {
        let phase: String = sqlx::query_scalar(
            "SELECT phase FROM overview_excluded_error_attribution_v1 WHERE id=1",
        )
        .fetch_one(&storage.pool)
        .await
        .map_err(map_sqlx_error)?;
        if phase == "complete" {
            return Ok(());
        }
        let mut tx = storage.pool.begin().await.map_err(map_sqlx_error)?;
        let (phase, cursor_xid, cursor_seq, upper_xid, upper_seq): (
            String,
            String,
            i64,
            String,
            i64,
        ) = sqlx::query_as(
            "SELECT phase,cursor_xid::text,cursor_seq,upper_xid::text,upper_seq
                 FROM overview_excluded_error_attribution_v1 WHERE id=1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if phase == "complete" {
            return Ok(());
        }
        if phase == "copy" {
            let copied = sqlx::query(
                "WITH batch AS (
                     DELETE FROM overview_excluded_error_legacy_v1 WHERE ctid IN (
                         SELECT ctid FROM overview_excluded_error_legacy_v1
                         ORDER BY resolution,bucket_start_unix_secs LIMIT 1000
                     ) RETURNING resolution,bucket_start_unix_secs,error_count,updated_at
                 )
                 INSERT INTO overview_excluded_error_rollups_v1
                 (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
                 SELECT resolution,bucket_start_unix_secs,'',error_count,updated_at FROM batch",
            )
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
            .rows_affected();
            if copied < 1000 {
                sqlx::query("DROP TABLE overview_excluded_error_legacy_v1")
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                sqlx::query(
                    "UPDATE overview_excluded_error_attribution_v1 SET phase='events' WHERE id=1",
                )
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            }
        } else {
            let rows: Vec<(String, i64, i64, String, i32)> = sqlx::query_as(
                "SELECT r.tx_id::text AS tx_id_text,r.seq,r.list_ts_ms,
                        COALESCE(r.upstream_id::text,'00000000-0000-0000-0000-000000000000'),
                        r.list_status::integer
                 FROM request_events_v1 r
                 WHERE (r.tx_id,r.seq)>($1::xid8,$2) AND (r.tx_id,r.seq)<=($3::xid8,$4)
                 ORDER BY r.tx_id,r.seq LIMIT $5",
            )
            .bind(&cursor_xid)
            .bind(cursor_seq)
            .bind(&upper_xid)
            .bind(upper_seq)
            .bind(ROLLUP_BATCH_LIMIT)
            .fetch_all(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            let mut deltas = BTreeMap::new();
            for (_, _, ts_ms, upstream, status) in &rows {
                if !matches!(*status, 401 | 403 | 404) {
                    continue;
                }
                for (resolution, width) in [("minute", 60_000), ("hour", 3_600_000)] {
                    *deltas
                        .entry((resolution, ts_ms / width * (width / 1000), upstream))
                        .or_insert(0_i64) += 1;
                }
            }
            for ((resolution, bucket, upstream), count) in deltas {
                let moved = sqlx::query(
                    "WITH moved AS (
                         UPDATE overview_excluded_error_rollups_v1 SET error_count=error_count-$1
                         WHERE resolution=$2 AND bucket_start_unix_secs=$3
                           AND upstream_id='' AND error_count>=$1
                         RETURNING 1
                     )
                     INSERT INTO overview_excluded_error_rollups_v1
                     (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
                     SELECT $2,$3,$4,$1,NOW() FROM moved
                     ON CONFLICT (resolution,bucket_start_unix_secs,upstream_id)
                     DO UPDATE SET error_count=overview_excluded_error_rollups_v1.error_count+EXCLUDED.error_count,
                                   updated_at=NOW()",
                )
                .bind(count)
                .bind(resolution)
                .bind(bucket)
                .bind(upstream)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
                .rows_affected();
                if moved == 0 {
                    let bucket_exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM overview_excluded_error_rollups_v1
                         WHERE resolution=$1 AND bucket_start_unix_secs=$2)",
                    )
                    .bind(resolution)
                    .bind(bucket)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                    if bucket_exists {
                        return Err(StorageError::Corrupted {
                            message: format!(
                                "overview excluded error attribution undercount: {resolution} bucket {bucket} cannot attribute {count} retained events"
                            ),
                        });
                    }
                    continue;
                }
                sqlx::query(
                    "DELETE FROM overview_excluded_error_rollups_v1
                     WHERE resolution=$1 AND bucket_start_unix_secs=$2 AND upstream_id='' AND error_count=0",
                ).bind(resolution).bind(bucket).execute(&mut *tx).await.map_err(map_sqlx_error)?;
            }
            let (next_xid, next_seq) = rows.last().map_or((upper_xid.as_str(), upper_seq), |row| {
                (row.0.as_str(), row.1)
            });
            sqlx::query(
                "UPDATE overview_excluded_error_attribution_v1
                 SET cursor_xid=$1::xid8,cursor_seq=$2,phase=$3 WHERE id=1",
            )
            .bind(next_xid)
            .bind(next_seq)
            .bind(if rows.len() < ROLLUP_BATCH_LIMIT as usize {
                "complete"
            } else {
                "events"
            })
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
    }
}

#[cfg(test)]
mod tests {
    use super::sqlx_migration_lock_id;

    #[test]
    fn sqlx_migration_lock_id_differs_across_schemas() {
        assert_ne!(
            sqlx_migration_lock_id("cclb", "schema_a"),
            sqlx_migration_lock_id("cclb", "schema_b")
        );
        assert_ne!(
            sqlx_migration_lock_id("db_a", "public"),
            sqlx_migration_lock_id("db_b", "public")
        );
    }
}
