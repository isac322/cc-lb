use std::sync::OnceLock;

use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, MetaStore, StorageError, StorageResult, verify_applied_migrations,
};

use crate::{SqliteStorage, map_sqlx_error};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
static FAST_INIT_SQL: OnceLock<String> = OnceLock::new();

async fn get_or_build_fast_init_sql() -> Result<&'static String, StorageError> {
    if let Some(sql) = FAST_INIT_SQL.get() {
        return Ok(sql);
    }
    let mem_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .map_err(map_sqlx_error)?;

    MIGRATOR
        .run(&mem_pool)
        .await
        .map_err(|error| StorageError::Fatal {
            message: error.to_string(),
        })?;

    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES ('backend_kind', ?) ON CONFLICT(key) DO NOTHING",
    )
    .bind(BackendKind::Sqlite.as_str())
    .execute(&mem_pool)
    .await
    .map_err(map_sqlx_error)?;

    let mut script = String::new();

    let tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid",
    )
    .fetch_all(&mem_pool)
    .await
    .map_err(map_sqlx_error)?;

    for (_, ddl) in &tables {
        script.push_str(ddl);
        script.push_str(";\n");
    }

    for (table_name, _) in &tables {
        let col_info: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT name FROM pragma_table_info('{table_name}') ORDER BY cid"
        )))
        .fetch_all(&mem_pool)
        .await
        .map_err(map_sqlx_error)?;
        if col_info.is_empty() {
            continue;
        }
        let cols_quoted: Vec<String> = col_info.iter().map(|(c,)| format!("\"{c}\"")).collect();
        let quote_exprs: Vec<String> = col_info
            .iter()
            .map(|(c,)| format!("quote(\"{c}\")"))
            .collect();
        let select_sql = format!(
            "SELECT 'INSERT INTO \"{table_name}\" ({}) VALUES (' || {} || ');' FROM \"{table_name}\"",
            cols_quoted.join(", "),
            quote_exprs.join(" || ', ' || ")
        );
        let inserts: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(select_sql))
            .fetch_all(&mem_pool)
            .await
            .map_err(map_sqlx_error)?;
        for (ins,) in inserts {
            script.push_str(&ins);
            script.push('\n');
        }
    }

    let other_objects: Vec<(String,)> = sqlx::query_as(
        "SELECT sql FROM sqlite_master WHERE type != 'table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid",
    )
    .fetch_all(&mem_pool)
    .await
    .map_err(map_sqlx_error)?;

    for (ddl,) in other_objects {
        script.push_str(&ddl);
        script.push_str(";\n");
    }

    mem_pool.close().await;
    let _ = FAST_INIT_SQL.set(script);
    Ok(FAST_INIT_SQL.get().expect("FAST_INIT_SQL initialized"))
}

#[async_trait]
impl MetaStore for SqliteStorage {
    async fn initialize(&self) -> StorageResult<()> {
        let fast_sql = get_or_build_fast_init_sql().await?;
        let mut tx = crate::begin_immediate(self.pool()).await?;

        let (existing_objects,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if existing_objects == 0 {
            sqlx::raw_sql(sqlx::AssertSqlSafe(fast_sql.as_str()))
                .execute(&mut *tx)
                .await
                .map_err(|error| StorageError::Fatal {
                    message: error.to_string(),
                })?;
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(());
        }
        tx.rollback().await.map_err(map_sqlx_error)?;

        MIGRATOR
            .run(self.pool())
            .await
            .map_err(|error| StorageError::Fatal {
                message: error.to_string(),
            })?;

        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES ('backend_kind', ?) ON CONFLICT(key) DO NOTHING",
        )
        .bind(BackendKind::Sqlite.as_str())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn backend_kind(&self) -> StorageResult<BackendKind> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = 'backend_kind'")
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        parse_backend_kind(value.as_deref().ok_or_else(|| StorageError::Corrupted {
            message: "missing backend_kind meta value".to_owned(),
        })?)
    }

    async fn get_meta_value(&self, key: &str) -> StorageResult<Option<String>> {
        sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = ?")
            .bind(key)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)
    }

    async fn put_meta_value(&self, key: &str, value: &str) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(self.pool())
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
                    sqlx::query("UPDATE meta_v1 SET value = ? WHERE key = ? AND value = ?")
                        .bind(value)
                        .bind(key)
                        .bind(expected)
                        .execute(self.pool())
                        .await
                }
                None => sqlx::query(
                    "INSERT INTO meta_v1 (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                )
                .bind(key)
                .bind(value)
                .execute(self.pool())
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
        .fetch_all(self.pool())
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
