use std::sync::OnceLock;

use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, MetaStore, StorageError, StorageResult, verify_applied_migrations,
};

use crate::{SqliteStorage, map_sqlx_error};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
static COMPILED_FAST_INIT_SQL: &str = include_str!(concat!(env!("OUT_DIR"), "/fast_init.sql"));
static FAST_INIT_SQL: OnceLock<String> = OnceLock::new();

fn meta_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let code = last as u32;
        let next_code = match code {
            0xD7FF => Some(0xE000),
            0x10_FFFF => None,
            _ => Some(code + 1),
        };
        if let Some(ch) = next_code.and_then(char::from_u32) {
            chars.push(ch);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

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
        let mut tx = crate::begin_immediate(self.pool()).await?;

        let (existing_objects,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if existing_objects == 0 {
            if !COMPILED_FAST_INIT_SQL.is_empty() {
                sqlx::raw_sql(sqlx::AssertSqlSafe(COMPILED_FAST_INIT_SQL))
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StorageError::Fatal {
                        message: error.to_string(),
                    })?;
            } else {
                let fast_sql = get_or_build_fast_init_sql().await?;
                sqlx::raw_sql(sqlx::AssertSqlSafe(fast_sql.as_str()))
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| StorageError::Fatal {
                        message: error.to_string(),
                    })?;
            }
            tx.commit().await.map_err(map_sqlx_error)?;
            return finish_overview_error_attribution(self).await;
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

        finish_overview_error_attribution(self).await
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

    async fn delete_meta_value(&self, key: &str) -> StorageResult<bool> {
        let result = sqlx::query("DELETE FROM meta_v1 WHERE key = ?")
            .bind(key)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected() > 0)
    }

    async fn list_meta_values_by_prefix(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<(String, String)>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        match (meta_prefix_upper_bound(prefix), after_key) {
            (Some(upper), Some(after)) => sqlx::query_as(
                "SELECT key, value FROM meta_v1 \
                 WHERE key >= ?1 AND key < ?2 AND key > ?3 \
                 ORDER BY key ASC LIMIT ?4",
            )
            .bind(prefix)
            .bind(upper)
            .bind(after)
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error),
            (Some(upper), None) => sqlx::query_as(
                "SELECT key, value FROM meta_v1 \
                 WHERE key >= ?1 AND key < ?2 \
                 ORDER BY key ASC LIMIT ?3",
            )
            .bind(prefix)
            .bind(upper)
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error),
            (None, Some(after)) => sqlx::query_as(
                "SELECT key, value FROM meta_v1 \
                 WHERE key >= ?1 AND key > ?2 \
                 ORDER BY key ASC LIMIT ?3",
            )
            .bind(prefix)
            .bind(after)
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error),
            (None, None) => sqlx::query_as(
                "SELECT key, value FROM meta_v1 \
                 WHERE key >= ?1 \
                 ORDER BY key ASC LIMIT ?2",
            )
            .bind(prefix)
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error),
        }
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

#[derive(sqlx::FromRow)]
struct OverviewAttributionEvent {
    id: i64,
    list_ts_ms: i64,
    upstream_id: Option<String>,
    list_status: i64,
}

async fn finish_overview_error_attribution(storage: &SqliteStorage) -> StorageResult<()> {
    use super::usage_rollups::LEGACY_UNATTRIBUTED_UPSTREAM_ID;
    use std::collections::BTreeMap;

    loop {
        let phase: String = sqlx::query_scalar(
            "SELECT phase FROM overview_excluded_error_attribution_v1 WHERE id=1",
        )
        .fetch_one(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
        if phase == "complete" {
            return Ok(());
        }
        let mut tx = storage.begin_immediate().await?;
        let (phase, cursor, upper): (String, i64, i64) = sqlx::query_as(
            "SELECT phase,cursor,upper_bound FROM overview_excluded_error_attribution_v1 WHERE id=1",
        ).fetch_one(&mut *tx).await.map_err(map_sqlx_error)?;
        if phase == "complete" {
            return Ok(());
        }
        if phase == "copy" {
            let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
                "SELECT resolution,bucket_start_unix_secs,error_count,updated_at
                 FROM overview_excluded_error_legacy_v1
                 ORDER BY resolution,bucket_start_unix_secs LIMIT 1000",
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            for (resolution, bucket, count, updated) in &rows {
                sqlx::query(
                    "INSERT INTO overview_excluded_error_rollups_v1
                     (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
                     VALUES (?,?,'',?,?)",
                )
                .bind(resolution)
                .bind(bucket)
                .bind(count)
                .bind(updated)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
                sqlx::query(
                    "DELETE FROM overview_excluded_error_legacy_v1
                     WHERE resolution=? AND bucket_start_unix_secs=?",
                )
                .bind(resolution)
                .bind(bucket)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;
            }
            if rows.len() < 1000 {
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
            let rows: Vec<OverviewAttributionEvent> = sqlx::query_as(
                "SELECT r.id,r.list_ts_ms,r.upstream_id,r.list_status
                 FROM request_events_v1 r WHERE r.id>? AND r.id<=?
                 ORDER BY r.id LIMIT 1000",
            )
            .bind(cursor)
            .bind(upper)
            .fetch_all(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            let mut deltas = BTreeMap::new();
            for event in &rows {
                if !matches!(event.list_status, 401 | 403 | 404) {
                    continue;
                }
                let owner = if let Some(upstream) = &event.upstream_id {
                    Some(uuid::Uuid::parse_str(upstream).map_err(|error| {
                        StorageError::Corrupted {
                            message: format!("invalid request event upstream_id: {error}"),
                        }
                    })?)
                } else {
                    // Prunable/mutable aggregates cannot prove ownership of a
                    // legacy event whose captured UUID is missing.
                    None
                };
                for (resolution, width) in [("minute", 60_000), ("hour", 3_600_000)] {
                    *deltas
                        .entry((resolution, event.list_ts_ms / width * (width / 1000), owner))
                        .or_insert(0_i64) += 1;
                }
            }
            for ((resolution, bucket, upstream), count) in deltas {
                let moved = sqlx::query(
                    "UPDATE overview_excluded_error_rollups_v1 SET error_count=error_count-?
                     WHERE resolution=? AND bucket_start_unix_secs=? AND upstream_id='' AND error_count>=?",
                ).bind(count).bind(resolution).bind(bucket).bind(count)
                    .execute(&mut *tx).await.map_err(map_sqlx_error)?.rows_affected();
                if moved == 0 {
                    let bucket_exists: bool = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM overview_excluded_error_rollups_v1
                         WHERE resolution=? AND bucket_start_unix_secs=?)",
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
                let upstream_id = upstream.map(|id| id.to_string());
                sqlx::query(
                    "INSERT INTO overview_excluded_error_rollups_v1
                     (resolution,bucket_start_unix_secs,upstream_id,error_count,updated_at)
                     VALUES (?,?,?,?,unixepoch())
                     ON CONFLICT (resolution,bucket_start_unix_secs,upstream_id)
                     DO UPDATE SET error_count=error_count+excluded.error_count,updated_at=unixepoch()",
                ).bind(resolution).bind(bucket)
                    .bind(upstream_id.as_deref().unwrap_or(LEGACY_UNATTRIBUTED_UPSTREAM_ID)).bind(count)
                    .execute(&mut *tx).await.map_err(map_sqlx_error)?;
                sqlx::query(
                    "DELETE FROM overview_excluded_error_rollups_v1
                     WHERE resolution=? AND bucket_start_unix_secs=? AND upstream_id='' AND error_count=0",
                ).bind(resolution).bind(bucket).execute(&mut *tx).await.map_err(map_sqlx_error)?;
            }
            let next = rows.last().map_or(upper, |row| row.id);
            sqlx::query(
                "UPDATE overview_excluded_error_attribution_v1 SET cursor=?,phase=? WHERE id=1",
            )
            .bind(next)
            .bind(if rows.len() < 1000 {
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
