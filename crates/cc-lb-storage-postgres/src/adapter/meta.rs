use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, MetaStore, StorageError, StorageResult};

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl MetaStore for PostgresStorage {
    async fn initialize(&self) -> StorageResult<()> {
        sqlx::migrate!("./migrations")
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
