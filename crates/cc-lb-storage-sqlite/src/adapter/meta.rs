use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, CURRENT_CONTRACT_VERSION, MetaStore, StorageError, StorageResult,
};

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl MetaStore for SqliteStorage {
    async fn initialize(&self, requested: BackendKind) -> StorageResult<()> {
        sqlx::migrate!("./migrations")
            .run(self.pool())
            .await
            .map_err(|error| StorageError::Fatal {
                message: error.to_string(),
            })?;

        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES ('backend_kind', ?) ON CONFLICT(key) DO NOTHING",
        )
        .bind(requested.as_str())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES ('contract_version', ?) ON CONFLICT(key) DO NOTHING",
        )
        .bind(CURRENT_CONTRACT_VERSION.to_string())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES ('killswitch_enabled', 'false') ON CONFLICT(key) DO NOTHING",
        )
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        let stored = self.backend_kind().await?;
        if stored != requested {
            return Err(StorageError::BackendKindMismatch {
                stored,
                configured: requested,
            });
        }

        let version = self.contract_version().await?;
        if version > CURRENT_CONTRACT_VERSION {
            return Err(StorageError::SchemaMismatch {
                found: version,
                expected: CURRENT_CONTRACT_VERSION,
            });
        }

        Ok(())
    }

    async fn contract_version(&self) -> StorageResult<u32> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = 'contract_version'")
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        value
            .map(|value| parse_contract_version(&value))
            .transpose()
            .map(|value| value.unwrap_or(CURRENT_CONTRACT_VERSION))
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

    async fn killswitch_enabled(&self) -> StorageResult<bool> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = 'killswitch_enabled'")
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        value
            .map(|value| parse_bool(&value, "killswitch_enabled"))
            .transpose()
            .map(|value| value.unwrap_or(false))
    }

    async fn set_killswitch_enabled(&self, enabled: bool) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO meta_v1 (key, value) VALUES ('killswitch_enabled', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(enabled.to_string())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }
}

fn parse_backend_kind(value: &str) -> StorageResult<BackendKind> {
    match value {
        "redb" => Ok(BackendKind::Redb),
        "postgres" => Ok(BackendKind::Postgres),
        "sqlite" => Ok(BackendKind::Sqlite),
        value => Err(StorageError::Corrupted {
            message: format!("invalid backend_kind meta value {value}"),
        }),
    }
}

fn parse_contract_version(value: &str) -> StorageResult<u32> {
    value.parse::<u32>().map_err(|_| StorageError::Corrupted {
        message: format!("invalid contract_version meta value {value}"),
    })
}

fn parse_bool(value: &str, key: &str) -> StorageResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        value => Err(StorageError::Corrupted {
            message: format!("invalid {key} meta value {value}"),
        }),
    }
}
