use async_trait::async_trait;
use cc_lb_storage_api::{ConfigDraftState, ConfigStore, HistoryEntry, StorageResult};
use serde_json::Value;
use sqlx::Row;

use crate::{
    adapter::{PostgresStorage, conflict, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl ConfigStore for PostgresStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        let config = sqlx::query_scalar::<_, Value>(
            "SELECT config FROM config_draft_v1 WHERE id = 'singleton'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        config
            .map(serde_json::from_value)
            .transpose()
            .map(|state| state.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn put_config_draft(
        &self,
        mut new: ConfigDraftState,
        expected_revision: u64,
    ) -> StorageResult<u64> {
        let revision = expected_revision.checked_add(1).ok_or_else(|| {
            cc_lb_storage_api::StorageError::Fatal {
                message: "config draft revision overflow".to_owned(),
            }
        })?;
        new.revision = revision;
        new.last_validated_revision = None;
        new.last_validation_error = None;
        let config = serde_json::to_value(&new)?;

        let stored_revision = sqlx::query_scalar::<_, i64>(
            "INSERT INTO config_draft_v1 (id, config, revision, updated_at)              SELECT 'singleton', $1, $2, NOW() WHERE $3 = 0              ON CONFLICT (id) DO UPDATE              SET config = EXCLUDED.config, updated_at = NOW(),              revision = config_draft_v1.revision + 1              WHERE config_draft_v1.revision = $3              RETURNING revision",
        )
        .bind(config)
        .bind(u64_to_i64(revision, "config revision")?)
        .bind(u64_to_i64(expected_revision, "expected config revision")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| conflict("stale config draft revision"))?;

        Ok(i64_to_u64(stored_revision, "config revision")?)
    }

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> StorageResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let row =
            sqlx::query("SELECT config FROM config_draft_v1 WHERE id = 'singleton' FOR UPDATE")
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?;

        let mut state = match row {
            Some(row) => serde_json::from_value::<ConfigDraftState>(
                row.try_get::<Value, _>("config").map_err(map_sqlx_error)?,
            )?,
            None => ConfigDraftState::default(),
        };

        if state.revision != revision {
            return Err(conflict("stale config draft revision"));
        }

        match error {
            Some(error) => {
                state.last_validation_error = Some(error);
            }
            None => {
                state.last_validated_revision = Some(revision);
                state.last_validation_error = None;
            }
        }

        let config = serde_json::to_value(&state)?;
        sqlx::query(
            "INSERT INTO config_draft_v1 (id, config, revision, updated_at)              VALUES ('singleton', $1, $2, NOW())              ON CONFLICT (id) DO UPDATE              SET config = EXCLUDED.config, revision = EXCLUDED.revision, updated_at = NOW()",
        )
        .bind(config)
        .bind(u64_to_i64(revision, "config revision")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn append_config_history(
        &self,
        revision: u64,
        applied_at_unix_secs: u64,
    ) -> StorageResult<()> {
        let entry = HistoryEntry {
            revision,
            applied_at_unix_secs,
        };
        let config = serde_json::to_value(&entry)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query(
            "INSERT INTO config_history_v1 (revision, config, created_at)              VALUES ($1, $2, NOW())",
        )
        .bind(u64_to_i64(revision, "history revision")?)
        .bind(config)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "DELETE FROM config_history_v1              WHERE revision NOT IN (                  SELECT revision FROM config_history_v1 ORDER BY revision DESC LIMIT 50              )",
        )
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query_scalar::<_, Value>(
            "SELECT config FROM config_history_v1 ORDER BY revision DESC LIMIT $1",
        )
        .bind(u64_to_i64(limit as u64, "history limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|config| serde_json::from_value(config).map_err(Into::into))
            .collect()
    }
}
