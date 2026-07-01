use async_trait::async_trait;
use cc_lb_storage_api::{
    ConfigDraftState, ConfigStore, EffectiveConfig, HistoryEntry, HistorySummary, StorageError,
    StorageResult,
};
use sqlx::Row;

use crate::{SqliteStorage, map_sqlx_error};

const CONFIG_DRAFT_ID: &str = "singleton";
const CONFIG_HISTORY_LIMIT: usize = 50;

#[async_trait]
impl ConfigStore for SqliteStorage {
    async fn get_config_draft(&self) -> StorageResult<ConfigDraftState> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM config_drafts_v1 WHERE id = ?")
                .bind(CONFIG_DRAFT_ID)
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        payload
            .map(|payload| serde_json::from_str(&payload))
            .transpose()
            .map(|state| state.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn put_config_draft(
        &self,
        mut new: ConfigDraftState,
        expected_revision: u64,
    ) -> StorageResult<u64> {
        let mut tx = self.begin_immediate().await?;
        let current = read_config_draft_in_tx(&mut tx).await?;
        if current.revision != expected_revision {
            return Err(conflict("stale config draft revision"));
        }

        let revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| StorageError::Fatal {
                message: "config draft revision overflow".to_owned(),
            })?;
        new.revision = revision;
        new.last_validated_revision = None;
        new.last_validation_error = None;

        let payload = serde_json::to_string(&new)?;
        sqlx::query(
            "INSERT INTO config_drafts_v1 (id, payload, created_at, updated_at) VALUES (?, ?, unixepoch(), unixepoch()) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
        )
        .bind(CONFIG_DRAFT_ID)
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(revision)
    }

    async fn set_last_validated_revision(
        &self,
        revision: u64,
        error: Option<String>,
    ) -> StorageResult<()> {
        let mut tx = self.begin_immediate().await?;
        let mut state = read_config_draft_in_tx(&mut tx).await?;
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

        let payload = serde_json::to_string(&state)?;
        sqlx::query(
            "INSERT INTO config_drafts_v1 (id, payload, created_at, updated_at) VALUES (?, ?, unixepoch(), unixepoch()) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
        )
        .bind(CONFIG_DRAFT_ID)
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn get_effective_config(&self) -> StorageResult<Option<EffectiveConfig>> {
        let row = sqlx::query(
            "SELECT revision, config, applied_at FROM effective_config_v1 WHERE id = ?",
        )
        .bind(CONFIG_DRAFT_ID)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| {
            let revision = row.try_get::<i64, _>("revision").map_err(map_sqlx_error)?;
            let config = row.try_get::<String, _>("config").map_err(map_sqlx_error)?;
            let applied_at = row
                .try_get::<i64, _>("applied_at")
                .map_err(map_sqlx_error)?;
            Ok(EffectiveConfig {
                revision: i64_to_u64(revision, "effective config revision")?,
                config: serde_json::from_str(&config)?,
                applied_at_unix_secs: i64_to_u64(applied_at, "effective config applied_at")?,
            })
        })
        .transpose()
    }

    async fn put_effective_config(
        &self,
        revision: u64,
        config_json: serde_json::Value,
        applied_at_unix_secs: u64,
    ) -> StorageResult<()> {
        let config = serde_json::to_string(&config_json)?;
        sqlx::query(
            "INSERT OR REPLACE INTO effective_config_v1 (id, revision, config, applied_at) VALUES (?, ?, ?, ?)",
        )
        .bind(CONFIG_DRAFT_ID)
        .bind(u64_to_i64(revision, "effective config revision")?)
        .bind(config)
        .bind(u64_to_i64(
            applied_at_unix_secs,
            "effective config applied_at",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn append_config_history(
        &self,
        revision: u64,
        config_toml: String,
        applied_at_unix_secs: u64,
        summary: HistorySummary,
    ) -> StorageResult<()> {
        let entry = HistoryEntry {
            revision,
            config_toml,
            applied_at_unix_secs,
            summary,
        };
        let payload = serde_json::to_string(&entry)?;
        let mut tx = self.begin_immediate().await?;

        sqlx::query(
            "INSERT INTO config_history_v1 (id, payload, created_at, updated_at) VALUES (?, ?, unixepoch(), unixepoch()) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
        )
        .bind(revision.to_string())
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(
            "DELETE FROM config_history_v1 WHERE id NOT IN (SELECT id FROM config_history_v1 ORDER BY CAST(id AS INTEGER) DESC LIMIT ?)",
        )
        .bind(u64_to_i64(CONFIG_HISTORY_LIMIT as u64, "config history limit")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn list_config_history(&self, limit: usize) -> StorageResult<Vec<HistoryEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            "SELECT payload FROM config_history_v1 ORDER BY CAST(id AS INTEGER) DESC LIMIT ?",
        )
        .bind(u64_to_i64(limit as u64, "config history limit")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                let payload = row
                    .try_get::<String, _>("payload")
                    .map_err(map_sqlx_error)?;
                serde_json::from_str(&payload).map_err(Into::into)
            })
            .collect()
    }

    async fn get_config_history(&self, revision: u64) -> StorageResult<Option<HistoryEntry>> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload FROM config_history_v1 WHERE id = ?")
                .bind(revision.to_string())
                .fetch_optional(self.pool())
                .await
                .map_err(map_sqlx_error)?;

        payload
            .map(|payload| serde_json::from_str(&payload))
            .transpose()
            .map_err(Into::into)
    }
}

async fn read_config_draft_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> StorageResult<ConfigDraftState> {
    let row = sqlx::query("SELECT payload FROM config_drafts_v1 WHERE id = ?")
        .bind(CONFIG_DRAFT_ID)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;

    row.map(|row| {
        let payload = row
            .try_get::<String, _>("payload")
            .map_err(map_sqlx_error)?;
        serde_json::from_str(&payload).map_err(Into::into)
    })
    .transpose()
    .map(|state| state.unwrap_or_default())
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} does not fit in SQLite INTEGER"),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

fn conflict(message: &str) -> StorageError {
    StorageError::Conflict {
        message: message.to_owned(),
    }
}
