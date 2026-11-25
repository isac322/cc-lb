use async_trait::async_trait;
use cc_lb_storage_api::{StorageError, StorageResult, ThreadStickySubagentStore};

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl ThreadStickySubagentStore for SqliteStorage {
    async fn seed_thread_sticky_subagent(
        &self,
        principal_id: &str,
        observed_session_id: &str,
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<()> {
        let observed_at = u64_to_i64(now_unix_secs, "thread sticky subagent observed_at")?;
        let expires_at = u64_to_i64(
            now_unix_secs.saturating_add(ttl_secs),
            "thread sticky subagent expires_at",
        )?;
        sqlx::query(
            "INSERT INTO thread_sticky_subagent_v1 \
             (principal_id, observed_session_id, observed_at_unix_secs, expires_at_unix_secs) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (principal_id, observed_session_id) DO UPDATE SET \
               observed_at_unix_secs = MAX(thread_sticky_subagent_v1.observed_at_unix_secs, excluded.observed_at_unix_secs), \
               expires_at_unix_secs = MAX(thread_sticky_subagent_v1.expires_at_unix_secs, excluded.expires_at_unix_secs)",
        )
        .bind(principal_id)
        .bind(observed_session_id)
        .bind(observed_at)
        .bind(expires_at)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn is_thread_sticky_subagent(
        &self,
        principal_id: &str,
        observed_session_id: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<u64>> {
        let now = u64_to_i64(now_unix_secs, "thread sticky subagent lookup time")?;
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT expires_at_unix_secs FROM thread_sticky_subagent_v1 \
             WHERE principal_id = ?1 AND observed_session_id = ?2 AND expires_at_unix_secs > ?3 \
             LIMIT 1",
        )
        .bind(principal_id)
        .bind(observed_session_id)
        .bind(now)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        found.map(i64_to_u64).transpose()
    }

    async fn purge_expired_thread_sticky_subagents(
        &self,
        now_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }
        let now = u64_to_i64(now_unix_secs, "thread sticky subagent purge time")?;
        let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
        let mut tx = self.begin_immediate().await?;
        let deleted = sqlx::query(
            "DELETE FROM thread_sticky_subagent_v1 \
             WHERE rowid IN ( \
               SELECT rowid FROM thread_sticky_subagent_v1 INDEXED BY idx_thread_sticky_subagent_v1_expires \
               WHERE expires_at_unix_secs <= ?1 \
               ORDER BY expires_at_unix_secs LIMIT ?2 \
             ) \
             AND expires_at_unix_secs <= ?1",
        )
        .bind(now)
        .bind(limit)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .rows_affected();
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(deleted)
    }
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn i64_to_u64(value: i64) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!(
            "negative expires_at_unix_secs value {value} in thread_sticky_subagent_v1"
        ),
    })
}
