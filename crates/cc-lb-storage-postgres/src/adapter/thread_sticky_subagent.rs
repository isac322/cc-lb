use async_trait::async_trait;
use cc_lb_storage_api::{
    ChangeChannel, MAX_CHANGE_PAYLOAD_LEN, StorageResult, ThreadStickySubagentStore,
};

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

fn encode_sticky_notify_payload(
    principal_id: &str,
    observed_session_id: &str,
    expires_at_unix_secs: u64,
) -> String {
    if !principal_id.contains('\t') && !observed_session_id.contains('\t') {
        let candidate = format!("{principal_id}\t{observed_session_id}\t{expires_at_unix_secs}");
        if candidate.chars().count() <= MAX_CHANGE_PAYLOAD_LEN {
            return candidate;
        }
    }
    String::new()
}

#[async_trait]
impl ThreadStickySubagentStore for PostgresStorage {
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let stored_expires_at: i64 = sqlx::query_scalar(
            "INSERT INTO thread_sticky_subagent_v1 \
             (principal_id, observed_session_id, observed_at_unix_secs, expires_at_unix_secs) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (principal_id, observed_session_id) DO UPDATE SET \
               observed_at_unix_secs = GREATEST(thread_sticky_subagent_v1.observed_at_unix_secs, EXCLUDED.observed_at_unix_secs), \
               expires_at_unix_secs = GREATEST(thread_sticky_subagent_v1.expires_at_unix_secs, EXCLUDED.expires_at_unix_secs) \
             RETURNING expires_at_unix_secs",
        )
        .bind(principal_id)
        .bind(observed_session_id)
        .bind(observed_at)
        .bind(expires_at)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        let effective_expires_at =
            i64_to_u64(stored_expires_at, "thread sticky subagent expires_at")?;
        let payload =
            encode_sticky_notify_payload(principal_id, observed_session_id, effective_expires_at);
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(ChangeChannel::ThreadStickySubagent.postgres_channel())
            .bind(payload)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
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
             WHERE principal_id = $1 AND observed_session_id = $2 AND expires_at_unix_secs > $3 \
             LIMIT 1",
        )
        .bind(principal_id)
        .bind(observed_session_id)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        found
            .map(|value| i64_to_u64(value, "thread sticky subagent expires_at"))
            .transpose()
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
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let deleted = sqlx::query(
            "WITH candidates AS ( \
               SELECT ctid \
               FROM thread_sticky_subagent_v1 \
               WHERE expires_at_unix_secs <= $1 \
               ORDER BY expires_at_unix_secs \
               LIMIT $2 FOR UPDATE SKIP LOCKED \
             ) \
             DELETE FROM thread_sticky_subagent_v1 AS target \
             WHERE target.ctid = ANY(ARRAY(SELECT ctid FROM candidates)) \
               AND target.expires_at_unix_secs <= $1",
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
