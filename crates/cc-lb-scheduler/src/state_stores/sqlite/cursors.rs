use sqlx::{Row, Sqlite, sqlite::SqliteRow};
use uuid::Uuid;

use super::sqlite_uuid;
use crate::{
    error::{Result, SchedulerError},
    state_stores::{
        OAuthUsagePollCursor, OAuthUsagePollCursorsStore, i64_to_u32, option_i64_to_i32,
        option_i64_to_u64, option_u64_to_i64, u32_to_i32,
    },
};

impl OAuthUsagePollCursorsStore<Sqlite> {
    pub async fn read(&self, upstream_id: Uuid) -> Result<Option<OAuthUsagePollCursor>> {
        let row = sqlx::query("SELECT * FROM oauth_usage_poll_cursors WHERE upstream_id = ?1")
            .bind(sqlite_uuid(upstream_id))
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_cursor).transpose()
    }

    pub async fn upsert(&self, cursor: &OAuthUsagePollCursor) -> Result<()> {
        sqlx::query(
            "INSERT INTO oauth_usage_poll_cursors \
             (upstream_id, last_observed_at_unix_secs, last_status, attempt_count) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(upstream_id) DO UPDATE SET \
             last_observed_at_unix_secs = excluded.last_observed_at_unix_secs, \
             last_status = excluded.last_status, \
             attempt_count = excluded.attempt_count",
        )
        .bind(sqlite_uuid(cursor.upstream_id))
        .bind(option_u64_to_i64(
            cursor.last_observed_at_unix_secs,
            "last_observed_at_unix_secs",
        )?)
        .bind(cursor.last_status)
        .bind(u32_to_i32(cursor.attempt_count, "attempt_count")?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomic, monotonic per-attempt cursor write.
    ///
    /// Always increments `attempt_count`. `last_observed_at_unix_secs` and `last_status`
    /// only advance when the incoming `observed_at_unix_secs` is greater than or equal
    /// to the previously recorded one, so a late-arriving older tick can never
    /// overwrite a newer cursor state.
    pub async fn record_attempt(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        status: i32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO oauth_usage_poll_cursors \
             (upstream_id, last_observed_at_unix_secs, last_status, attempt_count) \
             VALUES (?1, ?2, ?3, 1) \
             ON CONFLICT(upstream_id) DO UPDATE SET \
             attempt_count = oauth_usage_poll_cursors.attempt_count + 1, \
             last_observed_at_unix_secs = CASE \
                 WHEN excluded.last_observed_at_unix_secs >= \
                      COALESCE(oauth_usage_poll_cursors.last_observed_at_unix_secs, 0) \
                 THEN excluded.last_observed_at_unix_secs \
                 ELSE oauth_usage_poll_cursors.last_observed_at_unix_secs \
             END, \
             last_status = CASE \
                 WHEN excluded.last_observed_at_unix_secs >= \
                      COALESCE(oauth_usage_poll_cursors.last_observed_at_unix_secs, 0) \
                 THEN excluded.last_status \
                 ELSE oauth_usage_poll_cursors.last_status \
             END",
        )
        .bind(sqlite_uuid(upstream_id))
        .bind(option_u64_to_i64(
            Some(observed_at_unix_secs),
            "last_observed_at_unix_secs",
        )?)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn row_to_cursor(row: SqliteRow) -> Result<OAuthUsagePollCursor> {
    Ok(OAuthUsagePollCursor {
        upstream_id: Uuid::from_slice(&row.try_get::<Vec<u8>, _>("upstream_id")?)
            .map_err(|error| SchedulerError::Job(error.to_string()))?,
        last_observed_at_unix_secs: option_i64_to_u64(
            row.try_get("last_observed_at_unix_secs")?,
            "last_observed_at_unix_secs",
        )?,
        last_status: option_i64_to_i32(row.try_get("last_status")?, "last_status")?,
        attempt_count: i64_to_u32(row.try_get("attempt_count")?, "attempt_count")?,
    })
}
