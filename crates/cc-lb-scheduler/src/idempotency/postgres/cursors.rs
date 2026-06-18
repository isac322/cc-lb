use sqlx::{Postgres, Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    error::Result,
    idempotency::{
        OAuthUsagePollCursor, OAuthUsagePollCursorsStore, append_bounded, history_to_json,
        i32_to_u32, option_i64_to_u64, option_u64_to_i64, parse_history, u32_to_i32,
    },
};

impl OAuthUsagePollCursorsStore<Postgres> {
    pub async fn read(&self, upstream_id: Uuid) -> Result<Option<OAuthUsagePollCursor>> {
        let row = sqlx::query("SELECT * FROM oauth_usage_poll_cursors WHERE upstream_id = $1")
            .bind(upstream_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(row_to_cursor).transpose()
    }

    pub async fn upsert(&self, cursor: &OAuthUsagePollCursor) -> Result<()> {
        sqlx::query(
            "INSERT INTO oauth_usage_poll_cursors \
             (upstream_id, last_window_start_unix_millis, last_window_end_unix_millis, \
              last_throttle_at_unix_secs, last_throttle_count, last_status, attempt_count, \
              last_observed_at_unix_secs, recent_successes_unix_secs_json, recent_throttles_unix_secs_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT(upstream_id) DO UPDATE SET \
             last_window_start_unix_millis = EXCLUDED.last_window_start_unix_millis, \
             last_window_end_unix_millis = EXCLUDED.last_window_end_unix_millis, \
             last_throttle_at_unix_secs = EXCLUDED.last_throttle_at_unix_secs, \
             last_throttle_count = EXCLUDED.last_throttle_count, last_status = EXCLUDED.last_status, \
             attempt_count = EXCLUDED.attempt_count, last_observed_at_unix_secs = EXCLUDED.last_observed_at_unix_secs, \
             recent_successes_unix_secs_json = EXCLUDED.recent_successes_unix_secs_json, \
             recent_throttles_unix_secs_json = EXCLUDED.recent_throttles_unix_secs_json",
        )
        .bind(cursor.upstream_id)
        .bind(option_u64_to_i64(cursor.last_window_start_unix_millis, "last_window_start_unix_millis")?)
        .bind(option_u64_to_i64(cursor.last_window_end_unix_millis, "last_window_end_unix_millis")?)
        .bind(option_u64_to_i64(cursor.last_throttle_at_unix_secs, "last_throttle_at_unix_secs")?)
        .bind(u32_to_i32(cursor.last_throttle_count, "last_throttle_count")?)
        .bind(cursor.last_status)
        .bind(u32_to_i32(cursor.attempt_count, "attempt_count")?)
        .bind(option_u64_to_i64(cursor.last_observed_at_unix_secs, "last_observed_at_unix_secs")?)
        .bind(history_to_json(&cursor.recent_successes_unix_secs)?)
        .bind(history_to_json(&cursor.recent_throttles_unix_secs)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_throttle(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        throttle_count: u32,
        ring_cap: usize,
    ) -> Result<()> {
        let mut cursor = self
            .read(upstream_id)
            .await?
            .unwrap_or_else(|| OAuthUsagePollCursor::new(upstream_id));
        cursor.last_throttle_at_unix_secs = Some(observed_at_unix_secs);
        cursor.last_throttle_count = throttle_count;
        cursor.attempt_count = cursor.attempt_count.saturating_add(1);
        cursor.last_observed_at_unix_secs = Some(observed_at_unix_secs);
        cursor.recent_throttles_unix_secs = append_bounded(
            &cursor.recent_throttles_unix_secs,
            observed_at_unix_secs,
            ring_cap,
        );
        self.upsert(&cursor).await
    }

    pub async fn record_success(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        window_start_unix_millis: u64,
        window_end_unix_millis: u64,
        ring_cap: usize,
    ) -> Result<()> {
        let mut cursor = self
            .read(upstream_id)
            .await?
            .unwrap_or_else(|| OAuthUsagePollCursor::new(upstream_id));
        cursor.last_window_start_unix_millis = Some(window_start_unix_millis);
        cursor.last_window_end_unix_millis = Some(window_end_unix_millis);
        cursor.last_status = Some(200);
        cursor.attempt_count = 0;
        cursor.last_observed_at_unix_secs = Some(observed_at_unix_secs);
        cursor.recent_successes_unix_secs = append_bounded(
            &cursor.recent_successes_unix_secs,
            observed_at_unix_secs,
            ring_cap,
        );
        self.upsert(&cursor).await
    }
}

fn row_to_cursor(row: PgRow) -> Result<OAuthUsagePollCursor> {
    Ok(OAuthUsagePollCursor {
        upstream_id: row.try_get("upstream_id")?,
        last_window_start_unix_millis: option_i64_to_u64(
            row.try_get("last_window_start_unix_millis")?,
            "last_window_start_unix_millis",
        )?,
        last_window_end_unix_millis: option_i64_to_u64(
            row.try_get("last_window_end_unix_millis")?,
            "last_window_end_unix_millis",
        )?,
        last_throttle_at_unix_secs: option_i64_to_u64(
            row.try_get("last_throttle_at_unix_secs")?,
            "last_throttle_at_unix_secs",
        )?,
        last_throttle_count: i32_to_u32(
            row.try_get("last_throttle_count")?,
            "last_throttle_count",
        )?,
        last_status: row.try_get("last_status")?,
        attempt_count: i32_to_u32(row.try_get("attempt_count")?, "attempt_count")?,
        last_observed_at_unix_secs: option_i64_to_u64(
            row.try_get("last_observed_at_unix_secs")?,
            "last_observed_at_unix_secs",
        )?,
        recent_successes_unix_secs: parse_history(
            &row.try_get::<String, _>("recent_successes_unix_secs_json")?,
            "recent_successes_unix_secs_json",
        )?,
        recent_throttles_unix_secs: parse_history(
            &row.try_get::<String, _>("recent_throttles_unix_secs_json")?,
            "recent_throttles_unix_secs_json",
        )?,
    })
}
