use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamWarmupAttemptStore, WarmupAttemptListFilters, WarmupAttemptOutcome,
    WarmupAttemptRecord, WarmupAttemptSummary,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage, i64_to_u64,
        warmup_attempt_mapping::{
            outcome_from_str, outcome_to_str, reason_to_str, row_to_attempt, trigger_to_str,
        },
    },
    error_map::map_sqlx_error,
};

const DEFAULT_WARMUP_ATTEMPT_LIMIT: i64 = 50;

#[async_trait]
impl UpstreamWarmupAttemptStore for PostgresStorage {
    async fn insert_warmup_attempt(&self, attempt: &WarmupAttemptRecord) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO warmup_attempts_v1 \
             (id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs, scheduled_for_unix_secs, \
              trigger, outcome, reason, http_status, cycle_key, expected_cycle_key, \
              idle_secs_since_prev_window, replica_id, lease_holder, upstream_spec_revision, \
              dialect_plugin_snapshot, error_detail) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17)",
        )
        .bind(attempt.id)
        .bind(attempt.upstream_id)
        .bind(attempt.attempted_at_unix_secs)
        .bind(attempt.completed_at_unix_secs)
        .bind(attempt.scheduled_for_unix_secs)
        .bind(trigger_to_str(attempt.trigger))
        .bind(outcome_to_str(attempt.outcome))
        .bind(attempt.reason.map(reason_to_str))
        .bind(attempt.http_status)
        .bind(attempt.cycle_key)
        .bind(attempt.expected_cycle_key)
        .bind(attempt.idle_secs_since_prev_window)
        .bind(attempt.replica_id)
        .bind(attempt.lease_holder.as_deref())
        .bind(attempt.upstream_spec_revision)
        .bind(attempt.dialect_plugin_snapshot.as_ref())
        .bind(attempt.error_detail.as_deref())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_warmup_attempts_for_upstream(
        &self,
        upstream_id: Uuid,
        filters: WarmupAttemptListFilters,
    ) -> StorageResult<Vec<WarmupAttemptRecord>> {
        let limit = filters
            .limit
            .map(i64::from)
            .unwrap_or(DEFAULT_WARMUP_ATTEMPT_LIMIT);
        let rows = match (filters.outcome, filters.before) {
            (Some(outcome), Some(before)) => {
                sqlx::query(
                    "SELECT * FROM warmup_attempts_v1 \
                 WHERE upstream_id = $1 \
                 AND outcome = $2::text \
                 AND (attempted_at_unix_secs < $3 \
                      OR (attempted_at_unix_secs = $3 AND id < $4)) \
                 ORDER BY attempted_at_unix_secs DESC, id DESC \
                 LIMIT $5",
                )
                .bind(upstream_id)
                .bind(outcome_to_str(outcome))
                .bind(before.attempted_at_unix_secs)
                .bind(before.id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (Some(outcome), None) => {
                sqlx::query(
                    "SELECT * FROM warmup_attempts_v1 \
                 WHERE upstream_id = $1 \
                 AND outcome = $2::text \
                 ORDER BY attempted_at_unix_secs DESC, id DESC \
                 LIMIT $3",
                )
                .bind(upstream_id)
                .bind(outcome_to_str(outcome))
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, Some(before)) => {
                sqlx::query(
                    "SELECT * FROM warmup_attempts_v1 \
                 WHERE upstream_id = $1 \
                 AND (attempted_at_unix_secs < $2 \
                      OR (attempted_at_unix_secs = $2 AND id < $3)) \
                 ORDER BY attempted_at_unix_secs DESC, id DESC \
                 LIMIT $4",
                )
                .bind(upstream_id)
                .bind(before.attempted_at_unix_secs)
                .bind(before.id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, None) => {
                sqlx::query(
                    "SELECT * FROM warmup_attempts_v1 \
                 WHERE upstream_id = $1 \
                 ORDER BY attempted_at_unix_secs DESC, id DESC \
                 LIMIT $2",
                )
                .bind(upstream_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_attempt).collect()
    }

    async fn summarize_recent_warmup_attempts(
        &self,
        upstream_id: Uuid,
        window_secs: i64,
    ) -> StorageResult<WarmupAttemptSummary> {
        let rows = sqlx::query(
            "SELECT outcome, COUNT(*) AS attempt_count \
             FROM warmup_attempts_v1 \
             WHERE upstream_id = $1 \
             AND attempted_at_unix_secs >= (extract(epoch from now())::bigint - $2) \
             GROUP BY outcome",
        )
        .bind(upstream_id)
        .bind(window_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        let mut summary = WarmupAttemptSummary::default();
        for row in rows {
            let outcome = outcome_from_str(
                &row.try_get::<String, _>("outcome")
                    .map_err(map_sqlx_error)?,
            )?;
            let count = i64_to_u64(
                row.try_get("attempt_count").map_err(map_sqlx_error)?,
                "warmup attempt summary count",
            )?;
            match outcome {
                WarmupAttemptOutcome::SuccessFresh => summary.success_fresh = count,
                WarmupAttemptOutcome::SuccessRedundant => summary.success_redundant = count,
                WarmupAttemptOutcome::TransientFailure => summary.transient_failure = count,
                WarmupAttemptOutcome::PermanentFailure => summary.permanent_failure = count,
                WarmupAttemptOutcome::Skipped => summary.skipped = count,
            }
        }
        Ok(summary)
    }

    async fn latest_warmup_attempt_for_upstream(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<WarmupAttemptRecord>> {
        sqlx::query(
            "SELECT * FROM warmup_attempts_v1 \
             WHERE upstream_id = $1 \
             ORDER BY attempted_at_unix_secs DESC, id DESC \
             LIMIT 1",
        )
        .bind(upstream_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?
        .map(row_to_attempt)
        .transpose()
    }
}
