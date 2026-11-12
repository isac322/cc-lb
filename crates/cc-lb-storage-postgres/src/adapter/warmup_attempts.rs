use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageResult, UpstreamWarmupAttemptStore, WarmupAttemptListFilters, WarmupAttemptRecord,
};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage,
        warmup_attempt_mapping::{
            dispatch_kind_to_str, outcome_components, row_to_attempt, status_to_str, trigger_to_str,
        },
    },
    error_map::map_sqlx_error,
};

const DEFAULT_WARMUP_ATTEMPT_LIMIT: i64 = 50;

#[async_trait]
impl UpstreamWarmupAttemptStore for PostgresStorage {
    async fn insert_warmup_attempt(&self, attempt: &WarmupAttemptRecord) -> StorageResult<()> {
        let (outcome, reason) = outcome_components(attempt.outcome);
        sqlx::query(
            "INSERT INTO warmup_attempts_v1 \
              (id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs, scheduled_for_unix_secs, \
               trigger, lane, outcome, reason, dispatch_kind, http_status, cycle_key, \
               expected_cycle_key, idle_secs_since_prev_window, replica_id, lease_holder, \
               upstream_spec_revision, dialect_plugin_snapshot, error_detail) \
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)",
        )
        .bind(attempt.id)
        .bind(attempt.upstream_id)
        .bind(attempt.attempted_at_unix_secs)
        .bind(attempt.completed_at_unix_secs)
        .bind(attempt.scheduled_for_unix_secs)
        .bind(trigger_to_str(attempt.trigger))
        .bind(attempt.lane.as_str())
        .bind(outcome)
        .bind(reason)
        .bind(attempt.dispatch_kind.map(dispatch_kind_to_str))
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
        let mut query =
            QueryBuilder::<Postgres>::new("SELECT * FROM warmup_attempts_v1 WHERE upstream_id = ");
        query.push_bind(upstream_id);
        if let Some(status) = filters.status {
            query
                .push(" AND outcome = ")
                .push_bind(status_to_str(status))
                .push("::text");
        }
        if let Some(lane) = filters.lane {
            query
                .push(" AND lane = ")
                .push_bind(lane.as_str())
                .push("::text");
        }
        if let Some(before) = filters.before {
            query
                .push(" AND (attempted_at_unix_secs < ")
                .push_bind(before.attempted_at_unix_secs)
                .push(" OR (attempted_at_unix_secs = ")
                .push_bind(before.attempted_at_unix_secs)
                .push(" AND id < ")
                .push_bind(before.id)
                .push("))");
        }
        query
            .push(" ORDER BY attempted_at_unix_secs DESC, id DESC LIMIT ")
            .push_bind(limit);
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        rows.into_iter().map(row_to_attempt).collect()
    }
}
