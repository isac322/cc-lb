use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamWarmupAttemptStore, WarmupAttemptListFilters,
    WarmupAttemptOutcome, WarmupAttemptRecord, WarmupAttemptSummary,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{SqliteStorage, map_sqlx_error};

const DEFAULT_LIST_LIMIT: u32 = 50;
const WARMUP_ATTEMPT_COLUMNS: &str = "id, upstream_id, attempted_at_unix_secs, \
    completed_at_unix_secs, scheduled_for_unix_secs, trigger, outcome, reason, http_status, \
    cycle_key, expected_cycle_key, idle_secs_since_prev_window, replica_id, lease_holder, \
    upstream_spec_revision, dialect_plugin_snapshot, error_detail";

#[async_trait]
impl UpstreamWarmupAttemptStore for SqliteStorage {
    async fn insert_warmup_attempt(&self, attempt: &WarmupAttemptRecord) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO warmup_attempts_v1 \
             (id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs, \
              scheduled_for_unix_secs, trigger, outcome, reason, http_status, cycle_key, \
              expected_cycle_key, idle_secs_since_prev_window, replica_id, lease_holder, \
              upstream_spec_revision, dialect_plugin_snapshot, error_detail) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(attempt.id.to_string())
        .bind(attempt.upstream_id.to_string())
        .bind(attempt.attempted_at_unix_secs)
        .bind(attempt.completed_at_unix_secs)
        .bind(attempt.scheduled_for_unix_secs)
        .bind(enum_to_db(attempt.trigger, "trigger")?)
        .bind(enum_to_db(attempt.outcome, "outcome")?)
        .bind(
            attempt
                .reason
                .map(|reason| enum_to_db(reason, "reason"))
                .transpose()?,
        )
        .bind(attempt.http_status)
        .bind(attempt.cycle_key)
        .bind(attempt.expected_cycle_key)
        .bind(attempt.idle_secs_since_prev_window)
        .bind(attempt.replica_id.map(|replica_id| replica_id.to_string()))
        .bind(attempt.lease_holder.as_deref())
        .bind(attempt.upstream_spec_revision)
        .bind(json_to_db(attempt.dialect_plugin_snapshot.as_ref())?)
        .bind(attempt.error_detail.as_deref())
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_warmup_attempts_for_upstream(
        &self,
        upstream_id: Uuid,
        filters: WarmupAttemptListFilters,
    ) -> StorageResult<Vec<WarmupAttemptRecord>> {
        let mut sql = format!(
            "SELECT {WARMUP_ATTEMPT_COLUMNS} FROM warmup_attempts_v1 WHERE upstream_id = ?"
        );
        if filters.outcome.is_some() {
            sql.push_str(" AND outcome = ?");
        }
        if filters.before.is_some() {
            sql.push_str(
                " AND (attempted_at_unix_secs < ? \
                 OR (attempted_at_unix_secs = ? AND id < ?))",
            );
        }
        sql.push_str(" ORDER BY attempted_at_unix_secs DESC, id DESC LIMIT ?");

        let mut query = sqlx::query(AssertSqlSafe(sql)).bind(upstream_id.to_string());
        if let Some(outcome) = filters.outcome {
            query = query.bind(enum_to_db(outcome, "outcome")?);
        }
        if let Some(cursor) = filters.before {
            query = query
                .bind(cursor.attempted_at_unix_secs)
                .bind(cursor.attempted_at_unix_secs)
                .bind(cursor.id.to_string());
        }
        let rows = query
            .bind(i64::from(filters.limit.unwrap_or(DEFAULT_LIST_LIMIT)))
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn summarize_recent_warmup_attempts(
        &self,
        upstream_id: Uuid,
        cutoff_unix_secs: i64,
    ) -> StorageResult<WarmupAttemptSummary> {
        let rows = sqlx::query(
            "SELECT outcome, COUNT(*) AS attempt_count \
             FROM warmup_attempts_v1 \
             WHERE upstream_id = ? AND attempted_at_unix_secs >= ? \
             GROUP BY outcome",
        )
        .bind(upstream_id.to_string())
        .bind(cutoff_unix_secs)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        let mut summary = WarmupAttemptSummary::default();
        for row in rows {
            let outcome = enum_from_db::<WarmupAttemptOutcome>(
                "outcome",
                &row.try_get::<String, _>("outcome")
                    .map_err(map_sqlx_error)?,
            )?;
            let count = i64_to_u64(
                row.try_get::<i64, _>("attempt_count")
                    .map_err(map_sqlx_error)?,
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
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {WARMUP_ATTEMPT_COLUMNS} FROM warmup_attempts_v1 \
             WHERE upstream_id = ? ORDER BY attempted_at_unix_secs DESC, id DESC LIMIT 1"
        )))
        .bind(upstream_id.to_string())
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }
}

fn row_to_record(row: SqliteRow) -> StorageResult<WarmupAttemptRecord> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let replica_id = row
        .try_get::<Option<String>, _>("replica_id")
        .map_err(map_sqlx_error)?;
    let reason = row
        .try_get::<Option<String>, _>("reason")
        .map_err(map_sqlx_error)?;

    Ok(WarmupAttemptRecord {
        id: uuid_from_db(&id, "warmup attempt id")?,
        upstream_id: uuid_from_db(&upstream_id, "warmup attempt upstream_id")?,
        attempted_at_unix_secs: row
            .try_get("attempted_at_unix_secs")
            .map_err(map_sqlx_error)?,
        completed_at_unix_secs: row
            .try_get("completed_at_unix_secs")
            .map_err(map_sqlx_error)?,
        scheduled_for_unix_secs: row
            .try_get("scheduled_for_unix_secs")
            .map_err(map_sqlx_error)?,
        trigger: enum_from_db(
            "trigger",
            &row.try_get::<String, _>("trigger")
                .map_err(map_sqlx_error)?,
        )?,
        outcome: enum_from_db(
            "outcome",
            &row.try_get::<String, _>("outcome")
                .map_err(map_sqlx_error)?,
        )?,
        reason: reason
            .as_deref()
            .map(|value| enum_from_db("reason", value))
            .transpose()?,
        http_status: row.try_get("http_status").map_err(map_sqlx_error)?,
        cycle_key: row.try_get("cycle_key").map_err(map_sqlx_error)?,
        expected_cycle_key: row.try_get("expected_cycle_key").map_err(map_sqlx_error)?,
        idle_secs_since_prev_window: row
            .try_get("idle_secs_since_prev_window")
            .map_err(map_sqlx_error)?,
        replica_id: replica_id
            .as_deref()
            .map(|value| uuid_from_db(value, "warmup attempt replica_id"))
            .transpose()?,
        lease_holder: row.try_get("lease_holder").map_err(map_sqlx_error)?,
        upstream_spec_revision: row
            .try_get("upstream_spec_revision")
            .map_err(map_sqlx_error)?,
        dialect_plugin_snapshot: json_from_db(
            row.try_get("dialect_plugin_snapshot")
                .map_err(map_sqlx_error)?,
        )?,
        error_detail: row.try_get("error_detail").map_err(map_sqlx_error)?,
    })
}

fn enum_to_db<T: Serialize>(value: T, field: &str) -> StorageResult<String> {
    match serde_json::to_value(value)? {
        Value::String(value) => Ok(value),
        _ => Err(StorageError::Fatal {
            message: format!("warmup attempt {field} did not serialize as a string"),
        }),
    }
}

fn enum_from_db<T: DeserializeOwned>(field: &str, value: &str) -> StorageResult<T> {
    serde_json::from_value(Value::String(value.to_owned())).map_err(|error| {
        StorageError::Corrupted {
            message: format!("invalid warmup attempt {field} {value}: {error}"),
        }
    })
}

fn json_to_db(value: Option<&Value>) -> StorageResult<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn json_from_db(value: Option<String>) -> StorageResult<Option<Value>> {
    value
        .map(|text| {
            serde_json::from_str::<Value>(&text).map_err(|error| StorageError::Corrupted {
                message: format!("invalid warmup attempt dialect_plugin_snapshot: {error}"),
            })
        })
        .transpose()
}

fn uuid_from_db(value: &str, field: &str) -> StorageResult<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::Corrupted {
        message: format!("invalid {field} {value}: {error}"),
    })
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative in sqlite storage"),
    })
}
