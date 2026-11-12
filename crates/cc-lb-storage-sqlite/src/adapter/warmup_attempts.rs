use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError, StorageResult, UpstreamWarmupAttemptStore, WarmupAttemptListFilters,
    WarmupAttemptRecord,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{AssertSqlSafe, Row, sqlite::SqliteRow};
use uuid::Uuid;

use crate::{
    SqliteStorage,
    adapter::warmup_attempt_mapping::{
        dispatch_kind_from_str, dispatch_kind_to_str, lane_from_str, outcome_components,
        parse_reason, status_from_str, status_to_str,
    },
    map_sqlx_error,
};

const DEFAULT_LIST_LIMIT: u32 = 50;
const WARMUP_ATTEMPT_COLUMNS: &str = "id, upstream_id, attempted_at_unix_secs, \
    completed_at_unix_secs, scheduled_for_unix_secs, trigger, lane, outcome, reason, \
    dispatch_kind, http_status, cycle_key, expected_cycle_key, idle_secs_since_prev_window, \
    replica_id, lease_holder, upstream_spec_revision, dialect_plugin_snapshot, error_detail";

#[async_trait]
impl UpstreamWarmupAttemptStore for SqliteStorage {
    async fn insert_warmup_attempt(&self, attempt: &WarmupAttemptRecord) -> StorageResult<()> {
        let (outcome, reason) = outcome_components(attempt.outcome);
        sqlx::query(
            "INSERT INTO warmup_attempts_v1 \
             (id, upstream_id, attempted_at_unix_secs, completed_at_unix_secs, \
              scheduled_for_unix_secs, trigger, lane, outcome, reason, dispatch_kind, \
              http_status, cycle_key, expected_cycle_key, idle_secs_since_prev_window, replica_id, \
              lease_holder, upstream_spec_revision, dialect_plugin_snapshot, error_detail) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(attempt.id.to_string())
        .bind(attempt.upstream_id.to_string())
        .bind(attempt.attempted_at_unix_secs)
        .bind(attempt.completed_at_unix_secs)
        .bind(attempt.scheduled_for_unix_secs)
        .bind(enum_to_db(attempt.trigger, "trigger")?)
        .bind(attempt.lane.as_str())
        .bind(outcome)
        .bind(reason)
        .bind(attempt.dispatch_kind.map(dispatch_kind_to_str))
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
        if filters.status.is_some() {
            sql.push_str(" AND outcome = ?");
        }
        if filters.lane.is_some() {
            sql.push_str(" AND lane = ?");
        }
        if filters.since_unix_secs.is_some() {
            sql.push_str(" AND attempted_at_unix_secs >= ?");
        }
        if filters.before.is_some() {
            sql.push_str(
                " AND (attempted_at_unix_secs < ? \
                 OR (attempted_at_unix_secs = ? AND id < ?))",
            );
        }
        sql.push_str(" ORDER BY attempted_at_unix_secs DESC, id DESC LIMIT ?");

        let mut query = sqlx::query(AssertSqlSafe(sql)).bind(upstream_id.to_string());
        if let Some(status) = filters.status {
            query = query.bind(status_to_str(status));
        }
        if let Some(lane) = filters.lane {
            query = query.bind(lane.as_str());
        }
        if let Some(since_unix_secs) = filters.since_unix_secs {
            query = query.bind(since_unix_secs);
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
}

fn row_to_record(row: SqliteRow) -> StorageResult<WarmupAttemptRecord> {
    let id = row.try_get::<String, _>("id").map_err(map_sqlx_error)?;
    let upstream_id = row
        .try_get::<String, _>("upstream_id")
        .map_err(map_sqlx_error)?;
    let replica_id = row
        .try_get::<Option<String>, _>("replica_id")
        .map_err(map_sqlx_error)?;
    let outcome = row
        .try_get::<String, _>("outcome")
        .map_err(map_sqlx_error)?;
    let reason = row.try_get::<String, _>("reason").map_err(map_sqlx_error)?;
    let dispatch_kind = row
        .try_get::<Option<String>, _>("dispatch_kind")
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
        lane: lane_from_str(&row.try_get::<String, _>("lane").map_err(map_sqlx_error)?)?,
        outcome: parse_reason(status_from_str(&outcome)?, &reason)?,
        dispatch_kind: dispatch_kind
            .as_deref()
            .map(dispatch_kind_from_str)
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
