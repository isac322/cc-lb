use async_trait::async_trait;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, StorageResult};
use chrono::{DateTime, Utc};

use crate::{
    adapter::{
        PostgresStorage, u64_to_i64, unix_secs_to_datetime, unix_secs_to_datetime_lower,
        unix_secs_to_datetime_upper,
    },
    error_map::map_sqlx_error,
};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl RequestEventStore for PostgresStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()> {
        let payload = serde_json::to_vec(event)?;
        sqlx::query(
            "INSERT INTO request_events_v1 (ts, principal_id, payload, created_at)              VALUES ($1,$2,$3,NOW())",
        )
        .bind(unix_secs_to_datetime(event.ts, "request event ts")?)
        .bind(event.principal_id.as_deref())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "request event since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "request event until")?;

        let rows = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              ORDER BY seq ASC LIMIT $3",
        )
        .bind(since)
        .bind(until)
        .bind(u64_to_i64(limit as u64, "request event limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|payload| serde_json::from_slice(&payload).map_err(Into::into))
            .collect()
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_ms = cutoff_ms_x_1m / KEY_SEQUENCE_SCALE;
        let result = sqlx::query(
            "DELETE FROM request_events_v1              WHERE seq IN (                 SELECT seq FROM request_events_v1                 WHERE ts < $1                 ORDER BY seq ASC                 LIMIT $2             )",
        )
        .bind(unix_millis_to_datetime(
            cutoff_ms,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }
}

impl PostgresStorage {
    pub async fn prune_request_events(&self, older_than: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM request_events_v1 WHERE ts < $1")
            .bind(unix_secs_to_datetime(
                older_than,
                "request event prune cutoff",
            )?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn unix_millis_to_datetime(value: u64, field: &str) -> StorageResult<DateTime<Utc>> {
    let millis = i64::try_from(value).map_err(|_| cc_lb_storage_api::StorageError::Fatal {
        message: format!("{field} cannot be represented as postgres timestamptz"),
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis).ok_or_else(|| {
        cc_lb_storage_api::StorageError::Fatal {
            message: format!("{field} cannot be represented as postgres timestamptz"),
        }
    })
}
