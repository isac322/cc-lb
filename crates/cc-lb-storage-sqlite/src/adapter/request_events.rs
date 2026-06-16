use async_trait::async_trait;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, StorageError, StorageResult};
use sqlx::AssertSqlSafe;

use crate::{SqliteStorage, map_sqlx_error};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl RequestEventStore for SqliteStorage {
    async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()> {
        let payload = serde_json::to_string(event)?;

        sqlx::query(
            "INSERT INTO request_events_v1 (request_id, ts, event_type, upstream_id, payload) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&event.request_id)
        .bind(u64_to_i64(event_ts_secs(event), "request event ts")?)
        .bind("request")
        .bind(event.upstream_id.map(|id| id.to_string()))
        .bind(payload)
        .execute(self.pool())
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
        query_request_events(self, since, until, limit, "ASC").await
    }

    async fn query_recent_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<RequestEvent>> {
        query_request_events(self, since, until, limit, "DESC").await
    }

    async fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_secs = (cutoff_ms_x_1m / KEY_SEQUENCE_SCALE) / 1_000;
        let result = sqlx::query(
            "DELETE FROM request_events_v1 \
             WHERE id IN ( \
                 SELECT id FROM request_events_v1 \
                 WHERE ts < ? \
                 ORDER BY id ASC \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(
            cutoff_secs,
            "request event prune before cutoff",
        )?)
        .bind(u64_to_i64(
            batch_size as u64,
            "request event prune before batch size",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }
}

async fn query_request_events(
    storage: &SqliteStorage,
    since: u64,
    until: u64,
    limit: usize,
    direction: &str,
) -> StorageResult<Vec<RequestEvent>> {
    if limit == 0 || until < since {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT payload FROM request_events_v1 \
         WHERE ts >= ? AND ts <= ? \
         ORDER BY id {direction} LIMIT ?"
    );
    let rows = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql))
        .bind(u64_to_i64(since, "request event since")?)
        .bind(u64_to_i64_upper(until))
        .bind(usize_to_i64(limit, "request event limit")?)
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
}

fn event_ts_secs(event: &RequestEvent) -> u64 {
    event.ts_ms.map(|ts_ms| ts_ms / 1_000).unwrap_or(event.ts)
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

fn u64_to_i64_upper(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_i64(value: usize, field: &str) -> StorageResult<i64> {
    i64::try_from(value).or_else(|_| {
        if value == usize::MAX {
            Ok(i64::MAX)
        } else {
            Err(StorageError::Fatal {
                message: format!("{field} cannot be represented as sqlite INTEGER"),
            })
        }
    })
}
