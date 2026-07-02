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
        let cache_breakpoints = serde_json::to_value(&event.cache_breakpoints)?;
        sqlx::query(
            "INSERT INTO request_events_v1 \
             (ts, principal_id, upstream_id, key_id, model, upstream_name, cache_state, thread_id, message_id, \
              message_index, message_count, cache_control_block_count, cache_breakpoints, cache_prefix_hash, \
               input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, \
               event_id, error_code, upstream_error_type, upstream_error_message, \
               thinking_tokens, web_search_requests, web_fetch_requests, \
               service_tier, inference_geo, \
               cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, \
               shadow_event_id, payload, created_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,NOW()) \
             ON CONFLICT(event_id) WHERE event_id IS NOT NULL DO NOTHING",
        )
        .bind(unix_secs_to_datetime(event.ts, "request event ts")?)
        .bind(event.principal_id.as_deref())
        .bind(event.upstream_id)
        .bind(event.key_id.as_deref())
        .bind(event.model.as_deref())
        .bind(event.upstream_name.as_deref())
        .bind(event.cache_state.map(|state| state.as_str()))
        .bind(event.thread_id.as_deref())
        .bind(event.message_id.as_deref())
        .bind(
            event
                .message_index
                .map(|value| u64_to_i64(value, "request event message_index"))
                .transpose()?,
        )
        .bind(
            event
                .message_count
                .map(|value| u64_to_i64(value, "request event message_count"))
                .transpose()?,
        )
        .bind(
            event
                .cache_control_block_count
                .map(|value| u64_to_i64(value, "request event cache_control_block_count"))
                .transpose()?,
        )
        .bind(cache_breakpoints)
        .bind(event.cache_prefix_hash.as_deref())
        .bind(
            event
                .input_tokens
                .map(|value| u64_to_i64(value, "request event input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .output_tokens
                .map(|value| u64_to_i64(value, "request event output_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .cache_read_input_tokens
                .map(|value| u64_to_i64(value, "request event cache_read_input_tokens"))
                .transpose()?,
        )
        .bind(event.event_id.as_deref())
        .bind(event.error_code.as_deref())
        .bind(event.upstream_error_type.as_deref())
        .bind(event.upstream_error_message.as_deref())
        .bind(
            event
                .thinking_tokens
                .map(|value| u64_to_i64(value, "request event thinking_tokens"))
                .transpose()?,
        )
        .bind(
            event
                .web_search_requests
                .map(|value| u64_to_i64(value, "request event web_search_requests"))
                .transpose()?,
        )
        .bind(
            event
                .web_fetch_requests
                .map(|value| u64_to_i64(value, "request event web_fetch_requests"))
                .transpose()?,
        )
        .bind(event.service_tier.as_deref())
        .bind(event.inference_geo.as_deref())
        .bind(
            event
                .cache_creation_input_tokens_5m
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_5m"))
                .transpose()?,
        )
        .bind(
            event
                .cache_creation_input_tokens_1h
                .map(|value| u64_to_i64(value, "request event cache_creation_input_tokens_1h"))
                .transpose()?,
        )
        .bind(event.shadow_event_id.as_deref())
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
            "SELECT payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2) AND shadow_event_id IS NULL              ORDER BY seq ASC LIMIT $3",
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

    async fn query_recent_request_events(
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
            "SELECT payload FROM request_events_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2) AND shadow_event_id IS NULL              ORDER BY seq DESC LIMIT $3",
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
