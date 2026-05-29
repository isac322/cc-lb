use async_trait::async_trait;
use cc_lb_storage_api::{
    RateLimitKind, StorageError, StorageResult, UpstreamRateLimitObservationRecord,
    UpstreamRateLimitStateStore,
};
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::{
    adapter::{PostgresStorage, i64_to_u64, u64_to_i64},
    error_map::map_sqlx_error,
};

#[async_trait]
impl UpstreamRateLimitStateStore for PostgresStorage {
    async fn put_observation(
        &self,
        observation: &UpstreamRateLimitObservationRecord,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO upstream_rate_limit_state_v1 \
              (upstream_id, \"window\", kind, limit_value, remaining, reset, observed_at_unix_secs) \
              VALUES ($1, $2, $3, $4, $5, $6, $7) \
              ON CONFLICT (upstream_id, \"window\", kind) DO UPDATE \
              SET limit_value = EXCLUDED.limit_value, \
                  remaining = EXCLUDED.remaining, \
                  reset = EXCLUDED.reset, \
                  observed_at_unix_secs = EXCLUDED.observed_at_unix_secs \
              WHERE EXCLUDED.observed_at_unix_secs >= upstream_rate_limit_state_v1.observed_at_unix_secs",
        )
        .bind(observation.upstream_id)
        .bind(&observation.window)
        .bind(rate_limit_kind_to_str(observation.kind))
        .bind(
            observation
                .limit
                .map(|value| u64_to_i64(value, "upstream rate limit limit"))
                .transpose()?,
        )
        .bind(
            observation
                .remaining
                .map(|value| u64_to_i64(value, "upstream rate limit remaining"))
                .transpose()?,
        )
        .bind(&observation.reset)
        .bind(u64_to_i64(
            observation.observed_at_unix_secs,
            "upstream rate limit observed_at_unix_secs",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn list_for_upstream_ids(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
        if upstream_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            "SELECT upstream_id, \"window\", kind, limit_value, remaining, reset, observed_at_unix_secs \
              FROM upstream_rate_limit_state_v1 \
              WHERE upstream_id = ANY($1::uuid[]) \
              ORDER BY upstream_id ASC, \"window\" ASC, kind ASC",
        )
        .bind(upstream_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_observation).collect()
    }
}

fn row_to_observation(row: PgRow) -> StorageResult<UpstreamRateLimitObservationRecord> {
    let kind =
        rate_limit_kind_from_str(&row.try_get::<String, _>("kind").map_err(map_sqlx_error)?)?;
    let limit: Option<i64> = row.try_get("limit_value").map_err(map_sqlx_error)?;
    let remaining: Option<i64> = row.try_get("remaining").map_err(map_sqlx_error)?;
    let observed_at_unix_secs: i64 = row
        .try_get("observed_at_unix_secs")
        .map_err(map_sqlx_error)?;

    Ok(UpstreamRateLimitObservationRecord {
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        window: row.try_get("window").map_err(map_sqlx_error)?,
        kind,
        limit: limit
            .map(|value| i64_to_u64(value, "upstream rate limit limit"))
            .transpose()?,
        remaining: remaining
            .map(|value| i64_to_u64(value, "upstream rate limit remaining"))
            .transpose()?,
        reset: row.try_get("reset").map_err(map_sqlx_error)?,
        observed_at_unix_secs: i64_to_u64(
            observed_at_unix_secs,
            "upstream rate limit observed_at_unix_secs",
        )?,
    })
}

fn rate_limit_kind_to_str(kind: RateLimitKind) -> &'static str {
    match kind {
        RateLimitKind::Requests => "requests",
        RateLimitKind::Tokens => "tokens",
        RateLimitKind::InputTokens => "input_tokens",
        RateLimitKind::OutputTokens => "output_tokens",
    }
}

fn rate_limit_kind_from_str(value: &str) -> StorageResult<RateLimitKind> {
    match value {
        "requests" => Ok(RateLimitKind::Requests),
        "tokens" => Ok(RateLimitKind::Tokens),
        "input_tokens" => Ok(RateLimitKind::InputTokens),
        "output_tokens" => Ok(RateLimitKind::OutputTokens),
        value => Err(StorageError::Corrupted {
            message: format!("invalid upstream rate limit kind {value}"),
        }),
    }
}
