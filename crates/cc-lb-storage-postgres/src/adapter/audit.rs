use async_trait::async_trait;
use cc_lb_storage_api::{AuditEntry, AuditStore, StorageResult};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, postgres::PgRow};

use crate::{
    adapter::{
        PostgresStorage, datetime_to_unix_secs, i32_to_u16, i64_to_u64, u64_to_i64,
        unix_secs_to_datetime, unix_secs_to_datetime_lower, unix_secs_to_datetime_upper,
    },
    error_map::map_sqlx_error,
};

#[async_trait]
impl AuditStore for PostgresStorage {
    async fn append_audit(&self, entry: &AuditEntry) -> StorageResult<()> {
        let payload = entry.payload.as_ref().map(serde_json::to_vec).transpose()?;

        sqlx::query(
            "INSERT INTO audit_log_v1 (ts, request_id, principal_id, route, upstream, model, status,              input_tokens, output_tokens, duration_ms, agent_label, kind, payload)              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        )
        .bind(unix_secs_to_datetime(entry.ts, "audit ts")?)
        .bind(&entry.request_id)
        .bind(&entry.principal_id)
        .bind(&entry.route)
        .bind(&entry.upstream)
        .bind(entry.model.as_deref())
        .bind(i32::from(entry.status))
        .bind(u64_to_i64(entry.input_tokens, "audit input_tokens")?)
        .bind(u64_to_i64(entry.output_tokens, "audit output_tokens")?)
        .bind(u64_to_i64(entry.duration_ms, "audit duration_ms")?)
        .bind(entry.agent_label.as_deref())
        .bind(entry.kind.as_deref())
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(())
    }

    async fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }
        let Some(since) = unix_secs_to_datetime_lower(since, "audit since")? else {
            return Ok(Vec::new());
        };
        let until = unix_secs_to_datetime_upper(until, "audit until")?;

        let rows = sqlx::query(
            "SELECT ts, request_id, principal_id, route, upstream, model, status,              input_tokens, output_tokens, duration_ms, agent_label, kind, payload              FROM audit_log_v1              WHERE ts >= $1 AND ($2::timestamptz IS NULL OR ts <= $2)              AND ($3::text IS NULL OR principal_id = $3)              ORDER BY seq ASC LIMIT $4",
        )
        .bind(since)
        .bind(until)
        .bind(principal_id)
        .bind(u64_to_i64(limit as u64, "audit limit")?)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_audit_entry).collect()
    }

    async fn prune_audit(&self, older_than: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM audit_log_v1 WHERE ts < $1")
            .bind(unix_secs_to_datetime(older_than, "audit prune cutoff")?)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(result.rows_affected())
    }
}

fn row_to_audit_entry(row: PgRow) -> StorageResult<AuditEntry> {
    let ts = row
        .try_get::<DateTime<Utc>, _>("ts")
        .map_err(map_sqlx_error)?;
    let payload = row
        .try_get::<Option<Vec<u8>>, _>("payload")
        .map_err(map_sqlx_error)?
        .map(|payload| serde_json::from_slice::<Value>(&payload))
        .transpose()?;

    Ok(AuditEntry {
        ts: datetime_to_unix_secs(ts, "audit ts")?,
        request_id: row.try_get("request_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        route: row.try_get("route").map_err(map_sqlx_error)?,
        upstream: row.try_get("upstream").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        status: i32_to_u16(
            row.try_get("status").map_err(map_sqlx_error)?,
            "audit status",
        )?,
        input_tokens: i64_to_u64(
            row.try_get("input_tokens").map_err(map_sqlx_error)?,
            "audit input_tokens",
        )?,
        output_tokens: i64_to_u64(
            row.try_get("output_tokens").map_err(map_sqlx_error)?,
            "audit output_tokens",
        )?,
        duration_ms: i64_to_u64(
            row.try_get("duration_ms").map_err(map_sqlx_error)?,
            "audit duration_ms",
        )?,
        agent_label: row.try_get("agent_label").map_err(map_sqlx_error)?,
        kind: row.try_get("kind").map_err(map_sqlx_error)?,
        payload,
    })
}
