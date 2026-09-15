use async_trait::async_trait;
use cc_lb_storage_api::{AuditEntry, AuditQueryScope, AuditStore, StorageError, StorageResult};
use serde_json::Value;
use sqlx::{Row, sqlite::SqliteRow};

use crate::{SqliteStorage, map_sqlx_error};

const KEY_SEQUENCE_SCALE: u64 = 1_000_000;

#[async_trait]
impl AuditStore for SqliteStorage {
    async fn append_audit(&self, entry: &AuditEntry) -> StorageResult<()> {
        insert_audit_entry(self.pool(), entry).await
    }

    async fn append_audit_entries(&self, entries: &[AuditEntry]) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut tx = self.begin_immediate().await?;
        for entry in entries {
            insert_audit_entry(&mut *tx, entry).await?;
        }
        tx.commit().await.map_err(map_sqlx_error)?;
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

        let rows = sqlx::query(
            "SELECT ts, request_id, principal_id, route, upstream, model, status, input_tokens, \
             output_tokens, duration_ms, agent_label, api_key_id, cost_usd_micros, \
             limit_violation, admin_action, actor, actor_authority, actor_subject, actor_kind, \
             actor_email, kind, payload \
             FROM audit_log_v1 \
             WHERE ts >= ? AND ts <= ? AND (? IS NULL OR principal_id = ?) \
             ORDER BY id ASC LIMIT ?",
        )
        .bind(u64_to_i64(since, "audit since")?)
        .bind(u64_to_i64_upper(until))
        .bind(principal_id)
        .bind(principal_id)
        .bind(u64_to_i64(limit as u64, "audit limit")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_audit_entry).collect()
    }

    async fn query_audit_by_actor(
        &self,
        authority: &str,
        subject: &str,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            "SELECT ts, request_id, principal_id, route, upstream, model, status, input_tokens, \
             output_tokens, duration_ms, agent_label, api_key_id, cost_usd_micros, \
             limit_violation, admin_action, actor, actor_authority, actor_subject, actor_kind, \
             actor_email, kind, payload \
             FROM audit_log_v1 \
             WHERE ts >= ? AND ts <= ? AND actor_authority = ? AND actor_subject = ? \
             ORDER BY id ASC LIMIT ?",
        )
        .bind(u64_to_i64(since, "audit since")?)
        .bind(u64_to_i64_upper(until))
        .bind(authority)
        .bind(subject)
        .bind(u64_to_i64(limit as u64, "audit limit")?)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_audit_entry).collect()
    }

    async fn query_recent_audit(
        &self,
        scope: AuditQueryScope<'_>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> StorageResult<Vec<AuditEntry>> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }

        let since = u64_to_i64(since, "audit since")?;
        let until = u64_to_i64_upper(until);
        let limit = u64_to_i64(limit as u64, "audit limit")?;
        let rows = match scope {
            AuditQueryScope::All => {
                sqlx::query(
                    "SELECT ts, request_id, principal_id, route, upstream, model, status, \
                     input_tokens, output_tokens, duration_ms, agent_label, api_key_id, \
                     cost_usd_micros, limit_violation, admin_action, actor, actor_authority, \
                     actor_subject, actor_kind, actor_email, kind, payload \
                     FROM audit_log_v1 \
                     WHERE ts >= ? AND ts <= ? \
                     ORDER BY ts DESC, id DESC LIMIT ?",
                )
                .bind(since)
                .bind(until)
                .bind(limit)
                .fetch_all(self.pool())
                .await
            }
            AuditQueryScope::Principal(principal_id) => {
                sqlx::query(
                    "SELECT ts, request_id, principal_id, route, upstream, model, status, \
                     input_tokens, output_tokens, duration_ms, agent_label, api_key_id, \
                     cost_usd_micros, limit_violation, admin_action, actor, actor_authority, \
                     actor_subject, actor_kind, actor_email, kind, payload \
                     FROM audit_log_v1 \
                     WHERE ts >= ? AND ts <= ? AND principal_id = ? \
                     ORDER BY ts DESC, id DESC LIMIT ?",
                )
                .bind(since)
                .bind(until)
                .bind(principal_id)
                .bind(limit)
                .fetch_all(self.pool())
                .await
            }
            AuditQueryScope::Actor { authority, subject } => {
                sqlx::query(
                    "SELECT ts, request_id, principal_id, route, upstream, model, status, \
                     input_tokens, output_tokens, duration_ms, agent_label, api_key_id, \
                     cost_usd_micros, limit_violation, admin_action, actor, actor_authority, \
                     actor_subject, actor_kind, actor_email, kind, payload \
                     FROM audit_log_v1 \
                     WHERE ts >= ? AND ts <= ? \
                     AND actor_authority = ? AND actor_subject = ? \
                     ORDER BY ts DESC, id DESC LIMIT ?",
                )
                .bind(since)
                .bind(until)
                .bind(authority)
                .bind(subject)
                .bind(limit)
                .fetch_all(self.pool())
                .await
            }
        }
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_audit_entry).collect()
    }

    async fn prune_audit(&self, older_than: u64) -> StorageResult<u64> {
        let result = sqlx::query("DELETE FROM audit_log_v1 WHERE ts < ?")
            .bind(u64_to_i64(older_than, "audit prune cutoff")?)
            .execute(self.pool())
            .await
            .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }

    async fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> StorageResult<u64> {
        if batch_size == 0 {
            return Ok(0);
        }

        let cutoff_ts = cutoff_ts_x_1m / KEY_SEQUENCE_SCALE;
        let result = sqlx::query(
            "DELETE FROM audit_log_v1 \
             WHERE id IN ( \
                 SELECT id FROM audit_log_v1 \
                 WHERE ts < ? \
                 ORDER BY id ASC \
                 LIMIT ? \
             )",
        )
        .bind(u64_to_i64(cutoff_ts, "audit prune before cutoff")?)
        .bind(u64_to_i64(
            batch_size as u64,
            "audit prune before batch size",
        )?)
        .execute(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(result.rows_affected())
    }
}

async fn insert_audit_entry<'e, E>(executor: E, entry: &AuditEntry) -> StorageResult<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let payload = entry
        .payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;

    sqlx::query(
        "INSERT INTO audit_log_v1 \
         (ts, request_id, principal_id, route, upstream, model, status, input_tokens, \
          output_tokens, duration_ms, agent_label, api_key_id, cost_usd_micros, \
          limit_violation, admin_action, actor, actor_authority, actor_subject, actor_kind, \
          actor_email, kind, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(u64_to_i64(entry.ts, "audit ts")?)
    .bind(&entry.request_id)
    .bind(&entry.principal_id)
    .bind(&entry.route)
    .bind(&entry.upstream)
    .bind(entry.model.as_deref())
    .bind(i64::from(entry.status))
    .bind(option_u64_to_i64(entry.input_tokens, "audit input_tokens")?)
    .bind(option_u64_to_i64(
        entry.output_tokens,
        "audit output_tokens",
    )?)
    .bind(u64_to_i64(entry.duration_ms, "audit duration_ms")?)
    .bind(entry.agent_label.as_deref())
    .bind(entry.api_key_id.as_deref())
    .bind(option_u64_to_i64(
        entry.cost_usd_micros,
        "audit cost_usd_micros",
    )?)
    .bind(entry.limit_violation.as_deref())
    .bind(entry.admin_action.as_deref())
    .bind(entry.actor.as_deref())
    .bind(entry.actor_authority.as_deref())
    .bind(entry.actor_subject.as_deref())
    .bind(entry.actor_kind.as_deref())
    .bind(entry.actor_email.as_deref())
    .bind(entry.kind.as_deref())
    .bind(payload.as_deref())
    .execute(executor)
    .await
    .map_err(map_sqlx_error)?;

    Ok(())
}

fn row_to_audit_entry(row: SqliteRow) -> StorageResult<AuditEntry> {
    let payload = row
        .try_get::<Option<String>, _>("payload")
        .map_err(map_sqlx_error)?
        .map(|payload| serde_json::from_str::<Value>(&payload))
        .transpose()?;

    Ok(AuditEntry {
        ts: i64_to_u64(row.try_get("ts").map_err(map_sqlx_error)?, "audit ts")?,
        request_id: row.try_get("request_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        route: row.try_get("route").map_err(map_sqlx_error)?,
        upstream: row.try_get("upstream").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        status: i64_to_u16(
            row.try_get("status").map_err(map_sqlx_error)?,
            "audit status",
        )?,
        input_tokens: row
            .try_get::<Option<i64>, _>("input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "audit input_tokens"))
            .transpose()?,
        output_tokens: row
            .try_get::<Option<i64>, _>("output_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "audit output_tokens"))
            .transpose()?,
        duration_ms: i64_to_u64(
            row.try_get("duration_ms").map_err(map_sqlx_error)?,
            "audit duration_ms",
        )?,
        agent_label: row.try_get("agent_label").map_err(map_sqlx_error)?,
        api_key_id: row.try_get("api_key_id").map_err(map_sqlx_error)?,
        cost_usd_micros: row
            .try_get::<Option<i64>, _>("cost_usd_micros")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u64(value, "audit cost_usd_micros"))
            .transpose()?,
        limit_violation: row.try_get("limit_violation").map_err(map_sqlx_error)?,
        admin_action: row.try_get("admin_action").map_err(map_sqlx_error)?,
        actor: row.try_get("actor").map_err(map_sqlx_error)?,
        actor_authority: row.try_get("actor_authority").map_err(map_sqlx_error)?,
        actor_subject: row.try_get("actor_subject").map_err(map_sqlx_error)?,
        actor_kind: row.try_get("actor_kind").map_err(map_sqlx_error)?,
        actor_email: row.try_get("actor_email").map_err(map_sqlx_error)?,
        kind: row.try_get("kind").map_err(map_sqlx_error)?,
        payload,
    })
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as sqlite INTEGER"),
    })
}

fn u64_to_i64_upper(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative in sqlite storage"),
    })
}

fn i64_to_u16(value: i64, field: &str) -> StorageResult<u16> {
    u16::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is outside u16 range in sqlite storage"),
    })
}
