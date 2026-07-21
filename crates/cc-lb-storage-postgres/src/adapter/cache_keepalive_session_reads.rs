use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRecord, CacheKeepaliveSessionCursor, CacheKeepaliveSessionListQuery,
    CacheKeepaliveSessionPage, CacheKeepaliveSessionReadStore, CacheKeepaliveTurnRecord,
    StorageResult,
};

use super::{
    PostgresStorage,
    cache_keepalive_session_read_row::{decision_from_row, list_item_from_row, turn_from_row},
    cache_keepalive_sessions::row_to_record,
    u64_to_i64,
};
use crate::error_map::map_sqlx_error;

const LIST_SQL: &str = "
WITH entries AS (
    SELECT
        'session'::TEXT AS entry_source,
        'session:' || session_key_hash AS entry_id,
        session_key_hash,
        principal_id,
        upstream_id,
        last_message_at_ms,
        ttl,
        generation,
        refresh_count,
        status,
        enqueue_state,
        terminal_reason,
        NULL::TEXT AS decision,
        CASE terminal_reason
            WHEN 'max_refreshes' THEN 'max renewals reached'
            WHEN 'max_duration' THEN 'max duration reached (4h)'
            WHEN 'expired' THEN 'TTL expired before follow-up'
            WHEN 'dispatch_error' THEN 'renewal dispatch unavailable'
            ELSE display_reason
        END AS reason,
        error,
        config_snapshot
    FROM cache_keepalive_sessions
    WHERE principal_id = $1
    UNION ALL
    SELECT
        'decision'::TEXT AS entry_source,
        'decision:' || source_ref_id AS entry_id,
        session_key_hash,
        principal_id,
        upstream_id,
        COALESCE(last_message_at_ms, ts * 1000) AS last_message_at_ms,
        ttl,
        generation,
        NULL::BIGINT AS refresh_count,
        NULL::TEXT AS status,
        NULL::TEXT AS enqueue_state,
        NULL::TEXT AS terminal_reason,
        decision,
        reason,
        error,
        config_snapshot
    FROM cache_keepalive_decisions
    WHERE principal_id = $2
      AND NOT EXISTS (
          SELECT 1 FROM cache_keepalive_turns turn_row
          WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id
      )
)
SELECT *
FROM entries
WHERE ($3::BIGINT IS NULL OR last_message_at_ms >= $4)
  AND CASE $5
      WHEN 'all' THEN TRUE
      WHEN 'renewed' THEN entry_source = 'session' AND status = 'active' AND refresh_count > 0
      WHEN 'scheduled' THEN entry_source = 'session' AND status = 'active' AND refresh_count = 0
      WHEN 'capped' THEN entry_source = 'session' AND terminal_reason IN ('max_refreshes', 'max_duration')
      WHEN 'expired' THEN entry_source = 'session' AND terminal_reason = 'expired'
      WHEN 'not_tracked' THEN entry_source = 'decision' AND decision = 'not_tracked'
      WHEN 'error' THEN error IS NOT NULL
      ELSE FALSE
  END
  AND ($6::BIGINT IS NULL OR last_message_at_ms < $7 OR (last_message_at_ms = $8 AND entry_id > $9))
ORDER BY last_message_at_ms DESC, entry_id ASC
LIMIT $10
";

#[async_trait]
impl CacheKeepaliveSessionReadStore for PostgresStorage {
    async fn list_cache_keepalive_sessions(
        &self,
        query: &CacheKeepaliveSessionListQuery,
    ) -> StorageResult<CacheKeepaliveSessionPage> {
        query.validate_cursor()?;
        if query.limit == 0 {
            return Ok(CacheKeepaliveSessionPage {
                rows: Vec::new(),
                next_cursor: None,
            });
        }
        let limit = usize::try_from(query.limit).map_err(|_| {
            cc_lb_storage_api::StorageError::InvalidInput {
                field: "cache_keepalive_limit".to_owned(),
                reason: "value cannot be represented as usize".to_owned(),
            }
        })?;
        let horizon_start_ms = query
            .horizon_start_ms
            .map(|value| u64_to_i64(value, "cache keepalive horizon start"))
            .transpose()?;
        let cursor_last_message_at_ms = query
            .cursor
            .as_ref()
            .map(|cursor| {
                u64_to_i64(
                    cursor.last_message_at_ms,
                    "cache keepalive cursor timestamp",
                )
            })
            .transpose()?;
        let cursor_entry_id = query.cursor.as_ref().map(|cursor| cursor.entry_id.as_str());
        let rows = sqlx::query(LIST_SQL)
            .bind(&query.principal_id)
            .bind(&query.principal_id)
            .bind(horizon_start_ms)
            .bind(horizon_start_ms)
            .bind(query.filter.as_str())
            .bind(cursor_last_message_at_ms)
            .bind(cursor_last_message_at_ms)
            .bind(cursor_last_message_at_ms)
            .bind(cursor_entry_id)
            .bind(i64::from(query.limit.saturating_add(1)))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        let mut rows = rows
            .into_iter()
            .map(list_item_from_row)
            .collect::<StorageResult<Vec<_>>>()?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                let row = rows.last()?;
                Some(CacheKeepaliveSessionCursor {
                    principal_id: query.principal_id.clone(),
                    horizon_start_ms: query.horizon_start_ms,
                    filter: query.filter,
                    last_message_at_ms: row.last_message_at_ms,
                    entry_id: row.source.cursor_entry_id(&row.id),
                })
            })
            .flatten();
        Ok(CacheKeepaliveSessionPage { rows, next_cursor })
    }

    async fn get_cache_keepalive_session_for_principal(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> StorageResult<Option<cc_lb_storage_api::CacheKeepaliveSessionRecord>> {
        let row = sqlx::query("SELECT * FROM cache_keepalive_sessions WHERE principal_id = $1 AND session_key_hash = $2")
            .bind(principal_id).bind(session_key_hash).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list_cache_keepalive_turns(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> StorageResult<Vec<CacheKeepaliveTurnRecord>> {
        let rows = sqlx::query("SELECT * FROM cache_keepalive_turns WHERE principal_id = $1 AND session_key_hash = $2 ORDER BY ts DESC, source_ref_id ASC")
            .bind(principal_id).bind(session_key_hash).fetch_all(&self.pool).await.map_err(map_sqlx_error)?;
        rows.into_iter().map(turn_from_row).collect()
    }

    async fn get_cache_keepalive_decision_for_principal(
        &self,
        principal_id: &str,
        source_ref_id: &str,
    ) -> StorageResult<Option<CacheKeepaliveDecisionRecord>> {
        let row = sqlx::query("SELECT * FROM cache_keepalive_decisions WHERE principal_id = $1 AND source_ref_id = $2")
            .bind(principal_id).bind(source_ref_id).fetch_optional(&self.pool).await.map_err(map_sqlx_error)?;
        row.map(decision_from_row).transpose()
    }
}
