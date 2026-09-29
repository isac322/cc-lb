use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRecord, CacheKeepaliveSessionCursor, CacheKeepaliveSessionFilter,
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionPage,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSummaryInput, CacheKeepaliveTurnRecord,
    StorageError, StorageResult,
};
use sqlx::{Postgres, QueryBuilder};

use super::{
    PostgresStorage,
    cache_keepalive_session_read_row::{decision_from_row, list_item_from_row, turn_from_row},
    cache_keepalive_sessions::row_to_record,
    u64_to_i64,
};
use crate::error_map::map_sqlx_error;

const SESSION_ENTRY_SELECT: &str = "\
SELECT \
    'session'::TEXT AS entry_source, \
    'session:' || session_key_hash AS entry_id, \
    session_key_hash, \
    principal_id, \
    upstream_id, \
    last_message_at_ms, \
    ttl, \
    generation, \
    refresh_count, \
    status, \
    enqueue_state, \
    terminal_reason, \
    NULL::TEXT AS decision, \
    CASE terminal_reason \
        WHEN 'max_refreshes' THEN 'max renewals reached' \
        WHEN 'max_duration' THEN 'max duration reached (4h)' \
        WHEN 'expired' THEN 'TTL expired before follow-up' \
        WHEN 'dispatch_error' THEN 'renewal dispatch unavailable' \
        ELSE display_reason \
    END AS reason, \
    error, \
    config_snapshot";

const DECISION_ENTRY_SELECT: &str = "\
SELECT \
    'decision'::TEXT AS entry_source, \
    'decision:' || source_ref_id AS entry_id, \
    session_key_hash, \
    principal_id, \
    upstream_id, \
    last_message_at_ms, \
    ttl, \
    generation, \
    NULL::BIGINT AS refresh_count, \
    NULL::TEXT AS status, \
    NULL::TEXT AS enqueue_state, \
    NULL::TEXT AS terminal_reason, \
    decision, \
    reason, \
    error, \
    config_snapshot";

const VISIBLE_DECISION_ANTI_JOIN: &str = " AND NOT EXISTS (\
    SELECT 1 FROM cache_keepalive_turns turn_row \
    WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id\
)";

const RECENT_VISIBLE_DECISION_COUNT_SQL: &str = "\
SELECT COUNT(*) \
FROM cache_keepalive_decisions \
WHERE principal_id = $1 \
  AND last_message_at_ms >= $2 \
  AND NOT EXISTS (\
      SELECT 1 FROM cache_keepalive_turns turn_row \
      WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id\
  )";

const LIST_TURNS_FOR_SESSIONS_SQL: &str = "
SELECT
    source_ref_id,
    session_key_hash,
    principal_id,
    accounting_key_id,
    upstream_id,
    model,
    input_tokens,
    output_tokens,
    cache_creation_input_tokens,
    cache_creation_input_tokens_5m,
    cache_creation_input_tokens_1h,
    cache_read_input_tokens,
    cost_micros,
    hit_miss,
    ts
FROM cache_keepalive_turns
WHERE principal_id = $1
  AND session_key_hash = ANY($2)
ORDER BY session_key_hash ASC, ts ASC, source_ref_id ASC
";
fn query_u64_to_i64(value: u64, field: &str, reason: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: reason.to_owned(),
    })
}

fn filter_includes_sessions(filter: CacheKeepaliveSessionFilter) -> bool {
    !matches!(filter, CacheKeepaliveSessionFilter::NotTracked)
}

fn filter_includes_decisions(filter: CacheKeepaliveSessionFilter) -> bool {
    matches!(
        filter,
        CacheKeepaliveSessionFilter::All
            | CacheKeepaliveSessionFilter::NotTracked
            | CacheKeepaliveSessionFilter::Error
    )
}

fn push_session_filter(builder: &mut QueryBuilder<Postgres>, filter: CacheKeepaliveSessionFilter) {
    match filter {
        CacheKeepaliveSessionFilter::All | CacheKeepaliveSessionFilter::Error => {}
        CacheKeepaliveSessionFilter::Renewed => {
            builder.push(" AND status = 'active' AND refresh_count > 0");
        }
        CacheKeepaliveSessionFilter::Scheduled => {
            builder.push(" AND status = 'active' AND refresh_count = 0");
        }
        CacheKeepaliveSessionFilter::Capped => {
            builder.push(" AND terminal_reason IN ('max_refreshes', 'max_duration')");
        }
        CacheKeepaliveSessionFilter::Expired => {
            builder.push(" AND terminal_reason = 'expired'");
        }
        CacheKeepaliveSessionFilter::NotTracked => unreachable!("session branch is omitted"),
    }
    if matches!(filter, CacheKeepaliveSessionFilter::Error) {
        builder.push(" AND error IS NOT NULL");
    }
}

fn push_decision_filter(builder: &mut QueryBuilder<Postgres>, filter: CacheKeepaliveSessionFilter) {
    match filter {
        CacheKeepaliveSessionFilter::All => {}
        CacheKeepaliveSessionFilter::NotTracked => {
            builder.push(" AND decision = 'not_tracked'");
        }
        CacheKeepaliveSessionFilter::Error => {
            builder.push(" AND error IS NOT NULL");
        }
        _ => unreachable!("decision branch is omitted"),
    }
}

fn push_page_bounds(
    builder: &mut QueryBuilder<Postgres>,
    timestamp_expression: &'static str,
    entry_id_expression: &'static str,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    cursor_entry_id: Option<&str>,
) {
    if let Some(horizon_start_ms) = horizon_start_ms {
        builder.push(" AND ");
        builder.push(timestamp_expression);
        builder.push(" >= ");
        builder.push_bind(horizon_start_ms);
    }
    if let (Some(cursor_last_message_at_ms), Some(cursor_entry_id)) =
        (cursor_last_message_at_ms, cursor_entry_id)
    {
        builder.push(" AND ");
        builder.push(timestamp_expression);
        builder.push(" <= ");
        builder.push_bind(cursor_last_message_at_ms);
        builder.push(" AND (");
        builder.push(timestamp_expression);
        builder.push(" < ");
        builder.push_bind(cursor_last_message_at_ms);
        builder.push(" OR (");
        builder.push(timestamp_expression);
        builder.push(" = ");
        builder.push_bind(cursor_last_message_at_ms);
        builder.push(" AND ");
        builder.push(entry_id_expression);
        builder.push(" > ");
        builder.push_bind(cursor_entry_id);
        builder.push("))");
    }
}

fn push_session_list_branch(
    builder: &mut QueryBuilder<Postgres>,
    query: &CacheKeepaliveSessionListQuery,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    cursor_entry_id: Option<&str>,
    fetch_limit: i64,
) {
    builder.push(SESSION_ENTRY_SELECT);
    builder.push(" FROM cache_keepalive_sessions WHERE principal_id = ");
    builder.push_bind(&query.principal_id);
    push_session_filter(builder, query.filter);
    push_page_bounds(
        builder,
        "last_message_at_ms",
        "('session:' || session_key_hash)",
        horizon_start_ms,
        cursor_last_message_at_ms,
        cursor_entry_id,
    );
    builder.push(" ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ");
    builder.push_bind(fetch_limit);
}

fn push_decision_list_branch(
    builder: &mut QueryBuilder<Postgres>,
    query: &CacheKeepaliveSessionListQuery,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    cursor_entry_id: Option<&str>,
    fetch_limit: i64,
) {
    builder.push(DECISION_ENTRY_SELECT);
    builder.push(" FROM cache_keepalive_decisions WHERE principal_id = ");
    builder.push_bind(&query.principal_id);
    builder.push(VISIBLE_DECISION_ANTI_JOIN);
    push_decision_filter(builder, query.filter);
    push_page_bounds(
        builder,
        "last_message_at_ms",
        "('decision:' || source_ref_id)",
        horizon_start_ms,
        cursor_last_message_at_ms,
        cursor_entry_id,
    );
    builder.push(" ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ");
    builder.push_bind(fetch_limit);
}

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
        let limit = usize::try_from(query.limit).map_err(|_| StorageError::InvalidInput {
            field: "cache_keepalive_limit".to_owned(),
            reason: "value cannot be represented as usize".to_owned(),
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
        let fetch_limit = i64::from(query.limit.saturating_add(1));

        let include_sessions = filter_includes_sessions(query.filter);
        let include_decisions = filter_includes_decisions(query.filter);
        let mut builder = QueryBuilder::<Postgres>::new("WITH ");
        if include_sessions {
            builder.push("session_entries AS (");
            push_session_list_branch(
                &mut builder,
                query,
                horizon_start_ms,
                cursor_last_message_at_ms,
                cursor_entry_id,
                fetch_limit,
            );
            builder.push(")");
        }
        if include_decisions {
            if include_sessions {
                builder.push(", ");
            }
            builder.push("decision_entries AS (");
            push_decision_list_branch(
                &mut builder,
                query,
                horizon_start_ms,
                cursor_last_message_at_ms,
                cursor_entry_id,
                fetch_limit,
            );
            builder.push(")");
        }
        builder.push(" SELECT * FROM (");
        if include_sessions {
            builder.push("SELECT * FROM session_entries");
        }
        if include_sessions && include_decisions {
            builder.push(" UNION ALL ");
        }
        if include_decisions {
            builder.push("SELECT * FROM decision_entries");
        }
        builder.push(") entries ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ");
        builder.push_bind(fetch_limit);

        let rows = builder
            .build()
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

    async fn read_cache_keepalive_summary_input(
        &self,
        principal_id: &str,
        cutoff_ms: u64,
    ) -> StorageResult<CacheKeepaliveSummaryInput> {
        let cutoff_ms = query_u64_to_i64(
            cutoff_ms,
            "cache_keepalive_summary_cutoff_ms",
            "value exceeds i64::MAX",
        )?;
        let mut sessions_query = QueryBuilder::<Postgres>::new(SESSION_ENTRY_SELECT);
        sessions_query.push(" FROM cache_keepalive_sessions WHERE principal_id = ");
        sessions_query.push_bind(principal_id);
        sessions_query.push(" ORDER BY last_message_at_ms DESC, entry_id ASC");
        let sessions = sessions_query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .map(list_item_from_row)
            .collect::<StorageResult<Vec<CacheKeepaliveSessionListItem>>>()?;
        let recent_decisions = sqlx::query_scalar::<_, i64>(RECENT_VISIBLE_DECISION_COUNT_SQL)
            .bind(principal_id)
            .bind(cutoff_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        let recent_decisions =
            u64::try_from(recent_decisions).map_err(|_| StorageError::Corrupted {
                message: "negative cache keepalive recent decision count".to_owned(),
            })?;
        Ok(CacheKeepaliveSummaryInput {
            sessions,
            recent_decisions,
        })
    }

    async fn get_cache_keepalive_list_item(
        &self,
        principal_id: &str,
        id: &str,
    ) -> StorageResult<Option<CacheKeepaliveSessionListItem>> {
        let mut builder = QueryBuilder::<Postgres>::new("WITH candidates AS (");
        builder.push(SESSION_ENTRY_SELECT);
        builder.push(" FROM cache_keepalive_sessions WHERE principal_id = ");
        builder.push_bind(principal_id);
        builder.push(" AND session_key_hash = ");
        builder.push_bind(id);
        builder.push(" UNION ALL ");
        builder.push(DECISION_ENTRY_SELECT);
        builder.push(" FROM cache_keepalive_decisions WHERE principal_id = ");
        builder.push_bind(principal_id);
        builder.push(" AND source_ref_id = ");
        builder.push_bind(id);
        builder.push(VISIBLE_DECISION_ANTI_JOIN);
        builder.push(
            ") SELECT * FROM candidates ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT 1",
        );
        builder
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .map(list_item_from_row)
            .transpose()
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

    async fn list_cache_keepalive_turns_for_sessions(
        &self,
        principal_id: &str,
        session_key_hashes: &[String],
    ) -> StorageResult<Vec<CacheKeepaliveTurnRecord>> {
        if session_key_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(LIST_TURNS_FOR_SESSIONS_SQL)
            .bind(principal_id)
            .bind(session_key_hashes)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
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
