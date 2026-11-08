use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRecord, CacheKeepaliveSessionCursor, CacheKeepaliveSessionFilter,
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionPage,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSummaryInput, CacheKeepaliveTurnRecord,
    StorageError, StorageResult,
};
use sqlx::{Postgres, QueryBuilder, Row};

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
    cycle_id, \
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
        WHEN 'max_duration' THEN 'max duration reached' \
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
    NULL::UUID AS cycle_id, \
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
    cycle_id,
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
            builder.push(
                " AND status = 'terminal' AND terminal_reason NOT IN ('expired', 'cache_miss')",
            );
        }
        CacheKeepaliveSessionFilter::Expired => {
            builder.push(" AND terminal_reason IN ('expired', 'cache_miss')");
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
            .bind(principal_id)
            .bind(source_ref_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(decision_from_row).transpose()
    }

    async fn list_cache_keepalive_evidence_for_activity(
        &self,
        principal_id: &str,
        cycle_ids: &[uuid::Uuid],
        source_ref_ids: &[String],
        follow_up_from_ms: u64,
        follow_up_until_ms: u64,
    ) -> StorageResult<Vec<cc_lb_storage_api::CacheKeepaliveEvidenceRecord>> {
        if cycle_ids.is_empty() && source_ref_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut renewal_query = QueryBuilder::<Postgres>::new(
            "SELECT * FROM cache_keepalive_evidence WHERE principal_id = ",
        );
        renewal_query.push_bind(principal_id);
        renewal_query.push(" AND (");
        let mut has_clause = false;
        if !cycle_ids.is_empty() {
            renewal_query.push("cycle_id = ANY(");
            renewal_query.push_bind(cycle_ids);
            renewal_query.push(")");
            has_clause = true;
        }
        if !source_ref_ids.is_empty() {
            if has_clause {
                renewal_query.push(" OR ");
            }
            renewal_query.push("event_ref = ANY(");
            renewal_query.push_bind(source_ref_ids);
            renewal_query.push(")");
        }
        renewal_query.push(") ORDER BY request_start_ms ASC, evidence_id ASC");
        let renewal_rows = renewal_query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        let mut evidence = renewal_rows
            .into_iter()
            .map(postgres_evidence_from_row)
            .collect::<StorageResult<Vec<_>>>()?;
        let prefix_hashes = evidence
            .iter()
            .filter_map(|entry| entry.prefix_hash.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if prefix_hashes.is_empty() {
            return Ok(evidence);
        }
        let follow_up_from_ms = evidence
            .iter()
            .filter(|entry| entry.evidence_kind == "renewal")
            .map(|entry| entry.request_start_ms)
            .min()
            .unwrap_or(follow_up_from_ms);
        let follow_up_until_ms = evidence
            .iter()
            .filter(|entry| entry.evidence_kind == "renewal")
            .map(|entry| {
                entry
                    .request_start_ms
                    .saturating_add(entry.ttl.as_secs().saturating_mul(1_000))
            })
            .max()
            .unwrap_or(follow_up_until_ms);
        let mut follow_up_query = QueryBuilder::<Postgres>::new(
            "SELECT * FROM cache_keepalive_evidence \
             WHERE principal_id = ",
        );
        follow_up_query.push_bind(principal_id);
        follow_up_query.push(" AND evidence_kind = 'follow_up' AND prefix_hash = ANY(");
        follow_up_query.push_bind(prefix_hashes.into_iter().collect::<Vec<_>>());
        follow_up_query.push(") AND request_start_ms > ");
        follow_up_query.push_bind(u64_to_i64(
            follow_up_from_ms,
            "cache_keepalive_follow_up_from_ms",
        )?);
        follow_up_query.push(" AND request_start_ms <= ");
        follow_up_query.push_bind(u64_to_i64(
            follow_up_until_ms,
            "cache_keepalive_follow_up_until_ms",
        )?);
        follow_up_query.push(" ORDER BY request_start_ms ASC, evidence_id ASC");
        let follow_up_rows = follow_up_query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        evidence.extend(
            follow_up_rows
                .into_iter()
                .map(postgres_evidence_from_row)
                .collect::<StorageResult<Vec<_>>>()?,
        );
        evidence.sort_unstable_by(|left, right| {
            left.request_start_ms
                .cmp(&right.request_start_ms)
                .then_with(|| left.evidence_id.cmp(&right.evidence_id))
        });
        Ok(evidence)
    }

    async fn read_cache_keepalive_summary_aggregate(
        &self,
        principal_id: &str,
        cutoff_ms: u64,
    ) -> StorageResult<cc_lb_storage_api::CacheKeepaliveSummaryAggregate> {
        self.read_cache_keepalive_summary_aggregate_for_horizon(principal_id, cutoff_ms, None)
            .await
    }

    async fn read_cache_keepalive_summary_aggregate_for_horizon(
        &self,
        principal_id: &str,
        cutoff_ms: u64,
        horizon_start_ms: Option<u64>,
    ) -> StorageResult<cc_lb_storage_api::CacheKeepaliveSummaryAggregate> {
        let horizon_start_ms = horizon_start_ms
            .map(|value| query_u64_to_i64(value, "cache_keepalive_summary_horizon_start_ms", "value exceeds i64::MAX"))
            .transpose()?;
        let cutoff_ms = query_u64_to_i64(
            cutoff_ms,
            "cache_keepalive_summary_cutoff_ms",
            "value exceeds i64::MAX",
        )?;
        let row = sqlx::query(
            "WITH active_current_turns AS (
                SELECT s.principal_id, s.session_key_hash, s.cycle_id, t.source_ref_id,
                       t.upstream_id, t.model,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.principal_id, s.session_key_hash
                           ORDER BY t.ts DESC, t.source_ref_id ASC
                       ) AS rn
                FROM cache_keepalive_sessions s
                JOIN cache_keepalive_turns t
                  ON t.principal_id = s.principal_id
                 AND t.session_key_hash = s.session_key_hash
                 AND (
                     t.cycle_id = s.cycle_id
                     OR (
                         t.cycle_id IS NULL
                         AND EXISTS (
                             SELECT 1
                             FROM cache_keepalive_evidence e
                             WHERE e.principal_id = t.principal_id
                               AND e.evidence_kind = 'renewal'
                               AND e.event_ref = t.source_ref_id
                               AND e.upstream_id = t.upstream_id
                               AND e.model = t.model
                               AND (e.cycle_id IS NULL OR e.cycle_id = s.cycle_id)
                         )
                     )
                 )
                WHERE s.principal_id = $1 AND s.status = 'active' AND s.cycle_id IS NOT NULL
                  AND ($14 IS NULL OR t.ts * 1000 >= $14)
            ),
            current_renewals AS (
                SELECT a.principal_id, a.session_key_hash, a.source_ref_id,
                       e.cycle_id, e.upstream_id, e.model
                FROM active_current_turns a
                JOIN cache_keepalive_evidence e
                  ON e.principal_id = a.principal_id
                 AND e.evidence_kind = 'renewal'
                 AND e.event_ref = a.source_ref_id
                 AND e.upstream_id = a.upstream_id
                 AND e.model = a.model
                 AND e.cycle_id = a.cycle_id
                 AND e.prefix_hash IS NOT NULL
                 AND e.prefix_policy_version IS NOT NULL
                 AND e.prefix_schema_version IS NOT NULL
                WHERE a.rn = 1
            ),
            attributed_follow_ups AS (
                SELECT a.principal_id, a.session_key_hash, r.event_ref,
                       ROW_NUMBER() OVER (
                           PARTITION BY a.principal_id, a.session_key_hash
                           ORDER BY f.request_start_ms ASC, f.evidence_id ASC,
                                    r.request_start_ms DESC, r.evidence_id DESC
                       ) AS rn
                FROM current_renewals a
                JOIN cache_keepalive_evidence r
                  ON r.principal_id = a.principal_id
                 AND r.evidence_kind = 'renewal'
                 AND r.cycle_id = a.cycle_id
                 AND r.upstream_id = a.upstream_id
                 AND r.model = a.model
                 AND r.prefix_hash IS NOT NULL
                 AND r.prefix_policy_version IS NOT NULL
                 AND r.prefix_schema_version IS NOT NULL
                JOIN cache_keepalive_evidence f
                  ON f.principal_id = r.principal_id
                 AND f.evidence_kind = 'follow_up'
                 AND f.upstream_id = r.upstream_id
                 AND f.model = r.model
                 AND COALESCE(f.cache_read_input_tokens, 0) > 0
                 AND f.prefix_hash = r.prefix_hash
                 AND f.request_start_ms > r.request_start_ms
                 AND f.request_start_ms <= r.request_start_ms
                     + CASE r.ttl WHEN '1h' THEN 3600000 ELSE 300000 END
            ),
            pending_current AS (
                SELECT a.principal_id, a.source_ref_id
                FROM current_renewals a
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM attributed_follow_ups f
                    WHERE f.principal_id = a.principal_id
                      AND f.session_key_hash = a.session_key_hash
                      AND f.rn = 1
                      AND f.event_ref = a.source_ref_id
                )
            )
            SELECT
                (SELECT COUNT(*) FROM cache_keepalive_sessions WHERE principal_id = $2 AND status = 'active') AS renewing_now,
                ((SELECT COUNT(*) FROM cache_keepalive_sessions WHERE principal_id = $3 AND last_message_at_ms >= $4)
                 + (SELECT COUNT(*) FROM cache_keepalive_decisions WHERE principal_id = $5 AND last_message_at_ms >= $6 AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id))) AS sessions_last_5m,
                (SELECT COUNT(*) FROM cache_keepalive_decisions WHERE principal_id = $7 AND last_message_at_ms >= $8 AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id)) AS recent_decisions,
                (SELECT COALESCE(SUM(cost_micros), 0)::BIGINT FROM cache_keepalive_turns WHERE principal_id = $9 AND ($14 IS NULL OR ts * 1000 >= $14)) AS total_spent_micros,
                (SELECT COUNT(*) FROM cache_keepalive_turns WHERE principal_id = $10 AND lower(hit_miss) = 'hit' AND ($14 IS NULL OR ts * 1000 >= $14)) AS successful_hits,
                (SELECT COUNT(*) FROM pending_current WHERE principal_id = $11) AS pending_evidence,
                (SELECT COUNT(*) FROM cache_keepalive_turns t
                 WHERE t.principal_id = $12
                   AND ($14 IS NULL OR t.ts * 1000 >= $14)
                   AND NOT EXISTS (
                       SELECT 1 FROM cache_keepalive_evidence e
                       WHERE e.principal_id = t.principal_id
                         AND e.evidence_kind = 'renewal'
                         AND e.event_ref = t.source_ref_id
                   )) +
                COALESCE(SUM(CASE WHEN e_summary.cycle_id IS NULL OR e_summary.prefix_hash IS NULL OR e_summary.prefix_policy_version IS NULL OR e_summary.prefix_schema_version IS NULL THEN 1 ELSE 0 END), 0) AS unknown_cycles,
                COALESCE(SUM(CASE WHEN e_summary.cycle_id IS NOT NULL AND e_summary.prefix_hash IS NOT NULL AND e_summary.prefix_policy_version IS NOT NULL AND e_summary.prefix_schema_version IS NOT NULL THEN 1 ELSE 0 END), 0) AS observed_evidence,
                MIN(e_summary.request_start_ms) AS retained_from_ms
             FROM cache_keepalive_evidence e_summary
             WHERE e_summary.principal_id = $13
               AND e_summary.evidence_kind = 'renewal'
               AND ($14 IS NULL OR e_summary.request_start_ms >= $14 OR EXISTS (
                   SELECT 1 FROM cache_keepalive_turns t
                   WHERE t.principal_id = e_summary.principal_id
                     AND t.source_ref_id = e_summary.event_ref
                     AND t.ts * 1000 >= $14
               ))",
        )
        .bind(principal_id)
        .bind(principal_id)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(principal_id)
        .bind(principal_id)
        .bind(principal_id)
        .bind(principal_id)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let pending_evidence: u64 = row
            .try_get::<i64, _>("pending_evidence")
            .map_err(map_sqlx_error)?
            .try_into()
            .map_err(|_| StorageError::Corrupted {
                message: "negative cache keepalive pending evidence count".to_owned(),
            })?;
        let unknown_cycles: u64 = row
            .try_get::<i64, _>("unknown_cycles")
            .map_err(map_sqlx_error)?
            .try_into()
            .map_err(|_| StorageError::Corrupted {
                message: "negative cache keepalive unknown cycle count".to_owned(),
            })?;
        let observed_evidence: u64 = row
            .try_get::<i64, _>("observed_evidence")
            .map_err(map_sqlx_error)?
            .try_into()
            .map_err(|_| StorageError::Corrupted {
                message: "negative cache keepalive observed evidence count".to_owned(),
            })?;
        let successful_hits: u64 = row
            .try_get::<i64, _>("successful_hits")
            .map_err(map_sqlx_error)?
            .try_into()
            .map_err(|_| StorageError::Corrupted {
                message: "negative cache keepalive successful hit count".to_owned(),
            })?;
        let group_rows = sqlx::query(
"WITH renewals AS (
                SELECT *
                FROM cache_keepalive_evidence
                WHERE principal_id = $1 AND evidence_kind = 'renewal' AND cycle_id IS NOT NULL
                  AND ($2 IS NULL OR request_start_ms >= $2 OR EXISTS (
                      SELECT 1 FROM cache_keepalive_turns t
                      WHERE t.principal_id = cache_keepalive_evidence.principal_id
                        AND t.source_ref_id = cache_keepalive_evidence.event_ref
                        AND t.ts * 1000 >= $2
                  ))
             ),
             qualifying_follow_ups AS (
                SELECT r.cycle_id, r.principal_id, r.upstream_id, r.model,
                       f.prefix_hash,
                       f.request_start_ms AS follow_up_request_start_ms,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.cycle_id
                           ORDER BY f.request_start_ms ASC, f.evidence_id ASC
                       ) AS follow_up_rn
                FROM renewals r
                JOIN cache_keepalive_evidence f
                  ON f.principal_id = r.principal_id
                 AND f.evidence_kind = 'follow_up'
                 AND f.upstream_id = r.upstream_id
                 AND f.model = r.model
                 AND COALESCE(f.cache_read_input_tokens, 0) > 0
                 AND f.prefix_hash = r.prefix_hash
                 AND f.request_start_ms > r.request_start_ms
                 AND f.request_start_ms <= r.request_start_ms
                     + CASE r.ttl WHEN '1h' THEN 3600000 ELSE 300000 END
             ),
             selected_follow_ups AS (
                SELECT cycle_id, principal_id, upstream_id, model, prefix_hash,
                       follow_up_request_start_ms
                FROM qualifying_follow_ups
                WHERE follow_up_rn = 1
             ),
             proven AS (
                SELECT r.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.cycle_id
                           ORDER BY r.request_start_ms DESC, r.evidence_id DESC
                       ) AS rn
                FROM renewals r
                JOIN selected_follow_ups f
                  ON f.cycle_id = r.cycle_id
                 AND f.principal_id = r.principal_id
                 AND f.upstream_id = r.upstream_id
                 AND f.model = r.model
                 AND f.prefix_hash = r.prefix_hash
                 AND f.follow_up_request_start_ms > r.request_start_ms
                 AND f.follow_up_request_start_ms <= r.request_start_ms
                     + CASE r.ttl WHEN '1h' THEN 3600000 ELSE 300000 END
             )
             SELECT r.model, r.ttl, COALESCE(SUM(r.cache_read_input_tokens), 0)::BIGINT AS avoided_cache_read_tokens,
                    COUNT(*) AS observed_cycles
             FROM proven r
             WHERE r.rn = 1 AND lower(r.hit_miss) = 'hit'
             GROUP BY r.model, r.ttl
             ORDER BY r.model, r.ttl",
        )
        .bind(principal_id)
        .bind(horizon_start_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        let mut observed_cycles = 0_u64;
        let groups = group_rows
            .into_iter()
            .map(|row| {
                observed_cycles = observed_cycles.saturating_add(
                    u64::try_from(
                        row.try_get::<i64, _>("observed_cycles")
                            .map_err(map_sqlx_error)?,
                    )
                    .map_err(|_| StorageError::Corrupted {
                        message: "negative cache keepalive observed cycle count".to_owned(),
                    })?,
                );
                Ok(cc_lb_storage_api::CacheKeepaliveSummaryGroup {
                    model: row.try_get("model").map_err(map_sqlx_error)?,
                    ttl: cc_lb_storage_api::CacheTtl::from_ttl_str(Some(
                        &row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?,
                    )),
                    avoided_cache_read_tokens: u64::try_from(
                        row.try_get::<i64, _>("avoided_cache_read_tokens")
                            .map_err(map_sqlx_error)?,
                    )
                    .map_err(|_| StorageError::Corrupted {
                        message: "negative cache keepalive avoided token count".to_owned(),
                    })?,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(cc_lb_storage_api::CacheKeepaliveSummaryAggregate {
            renewing_now: row
                .try_get::<i64, _>("renewing_now")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive renewing_now".to_owned(),
                })?,
            sessions_last_5m: row
                .try_get::<i64, _>("sessions_last_5m")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive sessions_last_5m".to_owned(),
                })?,
            recent_decisions: row
                .try_get::<i64, _>("recent_decisions")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive recent decisions".to_owned(),
                })?,
            total_spent_micros: row.try_get("total_spent_micros").map_err(map_sqlx_error)?,
            successful_hits,
            unknown_cycles,
            retained_from_ms: row
                .try_get::<Option<i64>, _>("retained_from_ms")
                .map_err(map_sqlx_error)?
                .map(|value| {
                    u64::try_from(value).map_err(|_| StorageError::Corrupted {
                        message: "negative cache keepalive retained_from_ms".to_owned(),
                    })
                })
                .transpose()?,
            evidence_status: if unknown_cycles > 0 {
                "unknown".to_owned()
            } else if pending_evidence > 0 {
                "pending".to_owned()
            } else if observed_evidence > 0 {
                "observed".to_owned()
            } else {
                "none".to_owned()
            },
            groups,
        })
    }
}

fn postgres_evidence_from_row(
    row: sqlx::postgres::PgRow,
) -> StorageResult<cc_lb_storage_api::CacheKeepaliveEvidenceRecord> {
    use sqlx::Row;
    Ok(cc_lb_storage_api::CacheKeepaliveEvidenceRecord {
        evidence_id: row.try_get("evidence_id").map_err(map_sqlx_error)?,
        cycle_id: row.try_get("cycle_id").map_err(map_sqlx_error)?,
        evidence_kind: row.try_get("evidence_kind").map_err(map_sqlx_error)?,
        request_start_ms: u64::try_from(
            row.try_get::<i64, _>("request_start_ms")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|_| StorageError::Corrupted {
            message: "negative cache keepalive evidence request_start_ms".to_owned(),
        })?,
        prefix_fingerprint: row.try_get("prefix_fingerprint").map_err(map_sqlx_error)?,
        prefix_hash: row.try_get("prefix_hash").map_err(map_sqlx_error)?,
        prefix_policy_version: row
            .try_get("prefix_policy_version")
            .map_err(map_sqlx_error)?,
        prefix_schema_version: row
            .try_get("prefix_schema_version")
            .map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        ttl: cc_lb_storage_api::CacheTtl::from_ttl_str(Some(
            &row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?,
        )),
        hit_miss: row.try_get("hit_miss").map_err(map_sqlx_error)?,
        cache_read_input_tokens: row
            .try_get::<Option<i64>, _>("cache_read_input_tokens")
            .map_err(map_sqlx_error)?
            .map(|value| {
                u64::try_from(value).map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive evidence cache_read_input_tokens".to_owned(),
                })
            })
            .transpose()?,
        cost_micros: row.try_get("cost_micros").map_err(map_sqlx_error)?,
        request_ref: row.try_get("request_ref").map_err(map_sqlx_error)?,
        event_ref: row.try_get("event_ref").map_err(map_sqlx_error)?,
    })
}
