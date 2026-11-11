use super::{
    cache_keepalive_session_read_row::{decision_from_row, list_item_from_row, turn_from_row},
    cache_keepalive_sessions::row_to_record,
};
use crate::{SqliteStorage, map_sqlx_error};
use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRecord, CacheKeepaliveSessionCursor, CacheKeepaliveSessionFilter,
    CacheKeepaliveSessionListQuery, CacheKeepaliveSessionPage, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSummaryInput, CacheKeepaliveTurnRecord, StorageError, StorageResult,
};
use sqlx::{QueryBuilder, Row, Sqlite};

// Stay below SQLite's historical 999-variable default while reserving one bind
// for principal_id. Rust byte order matches the column's default BINARY collation,
// so sorted input makes concatenated chunk results globally ordered.
const SESSION_HASH_BATCH_SIZE: usize = 900;
const EVIDENCE_ID_BATCH_SIZE: usize = 4_000;

// The timestamp distinguishes provisional markers from legacy final cancellations.
const REQUEST_MARKER_SQL: &str = "(status = 'terminal' AND terminal_reason = 'cancelled' AND COALESCE(cancel_reason, '') = 'new_request' AND running_since_unix_secs IS NOT NULL)";
const REQUEST_FRESH_SQL: &str = "(status = 'terminal' AND terminal_reason = 'cancelled' AND COALESCE(cancel_reason, '') = 'new_request' AND running_since_unix_secs IS NOT NULL AND run_at > CAST(strftime('%s', 'now') AS INTEGER))";
const REQUEST_INCOMPLETE_SQL: &str = "(status = 'terminal' AND terminal_reason = 'cancelled' AND COALESCE(cancel_reason, '') = 'new_request' AND running_since_unix_secs IS NOT NULL AND run_at <= CAST(strftime('%s', 'now') AS INTEGER))";

const SESSION_ENTRY_SELECT: &str = "
SELECT
    'session' AS entry_source,
    'session:' || session_key_hash AS entry_id,
    session_key_hash,
    principal_id,
    upstream_id,
    cycle_id,
    last_message_at_ms,
    ttl,
    generation,
    refresh_count,
    CASE WHEN status = 'terminal' AND terminal_reason = 'cancelled'
          AND cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL
          AND run_at > CAST(strftime('%s', 'now') AS INTEGER)
         THEN 'in_request' ELSE status END AS status,
    CASE WHEN status = 'terminal' AND terminal_reason = 'cancelled'
          AND cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL
         THEN NULL ELSE enqueue_state END AS enqueue_state,
    CASE WHEN status = 'terminal' AND terminal_reason = 'cancelled'
          AND cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL
         THEN CASE WHEN run_at > CAST(strftime('%s', 'now') AS INTEGER) THEN NULL ELSE 'stale' END
         ELSE terminal_reason END AS terminal_reason,
    CASE WHEN status = 'terminal' AND terminal_reason = 'cancelled'
          AND cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL
         THEN NULL ELSE cancel_reason END AS cancel_reason,
    (status = 'terminal' AND terminal_reason = 'cancelled'
     AND COALESCE(cancel_reason, '') = 'new_request' AND running_since_unix_secs IS NOT NULL
     AND run_at <= CAST(strftime('%s', 'now') AS INTEGER)) AS request_incomplete,
    NULL AS decision,
    CASE WHEN status = 'terminal' AND terminal_reason = 'cancelled'
          AND cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL
         THEN CASE WHEN run_at > CAST(strftime('%s', 'now') AS INTEGER)
                   THEN 'In request'
                   ELSE 'Request did not complete before its deadline' END
         ELSE CASE terminal_reason
        WHEN 'cancelled' THEN CASE cancel_reason
            WHEN 'new_request' THEN 'Cancelled: a new request arrived'
            WHEN 'user_turn_detected' THEN 'Cancelled: the agent is waiting for the user'
            WHEN 'snapshot_too_large' THEN 'Cancelled: the request snapshot is too large'
            WHEN 'cached_prefix_too_small' THEN 'Cancelled: the cached prefix is too small'
            WHEN 'zero_token_renewal_unsupported' THEN 'Cancelled: zero-token renewals are unsupported'
            WHEN 'ambiguous_turn' THEN 'Cancelled: the agent turn could not be determined'
            WHEN 'retention_recorded' THEN 'Retention recorded without renewal'
            WHEN 'renewal_not_beneficial' THEN 'Refresh expected value is unknown or non-positive'
            WHEN 'session_identity_changed' THEN 'Cancelled: the request completed under a different cache identity'
            WHEN 'completed_not_renewable' THEN 'Cancelled: request completed, renewal not possible'
            ELSE 'Cancelled'
        END
        WHEN 'max_refreshes' THEN 'max renewals reached'
        WHEN 'max_duration' THEN 'max duration reached'
        WHEN 'expired' THEN 'TTL expired before follow-up'
        WHEN 'dispatch_error' THEN 'renewal dispatch unavailable'
        WHEN 'cache_miss' THEN 'protected cache prefix was not fully preserved'
        WHEN 'decrypt_failed' THEN 'renewal snapshot could not be decrypted'
        WHEN 'unsupported_provider' THEN 'renewals are unsupported for this provider'
        WHEN 'stale' THEN 'renewal attempt became stale'
        ELSE CASE WHEN status = 'terminal' THEN 'Renewal stopped' ELSE display_reason END
    END END AS reason,
    display_reason,
    error,
    config_snapshot";

const SESSION_WITH_PURGED_TURNS_SQL: &str = concat!("
(
    SELECT
        session_key_hash,
        principal_id,
        upstream_id,
        cycle_id,
        last_message_at_ms,
        ttl,
        generation,
        refresh_count,
        status,
        enqueue_state,
        terminal_reason,
        cancel_reason,
        run_at,
        running_since_unix_secs,
        display_reason,
        error,
        config_snapshot
    FROM cache_keepalive_sessions
    UNION ALL
    SELECT
        t.session_key_hash,
        t.principal_id,
        (
            SELECT t2.upstream_id
            FROM cache_keepalive_turns t2
            WHERE t2.principal_id = t.principal_id
              AND t2.session_key_hash = t.session_key_hash
            ORDER BY t2.ts DESC, t2.source_ref_id ASC
            LIMIT 1
        ) AS upstream_id,
        (
            SELECT t2.cycle_id
            FROM cache_keepalive_turns t2
            WHERE t2.principal_id = t.principal_id
              AND t2.session_key_hash = t.session_key_hash
            ORDER BY t2.ts DESC, t2.source_ref_id ASC
            LIMIT 1
        ) AS cycle_id,
        MAX(t.ts) * 1000 AS last_message_at_ms,
        COALESCE((
            SELECT e.ttl
            FROM cache_keepalive_evidence e
            WHERE e.principal_id = t.principal_id
              AND e.event_ref = (
                  SELECT t2.source_ref_id
                  FROM cache_keepalive_turns t2
                  WHERE t2.principal_id = t.principal_id
                    AND t2.session_key_hash = t.session_key_hash
                  ORDER BY t2.ts DESC, t2.source_ref_id ASC
                  LIMIT 1
              )
              AND e.evidence_kind = 'renewal'
            ORDER BY e.request_start_ms DESC, e.evidence_id DESC
            LIMIT 1
        ), '5m') AS ttl,
        (
            SELECT COUNT(*)
            FROM cache_keepalive_turns tc
            WHERE tc.principal_id = t.principal_id
              AND tc.session_key_hash = t.session_key_hash
              AND tc.cycle_id IS (
                  SELECT t2.cycle_id
                  FROM cache_keepalive_turns t2
                  WHERE t2.principal_id = t.principal_id
                    AND t2.session_key_hash = t.session_key_hash
                  ORDER BY t2.ts DESC, t2.source_ref_id ASC
                  LIMIT 1
              )
        ) AS generation,
        (
            SELECT COUNT(*)
            FROM cache_keepalive_turns tc
            WHERE tc.principal_id = t.principal_id
              AND tc.session_key_hash = t.session_key_hash
              AND tc.cycle_id IS (
                  SELECT t2.cycle_id
                  FROM cache_keepalive_turns t2
                  WHERE t2.principal_id = t.principal_id
                    AND t2.session_key_hash = t.session_key_hash
                  ORDER BY t2.ts DESC, t2.source_ref_id ASC
                  LIMIT 1
              )
        ) AS refresh_count,
        'terminal' AS status,
        'pending' AS enqueue_state,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM cache_keepalive_evidence r
                JOIN cache_keepalive_evidence f
                  ON f.principal_id = r.principal_id
                 AND f.evidence_kind = 'follow_up'
                 AND f.upstream_id = r.upstream_id
                 AND f.model = r.model
                 AND COALESCE(f.cache_read_input_tokens, 0) > 0
                 AND f.prefix_hash = r.prefix_hash
                 AND f.request_start_ms > r.request_start_ms
                 AND f.request_start_ms <= r.request_start_ms + ",
    cc_lb_storage_api::cache_keepalive_ttl_millis_sql!("r.ttl"),
    "
                WHERE r.principal_id = t.principal_id
                  AND r.evidence_kind = 'renewal'
                  AND r.event_ref = (
                      SELECT t2.source_ref_id
                      FROM cache_keepalive_turns t2
                      WHERE t2.principal_id = t.principal_id
                        AND t2.session_key_hash = t.session_key_hash
                      ORDER BY t2.ts DESC, t2.source_ref_id ASC
                      LIMIT 1
                  )
                  AND r.cycle_id IS NOT NULL
                  AND r.prefix_hash IS NOT NULL
                  AND r.prefix_policy_version IS NOT NULL
                  AND r.prefix_schema_version IS NOT NULL
            ) THEN 'cancelled'
            WHEN (
                SELECT COUNT(*)
                FROM cache_keepalive_turns tc
                WHERE tc.principal_id = t.principal_id
                  AND tc.session_key_hash = t.session_key_hash
                  AND tc.cycle_id IS (
                      SELECT t2.cycle_id
                      FROM cache_keepalive_turns t2
                      WHERE t2.principal_id = t.principal_id
                        AND t2.session_key_hash = t.session_key_hash
                      ORDER BY t2.ts DESC, t2.source_ref_id ASC
                      LIMIT 1
                  )
            ) >= COALESCE((
                SELECT CAST(json_extract(d.config_snapshot, '$.max_refreshes_per_session') AS INTEGER)
                FROM cache_keepalive_decisions d
                WHERE d.principal_id = t.principal_id
                  AND d.source_ref_id = (
                      SELECT t2.source_ref_id
                      FROM cache_keepalive_turns t2
                      WHERE t2.principal_id = t.principal_id
                        AND t2.session_key_hash = t.session_key_hash
                      ORDER BY t2.ts DESC, t2.source_ref_id ASC
                      LIMIT 1
                  )
                  AND d.config_snapshot IS NOT NULL
                LIMIT 1
            ), 12) THEN 'max_refreshes'
            ELSE 'expired'
        END AS terminal_reason,
        NULL AS cancel_reason,
        NULL AS run_at,
        NULL AS running_since_unix_secs,
        COALESCE(
            (
                SELECT NULLIF(t2.display_reason, '')
                FROM cache_keepalive_turns t2
                WHERE t2.principal_id = t.principal_id
                  AND t2.session_key_hash = t.session_key_hash
                  AND t2.display_reason <> ''
                ORDER BY t2.ts DESC, t2.source_ref_id ASC
                LIMIT 1
            ),
            'agent-in-turn'
        ) AS display_reason,
        NULL AS error,
        (
            SELECT d.config_snapshot
            FROM cache_keepalive_decisions d
            WHERE d.principal_id = t.principal_id
              AND d.source_ref_id = (
                  SELECT t2.source_ref_id
                  FROM cache_keepalive_turns t2
                  WHERE t2.principal_id = t.principal_id
                    AND t2.session_key_hash = t.session_key_hash
                  ORDER BY t2.ts DESC, t2.source_ref_id ASC
                  LIMIT 1
              )
            LIMIT 1
        ) AS config_snapshot
    FROM cache_keepalive_turns t
    WHERE NOT EXISTS (
        SELECT 1
        FROM cache_keepalive_sessions s
        WHERE s.principal_id = t.principal_id
          AND s.session_key_hash = t.session_key_hash
    )
    AND EXISTS (
        SELECT 1
        FROM cache_keepalive_evidence e
        WHERE e.principal_id = t.principal_id
          AND e.event_ref = t.source_ref_id
          AND e.evidence_kind = 'renewal'
    )
    GROUP BY t.principal_id, t.session_key_hash
) AS cache_keepalive_sessions");

fn push_renewal_in_horizon(builder: &mut QueryBuilder<Sqlite>, horizon_start_ms: Option<i64>) {
    builder.push("EXISTS (SELECT 1 FROM cache_keepalive_turns turn_row WHERE turn_row.principal_id = cache_keepalive_sessions.principal_id AND turn_row.session_key_hash = cache_keepalive_sessions.session_key_hash");
    if let Some(start_ms) = horizon_start_ms {
        builder
            .push(" AND turn_row.ts * 1000 >= ")
            .push_bind(start_ms);
    }
    builder.push(")");
}

fn push_cancelled_without_turns(builder: &mut QueryBuilder<Sqlite>, horizon_start_ms: Option<i64>) {
    builder.push("(status = 'terminal' AND terminal_reason = 'cancelled' AND NOT ");
    push_renewal_in_horizon(builder, horizon_start_ms);
    builder.push(")");
}

fn push_session_entry_select(builder: &mut QueryBuilder<Sqlite>, horizon_start_ms: Option<i64>) {
    builder.push(SESSION_ENTRY_SELECT).push(", ");
    push_renewal_in_horizon(builder, horizon_start_ms);
    builder.push(" AS has_renewal_in_horizon");
}

const DECISION_ENTRY_SELECT: &str = "
SELECT
    'decision' AS entry_source,
    'decision:' || source_ref_id AS entry_id,
    session_key_hash,
    principal_id,
    upstream_id,
    CAST(NULL AS TEXT) AS cycle_id,
    last_message_at_ms,
    ttl,
    generation,
    NULL AS refresh_count,
    NULL AS status,
    NULL AS enqueue_state,
    NULL AS terminal_reason,
    NULL AS cancel_reason,
    0 AS request_incomplete,
    decision,
    reason,
    reason AS display_reason,
    error,
    config_snapshot,
    0 AS has_renewal_in_horizon
FROM cache_keepalive_decisions";

#[async_trait]
impl CacheKeepaliveSessionReadStore for SqliteStorage {
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
            .map(|value| {
                u64_to_i64(
                    value,
                    "cache_keepalive_horizon_start_ms",
                    "value exceeds i64::MAX",
                )
            })
            .transpose()?;
        let cursor_last_message_at_ms = query
            .cursor
            .as_ref()
            .map(|cursor| {
                u64_to_i64(
                    cursor.last_message_at_ms,
                    "cache_keepalive_session_cursor",
                    "last_message_at_ms exceeds i64::MAX",
                )
            })
            .transpose()?;
        let branch_limit = i64::from(query.limit.saturating_add(1));

        let include_sessions = true;
        let include_decisions = matches!(
            query.filter,
            CacheKeepaliveSessionFilter::All
                | CacheKeepaliveSessionFilter::Visible
                | CacheKeepaliveSessionFilter::NotTracked
                | CacheKeepaliveSessionFilter::Error
        );
        let mut builder = QueryBuilder::<Sqlite>::new("WITH ");
        if include_sessions {
            builder.push("session_entries AS (");
            push_session_branch(
                &mut builder,
                query,
                horizon_start_ms,
                cursor_last_message_at_ms,
                branch_limit,
            );
            builder.push(")");
        }
        if include_decisions {
            if include_sessions {
                builder.push(", ");
            }
            builder.push("decision_entries AS (");
            push_decision_branch(
                &mut builder,
                query,
                horizon_start_ms,
                cursor_last_message_at_ms,
                branch_limit,
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
        builder
            .push(") entries ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ")
            .push_bind(branch_limit);

        let rows = builder
            .build()
            .fetch_all(self.pool())
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
        let cutoff_ms = u64_to_i64(
            cutoff_ms,
            "cache_keepalive_summary_cutoff_ms",
            "value exceeds i64::MAX",
        )?;
        let mut sessions_query = QueryBuilder::<Sqlite>::new("");
        push_session_entry_select(&mut sessions_query, None);
        sessions_query
            .push(" FROM cache_keepalive_sessions WHERE principal_id = ")
            .push_bind(principal_id)
            .push(" ORDER BY last_message_at_ms DESC, entry_id ASC");
        let sessions = sessions_query
            .build()
            .fetch_all(self.pool())
            .await
            .map_err(map_sqlx_error)?
            .into_iter()
            .map(list_item_from_row)
            .collect::<StorageResult<Vec<_>>>()?;

        let recent_decisions = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM cache_keepalive_decisions
             WHERE principal_id = ?
               AND last_message_at_ms >= ?
               AND NOT EXISTS (
                   SELECT 1 FROM cache_keepalive_turns turn_row
                   WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id
               )",
        )
        .bind(principal_id)
        .bind(cutoff_ms)
        .fetch_one(self.pool())
        .await
        .map_err(map_sqlx_error)?;

        Ok(CacheKeepaliveSummaryInput {
            sessions,
            recent_decisions: i64_to_u64_count(recent_decisions)?,
        })
    }

    async fn get_cache_keepalive_list_item(
        &self,
        principal_id: &str,
        id: &str,
    ) -> StorageResult<Option<cc_lb_storage_api::CacheKeepaliveSessionListItem>> {
        let mut builder = QueryBuilder::<Sqlite>::new("WITH candidates AS (");
        push_session_entry_select(&mut builder, None);
        builder
            .push(" FROM ")
            .push(SESSION_WITH_PURGED_TURNS_SQL)
            .push(" WHERE principal_id = ")
            .push_bind(principal_id)
            .push(" AND session_key_hash = ")
            .push_bind(id)
            .push(" UNION ALL ")
            .push(DECISION_ENTRY_SELECT)
            .push(" WHERE principal_id = ")
            .push_bind(principal_id)
            .push(" AND source_ref_id = ")
            .push_bind(id)
            .push(
                " AND NOT EXISTS (
                    SELECT 1 FROM cache_keepalive_turns turn_row
                    WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id
                )
                )
                SELECT * FROM candidates
                ORDER BY last_message_at_ms DESC, entry_id ASC
                LIMIT 1",
            );
        builder
            .build()
            .fetch_optional(self.pool())
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
        let row = sqlx::query(
            "SELECT * FROM cache_keepalive_sessions WHERE principal_id = ? AND session_key_hash = ?",
        )
        .bind(principal_id)
        .bind(session_key_hash)
        .fetch_optional(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        row.map(row_to_record).transpose()
    }

    async fn list_cache_keepalive_turns(
        &self,
        principal_id: &str,
        session_key_hash: &str,
    ) -> StorageResult<Vec<CacheKeepaliveTurnRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM cache_keepalive_turns WHERE principal_id = ? AND session_key_hash = ? ORDER BY ts DESC, source_ref_id ASC",
        )
        .bind(principal_id)
        .bind(session_key_hash)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
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

        let mut session_key_hashes = session_key_hashes
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        session_key_hashes.sort_unstable();
        session_key_hashes.dedup();

        let mut turns = Vec::new();
        for batch in session_key_hashes.chunks(SESSION_HASH_BATCH_SIZE) {
            let mut query = QueryBuilder::<Sqlite>::new(
                "SELECT source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, cycle_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, cost_known, hit_miss, ts FROM cache_keepalive_turns WHERE principal_id = ",
            );
            query
                .push_bind(principal_id)
                .push(" AND session_key_hash IN (");
            {
                let mut hashes = query.separated(", ");
                for session_key_hash in batch {
                    hashes.push_bind(session_key_hash);
                }
                hashes
                    .push_unseparated(") ORDER BY session_key_hash ASC, ts ASC, source_ref_id ASC");
            }
            let rows = query
                .build()
                .fetch_all(self.pool())
                .await
                .map_err(map_sqlx_error)?;
            for row in rows {
                turns.push(turn_from_row(row)?);
            }
        }
        Ok(turns)
    }

    async fn get_cache_keepalive_decision_for_principal(
        &self,
        principal_id: &str,
        source_ref_id: &str,
    ) -> StorageResult<Option<CacheKeepaliveDecisionRecord>> {
        let row = sqlx::query(
            "SELECT * FROM cache_keepalive_decisions WHERE principal_id = ? AND source_ref_id = ?",
        )
        .bind(principal_id)
        .bind(source_ref_id)
        .fetch_optional(self.pool())
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
        // Both OR branches are unions of independent ids. Merge their matches
        // before deriving the shared follow-up time window and prefix scope.
        let mut evidence = Vec::new();
        let batches = cycle_ids
            .len()
            .max(source_ref_ids.len())
            .div_ceil(EVIDENCE_ID_BATCH_SIZE);
        for batch in 0..batches {
            let start = batch * EVIDENCE_ID_BATCH_SIZE;
            let cycle_ids = &cycle_ids
                [start.min(cycle_ids.len())..(start + EVIDENCE_ID_BATCH_SIZE).min(cycle_ids.len())];
            let source_ref_ids = &source_ref_ids[start.min(source_ref_ids.len())
                ..(start + EVIDENCE_ID_BATCH_SIZE).min(source_ref_ids.len())];
            let mut renewal_query = QueryBuilder::<Sqlite>::new(
                "SELECT * FROM cache_keepalive_evidence WHERE principal_id = ",
            );
            renewal_query.push_bind(principal_id);
            renewal_query.push(" AND (");
            let mut has_clause = false;
            if !cycle_ids.is_empty() {
                renewal_query.push("cycle_id IN (");
                let mut separated = renewal_query.separated(", ");
                for cycle_id in cycle_ids {
                    separated.push_bind(cycle_id.to_string());
                }
                separated.push_unseparated(")");
                has_clause = true;
            }
            if !source_ref_ids.is_empty() {
                if has_clause {
                    renewal_query.push(" OR ");
                }
                renewal_query.push("event_ref IN (");
                let mut separated = renewal_query.separated(", ");
                for source_ref_id in source_ref_ids {
                    separated.push_bind(source_ref_id);
                }
                separated.push_unseparated(")");
            }
            renewal_query.push(") ORDER BY request_start_ms ASC, evidence_id ASC");
            let renewal_rows = renewal_query
                .build()
                .fetch_all(self.pool())
                .await
                .map_err(map_sqlx_error)?;
            evidence.extend(
                renewal_rows
                    .into_iter()
                    .map(sqlite_evidence_from_row)
                    .collect::<StorageResult<Vec<_>>>()?,
            );
        }
        let mut seen = std::collections::HashSet::new();
        evidence.retain(|entry| seen.insert(entry.evidence_id.clone()));
        evidence.sort_unstable_by(|left, right| {
            left.request_start_ms
                .cmp(&right.request_start_ms)
                .then_with(|| left.evidence_id.cmp(&right.evidence_id))
        });
        let prefix_hashes = evidence
            .iter()
            .filter_map(|entry| entry.prefix_hash.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
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
        for prefix_hashes in prefix_hashes.chunks(EVIDENCE_ID_BATCH_SIZE) {
            let mut follow_up_query = QueryBuilder::<Sqlite>::new(
                "SELECT * FROM cache_keepalive_evidence \
             WHERE principal_id = ",
            );
            follow_up_query.push_bind(principal_id);
            follow_up_query.push(" AND evidence_kind = 'follow_up' AND prefix_hash IN (");
            let mut separated = follow_up_query.separated(", ");
            for prefix_hash in prefix_hashes {
                separated.push_bind(prefix_hash);
            }
            separated.push_unseparated(") AND request_start_ms > ");
            follow_up_query.push_bind(u64_to_i64(
                follow_up_from_ms,
                "cache_keepalive_follow_up_from_ms",
                "value exceeds i64::MAX",
            )?);
            follow_up_query.push(" AND request_start_ms <= ");
            follow_up_query.push_bind(u64_to_i64(
                follow_up_until_ms,
                "cache_keepalive_follow_up_until_ms",
                "value exceeds i64::MAX",
            )?);
            follow_up_query.push(" ORDER BY request_start_ms ASC, evidence_id ASC");
            let follow_up_rows = follow_up_query
                .build()
                .fetch_all(self.pool())
                .await
                .map_err(map_sqlx_error)?;
            evidence.extend(
                follow_up_rows
                    .into_iter()
                    .map(sqlite_evidence_from_row)
                    .collect::<StorageResult<Vec<_>>>()?,
            );
        }
        seen.clear();
        evidence.retain(|entry| seen.insert(entry.evidence_id.clone()));
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
            .map(|value| {
                u64_to_i64(
                    value,
                    "cache_keepalive_summary_horizon_start_ms",
                    "value exceeds i64::MAX",
                )
            })
            .transpose()?;
        let cutoff_ms = u64_to_i64(
            cutoff_ms,
            "cache_keepalive_summary_cutoff_ms",
            "value exceeds i64::MAX",
        )?;
        let row = sqlx::query(concat!(
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
                WHERE s.principal_id = ? AND s.status = 'active' AND s.cycle_id IS NOT NULL
                  AND (? IS NULL OR t.ts * 1000 >= ?)
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
                     + ",
            cc_lb_storage_api::cache_keepalive_ttl_millis_sql!("r.ttl"),
            "
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
                (SELECT COUNT(*) FROM cache_keepalive_sessions WHERE principal_id = ? AND status = 'active') AS renewing_now,
                ((SELECT COUNT(*) FROM cache_keepalive_sessions WHERE principal_id = ? AND last_message_at_ms >= ?
                    AND (status <> 'terminal' OR COALESCE(terminal_reason, '') <> 'cancelled'
                        OR (cancel_reason = 'new_request' AND running_since_unix_secs IS NOT NULL AND run_at > CAST(strftime('%s', 'now') AS INTEGER))
                        OR EXISTS (
                        SELECT 1 FROM cache_keepalive_turns t_recent
                        WHERE t_recent.principal_id = cache_keepalive_sessions.principal_id
                          AND t_recent.session_key_hash = cache_keepalive_sessions.session_key_hash
                          AND (? IS NULL OR t_recent.ts * 1000 >= ?))))
                 + (SELECT COUNT(*) FROM cache_keepalive_decisions WHERE principal_id = ? AND last_message_at_ms >= ? AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id))) AS sessions_last_5m,
                (SELECT COUNT(*) FROM cache_keepalive_decisions WHERE principal_id = ? AND last_message_at_ms >= ? AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns WHERE source_ref_id = cache_keepalive_decisions.source_ref_id)) AS recent_decisions,
                (SELECT COALESCE(SUM(CASE WHEN cost_known = 1 THEN cost_micros ELSE 0 END), 0) FROM cache_keepalive_turns WHERE principal_id = ? AND (? IS NULL OR ts * 1000 >= ?)) AS total_spent_micros,
                (SELECT COUNT(*) FROM cache_keepalive_turns WHERE principal_id = ? AND lower(hit_miss) = 'hit' AND (? IS NULL OR ts * 1000 >= ?)) AS successful_hits,
                (SELECT COUNT(*) FROM pending_current WHERE principal_id = ?) AS pending_evidence,
                (SELECT COUNT(*) FROM cache_keepalive_turns t
                 WHERE t.principal_id = ?
                   AND (? IS NULL OR t.ts * 1000 >= ?)
                   AND NOT EXISTS (
                       SELECT 1 FROM cache_keepalive_evidence e
                       WHERE e.principal_id = t.principal_id
                         AND e.evidence_kind = 'renewal'
                         AND e.event_ref = t.source_ref_id
                   )) +
                COALESCE(SUM(CASE WHEN e_summary.cycle_id IS NULL OR e_summary.prefix_hash IS NULL OR e_summary.prefix_policy_version IS NULL OR e_summary.prefix_schema_version IS NULL THEN 1 ELSE 0 END), 0) AS unknown_cycles,
                COALESCE(SUM(CASE WHEN e_summary.cycle_id IS NOT NULL AND e_summary.prefix_hash IS NOT NULL AND e_summary.prefix_policy_version IS NOT NULL AND e_summary.prefix_schema_version IS NOT NULL THEN 1 ELSE 0 END), 0) AS observed_evidence,
                MIN(e_summary.request_start_ms) AS retained_from_ms,
                (SELECT COUNT(*) FROM cache_keepalive_turns t_cost
                 WHERE t_cost.principal_id = ?
                   AND (? IS NULL OR t_cost.ts * 1000 >= ?)
                   AND cost_known = 0) AS unknown_costs,
                (SELECT COUNT(DISTINCT t_unlisted.session_key_hash) FROM cache_keepalive_turns t_unlisted
                 WHERE t_unlisted.principal_id = ?
                   AND (? IS NULL OR t_unlisted.ts * 1000 >= ?)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_sessions s_unlisted WHERE s_unlisted.principal_id = t_unlisted.principal_id AND s_unlisted.session_key_hash = t_unlisted.session_key_hash)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t_evidence JOIN cache_keepalive_evidence e_unlisted ON e_unlisted.principal_id = t_evidence.principal_id AND e_unlisted.evidence_kind = 'renewal' AND e_unlisted.event_ref = t_evidence.source_ref_id WHERE t_evidence.principal_id = t_unlisted.principal_id AND t_evidence.session_key_hash = t_unlisted.session_key_hash)) AS unlisted_sessions,
                (SELECT COUNT(*) FROM cache_keepalive_turns t_unlisted
                 WHERE t_unlisted.principal_id = ?
                   AND (? IS NULL OR t_unlisted.ts * 1000 >= ?)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_sessions s_unlisted WHERE s_unlisted.principal_id = t_unlisted.principal_id AND s_unlisted.session_key_hash = t_unlisted.session_key_hash)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t_evidence JOIN cache_keepalive_evidence e_unlisted ON e_unlisted.principal_id = t_evidence.principal_id AND e_unlisted.evidence_kind = 'renewal' AND e_unlisted.event_ref = t_evidence.source_ref_id WHERE t_evidence.principal_id = t_unlisted.principal_id AND t_evidence.session_key_hash = t_unlisted.session_key_hash)) AS unlisted_renewals,
                (SELECT COALESCE(SUM(CASE WHEN t_unlisted.cost_known = 1 THEN t_unlisted.cost_micros ELSE 0 END), 0) FROM cache_keepalive_turns t_unlisted
                 WHERE t_unlisted.principal_id = ?
                   AND (? IS NULL OR t_unlisted.ts * 1000 >= ?)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_sessions s_unlisted WHERE s_unlisted.principal_id = t_unlisted.principal_id AND s_unlisted.session_key_hash = t_unlisted.session_key_hash)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t_evidence JOIN cache_keepalive_evidence e_unlisted ON e_unlisted.principal_id = t_evidence.principal_id AND e_unlisted.evidence_kind = 'renewal' AND e_unlisted.event_ref = t_evidence.source_ref_id WHERE t_evidence.principal_id = t_unlisted.principal_id AND t_evidence.session_key_hash = t_unlisted.session_key_hash)) AS unlisted_spent_micros,
                (SELECT COUNT(*) FROM cache_keepalive_turns t_unlisted
                 WHERE t_unlisted.principal_id = ?
                   AND (? IS NULL OR t_unlisted.ts * 1000 >= ?)
                   AND t_unlisted.cost_known = 0
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_sessions s_unlisted WHERE s_unlisted.principal_id = t_unlisted.principal_id AND s_unlisted.session_key_hash = t_unlisted.session_key_hash)
                   AND NOT EXISTS (SELECT 1 FROM cache_keepalive_turns t_evidence JOIN cache_keepalive_evidence e_unlisted ON e_unlisted.principal_id = t_evidence.principal_id AND e_unlisted.evidence_kind = 'renewal' AND e_unlisted.event_ref = t_evidence.source_ref_id WHERE t_evidence.principal_id = t_unlisted.principal_id AND t_evidence.session_key_hash = t_unlisted.session_key_hash)) AS unlisted_unknown_costs
             FROM cache_keepalive_evidence e_summary
             WHERE e_summary.principal_id = ?
               AND e_summary.evidence_kind = 'renewal'
               AND (? IS NULL OR e_summary.request_start_ms >= ? OR EXISTS (
                   SELECT 1 FROM cache_keepalive_turns t
                   WHERE t.principal_id = e_summary.principal_id
                     AND t.source_ref_id = e_summary.event_ref
                     AND t.ts * 1000 >= ?
               ))",
        ))
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(principal_id)
        .bind(cutoff_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .fetch_one(self.pool())
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
        let group_rows = sqlx::query(concat!(
"WITH renewals AS (
                SELECT *
                FROM cache_keepalive_evidence
                WHERE principal_id = ? AND evidence_kind = 'renewal' AND cycle_id IS NOT NULL
                  AND (? IS NULL OR request_start_ms >= ? OR EXISTS (
                      SELECT 1 FROM cache_keepalive_turns t
                      WHERE t.principal_id = cache_keepalive_evidence.principal_id
                        AND t.source_ref_id = cache_keepalive_evidence.event_ref
                        AND t.ts * 1000 >= ?
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
                     + ",
            cc_lb_storage_api::cache_keepalive_ttl_millis_sql!("r.ttl"),
            "
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
                     + ",
            cc_lb_storage_api::cache_keepalive_ttl_millis_sql!("r.ttl"),
            "
             )
             SELECT r.model, r.ttl, COALESCE(SUM(r.cache_read_input_tokens), 0) AS avoided_cache_read_tokens,
                    COUNT(*) AS observed_cycles
             FROM proven r
             WHERE r.rn = 1 AND lower(r.hit_miss) = 'hit'
             GROUP BY r.model, r.ttl, r.cache_read_input_tokens
             ORDER BY r.model, r.ttl, r.cache_read_input_tokens",
        ))
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        let mut observed_cycles = 0_u64;
        let mut groups = group_rows
            .into_iter()
            .map(|row| {
                let renewals =
                    i64_to_u64_count(row.try_get("observed_cycles").map_err(map_sqlx_error)?)?;
                observed_cycles = observed_cycles.saturating_add(renewals);
                Ok(cc_lb_storage_api::CacheKeepaliveSummaryGroup {
                    model: row.try_get("model").map_err(map_sqlx_error)?,
                    ttl: ttl_from_evidence_db(
                        &row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?,
                    )?,
                    avoided_cache_read_tokens: i64_to_u64_count(
                        row.try_get("avoided_cache_read_tokens")
                            .map_err(map_sqlx_error)?,
                    )?,
                    renewals,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let model_rows = sqlx::query(
            "SELECT DISTINCT t.model,
                COALESCE(
                    (SELECT e.ttl FROM cache_keepalive_evidence e
                     WHERE e.principal_id = t.principal_id AND e.event_ref = t.source_ref_id
                       AND e.evidence_kind = 'renewal' LIMIT 1),
                    (SELECT s.ttl FROM cache_keepalive_sessions s
                     WHERE s.principal_id = t.principal_id AND s.session_key_hash = t.session_key_hash),
                    '5m'
                ) AS ttl
             FROM cache_keepalive_turns t
             WHERE t.principal_id = ?
               AND (? IS NULL OR t.ts * 1000 >= ?)
             ORDER BY t.model, ttl",
        )
        .bind(principal_id)
        .bind(horizon_start_ms)
        .bind(horizon_start_ms)
        .fetch_all(self.pool())
        .await
        .map_err(map_sqlx_error)?;
        for model_row in model_rows {
            let model = model_row
                .try_get::<String, _>("model")
                .map_err(map_sqlx_error)?;
            let ttl = ttl_from_evidence_db(
                &model_row
                    .try_get::<String, _>("ttl")
                    .map_err(map_sqlx_error)?,
            )?;
            if !groups
                .iter()
                .any(|group| group.model == model && group.ttl == ttl)
            {
                groups.push(cc_lb_storage_api::CacheKeepaliveSummaryGroup {
                    model,
                    ttl,
                    avoided_cache_read_tokens: 0,
                    renewals: 0,
                });
            }
        }
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
            successful_hits: row
                .try_get::<i64, _>("successful_hits")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive successful hit count".to_owned(),
                })?,
            unknown_cycles,
            unknown_costs: row
                .try_get::<i64, _>("unknown_costs")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive unknown cost count".to_owned(),
                })?,
            unlisted_sessions: row
                .try_get::<i64, _>("unlisted_sessions")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive unlisted session count".to_owned(),
                })?,
            unlisted_renewals: row
                .try_get::<i64, _>("unlisted_renewals")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive unlisted renewal count".to_owned(),
                })?,
            unlisted_spent_micros: row
                .try_get("unlisted_spent_micros")
                .map_err(map_sqlx_error)?,
            unlisted_unknown_costs: row
                .try_get::<i64, _>("unlisted_unknown_costs")
                .map_err(map_sqlx_error)?
                .try_into()
                .map_err(|_| StorageError::Corrupted {
                    message: "negative cache keepalive unlisted unknown cost count".to_owned(),
                })?,
            retained_from_ms: row
                .try_get::<Option<i64>, _>("retained_from_ms")
                .map_err(map_sqlx_error)?
                .map(i64_to_u64_count)
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

fn ttl_from_evidence_db(value: &str) -> StorageResult<cc_lb_storage_api::CacheTtl> {
    match value {
        "5m" => Ok(cc_lb_storage_api::CacheTtl::Ttl5m),
        "1h" => Ok(cc_lb_storage_api::CacheTtl::Ttl1h),
        "30m" => Ok(cc_lb_storage_api::CacheTtl::Ttl30m),
        "12h" => Ok(cc_lb_storage_api::CacheTtl::Ttl12h),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive evidence ttl {value}"),
        }),
    }
}

fn sqlite_evidence_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> StorageResult<cc_lb_storage_api::CacheKeepaliveEvidenceRecord> {
    use sqlx::Row;
    Ok(cc_lb_storage_api::CacheKeepaliveEvidenceRecord {
        evidence_id: row.try_get("evidence_id").map_err(map_sqlx_error)?,
        cycle_id: row
            .try_get::<Option<String>, _>("cycle_id")
            .map_err(map_sqlx_error)?
            .map(|value| {
                uuid::Uuid::parse_str(&value).map_err(|error| StorageError::Corrupted {
                    message: format!("invalid cache keepalive evidence cycle_id: {error}"),
                })
            })
            .transpose()?,
        evidence_kind: row.try_get("evidence_kind").map_err(map_sqlx_error)?,
        request_start_ms: i64_to_u64_count(
            row.try_get("request_start_ms").map_err(map_sqlx_error)?,
        )?,
        prefix_fingerprint: row.try_get("prefix_fingerprint").map_err(map_sqlx_error)?,
        prefix_hash: row.try_get("prefix_hash").map_err(map_sqlx_error)?,
        prefix_policy_version: row
            .try_get("prefix_policy_version")
            .map_err(map_sqlx_error)?,
        prefix_schema_version: row
            .try_get("prefix_schema_version")
            .map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id: uuid::Uuid::parse_str(
            &row.try_get::<String, _>("upstream_id")
                .map_err(map_sqlx_error)?,
        )
        .map_err(|error| StorageError::Corrupted {
            message: format!("invalid cache keepalive evidence upstream_id: {error}"),
        })?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        ttl: ttl_from_evidence_db(&row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?)?,
        hit_miss: row.try_get("hit_miss").map_err(map_sqlx_error)?,
        cache_read_input_tokens: row
            .try_get::<Option<i64>, _>("cache_read_input_tokens")
            .map_err(map_sqlx_error)?
            .map(i64_to_u64_count)
            .transpose()?,
        cost_micros: row.try_get("cost_micros").map_err(map_sqlx_error)?,
        request_ref: row.try_get("request_ref").map_err(map_sqlx_error)?,
        event_ref: row.try_get("event_ref").map_err(map_sqlx_error)?,
    })
}

fn push_session_branch(
    builder: &mut QueryBuilder<Sqlite>,
    query: &CacheKeepaliveSessionListQuery,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    limit: i64,
) {
    push_session_entry_select(builder, horizon_start_ms);
    builder
        .push(" FROM ")
        .push(SESSION_WITH_PURGED_TURNS_SQL)
        .push(" WHERE principal_id = ")
        .push_bind(&query.principal_id);
    if let Some(horizon_start_ms) = horizon_start_ms {
        builder.push(" AND (last_message_at_ms >= ");
        builder.push_bind(horizon_start_ms);
        builder.push(" OR EXISTS (SELECT 1 FROM cache_keepalive_turns h_turn WHERE h_turn.principal_id = cache_keepalive_sessions.principal_id AND h_turn.session_key_hash = cache_keepalive_sessions.session_key_hash AND h_turn.ts * 1000 >= ");
        builder.push_bind(horizon_start_ms);
        builder.push("))");
    }
    match query.filter {
        CacheKeepaliveSessionFilter::All | CacheKeepaliveSessionFilter::Error => {}
        CacheKeepaliveSessionFilter::Visible | CacheKeepaliveSessionFilter::Tracked => {
            builder.push(" AND (");
            builder.push(REQUEST_FRESH_SQL);
            builder.push(" OR NOT ");
            push_cancelled_without_turns(builder, horizon_start_ms);
            builder.push(")");
        }
        CacheKeepaliveSessionFilter::Renewed => {
            builder.push(" AND status = 'active' AND refresh_count > 0");
        }
        CacheKeepaliveSessionFilter::Scheduled => {
            builder.push(" AND status = 'active' AND refresh_count = 0");
        }
        CacheKeepaliveSessionFilter::InRequest => {
            builder.push(" AND ");
            builder.push(REQUEST_FRESH_SQL);
        }
        CacheKeepaliveSessionFilter::Capped => {
            builder.push(
                " AND status = 'terminal' AND terminal_reason IN ('max_refreshes', 'max_duration')",
            );
        }
        CacheKeepaliveSessionFilter::Cancelled => {
            builder.push(" AND status = 'terminal' AND terminal_reason = 'cancelled' AND NOT ");
            builder.push(REQUEST_MARKER_SQL);
            builder.push(" AND NOT ");
            push_cancelled_without_turns(builder, horizon_start_ms);
        }
        CacheKeepaliveSessionFilter::Failed => {
            builder.push(" AND status = 'terminal' AND terminal_reason IN ('dispatch_error', 'decrypt_failed', 'unsupported_provider', 'stale')");
            builder.push(" AND NOT ");
            builder.push(REQUEST_MARKER_SQL);
        }
        CacheKeepaliveSessionFilter::Expired => {
            builder.push(" AND (terminal_reason IN ('expired', 'cache_miss') OR ");
            builder.push(REQUEST_INCOMPLETE_SQL);
            builder.push(")");
        }
        CacheKeepaliveSessionFilter::NotTracked => {
            builder.push(" AND ");
            push_cancelled_without_turns(builder, horizon_start_ms);
            builder.push(" AND NOT ");
            builder.push(REQUEST_MARKER_SQL);
        }
    }
    if matches!(query.filter, CacheKeepaliveSessionFilter::Error) {
        builder.push(" AND error IS NOT NULL");
    }
    if let (Some(cursor), Some(cursor_last_message_at_ms)) =
        (query.cursor.as_ref(), cursor_last_message_at_ms)
    {
        builder
            .push(" AND last_message_at_ms <= ")
            .push_bind(cursor_last_message_at_ms)
            .push(" AND (last_message_at_ms < ")
            .push_bind(cursor_last_message_at_ms)
            .push(" OR (last_message_at_ms = ")
            .push_bind(cursor_last_message_at_ms)
            .push(" AND 'session:' || session_key_hash > ")
            .push_bind(cursor.entry_id.as_str())
            .push("))");
    }
    builder
        .push(" ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ")
        .push_bind(limit);
}

fn push_decision_branch(
    builder: &mut QueryBuilder<Sqlite>,
    query: &CacheKeepaliveSessionListQuery,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    limit: i64,
) {
    builder
        .push(DECISION_ENTRY_SELECT)
        .push(" WHERE principal_id = ")
        .push_bind(&query.principal_id)
        .push(
            " AND NOT EXISTS (
                SELECT 1 FROM cache_keepalive_turns turn_row
                WHERE turn_row.source_ref_id = cache_keepalive_decisions.source_ref_id
            )",
        );
    if let Some(horizon_start_ms) = horizon_start_ms {
        builder
            .push(" AND last_message_at_ms >= ")
            .push_bind(horizon_start_ms);
    }
    match query.filter {
        CacheKeepaliveSessionFilter::All => {}
        CacheKeepaliveSessionFilter::Visible => {
            builder.push(" AND (error IS NOT NULL OR ttl = '12h')");
        }
        CacheKeepaliveSessionFilter::NotTracked => {
            builder.push(" AND decision = 'not_tracked'");
        }
        CacheKeepaliveSessionFilter::Error => {
            builder.push(" AND error IS NOT NULL");
        }
        CacheKeepaliveSessionFilter::Tracked
        | CacheKeepaliveSessionFilter::Renewed
        | CacheKeepaliveSessionFilter::Scheduled
        | CacheKeepaliveSessionFilter::InRequest
        | CacheKeepaliveSessionFilter::Capped
        | CacheKeepaliveSessionFilter::Cancelled
        | CacheKeepaliveSessionFilter::Failed
        | CacheKeepaliveSessionFilter::Expired => unreachable!("decision branch is skipped"),
    }
    if let (Some(cursor), Some(cursor_last_message_at_ms)) =
        (query.cursor.as_ref(), cursor_last_message_at_ms)
    {
        builder
            .push(" AND last_message_at_ms <= ")
            .push_bind(cursor_last_message_at_ms)
            .push(" AND (last_message_at_ms < ")
            .push_bind(cursor_last_message_at_ms)
            .push(" OR (last_message_at_ms = ")
            .push_bind(cursor_last_message_at_ms)
            .push(" AND 'decision:' || source_ref_id > ")
            .push_bind(cursor.entry_id.as_str())
            .push("))");
    }
    builder
        .push(" ORDER BY last_message_at_ms DESC, entry_id ASC LIMIT ")
        .push_bind(limit);
}

fn u64_to_i64(value: u64, field: &str, reason: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: reason.to_owned(),
    })
}

fn i64_to_u64_count(value: i64) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: "negative cache keepalive recent decision count".to_owned(),
    })
}
