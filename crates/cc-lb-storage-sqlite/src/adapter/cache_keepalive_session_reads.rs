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
use sqlx::{QueryBuilder, Sqlite};

// Stay below SQLite's historical 999-variable default while reserving one bind
// for principal_id. Rust byte order matches the column's default BINARY collation,
// so sorted input makes concatenated chunk results globally ordered.
const SESSION_HASH_BATCH_SIZE: usize = 900;

const SESSION_ENTRY_SELECT: &str = "
SELECT
    'session' AS entry_source,
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
    NULL AS decision,
    CASE terminal_reason
        WHEN 'max_refreshes' THEN 'max renewals reached'
        WHEN 'max_duration' THEN 'max duration reached (4h)'
        WHEN 'expired' THEN 'TTL expired before follow-up'
        WHEN 'dispatch_error' THEN 'renewal dispatch unavailable'
        ELSE display_reason
    END AS reason,
    error,
    config_snapshot
FROM cache_keepalive_sessions";

const DECISION_ENTRY_SELECT: &str = "
SELECT
    'decision' AS entry_source,
    'decision:' || source_ref_id AS entry_id,
    session_key_hash,
    principal_id,
    upstream_id,
    last_message_at_ms,
    ttl,
    generation,
    NULL AS refresh_count,
    NULL AS status,
    NULL AS enqueue_state,
    NULL AS terminal_reason,
    decision,
    reason,
    error,
    config_snapshot
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

        let include_sessions = !matches!(query.filter, CacheKeepaliveSessionFilter::NotTracked);
        let include_decisions = matches!(
            query.filter,
            CacheKeepaliveSessionFilter::All
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
        let mut sessions_query = QueryBuilder::<Sqlite>::new(SESSION_ENTRY_SELECT);
        sessions_query
            .push(" WHERE principal_id = ")
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
        builder
            .push(SESSION_ENTRY_SELECT)
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
                "SELECT source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts FROM cache_keepalive_turns WHERE principal_id = ",
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
}

fn push_session_branch(
    builder: &mut QueryBuilder<Sqlite>,
    query: &CacheKeepaliveSessionListQuery,
    horizon_start_ms: Option<i64>,
    cursor_last_message_at_ms: Option<i64>,
    limit: i64,
) {
    builder
        .push(SESSION_ENTRY_SELECT)
        .push(" WHERE principal_id = ")
        .push_bind(&query.principal_id);
    if let Some(horizon_start_ms) = horizon_start_ms {
        builder
            .push(" AND last_message_at_ms >= ")
            .push_bind(horizon_start_ms);
    }
    match query.filter {
        CacheKeepaliveSessionFilter::All => {}
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
        CacheKeepaliveSessionFilter::NotTracked => unreachable!("session branch is skipped"),
        CacheKeepaliveSessionFilter::Error => {
            builder.push(" AND error IS NOT NULL");
        }
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
        CacheKeepaliveSessionFilter::NotTracked => {
            builder.push(" AND decision = 'not_tracked'");
        }
        CacheKeepaliveSessionFilter::Error => {
            builder.push(" AND error IS NOT NULL");
        }
        CacheKeepaliveSessionFilter::Renewed
        | CacheKeepaliveSessionFilter::Scheduled
        | CacheKeepaliveSessionFilter::Capped
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
