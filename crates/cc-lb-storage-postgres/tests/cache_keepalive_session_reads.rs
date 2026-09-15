use std::{collections::BTreeSet, future::Future, str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow,
    CacheKeepaliveSessionCursor, CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionFilter,
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheKeepaliveTurnRecord, CacheTtl,
    MetaStore, RequestEvent, RequestEventProjections, RequestEventStore, StorageError,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const PRINCIPAL_ID: &str = "principal-a";
const UPSTREAM_ID: Uuid = Uuid::from_u128(7);

fn run_postgres_case<F, Fut>(name: &str, test: F)
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let Some(url) = std::env::var("CI_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("PG_URL").ok())
    else {
        eprintln!(
            "skip: CI_POSTGRES_URL or PG_URL not set; requires isolated local/test postgres DSN"
        );
        return;
    };
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(test(url))
        .unwrap_or_else(|error| panic!("{name}: {error:#}"));
}

#[test]
fn cache_keepalive_session_reads_postgres() {
    run_postgres_case("cache keepalive postgres read model", |url| async move {
        read_model_contract(&url).await
    });
}

#[test]
fn direct_lookup_returns_exact_session() {
    run_postgres_case("ST-01 direct session", |url| async move {
        direct_lookup_returns_exact_session_case(&url).await
    });
}

#[test]
fn direct_lookup_returns_visible_decision() {
    run_postgres_case("ST-02 direct decision", |url| async move {
        direct_lookup_returns_visible_decision_case(&url).await
    });
}

#[test]
fn direct_lookup_hides_decision_after_late_turn() {
    run_postgres_case("ST-03 late turn", |url| async move {
        direct_lookup_hides_decision_after_late_turn_case(&url).await
    });
}

#[test]
fn direct_lookup_uses_newest_collision_candidate() {
    run_postgres_case("ST-04 newest collision", |url| async move {
        direct_lookup_uses_newest_collision_candidate_case(&url).await
    });
}

#[test]
fn direct_lookup_preserves_equal_timestamp_namespace_order() {
    run_postgres_case("ST-05 collision tie", |url| async move {
        direct_lookup_preserves_equal_timestamp_namespace_order_case(&url).await
    });
}

#[test]
fn direct_lookup_is_principal_scoped() {
    run_postgres_case("ST-06 tenant scope", |url| async move {
        direct_lookup_is_principal_scoped_case(&url).await
    });
}

#[test]
fn summary_input_matches_legacy_full_scan() {
    run_postgres_case("ST-07 summary oracle", |url| async move {
        summary_input_matches_legacy_full_scan_case(&url).await
    });
}

#[test]
fn summary_recent_decisions_applies_anti_join() {
    run_postgres_case("ST-08 summary anti-join", |url| async move {
        summary_recent_decisions_applies_anti_join_case(&url).await
    });
}

#[test]
fn pagination_reaches_terminal_with_engine_parity() {
    run_postgres_case("ST-09 terminal pagination", |url| async move {
        pagination_reaches_terminal_with_engine_parity_case(&url).await
    });
}

#[test]
fn single_source_filters_skip_unrelated_branch() {
    run_postgres_case("ST-10 source branches", |url| async move {
        single_source_filters_skip_unrelated_branch_case(&url).await
    });
}

#[test]
fn cursor_mismatch_fails_before_query() {
    run_postgres_case("ST-11 cursor validation", |url| async move {
        cursor_mismatch_fails_before_query_case(&url).await
    });
}

#[test]
fn nullable_decision_timestamp_uses_ts_millis() {
    run_postgres_case("ST-12 nullable timestamp", |url| async move {
        nullable_decision_timestamp_uses_ts_millis_case(&url).await
    });
}

#[test]
fn list_boundaries_preserve_error_contract() {
    run_postgres_case("ST-13 limits and ranges", |url| async move {
        list_boundaries_preserve_error_contract_case(&url).await
    });
}

#[test]
fn selected_corruption_is_not_silently_dropped() {
    run_postgres_case("ST-14 corruption", |url| async move {
        selected_corruption_is_not_silently_dropped_case(&url).await
    });
}

#[test]
fn cursor_entry_id_preserves_legacy_sql_comparison() {
    run_postgres_case("ST-15 arbitrary cursor", |url| async move {
        cursor_entry_id_preserves_legacy_sql_comparison_case(&url).await
    });
}

#[test]
fn list_and_detail_match_legacy_on_same_database_collation() {
    run_postgres_case("ST-16 database collation", |url| async move {
        list_and_detail_match_legacy_on_same_database_collation_case(&url).await
    });
}

async fn read_model_contract(url: &str) -> Result<()> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize(BackendKind::Postgres).await?;

    // Given: stateful sessions and an unjoined decision-only row.
    let renewed = storage
        .replace_from_real_request(&request(
            "renewed-session",
            1_730_000_003,
            "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s",
        ))
        .await?;
    storage
        .reschedule_after_cache_hit(&cc_lb_storage_api::CacheKeepaliveHitRefreshRequest {
            session_key_hash: renewed.session_key_hash,
            generation: renewed.generation,
            cache_anchor_at_unix_secs: 1_730_000_004,
            run_at_unix_secs: 1_730_000_274,
            expires_at_unix_secs: 1_730_000_304,
            encrypted_payload: None,
            now_unix_secs: 1_730_000_004,
        })
        .await?;
    terminalize(
        &storage,
        "capped-session",
        1_730_000_002,
        CacheKeepaliveTerminalReason::MaxRefreshes,
    )
    .await?;
    terminalize(
        &storage,
        "expired-session",
        1_730_000_001,
        CacheKeepaliveTerminalReason::Expired,
    )
    .await?;
    terminalize(
        &storage,
        "max-duration-session",
        1_730_000_000,
        CacheKeepaliveTerminalReason::MaxDuration,
    )
    .await?;
    storage
        .replace_from_real_request(&request(
            "scheduled-session",
            1_730_000_006,
            "agent-in-turn",
        ))
        .await?;
    storage
        .append_request_event_with_projections(
            &event("not-tracked-decision", 1_730_000_005),
            &decision_projection("not-tracked-decision", 1_730_000_005),
        )
        .await?;
    storage
        .append_request_event_with_projections(
            &event("renewed-turn", 1_730_000_003),
            &renewal_projection("renewed-turn", 1_730_000_003),
        )
        .await?;

    // When: the principal reads the first page and follows its stable cursor.
    let query = CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: Some(1_730_000_000_000),
        filter: CacheKeepaliveSessionFilter::All,
        cursor: None,
        limit: 3,
    };
    let first = storage.list_cache_keepalive_sessions(&query).await?;
    let second = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: first.next_cursor.clone(),
            ..query
        })
        .await?;

    // Then: all frozen states sort without duplicates and the decision projection keeps UI data.
    let ids = first
        .rows
        .iter()
        .chain(&second.rows)
        .map(|row| row.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            "scheduled-session",
            "not-tracked-decision",
            "renewed-session",
            "capped-session",
            "expired-session",
            "max-duration-session"
        ]
    );
    assert!(first.rows[1].is_decision());
    assert_eq!(first.rows[1].reason, "user turn (stop_reason=end_turn)");
    assert_eq!(
        first.rows[1].error.as_deref(),
        Some("renewal dispatch unavailable")
    );
    assert_eq!(first.rows[1].config_snapshot.as_ref(), Some(&snapshot()));
    assert_eq!(first.rows[0].reason, "agent-in-turn");
    assert_eq!(
        first.rows[2].reason,
        "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
    );
    assert_eq!(second.rows[0].reason, "max renewals reached");
    assert_eq!(second.rows[1].reason, "TTL expired before follow-up");
    assert_eq!(second.rows[2].reason, "max duration reached (4h)");
    assert_eq!(
        ids.iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        ids.len()
    );

    let session = storage
        .get_cache_keepalive_session_for_principal(PRINCIPAL_ID, "renewed-session")
        .await?
        .expect("renewed session exists");
    let turns = storage
        .list_cache_keepalive_turns(PRINCIPAL_ID, "renewed-session")
        .await?;
    let decision = storage
        .get_cache_keepalive_decision_for_principal(PRINCIPAL_ID, "renewed-turn")
        .await?
        .expect("renewed decision exists");
    assert_eq!(session.config_snapshot, Some(snapshot()));
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].source_ref_id, "renewed-turn");
    assert_eq!(
        decision.reason,
        "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
    );

    for (filter, expected_ids) in [
        (
            CacheKeepaliveSessionFilter::Renewed,
            vec!["renewed-session"],
        ),
        (
            CacheKeepaliveSessionFilter::Scheduled,
            vec!["scheduled-session"],
        ),
        (
            CacheKeepaliveSessionFilter::Capped,
            vec!["capped-session", "max-duration-session"],
        ),
        (
            CacheKeepaliveSessionFilter::Expired,
            vec!["expired-session"],
        ),
        (
            CacheKeepaliveSessionFilter::NotTracked,
            vec!["not-tracked-decision"],
        ),
        (
            CacheKeepaliveSessionFilter::Error,
            vec!["not-tracked-decision"],
        ),
    ] {
        let filtered = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: PRINCIPAL_ID.to_owned(),
                horizon_start_ms: None,
                filter,
                cursor: None,
                limit: 10,
            })
            .await?;
        assert_eq!(
            filtered
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            expected_ids
        );
    }

    let older = storage
        .replace_from_real_request(&request("older-session", 100, "agent-in-turn"))
        .await?;
    storage
        .replace_from_real_request(&request("newer-session", 200, "agent-in-turn"))
        .await?;
    storage
        .mark_cache_keepalive_enqueued(&older.session_key_hash, older.generation, 999)
        .await?;
    let frozen_order = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::All,
            cursor: None,
            limit: 10,
        })
        .await?;
    let frozen_ids: Vec<&str> = frozen_order
        .rows
        .iter()
        .map(|row| row.id.as_str())
        .collect();
    assert_eq!(
        &frozen_ids[frozen_ids.len() - 2..],
        ["newer-session", "older-session"],
    );

    storage
        .replace_from_real_request(&request("outside-horizon", 1_730_000_006, "agent-in-turn"))
        .await?;
    storage
        .replace_from_real_request(&request("tie-alpha", 1_730_000_007, "agent-in-turn"))
        .await?;
    storage
        .replace_from_real_request(&request("tie-beta", 1_730_000_007, "agent-in-turn"))
        .await?;
    let horizon_query = CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: Some(1_730_000_007_000),
        filter: CacheKeepaliveSessionFilter::Scheduled,
        cursor: None,
        limit: 1,
    };
    let tied_first = storage
        .list_cache_keepalive_sessions(&horizon_query)
        .await?;
    let tied_second = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: tied_first.next_cursor.clone(),
            ..horizon_query
        })
        .await?;
    assert_eq!(
        tied_first
            .rows
            .iter()
            .chain(&tied_second.rows)
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["tie-alpha", "tie-beta"]
    );

    let cursor_error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::All,
            cursor: Some(cc_lb_storage_api::CacheKeepaliveSessionCursor {
                principal_id: "other-principal".to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: 200_000,
                entry_id: "session:newer-session".to_owned(),
            }),
            limit: 10,
        })
        .await
        .expect_err("mismatched cursor must be rejected");
    assert!(
        cursor_error
            .to_string()
            .contains("cursor does not match principal")
    );

    let expected_turns = vec![
        turn_record("batch-a", "turn-a-early", PRINCIPAL_ID, 10, 1),
        turn_record("batch-a", "turn-a-alpha", PRINCIPAL_ID, 20, 2),
        turn_record("batch-a", "turn-a-zulu", PRINCIPAL_ID, 20, 3),
        turn_record("batch-b", "turn-b", PRINCIPAL_ID, 5, 4),
    ];
    for turn in &expected_turns {
        insert_turn(&storage, turn).await?;
    }
    insert_turn(
        &storage,
        &turn_record("batch-a", "turn-other-principal", "principal-b", 1, 5),
    )
    .await?;

    let turns = storage
        .list_cache_keepalive_turns_for_sessions(
            PRINCIPAL_ID,
            &[
                "batch-b".to_owned(),
                "unknown".to_owned(),
                "batch-a".to_owned(),
                "batch-a".to_owned(),
            ],
        )
        .await?;
    assert_eq!(turns, expected_turns);

    fixture.pool.close().await;
    assert!(
        storage
            .list_cache_keepalive_turns_for_sessions(PRINCIPAL_ID, &[])
            .await?
            .is_empty()
    );

    fixture.drop_schema().await
}

const LEGACY_ORDER_SQL: &str = "
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

async fn initialized_storage(url: &str) -> Result<(Fixture, PostgresStorage)> {
    let fixture = Fixture::create(url).await?;
    let storage = PostgresStorage::new(fixture.pool.clone(), Arc::new(cc_lb_clock::SystemClock));
    storage.initialize(BackendKind::Postgres).await?;
    Ok((fixture, storage))
}

fn query(principal_id: &str, limit: u32) -> CacheKeepaliveSessionListQuery {
    CacheKeepaliveSessionListQuery {
        principal_id: principal_id.to_owned(),
        horizon_start_ms: None,
        filter: CacheKeepaliveSessionFilter::All,
        cursor: None,
        limit,
    }
}

fn item_key(item: &CacheKeepaliveSessionListItem) -> (String, u64) {
    (
        item.source.cursor_entry_id(&item.id),
        item.last_message_at_ms,
    )
}

async fn candidate_keys(
    storage: &PostgresStorage,
    mut query: CacheKeepaliveSessionListQuery,
) -> Result<Vec<(String, u64)>> {
    let mut keys = Vec::new();
    let mut seen_cursors = BTreeSet::new();
    loop {
        let page = storage.list_cache_keepalive_sessions(&query).await?;
        keys.extend(page.rows.iter().map(item_key));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(
            seen_cursors.insert((cursor.last_message_at_ms, cursor.entry_id.clone())),
            "cursor must make progress"
        );
        query.cursor = Some(cursor);
    }
    Ok(keys)
}

async fn legacy_keys(
    storage: &PostgresStorage,
    query: &CacheKeepaliveSessionListQuery,
) -> Result<Vec<(String, u64)>> {
    query.validate_cursor()?;
    let horizon = query.horizon_start_ms.map(i64::try_from).transpose()?;
    let cursor_timestamp = query
        .cursor
        .as_ref()
        .map(|cursor| i64::try_from(cursor.last_message_at_ms))
        .transpose()?;
    let cursor_entry_id = query.cursor.as_ref().map(|cursor| cursor.entry_id.as_str());
    let rows = sqlx::query(LEGACY_ORDER_SQL)
        .bind(&query.principal_id)
        .bind(&query.principal_id)
        .bind(horizon)
        .bind(horizon)
        .bind(query.filter.as_str())
        .bind(cursor_timestamp)
        .bind(cursor_timestamp)
        .bind(cursor_timestamp)
        .bind(cursor_entry_id)
        .bind(i64::from(query.limit))
        .fetch_all(storage.pool())
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("entry_id")?,
                u64::try_from(row.try_get::<i64, _>("last_message_at_ms")?)?,
            ))
        })
        .collect()
}

struct DecisionFixture<'a> {
    principal: &'a str,
    id: &'a str,
    last_message_ms: Option<i64>,
    ts: i64,
    decision: &'a str,
    error: Option<&'a str>,
    config_snapshot: Option<&'a str>,
}

async fn insert_decision_raw(
    storage: &PostgresStorage,
    fixture: DecisionFixture<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_decisions \
         (source_ref_id, decision, reason, generation, ts, principal_id, session_key_hash, \
          upstream_id, error, ttl, config_snapshot, last_message_at_ms) \
         VALUES ($1, $2, 'fixture decision', 1, $3, $4, NULL, $5, $6, '5m', $7, $8)",
    )
    .bind(fixture.id)
    .bind(fixture.decision)
    .bind(fixture.ts)
    .bind(fixture.principal)
    .bind(UPSTREAM_ID)
    .bind(fixture.error)
    .bind(fixture.config_snapshot)
    .bind(fixture.last_message_ms)
    .execute(storage.pool())
    .await?;
    Ok(())
}

async fn set_session_fields(
    storage: &PostgresStorage,
    id: &str,
    last_message_at_ms: i64,
    refresh_count: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE cache_keepalive_sessions \
         SET last_message_at_ms = $1, refresh_count = $2 \
         WHERE session_key_hash = $3",
    )
    .bind(last_message_at_ms)
    .bind(refresh_count)
    .bind(id)
    .execute(storage.pool())
    .await?;
    Ok(())
}

async fn insert_decision_series(
    storage: &PostgresStorage,
    principal_id: &str,
    prefix: &str,
    count: i64,
    base_ts: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_decisions \
         (source_ref_id, decision, reason, generation, ts, principal_id, session_key_hash, \
          upstream_id, error, ttl, config_snapshot, last_message_at_ms) \
         SELECT $1 || value::TEXT, 'not_tracked', 'fixture decision', 1, $4 - value, $2, NULL, \
                $3, NULL, '5m', NULL, ($4 - value) * 1000 \
         FROM generate_series(1, $5) AS series(value)",
    )
    .bind(prefix)
    .bind(principal_id)
    .bind(UPSTREAM_ID)
    .bind(base_ts)
    .bind(count)
    .execute(storage.pool())
    .await?;
    Ok(())
}

async fn direct_lookup_returns_exact_session_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    storage
        .replace_from_real_request(&request("sess-100", 1_700_000, "active fixture"))
        .await?;
    set_session_fields(&storage, "sess-100", 1_700_000_000, 3).await?;
    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "sess-100")
        .await?
        .expect("session item");
    assert_eq!(item.source, CacheKeepaliveSessionEntrySource::Session);
    assert_eq!(item.id, "sess-100");
    assert_eq!(item.refresh_count, Some(3));
    assert_eq!(
        item.status,
        Some(cc_lb_storage_api::CacheKeepaliveSessionStatus::Active)
    );
    assert_eq!(item.ttl, CacheTtl::Ttl5m);
    assert_eq!(item.generation, 1);
    assert_eq!(item.principal_id, PRINCIPAL_ID);
    assert_eq!(
        legacy_keys(&storage, &query(PRINCIPAL_ID, 10)).await?[0],
        item_key(&item)
    );
    fixture.drop_schema().await
}

async fn direct_lookup_returns_visible_decision_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "dec-200",
            last_message_ms: Some(1_700_000_123),
            ts: 1_700_000,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-200")
        .await?
        .expect("decision item");
    assert_eq!(item.source, CacheKeepaliveSessionEntrySource::Decision);
    assert_eq!(item.id, "dec-200");
    assert_eq!(item.last_message_at_ms, 1_700_000_123);
    assert_eq!(item.refresh_count, None);
    assert_eq!(item.status, None);
    fixture.drop_schema().await
}

async fn direct_lookup_hides_decision_after_late_turn_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "dec-300",
            last_message_ms: Some(3_000),
            ts: 3,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-300")
            .await?
            .is_some()
    );
    insert_turn(
        &storage,
        &turn_record("late-session", "dec-300", PRINCIPAL_ID, 4, 1),
    )
    .await?;
    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-300")
            .await?
            .is_none()
    );
    fixture.drop_schema().await
}

async fn direct_lookup_uses_newest_collision_candidate_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    storage
        .replace_from_real_request(&request("clash-400", 2, "session"))
        .await?;
    set_session_fields(&storage, "clash-400", 2_000, 0).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "clash-400",
            last_message_ms: Some(1_000),
            ts: 1,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    assert_eq!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-400")
            .await?
            .expect("session newer")
            .source,
        CacheKeepaliveSessionEntrySource::Session
    );
    sqlx::query(
        "UPDATE cache_keepalive_decisions SET last_message_at_ms = 3000 WHERE source_ref_id = $1",
    )
    .bind("clash-400")
    .execute(storage.pool())
    .await?;
    assert_eq!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-400")
            .await?
            .expect("decision newer")
            .source,
        CacheKeepaliveSessionEntrySource::Decision
    );
    fixture.drop_schema().await
}

async fn direct_lookup_preserves_equal_timestamp_namespace_order_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    storage
        .replace_from_real_request(&request("clash-500", 1, "session"))
        .await?;
    set_session_fields(&storage, "clash-500", 1_000, 0).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "clash-500",
            last_message_ms: Some(1_000),
            ts: 1,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    let legacy = legacy_keys(&storage, &query(PRINCIPAL_ID, 2)).await?;
    let selected = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-500")
        .await?
        .expect("collision candidate");
    assert_eq!(item_key(&selected), legacy[0]);
    assert_eq!(selected.source, CacheKeepaliveSessionEntrySource::Decision);
    fixture.drop_schema().await
}

async fn direct_lookup_is_principal_scoped_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    storage
        .replace_from_real_request(&request_for_principal(
            "tenant-session",
            "principal-b",
            10,
            "tenant",
        ))
        .await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: "principal-b",
            id: "tenant-decision",
            last_message_ms: Some(10_000),
            ts: 10,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "tenant-session")
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "tenant-decision")
            .await?
            .is_none()
    );
    assert!(
        storage
            .get_cache_keepalive_list_item("principal-b", "tenant-session")
            .await?
            .is_some()
    );
    assert!(
        storage
            .get_cache_keepalive_list_item("principal-b", "tenant-decision")
            .await?
            .is_some()
    );
    fixture.drop_schema().await
}

async fn summary_input_matches_legacy_full_scan_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    for index in 0..50 {
        storage
            .replace_from_real_request(&request(
                &format!("summary-session-{index:02}"),
                2_000_000 + index,
                "summary",
            ))
            .await?;
    }
    insert_decision_series(&storage, PRINCIPAL_ID, "old-", 10_000, 1_000_000).await?;
    for index in 0..5 {
        let id = format!("recent-{index}");
        insert_decision_raw(
            &storage,
            DecisionFixture {
                principal: PRINCIPAL_ID,
                id: &id,
                last_message_ms: Some(2_000_000_000 + index),
                ts: 2_000_000,
                decision: "not_tracked",
                error: None,
                config_snapshot: None,
            },
        )
        .await?;
        if index >= 3 {
            insert_turn(
                &storage,
                &turn_record("joined", &id, PRINCIPAL_ID, 2_000_001, index as u32),
            )
            .await?;
        }
    }
    let summary = storage
        .read_cache_keepalive_summary_input(PRINCIPAL_ID, 1_999_999_999)
        .await?;
    assert_eq!(summary.recent_decisions, 3);
    let legacy = legacy_keys(&storage, &query(PRINCIPAL_ID, 20_000)).await?;
    let legacy_sessions = legacy
        .into_iter()
        .filter(|(id, _)| id.starts_with("session:"))
        .collect::<Vec<_>>();
    assert_eq!(
        summary.sessions.iter().map(item_key).collect::<Vec<_>>(),
        legacy_sessions
    );
    fixture.drop_schema().await
}

async fn summary_recent_decisions_applies_anti_join_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    for index in 0..15 {
        let id = format!("anti-join-{index}");
        insert_decision_raw(
            &storage,
            DecisionFixture {
                principal: PRINCIPAL_ID,
                id: &id,
                last_message_ms: Some(5_000 + index),
                ts: 5,
                decision: "not_tracked",
                error: None,
                config_snapshot: None,
            },
        )
        .await?;
        if index < 10 {
            insert_turn(
                &storage,
                &turn_record("joined", &id, PRINCIPAL_ID, 6, index as u32),
            )
            .await?;
        }
    }
    assert_eq!(
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, 5_000)
            .await?
            .recent_decisions,
        5
    );
    fixture.drop_schema().await
}

async fn pagination_reaches_terminal_with_engine_parity_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    sqlx::query(
        "INSERT INTO cache_keepalive_decisions \
         (source_ref_id, decision, reason, generation, ts, principal_id, session_key_hash, \
          upstream_id, error, ttl, config_snapshot, last_message_at_ms) \
         SELECT 'page-' || lpad(value::TEXT, 4, '0'), 'not_tracked', 'page', 1, \
                2_000_000 - (value / 5), $1, NULL, $2, NULL, '5m', NULL, \
                CASE WHEN value % 3 = 0 THEN NULL ELSE (2_000_000 - (value / 5)) * 1000 END \
         FROM generate_series(1, 1005) AS series(value)",
    )
    .bind(PRINCIPAL_ID)
    .bind(UPSTREAM_ID)
    .execute(storage.pool())
    .await?;
    let legacy = legacy_keys(&storage, &query(PRINCIPAL_ID, 2_000)).await?;
    let candidate = candidate_keys(&storage, query(PRINCIPAL_ID, 47)).await?;
    assert_eq!(candidate, legacy);
    assert_eq!(candidate.len(), 1005);
    fixture.drop_schema().await
}

async fn single_source_filters_skip_unrelated_branch_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    storage
        .replace_from_real_request(&request("corrupt-session", 20, "session"))
        .await?;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET config_snapshot = '{' WHERE session_key_hash = $1",
    )
    .bind("corrupt-session")
    .execute(storage.pool())
    .await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "visible-decision",
            last_message_ms: Some(21_000),
            ts: 21,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    let not_tracked = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            filter: CacheKeepaliveSessionFilter::NotTracked,
            ..query(PRINCIPAL_ID, 10)
        })
        .await?;
    assert_eq!(not_tracked.rows.len(), 1);
    assert_eq!(not_tracked.rows[0].id, "visible-decision");

    sqlx::query(
        "UPDATE cache_keepalive_sessions SET config_snapshot = NULL WHERE session_key_hash = $1",
    )
    .bind("corrupt-session")
    .execute(storage.pool())
    .await?;
    storage
        .replace_from_real_request(&request("scheduled-ok", 22, "session"))
        .await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "corrupt-decision",
            last_message_ms: Some(23_000),
            ts: 23,
            decision: "not_tracked",
            error: None,
            config_snapshot: Some("{"),
        },
    )
    .await?;
    let scheduled = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            filter: CacheKeepaliveSessionFilter::Scheduled,
            ..query(PRINCIPAL_ID, 10)
        })
        .await?;
    assert_eq!(
        scheduled
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["corrupt-session", "scheduled-ok"])
    );
    fixture.drop_schema().await
}

async fn cursor_mismatch_fails_before_query_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    fixture.pool.close().await;
    let error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::All,
            cursor: Some(CacheKeepaliveSessionCursor {
                principal_id: "principal-b".to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: 1,
                entry_id: "x".to_owned(),
            }),
            limit: 10,
        })
        .await
        .expect_err("cursor mismatch");
    assert!(matches!(
        error,
        StorageError::InvalidInput { ref field, .. }
            if field == "cache_keepalive_session_cursor"
    ));
    fixture.drop_schema().await
}

async fn nullable_decision_timestamp_uses_ts_millis_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "nullable-ts",
            last_message_ms: None,
            ts: 1_700_000,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    let direct = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "nullable-ts")
        .await?
        .expect("nullable timestamp decision");
    assert_eq!(direct.last_message_at_ms, 1_700_000_000);
    assert_eq!(
        storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                horizon_start_ms: Some(1_700_000_000),
                ..query(PRINCIPAL_ID, 10)
            })
            .await?
            .rows[0]
            .last_message_at_ms,
        1_700_000_000
    );
    assert_eq!(
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, 1_700_000_000)
            .await?
            .recent_decisions,
        1
    );
    fixture.drop_schema().await
}

async fn list_boundaries_preserve_error_contract_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    insert_decision_series(&storage, PRINCIPAL_ID, "boundary-", 101, 2_000_000).await?;
    let empty = storage
        .list_cache_keepalive_sessions(&query(PRINCIPAL_ID, 0))
        .await?;
    assert!(empty.rows.is_empty());
    assert!(empty.next_cursor.is_none());
    for limit in [1, 50, 100] {
        let page = storage
            .list_cache_keepalive_sessions(&query(PRINCIPAL_ID, limit))
            .await?;
        assert_eq!(page.rows.len(), limit as usize);
        assert!(page.next_cursor.is_some());
    }
    assert!(
        storage
            .list_cache_keepalive_sessions(&query("missing-principal", u32::MAX))
            .await?
            .rows
            .is_empty()
    );
    let horizon_error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            horizon_start_ms: Some(u64::MAX),
            ..query(PRINCIPAL_ID, 1)
        })
        .await
        .expect_err("horizon range");
    assert!(matches!(
        horizon_error,
        StorageError::Fatal { ref message }
            if message == "cache keepalive horizon start cannot be represented as bigint"
    ));
    let cursor_error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: Some(CacheKeepaliveSessionCursor {
                principal_id: PRINCIPAL_ID.to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: u64::MAX,
                entry_id: "x".to_owned(),
            }),
            ..query(PRINCIPAL_ID, 1)
        })
        .await
        .expect_err("cursor range");
    assert!(matches!(
        cursor_error,
        StorageError::Fatal { ref message }
            if message == "cache keepalive cursor timestamp cannot be represented as bigint"
    ));
    assert!(matches!(
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, u64::MAX)
            .await
            .expect_err("summary cutoff range"),
        StorageError::InvalidInput { .. }
    ));
    fixture.drop_schema().await
}

async fn selected_corruption_is_not_silently_dropped_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "old-corrupt",
            last_message_ms: Some(1_000),
            ts: 1,
            decision: "not_tracked",
            error: None,
            config_snapshot: Some("{"),
        },
    )
    .await?;
    assert_eq!(
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, 2_000)
            .await?
            .recent_decisions,
        0
    );
    assert!(matches!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "old-corrupt")
            .await
            .expect_err("selected decision corruption"),
        StorageError::Corrupted { .. }
    ));
    storage
        .replace_from_real_request(&request("corrupt-summary-session", 3, "session"))
        .await?;
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET config_snapshot = '{' WHERE session_key_hash = $1",
    )
    .bind("corrupt-summary-session")
    .execute(storage.pool())
    .await?;
    assert!(matches!(
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, 2_000)
            .await
            .expect_err("selected session corruption"),
        StorageError::Corrupted { .. }
    ));
    assert!(matches!(
        storage
            .list_cache_keepalive_sessions(&query(PRINCIPAL_ID, 10))
            .await
            .expect_err("selected list corruption"),
        StorageError::Corrupted { .. }
    ));
    fixture.drop_schema().await
}

async fn cursor_entry_id_preserves_legacy_sql_comparison_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    for id in ["x", "session::x", "decision:é", "Ω", "colon:value"] {
        storage
            .replace_from_real_request(&request(id, 10, "cursor"))
            .await?;
        set_session_fields(&storage, id, 10_000, 0).await?;
        insert_decision_raw(
            &storage,
            DecisionFixture {
                principal: PRINCIPAL_ID,
                id: &format!("d-{id}"),
                last_message_ms: Some(10_000),
                ts: 10,
                decision: "not_tracked",
                error: None,
                config_snapshot: None,
            },
        )
        .await?;
    }
    for cursor_entry_id in ["x", "session::x", "decision:é", "Ω", "decision::x"] {
        let query = CacheKeepaliveSessionListQuery {
            cursor: Some(CacheKeepaliveSessionCursor {
                principal_id: PRINCIPAL_ID.to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: 10_000,
                entry_id: cursor_entry_id.to_owned(),
            }),
            ..query(PRINCIPAL_ID, 100)
        };
        let candidate = storage
            .list_cache_keepalive_sessions(&query)
            .await?
            .rows
            .iter()
            .map(item_key)
            .collect::<Vec<_>>();
        assert_eq!(candidate, legacy_keys(&storage, &query).await?);
    }
    fixture.drop_schema().await
}

async fn list_and_detail_match_legacy_on_same_database_collation_case(url: &str) -> Result<()> {
    let (fixture, storage) = initialized_storage(url).await?;
    let collation: String =
        sqlx::query_scalar("SELECT datcollate FROM pg_database WHERE datname = current_database()")
            .fetch_one(storage.pool())
            .await?;
    assert!(!collation.is_empty());
    for id in ["ascii", "é", "Ω", "colon:value", "session::shape"] {
        storage
            .replace_from_real_request(&request(id, 20, "collation"))
            .await?;
        set_session_fields(&storage, id, 20_000, 0).await?;
        insert_decision_raw(
            &storage,
            DecisionFixture {
                principal: PRINCIPAL_ID,
                id: &format!("decision-{id}"),
                last_message_ms: Some(20_000),
                ts: 20,
                decision: "not_tracked",
                error: None,
                config_snapshot: None,
            },
        )
        .await?;
    }
    storage
        .replace_from_real_request(&request("collation-clash", 20, "collision"))
        .await?;
    set_session_fields(&storage, "collation-clash", 20_000, 0).await?;
    insert_decision_raw(
        &storage,
        DecisionFixture {
            principal: PRINCIPAL_ID,
            id: "collation-clash",
            last_message_ms: Some(20_000),
            ts: 20,
            decision: "not_tracked",
            error: None,
            config_snapshot: None,
        },
    )
    .await?;
    let legacy = legacy_keys(&storage, &query(PRINCIPAL_ID, 100)).await?;
    assert_eq!(
        candidate_keys(&storage, query(PRINCIPAL_ID, 3)).await?,
        legacy
    );
    let expected = legacy
        .iter()
        .find(|(entry_id, _)| {
            entry_id == "decision:collation-clash" || entry_id == "session:collation-clash"
        })
        .expect("legacy collision");
    assert_eq!(
        item_key(
            &storage
                .get_cache_keepalive_list_item(PRINCIPAL_ID, "collation-clash")
                .await?
                .expect("direct collision"),
        ),
        expected.clone()
    );
    fixture.drop_schema().await
}

async fn insert_turn(storage: &PostgresStorage, turn: &CacheKeepaliveTurnRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns \
         (source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, model, input_tokens, output_tokens, cache_creation_input_tokens, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h, cache_read_input_tokens, cost_micros, hit_miss, ts) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
    )
    .bind(&turn.source_ref_id)
    .bind(&turn.session_key_hash)
    .bind(&turn.principal_id)
    .bind(turn.accounting_key_id.as_deref())
    .bind(turn.upstream_id)
    .bind(&turn.model)
    .bind(i64::try_from(turn.input_tokens)?)
    .bind(i64::try_from(turn.output_tokens)?)
    .bind(i64::try_from(turn.cache_creation_input_tokens)?)
    .bind(i64::try_from(turn.cache_creation_input_tokens_5m)?)
    .bind(i64::try_from(turn.cache_creation_input_tokens_1h)?)
    .bind(i64::try_from(turn.cache_read_input_tokens)?)
    .bind(turn.cost_micros)
    .bind(&turn.hit_miss)
    .bind(i64::try_from(turn.ts)?)
    .execute(storage.pool())
    .await?;
    Ok(())
}

fn turn_record(
    session_key_hash: &str,
    source_ref_id: &str,
    principal_id: &str,
    ts: u64,
    seed: u32,
) -> CacheKeepaliveTurnRecord {
    CacheKeepaliveTurnRecord {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: Some(format!("accounting-key-{seed}")),
        upstream_id: Uuid::from_u128(100 + u128::from(seed)),
        model: format!("model-{seed}"),
        input_tokens: u64::from(seed) + 1,
        output_tokens: u64::from(seed) + 2,
        cache_creation_input_tokens: u64::from(seed) + 3,
        cache_creation_input_tokens_5m: u64::from(seed) + 4,
        cache_creation_input_tokens_1h: u64::from(seed) + 5,
        cache_read_input_tokens: u64::from(seed) + 6,
        cost_micros: -i64::from(seed),
        hit_miss: format!("hit-miss-{seed}"),
        ts,
    }
}

async fn terminalize(
    storage: &PostgresStorage,
    session_key_hash: &str,
    now_unix_secs: u64,
    reason: CacheKeepaliveTerminalReason,
) -> Result<()> {
    let record = storage
        .replace_from_real_request(&request(
            session_key_hash,
            now_unix_secs,
            reason.display_reason(),
        ))
        .await?;
    storage
        .mark_cache_keepalive_terminal(
            &record.session_key_hash,
            record.generation,
            reason,
            now_unix_secs,
        )
        .await?;
    Ok(())
}

fn snapshot() -> CacheKeepaliveConfigSnapshot {
    CacheKeepaliveConfigSnapshot {
        refresh_lead_time_5m_secs: 30,
        refresh_lead_time_1h_secs: 300,
        max_refreshes_per_session: 12,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 524_288,
    }
}

fn request(
    session_key_hash: &str,
    now_unix_secs: u64,
    display_reason: &str,
) -> cc_lb_storage_api::CacheKeepaliveReplaceRequest {
    request_for_principal(
        session_key_hash,
        PRINCIPAL_ID,
        now_unix_secs,
        display_reason,
    )
}

fn request_for_principal(
    session_key_hash: &str,
    principal_id: &str,
    now_unix_secs: u64,
    display_reason: &str,
) -> cc_lb_storage_api::CacheKeepaliveReplaceRequest {
    cc_lb_storage_api::CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: None,
        upstream_id: UPSTREAM_ID,
        cache_anchor_at_unix_secs: now_unix_secs,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now_unix_secs + 270,
        expires_at_unix_secs: now_unix_secs + 300,
        encrypted_payload: vec![1],
        display_reason: display_reason.to_owned(),
        config_snapshot: snapshot(),
        now_unix_secs,
    }
}

fn event(source_ref_id: &str, ts: u64) -> RequestEvent {
    RequestEvent {
        ts,
        ts_ms: Some(ts * 1_000),
        request_id: source_ref_id.to_owned(),
        source_kind: Some("cache_keepalive_decision".to_owned()),
        source_ref_id: Some(source_ref_id.to_owned()),
        event_id: Some(format!("event-{source_ref_id}")),
        principal_id: Some(PRINCIPAL_ID.to_owned()),
        upstream_id: Some(UPSTREAM_ID),
        status: 200,
        duration_ms: 1,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..RequestEvent::default()
    }
}

fn decision_projection(source_ref_id: &str, ts: u64) -> RequestEventProjections {
    RequestEventProjections {
        turn: None,
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: PRINCIPAL_ID.to_owned(),
            session_key_hash: None,
            upstream_id: UPSTREAM_ID,
            decision: "not_tracked".to_owned(),
            reason: "user turn (stop_reason=end_turn)".to_owned(),
            error: Some("renewal dispatch unavailable".to_owned()),
            generation: 4,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: Some(snapshot()),
            last_message_at_ms: ts * 1_000,
            ts,
        },
    }
}

fn renewal_projection(source_ref_id: &str, ts: u64) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(cc_lb_storage_api::CacheKeepaliveTurnRow {
            source_ref_id: source_ref_id.to_owned(),
            session_key_hash: "renewed-session".to_owned(),
            principal_id: PRINCIPAL_ID.to_owned(),
            accounting_key_id: None,
            upstream_id: UPSTREAM_ID,
            model: "claude-sonnet-4-5".to_owned(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 0,
            cache_creation_input_tokens_5m: 0,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 100,
            cost_micros: 123,
            hit_miss: "hit".to_owned(),
            ts,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: PRINCIPAL_ID.to_owned(),
            session_key_hash: Some("renewed-session".to_owned()),
            upstream_id: UPSTREAM_ID,
            decision: "reschedule".to_owned(),
            reason: "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s".to_owned(),
            error: None,
            generation: 2,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: Some(snapshot()),
            last_message_at_ms: ts * 1_000,
            ts,
        },
    }
}

struct Fixture {
    schema: String,
    admin_pool: PgPool,
    pool: PgPool,
}

impl Fixture {
    async fn create(url: &str) -> Result<Self> {
        let schema = format!("cache_keepalive_reads_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new().max_connections(1).connect(url).await?;
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin_pool)
            .await?;
        let options = PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        Ok(Self {
            schema,
            admin_pool,
            pool,
        })
    }

    async fn drop_schema(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.admin_pool)
        .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}
