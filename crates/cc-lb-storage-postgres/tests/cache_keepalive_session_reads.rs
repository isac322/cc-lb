use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow,
    CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheKeepaliveTurnRecord, CacheTtl,
    MetaStore, RequestEvent, RequestEventProjections, RequestEventStore,
};
use cc_lb_storage_postgres::PostgresStorage;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgConnectOptions, postgres::PgPoolOptions};
use uuid::Uuid;

const PRINCIPAL_ID: &str = "principal-a";
const UPSTREAM_ID: Uuid = Uuid::from_u128(7);

#[test]
fn cache_keepalive_session_reads_postgres() {
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
        .block_on(async move { read_model_contract(&url).await })
        .expect("cache keepalive postgres read model");
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
    cc_lb_storage_api::CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: PRINCIPAL_ID.to_owned(),
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
