use std::sync::Arc;

use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveSessionCursor,
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheKeepaliveTurnRow, CacheTtl, MetaStore, RequestEvent, RequestEventProjections,
    RequestEventStore, StorageError,
};
use sqlx::Row;
use uuid::Uuid;

const PRINCIPAL_ID: &str = "principal-a";
const UPSTREAM_ID: Uuid = Uuid::from_u128(7);

async fn storage() -> (tempfile::TempDir, cc_lb_storage_sqlite::SqliteStorage) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let database_url = format!(
        "sqlite://{}",
        temp_dir
            .path()
            .join("cache-keepalive-reads.sqlite")
            .display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await
            .expect("open sqlite");
    storage.initialize().await.expect("initialize sqlite");
    (temp_dir, storage)
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
        ..RequestEvent::default()
    }
}

fn decision_projection(
    source_ref_id: &str,
    ts: u64,
    error: Option<&str>,
) -> RequestEventProjections {
    RequestEventProjections {
        turn: None,
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: PRINCIPAL_ID.to_owned(),
            session_key_hash: None,
            upstream_id: UPSTREAM_ID,
            decision: "not_tracked".to_owned(),
            reason: "user turn (stop_reason=end_turn)".to_owned(),
            error: error.map(ToOwned::to_owned),
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
        turn: Some(CacheKeepaliveTurnRow {
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

fn batched_turn_projection(
    source_ref_id: &str,
    session_key_hash: &str,
    principal_id: &str,
    ts: u64,
) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: source_ref_id.to_owned(),
            session_key_hash: session_key_hash.to_owned(),
            principal_id: principal_id.to_owned(),
            accounting_key_id: Some("accounting-key".to_owned()),
            upstream_id: UPSTREAM_ID,
            model: "claude-opus-4-1".to_owned(),
            input_tokens: 101,
            output_tokens: 23,
            cache_creation_input_tokens: 47,
            cache_creation_input_tokens_5m: 29,
            cache_creation_input_tokens_1h: 18,
            cache_read_input_tokens: 89,
            cost_micros: 12_345,
            hit_miss: "miss".to_owned(),
            ts,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: source_ref_id.to_owned(),
            principal_id: principal_id.to_owned(),
            session_key_hash: Some(session_key_hash.to_owned()),
            upstream_id: UPSTREAM_ID,
            decision: "reschedule".to_owned(),
            reason: "scheduled next renewal".to_owned(),
            error: None,
            generation: 3,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: Some(snapshot()),
            last_message_at_ms: ts * 1_000,
            ts,
        },
    }
}

#[tokio::test]
async fn lists_frozen_session_and_decision_projection_rows() {
    // Given: sessions in every storage state and a decision-only not-tracked row.
    let (_temp_dir, storage) = storage().await;
    let renewed = storage
        .replace_from_real_request(&request(
            "renewed-session",
            1_730_000_003,
            "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s",
        ))
        .await
        .expect("insert renewed session");
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
        .await
        .expect("renew session");

    let capped = storage
        .replace_from_real_request(&request(
            "capped-session",
            1_730_000_002,
            "max renewals reached",
        ))
        .await
        .expect("insert capped session");
    storage
        .mark_cache_keepalive_terminal(
            &capped.session_key_hash,
            capped.generation,
            CacheKeepaliveTerminalReason::MaxRefreshes,
            1_730_000_002,
        )
        .await
        .expect("cap session");

    let expired = storage
        .replace_from_real_request(&request(
            "expired-session",
            1_730_000_001,
            "TTL expired before follow-up",
        ))
        .await
        .expect("insert expired session");
    storage
        .mark_cache_keepalive_terminal(
            &expired.session_key_hash,
            expired.generation,
            CacheKeepaliveTerminalReason::Expired,
            1_730_000_001,
        )
        .await
        .expect("expire session");
    let max_duration = storage
        .replace_from_real_request(&request(
            "max-duration-session",
            1_730_000_000,
            "max duration reached (4h)",
        ))
        .await
        .expect("insert max-duration session");
    storage
        .mark_cache_keepalive_terminal(
            &max_duration.session_key_hash,
            max_duration.generation,
            CacheKeepaliveTerminalReason::MaxDuration,
            1_730_000_000,
        )
        .await
        .expect("terminate max-duration session");
    let scheduled = storage
        .replace_from_real_request(&request(
            "scheduled-session",
            1_730_000_006,
            "agent-in-turn",
        ))
        .await
        .expect("insert scheduled session");
    storage
        .append_request_event_with_projections(
            &event("not-tracked-decision", 1_730_000_005),
            &decision_projection(
                "not-tracked-decision",
                1_730_000_005,
                Some("renewal dispatch unavailable"),
            ),
        )
        .await
        .expect("insert not-tracked decision");
    storage
        .append_request_event_with_projections(
            &event("renewed-turn", 1_730_000_003),
            &renewal_projection("renewed-turn", 1_730_000_003),
        )
        .await
        .expect("insert renewed turn and decision");

    // When: the principal reads its cache-keepalive history.
    let page = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::All,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("list cache keepalive rows");

    // Then: decision-only rows are retained alongside exact frozen display data.
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "scheduled-session",
            "not-tracked-decision",
            "renewed-session",
            "capped-session",
            "expired-session",
            "max-duration-session"
        ]
    );
    let decision = &page.rows[1];
    assert!(decision.is_decision());
    assert_eq!(decision.reason, "user turn (stop_reason=end_turn)");
    assert_eq!(
        decision.error.as_deref(),
        Some("renewal dispatch unavailable")
    );
    assert_eq!(decision.config_snapshot.as_ref(), Some(&snapshot()));
    assert_eq!(page.rows[0].reason, "agent-in-turn");
    assert_eq!(
        page.rows[2].reason,
        "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
    );
    assert_eq!(page.rows[3].reason, "max renewals reached");
    assert_eq!(page.rows[4].reason, "TTL expired before follow-up");
    assert_eq!(page.rows[5].reason, "max duration reached (4h)");

    let session = storage
        .get_cache_keepalive_session_for_principal(PRINCIPAL_ID, "renewed-session")
        .await
        .expect("load principal session")
        .expect("renewed session exists");
    let turns = storage
        .list_cache_keepalive_turns(PRINCIPAL_ID, "renewed-session")
        .await
        .expect("load session turns");
    let renewal_decision = storage
        .get_cache_keepalive_decision_for_principal(PRINCIPAL_ID, "renewed-turn")
        .await
        .expect("load session decision")
        .expect("renewed decision exists");
    assert_eq!(session.config_snapshot, Some(snapshot()));
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].source_ref_id, "renewed-turn");
    assert_eq!(
        renewal_decision.reason,
        "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
    );
    assert_eq!(
        scheduled.status,
        cc_lb_storage_api::CacheKeepaliveSessionStatus::Active
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
            .await
            .expect("list filtered cache keepalive sessions");
        assert_eq!(
            filtered
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            expected_ids
        );
    }
}

#[tokio::test]
async fn paginates_horizon_and_stable_tie_breaks_without_duplicates() {
    // Given: equally recent sessions and one row outside the horizon.
    let (_temp_dir, storage) = storage().await;
    for session_key_hash in ["alpha-session", "beta-session", "old-session"] {
        let timestamp = if session_key_hash == "old-session" {
            100
        } else {
            200
        };
        storage
            .replace_from_real_request(&request(session_key_hash, timestamp, "agent-in-turn"))
            .await
            .expect("insert session");
    }
    let query = CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: Some(200_000),
        filter: CacheKeepaliveSessionFilter::Scheduled,
        cursor: None,
        limit: 1,
    };

    // When: the caller follows the opaque cursor through the horizon-filtered list.
    let first = storage
        .list_cache_keepalive_sessions(&query)
        .await
        .expect("read first page");
    let second = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: first.next_cursor.clone(),
            ..query
        })
        .await
        .expect("read second page");

    // Then: tied rows are sorted by opaque session identity and never repeat.
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].id, "alpha-session");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].id, "beta-session");
    assert_ne!(first.rows[0].id, second.rows[0].id);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn frozen_message_timestamp_ignores_scheduler_state_updates_and_rejects_malformed_cursor() {
    // Given: an older session whose scheduler state changes after a newer message is stored.
    let (_temp_dir, storage) = storage().await;
    let older = storage
        .replace_from_real_request(&request("older-session", 100, "agent-in-turn"))
        .await
        .expect("insert older session");
    storage
        .replace_from_real_request(&request("newer-session", 200, "agent-in-turn"))
        .await
        .expect("insert newer session");
    storage
        .mark_cache_keepalive_enqueued(&older.session_key_hash, older.generation, 999)
        .await
        .expect("mark older session enqueued");

    // When: reading the complete history after the scheduler-only transition.
    let query = CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: None,
        filter: CacheKeepaliveSessionFilter::All,
        cursor: None,
        limit: 10,
    };
    let page = storage
        .list_cache_keepalive_sessions(&query)
        .await
        .expect("list frozen timestamp order");

    // Then: the newer user message remains first, independent of updated_at.
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["newer-session", "older-session"]
    );
    let error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: Some(cc_lb_storage_api::CacheKeepaliveSessionCursor {
                principal_id: "other-principal".to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: 200_000,
                entry_id: "session:newer-session".to_owned(),
            }),
            ..query
        })
        .await
        .expect_err("mismatched cursor must be rejected");
    assert!(
        error
            .to_string()
            .contains("cursor does not match principal")
    );
}

#[tokio::test]
async fn batches_turn_reads_with_deduplication_principal_isolation_and_canonical_order() {
    let (_temp_dir, storage) = storage().await;
    for (source_ref_id, session_key_hash, principal_id, ts) in [
        ("alpha-z", "alpha-session", PRINCIPAL_ID, 20),
        ("beta-a", "beta-session", PRINCIPAL_ID, 5),
        ("alpha-b", "alpha-session", PRINCIPAL_ID, 10),
        ("alpha-a", "alpha-session", PRINCIPAL_ID, 20),
        ("other-alpha", "alpha-session", "principal-b", 1),
        ("zeta-a", "zeta-session", PRINCIPAL_ID, 30),
    ] {
        let mut request_event = event(source_ref_id, ts);
        request_event.principal_id = Some(principal_id.to_owned());
        storage
            .append_request_event_with_projections(
                &request_event,
                &batched_turn_projection(source_ref_id, session_key_hash, principal_id, ts),
            )
            .await
            .expect("insert cache keepalive turn");
    }

    let turns = storage
        .list_cache_keepalive_turns_for_sessions(
            PRINCIPAL_ID,
            &[
                "beta-session".to_owned(),
                "unknown-session".to_owned(),
                "alpha-session".to_owned(),
                "beta-session".to_owned(),
                "zeta-session".to_owned(),
            ],
        )
        .await
        .expect("list cache keepalive turns for sessions");

    assert_eq!(
        turns
            .iter()
            .map(|turn| {
                (
                    turn.session_key_hash.as_str(),
                    turn.ts,
                    turn.source_ref_id.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("alpha-session", 10, "alpha-b"),
            ("alpha-session", 20, "alpha-a"),
            ("alpha-session", 20, "alpha-z"),
            ("beta-session", 5, "beta-a"),
            ("zeta-session", 30, "zeta-a"),
        ]
    );
    assert_eq!(
        turns[0],
        cc_lb_storage_api::CacheKeepaliveTurnRecord {
            source_ref_id: "alpha-b".to_owned(),
            session_key_hash: "alpha-session".to_owned(),
            principal_id: PRINCIPAL_ID.to_owned(),
            accounting_key_id: Some("accounting-key".to_owned()),
            upstream_id: UPSTREAM_ID,
            model: "claude-opus-4-1".to_owned(),
            input_tokens: 101,
            output_tokens: 23,
            cache_creation_input_tokens: 47,
            cache_creation_input_tokens_5m: 29,
            cache_creation_input_tokens_1h: 18,
            cache_read_input_tokens: 89,
            cost_micros: 12_345,
            hit_miss: "miss".to_owned(),
            ts: 10,
        }
    );
    let mut multi_batch_hashes = vec!["zeta-session".to_owned()];
    multi_batch_hashes.extend((0..901).map(|index| format!("middle-session-{index:04}")));
    multi_batch_hashes.extend(["alpha-session".to_owned(), "beta-session".to_owned()]);
    let multi_batch_turns = storage
        .list_cache_keepalive_turns_for_sessions(PRINCIPAL_ID, &multi_batch_hashes)
        .await
        .expect("list cache keepalive turns across SQLite bind batches");
    assert_eq!(multi_batch_turns, turns);
    assert!(
        storage
            .list_cache_keepalive_turns_for_sessions(PRINCIPAL_ID, &["unknown-session".to_owned()],)
            .await
            .expect("unknown session hash returns no turns")
            .is_empty()
    );
    assert!(
        storage
            .list_cache_keepalive_turns_for_sessions(PRINCIPAL_ID, &[])
            .await
            .expect("empty session hash list returns no turns")
            .is_empty()
    );
}

fn request_for_principal(
    principal_id: &str,
    session_key_hash: &str,
    now_unix_secs: u64,
) -> cc_lb_storage_api::CacheKeepaliveReplaceRequest {
    let mut request = request(session_key_hash, now_unix_secs, "agent-in-turn");
    request.principal_id = principal_id.to_owned();
    request
}

async fn insert_decision(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    source_ref_id: &str,
    ts: u64,
) {
    let mut request_event = event(source_ref_id, ts);
    request_event.principal_id = Some(principal_id.to_owned());
    let mut projections = decision_projection(source_ref_id, ts, None);
    projections.decision.principal_id = principal_id.to_owned();
    storage
        .append_request_event_with_projections(&request_event, &projections)
        .await
        .unwrap_or_else(|error| panic!("insert decision {source_ref_id}: {error}"));
}

async fn insert_turn_for_source(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    source_ref_id: &str,
) {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns (
            source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id,
            model, input_tokens, output_tokens, cache_creation_input_tokens,
            cache_creation_input_tokens_5m, cache_creation_input_tokens_1h,
            cache_read_input_tokens, cost_micros, hit_miss, ts
         ) VALUES (?, ?, ?, NULL, ?, ?, 1, 1, 0, 0, 0, 1, 1, 'hit', 1)",
    )
    .bind(source_ref_id)
    .bind(format!("session-for-{source_ref_id}"))
    .bind(principal_id)
    .bind(UPSTREAM_ID.to_string())
    .bind("claude-sonnet-4-5")
    .execute(storage.pool())
    .await
    .unwrap_or_else(|error| panic!("insert turn {source_ref_id}: {error}"));
}

fn all_query(limit: u32) -> CacheKeepaliveSessionListQuery {
    CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: None,
        filter: CacheKeepaliveSessionFilter::All,
        cursor: None,
        limit,
    }
}

#[tokio::test]
async fn direct_lookup_returns_exact_session() {
    let (_temp_dir, storage) = storage().await;
    let inserted = storage
        .replace_from_real_request(&request("sess-100", 1_700_000_100, "agent-in-turn"))
        .await
        .expect("insert session");
    storage
        .reschedule_after_cache_hit(&cc_lb_storage_api::CacheKeepaliveHitRefreshRequest {
            session_key_hash: inserted.session_key_hash,
            generation: inserted.generation,
            cache_anchor_at_unix_secs: 1_700_000_101,
            run_at_unix_secs: 1_700_000_371,
            expires_at_unix_secs: 1_700_000_401,
            encrypted_payload: None,
            now_unix_secs: 1_700_000_101,
        })
        .await
        .expect("increment refresh count");

    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "sess-100")
        .await
        .expect("direct lookup")
        .expect("session exists");
    let legacy = storage
        .list_cache_keepalive_sessions(&all_query(10))
        .await
        .expect("legacy-shaped list")
        .rows
        .into_iter()
        .find(|row| row.id == "sess-100")
        .expect("session in list");

    assert_eq!(item, legacy);
    assert_eq!(item.source, CacheKeepaliveSessionEntrySource::Session);
    assert_eq!(item.refresh_count, Some(1));
}

#[tokio::test]
async fn direct_lookup_returns_visible_decision() {
    let (_temp_dir, storage) = storage().await;
    insert_decision(&storage, PRINCIPAL_ID, "dec-200", 1_700_000_200).await;

    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-200")
        .await
        .expect("direct lookup")
        .expect("decision exists");

    assert_eq!(item.source, CacheKeepaliveSessionEntrySource::Decision);
    assert_eq!(item.id, "dec-200");
    assert_eq!(item.last_message_at_ms, 1_700_000_200_000);
    assert_eq!(item.refresh_count, None);
    assert_eq!(item.status, None);
}

#[tokio::test]
async fn direct_lookup_hides_decision_after_late_turn() {
    let (_temp_dir, storage) = storage().await;
    insert_decision(&storage, PRINCIPAL_ID, "dec-300", 1_700_000_300).await;
    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-300")
            .await
            .expect("lookup before turn")
            .is_some()
    );

    insert_turn_for_source(&storage, PRINCIPAL_ID, "dec-300").await;

    assert!(
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "dec-300")
            .await
            .expect("lookup after turn")
            .is_none()
    );
}

#[tokio::test]
async fn direct_lookup_uses_newest_collision_candidate() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("clash-400", 2_000, "agent-in-turn"))
        .await
        .expect("insert collision session");
    insert_decision(&storage, PRINCIPAL_ID, "clash-400", 1).await;

    let session_newer = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-400")
        .await
        .expect("lookup session-newer collision")
        .expect("collision candidate");
    assert_eq!(
        session_newer.source,
        CacheKeepaliveSessionEntrySource::Session
    );

    sqlx::query(
        "UPDATE cache_keepalive_decisions SET last_message_at_ms = ? WHERE source_ref_id = ?",
    )
    .bind(3_000_000_i64)
    .bind("clash-400")
    .execute(storage.pool())
    .await
    .expect("make decision newer");
    let decision_newer = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-400")
        .await
        .expect("lookup decision-newer collision")
        .expect("collision candidate");
    assert_eq!(
        decision_newer.source,
        CacheKeepaliveSessionEntrySource::Decision
    );
}

#[tokio::test]
async fn direct_lookup_preserves_equal_timestamp_namespace_order() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("clash-tie", 4_000, "agent-in-turn"))
        .await
        .expect("insert collision session");
    insert_decision(&storage, PRINCIPAL_ID, "clash-tie", 4_000).await;

    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "clash-tie")
        .await
        .expect("lookup tied collision")
        .expect("collision candidate");
    let legacy = storage
        .list_cache_keepalive_sessions(&all_query(10))
        .await
        .expect("list tied collision")
        .rows
        .into_iter()
        .find(|row| row.id == "clash-tie")
        .expect("legacy first match");

    assert_eq!(item, legacy);
    assert_eq!(item.source, CacheKeepaliveSessionEntrySource::Decision);
}

#[tokio::test]
async fn direct_lookup_is_principal_scoped() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request_for_principal(
            "principal-b",
            "tenant-session",
            5_000,
        ))
        .await
        .expect("insert other-principal session");
    insert_decision(&storage, "principal-b", "tenant-decision", 5_001).await;

    for id in ["tenant-session", "tenant-decision"] {
        assert!(
            storage
                .get_cache_keepalive_list_item(PRINCIPAL_ID, id)
                .await
                .expect("principal-a lookup")
                .is_none()
        );
        assert!(
            storage
                .get_cache_keepalive_list_item("principal-b", id)
                .await
                .expect("principal-b lookup")
                .is_some()
        );
    }
}

#[tokio::test]
async fn summary_input_matches_legacy_full_scan() {
    let (_temp_dir, storage) = storage().await;
    for (id, ts) in [("summary-new", 7_000), ("summary-old", 6_000)] {
        storage
            .replace_from_real_request(&request(id, ts, "agent-in-turn"))
            .await
            .expect("insert summary session");
    }
    insert_decision(&storage, PRINCIPAL_ID, "recent-visible", 7_001).await;
    insert_decision(&storage, PRINCIPAL_ID, "recent-joined", 7_002).await;
    insert_turn_for_source(&storage, PRINCIPAL_ID, "recent-joined").await;
    insert_decision(&storage, PRINCIPAL_ID, "old-visible", 5_000).await;

    let summary = storage
        .read_cache_keepalive_summary_input(PRINCIPAL_ID, 7_000_000)
        .await
        .expect("read narrow summary input");
    let legacy_sessions = storage
        .list_cache_keepalive_sessions(&all_query(100))
        .await
        .expect("read full visible list")
        .rows
        .into_iter()
        .filter(|row| row.source == CacheKeepaliveSessionEntrySource::Session)
        .collect::<Vec<_>>();

    assert_eq!(summary.sessions, legacy_sessions);
    assert_eq!(summary.recent_decisions, 1);
}

#[tokio::test]
async fn summary_recent_decisions_applies_anti_join() {
    let (_temp_dir, storage) = storage().await;
    for id in [
        "visible-a",
        "visible-b",
        "visible-c",
        "joined-a",
        "joined-b",
    ] {
        insert_decision(&storage, PRINCIPAL_ID, id, 8_000).await;
    }
    for id in ["joined-a", "joined-b"] {
        insert_turn_for_source(&storage, PRINCIPAL_ID, id).await;
    }

    let summary = storage
        .read_cache_keepalive_summary_input(PRINCIPAL_ID, 8_000_000)
        .await
        .expect("count visible decisions");
    assert_eq!(summary.recent_decisions, 3);
}

#[tokio::test]
async fn pagination_reaches_terminal_with_engine_parity() {
    let (_temp_dir, storage) = storage().await;
    for index in 0..85_u64 {
        storage
            .replace_from_real_request(&request(
                &format!("many-page-{index:03}"),
                20_000 - index / 3,
                "agent-in-turn",
            ))
            .await
            .expect("insert paginated session");
    }

    let expected = storage
        .list_cache_keepalive_sessions(&all_query(100))
        .await
        .expect("read expected order")
        .rows
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    let mut query = all_query(4);
    let mut actual = Vec::new();
    let mut page_count = 0;
    loop {
        let page = storage
            .list_cache_keepalive_sessions(&query)
            .await
            .expect("read paginated page");
        actual.extend(page.rows.into_iter().map(|row| row.id));
        page_count += 1;
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
    }

    assert_eq!(actual, expected);
    assert!(page_count > 20);
}

#[tokio::test]
async fn single_source_filters_skip_unrelated_branch() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("scheduled-safe", 9_000, "agent-in-turn"))
        .await
        .expect("insert scheduled session");
    insert_decision(&storage, PRINCIPAL_ID, "decision-corrupt", 9_001).await;
    sqlx::query(
        "UPDATE cache_keepalive_decisions SET config_snapshot = '{' WHERE source_ref_id = ?",
    )
    .bind("decision-corrupt")
    .execute(storage.pool())
    .await
    .expect("corrupt unrelated decision");

    let scheduled = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            filter: CacheKeepaliveSessionFilter::Scheduled,
            ..all_query(10)
        })
        .await
        .expect("session-only filter skips decision mapping");
    assert_eq!(scheduled.rows[0].id, "scheduled-safe");

    sqlx::query("UPDATE cache_keepalive_decisions SET config_snapshot = ? WHERE source_ref_id = ?")
        .bind(serde_json::to_string(&snapshot()).expect("serialize snapshot"))
        .bind("decision-corrupt")
        .execute(storage.pool())
        .await
        .expect("restore decision snapshot");
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET config_snapshot = '{' WHERE session_key_hash = ?",
    )
    .bind("scheduled-safe")
    .execute(storage.pool())
    .await
    .expect("corrupt unrelated session");
    let not_tracked = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            filter: CacheKeepaliveSessionFilter::NotTracked,
            ..all_query(10)
        })
        .await
        .expect("decision-only filter skips session mapping");
    assert_eq!(not_tracked.rows[0].id, "decision-corrupt");
}

#[tokio::test]
async fn cursor_mismatch_fails_before_query() {
    let (_temp_dir, storage) = storage().await;
    let error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: Some(CacheKeepaliveSessionCursor {
                principal_id: "principal-b".to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: 1,
                entry_id: "session:x".to_owned(),
            }),
            ..all_query(1)
        })
        .await
        .expect_err("reject mismatched cursor");
    assert!(matches!(
        error,
        StorageError::InvalidInput {
            ref field,
            ref reason
        } if field == "cache_keepalive_session_cursor"
            && reason == "cursor does not match principal, horizon, or filter"
    ));
}

#[tokio::test]
async fn list_boundaries_preserve_invalid_input_contract() {
    let (_temp_dir, storage) = storage().await;
    for index in 0..3 {
        storage
            .replace_from_real_request(&request(
                &format!("boundary-{index}"),
                10_000 + index,
                "agent-in-turn",
            ))
            .await
            .expect("insert boundary session");
    }

    let empty = storage
        .list_cache_keepalive_sessions(&all_query(0))
        .await
        .expect("zero limit");
    assert!(empty.rows.is_empty());
    assert!(empty.next_cursor.is_none());
    for limit in [1, 50, 100, u32::MAX] {
        let page = storage
            .list_cache_keepalive_sessions(&all_query(limit))
            .await
            .expect("valid direct storage limit");
        assert_eq!(page.rows.len(), (limit as usize).min(3));
    }

    let horizon_error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            horizon_start_ms: Some(u64::MAX),
            ..all_query(1)
        })
        .await
        .expect_err("oversized horizon");
    assert!(matches!(
        horizon_error,
        StorageError::InvalidInput { ref field, .. }
            if field == "cache_keepalive_horizon_start_ms"
    ));
    let cursor_error = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: Some(CacheKeepaliveSessionCursor {
                principal_id: PRINCIPAL_ID.to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                last_message_at_ms: u64::MAX,
                entry_id: "session:x".to_owned(),
            }),
            ..all_query(1)
        })
        .await
        .expect_err("oversized cursor timestamp");
    assert!(matches!(
        cursor_error,
        StorageError::InvalidInput { ref field, .. }
            if field == "cache_keepalive_session_cursor"
    ));
}

#[tokio::test]
async fn selected_corruption_is_not_silently_dropped() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("corrupt-session", 11_000, "agent-in-turn"))
        .await
        .expect("insert corruptible session");
    sqlx::query(
        "UPDATE cache_keepalive_sessions SET config_snapshot = '{' WHERE session_key_hash = ?",
    )
    .bind("corrupt-session")
    .execute(storage.pool())
    .await
    .expect("corrupt selected session");

    for error in [
        storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, "corrupt-session")
            .await
            .expect_err("direct lookup reports corruption"),
        storage
            .list_cache_keepalive_sessions(&all_query(10))
            .await
            .expect_err("list reports corruption"),
        storage
            .read_cache_keepalive_summary_input(PRINCIPAL_ID, 0)
            .await
            .expect_err("summary reports corruption"),
    ] {
        assert!(matches!(error, StorageError::Corrupted { .. }));
    }
}

#[tokio::test]
async fn list_and_detail_match_legacy_on_same_database_collation() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("collation-clash", 13_000, "agent-in-turn"))
        .await
        .expect("insert collision session");
    insert_decision(&storage, PRINCIPAL_ID, "collation-clash", 13_000).await;

    let first = storage
        .list_cache_keepalive_sessions(&all_query(1))
        .await
        .expect("read first collision row");
    let detail = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "collation-clash")
        .await
        .expect("read collision detail")
        .expect("collision detail exists");
    assert_eq!(first.rows[0], detail);
    assert_eq!(detail.source, CacheKeepaliveSessionEntrySource::Decision);

    let second = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            cursor: first.next_cursor.clone(),
            ..all_query(1)
        })
        .await
        .expect("read second collision row");
    assert_eq!(
        second.rows[0].source,
        CacheKeepaliveSessionEntrySource::Session
    );
    assert!(second.next_cursor.is_none());

    let collations = sqlx::query("PRAGMA collation_list")
        .fetch_all(storage.pool())
        .await
        .expect("read SQLite collations");
    assert!(
        collations
            .iter()
            .any(|row| row.get::<String, _>("name") == "BINARY")
    );
}
