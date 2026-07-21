use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow,
    CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, CacheKeepaliveTurnRow, CacheTtl,
    MetaStore, RequestEvent, RequestEventProjections, RequestEventStore,
};
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
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite");
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
            encrypted_payload: vec![2],
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
