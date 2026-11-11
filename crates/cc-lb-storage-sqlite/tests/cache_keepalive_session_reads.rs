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

#[tokio::test]
async fn activity_evidence_bounds_id_and_prefix_parameters_and_deduplicates_union() {
    const COUNT: usize = 17_000;
    let (_directory, storage) = storage().await;
    let mut transaction = storage
        .pool()
        .begin()
        .await
        .expect("begin evidence fixture");
    sqlx::query(
        "WITH RECURSIVE ids(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM ids WHERE n < 17000
         ), kinds(kind) AS (SELECT 'renewal' UNION ALL SELECT 'follow_up')
         INSERT INTO cache_keepalive_evidence (
            evidence_id, cycle_id, evidence_kind, request_start_ms, prefix_hash,
            principal_id, upstream_id, model, ttl, event_ref, created_at_ms
         )
         SELECT kind || '-' || n,
            CASE WHEN kind = 'renewal' THEN printf('00000000-0000-0000-0000-%012x', n) END,
            kind, CASE WHEN kind = 'renewal' THEN 1000 ELSE 2000 END,
            'prefix-' || n, 'principal-a', '00000000-0000-0000-0000-000000000007',
            'claude-sonnet-4-5', '5m',
            CASE WHEN kind = 'renewal' THEN 'source-' || n END, 1000
         FROM ids CROSS JOIN kinds",
    )
    .execute(&mut *transaction)
    .await
    .expect("insert all evidence in one transaction");
    transaction.commit().await.expect("commit evidence fixture");
    let cycle_ids = (1..=COUNT)
        .map(|id| Uuid::from_u128(id as u128))
        .collect::<Vec<_>>();
    // Reverse the source scope so some rows match different id batches.
    let source_refs = (1..=COUNT)
        .rev()
        .map(|id| format!("source-{id}"))
        .collect::<Vec<_>>();
    let evidence = storage
        .list_cache_keepalive_evidence_for_activity(
            PRINCIPAL_ID,
            &cycle_ids,
            &source_refs,
            0,
            300_000,
        )
        .await
        .expect("read evidence beyond SQLite's parameter limit");
    let ids = evidence
        .iter()
        .map(|entry| entry.evidence_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(evidence.len(), COUNT * 2);
    assert_eq!(
        ids.len(),
        evidence.len(),
        "union must not duplicate evidence"
    );
    assert!(
        evidence
            .windows(2)
            .all(|pair| (pair[0].request_start_ms, &pair[0].evidence_id)
                < (pair[1].request_start_ms, &pair[1].evidence_id))
    );
    let mut expected = std::collections::BTreeSet::new();
    for (cycles, sources) in cycle_ids.chunks(500).zip(source_refs.chunks(500)) {
        expected.extend(
            storage
                .list_cache_keepalive_evidence_for_activity(
                    PRINCIPAL_ID,
                    cycles,
                    sources,
                    0,
                    300_000,
                )
                .await
                .expect("read small evidence scope")
                .into_iter()
                .map(|entry| entry.evidence_id),
        );
    }
    assert_eq!(ids, expected, "large scope must equal union of small reads");
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
        cycle_id: uuid::Uuid::nil(),
        ttl: CacheTtl::Ttl5m,
        cache_anchor_at_unix_secs: now_unix_secs,
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
        evidence: Vec::new(),
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
            cycle_id: None,
            model: "claude-sonnet-4-5".to_owned(),
            input_tokens: 100,
            output_tokens: 20,
            cache_creation_input_tokens: 0,
            cache_creation_input_tokens_5m: 0,
            cache_creation_input_tokens_1h: 0,
            cache_read_input_tokens: 100,
            cost_micros: Some(123),
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
        evidence: Vec::new(),
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
            cycle_id: None,
            model: "claude-opus-4-1".to_owned(),
            input_tokens: 101,
            output_tokens: 23,
            cache_creation_input_tokens: 47,
            cache_creation_input_tokens_5m: 29,
            cache_creation_input_tokens_1h: 18,
            cache_read_input_tokens: 89,
            cost_micros: Some(12_345),
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
        evidence: Vec::new(),
    }
}

#[tokio::test]
async fn append_filtered_evidence_uses_bounded_cycle_window() {
    let (_temp_dir, storage) = storage().await;
    let mut no_recent = follow_up_evidence("sqlite-no-recent", 5_000, "claude-sonnet-4-5");
    no_recent.request_start_ms = 5_000_000;
    storage
        .append_request_event_with_evidence(&event("sqlite-no-recent", 5_000), &[no_recent])
        .await
        .expect("append no-recent candidate");
    let no_recent_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cache_keepalive_evidence WHERE evidence_id = 'follow-up-sqlite-no-recent'",
    )
    .fetch_one(storage.pool())
    .await
    .expect("count no-recent evidence");
    assert_eq!(
        no_recent_count, 0,
        "unrelated historical rows are fast-rejected"
    );

    let cycle_id = Uuid::from_u128(0x0011_4901);
    let mut old_renewal = economics_projection("sqlite-old-renewal", Some(cycle_id), 0, "hit", 1);
    old_renewal.evidence[0].prefix_hash = Some("retained-prefix-hash".to_owned());
    storage
        .append_request_event_with_projections(&event("sqlite-old-renewal", 0), &old_renewal)
        .await
        .expect("append old renewal");
    let mut first_follow_up = follow_up_evidence("sqlite-old-follow-up", 0, "claude-sonnet-4-5");
    first_follow_up.request_start_ms = 1_000;
    storage
        .append_request_event_with_evidence(&event("sqlite-old-follow-up", 0), &[first_follow_up])
        .await
        .expect("append old follow-up");
    let mut recent_renewal =
        economics_projection("sqlite-recent-renewal", Some(cycle_id), 4_000, "hit", 1);
    recent_renewal.evidence[0].prefix_hash = Some("retained-prefix-hash".to_owned());
    storage
        .append_request_event_with_projections(
            &event("sqlite-recent-renewal", 4_000),
            &recent_renewal,
        )
        .await
        .expect("append recent renewal");
    let mut blocked_follow_up =
        follow_up_evidence("sqlite-blocked-follow-up", 4_000, "claude-sonnet-4-5");
    blocked_follow_up.request_start_ms = 4_000_001;
    storage
        .append_request_event_with_evidence(
            &event("sqlite-blocked-follow-up", 4_000),
            &[blocked_follow_up],
        )
        .await
        .expect("append blocked follow-up");
    let blocked_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cache_keepalive_evidence WHERE evidence_id = 'follow-up-sqlite-blocked-follow-up'",
    )
    .fetch_one(storage.pool())
    .await
    .expect("count blocked evidence");
    assert_eq!(
        blocked_count, 0,
        "a follow-up older than one hour still blocks through same-cycle sibling renewals"
    );

    let boundary_cycle = Uuid::from_u128(0x0011_4902);
    let mut boundary_renewal = economics_projection(
        "sqlite-boundary-renewal",
        Some(boundary_cycle),
        10_000,
        "hit",
        1,
    );
    boundary_renewal.evidence[0].ttl = CacheTtl::Ttl1h;
    storage
        .append_request_event_with_projections(
            &event("sqlite-boundary-renewal", 10_000),
            &boundary_renewal,
        )
        .await
        .expect("append boundary renewal");
    let mut boundary_follow_up =
        follow_up_evidence("sqlite-boundary-follow-up", 10_000, "claude-sonnet-4-5");
    boundary_follow_up.request_start_ms = 10_000_000 + 3_600_000;
    storage
        .append_request_event_with_evidence(
            &event("sqlite-boundary-follow-up", 10_000),
            &[boundary_follow_up],
        )
        .await
        .expect("append boundary follow-up");
    let boundary_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cache_keepalive_evidence WHERE evidence_id = 'follow-up-sqlite-boundary-follow-up'",
    )
    .fetch_one(storage.pool())
    .await
    .expect("count boundary evidence");
    assert_eq!(
        boundary_count, 1,
        "the one-hour TTL boundary remains inclusive"
    );

    storage
        .replace_from_real_request(&request("sqlite-running", 20_000, "running"))
        .await
        .expect("create running session");
    sqlx::query(
        "UPDATE cache_keepalive_sessions
         SET status = 'active', enqueue_state = 'running'
         WHERE session_key_hash = 'sqlite-running'",
    )
    .execute(storage.pool())
    .await
    .expect("mark running session");
    let mut wrong_pair = follow_up_evidence("sqlite-wrong-pair", 20_000, "claude-sonnet-4-5");
    wrong_pair.principal_id = "principal-other".to_owned();
    wrong_pair.request_start_ms = 20_000_001;
    let mut running_pair = follow_up_evidence("sqlite-running-pair", 20_000, "claude-sonnet-4-5");
    running_pair.request_start_ms = 20_000_002;
    storage
        .append_request_event_with_evidence(
            &event("sqlite-pair", 20_000),
            &[wrong_pair, running_pair],
        )
        .await
        .expect("append pair candidates");
    let pair_ids: Vec<String> = sqlx::query_scalar(
        "SELECT evidence_id FROM cache_keepalive_evidence
         WHERE evidence_id IN ('follow-up-sqlite-wrong-pair', 'follow-up-sqlite-running-pair')
         ORDER BY evidence_id",
    )
    .fetch_all(storage.pool())
    .await
    .expect("read pair candidates");
    assert_eq!(
        pair_ids,
        vec!["follow-up-sqlite-running-pair"],
        "running allowance must remain scoped to the exact principal/upstream pair"
    );
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
            None,
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
            None,
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
            None,
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
            "max-duration-session",
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
    assert_eq!(page.rows[5].reason, "max duration reached");

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
        (
            CacheKeepaliveSessionFilter::Visible,
            vec![
                "scheduled-session",
                "not-tracked-decision",
                "renewed-session",
                "capped-session",
                "expired-session",
                "max-duration-session",
            ],
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
async fn terminal_error_is_listed_and_cleared_after_real_request() {
    let (_temp_dir, storage) = storage().await;
    let record = storage
        .replace_from_real_request(&request(
            "terminal-error-session",
            1_730_000_010,
            "dispatch failed",
        ))
        .await
        .expect("insert session");
    storage
        .mark_cache_keepalive_terminal(
            &record.session_key_hash,
            record.generation,
            CacheKeepaliveTerminalReason::DispatchError,
            Some("status=400 body=invalid"),
            1_730_000_010,
        )
        .await
        .expect("mark terminal error");

    let filtered = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::Error,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("list terminal errors");
    assert_eq!(
        filtered
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["terminal-error-session"]
    );
    assert_eq!(
        filtered.rows[0].error.as_deref(),
        Some("status=400 body=invalid")
    );

    let replaced = storage
        .replace_from_real_request(&request(
            "terminal-error-session",
            1_730_000_020,
            "recovered",
        ))
        .await
        .expect("replace recovered session");
    assert_eq!(replaced.error, None);
    let cleared = storage
        .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: None,
            filter: CacheKeepaliveSessionFilter::Error,
            cursor: None,
            limit: 10,
        })
        .await
        .expect("list after recovery");
    assert!(cleared.rows.is_empty());
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
            cycle_id: None,
            model: "claude-opus-4-1".to_owned(),
            input_tokens: 101,
            output_tokens: 23,
            cache_creation_input_tokens: 47,
            cache_creation_input_tokens_5m: 29,
            cache_creation_input_tokens_1h: 18,
            cache_read_input_tokens: 89,
            cost_micros: Some(12_345),
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

fn economics_projection(
    source_ref_id: &str,
    cycle_id: Option<Uuid>,
    ts: u64,
    hit_miss: &str,
    cost_micros: i64,
) -> RequestEventProjections {
    let mut projection = renewal_projection(source_ref_id, ts);
    let turn = projection.turn.as_mut().expect("renewal turn");
    turn.cycle_id = cycle_id;
    turn.hit_miss = hit_miss.to_owned();
    turn.cost_micros = Some(cost_micros);
    turn.cache_read_input_tokens = if hit_miss.eq_ignore_ascii_case("hit") {
        20_000
    } else {
        0
    };
    projection
        .evidence
        .push(cc_lb_storage_api::CacheKeepaliveEvidenceRow {
            evidence_id: format!("evidence-{source_ref_id}"),
            cycle_id,
            evidence_kind: "renewal".to_owned(),
            request_start_ms: ts * 1_000,
            prefix_fingerprint: Some("retained-prefix".to_owned()),
            prefix_hash: Some("retained-prefix-hash".to_owned()),
            prefix_policy_version: Some("policy-v1".to_owned()),
            prefix_schema_version: Some("schema-v1".to_owned()),
            principal_id: PRINCIPAL_ID.to_owned(),
            upstream_id: UPSTREAM_ID,
            model: turn.model.clone(),
            ttl: CacheTtl::Ttl5m,
            hit_miss: Some(hit_miss.to_owned()),
            cache_read_input_tokens: Some(turn.cache_read_input_tokens),
            cost_micros: Some(cost_micros),
            request_ref: Some(source_ref_id.to_owned()),
            event_ref: Some(source_ref_id.to_owned()),
        });
    projection
}

fn follow_up_evidence(
    source_ref_id: &str,
    ts: u64,
    model: &str,
) -> cc_lb_storage_api::CacheKeepaliveEvidenceRow {
    cc_lb_storage_api::CacheKeepaliveEvidenceRow {
        evidence_id: format!("follow-up-{source_ref_id}"),
        cycle_id: None,
        evidence_kind: "follow_up".to_owned(),
        request_start_ms: ts * 1_000 + 1_000,
        prefix_fingerprint: Some("follow-up-prefix".to_owned()),
        prefix_hash: Some("retained-prefix-hash".to_owned()),
        prefix_policy_version: Some("policy-v1".to_owned()),
        prefix_schema_version: Some("schema-v1".to_owned()),
        principal_id: PRINCIPAL_ID.to_owned(),
        upstream_id: UPSTREAM_ID,
        model: model.to_owned(),
        ttl: CacheTtl::Ttl5m,
        hit_miss: Some("hit".to_owned()),
        cache_read_input_tokens: Some(20_000),
        cost_micros: Some(0),
        request_ref: Some(format!("follow-up-request-{source_ref_id}")),
        event_ref: Some(format!("follow-up-event-{source_ref_id}")),
    }
}

struct EconomicsTurnParams<'a> {
    session_key_hash: &'a str,
    cycle_id: Uuid,
    source_ref_id: &'a str,
    ts: u64,
    hit_miss: &'a str,
    cost_micros: i64,
    renewal_tokens: u64,
    with_follow_up: bool,
}

async fn append_economics_turn(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    params: EconomicsTurnParams<'_>,
) {
    let mut projection = economics_projection(
        params.source_ref_id,
        Some(params.cycle_id),
        params.ts,
        params.hit_miss,
        params.cost_micros,
    );
    {
        let turn = projection.turn.as_mut().expect("renewal turn");
        turn.session_key_hash = params.session_key_hash.to_owned();
        turn.cache_read_input_tokens = params.renewal_tokens;
    }
    let prefix_hash = format!("prefix-hash-{}", params.source_ref_id);
    projection.evidence[0].cache_read_input_tokens = Some(params.renewal_tokens);
    projection.evidence[0].prefix_hash = Some(prefix_hash.clone());
    if params.with_follow_up {
        let model = projection
            .turn
            .as_ref()
            .expect("renewal turn")
            .model
            .clone();
        let mut follow_up = follow_up_evidence(params.source_ref_id, params.ts, &model);
        follow_up.prefix_hash = Some(prefix_hash);
        projection.evidence.push(follow_up);
    }
    storage
        .append_request_event_with_projections(&event(params.source_ref_id, params.ts), &projection)
        .await
        .expect("persist economics turn");
}

#[tokio::test]
async fn summary_aggregate_observes_complete_unfollowed_terminal_loss() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_074);
    let mut replace = request("renewed-session", 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    let session = storage
        .replace_from_real_request(&replace)
        .await
        .expect("create terminal cycle");
    for (id, ts, outcome, cost) in [
        ("terminal-miss", 1_001, "miss", 15_000),
        ("terminal-hit", 1_002, "HiT", 6_000),
    ] {
        storage
            .append_request_event_with_projections(
                &event(id, ts),
                &economics_projection(id, Some(cycle_id), ts, outcome, cost),
            )
            .await
            .expect("persist complete renewal evidence");
    }
    storage
        .mark_cache_keepalive_terminal(
            &session.session_key_hash,
            session.generation,
            CacheKeepaliveTerminalReason::MaxRefreshes,
            None,
            1_003,
        )
        .await
        .expect("finish cycle without follow-up");

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate terminal retained economics");
    assert_eq!(aggregate.renewing_now, 0);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.retained_from_ms, Some(1_001_000));
    assert_eq!(aggregate.evidence_status, "observed");
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0),
        "no follow-up means zero avoided tokens"
    );
}

#[tokio::test]
async fn summary_aggregate_active_complete_unfollowed_hit_and_miss_are_pending() {
    let (_temp_dir, storage) = storage().await;
    for (index, outcome, cost) in [(1_u128, "hit", 6_000_i64), (2, "miss", 15_000)] {
        let session_key_hash = format!("active-{outcome}");
        let cycle_id = Uuid::from_u128(1_074 + index);
        let mut replace = request(&session_key_hash, 1_000, "agent-in-turn");
        replace.cycle_id = cycle_id;
        let session = storage
            .replace_from_real_request(&replace)
            .await
            .expect("create active cycle");
        let source_ref_id = format!("active-{outcome}-renewal");
        let mut projection = economics_projection(
            &source_ref_id,
            Some(cycle_id),
            1_001 + index as u64,
            outcome,
            cost,
        );
        projection
            .turn
            .as_mut()
            .expect("renewal turn")
            .session_key_hash = session.session_key_hash.clone();
        projection.decision.session_key_hash = Some(session.session_key_hash.clone());
        storage
            .append_request_event_with_projections(
                &event(&source_ref_id, 1_001 + index as u64),
                &projection,
            )
            .await
            .expect("persist active renewal evidence");
    }

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate active pending economics");
    assert_eq!(aggregate.renewing_now, 2);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "pending");
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0)
    );
}

#[tokio::test]
async fn summary_aggregate_active_complete_follow_up_is_observed() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_076);
    let mut replace = request("active-follow-up", 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    let session = storage
        .replace_from_real_request(&replace)
        .await
        .expect("create active cycle");
    let source_ref_id = "active-follow-up-renewal";
    let ts = 1_001;
    let mut projection = economics_projection(source_ref_id, Some(cycle_id), ts, "hit", 6_000);
    let model = projection
        .turn
        .as_ref()
        .expect("renewal turn")
        .model
        .clone();
    projection
        .turn
        .as_mut()
        .expect("renewal turn")
        .session_key_hash = session.session_key_hash.clone();
    projection.decision.session_key_hash = Some(session.session_key_hash.clone());
    projection
        .evidence
        .push(follow_up_evidence(source_ref_id, ts, &model));
    storage
        .append_request_event_with_projections(&event(source_ref_id, ts), &projection)
        .await
        .expect("persist active proven follow-up");

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate active observed economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 6_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "observed");
    assert_eq!(aggregate.groups.len(), 1);
    assert_eq!(aggregate.groups[0].avoided_cache_read_tokens, 20_000);
}

#[tokio::test]
async fn summary_aggregate_active_follow_up_credits_hits_but_not_misses() {
    let (_temp_dir, storage) = storage().await;
    for (index, outcome, cost) in [(1_u128, "hit", 6_000_i64), (2, "miss", 15_000)] {
        let session_key_hash = format!("active-follow-up-{outcome}");
        let source_ref_id = format!("active-follow-up-{outcome}-renewal");
        let cycle_id = Uuid::from_u128(1_080 + index);
        let mut replace = request(&session_key_hash, 1_000, "agent-in-turn");
        replace.cycle_id = cycle_id;
        storage
            .replace_from_real_request(&replace)
            .await
            .expect("create follow-up cycle");
        append_economics_turn(
            &storage,
            EconomicsTurnParams {
                session_key_hash: &session_key_hash,
                cycle_id,
                source_ref_id: &source_ref_id,
                ts: 1_001 + index as u64,
                hit_miss: outcome,
                cost_micros: cost,
                renewal_tokens: 20_000,
                with_follow_up: true,
            },
        )
        .await;
    }

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate hit and miss follow-up economics");
    assert_eq!(aggregate.renewing_now, 2);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "observed");
    assert_eq!(aggregate.groups.len(), 1);
    assert_eq!(aggregate.groups[0].avoided_cache_read_tokens, 20_000);
}

#[tokio::test]
async fn summary_aggregate_active_same_cycle_hit_then_miss_shared_follow_up_credits_neither() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_081);
    let session_key_hash = "active-same-cycle-hit-miss";
    let mut replace = request(session_key_hash, 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("create same-cycle follow-up cycle");

    let shared_prefix_hash = "same-cycle-shared-prefix";
    let mut hit =
        economics_projection("active-same-cycle-hit", Some(cycle_id), 1_001, "hit", 6_000);
    hit.turn
        .as_mut()
        .expect("hit renewal turn")
        .session_key_hash = session_key_hash.to_owned();
    hit.decision.session_key_hash = Some(session_key_hash.to_owned());
    hit.evidence[0].prefix_hash = Some(shared_prefix_hash.to_owned());
    storage
        .append_request_event_with_projections(&event("active-same-cycle-hit", 1_001), &hit)
        .await
        .expect("persist same-cycle hit renewal");

    let mut miss = economics_projection(
        "active-same-cycle-miss",
        Some(cycle_id),
        1_002,
        "miss",
        15_000,
    );
    miss.turn
        .as_mut()
        .expect("miss renewal turn")
        .session_key_hash = session_key_hash.to_owned();
    miss.turn
        .as_mut()
        .expect("miss renewal turn")
        .cache_read_input_tokens = 20_000;
    miss.decision.session_key_hash = Some(session_key_hash.to_owned());
    miss.evidence[0].prefix_hash = Some(shared_prefix_hash.to_owned());
    miss.evidence[0].cache_read_input_tokens = Some(20_000);
    let model = miss.turn.as_ref().expect("miss renewal turn").model.clone();
    let mut follow_up = follow_up_evidence("active-same-cycle-miss", 1_002, &model);
    follow_up.prefix_hash = Some(shared_prefix_hash.to_owned());
    miss.evidence.push(follow_up);
    storage
        .append_request_event_with_projections(&event("active-same-cycle-miss", 1_002), &miss)
        .await
        .expect("persist same-cycle miss renewal and follow-up");

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate same-cycle hit then miss economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "observed");
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0),
        "the latest miss must suppress avoided-token credit for the shared follow-up"
    );
}

#[tokio::test]
async fn summary_aggregate_active_latest_unresolved_keeps_older_observed_turn_pending() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_077);
    let mut replace = request("active-multi-turn", 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("create active multi-turn cycle");
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "active-multi-turn",
            cycle_id,
            source_ref_id: "active-multi-turn-old",
            ts: 1_001,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 20_000,
            with_follow_up: false,
        },
    )
    .await;
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "active-multi-turn",
            cycle_id,
            source_ref_id: "active-multi-turn-latest",
            ts: 1_002,
            hit_miss: "miss",
            cost_micros: 15_000,
            renewal_tokens: 20_000,
            with_follow_up: false,
        },
    )
    .await;

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate active multi-turn pending economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "pending");
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0)
    );
}

#[tokio::test]
async fn summary_aggregate_multiple_follow_ups_selects_first_qualifying_follow_up() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_078);
    let mut replace = request("active-multiple-follow-ups", 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("create active first-qualifying follow-up cycle");
    // Each follow-up carries its renewal's prefix hash; the cycle-wide group
    // intentionally reports the first qualifying follow-up's renewal tokens.
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "active-multiple-follow-ups",
            cycle_id,
            source_ref_id: "active-multiple-follow-ups-first",
            ts: 1_001,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 10_000,
            with_follow_up: true,
        },
    )
    .await;
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "active-multiple-follow-ups",
            cycle_id,
            source_ref_id: "active-multiple-follow-ups-latest",
            ts: 1_002,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 30_000,
            with_follow_up: true,
        },
    )
    .await;

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate multiple follow-up pending economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 12_000);
    assert_eq!(aggregate.successful_hits, 2);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "pending");
    assert_eq!(aggregate.groups.len(), 1);
    assert_eq!(aggregate.groups[0].avoided_cache_read_tokens, 10_000);
}

#[tokio::test]
async fn summary_aggregate_active_two_follow_ups_selects_earliest_follow_up_renewal() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(1_082);
    let session_key_hash = "active-two-follow-ups";
    let mut replace = request(session_key_hash, 1_000, "agent-in-turn");
    replace.cycle_id = cycle_id;
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("create two-follow-up cycle");

    // F1 matches only the hit R1; F2 matches only the miss R2. The selected
    // follow-up is the earliest one, so only R1 contributes avoided tokens.
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash,
            cycle_id,
            source_ref_id: "active-two-follow-ups-r1",
            ts: 1_001,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 20_000,
            with_follow_up: true,
        },
    )
    .await;
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash,
            cycle_id,
            source_ref_id: "active-two-follow-ups-r2",
            ts: 1_002,
            hit_miss: "miss",
            cost_micros: 15_000,
            renewal_tokens: 20_000,
            with_follow_up: true,
        },
    )
    .await;

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate two-follow-up economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 1);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "pending");
    assert_eq!(aggregate.groups.len(), 1);
    assert_eq!(aggregate.groups[0].avoided_cache_read_tokens, 20_000);
}

#[tokio::test]
async fn summary_aggregate_historical_observed_turn_does_not_clear_active_pending() {
    let (_temp_dir, storage) = storage().await;
    let historical_cycle_id = Uuid::from_u128(1_079);
    let current_cycle_id = Uuid::from_u128(1_080);
    let mut historical = request("historical-active-pending", 1_000, "agent-in-turn");
    historical.cycle_id = historical_cycle_id;
    storage
        .replace_from_real_request(&historical)
        .await
        .expect("create historical cycle");
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "historical-active-pending",
            cycle_id: historical_cycle_id,
            source_ref_id: "historical-active-pending-old",
            ts: 1_001,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 20_000,
            with_follow_up: false,
        },
    )
    .await;
    let mut current = request("historical-active-pending", 1_005, "agent-in-turn");
    current.cycle_id = current_cycle_id;
    storage
        .replace_from_real_request(&current)
        .await
        .expect("replace with current cycle");
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "historical-active-pending",
            cycle_id: current_cycle_id,
            source_ref_id: "historical-active-pending-current",
            ts: 1_006,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 20_000,
            with_follow_up: false,
        },
    )
    .await;

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 2_000_000)
        .await
        .expect("aggregate historical plus active pending economics");
    assert_eq!(aggregate.renewing_now, 1);
    assert_eq!(aggregate.total_spent_micros, 12_000);
    assert_eq!(aggregate.successful_hits, 2);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.evidence_status, "pending");
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0)
    );
}

#[tokio::test]
async fn summary_aggregate_empty_has_no_economics_evidence() {
    let (_temp_dir, storage) = storage().await;
    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 0)
        .await
        .expect("aggregate empty history");
    assert_eq!(aggregate.total_spent_micros, 0);
    assert_eq!(aggregate.successful_hits, 0);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert_eq!(aggregate.retained_from_ms, None);
    assert_eq!(aggregate.evidence_status, "none");
    assert!(aggregate.groups.is_empty());
}

#[tokio::test]
async fn summary_aggregate_legacy_and_incomplete_evidence_remain_unknown() {
    for missing in ["evidence", "cycle", "hash", "policy", "schema"] {
        let (_temp_dir, storage) = storage().await;
        let mut projection = economics_projection(
            "incomplete-miss",
            Some(Uuid::from_u128(1_074)),
            1_000,
            "miss",
            15_000,
        );
        match missing {
            "evidence" => {
                projection.evidence.clear();
                projection.turn.as_mut().expect("legacy turn").cycle_id = None;
            }
            "cycle" => projection.evidence[0].cycle_id = None,
            "hash" => projection.evidence[0].prefix_hash = None,
            "policy" => projection.evidence[0].prefix_policy_version = None,
            "schema" => projection.evidence[0].prefix_schema_version = None,
            _ => unreachable!(),
        }
        storage
            .append_request_event_with_projections(&event("incomplete-miss", 1_000), &projection)
            .await
            .expect("persist incomplete evidence control");
        let aggregate = storage
            .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 0)
            .await
            .expect("aggregate incomplete evidence");
        assert_eq!(aggregate.total_spent_micros, 15_000, "{missing}");
        assert_eq!(aggregate.successful_hits, 0, "{missing}");
        assert_eq!(aggregate.unknown_cycles, 1, "{missing}");
        assert_eq!(aggregate.evidence_status, "unknown", "{missing}");
        assert_eq!(
            aggregate.retained_from_ms,
            (missing != "evidence").then_some(1_000_000),
            "{missing}"
        );
        assert!(
            aggregate
                .groups
                .iter()
                .all(|group| group.avoided_cache_read_tokens == 0),
            "{missing}"
        );
    }
}

#[tokio::test]
async fn summary_aggregate_counts_only_hits_and_keeps_miss_and_zero_costs() {
    let (_temp_dir, storage) = storage().await;
    for (id, ts, outcome, cost) in [
        ("paid-hit", 1_000, "hit", 6_000),
        ("free-hit", 1_001, "HiT", 0),
        ("paid-miss", 1_002, "miss", 15_000),
    ] {
        storage
            .append_request_event_with_projections(
                &event(id, ts),
                &economics_projection(id, Some(Uuid::from_u128(1_074)), ts, outcome, cost),
            )
            .await
            .expect("persist mixed renewal outcomes");
    }
    let aggregate = storage
        .read_cache_keepalive_summary_aggregate(PRINCIPAL_ID, 0)
        .await
        .expect("aggregate mixed renewal outcomes");
    assert_eq!(aggregate.total_spent_micros, 21_000);
    assert_eq!(aggregate.successful_hits, 2);
    assert_eq!(aggregate.unknown_cycles, 0);
    assert!(
        aggregate
            .groups
            .iter()
            .all(|group| group.avoided_cache_read_tokens == 0)
    );
}

#[tokio::test]
async fn scalar_session_reads_project_current_cycle_and_null_decision_cycle() {
    let (_temp_dir, storage) = storage().await;
    let mut replace = request("scalar-cycle-session", 1_000, "agent-in-turn");
    replace.cycle_id = Uuid::from_u128(1_074);
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("create first cycle");
    replace.cycle_id = Uuid::from_u128(1_075);
    replace.now_unix_secs = 1_001;
    storage
        .replace_from_real_request(&replace)
        .await
        .expect("replace current cycle");
    insert_decision(&storage, PRINCIPAL_ID, "scalar-cycle-decision", 1_002).await;

    let page = storage
        .list_cache_keepalive_sessions(&all_query(10))
        .await
        .expect("list scalar cycle projections");
    let summary = storage
        .read_cache_keepalive_summary_input(PRINCIPAL_ID, 0)
        .await
        .expect("read scalar summary projection");
    let session = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "scalar-cycle-session")
        .await
        .expect("read scalar session detail")
        .expect("session exists");
    let decision = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "scalar-cycle-decision")
        .await
        .expect("read scalar decision detail")
        .expect("decision exists");
    for row in page
        .rows
        .iter()
        .chain(&summary.sessions)
        .chain([&session, &decision])
    {
        let value = serde_json::to_value(row).expect("serialize scalar projection");
        let expected = if row.is_decision() {
            serde_json::Value::Null
        } else {
            serde_json::json!(replace.cycle_id)
        };
        assert_eq!(value.get("cycle_id"), Some(&expected), "{}", row.id);
    }
}

#[tokio::test]
async fn reconstructed_purged_session_scopes_refresh_count_and_reason_to_selected_cycle() {
    let (_temp_dir, storage) = storage().await;
    let old_cycle = Uuid::from_u128(2_001);
    let new_cycle = Uuid::from_u128(2_002);

    for index in 0..12_u64 {
        let source_ref = format!("purged-old-{index}");
        let mut proj =
            economics_projection(&source_ref, Some(old_cycle), 1_000 + index, "hit", 6_000);
        proj.turn.as_mut().expect("turn").session_key_hash = "purged-multi-cycle".to_owned();
        proj.decision.session_key_hash = Some("purged-multi-cycle".to_owned());
        storage
            .append_request_event_with_projections(&event(&source_ref, 1_000 + index), &proj)
            .await
            .expect("append old cycle turn");
    }

    let mut newest_proj =
        economics_projection("purged-new-0", Some(new_cycle), 2_000, "hit", 6_000);
    newest_proj.turn.as_mut().expect("turn").session_key_hash = "purged-multi-cycle".to_owned();
    newest_proj.decision.session_key_hash = Some("purged-multi-cycle".to_owned());
    storage
        .append_request_event_with_projections(&event("purged-new-0", 2_000), &newest_proj)
        .await
        .expect("append new cycle turn");

    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "purged-multi-cycle")
        .await
        .expect("lookup reconstructed session")
        .expect("reconstructed session exists");
    assert_eq!(item.cycle_id, Some(new_cycle));
    assert_eq!(item.refresh_count, Some(1));
    assert_eq!(
        item.terminal_reason,
        Some(cc_lb_storage_api::CacheKeepaliveTerminalReason::Expired)
    );
    assert_eq!(item.reason, "TTL expired before follow-up");
}

#[tokio::test]
async fn purged_session_reconstructs_display_reason_from_turn() {
    let (_temp_dir, storage) = storage().await;
    let display_reason = "agent-in-turn (tool_use: `Bash`) — first renewal in 4m 30s";
    let session = storage
        .replace_from_real_request(&request("purged-display-reason", 100, display_reason))
        .await
        .expect("insert display-reason session");
    let mut projection = economics_projection("purged-display-renewal", None, 200, "hit", 6_000);
    projection
        .turn
        .as_mut()
        .expect("renewal turn")
        .session_key_hash = session.session_key_hash.clone();
    projection.decision.session_key_hash = Some(session.session_key_hash.clone());
    storage
        .append_request_event_with_projections(&event("purged-display-renewal", 200), &projection)
        .await
        .expect("append renewal turn");

    let persisted_reason: String = sqlx::query_scalar(
        "SELECT display_reason FROM cache_keepalive_turns WHERE source_ref_id = ?",
    )
    .bind("purged-display-renewal")
    .fetch_one(storage.pool())
    .await
    .expect("read persisted display reason");
    assert_eq!(persisted_reason, display_reason);

    assert_eq!(
        storage
            .purge_cache_keepalive_expired(401)
            .await
            .expect("purge expired session"),
        1
    );
    let page = storage
        .list_cache_keepalive_sessions(&all_query(10))
        .await
        .expect("list reconstructed session");
    let reconstructed = page
        .rows
        .iter()
        .find(|row| row.id == "purged-display-reason")
        .expect("reconstructed purged session");
    assert_eq!(
        reconstructed.display_reason.as_deref(),
        Some(display_reason)
    );
    assert_eq!(reconstructed.reason, "TTL expired before follow-up");
}

#[tokio::test]
async fn unlisted_aggregate_counts_only_turns_without_renewal_evidence() {
    let (_temp_dir, storage) = storage().await;
    let cycle_id = Uuid::from_u128(9_001);
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "reconstructable-purged",
            cycle_id,
            source_ref_id: "reconstructable-purged-turn",
            ts: 10_000,
            hit_miss: "hit",
            cost_micros: 7_000,
            renewal_tokens: 20_000,
            with_follow_up: true,
        },
    )
    .await;
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "unlisted-purged",
            cycle_id,
            source_ref_id: "unlisted-purged-turn",
            ts: 10_001,
            hit_miss: "hit",
            cost_micros: 11_000,
            renewal_tokens: 20_000,
            with_follow_up: false,
        },
    )
    .await;
    sqlx::query("DELETE FROM cache_keepalive_evidence WHERE event_ref = ?")
        .bind("unlisted-purged-turn")
        .execute(storage.pool())
        .await
        .expect("delete unlisted renewal evidence");

    let aggregate = storage
        .read_cache_keepalive_summary_aggregate_for_horizon(PRINCIPAL_ID, 0, None)
        .await
        .expect("read unlisted aggregate");
    assert_eq!(aggregate.unlisted_sessions, 1);
    assert_eq!(aggregate.unlisted_renewals, 1);
    assert_eq!(aggregate.unlisted_spent_micros, 11_000);
}

#[tokio::test]
async fn list_horizon_membership_uses_in_horizon_turn_timestamp() {
    let (_temp_dir, storage) = storage().await;
    storage
        .replace_from_real_request(&request("old-last-message", 100, "scheduled"))
        .await
        .expect("seed old session");
    let mut projection = economics_projection("inside-horizon", None, 200, "hit", 6_000);
    projection.turn.as_mut().expect("turn").session_key_hash = "old-last-message".to_owned();
    projection.decision.session_key_hash = Some("old-last-message".to_owned());
    storage
        .append_request_event_with_projections(&event("inside-horizon", 200), &projection)
        .await
        .expect("append in-horizon turn");
    let query = CacheKeepaliveSessionListQuery {
        principal_id: PRINCIPAL_ID.to_owned(),
        horizon_start_ms: Some(150_000),
        filter: CacheKeepaliveSessionFilter::All,
        cursor: None,
        limit: 10,
    };
    let page = storage
        .list_cache_keepalive_sessions(&query)
        .await
        .expect("list horizon membership");
    assert_eq!(
        page.rows
            .iter()
            .filter(|row| row.id == "old-last-message")
            .count(),
        1
    );
}

#[tokio::test]
async fn reconstructed_followed_purged_session_is_cancelled() {
    let (_temp_dir, storage) = storage().await;
    append_economics_turn(
        &storage,
        EconomicsTurnParams {
            session_key_hash: "purged-followed-terminal",
            cycle_id: Uuid::from_u128(9_002),
            source_ref_id: "purged-followed-terminal-turn",
            ts: 10_000,
            hit_miss: "hit",
            cost_micros: 6_000,
            renewal_tokens: 20_000,
            with_follow_up: true,
        },
    )
    .await;
    let item = storage
        .get_cache_keepalive_list_item(PRINCIPAL_ID, "purged-followed-terminal")
        .await
        .expect("lookup reconstructed followed session")
        .expect("reconstructed followed session");
    assert_eq!(
        item.terminal_reason,
        Some(cc_lb_storage_api::CacheKeepaliveTerminalReason::Cancelled)
    );
}

#[tokio::test]
async fn status_filters_match_capped_cancelled_and_failed_terminal_reasons() {
    let (_temp_dir, storage) = storage().await;
    for (id, reason) in [
        (
            "filter-capped",
            cc_lb_storage_api::CacheKeepaliveTerminalReason::MaxRefreshes,
        ),
        (
            "filter-cancelled",
            cc_lb_storage_api::CacheKeepaliveTerminalReason::Cancelled,
        ),
        (
            "filter-failed",
            cc_lb_storage_api::CacheKeepaliveTerminalReason::DispatchError,
        ),
    ] {
        storage
            .replace_from_real_request(&request(id, 1_000, "terminal"))
            .await
            .expect("seed filter session");
        let mut projection = economics_projection(id, None, 1_001, "hit", 1_000);
        projection
            .turn
            .as_mut()
            .expect("filter turn")
            .session_key_hash = id.to_owned();
        projection.decision.session_key_hash = Some(id.to_owned());
        storage
            .append_request_event_with_projections(&event(id, 1_001), &projection)
            .await
            .expect("append filter turn");
        storage
            .mark_cache_keepalive_terminal(id, 1, reason, None, 1_002)
            .await
            .expect("terminalize filter session");
    }
    sqlx::query("UPDATE cache_keepalive_sessions SET refresh_count = 1")
        .execute(storage.pool())
        .await
        .expect("set filter refresh counts");
    for (filter, expected_id) in [
        (CacheKeepaliveSessionFilter::Capped, "filter-capped"),
        (CacheKeepaliveSessionFilter::Cancelled, "filter-cancelled"),
        (CacheKeepaliveSessionFilter::Failed, "filter-failed"),
    ] {
        let page = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: PRINCIPAL_ID.to_owned(),
                horizon_start_ms: None,
                filter,
                cursor: None,
                limit: 10,
            })
            .await
            .expect("list status filter");
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec![expected_id]
        );
    }
}

#[tokio::test]
async fn cancelled_sessions_require_a_renewal_inside_the_selected_horizon() {
    let (_directory, storage) = storage().await;
    for (id, ts) in [("old-renewal", 100), ("recent-renewal", 200)] {
        storage
            .replace_from_real_request(&request(id, 210, "first renewal in 4m 30s"))
            .await
            .expect("seed recent session");
        let mut projection = economics_projection(id, None, ts, "hit", 6_000);
        projection.turn.as_mut().expect("turn").session_key_hash = id.to_owned();
        projection.decision.session_key_hash = Some(id.to_owned());
        storage
            .append_request_event_with_projections(&event(id, ts), &projection)
            .await
            .expect("append renewal");
        storage
            .mark_cache_keepalive_terminal(
                id,
                1,
                CacheKeepaliveTerminalReason::Cancelled,
                None,
                211,
            )
            .await
            .expect("cancel session");
    }
    // A nonzero lifetime refresh counter must not override the selected horizon.
    sqlx::query("UPDATE cache_keepalive_sessions SET refresh_count = 1")
        .execute(storage.pool())
        .await
        .expect("set lifetime renewal count");
    for filter in [
        CacheKeepaliveSessionFilter::Visible,
        CacheKeepaliveSessionFilter::Tracked,
        CacheKeepaliveSessionFilter::Cancelled,
    ] {
        let query = CacheKeepaliveSessionListQuery {
            principal_id: PRINCIPAL_ID.to_owned(),
            horizon_start_ms: Some(150_000),
            filter,
            cursor: None,
            limit: 10,
        };
        let page = storage
            .list_cache_keepalive_sessions(&query)
            .await
            .expect("24h list");
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["recent-renewal"],
            "{filter:?}"
        );
        let all = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                horizon_start_ms: None,
                ..query
            })
            .await
            .expect("all retained history");
        assert_eq!(all.rows.len(), 2, "{filter:?}");
    }
    let summary = storage
        .read_cache_keepalive_summary_aggregate_for_horizon(PRINCIPAL_ID, 200_000, Some(150_000))
        .await
        .expect("selected-horizon summary");
    assert_eq!(summary.sessions_last_5m, 1);
    assert_eq!(summary.total_spent_micros, 6_000);
    let all = storage
        .read_cache_keepalive_summary_aggregate_for_horizon(PRINCIPAL_ID, 200_000, None)
        .await
        .expect("all-history summary");
    assert_eq!(all.sessions_last_5m, 2);
    assert_eq!(all.total_spent_micros, 12_000);
}

#[tokio::test]
async fn cancelled_session_read_model_preserves_each_cause_and_legacy_null() {
    let (_directory, storage) = storage().await;
    for (cause, expected) in [
        (Some("new_request"), "Cancelled: a new request arrived"),
        (
            Some("user_turn_detected"),
            "Cancelled: the agent is waiting for the user",
        ),
        (
            Some("snapshot_too_large"),
            "Cancelled: the request snapshot is too large",
        ),
        (
            Some("cached_prefix_too_small"),
            "Cancelled: the cached prefix is too small",
        ),
        (
            Some("zero_token_renewal_unsupported"),
            "Cancelled: zero-token renewals are unsupported",
        ),
        (None, "Cancelled"),
    ] {
        let id = cause.unwrap_or("legacy");
        storage
            .replace_from_real_request(&request(id, 210, "agent-in-turn — first renewal in 4m 30s"))
            .await
            .expect("seed session");
        assert!(
            storage
                .cancel_cache_keepalive_fenced(
                    id,
                    None,
                    210,
                    CacheKeepaliveTerminalReason::Cancelled,
                    cause,
                    211,
                )
                .await
                .expect("cancel session")
        );
        let item = storage
            .get_cache_keepalive_list_item(PRINCIPAL_ID, id)
            .await
            .expect("read cancelled item")
            .expect("cancelled item");
        assert_eq!(item.cancel_reason.as_deref(), cause);
        assert_eq!(item.reason, expected);
        let session = storage
            .get_cache_keepalive_session(id)
            .await
            .expect("read durable cancellation")
            .expect("durable session");
        assert_eq!(session.cancel_reason.as_deref(), cause);
    }
}
