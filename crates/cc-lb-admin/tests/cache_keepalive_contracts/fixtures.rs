use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveHitRefreshRequest,
    CacheKeepaliveProjectionStore, CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheKeepaliveTurnRow, CacheTtl, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use serde_json::{Value, json};
use uuid::Uuid;

const UPSTREAM_ID: Uuid = Uuid::from_u128(0xa11ce);
const SEEDED_AT_UNIX_SECS: u64 = 1_730_000_000;

fn config_snapshot() -> CacheKeepaliveConfigSnapshot {
    CacheKeepaliveConfigSnapshot {
        refresh_lead_time_5m_secs: 30,
        refresh_lead_time_1h_secs: 300,
        max_refreshes_per_session: 12,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 524_288,
    }
}

#[derive(Clone)]
pub struct CacheKeepaliveSessionSeed {
    pub session_key_hash: String,
    pub ttl: CacheTtl,
    pub terminal_reason: Option<CacheKeepaliveTerminalReason>,
}

#[derive(Clone)]
pub struct CacheKeepaliveContractSeed {
    pub sessions: Vec<CacheKeepaliveSessionSeed>,
    pub projections: Vec<(RequestEvent, RequestEventProjections)>,
}

pub fn cache_keepalive_contract_seed(principal_id: &str) -> CacheKeepaliveContractSeed {
    let sessions = [
        ("a1f39c2b7e04", CacheTtl::Ttl5m, None),
        ("7b204de1c83f", CacheTtl::Ttl5m, None),
        (
            "f4b71a0c9d52",
            CacheTtl::Ttl5m,
            Some(CacheKeepaliveTerminalReason::MaxRefreshes),
        ),
        (
            "capped-max-duration-4h",
            CacheTtl::Ttl1h,
            Some(CacheKeepaliveTerminalReason::MaxDuration),
        ),
        (
            "0b33e9f71a2c",
            CacheTtl::Ttl1h,
            Some(CacheKeepaliveTerminalReason::Expired),
        ),
        (
            "error-overlaps-reason",
            CacheTtl::Ttl5m,
            Some(CacheKeepaliveTerminalReason::DispatchError),
        ),
    ]
    .into_iter()
    .map(
        |(session_key_hash, ttl, terminal_reason)| CacheKeepaliveSessionSeed {
            session_key_hash: session_key_hash.to_owned(),
            ttl,
            terminal_reason,
        },
    )
    .collect();

    let projections = (1..=3)
        .map(|turn_number| {
            let source_ref_id = format!("f4b71a0c9d52:{turn_number}");
            let timestamp = SEEDED_AT_UNIX_SECS + turn_number;
            (
                RequestEvent {
                    ts: timestamp,
                    ts_ms: Some(timestamp * 1_000),
                    request_id: format!("cache-keepalive-turn-{turn_number}"),
                    source_kind: Some("renewal".to_owned()),
                    source_ref_id: Some(source_ref_id.clone()),
                    event_id: Some(format!("cache-keepalive-event-{turn_number}")),
                    principal_id: Some(principal_id.to_owned()),
                    upstream_id: Some(UPSTREAM_ID),
                    upstream_name: Some("anthropic-primary".to_owned()),
                    model: Some("claude-sonnet-4-5".to_owned()),
                    status: 200,
                    duration_ms: 12,
                    ..RequestEvent::default()
                },
                RequestEventProjections {
                    turn: Some(CacheKeepaliveTurnRow {
                        source_ref_id: source_ref_id.clone(),
                        session_key_hash: "f4b71a0c9d52".to_owned(),
                        principal_id: principal_id.to_owned(),
                        accounting_key_id: None,
                        upstream_id: UPSTREAM_ID,
                        model: "claude-sonnet-4-5".to_owned(),
                        input_tokens: 8_000,
                        output_tokens: 20,
                        cache_creation_input_tokens: 0,
                        cache_creation_input_tokens_5m: 0,
                        cache_creation_input_tokens_1h: 0,
                        cache_read_input_tokens: 8_000,
                        cost_micros: 14_400,
                        hit_miss: "hit".to_owned(),
                        ts: timestamp,
                    }),
                    decision: CacheKeepaliveDecisionRow {
                        source_ref_id,
                        principal_id: principal_id.to_owned(),
                        session_key_hash: Some("f4b71a0c9d52".to_owned()),
                        upstream_id: UPSTREAM_ID,
                        decision: "reschedule".to_owned(),
                        reason: "cache_hit".to_owned(),
                        error: None,
                        generation: turn_number,
                        ttl: CacheTtl::Ttl5m,
                        config_snapshot: Some(config_snapshot()),
                        last_message_at_ms: timestamp * 1_000,
                        ts: timestamp,
                    },
                },
            )
        })
        .collect();

    CacheKeepaliveContractSeed {
        sessions,
        projections,
    }
}

pub async fn seed_cache_keepalive_contract_rows(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
) {
    let seed = cache_keepalive_contract_seed(principal_id);

    for session in seed.sessions {
        let now = if session.session_key_hash == "capped-max-duration-4h" {
            SEEDED_AT_UNIX_SECS - 8 * 24 * 60 * 60
        } else {
            SEEDED_AT_UNIX_SECS
        };
        let display_reason = if session.session_key_hash == "7b204de1c83f" {
            "agent-in-turn (tool_use: `edit_file`) — first renewal in 4m 30s"
        } else {
            "agent-in-turn (tool_use: `bash`)"
        };
        let record = storage
            .replace_from_real_request(&CacheKeepaliveReplaceRequest {
                session_key_hash: session.session_key_hash,
                principal_id: principal_id.to_owned(),
                accounting_key_id: None,
                upstream_id: UPSTREAM_ID,
                cache_anchor_at_unix_secs: now,
                ttl: session.ttl,
                run_at_unix_secs: now + 270,
                expires_at_unix_secs: now + 300,
                encrypted_payload: vec![1],
                display_reason: display_reason.to_owned(),
                config_snapshot: config_snapshot(),
                now_unix_secs: now,
            })
            .await
            .expect("seed cache keepalive session");

        if let Some(reason) = session.terminal_reason {
            assert!(
                storage
                    .mark_cache_keepalive_terminal(
                        &record.session_key_hash,
                        record.generation,
                        reason,
                        now + 1,
                    )
                    .await
                    .expect("terminalize seeded cache keepalive session")
            );
        }
    }

    let renewed = storage
        .get_cache_keepalive_session("a1f39c2b7e04")
        .await
        .expect("load renewed session")
        .expect("renewed session exists");
    storage
        .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
            session_key_hash: renewed.session_key_hash,
            generation: renewed.generation,
            cache_anchor_at_unix_secs: SEEDED_AT_UNIX_SECS + 1,
            run_at_unix_secs: SEEDED_AT_UNIX_SECS + 271,
            expires_at_unix_secs: SEEDED_AT_UNIX_SECS + 301,
            encrypted_payload: None,
            now_unix_secs: SEEDED_AT_UNIX_SECS + 1,
        })
        .await
        .expect("renew seeded cache keepalive session");

    for (event, projections) in seed.projections {
        storage
            .append_request_event_with_projections(&event, &projections)
            .await
            .expect("seed cache keepalive turn and decision");
    }

    storage
        .append_request_event_with_projections(
            &RequestEvent {
                ts: SEEDED_AT_UNIX_SECS + 4,
                ts_ms: Some((SEEDED_AT_UNIX_SECS + 4) * 1_000),
                request_id: "not-tracked-decision".to_owned(),
                source_kind: Some("cache_keepalive_decision".to_owned()),
                source_ref_id: Some("decision-2d8077e9a1c4".to_owned()),
                event_id: Some("not-tracked-event".to_owned()),
                principal_id: Some(principal_id.to_owned()),
                upstream_id: Some(UPSTREAM_ID),
                status: 200,
                duration_ms: 12,
                ..RequestEvent::default()
            },
            &RequestEventProjections {
                turn: None,
                decision: CacheKeepaliveDecisionRow {
                    source_ref_id: "decision-2d8077e9a1c4".to_owned(),
                    principal_id: principal_id.to_owned(),
                    session_key_hash: None,
                    upstream_id: UPSTREAM_ID,
                    decision: "not_tracked".to_owned(),
                    reason: "user turn (stop_reason=end_turn)".to_owned(),
                    error: None,
                    generation: 4,
                    ttl: CacheTtl::Ttl5m,
                    config_snapshot: Some(config_snapshot()),
                    last_message_at_ms: (SEEDED_AT_UNIX_SECS + 4) * 1_000,
                    ts: SEEDED_AT_UNIX_SECS + 4,
                },
            },
        )
        .await
        .expect("seed not tracked cache keepalive decision");
}
pub fn cache_keepalive_replace_request(
    principal_id: &str,
    session_key_hash: &str,
    cache_anchor_at_unix_secs: u64,
    expires_at_unix_secs: u64,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: None,
        upstream_id: UPSTREAM_ID,
        cache_anchor_at_unix_secs,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: cache_anchor_at_unix_secs.saturating_add(270),
        expires_at_unix_secs,
        encrypted_payload: vec![1],
        display_reason: format!("fixture {session_key_hash}"),
        config_snapshot: config_snapshot(),
        now_unix_secs: cache_anchor_at_unix_secs,
    }
}

pub fn cache_keepalive_decision(
    principal_id: &str,
    source_ref_id: &str,
    last_message_at_ms: u64,
) -> CacheKeepaliveDecisionRow {
    CacheKeepaliveDecisionRow {
        source_ref_id: source_ref_id.to_owned(),
        principal_id: principal_id.to_owned(),
        session_key_hash: None,
        upstream_id: UPSTREAM_ID,
        decision: "not_tracked".to_owned(),
        reason: format!("fixture decision {source_ref_id}"),
        error: None,
        generation: 1,
        ttl: CacheTtl::Ttl5m,
        config_snapshot: Some(config_snapshot()),
        last_message_at_ms,
        ts: last_message_at_ms / 1_000,
    }
}

pub async fn seed_cache_keepalive_decision(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    source_ref_id: &str,
    last_message_at_ms: u64,
) {
    storage
        .append_cache_keepalive_decision(&cache_keepalive_decision(
            principal_id,
            source_ref_id,
            last_message_at_ms,
        ))
        .await
        .expect("seed cache keepalive decision");
}

pub async fn seed_cache_keepalive_turn(
    storage: &cc_lb_storage_sqlite::SqliteStorage,
    principal_id: &str,
    session_key_hash: &str,
    source_ref_id: &str,
    ts: u64,
    model: &str,
    cache_read_input_tokens: u64,
) {
    sqlx::query(
        "INSERT INTO cache_keepalive_turns (
            source_ref_id, session_key_hash, principal_id, accounting_key_id, upstream_id, model,
            input_tokens, output_tokens, cache_creation_input_tokens,
            cache_creation_input_tokens_5m, cache_creation_input_tokens_1h,
            cache_read_input_tokens, cost_micros, hit_miss, ts
         ) VALUES (?, ?, ?, NULL, ?, ?, 0, 0, 0, 0, 0, ?, 0, 'hit', ?)",
    )
    .bind(source_ref_id)
    .bind(session_key_hash)
    .bind(principal_id)
    .bind(UPSTREAM_ID.to_string())
    .bind(model)
    .bind(i64::try_from(cache_read_input_tokens).expect("fixture tokens fit i64"))
    .bind(i64::try_from(ts).expect("fixture timestamp fits i64"))
    .execute(storage.pool())
    .await
    .expect("seed cache keepalive turn");
}

pub fn principal_create_body_named(name: &str, enabled: bool) -> Value {
    let mut body = principal_create_body();
    body["name"] = json!(name);
    body["cache_keepalive"]["enabled"] = json!(enabled);
    body
}

pub fn principal_create_body() -> Value {
    json!({
        "name": "cache-keepalive-contract",
        "kind": "machine",
        "cache_keepalive": {
            "enabled": true,
            "refresh_lead_time_5m_secs": 30,
            "refresh_lead_time_1h_secs": 300,
            "max_refreshes_per_session": 12,
            "max_total_duration_secs": 14400,
            "snapshot_max_bytes": 524288
        }
    })
}
