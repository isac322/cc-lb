use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRecord, CacheKeepaliveDecisionRow,
    CacheKeepaliveHitRefreshRequest, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery,
    CacheKeepaliveSessionReadStore, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheKeepaliveTurnRecord, CacheKeepaliveTurnRow, CacheTtl, RequestEventProjections,
    RequestEventStore, types::RequestEvent,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

const TARGET_PRINCIPAL: &str = "keepalive-batch-principal";
const OTHER_PRINCIPAL: &str = "keepalive-batch-other-principal";

pub async fn batch_turn_reads_match_canonical_per_session_reads<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let empty = storage
            .list_cache_keepalive_turns_for_sessions(TARGET_PRINCIPAL, &[])
            .await?;
        ensure!(empty.is_empty(), "empty session hash input must return no turns");

        let fixtures = [
            turn_fixture(TurnSeed {
                source_ref_id: "session-a:2",
                session_key_hash: "session-a",
                principal_id: TARGET_PRINCIPAL,
                accounting_key_id: Some("accounting-a"),
                upstream_id: Uuid::from_u128(11),
                model: "claude-sonnet-4-5",
                input_tokens: 12,
                output_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_creation_input_tokens_5m: 4,
                cache_creation_input_tokens_1h: 5,
                cache_read_input_tokens: 6,
                cost_micros: -7,
                hit_miss: "miss",
                ts: 1_800_100_002,
            }),
            turn_fixture(TurnSeed {
                source_ref_id: "session-a:1",
                session_key_hash: "session-a",
                principal_id: TARGET_PRINCIPAL,
                accounting_key_id: None,
                upstream_id: Uuid::from_u128(12),
                model: "claude-opus-4-1",
                input_tokens: 21,
                output_tokens: 22,
                cache_creation_input_tokens: 23,
                cache_creation_input_tokens_5m: 24,
                cache_creation_input_tokens_1h: 25,
                cache_read_input_tokens: 26,
                cost_micros: 27,
                hit_miss: "hit",
                ts: 1_800_100_002,
            }),
            turn_fixture(TurnSeed {
                source_ref_id: "session-b:2",
                session_key_hash: "session-b",
                principal_id: TARGET_PRINCIPAL,
                accounting_key_id: Some("accounting-b"),
                upstream_id: Uuid::from_u128(13),
                model: "claude-haiku-3-5",
                input_tokens: 31,
                output_tokens: 32,
                cache_creation_input_tokens: 33,
                cache_creation_input_tokens_5m: 34,
                cache_creation_input_tokens_1h: 35,
                cache_read_input_tokens: 36,
                cost_micros: 37,
                hit_miss: "hit",
                ts: 1_800_100_003,
            }),
            turn_fixture(TurnSeed {
                source_ref_id: "session-b:1",
                session_key_hash: "session-b",
                principal_id: TARGET_PRINCIPAL,
                accounting_key_id: None,
                upstream_id: Uuid::from_u128(14),
                model: "claude-sonnet-4",
                input_tokens: 41,
                output_tokens: 42,
                cache_creation_input_tokens: 43,
                cache_creation_input_tokens_5m: 44,
                cache_creation_input_tokens_1h: 45,
                cache_read_input_tokens: 46,
                cost_micros: 47,
                hit_miss: "miss",
                ts: 1_800_100_001,
            }),
            turn_fixture(TurnSeed {
                source_ref_id: "foreign-session-a:1",
                session_key_hash: "session-a",
                principal_id: OTHER_PRINCIPAL,
                accounting_key_id: Some("foreign-accounting"),
                upstream_id: Uuid::from_u128(21),
                model: "foreign-model",
                input_tokens: 51,
                output_tokens: 52,
                cache_creation_input_tokens: 53,
                cache_creation_input_tokens_5m: 54,
                cache_creation_input_tokens_1h: 55,
                cache_read_input_tokens: 56,
                cost_micros: 57,
                hit_miss: "foreign",
                ts: 1_800_100_000,
            }),
            turn_fixture(TurnSeed {
                source_ref_id: "foreign-only:1",
                session_key_hash: "foreign-only",
                principal_id: OTHER_PRINCIPAL,
                accounting_key_id: None,
                upstream_id: Uuid::from_u128(22),
                model: "foreign-only-model",
                input_tokens: 61,
                output_tokens: 62,
                cache_creation_input_tokens: 63,
                cache_creation_input_tokens_5m: 64,
                cache_creation_input_tokens_1h: 65,
                cache_read_input_tokens: 66,
                cost_micros: 67,
                hit_miss: "foreign-only",
                ts: 1_800_100_004,
            }),
        ];

        for (event, projections, _) in &fixtures {
            storage
                .append_request_event_with_projections(event, projections)
                .await?;
        }

        let requested_hashes = vec![
            "session-b".to_owned(),
            "unknown".to_owned(),
            "session-a".to_owned(),
            "session-b".to_owned(),
            "foreign-only".to_owned(),
        ];
        let mut canonical_hashes = requested_hashes.clone();
        canonical_hashes.sort();
        canonical_hashes.dedup();

        let mut expected = Vec::new();
        for session_key_hash in &canonical_hashes {
            expected.extend(
                storage
                    .list_cache_keepalive_turns(TARGET_PRINCIPAL, session_key_hash)
                    .await?,
            );
        }
        canonical_sort(&mut expected);

        let mut seeded_target = fixtures
            .iter()
            .map(|(_, _, record)| record)
            .filter(|record| {
                record.principal_id == TARGET_PRINCIPAL
                    && canonical_hashes.binary_search(&record.session_key_hash).is_ok()
            })
            .cloned()
            .collect::<Vec<_>>();
        canonical_sort(&mut seeded_target);
        ensure!(
            expected == seeded_target,
            "per-session reference reads must preserve every seeded field and canonical order; expected {seeded_target:#?}, got {expected:#?}"
        );

        let actual = storage
            .list_cache_keepalive_turns_for_sessions(TARGET_PRINCIPAL, &requested_hashes)
            .await?;
        ensure!(
            actual == expected,
            "batch keepalive turn read must equal canonicalized per-session reads; expected {expected:#?}, got {actual:#?}"
        );

        Ok(())
    })
    .await
}

fn canonical_sort(turns: &mut [CacheKeepaliveTurnRecord]) {
    turns.sort_by(|left, right| {
        left.session_key_hash
            .cmp(&right.session_key_hash)
            .then_with(|| left.ts.cmp(&right.ts))
            .then_with(|| left.source_ref_id.cmp(&right.source_ref_id))
    });
}

struct TurnSeed<'a> {
    source_ref_id: &'a str,
    session_key_hash: &'a str,
    principal_id: &'a str,
    accounting_key_id: Option<&'a str>,
    upstream_id: Uuid,
    model: &'a str,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_creation_input_tokens_5m: u64,
    cache_creation_input_tokens_1h: u64,
    cache_read_input_tokens: u64,
    cost_micros: i64,
    hit_miss: &'a str,
    ts: u64,
}

fn turn_fixture(
    seed: TurnSeed<'_>,
) -> (
    RequestEvent,
    RequestEventProjections,
    CacheKeepaliveTurnRecord,
) {
    let record = CacheKeepaliveTurnRecord {
        source_ref_id: seed.source_ref_id.to_owned(),
        session_key_hash: seed.session_key_hash.to_owned(),
        principal_id: seed.principal_id.to_owned(),
        accounting_key_id: seed.accounting_key_id.map(str::to_owned),
        upstream_id: seed.upstream_id,
        model: seed.model.to_owned(),
        input_tokens: seed.input_tokens,
        output_tokens: seed.output_tokens,
        cache_creation_input_tokens: seed.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: seed.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: seed.cache_creation_input_tokens_1h,
        cache_read_input_tokens: seed.cache_read_input_tokens,
        cost_micros: seed.cost_micros,
        hit_miss: seed.hit_miss.to_owned(),
        ts: seed.ts,
    };
    let event = RequestEvent {
        ts: seed.ts,
        ts_ms: Some(seed.ts.saturating_mul(1_000)),
        request_id: format!("keepalive-batch-event-{}", seed.source_ref_id),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(seed.source_ref_id.to_owned()),
        event_id: Some(format!("keepalive-batch-event-id-{}", seed.source_ref_id)),
        principal_id: Some(seed.principal_id.to_owned()),
        upstream_id: Some(seed.upstream_id),
        upstream_name: Some("keepalive-upstream".to_owned()),
        model: Some(seed.model.to_owned()),
        status: 200,
        duration_ms: 10,
        ..RequestEvent::default()
    };
    let projections = RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: record.source_ref_id.clone(),
            session_key_hash: record.session_key_hash.clone(),
            principal_id: record.principal_id.clone(),
            accounting_key_id: record.accounting_key_id.clone(),
            upstream_id: record.upstream_id,
            model: record.model.clone(),
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cache_creation_input_tokens: record.cache_creation_input_tokens,
            cache_creation_input_tokens_5m: record.cache_creation_input_tokens_5m,
            cache_creation_input_tokens_1h: record.cache_creation_input_tokens_1h,
            cache_read_input_tokens: record.cache_read_input_tokens,
            cost_micros: record.cost_micros,
            hit_miss: record.hit_miss.clone(),
            ts: record.ts,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: format!("decision-{}", seed.source_ref_id),
            principal_id: seed.principal_id.to_owned(),
            session_key_hash: Some(seed.session_key_hash.to_owned()),
            upstream_id: seed.upstream_id,
            decision: "reschedule".to_owned(),
            reason: "conformance-fixture".to_owned(),
            error: None,
            generation: 1,
            ttl: CacheTtl::Ttl5m,
            config_snapshot: None,
            last_message_at_ms: seed.ts.saturating_mul(1_000),
            ts: seed.ts,
        },
    };

    (event, projections, record)
}

pub async fn list_detail_filters_preserve_frozen_projection_contract<B>(
    backend: Arc<B>,
) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let renewed = storage
            .replace_from_real_request(&read_request(
                "renewed-session",
                TARGET_PRINCIPAL,
                103,
                "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s",
            ))
            .await?;
        let renewed_after_hit = storage
            .reschedule_after_cache_hit(&CacheKeepaliveHitRefreshRequest {
                session_key_hash: renewed.session_key_hash,
                generation: renewed.generation,
                cache_anchor_at_unix_secs: 104,
                run_at_unix_secs: 374,
                expires_at_unix_secs: 404,
                encrypted_payload: None,
                now_unix_secs: 104,
            })
            .await?
            .expect("renewed session must reschedule");
        ensure!(
            renewed_after_hit.first_scheduled_at_unix_secs == 103
                && renewed_after_hit.cache_anchor_at_unix_secs == 104,
            "renewal must preserve the frozen message timestamp while advancing the cache anchor"
        );
        storage
            .replace_from_real_request(&read_request(
                "scheduled-session",
                TARGET_PRINCIPAL,
                106,
                "agent-in-turn",
            ))
            .await?;
        terminalize_read_session(
            storage.as_ref(),
            "capped-session",
            102,
            CacheKeepaliveTerminalReason::MaxRefreshes,
        )
        .await?;
        terminalize_read_session(
            storage.as_ref(),
            "expired-session",
            101,
            CacheKeepaliveTerminalReason::Expired,
        )
        .await?;
        terminalize_read_session(
            storage.as_ref(),
            "max-duration-session",
            100,
            CacheKeepaliveTerminalReason::MaxDuration,
        )
        .await?;
        storage
            .replace_from_real_request(&read_request(
                "foreign-session",
                OTHER_PRINCIPAL,
                110,
                "foreign",
            ))
            .await?;

        append_read_projection(
            storage.as_ref(),
            "not-tracked-decision",
            None,
            TARGET_PRINCIPAL,
            105,
            "not_tracked",
            "user turn (stop_reason=end_turn)",
            Some("renewal dispatch unavailable"),
        )
        .await?;
        append_read_projection(
            storage.as_ref(),
            "renewed-z",
            Some("renewed-session"),
            TARGET_PRINCIPAL,
            103,
            "reschedule",
            "first renewal",
            None,
        )
        .await?;
        append_read_projection(
            storage.as_ref(),
            "renewed-a",
            Some("renewed-session"),
            TARGET_PRINCIPAL,
            103,
            "reschedule",
            "second renewal",
            None,
        )
        .await?;

        let page = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: TARGET_PRINCIPAL.to_owned(),
                horizon_start_ms: None,
                filter: CacheKeepaliveSessionFilter::All,
                cursor: None,
                limit: 10,
            })
            .await?;
        let ids = page
            .rows
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>();
        ensure!(
            ids
                == [
                    "scheduled-session",
                    "not-tracked-decision",
                    "renewed-session",
                    "capped-session",
                    "expired-session",
                    "max-duration-session",
                ],
            "list order and decision/session projection membership must be stable; got {ids:?}"
        );
        ensure!(page.next_cursor.is_none(), "single page must not emit a cursor");

        let decision = &page.rows[1];
        ensure!(
            decision.source == CacheKeepaliveSessionEntrySource::Decision
                && decision.session_key_hash.is_none()
                && decision.reason == "user turn (stop_reason=end_turn)"
                && decision.error.as_deref() == Some("renewal dispatch unavailable")
                && decision.config_snapshot.as_ref() == Some(&read_snapshot()),
            "decision-only rows must preserve their frozen display projection: {decision:#?}"
        );
        let renewed_item = &page.rows[2];
        ensure!(
            renewed_item.source == CacheKeepaliveSessionEntrySource::Session
                && renewed_item.refresh_count == Some(1)
                && renewed_item.last_message_at_ms == 103_000
                && renewed_item.reason
                    == "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s"
                && renewed_item.config_snapshot.as_ref() == Some(&read_snapshot()),
            "session rows must preserve the frozen message projection: {renewed_item:#?}"
        );
        ensure!(
            page.rows[3].reason == "max renewals reached"
                && page.rows[4].reason == "TTL expired before follow-up"
                && page.rows[5].reason == "max duration reached (4h)",
            "terminal display reasons must match the public session contract"
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
                    principal_id: TARGET_PRINCIPAL.to_owned(),
                    horizon_start_ms: None,
                    filter,
                    cursor: None,
                    limit: 10,
                })
                .await?;
            let actual = filtered
                .rows
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>();
            ensure!(
                actual == expected_ids,
                "{filter:?} filter mismatch: expected {expected_ids:?}, got {actual:?}"
            );
        }

        let session = storage
            .get_cache_keepalive_session_for_principal(
                TARGET_PRINCIPAL,
                "renewed-session",
            )
            .await?
            .expect("principal-scoped session must exist");
        ensure!(
            session == renewed_after_hit,
            "principal-scoped detail must return the complete stored session after renewal"
        );
        ensure!(
            storage
                .get_cache_keepalive_session_for_principal(
                    OTHER_PRINCIPAL,
                    "renewed-session",
                )
                .await?
                .is_none(),
            "session detail must not cross principal boundaries"
        );

        let turns = storage
            .list_cache_keepalive_turns(TARGET_PRINCIPAL, "renewed-session")
            .await?;
        ensure!(
            turns
                .iter()
                .map(|turn| turn.source_ref_id.as_str())
                .collect::<Vec<_>>()
                == ["renewed-a", "renewed-z"],
            "per-session turns must sort by timestamp descending then source id ascending: {turns:#?}"
        );
        let stored_decision = storage
            .get_cache_keepalive_decision_for_principal(
                TARGET_PRINCIPAL,
                "not-tracked-decision",
            )
            .await?
            .expect("principal-scoped decision must exist");
        ensure!(
            stored_decision
                == CacheKeepaliveDecisionRecord {
                    source_ref_id: "not-tracked-decision".to_owned(),
                    principal_id: TARGET_PRINCIPAL.to_owned(),
                    session_key_hash: None,
                    upstream_id: Uuid::from_u128(7),
                    decision: "not_tracked".to_owned(),
                    reason: "user turn (stop_reason=end_turn)".to_owned(),
                    error: Some("renewal dispatch unavailable".to_owned()),
                    generation: 1,
                    ttl: CacheTtl::Ttl5m,
                    config_snapshot: Some(read_snapshot()),
                    last_message_at_ms: 105_000,
                    ts: 105,
                },
            "decision detail must preserve every frozen field"
        );
        ensure!(
            storage
                .get_cache_keepalive_decision_for_principal(
                    OTHER_PRINCIPAL,
                    "not-tracked-decision",
                )
                .await?
                .is_none(),
            "decision detail must not cross principal boundaries"
        );
        Ok(())
    })
    .await
}

pub async fn pagination_horizon_cursor_and_frozen_order_contract<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let alpha = storage
            .replace_from_real_request(&read_request(
                "alpha-session",
                TARGET_PRINCIPAL,
                200,
                "agent-in-turn",
            ))
            .await?;
        storage
            .replace_from_real_request(&read_request(
                "beta-session",
                TARGET_PRINCIPAL,
                200,
                "agent-in-turn",
            ))
            .await?;
        storage
            .replace_from_real_request(&read_request(
                "old-session",
                TARGET_PRINCIPAL,
                100,
                "agent-in-turn",
            ))
            .await?;
        ensure!(
            storage
                .mark_cache_keepalive_enqueued("alpha-session", alpha.generation, 999,)
                .await?,
            "scheduler state update fixture must apply"
        );

        let query = CacheKeepaliveSessionListQuery {
            principal_id: TARGET_PRINCIPAL.to_owned(),
            horizon_start_ms: Some(200_000),
            filter: CacheKeepaliveSessionFilter::Scheduled,
            cursor: None,
            limit: 1,
        };
        let first = storage.list_cache_keepalive_sessions(&query).await?;
        ensure!(
            first.rows.len() == 1 && first.rows[0].id == "alpha-session",
            "first tied page must use stable entry-id ordering: {first:#?}"
        );
        let second = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                cursor: first.next_cursor.clone(),
                ..query.clone()
            })
            .await?;
        ensure!(
            second.rows.len() == 1
                && second.rows[0].id == "beta-session"
                && second.next_cursor.is_none(),
            "cursor must continue without duplicates and exclude the old horizon row: {second:#?}"
        );

        let zero = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                limit: 0,
                cursor: None,
                ..query.clone()
            })
            .await?;
        ensure!(
            zero.rows.is_empty() && zero.next_cursor.is_none(),
            "zero limit must return an empty terminal page"
        );

        let error = storage
            .list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                cursor: Some(cc_lb_storage_api::CacheKeepaliveSessionCursor {
                    principal_id: OTHER_PRINCIPAL.to_owned(),
                    horizon_start_ms: query.horizon_start_ms,
                    filter: query.filter,
                    last_message_at_ms: 200_000,
                    entry_id: "session:alpha-session".to_owned(),
                }),
                ..query
            })
            .await
            .expect_err("mismatched cursor must be rejected");
        ensure!(
            matches!(
                error,
                cc_lb_storage_api::StorageError::InvalidInput {
                    ref field,
                    ..
                } if field == "cache_keepalive_session_cursor"
            ),
            "cursor mismatch must return the public invalid-input field: {error}"
        );
        Ok(())
    })
    .await
}

async fn terminalize_read_session<S>(
    storage: &S,
    session_key_hash: &str,
    now: u64,
    reason: CacheKeepaliveTerminalReason,
) -> Result<()>
where
    S: CacheKeepaliveSessionStore + Sync,
{
    let record = storage
        .replace_from_real_request(&read_request(
            session_key_hash,
            TARGET_PRINCIPAL,
            now,
            reason.display_reason(),
        ))
        .await?;
    ensure!(
        storage
            .mark_cache_keepalive_terminal(session_key_hash, record.generation, reason, now,)
            .await?,
        "terminal fixture {session_key_hash} must transition"
    );
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the conformance fixture keeps each projected field explicit at call sites"
)]
async fn append_read_projection<S>(
    storage: &S,
    source_ref_id: &str,
    session_key_hash: Option<&str>,
    principal_id: &str,
    ts: u64,
    decision: &str,
    reason: &str,
    error: Option<&str>,
) -> Result<()>
where
    S: RequestEventStore + Sync,
{
    let turn = session_key_hash.map(|session_key_hash| CacheKeepaliveTurnRow {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: Some("accounting-key".to_owned()),
        upstream_id: Uuid::from_u128(7),
        model: "claude-sonnet-4-5".to_owned(),
        input_tokens: 100,
        output_tokens: 20,
        cache_creation_input_tokens: 30,
        cache_creation_input_tokens_5m: 10,
        cache_creation_input_tokens_1h: 20,
        cache_read_input_tokens: 80,
        cost_micros: 123,
        hit_miss: "hit".to_owned(),
        ts,
    });
    storage
        .append_request_event_with_projections(
            &RequestEvent {
                ts,
                ts_ms: Some(ts.saturating_mul(1_000)),
                request_id: format!("read-contract-{source_ref_id}"),
                source_kind: Some("cache_keepalive_decision".to_owned()),
                source_ref_id: Some(source_ref_id.to_owned()),
                event_id: Some(format!("read-contract-event-{source_ref_id}")),
                principal_id: Some(principal_id.to_owned()),
                upstream_id: Some(Uuid::from_u128(7)),
                status: 200,
                duration_ms: 1,
                ..RequestEvent::default()
            },
            &RequestEventProjections {
                turn,
                decision: CacheKeepaliveDecisionRow {
                    source_ref_id: source_ref_id.to_owned(),
                    principal_id: principal_id.to_owned(),
                    session_key_hash: session_key_hash.map(str::to_owned),
                    upstream_id: Uuid::from_u128(7),
                    decision: decision.to_owned(),
                    reason: reason.to_owned(),
                    error: error.map(str::to_owned),
                    generation: 1,
                    ttl: CacheTtl::Ttl5m,
                    config_snapshot: Some(read_snapshot()),
                    last_message_at_ms: ts.saturating_mul(1_000),
                    ts,
                },
            },
        )
        .await?;
    Ok(())
}

fn read_request(
    session_key_hash: &str,
    principal_id: &str,
    now: u64,
    display_reason: &str,
) -> CacheKeepaliveReplaceRequest {
    CacheKeepaliveReplaceRequest {
        session_key_hash: session_key_hash.to_owned(),
        principal_id: principal_id.to_owned(),
        accounting_key_id: None,
        upstream_id: Uuid::from_u128(7),
        cache_anchor_at_unix_secs: now,
        ttl: CacheTtl::Ttl5m,
        run_at_unix_secs: now + 270,
        expires_at_unix_secs: now + 300,
        encrypted_payload: vec![1],
        display_reason: display_reason.to_owned(),
        config_snapshot: read_snapshot(),
        now_unix_secs: now,
    }
}

fn read_snapshot() -> CacheKeepaliveConfigSnapshot {
    CacheKeepaliveConfigSnapshot {
        refresh_lead_time_5m_secs: 30,
        refresh_lead_time_1h_secs: 300,
        max_refreshes_per_session: 12,
        max_total_duration_secs: 14_400,
        snapshot_max_bytes: 524_288,
    }
}
