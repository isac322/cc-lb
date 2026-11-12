use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRow, CacheKeepaliveReplaceRequest,
    CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery, CacheKeepaliveSessionReadStore,
    CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason,
    CacheKeepaliveTurnRecord, CacheKeepaliveTurnRow, CacheTtl, RequestEvent,
    RequestEventProjections, RequestEventStore,
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

        let mut fixtures = vec![
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

        // Cost availability is independent of its signed numeric value.
        // Keep both per-session and batched reads honest for unknown and zero.
        for (label, cost) in [
            ("known-negative-one", Some(-1)),
            ("known-zero", Some(0)),
            ("unknown-cost", None),
        ] {
            let (mut event, mut projections, mut record) = fixtures[0].clone();
            let source_ref_id = format!("session-a:{label}");
            event.request_id = format!("keepalive-batch-event-{source_ref_id}");
            event.event_id = Some(format!("keepalive-batch-event-id-{source_ref_id}"));
            event.source_ref_id = Some(source_ref_id.clone());
            let turn = projections.turn.as_mut().expect("turn fixture");
            turn.source_ref_id = source_ref_id.clone();
            turn.cost_micros = cost;
            projections.decision.source_ref_id = format!("decision-{source_ref_id}");
            record.source_ref_id = source_ref_id;
            record.cost_micros = cost;
            fixtures.push((event, projections, record));
        }

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

/// Identical assertions run against SQLite and Postgres, including legacy rows
/// whose final cancellation must not turn back into a live request.
pub async fn request_markers_project_consistently<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let raw_backend = Arc::clone(&backend);
    with_conformance_fixture(backend, |storage| async move {
        let request = |session: &str, now| CacheKeepaliveReplaceRequest {
            session_key_hash: session.to_owned(),
            principal_id: TARGET_PRINCIPAL.to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(11),
            cycle_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: now,
            run_at_unix_secs: now + 270,
            expires_at_unix_secs: now + 300,
            encrypted_payload: vec![1],
            display_reason: "agent-in-turn".to_owned(),
            config_snapshot: CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: now,
        };
        for session in [
            "active", "fresh", "fresh-renewed", "incomplete", "legacy",
            "user-cancelled", "renewal-stale",
        ] {
            storage.replace_from_real_request(&request(session, 1_000)).await?;
        }
        for (session, deadline) in [
            ("fresh", 4_000_000_000),
            ("fresh-renewed", 4_000_000_000),
            ("incomplete", 0),
        ] {
            ensure!(
                storage.begin_cache_keepalive_request_fenced(session, None, 1_001, deadline).await?,
                "begin request marker for {session}"
            );
        }
        for (session, reason) in [
            ("legacy", "new_request"),
            ("user-cancelled", "user_turn_detected"),
        ] {
            ensure!(
                storage.cancel_cache_keepalive_fenced(
                    session, None, 1_001, CacheKeepaliveTerminalReason::Cancelled,
                    Some(reason), 1_001,
                ).await?,
                "final cancellation for {session}"
            );
        }
        ensure!(
            storage.mark_cache_keepalive_terminal(
                "renewal-stale", 1, CacheKeepaliveTerminalReason::Stale, None, 1_001,
            ).await?,
            "seed an actual stale renewal"
        );
        for session in ["fresh-renewed", "user-cancelled"] {
            let (event, mut projections, _) = turn_fixture(TurnSeed {
                source_ref_id: session,
                session_key_hash: session,
                principal_id: TARGET_PRINCIPAL,
                accounting_key_id: None,
                upstream_id: Uuid::from_u128(11),
                model: "claude-sonnet-4-5",
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_creation_input_tokens_5m: 0,
                cache_creation_input_tokens_1h: 0,
                cache_read_input_tokens: 20_000,
                cost_micros: 7,
                hit_miss: "hit",
                ts: 1_000,
            });
            projections.decision.source_ref_id = session.to_owned();
            storage.append_request_event_with_projections(&event, &projections).await?;
        }
        raw_backend.execute_raw_sql(
            &storage,
            "UPDATE cache_keepalive_sessions SET refresh_count = 1 WHERE session_key_hash IN ('fresh-renewed', 'user-cancelled')",
        ).await?;
        for session in ["fresh", "fresh-renewed"] {
            let row = storage.get_cache_keepalive_list_item(TARGET_PRINCIPAL, session).await?
                .expect("fresh request exists");
            ensure!(row.status == Some(CacheKeepaliveSessionStatus::InRequest));
            ensure!(row.status.expect("status").as_str() == "in_request");
            ensure!(row.terminal_reason.is_none() && row.cancel_reason.is_none());
            ensure!(row.enqueue_state.is_none() && row.error.is_none());
            ensure!(!row.request_incomplete && row.reason == "In request");
        }
        let incomplete = storage.get_cache_keepalive_list_item(TARGET_PRINCIPAL, "incomplete").await?
            .expect("incomplete request exists");
        ensure!(incomplete.status == Some(CacheKeepaliveSessionStatus::Terminal));
        ensure!(incomplete.terminal_reason == Some(CacheKeepaliveTerminalReason::Stale));
        ensure!(incomplete.request_incomplete && incomplete.cancel_reason.is_none());
        ensure!(incomplete.reason == "Request did not complete before its deadline");
        ensure!(incomplete.error.is_none());
        let legacy = storage.get_cache_keepalive_list_item(TARGET_PRINCIPAL, "legacy").await?
            .expect("legacy cancellation exists");
        ensure!(legacy.status == Some(CacheKeepaliveSessionStatus::Terminal));
        ensure!(legacy.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled));
        ensure!(!legacy.request_incomplete && legacy.cancel_reason.as_deref() == Some("new_request"));
        let summary_input = storage.read_cache_keepalive_summary_input(TARGET_PRINCIPAL, 0).await?;
        ensure!(summary_input.sessions.iter().filter(|row| row.status == Some(CacheKeepaliveSessionStatus::InRequest)).count() == 2);
        ensure!(summary_input.sessions.iter().filter(|row| row.request_incomplete).count() == 1);

        // The projections and every filter must agree, including pagination and
        // the cancellation-without-turns visibility rule.
        for (filter, expected) in [
            (CacheKeepaliveSessionFilter::All, vec!["active", "fresh", "fresh-renewed", "incomplete", "legacy", "renewal-stale", "user-cancelled"]),
            (CacheKeepaliveSessionFilter::Visible, vec!["active", "fresh", "fresh-renewed", "renewal-stale", "user-cancelled"]),
            (CacheKeepaliveSessionFilter::Tracked, vec!["active", "fresh", "fresh-renewed", "renewal-stale", "user-cancelled"]),
            (CacheKeepaliveSessionFilter::InRequest, vec!["fresh", "fresh-renewed"]),
            (CacheKeepaliveSessionFilter::Renewed, vec![]),
            (CacheKeepaliveSessionFilter::Scheduled, vec!["active"]),
            (CacheKeepaliveSessionFilter::Expired, vec!["incomplete"]),
            (CacheKeepaliveSessionFilter::Cancelled, vec!["user-cancelled"]),
            (CacheKeepaliveSessionFilter::Failed, vec!["renewal-stale"]),
            (CacheKeepaliveSessionFilter::NotTracked, vec!["legacy"]),
            (CacheKeepaliveSessionFilter::Capped, vec![]),
            (CacheKeepaliveSessionFilter::Error, vec![]),
        ] {
            for horizon_start_ms in [None, Some(0)] {
                let mut query = CacheKeepaliveSessionListQuery {
                    principal_id: TARGET_PRINCIPAL.to_owned(),
                    horizon_start_ms,
                    filter,
                    cursor: None,
                    limit: 1,
                };
                let mut ids = Vec::new();
                loop {
                    let page = storage.list_cache_keepalive_sessions(&query).await?;
                    ids.extend(page.rows.into_iter().map(|row| row.id));
                    query.cursor = page.next_cursor;
                    if query.cursor.is_none() { break; }
                }
                ids.sort();
                ensure!(ids == expected, "{filter:?} / {horizon_start_ms:?}: {ids:?} != {expected:?}");
            }
        }
        for horizon in [None, Some(0)] {
            let summary = storage.read_cache_keepalive_summary_aggregate_for_horizon(
                TARGET_PRINCIPAL, 0, horizon,
            ).await?;
            ensure!(summary.renewing_now == 1, "requests must not count as scheduled renewals");
            ensure!(summary.sessions_last_5m == 5, "only fresh request markers without turns must count");
            ensure!(summary.total_spent_micros == 14 && summary.successful_hits == 2);
            ensure!(summary.unlisted_sessions == 0 && summary.unlisted_spent_micros == 0);
        }
        storage.replace_from_real_request(&request("fresh", 1_002)).await?;
        let completed = storage.get_cache_keepalive_list_item(TARGET_PRINCIPAL, "fresh").await?
            .expect("completed request exists");
        ensure!(completed.status == Some(CacheKeepaliveSessionStatus::Active));
        ensure!(!completed.request_incomplete && completed.terminal_reason.is_none());
        ensure!(
            storage.cancel_cache_keepalive_fenced(
                "fresh-renewed", None, 1_001, CacheKeepaliveTerminalReason::Cancelled,
                Some("user_turn_detected"), 1_002,
            ).await?,
            "real completion resolves the provisional marker"
        );
        let cancelled = storage.get_cache_keepalive_list_item(TARGET_PRINCIPAL, "fresh-renewed").await?
            .expect("final cancellation exists");
        ensure!(cancelled.status == Some(CacheKeepaliveSessionStatus::Terminal));
        ensure!(cancelled.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled));
        ensure!(!cancelled.request_incomplete && cancelled.cancel_reason.as_deref() == Some("user_turn_detected"));
        for (index, (cancel_reason, expected_reason)) in [
            (Some("new_request"), "A new request arrived"),
            (Some("user_turn_detected"), "The agent is waiting for the user"),
            (Some("snapshot_too_large"), "The request snapshot is too large"),
            (Some("cached_prefix_too_small"), "The cached prefix is too small"),
            (Some("zero_token_renewal_unsupported"), "Zero-token renewals are unsupported"),
            (Some("ambiguous_turn"), "The agent turn could not be determined"),
            (Some("retention_recorded"), "Retention recorded without renewal"),
            (Some("renewal_not_beneficial"), "Refresh expected value is unknown or non-positive"),
            (Some("session_identity_changed"), "The request completed under a different cache identity"),
            (Some("completed_not_renewable"), "Request completed, renewal not possible"),
            (None, "Renewals ended"),
        ].into_iter().enumerate() {
            let now = 1_100 + index as u64;
            storage.replace_from_real_request(&request("final-reason", now)).await?;
            if cancel_reason != Some("new_request") {
                ensure!(
                    storage.begin_cache_keepalive_request_fenced(
                        "final-reason", None, now, 4_000_000_000,
                    ).await?
                );
            }
            ensure!(
                storage.cancel_cache_keepalive_fenced(
                    "final-reason", None, now, CacheKeepaliveTerminalReason::Cancelled,
                    cancel_reason, now + 1,
                ).await?
            );
            let final_row = storage.get_cache_keepalive_list_item(
                TARGET_PRINCIPAL, "final-reason",
            ).await?.expect("resolved cancellation");
            ensure!(final_row.status == Some(CacheKeepaliveSessionStatus::Terminal));
            ensure!(final_row.terminal_reason == Some(CacheKeepaliveTerminalReason::Cancelled));
            ensure!(!final_row.request_incomplete && final_row.error.is_none());
            ensure!(final_row.cancel_reason.as_deref() == cancel_reason);
            ensure!(final_row.reason == expected_reason);
        }
        // A newly scheduled session with no renewal history stays visible while
        // its real request runs, then follows normal no-turn horizon hiding.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let horizon_start_ms = now.saturating_sub(24 * 60 * 60).saturating_mul(1_000);
        let baseline_summary = storage.read_cache_keepalive_summary_aggregate_for_horizon(
            TARGET_PRINCIPAL, horizon_start_ms, Some(horizon_start_ms),
        ).await?;
        storage.replace_from_real_request(&request("zero-turn-live", now)).await?;
        ensure!(
            storage.begin_cache_keepalive_request_fenced(
                "zero-turn-live", None, now, now.saturating_add(300),
            ).await?
        );
        for filter in [CacheKeepaliveSessionFilter::Visible, CacheKeepaliveSessionFilter::Tracked] {
            let page = storage.list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: TARGET_PRINCIPAL.to_owned(),
                horizon_start_ms: Some(horizon_start_ms),
                filter,
                cursor: None,
                limit: 100,
            }).await?;
            ensure!(page.rows.len() == 1, "{filter:?}: expected only the recent zero-turn request");
            ensure!(page.rows[0].id == "zero-turn-live");
            ensure!(page.rows[0].status == Some(CacheKeepaliveSessionStatus::InRequest));
            ensure!(page.rows[0].refresh_count == Some(0));
        }
        let fresh_summary = storage.read_cache_keepalive_summary_aggregate_for_horizon(
            TARGET_PRINCIPAL, horizon_start_ms, Some(horizon_start_ms),
        ).await?;
        ensure!(fresh_summary.sessions_last_5m == 1);
        ensure!(fresh_summary.renewing_now == baseline_summary.renewing_now);
        raw_backend.execute_raw_sql(
            &storage,
            "UPDATE cache_keepalive_sessions SET run_at = 0 WHERE session_key_hash = 'zero-turn-live'",
        ).await?;
        for filter in [CacheKeepaliveSessionFilter::Visible, CacheKeepaliveSessionFilter::Tracked] {
            let page = storage.list_cache_keepalive_sessions(&CacheKeepaliveSessionListQuery {
                principal_id: TARGET_PRINCIPAL.to_owned(),
                horizon_start_ms: Some(horizon_start_ms),
                filter,
                cursor: None,
                limit: 100,
            }).await?;
            ensure!(page.rows.is_empty(), "{filter:?}: expired zero-turn request must be hidden");
        }
        let expired_summary = storage.read_cache_keepalive_summary_aggregate_for_horizon(
            TARGET_PRINCIPAL, horizon_start_ms, Some(horizon_start_ms),
        ).await?;
        ensure!(expired_summary.sessions_last_5m == 0);
        ensure!(expired_summary.renewing_now == baseline_summary.renewing_now);
        ensure!(fresh_summary.total_spent_micros == expired_summary.total_spent_micros);
        ensure!(fresh_summary.unlisted_spent_micros == expired_summary.unlisted_spent_micros);
        Ok(())
    }).await
}

/// Checks bounded ordering through same-adapter and independent connections to
/// one existing conformance fixture, with teardown even on assertion failure.
pub async fn bounded_order_namespaces<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let fixture = backend.create_fixture().await?;
    let result = async {
        let storage = backend.open(&fixture).await?;
        let independent = backend.open_independent(&fixture).await?;
        let seed = CacheKeepaliveReplaceRequest {
            session_key_hash: "lease-seed".to_owned(),
            principal_id: TARGET_PRINCIPAL.to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::from_u128(11),
            cycle_id: Uuid::from_u128(1),
            ttl: CacheTtl::Ttl5m,
            cache_anchor_at_unix_secs: 100,
            run_at_unix_secs: 370,
            expires_at_unix_secs: 400,
            encrypted_payload: vec![1],
            display_reason: "agent-in-turn".to_owned(),
            config_snapshot: CacheKeepaliveConfigSnapshot {
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
            },
            now_unix_secs: 100,
        };
        check_bounded_order_namespaces(&storage, &independent, &seed).await
    }
    .await;
    let teardown = backend.teardown(fixture).await;
    result?;
    teardown
}

async fn check_bounded_order_namespaces<S>(
    storage: &S,
    independent: &S,
    seed: &CacheKeepaliveReplaceRequest,
) -> Result<()>
where
    S: CacheKeepaliveSessionStore + cc_lb_storage_api::MetaStore,
{
    use cc_lb_storage_api::{
        CacheKeepaliveCompletionTicket, cache_keepalive_legacy_order_meta_key,
        cache_keepalive_order_meta_key, parse_keepalive_order_meta,
    };

    // Initial v2 allocation imports issuance only, including an incorrectly
    // leased value at the old key. Epoch, applied owner and lease stay isolated.
    for (index, legacy) in [
        "41:17",
        "v2:9999:50:00000000-0000-0000-0000-000000000007:41:17",
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("bounded-legacy-floor-{index}");
        let legacy_key = cache_keepalive_legacy_order_meta_key(&key);
        let order_key = cache_keepalive_order_meta_key(&key);
        storage.put_meta_value(&legacy_key, legacy).await?;
        let ticket = independent
            .allocate_cache_keepalive_completion_ticket(&key, 100, 800)
            .await?;
        ensure!(ticket.sequence == 42 && ticket.epoch != Uuid::from_u128(7));
        let raw = storage.get_meta_value(&order_key).await?;
        let meta = parse_keepalive_order_meta(raw.as_deref())?;
        let lease = meta.lease.context("isolated v2 lease")?;
        ensure!(meta.applied == 0 && lease.epoch == ticket.epoch);
        ensure!(lease.last_issued_at_unix_secs == 100 && lease.protected_until_unix_secs == 800);
        ensure!(storage.get_meta_value(&legacy_key).await?.as_deref() == Some(legacy));
        // Existing v2 ignores both larger legacy issuance and legacy corruption.
        for legacy_update in ["999:17", "broken"] {
            storage.put_meta_value(&legacy_key, legacy_update).await?;
            let next = independent
                .allocate_cache_keepalive_completion_ticket(&key, 101, 200)
                .await?;
            ensure!(next.epoch == ticket.epoch && next.valid_until_unix_secs == 200);
        }
        let raw = storage.get_meta_value(&order_key).await?;
        let meta = parse_keepalive_order_meta(raw.as_deref())?;
        ensure!(meta.issued == 44 && meta.applied == 0);
        ensure!(
            meta.lease
                .context("unchanged lease")?
                .protected_until_unix_secs
                == 800
        );
    }
    for (connection, other) in [storage, independent].into_iter().enumerate() {
        // Shorter configuration durations and clock regressions cannot shorten
        // another request's protection; each ticket retains its own deadline.
        let key = format!("bounded-maximum-{connection}");
        let first = storage
            .allocate_cache_keepalive_completion_ticket(&key, 100, 400)
            .await?;
        let short = other
            .allocate_cache_keepalive_completion_ticket(&key, 110, 200)
            .await?;
        let long = other
            .allocate_cache_keepalive_completion_ticket(&key, 105, 600)
            .await?;
        let intermediate = storage
            .get_meta_value(&cache_keepalive_order_meta_key(&key))
            .await?;
        ensure!(
            parse_keepalive_order_meta(intermediate.as_deref())?
                .lease
                .context("issued lease")?
                .last_issued_at_unix_secs
                == 110
        );
        let latest = storage
            .allocate_cache_keepalive_completion_ticket(&key, 120, 300)
            .await?;
        ensure!(short.valid_until_unix_secs == 200 && long.valid_until_unix_secs == 600);
        ensure!(first.epoch == latest.epoch && latest.sequence == 4);
        let order_key = cache_keepalive_order_meta_key(&key);
        let raw = storage.get_meta_value(&order_key).await?;
        let meta = parse_keepalive_order_meta(raw.as_deref())?;
        let lease = meta.lease.context("issued lease")?;
        ensure!(lease.last_issued_at_unix_secs == 120 && lease.protected_until_unix_secs == 600);
        for cutoff in [401, 600] {
            other.purge_cache_keepalive_expired(cutoff).await?;
            ensure!(storage.get_meta_value(&order_key).await? == raw);
        }
        other.purge_cache_keepalive_expired(601).await?;
        ensure!(storage.get_meta_value(&order_key).await?.is_none());
        let recreated = other
            .allocate_cache_keepalive_completion_ticket(&key, 602, 700)
            .await?;
        ensure!(recreated.sequence == 1 && recreated.epoch != first.epoch);

        // Allocation races orphan collection on both previously leased and
        // legacy namespaces. Either winner is valid, but every returned ticket
        // must name persisted, protected metadata and authorize its completion.
        for legacy in [false, true] {
            for attempt in 0..4 {
                let key = format!("bounded-allocation-gc-{connection}-{legacy}-{attempt}");
                let order_key = cache_keepalive_order_meta_key(&key);
                if legacy {
                    storage
                        .put_meta_value(&cache_keepalive_legacy_order_meta_key(&key), "9:7")
                        .await?;
                } else {
                    storage
                        .allocate_cache_keepalive_completion_ticket(&key, 100, 200)
                        .await?;
                }
                let (allocation, purge) = tokio::join!(
                    storage.allocate_cache_keepalive_completion_ticket(&key, 201, 800),
                    other.purge_cache_keepalive_expired(201),
                );
                purge?;
                let ticket = allocation?;
                let raw = other.get_meta_value(&order_key).await?;
                let meta = parse_keepalive_order_meta(raw.as_deref())?;
                let lease = meta.lease.context("persisted allocation raced with GC")?;
                ensure!(lease.epoch == ticket.epoch && meta.issued == ticket.sequence);
                ensure!(lease.protected_until_unix_secs == 800);
                ensure!(lease.last_issued_at_unix_secs == 201);
                let mut request = seed.clone();
                request.session_key_hash = key;
                request.cache_anchor_at_unix_secs = 201;
                request.now_unix_secs = 201;
                request.expires_at_unix_secs = 900;
                ensure!(
                    other
                        .replace_from_real_request_fenced(&request, Some(ticket))
                        .await?
                        .is_some()
                );
                let after = other.get_meta_value(&order_key).await?;
                let applied = parse_keepalive_order_meta(after.as_deref())?;
                ensure!(applied.applied == ticket.sequence && applied.lease == Some(lease));
            }
        }

        for replacement in [false, true] {
            for stale_attempt in [false, true] {
                let key = format!("bounded-aba-{connection}-{replacement}-{stale_attempt}");
                let mut request = seed.clone();
                request.session_key_hash = key.clone();
                request.cache_anchor_at_unix_secs = 100;
                request.now_unix_secs = 100;
                request.expires_at_unix_secs = 200;
                let original = storage.replace_from_real_request(&request).await?;
                let old = storage
                    .allocate_cache_keepalive_completion_ticket(&key, 100, 200)
                    .await?;
                ensure!(
                    storage
                        .begin_cache_keepalive_request_fenced(&key, Some(old), 100, 190)
                        .await?
                );
                let old_marker = storage.get_cache_keepalive_session(&key).await?;
                let order_key = cache_keepalive_order_meta_key(&key);
                let old_order = storage.get_meta_value(&order_key).await?;
                request.cache_anchor_at_unix_secs = 201;
                request.now_unix_secs = 201;
                // Expiry must reject a callback even while its original marker
                // and metadata still exist and the applied sequence matches.
                ensure!(
                    !other
                        .begin_cache_keepalive_request_fenced(&key, Some(old), 201, 300)
                        .await?
                );
                ensure!(
                    other
                        .replace_from_real_request_fenced(&request, Some(old))
                        .await?
                        .is_none()
                );
                ensure!(
                    !other
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(old),
                            201,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("request_failed"),
                            201,
                        )
                        .await?
                );
                ensure!(storage.get_cache_keepalive_session(&key).await? == old_marker);
                ensure!(storage.get_meta_value(&order_key).await? == old_order);
                other.purge_cache_keepalive_expired(201).await?;
                ensure!(storage.get_cache_keepalive_session(&key).await?.is_none());
                ensure!(storage.get_meta_value(&order_key).await?.is_none());

                // B deliberately reuses A's ordinal. Its namespace must differ.
                let current = other
                    .allocate_cache_keepalive_completion_ticket(&key, 202, 600)
                    .await?;
                ensure!(current.sequence == old.sequence && current.epoch != old.epoch);
                request.cache_anchor_at_unix_secs = 202;
                request.now_unix_secs = 202;
                request.expires_at_unix_secs = 900;
                other.replace_from_real_request(&request).await?;
                ensure!(
                    other
                        .begin_cache_keepalive_request_fenced(&key, Some(current), 202, 500)
                        .await?
                );
                let marker = storage.get_cache_keepalive_session(&key).await?;
                let order = storage.get_meta_value(&order_key).await?;
                request.cache_anchor_at_unix_secs = 450;
                request.now_unix_secs = 450;
                request.encrypted_payload = b"healthy-b".to_vec();
                if stale_attempt {
                    // The original expired token and an unexpired same-ordinal
                    // token from the old epoch must both fail independently.
                    for stale in [
                        old,
                        CacheKeepaliveCompletionTicket {
                            valid_until_unix_secs: 600,
                            ..old
                        },
                    ] {
                        ensure!(
                            !storage
                                .begin_cache_keepalive_request_fenced(&key, Some(stale), 450, 550)
                                .await?
                        );
                        ensure!(
                            storage
                                .replace_from_real_request_fenced(&request, Some(stale))
                                .await?
                                .is_none()
                        );
                        ensure!(
                            !storage
                                .cancel_cache_keepalive_fenced(
                                    &key,
                                    Some(stale),
                                    450,
                                    CacheKeepaliveTerminalReason::Cancelled,
                                    Some("request_failed"),
                                    450,
                                )
                                .await?
                        );
                        ensure!(storage.get_cache_keepalive_session(&key).await? == marker);
                        ensure!(storage.get_meta_value(&order_key).await? == order);
                    }
                }
                if replacement {
                    ensure!(
                        other
                            .replace_from_real_request_fenced(&request, Some(current))
                            .await?
                            .is_some()
                    );
                } else {
                    ensure!(
                        other
                            .cancel_cache_keepalive_fenced(
                                &key,
                                Some(current),
                                450,
                                CacheKeepaliveTerminalReason::Cancelled,
                                Some("user_turn_detected"),
                                450,
                            )
                            .await?
                    );
                }
                let resolved = storage
                    .get_cache_keepalive_session(&key)
                    .await?
                    .context("resolved B")?;
                ensure!(resolved.running_since_unix_secs.is_none());
                ensure!(resolved.generation == original.generation + u64::from(replacement));
                let after = storage.get_meta_value(&order_key).await?;
                ensure!(
                    after == order,
                    "completion must not extend the lease or change its owner"
                );
            }
        }

        // Inclusive individual deadline and session retention beyond protection.
        for replacement in [false, true] {
            let key = format!("bounded-inclusive-{connection}-{replacement}");
            let mut request = seed.clone();
            request.session_key_hash = key.clone();
            request.cache_anchor_at_unix_secs = 100;
            request.now_unix_secs = 100;
            request.expires_at_unix_secs = 900;
            storage.replace_from_real_request(&request).await?;
            let ticket = storage
                .allocate_cache_keepalive_completion_ticket(&key, 100, 200)
                .await?;
            ensure!(
                other
                    .begin_cache_keepalive_request_fenced(&key, Some(ticket), 100, 190)
                    .await?
            );
            request.cache_anchor_at_unix_secs = 200;
            request.now_unix_secs = 200;
            if replacement {
                ensure!(
                    storage
                        .replace_from_real_request_fenced(&request, Some(ticket))
                        .await?
                        .is_some()
                );
            } else {
                ensure!(
                    storage
                        .cancel_cache_keepalive_fenced(
                            &key,
                            Some(ticket),
                            200,
                            CacheKeepaliveTerminalReason::Cancelled,
                            Some("user_turn_detected"),
                            200,
                        )
                        .await?
                );
            }
            let order_key = cache_keepalive_order_meta_key(&key);
            let order = storage.get_meta_value(&order_key).await?;
            other.purge_cache_keepalive_expired(201).await?;
            ensure!(storage.get_cache_keepalive_session(&key).await?.is_some());
            ensure!(storage.get_meta_value(&order_key).await? == order);
        }
    }

    // Missing, legacy, malformed and future-version metadata never authorize a
    // leased completion; malformed counters are never silently reset or purged.
    for (index, raw) in [
        None,
        Some("9:7"),
        Some("broken"),
        Some("1:2"),
        Some("v3:200:100:unknown:1:0"),
        Some("v2:200:100:bad:1:0"),
        Some("v2:1:100:00000000-0000-0000-0000-000000000007:1:0:extra"),
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("bounded-invalid-{index}");
        let order_key = cache_keepalive_order_meta_key(&key);
        let mut request = seed.clone();
        request.session_key_hash = key.clone();
        request.cache_anchor_at_unix_secs = 100;
        request.now_unix_secs = 100;
        request.expires_at_unix_secs = 900;
        let original = storage.replace_from_real_request(&request).await?;
        if let Some(raw) = raw {
            storage.put_meta_value(&order_key, raw).await?;
        }
        let ticket = CacheKeepaliveCompletionTicket {
            epoch: Uuid::from_u128(7),
            sequence: 9,
            valid_until_unix_secs: 800,
        };
        // Invalid encodings may report an error; valid-but-unauthorized state
        // returns rejection. Neither outcome may consume a fence.
        ensure!(!matches!(
            storage
                .begin_cache_keepalive_request_fenced(&key, Some(ticket), 200, 300)
                .await,
            Ok(true)
        ));
        request.cache_anchor_at_unix_secs = 200;
        request.now_unix_secs = 200;
        ensure!(!matches!(
            storage
                .replace_from_real_request_fenced(&request, Some(ticket))
                .await,
            Ok(Some(_))
        ));
        ensure!(!matches!(
            storage
                .cancel_cache_keepalive_fenced(
                    &key,
                    Some(ticket),
                    200,
                    CacheKeepaliveTerminalReason::Cancelled,
                    Some("request_failed"),
                    200,
                )
                .await,
            Ok(true)
        ));
        ensure!(storage.get_cache_keepalive_session(&key).await?.as_ref() == Some(&original));
        ensure!(storage.get_meta_value(&order_key).await?.as_deref() == raw);
        match raw {
            Some("9:7") => {
                let modern = independent
                    .allocate_cache_keepalive_completion_ticket(&key, 100, 800)
                    .await?;
                ensure!(modern.sequence == 10);
                let value = storage.get_meta_value(&order_key).await?;
                let meta = parse_keepalive_order_meta(value.as_deref())?;
                ensure!(
                    meta.applied == 7
                        && meta.lease.context("modernized lease")?.epoch == modern.epoch
                );
            }
            Some(_) => {
                ensure!(
                    independent
                        .allocate_cache_keepalive_completion_ticket(&key, 100, 800)
                        .await
                        .is_err()
                );
                ensure!(storage.get_meta_value(&order_key).await?.as_deref() == raw);
            }
            None => {}
        }
    }
    for legacy in [false, true] {
        for (suffix, value, retained) in [
            ("numeric", "9:7", false),
            ("malformed", "broken", true),
            ("future", "v3:1:0:unknown:1:0", true),
            ("malformed-v2", "v2:1:0:bad:1:0", true),
        ] {
            let session = format!("bounded-orphan-{legacy}-{suffix}");
            let key = if legacy {
                cache_keepalive_legacy_order_meta_key(&session)
            } else {
                cache_keepalive_order_meta_key(&session)
            };
            storage.put_meta_value(&key, value).await?;
            independent.purge_cache_keepalive_expired(1_000).await?;
            ensure!(storage.get_meta_value(&key).await?.is_some() == retained);
        }
    }
    // Legacy overflow must fail before modernization, preserving the raw value.
    let key = "bounded-legacy-overflow";
    let order_key = cache_keepalive_legacy_order_meta_key(key);
    let raw = format!("{}:7", u64::MAX);
    storage.put_meta_value(&order_key, &raw).await?;
    ensure!(
        matches!(independent.allocate_cache_keepalive_completion_ticket(key, 100, 800).await,
        Err(cc_lb_storage_api::StorageError::Fatal { message })
        if message == "cache keepalive completion ticket overflow")
    );
    ensure!(storage.get_meta_value(&order_key).await?.as_deref() == Some(raw.as_str()));
    ensure!(
        storage
            .get_meta_value(&cache_keepalive_order_meta_key(key))
            .await?
            .is_none()
    );
    let key = "bounded-legacy-corrupt-floor";
    let legacy_key = cache_keepalive_legacy_order_meta_key(key);
    storage.put_meta_value(&legacy_key, "broken").await?;
    ensure!(
        independent
            .allocate_cache_keepalive_completion_ticket(key, 100, 800)
            .await
            .is_err()
    );
    ensure!(storage.get_meta_value(&legacy_key).await?.as_deref() == Some("broken"));
    ensure!(
        storage
            .get_meta_value(&cache_keepalive_order_meta_key(key))
            .await?
            .is_none()
    );
    Ok(())
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
        cycle_id: None,
        model: seed.model.to_owned(),
        input_tokens: seed.input_tokens,
        output_tokens: seed.output_tokens,
        cache_creation_input_tokens: seed.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: seed.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: seed.cache_creation_input_tokens_1h,
        cache_read_input_tokens: seed.cache_read_input_tokens,
        cost_micros: Some(seed.cost_micros),
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
            cycle_id: record.cycle_id,
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
        evidence: Vec::new(),
    };

    (event, projections, record)
}
