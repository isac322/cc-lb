use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveSessionReadStore, CacheKeepaliveTurnRecord,
    CacheKeepaliveTurnRow, CacheTtl, RequestEventProjections, RequestEventStore,
    types::RequestEvent,
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
