use std::sync::Arc;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRecord, CacheKeepaliveDecisionRow, CacheKeepaliveProjectionStore,
    CacheKeepaliveSessionReadStore, CacheKeepaliveTurnRow, CacheTtl, RequestEvent,
    RequestEventProjections, RequestEventStore,
};
use uuid::Uuid;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn decision_append_roundtrip<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    with_conformance_fixture(backend, |storage| async move {
        let decision = decision("projection-decision");
        storage.append_cache_keepalive_decision(&decision).await?;

        let stored = storage
            .get_cache_keepalive_decision_for_principal(
                &decision.principal_id,
                &decision.source_ref_id,
            )
            .await?;
        ensure!(
            stored == Some(decision_record(&decision)),
            "cache keepalive decision append must preserve every field"
        );
        ensure!(
            storage
                .get_cache_keepalive_decision_for_principal(
                    &decision.principal_id,
                    "projection-missing",
                )
                .await?
                .is_none(),
            "an unknown cache keepalive decision must return None"
        );
        Ok(())
    })
    .await
}

#[async_trait]
pub trait ProjectionAtomicityBackend: ConformanceBackend {
    async fn install_turn_insert_failure(&self, fixture: &Self::Fixture) -> Result<()>;
    async fn projection_row_counts(&self, fixture: &Self::Fixture) -> Result<(i64, i64, i64)>;
}

pub async fn append_with_projections_is_atomic<B>(backend: Arc<B>) -> Result<()>
where
    B: ProjectionAtomicityBackend,
{
    let fixture = backend.create_fixture().await?;
    let storage = backend.open(&fixture).await?;
    backend.install_turn_insert_failure(&fixture).await?;

    let projections = RequestEventProjections {
        turn: Some(turn("projection-atomic")),
        decision: decision("projection-atomic"),
    };
    let append_result = storage
        .append_request_event_with_projections(&event("projection-atomic"), &projections)
        .await;
    let scenario_result = async {
        ensure!(
            append_result.is_err(),
            "a forced keepalive turn failure must fail the projection append"
        );
        ensure!(
            backend.projection_row_counts(&fixture).await? == (0, 0, 0),
            "a failed projection append must roll back the request event, turn, and decision rows"
        );
        Ok(())
    }
    .await;
    let teardown_result = backend.teardown(fixture).await;
    scenario_result?;
    teardown_result
}

fn event(source_ref_id: &str) -> RequestEvent {
    RequestEvent {
        ts: 1_800_710_000,
        ts_ms: Some(1_800_710_000_123),
        request_id: format!("request-{source_ref_id}"),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(source_ref_id.to_owned()),
        event_id: Some(format!("event-{source_ref_id}")),
        principal_id: Some("projection-principal".to_owned()),
        upstream_id: Some(Uuid::from_u128(71)),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 12,
        ..Default::default()
    }
}

fn turn(source_ref_id: &str) -> CacheKeepaliveTurnRow {
    CacheKeepaliveTurnRow {
        source_ref_id: source_ref_id.to_owned(),
        session_key_hash: "projection-session".to_owned(),
        principal_id: "projection-principal".to_owned(),
        accounting_key_id: Some("projection-key".to_owned()),
        upstream_id: Uuid::from_u128(71),
        model: "claude-sonnet-4-5".to_owned(),
        input_tokens: 101,
        output_tokens: 102,
        cache_creation_input_tokens: 103,
        cache_creation_input_tokens_5m: 104,
        cache_creation_input_tokens_1h: 105,
        cache_read_input_tokens: 106,
        cost_micros: 107,
        hit_miss: "hit".to_owned(),
        ts: 1_800_710_000,
    }
}

fn decision(source_ref_id: &str) -> CacheKeepaliveDecisionRow {
    CacheKeepaliveDecisionRow {
        source_ref_id: source_ref_id.to_owned(),
        principal_id: "projection-principal".to_owned(),
        session_key_hash: Some("projection-session".to_owned()),
        upstream_id: Uuid::from_u128(71),
        decision: "reschedule".to_owned(),
        reason: "cache_hit".to_owned(),
        error: Some("recorded warning".to_owned()),
        generation: 7,
        ttl: CacheTtl::Ttl5m,
        config_snapshot: None,
        last_message_at_ms: 1_800_709_999_999,
        ts: 1_800_710_000,
    }
}

fn decision_record(row: &CacheKeepaliveDecisionRow) -> CacheKeepaliveDecisionRecord {
    CacheKeepaliveDecisionRecord {
        source_ref_id: row.source_ref_id.clone(),
        principal_id: row.principal_id.clone(),
        session_key_hash: row.session_key_hash.clone(),
        upstream_id: row.upstream_id,
        decision: row.decision.clone(),
        reason: row.reason.clone(),
        error: row.error.clone(),
        generation: row.generation,
        ttl: row.ttl,
        config_snapshot: row.config_snapshot.clone(),
        last_message_at_ms: row.last_message_at_ms,
        ts: row.ts,
    }
}
