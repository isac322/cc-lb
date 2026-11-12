use cc_lb_clock::unix_secs;
use cc_lb_engine::cache_keepalive::{
    RenewalFinalization, RenewalUsage, RequestSnapshot, SessionKey,
};
use cc_lb_pricing::{ComputedCostBreakdown, virtual_cost_micros_full_with_context};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_storage_api::upstream::UpstreamRecord;
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveEvidenceRow, CacheKeepaliveSessionRecord,
    CacheKeepaliveTerminalReason, CacheKeepaliveTurnRow, RequestEvent, RequestEventProjections,
    RequestEventStore, UpstreamStore,
};

use super::lifecycle::{RenewalLifecycleInput, publish_renewal_lifecycle};
use crate::scheduler_dispatch::SchedulerDispatch;

impl SchedulerDispatch {
    pub(super) async fn finalize_cache_keepalive_renewal(
        &self,
        job: &CacheKeepaliveJob,
        record: &CacheKeepaliveSessionRecord,
        snapshot: &RequestSnapshot,
        finalization: RenewalFinalization,
        hit_miss: &str,
        authorized_after_dispatch: bool,
    ) -> SchedulerResult<()> {
        let source_ref_id = super::renewal_source_ref_id(job, record);
        let event_id = format!("renewal:{source_ref_id}");
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), snapshot.upstream_id)
            .await
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?
            .ok_or_else(|| {
                cc_lb_scheduler::error::SchedulerError::Job(
                    "cache keepalive upstream is unavailable during finalization".to_owned(),
                )
            })?;
        let model = keepalive_model(snapshot);
        let cost = if finalization.usage.provider_cost_unknown {
            ComputedCostBreakdown::unknown()
        } else {
            virtual_cost_micros_full_with_context(
                model,
                finalization.usage.input_tokens,
                finalization.usage.output_tokens,
                finalization.usage.cache_creation_input_tokens,
                finalization.usage.cache_creation_input_tokens_5m,
                finalization.usage.cache_creation_input_tokens_1h,
                finalization.usage.cache_read_input_tokens,
                None,
            )
            .unwrap_or_else(ComputedCostBreakdown::unknown)
        };
        let actual_cost_micros = (cost.pricing_status == cc_lb_pricing::PricingStatus::Known)
            .then_some(cost.total_micros);
        let ts = unix_secs(self.clock.now());
        let duration_ms = duration_ms(finalization.duration);
        let thread_id = SessionKey::extract_thread_id(&record.session_key_hash);
        let event = renewal_request_event(RenewalRequestEventInput {
            event_id: &event_id,
            source_ref_id: &source_ref_id,
            record,
            thread_id: thread_id.as_deref(),
            upstream: &upstream,
            model,
            usage: &finalization.usage,
            cost: &cost,
            status: finalization.status,
            duration_ms,
            ts,
        });
        let mut projections = renewal_projections(RenewalProjectionInput {
            source_ref_id: &source_ref_id,
            record,
            model,
            usage: &finalization.usage,
            cost_micros: actual_cost_micros,
            hit_miss,
            authorized_after_dispatch,
            generation: job.generation,
            ts,
        });
        if let Some(cycle_id) = record.cycle_id {
            projections.evidence.push(CacheKeepaliveEvidenceRow {
                evidence_id: format!("renewal-evidence:{cycle_id}:{event_id}"),
                cycle_id: Some(cycle_id),
                evidence_kind: "renewal".to_owned(),
                request_start_ms: finalization.request_start_ms,
                prefix_fingerprint: snapshot.prefix_fingerprint().map(ToOwned::to_owned),
                prefix_hash: snapshot.prefix_hash().map(ToOwned::to_owned),
                prefix_policy_version: snapshot.prefix_policy_version().map(ToOwned::to_owned),
                prefix_schema_version: snapshot.prefix_schema_version().map(ToOwned::to_owned),
                principal_id: record.principal_id.clone(),
                upstream_id: record.upstream_id,
                model: model.to_owned(),
                ttl: record.ttl,
                hit_miss: Some(hit_miss.to_owned()),
                cache_read_input_tokens: Some(finalization.usage.cache_read_input_tokens),
                cost_micros: actual_cost_micros,
                request_ref: Some(event_id.clone()),
                event_ref: Some(source_ref_id.to_owned()),
                session_key_hash: Some(record.session_key_hash.clone()),
            });
        }
        RequestEventStore::append_request_event_with_projections(
            self.storage.as_ref(),
            &event,
            &projections,
        )
        .await
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;

        let reservation_id = finalization
            .accounting_guard
            .reservation_id()
            .map(ToOwned::to_owned);
        if let Some(reservation_id) = reservation_id.as_deref() {
            let reconciled = self.limit_engine.reconcile_by_id(
                reservation_id,
                finalization.usage.input_tokens,
                finalization.usage.output_tokens,
                actual_cost_micros,
            );
            if !reconciled {
                return Err(cc_lb_scheduler::error::SchedulerError::Job(format!(
                    "cache keepalive renewal reconcile failed for reservation {reservation_id}"
                )));
            }
            finalization.accounting_guard.forget();
        }
        publish_renewal_lifecycle(
            self.event_bus.as_ref(),
            RenewalLifecycleInput {
                event_id: &event_id,
                source_ref_id: &source_ref_id,
                record,
                thread_id,
                upstream: &upstream,
                model,
                usage: &finalization.usage,
                reservation_id,
                status: finalization.status,
                duration_ms,
                ts,
            },
        );
        Ok(())
    }
}

struct RenewalRequestEventInput<'a> {
    event_id: &'a str,
    source_ref_id: &'a str,
    record: &'a CacheKeepaliveSessionRecord,
    thread_id: Option<&'a str>,
    upstream: &'a UpstreamRecord,
    model: &'a str,
    usage: &'a RenewalUsage,
    cost: &'a cc_lb_pricing::ComputedCostBreakdown,
    status: u16,
    duration_ms: u64,
    ts: u64,
}

struct RenewalProjectionInput<'a> {
    source_ref_id: &'a str,
    record: &'a CacheKeepaliveSessionRecord,
    model: &'a str,
    usage: &'a RenewalUsage,
    cost_micros: Option<i64>,
    hit_miss: &'a str,
    authorized_after_dispatch: bool,
    generation: u64,
    ts: u64,
}

fn keepalive_model(snapshot: &RequestSnapshot) -> &str {
    snapshot.keepalive_model()
}

fn duration_ms(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn renewal_request_event(input: RenewalRequestEventInput<'_>) -> RequestEvent {
    let known = input.cost.pricing_status == cc_lb_pricing::PricingStatus::Known;
    RequestEvent {
        ts: input.ts,
        ts_ms: Some(input.ts.saturating_mul(1_000)),
        request_id: input.event_id.to_owned(),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(input.source_ref_id.to_owned()),
        event_kind: Some(cc_lb_request_log::RequestEventKind::Renewal),
        principal_id: Some(input.record.principal_id.clone()),
        key_id: input.record.accounting_key_id.clone(),
        principal_kind: None,
        upstream_id: Some(input.upstream.id),
        upstream_name: Some(input.upstream.name.clone()),
        model: Some(input.model.to_owned()),
        thread_id: input.thread_id.map(ToOwned::to_owned),
        status: input.status,
        input_tokens: Some(input.usage.input_tokens),
        output_tokens: Some(input.usage.output_tokens),
        cache_creation_input_tokens: Some(input.usage.cache_creation_input_tokens),
        cache_creation_input_tokens_5m: Some(input.usage.cache_creation_input_tokens_5m),
        cache_creation_input_tokens_1h: Some(input.usage.cache_creation_input_tokens_1h),
        cache_read_input_tokens: Some(input.usage.cache_read_input_tokens),
        cost_usd_micros: known.then_some(input.cost.total_micros),
        cost_input_micros: known.then_some(input.cost.input_micros),
        cost_output_micros: known.then_some(input.cost.output_micros),
        cost_cache_creation_5m_micros: known.then_some(input.cost.cache_creation_5m_micros),
        cost_cache_creation_1h_micros: known.then_some(input.cost.cache_creation_1h_micros),
        cost_cache_read_micros: known.then_some(input.cost.cache_read_micros),
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        duration_ms: input.duration_ms,
        event_id: Some(input.event_id.to_owned()),
        ..RequestEvent::default()
    }
}

fn renewal_projections(input: RenewalProjectionInput<'_>) -> RequestEventProjections {
    RequestEventProjections {
        turn: Some(CacheKeepaliveTurnRow {
            source_ref_id: input.source_ref_id.to_owned(),
            session_key_hash: input.record.session_key_hash.clone(),
            principal_id: input.record.principal_id.clone(),
            accounting_key_id: input.record.accounting_key_id.clone(),
            upstream_id: input.record.upstream_id,
            cycle_id: input.record.cycle_id,
            model: input.model.to_owned(),
            input_tokens: input.usage.input_tokens,
            output_tokens: input.usage.output_tokens,
            cache_creation_input_tokens: input.usage.cache_creation_input_tokens,
            cache_creation_input_tokens_5m: input.usage.cache_creation_input_tokens_5m,
            cache_creation_input_tokens_1h: input.usage.cache_creation_input_tokens_1h,
            cache_read_input_tokens: input.usage.cache_read_input_tokens,
            cost_micros: input.cost_micros,
            hit_miss: input.hit_miss.to_owned(),
            ts: input.ts,
        }),
        decision: CacheKeepaliveDecisionRow {
            source_ref_id: input.source_ref_id.to_owned(),
            principal_id: input.record.principal_id.clone(),
            session_key_hash: Some(input.record.session_key_hash.clone()),
            upstream_id: input.record.upstream_id,
            decision: if input.hit_miss == "hit" && input.authorized_after_dispatch {
                "reschedule"
            } else {
                "terminal"
            }
            .to_owned(),
            reason: if input.authorized_after_dispatch {
                cache_keepalive_renewal_reason(input.hit_miss)
            } else {
                CacheKeepaliveTerminalReason::Cancelled
                    .display_reason()
                    .to_owned()
            },
            error: None,
            generation: input.generation,
            ttl: input.record.ttl,
            config_snapshot: input.record.config_snapshot.clone(),
            last_message_at_ms: input.ts.saturating_mul(1_000),
            ts: input.ts,
        },
        evidence: Vec::new(),
    }
}

fn cache_keepalive_renewal_reason(hit_miss: &str) -> String {
    match hit_miss {
        "hit" => "cache hit".to_owned(),
        "miss" => "cache miss".to_owned(),
        outcome => format!("cache {outcome}"),
    }
}

#[cfg(test)]
mod tests {

    use super::{RenewalProjectionInput, cache_keepalive_renewal_reason, renewal_projections};
    use cc_lb_engine::cache_keepalive::RenewalUsage;
    use cc_lb_storage_api::{
        CacheKeepaliveEnqueueState, CacheKeepaliveSessionRecord, CacheKeepaliveSessionStatus,
        CacheTtl,
    };
    use uuid::Uuid;

    #[test]
    fn unknown_pricing_keeps_turn_projection_with_hit_identity() {
        let record = CacheKeepaliveSessionRecord {
            session_key_hash: "session".to_owned(),
            principal_id: "principal".to_owned(),
            accounting_key_id: None,
            upstream_id: Uuid::nil(),
            cycle_id: None,
            generation: 1,
            refresh_count: 0,
            first_scheduled_at_unix_secs: 1,
            cache_anchor_at_unix_secs: 1,
            run_at_unix_secs: 2,
            ttl: CacheTtl::Ttl5m,
            status: CacheKeepaliveSessionStatus::Active,
            enqueue_state: CacheKeepaliveEnqueueState::Enqueued,
            running_since_unix_secs: None,
            current_job_key: "job".to_owned(),
            encrypted_payload: Vec::new(),
            display_reason: "cache hit".to_owned(),
            error: None,
            config_snapshot: None,
            terminal_reason: None,
            cancel_reason: None,
            expires_at_unix_secs: 3,
            created_at_unix_secs: 1,
            updated_at_unix_secs: 1,
        };
        let projections = renewal_projections(RenewalProjectionInput {
            source_ref_id: "session:1",
            record: &record,
            model: "gpt-5",
            usage: &RenewalUsage {
                cache_read_input_tokens: 42,
                provider_cost_unknown: true,
                ..RenewalUsage::default()
            },
            cost_micros: None,
            hit_miss: "hit",
            authorized_after_dispatch: true,
            generation: 1,
            ts: 2,
        });

        let turn = projections.turn.expect("unknown-priced turn is retained");
        assert_eq!(turn.hit_miss, "hit");
        assert_eq!(turn.cost_micros, None);
        assert_eq!(projections.decision.decision, "reschedule");
    }

    #[test]
    fn cache_hit_reason_is_human_readable() {
        // Given: a successful cache keep-alive renewal.
        let hit_miss = "hit";

        // When: its display reason is rendered.
        let reason = cache_keepalive_renewal_reason(hit_miss);

        // Then: it is not the generic storage token.
        assert_eq!(reason, "cache hit");
    }
}
