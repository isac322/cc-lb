use cc_lb_engine::cache_keepalive::{RenewalFinalization, RenewalUsage, RequestSnapshot};
use cc_lb_engine::clock::unix_secs;
use cc_lb_pricing::{UpstreamKind as PricingUpstreamKind, virtual_cost_micros_full};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    CacheKeepaliveDecisionRow, CacheKeepaliveSessionRecord, CacheKeepaliveTurnRow, RequestEvent,
    RequestEventProjections, RequestEventStore, RequestEventUpstream, UpstreamStore,
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
    ) -> SchedulerResult<()> {
        let source_ref_id = format!("{}:{}", job.session_key_hash, job.generation);
        let event_id = format!("renewal:{source_ref_id}");
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), snapshot.upstream_id)
            .await
            .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?
            .ok_or_else(|| {
                cc_lb_scheduler::error::SchedulerError::Job(
                    "cache keepalive upstream is unavailable during finalization".to_owned(),
                )
            })?;
        let model = keepalive_model(snapshot)?;
        let cost = virtual_cost_micros_full(
            &model,
            finalization.usage.input_tokens,
            finalization.usage.output_tokens,
            finalization.usage.cache_creation_input_tokens_5m,
            finalization.usage.cache_creation_input_tokens_1h,
            finalization.usage.cache_read_input_tokens,
            pricing_upstream_kind(upstream.kind),
            None,
        );
        let ts = unix_secs(self.clock.now());
        let duration_ms = duration_ms(finalization.duration);
        let event = renewal_request_event(RenewalRequestEventInput {
            event_id: &event_id,
            source_ref_id: &source_ref_id,
            record,
            upstream: &upstream,
            model: &model,
            usage: &finalization.usage,
            cost: &cost,
            status: finalization.status,
            duration_ms,
            ts,
        });
        let projections = renewal_projections(RenewalProjectionInput {
            source_ref_id: &source_ref_id,
            record,
            model: &model,
            usage: &finalization.usage,
            cost_micros: cost.total_micros,
            hit_miss,
            generation: job.generation,
            ts,
        });
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
                cost.total_micros,
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
                upstream: &upstream,
                model: &model,
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
    cost_micros: i64,
    hit_miss: &'a str,
    generation: u64,
    ts: u64,
}

fn keepalive_model(snapshot: &RequestSnapshot) -> SchedulerResult<String> {
    let body = snapshot
        .build_keepalive_body()
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
    let value: serde_json::Value = sonic_rs::from_slice(&body)
        .map_err(|error| cc_lb_scheduler::error::SchedulerError::Job(error.to_string()))?;
    Ok(value
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_owned())
}

fn duration_ms(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn pricing_upstream_kind(kind: UpstreamKind) -> Option<PricingUpstreamKind> {
    match kind {
        UpstreamKind::AnthropicApiKey => Some(PricingUpstreamKind::AnthropicKey),
        UpstreamKind::AnthropicOauth => Some(PricingUpstreamKind::AnthropicOAuth),
    }
}

fn renewal_request_event(input: RenewalRequestEventInput<'_>) -> RequestEvent {
    RequestEvent {
        ts: input.ts,
        ts_ms: Some(input.ts.saturating_mul(1_000)),
        request_id: input.event_id.to_owned(),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(input.source_ref_id.to_owned()),
        principal_id: Some(input.record.principal_id.clone()),
        key_id: input.record.accounting_key_id.clone(),
        principal_kind: None,
        upstream: Some(RequestEventUpstream::AnthropicDirect),
        upstream_id: Some(input.upstream.id),
        upstream_name: Some(input.upstream.name.clone()),
        model: Some(input.model.to_owned()),
        status: input.status,
        input_tokens: Some(input.usage.input_tokens),
        output_tokens: Some(input.usage.output_tokens),
        cache_creation_input_tokens: Some(input.usage.cache_creation_input_tokens),
        cache_creation_input_tokens_5m: Some(input.usage.cache_creation_input_tokens_5m),
        cache_creation_input_tokens_1h: Some(input.usage.cache_creation_input_tokens_1h),
        cache_read_input_tokens: Some(input.usage.cache_read_input_tokens),
        cost_usd_micros: Some(input.cost.total_micros),
        cost_input_micros: Some(input.cost.input_micros),
        cost_output_micros: Some(input.cost.output_micros),
        cost_cache_creation_5m_micros: Some(input.cost.cache_creation_5m_micros),
        cost_cache_creation_1h_micros: Some(input.cost.cache_creation_1h_micros),
        cost_cache_read_micros: Some(input.cost.cache_read_micros),
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
            decision: if input.hit_miss == "hit" {
                "reschedule"
            } else {
                "terminal"
            }
            .to_owned(),
            reason: cache_keepalive_renewal_reason(input.hit_miss),
            error: None,
            generation: input.generation,
            ttl: input.record.ttl,
            config_snapshot: input.record.config_snapshot.clone(),
            last_message_at_ms: input.ts.saturating_mul(1_000),
            ts: input.ts,
        },
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
    use super::cache_keepalive_renewal_reason;

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
