use cc_lb_control::RequestEventBus;
use cc_lb_engine::cache_keepalive::RenewalUsage;
use cc_lb_lifecycle::{
    AuthInfo, LifecycleEvent, LimitDecisionKind, ParseInfo, RouteInfo, StreamSuccess,
    TerminationReason, UsageSnapshot, UsageSource,
};
use cc_lb_request_log::HeaderSnapshot;
use cc_lb_storage_api::CacheKeepaliveSessionRecord;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};

pub(super) struct RenewalLifecycleInput<'a> {
    pub(super) event_id: &'a str,
    pub(super) source_ref_id: &'a str,
    pub(super) record: &'a CacheKeepaliveSessionRecord,
    pub(super) upstream: &'a UpstreamRecord,
    pub(super) model: &'a str,
    pub(super) usage: &'a RenewalUsage,
    pub(super) reservation_id: Option<String>,
    pub(super) status: u16,
    pub(super) duration_ms: u64,
    pub(super) ts: u64,
}

pub(super) fn publish_renewal_lifecycle(
    event_bus: &dyn RequestEventBus,
    input: RenewalLifecycleInput<'_>,
) {
    let usage_snapshot = usage_snapshot(input.usage);
    event_bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: input.event_id.to_owned(),
        request_id: input.event_id.to_owned(),
        ts_ms: input.ts.saturating_mul(1_000),
        stream: false,
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(input.source_ref_id.to_owned()),
    });
    event_bus.publish_lifecycle(LifecycleEvent::ParseCompleted {
        event_id: input.event_id.to_owned(),
        result: Ok(ParseInfo {
            path: "/v1/messages".to_owned(),
            method: "POST".to_owned(),
            model: Some(input.model.to_owned()),
            stream: false,
            body_bytes: 0,
            cache_control_block_count: None,
            cache_breakpoints: Vec::new(),
            cache_prefix_hash: None,
            matched_v3_cache_key: None,
            thread_id: None,
            message_id: None,
            message_index: None,
            message_count: None,
            cache_control_message_indices: Vec::new(),
            thinking_budget_tokens: None,
            reasoning_effort: None,
        }),
    });
    event_bus.publish_lifecycle(LifecycleEvent::AuthCompleted {
        event_id: input.event_id.to_owned(),
        result: Ok(AuthInfo {
            principal_id: input.record.principal_id.clone(),
            key_id: input.record.accounting_key_id.clone(),
            principal_kind: None,
            auth_ms: None,
        }),
    });
    event_bus.publish_lifecycle(LifecycleEvent::RouteCompleted {
        event_id: input.event_id.to_owned(),
        result: Ok(route_info(input.upstream, input.model)),
        routing_trace: None,
    });
    if let Some(reservation_id) = input.reservation_id {
        event_bus.publish_lifecycle(LifecycleEvent::LimitDecision {
            event_id: input.event_id.to_owned(),
            decision: LimitDecisionKind::Reserved {
                reservation_id,
                amount: input.usage.output_tokens,
                limit_reserve_ms: None,
            },
        });
    }
    event_bus.publish_lifecycle(LifecycleEvent::UpstreamAttempt {
        event_id: input.event_id.to_owned(),
        attempt_num: 1,
        upstream_id: input.upstream.id,
    });
    event_bus.publish_lifecycle(LifecycleEvent::UpstreamResponseStarted {
        event_id: input.event_id.to_owned(),
        status: input.status,
        headers: HeaderSnapshot::default(),
        bulkhead_wait_ms: None,
        dns_ms: None,
        connect_ms: None,
        connection_reused: None,
        shape_ms: None,
        sign_ms: None,
        upstream_ttfb_ms: None,
    });
    event_bus.publish_lifecycle(LifecycleEvent::UsageObserved {
        event_id: input.event_id.to_owned(),
        usage: usage_snapshot.clone(),
        source: UsageSource::NonStreamBody,
    });
    event_bus.publish_lifecycle(LifecycleEvent::StreamCompleted {
        event_id: input.event_id.to_owned(),
        result: Ok(StreamSuccess {
            usage: usage_snapshot,
            sse_event_count: 0,
            body_bytes: None,
            body_chunk_count: None,
            first_body_chunk_ms: None,
            stream_message_start_ms: None,
            stream_content_block_start_ms: None,
            stream_first_content_delta_ms: None,
            stream_last_content_delta_ms: None,
            stream_message_stop_ms: None,
            stream_last_chunk_ms: None,
            stream_total_ms: None,
            content_delta_count: None,
            ping_count: None,
            inter_token_avg_ms: None,
        }),
    });
    event_bus.publish_lifecycle(LifecycleEvent::RequestTerminated {
        event_id: input.event_id.to_owned(),
        reason: TerminationReason::Success,
        client_status: input.status,
        duration_ms: input.duration_ms,
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
    });
}

fn upstream_kind_label(kind: UpstreamKind) -> &'static str {
    match kind {
        UpstreamKind::AnthropicApiKey => "anthropic_key",
        UpstreamKind::AnthropicOauth => "anthropic_oauth",
    }
}

fn usage_snapshot(usage: &RenewalUsage) -> UsageSnapshot {
    UsageSnapshot {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_creation_input_tokens_5m: usage.cache_creation_input_tokens_5m,
        cache_creation_input_tokens_1h: usage.cache_creation_input_tokens_1h,
        cache_read_input_tokens: usage.cache_read_input_tokens,
        ..UsageSnapshot::default()
    }
}

fn route_info(upstream: &UpstreamRecord, model: &str) -> RouteInfo {
    RouteInfo {
        upstream_id: upstream.id,
        upstream_name: upstream.name.clone(),
        model: Some(model.to_owned()),
        upstream_kind: Some(upstream_kind_label(upstream.kind).to_owned()),
        route_ms: None,
        routing_trace: None,
        predicted_cache_read_tokens: None,
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        predicted_cache_creation_tokens_5m: None,
        predicted_cache_creation_tokens_1h: None,
        token_estimate_source: None,
        cache_value_micros: None,
        formula_winner_upstream_id: None,
        kept_upstream_id: None,
        quota_urgency_5h: None,
        quota_urgency_7d: None,
        quota_urgency_combined: None,
        quota_warning_multiplier: None,
        lineage_would_have_predicted_read_tokens: None,
        lineage_would_have_picked_upstream_id: None,
    }
}
