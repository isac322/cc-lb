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
    let _renewal_span = renewal_lifecycle_span(input.duration_ms).entered();
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
            observed_session_id: None,
            request_kind: None,
            claude_agent_id: None,
            claude_parent_agent_id: None,
            parent_session_id: None,
            client_app: None,
            session_id_source: None,
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
        request_body_read_ms: None,
        request_body_bytes: None,
        finalize_ms: None,
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        setup_timings: Default::default(),
        upstream_body_ms: None,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
    });
}

fn renewal_lifecycle_span(duration_ms: u64) -> tracing::Span {
    tracing::info_span!(
        "cache_keepalive.renewal_lifecycle",
        otel.kind = "internal",
        cc_lb.source_kind = "renewal",
        cc_lb.renewal_cycle_ms = duration_ms,
    )
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
    use tracing_subscriber::{Layer, Registry};

    use super::renewal_lifecycle_span;

    #[derive(Clone, Default)]
    struct RenewalSpanLayer {
        values: Arc<Mutex<HashMap<String, String>>>,
    }

    impl<S> Layer<S> for RenewalSpanLayer
    where
        S: Subscriber,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: LayerContext<'_, S>) {
            if attrs.metadata().name() == "cache_keepalive.renewal_lifecycle" {
                attrs.values().record(&mut RenewalSpanVisitor {
                    values: &self.values,
                });
            }
        }
    }

    struct RenewalSpanVisitor<'a> {
        values: &'a Arc<Mutex<HashMap<String, String>>>,
    }

    impl Visit for RenewalSpanVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.insert(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.insert(field, value.to_owned());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.insert(field, value.to_string());
        }
    }

    impl RenewalSpanVisitor<'_> {
        fn insert(&self, field: &Field, value: String) {
            self.values
                .lock()
                .expect("renewal span values lock")
                .insert(field.name().to_owned(), value);
        }
    }

    #[test]
    fn renewal_lifecycle_span_records_source_and_cycle_duration() {
        let layer = RenewalSpanLayer::default();
        let values = Arc::clone(&layer.values);
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let _span = renewal_lifecycle_span(37);
        });

        let values = values.lock().expect("renewal span values lock");
        assert_eq!(
            values.get("cc_lb.source_kind").map(String::as_str),
            Some("renewal")
        );
        assert_eq!(
            values.get("cc_lb.renewal_cycle_ms").map(String::as_str),
            Some("37")
        );
    }
}
