use cc_lb_capture::schema::CapturedRequestInput;
use cc_lb_domain::{CacheBreakpoint, CachePricingSummary, RoutingTrace, UpstreamCandidate};
use uuid::Uuid;

use crate::builtin_filters::subscription_preference::CACHE_COST_BASIS_VERSION;
use crate::lifecycle::subscription_preference_trace;
use crate::request_context::RequestContext;

pub(crate) struct PendingCapturedRequestInput {
    event_id: String,
    canonical_model_id: String,
    cache_pricing: CachePricingSummary,
    breakpoints: Vec<CacheBreakpoint>,
    candidates: Vec<UpstreamCandidate>,
    captured_at_unix_ms: u64,
}

impl PendingCapturedRequestInput {
    pub(crate) fn new(
        event_id: String,
        canonical_model_id: String,
        cache_pricing: CachePricingSummary,
        breakpoints: Vec<CacheBreakpoint>,
        candidates: Vec<UpstreamCandidate>,
        captured_at_unix_ms: u64,
    ) -> Self {
        Self {
            event_id,
            canonical_model_id,
            cache_pricing,
            breakpoints,
            candidates,
            captured_at_unix_ms,
        }
    }

    pub(crate) fn complete(
        self,
        ctx: &RequestContext,
        subscription_preference_input_upstream_ids: Vec<Uuid>,
        routing_trace: RoutingTrace,
    ) -> CapturedRequestInput {
        let subscription_trace = subscription_preference_trace(&routing_trace);
        let salt_version = subscription_trace
            .and_then(|trace| trace.rendezvous_salt_version.clone())
            .unwrap_or_else(|| "none".to_owned());
        let cache_cost_basis_version = subscription_trace
            .and_then(|trace| trace.cache_cost_basis_version.clone())
            .unwrap_or_else(|| CACHE_COST_BASIS_VERSION.to_owned());
        CapturedRequestInput {
            event_id: self.event_id,
            request_id: ctx.request_id.clone(),
            thread_id: ctx.thread_id.clone(),
            canonical_model_id: self.canonical_model_id,
            cache_pricing: self.cache_pricing,
            breakpoints: self.breakpoints,
            candidates: self.candidates,
            subscription_preference_input_upstream_ids,
            routing_trace,
            captured_at_unix_ms: self.captured_at_unix_ms,
            salt_version,
            cache_cost_basis_version,
            capture_schema_version: 1,
            build_version: env!("CARGO_PKG_VERSION").to_owned(),
        }
    }
}
