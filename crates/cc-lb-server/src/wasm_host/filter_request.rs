use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate};
use cc_lb_plugin_wire::{
    schema::WireVersion,
    v1::{
        CachePricingSummaryRef, FilterRequestRef, HeaderRef, PrincipalRef, QueryRef,
        UpstreamCandidateRef,
    },
};
use cc_lb_routing::RoutingContext;
use rkyv::rancor::Error as RkyvError;

use super::serialize_with_input_scratch;

pub(super) struct FilterWireRequest<'a> {
    pub wire_version: WireVersion,
    pub ctx: &'a RoutingContext,
    pub principal: &'a Principal,
    pub candidates: &'a [UpstreamCandidate],
    pub cookie_redaction: bool,
}

impl FilterWireRequest<'_> {
    pub(super) fn encode<R>(
        self,
        with_bytes: impl for<'a> FnOnce(&'a [u8]) -> R,
    ) -> Result<R, RkyvError> {
        let principal_kind = principal_kind_to_wire(self.principal);
        let headers: Vec<HeaderRef<'_>> = self
            .ctx
            .downstream_headers
            .iter()
            .filter(|(name, _)| {
                !is_stripped_downstream_header(name.as_str(), self.cookie_redaction)
            })
            .map(|(name, value)| HeaderRef {
                name: name.as_str(),
                value: value.as_bytes(),
            })
            .collect();
        let candidate_ids: Vec<String> = self
            .candidates
            .iter()
            .map(|candidate| candidate.upstream_id.to_string())
            .collect();
        let candidates: Vec<UpstreamCandidateRef<'_>> = self
            .candidates
            .iter()
            .zip(candidate_ids.iter())
            .map(|(candidate, upstream_id)| {
                let cache_score = candidate.cache_score.as_ref();
                UpstreamCandidateRef {
                    upstream_id,
                    name: candidate.name.as_str(),
                    kind: candidate.kind.as_str(),
                    observed_at_unix_secs: candidate.observed_at_unix_secs,
                    predicted_cache_read_tokens: cache_score
                        .map(|score| score.predicted_cache_read_tokens)
                        .unwrap_or(0),
                    predicted_cache_creation_tokens_5m: cache_score
                        .map(|score| score.predicted_cache_creation_tokens_5m)
                        .unwrap_or(0),
                    predicted_cache_creation_tokens_1h: cache_score
                        .map(|score| score.predicted_cache_creation_tokens_1h)
                        .unwrap_or(0),
                    predicted_uncached_input_tokens: cache_score
                        .map(|score| score.predicted_uncached_input_tokens)
                        .unwrap_or(0),
                    plan_capacity_ratio: candidate.plan_capacity_ratio.unwrap_or(1.0),
                    organization_type: candidate.organization_type.as_deref().unwrap_or(""),
                    rate_limit_tier: candidate.rate_limit_tier.as_deref().unwrap_or(""),
                    seat_tier: candidate.seat_tier.as_deref().unwrap_or(""),
                }
            })
            .collect();
        let query = self.ctx.query.as_deref().map(|value| QueryRef { value });
        let thread_id = self
            .ctx
            .thread_id
            .as_deref()
            .map(|value| QueryRef { value });
        let principal = || PrincipalRef {
            id: self.principal.id.as_str(),
            kind: principal_kind,
            claims: &[],
        };
        let cache_pricing = || CachePricingSummaryRef {
            status: self.ctx.cache_pricing.status.as_str(),
            input_micros_per_million: self.ctx.cache_pricing.input_micros_per_million,
            cache_creation_5m_micros_per_million: self
                .ctx
                .cache_pricing
                .cache_creation_5m_micros_per_million,
            cache_creation_1h_micros_per_million: self
                .ctx
                .cache_pricing
                .cache_creation_1h_micros_per_million,
            cache_read_micros_per_million: self.ctx.cache_pricing.cache_read_micros_per_million,
        };

        let service_tier = self
            .ctx
            .requested_service_tier
            .as_deref()
            .map(|value| QueryRef { value });
        match self.wire_version {
            WireVersion::V1 => serialize_with_input_scratch(
                &FilterRequestRef {
                    request_id: self.ctx.request_id.as_str(),
                    thread_id,
                    service_tier,
                    canonical_model_id: self.ctx.canonical_model_id.as_str(),
                    cache_pricing: cache_pricing(),
                    method: self.ctx.method.as_str(),
                    path: self.ctx.path.as_str(),
                    query,
                    headers: &headers,
                    body: self.ctx.body_bytes.as_ref(),
                    principal: principal(),
                    candidates: &candidates,
                },
                with_bytes,
            ),
        }
    }
}

pub(super) fn principal_kind_to_wire(principal: &Principal) -> &'static str {
    match principal.kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
    }
}

pub(super) fn is_stripped_downstream_header(name: &str, cookie_redaction: bool) -> bool {
    let lower = name.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "authorization" | "x-api-key" | "host" | "proxy-authorization"
    ) {
        return true;
    }
    cookie_redaction && lower == "cookie"
}

#[cfg(test)]
#[path = "filter_request/tests.rs"]
mod tests;
