use std::borrow::Cow;

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
        let scrubbed_query = self.ctx.query.as_deref().map(scrub_query_api_key);
        let query = scrubbed_query.as_deref().map(|value| QueryRef { value });
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
        "authorization"
            | "x-api-key"
            | "x-goog-api-key"
            | "api-key"
            | "host"
            | "proxy-authorization"
    ) {
        return true;
    }
    cookie_redaction && lower == "cookie"
}

pub(super) fn scrub_query_api_key(query: &str) -> Cow<'_, str> {
    let has_api_key = query.split('&').any(|pair| {
        pair.split_once('=')
            .map(|(name, _)| name.eq_ignore_ascii_case("key"))
            .unwrap_or_else(|| pair.eq_ignore_ascii_case("key"))
    });
    if !has_api_key {
        return Cow::Borrowed(query);
    }

    Cow::Owned(
        query
            .split('&')
            .filter(|pair| {
                !pair
                    .split_once('=')
                    .map(|(name, _)| name.eq_ignore_ascii_case("key"))
                    .unwrap_or_else(|| pair.eq_ignore_ascii_case("key"))
            })
            .collect::<Vec<_>>()
            .join("&"),
    )
}

#[cfg(test)]
#[path = "filter_request/tests.rs"]
mod tests;

#[cfg(test)]
mod security_tests {
    use super::*;

    #[test]
    fn strips_all_provider_credential_headers() {
        for name in ["Authorization", "x-api-key", "X-Goog-Api-Key", "api-key"] {
            assert!(is_stripped_downstream_header(name, false), "{name}");
        }
    }

    #[test]
    fn scrubs_key_query_pairs_without_touching_other_parameters() {
        assert_eq!(
            scrub_query_api_key("foo=bar&key=secret&KEY=other&baz=qux"),
            "foo=bar&baz=qux"
        );
        assert_eq!(scrub_query_api_key("foo=bar"), "foo=bar");
    }

    #[test]
    fn filter_wire_scrubs_provider_headers_and_query_keys() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-goog-api-key",
            http::HeaderValue::from_static("gemini-secret"),
        );
        headers.insert("api-key", http::HeaderValue::from_static("provider-secret"));
        headers.insert("x-request-id", http::HeaderValue::from_static("request"));
        let context = RoutingContext {
            request_id: "req-filter".to_owned(),
            thread_id: None,
            requested_service_tier: None,
            downstream_headers: headers,
            method: http::Method::POST,
            path: "/v1/responses".to_owned(),
            query: Some("key=raw-secret&foo=bar&KEY=other".to_owned()),
            body_bytes: bytes::Bytes::from_static(b"{}"),
            canonical_model_id: "gpt-5".to_owned(),
            cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        };
        let principal = Principal {
            id: "principal-filter".to_owned(),
            kind: PrincipalKind::ApiKey,
        };

        FilterWireRequest {
            wire_version: WireVersion::V1,
            ctx: &context,
            principal: &principal,
            candidates: &[],
            cookie_redaction: false,
        }
        .encode(|bytes| {
            let archived =
                rkyv::access::<cc_lb_plugin_wire::v1::ArchivedFilterRequest, RkyvError>(bytes)
                    .expect("filter request");
            let query: Option<&str> = archived.query.as_ref().map(|value| &**value);
            assert_eq!(query, Some("foo=bar"));
            let header_names: Vec<&str> = archived
                .headers
                .iter()
                .map(|header| {
                    let name: &str = &header.name;
                    name
                })
                .collect();
            assert_eq!(header_names, vec!["x-request-id"]);
        })
        .expect("encode filter request");
    }
}
