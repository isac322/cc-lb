use std::borrow::Cow;

use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate};
use cc_lb_plugin_wire::{
    schema::WireVersion,
    v1::{
        CachePricingSummaryRef, FilterRequestRef, HeaderRef, PrincipalRef, QueryRef,
        UpstreamCandidateRef,
    },
    v2, v3,
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
            WireVersion::V2 => {
                let thread_id = self
                    .ctx
                    .thread_id
                    .as_deref()
                    .map(|value| v2::OptionalStrRef { value });
                let service_tier = self
                    .ctx
                    .requested_service_tier
                    .as_deref()
                    .map(|value| v2::OptionalStrRef { value });
                let query = self
                    .ctx
                    .query
                    .as_deref()
                    .map(|value| v2::OptionalStrRef { value });
                let principal = v2::PrincipalRef {
                    id: self.principal.id.as_str(),
                    kind: principal_kind,
                    claims: &[],
                };
                let quota_refs: Vec<Vec<v2::SubscriptionQuotaSampleRef<'_>>> = self
                    .candidates
                    .iter()
                    .map(|candidate| {
                        candidate
                            .subscription_quotas
                            .iter()
                            .map(|quota| v2::SubscriptionQuotaSampleRef {
                                window: quota.window.as_str(),
                                utilization: quota.utilization,
                                resets_at_unix_secs: quota.resets_at_unix_secs,
                                status: quota
                                    .status
                                    .as_deref()
                                    .map(|value| v2::OptionalStrRef { value }),
                                sample_kind: match quota.state {
                                    cc_lb_domain::SubscriptionQuotaDataState::Fresh
                                    | cc_lb_domain::SubscriptionQuotaDataState::Stale => "sample",
                                    cc_lb_domain::SubscriptionQuotaDataState::Absent => "absent",
                                    cc_lb_domain::SubscriptionQuotaDataState::Unobserved => {
                                        "unobserved"
                                    }
                                },
                                observed_at_unix_millis: quota.observed_at_unix_millis,
                            })
                            .collect()
                    })
                    .collect();
                let candidates: Vec<v2::UpstreamCandidateRef<'_>> = self
                    .candidates
                    .iter()
                    .zip(candidate_ids.iter())
                    .zip(quota_refs.iter())
                    .map(|((candidate, upstream_id), quotas)| {
                        let cache_score = candidate.cache_score.as_ref();
                        v2::UpstreamCandidateRef {
                            upstream_id,
                            name: candidate.name.as_str(),
                            kind: candidate.kind.as_str(),
                            base_url: candidate
                                .base_url
                                .as_deref()
                                .map(|value| v2::OptionalStrRef { value }),
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
                            subscription_quotas: quotas,
                        }
                    })
                    .collect();
                let v2_headers: Vec<v2::HeaderRef<'_>> = self
                    .ctx
                    .downstream_headers
                    .iter()
                    .filter(|(name, _)| {
                        !is_stripped_downstream_header(name.as_str(), self.cookie_redaction)
                    })
                    .map(|(name, value)| v2::HeaderRef {
                        name: name.as_str(),
                        value: value.as_bytes(),
                    })
                    .collect();
                serialize_with_input_scratch(
                    &v2::FilterRequestRef {
                        request_id: self.ctx.request_id.as_str(),
                        thread_id,
                        service_tier,
                        canonical_model_id: self.ctx.canonical_model_id.as_str(),
                        cache_pricing: v2::CachePricingSummaryRef {
                            status: self.ctx.cache_pricing.status.as_str(),
                            input_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .input_micros_per_million,
                            cache_creation_5m_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_creation_5m_micros_per_million,
                            cache_creation_1h_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_creation_1h_micros_per_million,
                            cache_read_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_read_micros_per_million,
                        },
                        method: self.ctx.method.as_str(),
                        path: self.ctx.path.as_str(),
                        query,
                        headers: &v2_headers,
                        body: self.ctx.body_bytes.as_ref(),
                        principal,
                        candidates: &candidates,
                    },
                    with_bytes,
                )
            }
            WireVersion::V3 => {
                let thread_id = self
                    .ctx
                    .thread_id
                    .as_deref()
                    .map(|value| v3::OptionalStrRef { value });
                let service_tier = self
                    .ctx
                    .requested_service_tier
                    .as_deref()
                    .map(|value| v3::OptionalStrRef { value });
                let query = self
                    .ctx
                    .query
                    .as_deref()
                    .map(|value| v3::OptionalStrRef { value });
                let principal = v3::PrincipalRef {
                    id: self.principal.id.as_str(),
                    kind: principal_kind,
                    claims: &[],
                };
                let quota_refs: Vec<Vec<v2::SubscriptionQuotaSampleRef<'_>>> = self
                    .candidates
                    .iter()
                    .map(|candidate| {
                        candidate
                            .subscription_quotas
                            .iter()
                            .map(|quota| v2::SubscriptionQuotaSampleRef {
                                window: quota.window.as_str(),
                                utilization: quota.utilization,
                                resets_at_unix_secs: quota.resets_at_unix_secs,
                                status: quota
                                    .status
                                    .as_deref()
                                    .map(|value| v3::OptionalStrRef { value }),
                                sample_kind: match quota.state {
                                    cc_lb_domain::SubscriptionQuotaDataState::Fresh
                                    | cc_lb_domain::SubscriptionQuotaDataState::Stale => "sample",
                                    cc_lb_domain::SubscriptionQuotaDataState::Absent => "absent",
                                    cc_lb_domain::SubscriptionQuotaDataState::Unobserved => {
                                        "unobserved"
                                    }
                                },
                                observed_at_unix_millis: quota.observed_at_unix_millis,
                            })
                            .collect()
                    })
                    .collect();
                let candidates: Vec<v3::UpstreamCandidateRef<'_>> = self
                    .candidates
                    .iter()
                    .zip(candidate_ids.iter())
                    .zip(quota_refs.iter())
                    .map(|((candidate, upstream_id), quotas)| {
                        let cache_score = candidate.cache_score.as_ref();
                        v3::UpstreamCandidateRef {
                            upstream_id,
                            name: candidate.name.as_str(),
                            kind: candidate.kind.as_str(),
                            base_url: candidate
                                .base_url
                                .as_deref()
                                .map(|value| v3::OptionalStrRef { value }),
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
                            subscription_quotas: quotas,
                        }
                    })
                    .collect();
                let headers: Vec<v3::HeaderRef<'_>> = self
                    .ctx
                    .downstream_headers
                    .iter()
                    .filter(|(name, _)| {
                        !is_stripped_downstream_header(name.as_str(), self.cookie_redaction)
                    })
                    .map(|(name, value)| v3::HeaderRef {
                        name: name.as_str(),
                        value: value.as_bytes(),
                    })
                    .collect();
                let native_pricing = self.ctx.cache_pricing.native.as_ref();
                let (native_version, native_provider, native_rate_refs) =
                    native_pricing_refs(native_pricing);
                serialize_with_input_scratch(
                    &v3::FilterRequestRef {
                        request_id: self.ctx.request_id.as_str(),
                        thread_id,
                        service_tier,
                        canonical_model_id: self.ctx.canonical_model_id.as_str(),
                        cache_pricing: v2::CachePricingSummaryRef {
                            status: self.ctx.cache_pricing.status.as_str(),
                            input_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .input_micros_per_million,
                            cache_creation_5m_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_creation_5m_micros_per_million,
                            cache_creation_1h_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_creation_1h_micros_per_million,
                            cache_read_micros_per_million: self
                                .ctx
                                .cache_pricing
                                .cache_read_micros_per_million,
                        },
                        native_pricing: v3::NativeCachePricingRef {
                            version: native_version,
                            provider: native_provider,
                            rates: &native_rate_refs,
                        },
                        method: self.ctx.method.as_str(),
                        path: self.ctx.path.as_str(),
                        query,
                        headers: &headers,
                        body: self.ctx.body_bytes.as_ref(),
                        principal,
                        candidates: &candidates,
                    },
                    with_bytes,
                )
            }
        }
    }
}

fn native_pricing_refs(
    native: Option<&cc_lb_domain::NativeCachePricing>,
) -> (
    u8,
    v3::NativeCachePricingProvider,
    Vec<v3::NativeCachePricingRateRef>,
) {
    let Some(native) = native else {
        return (1, v3::NativeCachePricingProvider::Unknown, Vec::new());
    };
    let provider = match native.provider {
        cc_lb_domain::NativeCachePricingProvider::Unknown => {
            v3::NativeCachePricingProvider::Unknown
        }
        cc_lb_domain::NativeCachePricingProvider::Anthropic => {
            v3::NativeCachePricingProvider::Anthropic
        }
        cc_lb_domain::NativeCachePricingProvider::OpenAi => v3::NativeCachePricingProvider::OpenAi,
        cc_lb_domain::NativeCachePricingProvider::Gemini => v3::NativeCachePricingProvider::Gemini,
    };
    let rates = native
        .rates
        .iter()
        .map(|rate| v3::NativeCachePricingRateRef {
            component: match rate.component {
                cc_lb_domain::NativeCachePricingComponent::OrdinaryInput => {
                    v3::NativeCachePricingComponent::OrdinaryInput
                }
                cc_lb_domain::NativeCachePricingComponent::CachedInputRead => {
                    v3::NativeCachePricingComponent::CachedInputRead
                }
                cc_lb_domain::NativeCachePricingComponent::TotalWrite => {
                    v3::NativeCachePricingComponent::TotalWrite
                }
                cc_lb_domain::NativeCachePricingComponent::ExplicitResourceToken => {
                    v3::NativeCachePricingComponent::ExplicitResourceToken
                }
                cc_lb_domain::NativeCachePricingComponent::ExplicitResourceStorage => {
                    v3::NativeCachePricingComponent::ExplicitResourceStorage
                }
                cc_lb_domain::NativeCachePricingComponent::ExplicitResourceTtl => {
                    v3::NativeCachePricingComponent::ExplicitResourceTtl
                }
            },
            lifetime: rate.lifetime.map(|lifetime| match lifetime {
                cc_lb_domain::NativeCachePricingLifetime::ModelRetention => {
                    v3::NativeCachePricingLifetime::ModelRetention
                }
                cc_lb_domain::NativeCachePricingLifetime::ThirtyMinutes => {
                    v3::NativeCachePricingLifetime::ThirtyMinutes
                }
                cc_lb_domain::NativeCachePricingLifetime::OneHour => {
                    v3::NativeCachePricingLifetime::OneHour
                }
                cc_lb_domain::NativeCachePricingLifetime::TwelveHours => {
                    v3::NativeCachePricingLifetime::TwelveHours
                }
                cc_lb_domain::NativeCachePricingLifetime::ExplicitResourceTtl => {
                    v3::NativeCachePricingLifetime::ExplicitResourceTtl
                }
            }),
            status: match rate.status {
                cc_lb_domain::NativeCachePricingStatus::Known => {
                    v3::NativeCachePricingStatus::Known
                }
                cc_lb_domain::NativeCachePricingStatus::Unknown => {
                    v3::NativeCachePricingStatus::Unknown
                }
                cc_lb_domain::NativeCachePricingStatus::NotApplicable => {
                    v3::NativeCachePricingStatus::NotApplicable
                }
            },
            micros_per_million: rate.micros_per_million,
        })
        .collect();
    (native.version, provider, rates)
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
    cc_lb_upstream::scrub_query_api_key(query)
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
