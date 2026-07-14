use std::sync::Arc;

use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate};
use cc_lb_plugin_wire::{
    ArchivedFilterResponse, CachePricingSummaryRef, ClaimRef, FilterRequestRef, HeaderRef,
    PrincipalRef, QueryRef, UpstreamCandidateRef,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, PerCandidateReason, RoutingContext};
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntimeError};
use rkyv::rancor::Error as RkyvError;
use uuid::Uuid;

use super::{access_archived_scoped_or_copy, serialize_with_input_scratch};

pub struct WasmtimeFilterPlugin {
    dispatch: Arc<WasmPluginWireDispatch>,
    plugin_id: Uuid,
    plugin_name: String,
}

impl WasmtimeFilterPlugin {
    pub fn new(
        dispatch: Arc<WasmPluginWireDispatch>,
        plugin_id: Uuid,
        plugin_name: impl Into<String>,
    ) -> Self {
        Self {
            dispatch,
            plugin_id,
            plugin_name: plugin_name.into(),
        }
    }
}

impl FilterPlugin for WasmtimeFilterPlugin {
    fn filter(
        &self,
        ctx: &RoutingContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let wire_version = self.dispatch.filter_wire_version();
        match wire_version {
            Some(cc_lb_plugin_wire::schema::WireVersion::V1) => {}
            None => {
                return Err(FilterError::Runtime {
                    reason: "plugin metadata missing filter hook".to_owned(),
                });
            }
        }

        let guest_result = with_wire_request(
            ctx,
            principal,
            candidates,
            self.dispatch.cookie_redaction(),
            |in_bytes| {
                self.dispatch.call_filter_scoped(in_bytes, |guest_bytes| {
                    let bound = self.dispatch.wire_bounds().output_body_bytes;
                    if guest_bytes.len() as u64 > bound {
                        return Err(FilterError::Runtime {
                            reason: format!(
                                "filter output {} bytes exceeds wire_bounds.output_body_bytes ({bound})",
                                guest_bytes.len(),
                            ),
                        });
                    }
                    access_archived_scoped_or_copy::<ArchivedFilterResponse, _>(
                        guest_bytes,
                        |archived| {
                            wire_to_host_output(
                                archived,
                                self.dispatch.wire_bounds().reason_bytes as usize,
                            )
                        },
                    )
                    .map_err(|e| FilterError::Runtime {
                        reason: format!("rkyv access response: {e}"),
                    })
                    .and_then(std::convert::identity)
                })
            },
        )
        .map_err(|e| FilterError::Runtime {
            reason: format!("rkyv encode request: {e}"),
        })?;
        guest_result.map_err(runtime_error_to_filter)?
    }

    fn plugin_id(&self) -> Uuid {
        self.plugin_id
    }

    fn plugin_name(&self) -> &str {
        &self.plugin_name
    }
}

fn runtime_error_to_filter(err: WasmtimeRuntimeError) -> FilterError {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, source } => FilterError::Trap {
            reason: format!("{phase}: {source}"),
        },
        other => FilterError::Runtime {
            reason: other.to_string(),
        },
    }
}

pub(super) fn with_wire_request<R>(
    ctx: &RoutingContext,
    principal: &Principal,
    candidates: &[UpstreamCandidate],
    cookie_redaction: bool,
    with_bytes: impl for<'a> FnOnce(&'a [u8]) -> R,
) -> Result<R, RkyvError> {
    let principal_kind_str = principal_kind_to_wire(principal);
    let claim_bufs: Vec<(&str, Vec<u8>)> = principal
        .claims
        .iter()
        .filter_map(|(k, v)| serde_json::to_vec(v).ok().map(|bytes| (k.as_str(), bytes)))
        .collect();
    let claim_refs: Vec<ClaimRef<'_>> = claim_bufs
        .iter()
        .map(|(k, v)| ClaimRef {
            key: k,
            value: v.as_slice(),
        })
        .collect();
    let header_refs: Vec<HeaderRef<'_>> = ctx
        .downstream_headers
        .iter()
        .filter(|(name, _)| !is_stripped_downstream_header(name.as_str(), cookie_redaction))
        .map(|(name, value)| HeaderRef {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect();
    let candidate_id_bufs: Vec<String> = candidates
        .iter()
        .map(|c| c.upstream_id.to_string())
        .collect();
    let candidate_refs: Vec<UpstreamCandidateRef<'_>> = candidates
        .iter()
        .zip(candidate_id_bufs.iter())
        .map(|(c, id_str)| {
            let cache_score = c.cache_score.as_ref();
            UpstreamCandidateRef {
                upstream_id: id_str.as_str(),
                name: c.name.as_str(),
                kind: c.kind.as_str(),
                observed_at_unix_secs: c.observed_at_unix_secs,
                predicted_cache_read_tokens: cache_score
                    .map(|s| s.predicted_cache_read_tokens)
                    .unwrap_or(0),
                predicted_cache_creation_tokens_5m: cache_score
                    .map(|s| s.predicted_cache_creation_tokens_5m)
                    .unwrap_or(0),
                predicted_cache_creation_tokens_1h: cache_score
                    .map(|s| s.predicted_cache_creation_tokens_1h)
                    .unwrap_or(0),
                predicted_uncached_input_tokens: cache_score
                    .map(|s| s.predicted_uncached_input_tokens)
                    .unwrap_or(0),
                plan_capacity_ratio: c.plan_capacity_ratio.unwrap_or(1.0),
                organization_type: c.organization_type.as_deref().unwrap_or(""),
                rate_limit_tier: c.rate_limit_tier.as_deref().unwrap_or(""),
                seat_tier: c.seat_tier.as_deref().unwrap_or(""),
            }
        })
        .collect();
    let query_ref = ctx.query.as_deref().map(|s| QueryRef { value: s });
    let thread_id_ref = ctx.thread_id.as_deref().map(|s| QueryRef { value: s });
    let request = FilterRequestRef {
        request_id: ctx.request_id.as_str(),
        thread_id: thread_id_ref,
        canonical_model_id: ctx.canonical_model_id.as_str(),
        cache_pricing: CachePricingSummaryRef {
            status: ctx.cache_pricing.status.as_str(),
            input_micros_per_million: ctx.cache_pricing.input_micros_per_million,
            cache_creation_5m_micros_per_million: ctx
                .cache_pricing
                .cache_creation_5m_micros_per_million,
            cache_creation_1h_micros_per_million: ctx
                .cache_pricing
                .cache_creation_1h_micros_per_million,
            cache_read_micros_per_million: ctx.cache_pricing.cache_read_micros_per_million,
        },
        method: ctx.method.as_str(),
        path: ctx.path.as_str(),
        query: query_ref,
        headers: &header_refs,
        body: ctx.body_bytes.as_ref(),
        principal: PrincipalRef {
            id: principal.id.as_str(),
            kind: principal_kind_str,
            claims: &claim_refs,
        },
        candidates: &candidate_refs,
    };
    serialize_with_input_scratch(&request, with_bytes)
}

pub(super) fn principal_kind_to_wire(principal: &Principal) -> &'static str {
    match principal.kind {
        PrincipalKind::ApiKey => "api_key",
        PrincipalKind::OAuthSubject => "oauth_subject",
        PrincipalKind::InternalKey => "internal_key",
        PrincipalKind::WorkloadIdentity => "workload_identity",
        PrincipalKind::SubscriptionBearer => "subscription_bearer",
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
    cookie_redaction && lower.as_str() == "cookie"
}

fn wire_to_host_output(
    archived: &ArchivedFilterResponse,
    reason_cap: usize,
) -> Result<FilterOutput, FilterError> {
    let mut kept_upstream_ids = Vec::new();
    let mut per_candidate_reasons = Vec::new();
    let mut reasons = Vec::new();

    for result in archived.results.iter() {
        let upstream_id_str: &str = &result.upstream_id;
        let decision_str: &str = &result.decision;
        let reason_str: &str = &result.reason;
        let upstream_id =
            Uuid::parse_str(upstream_id_str).map_err(|source| FilterError::Runtime {
                reason: format!(
                    "plugin returned invalid upstream_id `{upstream_id_str}`: {source}"
                ),
            })?;
        if decision_str == "accept" {
            kept_upstream_ids.push(upstream_id);
        } else {
            per_candidate_reasons.push(per_candidate_reason_from_label(decision_str, reason_str));
        }
        if !reason_str.is_empty() {
            let truncated = truncate_reason(reason_str, reason_cap);
            reasons.push(format!("{upstream_id_str}: {truncated}"));
        }
    }

    Ok(FilterOutput {
        kept_upstream_ids,
        reason: reasons.join("; "),
        per_candidate_reasons,
        subscription_preference: None,
        cache_affinity: None,
    })
}

fn truncate_reason(reason: &str, cap: usize) -> std::borrow::Cow<'_, str> {
    if reason.len() <= cap {
        return std::borrow::Cow::Borrowed(reason);
    }
    let mut end = cap;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Owned(reason[..end].to_owned())
}

fn per_candidate_reason_from_label(decision: &str, reason: &str) -> PerCandidateReason {
    let label = if decision == "accept" {
        reason
    } else {
        decision
    };
    let label = label.replace('-', "_").to_ascii_lowercase();
    if label.contains("rate_limit") {
        PerCandidateReason::RateLimited
    } else if label.contains("quota") {
        PerCandidateReason::InsufficientQuota
    } else if label.contains("unhealthy") {
        PerCandidateReason::Unhealthy
    } else {
        PerCandidateReason::RejectedByPlugin
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_domain::PrincipalKind;
    use cc_lb_plugin_wire::FilterResponse as WireFilterResponse;
    use cc_lb_plugin_wire::PerCandidateReason as WirePerCandidateReason;

    fn fixture_principal() -> Principal {
        let mut claims = serde_json::Map::new();
        claims.insert("scope".to_owned(), serde_json::Value::from("inference"));
        Principal {
            id: "tenant-a".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims,
        }
    }

    fn fixture_request() -> RoutingContext {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer secret"),
        );
        RoutingContext {
            request_id: "req-123".to_owned(),
            thread_id: None,
            downstream_headers: headers,
            method: http::Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: bytes::Bytes::from_static(b"{\"msg\":\"hi\"}"),
            canonical_model_id: "claude-fixture".to_owned(),
            cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        }
    }

    #[test]
    fn host_to_wire_strips_auth_headers() {
        let principal = fixture_principal();
        let ctx = fixture_request();
        with_wire_request(&ctx, &principal, &[], false, |bytes| {
            let archived =
                rkyv::access::<cc_lb_plugin_wire::ArchivedFilterRequest, RkyvError>(bytes)
                    .expect("archived");
            let request_id: &str = &archived.request_id;
            let method: &str = &archived.method;
            let path: &str = &archived.path;
            assert_eq!(request_id, "req-123");
            assert_eq!(method, "POST");
            assert_eq!(path, "/v1/messages");
            assert_eq!(
                archived.headers.len(),
                1,
                "authorization must be filtered out"
            );
            let header_name: &str = &archived.headers[0].name;
            assert_eq!(header_name, "content-type");
        })
        .expect("encode");
    }

    #[test]
    fn wire_to_host_splits_kept_and_rejected() {
        let response = WireFilterResponse {
            results: Box::new([
                WirePerCandidateReason {
                    upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
                    decision: Box::from("accept"),
                    reason: Box::from("top-K"),
                },
                WirePerCandidateReason {
                    upstream_id: Box::from("22222222-2222-2222-2222-222222222222"),
                    decision: Box::from("rate-limit"),
                    reason: Box::from("burst exceeded"),
                },
            ]),
        };
        let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
        let archived =
            rkyv::access::<ArchivedFilterResponse, RkyvError>(&bytes).expect("archived view");
        let out = wire_to_host_output(archived, 256).expect("conversion must succeed");
        assert_eq!(out.kept_upstream_ids.len(), 1);
        assert_eq!(
            out.kept_upstream_ids[0],
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
        );
        assert_eq!(out.per_candidate_reasons.len(), 1);
        assert_eq!(
            out.per_candidate_reasons[0],
            PerCandidateReason::RateLimited
        );
    }
}
