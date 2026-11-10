//! Thin helpers for the boilerplate every conformance test hits.
//!
//! Deliberately narrow: builder pattern is NOT provided because rkyv
//! wire types benefit from explicit struct-literal construction (the
//! author can see every field they're setting, and unset fields fall
//! through to `Default` explicitly in the plugin's own test). What the
//! author actually re-types every time is header construction and a
//! synthetic principal.

use cc_lb_plugin_wire::v1::FilterRequest;
use cc_lb_plugin_wire::{
    CachePricingSummary, Header, Principal, ShapeRequest, SseEvent, TransformResponseRequest,
    TransformSseEventRequest, Upstream, schema::ProviderFamily, v2, v3,
};

/// Build a `Header` from `(name, value)`. Value can be `&str`, `&[u8]`,
/// `String`, or anything that dereferences to bytes.
pub fn hdr(name: impl Into<String>, value: impl AsRef<[u8]>) -> Header {
    Header {
        name: name.into().into_boxed_str(),
        value: value.as_ref().to_vec().into_boxed_slice(),
    }
}

/// Synthetic API-key principal — safe defaults, no real credentials.
/// Use as the `principal` field of a `ShapeRequest` / `FilterRequest`
/// literal when the plugin under test does not care about principal
/// details.
pub fn synth_principal() -> Principal {
    Principal {
        id: Box::from("conformance-principal"),
        kind: Box::from("api_key"),
        claims: Box::new([]),
    }
}

/// Protocol-valid minimal `ShapeRequest` — POST /v1/messages, JSON
/// content-type header, small JSON body, [`synth_principal`],
/// `AnthropicDirect { base_url: None }`. Returned owned + mutable so
/// authors can tweak individual fields before passing to
/// `PluginSession::call_shape`.
pub fn sample_shape_request() -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("conformance-req-1"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect { base_url: None },
    }
}

/// Protocol-valid minimal `FilterRequest` — POST /v1/messages, JSON
/// content-type header, small JSON body, [`synth_principal`], no
/// candidates. Returned owned + mutable so authors can push
/// `UpstreamCandidate`s before dispatch.
pub fn sample_filter_request() -> FilterRequest {
    FilterRequest {
        request_id: Box::from("conformance-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        cache_pricing: sample_cache_pricing(),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        candidates: Box::new([]),
    }
}

fn sample_cache_pricing() -> CachePricingSummary {
    CachePricingSummary {
        status: Box::from("unknown"),
        input_micros_per_million: None,
        cache_creation_5m_micros_per_million: None,
        cache_creation_1h_micros_per_million: None,
        cache_read_micros_per_million: None,
    }
}

/// Protocol-valid minimal `TransformResponseRequest`.
pub fn sample_transform_response_request() -> TransformResponseRequest {
    TransformResponseRequest {
        request_id: Box::from("conformance-req-1"),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect { base_url: None },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"content":[]}"#[..]),
    }
}

/// Protocol-valid minimal `TransformSseEventRequest`.
pub fn sample_transform_sse_event_request() -> TransformSseEventRequest {
    TransformSseEventRequest {
        request_id: Box::from("conformance-req-1"),
        principal: synth_principal(),
        upstream: Upstream::AnthropicDirect { base_url: None },
        request_method: Box::from("POST"),
        request_path: Box::from("/v1/messages"),
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "text/event-stream")]),
        event: SseEvent {
            event: Box::from("content_block_start"),
            data: Box::from(&br#"{"type":"content_block_start"}"#[..]),
        },
    }
}

/// Provider-native V1 shape fixture. Legacy callers should use
/// [`sample_shape_request`] to retain the Anthropic default.
pub fn sample_shape_request_for(provider: ProviderFamily) -> ShapeRequest {
    let (path, _model, body) = match provider {
        ProviderFamily::Anthropic => (
            "/v1/messages",
            "claude-3-haiku-20240307",
            br#"{"model":"claude-3-haiku-20240307","messages":[]}"#.as_slice(),
        ),
        ProviderFamily::Openai => (
            "/v1/responses",
            "gpt-4.1",
            br#"{"model":"gpt-4.1","input":"probe"}"#.as_slice(),
        ),
        ProviderFamily::Gemini => (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            "gemini-2.5-flash",
            br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#.as_slice(),
        ),
    };
    ShapeRequest {
        request_id: Box::from("conformance-req-1"),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(body),
        principal: synth_principal(),
        upstream: upstream_for(provider),
    }
}

/// OpenAI Responses shape fixture.
pub fn sample_openai_responses_shape_request() -> ShapeRequest {
    sample_shape_request_for(ProviderFamily::Openai)
}

/// OpenAI Chat Completions shape fixture.
pub fn sample_openai_chat_completions_shape_request() -> ShapeRequest {
    sample_shape_request_with(
        ProviderFamily::Openai,
        "/v1/chat/completions",
        "gpt-4.1",
        br#"{"model":"gpt-4.1","messages":[{"role":"user","content":"probe"}]}"#,
    )
}

/// Gemini native `generateContent` shape fixture.
pub fn sample_gemini_generate_content_shape_request() -> ShapeRequest {
    sample_shape_request_for(ProviderFamily::Gemini)
}

/// Gemini native `streamGenerateContent` shape fixture.
pub fn sample_gemini_stream_generate_content_shape_request() -> ShapeRequest {
    sample_shape_request_with(
        ProviderFamily::Gemini,
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
        "gemini-2.5-flash",
        br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#,
    )
}

fn sample_shape_request_with(
    provider: ProviderFamily,
    path: &str,
    _model: &str,
    body: &[u8],
) -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("conformance-req-1"),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(body),
        principal: synth_principal(),
        upstream: upstream_for(provider),
    }
}

/// Provider-native V1 buffered-response fixture.
pub fn sample_transform_response_request_for(provider: ProviderFamily) -> TransformResponseRequest {
    let (path, model, body) = response_values(provider, false);
    TransformResponseRequest {
        request_id: Box::from("conformance-req-1"),
        principal: synth_principal(),
        upstream: upstream_for(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(path),
        canonical_model_id: Box::from(model),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(body),
    }
}

pub fn sample_openai_responses_transform_response_request() -> TransformResponseRequest {
    sample_transform_response_request_for(ProviderFamily::Openai)
}

pub fn sample_openai_chat_completions_transform_response_request() -> TransformResponseRequest {
    let mut request = sample_transform_response_request_for(ProviderFamily::Openai);
    request.request_path = Box::from("/v1/chat/completions");
    request.body = Box::from(&br#"{"id":"chatcmpl_probe","choices":[{"message":{"role":"assistant","content":"probe"}}]}"#[..]);
    request
}

pub fn sample_gemini_generate_content_transform_response_request() -> TransformResponseRequest {
    sample_transform_response_request_for(ProviderFamily::Gemini)
}

pub fn sample_gemini_stream_generate_content_transform_response_request() -> TransformResponseRequest
{
    let mut request = sample_transform_response_request_for(ProviderFamily::Gemini);
    request.request_path = Box::from("/v1beta/models/gemini-2.5-flash:streamGenerateContent");
    request
}

/// Provider-native V1 streaming-event fixture.
pub fn sample_transform_sse_event_request_for(
    provider: ProviderFamily,
) -> TransformSseEventRequest {
    let (path, model, event, data) = sse_values(provider, false);
    TransformSseEventRequest {
        request_id: Box::from("conformance-req-1"),
        principal: synth_principal(),
        upstream: upstream_for(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(path),
        canonical_model_id: Box::from(model),
        response_status: 200,
        response_headers: Box::new([hdr("content-type", "text/event-stream")]),
        event: SseEvent {
            event: Box::from(event),
            data: Box::from(data),
        },
    }
}

pub fn sample_openai_responses_transform_sse_event_request() -> TransformSseEventRequest {
    sample_transform_sse_event_request_for(ProviderFamily::Openai)
}

pub fn sample_openai_chat_completions_transform_sse_event_request() -> TransformSseEventRequest {
    let mut request = sample_transform_sse_event_request_for(ProviderFamily::Openai);
    request.request_path = Box::from("/v1/chat/completions");
    request.event.event = Box::from("message");
    request.event.data =
        Box::from(&br#"{"id":"chatcmpl_probe","choices":[{"delta":{"content":"probe"}}]}"#[..]);
    request
}

pub fn sample_gemini_generate_content_transform_sse_event_request() -> TransformSseEventRequest {
    sample_transform_sse_event_request_for(ProviderFamily::Gemini)
}

pub fn sample_gemini_stream_generate_content_transform_sse_event_request()
-> TransformSseEventRequest {
    let mut request = sample_transform_sse_event_request_for(ProviderFamily::Gemini);
    request.request_path = Box::from("/v1beta/models/gemini-2.5-flash:streamGenerateContent");
    request
}

fn upstream_for(provider: ProviderFamily) -> Upstream {
    let base_url = Some(Box::from("https://example.test"));
    match provider {
        ProviderFamily::Anthropic => Upstream::AnthropicDirect { base_url },
        ProviderFamily::Openai => Upstream::OpenaiDirect { base_url },
        ProviderFamily::Gemini => Upstream::GeminiDirect { base_url },
    }
}

fn response_values(
    provider: ProviderFamily,
    _stream: bool,
) -> (&'static str, &'static str, &'static [u8]) {
    match provider {
        ProviderFamily::Anthropic => (
            "/v1/messages",
            "claude-3-haiku-20240307",
            br#"{"content":[]}"#,
        ),
        ProviderFamily::Openai => (
            "/v1/responses",
            "gpt-4.1",
            br#"{"id":"resp_probe","output":[]}"#,
        ),
        ProviderFamily::Gemini => (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            "gemini-2.5-flash",
            br#"{"candidates":[]}"#,
        ),
    }
}

fn sse_values(
    provider: ProviderFamily,
    _stream: bool,
) -> (&'static str, &'static str, &'static str, &'static [u8]) {
    match provider {
        ProviderFamily::Anthropic => (
            "/v1/messages",
            "claude-3-haiku-20240307",
            "content_block_start",
            br#"{"type":"content_block_start"}"#,
        ),
        ProviderFamily::Openai => (
            "/v1/responses",
            "gpt-4.1",
            "response.created",
            br#"{"type":"response.created","response":{"id":"resp_probe","output":[]}}"#,
        ),
        ProviderFamily::Gemini => (
            "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
            "gemini-2.5-flash",
            "",
            br#"{"candidates":[]}"#,
        ),
    }
}

/// V2 filter fixture. V2 deliberately has no upstream identity field.
pub fn sample_filter_request_v2(provider: ProviderFamily) -> v2::FilterRequest {
    let (path, model, body) = match provider {
        ProviderFamily::Anthropic => (
            "/v1/messages",
            "claude-3-haiku-20240307",
            br#"{"model":"claude-3-haiku-20240307","messages":[]}"#.as_slice(),
        ),
        ProviderFamily::Openai => (
            "/v1/responses",
            "gpt-4.1",
            br#"{"model":"gpt-4.1","input":"probe"}"#.as_slice(),
        ),
        ProviderFamily::Gemini => (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            "gemini-2.5-flash",
            br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#.as_slice(),
        ),
    };
    v2::FilterRequest {
        request_id: Box::from("conformance-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("default")),
        canonical_model_id: Box::from(model),
        cache_pricing: v2_cache_pricing(),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([v2::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        body: Box::from(body),
        principal: v2_principal(),
        candidates: Box::new([v2::UpstreamCandidate {
            upstream_id: Box::from("conformance-upstream"),
            name: Box::from("Conformance upstream"),
            kind: Box::from(v2_candidate_kind(provider)),
            base_url: Some(Box::from("https://example.test")),
            observed_at_unix_secs: 1,
            predicted_cache_read_tokens: 0,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 1,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from("individual"),
            rate_limit_tier: Box::from("default"),
            seat_tier: Box::from("default"),
            subscription_quotas: Box::new([]),
        }]),
    }
}

/// V3 filter fixture with provider-native pricing in every supported state.
pub fn sample_filter_request_v3(provider: ProviderFamily) -> v3::FilterRequest {
    let request = sample_filter_request_v2(provider);
    v3::FilterRequest {
        request_id: request.request_id,
        thread_id: request.thread_id,
        service_tier: request.service_tier,
        canonical_model_id: request.canonical_model_id,
        cache_pricing: request.cache_pricing,
        native_pricing: v3::NativeCachePricing {
            version: 1,
            provider: match provider {
                ProviderFamily::Anthropic => v3::NativeCachePricingProvider::Anthropic,
                ProviderFamily::Openai => v3::NativeCachePricingProvider::OpenAi,
                ProviderFamily::Gemini => v3::NativeCachePricingProvider::Gemini,
            },
            rates: Box::new([
                v3::NativeCachePricingRate {
                    component: v3::NativeCachePricingComponent::OrdinaryInput,
                    lifetime: None,
                    status: v3::NativeCachePricingStatus::Known,
                    micros_per_million: Some(2_000_000),
                },
                v3::NativeCachePricingRate {
                    component: v3::NativeCachePricingComponent::CachedInputRead,
                    lifetime: None,
                    status: v3::NativeCachePricingStatus::Known,
                    micros_per_million: Some(100_000),
                },
                v3::NativeCachePricingRate {
                    component: v3::NativeCachePricingComponent::TotalWrite,
                    lifetime: Some(v3::NativeCachePricingLifetime::ThirtyMinutes),
                    status: v3::NativeCachePricingStatus::Unknown,
                    micros_per_million: None,
                },
                v3::NativeCachePricingRate {
                    component: v3::NativeCachePricingComponent::ExplicitResourceStorage,
                    lifetime: Some(v3::NativeCachePricingLifetime::ExplicitResourceTtl),
                    status: v3::NativeCachePricingStatus::NotApplicable,
                    micros_per_million: None,
                },
            ]),
        },
        method: request.method,
        path: request.path,
        query: request.query,
        headers: request
            .headers
            .into_iter()
            .map(|header| v3::Header {
                name: header.name,
                value: header.value,
            })
            .collect(),
        body: request.body,
        principal: v3::Principal {
            id: request.principal.id,
            kind: request.principal.kind,
            claims: request.principal.claims,
        },
        candidates: request
            .candidates
            .into_iter()
            .map(|candidate| v3::UpstreamCandidate {
                upstream_id: candidate.upstream_id,
                name: candidate.name,
                kind: candidate.kind,
                base_url: candidate.base_url,
                observed_at_unix_secs: candidate.observed_at_unix_secs,
                predicted_cache_read_tokens: candidate.predicted_cache_read_tokens,
                predicted_cache_creation_tokens_5m: candidate.predicted_cache_creation_tokens_5m,
                predicted_cache_creation_tokens_1h: candidate.predicted_cache_creation_tokens_1h,
                predicted_uncached_input_tokens: candidate.predicted_uncached_input_tokens,
                plan_capacity_ratio: candidate.plan_capacity_ratio,
                organization_type: candidate.organization_type,
                rate_limit_tier: candidate.rate_limit_tier,
                seat_tier: candidate.seat_tier,
                subscription_quotas: candidate.subscription_quotas,
            })
            .collect(),
    }
}

pub fn sample_shape_request_v2(provider: ProviderFamily) -> v2::ShapeRequest {
    let (path, _model, body) = match provider {
        ProviderFamily::Anthropic => (
            "/v1/messages",
            "claude-3-haiku-20240307",
            br#"{"model":"claude-3-haiku-20240307","messages":[]}"#.as_slice(),
        ),
        ProviderFamily::Openai => (
            "/v1/responses",
            "gpt-4.1",
            br#"{"model":"gpt-4.1","input":"probe"}"#.as_slice(),
        ),
        ProviderFamily::Gemini => (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            "gemini-2.5-flash",
            br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#.as_slice(),
        ),
    };
    v2::ShapeRequest {
        request_id: Box::from("conformance-req-1"),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([v2::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        body: Box::from(body),
        principal: v2_principal(),
        upstream: v2_upstream_for(provider),
        upstream_identity: v2_identity_for(provider),
    }
}

/// Provider-native V2 realtime-frame fixture.
///
/// Anthropic uses a synthetic boundary payload because cc-lb currently has no
/// Anthropic realtime transport; this does not imply transport support.
pub fn sample_transform_realtime_frame_request_v2(
    provider: ProviderFamily,
) -> v2::TransformRealtimeFrameRequest {
    let (path, model, payload) = match provider {
        ProviderFamily::Anthropic => (
            "/v1/realtime",
            "claude-3-haiku-20240307",
            br#"{"type":"probe"}"#.as_slice(),
        ),
        ProviderFamily::Openai => (
            "/v1/realtime",
            "gpt-realtime",
            br#"{"type":"response.created","response":{"id":"resp_probe"}}"#.as_slice(),
        ),
        ProviderFamily::Gemini => (
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            "gemini-live",
            br#"{"serverContent":{"turnComplete":true}}"#.as_slice(),
        ),
    };
    v2::TransformRealtimeFrameRequest {
        request_id: Box::from("conformance-req-1"),
        principal: v2_principal(),
        upstream: v2_upstream_for(provider),
        upstream_identity: v2_identity_for(provider),
        request_method: Box::from("GET"),
        request_path: Box::from(path),
        canonical_model_id: Box::from(model),
        direction: Box::from("provider_to_client"),
        kind: Box::from("json"),
        payload: Box::from(payload),
    }
}

pub fn sample_transform_response_request_v2(
    provider: ProviderFamily,
) -> v2::TransformResponseRequest {
    let (path, model, body) = response_values(provider, false);
    v2::TransformResponseRequest {
        request_id: Box::from("conformance-req-1"),
        principal: v2_principal(),
        upstream: v2_upstream_for(provider),
        upstream_identity: v2_identity_for(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(path),
        canonical_model_id: Box::from(model),
        response_status: 200,
        response_headers: Box::new([v2::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        body: Box::from(body),
    }
}

pub fn sample_transform_sse_event_request_v2(
    provider: ProviderFamily,
) -> v2::TransformSseEventRequest {
    let (path, model, event, data) = sse_values(provider, false);
    v2::TransformSseEventRequest {
        request_id: Box::from("conformance-req-1"),
        principal: v2_principal(),
        upstream: v2_upstream_for(provider),
        upstream_identity: v2_identity_for(provider),
        request_method: Box::from("POST"),
        request_path: Box::from(path),
        canonical_model_id: Box::from(model),
        response_status: 200,
        response_headers: Box::new([v2::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"text/event-stream"[..]),
        }]),
        event: v2::SseEvent {
            event: Box::from(event),
            data: Box::from(data),
        },
    }
}

pub fn sample_gemini_shape_envelope_request() -> v2::ShapeEnvelopeGeminiRequest {
    v2::ShapeEnvelopeGeminiRequest {
        envelope_body: Box::from(
            &br#"{"model":"gemini-2.5-flash","project":"probe-project","request":{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}}"#[..],
        ),
        url: Box::from("https://cloudcode-pa.googleapis.com/v1internal:generateContent"),
        headers: Box::new([v2::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        upstream: v2::UpstreamIdentity {
            provider: Box::from("gemini"),
            credential_kind: Box::from("gemini_oauth"),
            upstream_id: Box::from("conformance-upstream"),
            name: Box::from("Conformance Gemini OAuth"),
            base_url: Some(Box::from("https://example.test")),
            gemini_oauth_profile: Some(Box::from("gemini_cli")),
            code_assist_project_id: Some(Box::from("probe-project")),
        },
    }
}

fn v2_principal() -> v2::Principal {
    v2::Principal {
        id: Box::from("conformance-principal"),
        kind: Box::from("api_key"),
        claims: Box::new([]),
    }
}

fn v2_cache_pricing() -> v2::CachePricingSummary {
    v2::CachePricingSummary {
        status: Box::from("unknown"),
        input_micros_per_million: None,
        cache_creation_5m_micros_per_million: None,
        cache_creation_1h_micros_per_million: None,
        cache_read_micros_per_million: None,
    }
}

fn v2_upstream_for(provider: ProviderFamily) -> v2::Upstream {
    let base_url = Some(Box::from("https://example.test"));
    match provider {
        ProviderFamily::Anthropic => v2::Upstream::AnthropicDirect { base_url },
        ProviderFamily::Openai => v2::Upstream::OpenaiDirect { base_url },
        ProviderFamily::Gemini => v2::Upstream::GeminiDirect { base_url },
    }
}

// Mirrors `cc_lb_domain::UpstreamKind::as_str`; this crate only depends on the
// public plugin wire crate.
fn v2_candidate_kind(provider: ProviderFamily) -> &'static str {
    match provider {
        ProviderFamily::Anthropic => "anthropic_api_key",
        ProviderFamily::Openai => "openai_api_key",
        ProviderFamily::Gemini => "gemini_api_key",
    }
}

fn v2_identity_for(provider: ProviderFamily) -> v2::UpstreamIdentity {
    v2::UpstreamIdentity {
        provider: Box::from(provider.as_str()),
        credential_kind: Box::from("api_key"),
        upstream_id: Box::from("conformance-upstream"),
        name: Box::from("Conformance upstream"),
        base_url: Some(Box::from("https://example.test")),
        gemini_oauth_profile: None,
        code_assist_project_id: None,
    }
}
