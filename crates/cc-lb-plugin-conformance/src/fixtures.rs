//! Thin helpers for the boilerplate every conformance test hits.
//!
//! Deliberately narrow: builder pattern is NOT provided because rkyv
//! wire types benefit from explicit struct-literal construction (the
//! author can see every field they're setting, and unset fields fall
//! through to `Default` explicitly in the plugin's own test). What the
//! author actually re-types every time is header construction and a
//! synthetic principal.

use cc_lb_plugin_wire::{schema::ProviderFamily, v2, v3};

/// Build a `v2::Header` from `(name, value)`. Value can be `&str`, `&[u8]`,
/// `String`, or anything that dereferences to bytes.
pub fn hdr(name: impl Into<String>, value: impl AsRef<[u8]>) -> v2::Header {
    v2::Header {
        name: name.into().into_boxed_str(),
        value: value.as_ref().to_vec().into_boxed_slice(),
    }
}

/// Synthetic API-key principal — safe defaults, no real credentials.
/// Use as the `principal` field of a `v2::ShapeRequest` literal when the plugin
/// under test does not care about principal details.
pub fn synth_principal() -> v2::Principal {
    v2_principal()
}

/// Protocol-valid minimal V2 `ShapeRequest` — POST /v1/messages, JSON
/// content-type header, small JSON body, [`synth_principal`],
/// `AnthropicDirect`. Returned owned + mutable so authors can tweak individual
/// fields before passing to `PluginSession::call_shape`.
pub fn sample_shape_request() -> v2::ShapeRequest {
    sample_shape_request_for(ProviderFamily::Anthropic)
}

/// Protocol-valid minimal V3 `FilterRequest` for Anthropic — POST /v1/messages,
/// JSON content-type header, small JSON body, `service_tier = Some("priority")`.
pub fn sample_filter_request() -> v3::FilterRequest {
    sample_filter_request_v3(ProviderFamily::Anthropic)
}

/// Protocol-valid minimal V2 `TransformResponseRequest` for Anthropic.
pub fn sample_transform_response_request() -> v2::TransformResponseRequest {
    sample_transform_response_request_for(ProviderFamily::Anthropic)
}

/// Protocol-valid minimal V2 `TransformSseEventRequest` for Anthropic.
pub fn sample_transform_sse_event_request() -> v2::TransformSseEventRequest {
    sample_transform_sse_event_request_for(ProviderFamily::Anthropic)
}

/// Provider-native V2 shape fixture.
pub fn sample_shape_request_for(provider: ProviderFamily) -> v2::ShapeRequest {
    sample_shape_request_v2(provider)
}

/// OpenAI Responses shape fixture.
pub fn sample_openai_responses_shape_request() -> v2::ShapeRequest {
    sample_shape_request_for(ProviderFamily::Openai)
}

/// OpenAI Chat Completions shape fixture.
pub fn sample_openai_chat_completions_shape_request() -> v2::ShapeRequest {
    sample_shape_request_with(
        ProviderFamily::Openai,
        "/v1/chat/completions",
        br#"{"model":"gpt-4.1","messages":[{"role":"user","content":"probe"}]}"#,
    )
}

/// Gemini native `generateContent` shape fixture.
pub fn sample_gemini_generate_content_shape_request() -> v2::ShapeRequest {
    sample_shape_request_for(ProviderFamily::Gemini)
}

/// Gemini native `streamGenerateContent` shape fixture.
pub fn sample_gemini_stream_generate_content_shape_request() -> v2::ShapeRequest {
    sample_shape_request_with(
        ProviderFamily::Gemini,
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
        br#"{"contents":[{"role":"user","parts":[{"text":"probe"}]}]}"#,
    )
}

fn sample_shape_request_with(
    provider: ProviderFamily,
    path: &str,
    body: &[u8],
) -> v2::ShapeRequest {
    v2::ShapeRequest {
        request_id: Box::from("conformance-req-1"),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(body),
        principal: synth_principal(),
        upstream: v2_upstream_for(provider),
        upstream_identity: v2_identity_for(provider),
    }
}

/// Provider-native V2 buffered-response fixture.
pub fn sample_transform_response_request_for(
    provider: ProviderFamily,
) -> v2::TransformResponseRequest {
    sample_transform_response_request_v2(provider)
}

pub fn sample_openai_responses_transform_response_request() -> v2::TransformResponseRequest {
    sample_transform_response_request_for(ProviderFamily::Openai)
}

pub fn sample_openai_chat_completions_transform_response_request() -> v2::TransformResponseRequest {
    let mut request = sample_transform_response_request_for(ProviderFamily::Openai);
    request.request_path = Box::from("/v1/chat/completions");
    request.body = Box::from(
        &br#"{"id":"chatcmpl_probe","choices":[{"message":{"role":"assistant","content":"probe"}}]}"#[..],
    );
    request
}

pub fn sample_gemini_generate_content_transform_response_request() -> v2::TransformResponseRequest {
    sample_transform_response_request_for(ProviderFamily::Gemini)
}

pub fn sample_gemini_stream_generate_content_transform_response_request()
-> v2::TransformResponseRequest {
    let mut request = sample_transform_response_request_for(ProviderFamily::Gemini);
    request.request_path = Box::from("/v1beta/models/gemini-2.5-flash:streamGenerateContent");
    request
}

/// Provider-native V2 streaming-event fixture.
pub fn sample_transform_sse_event_request_for(
    provider: ProviderFamily,
) -> v2::TransformSseEventRequest {
    sample_transform_sse_event_request_v2(provider)
}

pub fn sample_openai_responses_transform_sse_event_request() -> v2::TransformSseEventRequest {
    sample_transform_sse_event_request_for(ProviderFamily::Openai)
}

pub fn sample_openai_chat_completions_transform_sse_event_request() -> v2::TransformSseEventRequest
{
    let mut request = sample_transform_sse_event_request_for(ProviderFamily::Openai);
    request.request_path = Box::from("/v1/chat/completions");
    request.event.event = Box::from("message");
    request.event.data =
        Box::from(&br#"{"id":"chatcmpl_probe","choices":[{"delta":{"content":"probe"}}]}"#[..]);
    request
}

pub fn sample_gemini_generate_content_transform_sse_event_request() -> v2::TransformSseEventRequest
{
    sample_transform_sse_event_request_for(ProviderFamily::Gemini)
}

pub fn sample_gemini_stream_generate_content_transform_sse_event_request()
-> v2::TransformSseEventRequest {
    let mut request = sample_transform_sse_event_request_for(ProviderFamily::Gemini);
    request.request_path = Box::from("/v1beta/models/gemini-2.5-flash:streamGenerateContent");
    request
}

fn response_values(provider: ProviderFamily) -> (&'static str, &'static str, &'static [u8]) {
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

/// V3 filter fixture with provider-native pricing in every supported state.
pub fn sample_filter_request_v3(provider: ProviderFamily) -> v3::FilterRequest {
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
    v3::FilterRequest {
        request_id: Box::from("conformance-req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from(model),
        cache_pricing: v2_cache_pricing(),
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
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([v3::Header {
            name: Box::from("content-type"),
            value: Box::from(&b"application/json"[..]),
        }]),
        body: Box::from(body),
        principal: v3::Principal {
            id: Box::from("conformance-principal"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([v3::UpstreamCandidate {
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
    let (path, model, body) = response_values(provider);
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
    let (path, model, event, data) = sse_values(provider);
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
