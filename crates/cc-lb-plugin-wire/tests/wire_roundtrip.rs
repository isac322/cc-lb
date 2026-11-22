//! Round-trip every current wire type through `rkyv::to_bytes` →
//! `rkyv::access` → `rkyv::deserialize` to catch derive mismatches
//! and bytecheck failures before they reach the host/guest boundary.
//!
//! Also asserts the RFC-0001 #9 borrowed-wire ABI contract: the bytes
//! emitted by encoding a `*RequestRef<'_>` are structurally decodable via
//! the owned `Archived*` view.

use cc_lb_plugin_wire::{
    WireSchema,
    schema::{HookKind, WireVersion},
    v2::{
        ArchivedShapeEnvelopeGeminiRequest, ArchivedShapeEnvelopeGeminiResponse,
        ArchivedShapeRequest, ArchivedTransformRealtimeFrameRequest,
        ArchivedTransformRealtimeFrameResult, ArchivedTransformResponseRequest,
        ArchivedTransformResponseResult, ArchivedTransformSseEventRequest,
        ArchivedTransformSseEventResult, CachePricingSummary, CachePricingSummaryRef, Claim,
        ClaimRef, FilterResponse, Header, HeaderRef, OptionalStrRef, PerCandidateReason, Principal,
        PrincipalRef, ShapeEnvelopeGeminiRequest, ShapeEnvelopeGeminiRequestRef,
        ShapeEnvelopeGeminiResponse, ShapeRequest, ShapeRequestRef, ShapeResponse, SseEvent,
        SseEventRef, SubscriptionQuotaSample, SubscriptionQuotaSampleRef,
        TransformRealtimeFrameRequest, TransformRealtimeFrameRequestRef,
        TransformRealtimeFrameResult, TransformResponseRequest, TransformResponseRequestRef,
        TransformResponseResult, TransformSseEventRequest, TransformSseEventRequestRef,
        TransformSseEventResult, Upstream, UpstreamCandidate, UpstreamCandidateRef,
        UpstreamIdentity, UpstreamIdentityRef, UpstreamRef,
    },
    v3::{
        ArchivedFilterRequest as ArchivedFilterRequestV3, FilterRequest, FilterRequestRef,
        NativeCachePricing, NativeCachePricingComponent, NativeCachePricingProvider,
        NativeCachePricingRate, NativeCachePricingRateRef, NativeCachePricingRef,
        NativeCachePricingStatus,
    },
};
use rkyv::rancor::Error;

fn principal() -> Principal {
    Principal {
        id: Box::from("tenant-a"),
        kind: Box::from("api_key"),
        claims: Box::new([Claim {
            key: Box::from("scope"),
            value: Box::from(&b"inference"[..]),
        }]),
    }
}

fn header(name: &str, value: &str) -> Header {
    Header {
        name: Box::from(name),
        value: Box::from(value.as_bytes()),
    }
}

fn upstream_identity() -> UpstreamIdentity {
    UpstreamIdentity {
        provider: Box::from("anthropic"),
        credential_kind: Box::from("anthropic_api_key"),
        upstream_id: Box::from("u-1"),
        name: Box::from("anthropic-direct"),
        base_url: Some(Box::from("https://api.anthropic.com")),
        gemini_oauth_profile: None,
        code_assist_project_id: None,
    }
}

fn upstream_identity_ref() -> UpstreamIdentityRef<'static> {
    UpstreamIdentityRef {
        provider: "anthropic",
        credential_kind: "anthropic_api_key",
        upstream_id: "u-1",
        name: "anthropic-direct",
        base_url: Some(OptionalStrRef {
            value: "https://api.anthropic.com",
        }),
        gemini_oauth_profile: None,
        code_assist_project_id: None,
    }
}

#[test]
fn filter_request_v3_and_response_round_trip() {
    let req = FilterRequest {
        request_id: Box::from("req-1"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: CachePricingSummary {
            status: Box::from("known"),
            input_micros_per_million: Some(5_000_000),
            cache_creation_5m_micros_per_million: Some(6_250_000),
            cache_creation_1h_micros_per_million: Some(10_000_000),
            cache_read_micros_per_million: Some(500_000),
        },
        native_pricing: NativeCachePricing {
            version: 1,
            provider: NativeCachePricingProvider::Anthropic,
            rates: Box::new([NativeCachePricingRate {
                component: NativeCachePricingComponent::OrdinaryInput,
                lifetime: None,
                status: NativeCachePricingStatus::Known,
                micros_per_million: Some(5_000_000),
            }]),
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([header("content-type", "application/json")]),
        body: Box::from(&b"{\"k\":1}"[..]),
        principal: principal(),
        candidates: Box::new([UpstreamCandidate {
            upstream_id: Box::from("u-1"),
            name: Box::from("anthropic-direct"),
            kind: Box::from("anthropic_api_key"),
            base_url: Some(Box::from("https://api.anthropic.com")),
            observed_at_unix_secs: 42,
            predicted_cache_read_tokens: 256,
            predicted_cache_creation_tokens_5m: 512,
            predicted_cache_creation_tokens_1h: 1024,
            predicted_uncached_input_tokens: 32,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([SubscriptionQuotaSample {
                window: Box::from("5h"),
                utilization: Some(0.25),
                resets_at_unix_secs: Some(1_700_000_000),
                status: Some(Box::from("allowed")),
                sample_kind: Box::from("sample"),
                observed_at_unix_millis: Some(1_699_999_000_000),
            }]),
        }]),
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedFilterRequestV3, Error>(&bytes).expect("access");
    let owned: FilterRequest =
        rkyv::deserialize::<FilterRequest, Error>(archived).expect("deserialize");
    assert_eq!(&*owned.request_id, "req-1");
    assert_eq!(&*owned.canonical_model_id, "claude-test");
    assert_eq!(owned.candidates.len(), 1);
    assert_eq!(owned.candidates[0].predicted_cache_read_tokens, 256);
    assert_eq!(owned.candidates[0].predicted_cache_creation_tokens_5m, 512);

    let resp = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("u-1"),
            decision: Box::from("accept"),
            reason: Box::from("ok"),
        }]),
    };
    let resp_bytes = rkyv::to_bytes::<Error>(&resp).expect("encode resp");
    let archived_resp =
        rkyv::access::<cc_lb_plugin_wire::v2::ArchivedFilterResponse, Error>(&resp_bytes)
            .expect("access resp");
    assert_eq!(archived_resp.results.len(), 1);
}

#[test]
fn filter_request_v3_ref_encodes_to_owned_wire() {
    let claim_refs = [ClaimRef {
        key: "scope",
        value: b"inference",
    }];
    let header_refs = [HeaderRef {
        name: "content-type",
        value: b"application/json",
    }];
    let quota_refs = [SubscriptionQuotaSampleRef {
        window: "5h",
        utilization: Some(0.25),
        resets_at_unix_secs: Some(1_700_000_000),
        status: Some(OptionalStrRef { value: "allowed" }),
        sample_kind: "sample",
        observed_at_unix_millis: Some(1_699_999_000_000),
    }];
    let candidate_refs = [UpstreamCandidateRef {
        upstream_id: "u-1",
        name: "anthropic-direct",
        kind: "anthropic_api_key",
        base_url: Some(OptionalStrRef {
            value: "https://api.anthropic.com",
        }),
        observed_at_unix_secs: 42,
        predicted_cache_read_tokens: 256,
        predicted_cache_creation_tokens_5m: 512,
        predicted_cache_creation_tokens_1h: 1024,
        predicted_uncached_input_tokens: 32,
        plan_capacity_ratio: 1.0,
        organization_type: "",
        rate_limit_tier: "",
        seat_tier: "",
        subscription_quotas: &quota_refs,
    }];
    let rate_refs = [NativeCachePricingRateRef {
        component: NativeCachePricingComponent::OrdinaryInput,
        lifetime: None,
        status: NativeCachePricingStatus::Known,
        micros_per_million: Some(5_000_000),
    }];
    let req_ref = FilterRequestRef {
        request_id: "req-1",
        thread_id: None,
        service_tier: Some(OptionalStrRef { value: "priority" }),
        canonical_model_id: "claude-test",
        cache_pricing: CachePricingSummaryRef {
            status: "known",
            input_micros_per_million: Some(5_000_000),
            cache_creation_5m_micros_per_million: Some(6_250_000),
            cache_creation_1h_micros_per_million: Some(10_000_000),
            cache_read_micros_per_million: Some(500_000),
        },
        native_pricing: NativeCachePricingRef {
            version: 1,
            provider: NativeCachePricingProvider::Anthropic,
            rates: &rate_refs,
        },
        method: "POST",
        path: "/v1/messages",
        query: None,
        headers: &header_refs,
        body: b"{\"k\":1}",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &claim_refs,
        },
        candidates: &candidate_refs,
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived = rkyv::access::<ArchivedFilterRequestV3, Error>(&bytes).expect("access owned");
    assert_eq!(&*archived.body, b"{\"k\":1}");
    assert_eq!(&*archived.request_id, "req-1");
    assert_eq!(
        archived.service_tier.as_ref().map(|v| &**v),
        Some("priority")
    );
    assert_eq!(&*archived.canonical_model_id, "claude-test");
}

#[test]
fn shape_request_round_trips() {
    let req = ShapeRequest {
        request_id: Box::from("req-2"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: Some(Box::from("stream=true")),
        headers: Box::new([header("accept", "text/event-stream")]),
        body: Box::from(&b"body"[..]),
        principal: principal(),
        upstream: Upstream::AnthropicDirect {
            base_url: Some(Box::from("https://api.anthropic.com")),
        },
        upstream_identity: upstream_identity(),
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedShapeRequest, Error>(&bytes).expect("access");
    let owned: ShapeRequest =
        rkyv::deserialize::<ShapeRequest, Error>(archived).expect("deserialize");
    assert_eq!(owned.query.as_deref().unwrap_or(""), "stream=true");
    match owned.upstream {
        Upstream::AnthropicDirect { base_url }
        | Upstream::OpenaiDirect { base_url }
        | Upstream::GeminiDirect { base_url } => {
            assert_eq!(
                base_url.as_deref().unwrap_or(""),
                "https://api.anthropic.com"
            )
        }
    }
}

#[test]
fn shape_request_ref_encodes_to_owned_wire() {
    let header_refs = [HeaderRef {
        name: "accept",
        value: b"text/event-stream",
    }];
    let req_ref = ShapeRequestRef {
        request_id: "req-2",
        method: "POST",
        path: "/v1/messages",
        query: Some(OptionalStrRef {
            value: "stream=true",
        }),
        headers: &header_refs,
        body: b"body",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &[],
        },
        upstream: UpstreamRef::AnthropicDirect {
            base_url: Some(OptionalStrRef {
                value: "https://api.anthropic.com",
            }),
        },
        upstream_identity: upstream_identity_ref(),
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived = rkyv::access::<ArchivedShapeRequest, Error>(&bytes).expect("access owned");
    assert_eq!(&*archived.body, b"body");
    assert_eq!(&*archived.upstream_identity.provider, "anthropic");
}

#[test]
fn shape_response_round_trips() {
    let resp = ShapeResponse {
        url: Box::from("https://api.anthropic.com/v1/messages"),
        method: Box::from("POST"),
        headers: Box::new([header("x-api-key", "redacted")]),
        body: Box::from(&b"shaped"[..]),
    };
    let bytes = rkyv::to_bytes::<Error>(&resp).expect("encode");
    let _ = bytes;
}

#[test]
fn transform_response_request_ref_encodes_to_owned_wire() {
    let claim_refs = [ClaimRef {
        key: "scope",
        value: b"inference",
    }];
    let header_refs = [HeaderRef {
        name: "content-type",
        value: b"application/json",
    }];
    let req_ref = TransformResponseRequestRef {
        request_id: "req-transform",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &claim_refs,
        },
        upstream: UpstreamRef::AnthropicDirect { base_url: None },
        upstream_identity: upstream_identity_ref(),
        request_method: "POST",
        request_path: "/v1/messages",
        canonical_model_id: "claude-test",
        response_status: 200,
        response_headers: &header_refs,
        body: b"{\"content\":[]}",
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived =
        rkyv::access::<ArchivedTransformResponseRequest, Error>(&bytes).expect("access owned");
    assert_eq!(&*archived.body, b"{\"content\":[]}");
    assert_eq!(&*archived.request_id, "req-transform");
}

#[test]
fn transform_response_result_round_trips() {
    let result = TransformResponseResult::Replace {
        status: Some(202),
        headers: Some(Box::new([header("x-plugin", "yes")])),
        body: Some(Box::from(&b"{\"ok\":true}"[..])),
    };
    let bytes = rkyv::to_bytes::<Error>(&result).expect("encode");
    let archived = rkyv::access::<ArchivedTransformResponseResult, Error>(&bytes).expect("access");
    let owned: TransformResponseResult =
        rkyv::deserialize::<TransformResponseResult, Error>(archived).expect("deserialize");
    match owned {
        TransformResponseResult::Replace { status, body, .. } => {
            assert_eq!(status, Some(202));
            assert_eq!(body.as_deref(), Some(&b"{\"ok\":true}"[..]));
        }
        TransformResponseResult::Unchanged => panic!("variant mismatch"),
    }
}

#[test]
fn transform_sse_event_request_ref_encodes_to_owned_wire() {
    let header_refs = [HeaderRef {
        name: "content-type",
        value: b"text/event-stream",
    }];
    let event_ref = SseEventRef {
        event: "content_block_start",
        data: br#"{"type":"content_block_start"}"#,
    };
    let req_ref = TransformSseEventRequestRef {
        request_id: "req-sse",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &[],
        },
        upstream: UpstreamRef::AnthropicDirect { base_url: None },
        upstream_identity: upstream_identity_ref(),
        request_method: "POST",
        request_path: "/v1/messages",
        canonical_model_id: "claude-test",
        response_status: 200,
        response_headers: &header_refs,
        event: event_ref,
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived =
        rkyv::access::<ArchivedTransformSseEventRequest, Error>(&bytes).expect("access owned");
    assert_eq!(&*archived.event.event, "content_block_start");
    assert_eq!(&*archived.event.data, br#"{"type":"content_block_start"}"#);
}

#[test]
fn transform_sse_event_result_round_trips() {
    let result = TransformSseEventResult::Replace {
        events: Box::new([SseEvent {
            event: Box::from("content_block_start"),
            data: Box::from(&br#"{"rewritten":true}"#[..]),
        }]),
    };
    let bytes = rkyv::to_bytes::<Error>(&result).expect("encode");
    let archived = rkyv::access::<ArchivedTransformSseEventResult, Error>(&bytes).expect("access");
    let owned: TransformSseEventResult =
        rkyv::deserialize::<TransformSseEventResult, Error>(archived).expect("deserialize");
    match owned {
        TransformSseEventResult::Replace { events } => {
            assert_eq!(events.len(), 1);
            assert_eq!(&*events[0].event, "content_block_start");
        }
        TransformSseEventResult::Unchanged | TransformSseEventResult::Drop => {
            panic!("variant mismatch")
        }
    }
}

#[test]
fn realtime_frame_and_gemini_envelope_round_trip() {
    let frame_ref = TransformRealtimeFrameRequestRef {
        request_id: "req-rt",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &[],
        },
        upstream: UpstreamRef::OpenaiDirect { base_url: None },
        upstream_identity: UpstreamIdentityRef {
            provider: "openai",
            credential_kind: "openai_api_key",
            upstream_id: "u-rt",
            name: "openai-direct",
            base_url: None,
            gemini_oauth_profile: None,
            code_assist_project_id: None,
        },
        request_method: "GET",
        request_path: "/v1/realtime",
        canonical_model_id: "gpt-realtime",
        direction: "client_to_upstream",
        kind: "text",
        payload: br#"{"type":"session.update"}"#,
    };
    let bytes = rkyv::to_bytes::<Error>(&frame_ref).expect("encode frame");
    let archived =
        rkyv::access::<ArchivedTransformRealtimeFrameRequest, Error>(&bytes).expect("access");
    assert_eq!(&*archived.direction, "client_to_upstream");

    let frame_result = TransformRealtimeFrameResult::Replace {
        kind: Box::from("text"),
        payload: Box::from(&br#"{"ok":true}"#[..]),
    };
    let res_bytes = rkyv::to_bytes::<Error>(&frame_result).expect("encode frame res");
    let archived_res =
        rkyv::access::<ArchivedTransformRealtimeFrameResult, Error>(&res_bytes).expect("access");
    assert!(matches!(
        archived_res,
        cc_lb_plugin_wire::v2::ArchivedTransformRealtimeFrameResult::Replace { .. }
    ));

    let env_ref = ShapeEnvelopeGeminiRequestRef {
        envelope_body: br#"{"project":"p"}"#,
        url: "https://cloudcode-pa.googleapis.com/v1internal:generateContent",
        headers: &[],
        upstream: UpstreamIdentityRef {
            provider: "gemini",
            credential_kind: "gemini_oauth",
            upstream_id: "u-g",
            name: "gemini-oauth",
            base_url: None,
            gemini_oauth_profile: Some(OptionalStrRef { value: "default" }),
            code_assist_project_id: Some(OptionalStrRef { value: "proj-1" }),
        },
    };
    let env_bytes = rkyv::to_bytes::<Error>(&env_ref).expect("encode envelope");
    let archived_env =
        rkyv::access::<ArchivedShapeEnvelopeGeminiRequest, Error>(&env_bytes).expect("access");
    assert_eq!(&*archived_env.envelope_body, br#"{"project":"p"}"#);

    let env_resp = ShapeEnvelopeGeminiResponse {
        envelope_body: Some(Box::from(&br#"{"project":"p2"}"#[..])),
        headers: None,
    };
    let env_resp_bytes = rkyv::to_bytes::<Error>(&env_resp).expect("encode env resp");
    let archived_env_resp =
        rkyv::access::<ArchivedShapeEnvelopeGeminiResponse, Error>(&env_resp_bytes)
            .expect("access");
    assert!(archived_env_resp.envelope_body.is_some());
}

#[test]
fn schema_fingerprints_and_sections_are_distinct() {
    let fingerprints = [
        <FilterRequest as WireSchema>::FINGERPRINT,
        <ShapeRequest as WireSchema>::FINGERPRINT,
        <TransformResponseRequest as WireSchema>::FINGERPRINT,
        <TransformSseEventRequest as WireSchema>::FINGERPRINT,
        <TransformRealtimeFrameRequest as WireSchema>::FINGERPRINT,
        <ShapeEnvelopeGeminiRequest as WireSchema>::FINGERPRINT,
    ];
    for i in 0..fingerprints.len() {
        for j in i + 1..fingerprints.len() {
            assert_ne!(
                fingerprints[i], fingerprints[j],
                "schema fingerprints {i} and {j} collide"
            );
        }
    }
    let sections = [
        format!(
            "{}.{}",
            HookKind::Filter.section_prefix(),
            WireVersion::V3.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::Shape.section_prefix(),
            WireVersion::V2.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::TransformResponse.section_prefix(),
            WireVersion::V2.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::TransformSseEvent.section_prefix(),
            WireVersion::V2.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::TransformRealtimeFrame.section_prefix(),
            WireVersion::V2.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::ShapeEnvelopeGemini.section_prefix(),
            WireVersion::V2.as_str()
        ),
    ];
    for i in 0..sections.len() {
        for j in i + 1..sections.len() {
            assert_ne!(
                sections[i], sections[j],
                "schema sections {i} and {j} collide"
            );
        }
    }
}
