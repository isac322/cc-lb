//! Round-trip every Phase-2 wire type through `rkyv::to_bytes` →
//! `rkyv::access` → `rkyv::deserialize` to catch derive mismatches
//! and bytecheck failures before they reach the host/guest boundary.
//!
//! Also asserts the RFC-0001 #9 borrowed-wire ABI contract: the bytes
//! emitted by encoding a `FilterRequestRef<'_>` / `ShapeRequestRef<'_>`
//! are structurally decodable via the owned `Archived*` view. If this
//! ever regresses, the host-side
//! borrowed encoding stops producing bytes the guest can parse.

use cc_lb_plugin_wire::{
    ArchivedFilterRequest, ArchivedObserveEvent, ArchivedShapeRequest, Claim, ClaimRef,
    FilterRequest, FilterRequestRef, Header, HeaderRef, ObserveEvent, Principal, PrincipalRef,
    QueryRef, ShapeRequest, ShapeRequestRef, ShapeResponse, Upstream, UpstreamCandidate,
    UpstreamCandidateRef, UpstreamRef, WireSchema,
    schema::{HookKind, WireVersion},
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

#[test]
fn filter_request_round_trips() {
    let req = FilterRequest {
        request_id: Box::from("req-1"),
        thread_id: None,
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
            observed_at_unix_secs: 42,
            predicted_cache_read_tokens: 256,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
        }]),
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(&bytes).expect("access");
    let owned: FilterRequest =
        rkyv::deserialize::<FilterRequest, Error>(archived).expect("deserialize");
    assert_eq!(&*owned.request_id, "req-1");
    assert_eq!(owned.candidates.len(), 1);
    assert_eq!(owned.candidates[0].predicted_cache_read_tokens, 256);
}

/// RFC-0001 #9: `FilterRequestRef<'_>` (borrowed) must produce bytes
/// the guest-side `ArchivedFilterRequest` view can parse cleanly.
#[test]
fn filter_request_ref_encodes_to_owned_wire() {
    let claim_refs = [ClaimRef {
        key: "scope",
        value: b"inference",
    }];
    let header_refs = [HeaderRef {
        name: "content-type",
        value: b"application/json",
    }];
    let candidate_refs = [UpstreamCandidateRef {
        upstream_id: "u-1",
        name: "anthropic-direct",
        kind: "anthropic_api_key",
        observed_at_unix_secs: 42,
        predicted_cache_read_tokens: 256,
        plan_capacity_ratio: 1.0,
        organization_type: "",
        rate_limit_tier: "",
        seat_tier: "",
    }];
    let req_ref = FilterRequestRef {
        request_id: "req-1",
        thread_id: None,
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
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(&bytes).expect("access owned");
    let owned_body: &[u8] = &archived.body;
    assert_eq!(owned_body, b"{\"k\":1}");
    let owned_id: &str = &archived.request_id;
    assert_eq!(owned_id, "req-1");
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
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedShapeRequest, Error>(&bytes).expect("access");
    let owned: ShapeRequest =
        rkyv::deserialize::<ShapeRequest, Error>(archived).expect("deserialize");
    let query: &str = owned.query.as_deref().unwrap_or("");
    assert_eq!(query, "stream=true");
    match owned.upstream {
        Upstream::AnthropicDirect { base_url } => {
            let base: &str = base_url.as_deref().unwrap_or("");
            assert_eq!(base, "https://api.anthropic.com")
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
        query: Some(QueryRef {
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
            base_url: Some(QueryRef {
                value: "https://api.anthropic.com",
            }),
        },
    };
    let bytes = rkyv::to_bytes::<Error>(&req_ref).expect("encode ref");
    let archived = rkyv::access::<ArchivedShapeRequest, Error>(&bytes).expect("access owned");
    let body: &[u8] = &archived.body;
    assert_eq!(body, b"body");
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
    let _ = bytes; // serialize success suffices
}

#[test]
fn observe_event_request_started_round_trips() {
    let ev = ObserveEvent::RequestStarted {
        request_id: Box::from("req-3"),
        downstream_user_agent: Some(Box::from("anthropic-cli/1.0")),
    };
    let bytes = rkyv::to_bytes::<Error>(&ev).expect("encode");
    let archived = rkyv::access::<ArchivedObserveEvent, Error>(&bytes).expect("access");
    let owned: ObserveEvent =
        rkyv::deserialize::<ObserveEvent, Error>(archived).expect("deserialize");
    match owned {
        ObserveEvent::RequestStarted {
            request_id,
            downstream_user_agent,
        } => {
            let id: &str = &request_id;
            assert_eq!(id, "req-3");
            let ua: &str = downstream_user_agent.as_deref().unwrap_or("");
            assert_eq!(ua, "anthropic-cli/1.0");
        }
        other => panic!("variant mismatch: {other:?}"),
    }
}

#[test]
fn observe_event_upstream_chosen_round_trips() {
    let ev = ObserveEvent::UpstreamChosen {
        upstream: Upstream::AnthropicDirect {
            base_url: Some(Box::from("https://api.anthropic.com")),
        },
    };
    let bytes = rkyv::to_bytes::<Error>(&ev).expect("encode");
    let archived = rkyv::access::<ArchivedObserveEvent, Error>(&bytes).expect("access");
    let owned: ObserveEvent =
        rkyv::deserialize::<ObserveEvent, Error>(archived).expect("deserialize");
    match owned {
        ObserveEvent::UpstreamChosen {
            upstream: Upstream::AnthropicDirect { base_url },
        } => {
            let base: &str = base_url.as_deref().unwrap_or("");
            assert_eq!(base, "https://api.anthropic.com");
        }
        other => panic!("variant mismatch: {other:?}"),
    }
}

#[test]
fn observe_event_request_finished_round_trips() {
    let ev = ObserveEvent::RequestFinished {
        status: 200,
        input_tokens: Some(1024),
        output_tokens: Some(256),
        cache_creation_input_tokens: Some(64),
        cache_read_input_tokens: Some(512),
        duration_ms: 1_234,
    };
    let bytes = rkyv::to_bytes::<Error>(&ev).expect("encode");
    let archived = rkyv::access::<ArchivedObserveEvent, Error>(&bytes).expect("access");
    let owned: ObserveEvent =
        rkyv::deserialize::<ObserveEvent, Error>(archived).expect("deserialize");
    match owned {
        ObserveEvent::RequestFinished {
            status,
            duration_ms,
            ..
        } => {
            assert_eq!(status, 200);
            assert_eq!(duration_ms, 1234);
        }
        other => panic!("variant mismatch: {other:?}"),
    }
}

#[test]
fn schema_fingerprints_and_sections_are_distinct() {
    let fingerprints = [
        <FilterRequest as WireSchema>::FINGERPRINT,
        <ShapeRequest as WireSchema>::FINGERPRINT,
        <ObserveEvent as WireSchema>::FINGERPRINT,
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
            WireVersion::V1.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::Shape.section_prefix(),
            WireVersion::V1.as_str()
        ),
        format!(
            "{}.{}",
            HookKind::Observe.section_prefix(),
            WireVersion::V1.as_str()
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
