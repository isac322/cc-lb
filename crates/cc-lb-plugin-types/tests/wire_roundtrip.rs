//! Round-trip every Phase-2 wire type through `rkyv::to_bytes` →
//! `rkyv::access` → `rkyv::deserialize` to catch derive mismatches
//! and bytecheck failures before they reach the host/guest boundary.

use cc_lb_plugin_types::{
    ArchivedFilterRequest, ArchivedNormalizeErrorRequest, ArchivedObserveEvent,
    ArchivedShapeRequest, FilterRequest, Header, NormalizeErrorRequest, NormalizeErrorResponse,
    ObserveEvent, Principal, ShapeRequest, ShapeResponse, Upstream, UpstreamCandidate, schema,
};
use rkyv::rancor::Error;

fn principal() -> Principal {
    Principal {
        id: "tenant-a".to_owned(),
        kind: "api_key".to_owned(),
        claims: vec![("scope".to_owned(), b"inference".to_vec())],
    }
}

fn header(name: &str, value: &str) -> Header {
    Header {
        name: name.to_owned(),
        value: value.as_bytes().to_vec(),
    }
}

#[test]
fn filter_request_round_trips() {
    let req = FilterRequest {
        request_id: "req-1".to_owned(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: None,
        headers: vec![header("content-type", "application/json")],
        body: b"{\"k\":1}".to_vec(),
        principal: principal(),
        candidates: vec![UpstreamCandidate {
            upstream_id: "u-1".to_owned(),
            name: "anthropic-direct".to_owned(),
            kind: "anthropic_api_key".to_owned(),
            observed_at_unix_secs: 42,
            predicted_cache_read_tokens: 256,
        }],
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedFilterRequest, Error>(&bytes).expect("access");
    let owned: FilterRequest =
        rkyv::deserialize::<FilterRequest, Error>(archived).expect("deserialize");
    assert_eq!(owned.request_id, "req-1");
    assert_eq!(owned.candidates.len(), 1);
    assert_eq!(owned.candidates[0].predicted_cache_read_tokens, 256);
}

#[test]
fn shape_request_round_trips() {
    let req = ShapeRequest {
        request_id: "req-2".to_owned(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: Some("stream=true".to_owned()),
        headers: vec![header("accept", "text/event-stream")],
        body: b"body".to_vec(),
        principal: principal(),
        upstream: Upstream::AnthropicDirect {
            base_url: Some("https://api.anthropic.com".to_owned()),
        },
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let archived = rkyv::access::<ArchivedShapeRequest, Error>(&bytes).expect("access");
    let owned: ShapeRequest =
        rkyv::deserialize::<ShapeRequest, Error>(archived).expect("deserialize");
    assert_eq!(owned.query.as_deref(), Some("stream=true"));
    match owned.upstream {
        Upstream::AnthropicDirect { base_url } => {
            assert_eq!(base_url.as_deref(), Some("https://api.anthropic.com"))
        }
    }
}

#[test]
fn shape_response_round_trips() {
    let resp = ShapeResponse {
        url: "https://api.anthropic.com/v1/messages".to_owned(),
        method: "POST".to_owned(),
        headers: vec![header("x-api-key", "redacted")],
        body: b"shaped".to_vec(),
    };
    let bytes = rkyv::to_bytes::<Error>(&resp).expect("encode");
    let _ = bytes; // serialize success suffices
}

#[test]
fn normalize_error_round_trips() {
    let req = NormalizeErrorRequest {
        status: 429,
        body: b"rate_limited".to_vec(),
    };
    let bytes = rkyv::to_bytes::<Error>(&req).expect("encode req");
    let archived =
        rkyv::access::<ArchivedNormalizeErrorRequest, Error>(&bytes).expect("access req");
    let owned: NormalizeErrorRequest =
        rkyv::deserialize::<NormalizeErrorRequest, Error>(archived).expect("deserialize req");
    assert_eq!(owned.status, 429);

    let resp = NormalizeErrorResponse {
        normalized: Some(b"{\"type\":\"rate_limit_error\"}".to_vec()),
    };
    let bytes = rkyv::to_bytes::<Error>(&resp).expect("encode resp");
    let _ = bytes;
}

#[test]
fn observe_event_request_started_round_trips() {
    let ev = ObserveEvent::RequestStarted {
        request_id: "req-3".to_owned(),
        downstream_user_agent: Some("anthropic-cli/1.0".to_owned()),
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
            assert_eq!(request_id, "req-3");
            assert_eq!(downstream_user_agent.as_deref(), Some("anthropic-cli/1.0"));
        }
        other => panic!("variant mismatch: {other:?}"),
    }
}

#[test]
fn observe_event_upstream_chosen_round_trips() {
    let ev = ObserveEvent::UpstreamChosen {
        upstream: Upstream::AnthropicDirect {
            base_url: Some("https://api.anthropic.com".to_owned()),
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
            assert_eq!(base_url.as_deref(), Some("https://api.anthropic.com"));
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
fn schema_constants_are_distinct() {
    let tags = [
        schema::WIRE_SCHEMA_TAG_FILTER,
        schema::WIRE_SCHEMA_TAG_SHAPE,
        schema::WIRE_SCHEMA_TAG_NORMALIZE_ERROR,
        schema::WIRE_SCHEMA_TAG_OBSERVE,
    ];
    for i in 0..tags.len() {
        for j in i + 1..tags.len() {
            assert_ne!(tags[i], tags[j], "schema tags {i} and {j} collide");
        }
    }
    let sections = [
        schema::SECTION_FILTER,
        schema::SECTION_SHAPE,
        schema::SECTION_NORMALIZE_ERROR,
        schema::SECTION_OBSERVE,
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
