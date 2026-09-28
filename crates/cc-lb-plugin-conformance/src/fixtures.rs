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
    TransformSseEventRequest, Upstream,
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
