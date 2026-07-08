//! Thin helpers for the boilerplate every conformance test hits.
//!
//! Deliberately narrow: builder pattern is NOT provided because rkyv
//! wire types benefit from explicit struct-literal construction (the
//! author can see every field they're setting, and unset fields fall
//! through to `Default` explicitly in the plugin's own test). What the
//! author actually re-types every time is header construction, a
//! synthetic principal, and the six-variant observe sample.

use cc_lb_plugin_wire::{
    CachePricingSummary, FilterRequest, Header, ObserveEvent, Principal, ShapeRequest, Upstream,
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

/// One synthetic `ObserveEvent` per enum variant. Used by
/// [`crate::PluginSession::exercise_observe_variants`] to prove no
/// variant traps in the guest.
pub fn observe_event_samples() -> Vec<ObserveEvent> {
    vec![
        ObserveEvent::RequestStarted {
            request_id: Box::from("conformance-req-1"),
            downstream_user_agent: Some(Box::from("conformance/1.0")),
        },
        ObserveEvent::AuthnComplete {
            principal_id: Box::from("conformance-principal"),
            principal_kind: Box::from("api_key"),
        },
        ObserveEvent::UpstreamChosen {
            upstream: Upstream::AnthropicDirect { base_url: None },
        },
        ObserveEvent::Chunk {
            batch_index: 0,
            event_count: 1,
            total_bytes: 64,
        },
        ObserveEvent::RequestFinished {
            status: 200,
            input_tokens: Some(10),
            output_tokens: Some(20),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            duration_ms: 42,
        },
        ObserveEvent::Error {
            code: Box::from("conformance_error"),
            message: Box::from("synthetic"),
            source: Box::from("conformance"),
        },
    ]
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
        canonical_model_id: Box::from("claude-3-haiku-20240307"),
        cache_pricing: CachePricingSummary {
            status: Box::from("unknown"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([hdr("content-type", "application/json")]),
        body: Box::from(&br#"{"model":"claude-3-haiku-20240307","messages":[]}"#[..]),
        principal: synth_principal(),
        candidates: Box::new([]),
    }
}
