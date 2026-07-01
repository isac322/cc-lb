//! Thin helpers for the boilerplate every conformance test hits.
//!
//! Deliberately narrow: builder pattern is NOT provided because rkyv
//! wire types benefit from explicit struct-literal construction (the
//! author can see every field they're setting, and unset fields fall
//! through to `Default` explicitly in the plugin's own test). What the
//! author actually re-types every time is header construction, a
//! synthetic principal, and the six-variant observe sample.

use cc_lb_plugin_types::{
    FilterRequest, Header, NormalizeErrorRequest, ObserveEvent, Principal, ShapeRequest, Upstream,
};

/// Build a `Header` from `(name, value)`. Value can be `&str`, `&[u8]`,
/// `String`, or anything that dereferences to bytes.
pub fn hdr(name: impl Into<String>, value: impl AsRef<[u8]>) -> Header {
    Header {
        name: name.into(),
        value: value.as_ref().to_vec(),
    }
}

/// Synthetic API-key principal — safe defaults, no real credentials.
/// Use as the `principal` field of a `ShapeRequest` / `FilterRequest`
/// literal when the plugin under test does not care about principal
/// details.
pub fn synth_principal() -> Principal {
    Principal {
        id: "conformance-principal".to_string(),
        kind: "api_key".to_string(),
        claims: Vec::new(),
    }
}

/// One synthetic `ObserveEvent` per enum variant. Used by
/// [`crate::PluginSession::exercise_observe_variants`] to prove no
/// variant traps in the guest.
pub fn observe_event_samples() -> Vec<ObserveEvent> {
    vec![
        ObserveEvent::RequestStarted {
            request_id: "conformance-req-1".to_string(),
            downstream_user_agent: Some("conformance/1.0".to_string()),
        },
        ObserveEvent::AuthnComplete {
            principal_id: "conformance-principal".to_string(),
            principal_kind: "api_key".to_string(),
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
            code: "conformance_error".to_string(),
            message: "synthetic".to_string(),
            source: "conformance".to_string(),
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
        request_id: "conformance-req-1".to_string(),
        method: "POST".to_string(),
        path: "/v1/messages".to_string(),
        query: None,
        headers: vec![hdr("content-type", "application/json")],
        body: br#"{"model":"claude-3-haiku-20240307","messages":[]}"#.to_vec(),
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
        request_id: "conformance-req-1".to_string(),
        method: "POST".to_string(),
        path: "/v1/messages".to_string(),
        query: None,
        headers: vec![hdr("content-type", "application/json")],
        body: br#"{"model":"claude-3-haiku-20240307","messages":[]}"#.to_vec(),
        principal: synth_principal(),
        candidates: Vec::new(),
    }
}

/// Protocol-valid minimal `NormalizeErrorRequest` — HTTP 500 with a
/// short synthetic error body. Plugins that want richer error shapes
/// can construct their own; this one exists so the harness can push a
/// non-empty payload through `cc_lb_normalize_error` for the ABI smoke.
pub fn sample_normalize_error_request() -> NormalizeErrorRequest {
    NormalizeErrorRequest {
        status: 500,
        body: br#"{"error":{"type":"internal_error","message":"synthetic"}}"#.to_vec(),
    }
}
