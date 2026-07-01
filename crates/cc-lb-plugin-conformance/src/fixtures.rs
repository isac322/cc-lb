//! Thin helpers for the boilerplate every conformance test hits.
//!
//! Deliberately narrow: builder pattern is NOT provided because rkyv
//! wire types benefit from explicit struct-literal construction (the
//! author can see every field they're setting, and unset fields fall
//! through to `Default` explicitly in the plugin's own test). What the
//! author actually re-types every time is header construction, a
//! synthetic principal, and the six-variant observe sample.

use cc_lb_plugin_types::{Header, ObserveEvent, Principal, Upstream};

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
