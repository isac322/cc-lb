//! Minimal passthrough Shape plugin for `cc-lb-runtime-wasmtime`
//! integration tests.
//!
//! `shape` returns a [`ShapeResponse`] that echoes the inbound path,
//! method, headers, and body against the upstream's base URL
//! (defaulting to `https://api.anthropic.com` when no override is
//! present on `AnthropicDirect`).
//!
//! Used by `crates/cc-lb-runtime-wasmtime/tests/shape_round_trip.rs`.
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use cc_lb_pdk_wasmtime::types::{
    ArchivedShapeRequest, ArchivedTransformResponseRequest, ArchivedTransformSseEventRequest,
    ArchivedUpstream, Header as WireHeader, ShapeResponse, TransformResponseResult,
    TransformSseEventResult,
};

#[cc_lb_pdk_wasmtime::plugin(
    name = "wasmtime-shape-passthrough",
    version = "0.1.0",
    description = "Test fixture: passes request through unchanged",
    usage = "Testing only, no config. Echoes the request method, headers, and body against the host-provided upstream base URL."
)]
mod passthrough {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(
        shape,
        wire = 1,
        description = "Passes the incoming request through to the selected upstream",
        usage = "Testing only, no config. Requires the host to populate Upstream::AnthropicDirect.base_url before dispatch.",
        view
    )]
    pub fn shape(req: &ArchivedShapeRequest) -> ShapeResponse {
        // The host is responsible for filling `base_url` before
        // calling `shape` — a plugin must never invent a production
        // URL. A `None` here means the host adapter shipped a
        // half-built request, which we surface as a trap.
        let base = upstream_base_url(&req.upstream)
            .expect("host must populate Upstream::AnthropicDirect.base_url before calling shape");
        let path_str: &str = &req.path;
        let query_str: Option<&str> = req.query.as_ref().map(|q| &**q);
        let url = build_url(&base, path_str, query_str);
        let headers: Vec<WireHeader> = req
            .headers
            .iter()
            .map(|h| {
                let name_ref: &str = &h.name;
                let value_ref: &[u8] = &h.value;
                WireHeader {
                    name: Box::from(name_ref),
                    value: Box::from(value_ref),
                }
            })
            .collect();
        let method_ref: &str = &req.method;
        let body_ref: &[u8] = &req.body;
        ShapeResponse {
            url: Box::from(url.as_str()),
            method: Box::from(method_ref),
            headers: headers.into_boxed_slice(),
            body: Box::from(body_ref),
        }
    }

    #[cc_lb_pdk_wasmtime::handler(
        transform_response,
        wire = 1,
        description = "Explicit no-op buffered response transform for shape-owned fixture",
        usage = "Testing only. Declares that the shape plugin intentionally leaves buffered responses unchanged.",
        mode = "noop",
        view
    )]
    pub fn transform_response(_req: &ArchivedTransformResponseRequest) -> TransformResponseResult {
        TransformResponseResult::Unchanged
    }

    #[cc_lb_pdk_wasmtime::handler(
        transform_sse_event,
        wire = 1,
        description = "Explicit no-op SSE event transform for shape-owned fixture",
        usage = "Testing only. Declares that the shape plugin intentionally leaves SSE events unchanged.",
        mode = "noop",
        view
    )]
    pub fn transform_sse_event(_req: &ArchivedTransformSseEventRequest) -> TransformSseEventResult {
        TransformSseEventResult::Unchanged
    }
}

pub use passthrough::{shape, transform_response, transform_sse_event};

fn upstream_base_url(upstream: &ArchivedUpstream) -> Option<String> {
    match upstream {
        ArchivedUpstream::AnthropicDirect { base_url } => base_url.as_ref().map(|b| {
            let s: &str = b;
            s.to_string()
        }),
    }
}

fn build_url(base: &str, path: &str, query: Option<&str>) -> String {
    let trimmed = base.trim_end_matches('/');
    match query {
        Some(q) if !q.is_empty() => format!("{trimmed}{path}?{q}"),
        _ => format!("{trimmed}{path}"),
    }
}
