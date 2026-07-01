//! Minimal passthrough Shape plugin for `cc-lb-runtime-wasmtime`
//! integration tests.
//!
//! `shape` returns a [`ShapeResponse`] that echoes the inbound path,
//! method, headers, and body against the upstream's base URL
//! (defaulting to `https://api.anthropic.com` when no override is
//! present on `AnthropicDirect`).
//!
//! `normalize_error` always returns `None` — passthrough.
//!
//! Used by `crates/cc-lb-runtime-wasmtime/tests/shape_round_trip.rs`.
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use cc_lb_pdk_wasmtime::types::{
    ArchivedShapeRequest, ArchivedUpstream, Header as WireHeader, NormalizeErrorRequest,
    NormalizeErrorResponse, ShapeResponse,
};

#[cc_lb_pdk_wasmtime::plugin(name = "wasmtime-shape-passthrough", version = "0.1.0")]
mod passthrough {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(name = "shape", view)]
    pub fn shape(req: &ArchivedShapeRequest) -> ShapeResponse {
        // The host is responsible for filling `base_url` before
        // calling `shape` — a plugin must never invent a production
        // URL. A `None` here means the host adapter shipped a
        // half-built request, which we surface as a trap.
        let base = upstream_base_url(&req.upstream)
            .expect("host must populate Upstream::AnthropicDirect.base_url before calling shape");
        let url = build_url(
            &base,
            req.path.as_str(),
            req.query.as_ref().map(|q| q.as_str()),
        );
        let headers: Vec<WireHeader> = req
            .headers
            .iter()
            .map(|h| WireHeader {
                name: h.name.as_str().to_string(),
                value: h.value.as_slice().to_vec(),
            })
            .collect();
        ShapeResponse {
            url,
            method: req.method.as_str().to_string(),
            headers,
            body: req.body.as_slice().to_vec(),
        }
    }

    #[cc_lb_pdk_wasmtime::handler(name = "normalize_error")]
    pub fn normalize_error(_req: NormalizeErrorRequest) -> NormalizeErrorResponse {
        NormalizeErrorResponse { normalized: None }
    }
}

pub use passthrough::{normalize_error, shape};

fn upstream_base_url(upstream: &ArchivedUpstream) -> Option<String> {
    match upstream {
        ArchivedUpstream::AnthropicDirect { base_url } => {
            base_url.as_ref().map(|b| b.as_str().to_string())
        }
    }
}

fn build_url(base: &str, path: &str, query: Option<&str>) -> String {
    let trimmed = base.trim_end_matches('/');
    match query {
        Some(q) if !q.is_empty() => format!("{trimmed}{path}?{q}"),
        _ => format!("{trimmed}{path}"),
    }
}
