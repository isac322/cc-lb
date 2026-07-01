//! Shared types between host (`cc-lb-runtime-wasmtime`) and guest
//! (`cc-lb-pdk-wasmtime`) compiled in lockstep.
//!
//! Wire types for the three hooks the wasmtime runtime ships:
//! filter (Phase 1), shape + normalize_error (Phase 2), observe
//! (Phase 2). Signer extension is intentionally not exposed across
//! the plugin boundary — host-side built-in `AnthropicKeySigner` /
//! `AnthropicOAuthSigner` handle credential signing in-process.
//!
//! rkyv derives `Archive` + `Serialize` + `Deserialize` for every wire type.
//! The host calls `rkyv::access::<ArchivedFilterRequest, rkyv::rancor::Error>`
//! to get a zero-copy `&ArchivedFilterRequest` view straight out of guest
//! linear memory; the guest mirrors that pattern in reverse for the response.
//!
//! ## ABI invariants (review consensus)
//!
//! * Every guest-allocated buffer is aligned to `align_of::<Archived<T>>()`
//!   for its root type. rkyv 0.8's default relative pointers require this;
//!   the alignment is propagated through `cc_lb_alloc(size, align)`.
//! * `Arc`/`Rc` MUST NOT appear in any wire type (rkyv 0.8 Issue #670 —
//!   `ArchivedRc::verify` can be bypassed for DST shared pointers).
//! * The wire schema is fingerprinted via BLAKE3 of `cc_lb_schema_hash`
//!   custom section content. Host and guest must agree byte-for-byte.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use rkyv::{Archive, Deserialize, Serialize};

/// Principal context as seen by the filter plugin.
///
/// Mirrors `cc_lb_plugin_api::Principal` semantically but the rkyv-derived
/// wire form is the source of truth on the host↔guest boundary.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Principal {
    pub id: String,
    pub kind: String,
    /// Free-form key/value claims. Bytes intentionally, not JSON.
    pub claims: Vec<(String, Vec<u8>)>,
}

/// One upstream candidate the filter plugin can choose to keep or drop.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct UpstreamCandidate {
    pub upstream_id: String,
    pub name: String,
    pub kind: String,
    pub observed_at_unix_secs: u64,
    pub predicted_cache_read_tokens: u32,
}

/// One header on the inbound request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Header {
    pub name: String,
    /// Raw header bytes — never base64'd. The whole point of the rkyv wire is
    /// to remove that encode/decode hop.
    pub value: Vec<u8>,
}

/// Filter hook input.
///
/// Sent host → guest. The host serialises via `rkyv::to_bytes`, allocates
/// `cc_lb_alloc(size, align_of::<Archived<FilterRequest>>())` in guest
/// memory, writes the bytes, and calls `cc_lb_filter(in_ptr, in_len)`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterRequest {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
    pub principal: Principal,
    pub candidates: Vec<UpstreamCandidate>,
}

/// Decision the plugin made for one candidate.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct PerCandidateReason {
    pub upstream_id: String,
    /// `"accept"` or `"reject"` — kept as string so plugins remain forward-
    /// compatible with future decision variants without a host re-bump.
    pub decision: String,
    pub reason: String,
}

/// Filter hook output.
///
/// Guest packs `(out_ptr, out_len)` into a single `u64` (`(ptr << 32) | len`)
/// for the return value of `cc_lb_filter`. The host reads the bytes via
/// `Memory::data(&store)` and runs `rkyv::access::<ArchivedFilterResponse, _>`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterResponse {
    pub results: Vec<PerCandidateReason>,
}

/// Upstream backend exposed to plugins. Mirrors
/// `cc_lb_plugin_api::Upstream` (currently a single variant —
/// extending the host enum requires extending this one in lockstep
/// and bumping a wire schema tag).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum Upstream {
    AnthropicDirect {
        /// Operator-configured base URL override, if any.
        base_url: Option<String>,
    },
}

/// Shape hook input.
///
/// Sent host → guest via `cc_lb_shape(in_ptr, in_len) -> u64` packing
/// `(out_ptr, out_len)` for a [`ShapeResponse`].
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeRequest {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
    pub principal: Principal,
    pub upstream: Upstream,
}

/// Shape hook output — the upstream-bound request the plugin produced.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeResponse {
    pub url: String,
    pub method: String,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

/// `normalize_error` hook input.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct NormalizeErrorRequest {
    pub status: u16,
    pub body: Vec<u8>,
}

/// `normalize_error` hook output. `None` means "pass through original
/// upstream body" so plugins do not have to opt out per-status.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct NormalizeErrorResponse {
    pub normalized: Option<Vec<u8>>,
}

/// Lifecycle event delivered to the observe hook. rkyv mirror of
/// `cc_lb_plugin_api::ObserveEvent`. The only mechanical changes vs
/// the host enum are `http::StatusCode` → `u16` and `url::Url` →
/// `String` (rkyv-incompatible types flattened).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum ObserveEvent {
    RequestStarted {
        request_id: String,
        downstream_user_agent: Option<String>,
    },
    AuthnComplete {
        principal_id: String,
        principal_kind: String,
    },
    UpstreamChosen {
        upstream: Upstream,
    },
    Chunk {
        batch_index: u64,
        event_count: u64,
        total_bytes: u64,
    },
    RequestFinished {
        status: u16,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cache_creation_input_tokens: Option<u64>,
        cache_read_input_tokens: Option<u64>,
        duration_ms: u64,
    },
    Error {
        code: String,
        message: String,
        source: String,
    },
}

/// Schema fingerprint constants shared by `cc-lb-pdk-wasmtime-macros`
/// (proc-macro time) and `cc-lb-runtime-wasmtime::inspect`
/// (load-time). Single source of truth — bumping any tag requires
/// rebuilding every plugin against the new value.
pub mod schema {
    pub const WIRE_SCHEMA_TAG_FILTER: &[u8] = b"cc_lb.wire.v1.filter.rkyv";
    pub const WIRE_SCHEMA_TAG_SHAPE: &[u8] = b"cc_lb.wire.v1.shape.rkyv";
    pub const WIRE_SCHEMA_TAG_NORMALIZE_ERROR: &[u8] = b"cc_lb.wire.v1.normalize_error.rkyv";
    pub const WIRE_SCHEMA_TAG_OBSERVE: &[u8] = b"cc_lb.wire.v1.observe.rkyv";

    pub const SECTION_FILTER: &str = "cc_lb.schema.filter.v1";
    pub const SECTION_SHAPE: &str = "cc_lb.schema.shape.v1";
    pub const SECTION_NORMALIZE_ERROR: &str = "cc_lb.schema.normalize_error.v1";
    pub const SECTION_OBSERVE: &str = "cc_lb.schema.observe.v1";
}

/// Packed `(out_ptr, out_len)` return value used by every guest hook export.
pub const fn pack_ret(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}

/// Inverse of [`pack_ret`].
pub const fn unpack_ret(packed: u64) -> (u32, u32) {
    let ptr = (packed >> 32) as u32;
    let len = (packed & 0xFFFF_FFFF) as u32;
    (ptr, len)
}
