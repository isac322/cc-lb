//! Wire schema version 1 (baseline).
//!
//! Shared types between host (`cc-lb-runtime-wasmtime`) and guest
//! (`cc-lb-pdk-wasmtime`) compiled in lockstep.
//!
//! Wire types for the three hooks the wasmtime runtime ships:
//! filter (Phase 1), shape (Phase 2), observe
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
//! ## Borrowed-wire optimisation (RFC-0001 #9)
//!
//! Host-to-guest request types (FilterRequest, ShapeRequest) come in
//! two flavours that produce IDENTICAL archived bytes:
//! * **Owned** (`FilterRequest`, `ShapeRequest`, ...) — owned `Box<str>`
//!   / `Box<[u8]>` fields. Used by the guest for `rkyv::deserialize`
//!   round-trips and for round-trip test fixtures.
//! * **Borrowed ref** (`FilterRequestRef<'a>`, `ShapeRequestRef<'a>`,
//!   ...) — `&'a str` / `&'a [u8]` fields with `#[rkyv(with =
//!   InlineAsBox)]`. Used by the host in `wire_to_host_wire_request`
//!   so the request body (up to 100 MiB on `/v1/files`) is serialised
//!   IN PLACE from the request pipeline's `bytes::Bytes` without any
//!   `.to_vec()` copy.
//!
//! Both variants archive to the same `ArchivedBox<ArchivedSlice<u8>>`
//! / `ArchivedBox<ArchivedStr>` byte layouts — verified by the wire
//! round-trip tests in `crates/cc-lb-plugin-wire/tests/borrowed_wire_roundtrip.rs`.
//! When either variant is written to guest memory, the guest reads it
//! via `rkyv::access::<ArchivedFilterRequest, _>` — the archived
//! type name is identical because the owned type is the sole `Archive`
//! source of truth; the ref type carries `#[rkyv(archived = ...)]` to
//! reuse the owned type's archived form.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.

use alloc::boxed::Box;

use rkyv::{Archive, Deserialize, Serialize, with::InlineAsBox};

/// Principal context as seen by the filter plugin.
///
/// Mirrors `cc_lb_domain::Principal` semantically but the rkyv-derived
/// wire form is the source of truth on the host↔guest boundary.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Principal {
    pub id: Box<str>,
    pub kind: Box<str>,
    /// Free-form key/value claims. Bytes intentionally, not JSON.
    /// Modelled as a named `Claim` struct (not a tuple) so the
    /// borrowed [`ClaimRef`] and this owned form archive to the same
    /// byte layout for host/guest wire compatibility.
    pub claims: Box<[Claim]>,
}

/// One claim key/value pair — see [`Principal::claims`]. Named
/// struct (not tuple) so that `ClaimRef<'a>` matches the archived
/// byte layout without an extra Ref wrapper.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Claim {
    pub key: Box<str>,
    pub value: Box<[u8]>,
}

/// Borrowed mirror of [`Principal`] used by the host encode path. Every
/// reference field carries `#[rkyv(with = InlineAsBox)]` so serialising
/// this struct emits the same archived byte layout as [`Principal`].
#[derive(Archive, Serialize)]
pub struct PrincipalRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub kind: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub claims: &'a [ClaimRef<'a>],
}

/// One claim entry inside [`PrincipalRef`]. The tuple form used by the
/// owned type does not have a straight borrowed equivalent, so we
/// promote it to a named struct with two borrowed fields.
#[derive(Archive, Serialize)]
pub struct ClaimRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub key: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub value: &'a [u8],
}

/// One upstream candidate the filter plugin can choose to keep or drop.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct UpstreamCandidate {
    pub upstream_id: Box<str>,
    pub name: Box<str>,
    pub kind: Box<str>,
    pub observed_at_unix_secs: u64,
    pub predicted_cache_read_tokens: u32,
    pub predicted_cache_creation_tokens_5m: u32,
    pub predicted_cache_creation_tokens_1h: u32,
    pub predicted_uncached_input_tokens: u32,
    pub plan_capacity_ratio: f64,
    pub organization_type: Box<str>,
    pub rate_limit_tier: Box<str>,
    pub seat_tier: Box<str>,
}

/// Borrowed mirror of [`UpstreamCandidate`].
#[derive(Archive, Serialize)]
pub struct UpstreamCandidateRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub upstream_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub name: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub kind: &'a str,
    pub observed_at_unix_secs: u64,
    pub predicted_cache_read_tokens: u32,
    pub predicted_cache_creation_tokens_5m: u32,
    pub predicted_cache_creation_tokens_1h: u32,
    pub predicted_uncached_input_tokens: u32,
    pub plan_capacity_ratio: f64,
    #[rkyv(with = InlineAsBox)]
    pub organization_type: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub rate_limit_tier: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub seat_tier: &'a str,
}

/// Model pricing summary supplied to filter plugins.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct CachePricingSummary {
    pub status: Box<str>,
    pub input_micros_per_million: Option<u64>,
    pub cache_creation_5m_micros_per_million: Option<u64>,
    pub cache_creation_1h_micros_per_million: Option<u64>,
    pub cache_read_micros_per_million: Option<u64>,
}

/// Borrowed mirror of [`CachePricingSummary`].
#[derive(Archive, Serialize)]
pub struct CachePricingSummaryRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub status: &'a str,
    pub input_micros_per_million: Option<u64>,
    pub cache_creation_5m_micros_per_million: Option<u64>,
    pub cache_creation_1h_micros_per_million: Option<u64>,
    pub cache_read_micros_per_million: Option<u64>,
}

/// One header on the inbound request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Header {
    pub name: Box<str>,
    /// Raw header bytes — never base64'd. The whole point of the rkyv wire is
    /// to remove that encode/decode hop.
    pub value: Box<[u8]>,
}

/// Borrowed mirror of [`Header`].
#[derive(Archive, Serialize)]
pub struct HeaderRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub name: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub value: &'a [u8],
}

/// Filter hook input (owned form used by the guest).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterRequest {
    pub request_id: Box<str>,
    pub thread_id: Option<Box<str>>,
    pub canonical_model_id: Box<str>,
    pub cache_pricing: CachePricingSummary,
    pub method: Box<str>,
    pub path: Box<str>,
    pub query: Option<Box<str>>,
    pub headers: Box<[Header]>,
    pub body: Box<[u8]>,
    pub principal: Principal,
    pub candidates: Box<[UpstreamCandidate]>,
}

/// Borrowed mirror of [`FilterRequest`] used by the host encode path.
/// Serialising this emits the same archived byte layout as
/// [`FilterRequest`] so the guest reads it via
/// `rkyv::access::<ArchivedFilterRequest, _>` unchanged.
#[derive(Archive, Serialize)]
pub struct FilterRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub thread_id: Option<QueryRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    pub cache_pricing: CachePricingSummaryRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub path: &'a str,
    pub query: Option<QueryRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
    pub principal: PrincipalRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub candidates: &'a [UpstreamCandidateRef<'a>],
}

/// Borrowed wrapper for the optional `query` field. Necessary because
/// `Option<InlineAsBox<&'a str>>` does not compose directly at the
/// derive layer; a named struct pushes the `#[rkyv(with = ...)]`
/// attribute onto the inner reference.
#[derive(Archive, Serialize)]
pub struct QueryRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub value: &'a str,
}

/// Decision the plugin made for one candidate.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct PerCandidateReason {
    pub upstream_id: Box<str>,
    /// `"accept"` or `"reject"` — kept as string so plugins remain forward-
    /// compatible with future decision variants without a host re-bump.
    pub decision: Box<str>,
    pub reason: Box<str>,
}

/// Filter hook output.
///
/// Guest packs `(out_ptr, out_len)` into a single `u64` (`(ptr << 32) | len`)
/// for the return value of `cc_lb_filter`. The host reads the bytes via
/// `Memory::data(&store)` and runs `rkyv::access::<ArchivedFilterResponse, _>`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterResponse {
    pub results: Box<[PerCandidateReason]>,
}

/// Upstream backend exposed to plugins. Mirrors
/// `cc_lb_domain::Upstream` (currently a single variant —
/// extending the host enum requires extending this one in lockstep
/// and bumping a wire schema tag).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum Upstream {
    AnthropicDirect {
        /// Operator-configured base URL override, if any.
        base_url: Option<Box<str>>,
    },
}

/// Borrowed mirror of [`Upstream`].
#[derive(Archive, Serialize)]
pub enum UpstreamRef<'a> {
    AnthropicDirect { base_url: Option<QueryRef<'a>> },
}

/// Shape hook input (owned form).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeRequest {
    pub request_id: Box<str>,
    pub method: Box<str>,
    pub path: Box<str>,
    pub query: Option<Box<str>>,
    pub headers: Box<[Header]>,
    pub body: Box<[u8]>,
    pub principal: Principal,
    pub upstream: Upstream,
}

/// Borrowed mirror of [`ShapeRequest`] used by the host encode path.
#[derive(Archive, Serialize)]
pub struct ShapeRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub path: &'a str,
    pub query: Option<QueryRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
    pub principal: PrincipalRef<'a>,
    pub upstream: UpstreamRef<'a>,
}

/// Shape hook output — the upstream-bound request the plugin produced.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeResponse {
    pub url: Box<str>,
    pub method: Box<str>,
    pub headers: Box<[Header]>,
    pub body: Box<[u8]>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct TransformResponseRequest {
    pub request_id: Box<str>,
    pub principal: Principal,
    pub upstream: Upstream,
    pub request_method: Box<str>,
    pub request_path: Box<str>,
    pub canonical_model_id: Box<str>,
    pub response_status: u16,
    pub response_headers: Box<[Header]>,
    pub body: Box<[u8]>,
}

#[derive(Archive, Serialize)]
pub struct TransformResponseRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub principal: PrincipalRef<'a>,
    pub upstream: UpstreamRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub request_method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub request_path: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    pub response_status: u16,
    #[rkyv(with = InlineAsBox)]
    pub response_headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum TransformResponseResult {
    Unchanged,
    Replace {
        status: Option<u16>,
        headers: Option<Box<[Header]>>,
        body: Option<Box<[u8]>>,
    },
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct SseEvent {
    pub event: Box<str>,
    pub data: Box<[u8]>,
}

#[derive(Archive, Serialize)]
pub struct SseEventRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub event: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub data: &'a [u8],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct TransformSseEventRequest {
    pub request_id: Box<str>,
    pub principal: Principal,
    pub upstream: Upstream,
    pub request_method: Box<str>,
    pub request_path: Box<str>,
    pub canonical_model_id: Box<str>,
    pub response_status: u16,
    pub response_headers: Box<[Header]>,
    pub event: SseEvent,
}

#[derive(Archive, Serialize)]
pub struct TransformSseEventRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub principal: PrincipalRef<'a>,
    pub upstream: UpstreamRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub request_method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub request_path: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    pub response_status: u16,
    #[rkyv(with = InlineAsBox)]
    pub response_headers: &'a [HeaderRef<'a>],
    pub event: SseEventRef<'a>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum TransformSseEventResult {
    Unchanged,
    Replace { events: Box<[SseEvent]> },
    Drop,
}

/// Lifecycle event delivered to the observe hook. rkyv mirror of
/// `cc_lb_observability::ObserveEvent`.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum ObserveEvent {
    RequestStarted {
        request_id: Box<str>,
        downstream_user_agent: Option<Box<str>>,
    },
    AuthnComplete {
        principal_id: Box<str>,
        principal_kind: Box<str>,
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
        code: Box<str>,
        message: Box<str>,
        source: Box<str>,
    },
}

include!(concat!(env!("OUT_DIR"), "/wire_schema_impls.rs"));
