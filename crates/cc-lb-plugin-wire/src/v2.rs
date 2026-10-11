//! Filter V2 adds subscription quota snapshots to each upstream candidate.
//!
//! V1 remains unchanged. Other hooks and the filter response reuse V1 types.
//! `routing_now_unix_secs` is the injected host clock sampled for this routing pass.
//! Observation timestamps describe measurements, not the current routing time.
//! Current known quota windows include unobserved entries; match by label, never
//! position, and accept future unknown labels. This projection omits policy
//! thresholds, source metadata, overage/fallback decisions, and extra usage.

pub use crate::v1::{ArchivedFilterResponse, FilterResponse, PerCandidateReason};
use crate::v1::{
    CachePricingSummary, CachePricingSummaryRef, Header, HeaderRef, Principal, PrincipalRef,
    QueryRef,
};
use alloc::boxed::Box;
use rkyv::with::InlineAsBox;
use rkyv::{Archive, Deserialize, Serialize};

/// One subscription quota window snapshot (5-hour / 7-day style) for an
/// upstream candidate, as observed by the host's quota cache.
///
/// Mirrors the subset of `cc_lb_domain::SubscriptionQuotaCandidateSnapshot`
/// a reset-aware routing plugin needs. `state` carries the domain's
/// snake_case serde name (`"fresh"`, `"stale"`, `"absent"`,
/// `"unobserved"`) as a string so plugins stay forward-compatible with
/// future states without a host re-bump.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct QuotaWindowSnapshot {
    /// Provider window label, for example `5h` or `7d`; unknown labels are valid.
    pub window: Box<str>,
    /// Host freshness classification: `fresh`, `stale`, `absent`, or `unobserved`.
    pub state: Box<str>,
    /// Provider-reported utilization fraction, if known.
    pub utilization: Option<f64>,
    /// Provider reset timestamp in Unix seconds, if known.
    pub resets_at_unix_secs: Option<u64>,
    /// Measurement timestamp in Unix milliseconds, not the routing clock.
    pub observed_at_unix_millis: Option<u64>,
    /// Provider-reported quota status, if known.
    pub status: Option<Box<str>>,
    /// Provider reason the window is disabled, if known.
    pub disabled_reason: Option<Box<str>>,
    /// Configured maximum observation age before the host marks it stale.
    pub max_staleness_secs: u64,
}

/// Borrowed mirror of [`QuotaWindowSnapshot`] used by the host encode
/// path. Every reference field carries `#[rkyv(with = InlineAsBox)]` (and
/// the optional strings use [`QueryRef`]) so serialising this struct
/// emits the same archived byte layout as [`QuotaWindowSnapshot`].
#[derive(Archive, Serialize)]
pub struct QuotaWindowSnapshotRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub window: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub state: &'a str,
    pub utilization: Option<f64>,
    pub resets_at_unix_secs: Option<u64>,
    pub observed_at_unix_millis: Option<u64>,
    pub status: Option<QueryRef<'a>>,
    pub disabled_reason: Option<QueryRef<'a>>,
    pub max_staleness_secs: u64,
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
    pub subscription_quotas: Box<[QuotaWindowSnapshot]>,
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
    #[rkyv(with = InlineAsBox)]
    pub subscription_quotas: &'a [QuotaWindowSnapshotRef<'a>],
}

/// Filter hook input (owned form used by the guest).
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterRequest {
    /// Current host routing clock sampled for this pass, in Unix seconds.
    pub routing_now_unix_secs: u64,
    pub request_id: Box<str>,
    pub thread_id: Option<Box<str>>,
    pub service_tier: Option<Box<str>>,
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
    pub routing_now_unix_secs: u64,
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub thread_id: Option<QueryRef<'a>>,
    pub service_tier: Option<QueryRef<'a>>,
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

include!(concat!(env!("OUT_DIR"), "/wire_schema_v2_impls.rs"));
