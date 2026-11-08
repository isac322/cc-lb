//! Provider-native pricing context wire schema version 3.
//!
//! V3 keeps the V2 filter fields intact and adds a provider-scoped pricing
//! context. Native cache rates are tagged by provider, status, lifetime, and
//! component so a plugin never has to infer OpenAI or Gemini semantics from
//! Anthropic's five-minute/one-hour fields.

use alloc::boxed::Box;

use rkyv::{Archive, Deserialize, Serialize, with::InlineAsBox};

pub use crate::v2::{
    Claim, ClaimRef, Header, HeaderRef, OptionalStrRef, Principal, PrincipalRef, UpstreamCandidate,
    UpstreamCandidateRef,
};

/// Provider that owns native cache pricing semantics.
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum NativeCachePricingProvider {
    Unknown,
    Anthropic,
    OpenAi,
    Gemini,
}

/// Explicit status for each provider-native pricing component.
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum NativeCachePricingStatus {
    Known,
    Unknown,
    NotApplicable,
}

/// Provider retention lifetime associated with a native pricing rate.
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum NativeCachePricingLifetime {
    ModelRetention,
    ThirtyMinutes,
    OneHour,
    TwelveHours,
    ExplicitResourceTtl,
}

/// Native cache component represented by one tagged rate.
#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum NativeCachePricingComponent {
    OrdinaryInput,
    CachedInputRead,
    TotalWrite,
    ExplicitResourceToken,
    ExplicitResourceStorage,
    ExplicitResourceTtl,
}

/// One provider-native rate. A missing numeric rate is explicit through the
/// component status and is never interpreted as zero.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct NativeCachePricingRate {
    pub component: NativeCachePricingComponent,
    pub lifetime: Option<NativeCachePricingLifetime>,
    pub status: NativeCachePricingStatus,
    pub micros_per_million: Option<u64>,
}

#[derive(Archive, Serialize)]
pub struct NativeCachePricingRateRef {
    pub component: NativeCachePricingComponent,
    pub lifetime: Option<NativeCachePricingLifetime>,
    pub status: NativeCachePricingStatus,
    pub micros_per_million: Option<u64>,
}

/// Versioned provider-native pricing context carried by a V3 filter request.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct NativeCachePricing {
    pub version: u8,
    pub provider: NativeCachePricingProvider,
    pub rates: Box<[NativeCachePricingRate]>,
}

#[derive(Archive, Serialize)]
pub struct NativeCachePricingRef<'a> {
    pub version: u8,
    pub provider: NativeCachePricingProvider,
    #[rkyv(with = InlineAsBox)]
    pub rates: &'a [NativeCachePricingRateRef],
}

/// Naming aliases used by pricing and plugin callers.
pub type NativePricingProvider = NativeCachePricingProvider;
pub type NativePricingStatus = NativeCachePricingStatus;
pub type NativePricingLifetime = NativeCachePricingLifetime;
pub type NativePricingRate = NativeCachePricingRate;
pub type NativePricingRateRef = NativeCachePricingRateRef;
pub type ProviderNativePricing = NativeCachePricing;
pub type ProviderNativePricingRef<'a> = NativeCachePricingRef<'a>;
pub type NativePricingContext = NativeCachePricing;

/// V3 filter input. The V2 `cache_pricing` fields remain available for legacy
/// Anthropic behavior; `native_pricing` carries explicit provider semantics.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterRequest {
    pub request_id: Box<str>,
    pub thread_id: Option<Box<str>>,
    pub service_tier: Option<Box<str>>,
    pub canonical_model_id: Box<str>,
    pub cache_pricing: crate::v2::CachePricingSummary,
    pub native_pricing: NativeCachePricing,
    pub method: Box<str>,
    pub path: Box<str>,
    pub query: Option<Box<str>>,
    pub headers: Box<[Header]>,
    pub body: Box<[u8]>,
    pub principal: Principal,
    pub candidates: Box<[UpstreamCandidate]>,
}

#[derive(Archive, Serialize)]
pub struct FilterRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub thread_id: Option<crate::v2::OptionalStrRef<'a>>,
    pub service_tier: Option<crate::v2::OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    pub cache_pricing: crate::v2::CachePricingSummaryRef<'a>,
    pub native_pricing: NativeCachePricingRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub path: &'a str,
    pub query: Option<crate::v2::OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
    pub principal: PrincipalRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub candidates: &'a [UpstreamCandidateRef<'a>],
}

include!(concat!(env!("OUT_DIR"), "/wire_schema_v3_impls.rs"));
