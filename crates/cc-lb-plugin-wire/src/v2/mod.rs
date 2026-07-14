//! Filter wire schema version 2.
//!
//! V2 extends only the filter request with the requested service tier. Shape,
//! observe, and response-transform hooks remain on V1.

use alloc::boxed::Box;

use rkyv::{Archive, Deserialize, Serialize, with::InlineAsBox};

pub use crate::v1::{
    ArchivedCachePricingSummary, ArchivedClaim, ArchivedFilterResponse, ArchivedHeader,
    ArchivedPerCandidateReason, ArchivedPrincipal, ArchivedQueryRef, ArchivedUpstreamCandidate,
    CachePricingSummary, CachePricingSummaryRef, Claim, ClaimRef, FilterResponse, Header,
    HeaderRef, PerCandidateReason, Principal, PrincipalRef, QueryRef, UpstreamCandidate,
    UpstreamCandidateRef,
};

/// Filter V2 input received by an owned-mode guest handler.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterRequest {
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

/// Borrowed host-side encoding form for [`FilterRequest`].
#[derive(Archive, Serialize)]
pub struct FilterRequestRef<'a> {
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
