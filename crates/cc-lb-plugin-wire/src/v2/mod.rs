//! Provider-qualified wire schema version 2.
//!
//! V2 adds provider/upstream identity to shape and response hooks and exposes
//! subscription quota samples to filters. Legacy V1 remains byte-for-byte
//! unchanged and is selected from the guest's declared metadata.

use alloc::boxed::Box;
use rkyv::{Archive, Deserialize, Serialize, with::InlineAsBox};

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Principal {
    pub id: Box<str>,
    pub kind: Box<str>,
    pub claims: Box<[Claim]>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Claim {
    pub key: Box<str>,
    pub value: Box<[u8]>,
}

#[derive(Archive, Serialize)]
pub struct PrincipalRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub kind: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub claims: &'a [ClaimRef<'a>],
}

#[derive(Archive, Serialize)]
pub struct ClaimRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub key: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub value: &'a [u8],
}

/// Identity of the selected upstream. OAuth profile and Code Assist project
/// are only populated for Gemini OAuth credentials; they never contain tokens.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct UpstreamIdentity {
    pub provider: Box<str>,
    pub credential_kind: Box<str>,
    pub upstream_id: Box<str>,
    pub name: Box<str>,
    pub base_url: Option<Box<str>>,
    pub gemini_oauth_profile: Option<Box<str>>,
    pub code_assist_project_id: Option<Box<str>>,
}

#[derive(Archive, Serialize)]
pub struct UpstreamIdentityRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub provider: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub credential_kind: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub upstream_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub name: &'a str,
    pub base_url: Option<OptionalStrRef<'a>>,
    pub gemini_oauth_profile: Option<OptionalStrRef<'a>>,
    pub code_assist_project_id: Option<OptionalStrRef<'a>>,
}

#[derive(Archive, Serialize)]
pub struct OptionalStrRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub value: &'a str,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct SubscriptionQuotaSample {
    pub window: Box<str>,
    pub utilization: Option<f64>,
    pub resets_at_unix_secs: Option<u64>,
    pub status: Option<Box<str>>,
    /// `sample`, `absent`, or `unobserved`.
    pub sample_kind: Box<str>,
    pub observed_at_unix_millis: Option<u64>,
}

#[derive(Archive, Serialize)]
pub struct SubscriptionQuotaSampleRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub window: &'a str,
    pub utilization: Option<f64>,
    pub resets_at_unix_secs: Option<u64>,
    pub status: Option<OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub sample_kind: &'a str,
    pub observed_at_unix_millis: Option<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct UpstreamCandidate {
    pub upstream_id: Box<str>,
    pub name: Box<str>,
    pub kind: Box<str>,
    pub base_url: Option<Box<str>>,
    pub observed_at_unix_secs: u64,
    pub predicted_cache_read_tokens: u32,
    pub predicted_cache_creation_tokens_5m: u32,
    pub predicted_cache_creation_tokens_1h: u32,
    pub predicted_uncached_input_tokens: u32,
    pub plan_capacity_ratio: f64,
    pub organization_type: Box<str>,
    pub rate_limit_tier: Box<str>,
    pub seat_tier: Box<str>,
    pub subscription_quotas: Box<[SubscriptionQuotaSample]>,
}

#[derive(Archive, Serialize)]
pub struct UpstreamCandidateRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub upstream_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub name: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub kind: &'a str,
    pub base_url: Option<OptionalStrRef<'a>>,
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
    pub subscription_quotas: &'a [SubscriptionQuotaSampleRef<'a>],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct CachePricingSummary {
    pub status: Box<str>,
    pub input_micros_per_million: Option<u64>,
    pub cache_creation_5m_micros_per_million: Option<u64>,
    pub cache_creation_1h_micros_per_million: Option<u64>,
    pub cache_read_micros_per_million: Option<u64>,
}

#[derive(Archive, Serialize)]
pub struct CachePricingSummaryRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub status: &'a str,
    pub input_micros_per_million: Option<u64>,
    pub cache_creation_5m_micros_per_million: Option<u64>,
    pub cache_creation_1h_micros_per_million: Option<u64>,
    pub cache_read_micros_per_million: Option<u64>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct Header {
    pub name: Box<str>,
    pub value: Box<[u8]>,
}

#[derive(Archive, Serialize)]
pub struct HeaderRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub name: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub value: &'a [u8],
}

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

#[derive(Archive, Serialize)]
pub struct FilterRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub thread_id: Option<OptionalStrRef<'a>>,
    pub service_tier: Option<OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    pub cache_pricing: CachePricingSummaryRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub path: &'a str,
    pub query: Option<OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
    pub principal: PrincipalRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub candidates: &'a [UpstreamCandidateRef<'a>],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct PerCandidateReason {
    pub upstream_id: Box<str>,
    pub decision: Box<str>,
    pub reason: Box<str>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct FilterResponse {
    pub results: Box<[PerCandidateReason]>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum Upstream {
    AnthropicDirect { base_url: Option<Box<str>> },
    OpenaiDirect { base_url: Option<Box<str>> },
    GeminiDirect { base_url: Option<Box<str>> },
}

#[derive(Archive, Serialize)]
pub enum UpstreamRef<'a> {
    AnthropicDirect {
        base_url: Option<OptionalStrRef<'a>>,
    },
    OpenaiDirect {
        base_url: Option<OptionalStrRef<'a>>,
    },
    GeminiDirect {
        base_url: Option<OptionalStrRef<'a>>,
    },
}

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
    pub upstream_identity: UpstreamIdentity,
}

#[derive(Archive, Serialize)]
pub struct ShapeRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub path: &'a str,
    pub query: Option<OptionalStrRef<'a>>,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    #[rkyv(with = InlineAsBox)]
    pub body: &'a [u8],
    pub principal: PrincipalRef<'a>,
    pub upstream: UpstreamRef<'a>,
    pub upstream_identity: UpstreamIdentityRef<'a>,
}

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
    pub upstream_identity: UpstreamIdentity,
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
    pub upstream_identity: UpstreamIdentityRef<'a>,
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
    pub upstream_identity: UpstreamIdentity,
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
    pub upstream_identity: UpstreamIdentityRef<'a>,
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
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct TransformRealtimeFrameRequest {
    pub request_id: Box<str>,
    pub principal: Principal,
    pub upstream: Upstream,
    pub upstream_identity: UpstreamIdentity,
    pub request_method: Box<str>,
    pub request_path: Box<str>,
    pub canonical_model_id: Box<str>,
    pub direction: Box<str>,
    pub kind: Box<str>,
    pub payload: Box<[u8]>,
}

#[derive(Archive, Serialize)]
pub struct TransformRealtimeFrameRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub request_id: &'a str,
    pub principal: PrincipalRef<'a>,
    pub upstream: UpstreamRef<'a>,
    pub upstream_identity: UpstreamIdentityRef<'a>,
    #[rkyv(with = InlineAsBox)]
    pub request_method: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub request_path: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub canonical_model_id: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub direction: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub kind: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub payload: &'a [u8],
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub enum TransformRealtimeFrameResult {
    Unchanged,
    Replace { kind: Box<str>, payload: Box<[u8]> },
    Drop,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeEnvelopeGeminiRequest {
    pub envelope_body: Box<[u8]>,
    pub url: Box<str>,
    pub headers: Box<[Header]>,
    pub upstream: UpstreamIdentity,
}

#[derive(Archive, Serialize)]
pub struct ShapeEnvelopeGeminiRequestRef<'a> {
    #[rkyv(with = InlineAsBox)]
    pub envelope_body: &'a [u8],
    #[rkyv(with = InlineAsBox)]
    pub url: &'a str,
    #[rkyv(with = InlineAsBox)]
    pub headers: &'a [HeaderRef<'a>],
    pub upstream: UpstreamIdentityRef<'a>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
#[rkyv(derive(Debug))]
pub struct ShapeEnvelopeGeminiResponse {
    pub envelope_body: Option<Box<[u8]>>,
    pub headers: Option<Box<[Header]>>,
}

include!(concat!(env!("OUT_DIR"), "/wire_schema_v2_impls.rs"));
