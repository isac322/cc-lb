use cc_lb_plugin_wire::{
    WireSchema,
    v1::{
        self, CachePricingSummary, CachePricingSummaryRef, FilterRequest, FilterRequestRef,
        Principal, PrincipalRef,
    },
};
use rkyv::rancor::Error;

const FILTER_DESCRIPTOR: &str = "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,service_tier:Option<Box<str>>,thread_id:Option<Box<str>>}";
const FILTER_REF_DESCRIPTOR: &str = "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<QueryRef>,request_id:&str,service_tier:Option<QueryRef>,thread_id:Option<QueryRef>}";
const FILTER_FINGERPRINT: [u8; 32] = [
    242, 15, 102, 149, 66, 52, 156, 0, 42, 205, 4, 178, 48, 143, 240, 216, 56, 153, 191, 12, 181,
    161, 123, 129, 140, 29, 147, 138, 221, 165, 40, 52,
];

fn principal() -> Principal {
    Principal {
        id: Box::from("tenant-a"),
        kind: Box::from("api_key"),
        claims: Box::new([]),
    }
}

fn cache_pricing() -> CachePricingSummary {
    CachePricingSummary {
        status: Box::from("known"),
        input_micros_per_million: Some(5_000_000),
        cache_creation_5m_micros_per_million: Some(6_250_000),
        cache_creation_1h_micros_per_million: Some(10_000_000),
        cache_read_micros_per_million: Some(500_000),
    }
}

fn cache_pricing_ref() -> CachePricingSummaryRef<'static> {
    CachePricingSummaryRef {
        status: "known",
        input_micros_per_million: Some(5_000_000),
        cache_creation_5m_micros_per_million: Some(6_250_000),
        cache_creation_1h_micros_per_million: Some(10_000_000),
        cache_read_micros_per_million: Some(500_000),
    }
}

#[test]
fn filter_descriptor_includes_service_tier() {
    assert_eq!(<FilterRequest as WireSchema>::DESCRIPTOR, FILTER_DESCRIPTOR);
    assert_eq!(
        <FilterRequestRef<'_> as WireSchema>::DESCRIPTOR,
        FILTER_REF_DESCRIPTOR
    );
    assert_eq!(
        <FilterRequest as WireSchema>::FINGERPRINT,
        FILTER_FINGERPRINT
    );

    let request = FilterRequest {
        request_id: Box::from("filter-request"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: cache_pricing(),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([]),
        body: Box::from(&b"{}"[..]),
        principal: principal(),
        candidates: Box::new([]),
    };

    let bytes = rkyv::to_bytes::<Error>(&request).expect("encode");
    let archived = rkyv::access::<v1::ArchivedFilterRequest, Error>(&bytes).expect("access");
    let tier: Option<&str> = archived.service_tier.as_ref().map(|value| &**value);

    assert_eq!(tier, Some("priority"));
}

#[test]
fn filter_borrowed_request_encodes_to_owned_layout() {
    let request = FilterRequestRef {
        request_id: "filter-request",
        thread_id: None,
        service_tier: Some(v1::QueryRef { value: "priority" }),
        canonical_model_id: "claude-test",
        cache_pricing: cache_pricing_ref(),
        method: "POST",
        path: "/v1/messages",
        query: None,
        headers: &[],
        body: b"{}",
        principal: PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &[],
        },
        candidates: &[],
    };

    let bytes = rkyv::to_bytes::<Error>(&request).expect("encode borrowed");
    let archived = rkyv::access::<v1::ArchivedFilterRequest, Error>(&bytes).expect("access owned");
    let tier: Option<&str> = archived.service_tier.as_ref().map(|value| &**value);

    assert_eq!(tier, Some("priority"));
}
