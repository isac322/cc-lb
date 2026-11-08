use cc_lb_plugin_wire::{
    WireSchema,
    v1::{
        self, CachePricingSummary, CachePricingSummaryRef, FilterRequest, FilterRequestRef,
        Principal, PrincipalRef,
    },
    v2, v3,
};
use rkyv::rancor::Error;

const FILTER_DESCRIPTOR: &str = "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,service_tier:Option<Box<str>>,thread_id:Option<Box<str>>}";
const FILTER_REF_DESCRIPTOR: &str = "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<QueryRef>,request_id:&str,service_tier:Option<QueryRef>,thread_id:Option<QueryRef>}";
const FILTER_FINGERPRINT: [u8; 32] = [
    242, 15, 102, 149, 66, 52, 156, 0, 42, 205, 4, 178, 48, 143, 240, 216, 56, 153, 191, 12, 181,
    161, 123, 129, 140, 29, 147, 138, 221, 165, 40, 52,
];
const V2_FILTER_DESCRIPTOR: &str = "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<OptionalStrRef>,request_id:Box<str>,service_tier:Option<OptionalStrRef>,thread_id:Option<OptionalStrRef>}";
const V3_FILTER_DESCRIPTOR: &str = "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,native_pricing:NativeCachePricing,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,service_tier:Option<Box<str>>,thread_id:Option<Box<str>}";
const V3_FILTER_REF_DESCRIPTOR: &str = "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,native_pricing:NativeCachePricingRef,path:&str,principal:PrincipalRef,query:Option<OptionalStrRef>,request_id:&str,service_tier:Option<OptionalStrRef>,thread_id:Option<OptionalStrRef>}";
const V2_FILTER_FINGERPRINT: [u8; 32] = [
    7, 86, 95, 145, 188, 140, 153, 104, 177, 41, 88, 226, 69, 218, 166, 87, 69, 67, 136, 109, 50,
    129, 10, 12, 174, 4, 184, 106, 53, 7, 111, 119,
];
const V3_FILTER_FINGERPRINT: [u8; 32] = [
    189, 190, 212, 170, 251, 172, 215, 31, 37, 157, 206, 64, 62, 170, 229, 231, 38, 38, 84, 116,
    49, 92, 15, 163, 78, 149, 18, 135, 193, 162, 200, 128,
];
const V3_FILTER_REF_FINGERPRINT: [u8; 32] = [
    128, 157, 18, 42, 252, 19, 139, 246, 17, 48, 207, 5, 7, 38, 158, 3, 185, 48, 134, 156, 211, 16,
    234, 35, 231, 170, 164, 26, 78, 92, 231, 137,
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

fn native_pricing() -> v3::NativeCachePricing {
    v3::NativeCachePricing {
        version: 1,
        provider: v3::NativeCachePricingProvider::OpenAi,
        rates: Box::new([
            v3::NativeCachePricingRate {
                component: v3::NativeCachePricingComponent::OrdinaryInput,
                lifetime: None,
                status: v3::NativeCachePricingStatus::Known,
                micros_per_million: Some(2_000_000),
            },
            v3::NativeCachePricingRate {
                component: v3::NativeCachePricingComponent::CachedInputRead,
                lifetime: None,
                status: v3::NativeCachePricingStatus::Known,
                micros_per_million: Some(100_000),
            },
            v3::NativeCachePricingRate {
                component: v3::NativeCachePricingComponent::TotalWrite,
                lifetime: Some(v3::NativeCachePricingLifetime::ThirtyMinutes),
                status: v3::NativeCachePricingStatus::Known,
                micros_per_million: Some(2_500_000),
            },
            v3::NativeCachePricingRate {
                component: v3::NativeCachePricingComponent::ExplicitResourceStorage,
                lifetime: Some(v3::NativeCachePricingLifetime::ExplicitResourceTtl),
                status: v3::NativeCachePricingStatus::Unknown,
                micros_per_million: None,
            },
            v3::NativeCachePricingRate {
                component: v3::NativeCachePricingComponent::ExplicitResourceTtl,
                lifetime: Some(v3::NativeCachePricingLifetime::ExplicitResourceTtl),
                status: v3::NativeCachePricingStatus::NotApplicable,
                micros_per_million: None,
            },
        ]),
    }
}

fn v2_cache_pricing() -> v2::CachePricingSummary {
    v2::CachePricingSummary {
        status: Box::from("known"),
        input_micros_per_million: Some(2_000_000),
        cache_creation_5m_micros_per_million: None,
        cache_creation_1h_micros_per_million: None,
        cache_read_micros_per_million: Some(100_000),
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

#[test]
fn v3_wire_version_is_parseable_and_host_supported_for_filter_only() {
    assert_eq!(
        cc_lb_plugin_wire::WireVersion::from_u8(3),
        Some(cc_lb_plugin_wire::WireVersion::V3)
    );
    assert_eq!(cc_lb_plugin_wire::WireVersion::V3.as_str(), "v3");
    assert!(cc_lb_plugin_wire::schema::host_supports(
        cc_lb_plugin_wire::HookKind::Filter,
        cc_lb_plugin_wire::WireVersion::V3
    ));
    assert!(!cc_lb_plugin_wire::schema::host_supports(
        cc_lb_plugin_wire::HookKind::Shape,
        cc_lb_plugin_wire::WireVersion::V3
    ));
}

#[test]
fn v3_filter_round_trips_native_pricing_context() {
    assert_eq!(
        <v2::FilterRequest as WireSchema>::DESCRIPTOR,
        V2_FILTER_DESCRIPTOR
    );
    assert_eq!(
        <v2::FilterRequest as WireSchema>::FINGERPRINT,
        V2_FILTER_FINGERPRINT
    );
    assert_eq!(
        <v3::FilterRequest as WireSchema>::DESCRIPTOR,
        V3_FILTER_DESCRIPTOR
    );
    assert_eq!(
        <v3::FilterRequest as WireSchema>::FINGERPRINT,
        V3_FILTER_FINGERPRINT
    );
    assert_eq!(
        <v3::FilterRequestRef<'_> as WireSchema>::DESCRIPTOR,
        V3_FILTER_REF_DESCRIPTOR
    );
    assert_eq!(
        <v3::FilterRequestRef<'_> as WireSchema>::FINGERPRINT,
        V3_FILTER_REF_FINGERPRINT
    );

    let request = v3::FilterRequest {
        request_id: Box::from("v3-filter-request"),
        thread_id: None,
        service_tier: None,
        canonical_model_id: Box::from("gpt-6.1-sol"),
        cache_pricing: v2_cache_pricing(),
        native_pricing: native_pricing(),
        method: Box::from("POST"),
        path: Box::from("/v1/responses"),
        query: None,
        headers: Box::new([]),
        body: Box::from(&b"{}"[..]),
        principal: v2::Principal {
            id: Box::from("tenant-a"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([]),
    };

    let bytes = rkyv::to_bytes::<Error>(&request).expect("encode v3");
    let archived = rkyv::access::<v3::ArchivedFilterRequest, Error>(&bytes).expect("access v3");
    let owned: v3::FilterRequest =
        rkyv::deserialize::<v3::FilterRequest, Error>(archived).expect("deserialize v3");
    assert_eq!(
        owned.native_pricing.provider,
        v3::NativeCachePricingProvider::OpenAi
    );
    assert_eq!(
        owned.native_pricing.rates[0].status,
        v3::NativeCachePricingStatus::Known
    );
    assert_eq!(
        owned.native_pricing.rates[2].lifetime,
        Some(v3::NativeCachePricingLifetime::ThirtyMinutes)
    );
    assert_eq!(
        owned.native_pricing.rates[3].status,
        v3::NativeCachePricingStatus::Unknown
    );
}

#[test]
fn v3_filter_borrowed_request_uses_owned_archived_layout() {
    let native = native_pricing();
    let rate_refs = native
        .rates
        .iter()
        .map(|rate| v3::NativeCachePricingRateRef {
            component: rate.component,
            lifetime: rate.lifetime,
            status: rate.status,
            micros_per_million: rate.micros_per_million,
        })
        .collect::<Vec<_>>();
    let request = v3::FilterRequestRef {
        request_id: "v3-filter-request",
        thread_id: None,
        service_tier: None,
        canonical_model_id: "gpt-6.1-sol",
        cache_pricing: v2::CachePricingSummaryRef {
            status: "known",
            input_micros_per_million: Some(2_000_000),
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: Some(100_000),
        },
        native_pricing: v3::NativeCachePricingRef {
            version: native.version,
            provider: native.provider,
            rates: &rate_refs,
        },
        method: "POST",
        path: "/v1/responses",
        query: None,
        headers: &[],
        body: b"{}",
        principal: v2::PrincipalRef {
            id: "tenant-a",
            kind: "api_key",
            claims: &[],
        },
        candidates: &[],
    };
    let bytes = rkyv::to_bytes::<Error>(&request).expect("encode v3 ref");
    let archived = rkyv::access::<v3::ArchivedFilterRequest, Error>(&bytes).expect("access v3 ref");
    let owned: v3::FilterRequest =
        rkyv::deserialize::<v3::FilterRequest, Error>(archived).expect("deserialize v3 ref");
    assert_eq!(
        owned.native_pricing.provider,
        v3::NativeCachePricingProvider::OpenAi
    );
    assert_eq!(owned.native_pricing.rates.len(), 5);
    assert_eq!(
        owned.native_pricing.rates[2].lifetime,
        Some(v3::NativeCachePricingLifetime::ThirtyMinutes)
    );
}
