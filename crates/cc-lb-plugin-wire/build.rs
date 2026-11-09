use std::env;
use std::fs;
use std::path::PathBuf;

struct SchemaEntry {
    impl_generics: &'static str,
    ty: &'static str,
    descriptor: &'static str,
}

fn main() {
    let entries = [
        SchemaEntry {
            impl_generics: "",
            ty: "Principal",
            descriptor: "Principal{claims:Box<[Claim]>,id:Box<str>,kind:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "Claim",
            descriptor: "Claim{key:Box<str>,value:Box<[u8]>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "PrincipalRef<'a>",
            descriptor: "PrincipalRef{claims:&[ClaimRef],id:&str,kind:&str}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "ClaimRef<'a>",
            descriptor: "ClaimRef{key:&str,value:&[u8]}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "UpstreamCandidate",
            descriptor: "UpstreamCandidate{kind:Box<str>,name:Box<str>,observed_at_unix_secs:u64,organization_type:Box<str>,plan_capacity_ratio:f64,predicted_cache_read_tokens:u32,rate_limit_tier:Box<str>,seat_tier:Box<str>,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "UpstreamCandidateRef<'a>",
            descriptor: "UpstreamCandidateRef{kind:&str,name:&str,observed_at_unix_secs:u64,organization_type:&str,plan_capacity_ratio:f64,predicted_cache_read_tokens:u32,rate_limit_tier:&str,seat_tier:&str,upstream_id:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "Header",
            descriptor: "Header{name:Box<str>,value:Box<[u8]>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "HeaderRef<'a>",
            descriptor: "HeaderRef{name:&str,value:&[u8]}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "FilterRequest",
            descriptor: "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,service_tier:Option<Box<str>>,thread_id:Option<Box<str>>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "FilterRequestRef<'a>",
            descriptor: "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<QueryRef>,request_id:&str,service_tier:Option<QueryRef>,thread_id:Option<QueryRef>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "QueryRef<'a>",
            descriptor: "QueryRef{value:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "PerCandidateReason",
            descriptor: "PerCandidateReason{decision:Box<str>,reason:Box<str>,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "FilterResponse",
            descriptor: "FilterResponse{results:Box<[PerCandidateReason]>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "Upstream",
            descriptor: "Upstream[AnthropicDirect{base_url:Option<Box<str>>},OpenaiDirect{base_url:Option<Box<str>>},GeminiDirect{base_url:Option<Box<str>>}]",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "UpstreamRef<'a>",
            descriptor: "UpstreamRef[AnthropicDirect{base_url:Option<QueryRef>},OpenaiDirect{base_url:Option<QueryRef>},GeminiDirect{base_url:Option<QueryRef>}]",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "ShapeRequest",
            descriptor: "ShapeRequest{body:Box<[u8]>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,upstream:Upstream}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "ShapeRequestRef<'a>",
            descriptor: "ShapeRequestRef{body:&[u8],headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<QueryRef>,request_id:&str,upstream:UpstreamRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "ShapeResponse",
            descriptor: "ShapeResponse{body:Box<[u8]>,headers:Box<[Header]>,method:Box<str>,url:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "TransformResponseRequest",
            descriptor: "TransformResponseRequest{body:Box<[u8]>,canonical_model_id:Box<str>,principal:Principal,request_id:Box<str>,request_method:Box<str>,request_path:Box<str>,response_headers:Box<[Header]>,response_status:u16,upstream:Upstream}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "TransformResponseRequestRef<'a>",
            descriptor: "TransformResponseRequestRef{body:&[u8],canonical_model_id:&str,principal:PrincipalRef,request_id:&str,request_method:&str,request_path:&str,response_headers:&[HeaderRef],response_status:u16,upstream:UpstreamRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "TransformResponseResult",
            descriptor: "TransformResponseResult[Unchanged,Replace{body:Option<Box<[u8]>>,headers:Option<Box<[Header]>>,status:Option<u16>} ]",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "SseEvent",
            descriptor: "SseEvent{data:Box<[u8]>,event:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "SseEventRef<'a>",
            descriptor: "SseEventRef{data:&[u8],event:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "TransformSseEventRequest",
            descriptor: "TransformSseEventRequest{canonical_model_id:Box<str>,event:SseEvent,principal:Principal,request_id:Box<str>,request_method:Box<str>,request_path:Box<str>,response_headers:Box<[Header]>,response_status:u16,upstream:Upstream}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "TransformSseEventRequestRef<'a>",
            descriptor: "TransformSseEventRequestRef{canonical_model_id:&str,event:SseEventRef,principal:PrincipalRef,request_id:&str,request_method:&str,request_path:&str,response_headers:&[HeaderRef],response_status:u16,upstream:UpstreamRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "TransformSseEventResult",
            descriptor: "TransformSseEventResult[Unchanged,Replace{events:Box<[SseEvent]>},Drop]",
        },
    ];

    let v2_entries = [
        SchemaEntry {
            impl_generics: "",
            ty: "v2::Principal",
            descriptor: "Principal{claims:Box<[Claim]>,id:Box<str>,kind:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::Claim",
            descriptor: "Claim{key:Box<str>,value:Box<[u8]>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::PrincipalRef<'a>",
            descriptor: "PrincipalRef{claims:&[ClaimRef],id:&str,kind:&str}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::ClaimRef<'a>",
            descriptor: "ClaimRef{key:&str,value:&[u8]}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::UpstreamIdentity",
            descriptor: "UpstreamIdentity{base_url:Option<Box<str>>,code_assist_project_id:Option<Box<str>>,credential_kind:Box<str>,gemini_oauth_profile:Option<Box<str>>,name:Box<str>,provider:Box<str>,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::UpstreamIdentityRef<'a>",
            descriptor: "UpstreamIdentityRef{base_url:Option<OptionalStrRef>,code_assist_project_id:Option<OptionalStrRef>,credential_kind:&str,gemini_oauth_profile:Option<OptionalStrRef>,name:&str,provider:&str,upstream_id:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::SubscriptionQuotaSample",
            descriptor: "SubscriptionQuotaSample{observed_at_unix_millis:Option<u64>,resets_at_unix_secs:Option<u64>,sample_kind:Box<str>,status:Option<Box<str>>,utilization:Option<f64>,window:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::SubscriptionQuotaSampleRef<'a>",
            descriptor: "SubscriptionQuotaSampleRef{observed_at_unix_millis:Option<u64>,resets_at_unix_secs:Option<u64>,sample_kind:&str,status:Option<OptionalStrRef>,utilization:Option<f64>,window:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::UpstreamCandidate",
            descriptor: "UpstreamCandidate{base_url:Option<Box<str>>,kind:Box<str>,name:Box<str>,observed_at_unix_secs:u64,organization_type:Box<str>,plan_capacity_ratio:f64,predicted_cache_creation_tokens_1h:u32,predicted_cache_creation_tokens_5m:u32,predicted_cache_read_tokens:u32,predicted_uncached_input_tokens:u32,rate_limit_tier:Box<str>,seat_tier:Box<str>,subscription_quotas:Box<[SubscriptionQuotaSample]>,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::UpstreamCandidateRef<'a>",
            descriptor: "UpstreamCandidateRef{base_url:Option<OptionalStrRef>,kind:&str,name:&str,observed_at_unix_secs:u64,organization_type:&str,plan_capacity_ratio:f64,predicted_cache_creation_tokens_1h:u32,predicted_cache_creation_tokens_5m:u32,predicted_cache_read_tokens:u32,predicted_uncached_input_tokens:u32,rate_limit_tier:&str,seat_tier:&str,subscription_quotas:&[SubscriptionQuotaSampleRef],upstream_id:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::CachePricingSummary",
            descriptor: "CachePricingSummary{cache_creation_1h_micros_per_million:Option<u64>,cache_creation_5m_micros_per_million:Option<u64>,cache_read_micros_per_million:Option<u64>,input_micros_per_million:Option<u64>,status:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::CachePricingSummaryRef<'a>",
            descriptor: "CachePricingSummaryRef{cache_creation_1h_micros_per_million:Option<u64>,cache_creation_5m_micros_per_million:Option<u64>,cache_read_micros_per_million:Option<u64>,input_micros_per_million:Option<u64>,status:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::Header",
            descriptor: "Header{name:Box<str>,value:Box<[u8]>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::HeaderRef<'a>",
            descriptor: "HeaderRef{name:&str,value:&[u8]}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::OptionalStrRef<'a>",
            descriptor: "OptionalStrRef{value:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::FilterRequest",
            descriptor: "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<OptionalStrRef>,request_id:Box<str>,service_tier:Option<OptionalStrRef>,thread_id:Option<OptionalStrRef>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::FilterRequestRef<'a>",
            descriptor: "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<OptionalStrRef>,request_id:&str,service_tier:Option<OptionalStrRef>,thread_id:Option<OptionalStrRef>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::PerCandidateReason",
            descriptor: "PerCandidateReason{decision:Box<str>,reason:Box<str>,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::FilterResponse",
            descriptor: "FilterResponse{results:Box<[PerCandidateReason]>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::Upstream",
            descriptor: "Upstream[AnthropicDirect{base_url:Option<Box<str>>},GeminiDirect{base_url:Option<Box<str>>},OpenaiDirect{base_url:Option<Box<str>>}]",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::UpstreamRef<'a>",
            descriptor: "UpstreamRef[AnthropicDirect{base_url:Option<OptionalStrRef>},GeminiDirect{base_url:Option<OptionalStrRef>},OpenaiDirect{base_url:Option<OptionalStrRef>}]",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::ShapeRequest",
            descriptor: "ShapeRequest{body:Box<[u8]>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,upstream:Upstream,upstream_identity:UpstreamIdentity}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::ShapeRequestRef<'a>",
            descriptor: "ShapeRequestRef{body:&[u8],headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<OptionalStrRef>,request_id:&str,upstream:UpstreamRef,upstream_identity:UpstreamIdentityRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::ShapeResponse",
            descriptor: "ShapeResponse{body:Box<[u8]>,headers:Box<[Header]>,method:Box<str>,url:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::TransformResponseRequest",
            descriptor: "TransformResponseRequest{body:Box<[u8]>,canonical_model_id:Box<str>,principal:Principal,request_id:Box<str>,request_method:Box<str>,request_path:Box<str>,response_headers:Box<[Header]>,response_status:u16,upstream:Upstream,upstream_identity:UpstreamIdentity}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::TransformResponseRequestRef<'a>",
            descriptor: "TransformResponseRequestRef{body:&[u8],canonical_model_id:&str,principal:PrincipalRef,request_id:&str,request_method:&str,request_path:&str,response_headers:&[HeaderRef],response_status:u16,upstream:UpstreamRef,upstream_identity:UpstreamIdentityRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::TransformResponseResult",
            descriptor: "TransformResponseResult[Unchanged,Replace{body:Option<Box<[u8]>>,headers:Option<Box<[Header]>>,status:Option<u16>} ]",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::SseEvent",
            descriptor: "SseEvent{data:Box<[u8]>,event:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::SseEventRef<'a>",
            descriptor: "SseEventRef{data:&[u8],event:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::TransformSseEventRequest",
            descriptor: "TransformSseEventRequest{canonical_model_id:Box<str>,event:SseEvent,principal:Principal,request_id:Box<str>,request_method:Box<str>,request_path:Box<str>,response_headers:Box<[Header]>,response_status:u16,upstream:Upstream,upstream_identity:UpstreamIdentity}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::TransformSseEventRequestRef<'a>",
            descriptor: "TransformSseEventRequestRef{canonical_model_id:&str,event:SseEventRef,principal:PrincipalRef,request_id:&str,request_method:&str,request_path:&str,response_headers:&[HeaderRef],response_status:u16,upstream:UpstreamRef,upstream_identity:UpstreamIdentityRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::TransformRealtimeFrameRequest",
            descriptor: "TransformRealtimeFrameRequest{canonical_model_id:Box<str>,direction:Box<str>,kind:Box<str>,payload:Box<[u8]>,principal:Principal,request_id:Box<str>,request_method:Box<str>,request_path:Box<str>,upstream:Upstream,upstream_identity:UpstreamIdentity}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::TransformRealtimeFrameRequestRef<'a>",
            descriptor: "TransformRealtimeFrameRequestRef{canonical_model_id:&str,direction:&str,kind:&str,payload:&[u8],principal:PrincipalRef,request_id:&str,request_method:&str,request_path:&str,upstream:UpstreamRef,upstream_identity:UpstreamIdentityRef}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::TransformRealtimeFrameResult",
            descriptor: "TransformRealtimeFrameResult[Unchanged,Replace{kind:Box<str>,payload:Box<[u8]>},Drop]",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::ShapeEnvelopeGeminiRequest",
            descriptor: "ShapeEnvelopeGeminiRequest{envelope_body:Box<[u8]>,headers:Box<[Header]>,upstream:UpstreamIdentity,url:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v2::ShapeEnvelopeGeminiRequestRef<'a>",
            descriptor: "ShapeEnvelopeGeminiRequestRef{envelope_body:&[u8],headers:&[HeaderRef],upstream:UpstreamIdentityRef,url:&str}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v2::ShapeEnvelopeGeminiResponse",
            descriptor: "ShapeEnvelopeGeminiResponse{envelope_body:Option<Box<[u8]>>,headers:Option<Box<[Header]>>}",
        },
    ];

    let v3_entries = [
        SchemaEntry {
            impl_generics: "",
            ty: "v3::NativeCachePricingRate",
            descriptor: "NativeCachePricingRate{component:NativeCachePricingComponent,lifetime:Option<NativeCachePricingLifetime>,micros_per_million:Option<u64>,status:NativeCachePricingStatus}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v3::NativeCachePricingRateRef",
            descriptor: "NativeCachePricingRateRef{component:NativeCachePricingComponent,lifetime:Option<NativeCachePricingLifetime>,micros_per_million:Option<u64>,status:NativeCachePricingStatus}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v3::NativeCachePricing",
            descriptor: "NativeCachePricing{provider:NativeCachePricingProvider,rates:Box<[NativeCachePricingRate]>,version:u8}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v3::NativeCachePricingRef<'a>",
            descriptor: "NativeCachePricingRef{provider:NativeCachePricingProvider,rates:&[NativeCachePricingRateRef],version:u8}",
        },
        SchemaEntry {
            impl_generics: "",
            ty: "v3::FilterRequest",
            descriptor: "FilterRequest{body:Box<[u8]>,cache_pricing:CachePricingSummary,candidates:Box<[UpstreamCandidate]>,canonical_model_id:Box<str>,headers:Box<[Header]>,method:Box<str>,native_pricing:NativeCachePricing,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>,service_tier:Option<Box<str>>,thread_id:Option<Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "v3::FilterRequestRef<'a>",
            descriptor: "FilterRequestRef{body:&[u8],cache_pricing:CachePricingSummaryRef,candidates:&[UpstreamCandidateRef],canonical_model_id:&str,headers:&[HeaderRef],method:&str,native_pricing:NativeCachePricingRef,path:&str,principal:PrincipalRef,query:Option<OptionalStrRef>,request_id:&str,service_tier:Option<OptionalStrRef>,thread_id:Option<OptionalStrRef>}",
        },
    ];

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR set by Cargo"));
    fs::write(
        out_dir.join("wire_schema_impls.rs"),
        render_schema_impls(&entries),
    )
    .expect("write generated V1 WireSchema impls");
    fs::write(
        out_dir.join("wire_schema_v2_impls.rs"),
        render_schema_impls(&v2_entries),
    )
    .expect("write generated V2 WireSchema impls");
    fs::write(
        out_dir.join("wire_schema_v3_impls.rs"),
        render_schema_impls(&v3_entries),
    )
    .expect("write generated V3 WireSchema impls");
}

fn render_schema_impls(entries: &[SchemaEntry]) -> String {
    let mut output = String::new();
    for entry in entries {
        let digest = blake3::hash(entry.descriptor.as_bytes());
        let bytes = digest
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let ty = if entry.ty.starts_with("v2::") || entry.ty.starts_with("v3::") {
            format!("crate::{}", entry.ty)
        } else {
            entry.ty.to_owned()
        };
        output.push_str(&format!(
            "impl{} crate::schema::WireSchema for {} {{ const FINGERPRINT: [u8; 32] = [{}]; const DESCRIPTOR: &'static str = {:?}; }}\n",
            entry.impl_generics, ty, bytes, entry.descriptor
        ));
    }

    output
}
