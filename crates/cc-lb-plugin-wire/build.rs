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
            descriptor: "UpstreamCandidate{kind:Box<str>,name:Box<str>,observed_at_unix_secs:u64,predicted_cache_read_tokens:u32,upstream_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "UpstreamCandidateRef<'a>",
            descriptor: "UpstreamCandidateRef{kind:&str,name:&str,observed_at_unix_secs:u64,predicted_cache_read_tokens:u32,upstream_id:&str}",
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
            descriptor: "FilterRequest{body:Box<[u8]>,candidates:Box<[UpstreamCandidate]>,headers:Box<[Header]>,method:Box<str>,path:Box<str>,principal:Principal,query:Option<Box<str>>,request_id:Box<str>}",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "FilterRequestRef<'a>",
            descriptor: "FilterRequestRef{body:&[u8],candidates:&[UpstreamCandidateRef],headers:&[HeaderRef],method:&str,path:&str,principal:PrincipalRef,query:Option<QueryRef>,request_id:&str}",
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
            descriptor: "Upstream[AnthropicDirect{base_url:Option<Box<str>>}]",
        },
        SchemaEntry {
            impl_generics: "<'a>",
            ty: "UpstreamRef<'a>",
            descriptor: "UpstreamRef[AnthropicDirect{base_url:Option<QueryRef>}]",
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
            ty: "ObserveEvent",
            descriptor: "ObserveEvent[RequestStarted{request_id:Box<str>,downstream_user_agent:Option<Box<str>>},AuthnComplete{principal_id:Box<str>,principal_kind:Box<str>},UpstreamChosen{upstream:Upstream},Chunk{batch_index:u64,event_count:u64,total_bytes:u64},RequestFinished{status:u16,input_tokens:Option<u64>,output_tokens:Option<u64>,cache_creation_input_tokens:Option<u64>,cache_read_input_tokens:Option<u64>,duration_ms:u64},Error{code:Box<str>,message:Box<str>,source:Box<str>}]",
        },
    ];

    let mut output = String::new();
    for entry in entries {
        let digest = blake3::hash(entry.descriptor.as_bytes());
        let bytes = digest
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(
            "impl{} crate::schema::WireSchema for {} {{ const FINGERPRINT: [u8; 32] = [{}]; const DESCRIPTOR: &'static str = {:?}; }}\n",
            entry.impl_generics, entry.ty, bytes, entry.descriptor
        ));
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR set by Cargo"));
    fs::write(out_dir.join("wire_schema_impls.rs"), output)
        .expect("write generated WireSchema impls");
}
