use std::path::PathBuf;

use cc_lb_plugin_wire::v1::{
    ArchivedFilterResponse, CachePricingSummary, FilterRequest, FilterResponse, Principal,
};
use cc_lb_plugin_wire::{HookKind, WireVersion};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmtimeRuntime};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

fn wasm_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/wasmtime_filter_service_tier.wasm")
}

#[test]
fn filter_plugin_admits_and_receives_service_tier() {
    let wasm =
        std::fs::read(wasm_path()).expect("build script produces filter service-tier fixture");
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
    let inspection = runtime
        .admit_wasm(HookKind::Filter, &wasm)
        .expect("filter admission and probe");
    assert_eq!(
        inspection.hook_versions.get(&HookKind::Filter),
        Some(&WireVersion::V1)
    );

    let slot = RuntimeSlotKey::global("filter-service-tier");
    runtime
        .register_filter(slot.clone(), "wasmtime-filter-service-tier", &wasm)
        .expect("register filter service-tier");
    let request = FilterRequest {
        request_id: Box::from("req-service-tier"),
        thread_id: None,
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: CachePricingSummary {
            status: Box::from("known"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([]),
        body: Box::from(&b"{}"[..]),
        principal: Principal {
            id: Box::from("tenant"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([]),
    };
    let input = rkyv::to_bytes::<RkyvError>(&request).expect("encode filter request");
    let output = runtime
        .call_filter(&slot, input.as_slice())
        .expect("dispatch filter");
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    let archived = rkyv::access::<ArchivedFilterResponse, RkyvError>(&aligned)
        .expect("access filter response");
    let response = rkyv::deserialize::<FilterResponse, RkyvError>(archived)
        .expect("deserialize filter response");

    assert_eq!(&*response.results[0].reason, "priority");
}
