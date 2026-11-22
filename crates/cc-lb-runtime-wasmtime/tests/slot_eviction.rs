use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::v2::{CachePricingSummary, Claim, Principal, UpstreamCandidate};
use cc_lb_plugin_wire::v3::{FilterRequest, NativeCachePricing, NativeCachePricingProvider};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
};
use rkyv::rancor::Error;

fn cache_aware_wasm() -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm");
    std::fs::read(path).ok()
}

fn tiny_filter_request() -> FilterRequest {
    FilterRequest {
        request_id: Box::from("evict-probe"),
        thread_id: None,
        service_tier: None,
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: CachePricingSummary {
            status: Box::from("unknown"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        native_pricing: NativeCachePricing {
            version: 1,
            provider: NativeCachePricingProvider::Anthropic,
            rates: Box::new([]),
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([]),
        body: Box::from(&[][..]),
        principal: Principal {
            id: Box::from("tenant"),
            kind: Box::from("api_key"),
            claims: Box::new([Claim {
                key: Box::from("keep_k"),
                value: Box::from(&b"1"[..]),
            }]),
        },
        candidates: Box::new([UpstreamCandidate {
            upstream_id: Box::from("a"),
            name: Box::from("upstream-a"),
            kind: Box::from("anthropic_api_key"),
            base_url: None,
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: 10,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([]),
        }]),
    }
}

#[test]
fn retain_slots_evicts_unretained_registrations() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-probe-A", RuntimeSlotRole::Filter);

    rt.register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");
    assert!(rt.get_slot(&key).is_some(), "slot registered");

    let removed = rt.retain_slots(&HashSet::new());
    assert_eq!(removed, vec![key.clone()]);
    assert!(
        rt.get_slot(&key).is_none(),
        "get_slot returns None post-retain"
    );

    let removed_again = rt.retain_slots(&HashSet::new());
    assert!(removed_again.is_empty(), "second retain is a no-op");
}

#[test]
fn retain_slots_leaves_prior_arc_dispatchable() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-probe-B", RuntimeSlotRole::Filter);

    let slot = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    let dispatch = WasmPluginWireDispatch::from_slot(slot, rt.config_arc());
    assert_eq!(rt.retain_slots(&HashSet::new()), vec![key.clone()]);
    assert!(rt.get_slot(&key).is_none(), "map entry gone");

    let req = tiny_filter_request();
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let out = dispatch
        .call_filter_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("hook still callable");
    assert!(!out.is_empty());
}
