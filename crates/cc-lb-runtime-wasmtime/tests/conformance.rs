//! Phase 1 W8 conformance scenarios.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use cc_lb_plugin_wire::v2::{
    ArchivedFilterResponse, CachePricingSummary, Claim, FilterResponse, Principal,
    UpstreamCandidate,
};
use cc_lb_plugin_wire::v3::{FilterRequest, NativeCachePricing, NativeCachePricingProvider};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
};
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

fn wasm_path() -> PathBuf {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    workspace_root.join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm")
}

fn load_wasm_or_skip() -> Option<Vec<u8>> {
    let path = wasm_path();
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            eprintln!(
                "skipping conformance: wasm artifact missing at {} ({err}). \
                Run `cargo build --target wasm32-unknown-unknown --release -p cache-aware-wasmtime` first.",
                path.display(),
            );
            None
        }
    }
}

fn request(keep_k: Option<usize>, predicted: &[(&str, u32)]) -> FilterRequest {
    let claims: Box<[Claim]> = match keep_k {
        Some(k) => Box::new([Claim {
            key: Box::from("keep_k"),
            value: Box::from(k.to_string().into_bytes().as_slice()),
        }]),
        None => Box::new([]),
    };
    let candidates: Box<[UpstreamCandidate]> = predicted
        .iter()
        .map(|(id, p)| UpstreamCandidate {
            upstream_id: Box::from(*id),
            name: format!("upstream-{id}").into_boxed_str(),
            kind: Box::from("anthropic_api_key"),
            base_url: None,
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: *p,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([]),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    FilterRequest {
        request_id: Box::from("req"),
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
            claims,
        },
        candidates,
    }
}

fn call(dispatch: &WasmPluginWireDispatch, req: &FilterRequest) -> FilterResponse {
    let bytes = rkyv::to_bytes::<Error>(req).expect("encode");
    let out = dispatch
        .call_filter_scoped(bytes.as_slice(), <[u8]>::to_vec)
        .expect("call");
    let mut aligned = AlignedVec::<16>::with_capacity(out.len());
    aligned.extend_from_slice(&out);
    let archived = rkyv::access::<ArchivedFilterResponse, Error>(&aligned).expect("access");
    rkyv::deserialize::<FilterResponse, Error>(archived).expect("deserialize")
}

fn accepted_ids(resp: &FilterResponse) -> Vec<String> {
    let mut ids: Vec<String> = resp
        .results
        .iter()
        .filter(|r| &*r.decision == "accept")
        .map(|r| r.upstream_id.to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn multi_slot_independent() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

    let slot_a = RuntimeSlotKey::new("tenant-a", "cache-aware-wasmtime", RuntimeSlotRole::Filter);
    let slot_b = RuntimeSlotKey::new("tenant-b", "cache-aware-wasmtime", RuntimeSlotRole::Filter);
    let loaded_a = runtime
        .register_filter(slot_a.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register a");
    let loaded_b = runtime
        .register_filter(slot_b.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register b");
    assert!(runtime.get_slot(&slot_a).is_some());
    assert!(runtime.get_slot(&slot_b).is_some());
    let dispatch_a = WasmPluginWireDispatch::from_slot(loaded_a, runtime.config_arc());
    let dispatch_b = WasmPluginWireDispatch::from_slot(loaded_b, runtime.config_arc());

    let resp_a = call(&dispatch_a, &request(Some(1), &[("x", 50), ("y", 200)]));
    let resp_b = call(
        &dispatch_b,
        &request(Some(2), &[("p", 10), ("q", 20), ("r", 30)]),
    );

    assert_eq!(accepted_ids(&resp_a), vec!["y".to_owned()]);
    assert_eq!(accepted_ids(&resp_b), vec!["q".to_owned(), "r".to_owned()]);
}

#[test]
fn concurrent_callers_each_build_their_own_worker() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot_key = RuntimeSlotKey::global("concurrent", RuntimeSlotRole::Filter);
    let slot = runtime
        .register_filter(slot_key, "cache-aware-wasmtime", &wasm)
        .expect("register");
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let dispatch = Arc::clone(&dispatch);
            thread::spawn(move || {
                for _ in 0..16 {
                    let req = request(Some(1), &[("a", 10 + i), ("b", 200 + i), ("c", 50 + i)]);
                    let resp = call(&dispatch, &req);
                    assert_eq!(accepted_ids(&resp), vec!["b".to_owned()]);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread");
    }
}

#[test]
fn large_payload_round_trips() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot_key = RuntimeSlotKey::global("large-payload", RuntimeSlotRole::Filter);
    let slot = runtime
        .register_filter(slot_key, "cache-aware-wasmtime", &wasm)
        .expect("register");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());

    let candidates: Vec<(String, u32)> = (0..256u32)
        .map(|i| (format!("upstream-{:0>32}-{i}", "x"), i))
        .collect();
    let candidate_refs: Vec<(&str, u32)> =
        candidates.iter().map(|(s, p)| (s.as_str(), *p)).collect();
    let req = request(Some(3), &candidate_refs);

    let resp = call(&dispatch, &req);
    assert_eq!(resp.results.len(), 256);

    let accepted = accepted_ids(&resp);
    let expected: Vec<String> = (253..256u32)
        .map(|i| format!("upstream-{:0>32}-{i}", "x"))
        .collect();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(accepted, expected_sorted);
}
