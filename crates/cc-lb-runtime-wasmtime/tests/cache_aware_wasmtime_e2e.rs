//! End-to-end integration test against the wasm-compiled
//! `cache-aware-wasmtime` plugin.

use std::path::PathBuf;
use std::sync::Arc;

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
                "skipping cache_aware_wasmtime e2e: wasm artifact missing at {} ({err}). \
                Run `cargo build --target wasm32-unknown-unknown --release -p cache-aware-wasmtime` first.",
                path.display(),
            );
            None
        }
    }
}

fn fixture_request(keep_k: Option<usize>, predicted: &[(&str, u32)]) -> FilterRequest {
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
        request_id: Box::from("req-e2e"),
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

fn decode_response(bytes: &[u8]) -> FilterResponse {
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    let archived =
        rkyv::access::<ArchivedFilterResponse, Error>(&aligned).expect("rkyv access response");
    rkyv::deserialize::<FilterResponse, Error>(archived).expect("rkyv deserialize response")
}

#[test]
fn cache_aware_wasmtime_round_trips_filter() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot_key = RuntimeSlotKey::global("cache-aware-wasmtime", RuntimeSlotRole::Filter);
    let slot = runtime
        .register_filter(slot_key, "cache-aware-wasmtime", &wasm)
        .expect("inspect + register must accept the plugin");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());

    let req = fixture_request(
        Some(2),
        &[("aaaa", 10), ("bbbb", 200), ("cccc", 50), ("dddd", 80)],
    );
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("rkyv encode request");

    let out_bytes = dispatch
        .call_filter_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("filter call succeeds");

    let resp = decode_response(&out_bytes);
    assert_eq!(resp.results.len(), 4, "one decision per candidate");

    let accepted: Vec<String> = resp
        .results
        .iter()
        .filter(|r| &*r.decision == "accept")
        .map(|r| r.upstream_id.to_string())
        .collect();
    assert_eq!(accepted.len(), 2, "keep_k=2 keeps two candidates");

    let accepted: std::collections::BTreeSet<String> = accepted.into_iter().collect();
    let expected: std::collections::BTreeSet<String> =
        ["bbbb".to_owned(), "dddd".to_owned()].into_iter().collect();
    assert_eq!(accepted, expected);
}

#[test]
fn cache_aware_wasmtime_default_keep_k_keeps_one() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot_key = RuntimeSlotKey::global("cache-aware-wasmtime-default", RuntimeSlotRole::Filter);
    let slot = runtime
        .register_filter(slot_key, "cache-aware-wasmtime", &wasm)
        .expect("register");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());

    let req = fixture_request(None, &[("a", 10), ("b", 100), ("c", 50)]);
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("rkyv encode");

    let out_bytes = dispatch
        .call_filter_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("filter");

    let resp = decode_response(&out_bytes);
    let accepted: Vec<String> = resp
        .results
        .iter()
        .filter(|r| &*r.decision == "accept")
        .map(|r| r.upstream_id.to_string())
        .collect();
    assert_eq!(accepted, vec!["b".to_owned()]);
}
