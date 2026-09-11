//! End-to-end integration test against the wasm-compiled
//! `cache-aware-wasmtime` plugin.
//!
//! Phase 1 W7 verification: the host inspect + register + call_filter
//! path round-trips with a real plugin built by the W4 PDK macros and
//! sees the BLAKE3 schema hash + required exports planted by the
//! schema/plugin custom sections.
//!
//! The test assumes `cargo build --target wasm32-unknown-unknown
//! --release -p cache-aware-wasmtime` has run beforehand. The
//! `wasm32-unknown-unknown` toolchain is part of the standard dev
//! setup per docs, so we don't shell out to cargo from the test.

use std::sync::Arc;

use crate::support::required_wasm;
use cc_lb_plugin_wire::{
    ArchivedFilterResponse, CachePricingSummary, FilterRequest, FilterResponse, Principal,
    UpstreamCandidate,
};
use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

fn fixture_request(keep_k: Option<usize>, predicted: &[(&str, u32)]) -> FilterRequest {
    use cc_lb_plugin_wire::Claim;
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
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: *p,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
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
fn t3__cache_aware_wasmtime_round_trips_filter() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot_key = RuntimeSlotKey::global("cache-aware-wasmtime");
    runtime
        .register_filter(slot_key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("inspect + register must accept the plugin");

    let req = fixture_request(
        Some(2),
        &[("aaaa", 10), ("bbbb", 200), ("cccc", 50), ("dddd", 80)],
    );
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("rkyv encode request");

    let out_bytes = runtime
        .call_filter(&slot_key, in_bytes.as_slice())
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

    // Top two by predicted_cache_read_tokens are bbbb(200) and dddd(80).
    let accepted: std::collections::BTreeSet<String> = accepted.into_iter().collect();
    let expected: std::collections::BTreeSet<String> =
        ["bbbb".to_owned(), "dddd".to_owned()].into_iter().collect();
    assert_eq!(accepted, expected);
}

#[test]
fn t3__cache_aware_wasmtime_default_keep_k_keeps_one() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot_key = RuntimeSlotKey::global("cache-aware-wasmtime-default");
    runtime
        .register_filter(slot_key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    let req = fixture_request(None, &[("a", 10), ("b", 100), ("c", 50)]);
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("rkyv encode");

    let out_bytes = runtime
        .call_filter(&slot_key, in_bytes.as_slice())
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
