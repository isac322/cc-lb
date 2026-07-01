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

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_types::{
    ArchivedFilterResponse, FilterRequest, FilterResponse, Header, Principal, UpstreamCandidate,
};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
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
    let claims = match keep_k {
        Some(k) => Vec::from([(String::from("keep_k"), k.to_string().into_bytes())]),
        None => Vec::new(),
    };
    FilterRequest {
        request_id: "req-e2e".to_owned(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: None,
        headers: Vec::<Header>::new(),
        body: Vec::new(),
        principal: Principal {
            id: "tenant".to_owned(),
            kind: "api_key".to_owned(),
            claims,
        },
        candidates: predicted
            .iter()
            .map(|(id, p)| UpstreamCandidate {
                upstream_id: (*id).to_owned(),
                name: format!("upstream-{id}"),
                kind: "anthropic_api_key".to_owned(),
                observed_at_unix_secs: 0,
                predicted_cache_read_tokens: *p,
            })
            .collect(),
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
    let slot_key = SlotKey::global("cache-aware-wasmtime");
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

    let accepted: Vec<_> = resp
        .results
        .iter()
        .filter(|r| r.decision == "accept")
        .map(|r| r.upstream_id.clone())
        .collect();
    assert_eq!(accepted.len(), 2, "keep_k=2 keeps two candidates");

    // Top two by predicted_cache_read_tokens are bbbb(200) and dddd(80).
    let accepted: std::collections::BTreeSet<_> = accepted.into_iter().collect();
    let expected: std::collections::BTreeSet<_> =
        ["bbbb".to_owned(), "dddd".to_owned()].into_iter().collect();
    assert_eq!(accepted, expected);
}

#[test]
fn cache_aware_wasmtime_default_keep_k_keeps_one() {
    let Some(wasm) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot_key = SlotKey::global("cache-aware-wasmtime-default");
    runtime
        .register_filter(slot_key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    let req = fixture_request(None, &[("a", 10), ("b", 100), ("c", 50)]);
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("rkyv encode");

    let out_bytes = runtime
        .call_filter(&slot_key, in_bytes.as_slice())
        .expect("filter");

    let resp = decode_response(&out_bytes);
    let accepted: Vec<_> = resp
        .results
        .iter()
        .filter(|r| r.decision == "accept")
        .map(|r| r.upstream_id.clone())
        .collect();
    assert_eq!(accepted, vec!["b".to_owned()]);
}
