//! Phase 1 W8 conformance scenarios.
//!
//! Builds on the W7 happy-path round-trip and exercises the
//! host-side invariants the review consensus pinned in the RFC:
//!
//! * **Multi-slot independence**: two registered plugins do not
//!   collide in the runtime slot map or leak state through the
//!   per-call fresh-`Store` dispatcher.
//! * **Concurrent calls**: dispatching from multiple threads against
//!   the same slot returns consistent results — each call gets its
//!   own instantiate + `Store`.
//! * **Large payload**: rkyv encode/decode + `Memory::write` /
//!   `Memory::data` cope with payloads larger than a single wasm
//!   page.
//!
//! All scenarios reuse the `cache-aware-wasmtime` artifact produced
//! for the W7 e2e test; no extra wasm fixtures are needed.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_types::{
    ArchivedFilterResponse, FilterRequest, FilterResponse, Header, Principal, UpstreamCandidate,
};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

fn wasm_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm")
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
    let claims = match keep_k {
        Some(k) => Vec::from([(String::from("keep_k"), k.to_string().into_bytes())]),
        None => Vec::new(),
    };
    FilterRequest {
        request_id: "req".to_owned(),
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

fn call(runtime: &WasmtimeRuntime, slot: &SlotKey, req: &FilterRequest) -> FilterResponse {
    let bytes = rkyv::to_bytes::<Error>(req).expect("encode");
    let out = runtime.call_filter(slot, bytes.as_slice()).expect("call");
    let mut aligned = AlignedVec::<16>::with_capacity(out.len());
    aligned.extend_from_slice(&out);
    let archived = rkyv::access::<ArchivedFilterResponse, Error>(&aligned).expect("access");
    rkyv::deserialize::<FilterResponse, Error>(archived).expect("deserialize")
}

fn accepted_ids(resp: &FilterResponse) -> Vec<String> {
    let mut ids: Vec<_> = resp
        .results
        .iter()
        .filter(|r| r.decision == "accept")
        .map(|r| r.upstream_id.clone())
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

    let slot_a = SlotKey::new("tenant-a", "cache-aware-wasmtime");
    let slot_b = SlotKey::new("tenant-b", "cache-aware-wasmtime");
    runtime
        .register_filter(slot_a.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register a");
    runtime
        .register_filter(slot_b.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register b");
    assert_eq!(runtime.slot_count(), 2);

    let resp_a = call(
        &runtime,
        &slot_a,
        &request(Some(1), &[("x", 50), ("y", 200)]),
    );
    let resp_b = call(
        &runtime,
        &slot_b,
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
    let slot = SlotKey::global("concurrent");
    runtime
        .register_filter(slot.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let runtime = Arc::clone(&runtime);
            let slot = slot.clone();
            thread::spawn(move || {
                for _ in 0..16 {
                    let req = request(Some(1), &[("a", 10 + i), ("b", 200 + i), ("c", 50 + i)]);
                    let resp = call(&runtime, &slot, &req);
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
    let slot = SlotKey::global("large-payload");
    runtime
        .register_filter(slot.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    // 256 candidates, each with a long upstream_id — well over one
    // 64 KiB wasm page once rkyv-serialised.
    let candidates: Vec<(String, u32)> = (0..256u32)
        .map(|i| (format!("upstream-{:0>32}-{i}", "x"), i))
        .collect();
    let candidate_refs: Vec<(&str, u32)> =
        candidates.iter().map(|(s, p)| (s.as_str(), *p)).collect();
    let req = request(Some(3), &candidate_refs);

    let resp = call(&runtime, &slot, &req);
    assert_eq!(resp.results.len(), 256);

    // Top 3 by predicted_cache_read_tokens — indices 253, 254, 255.
    let mut accepted = accepted_ids(&resp);
    accepted.sort();
    let expected: Vec<String> = (253..256u32)
        .map(|i| format!("upstream-{:0>32}-{i}", "x"))
        .collect();
    let mut expected_sorted = expected;
    expected_sorted.sort();
    assert_eq!(accepted, expected_sorted);
}
