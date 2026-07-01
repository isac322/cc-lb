//! Stage 2 pure-mode dispatch end-to-end.
//!
//! Verifies that `register_*_with(.., RegisterOptions { pure: true })`
//! routes every call through `call_*_hook_pure` — a fresh wasm `Store`
//! is built per call, the hook runs, the `Store` drops. No thread_local
//! cache reuse, no `version_id` compare.
//!
//! Each test runs the same plugin call twice; if the pure path were
//! accidentally caching `WorkerInstance` state across calls we would
//! still get a green run (the fixtures are stateless), but combined
//! with the conformance tests' explicit stateful (`pure: false`)
//! coverage this gives end-to-end signal for both dispatch shapes.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_types::{
    ArchivedFilterResponse, ArchivedShapeResponse, FilterRequest, FilterResponse, Header,
    ObserveEvent, Principal, ShapeRequest, ShapeResponse, Upstream, UpstreamCandidate,
};
use cc_lb_runtime_wasmtime::{RegisterOptions, WasmtimeRuntime};
use rkyv::rancor::Error;
use rkyv::util::AlignedVec;

fn artifact(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join(format!("target/wasm32-unknown-unknown/release/{file}"))
}

fn read_required(path: &PathBuf) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|err| {
        panic!(
            "wasm fixture missing at {} ({err}). build.rs should have produced it; CI must run \
            `cargo build --target wasm32-unknown-unknown --release` for the relevant fixture before \
            invoking this test.",
            path.display(),
        )
    })
}

fn filter_request(predicted: &[(&str, u32)]) -> FilterRequest {
    FilterRequest {
        request_id: "req-pure".to_owned(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: None,
        headers: Vec::<Header>::new(),
        body: Vec::new(),
        principal: Principal {
            id: "tenant".to_owned(),
            kind: "api_key".to_owned(),
            claims: Vec::from([(String::from("keep_k"), b"1".to_vec())]),
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

fn decode_filter(bytes: &[u8]) -> FilterResponse {
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    let archived =
        rkyv::access::<ArchivedFilterResponse, Error>(&aligned).expect("rkyv access filter");
    rkyv::deserialize::<FilterResponse, Error>(archived).expect("rkyv deserialize filter")
}

fn decode_shape(bytes: &[u8]) -> ShapeResponse {
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    let archived =
        rkyv::access::<ArchivedShapeResponse, Error>(&aligned).expect("rkyv access shape");
    rkyv::deserialize::<ShapeResponse, Error>(archived).expect("rkyv deserialize shape")
}

#[test]
fn filter_pure_round_trips_twice() {
    let wasm = read_required(&artifact("cache_aware_wasmtime.wasm"));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot = SlotKey::global("pure-filter");
    let cell = runtime
        .register_filter_with(
            slot.clone(),
            "cache-aware-wasmtime",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("register_filter_with pure");
    assert!(cell.current.load().pure, "cell should be marked pure");

    for _ in 0..2 {
        let req = filter_request(&[("a", 10), ("b", 200), ("c", 50)]);
        let bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
        let out = runtime.call_filter(&slot, bytes.as_slice()).expect("call");
        let resp = decode_filter(&out);
        let accepted: Vec<_> = resp
            .results
            .iter()
            .filter(|r| r.decision == "accept")
            .map(|r| r.upstream_id.clone())
            .collect();
        assert_eq!(accepted, vec!["b".to_owned()], "top-K should select b");
    }
}

#[test]
fn shape_pure_round_trips_twice() {
    let wasm = read_required(&artifact("wasmtime_shape_passthrough.wasm"));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot = SlotKey::global("pure-shape");
    runtime
        .register_shape_with(
            slot.clone(),
            "wasmtime-shape-passthrough",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("register_shape_with pure");

    let upstream_base = "https://test.invalid".to_owned();
    for path in ["/v1/messages", "/v1/messages/stream"] {
        let req = ShapeRequest {
            request_id: "req-shape-pure".to_owned(),
            method: "POST".to_owned(),
            path: path.to_owned(),
            query: None,
            headers: Vec::<Header>::new(),
            body: Vec::new(),
            principal: Principal {
                id: "tenant".to_owned(),
                kind: "api_key".to_owned(),
                claims: Vec::new(),
            },
            upstream: Upstream::AnthropicDirect {
                base_url: Some(upstream_base.clone()),
            },
        };
        let bytes = rkyv::to_bytes::<Error>(&req).expect("encode shape");
        let out = runtime
            .call_shape(&slot, bytes.as_slice())
            .expect("call shape");
        let resp = decode_shape(&out);
        let expected = format!("{upstream_base}{path}");
        assert_eq!(
            resp.url, expected,
            "passthrough must echo upstream base + path"
        );
    }
}

#[test]
fn observe_pure_round_trips_twice() {
    let wasm = read_required(&artifact("wasmtime_observe_noop.wasm"));
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot = SlotKey::global("pure-observe");
    runtime
        .register_observe_with(
            slot.clone(),
            "wasmtime-observe-noop",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("register_observe_with pure");

    for batch_index in 0..2u64 {
        let event = ObserveEvent::Chunk {
            batch_index,
            event_count: 1,
            total_bytes: 64,
        };
        let bytes = rkyv::to_bytes::<Error>(&event).expect("encode observe");
        let out = runtime
            .call_observe(&slot, bytes.as_slice())
            .expect("call observe");
        assert!(out.is_empty(), "observe contract = empty output");
    }
}
