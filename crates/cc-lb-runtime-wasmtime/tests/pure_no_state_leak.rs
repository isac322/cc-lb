//! Pure-mode dispatch contracts.
//!
//! The deterministic T3 contract proves that a fresh `Store` per call
//! produces byte-identical output. The Linux-only TX check separately
//! constrains RSS growth across the same ten-call workload.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::{CachePricingSummary, FilterRequest, Principal, UpstreamCandidate};
use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use rkyv::rancor::Error;

const ITERATIONS: usize = 10;
#[cfg(target_os = "linux")]
const RSS_GROWTH_CEILING_KIB: u64 = 8 * 1024;

fn wasm_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm")
}

fn request() -> FilterRequest {
    use cc_lb_plugin_wire::Claim;
    FilterRequest {
        request_id: Box::from("leak-probe"),
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
            claims: Box::new([Claim {
                key: Box::from("keep_k"),
                value: Box::from(&b"1"[..]),
            }]),
        },
        candidates: Box::new([
            UpstreamCandidate {
                upstream_id: Box::from("a"),
                name: Box::from("upstream-a"),
                kind: Box::from("anthropic_api_key"),
                observed_at_unix_secs: 0,
                predicted_cache_read_tokens: 10,
                predicted_cache_creation_tokens_5m: 0,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                plan_capacity_ratio: 1.0,
                organization_type: Box::from(""),
                rate_limit_tier: Box::from(""),
                seat_tier: Box::from(""),
            },
            UpstreamCandidate {
                upstream_id: Box::from("b"),
                name: Box::from("upstream-b"),
                kind: Box::from("anthropic_api_key"),
                observed_at_unix_secs: 0,
                predicted_cache_read_tokens: 200,
                predicted_cache_creation_tokens_5m: 0,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                plan_capacity_ratio: 1.0,
                organization_type: Box::from(""),
                rate_limit_tier: Box::from(""),
                seat_tier: Box::from(""),
            },
        ]),
    }
}

#[cfg(target_os = "linux")]
fn vmrss_kib() -> std::io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|rest| rest.trim().strip_suffix(" kB"))
                .and_then(|kib| kib.trim().parse::<u64>().ok())
        })
        .ok_or_else(|| std::io::Error::other("VmRSS missing"))
}

fn pure_mode_fixture() -> (Arc<WasmtimeRuntime>, RuntimeSlotKey, Vec<u8>) {
    let wasm = std::fs::read(wasm_path()).expect("build script produces cache-aware wasm fixture");
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot = RuntimeSlotKey::global("pure-leak-probe");
    runtime
        .register_filter(slot.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register filter");
    let in_bytes = rkyv::to_bytes::<Error>(&request()).expect("encode");
    (runtime, slot, in_bytes.to_vec())
}

fn call_pure_mode(
    runtime: &WasmtimeRuntime,
    slot: &RuntimeSlotKey,
    in_bytes: &[u8],
) -> Vec<Vec<u8>> {
    (0..ITERATIONS)
        .map(|_| runtime.call_filter(slot, in_bytes).expect("call"))
        .collect()
}

#[test]
fn t3__pure_mode_returns_deterministic_output() {
    let (runtime, slot, in_bytes) = pure_mode_fixture();
    let outputs = call_pure_mode(&runtime, &slot, &in_bytes);

    for (i, out) in outputs.iter().enumerate().skip(1) {
        assert_eq!(
            &outputs[0], out,
            "iteration {i} diverged from first call — state leaked",
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn tx__pure_mode_rss_stays_under_ceiling() {
    let (runtime, slot, in_bytes) = pure_mode_fixture();
    let baseline_rss = vmrss_kib().expect("read baseline VmRSS");
    let _outputs = call_pure_mode(&runtime, &slot, &in_bytes);
    let final_rss = vmrss_kib().expect("read final VmRSS");
    let growth = final_rss.saturating_sub(baseline_rss);

    assert!(
        growth <= RSS_GROWTH_CEILING_KIB,
        "RSS grew {growth} KiB over {ITERATIONS} pure-mode calls (ceiling {RSS_GROWTH_CEILING_KIB} KiB)",
    );
}
