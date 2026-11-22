//! Confirm hook dispatch drops per-call wasm state.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::v2::{CachePricingSummary, Claim, Principal, UpstreamCandidate};
use cc_lb_plugin_wire::v3::{FilterRequest, NativeCachePricing, NativeCachePricingProvider};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
};
use rkyv::rancor::Error;

const ITERATIONS: usize = 10;
const RSS_GROWTH_CEILING_KIB: u64 = 8 * 1024;

fn wasm_path() -> PathBuf {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    workspace_root.join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm")
}

fn request() -> FilterRequest {
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
        candidates: Box::new([
            UpstreamCandidate {
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
            },
            UpstreamCandidate {
                upstream_id: Box::from("b"),
                name: Box::from("upstream-b"),
                kind: Box::from("anthropic_api_key"),
                base_url: None,
                observed_at_unix_secs: 0,
                predicted_cache_read_tokens: 200,
                predicted_cache_creation_tokens_5m: 0,
                predicted_cache_creation_tokens_1h: 0,
                predicted_uncached_input_tokens: 0,
                plan_capacity_ratio: 1.0,
                organization_type: Box::from(""),
                rate_limit_tier: Box::from(""),
                seat_tier: Box::from(""),
                subscription_quotas: Box::new([]),
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

#[cfg(not(target_os = "linux"))]
fn vmrss_kib() -> std::io::Result<u64> {
    Ok(0)
}

#[test]
fn dispatch_does_not_accumulate_state() {
    let wasm = match std::fs::read(wasm_path()) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let slot_key = RuntimeSlotKey::global("state-leak-probe", RuntimeSlotRole::Filter);
    let slot = runtime
        .register_filter(slot_key, "cache-aware-wasmtime", &wasm)
        .expect("register filter");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let req = request();
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("encode");

    let baseline_rss = vmrss_kib().unwrap_or(0);
    let mut outputs: Vec<Vec<u8>> = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let bytes = dispatch
            .call_filter_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
            .expect("call");
        outputs.push(bytes);
    }
    let final_rss = vmrss_kib().unwrap_or(0);

    for (i, out) in outputs.iter().enumerate().skip(1) {
        assert_eq!(
            &outputs[0], out,
            "iteration {i} diverged from first call — state leaked",
        );
    }

    if baseline_rss > 0 {
        let growth = final_rss.saturating_sub(baseline_rss);
        assert!(
            growth <= RSS_GROWTH_CEILING_KIB,
            "RSS grew {growth} KiB over {ITERATIONS} calls (ceiling {RSS_GROWTH_CEILING_KIB} KiB)",
        );
    }
}
