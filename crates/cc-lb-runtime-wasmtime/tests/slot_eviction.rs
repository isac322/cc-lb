//! RFC-0001 gap-analysis item #1 — slot eviction API.
//!
//! Before this test lands, `WasmtimeRuntime` had no way to remove
//! registered slots. Principal deletion or plugin-chain-entry removal
//! left every compiled `Arc<InstancePre>` + PoolingAllocator memory /
//! core-instance slot occupied for the process lifetime, so:
//!
//!   * `WasmtimeRuntime::slot_count()` only grew.
//!   * After enough hot-swap cycles, `pool_total_memories` /
//!     `pool_total_core_instances` (64 / 64 by default) hit the hard
//!     ceiling and further `register_*` failed.
//!
//! The tests here pin the contract for the new `evict_slot` API:
//!   * evicting a live key returns `true` and drops the slot from
//!     both `get_slot` and `slot_count`;
//!   * evicting a missing key returns `false` and is a no-op (safe
//!     for defense-in-depth calls from the admin delete paths that
//!     may run after a rebuild has already dropped the slot);
//!   * evicting a key while an in-flight caller still holds an
//!     `Arc<LoadedPluginSlot>` clone from an earlier `get_slot` MUST keep
//!     that slot working — the runtime never yanks state out from
//!     under a live dispatch.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::{CachePricingSummary, FilterRequest, Principal, UpstreamCandidate};
use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::{WasmPluginWireDispatch, WasmtimeRuntime};
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
    use cc_lb_plugin_wire::Claim;
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
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: 10,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
        }]),
    }
}

#[test]
fn evict_slot_drops_registration_and_returns_true() {
    let Some(wasm) = cache_aware_wasm() else {
        // Wasm fixture missing on this machine — dispatch is exercised
        // by the workspace lib tests. Skip cleanly rather than fail so
        // the workstation-config path doesn't tank CI-adjacent runs.
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-probe-A");

    rt.register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    assert!(rt.get_slot(&key).is_some(), "slot registered");
    assert_eq!(rt.slot_count(), 1, "slot_count reflects registration");

    let removed = rt.evict_slot(&key);
    assert!(removed, "evict_slot returns true on live key");

    assert!(
        rt.get_slot(&key).is_none(),
        "get_slot returns None post-evict"
    );
    assert_eq!(rt.slot_count(), 0, "slot_count decrements to zero");
}

#[test]
fn evict_slot_is_idempotent_when_key_missing() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine");
    let key = RuntimeSlotKey::global("never-registered");

    let removed = rt.evict_slot(&key);
    assert!(!removed, "evict of absent key returns false");

    // Second call must also be a safe no-op — defense-in-depth callers
    // (admin delete handlers, rebuild-diff loops) may double-fire.
    let removed_again = rt.evict_slot(&key);
    assert!(!removed_again, "second evict is still a no-op");
    assert_eq!(rt.slot_count(), 0);
}

#[test]
fn evict_slot_leaves_prior_arc_dispatchable() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-probe-B");

    let slot = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("register");

    // Caller retained a dispatch handle from register (mimics in-flight
    // dispatch that pulled the Arc via get_slot moments earlier).
    let dispatch = WasmPluginWireDispatch::from_slot(slot, rt.config_arc());
    assert!(rt.evict_slot(&key), "evict_slot returns true");
    assert!(rt.get_slot(&key).is_none(), "map entry gone");

    let req = tiny_filter_request();
    let in_bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
    let out = dispatch
        .call_filter_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("hook still callable");
    assert!(
        !out.is_empty(),
        "filter response is non-empty (cache-aware returns at least one decision)",
    );
}
