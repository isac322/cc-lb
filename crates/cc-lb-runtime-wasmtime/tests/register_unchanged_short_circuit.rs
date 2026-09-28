//! RFC-0001 gap-analysis item #2 — skip re-register on unchanged input.
//!
//! Re-registering byte-identical wasm on every reconcile pass must not
//! recompile (wasmparser walk + wasmtime compile + precompile +
//! `instantiate_pre`) when the wasm bytes, memory knobs, and validation
//! policy are unchanged. Repeated compiles would (a) force repeated
//! `instantiate_pre` allocations and (b) periodically nudge the pooling
//! allocator toward its 64-slot ceiling for zero benefit.
//!
//! The contract pinned here: register the same wasm twice on the
//! same slot → the `ArcSwap<PluginCell>` payload is the SAME `Arc`
//! pointer. `Arc::ptr_eq` on the loaded cell is the observable signal
//! for "short-circuited, no rebuild happened" — a fresh compile always
//! produces a fresh `PluginCell` allocation.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;

fn cache_aware_wasm() -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm");
    std::fs::read(path).ok()
}

#[test]
fn register_same_content_reuses_cell() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("unchanged-short-circuit");

    let slot1 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("first register");
    let cell1 = slot1.current.load_full();

    let slot2 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("second register (unchanged)");
    let cell2 = slot2.current.load_full();

    assert!(
        Arc::ptr_eq(&slot1, &slot2),
        "same LoadedPluginSlot Arc returned on unchanged re-register",
    );
    assert!(
        Arc::ptr_eq(&cell1, &cell2),
        "same PluginCell Arc — no rebuild happened",
    );
}
