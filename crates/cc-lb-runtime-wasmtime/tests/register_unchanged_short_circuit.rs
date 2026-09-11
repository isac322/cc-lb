//! RFC-0001 gap-analysis item #2 — skip re-register on unchanged input.
//!
//! Before this test lands, `WasmtimeRuntime::register` unconditionally
//! called `compile_module` (wasmparser walk + wasmtime compile +
//! precompile + `instantiate_pre`) and then, on every reconcile pass,
//! bumped `PluginCell::version_id` by one even when the wasm bytes,
//! memory knobs, and validation policy were byte-identical. That
//! churn (a) forced repeated `instantiate_pre` allocations and (b)
//! periodically nudged the pooling allocator toward its 64-slot
//! ceiling for zero benefit.
//!
//! The contract pinned here: register the same wasm twice on the
//! same slot → the `ArcSwap<PluginCell>` payload is the SAME `Arc`
//! pointer and `version_id` is stable. `Arc::ptr_eq` on the loaded
//! cell is the observable signal for "short-circuited, no rebuild
//! happened" — a fresh compile always produces a fresh `PluginCell`
//! allocation.

use std::sync::Arc;

use crate::support::required_wasm;
use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;

#[test]
fn t3__register_same_content_reuses_cell() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("unchanged-short-circuit");

    let slot1 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("first register");
    let cell1 = slot1.current.load_full();
    let v1 = cell1.version_id;

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
    assert_eq!(cell2.version_id, v1, "version_id stable on unchanged input");
}
