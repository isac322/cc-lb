//! RFC-0001 gap-analysis item #2 — skip re-register on unchanged input.
//!
//! Before this test lands, `WasmtimeRuntime::register` unconditionally
//! called `compile_module` (wasmparser walk + wasmtime compile +
//! precompile + `instantiate_pre`) and then, on every reconcile pass,
//! bumped `PluginCell::version_id` by one even when the wasm bytes,
//! fuel/memory/pure knobs, and validation policy were byte-identical.
//! That churn (a) invalidated per-worker caches, (b) forced repeated
//! `instantiate_pre` allocations, and (c) periodically nudged the
//! pooling allocator toward its 64-slot ceiling for zero benefit.
//!
//! The contract pinned here:
//!   * Register the same wasm + same `RegisterOptions` twice → the
//!     `ArcSwap<PluginCell>` payload is the SAME `Arc` pointer.
//!     `version_id` therefore is stable.
//!   * Flip `RegisterOptions::pure` on the second call → the payload
//!     Arc changes and `version_id` increments.
//!   * Register a different wasm → payload Arc changes and
//!     `version_id` increments.
//!
//! `Arc::ptr_eq` on the loaded cell is the observable signal for
//! "short-circuited, no rebuild happened" — a fresh compile always
//! produces a fresh `PluginCell` allocation.

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;
use cc_lb_runtime_wasmtime::{RegisterOptions, WasmtimeRuntime};

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
    let key = SlotKey::global("unchanged-short-circuit");

    let slot1 = rt
        .register_filter_with(
            key.clone(),
            "cache-aware-wasmtime",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("first register");
    let cell1 = slot1.current.load_full();
    let v1 = cell1.version_id;

    let slot2 = rt
        .register_filter_with(
            key.clone(),
            "cache-aware-wasmtime",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("second register (unchanged)");
    let cell2 = slot2.current.load_full();

    assert!(
        Arc::ptr_eq(&slot1, &slot2),
        "same PluginSlot Arc returned on unchanged re-register",
    );
    assert!(
        Arc::ptr_eq(&cell1, &cell2),
        "same PluginCell Arc — no rebuild happened",
    );
    assert_eq!(cell2.version_id, v1, "version_id stable on unchanged input");
}

#[test]
fn register_flipping_pure_bumps_version() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = SlotKey::global("pure-flip");

    let slot1 = rt
        .register_filter_with(
            key.clone(),
            "cache-aware-wasmtime",
            &wasm,
            RegisterOptions { pure: true },
        )
        .expect("first register (pure)");
    let cell1 = slot1.current.load_full();
    let v1 = cell1.version_id;

    let slot2 = rt
        .register_filter_with(
            key.clone(),
            "cache-aware-wasmtime",
            &wasm,
            RegisterOptions { pure: false },
        )
        .expect("second register (stateful)");
    let cell2 = slot2.current.load_full();

    assert!(
        !Arc::ptr_eq(&cell1, &cell2),
        "PluginCell Arc changes when pure flag flips",
    );
    assert!(
        cell2.version_id > v1,
        "version_id bumps ({v1} -> {})",
        cell2.version_id,
    );
    assert!(!cell2.pure, "new cell reflects new pure=false");
}
