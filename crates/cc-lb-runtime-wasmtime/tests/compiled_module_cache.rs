//! Compiled-module cache — content-addressed reuse across principal slots.
//!
//! Prod jemalloc profile root cause: `cranelift_codegen::machinst::compile`
//! accounted for ~947 MB of cumulative churn because compiled artifacts were
//! keyed per-principal-slot only. The SAME wasm bytes, shared by many
//! principals (or re-registered after a warmup-slot `retain_slots`
//! eviction), were fed to cranelift again on every principal/slot. The
//! process-wide cache in `crate::compiled_cache` keys by wasm bytes +
//! validation-policy version + engine/config identity, so identical
//! content compiles once and every slot reuses the same `Arc<InstancePre>`.

use std::sync::Arc;

use crate::support::required_wasm;
use cc_lb_runtime_wasmtime::RuntimeSlotKey;
use cc_lb_runtime_wasmtime::WasmtimeRuntime;

#[test]
fn t3__identical_bytes_share_compiled_artifact_across_principal_slots() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

    let slot_a = rt
        .register_filter(
            RuntimeSlotKey::new("principal-a", "cache-aware-wasmtime"),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register slot A");
    let slot_b = rt
        .register_filter(
            RuntimeSlotKey::new("principal-b", "cache-aware-wasmtime"),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register slot B");

    let cell_a = slot_a.current.load_full();
    let cell_b = slot_b.current.load_full();

    assert!(
        Arc::ptr_eq(&cell_a.instance_pre, &cell_b.instance_pre),
        "byte-identical plugin registered under two different principal slots \
         must reuse the same compiled InstancePre — no duplicate cranelift compile",
    );
}

#[test]
fn t3__identical_bytes_share_compiled_artifact_across_separate_runtimes() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");
    let rt_a = WasmtimeRuntime::with_defaults().expect("engine A");
    let rt_b = WasmtimeRuntime::with_defaults().expect("engine B");

    let slot_a = rt_a
        .register_filter(
            RuntimeSlotKey::global("cache-aware-wasmtime"),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register on runtime A");
    let slot_b = rt_b
        .register_filter(
            RuntimeSlotKey::global("cache-aware-wasmtime"),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register on runtime B");

    let cell_a = slot_a.current.load_full();
    let cell_b = slot_b.current.load_full();

    assert!(
        Arc::ptr_eq(&cell_a.instance_pre, &cell_b.instance_pre),
        "the compiled-module cache is process-wide: two independent \
         WasmtimeRuntime/Engine instances with compatible config must reuse \
         one compiled artifact",
    );
}

#[test]
fn t3__recompile_after_evict_reuses_cached_artifact_instead_of_recompiling() {
    let wasm = required_wasm("cache_aware_wasmtime.wasm");
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-reregister-cache-probe");

    let slot1 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("first register");
    let instance_pre_before_evict = Arc::clone(&slot1.current.load_full().instance_pre);

    assert!(rt.evict_slot(&key), "evict registered slot");

    let slot2 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("re-register after eviction");
    let instance_pre_after_reregister = Arc::clone(&slot2.current.load_full().instance_pre);

    assert!(
        Arc::ptr_eq(&instance_pre_before_evict, &instance_pre_after_reregister),
        "re-registering byte-identical content after a warmup-slot eviction \
         must hit the compiled-module cache, not recompile",
    );
}
