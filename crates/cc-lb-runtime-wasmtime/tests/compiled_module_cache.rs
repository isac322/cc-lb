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

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, RuntimeSlotRole};

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
fn identical_bytes_share_compiled_artifact_across_principal_slots() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

    let slot_a = rt
        .register_filter(
            RuntimeSlotKey::new(
                "principal-a",
                "cache-aware-wasmtime",
                RuntimeSlotRole::Filter,
            ),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register slot A");
    let slot_b = rt
        .register_filter(
            RuntimeSlotKey::new(
                "principal-b",
                "cache-aware-wasmtime",
                RuntimeSlotRole::Filter,
            ),
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
fn identical_bytes_share_compiled_artifact_across_separate_runtimes() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt_a = WasmtimeRuntime::with_defaults().expect("engine A");
    let rt_b = WasmtimeRuntime::with_defaults().expect("engine B");

    let slot_a = rt_a
        .register_filter(
            RuntimeSlotKey::global("cache-aware-wasmtime", RuntimeSlotRole::Filter),
            "cache-aware-wasmtime",
            &wasm,
        )
        .expect("register on runtime A");
    let slot_b = rt_b
        .register_filter(
            RuntimeSlotKey::global("cache-aware-wasmtime", RuntimeSlotRole::Filter),
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
fn recompile_after_evict_reuses_cached_artifact_instead_of_recompiling() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
    let key = RuntimeSlotKey::global("evict-reregister-cache-probe", RuntimeSlotRole::Filter);

    let slot1 = rt
        .register_filter(key.clone(), "cache-aware-wasmtime", &wasm)
        .expect("first register");
    let instance_pre_before_evict = Arc::clone(&slot1.current.load_full().instance_pre);

    let evicted = rt.retain_slots(&std::collections::HashSet::new());
    assert_eq!(evicted, vec![key.clone()]);

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

#[test]
fn parallel_compilation_does_not_block_cache_hits_or_unrelated_keys() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));

    // Pre-warm key K0.
    let key_k0 = RuntimeSlotKey::global("cache-parallel-k0", RuntimeSlotRole::Filter);
    let _ = rt
        .register_filter(key_k0.clone(), "cache-aware-wasmtime", &wasm)
        .expect("prewarm K0");

    // Mutate custom section on wasm so K1 and K2 get distinct content hashes.
    let mut wasm_k1 = wasm.clone();
    wasm_k1.extend_from_slice(&[0x00, 0x03, 0x01, b'x', b'1']);
    let mut wasm_k2 = wasm;
    wasm_k2.extend_from_slice(&[0x00, 0x03, 0x01, b'x', b'2']);

    let entered_k1 = Arc::new(std::sync::Barrier::new(2));
    let release_k1 = Arc::new(std::sync::Barrier::new(2));
    let entered_clone = Arc::clone(&entered_k1);
    let release_clone = Arc::clone(&release_k1);
    let rt_k1 = Arc::clone(&rt);
    let rt2 = Arc::new(WasmtimeRuntime::with_defaults().expect("engine2"));
    let h = std::thread::spawn(move || {
        entered_clone.wait();
        let res = rt_k1.register_filter(
            RuntimeSlotKey::global("cache-parallel-k1", RuntimeSlotRole::Filter),
            "k1",
            &wasm_k1,
        );
        release_clone.wait();
        res
    });
    entered_k1.wait();
    // Unrelated runtime / key K2 compiles & K0 hits without deadlock while K1 compiles.
    let _ = rt2
        .register_filter(
            RuntimeSlotKey::global("cache-parallel-k2", RuntimeSlotRole::Filter),
            "k2",
            &wasm_k2,
        )
        .expect("K2 compiles");
    release_k1.wait();
    h.join().expect("thread join").expect("K1 compiles");
}
