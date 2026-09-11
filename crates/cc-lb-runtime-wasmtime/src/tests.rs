#![allow(non_snake_case)]

use crate::{
    HotEngineAllocationStrategy, HotEngineConfig, RuntimeSlotKey, WasmtimeRuntime,
    WasmtimeRuntimeError,
};

#[test]
fn t3__engine_build_only() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let _ = rt.engine();
    assert_eq!(rt.slot_count(), 0);
}

#[test]
fn t3__pooling_engine_builds_when_selected() {
    let rt = WasmtimeRuntime::new(HotEngineConfig {
        allocation_strategy: HotEngineAllocationStrategy::Pooling,
        ..HotEngineConfig::default()
    })
    .expect("pooling engine build");

    let _ = rt.engine();
    assert_eq!(rt.slot_count(), 0);
}

#[test]
fn t3__missing_slot_returns_error() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let err = rt
        .call_filter(&RuntimeSlotKey::global("nonexistent"), &[])
        .expect_err("must fail on missing slot");
    assert!(matches!(err, WasmtimeRuntimeError::ModuleRejected { .. }));
}
