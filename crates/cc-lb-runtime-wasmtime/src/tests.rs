use cc_lb_plugin_api::SlotKey;

use crate::{HotEngineAllocationStrategy, HotEngineConfig, WasmtimeRuntime, WasmtimeRuntimeError};

#[test]
fn engine_build_only() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let _ = rt.engine();
    assert_eq!(rt.slot_count(), 0);
}

#[test]
fn pooling_engine_builds_when_selected() {
    let rt = WasmtimeRuntime::new(HotEngineConfig {
        allocation_strategy: HotEngineAllocationStrategy::Pooling,
        ..HotEngineConfig::default()
    })
    .expect("pooling engine build");

    let _ = rt.engine();
    assert_eq!(rt.slot_count(), 0);
}

#[test]
fn missing_slot_returns_error() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let err = rt
        .call_filter(&SlotKey::global("nonexistent"), &[])
        .expect_err("must fail on missing slot");
    assert!(matches!(err, WasmtimeRuntimeError::ModuleRejected { .. }));
}

#[test]
fn missing_response_transform_slots_return_errors() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let slot_key = SlotKey::global("nonexistent-transform");

    let err = rt
        .call_transform_response(&slot_key, &[])
        .expect_err("must fail on missing transform_response slot");
    assert!(matches!(err, WasmtimeRuntimeError::ModuleRejected { .. }));

    let err = rt
        .call_transform_sse_event(&slot_key, &[])
        .expect_err("must fail on missing transform_sse_event slot");
    assert!(matches!(err, WasmtimeRuntimeError::ModuleRejected { .. }));
}
