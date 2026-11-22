use crate::{
    HotEngineAllocationStrategy, HotEngineConfig, RuntimeSlotKey, RuntimeSlotRole, WasmtimeRuntime,
};

#[test]
fn engine_build_only() {
    let rt = WasmtimeRuntime::with_defaults().expect("engine build");
    let _ = rt.engine();
    assert!(
        rt.get_slot(&RuntimeSlotKey::global(
            "nonexistent",
            RuntimeSlotRole::Filter
        ))
        .is_none()
    );
}

#[test]
fn pooling_engine_builds_when_selected() {
    let rt = WasmtimeRuntime::new(HotEngineConfig {
        allocation_strategy: HotEngineAllocationStrategy::Pooling,
        ..HotEngineConfig::default()
    })
    .expect("pooling engine build");

    let _ = rt.engine();
    assert!(
        rt.get_slot(&RuntimeSlotKey::global(
            "nonexistent",
            RuntimeSlotRole::Filter
        ))
        .is_none()
    );
}
