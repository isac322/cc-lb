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

#[test]
fn ticks_for_timeout_includes_partial_leading_tick() {
    use std::time::Duration;
    assert_eq!(crate::epoch::ticks_for_timeout(Duration::from_millis(1)), 2);
    assert_eq!(
        crate::epoch::ticks_for_timeout(Duration::from_millis(10)),
        2
    );
    assert_eq!(
        crate::epoch::ticks_for_timeout(Duration::from_millis(11)),
        3
    );
    assert_eq!(
        crate::epoch::ticks_for_timeout(Duration::from_millis(1_000)),
        crate::epoch::DEFAULT_EPOCH_DEADLINE_TICKS
    );
    assert_eq!(crate::epoch::DEFAULT_EPOCH_DEADLINE_TICKS, 101);
}

#[test]
fn is_pool_saturated_detects_direct_and_probe_wrapped_saturation() {
    use crate::WasmtimeRuntimeError;

    assert!(WasmtimeRuntimeError::PoolSaturated { resource: "pool" }.is_pool_saturated());
    assert!(
        WasmtimeRuntimeError::ProbeFailed {
            hook: "shape",
            reason: "plugin runtime saturated: pool limit reached".to_owned(),
        }
        .is_pool_saturated()
    );
    assert!(
        !WasmtimeRuntimeError::ProbeFailed {
            hook: "shape",
            reason: "guest trap during shape".to_owned(),
        }
        .is_pool_saturated()
    );
    assert!(
        !WasmtimeRuntimeError::ModuleRejected {
            reason: "bad wasm".to_owned(),
        }
        .is_pool_saturated()
    );
}

#[test]
fn is_epoch_interrupt_detects_trap_instantiate_and_probe_variants() {
    use crate::WasmtimeRuntimeError;

    assert!(
        WasmtimeRuntimeError::GuestTrap {
            phase: "filter",
            source: anyhow::Error::new(wasmtime::Trap::Interrupt),
        }
        .is_epoch_interrupt()
    );
    assert!(
        WasmtimeRuntimeError::InstantiateFailed(
            anyhow::Error::new(wasmtime::Trap::Interrupt).context("start function trapped"),
        )
        .is_epoch_interrupt()
    );
    assert!(
        WasmtimeRuntimeError::ProbeFailed {
            hook: "shape",
            reason: "failed to instantiate module: wasm trap: interrupt".to_owned(),
        }
        .is_epoch_interrupt()
    );
    assert!(
        WasmtimeRuntimeError::ProbeFailed {
            hook: "filter",
            reason: "guest trap during filter: wasm trap: interrupt".to_owned(),
        }
        .is_epoch_interrupt()
    );
    assert!(
        !WasmtimeRuntimeError::InstantiateFailed(anyhow::anyhow!("missing import"))
            .is_epoch_interrupt()
    );
}
