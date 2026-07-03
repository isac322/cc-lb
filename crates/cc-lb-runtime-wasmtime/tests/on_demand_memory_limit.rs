use cc_lb_runtime_wasmtime::{
    HostState, HotEngineAllocationStrategy, HotEngineConfig, build_hot_engine,
};
use wasmtime::{Instance, Module, Store};

#[test]
fn on_demand_store_limits_reject_initial_memory_above_configured_cap() {
    let cfg = HotEngineConfig {
        allocation_strategy: HotEngineAllocationStrategy::OnDemand,
        memory_max_pages: 1,
        ..HotEngineConfig::default()
    };
    let engine = build_hot_engine(&cfg).expect("engine builds");
    let mut store = Store::new(&engine, HostState::new(cfg.memory_max_pages));
    store.limiter(|state| state.limits());

    let wat = r#"(module (memory 2))"#;
    let module = Module::new(&engine, wat).expect("compile wat");

    let err = Instance::new(&mut store, &module, &[])
        .expect_err("StoreLimits must reject initial memory above memory_max_pages");
    let message = err.to_string().to_lowercase();
    assert!(
        message.contains("memory") && (message.contains("limit") || message.contains("exceed")),
        "instantiate failure should mention memory limit, got: {message}",
    );
}
