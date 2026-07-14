#[path = "cache_aware_wasmtime_e2e.rs"]
mod cache_aware_wasmtime_e2e;
#[path = "compiled_module_cache.rs"]
mod compiled_module_cache;
#[path = "conformance.rs"]
mod conformance;
#[path = "malicious_plugin.rs"]
mod malicious_plugin;
#[path = "observe_drain.rs"]
mod observe_drain;
#[path = "on_demand_memory_limit.rs"]
mod on_demand_memory_limit;
#[path = "plugin_metrics_emission.rs"]
mod plugin_metrics_emission;
#[path = "pool_saturation.rs"]
mod pool_saturation;
#[path = "pure_no_state_leak.rs"]
mod pure_no_state_leak;
#[path = "register_unchanged_short_circuit.rs"]
mod register_unchanged_short_circuit;
#[path = "scoped_dispatch.rs"]
mod scoped_dispatch;
#[path = "shape_round_trip.rs"]
mod shape_round_trip;
#[path = "slot_eviction.rs"]
mod slot_eviction;
