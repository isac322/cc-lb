//! RFC-0001 gap-analysis item #3 — expose `memory_max_pages` config knob.
//!
//! The wasmtime hot-engine default (`HotEngineConfig::memory_max_pages`
//! in `cc-lb-runtime-wasmtime`) is a workspace-level compile-time
//! constant. Operators need a runtime override so they can raise or
//! lower the wasm memory ceiling per deployment without patching the
//! server binary.
//!
//! Contract pinned:
//!
//!   * `[runtime.wasmtime]` section is optional; when absent, no
//!     override is applied (engine default wins).
//!   * `memory_max_pages` is an `Option<u32>`; `None` is the safe
//!     "use engine default" value.
//!   * `memory_max_pages` outside `1..=65_536` is rejected (65_536 =
//!     wasm32 memory cap of 4 GiB; 0 pages is nonsensical).

use cc_lb_config::{Config, WasmtimeAllocationStrategy};

fn parse(toml_str: &str) -> Config {
    toml::from_str::<Config>(toml_str).expect("parse succeeds")
}

#[test]
fn default_config_uses_ondemand_without_memory_overrides() {
    let cfg = Config::default();
    assert_eq!(
        cfg.runtime.wasmtime.allocation_strategy,
        WasmtimeAllocationStrategy::OnDemand,
    );
    assert!(
        cfg.runtime.wasmtime.memory_max_pages.is_none(),
        "no default override — engine default is authoritative",
    );
}

#[test]
fn wasmtime_memory_max_pages_parses_from_toml() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
memory_max_pages = 2048
"#,
    );
    assert_eq!(cfg.runtime.wasmtime.memory_max_pages, Some(2048));
}

#[test]
fn wasmtime_allocation_strategy_ondemand_alias_parses_from_toml() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
allocation_strategy = "on_demand"
"#,
    );

    assert_eq!(
        cfg.runtime.wasmtime.allocation_strategy,
        WasmtimeAllocationStrategy::OnDemand,
    );
}

#[test]
fn wasmtime_memory_pool_and_reservation_knobs_parse_from_toml() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
allocation_strategy = "pooling"
pool_total_memories = 8
pool_total_core_instances = 12
memory_reservation_bytes = 268435456
memory_guard_bytes = 67108864
"#,
    );

    assert_eq!(
        cfg.runtime.wasmtime.allocation_strategy,
        WasmtimeAllocationStrategy::Pooling,
    );
    assert_eq!(cfg.runtime.wasmtime.pool_total_memories, Some(8));
    assert_eq!(cfg.runtime.wasmtime.pool_total_core_instances, Some(12));
    assert_eq!(
        cfg.runtime.wasmtime.memory_reservation_bytes,
        Some(268_435_456)
    );
    assert_eq!(cfg.runtime.wasmtime.memory_guard_bytes, Some(67_108_864));
}

#[test]
fn wasmtime_invalid_allocation_strategy_is_rejected_by_deserialize() {
    let err = toml::from_str::<Config>(
        r#"
[runtime.wasmtime]
allocation_strategy = "prewarm_everything"
"#,
    )
    .expect_err("unknown allocation strategy must be rejected");

    assert!(
        err.to_string().contains("allocation_strategy"),
        "error mentions the offending field: {err}",
    );
}

#[test]
fn wasmtime_pool_zero_is_rejected_by_validation() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
pool_total_memories = 0
"#,
    );
    let err = cfg
        .validate()
        .expect_err("zero pooled memories must be rejected");
    assert!(
        err.to_string().contains("pool_total_memories"),
        "error mentions the offending field: {err}",
    );
}

#[test]
fn wasmtime_memory_reservation_below_max_memory_is_rejected_by_validation() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
memory_max_pages = 2048
memory_reservation_bytes = 67108864
"#,
    );
    let err = cfg
        .validate()
        .expect_err("reservation below max memory must be rejected");
    assert!(
        err.to_string().contains("memory_reservation_bytes"),
        "error mentions the offending field: {err}",
    );
}

#[test]
fn wasmtime_memory_guard_below_floor_is_rejected_by_validation() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
memory_guard_bytes = 65536
"#,
    );
    let err = cfg.validate().expect_err("tiny guard must be rejected");
    assert!(
        err.to_string().contains("memory_guard_bytes"),
        "error mentions the offending field: {err}",
    );
}

#[test]
fn wasmtime_memory_max_pages_zero_is_rejected_by_validation() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
memory_max_pages = 0
"#,
    );
    let err = cfg.validate().expect_err("zero pages must be rejected");
    assert!(
        err.to_string().contains("memory_max_pages"),
        "error mentions the offending field: {err}",
    );
}

#[test]
fn wasmtime_memory_max_pages_above_wasm32_ceiling_is_rejected() {
    let cfg = parse(
        r#"
[runtime.wasmtime]
memory_max_pages = 65537
"#,
    );
    let err = cfg
        .validate()
        .expect_err("above 4 GiB (65_536 pages) must be rejected");
    assert!(
        err.to_string().contains("memory_max_pages"),
        "error mentions the offending field: {err}",
    );
}
