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

use cc_lb_config::Config;

fn parse(toml_str: &str) -> Config {
    toml::from_str::<Config>(toml_str).expect("parse succeeds")
}

#[test]
fn default_config_leaves_wasmtime_override_absent() {
    let cfg = Config::default();
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
