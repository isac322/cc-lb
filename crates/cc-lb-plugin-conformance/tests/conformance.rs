use std::path::PathBuf;

use cc_lb_plugin_conformance::ConformanceSuite;

fn wasm_fixture_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release")
        .join(file_name)
}

#[test]
fn t3__run_sends_priority_service_tier_to_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("wasmtime_filter_service_tier.wasm"))
        .expect("runtime build script produces filter service-tier fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn t3__run_exercises_cache_aware_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("cache_aware_wasmtime.wasm"))
        .expect("runtime build script produces cache-aware filter fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}
