use std::path::PathBuf;

use super::ConformanceSuite;

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
fn run_sends_priority_service_tier_when_filter_declares_v2() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("wasmtime_filter_v2.wasm"))
        .expect("runtime build script produces filter V2 fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn run_keeps_v1_request_when_filter_declares_v1() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("cache_aware_wasmtime.wasm"))
        .expect("runtime build script produces filter V1 fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}
