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
fn run_sends_priority_service_tier_to_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("wasmtime_filter_service_tier.wasm"))
        .expect("runtime build script produces filter service-tier fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn run_exercises_cache_aware_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("cache_aware_wasmtime.wasm"))
        .expect("runtime build script produces cache-aware filter fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn quota_v2_owned_and_view_pass_conformance() {
    for artifact in [
        "wasmtime_filter_quota_v2.wasm",
        "wasmtime_filter_quota_v2_view.wasm",
    ] {
        let wasm = std::fs::read(wasm_fixture_path(artifact))
            .expect("runtime build script produces V2 filter fixtures");
        let suite = ConformanceSuite::for_filter(&wasm);
        suite.run();
        let response = suite
            .session()
            .call_filter_v2(super::fixtures::sample_filter_request_v2());
        assert!(response.results.is_empty());
    }
}
