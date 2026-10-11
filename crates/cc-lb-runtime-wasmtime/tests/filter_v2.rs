use cc_lb_plugin_wire::schema::{HookKind, WireVersion};
use cc_lb_plugin_wire::{v1, v2};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmtimeRuntime, inspect_wasm};
use rkyv::rancor::Error;

fn fixture(name: &str) -> Vec<u8> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    std::fs::read(
        root.join("target/wasm32-unknown-unknown/release")
            .join(name),
    )
    .expect("V2 wasm fixtures must be built by the runtime build script")
}

fn request() -> v2::FilterRequest {
    v2::FilterRequest {
        routing_now_unix_secs: 1_800_000_000,
        request_id: Box::from("v2-test"),
        thread_id: Some(Box::from("thread")),
        service_tier: Some(Box::from("priority")),
        canonical_model_id: Box::from("claude-test"),
        cache_pricing: v1::CachePricingSummary {
            status: Box::from("unknown"),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([]),
        body: Box::new([]),
        principal: v1::Principal {
            id: Box::from("test-principal"),
            kind: Box::from("internal_key"),
            claims: Box::new([]),
        },
        candidates: Box::new([v2::UpstreamCandidate {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            name: Box::from("subscription"),
            kind: Box::from("anthropic_oauth"),
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: 128,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            plan_capacity_ratio: 1.0,
            organization_type: Box::from(""),
            rate_limit_tier: Box::from(""),
            seat_tier: Box::from(""),
            subscription_quotas: Box::new([v2::QuotaWindowSnapshot {
                window: Box::from("7d"),
                state: Box::from("fresh"),
                utilization: Some(0.25),
                resets_at_unix_secs: Some(1_700_604_800),
                observed_at_unix_millis: Some(1_700_000_000_000),
                status: Some(Box::from("allowed")),
                disabled_reason: None,
                max_staleness_secs: 300,
            }]),
        }]),
    }
}

#[test]
fn v2_owned_and_view_guests_pass_admission_and_read_quota_arrays() {
    for artifact in [
        "wasmtime_filter_quota_v2.wasm",
        "wasmtime_filter_quota_v2_view.wasm",
    ] {
        let wasm = fixture(artifact);
        let inspection = inspect_wasm(HookKind::Filter, &wasm).expect("inspect V2");
        assert_eq!(inspection.hook_versions[&HookKind::Filter], WireVersion::V2);
        let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
        let slot = RuntimeSlotKey::global(artifact);
        runtime
            .register_filter(slot.clone(), "quota-fixture", &wasm)
            .expect("V2 admission probe");
        let request = request();
        let bytes = rkyv::to_bytes::<Error>(&request).expect("encode V2");
        let output = runtime.call_filter(&slot, &bytes).expect("V2 dispatch");
        let response = rkyv::access::<v1::ArchivedFilterResponse, Error>(&output)
            .expect("unchanged response layout");
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            &*response.results[0].upstream_id,
            &*request.candidates[0].upstream_id
        );
        assert_eq!(&*response.results[0].reason, "quota_windows=1");
    }
}
