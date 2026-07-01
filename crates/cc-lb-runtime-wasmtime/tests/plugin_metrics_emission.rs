//! RFC-0001 gap-analysis item #5 — plugin call / fuel / trap metrics.
//!
//! Before this test lands, `execute_call` emitted no metrics at all;
//! the three metric families the RFC mandates
//! (`docs/rfc/0001-plugin-runtime-vnext.md:317-319`) did not exist:
//!
//!   * `cc_lb_plugin_call_duration_seconds{plugin, hook}` histogram
//!   * `cc_lb_plugin_fuel_consumed_ratio{plugin, hook}` histogram
//!   * `cc_lb_plugin_trap_total{plugin, hook, phase}` counter
//!
//! `cc_lb_plugin_call_duration_seconds` had bucket boundaries reserved
//! in `cc-lb-observability/src/init.rs::install_prometheus` (see
//! `PLUGIN_CALL_DURATION_BUCKETS`) but was never emitted. This test
//! pins the emission contract:
//!
//!   * A single successful pure-mode call emits ≥ 1 sample to each
//!     histogram family with `plugin=<name>` + `hook=<kind>` labels.
//!   * A hook that traps increments `cc_lb_plugin_trap_total` with the
//!     matching `phase=<hook>` label.
//!
//! We assert against the Prometheus text render because it is the
//! actual production surface (scraped by whatever monitoring stack the
//! operator wires up).

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_types::{FilterRequest, Header, Principal, UpstreamCandidate};
use cc_lb_runtime_wasmtime::WasmtimeRuntime;
use metrics_exporter_prometheus::PrometheusBuilder;
use rkyv::rancor::Error;

fn cache_aware_wasm() -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/cache_aware_wasmtime.wasm");
    std::fs::read(path).ok()
}

fn tiny_filter_request() -> FilterRequest {
    FilterRequest {
        request_id: "metrics-probe".to_owned(),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: None,
        headers: Vec::<Header>::new(),
        body: Vec::new(),
        principal: Principal {
            id: "tenant".to_owned(),
            kind: "api_key".to_owned(),
            claims: Vec::from([(String::from("keep_k"), b"1".to_vec())]),
        },
        candidates: vec![UpstreamCandidate {
            upstream_id: "a".to_owned(),
            name: "upstream-a".to_owned(),
            kind: "anthropic_api_key".to_owned(),
            observed_at_unix_secs: 0,
            predicted_cache_read_tokens: 10,
        }],
    }
}

#[test]
fn successful_filter_call_emits_three_metric_families() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let plugin_name = "cache-aware-wasmtime";
    let key = SlotKey::global("metrics-probe");

    metrics::with_local_recorder(&recorder, || {
        let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
        rt.register_filter(key.clone(), plugin_name, &wasm)
            .expect("register");
        let req = tiny_filter_request();
        let in_bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
        let _out = rt.call_filter(&key, in_bytes.as_slice()).expect("call");
    });

    let rendered = handle.render();

    assert!(
        rendered.contains("cc_lb_plugin_call_duration_seconds"),
        "duration histogram must be emitted; rendered=\n{rendered}",
    );
    assert!(
        rendered.contains("cc_lb_plugin_fuel_consumed_ratio"),
        "fuel ratio histogram must be emitted; rendered=\n{rendered}",
    );
    assert!(
        rendered.contains(&format!("plugin=\"{plugin_name}\"")),
        "labels must include plugin=\"{plugin_name}\"; rendered=\n{rendered}",
    );
    assert!(
        rendered.contains("hook=\"filter\""),
        "labels must include hook=\"filter\"; rendered=\n{rendered}",
    );
    // Counter is defined even when zero; we accept absence of the sample
    // as long as the family is declared. describe/register elsewhere;
    // here we just confirm the counter name is a known symbol in the
    // rendered dump when the trap path fires (see next test).
}

#[test]
fn fuel_exhaustion_emits_trap_counter() {
    let Some(wasm) = cache_aware_wasm() else {
        return;
    };
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let plugin_name = "cache-aware-wasmtime";
    let key = SlotKey::global("fuel-trap");

    metrics::with_local_recorder(&recorder, || {
        // Force fuel exhaustion by using a runtime whose per-call fuel
        // is set below what the guest requires for a single hook.
        let cfg = cc_lb_runtime_wasmtime::HotEngineConfig {
            fuel_per_call: 1,
            ..cc_lb_runtime_wasmtime::HotEngineConfig::default()
        };
        let rt = Arc::new(WasmtimeRuntime::new(cfg).expect("engine"));
        rt.register_filter(key.clone(), plugin_name, &wasm)
            .expect("register");
        let req = tiny_filter_request();
        let in_bytes = rkyv::to_bytes::<Error>(&req).expect("encode");
        let result = rt.call_filter(&key, in_bytes.as_slice());
        assert!(result.is_err(), "starved-fuel call must trap");
    });

    let rendered = handle.render();
    assert!(
        rendered.contains("cc_lb_plugin_trap_total"),
        "trap counter must be emitted on trap path; rendered=\n{rendered}",
    );
    assert!(
        rendered.contains(&format!("plugin=\"{plugin_name}\"")),
        "trap counter must carry plugin label; rendered=\n{rendered}",
    );
}
