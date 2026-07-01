//! RFC-0001 gap-analysis item #6 — pool saturation observability.
//!
//! Before this test lands, the `PoolingAllocationConfig` limits
//! (`pool_total_memories = 64`, `pool_total_core_instances = 64`) were
//! hidden runtime constants — no way to know how close production
//! traffic was to the ceiling, no distinct error path when a store
//! could not acquire a slot.
//!
//! This test pins:
//!
//!   * The `WasmtimeRuntimeError::PoolSaturated` variant exists (a
//!     compile-time contract that downstream error-handling code can
//!     downcast onto). We reference it via a `matches!(...)` guard
//!     against a synthetic error so the variant becomes part of the
//!     type surface.
//!   * The `cc_lb_plugin_pool_memories_utilization_ratio` and
//!     `cc_lb_plugin_pool_core_instances_utilization_ratio` gauges are
//!     emitted at least once when the runtime surfaces pool telemetry.
//!
//! Deferred to a dedicated stress harness: forcing an actual
//! `PoolConcurrencyLimitError` from wasmtime under contention.

use std::sync::Arc;

use cc_lb_runtime_wasmtime::{WasmtimeRuntime, WasmtimeRuntimeError};
use metrics_exporter_prometheus::PrometheusBuilder;

#[test]
fn pool_saturated_variant_is_part_of_error_surface() {
    // Synthetic construction. If PoolSaturated ever gets removed or
    // renamed, this stops compiling — the point.
    let err = WasmtimeRuntimeError::PoolSaturated {
        resource: "memories",
        limit: 64,
    };
    assert!(matches!(
        &err,
        WasmtimeRuntimeError::PoolSaturated { resource: "memories", limit: 64 }
    ));
    // The Display impl must mention the resource so operators can
    // triage which pool dimension exhausted.
    let msg = err.to_string();
    assert!(msg.contains("memories"), "Display should name resource: {msg}");
}

#[test]
fn pool_utilization_gauges_are_emitted() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    metrics::with_local_recorder(&recorder, || {
        let rt = Arc::new(WasmtimeRuntime::with_defaults().expect("engine"));
        // A dedicated scrape hook publishes pool telemetry on demand;
        // building a runtime without any registered slot must still
        // produce a valid gauge (0.0 utilization).
        rt.publish_pool_metrics();
    });

    let rendered = handle.render();
    assert!(
        rendered.contains("cc_lb_plugin_pool_memories_utilization_ratio"),
        "memories utilization gauge must be emitted; rendered=\n{rendered}",
    );
    assert!(
        rendered.contains("cc_lb_plugin_pool_core_instances_utilization_ratio"),
        "core-instances utilization gauge must be emitted; rendered=\n{rendered}",
    );
}
