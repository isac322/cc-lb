//! Wire-compat snapshot: locks v1 wire backward-compat for round-robin.
//!
//! This test loads a committed snapshot of the round-robin .wasm plugin and invokes its `route`
//! function to verify it can still deserialize v1 wire types and return a well-formed v1 RouteResponse.
//!
//! **CRITICAL**: T8 onwards (T8-T11 task tasks involving wire evolution, compression, or v2 migration)
//! MUST NOT break this test. If this test fails after a change, it signals a wire-shape break.
//! Escalate immediately; do not regenerate the snapshot without explicit approval.
//!
//! The snapshot is committed to `tests/snapshots/router_round_robin_pre_v2.wasm` and is treated
//! as a baseline immutable artifact. Regeneration is only done when the wire protocol officially
//! changes versions (e.g., v1 → v2), not for plugin improvements or internal refactors.

mod common;

use std::collections::BTreeMap;

use cc_lb_plugin_api::{PluginManifest, PluginRuntime, Upstream};
use cc_lb_runtime_extism::ExtismRuntime;
use serde_json::json;

/// Loads the pre-v2 round-robin snapshot and verifies route() still works with v1 wire.
#[test]
fn wire_compat_round_robin_unchanged_v1() {
    let snapshot_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/snapshots/router_round_robin_pre_v2.wasm"
    );

    let manifest = PluginManifest {
        name: "round-robin-compat".to_owned(),
        artifact: snapshot_path.to_owned(),
        config: json!({}),
        metadata: BTreeMap::new(),
    };

    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&manifest)
        .expect("router instantiates from snapshot");

    // Build a minimal v1 RouteRequest with one candidate.
    let route_response = router
        .route(&common::ctx(), &common::principal(), &[common::candidate_wire()])
        .expect("route() invoked successfully");

    // Verify the RouteResponse is well-formed:
    // - upstream_id is Some (because we provided one candidate)
    // - upstream is AnthropicDirect (round-robin's default)
    assert!(
        route_response.upstream_id.is_some(),
        "route response must have upstream_id for non-empty candidates"
    );
    let upstream_id = route_response.upstream_id.as_ref().unwrap();
    assert_eq!(
        upstream_id,
        &common::candidate_wire().upstream_id,
        "upstream_id should match the input candidate"
    );
    assert!(
        matches!(route_response.upstream, Upstream::AnthropicDirect),
        "upstream must be AnthropicDirect"
    );
}

/// Verify route() returns None when given zero candidates (fallback behavior).
#[test]
fn wire_compat_round_robin_no_candidates_v1() {
    let snapshot_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/snapshots/router_round_robin_pre_v2.wasm"
    );

    let manifest = PluginManifest {
        name: "round-robin-compat-empty".to_owned(),
        artifact: snapshot_path.to_owned(),
        config: json!({}),
        metadata: BTreeMap::new(),
    };

    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&manifest)
        .expect("router instantiates from snapshot");

    // Invoke route with empty candidate list.
    let route_response = router
        .route(&common::ctx(), &common::principal(), &[])
        .expect("route() with empty candidates succeeds");

    // Verify fallback behavior: upstream_id is None, but upstream is still set.
    assert_eq!(
        route_response.upstream_id, None,
        "route response must have None upstream_id for empty candidates"
    );
    assert!(
        matches!(route_response.upstream, Upstream::AnthropicDirect),
        "upstream must still be AnthropicDirect in fallback"
    );
}
