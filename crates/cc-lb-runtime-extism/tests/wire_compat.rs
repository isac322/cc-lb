//! Wire-compat snapshot: v1 router snapshots are no longer accepted by runtime-extism.

mod common;

use std::collections::BTreeMap;

use cc_lb_plugin_api::{PluginManifest, PluginRuntime, RuntimeError};
use serde_json::json;

#[test]
fn wire_compat_round_robin_v1_router_snapshot_is_rejected() {
    let snapshot_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/snapshots/router_round_robin_pre_v2.wasm"
    );

    let manifest = PluginManifest {
        name: "round-robin-compat".to_owned(),
        artifact: snapshot_path.to_owned(),
        wire_version: None,
        config: json!({}),
        metadata: BTreeMap::new(),
    };

    let error = match common::runtime().instantiate_router(&manifest) {
        Ok(_) => panic!("v1/v2 router wire unexpectedly instantiated"),
        Err(error) => error,
    };

    assert_router_removed(error);
}

fn assert_router_removed(error: RuntimeError) {
    match error {
        RuntimeError::InstantiateFailed { reason } => assert!(
            reason.contains("router wire v1/v2 plugins are no longer supported"),
            "unexpected reason: {reason}"
        ),
        other => panic!("unexpected runtime error: {other:?}"),
    }
}
