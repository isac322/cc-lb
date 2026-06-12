mod common;

use std::collections::BTreeSet;

use cc_lb_plugin_api::{PluginRuntime, RuntimeError};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_runtime_extism::handshake::build_offer;

#[test]
fn router_wire_v1_v2_instantiation_is_removed() {
    let fixture = common::fixture(
        "router-removed",
        r#"(module (func (export "route") (result i32) (i32.const 0)))"#,
        common::metadata(&[]),
    );

    let error = match ExtismRuntime::new().instantiate_router(&fixture.manifest) {
        Ok(_) => panic!("router wire v1/v2 unexpectedly instantiated"),
        Err(error) => error,
    };

    assert_router_removed(error);
}

#[test]
fn host_offer_removes_route_but_keeps_non_router_wire_functions() {
    let offer = build_offer(&BTreeSet::new());

    assert!(!offer.function_versions.contains_key("route"));
    assert!(offer.function_versions.contains_key("shape"));
    assert!(offer.function_versions.contains_key("observe"));
    assert!(offer.function_versions.contains_key("filter"));
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
