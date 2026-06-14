mod common;

use std::collections::{BTreeMap, BTreeSet};

use cc_lb_plugin_api::RuntimeError;
use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::handshake::{HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept};
use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, PluginIdentity};
use cc_lb_runtime_extism::ExtismRuntime;
use uuid::Uuid;

#[test]
fn filter_instantiation_rejects_chain_wire_version_mismatch() {
    let mut fixture = common::fixture(
        "filter-wire-mismatch",
        &common::module_with_functions(&[("filter", r#"{"_v":1,"results":[]}"#)]),
        BTreeMap::from([(
            "augmented_metadata".to_owned(),
            serde_json::to_value(augmented_metadata_with_filter_version(1)).unwrap(),
        )]),
    );
    fixture.manifest.wire_version = Some(3);
    let runtime = ExtismRuntime::new();

    let error = match runtime.instantiate_filter_for(
        "principal-test",
        Uuid::from_u128(0x55555555555555555555555555555555),
        "filter-wire-mismatch",
        &fixture.manifest,
    ) {
        Ok(_) => panic!("wire-version mismatch unexpectedly instantiated"),
        Err(error) => error,
    };

    match error {
        RuntimeError::InstantiateFailed { reason } => {
            assert!(
                reason.contains("wire version mismatch"),
                "reason should identify wire mismatch, got: {reason}"
            );
            assert!(
                reason.contains("filter"),
                "reason should identify filter hook, got: {reason}"
            );
            assert!(
                reason.contains("requested 3"),
                "reason should include chain-requested version, got: {reason}"
            );
            assert!(
                reason.contains("negotiated 1"),
                "reason should include handshake-negotiated version, got: {reason}"
            );
        }
        other => panic!("unexpected runtime error: {other:?}"),
    }
}

fn augmented_metadata_with_filter_version(version: u32) -> AugmentedMetadata {
    let accept = HandshakeAccept {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: 1,
        chosen_versions: BTreeMap::from([("filter".to_owned(), version)]),
        plugin_supported: BTreeMap::from([("filter".to_owned(), vec![version])]),
        implemented_functions: BTreeSet::from(["filter".to_owned()]),
        required_capabilities: BTreeSet::new(),
    };

    AugmentedMetadata::from_handshake_and_self_check(
        PluginIdentity {
            magic: CC_LB_PLUGIN_MAGIC,
            abi_envelope: accept.envelope_version,
            plugin_name: "filter-wire-mismatch".to_owned(),
            plugin_version: "1.0.0".to_owned(),
        },
        accept.chosen_versions,
        accept.required_capabilities,
        1,
        true,
        1,
        60,
    )
    .expect("augmented metadata is valid")
}
