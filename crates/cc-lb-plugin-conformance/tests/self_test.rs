use cc_lb_plugin_conformance::{handshake, self_check};

#[cfg(feature = "dispatch")]
use std::collections::BTreeSet;

#[cfg(feature = "dispatch")]
use cc_lb_plugin_conformance::{
    VerifyError,
    dispatch::{DispatchOutcome, PluginSession},
    fixtures,
    handshake::HandshakeError,
    verify_observability_plugin, verify_observability_plugin_with_caps, verify_router_plugin,
    verify_shape_plugin,
};
#[cfg(feature = "dispatch")]
use cc_lb_plugin_wire::{identity::CC_LB_PLUGIN_SECTION_NAME, v2::shape::ShapeFn};
#[cfg(feature = "dispatch")]
use wasmparser::{Parser, Payload};

const SHAPE_WASM: &[u8] = include_bytes!(env!("CONFORMANCE_FIXTURE_SHAPE_WASM"));
#[cfg_attr(not(feature = "dispatch"), allow(dead_code))]
const ROUTER_WASM: &[u8] = include_bytes!(env!("CONFORMANCE_FIXTURE_ROUTER_WASM"));
#[cfg_attr(not(feature = "dispatch"), allow(dead_code))]
const OBSERVE_WASM: &[u8] = include_bytes!(env!("CONFORMANCE_FIXTURE_OBSERVE_WASM"));
#[cfg(feature = "dispatch")]
const MALFORMED_CUSTOM_SECTION_JSON: &[u8] = b"{";

#[test]
fn handshake_negotiates_shape_at_v1() {
    let report = handshake::run(SHAPE_WASM).unwrap();

    assert_eq!(report.chosen_versions["shape"], 1);
}

#[test]
fn self_check_passes_for_shape_fixture() {
    let report =
        self_check::run(SHAPE_WASM, &[cc_lb_plugin_api::types::PluginSlot::Shape]).unwrap();

    assert_eq!(report.status, self_check::SelfCheckStatus::Success);
    assert!(report.failures.is_empty());
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_shape_plugin_succeeds() {
    let report = verify_shape_plugin(SHAPE_WASM).unwrap();

    assert!(
        report.identity.passed
            && report.handshake.passed
            && report.self_check.passed
            && report.dispatch[0].passed
    );
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_router_plugin_succeeds() {
    let report = verify_router_plugin(ROUTER_WASM).unwrap();

    assert!(
        report.identity.passed
            && report.handshake.passed
            && report.self_check.passed
            && report.dispatch.iter().all(|result| result.passed)
    );
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_observability_plugin_succeeds() {
    let capabilities = observability_capabilities();
    let report = verify_observability_plugin_with_caps(OBSERVE_WASM, &capabilities).unwrap();

    assert!(
        report.identity.passed
            && report.handshake.passed
            && report.self_check.passed
            && report.dispatch.iter().all(|result| result.passed)
    );
}

#[cfg(feature = "dispatch")]
#[test]
fn shape_dispatch_round_trip_basic() {
    let mut session = PluginSession::new(SHAPE_WASM).unwrap();
    let request = fixtures::shape_request_builder().build();

    let outcome = session.dispatch::<ShapeFn>(request);

    match outcome {
        DispatchOutcome::Ok(response) => assert!(!response.url.is_empty()),
        DispatchOutcome::Fallback(policy) => panic!("shape dispatch fell back with {policy:?}"),
        _ => panic!("unexpected non-exhaustive dispatch outcome"),
    }
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_shape_plugin_rejects_malformed_custom_section() {
    let wasm = shape_wasm_with_malformed_custom_section();

    let error =
        verify_shape_plugin(&wasm).expect_err("malformed identity custom section is rejected");

    assert_malformed_custom_section_error(error);
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_router_plugin_rejects_wrong_kind_wasm() {
    let error = verify_router_plugin(SHAPE_WASM).expect_err("shape wasm is not a router plugin");

    assert!(matches!(
        error,
        VerifyError::Handshake(HandshakeError::FunctionMissing { name }) if name == "filter"
    ));
}

#[cfg(feature = "dispatch")]
#[test]
fn verify_observability_plugin_capability_gate() {
    let error = verify_observability_plugin(OBSERVE_WASM)
        .expect_err("observability fixture requires observability:emit");
    assert!(
        matches!(
            error,
            VerifyError::Handshake(HandshakeError::MissingCapability { ref name })
                if name == "observability:emit"
        ),
        "got {error:?}"
    );

    let capabilities = observability_capabilities();
    let report = verify_observability_plugin_with_caps(OBSERVE_WASM, &capabilities).unwrap();
    assert!(
        report.identity.passed
            && report.handshake.passed
            && report.self_check.passed
            && report.dispatch.iter().all(|result| result.passed)
    );
}

#[cfg(feature = "dispatch")]
fn observability_capabilities() -> BTreeSet<String> {
    BTreeSet::from(["observability:emit".to_owned()])
}

#[cfg(feature = "dispatch")]
fn shape_wasm_with_malformed_custom_section() -> Vec<u8> {
    let mut wasm = SHAPE_WASM.to_vec();
    let (offset, len) = custom_section_payload_range(SHAPE_WASM);
    let payload = malformed_json_payload(len);

    wasm[offset..offset + len].copy_from_slice(&payload);
    wasm
}

#[cfg(feature = "dispatch")]
fn custom_section_payload_range(wasm: &[u8]) -> (usize, usize) {
    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.expect("fixture wasm parses");
        let Payload::CustomSection(section) = payload else {
            continue;
        };
        if section.name() == CC_LB_PLUGIN_SECTION_NAME {
            return (section.data_offset(), section.data().len());
        }
    }

    panic!("fixture wasm contains {CC_LB_PLUGIN_SECTION_NAME} custom section");
}

#[cfg(feature = "dispatch")]
fn malformed_json_payload(len: usize) -> Vec<u8> {
    let mut payload = vec![b' '; len];
    payload[..MALFORMED_CUSTOM_SECTION_JSON.len()].copy_from_slice(MALFORMED_CUSTOM_SECTION_JSON);
    payload
}

#[cfg(feature = "dispatch")]
fn assert_malformed_custom_section_error(error: VerifyError) {
    match error {
        VerifyError::Identity(error) => {
            assert!(format!("{error}").contains("malformed"));
        }
        VerifyError::Handshake(HandshakeError::InvalidIdentity { field, reason }) => {
            assert!(matches!(field, "payload" | "custom_section"));
            assert!(reason.contains("malformed") || reason.contains("EOF"));
        }
        other => panic!("expected malformed identity custom section error, got {other:?}"),
    }
}
