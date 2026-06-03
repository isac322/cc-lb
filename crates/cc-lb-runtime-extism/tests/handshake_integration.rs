use std::collections::{BTreeMap, BTreeSet};

use cc_lb_plugin_wire::handshake::{
    HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeError, HandshakeOffer,
};
use cc_lb_runtime_extism::handshake::{HandshakeExecutionError, build_offer, execute_handshake};

const ENVELOPE_VERSION_V1: u32 = 1;

#[test]
fn pdk_macro_shaped_plugin_returns_handshake_accept() {
    let offer = offer_with_caps(&["streaming"]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &["route"],
        &[("route", &[1])],
        &[("route", 1)],
        &["streaming"],
    );
    let wasm = handshake_module(&accept_json(&accept), &["route", "cc_lb_self_check"], false);

    let actual = execute_handshake(&wasm, &offer).expect("handshake succeeds");

    assert_eq!(actual, accept);
}

#[test]
fn host_fn_call_during_handshake_is_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(HANDSHAKE_SCHEMA_VERSION_V1, &[], &[], &[], &[]);
    let wasm = handshake_module(&accept_json(&accept), &[], true);

    let err = execute_handshake(&wasm, &offer).expect_err("host function call is rejected");

    assert!(
        matches!(
            err,
            HandshakeExecutionError::Instantiate { .. } | HandshakeExecutionError::Call { .. }
        ),
        "expected host-function trap/reject, got {err:?}"
    );
}

#[test]
fn downgrade_attack_rejected() {
    let mut offer = offer_with_caps(&[]);
    offer
        .function_versions
        .insert("route".to_owned(), vec![1, 2]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &["route"],
        &[("route", &[1, 2])],
        &[("route", 1)],
        &[],
    );
    let wasm = handshake_module(&accept_json(&accept), &["route"], false);

    let err = execute_handshake(&wasm, &offer).expect_err("downgrade is rejected");

    match err {
        HandshakeExecutionError::Validation(HandshakeError::DowngradeAttempt {
            function,
            chosen,
            max_intersection,
        }) => {
            assert_eq!(function, "route");
            assert_eq!(chosen, 1);
            assert_eq!(max_intersection, 2);
        }
        other => panic!("expected DowngradeAttempt, got {other:?}"),
    }
}

#[test]
fn chosen_unknown_function_key_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &[],
        &[("unknown", &[1])],
        &[("unknown", 1)],
        &[],
    );
    let wasm = handshake_module(&accept_json(&accept), &[], false);

    let err = execute_handshake(&wasm, &offer).expect_err("unknown chosen function rejected");

    match err {
        HandshakeExecutionError::Validation(HandshakeError::ChosenForUnknownFunction {
            function,
        }) => assert_eq!(function, "unknown"),
        other => panic!("expected ChosenForUnknownFunction, got {other:?}"),
    }
}

#[test]
fn unsupported_chosen_version_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &["route"],
        &[("route", &[2])],
        &[("route", 2)],
        &[],
    );
    let wasm = handshake_module(&accept_json(&accept), &["route"], false);

    let err = execute_handshake(&wasm, &offer).expect_err("unsupported chosen version rejected");

    match err {
        HandshakeExecutionError::Validation(HandshakeError::ChosenVersionNotOffered {
            function,
            version,
        }) => {
            assert_eq!(function, "route");
            assert_eq!(version, 2);
        }
        other => panic!("expected ChosenVersionNotOffered, got {other:?}"),
    }
}

#[test]
fn declared_function_missing_export_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &["route"],
        &[("route", &[1])],
        &[("route", 1)],
        &[],
    );
    let wasm = handshake_module(&accept_json(&accept), &[], false);

    let err = execute_handshake(&wasm, &offer).expect_err("missing declared export rejected");

    match err {
        HandshakeExecutionError::DeclaredFunctionMissing { function } => {
            assert_eq!(function, "route");
        }
        other => panic!("expected DeclaredFunctionMissing, got {other:?}"),
    }
}

#[test]
fn undeclared_export_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(
        HANDSHAKE_SCHEMA_VERSION_V1,
        &["route"],
        &[("route", &[1])],
        &[("route", 1)],
        &[],
    );
    let wasm = handshake_module(&accept_json(&accept), &["route", "shape"], false);

    let err = execute_handshake(&wasm, &offer).expect_err("undeclared export rejected");

    match err {
        HandshakeExecutionError::UndeclaredExport { function } => {
            assert_eq!(function, "shape");
        }
        other => panic!("expected UndeclaredExport, got {other:?}"),
    }
}

#[test]
fn handshake_schema_version_mismatch_rejected() {
    let offer = offer_with_caps(&[]);
    let accept = accept(2, &[], &[], &[], &[]);
    let wasm = handshake_module(&accept_json(&accept), &[], false);

    let err = execute_handshake(&wasm, &offer).expect_err("schema mismatch rejected");

    match err {
        HandshakeExecutionError::Validation(HandshakeError::HandshakeSchemaVersionMismatch {
            got,
            expected,
        }) => {
            assert_eq!(got, 2);
            assert_eq!(expected, HANDSHAKE_SCHEMA_VERSION_V1);
        }
        other => panic!("expected HandshakeSchemaVersionMismatch, got {other:?}"),
    }
}

#[test]
fn handshake_timeout_rejected() {
    let offer = offer_with_caps(&[]);
    let wasm = timeout_module();

    let err = execute_handshake(&wasm, &offer).expect_err("timeout rejected");

    match err {
        HandshakeExecutionError::Timeout => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[test]
fn missing_required_capability_rejected() {
    let offer = offer_with_caps(&["log"]);
    let accept = accept(HANDSHAKE_SCHEMA_VERSION_V1, &[], &[], &[], &["storage"]);
    let wasm = handshake_module(&accept_json(&accept), &[], false);

    let err = execute_handshake(&wasm, &offer).expect_err("missing capability rejected");

    match err {
        HandshakeExecutionError::Validation(HandshakeError::RequiredCapabilityUnavailable {
            capability,
        }) => assert_eq!(capability, "storage"),
        other => panic!("expected RequiredCapabilityUnavailable, got {other:?}"),
    }
}

fn offer_with_caps(capabilities: &[&str]) -> HandshakeOffer {
    let host_caps = capabilities
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect();
    build_offer(&host_caps)
}

fn accept(
    handshake_schema_version: u32,
    implemented: &[&str],
    supported: &[(&str, &[u32])],
    chosen: &[(&str, u32)],
    required_capabilities: &[&str],
) -> HandshakeAccept {
    HandshakeAccept {
        handshake_schema_version,
        envelope_version: ENVELOPE_VERSION_V1,
        chosen_versions: chosen
            .iter()
            .map(|(function, version)| ((*function).to_owned(), *version))
            .collect::<BTreeMap<_, _>>(),
        plugin_supported: supported
            .iter()
            .map(|(function, versions)| ((*function).to_owned(), versions.to_vec()))
            .collect::<BTreeMap<_, _>>(),
        implemented_functions: implemented
            .iter()
            .map(|function| (*function).to_owned())
            .collect::<BTreeSet<_>>(),
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect::<BTreeSet<_>>(),
    }
}

fn accept_json(accept: &HandshakeAccept) -> String {
    serde_json::to_string(accept).expect("accept serializes")
}

fn handshake_module(output: &str, extra_exports: &[&str], import_user_host: bool) -> Vec<u8> {
    let output_helper = bytes_helper("handshake_out", output.as_bytes());
    let user_import = if import_user_host {
        r#"(import "extism:host/user" "cc_lb_log" (func $cc_lb_log (param i64 i64)))"#
    } else {
        ""
    };
    let user_call = if import_user_host {
        "  (call $cc_lb_log (call $handshake_out) (call $handshake_out))"
    } else {
        ""
    };
    let mut exports = String::new();
    for export in extra_exports {
        exports.push_str(&format!(
            r#"
(func (export "{export}") (result i32)
  (i32.const 0))
"#
        ));
    }
    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {user_import}
  {output_helper}
  (func (export "cc_lb_handshake") (result i32)
{user_call}
    (call $output_set (call $handshake_out) (i64.const {len}))
    (i32.const 0))
  {exports}
)
"#,
        len = output.len()
    );
    wat::parse_str(&wat).expect("handshake wat parses")
}

fn timeout_module() -> Vec<u8> {
    wat::parse_str(
        r#"
(module
  (func (export "cc_lb_handshake") (result i32)
    (loop $again
      br $again)
    (i32.const 0))
)
"#,
    )
    .expect("timeout wat parses")
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}
