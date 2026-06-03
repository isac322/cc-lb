use std::fmt::Write as _;

use cc_lb_plugin_wire::identity::{
    CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME, IdentityError, PluginIdentity,
};
use cc_lb_plugin_wire::limits::CUSTOM_SECTION_MAX_SIZE;
use cc_lb_runtime_extism::identity::{IdentityReadError, read_identity};
use serde_json::json;

#[test]
fn pdk_macro_plugin_identity_is_extracted() {
    let identity = pdk_macro_identity();
    let wasm = pdk_macro_plugin_wasm(&identity);

    let extracted = read_identity(&wasm).expect("identity is extracted");

    assert_eq!(extracted, identity);
}

#[test]
fn missing_custom_section_is_rejected() {
    let wasm = wat::parse_str("(module)").expect("minimal WAT parses");

    let error = read_identity(&wasm).expect_err("custom section is required");

    assert!(matches!(error, IdentityReadError::MissingCustomSection));
}

#[test]
fn duplicate_custom_sections_are_rejected() {
    let payload = identity_payload(&pdk_macro_identity());
    let wasm = wasm_with_custom_sections(&[
        (CC_LB_PLUGIN_SECTION_NAME, payload.clone()),
        (CC_LB_PLUGIN_SECTION_NAME, payload),
    ]);

    let error = read_identity(&wasm).expect_err("duplicate custom sections are rejected");

    assert!(matches!(error, IdentityReadError::DuplicateCustomSection));
}

#[test]
fn oversized_custom_section_is_rejected() {
    let payload = vec![b' '; CUSTOM_SECTION_MAX_SIZE + 1];
    let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, payload)]);

    let error = read_identity(&wasm).expect_err("oversized custom section is rejected");

    assert!(matches!(
        error,
        IdentityReadError::SectionTooLarge { size } if size == CUSTOM_SECTION_MAX_SIZE + 1
    ));
}

#[test]
fn magic_mismatch_is_rejected() {
    let mut identity = pdk_macro_identity();
    identity.magic[7] ^= 0xff;
    let wasm = wasm_with_identity(&identity);

    let error = read_identity(&wasm).expect_err("magic mismatch is rejected");

    match error {
        IdentityReadError::MagicMismatch { expected, found } => {
            assert_eq!(expected, CC_LB_PLUGIN_MAGIC);
            assert_eq!(found, identity.magic);
        }
        other => panic!("expected MagicMismatch, got {other:?}"),
    }
}

#[test]
fn extra_custom_section_fields_are_malformed_payload() {
    let payload = json!({
        "magic": CC_LB_PLUGIN_MAGIC,
        "abi_envelope": 1,
        "plugin_name": "pdk-macro-plugin",
        "plugin_version": "1.0.0",
        "extra": true,
    })
    .to_string()
    .into_bytes();
    let wasm = wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, payload)]);

    let error = read_identity(&wasm).expect_err("extra fields are rejected");

    assert!(matches!(error, IdentityReadError::MalformedPayload(_)));
}

#[test]
fn invalid_plugin_name_is_validation_error() {
    let mut identity = pdk_macro_identity();
    identity.plugin_name = "InvalidPlugin".to_owned();
    let wasm = wasm_with_identity(&identity);

    let error = read_identity(&wasm).expect_err("invalid plugin name is rejected");

    assert!(matches!(
        error,
        IdentityReadError::Validation(IdentityError::PluginNameInvalid)
    ));
}

fn pdk_macro_identity() -> PluginIdentity {
    PluginIdentity {
        magic: CC_LB_PLUGIN_MAGIC,
        abi_envelope: 1,
        plugin_name: "pdk-macro-plugin".to_owned(),
        plugin_version: "1.0.0".to_owned(),
    }
}

fn pdk_macro_plugin_wasm(identity: &PluginIdentity) -> Vec<u8> {
    wasm_module(
        r#"
            (func (export "route"))
            (func (export "cc_lb_handshake"))
            (func (export "cc_lb_self_check"))
        "#,
        &[(CC_LB_PLUGIN_SECTION_NAME, identity_payload(identity))],
    )
}

fn wasm_with_identity(identity: &PluginIdentity) -> Vec<u8> {
    wasm_with_custom_sections(&[(CC_LB_PLUGIN_SECTION_NAME, identity_payload(identity))])
}

fn identity_payload(identity: &PluginIdentity) -> Vec<u8> {
    serde_json::to_vec(identity).expect("identity serializes")
}

fn wasm_with_custom_sections(sections: &[(&str, Vec<u8>)]) -> Vec<u8> {
    wasm_module("", sections)
}

fn wasm_module(body: &str, sections: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut module = String::from("(module");
    module.push_str(body);
    for (name, payload) in sections {
        write!(
            &mut module,
            " (@custom {} {})",
            wat_string_literal(name.as_bytes()),
            wat_string_literal(payload)
        )
        .expect("writing to a String cannot fail");
    }
    module.push(')');
    wat::parse_str(&module).expect("fixture WAT parses")
}

fn wat_string_literal(bytes: &[u8]) -> String {
    let mut literal = String::with_capacity(bytes.len() * 3 + 2);
    literal.push('"');
    for byte in bytes {
        write!(&mut literal, "\\{byte:02x}").expect("writing to a String cannot fail");
    }
    literal.push('"');
    literal
}
