#![allow(non_snake_case)]

use cc_lb_plugin_wire::schema::{HookKind, WireSchema, WireVersion};

use super::{inspect_wasm, schema_section_name};
use crate::WasmtimeRuntimeError;

fn filter_plugin_wat() -> &'static str {
    r#"
    (module
        (memory (export "memory") 1)
        (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
        (func (export "cc_lb_free") (param i32 i32 i32))
        (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const 0)
    )
    "#
}

fn metadata(version: u8) -> Vec<u8> {
    format!(
        r#"{{"name":"filter-version-test","version":"0.0.1","description":"filter version fixture","usage":"test only","hooks":{{"filter":{{"wire_version":{version},"description":"filter hook","usage":"test only"}}}}}}"#
    )
    .into_bytes()
}

fn wasm_with_filter_schema(
    declared_version: u8,
    section_version: WireVersion,
    fingerprint: &[u8; 32],
) -> Vec<u8> {
    let mut wasm = wat::parse_str(filter_plugin_wat()).expect("valid WAT");
    append_custom_section(
        &mut wasm,
        &schema_section_name(HookKind::Filter, section_version),
        fingerprint,
    );
    append_custom_section(&mut wasm, "cc_lb.plugin.v1", &metadata(declared_version));
    wasm
}

fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    encode_leb128(&mut payload, name.len() as u64);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    module.push(0);
    encode_leb128(module, payload.len() as u64);
    module.extend_from_slice(&payload);
}

fn encode_leb128(buffer: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
    }
}

#[test]
fn accepts_published_filter_v1_fingerprint() {
    let fingerprint = <cc_lb_plugin_wire::v1::FilterRequest as WireSchema>::FINGERPRINT;
    let wasm = wasm_with_filter_schema(WireVersion::V1.as_u8(), WireVersion::V1, &fingerprint);

    let inspection = inspect_wasm(HookKind::Filter, &wasm).expect("V1 accepted");

    assert_eq!(
        inspection.hook_versions.get(&HookKind::Filter),
        Some(&WireVersion::V1)
    );
}

#[test]
fn t1__plugin_wire__mismatched_blake3_fingerprint_rejected_before_compilation() {
    let mut fingerprint = <cc_lb_plugin_wire::v1::FilterRequest as WireSchema>::FINGERPRINT;
    fingerprint[0] ^= 1;
    let wasm = wasm_with_filter_schema(WireVersion::V1.as_u8(), WireVersion::V1, &fingerprint);

    let error = inspect_wasm(HookKind::Filter, &wasm)
        .expect_err("a mismatched schema fingerprint must fail admission");

    match error {
        WasmtimeRuntimeError::ModuleRejected { reason } => {
            assert!(reason.contains("cc_lb.schema.filter.v1"), "{reason}");
            assert!(reason.contains("hash mismatch"), "{reason}");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn t1__plugin_wire__unsupported_wire_version_rejected() {
    let fingerprint = <cc_lb_plugin_wire::v1::FilterRequest as WireSchema>::FINGERPRINT;
    let wasm = wasm_with_filter_schema(255, WireVersion::V1, &fingerprint);

    let error = inspect_wasm(HookKind::Filter, &wasm)
        .expect_err("an unsupported declared wire version must fail admission");

    match error {
        WasmtimeRuntimeError::ModuleRejected { reason } => {
            assert!(
                reason.contains("filter") && reason.contains("unsupported wire version 255"),
                "{reason}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
