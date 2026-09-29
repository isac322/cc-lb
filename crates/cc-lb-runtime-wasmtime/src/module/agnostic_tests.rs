use cc_lb_plugin_wire::schema::{HookKind, WireVersion};
use cc_lb_plugin_wire::v1::{FilterResponse, ShapeResponse};

use crate::inspect::{expected_fingerprint, schema_section_name};
use crate::{WasmtimeRuntime, WasmtimeRuntimeError, inspect_wasm_agnostic};

const FILTER_RESPONSE_OFFSET: u64 = 4096;
const SHAPE_RESPONSE_OFFSET: u64 = 8192;

fn plugin_wasm(declared_hooks: &[(HookKind, &str)]) -> Vec<u8> {
    let filter_response = rkyv::to_bytes::<rkyv::rancor::Error>(&FilterResponse {
        results: Box::new([]),
    })
    .expect("encode filter response");
    let shape_response = rkyv::to_bytes::<rkyv::rancor::Error>(&ShapeResponse {
        url: Box::from("https://example.test/v1/messages"),
        method: Box::from("POST"),
        headers: Box::new([]),
        body: Box::new([]),
    })
    .expect("encode shape response");
    let filter_data = wat_data(&filter_response);
    let shape_data = wat_data(&shape_response);
    let filter_packed = (FILTER_RESPONSE_OFFSET << 32) | filter_response.len() as u64;
    let shape_packed = (SHAPE_RESPONSE_OFFSET << 32) | shape_response.len() as u64;
    let hook_exports = declared_hooks
        .iter()
        .map(|(hook, _)| hook_export(*hook, filter_packed, shape_packed))
        .collect::<String>();
    let wat = format!(
        r#"
        (module
            (memory (export "memory") 1)
            (data (i32.const {FILTER_RESPONSE_OFFSET}) "{filter_data}")
            (data (i32.const {SHAPE_RESPONSE_OFFSET}) "{shape_data}")
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 1024)
            (func (export "cc_lb_free") (param i32 i32 i32))
            {hook_exports}
        )
        "#
    );
    let mut wasm = wat::parse_str(wat).expect("valid WAT");
    for (hook, _) in declared_hooks {
        let fingerprint = expected_fingerprint(*hook, WireVersion::V1)
            .expect("all test hooks have a V1 fingerprint");
        append_custom_section(
            &mut wasm,
            &schema_section_name(*hook, WireVersion::V1),
            &fingerprint,
        );
    }
    append_custom_section(&mut wasm, "cc_lb.plugin.v1", &metadata(declared_hooks));
    wasm
}

fn wat_data(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>()
}

fn hook_export(hook: HookKind, filter_packed: u64, shape_packed: u64) -> String {
    match hook {
        HookKind::Filter => format!(
            r#"(func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const {filter_packed})"#
        ),
        HookKind::Shape => format!(
            r#"(func (export "cc_lb_shape") (param i32 i32) (result i64) i64.const {shape_packed})"#
        ),
        HookKind::TransformResponse => {
            r#"(func (export "cc_lb_transform_response") (param i32 i32) (result i64) i64.const 0)"#
                .to_owned()
        }
        HookKind::TransformSseEvent => {
            r#"(func (export "cc_lb_transform_sse_event") (param i32 i32) (result i64) i64.const 0)"#
                .to_owned()
        }
    }
}

fn metadata(declared_hooks: &[(HookKind, &str)]) -> Vec<u8> {
    let hooks = declared_hooks
        .iter()
        .map(|(hook, mode)| {
            let hook_metadata = serde_json::json!({
                "wire_version": 1,
                "description": format!("{} hook", hook.as_str()),
                "usage": format!("call {}", hook.as_str()),
                "mode": mode,
            });
            (hook.as_str().to_owned(), hook_metadata)
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::to_vec(&serde_json::json!({
        "name": "agnostic-test",
        "version": "0.0.1",
        "description": "agnostic admission fixture",
        "usage": "test only",
        "hooks": hooks,
    }))
    .expect("serialize metadata")
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
fn agnostic_admission_accepts_filter_and_shape_hooks() {
    // Given: one artifact declaring both independent slot hooks.
    let wasm = plugin_wasm(&[
        (HookKind::Filter, "active"),
        (HookKind::Shape, "active"),
        (HookKind::TransformResponse, "noop"),
        (HookKind::TransformSseEvent, "noop"),
    ]);
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");

    // When: admission is requested without selecting a slot.
    let inspection = runtime
        .admit_wasm_agnostic(&wasm)
        .expect("multi-hook artifact admitted");

    // Then: every declared contract is retained by the inspection.
    assert_eq!(inspection.hook_versions.len(), 4);
    assert_eq!(
        inspection.hook_versions.get(&HookKind::Filter),
        Some(&WireVersion::V1)
    );
    assert_eq!(
        inspection.hook_versions.get(&HookKind::Shape),
        Some(&WireVersion::V1)
    );
}

#[test]
fn agnostic_inspection_rejects_shape_without_owned_response_hooks() {
    // Given: Shape is declared without either Shape-owned response hook.
    let wasm = plugin_wasm(&[(HookKind::Shape, "active")]);

    // When: the artifact is inspected without selecting a slot.
    let error = inspect_wasm_agnostic(&wasm).expect_err("incomplete Shape ownership rejected");

    // Then: the missing owned hook is identified as a module contract failure.
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("transform_response") && reason.contains("shape plugin")
    ));
}

#[test]
fn agnostic_inspection_rejects_orphan_transform_response() {
    // Given: a response transform is declared without Shape ownership.
    let wasm = plugin_wasm(&[(HookKind::TransformResponse, "active")]);

    // When: the artifact is inspected without selecting a slot.
    let error = inspect_wasm_agnostic(&wasm).expect_err("orphan response hook rejected");

    // Then: the orphaned hook is identified as a module contract failure.
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("transform_response") && reason.contains("shape")
    ));
}

#[test]
fn agnostic_inspection_rejects_orphan_transform_sse_event() {
    // Given: an SSE transform is declared without Shape ownership.
    let wasm = plugin_wasm(&[(HookKind::TransformSseEvent, "active")]);

    // When: the artifact is inspected without selecting a slot.
    let error = inspect_wasm_agnostic(&wasm).expect_err("orphan SSE hook rejected");

    // Then: the orphaned hook is identified as a module contract failure.
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("transform_sse_event") && reason.contains("shape")
    ));
}

#[test]
fn agnostic_inspection_rejects_noop_primary_hook() {
    // Given: a primary slot hook declares the response-only noop mode.
    let wasm = plugin_wasm(&[(HookKind::Filter, "noop")]);

    // When: the artifact is inspected without selecting a slot.
    let error = inspect_wasm_agnostic(&wasm).expect_err("primary noop rejected");

    // Then: the invalid mode is rejected by the shared contract gate.
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("filter") && reason.contains("noop")
    ));
}

#[test]
fn agnostic_admission_probes_each_declared_hook() {
    // Given: Filter and Shape are valid but the active response transform
    // returns the invalid `(0, 0)` output.
    let wasm = plugin_wasm(&[
        (HookKind::Filter, "active"),
        (HookKind::Shape, "active"),
        (HookKind::TransformResponse, "active"),
        (HookKind::TransformSseEvent, "noop"),
    ]);
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");

    // When: the structurally valid artifact reaches agnostic admission.
    let error = runtime
        .admit_wasm_agnostic(&wasm)
        .expect_err("secondary hook probe must run");

    // Then: probing reaches and rejects the malformed response transform.
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ProbeFailed {
            hook: "transform_response",
            ..
        }
    ));
}
