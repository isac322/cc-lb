use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireVersion};
use cc_lb_plugin_wire::v2::{FilterResponse, ShapeResponse};

use crate::inspect::{expected_fingerprint, schema_section_name};
use crate::{WasmtimeRuntime, WasmtimeRuntimeError, inspect_wasm_agnostic};

const FILTER_RESPONSE_OFFSET: u64 = 4096;
const SHAPE_RESPONSE_OFFSET: u64 = 8192;

fn plugin_wasm(declared_hooks: &[(HookKind, ProviderFamily, &str)]) -> Vec<u8> {
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
        .map(|(hook, provider, _)| hook_export(*hook, *provider, filter_packed, shape_packed))
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
    for (hook, _, _) in declared_hooks {
        let wire_version = wire_version_for(*hook);
        let fingerprint = expected_fingerprint(*hook, wire_version).expect("test hook fingerprint");
        append_custom_section(
            &mut wasm,
            &schema_section_name(*hook, wire_version),
            &fingerprint,
        );
    }
    append_custom_section(&mut wasm, "cc_lb.plugin.v2", &metadata(declared_hooks));
    wasm
}

fn wire_version_for(hook: HookKind) -> WireVersion {
    match hook {
        HookKind::Filter => WireVersion::V3,
        _ => WireVersion::V2,
    }
}

fn wat_data(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>()
}

fn hook_export(
    hook: HookKind,
    provider: ProviderFamily,
    filter_packed: u64,
    shape_packed: u64,
) -> String {
    let export = hook.qualified_export_name(provider);
    let ret = match hook {
        HookKind::Filter => filter_packed,
        HookKind::Shape => shape_packed,
        _ => 0,
    };
    format!(r#"(func (export "{export}") (param i32 i32) (result i64) i64.const {ret})"#)
}

fn metadata(declared_hooks: &[(HookKind, ProviderFamily, &str)]) -> Vec<u8> {
    let hooks = declared_hooks
        .iter()
        .map(|(hook, provider, mode)| {
            let wire_version = wire_version_for(*hook).as_u8();
            let hook_metadata = serde_json::json!({
                "wire_version": wire_version,
                "description": format!("{} hook", hook.as_str()),
                "usage": format!("call {}", hook.as_str()),
                "mode": mode,
            });
            let hook_name = hook
                .qualified_export_name(*provider)
                .strip_prefix("cc_lb_")
                .expect("qualified hook export has cc_lb prefix");
            (hook_name.to_owned(), hook_metadata)
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
    let wasm = plugin_wasm(&[
        (HookKind::Filter, ProviderFamily::Anthropic, "active"),
        (HookKind::Shape, ProviderFamily::Anthropic, "active"),
        (
            HookKind::TransformResponse,
            ProviderFamily::Anthropic,
            "noop",
        ),
        (
            HookKind::TransformSseEvent,
            ProviderFamily::Anthropic,
            "noop",
        ),
        (
            HookKind::TransformRealtimeFrame,
            ProviderFamily::Openai,
            "noop",
        ),
    ]);
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");

    let inspection = runtime
        .admit_wasm_agnostic(&wasm)
        .expect("multi-hook artifact admitted");

    assert_eq!(inspection.hook_versions.len(), 5);
    assert_eq!(
        inspection.hook_versions.get(&HookKind::Filter),
        Some(&WireVersion::V3)
    );
    assert_eq!(
        inspection.hook_versions.get(&HookKind::Shape),
        Some(&WireVersion::V2)
    );
}

#[test]
fn same_module_registers_both_slot_roles_in_either_order() {
    let wasm = plugin_wasm(&[
        (HookKind::Filter, ProviderFamily::Anthropic, "active"),
        (HookKind::Shape, ProviderFamily::Anthropic, "active"),
        (
            HookKind::TransformResponse,
            ProviderFamily::Anthropic,
            "noop",
        ),
        (
            HookKind::TransformSseEvent,
            ProviderFamily::Anthropic,
            "noop",
        ),
    ]);
    for shape_first in [true, false] {
        let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
        let filter_key = crate::RuntimeSlotKey::new(
            "dual-slot",
            "agnostic-test",
            crate::RuntimeSlotRole::Filter,
        );
        let shape_key =
            crate::RuntimeSlotKey::new("dual-slot", "agnostic-test", crate::RuntimeSlotRole::Shape);
        if shape_first {
            runtime
                .register_shape(shape_key.clone(), "agnostic-test", &wasm)
                .expect("shape registration");
            runtime
                .register_filter(filter_key.clone(), "agnostic-test", &wasm)
                .expect("filter registration must not collide with shape");
        } else {
            runtime
                .register_filter(filter_key.clone(), "agnostic-test", &wasm)
                .expect("filter registration");
            runtime
                .register_shape(shape_key.clone(), "agnostic-test", &wasm)
                .expect("shape registration must not collide with filter");
        }
        let filter = runtime.get_slot(&filter_key).expect("filter slot");
        let shape = runtime.get_slot(&shape_key).expect("shape slot");
        assert!(!std::sync::Arc::ptr_eq(&filter, &shape));
        assert!(std::sync::Arc::ptr_eq(
            &filter.current.load().instance_pre,
            &shape.current.load().instance_pre,
        ));
        let same_filter = runtime
            .register_filter(filter_key.clone(), "agnostic-test", &wasm)
            .expect("unchanged filter");
        assert!(std::sync::Arc::ptr_eq(&filter, &same_filter));
        let old_shape_cell = shape.current.load_full();
        let mut updated = wasm.clone();
        append_custom_section(&mut updated, "test.replacement", b"new");
        runtime
            .register_filter(filter_key.clone(), "agnostic-test", &updated)
            .expect("replace filter only");
        assert!(std::sync::Arc::ptr_eq(
            &old_shape_cell,
            &shape.current.load_full(),
        ));
        assert_ne!(
            filter.current.load().content_hash,
            shape.current.load().content_hash,
        );
        let shape_dispatch = crate::WasmPluginWireDispatch::from_slot(shape, runtime.config_arc());
        let evicted = runtime.retain_slots(&std::collections::HashSet::from([filter_key.clone()]));
        assert_eq!(evicted, vec![shape_key.clone()]);
        assert!(runtime.get_slot(&shape_key).is_none());
        assert!(runtime.get_slot(&filter_key).is_some());
        let output = shape_dispatch
            .call_shape_scoped(&[], |bytes| {
                rkyv::from_bytes::<ShapeResponse, rkyv::rancor::Error>(bytes)
                    .expect("shape response")
            })
            .expect("retained dispatch remains callable");
        assert_eq!(output.url.as_ref(), "https://example.test/v1/messages");
        runtime
            .register_shape(shape_key.clone(), "agnostic-test", &wasm)
            .expect("reattach shape");
        let evicted = runtime.retain_slots(&std::collections::HashSet::from([shape_key.clone()]));
        assert_eq!(evicted, vec![filter_key.clone()]);
        assert!(runtime.get_slot(&filter_key).is_none());
        assert!(runtime.get_slot(&shape_key).is_some());
    }
}

#[test]
fn registration_rejects_wrong_role_before_loading_module() {
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
    for (role, filter) in [
        (crate::RuntimeSlotRole::Shape, true),
        (crate::RuntimeSlotRole::Filter, false),
    ] {
        let key = crate::RuntimeSlotKey::global("wrong-role", role);
        let result = if filter {
            runtime.register_filter(key.clone(), "wrong-role", &[])
        } else {
            runtime.register_shape(key.clone(), "wrong-role", &[])
        };
        assert!(matches!(
            result,
            Err(WasmtimeRuntimeError::ModuleRejected { reason })
                if reason.contains("cannot be registered")
        ));
        assert!(runtime.get_slot(&key).is_none());
    }
}

#[test]
fn agnostic_inspection_accepts_independent_shape_and_response_hooks() {
    for hooks in [
        vec![(HookKind::Shape, ProviderFamily::Openai, "active")],
        vec![(
            HookKind::TransformResponse,
            ProviderFamily::Openai,
            "active",
        )],
        vec![(
            HookKind::TransformSseEvent,
            ProviderFamily::Openai,
            "active",
        )],
    ] {
        let wasm = plugin_wasm(&hooks);
        let inspection = inspect_wasm_agnostic(&wasm)
            .expect("provider-qualified V2 shape-family hooks are independent");
        assert_eq!(inspection.hook_versions.len(), 1);
    }
}

#[test]
fn agnostic_inspection_rejects_envelope_without_gemini_shape() {
    let wasm = plugin_wasm(&[(
        HookKind::ShapeEnvelopeGemini,
        ProviderFamily::Gemini,
        "active",
    )]);
    let error = inspect_wasm_agnostic(&wasm).expect_err("envelope without shape_gemini rejected");
    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("shape_envelope_gemini") && reason.contains("shape")
    ));
}

#[test]
fn agnostic_inspection_rejects_noop_primary_hook() {
    let wasm = plugin_wasm(&[(HookKind::Filter, ProviderFamily::Anthropic, "noop")]);

    let error = inspect_wasm_agnostic(&wasm).expect_err("primary noop rejected");

    assert!(matches!(
        error,
        WasmtimeRuntimeError::ModuleRejected { reason }
            if reason.contains("filter") && reason.contains("noop")
    ));
}

#[test]
fn agnostic_admission_probes_each_declared_hook() {
    let wasm = plugin_wasm(&[
        (HookKind::Filter, ProviderFamily::Anthropic, "active"),
        (HookKind::Shape, ProviderFamily::Anthropic, "active"),
        (
            HookKind::TransformResponse,
            ProviderFamily::Anthropic,
            "active",
        ),
        (
            HookKind::TransformSseEvent,
            ProviderFamily::Anthropic,
            "noop",
        ),
    ]);
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");

    let error = runtime
        .admit_wasm_agnostic(&wasm)
        .expect_err("secondary hook probe must run");

    assert!(matches!(
        error,
        WasmtimeRuntimeError::ProbeFailed {
            hook: "transform_response",
            ..
        }
    ));
}
