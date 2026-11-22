use cc_lb_plugin_wire::schema::WireSchema;
use cc_lb_plugin_wire::v2::{FilterResponse, PerCandidateReason};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime, WasmtimeRuntimeError,
};
use rkyv::rancor::Error as RkyvError;
use std::sync::Arc;

fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    encode_leb128(&mut payload, name.len() as u64);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);

    module.push(0);
    encode_leb128(module, payload.len() as u64);
    module.extend_from_slice(&payload);
}

fn encode_leb128(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn build_malicious_wat(wat_body: &str, initial_pages: u32) -> Vec<u8> {
    let wat = format!(
        r#"
        (module
            (memory (export "memory") {})
            (func (export "cc_lb_alloc") (param i32 i32) (result i32)
                i32.const 4096
            )
            (func (export "cc_lb_free") (param i32 i32 i32))
            {}
        )
        "#,
        initial_pages, wat_body
    );
    let mut wasm = wat::parse_str(&wat).expect("valid wat");

    let metadata = r#"{"name":"malicious-plugin","version":"0.1.0","description":"Malicious test plugin","usage":"Testing only","hooks":{"filter_anthropic":{"wire_version":3,"description":"filter hook","usage":"call filter","mode":"active"}}}"#;
    append_custom_section(&mut wasm, "cc_lb.plugin.v2", metadata.as_bytes());

    let fingerprint = <cc_lb_plugin_wire::v3::FilterRequest as WireSchema>::FINGERPRINT;
    append_custom_section(&mut wasm, "cc_lb.schema.filter.v3", &fingerprint);

    wasm
}

#[test]
fn test_null_ptr_behavior() {
    let wasm_bytes = build_malicious_wat(
        r#"
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            i64.const 10
        )
        "#,
        1,
    );
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));

    let result = runtime.register_filter(
        RuntimeSlotKey::global("null-ptr-test", RuntimeSlotRole::Filter),
        "null-ptr-test",
        &wasm_bytes,
    );

    assert!(result.is_err());
    let err = match result {
        Ok(_) => unreachable!(),
        Err(e) => e,
    };
    assert!(
        matches!(err, WasmtimeRuntimeError::ProbeFailed { .. }),
        "expected ProbeFailed, got: {:?}",
        err
    );
    let msg = err.to_string();
    assert!(
        msg.contains("guest returned invalid (ptr=0, len=10)"),
        "expected error message to mention invalid ptr=0, got: {}",
        msg
    );
}

#[test]
fn test_zero_len_behavior() {
    let wasm_bytes = build_malicious_wat(
        r#"
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            i64.const 4398046511104
        )
        "#,
        1,
    );
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));

    let result = runtime.register_filter(
        RuntimeSlotKey::global("zero-len-test", RuntimeSlotRole::Filter),
        "zero-len-test",
        &wasm_bytes,
    );

    assert!(result.is_err());
    let err = match result {
        Ok(_) => unreachable!(),
        Err(e) => e,
    };
    assert!(
        matches!(err, WasmtimeRuntimeError::ProbeFailed { .. }),
        "expected ProbeFailed, got: {:?}",
        err
    );
    let msg = err.to_string();
    assert!(
        msg.contains("guest returned invalid (ptr=1024, len=0)"),
        "expected error message to mention invalid len=0, got: {}",
        msg
    );
}

#[test]
fn test_oob_output_behavior() {
    let wasm_bytes = build_malicious_wat(
        r#"
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            i64.const 4398047511104
        )
        "#,
        1,
    );
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));

    let result = runtime.register_filter(
        RuntimeSlotKey::global("oob-output-test", RuntimeSlotRole::Filter),
        "oob-output-test",
        &wasm_bytes,
    );

    assert!(result.is_err());
    let err = match result {
        Ok(_) => unreachable!(),
        Err(e) => e,
    };
    assert!(
        matches!(err, WasmtimeRuntimeError::ProbeFailed { .. }),
        "expected ProbeFailed, got: {:?}",
        err
    );
    let msg = err.to_string();
    assert!(
        msg.contains("out of bounds"),
        "expected error message to mention out of bounds, got: {}",
        msg
    );
}

#[test]
fn test_ptr_len_overflow_behavior() {
    let wasm_bytes = build_malicious_wat(
        r#"
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            i64.const -1
        )
        "#,
        1,
    );
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));

    let result = runtime.register_filter(
        RuntimeSlotKey::global("overflow-test", RuntimeSlotRole::Filter),
        "overflow-test",
        &wasm_bytes,
    );

    assert!(result.is_err());
    let err = match result {
        Ok(_) => unreachable!(),
        Err(e) => e,
    };
    assert!(
        matches!(err, WasmtimeRuntimeError::ProbeFailed { .. }),
        "expected ProbeFailed, got: {:?}",
        err
    );
    let msg = err.to_string();
    assert!(
        msg.contains("out of bounds") || msg.contains("overflows usize"),
        "expected error message to mention out of bounds or overflow, got: {}",
        msg
    );
}

#[test]
fn test_memory_grow_before_return_behavior() {
    let response = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            decision: Box::from("accept"),
            reason: Box::from("grow-ok"),
        }]),
    };
    let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
    let mut data_section = String::new();
    for b in bytes.iter() {
        data_section.push_str(&format!("\\{:02x}", b));
    }

    let wat_body = format!(
        r#"
        (data (i32.const 65536) "{}")
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            i32.const 1
            memory.grow
            drop
            i64.const {}
        )
        "#,
        data_section,
        (65536u64 << 32) | (bytes.len() as u64)
    );

    let wasm_bytes = build_malicious_wat(&wat_body, 2);
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));

    let slot = runtime
        .register_filter(
            RuntimeSlotKey::global("grow-test", RuntimeSlotRole::Filter),
            "grow-test",
            &wasm_bytes,
        )
        .expect("register filter");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let result = dispatch.call_filter_scoped(&[], <[u8]>::to_vec);

    assert!(result.is_ok());
    let out_bytes = result.unwrap();
    assert_eq!(out_bytes, bytes.to_vec());
}

#[test]
fn test_misaligned_valid_rkyv() {
    let response = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            decision: Box::from("accept"),
            reason: Box::from("aligned-or-copy"),
        }]),
    };
    let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
    let mut data_section = String::new();
    for b in bytes.iter() {
        data_section.push_str(&format!("\\{:02x}", b));
    }

    let wat_body = format!(
        r#"
        (data (i32.const 1024) "{}")
        (data (i32.const 2049) "{}")
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            local.get 1
            i32.eqz
            if (result i64)
                i64.const {}
            else
                i64.const {}
            end
        )
        "#,
        data_section,
        data_section,
        (2049u64 << 32) | (bytes.len() as u64),
        (1024u64 << 32) | (bytes.len() as u64)
    );

    let wasm_bytes = build_malicious_wat(&wat_body, 1);
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_filter(
            RuntimeSlotKey::global("misaligned-test", RuntimeSlotRole::Filter),
            "misaligned-test",
            &wasm_bytes,
        )
        .expect("register filter");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let result = dispatch.call_filter_scoped(&[], <[u8]>::to_vec);

    assert!(result.is_ok());
    let out_bytes = result.unwrap();
    assert_eq!(out_bytes, bytes.to_vec());
}

#[test]
fn test_corrupt_rkyv() {
    let corrupt_bytes = vec![0xAA; 32];
    let mut data_section = String::new();
    for b in corrupt_bytes.iter() {
        data_section.push_str(&format!("\\{:02x}", b));
    }

    let response = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            decision: Box::from("accept"),
            reason: Box::from("corrupt-probe-ok"),
        }]),
    };
    let valid_bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
    let mut valid_data_section = String::new();
    for b in valid_bytes.iter() {
        valid_data_section.push_str(&format!("\\{:02x}", b));
    }

    let wat_body = format!(
        r#"
        (data (i32.const 1024) "{}")
        (data (i32.const 2048) "{}")
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            local.get 1
            i32.eqz
            if (result i64)
                i64.const 8796093022240
            else
                i64.const {}
            end
        )
        "#,
        valid_data_section,
        data_section,
        (1024u64 << 32) | (valid_bytes.len() as u64)
    );

    let wasm_bytes = build_malicious_wat(&wat_body, 1);
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_filter(
            RuntimeSlotKey::global("corrupt-test", RuntimeSlotRole::Filter),
            "corrupt-test",
            &wasm_bytes,
        )
        .expect("register filter");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let result = dispatch.call_filter_scoped(&[], <[u8]>::to_vec);

    assert!(result.is_ok());
    let out_bytes = result.unwrap();
    assert_eq!(out_bytes, corrupt_bytes);
}
