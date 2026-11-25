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

#[test]
fn infinite_loop_is_preempted_during_admission_and_hot_path_and_after_runtime_drop() {
    // 1. Admission probe infinite loop traps with Trap::Interrupt.
    let loop_on_probe_wasm = build_malicious_wat(
        r#"
        (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
            (loop $l
                br $l
            )
            i64.const 0
        )
        "#,
        1,
    );
    let fast_rt = Arc::new(
        WasmtimeRuntime::new_with_execution_timeout(
            Default::default(),
            std::time::Duration::from_millis(50),
        )
        .expect("engine build"),
    );
    let err = fast_rt
        .admit_wasm_agnostic(&loop_on_probe_wasm)
        .expect_err("infinite loop on admission probe must time out");
    assert!(
        err.is_epoch_interrupt(),
        "expected epoch interrupt error on probe, got {err:?}"
    );
    let Err(filter_reg_err) = fast_rt.register_filter(
        RuntimeSlotKey::global("loop-filter-probe", RuntimeSlotRole::Filter),
        "loop-filter-probe",
        &loop_on_probe_wasm,
    ) else {
        panic!("register_filter must preserve probe epoch interrupt");
    };
    assert!(
        filter_reg_err.is_epoch_interrupt()
            && matches!(filter_reg_err, WasmtimeRuntimeError::GuestTrap { .. }),
        "expected preserved GuestTrap epoch interrupt on register_filter, got {filter_reg_err:?}"
    );

    let mut shape_loop_wasm = wat::parse_str(
        r#"
        (module
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 4096)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64)
                (loop $hang
                    br $hang
                )
                i64.const 0
            )
        )
        "#,
    )
    .expect("valid shape wat");
    append_custom_section(
        &mut shape_loop_wasm,
        "cc_lb.plugin.v2",
        br#"{"name":"malicious-shape-plugin","version":"0.1.0","description":"Malicious shape plugin","usage":"Testing only","hooks":{"shape_anthropic":{"wire_version":2,"description":"shape hook","usage":"call shape","mode":"active"}}}"#,
    );
    let shape_fp = <cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT;
    append_custom_section(&mut shape_loop_wasm, "cc_lb.schema.shape.v2", &shape_fp);
    let Err(shape_reg_err) = fast_rt.register_shape(
        RuntimeSlotKey::global("loop-shape-probe", RuntimeSlotRole::Shape),
        "loop-shape-probe",
        &shape_loop_wasm,
    ) else {
        panic!("register_shape must preserve probe epoch interrupt");
    };
    assert!(
        shape_reg_err.is_epoch_interrupt()
            && matches!(shape_reg_err, WasmtimeRuntimeError::GuestTrap { .. }),
        "expected preserved GuestTrap epoch interrupt on register_shape, got {shape_reg_err:?}"
    );

    // 1b. Infinite loop inside WASM `(start ...)` initializer preempts during
    // `InstancePre::instantiate` (`InstantiateFailed`) and preserves `is_epoch_interrupt()`.
    let mut filter_start_wasm = wat::parse_str(
        r#"
        (module
            (memory (export "memory") 1)
            (func $init
                (loop $hang
                    br $hang
                )
            )
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 4096)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64) i64.const 0)
            (start $init)
        )
        "#,
    )
    .expect("valid filter start wat");
    append_custom_section(
        &mut filter_start_wasm,
        "cc_lb.plugin.v2",
        br#"{"name":"malicious-filter-start","version":"0.1.0","description":"Malicious filter start","usage":"Testing only","hooks":{"filter_anthropic":{"wire_version":3,"description":"filter hook","usage":"call filter","mode":"active"}}}"#,
    );
    let filter_start_fp = <cc_lb_plugin_wire::v3::FilterRequest as WireSchema>::FINGERPRINT;
    append_custom_section(
        &mut filter_start_wasm,
        "cc_lb.schema.filter.v3",
        &filter_start_fp,
    );
    let Err(filter_start_err) = fast_rt.register_filter(
        RuntimeSlotKey::global("loop-filter-start", RuntimeSlotRole::Filter),
        "loop-filter-start",
        &filter_start_wasm,
    ) else {
        panic!("register_filter must preempt infinite `(start ...)` initializer");
    };
    assert!(
        filter_start_err.is_epoch_interrupt()
            && matches!(filter_start_err, WasmtimeRuntimeError::InstantiateFailed(_)),
        "expected preserved InstantiateFailed epoch interrupt on register_filter, got {filter_start_err:?}"
    );

    let mut shape_start_wasm = wat::parse_str(
        r#"
        (module
            (memory (export "memory") 1)
            (func $init
                (loop $hang
                    br $hang
                )
            )
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 4096)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64) i64.const 0)
            (start $init)
        )
        "#,
    )
    .expect("valid shape start wat");
    append_custom_section(
        &mut shape_start_wasm,
        "cc_lb.plugin.v2",
        br#"{"name":"malicious-shape-start","version":"0.1.0","description":"Malicious shape start","usage":"Testing only","hooks":{"shape_anthropic":{"wire_version":2,"description":"shape hook","usage":"call shape","mode":"active"}}}"#,
    );
    append_custom_section(&mut shape_start_wasm, "cc_lb.schema.shape.v2", &shape_fp);
    let Err(shape_start_err) = fast_rt.register_shape(
        RuntimeSlotKey::global("loop-shape-start", RuntimeSlotRole::Shape),
        "loop-shape-start",
        &shape_start_wasm,
    ) else {
        panic!("register_shape must preempt infinite `(start ...)` initializer");
    };
    assert!(
        shape_start_err.is_epoch_interrupt()
            && matches!(shape_start_err, WasmtimeRuntimeError::InstantiateFailed(_)),
        "expected preserved InstantiateFailed epoch interrupt on register_shape, got {shape_start_err:?}"
    );

    // 2. Hot-path infinite loop (passes probe when input_len > 0, hangs when input_len == 0)
    // preempts even after the runtime that originally compiled/cached InstancePre is dropped!
    let response = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            decision: Box::from("accept"),
            reason: Box::from("probe-ok"),
        }]),
    };
    let valid_bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");
    let data_section: String = valid_bytes.iter().map(|b| format!("\\{b:02x}")).collect();
    let packed = (1024u64 << 32) | (valid_bytes.len() as u64);
    let hot_loop_wasm = build_malicious_wat(
        &format!(
            r#"
            (data (i32.const 1024) "{data_section}")
            (func (export "cc_lb_filter_anthropic") (param i32 i32) (result i64)
                local.get 1
                i32.eqz
                if
                    (loop $hang
                        br $hang
                    )
                end
                i64.const {packed}
            )
            "#
        ),
        1,
    );

    // First runtime compiles and caches `InstancePre`, then is dropped!
    let rt1 = Arc::new(
        WasmtimeRuntime::new_with_execution_timeout(
            Default::default(),
            std::time::Duration::from_millis(50),
        )
        .expect("rt1"),
    );
    let slot1 = rt1
        .register_filter(
            RuntimeSlotKey::global("hot-loop-drop", RuntimeSlotRole::Filter),
            "hot-loop-drop",
            &hot_loop_wasm,
        )
        .expect("probe succeeds on non-empty FilterRequest");
    let dispatch_from_rt1 = WasmPluginWireDispatch::from_slot(slot1, rt1.config_arc());
    drop(rt1);

    // Second runtime reuses cached `InstancePre` whose underlying Engine belonged to dropped `rt1`.
    let rt2 = Arc::new(
        WasmtimeRuntime::new_with_execution_timeout(
            Default::default(),
            std::time::Duration::from_millis(50),
        )
        .expect("rt2"),
    );
    let slot2 = rt2
        .register_filter(
            RuntimeSlotKey::global("hot-loop-drop-2", RuntimeSlotRole::Filter),
            "hot-loop-drop-2",
            &hot_loop_wasm,
        )
        .expect("register on rt2 hits cache");
    let dispatch_from_rt2 = WasmPluginWireDispatch::from_slot(slot2, rt2.config_arc());

    let err1 = dispatch_from_rt1
        .call_filter_scoped(&[], <[u8]>::to_vec)
        .expect_err("dispatched cell after rt1 drop must still preempt");
    assert!(err1.is_epoch_interrupt(), "got {err1:?}");

    let err2 = dispatch_from_rt2
        .call_filter_scoped(&[], <[u8]>::to_vec)
        .expect_err("cached InstancePre on rt2 must still preempt");
    assert!(err2.is_epoch_interrupt(), "got {err2:?}");
}
