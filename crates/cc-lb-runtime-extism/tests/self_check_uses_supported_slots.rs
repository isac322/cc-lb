use cc_lb_core::TestClock;
use cc_lb_plugin_wire::self_check::SelfCheckStatus;
use cc_lb_runtime_extism::self_check::execute_self_check;
use cc_lb_storage_api::PluginSlot;
use serde_json::json;

#[test]
fn router_supported_slot_passes_only_filter_to_self_check() {
    let wasm = request_asserting_self_check_module(
        &["filter"],
        &[
            "shape",
            "observe",
            "normalize_error",
            "build_signer",
            "sign",
            "on_unauthorized",
        ],
    );

    let clock = TestClock::new_at_secs(1_800_000_000);

    let response = execute_self_check(&wasm, &[PluginSlot::Router], &clock)
        .expect("router self-check receives only filter");

    assert_eq!(response.status, SelfCheckStatus::Success);
    assert!(response.failures.is_empty());
}

#[test]
fn router_and_shape_supported_slots_pass_filter_and_shape_to_self_check() {
    let wasm = request_asserting_self_check_module(
        &["filter", "shape"],
        &[
            "observe",
            "normalize_error",
            "build_signer",
            "sign",
            "on_unauthorized",
        ],
    );

    let clock = TestClock::new_at_secs(1_800_000_000);

    let response = execute_self_check(&wasm, &[PluginSlot::Router, PluginSlot::Shape], &clock)
        .expect("router plus shape self-check receives filter and shape");

    assert_eq!(response.status, SelfCheckStatus::Success);
    assert!(response.failures.is_empty());
}

#[test]
fn empty_supported_slots_skip_self_check_invocation() {
    let wasm = trapping_self_check_module();

    let clock = TestClock::new_at_secs(1_800_000_000);

    let response =
        execute_self_check(&wasm, &[], &clock).expect("empty slots skip self-check invocation");

    assert_eq!(response.status, SelfCheckStatus::Success);
    assert!(response.failures.is_empty());
    assert_eq!(response.completed_at, 1_800_000_000);
}

fn request_asserting_self_check_module(required: &[&str], forbidden: &[&str]) -> Vec<u8> {
    let success = json!({"status": "success", "failures": [], "completed_at": 1}).to_string();
    let failure = json!({
        "status": "failure",
        "failures": [{"stage": "wire_function_test", "message": "unexpected functions_to_test"}],
        "completed_at": 1,
    })
    .to_string();
    let success_helper = bytes_helper("success_out", success.as_bytes());
    let failure_helper = bytes_helper("failure_out", failure.as_bytes());

    let mut helpers = String::new();
    let mut checks = String::new();
    for (index, function) in required.iter().enumerate() {
        let helper = format!("contains_required_{index}");
        helpers.push_str(&contains_helper(&helper, quoted(function).as_bytes()));
        checks.push_str(&format!(
            r#"
  (if (i32.eqz (call ${helper}))
    (then
      (local.set $out (call $failure_out))
      (local.set $out_len (i64.const {failure_len}))))
"#,
            failure_len = failure.len()
        ));
    }
    for (index, function) in forbidden.iter().enumerate() {
        let helper = format!("contains_forbidden_{index}");
        helpers.push_str(&contains_helper(&helper, quoted(function).as_bytes()));
        checks.push_str(&format!(
            r#"
  (if (call ${helper})
    (then
      (local.set $out (call $failure_out))
      (local.set $out_len (i64.const {failure_len}))))
"#,
            failure_len = failure.len()
        ));
    }

    let wat = format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
  {success_helper}
  {failure_helper}
  {helpers}
  (func (export "cc_lb_self_check") (result i32)
    (local $out i64)
    (local $out_len i64)
    (local.set $out (call $success_out))
    (local.set $out_len (i64.const {success_len}))
{checks}
    (call $output_set (local.get $out) (local.get $out_len))
    (i32.const 0))
  (func (export "filter") (result i32)
    (i32.const 0))
  (func (export "shape") (result i32)
    (i32.const 0))
  (func (export "observe") (result i32)
    (i32.const 0)))
"#,
        success_len = success.len(),
    );
    wat::parse_str(&wat).expect("self-check request assertion wat parses")
}

fn trapping_self_check_module() -> Vec<u8> {
    wat::parse_str(
        r#"
(module
  (func (export "cc_lb_self_check") (result i32)
    unreachable
    (i32.const 0)))
"#,
    )
    .expect("trapping self-check wat parses")
}

fn quoted(value: &str) -> String {
    format!("\"{value}\"")
}

fn contains_helper(name: &str, marker: &[u8]) -> String {
    let mut comparisons = String::new();
    for (offset, byte) in marker.iter().enumerate() {
        comparisons.push_str(&format!(
            r#"
      (if (i32.ne (call $input_load_u8 (i64.add (local.get $i) (i64.const {offset}))) (i32.const {byte}))
        (then (local.set $matched (i32.const 0))))
"#,
        ));
    }
    format!(
        r#"
(func ${name} (result i32)
  (local $input_len i64)
  (local $i i64)
  (local $matched i32)
  (local.set $input_len (call $input_length))
  (if (i64.lt_u (local.get $input_len) (i64.const {marker_len}))
    (then (return (i32.const 0))))
  (loop $scan
    (local.set $matched (i32.const 1))
{comparisons}
    (if (local.get $matched)
      (then (return (i32.const 1))))
    (local.set $i (i64.add (local.get $i) (i64.const 1)))
    (br_if $scan (i64.le_u (i64.add (local.get $i) (i64.const {marker_len})) (local.get $input_len))))
  (i32.const 0))
"#,
        marker_len = marker.len()
    )
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
