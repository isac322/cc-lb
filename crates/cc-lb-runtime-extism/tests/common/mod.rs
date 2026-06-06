#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;

use bytes::Bytes;
use cc_lb_plugin_api::{PluginManifest, Principal, PrincipalKind, RequestContext};
use http::{HeaderMap, Method};
use serde_json::{Value, json};
use tempfile::TempDir;

pub struct WasmFixture {
    _dir: TempDir,
    pub manifest: PluginManifest,
}

pub fn fixture(name: &str, wat: &str, metadata: BTreeMap<String, Value>) -> WasmFixture {
    let dir = tempfile::tempdir().expect("tempdir is created");
    let wasm = wat::parse_str(wat).expect("wat parses");
    let artifact = dir.path().join(format!("{name}.wasm"));
    fs::write(&artifact, wasm).expect("wasm fixture is written");
    WasmFixture {
        _dir: dir,
        manifest: PluginManifest {
            name: name.to_owned(),
            artifact: artifact.to_string_lossy().into_owned(),
            config: json!({}),
            metadata,
        },
    }
}

pub fn rewrite_fixture(fixture: &WasmFixture, wat: &str) {
    let wasm = wat::parse_str(wat).expect("wat parses");
    fs::write(&fixture.manifest.artifact, wasm).expect("wasm fixture is rewritten");
}

pub fn metadata(pairs: &[(&str, u64)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), Value::from(*value)))
        .collect()
}

pub fn ctx() -> RequestContext {
    RequestContext {
        request_id: "req-test".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(br#"{"model":"claude-test"}"#),
    }
}

pub fn principal() -> Principal {
    Principal {
        id: "principal-test".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}

pub fn authn_response(principal_id: &str) -> String {
    json!({
        "_version": 1,
        "principal": {
            "id": principal_id,
            "kind": "api_key",
            "claims": {}
        },
        "quotas": {
            "requests_per_window": 10,
            "input_tokens_per_window": 1000,
            "output_tokens_per_window": 1000,
            "window_ms": 60000,
            "allowed_models": ["claude-*"]
        },
        "signer_state": {"key": "test"}
    })
    .to_string()
}

pub fn build_signer_response() -> String {
    json!({
        "_v": 1,
        "signer_state": {"key": "signed"}
    })
    .to_string()
}

pub fn sign_response(header_value: &str) -> String {
    json!({
        "_v": 1,
        "headers": [{
            "name": "x-api-key",
            "value_base64": base64(header_value.as_bytes())
        }]
    })
    .to_string()
}

pub fn route_response() -> String {
    json!({
        "_v": 1,
        "upstream": {
            "kind": "anthropic_direct"
        },
        "dialect": {"kind": "self"}
    })
    .to_string()
}

pub fn route_response_with_upstream_id(upstream_id: &str) -> String {
    json!({
        "_v": 1,
        "upstream": {
            "kind": "anthropic_direct"
        },
        "dialect": {"kind": "self"},
        "upstream_id": upstream_id
    })
    .to_string()
}

pub fn route_response_with_base_url(base_url: &str) -> String {
    let _ = base_url;
    json!({
        "_v": 1,
        "upstream": {
            "kind": "anthropic_direct"
        },
        "dialect": {"kind": "self"}
    })
    .to_string()
}

pub fn shape_response() -> String {
    json!({
        "_v": 1,
        "url": "http://upstream.test/v1/messages",
        "method": "POST",
        "headers": [{
            "name": "content-type",
            "value_base64": base64(b"application/json")
        }],
        "body_base64": base64(br#"{"shaped":true}"#)
    })
    .to_string()
}

pub fn observe_response() -> String {
    json!({"_v": 1}).to_string()
}

pub fn module_with_authn(output: &str) -> String {
    module_with_functions(&[("authenticate", output)])
}

pub fn module_with_functions(outputs: &[(&str, &str)]) -> String {
    let mut helpers = String::new();
    let mut funcs = String::new();
    for (index, (export, output)) in outputs.iter().enumerate() {
        let helper = format!("bytes_{index}");
        helpers.push_str(&bytes_helper(&helper, output.as_bytes()));
        funcs.push_str(&format!(
            r#"
(func (export "{export}") (result i32)
  (local $out i64)
  (local.set $out (call ${helper}))
  (call $output_set (local.get $out) (i64.const {len}))
  (i32.const 0))
"#,
            len = output.len()
        ));
    }
    module(&format!("{helpers}\n{funcs}"))
}

pub fn route_module_requiring_input_markers(
    markers: &[&[u8]],
    success_output: &str,
    failure_output: &str,
) -> String {
    let mut contains_helpers = String::new();
    for (index, marker) in markers.iter().enumerate() {
        contains_helpers.push_str(&contains_helper(index, marker));
    }

    let mut marker_checks = String::new();
    for index in 0..markers.len() {
        marker_checks.push_str(&format!(
            r#"
  (if (i32.eqz (call $contains_{index}))
    (then
      (local.set $out (call $failure_out))
      (local.set $out_len (i64.const {failure_len}))))
"#,
            failure_len = failure_output.len()
        ));
    }

    let helpers = format!(
        "{}{}{}",
        bytes_helper("success_out", success_output.as_bytes()),
        bytes_helper("failure_out", failure_output.as_bytes()),
        contains_helpers
    );

    module_with_extra_env_imports(
        r#"
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
"#,
        &format!(
            r#"
{helpers}
(func (export "route") (result i32)
  (local $out i64)
  (local $out_len i64)
  (local.set $out (call $success_out))
  (local.set $out_len (i64.const {success_len}))
{marker_checks}
  (call $output_set (local.get $out) (local.get $out_len))
  (i32.const 0))
"#,
            success_len = success_output.len()
        ),
    )
}

pub fn host_log_module(output: &str) -> String {
    let helpers = format!(
        "{}{}{}",
        bytes_helper("level", b"info"),
        bytes_helper("message", b"hello"),
        bytes_helper("out", output.as_bytes())
    );
    module(&format!(
        r#"
{helpers}
(func (export "authenticate") (result i32)
  (local $out i64)
  (call $cc_lb_log (call $level) (call $message))
  (local.set $out (call $out))
  (call $output_set (local.get $out) (i64.const {len}))
  (i32.const 0))
"#,
        len = output.len()
    ))
}

pub fn storage_put_module(output: &str, key: &[u8], value: &[u8]) -> String {
    let helpers = format!(
        "{}{}{}",
        bytes_helper("key", key),
        bytes_helper("value", value),
        bytes_helper("out", output.as_bytes())
    );
    module(&format!(
        r#"
{helpers}
(func (export "authenticate") (result i32)
  (local $out i64)
  (call $cc_lb_storage_put (call $key) (call $value))
  (local.set $out (call $out))
  (call $output_set (local.get $out) (i64.const {len}))
  (i32.const 0))
"#,
        len = output.len()
    ))
}

pub fn storage_read_module(missing_output: &str, leaked_output: &str, key: &[u8]) -> String {
    let helpers = format!(
        "{}{}{}",
        bytes_helper("key", key),
        bytes_helper("missing", missing_output.as_bytes()),
        bytes_helper("leaked", leaked_output.as_bytes())
    );
    module(&format!(
        r#"
{helpers}
(func (export "authenticate") (result i32)
  (local $value i64)
  (local $out i64)
  (local.set $value (call $cc_lb_storage_get (call $key)))
  (if (i64.eqz (call $length (local.get $value)))
    (then
      (local.set $out (call $missing))
      (call $output_set (local.get $out) (i64.const {missing_len})))
    (else
      (local.set $out (call $leaked))
      (call $output_set (local.get $out) (i64.const {leaked_len}))))
  (i32.const 0))
"#,
        missing_len = missing_output.len(),
        leaked_len = leaked_output.len()
    ))
}

pub fn reload_old_module(output: &str) -> String {
    let helpers = format!(
        "{}{}{}",
        bytes_helper("release", b"release"),
        bytes_helper("started_key", b"started"),
        bytes_helper("started_value", b"old")
    );
    let output_helper = bytes_helper("old_out", output.as_bytes());
    module(&format!(
        r#"
{helpers}
{output_helper}
(func (export "authenticate") (result i32)
  (local $value i64)
  (local $i i64)
  (local $out i64)
  (call $cc_lb_storage_put (call $started_key) (call $started_value))
  (loop $wait
    (local.set $i (i64.const 0))
    (loop $spin
      (local.set $i (i64.add (local.get $i) (i64.const 1)))
      (br_if $spin (i64.lt_u (local.get $i) (i64.const 100000))))
    (local.set $value (call $cc_lb_storage_get (call $release)))
    (br_if $wait (i64.eqz (call $length (local.get $value)))))
  (local.set $out (call $old_out))
  (call $output_set (local.get $out) (i64.const {len}))
  (i32.const 0))
"#,
        len = output.len()
    ))
}

pub fn lifecycle_counted_old_module(output: &str, hold_after_call: u64) -> String {
    let helpers = format!(
        "{}{}{}",
        bytes_helper("release", b"release"),
        bytes_helper("started_key", b"started"),
        bytes_helper("started_value", b"old")
    );
    let output_helper = bytes_helper("old_out", output.as_bytes());
    module(&format!(
        r#"
{helpers}
{output_helper}
(global $calls (mut i64) (i64.const 0))
(func (export "authenticate") (result i32)
  (local $value i64)
  (local $i i64)
  (local $out i64)
  (global.set $calls (i64.add (global.get $calls) (i64.const 1)))
  (if (i64.ge_u (global.get $calls) (i64.const {hold_after_call}))
    (then
      (call $cc_lb_storage_put (call $started_key) (call $started_value))
      (loop $wait
        (local.set $i (i64.const 0))
        (loop $spin
          (local.set $i (i64.add (local.get $i) (i64.const 1)))
          (br_if $spin (i64.lt_u (local.get $i) (i64.const 100000))))
        (local.set $value (call $cc_lb_storage_get (call $release)))
        (br_if $wait (i64.eqz (call $length (local.get $value)))))))
  (local.set $out (call $old_out))
  (call $output_set (local.get $out) (i64.const {len}))
  (i32.const 0))
"#,
        len = output.len()
    ))
}

pub fn reload_new_module(output: &str) -> String {
    storage_put_module(output, b"release", b"1")
}

pub fn memory_pressure_module() -> String {
    r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (func (export "authenticate") (result i32)
    (drop (call $alloc (i64.const 268435456)))
    unreachable
    (i32.const 0)))
"#
    .to_owned()
}

pub fn infinite_loop_module() -> String {
    r#"
(module
  (func (export "authenticate") (result i32)
    (loop $again
      br $again)
    (i32.const 0)))
"#
    .to_owned()
}

pub fn panic_module() -> String {
    r#"
(module
  (func (export "authenticate") (result i32)
    unreachable
    (i32.const 0)))
"#
    .to_owned()
}

fn module(functions: &str) -> String {
    module_with_extra_env_imports("", functions)
}

fn module_with_extra_env_imports(extra_imports: &str, functions: &str) -> String {
    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/env" "length" (func $length (param i64) (result i64)))
  {extra_imports}
  (import "extism:host/user" "cc_lb_log" (func $cc_lb_log (param i64 i64)))
  (import "extism:host/user" "cc_lb_storage_get" (func $cc_lb_storage_get (param i64) (result i64)))
  (import "extism:host/user" "cc_lb_storage_put" (func $cc_lb_storage_put (param i64 i64)))
  {functions}
)
"#
    )
}

fn contains_helper(index: usize, marker: &[u8]) -> String {
    let mut comparisons = String::new();
    for (offset, byte) in marker.iter().enumerate() {
        comparisons.push_str(&format!(
            r#"
      (if (i32.ne (call $input_load_u8 (i64.add (local.get $i) (i64.const {offset}))) (i32.const {byte}))
        (then (local.set $matched (i32.const 0))))
"#
        ));
    }

    format!(
        r#"
(func $contains_{index} (result i32)
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

fn base64(bytes: &[u8]) -> String {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    STANDARD.encode(bytes)
}
