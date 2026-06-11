mod common;

use std::collections::BTreeMap;

use cc_lb_plugin_api::{FilterError, PerCandidateReason, UpstreamCandidate};
use cc_lb_runtime_extism::ExtismRuntime;
use serde_json::json;
use uuid::Uuid;

#[test]
fn filter_plugin_happy_path_maps_wire_v3_response() {
    let accepted = common::candidate_wire();
    let rejected = candidate_with_id("22222222-2222-2222-2222-222222222222");
    let response = json!({
        "_v": 1,
        "kept_upstream_ids": [accepted.upstream_id],
        "reason": "quota available; plugin policy",
        "per_candidate_reasons": [
            {
                "upstream_id": accepted.upstream_id.to_string(),
                "kept": true,
                "reason": "quota available"
            },
            {
                "upstream_id": rejected.upstream_id.to_string(),
                "kept": false,
                "reason": "plugin policy"
            }
        ]
    })
    .to_string();
    let accepted_id = accepted.upstream_id.to_string();
    let rejected_id = rejected.upstream_id.to_string();
    let mut fixture = common::fixture(
        "filter-happy",
        &filter_module_requiring_input_markers(
            &[
                b"principal-test",
                accepted_id.as_bytes(),
                rejected_id.as_bytes(),
            ],
            &response,
            r#"{"_v":1,"kept_upstream_ids":[],"reason":"","per_candidate_reasons":[]}"#,
        ),
        BTreeMap::new(),
    );
    fixture.manifest.wire_version = Some(3);
    let runtime = ExtismRuntime::new();
    let plugin_id =
        Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("fixture UUID parses");
    let (filter, staged) = runtime
        .instantiate_filter_for(
            "principal-test",
            plugin_id,
            "filter-happy",
            &fixture.manifest,
        )
        .expect("filter plugin instantiates");
    runtime
        .commit_staged(vec![staged])
        .expect("staged filter commits");

    let output = filter
        .filter(
            &common::ctx(),
            &common::principal(),
            &[accepted.clone(), rejected.clone()],
        )
        .expect("filter call succeeds");

    assert_eq!(filter.plugin_id(), plugin_id);
    assert_eq!(filter.plugin_name(), "filter-happy");
    assert_eq!(output.kept_upstream_ids, vec![accepted.upstream_id]);
    assert_eq!(
        output.per_candidate_reasons,
        vec![
            PerCandidateReason {
                upstream_id: accepted.upstream_id,
                kept: true,
                reason: "quota available".to_owned(),
            },
            PerCandidateReason {
                upstream_id: rejected.upstream_id,
                kept: false,
                reason: "plugin policy".to_owned(),
            }
        ]
    );
    assert_eq!(output.reason, "quota available; plugin policy");
}

#[test]
fn filter_plugin_maps_wasm_trap_to_filter_trap() {
    let mut fixture = common::fixture("filter-trap", trap_filter_module(), BTreeMap::new());
    fixture.manifest.wire_version = Some(3);
    let runtime = ExtismRuntime::new();
    let filter = runtime
        .instantiate_filter(&fixture.manifest)
        .expect("filter plugin instantiates");

    let error = filter
        .filter(
            &common::ctx(),
            &common::principal(),
            &[common::candidate_wire()],
        )
        .expect_err("trap maps to FilterError::Trap");

    assert!(matches!(error, FilterError::Trap { .. }));
}

#[test]
fn filter_plugin_maps_invalid_encoding_to_filter_runtime() {
    let mut fixture = common::fixture(
        "filter-invalid-encoding",
        &common::module_with_functions(&[("filter", "not-json")]),
        BTreeMap::new(),
    );
    fixture.manifest.wire_version = Some(3);
    let runtime = ExtismRuntime::new();
    let filter = runtime
        .instantiate_filter(&fixture.manifest)
        .expect("filter plugin instantiates");

    let error = filter
        .filter(
            &common::ctx(),
            &common::principal(),
            &[common::candidate_wire()],
        )
        .expect_err("malformed output maps to FilterError::Runtime");

    assert!(matches!(error, FilterError::Runtime { .. }));
}

fn candidate_with_id(id: &str) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: Uuid::parse_str(id).expect("fixture UUID parses"),
        name: "anthropic-fallback".to_owned(),
        ..common::candidate_wire()
    }
}

fn trap_filter_module() -> &'static str {
    r#"
(module
  (func (export "filter") (result i32)
    unreachable
    (i32.const 0)))
"#
}

fn filter_module_requiring_input_markers(
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

    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
{helpers}
  (func (export "filter") (result i32)
    (local $out i64)
    (local $out_len i64)
    (local.set $out (call $success_out))
    (local.set $out_len (i64.const {success_len}))
{marker_checks}
    (call $output_set (local.get $out) (local.get $out_len))
    (i32.const 0))
)
"#,
        success_len = success_output.len()
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
{stores}    (local.get $ptr))
"#,
        len = bytes.len()
    )
}
