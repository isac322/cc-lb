use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use cc_lb_plugin_wire::handshake::{
    HANDSHAKE_SCHEMA_VERSION_V1, HandshakeAccept, HandshakeError, HandshakeOffer,
};
use cc_lb_plugin_wire::limits::{
    HANDSHAKE_FUEL, HANDSHAKE_OUTPUT_MAX_BYTES, HANDSHAKE_WALL_MS, IMPLEMENTED_FUNCTIONS_MAX,
    VERSION_MAX, VERSION_MIN,
};
use cc_lb_plugin_wire::v1::{
    build_signer::BuildSignerFn, normalize_error::NormalizeErrorFn, observe::ObserveFn,
    on_unauthorized::OnUnauthorizedFn, shape::ShapeFn, sign::SignFn,
};
use cc_lb_plugin_wire::v3::filter::FilterFn;
use cc_lb_plugin_wire::wire_function::{WireFunction, all_wire_functions};
use cc_lb_storage_api::PluginSlot;
use extism::{Manifest, PluginBuilder, Wasm};
use thiserror::Error;

const HANDSHAKE_EXPORT: &str = "cc_lb_handshake";
const ENVELOPE_VERSION_V1: u32 = 1;

/// Returns the slots whose wire function the plugin claims to implement.
/// The result is deterministically sorted, deduplicated, and ignores unknown
/// function names so newer plugins remain forward-compatible with this host.
pub fn slot_set_from_handshake(implemented_functions: &BTreeSet<String>) -> Vec<PluginSlot> {
    let mut slots = BTreeSet::new();
    for name in implemented_functions {
        if let Some(slot) = wire_function_to_slot(name) {
            slots.insert(slot);
        }
    }
    slots.into_iter().collect()
}

fn wire_function_to_slot(name: &str) -> Option<PluginSlot> {
    match name {
        "filter" => Some(PluginSlot::Router),
        "shape" => Some(PluginSlot::Shape),
        "observe" => Some(PluginSlot::ObservabilityHook),
        _ => None,
    }
}

pub fn build_offer(host_caps: &BTreeSet<String>) -> HandshakeOffer {
    let mut function_versions = BTreeMap::new();
    for (name, versions) in wire_function_versions() {
        function_versions.insert(name.to_owned(), versions.to_vec());
    }

    HandshakeOffer {
        handshake_schema_version: HANDSHAKE_SCHEMA_VERSION_V1,
        envelope_version: ENVELOPE_VERSION_V1,
        function_versions,
        host_capabilities: host_caps.clone(),
    }
}

pub fn execute_handshake(
    plugin_bytes: &[u8],
    offer: &HandshakeOffer,
) -> Result<HandshakeAccept, HandshakeExecutionError> {
    offer.validate()?;
    metrics::counter!("cc_lb_plugin_handshake_total").increment(1);

    let manifest = Manifest::new([Wasm::data(plugin_bytes.to_vec())])
        .with_timeout(Duration::from_millis(HANDSHAKE_WALL_MS))
        .disallow_all_hosts();
    let mut plugin = PluginBuilder::new(&manifest)
        .with_wasi(false)
        .with_cache_disabled()
        .with_fuel_limit(HANDSHAKE_FUEL)
        .build()
        .map_err(|source| HandshakeExecutionError::Instantiate {
            reason: source.to_string(),
        })?;

    if !plugin.function_exists(HANDSHAKE_EXPORT) {
        return Err(HandshakeExecutionError::MissingHandshakeExport);
    }

    let request =
        serde_json::to_string(offer).map_err(|source| HandshakeExecutionError::SerializeOffer {
            reason: source.to_string(),
        })?;
    let response = plugin
        .call::<&str, String>(HANDSHAKE_EXPORT, request.as_str())
        .map_err(|source| classify_call_error(source.to_string()))?;

    if response.len() > HANDSHAKE_OUTPUT_MAX_BYTES {
        return Err(HandshakeExecutionError::OutputTooLarge {
            bytes: response.len(),
            max: HANDSHAKE_OUTPUT_MAX_BYTES,
        });
    }

    let accept: HandshakeAccept = serde_json::from_str(&response).map_err(|source| {
        HandshakeExecutionError::DecodeAccept {
            reason: source.to_string(),
        }
    })?;
    accept.validate_against_offer(offer)?;
    validate_accept_shape(&accept, offer)?;
    cross_check_implemented_exports(&plugin, &accept)?;

    Ok(accept)
}

fn classify_call_error(reason: String) -> HandshakeExecutionError {
    let lower = reason.to_ascii_lowercase();
    if lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("deadline")
        || lower.contains("fuel")
    {
        HandshakeExecutionError::Timeout
    } else {
        HandshakeExecutionError::Call { reason }
    }
}

fn wire_function_versions() -> [(&'static str, &'static [u32]); 7] {
    [
        (
            <ShapeFn as WireFunction>::NAME,
            <ShapeFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <NormalizeErrorFn as WireFunction>::NAME,
            <NormalizeErrorFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <BuildSignerFn as WireFunction>::NAME,
            <BuildSignerFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <SignFn as WireFunction>::NAME,
            <SignFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <OnUnauthorizedFn as WireFunction>::NAME,
            <OnUnauthorizedFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <ObserveFn as WireFunction>::NAME,
            <ObserveFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
        (
            <FilterFn as WireFunction>::NAME,
            <FilterFn as WireFunction>::SUPPORTED_VERSIONS,
        ),
    ]
}

fn validate_accept_shape(
    accept: &HandshakeAccept,
    offer: &HandshakeOffer,
) -> Result<(), HandshakeExecutionError> {
    if accept.implemented_functions.len() > IMPLEMENTED_FUNCTIONS_MAX {
        return Err(HandshakeExecutionError::ImplementedFunctionCountExceeded {
            count: accept.implemented_functions.len(),
            max: IMPLEMENTED_FUNCTIONS_MAX,
        });
    }

    for function in &accept.implemented_functions {
        if !offer.function_versions.contains_key(function) {
            return Err(HandshakeExecutionError::ImplementedUnknownFunction {
                function: function.clone(),
            });
        }
    }

    for (function, version) in &accept.plugin_supported {
        if !offer.function_versions.contains_key(function) {
            return Err(HandshakeExecutionError::SupportedUnknownFunction {
                function: function.clone(),
            });
        }
        for &supported in version {
            if !(VERSION_MIN..=VERSION_MAX).contains(&supported) {
                return Err(HandshakeExecutionError::SupportedVersionOutOfRange {
                    function: function.clone(),
                    version: supported,
                    min: VERSION_MIN,
                    max: VERSION_MAX,
                });
            }
        }
    }

    for (function, chosen) in &accept.chosen_versions {
        if !accept.implemented_functions.contains(function) {
            return Err(HandshakeExecutionError::ChosenFunctionNotImplemented {
                function: function.clone(),
            });
        }
        let Some(supported) = accept.plugin_supported.get(function) else {
            return Err(HandshakeExecutionError::ChosenVersionNotSupported {
                function: function.clone(),
                version: *chosen,
            });
        };
        if !supported.contains(chosen) {
            return Err(HandshakeExecutionError::ChosenVersionNotSupported {
                function: function.clone(),
                version: *chosen,
            });
        }
    }

    Ok(())
}

fn cross_check_implemented_exports(
    plugin: &extism::Plugin,
    accept: &HandshakeAccept,
) -> Result<(), HandshakeExecutionError> {
    for function in &accept.implemented_functions {
        if !plugin.function_exists(function) {
            return Err(HandshakeExecutionError::DeclaredFunctionMissing {
                function: function.clone(),
            });
        }
    }
    for function in all_wire_functions() {
        if plugin.function_exists(function) && !accept.implemented_functions.contains(*function) {
            return Err(HandshakeExecutionError::UndeclaredExport {
                function: (*function).to_owned(),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum HandshakeExecutionError {
    #[error("handshake validation failed: {0}")]
    Validation(#[from] HandshakeError),
    #[error("handshake plugin instantiation failed: {reason}")]
    Instantiate { reason: String },
    #[error("plugin does not export cc_lb_handshake")]
    MissingHandshakeExport,
    #[error("handshake offer serialization failed: {reason}")]
    SerializeOffer { reason: String },
    #[error("handshake call failed: {reason}")]
    Call { reason: String },
    #[error("handshake call exceeded timeout/fuel budget")]
    Timeout,
    #[error("handshake output size {bytes} exceeds maximum {max}")]
    OutputTooLarge { bytes: usize, max: usize },
    #[error("handshake accept decode failed: {reason}")]
    DecodeAccept { reason: String },
    #[error("implemented function count {count} exceeds maximum {max}")]
    ImplementedFunctionCountExceeded { count: usize, max: usize },
    #[error("implemented unknown function: {function}")]
    ImplementedUnknownFunction { function: String },
    #[error("supported unknown function: {function}")]
    SupportedUnknownFunction { function: String },
    #[error(
        "supported version {version} for function {function} outside valid range [{min}, {max}]"
    )]
    SupportedVersionOutOfRange {
        function: String,
        version: u32,
        min: u32,
        max: u32,
    },
    #[error("chosen function not listed as implemented: {function}")]
    ChosenFunctionNotImplemented { function: String },
    #[error("chosen version {version} for function {function} not listed as plugin-supported")]
    ChosenVersionNotSupported { function: String, version: u32 },
    #[error("declared function missing wasm export: {function}")]
    DeclaredFunctionMissing { function: String },
    #[error("undeclared wire function export present: {function}")]
    UndeclaredExport { function: String },
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use cc_lb_plugin_wire::handshake::HandshakeError;
    use serde_json::json;

    use super::*;

    #[test]
    fn slot_set_from_handshake_maps_filter_shape_observe() {
        let fns: BTreeSet<String> = ["filter", "shape", "observe"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(
            slot_set_from_handshake(&fns),
            vec![
                PluginSlot::Router,
                PluginSlot::ObservabilityHook,
                PluginSlot::Shape,
            ],
        );
    }

    #[test]
    fn slot_set_from_handshake_handles_partial_exports() {
        let only_shape: BTreeSet<String> = ["shape".to_owned()].into_iter().collect();
        assert_eq!(
            slot_set_from_handshake(&only_shape),
            vec![PluginSlot::Shape]
        );

        let only_filter: BTreeSet<String> = ["filter".to_owned()].into_iter().collect();
        assert_eq!(
            slot_set_from_handshake(&only_filter),
            vec![PluginSlot::Router],
        );
    }

    #[test]
    fn slot_set_from_handshake_is_empty_when_no_slot_functions_exported() {
        let empty: BTreeSet<String> = BTreeSet::new();
        assert!(slot_set_from_handshake(&empty).is_empty());

        let unrelated: BTreeSet<String> = ["sign".to_owned(), "build_signer".to_owned()]
            .into_iter()
            .collect();
        assert!(slot_set_from_handshake(&unrelated).is_empty());
    }

    #[test]
    fn slot_set_from_handshake_ignores_unknown_function_names_for_forward_compat() {
        let mixed: BTreeSet<String> = ["filter", "future_slot_v9000"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(slot_set_from_handshake(&mixed), vec![PluginSlot::Router]);
    }

    #[test]
    fn build_offer_lists_v1_wire_functions_and_host_capabilities() {
        let host_caps = BTreeSet::from(["streaming".to_owned(), "storage".to_owned()]);

        let offer = build_offer(&host_caps);

        assert_eq!(offer.handshake_schema_version, HANDSHAKE_SCHEMA_VERSION_V1);
        assert_eq!(offer.envelope_version, ENVELOPE_VERSION_V1);
        assert_eq!(offer.host_capabilities, host_caps);
        for (name, versions) in wire_function_versions() {
            assert_eq!(offer.function_versions.get(name), Some(&versions.to_vec()));
        }
        offer.validate().expect("host offer is valid");
    }

    #[test]
    fn execute_handshake_accepts_valid_plugin_and_checks_export() {
        let offer = build_offer(&BTreeSet::from(["streaming".to_owned()]));
        let accept = accept_json(
            &["shape"],
            &[("shape", &[1])],
            &[("shape", 1)],
            &["streaming"],
        );
        let wasm = handshake_module(&accept, &["shape"], false);

        let actual = execute_handshake(&wasm, &offer).expect("handshake succeeds");

        assert!(actual.implemented_functions.contains("shape"));
        assert_eq!(actual.chosen_versions.get("shape"), Some(&1));
    }

    #[test]
    fn execute_handshake_rejects_downgrade() {
        let mut offer = build_offer(&BTreeSet::new());
        offer
            .function_versions
            .insert("shape".to_owned(), vec![1, 2, 3]);
        let accept = accept_json(&["shape"], &[("shape", &[1, 2, 3])], &[("shape", 1)], &[]);
        let wasm = handshake_module(&accept, &["shape"], false);

        let err = execute_handshake(&wasm, &offer).expect_err("downgrade rejected");

        match err {
            HandshakeExecutionError::Validation(HandshakeError::DowngradeAttempt { .. }) => {}
            other => panic!("expected downgrade error, got {other:?}"),
        }
    }

    #[test]
    fn execute_handshake_rejects_implemented_function_without_export() {
        let offer = build_offer(&BTreeSet::new());
        let accept = accept_json(&["shape"], &[("shape", &[1])], &[("shape", 1)], &[]);
        let wasm = handshake_module(&accept, &[], false);

        let err = execute_handshake(&wasm, &offer).expect_err("missing export rejected");

        match err {
            HandshakeExecutionError::DeclaredFunctionMissing { function } => {
                assert_eq!(function, "shape");
            }
            other => panic!("expected missing export, got {other:?}"),
        }
    }

    #[test]
    fn execute_handshake_rejects_user_host_imports() {
        let offer = build_offer(&BTreeSet::new());
        let accept = accept_json(&[], &[], &[], &[]);
        let wasm = handshake_module(&accept, &[], true);

        let err = execute_handshake(&wasm, &offer).expect_err("host import rejected");

        match err {
            HandshakeExecutionError::Instantiate { .. } | HandshakeExecutionError::Call { .. } => {}
            other => panic!("expected purity failure, got {other:?}"),
        }
    }

    fn accept_json(
        implemented: &[&str],
        supported: &[(&str, &[u32])],
        chosen: &[(&str, u32)],
        required_caps: &[&str],
    ) -> String {
        let implemented_functions: BTreeSet<_> =
            implemented.iter().map(|name| name.to_string()).collect();
        let plugin_supported: BTreeMap<_, _> = supported
            .iter()
            .map(|(name, versions)| (name.to_string(), versions.to_vec()))
            .collect();
        let chosen_versions: BTreeMap<_, _> = chosen
            .iter()
            .map(|(name, version)| (name.to_string(), *version))
            .collect();
        let required_capabilities: BTreeSet<_> = required_caps
            .iter()
            .map(|capability| capability.to_string())
            .collect();

        json!({
            "handshake_schema_version": HANDSHAKE_SCHEMA_VERSION_V1,
            "envelope_version": ENVELOPE_VERSION_V1,
            "chosen_versions": chosen_versions,
            "plugin_supported": plugin_supported,
            "implemented_functions": implemented_functions,
            "required_capabilities": required_capabilities,
        })
        .to_string()
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
}
