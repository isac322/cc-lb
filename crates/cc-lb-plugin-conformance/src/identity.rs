use std::collections::BTreeSet;

use cc_lb_plugin_wire::identity::{CC_LB_PLUGIN_MAGIC, CC_LB_PLUGIN_SECTION_NAME, PluginIdentity};
use cc_lb_plugin_wire::limits;
use serde_json::Value;
use thiserror::Error;
use wasmparser::{ExternalKind, Parser, Payload};

const ABI_ENVELOPE_VERSION_V1: u64 = 1;
const REQUIRED_LIFECYCLE_EXPORTS: [&str; 2] = ["cc_lb_handshake", "cc_lb_self_check"];
const IDENTITY_FIELDS: [&str; 4] = ["abi_envelope", "magic", "plugin_name", "plugin_version"];

pub fn read(wasm: &[u8]) -> Result<IdentityReport, IdentityError> {
    let static_checks = run_static_checks(wasm);
    let identity = cc_lb_runtime_protocol::identity::read_identity(wasm)?;

    Ok(IdentityReport {
        identity,
        static_checks,
    })
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityReport {
    pub identity: PluginIdentity,
    pub static_checks: Vec<StaticCheck>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaticCheck {
    CustomSectionExactlyOnce { pass: bool, detail: Option<String> },
    CustomSectionSizeWithinLimit { pass: bool, detail: Option<String> },
    JsonFieldsExactlyFour { pass: bool, detail: Option<String> },
    IdentityFieldsWellFormed { pass: bool, detail: Option<String> },
    PluginNameRegexCompliant { pass: bool, detail: Option<String> },
    PluginVersionLengthCompliant { pass: bool, detail: Option<String> },
    AllRequiredExportsPresent { pass: bool, detail: Option<String> },
    ExtismCanInstantiate { pass: bool, detail: Option<String> },
    NoWasiImports { pass: bool, detail: Option<String> },
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error(transparent)]
    Read(#[from] cc_lb_runtime_protocol::identity::IdentityReadError),
}

#[derive(Default)]
struct WasmFacts {
    identity_sections: Vec<Vec<u8>>,
    function_exports: BTreeSet<String>,
    import_modules: BTreeSet<String>,
    parse_error: Option<String>,
}

impl WasmFacts {
    fn parse_error(&self) -> Result<(), String> {
        match &self.parse_error {
            Some(error) => Err(format!("invalid wasm: {error}")),
            None => Ok(()),
        }
    }

    fn single_identity_payload(&self) -> Result<&[u8], String> {
        self.parse_error()?;
        match self.identity_sections.as_slice() {
            [payload] => Ok(payload.as_slice()),
            [] => Err(format!(
                "missing {CC_LB_PLUGIN_SECTION_NAME} custom section"
            )),
            sections => Err(format!(
                "{CC_LB_PLUGIN_SECTION_NAME} custom section appears {} times",
                sections.len()
            )),
        }
    }
}

fn run_static_checks(wasm: &[u8]) -> Vec<StaticCheck> {
    let facts = collect_wasm_facts(wasm);
    let checks = [
        custom_section_exactly_once(&facts),
        custom_section_size_within_limit(&facts),
        json_fields_exactly_four(&facts),
        identity_fields_well_formed(&facts),
        plugin_name_regex_compliant(&facts),
        plugin_version_length_compliant(&facts),
        all_required_exports_present(&facts),
        extism_can_instantiate(wasm),
        no_wasi_imports(&facts),
    ];

    checks.into_iter().collect()
}

fn collect_wasm_facts(wasm: &[u8]) -> WasmFacts {
    let mut facts = WasmFacts::default();

    for payload in Parser::new(0).parse_all(wasm) {
        match payload {
            Ok(Payload::CustomSection(section)) => {
                if section.name() == CC_LB_PLUGIN_SECTION_NAME {
                    facts.identity_sections.push(section.data().to_vec());
                }
            }
            Ok(Payload::ExportSection(section)) => {
                for export in section {
                    match export {
                        Ok(export) if export.kind == ExternalKind::Func => {
                            facts.function_exports.insert(export.name.to_owned());
                        }
                        Ok(_) => {}
                        Err(error) => {
                            facts.parse_error = Some(error.to_string());
                            return facts;
                        }
                    }
                }
            }
            Ok(Payload::ImportSection(section)) => {
                for import in section.into_imports() {
                    match import {
                        Ok(import) => {
                            facts.import_modules.insert(import.module.to_owned());
                        }
                        Err(error) => {
                            facts.parse_error = Some(error.to_string());
                            return facts;
                        }
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                facts.parse_error = Some(error.to_string());
                return facts;
            }
        }
    }

    facts
}

fn custom_section_exactly_once(facts: &WasmFacts) -> StaticCheck {
    let result = facts.parse_error().and_then(|()| {
        if facts.identity_sections.len() == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one {CC_LB_PLUGIN_SECTION_NAME} custom section, found {}",
                facts.identity_sections.len()
            ))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::CustomSectionExactlyOnce { pass, detail }
}

fn custom_section_size_within_limit(facts: &WasmFacts) -> StaticCheck {
    let result = facts.parse_error().and_then(|()| {
        if facts.identity_sections.is_empty() {
            return Err(format!(
                "missing {CC_LB_PLUGIN_SECTION_NAME} custom section"
            ));
        }
        let oversized = facts
            .identity_sections
            .iter()
            .map(Vec::len)
            .filter(|size| *size > limits::CUSTOM_SECTION_MAX_SIZE)
            .collect::<Vec<_>>();
        if oversized.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "custom section payload size(s) {oversized:?} exceed max {} bytes",
                limits::CUSTOM_SECTION_MAX_SIZE
            ))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::CustomSectionSizeWithinLimit { pass, detail }
}

fn json_fields_exactly_four(facts: &WasmFacts) -> StaticCheck {
    let result = section_json_value(facts).and_then(|value| {
        let object = value
            .as_object()
            .ok_or_else(|| "identity payload top level is not a JSON object".to_owned())?;
        let mut keys = object.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        if keys == IDENTITY_FIELDS && object.len() == limits::CUSTOM_SECTION_FIELD_COUNT {
            Ok(())
        } else {
            Err(format!(
                "identity JSON fields must be exactly {IDENTITY_FIELDS:?}; found {keys:?}"
            ))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::JsonFieldsExactlyFour { pass, detail }
}

fn identity_fields_well_formed(facts: &WasmFacts) -> StaticCheck {
    let result = section_json_value(facts).and_then(|value| {
        let mut errors = Vec::new();

        match magic_field(&value) {
            Ok(magic) if magic == CC_LB_PLUGIN_MAGIC => {}
            Ok(magic) => errors.push(format!(
                "magic must equal {CC_LB_PLUGIN_MAGIC:?}; found {magic:?}"
            )),
            Err(error) => errors.push(error),
        }

        match value.get("abi_envelope").and_then(Value::as_u64) {
            Some(ABI_ENVELOPE_VERSION_V1) => {}
            Some(found) => errors.push(format!(
                "abi_envelope must be {ABI_ENVELOPE_VERSION_V1}; found {found}"
            )),
            None => errors.push("abi_envelope must be an unsigned integer".to_owned()),
        }

        if let Err(error) = string_field(&value, "plugin_name") {
            errors.push(error);
        }
        if let Err(error) = string_field(&value, "plugin_version") {
            errors.push(error);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::IdentityFieldsWellFormed { pass, detail }
}

fn plugin_name_regex_compliant(facts: &WasmFacts) -> StaticCheck {
    let result = section_json_value(facts).and_then(|value| {
        let name = string_field(&value, "plugin_name")?;
        if plugin_name_matches_wire_pattern(name) {
            Ok(())
        } else {
            Err(format!(
                "plugin_name {name:?} must match {} and be at most {} bytes",
                limits::PLUGIN_NAME_PATTERN,
                limits::PLUGIN_NAME_MAX_BYTES
            ))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::PluginNameRegexCompliant { pass, detail }
}

fn plugin_version_length_compliant(facts: &WasmFacts) -> StaticCheck {
    let result = section_json_value(facts).and_then(|value| {
        let version = string_field(&value, "plugin_version")?;
        let byte_len = version.len();
        if !version.is_empty() && byte_len <= limits::PLUGIN_VERSION_MAX_BYTES {
            Ok(())
        } else {
            Err(format!(
                "plugin_version must be non-empty and at most {} bytes; found {byte_len} bytes",
                limits::PLUGIN_VERSION_MAX_BYTES
            ))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::PluginVersionLengthCompliant { pass, detail }
}

fn all_required_exports_present(facts: &WasmFacts) -> StaticCheck {
    let result = facts.parse_error().and_then(|()| {
        let missing = REQUIRED_LIFECYCLE_EXPORTS
            .iter()
            .filter(|name| !facts.function_exports.contains(**name))
            .copied()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!("missing wasm function export(s): {missing:?}"))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::AllRequiredExportsPresent { pass, detail }
}

fn extism_can_instantiate(wasm: &[u8]) -> StaticCheck {
    let result = cc_lb_runtime_protocol::build_plugin(
        wasm,
        limits::HANDSHAKE_WALL_MS,
        limits::HANDSHAKE_FUEL,
    )
    .map(|_| ())
    .map_err(|error| error.to_string());
    let (pass, detail) = static_check_result(result);
    StaticCheck::ExtismCanInstantiate { pass, detail }
}

fn no_wasi_imports(facts: &WasmFacts) -> StaticCheck {
    let result = facts.parse_error().and_then(|()| {
        let forbidden = facts
            .import_modules
            .iter()
            .filter(|module| is_wasi_module(module))
            .map(String::as_str)
            .collect::<Vec<_>>();
        if forbidden.is_empty() {
            Ok(())
        } else {
            Err(format!("forbidden WASI import module(s): {forbidden:?}"))
        }
    });
    let (pass, detail) = static_check_result(result);
    StaticCheck::NoWasiImports { pass, detail }
}

fn static_check_result(result: Result<(), String>) -> (bool, Option<String>) {
    match result {
        Ok(()) => (true, None),
        Err(detail) => (false, Some(detail)),
    }
}

fn section_json_value(facts: &WasmFacts) -> Result<Value, String> {
    serde_json::from_slice(facts.single_identity_payload()?)
        .map_err(|error| format!("malformed identity JSON: {error}"))
}

fn magic_field(value: &Value) -> Result<[u8; 8], String> {
    let array = value
        .get("magic")
        .and_then(Value::as_array)
        .ok_or_else(|| "magic must be an array of 8 bytes".to_owned())?;
    if array.len() != CC_LB_PLUGIN_MAGIC.len() {
        return Err(format!(
            "magic must contain {} bytes; found {}",
            CC_LB_PLUGIN_MAGIC.len(),
            array.len()
        ));
    }

    let mut magic = [0u8; 8];
    for (index, byte) in array.iter().enumerate() {
        let value = byte
            .as_u64()
            .ok_or_else(|| format!("magic[{index}] must be an integer byte"))?;
        if value > u8::MAX as u64 {
            return Err(format!("magic[{index}] exceeds u8 max: {value}"));
        }
        magic[index] = value as u8;
    }
    Ok(magic)
}

fn string_field<'a>(value: &'a Value, field: &'static str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{field} must be a string"))
}

fn plugin_name_matches_wire_pattern(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= limits::PLUGIN_NAME_MAX_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_' || *byte == b'-'
        })
}

fn is_wasi_module(module: &str) -> bool {
    module == "wasi_snapshot_preview1"
        || module == "wasi_snapshot_preview2"
        || module == "wasi_unstable"
        || module.starts_with("wasi:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_identity_and_all_static_checks_pass_for_synthetic_module() {
        let wasm = wasm_with_identity_payload(valid_identity_payload("test-plugin").as_bytes());

        let report = read(&wasm).expect("synthetic module identity reads");

        assert_eq!(report.identity.magic, CC_LB_PLUGIN_MAGIC);
        assert_eq!(report.identity.abi_envelope, 1);
        assert_eq!(report.identity.plugin_name, "test-plugin");
        assert_eq!(report.identity.plugin_version, "1.0.0");
        assert_eq!(report.static_checks.len(), 9);
        assert_all_variants_present(&report.static_checks);
        assert!(report.static_checks.iter().all(check_passed));
    }

    #[test]
    fn malformed_json_fails_json_check_and_read() {
        let wasm = wasm_with_identity_payload(b"{");

        let error = read(&wasm).expect_err("malformed JSON is rejected");
        assert!(format!("{error}").contains("malformed"));

        let checks = run_static_checks(&wasm);
        let check = find_check(&checks, |check| {
            matches!(check, StaticCheck::JsonFieldsExactlyFour { .. })
        });
        assert!(!check_passed(check));
    }

    #[test]
    fn missing_section_fails_custom_section_check_and_read() {
        let wasm = wasm_with_exports_and_optional_identity(None, &REQUIRED_EXPORTS_FOR_TESTS);

        let error = read(&wasm).expect_err("missing custom section is rejected");
        assert!(format!("{error}").contains("missing"));

        let checks = run_static_checks(&wasm);
        let check = find_check(&checks, |check| {
            matches!(check, StaticCheck::CustomSectionExactlyOnce { .. })
        });
        assert!(!check_passed(check));
    }

    #[test]
    fn oversized_name_fails_name_check_and_read() {
        let oversized_name = format!("a{}", "b".repeat(limits::PLUGIN_NAME_MAX_BYTES));
        let payload = valid_identity_payload(&oversized_name);
        let wasm = wasm_with_identity_payload(payload.as_bytes());

        let error = read(&wasm).expect_err("oversized name is rejected");
        assert!(format!("{error}").contains("invalid plugin identity"));

        let checks = run_static_checks(&wasm);
        let check = find_check(&checks, |check| {
            matches!(check, StaticCheck::PluginNameRegexCompliant { .. })
        });
        assert!(!check_passed(check));
    }

    const REQUIRED_EXPORTS_FOR_TESTS: [&str; 2] = ["cc_lb_handshake", "cc_lb_self_check"];

    fn valid_identity_payload(name: &str) -> String {
        json!({
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": 1,
            "plugin_name": name,
            "plugin_version": "1.0.0",
        })
        .to_string()
    }

    fn wasm_with_identity_payload(payload: &[u8]) -> Vec<u8> {
        wasm_with_exports_and_optional_identity(Some(payload), &REQUIRED_EXPORTS_FOR_TESTS)
    }

    fn wasm_with_exports_and_optional_identity(
        identity_payload: Option<&[u8]>,
        exports: &[&str],
    ) -> Vec<u8> {
        let mut wasm = Vec::from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
        if let Some(payload) = identity_payload {
            push_custom_section(&mut wasm, CC_LB_PLUGIN_SECTION_NAME, payload);
        }
        if !exports.is_empty() {
            push_type_section(&mut wasm);
            push_function_section(&mut wasm, exports.len());
            push_export_section(&mut wasm, exports);
            push_code_section(&mut wasm, exports.len());
        }
        wasm
    }

    fn push_custom_section(wasm: &mut Vec<u8>, name: &str, payload: &[u8]) {
        let mut section = Vec::new();
        push_name(&mut section, name);
        section.extend_from_slice(payload);
        push_section(wasm, 0, &section);
    }

    fn push_type_section(wasm: &mut Vec<u8>) {
        let mut section = Vec::new();
        encode_u32(1, &mut section);
        section.push(0x60);
        encode_u32(0, &mut section);
        encode_u32(0, &mut section);
        push_section(wasm, 1, &section);
    }

    fn push_function_section(wasm: &mut Vec<u8>, function_count: usize) {
        let mut section = Vec::new();
        encode_u32(function_count as u32, &mut section);
        section.extend(std::iter::repeat_n(0, function_count));
        push_section(wasm, 3, &section);
    }

    fn push_export_section(wasm: &mut Vec<u8>, exports: &[&str]) {
        let mut section = Vec::new();
        encode_u32(exports.len() as u32, &mut section);
        for (index, export) in exports.iter().enumerate() {
            push_name(&mut section, export);
            section.push(0x00);
            encode_u32(index as u32, &mut section);
        }
        push_section(wasm, 7, &section);
    }

    fn push_code_section(wasm: &mut Vec<u8>, function_count: usize) {
        let mut section = Vec::new();
        encode_u32(function_count as u32, &mut section);
        for _ in 0..function_count {
            encode_u32(2, &mut section);
            section.push(0x00);
            section.push(0x0b);
        }
        push_section(wasm, 10, &section);
    }

    fn push_name(output: &mut Vec<u8>, name: &str) {
        encode_u32(name.len() as u32, output);
        output.extend_from_slice(name.as_bytes());
    }

    fn push_section(wasm: &mut Vec<u8>, id: u8, payload: &[u8]) {
        wasm.push(id);
        encode_u32(payload.len() as u32, wasm);
        wasm.extend_from_slice(payload);
    }

    fn encode_u32(mut value: u32, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn check_passed(check: &StaticCheck) -> bool {
        match check {
            StaticCheck::CustomSectionExactlyOnce { pass, .. }
            | StaticCheck::CustomSectionSizeWithinLimit { pass, .. }
            | StaticCheck::JsonFieldsExactlyFour { pass, .. }
            | StaticCheck::IdentityFieldsWellFormed { pass, .. }
            | StaticCheck::PluginNameRegexCompliant { pass, .. }
            | StaticCheck::PluginVersionLengthCompliant { pass, .. }
            | StaticCheck::AllRequiredExportsPresent { pass, .. }
            | StaticCheck::ExtismCanInstantiate { pass, .. }
            | StaticCheck::NoWasiImports { pass, .. } => *pass,
        }
    }

    fn find_check<F>(checks: &[StaticCheck], predicate: F) -> &StaticCheck
    where
        F: Fn(&StaticCheck) -> bool,
    {
        checks
            .iter()
            .find(|check| predicate(check))
            .expect("check exists")
    }

    fn assert_all_variants_present(checks: &[StaticCheck]) {
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::CustomSectionExactlyOnce { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::CustomSectionSizeWithinLimit { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::JsonFieldsExactlyFour { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::IdentityFieldsWellFormed { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::PluginNameRegexCompliant { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::PluginVersionLengthCompliant { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::AllRequiredExportsPresent { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::ExtismCanInstantiate { .. }))
        );
        assert!(
            checks
                .iter()
                .any(|check| matches!(check, StaticCheck::NoWasiImports { .. }))
        );
    }
}
