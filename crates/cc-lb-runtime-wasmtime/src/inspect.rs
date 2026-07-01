//! Load-time wasm inspection — per-slot-kind validation gate.
//!
//! The host walks raw `.wasm` bytes with `wasmparser` to enforce three
//! invariants before the module ever sees the wasmtime engine:
//!
//! 1. **Imports allow-list.** Stage 1 still disallows _every_ host
//!    import. The only legal direction of communication is host → guest
//!    via the declared exports.
//! 2. **Required exports per slot kind.** Every slot requires
//!    `memory`, `cc_lb_alloc`, `cc_lb_free`. On top of that each
//!    [`SlotKind`] adds its own hook exports — e.g. `Shape` requires
//!    both `cc_lb_shape` AND `cc_lb_normalize_error` because the
//!    `UpstreamDialect` trait fuses the two hooks into one host-side
//!    plugin. Signature validation is deferred to instantiate-time via
//!    `instance.get_typed_func`.
//! 3. **Schema hashes.** Each hook ships a `cc_lb.schema.<kind>.v1`
//!    custom section holding the 32-byte BLAKE3 of the matching
//!    `cc_lb.wire.v1.<kind>.rkyv` tag. Every required section must be
//!    present AND match the host's expected hash byte-for-byte;
//!    mismatch rejects the module before compilation.
//!
//! `wasmtime::Module::custom_sections` does NOT round-trip through
//! `precompile_module → Module::deserialize`, so this inspection runs
//! against the raw `.wasm` bytes.
//!
//! Section + tag names are sourced from
//! [`cc_lb_plugin_types::schema`] so the host, the
//! `cc-lb-pdk-wasmtime-macros` codegen, and this gate share a single
//! source of truth.

use crate::error::WasmtimeRuntimeError;
use cc_lb_plugin_types::schema as wire_schema;
use wasmparser::{ExternalKind, Parser, Payload};

/// Re-exported filter wire-schema tag — kept for callers that used the
/// Phase 1 single-hook constant. New callers should use
/// [`cc_lb_plugin_types::schema::WIRE_SCHEMA_TAG_FILTER`] directly or
/// route through [`SlotKind`].
pub const WIRE_SCHEMA_TAG: &[u8] = wire_schema::WIRE_SCHEMA_TAG_FILTER;

const PLUGIN_META_SECTION: &str = "cc_lb.plugin.v1";
const REQUIRED_MEMORY_EXPORT: &str = "memory";
const ALWAYS_REQUIRED_FUNC_EXPORTS: &[&str] = &["cc_lb_alloc", "cc_lb_free"];

/// What kind of hook surface a given plugin slot fills.
///
/// One [`crate::PluginSlot`] occupies exactly one variant — a single
/// `.wasm` plugin cannot simultaneously be a filter AND a dialect.
/// Hosts pick the kind from the manifest before calling
/// [`inspect_wasm`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotKind {
    /// Filter slot — implements `FilterPlugin`. Exports `cc_lb_filter`.
    Filter,
    /// Shape slot — implements `UpstreamDialect`. Exports both
    /// `cc_lb_shape` (request shaping) AND `cc_lb_normalize_error`
    /// (error body normalisation) because the trait fuses them.
    Shape,
    /// Observe slot — implements `ObservabilityHook`. Exports
    /// `cc_lb_observe`.
    Observe,
}

impl SlotKind {
    /// Hook function exports required for this slot kind, in addition
    /// to [`ALWAYS_REQUIRED_FUNC_EXPORTS`].
    pub fn required_hook_exports(self) -> &'static [&'static str] {
        match self {
            SlotKind::Filter => &["cc_lb_filter"],
            SlotKind::Shape => &["cc_lb_shape", "cc_lb_normalize_error"],
            SlotKind::Observe => &["cc_lb_observe"],
        }
    }

    /// `(section name, expected wire-schema tag bytes)` pairs that must
    /// all be present + match for this slot kind.
    pub fn required_schemas(self) -> &'static [(&'static str, &'static [u8])] {
        match self {
            SlotKind::Filter => &[(
                wire_schema::SECTION_FILTER,
                wire_schema::WIRE_SCHEMA_TAG_FILTER,
            )],
            SlotKind::Shape => &[
                (
                    wire_schema::SECTION_SHAPE,
                    wire_schema::WIRE_SCHEMA_TAG_SHAPE,
                ),
                (
                    wire_schema::SECTION_NORMALIZE_ERROR,
                    wire_schema::WIRE_SCHEMA_TAG_NORMALIZE_ERROR,
                ),
            ],
            SlotKind::Observe => &[(
                wire_schema::SECTION_OBSERVE,
                wire_schema::WIRE_SCHEMA_TAG_OBSERVE,
            )],
        }
    }

    fn label(self) -> &'static str {
        match self {
            SlotKind::Filter => "filter",
            SlotKind::Shape => "shape",
            SlotKind::Observe => "observe",
        }
    }
}

/// Outcome of [`inspect_wasm`].
///
/// `schema_hashes` carries one entry per required schema section in
/// the same order [`SlotKind::required_schemas`] returns them — the
/// caller stashes them on the [`crate::PluginCell`] for downstream
/// telemetry parity.
#[derive(Debug, Clone)]
pub struct ModuleInspection {
    pub slot_kind: SlotKind,
    pub schema_hashes: Vec<(&'static str, [u8; 32])>,
    /// Raw JSON bytes from `cc_lb.plugin.v1` if the plugin shipped
    /// one. Optional — present only for diagnostics, not part of the
    /// trust contract.
    pub plugin_metadata: Option<Vec<u8>>,
}

impl ModuleInspection {
    /// Primary hash — first entry from [`Self::schema_hashes`]. Useful
    /// for callers that historically stored a single fingerprint
    /// (e.g. [`crate::PluginCell::schema_hash`]).
    pub fn primary_schema_hash(&self) -> [u8; 32] {
        self.schema_hashes
            .first()
            .map(|(_, h)| *h)
            .expect("required_schemas() guarantees at least one entry per SlotKind")
    }
}

/// Walk `wasm` bytes once, enforcing the three invariants in the
/// module docs against the requested [`SlotKind`]. The check is purely
/// structural and never executes guest code.
pub fn inspect_wasm(kind: SlotKind, wasm: &[u8]) -> Result<ModuleInspection, WasmtimeRuntimeError> {
    let required_schemas = kind.required_schemas();
    let required_hook_exports = kind.required_hook_exports();

    let mut observed_sections: std::collections::HashMap<&'static str, [u8; 32]> =
        std::collections::HashMap::with_capacity(required_schemas.len());
    let mut plugin_metadata: Option<Vec<u8>> = None;
    let mut found_func_exports: Vec<String> =
        Vec::with_capacity(ALWAYS_REQUIRED_FUNC_EXPORTS.len() + required_hook_exports.len());
    let mut found_memory_export = false;

    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|e| WasmtimeRuntimeError::ModuleRejected {
            reason: format!("wasm parse error: {e}"),
        })?;

        match payload {
            Payload::ImportSection(imports) => {
                let count = imports.into_iter().count();
                if count > 0 {
                    return Err(WasmtimeRuntimeError::ModuleRejected {
                        reason: format!(
                            "plugin module imports {count} item(s); Stage 1 disallows every host import",
                        ),
                    });
                }
            }
            Payload::ExportSection(exports) => {
                for export in exports {
                    let export = export.map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                        reason: format!("invalid export entry: {e}"),
                    })?;
                    match export.kind {
                        ExternalKind::Func => {
                            let needed = ALWAYS_REQUIRED_FUNC_EXPORTS
                                .iter()
                                .chain(required_hook_exports.iter())
                                .any(|n| *n == export.name);
                            if needed {
                                found_func_exports.push(export.name.to_owned());
                            }
                        }
                        ExternalKind::Memory if export.name == REQUIRED_MEMORY_EXPORT => {
                            found_memory_export = true;
                        }
                        _ => {}
                    }
                }
            }
            Payload::CustomSection(section) => {
                let name = section.name();
                if name == PLUGIN_META_SECTION {
                    plugin_metadata = Some(section.data().to_vec());
                    continue;
                }
                if let Some((section_name, _)) = required_schemas.iter().find(|(n, _)| *n == name) {
                    let data = section.data();
                    if data.len() != 32 {
                        return Err(WasmtimeRuntimeError::ModuleRejected {
                            reason: format!(
                                "`{section_name}` section is {} bytes; expected 32",
                                data.len()
                            ),
                        });
                    }
                    let mut buf = [0u8; 32];
                    buf.copy_from_slice(data);
                    observed_sections.insert(section_name, buf);
                }
            }
            _ => {}
        }
    }

    if !found_memory_export {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: format!("missing required export `{REQUIRED_MEMORY_EXPORT}` (Memory)"),
        });
    }
    for needed in ALWAYS_REQUIRED_FUNC_EXPORTS
        .iter()
        .chain(required_hook_exports.iter())
    {
        if !found_func_exports.iter().any(|n| n == needed) {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "missing required function export `{needed}` for slot kind `{}`",
                    kind.label()
                ),
            });
        }
    }

    let mut schema_hashes = Vec::with_capacity(required_schemas.len());
    for (section_name, tag) in required_schemas {
        let observed = observed_sections
            .get(section_name)
            .copied()
            .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "missing `{section_name}` custom section for slot kind `{}`",
                    kind.label()
                ),
            })?;
        let expected = blake3::hash(tag);
        if observed != *expected.as_bytes() {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "`{section_name}` hash mismatch (host expects {} but plugin shipped {})",
                    hex32(expected.as_bytes()),
                    hex32(&observed),
                ),
            });
        }
        schema_hashes.push((*section_name, observed));
    }

    Ok(ModuleInspection {
        slot_kind: kind,
        schema_hashes,
        plugin_metadata,
    })
}

fn hex32(bytes: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_section_bytes() -> Vec<u8> {
        blake3::hash(wire_schema::WIRE_SCHEMA_TAG_FILTER)
            .as_bytes()
            .to_vec()
    }
    fn shape_section_bytes() -> Vec<u8> {
        blake3::hash(wire_schema::WIRE_SCHEMA_TAG_SHAPE)
            .as_bytes()
            .to_vec()
    }
    fn normalize_error_section_bytes() -> Vec<u8> {
        blake3::hash(wire_schema::WIRE_SCHEMA_TAG_NORMALIZE_ERROR)
            .as_bytes()
            .to_vec()
    }
    fn observe_section_bytes() -> Vec<u8> {
        blake3::hash(wire_schema::WIRE_SCHEMA_TAG_OBSERVE)
            .as_bytes()
            .to_vec()
    }

    fn wat_with_custom_sections(wat: &str, sections: &[(&str, &[u8])]) -> Vec<u8> {
        let mut module = wat::parse_str(wat).expect("valid wat");
        for (name, data) in sections {
            append_custom_section(&mut module, name, data);
        }
        module
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

    fn shape_plugin_wat() -> &'static str {
        r#"
        (module
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_shape") (param i32 i32) (result i64) i64.const 0)
            (func (export "cc_lb_normalize_error") (param i32 i32) (result i64) i64.const 0)
        )
        "#
    }

    fn observe_plugin_wat() -> &'static str {
        r#"
        (module
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_observe") (param i32 i32) (result i64) i64.const 0)
        )
        "#
    }

    #[test]
    fn accepts_filter_plugin() {
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[
                (wire_schema::SECTION_FILTER, &filter_section_bytes()),
                ("cc_lb.plugin.v1", br#"{"name":"x","version":"0.0.1"}"#),
            ],
        );
        let inspection = inspect_wasm(SlotKind::Filter, &bytes).expect("filter plugin OK");
        assert_eq!(inspection.slot_kind, SlotKind::Filter);
        assert_eq!(inspection.schema_hashes.len(), 1);
        assert_eq!(inspection.primary_schema_hash().len(), 32);
        assert!(inspection.plugin_metadata.is_some());
    }

    #[test]
    fn accepts_shape_plugin_with_both_sections() {
        let bytes = wat_with_custom_sections(
            shape_plugin_wat(),
            &[
                (wire_schema::SECTION_SHAPE, &shape_section_bytes()),
                (
                    wire_schema::SECTION_NORMALIZE_ERROR,
                    &normalize_error_section_bytes(),
                ),
            ],
        );
        let inspection = inspect_wasm(SlotKind::Shape, &bytes).expect("shape plugin OK");
        assert_eq!(inspection.slot_kind, SlotKind::Shape);
        assert_eq!(inspection.schema_hashes.len(), 2);
    }

    #[test]
    fn accepts_observe_plugin() {
        let bytes = wat_with_custom_sections(
            observe_plugin_wat(),
            &[(wire_schema::SECTION_OBSERVE, &observe_section_bytes())],
        );
        let inspection = inspect_wasm(SlotKind::Observe, &bytes).expect("observe plugin OK");
        assert_eq!(inspection.slot_kind, SlotKind::Observe);
        assert_eq!(inspection.schema_hashes.len(), 1);
    }

    #[test]
    fn rejects_filter_without_schema_section() {
        let bytes = wat::parse_str(filter_plugin_wat()).unwrap();
        let err = inspect_wasm(SlotKind::Filter, &bytes).expect_err("missing section");
        let msg = format!("{err}");
        assert!(msg.contains(wire_schema::SECTION_FILTER), "got: {msg}");
    }

    #[test]
    fn rejects_filter_wrong_hash() {
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[(wire_schema::SECTION_FILTER, &[0u8; 32])],
        );
        let err = inspect_wasm(SlotKind::Filter, &bytes).expect_err("bad hash");
        let msg = format!("{err}");
        assert!(msg.contains("hash mismatch"), "got: {msg}");
    }

    #[test]
    fn rejects_shape_missing_normalize_error_section() {
        let bytes = wat_with_custom_sections(
            shape_plugin_wat(),
            &[(wire_schema::SECTION_SHAPE, &shape_section_bytes())],
        );
        let err = inspect_wasm(SlotKind::Shape, &bytes).expect_err("missing ne section");
        let msg = format!("{err}");
        assert!(
            msg.contains(wire_schema::SECTION_NORMALIZE_ERROR),
            "got: {msg}"
        );
    }

    #[test]
    fn rejects_shape_missing_normalize_error_export() {
        let bytes = wat_with_custom_sections(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_shape") (param i32 i32) (result i64) i64.const 0)
            )
            "#,
            &[
                (wire_schema::SECTION_SHAPE, &shape_section_bytes()),
                (
                    wire_schema::SECTION_NORMALIZE_ERROR,
                    &normalize_error_section_bytes(),
                ),
            ],
        );
        let err = inspect_wasm(SlotKind::Shape, &bytes).expect_err("missing ne export");
        let msg = format!("{err}");
        assert!(msg.contains("cc_lb_normalize_error"), "got: {msg}");
    }

    #[test]
    fn rejects_imports_for_any_kind() {
        let bytes = wat_with_custom_sections(
            r#"
            (module
                (import "env" "host_log" (func (param i32 i32)))
                (memory (export "memory") 1)
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
                (func (export "cc_lb_free") (param i32 i32 i32))
                (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const 0)
            )
            "#,
            &[(wire_schema::SECTION_FILTER, &filter_section_bytes())],
        );
        let err = inspect_wasm(SlotKind::Filter, &bytes).expect_err("import rejected");
        let msg = format!("{err}");
        assert!(msg.contains("disallows every host import"), "got: {msg}");
    }

    #[test]
    fn rejects_missing_alloc_or_free() {
        let bytes = wat_with_custom_sections(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
                (func (export "cc_lb_filter") (param i32 i32) (result i64) i64.const 0)
            )
            "#,
            &[(wire_schema::SECTION_FILTER, &filter_section_bytes())],
        );
        let err = inspect_wasm(SlotKind::Filter, &bytes).expect_err("missing free");
        let msg = format!("{err}");
        assert!(msg.contains("cc_lb_free"), "got: {msg}");
    }

    #[test]
    fn filter_module_rejected_for_shape_slot() {
        // A filter-only plugin presented to a Shape slot must be
        // rejected — the shape exports and sections are simply absent.
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[(wire_schema::SECTION_FILTER, &filter_section_bytes())],
        );
        let err = inspect_wasm(SlotKind::Shape, &bytes).expect_err("kind mismatch");
        let msg = format!("{err}");
        assert!(
            msg.contains("cc_lb_shape") || msg.contains(wire_schema::SECTION_SHAPE),
            "got: {msg}"
        );
    }
}
