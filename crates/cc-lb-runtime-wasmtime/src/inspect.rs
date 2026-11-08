//! Load-time wasm inspection — metadata + fingerprint validation gate.
//!
//! The host walks raw `.wasm` bytes with `wasmparser` to enforce three
//! invariants before the module ever sees the wasmtime engine:
//!
//! 1. **Imports allow-list.** Stage 1 still disallows _every_ host
//!    import. The only legal direction of communication is host → guest
//!    via the declared exports.
//! 2. **Required exports per declared hook.** Every module requires
//!    `memory`, `cc_lb_alloc`, `cc_lb_free`. On top of that each hook
//!    listed in `cc_lb.plugin.v1` or `cc_lb.plugin.v2` requires its matching export.
//!    Signature validation of `cc_lb_alloc` and the hook export is
//!    deferred to call time via `instance.get_typed_func`.
//! 3. **Schema fingerprints.** Each declared hook ships a
//!    `cc_lb.schema.<hook>.v<N>` custom section holding the 32-byte
//!    [`cc_lb_plugin_wire::WireSchema::FINGERPRINT`] for the matching
//!    host wire type.
//!
//! `wasmtime::Module::custom_sections` does NOT round-trip through
//! `precompile_module → Module::deserialize`, so this inspection runs
//! against the raw `.wasm` bytes.
//!
//! Section + tag names are sourced from
//! [`cc_lb_plugin_wire::schema`] so the host, the
//! `cc-lb-pdk-wasmtime-macros` codegen, and this gate share a single
//! source of truth.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::WasmtimeRuntimeError;
use cc_lb_plugin_wire::metadata::PluginMetadata;
use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireSchema, WireVersion};
use wasmparser::{ExternalKind, Parser, Payload};

const PLUGIN_META_SECTION: &str = "cc_lb.plugin.v1";
const QUALIFIED_PLUGIN_META_SECTION: &str = "cc_lb.plugin.v2";
const REQUIRED_MEMORY_EXPORT: &str = "memory";
const ALWAYS_REQUIRED_FUNC_EXPORTS: &[&str] = &["cc_lb_alloc", "cc_lb_free"];
const SHAPE_OWNED_HOOKS: [HookKind; 5] = [
    HookKind::Shape,
    HookKind::TransformResponse,
    HookKind::TransformSseEvent,
    HookKind::TransformRealtimeFrame,
    HookKind::ShapeEnvelopeGemini,
];

/// Outcome of [`inspect_wasm`].
#[derive(Debug, Clone)]
pub struct ModuleInspection {
    pub metadata: PluginMetadata,
    pub hook_versions: BTreeMap<HookKind, WireVersion>,
    pub hook_fingerprints: BTreeMap<HookKind, [u8; 32]>,
}

enum InspectionScope {
    Slot(HookKind),
    DeclaredHooks,
}

/// Walk `wasm` bytes once, enforcing the structural invariants in the
/// module docs. The check is purely structural and never executes guest
/// code.
pub fn inspect_wasm(kind: HookKind, wasm: &[u8]) -> Result<ModuleInspection, WasmtimeRuntimeError> {
    inspect_wasm_with_scope(InspectionScope::Slot(kind), wasm)
}

/// Inspect an artifact against every metadata-declared hook contract without
/// selecting a runtime slot.
pub fn inspect_wasm_agnostic(wasm: &[u8]) -> Result<ModuleInspection, WasmtimeRuntimeError> {
    inspect_wasm_with_scope(InspectionScope::DeclaredHooks, wasm)
}

fn inspect_wasm_with_scope(
    scope: InspectionScope,
    wasm: &[u8],
) -> Result<ModuleInspection, WasmtimeRuntimeError> {
    let mut observed_sections: HashMap<String, [u8; 32]> = HashMap::new();
    let mut plugin_metadata: Option<Vec<u8>> = None;
    let mut metadata_section_name = PLUGIN_META_SECTION;
    let mut found_func_exports: HashSet<String> = HashSet::new();
    let mut found_memory_export = false;

    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|e| WasmtimeRuntimeError::ModuleRejected {
            reason: format!("wasm parse error: {e}"),
        })?;

        match payload {
            Payload::ImportSection(imports) => {
                let count = imports.into_imports().count();
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
                            found_func_exports.insert(export.name.to_owned());
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
                if name == PLUGIN_META_SECTION || name == QUALIFIED_PLUGIN_META_SECTION {
                    if plugin_metadata.is_some() {
                        return Err(WasmtimeRuntimeError::ModuleRejected {
                            reason: "plugin must contain exactly one metadata section (v1 or v2)"
                                .into(),
                        });
                    }
                    metadata_section_name = if name == QUALIFIED_PLUGIN_META_SECTION {
                        QUALIFIED_PLUGIN_META_SECTION
                    } else {
                        PLUGIN_META_SECTION
                    };
                    plugin_metadata = Some(section.data().to_vec());
                }
                if name.starts_with("cc_lb.schema.") {
                    let data = section.data();
                    if data.len() != 32 {
                        return Err(WasmtimeRuntimeError::ModuleRejected {
                            reason: format!(
                                "`{name}` section is {} bytes; expected 32",
                                data.len()
                            ),
                        });
                    }
                    let mut buf = [0u8; 32];
                    buf.copy_from_slice(data);
                    observed_sections.insert(name.to_owned(), buf);
                }
            }
            _ => {}
        }
    }

    let metadata_bytes =
        plugin_metadata
            .as_deref()
            .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
                reason: format!("missing required `{PLUGIN_META_SECTION}` custom section"),
            })?;
    let metadata = PluginMetadata::parse(metadata_bytes).map_err(|error| {
        WasmtimeRuntimeError::ModuleRejected {
            reason: format!("invalid `{PLUGIN_META_SECTION}` metadata: {error}"),
        }
    })?;

    reject_non_response_noop_modes(&metadata)?;
    validate_export_inventory(&metadata, &found_func_exports, metadata_section_name)?;
    match scope {
        InspectionScope::Slot(kind) => require_hooks_for_slot(kind, &metadata)?,
        InspectionScope::DeclaredHooks => require_declared_hook_contracts(&metadata)?,
    }

    if !found_memory_export {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: format!("missing required export `{REQUIRED_MEMORY_EXPORT}` (Memory)"),
        });
    }
    for needed in ALWAYS_REQUIRED_FUNC_EXPORTS {
        if !found_func_exports.contains(*needed) {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!("missing required function export `{needed}`"),
            });
        }
    }

    let mut hook_versions = BTreeMap::new();
    let mut hook_fingerprints = BTreeMap::new();
    for (hook_name, hook_metadata) in &metadata.hooks {
        let (hook, provider) = HookKind::parse_entrypoint(hook_name).ok_or_else(|| {
            WasmtimeRuntimeError::ModuleRejected {
                reason: format!("unknown hook `{hook_name}` in metadata"),
            }
        })?;
        if hook == HookKind::ShapeEnvelopeGemini
            && hook_name != HookKind::ShapeEnvelopeGemini.as_str()
        {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "hook `{hook_name}` must use the canonical Gemini envelope metadata key `{}`",
                    HookKind::ShapeEnvelopeGemini.as_str()
                ),
            });
        }
        if hook == HookKind::ShapeEnvelopeGemini && provider != Some(ProviderFamily::Gemini) {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: "shape_envelope_gemini is Gemini-only".into(),
            });
        }
        let wire_version = WireVersion::from_u8(hook_metadata.wire_version).ok_or_else(|| {
            WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "hook `{}` declares unsupported wire version {}",
                    hook.as_str(),
                    hook_metadata.wire_version
                ),
            }
        })?;
        if wire_version == WireVersion::V2 && provider.is_none() {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "hook `{hook_name}` wire version 2 requires provider-qualified metadata"
                ),
            });
        }
        let needed_export = provider.map_or_else(
            || hook.export_name(),
            |provider| hook.qualified_export_name(provider),
        );
        if !found_func_exports.contains(needed_export) {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "missing required function export `{needed_export}` for declared hook `{}`",
                    hook.as_str()
                ),
            });
        }

        let section_name = schema_section_name(hook, wire_version);
        let observed = observed_sections
            .get(&section_name)
            .copied()
            .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "missing `{section_name}` custom section for declared hook `{}`",
                    hook.as_str()
                ),
            })?;
        let expected = expected_fingerprint(hook, wire_version).ok_or_else(|| {
            WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "host has no schema for hook `{}` wire version {}",
                    hook.as_str(),
                    wire_version.as_u8()
                ),
            }
        })?;
        if observed != expected {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "`{section_name}` hash mismatch (host expects {} but plugin shipped {})",
                    hex32(&expected),
                    hex32(&observed),
                ),
            });
        }
        hook_versions.insert(hook, wire_version);
        hook_fingerprints.insert(hook, observed);
    }

    Ok(ModuleInspection {
        metadata,
        hook_versions,
        hook_fingerprints,
    })
}

fn require_hooks_for_slot(
    kind: HookKind,
    metadata: &PluginMetadata,
) -> Result<(), WasmtimeRuntimeError> {
    if metadata.hooks.keys().any(|name| {
        HookKind::parse_entrypoint(name).is_some_and(|(hook, _)| match kind {
            HookKind::Filter => hook == HookKind::Filter,
            HookKind::Shape => SHAPE_OWNED_HOOKS.contains(&hook),
            _ => false,
        })
    }) {
        require_declared_hook_contracts(metadata)
    } else {
        match kind {
            HookKind::TransformResponse
            | HookKind::TransformSseEvent
            | HookKind::TransformRealtimeFrame => reject_shape_owned_hook_as_slot(kind),
            _ => require_declared_hook(metadata, kind, "this slot"),
        }
    }
}

fn require_declared_hook_contracts(metadata: &PluginMetadata) -> Result<(), WasmtimeRuntimeError> {
    // Wire v1 shape plugins retain their historical all-hooks contract.
    if metadata.hooks.contains_key(HookKind::Shape.as_str()) {
        for hook in [
            HookKind::Shape,
            HookKind::TransformResponse,
            HookKind::TransformSseEvent,
        ] {
            require_declared_hook(metadata, hook, "legacy shape plugin")?;
        }
    }

    // Legacy bare response hooks remain shape-owned and cannot be admitted
    // without the legacy bare shape hook. Provider-qualified v2 response
    // hooks are intentionally independent.
    for name in metadata.hooks.keys() {
        let Some((hook, provider)) = HookKind::parse_entrypoint(name) else {
            continue;
        };
        if provider.is_none()
            && matches!(
                hook,
                HookKind::TransformResponse | HookKind::TransformSseEvent
            )
            && !metadata.hooks.contains_key(HookKind::Shape.as_str())
        {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "shape-owned hook `{}` requires a declared `shape` hook",
                    hook.as_str()
                ),
            });
        }
    }

    // The Gemini envelope transform is a provider-specific extension of the
    // Gemini shape contract, not an independent shape owner.
    if metadata
        .hooks
        .contains_key(HookKind::ShapeEnvelopeGemini.as_str())
        && !metadata.hooks.contains_key("shape_gemini")
    {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: "shape_envelope_gemini requires a declared shape hook for provider `gemini`"
                .into(),
        });
    }
    Ok(())
}

fn reject_shape_owned_hook_as_slot(hook: HookKind) -> Result<(), WasmtimeRuntimeError> {
    Err(WasmtimeRuntimeError::ModuleRejected {
        reason: format!(
            "hook `{}` is shape-owned and cannot be admitted as a standalone slot",
            hook.as_str()
        ),
    })
}

fn reject_non_response_noop_modes(metadata: &PluginMetadata) -> Result<(), WasmtimeRuntimeError> {
    for (name, hook_metadata) in &metadata.hooks {
        if HookKind::parse_entrypoint(name).is_some_and(|(hook, _)| {
            matches!(
                hook,
                HookKind::Filter | HookKind::Shape | HookKind::ShapeEnvelopeGemini
            )
        }) && hook_metadata.mode.is_noop()
        {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "hook `{}` cannot declare mode `noop`; no-op is only valid for shape-owned response hooks",
                    name,
                ),
            });
        }
    }
    Ok(())
}

fn require_declared_hook(
    metadata: &PluginMetadata,
    hook: HookKind,
    owner: &str,
) -> Result<(), WasmtimeRuntimeError> {
    if metadata.hooks.contains_key(hook.as_str()) {
        return Ok(());
    }
    Err(WasmtimeRuntimeError::ModuleRejected {
        reason: format!(
            "metadata does not declare required `{}` hook for {owner}",
            hook.as_str()
        ),
    })
}

fn validate_export_inventory(
    metadata: &PluginMetadata,
    exports: &HashSet<String>,
    section_name: &str,
) -> Result<(), WasmtimeRuntimeError> {
    for name in metadata.hooks.keys() {
        let Some((hook, provider)) = HookKind::parse_entrypoint(name) else {
            continue;
        };
        if provider.is_some() && section_name != QUALIFIED_PLUGIN_META_SECTION {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "qualified hook `{name}` requires `{QUALIFIED_PLUGIN_META_SECTION}` metadata"
                ),
            });
        }
        if let Some(provider) = provider {
            let legacy_overlaps = provider == cc_lb_plugin_wire::schema::ProviderFamily::Anthropic;
            if legacy_overlaps && metadata.hooks.contains_key(hook.as_str()) {
                return Err(WasmtimeRuntimeError::ModuleRejected {
                    reason: format!(
                        "legacy `{}` overlaps qualified `{name}`; export only one form for this provider",
                        hook.as_str()
                    ),
                });
            }
        }
    }
    for export in exports {
        let Some(name) = export.strip_prefix("cc_lb_") else {
            continue;
        };
        if name == "alloc" || name == "free" {
            continue;
        }
        let reserved = [
            "filter",
            "shape",
            "transform_response",
            "transform_sse_event",
            "shape_envelope_gemini",
        ];
        let reserved = reserved
            .iter()
            .any(|stem| name == *stem || name.starts_with(&format!("{stem}_")));
        if reserved
            && (HookKind::parse_entrypoint(name).is_none() || !metadata.hooks.contains_key(name))
        {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!("unrecognized or undeclared plugin entrypoint `{export}`"),
            });
        }
    }
    Ok(())
}

pub(crate) fn schema_section_name(hook: HookKind, version: WireVersion) -> String {
    format!("{}.{}", hook.section_prefix(), version.as_str())
}

pub(crate) fn expected_fingerprint(hook: HookKind, version: WireVersion) -> Option<[u8; 32]> {
    match (hook, version) {
        (HookKind::Filter, WireVersion::V1) => {
            Some(<cc_lb_plugin_wire::v1::FilterRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::Shape, WireVersion::V1) => {
            Some(<cc_lb_plugin_wire::v1::ShapeRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::TransformResponse, WireVersion::V1) => {
            Some(<cc_lb_plugin_wire::v1::TransformResponseRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::TransformSseEvent, WireVersion::V1) => {
            Some(<cc_lb_plugin_wire::v1::TransformSseEventRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::Filter, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::FilterRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::Shape, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::TransformResponse, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::TransformResponseRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::TransformSseEvent, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::TransformSseEventRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::TransformRealtimeFrame, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::TransformRealtimeFrameRequest as WireSchema>::FINGERPRINT)
        }
        (HookKind::ShapeEnvelopeGemini, WireVersion::V2) => {
            Some(<cc_lb_plugin_wire::v2::ShapeEnvelopeGeminiRequest as WireSchema>::FINGERPRINT)
        }
        _ => None,
    }
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
        expected_fingerprint(HookKind::Filter, WireVersion::V1)
            .expect("filter V1 schema")
            .to_vec()
    }
    fn shape_section_bytes() -> Vec<u8> {
        expected_fingerprint(HookKind::Shape, WireVersion::V1)
            .expect("shape V1 schema")
            .to_vec()
    }
    fn transform_response_section_bytes() -> Vec<u8> {
        expected_fingerprint(HookKind::TransformResponse, WireVersion::V1)
            .expect("transform response V1 schema")
            .to_vec()
    }
    fn transform_sse_event_section_bytes() -> Vec<u8> {
        expected_fingerprint(HookKind::TransformSseEvent, WireVersion::V1)
            .expect("transform SSE V1 schema")
            .to_vec()
    }

    fn metadata_section(hook: &str) -> Vec<u8> {
        format!(
            r#"{{"name":"x","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{{"{hook}":{{"wire_version":1,"description":"{hook} hook","usage":"call {hook}","mode":"active"}}}}}}"#
        )
        .into_bytes()
    }

    fn shape_owned_metadata_section(transform_response_mode: &str, sse_mode: &str) -> Vec<u8> {
        format!(
            r#"{{"name":"x","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{{"shape":{{"wire_version":1,"description":"shape hook","usage":"call shape","mode":"active"}},"transform_response":{{"wire_version":1,"description":"transform_response hook","usage":"call transform_response","mode":"{transform_response_mode}"}},"transform_sse_event":{{"wire_version":1,"description":"transform_sse_event hook","usage":"call transform_sse_event","mode":"{sse_mode}"}}}}}}"#
        )
        .into_bytes()
    }

    fn shape_owned_metadata_without_response_mode() -> Vec<u8> {
        r#"{"name":"x","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{"shape":{"wire_version":1,"description":"shape hook","usage":"call shape","mode":"active"},"transform_response":{"wire_version":1,"description":"transform_response hook","usage":"call transform_response"},"transform_sse_event":{"wire_version":1,"description":"transform_sse_event hook","usage":"call transform_sse_event","mode":"noop"}}}"#
            .as_bytes()
            .to_vec()
    }

    fn shape_noop_metadata_section() -> Vec<u8> {
        r#"{"name":"x","version":"0.0.1","description":"test plugin","usage":"test usage","hooks":{"shape":{"wire_version":1,"description":"shape hook","usage":"call shape","mode":"noop"},"transform_response":{"wire_version":1,"description":"transform_response hook","usage":"call transform_response","mode":"noop"},"transform_sse_event":{"wire_version":1,"description":"transform_sse_event hook","usage":"call transform_sse_event","mode":"noop"}}}"#
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
        )
        "#
    }

    fn shape_owned_plugin_wat() -> &'static str {
        r#"
        (module
            (memory (export "memory") 1)
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 0)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_shape") (param i32 i32) (result i64) i64.const 0)
            (func (export "cc_lb_transform_response") (param i32 i32) (result i64) i64.const 0)
            (func (export "cc_lb_transform_sse_event") (param i32 i32) (result i64) i64.const 0)
        )
        "#
    }

    #[test]
    fn accepts_filter_plugin() {
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Filter, WireVersion::V1),
                    &filter_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &metadata_section("filter")),
            ],
        );
        let inspection = inspect_wasm(HookKind::Filter, &bytes).expect("filter plugin OK");
        assert_eq!(inspection.metadata.name, "x");
        assert_eq!(inspection.hook_fingerprints.len(), 1);
        assert_eq!(inspection.hook_versions[&HookKind::Filter], WireVersion::V1);
    }

    #[test]
    fn accepts_shape_plugin_with_shape_response_hooks_and_noop_modes() {
        let bytes = wat_with_custom_sections(
            shape_owned_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Shape, WireVersion::V1),
                    &shape_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformResponse, WireVersion::V1),
                    &transform_response_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformSseEvent, WireVersion::V1),
                    &transform_sse_event_section_bytes(),
                ),
                (
                    "cc_lb.plugin.v1",
                    &shape_owned_metadata_section("noop", "noop"),
                ),
            ],
        );
        let inspection = inspect_wasm(HookKind::Shape, &bytes).expect("shape plugin OK");
        assert_eq!(inspection.hook_versions[&HookKind::Shape], WireVersion::V1);
        assert_eq!(
            inspection.hook_versions[&HookKind::TransformResponse],
            WireVersion::V1
        );
        assert_eq!(
            inspection.hook_versions[&HookKind::TransformSseEvent],
            WireVersion::V1
        );
    }

    #[test]
    fn rejects_shape_plugin_without_response_hook_metadata() {
        let bytes = wat_with_custom_sections(
            shape_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Shape, WireVersion::V1),
                    &shape_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &metadata_section("shape")),
            ],
        );
        let err = inspect_wasm(HookKind::Shape, &bytes).expect_err("shape response hooks required");
        let msg = format!("{err}");
        assert!(
            msg.contains("transform_response") && msg.contains("shape plugin"),
            "got: {msg}"
        );
    }

    #[test]
    fn rejects_shape_response_hook_without_explicit_mode() {
        let bytes = wat_with_custom_sections(
            shape_owned_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Shape, WireVersion::V1),
                    &shape_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformResponse, WireVersion::V1),
                    &transform_response_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformSseEvent, WireVersion::V1),
                    &transform_sse_event_section_bytes(),
                ),
                (
                    "cc_lb.plugin.v1",
                    &shape_owned_metadata_without_response_mode(),
                ),
            ],
        );
        let err = inspect_wasm(HookKind::Shape, &bytes).expect_err("response mode required");
        let msg = format!("{err}");
        assert!(msg.contains("missing field `mode`"), "got: {msg}");
    }

    #[test]
    fn rejects_noop_mode_on_primary_shape_hook() {
        let bytes = wat_with_custom_sections(
            shape_owned_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Shape, WireVersion::V1),
                    &shape_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformResponse, WireVersion::V1),
                    &transform_response_section_bytes(),
                ),
                (
                    &schema_section_name(HookKind::TransformSseEvent, WireVersion::V1),
                    &transform_sse_event_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &shape_noop_metadata_section()),
            ],
        );
        let err = inspect_wasm(HookKind::Shape, &bytes).expect_err("shape hook cannot be noop");
        let msg = format!("{err}");
        assert!(msg.contains("shape") && msg.contains("noop"), "got: {msg}");
    }

    #[test]
    fn rejects_filter_without_schema_section() {
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[("cc_lb.plugin.v1", &metadata_section("filter"))],
        );
        let err = inspect_wasm(HookKind::Filter, &bytes).expect_err("missing section");
        let msg = format!("{err}");
        assert!(msg.contains("cc_lb.schema.filter.v1"), "got: {msg}");
    }

    #[test]
    fn rejects_filter_wrong_hash() {
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Filter, WireVersion::V1),
                    &[0u8; 32],
                ),
                ("cc_lb.plugin.v1", &metadata_section("filter")),
            ],
        );
        let err = inspect_wasm(HookKind::Filter, &bytes).expect_err("bad hash");
        let msg = format!("{err}");
        assert!(msg.contains("hash mismatch"), "got: {msg}");
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
            &[
                (
                    &schema_section_name(HookKind::Filter, WireVersion::V1),
                    &filter_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &metadata_section("filter")),
            ],
        );
        let err = inspect_wasm(HookKind::Filter, &bytes).expect_err("import rejected");
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
            &[
                (
                    &schema_section_name(HookKind::Filter, WireVersion::V1),
                    &filter_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &metadata_section("filter")),
            ],
        );
        let err = inspect_wasm(HookKind::Filter, &bytes).expect_err("missing free");
        let msg = format!("{err}");
        assert!(msg.contains("cc_lb_free"), "got: {msg}");
    }

    #[test]
    fn filter_module_rejected_for_shape_slot() {
        // A filter-only plugin presented to a Shape slot must be
        // rejected — the shape exports and sections are simply absent.
        let bytes = wat_with_custom_sections(
            filter_plugin_wat(),
            &[
                (
                    &schema_section_name(HookKind::Filter, WireVersion::V1),
                    &filter_section_bytes(),
                ),
                ("cc_lb.plugin.v1", &metadata_section("filter")),
            ],
        );
        let err = inspect_wasm(HookKind::Shape, &bytes).expect_err("kind mismatch");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not declare required `shape`"),
            "got: {msg}"
        );
    }
    #[test]
    fn qualified_v2_shape_contracts_are_checked_per_provider() {
        use cc_lb_plugin_wire::metadata::{HookMetadata, HookMode};

        let hook = |name: &str| HookMetadata {
            wire_version: 2,
            description: format!("{name} hook"),
            usage: format!("call {name}"),
            mode: HookMode::Active,
        };
        let mut hooks = BTreeMap::new();
        hooks.insert("shape_openai".to_owned(), hook("shape_openai"));
        hooks.insert(
            "transform_response_openai".to_owned(),
            hook("transform_response_openai"),
        );
        hooks.insert(
            "transform_sse_event_openai".to_owned(),
            hook("transform_sse_event_openai"),
        );
        let metadata = PluginMetadata {
            name: "qualified".into(),
            version: "0.0.1".into(),
            description: "qualified test".into(),
            usage: "test only".into(),
            hooks,
        };

        assert!(require_declared_hook_contracts(&metadata).is_ok());
    }

    #[test]
    fn envelope_hook_requires_a_gemini_shape_contract() {
        use cc_lb_plugin_wire::metadata::{HookMetadata, HookMode};

        let mut hooks = BTreeMap::new();
        hooks.insert(
            "shape_envelope_gemini".to_owned(),
            HookMetadata {
                wire_version: 2,
                description: "envelope".into(),
                usage: "test only".into(),
                mode: HookMode::Active,
            },
        );
        let metadata = PluginMetadata {
            name: "envelope".into(),
            version: "0.0.1".into(),
            description: "envelope test".into(),
            usage: "test only".into(),
            hooks,
        };

        let error = require_declared_hook_contracts(&metadata)
            .expect_err("envelope cannot be admitted without shape ownership");
        assert!(format!("{error}").contains("shape"));
    }
}
