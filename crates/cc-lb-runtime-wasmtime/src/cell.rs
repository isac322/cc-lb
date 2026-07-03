//! [`PluginCell`] and [`PluginSlot`] — the hot-swap unit.
//!
//! See RFC §Hot-swap. The `ArcSwap<PluginCell>` per slot is what makes
//! a plugin replacement lock-free for in-flight readers.

use std::sync::Arc;

use arc_swap::ArcSwap;
use wasmtime::InstancePre;

use crate::engine::HostState;
use crate::inspect::SlotKind;

/// Immutable hot-swappable plugin payload.
///
/// `schema_hash` is the **primary** 32-byte BLAKE3 fingerprint of the
/// slot's first wire-schema tag (e.g. filter for a [`SlotKind::Filter`]
/// slot, shape for a [`SlotKind::Shape`] slot), lifted out of the
/// matching `cc_lb.schema.<kind>.v1` custom section by
/// [`crate::inspect::inspect_wasm`]. Telemetry uses it to detect
/// host↔guest version drift across hot-swaps; slots with multiple
/// required schemas (Shape carries both shape + normalize_error) still
/// verify every section at load time but expose only the primary one
/// here.
pub struct PluginCell {
    pub version_id: u64,
    pub instance_pre: Arc<InstancePre<HostState>>,
    pub schema_hash: [u8; 32],
    pub memory_max_pages: u32,
    /// Stable label used as the `plugin` dimension on every RFC-0001
    /// plugin metric. Owned by the cell so `execute_call` can emit
    /// without threading slot / registration state through every
    /// hook function signature.
    pub plugin_name: Arc<str>,
    /// Content-identity hash covering every input that affects
    /// runtime behaviour: wasm bytes, per-instance memory ceiling, plus a bumpable
    /// [`crate::module::VALIDATION_POLICY_VERSION`] constant so
    /// tightening validation rules force a recompile even when the
    /// wasm + knobs are unchanged. `register()` short-circuits when
    /// this hash matches, so it MUST NOT elide any input the
    /// runtime actually uses.
    pub content_hash: [u8; 32],
}

impl std::fmt::Debug for PluginCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginCell")
            .field("version_id", &self.version_id)
            .field("schema_hash_hex", &format_hex(&self.schema_hash))
            .field("content_hash_hex", &format_hex(&self.content_hash))
            .field("memory_max_pages", &self.memory_max_pages)
            .finish_non_exhaustive()
    }
}

fn format_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

/// A slot of pluggable behavior, identified by name + principal scope.
///
/// `current` is what hot-swap rotates. Old [`PluginCell`] arcs are dropped
/// naturally as in-flight readers release their `ArcSwap::load_full()`
/// handles. Per-worker thread_local store reclamation is a separate concern
/// handled in [`crate::cache`].
///
/// `kind` is set at registration time and never changes — a slot
/// registered as [`SlotKind::Filter`] cannot later be replaced by a
/// shape or observe plugin. [`WasmtimeRuntime::register_*`][crate::WasmtimeRuntime]
/// rejects a kind switch with `ModuleRejected`.
pub struct PluginSlot {
    pub name: String,
    pub kind: SlotKind,
    pub current: ArcSwap<PluginCell>,
}

impl PluginSlot {
    pub fn new(name: impl Into<String>, kind: SlotKind, initial: PluginCell) -> Self {
        Self {
            name: name.into(),
            kind,
            current: ArcSwap::from_pointee(initial),
        }
    }
}
