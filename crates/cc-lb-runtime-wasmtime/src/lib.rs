//! Wasmtime-based [`PluginRuntime`] implementation.
//!
//! Registers slots per hook kind via [`WasmtimeRuntime::register_filter`]
//! / [`register_shape`][WasmtimeRuntime::register_shape] /
//! [`register_observe`][WasmtimeRuntime::register_observe], returning
//! the `Arc<PluginSlot>` callers store in their dynamic view.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md` and
//! `.omo/plans/extism-removal.md`.
#![deny(unsafe_code)]

mod cache;
mod cell;
mod engine;
mod error;
mod inspect;
mod module;
mod plugin;

pub use cache::{
    DEFAULT_ALIGN, call_filter_hook, call_filter_hook_pure, call_filter_hook_stateful,
    call_normalize_error_hook, call_normalize_error_hook_pure, call_normalize_error_hook_stateful,
    call_observe_hook, call_observe_hook_pure, call_observe_hook_stateful, call_shape_hook,
    call_shape_hook_pure, call_shape_hook_stateful,
};
pub use cell::{PluginCell, PluginSlot};
pub use engine::{HostState, HotEngineConfig, build_hot_engine};
pub use error::WasmtimeRuntimeError;
pub use inspect::{ModuleInspection, SlotKind, WIRE_SCHEMA_TAG, inspect_wasm};
pub use module::compile_module;
pub use plugin::{WasmtimeFilterPlugin, WasmtimeObservabilityHookPlugin, WasmtimeUpstreamDialect};

use std::collections::HashMap;
use std::sync::Arc;

#[allow(deprecated)]
use cc_lb_plugin_api::RouterPlugin;
use cc_lb_plugin_api::SlotKey;
use cc_lb_plugin_api::{
    ObservabilityHook, PluginManifest, PluginRuntime, RuntimeError, UpstreamDialect,
};
use parking_lot::RwLock;
use wasmtime::{Engine, Linker};

/// Per-slot registration options.
///
/// `pure = true` (default) maps to the fresh-`Store`-per-call
/// dispatch path. This value is the runtime materialisation of
/// [`cc_lb_plugin_api::PluginManifest::pure`]; the Stage 3 server
/// adapter bridges manifest → options at the
/// `register_*_with` call site so [`PluginCell::pure`] reflects the
/// manifest as of the most recent register.
#[derive(Clone, Copy, Debug)]
pub struct RegisterOptions {
    pub pure: bool,
}

impl Default for RegisterOptions {
    fn default() -> Self {
        Self {
            pure: cc_lb_plugin_api::default_pure(),
        }
    }
}

/// Wasmtime-backed plugin runtime.
pub struct WasmtimeRuntime {
    engine: Arc<Engine>,
    linker: Arc<Linker<HostState>>,
    config: HotEngineConfig,
    slots: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>,
}

impl WasmtimeRuntime {
    pub fn new(config: HotEngineConfig) -> Result<Self, WasmtimeRuntimeError> {
        let engine = build_hot_engine(&config)?;
        let linker = Linker::new(&engine);
        Ok(Self {
            engine: Arc::new(engine),
            linker: Arc::new(linker),
            config,
            slots: RwLock::new(HashMap::new()),
        })
    }

    pub fn with_defaults() -> Result<Self, WasmtimeRuntimeError> {
        Self::new(HotEngineConfig::default())
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn config(&self) -> &HotEngineConfig {
        &self.config
    }

    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    /// Register (or replace) a filter-hook slot using
    /// [`RegisterOptions::default()`] (pure dispatch).
    pub fn register_filter(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register_filter_with(slot_key, name, wasm_bytes, RegisterOptions::default())
    }

    /// Like [`Self::register_filter`] with explicit
    /// [`RegisterOptions`]. Use to opt out of pure dispatch.
    pub fn register_filter_with(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        opts: RegisterOptions,
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Filter, slot_key, name, wasm_bytes, opts)
    }

    /// Register (or replace) a shape-hook slot using
    /// [`RegisterOptions::default()`] (pure dispatch). The plugin
    /// module must export both `cc_lb_shape` and `cc_lb_normalize_error`.
    pub fn register_shape(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register_shape_with(slot_key, name, wasm_bytes, RegisterOptions::default())
    }

    /// Like [`Self::register_shape`] with explicit
    /// [`RegisterOptions`].
    pub fn register_shape_with(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        opts: RegisterOptions,
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Shape, slot_key, name, wasm_bytes, opts)
    }

    /// Register (or replace) an observe-hook slot using
    /// [`RegisterOptions::default()`] (pure dispatch).
    pub fn register_observe(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register_observe_with(slot_key, name, wasm_bytes, RegisterOptions::default())
    }

    /// Like [`Self::register_observe`] with explicit
    /// [`RegisterOptions`].
    pub fn register_observe_with(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        opts: RegisterOptions,
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Observe, slot_key, name, wasm_bytes, opts)
    }

    fn register(
        &self,
        kind: SlotKind,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        opts: RegisterOptions,
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        let (instance_pre, inspection) =
            compile_module(&self.engine, &self.linker, kind, wasm_bytes)?;
        let new_cell = PluginCell {
            version_id: 1,
            instance_pre,
            schema_hash: inspection.primary_schema_hash(),
            fuel_per_call: self.config.fuel_per_call,
            memory_max_pages: self.config.memory_max_pages,
            pure: opts.pure,
        };

        let mut slots = self.slots.write();
        let slot = match slots.get(&slot_key) {
            Some(existing) => {
                if existing.kind != kind {
                    return Err(WasmtimeRuntimeError::ModuleRejected {
                        reason: format!(
                            "slot {slot_key:?} already registered as `{:?}`; cannot reuse for `{:?}`",
                            existing.kind, kind
                        ),
                    });
                }
                let prev = existing.current.load();
                let bumped = PluginCell {
                    version_id: prev.version_id + 1,
                    instance_pre: new_cell.instance_pre,
                    schema_hash: new_cell.schema_hash,
                    fuel_per_call: new_cell.fuel_per_call,
                    memory_max_pages: new_cell.memory_max_pages,
                    pure: new_cell.pure,
                };
                existing.current.store(Arc::new(bumped));
                Arc::clone(existing)
            }
            None => {
                let slot = Arc::new(PluginSlot::new(name, kind, new_cell));
                slots.insert(slot_key.clone(), Arc::clone(&slot));
                slot
            }
        };
        Ok(slot)
    }

    /// Look up a slot by key.
    pub fn get_slot(&self, slot_key: &SlotKey) -> Option<Arc<PluginSlot>> {
        self.slots.read().get(slot_key).map(Arc::clone)
    }

    fn dispatch<F>(
        &self,
        slot_key: &SlotKey,
        expected_kind: SlotKind,
        run: F,
    ) -> Result<Vec<u8>, WasmtimeRuntimeError>
    where
        F: FnOnce(&SlotKey, &Arc<PluginCell>) -> Result<Vec<u8>, WasmtimeRuntimeError>,
    {
        let slot = self
            .get_slot(slot_key)
            .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
                reason: format!("no slot registered for {:?}", slot_key),
            })?;
        if slot.kind != expected_kind {
            return Err(WasmtimeRuntimeError::ModuleRejected {
                reason: format!(
                    "slot {slot_key:?} is `{:?}`, callable as `{:?}` only",
                    slot.kind, slot.kind,
                ),
            });
        }
        let cell = slot.current.load_full();
        run(slot_key, &cell)
    }

    /// Synchronous filter call. Round-trips one request through the
    /// cached worker; alloc/filter/free share one fuel budget.
    pub fn call_filter(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Filter, |key, cell| {
            call_filter_hook(key, cell, input)
        })
    }

    /// Synchronous shape call. `input` is rkyv-encoded
    /// `ShapeRequest`; output is rkyv-encoded `ShapeResponse`.
    pub fn call_shape(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Shape, |key, cell| {
            call_shape_hook(key, cell, input)
        })
    }

    /// Synchronous normalize_error call against a Shape slot. Sibling
    /// of `call_shape` — both target the same plugin instance, picking
    /// the hook by name.
    pub fn call_normalize_error(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Shape, |key, cell| {
            call_normalize_error_hook(key, cell, input)
        })
    }

    /// Synchronous observe call. Plugin returns no payload; the
    /// `Ok(Vec<u8>)` is always empty.
    pub fn call_observe(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Observe, |key, cell| {
            call_observe_hook(key, cell, input)
        })
    }

    /// Number of currently registered slots. Test/observability helper.
    pub fn slot_count(&self) -> usize {
        self.slots.read().len()
    }
}

#[allow(deprecated)]
impl PluginRuntime for WasmtimeRuntime {
    fn instantiate_router(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn RouterPlugin>, RuntimeError> {
        unimplemented!("router wire deprecated; never implemented on wasmtime runtime")
    }

    fn instantiate_dialect(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn UpstreamDialect>, RuntimeError> {
        unimplemented!(
            "Stage 3 wires manifest → register_shape → WasmtimeUpstreamDialect; \
             callers should use register_shape + WasmtimeUpstreamDialect::new directly until then"
        )
    }

    fn instantiate_observability(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn ObservabilityHook>, RuntimeError> {
        unimplemented!(
            "Stage 3 wires manifest → register_observe → WasmtimeObservabilityHookPlugin; \
             callers should use register_observe + WasmtimeObservabilityHookPlugin::new directly until then"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_build_only() {
        let rt = WasmtimeRuntime::with_defaults().expect("engine build");
        let _ = rt.engine();
        assert_eq!(rt.slot_count(), 0);
    }

    #[test]
    fn missing_slot_returns_error() {
        let rt = WasmtimeRuntime::with_defaults().expect("engine build");
        let err = rt
            .call_filter(&SlotKey::global("nonexistent"), &[])
            .expect_err("must fail on missing slot");
        matches!(err, WasmtimeRuntimeError::ModuleRejected { .. });
    }
}
