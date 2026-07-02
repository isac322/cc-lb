//! Wasmtime-backed plugin runtime.
//!
//! Registers slots per hook kind via [`WasmtimeRuntime::register_filter`]
//! / [`register_shape`][WasmtimeRuntime::register_shape] /
//! [`register_observe`][WasmtimeRuntime::register_observe], returning
//! the `Arc<PluginSlot>` callers store in their dynamic view.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.
#![deny(unsafe_code)]

mod cache;
mod cell;
mod engine;
mod error;
mod inspect;
mod module;
mod plugin;
pub mod policy;

pub use cache::{
    DEFAULT_ALIGN, call_filter_hook, call_normalize_error_hook, call_observe_hook, call_shape_hook,
};
pub use cell::{PluginCell, PluginSlot};
pub use engine::{HostState, HotEngineConfig, build_hot_engine};
pub use error::WasmtimeRuntimeError;
pub use inspect::{ModuleInspection, SlotKind, inspect_wasm};
pub use module::compile_module;
pub use plugin::{WasmtimeFilterPlugin, WasmtimeObservabilityHookPlugin, WasmtimeUpstreamDialect};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cc_lb_plugin_api::SlotKey;
use parking_lot::RwLock;
use wasmtime::{Engine, Linker};

/// Wasmtime-backed plugin runtime.
pub struct WasmtimeRuntime {
    engine: Arc<Engine>,
    linker: Arc<Linker<HostState>>,
    config: Arc<HotEngineConfig>,
    slots: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>,
}

impl WasmtimeRuntime {
    pub fn new(config: HotEngineConfig) -> Result<Self, WasmtimeRuntimeError> {
        let engine = build_hot_engine(&config)?;
        let linker = Linker::new(&engine);
        Ok(Self {
            engine: Arc::new(engine),
            linker: Arc::new(linker),
            config: Arc::new(config),
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

    /// Shareable handle to the effective runtime config. Adapters
    /// snapshot this at construction so per-call knob reads are just
    /// atomic pointer loads, no lock or lookup.
    pub fn config_arc(&self) -> Arc<HotEngineConfig> {
        Arc::clone(&self.config)
    }

    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    /// Register (or replace) a filter-hook slot.
    pub fn register_filter(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Filter, slot_key, name, wasm_bytes)
    }

    /// Register (or replace) a shape-hook slot. The plugin module
    /// must export both `cc_lb_shape` and `cc_lb_normalize_error`.
    pub fn register_shape(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Shape, slot_key, name, wasm_bytes)
    }

    /// Register (or replace) an observe-hook slot.
    pub fn register_observe(
        &self,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Observe, slot_key, name, wasm_bytes)
    }

    fn register(
        &self,
        kind: SlotKind,
        slot_key: SlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<PluginSlot>, WasmtimeRuntimeError> {
        let new_content_hash = module::compute_content_hash(
            wasm_bytes,
            self.config.fuel_per_call,
            self.config.memory_max_pages,
        );

        // Fast path: skip the compile entirely when the slot is
        // already registered with byte-identical content. This is the
        // common case during reconcile ticks — RFC-0001 gap-analysis
        // item #2. Only trades a cheap read-lock + hash compare.
        {
            let slots = self.slots.read();
            if let Some(existing) = slots.get(&slot_key)
                && existing.kind == kind
                && existing.current.load().content_hash == new_content_hash
            {
                return Ok(Arc::clone(existing));
            }
        }

        let name_string: String = name.into();
        let plugin_name: Arc<str> = Arc::from(name_string.as_str());
        let (instance_pre, inspection) =
            compile_module(&self.engine, &self.linker, kind, wasm_bytes)?;
        let new_cell = PluginCell {
            version_id: 1,
            instance_pre,
            schema_hash: inspection.primary_schema_hash(),
            fuel_per_call: self.config.fuel_per_call,
            memory_max_pages: self.config.memory_max_pages,
            content_hash: new_content_hash,
            plugin_name: Arc::clone(&plugin_name),
        };

        let mut slots = self.slots.write();
        // Re-check under the write lock: a concurrent register on the
        // same key could have raced ahead while we compiled. If that
        // register produced the same content_hash, keep its result and
        // discard the wasted compile — do NOT bump version_id, do NOT
        // store the freshly compiled cell.
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
                if prev.content_hash == new_content_hash {
                    return Ok(Arc::clone(existing));
                }
                let bumped = PluginCell {
                    version_id: prev.version_id + 1,
                    instance_pre: new_cell.instance_pre,
                    schema_hash: new_cell.schema_hash,
                    fuel_per_call: new_cell.fuel_per_call,
                    memory_max_pages: new_cell.memory_max_pages,
                    content_hash: new_cell.content_hash,
                    plugin_name: new_cell.plugin_name,
                };
                existing.current.store(Arc::new(bumped));
                Arc::clone(existing)
            }
            None => {
                let slot = Arc::new(PluginSlot::new(name_string, kind, new_cell));
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

    /// Remove a slot from the runtime map. Returns `true` if a slot
    /// was present, `false` if the key was already gone (idempotent).
    ///
    /// The removed `Arc<PluginSlot>` is dropped AFTER the write lock
    /// is released so any downstream `Drop` impl work (releasing the
    /// pooled `InstancePre`, deregistering typed funcs) does not
    /// block other registers. In-flight dispatchers that already
    /// cloned an `Arc<PluginSlot>` via `get_slot` continue to run
    /// against the retained clone — the runtime never yanks state
    /// out from under a live call.
    pub fn evict_slot(&self, slot_key: &SlotKey) -> bool {
        let removed = {
            let mut slots = self.slots.write();
            slots.remove(slot_key)
        };
        let existed = removed.is_some();
        if existed {
            tracing::info!(
                target: "cc_lb_runtime_wasmtime::eviction",
                ?slot_key,
                "evicted plugin slot",
            );
        }
        drop(removed);
        existed
    }

    /// Sweep the runtime map so only slots whose keys are in `keep`
    /// remain. Returns the keys that were evicted (for logging /
    /// metrics on the reconcile boundary).
    ///
    /// Callers MUST only invoke this AFTER a successful view swap so
    /// a partially-built reconcile pass does not tear down slots the
    /// active view still points at (RFC-0001 gap-analysis item #1,
    /// Oracle adversarial review — never update the "previous set"
    /// before the new view is authoritative).
    pub fn retain_slots(&self, keep: &HashSet<SlotKey>) -> Vec<SlotKey> {
        let (evicted_keys, removed) = {
            let mut slots = self.slots.write();
            let orphan_keys: Vec<SlotKey> = slots
                .keys()
                .filter(|k| !keep.contains(*k))
                .cloned()
                .collect();
            let removed: Vec<Arc<PluginSlot>> =
                orphan_keys.iter().filter_map(|k| slots.remove(k)).collect();
            (orphan_keys, removed)
        };
        for key in &evicted_keys {
            tracing::info!(
                target: "cc_lb_runtime_wasmtime::eviction",
                slot_key = ?key,
                "evicted orphan slot (retain_slots sweep)",
            );
        }
        drop(removed);
        evicted_keys
    }

    fn dispatch<F>(
        &self,
        slot_key: &SlotKey,
        expected_kind: SlotKind,
        run: F,
    ) -> Result<Vec<u8>, WasmtimeRuntimeError>
    where
        F: FnOnce(&Arc<PluginCell>) -> Result<Vec<u8>, WasmtimeRuntimeError>,
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
        run(&cell)
    }

    /// Synchronous filter call. Round-trips one request through the
    /// cached worker; alloc/filter/free share one fuel budget.
    pub fn call_filter(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Filter, |cell| {
            call_filter_hook(cell, input)
        })
    }

    /// Synchronous shape call. `input` is rkyv-encoded
    /// `ShapeRequest`; output is rkyv-encoded `ShapeResponse`.
    pub fn call_shape(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Shape, |cell| {
            call_shape_hook(cell, input)
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
        self.dispatch(slot_key, SlotKind::Shape, |cell| {
            call_normalize_error_hook(cell, input)
        })
    }

    /// Synchronous observe call. Plugin returns no payload; the
    /// `Ok(Vec<u8>)` is always empty.
    pub fn call_observe(
        &self,
        slot_key: &SlotKey,
        input: &[u8],
    ) -> Result<Vec<u8>, WasmtimeRuntimeError> {
        self.dispatch(slot_key, SlotKind::Observe, |cell| {
            call_observe_hook(cell, input)
        })
    }

    /// Number of currently registered slots. Test/observability helper.
    pub fn slot_count(&self) -> usize {
        self.slots.read().len()
    }

    /// Publish current PoolingAllocator utilization as gauges.
    ///
    /// Reads `Engine::pooling_allocator_metrics` (wasmtime 46), which
    /// returns `None` when the engine was built with an on-demand
    /// allocator or the pooling allocator is unavailable — in that
    /// case we emit `0.0` so scrape stays healthy but flag with a
    /// single-shot debug log. Observation only: this method never
    /// rejects a register or fails a call — see RFC-0001 gap-analysis
    /// item #6, Oracle adversarial review "do not gate admin
    /// registers on transient pool utilization".
    pub fn publish_pool_metrics(&self) {
        let (memories_util, instances_util) = match self.engine.pooling_allocator_metrics() {
            Some(m) => {
                let mem_denom = self.config.pool_total_memories.max(1) as f64;
                let inst_denom = self.config.pool_total_core_instances.max(1) as f64;
                let mem_util = (m.memories() as f64) / mem_denom;
                let inst_util = (m.core_instances() as f64) / inst_denom;
                (mem_util.clamp(0.0, 1.0), inst_util.clamp(0.0, 1.0))
            }
            None => (0.0, 0.0),
        };
        metrics::gauge!("cc_lb_plugin_pool_memories_utilization_ratio").set(memories_util);
        metrics::gauge!("cc_lb_plugin_pool_core_instances_utilization_ratio").set(instances_util);
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
