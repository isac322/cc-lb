//! Wasmtime-backed plugin runtime.
//!
//! Registers slots per hook kind via [`WasmtimeRuntime::register_filter`]
//! / [`register_shape`][WasmtimeRuntime::register_shape] /
//! [`register_observe`][WasmtimeRuntime::register_observe], returning
//! the `Arc<LoadedPluginSlot>` callers store in their dynamic view.
//!
//! See `docs/rfc/0001-plugin-runtime-vnext.md`.
#![deny(unsafe_code)]

mod budget;
mod cache;
mod cell;
mod compiled_cache;
mod dispatch;
mod engine;
mod error;
mod inspect;
mod metrics;
mod module;
pub mod policy;
mod probe;
mod slot;
#[cfg(test)]
mod tests;
mod wire_dispatch;

pub use cache::{DEFAULT_ALIGN, call_filter_hook, call_observe_hook, call_shape_hook};
pub use cc_lb_plugin_wire::schema::HookKind;
pub use cc_lb_plugin_wire::schema::HookKind as SlotKind;
pub use cell::{LoadedPluginSlot, PluginCell};
pub use engine::{HostState, HotEngineAllocationStrategy, HotEngineConfig, build_hot_engine};
pub use error::WasmtimeRuntimeError;
pub use inspect::{ModuleInspection, inspect_wasm, inspect_wasm_agnostic};
pub use module::{admit_wasm, admit_wasm_agnostic, compile_module};
pub use slot::RuntimeSlotKey;
pub use wire_dispatch::WasmPluginWireDispatch;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use budget::StoreBudget;
use compiled_cache::process_wide_compiled_module_cache;
use parking_lot::RwLock;
use wasmtime::{Engine, Linker};

/// Wasmtime-backed plugin runtime.
pub struct WasmtimeRuntime {
    engine: Arc<Engine>,
    linker: Arc<Linker<HostState>>,
    config: Arc<HotEngineConfig>,
    store_budget: Arc<StoreBudget>,
    slots: RwLock<HashMap<RuntimeSlotKey, Arc<LoadedPluginSlot>>>,
}

impl WasmtimeRuntime {
    pub fn new(config: HotEngineConfig) -> Result<Self, WasmtimeRuntimeError> {
        let engine = build_hot_engine(&config)?;
        let linker = Linker::new(&engine);
        Ok(Self {
            engine: Arc::new(engine),
            linker: Arc::new(linker),
            store_budget: Arc::new(StoreBudget::new(config.pool_total_core_instances)),
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

    pub fn admit_wasm(
        &self,
        kind: HookKind,
        wasm_bytes: &[u8],
    ) -> Result<ModuleInspection, WasmtimeRuntimeError> {
        let (_, inspection) =
            module::admit_wasm(&self.engine, &self.linker, kind, wasm_bytes, &self.config)?;
        Ok(inspection)
    }

    pub fn admit_wasm_agnostic(
        &self,
        wasm_bytes: &[u8],
    ) -> Result<ModuleInspection, WasmtimeRuntimeError> {
        let (_, inspection) =
            module::admit_wasm_agnostic(&self.engine, &self.linker, wasm_bytes, &self.config)?;
        Ok(inspection)
    }

    /// Register (or replace) a filter-hook slot.
    pub fn register_filter(
        &self,
        slot_key: RuntimeSlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<LoadedPluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Filter, slot_key, name, wasm_bytes)
    }

    /// Register (or replace) a shape-hook slot. The plugin module
    /// must export `cc_lb_shape`.
    pub fn register_shape(
        &self,
        slot_key: RuntimeSlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<LoadedPluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Shape, slot_key, name, wasm_bytes)
    }

    /// Register (or replace) an observe-hook slot.
    pub fn register_observe(
        &self,
        slot_key: RuntimeSlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<LoadedPluginSlot>, WasmtimeRuntimeError> {
        self.register(SlotKind::Observe, slot_key, name, wasm_bytes)
    }

    fn register(
        &self,
        kind: SlotKind,
        slot_key: RuntimeSlotKey,
        name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<Arc<LoadedPluginSlot>, WasmtimeRuntimeError> {
        let new_content_hash = module::compute_content_hash(&self.engine, &self.config, wasm_bytes);

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
            module::admit_wasm(&self.engine, &self.linker, kind, wasm_bytes, &self.config)?;
        let new_cell = PluginCell {
            version_id: 1,
            instance_pre,
            metadata: inspection.metadata,
            memory_max_pages: self.config.memory_max_pages,
            store_budget: Arc::clone(&self.store_budget),
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
                    metadata: new_cell.metadata,
                    memory_max_pages: new_cell.memory_max_pages,
                    store_budget: new_cell.store_budget,
                    content_hash: new_cell.content_hash,
                    plugin_name: new_cell.plugin_name,
                };
                existing.current.store(Arc::new(bumped));
                Arc::clone(existing)
            }
            None => {
                let slot = Arc::new(LoadedPluginSlot::new(name_string, kind, new_cell));
                slots.insert(slot_key.clone(), Arc::clone(&slot));
                slot
            }
        };
        Ok(slot)
    }

    /// Look up a slot by key.
    pub fn get_slot(&self, slot_key: &RuntimeSlotKey) -> Option<Arc<LoadedPluginSlot>> {
        self.slots.read().get(slot_key).map(Arc::clone)
    }

    /// Remove a slot from the runtime map. Returns `true` if a slot
    /// was present, `false` if the key was already gone (idempotent).
    ///
    /// The removed `Arc<LoadedPluginSlot>` is dropped AFTER the write lock
    /// is released so any downstream `Drop` impl work (releasing the
    /// pooled `InstancePre`, deregistering typed funcs) does not
    /// block other registers. In-flight dispatchers that already
    /// cloned an `Arc<LoadedPluginSlot>` via `get_slot` continue to run
    /// against the retained clone — the runtime never yanks state
    /// out from under a live call.
    pub fn evict_slot(&self, slot_key: &RuntimeSlotKey) -> bool {
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
    pub fn retain_slots(&self, keep: &HashSet<RuntimeSlotKey>) -> Vec<RuntimeSlotKey> {
        let (evicted_keys, removed) = {
            let mut slots = self.slots.write();
            let warm_content = slots
                .iter()
                .filter(|(key, _)| keep.contains(*key))
                .map(|(_, slot)| slot.current.load().content_hash)
                .collect();
            process_wide_compiled_module_cache().retain_warm_content(warm_content);
            let orphan_keys: Vec<RuntimeSlotKey> = slots
                .keys()
                .filter(|k| !keep.contains(*k))
                .cloned()
                .collect();
            let removed: Vec<Arc<LoadedPluginSlot>> =
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
        metrics::publish_pool_metrics(&self.engine, &self.config);
    }
}
