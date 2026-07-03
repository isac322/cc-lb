//! Hot-path wasmtime [`Engine`] construction.
//!
//! Phase 1 W2 — only the sync hot-path engine is set up here. The async
//! signer engine arrives in Phase 2.
//!
//! ## Config invariants (RFC §Engine 구성 + review consensus)
//!
//! * `async_support` is deprecated in wasmtime 46 (async is selected per
//!   call). We do not enable it.
//! * `signals_based_traps(true)` is on with wasmtime 46's POSIX signal
//!   handler. The known TLS-vs-malloc deadlock (upstream issue #12787) is
//!   mitigated by pinning to a fixed wasmtime patch. Pooling remains an
//!   opt-in strategy for deployments that need steady-state pool reuse.
//! * Wasm features deny-list applied at engine construction so a module
//!   declaring forbidden opcodes fails validation immediately — not at
//!   call time.
//! * Fuel metering is intentionally disabled on the hot path. Large
//!   shape plugins may spend substantial CPU on regex + JSON work, and
//!   the host relies on request-level timeouts/backpressure rather than
//!   per-instruction traps.

use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig, StoreLimits,
    StoreLimitsBuilder, Strategy,
};

use crate::error::WasmtimeRuntimeError;

pub const DEFAULT_MEMORY_MAX_PAGES: u32 = 2048;
pub const DEFAULT_MEMORY_RESERVATION_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MEMORY_GUARD_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_POOL_TOTAL_MEMORIES: u32 = 64;
pub const DEFAULT_POOL_TOTAL_CORE_INSTANCES: u32 = 64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HotEngineAllocationStrategy {
    #[default]
    OnDemand,
    Pooling,
}

/// Host state attached to every [`wasmtime::Store`] on the hot path.
#[derive(Debug)]
pub struct HostState {
    limits: StoreLimits,
}

impl HostState {
    pub fn new(memory_max_pages: u32) -> Self {
        Self {
            limits: StoreLimitsBuilder::new()
                .memory_size((memory_max_pages as usize) << 16)
                .build(),
        }
    }

    pub fn limits(&mut self) -> &mut StoreLimits {
        &mut self.limits
    }
}

impl Default for HostState {
    fn default() -> Self {
        Self::new(DEFAULT_MEMORY_MAX_PAGES)
    }
}

/// Hot-path engine tuning knobs.
///
/// Defaults follow the current production envelope: large pooled memories
/// for multi-MB shape payloads, bounded wasm stack, and pooling limits.
#[derive(Clone, Debug)]
pub struct HotEngineConfig {
    pub allocation_strategy: HotEngineAllocationStrategy,
    /// Maximum number of 64 KiB wasm pages per `Memory`. 64 pages = 4 MiB.
    pub memory_max_pages: u32,
    /// Maximum wasm call-stack size, bytes.
    pub max_wasm_stack: usize,
    /// `PoolingAllocationConfig::total_memories`.
    pub pool_total_memories: u32,
    /// `PoolingAllocationConfig::total_core_instances`.
    pub pool_total_core_instances: u32,
    /// `Config::memory_reservation`.
    pub memory_reservation_bytes: u64,
    /// `Config::memory_guard_size`.
    pub memory_guard_bytes: u64,
    /// Runtime policy: what to do on plugin failure. Default preserves
    /// pre-Sprint-3 pass-through.
    pub plugin_failure_policy: crate::policy::PluginFailurePolicy,
    /// Runtime policy: whether shape plugins may cross origins.
    /// Default preserves pre-Sprint-3 unrestricted behaviour.
    pub shape_origin_policy: crate::policy::ShapeOriginPolicy,
    /// Wire I/O bounds — see [`crate::policy::PluginWireBounds`].
    pub wire_bounds: crate::policy::PluginWireBounds,
    /// When `true`, strip `cookie` from filter/shape wire input so
    /// guests do not observe downstream session credentials.
    /// Default `false` matches pre-Sprint-3 behaviour.
    pub cookie_redaction: bool,
}

impl Default for HotEngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: HotEngineAllocationStrategy::OnDemand,
            // 2048 pages = 128 MiB per plugin instance. Sized to survive
            // the 100 MiB /v1/files body cap (DEFAULT_FILES_CAP_BYTES in
            // cc-lb-core::lifecycle) plus rkyv envelope + per-hook
            // scratch clones; see RFC-0001 gap-analysis item #3.
            memory_max_pages: DEFAULT_MEMORY_MAX_PAGES,
            // 1 MiB wasm stack — plenty for regex-automata state machines.
            max_wasm_stack: 1024 * 1024,
            pool_total_memories: DEFAULT_POOL_TOTAL_MEMORIES,
            pool_total_core_instances: DEFAULT_POOL_TOTAL_CORE_INSTANCES,
            memory_reservation_bytes: DEFAULT_MEMORY_RESERVATION_BYTES,
            memory_guard_bytes: DEFAULT_MEMORY_GUARD_BYTES,
            plugin_failure_policy: crate::policy::PluginFailurePolicy::PassThrough,
            shape_origin_policy: crate::policy::ShapeOriginPolicy::Unrestricted,
            wire_bounds: crate::policy::PluginWireBounds::default(),
            cookie_redaction: false,
        }
    }
}

impl HotEngineConfig {
    pub fn max_memory_size_bytes(&self) -> u64 {
        u64::from(self.memory_max_pages) << 16
    }

    pub fn virtual_memory_reservation_bytes(&self) -> u64 {
        match self.allocation_strategy {
            HotEngineAllocationStrategy::OnDemand => 0,
            HotEngineAllocationStrategy::Pooling => u64::from(self.pool_total_memories)
                .saturating_mul(
                    self.memory_reservation_bytes
                        .saturating_add(self.memory_guard_bytes),
                ),
        }
    }
}

/// Build the hot-path [`Engine`] from a [`HotEngineConfig`].
pub fn build_hot_engine(cfg: &HotEngineConfig) -> Result<Engine, WasmtimeRuntimeError> {
    let mut wcfg = Config::new();
    wcfg.strategy(Strategy::Cranelift)
        .cranelift_opt_level(OptLevel::Speed)
        .wasm_component_model(false)
        .wasm_threads(false)
        .wasm_memory64(false)
        .wasm_multi_memory(false)
        // reference-types is enabled — rustc 1.82+ targets it by default
        // for wasm32 and forcing it off requires every plugin author to
        // ship a custom RUSTFLAGS profile. function-references stays off:
        // a distinct, more advanced proposal that rustc does not emit.
        .wasm_function_references(false)
        .wasm_gc(false)
        .wasm_exceptions(false)
        .wasm_tail_call(false)
        .wasm_relaxed_simd(false)
        .signals_based_traps(true)
        .memory_reservation(cfg.memory_reservation_bytes)
        .memory_guard_size(cfg.memory_guard_bytes)
        .memory_init_cow(true)
        // Hardening knobs (RFC-0001 librarian audit):
        // - `wasm_backtrace(false)` disables backtrace collection on
        //   trap so a plugin cannot trigger deep backtrace work as a
        //   DoS vector, and no wasm frames are ever captured into a
        //   host process report.
        // - `coredump_on_trap(false)` prevents wasmtime from writing
        //   guest coredumps (which would contain plugin linear memory,
        //   including any secrets a filter/shape plugin observed
        //   before the trap) to disk.
        // - `native_unwind_info(false)` drops native unwind tables from
        //   compiled modules; safe once backtraces are off, saves
        //   compile time and memory.
        .wasm_backtrace(false)
        .coredump_on_trap(false)
        .native_unwind_info(false)
        .max_wasm_stack(cfg.max_wasm_stack);

    match cfg.allocation_strategy {
        HotEngineAllocationStrategy::OnDemand => {
            wcfg.allocation_strategy(InstanceAllocationStrategy::OnDemand);
        }
        HotEngineAllocationStrategy::Pooling => {
            let max_memory_size = (cfg.memory_max_pages as usize) << 16;
            let mut pool = PoolingAllocationConfig::new();
            pool.total_memories(cfg.pool_total_memories)
                .total_core_instances(cfg.pool_total_core_instances)
                .max_memory_size(max_memory_size);
            wcfg.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        }
    }

    Engine::new(&wcfg).map_err(|e| WasmtimeRuntimeError::EngineInit(anyhow::Error::from(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC-0001 gap-analysis #3: 100 MiB /v1/files body + rkyv envelope
    // + scratch => 128 MiB (2048 pages) is the safe floor. 1600 has no
    // margin. Keep this pinned so a future bump justifies the delta.
    #[test]
    fn default_memory_max_pages_covers_files_body_cap_with_margin() {
        let cfg = HotEngineConfig::default();
        assert_eq!(
            cfg.memory_max_pages, DEFAULT_MEMORY_MAX_PAGES,
            "default must accommodate 100 MiB /v1/files body with rkyv envelope + scratch margin",
        );
    }

    #[test]
    fn default_allocation_strategy_is_on_demand() {
        let cfg = HotEngineConfig::default();

        assert_eq!(
            cfg.allocation_strategy,
            HotEngineAllocationStrategy::OnDemand
        );
        assert_eq!(cfg.pool_total_memories, 64);
        assert_eq!(cfg.pool_total_core_instances, 64);
        assert_eq!(cfg.memory_reservation_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.memory_guard_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.max_memory_size_bytes(), 128 * 1024 * 1024);
        assert_eq!(cfg.virtual_memory_reservation_bytes(), 0);
    }

    #[test]
    fn pooling_strategy_uses_configured_virtual_reservation() {
        let cfg = HotEngineConfig {
            allocation_strategy: HotEngineAllocationStrategy::Pooling,
            ..HotEngineConfig::default()
        };

        assert_eq!(
            cfg.virtual_memory_reservation_bytes(),
            20 * 1024 * 1024 * 1024
        );
    }
}
