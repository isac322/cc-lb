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
//!   mitigated by pinning to a fixed wasmtime patch and by allocating
//!   plugin instances through the pooling allocator (no malloc inside the
//!   signal critical path on the steady-state hot path).
//! * Wasm features deny-list applied at engine construction so a module
//!   declaring forbidden opcodes fails validation immediately — not at
//!   call time.
//! * `consume_fuel(true)` so every store starts at 0 fuel until the
//!   per-call wrapper sets it (review-consensus invariant: assign fuel
//!   before every guest call including alloc / hook / free).

use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, OptLevel, PoolingAllocationConfig, Strategy,
};

use crate::error::WasmtimeRuntimeError;

/// Host state attached to every [`wasmtime::Store`] on the hot path.
///
/// Phase 1 carries nothing — host imports are zero by load-time enforcement.
/// Future phases may add an audit-log handle for the signer engine or
/// per-call accounting. The `()` newtype keeps the type signature stable
/// across phases so downstream code never needs to switch on it.
#[derive(Clone, Copy, Debug, Default)]
pub struct HostState;

/// Hot-path engine tuning knobs.
///
/// Defaults follow the MVP-grade choices recorded in the Phase 1 design
/// decisions: 64 pooled memories, 4 MiB max guest memory, 10M instruction
/// fuel budget per call, 512 KiB wasm stack.
#[derive(Clone, Debug)]
pub struct HotEngineConfig {
    /// Maximum number of 64 KiB wasm pages per `Memory`. 64 pages = 4 MiB.
    pub memory_max_pages: u32,
    /// Default fuel per guest call (covers `cc_lb_alloc` + hook body + `cc_lb_free`).
    pub fuel_per_call: u64,
    /// Maximum wasm call-stack size, bytes.
    pub max_wasm_stack: usize,
    /// `PoolingAllocationConfig::total_memories`.
    pub pool_total_memories: u32,
    /// `PoolingAllocationConfig::total_core_instances`.
    pub pool_total_core_instances: u32,
}

impl Default for HotEngineConfig {
    fn default() -> Self {
        Self {
            // 2048 pages = 128 MiB per plugin instance. Sized to survive
            // the 100 MiB /v1/files body cap (DEFAULT_FILES_CAP_BYTES in
            // cc-lb-core::lifecycle) plus rkyv envelope + per-hook
            // scratch clones; see RFC-0001 gap-analysis item #3.
            memory_max_pages: 2048,
            // 1B fuel covers regex + serde_json parsing on multi-MB tool
            // schemas. Empirically 10M was insufficient for a 26 KiB
            // OpenCode system prompt, 200M for a 5 MB tools-heavy request.
            fuel_per_call: 1_000_000_000,
            // 1 MiB wasm stack — plenty for regex-automata state machines.
            max_wasm_stack: 1024 * 1024,
            pool_total_memories: 64,
            pool_total_core_instances: 64,
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
        .consume_fuel(true)
        .signals_based_traps(true)
        .memory_reservation(1u64 << 32)
        .memory_guard_size(1u64 << 32)
        .memory_init_cow(true)
        .max_wasm_stack(cfg.max_wasm_stack);

    let max_memory_size = (cfg.memory_max_pages as usize) << 16;
    let mut pool = PoolingAllocationConfig::new();
    pool.total_memories(cfg.pool_total_memories)
        .total_core_instances(cfg.pool_total_core_instances)
        .max_memory_size(max_memory_size);

    wcfg.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

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
            cfg.memory_max_pages, 2048,
            "default must accommodate 100 MiB /v1/files body with rkyv envelope + scratch margin",
        );
    }
}
