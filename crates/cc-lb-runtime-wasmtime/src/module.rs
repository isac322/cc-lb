//! Wasm module compilation pipeline.
//!
//! Phase 1 W5 — pipeline is:
//!
//!   `wasm bytes -> inspect_wasm (imports/exports/schema_hash) -> Engine::precompile_module -> Module::deserialize -> InstancePre`
//!
//! Inspection happens against the raw `.wasm` because
//! `Module::custom_sections` does not round-trip through
//! `precompile_module → deserialize`. Disk-cache trust + on-boot
//! re-verification is Phase 1+ once `data/plugins/wasm/cache/{sha}.wasm`
//! is wired in.

use std::sync::Arc;

use wasmtime::{Engine, Linker, Module};

use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;
use crate::inspect::{ModuleInspection, SlotKind, inspect_wasm};

// Bumpable on validation-policy change (import allowlist, schema-hash
// algorithm, per-hook export shape). `register()` mixes this into
// `PluginCell::content_hash`, so bumping it forces every registered
// plugin to recompile on next reconcile even when the wasm bytes are
// byte-identical — guarantees a policy tightening cannot be silently
// carried by a stale short-circuited slot.
pub(crate) const VALIDATION_POLICY_VERSION: u32 = 1;

pub(crate) fn compute_content_hash(
    wasm_bytes: &[u8],
    fuel_per_call: u64,
    memory_max_pages: u32,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&VALIDATION_POLICY_VERSION.to_le_bytes());
    hasher.update(&fuel_per_call.to_le_bytes());
    hasher.update(&memory_max_pages.to_le_bytes());
    hasher.update(wasm_bytes);
    *hasher.finalize().as_bytes()
}

/// Compile raw `.wasm` bytes into an [`InstancePre`] bound to `engine`,
/// returning the load-time [`ModuleInspection`] alongside it.
///
/// `Module::deserialize` is documented `unsafe` because tampered compiled
/// artifacts can execute arbitrary code. The wasmtime runtime always
/// feeds it bytes produced in-memory by `Engine::precompile_module(wasm_bytes)`
/// in the same process — i.e. the cwasm never crosses a trust boundary.
/// Disk reload of `.cwasm` (untrusted bytes) is deferred to a later
/// phase once the integrity-binding policy from §Operational invariants
/// is wired.
#[allow(unsafe_code)]
pub fn compile_module(
    engine: &Engine,
    linker: &Linker<HostState>,
    kind: SlotKind,
    wasm_bytes: &[u8],
) -> Result<(Arc<wasmtime::InstancePre<HostState>>, ModuleInspection), WasmtimeRuntimeError> {
    let inspection = inspect_wasm(kind, wasm_bytes)?;

    let cwasm = engine
        .precompile_module(wasm_bytes)
        .map_err(|e| WasmtimeRuntimeError::ModuleCompile(anyhow::Error::from(e)))?;

    // SAFETY: cwasm bytes were just produced by `engine.precompile_module`
    // in this process — they are not attacker-controlled.
    let module = unsafe {
        Module::deserialize(engine, &cwasm)
            .map_err(|e| WasmtimeRuntimeError::ModuleCompile(anyhow::Error::from(e)))?
    };

    let instance_pre = linker
        .instantiate_pre(&module)
        .map_err(|e| WasmtimeRuntimeError::InstantiateFailed(anyhow::Error::from(e)))?;

    Ok((Arc::new(instance_pre), inspection))
}
