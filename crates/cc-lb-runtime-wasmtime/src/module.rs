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
