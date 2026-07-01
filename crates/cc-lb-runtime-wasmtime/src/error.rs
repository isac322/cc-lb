//! Error type for the wasmtime-backed plugin runtime.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WasmtimeRuntimeError {
    #[error("failed to construct wasmtime engine: {0}")]
    EngineInit(#[source] anyhow::Error),

    #[error("failed to compile wasm module: {0}")]
    ModuleCompile(#[source] anyhow::Error),

    #[error("failed to instantiate module: {0}")]
    InstantiateFailed(#[source] anyhow::Error),

    #[error("module rejected at load time: {reason}")]
    ModuleRejected { reason: String },

    #[error("guest trap during {phase}: {source}")]
    GuestTrap {
        phase: &'static str,
        #[source]
        source: anyhow::Error,
    },

    // RFC-0001 gap-analysis #6. Surfaces `wasmtime::PoolConcurrencyLimitError`
    // as a distinct variant so callers on the request/instantiate path
    // (not the admin/register path) can pattern-match and translate
    // to their own retry / backpressure semantics without stringy
    // downcasting. `resource` is a coarse label (the wasmtime error
    // does not expose its `kind` publicly); `limit` mirrors the
    // configured pool ceiling when known, otherwise 0.
    #[error("pooling allocator saturated: {resource} limit reached ({limit})")]
    PoolSaturated { resource: &'static str, limit: u32 },
}
