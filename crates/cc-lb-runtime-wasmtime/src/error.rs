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
}
