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

    #[error("runtime probe failed for hook {hook}: {reason}")]
    ProbeFailed { hook: &'static str, reason: String },

    #[error("guest trap during {phase}: {source}")]
    GuestTrap {
        phase: &'static str,
        #[source]
        source: anyhow::Error,
    },

    // RFC-0001 gap-analysis #6. Surfaces `wasmtime::PoolConcurrencyLimitError`
    // as a distinct variant so callers on the request/instantiate path
    // can pattern-match and translate to retry / backpressure semantics.
    #[error("plugin runtime saturated: {resource} limit reached")]
    PoolSaturated { resource: &'static str },
}

impl WasmtimeRuntimeError {
    /// Returns `true` when this error represents a guest preemption via
    /// Wasmtime epoch interruption (`Trap::Interrupt`).
    pub fn is_epoch_interrupt(&self) -> bool {
        match self {
            Self::GuestTrap { source, .. } | Self::InstantiateFailed(source) => {
                source.chain().any(|cause| {
                    cause.downcast_ref::<wasmtime::Trap>() == Some(&wasmtime::Trap::Interrupt)
                })
            }
            Self::ProbeFailed { reason, .. } => {
                (reason.starts_with("guest trap during ")
                    || reason.starts_with("failed to instantiate module:"))
                    && reason.contains("wasm trap: interrupt")
            }
            _ => false,
        }
    }

    /// Returns `true` when this error represents transient store-budget or
    /// pooling-allocator saturation, including saturation wrapped in a probe error.
    pub fn is_pool_saturated(&self) -> bool {
        match self {
            Self::PoolSaturated { .. } => true,
            Self::ProbeFailed { reason, .. } => reason.starts_with("plugin runtime saturated:"),
            _ => false,
        }
    }
}
