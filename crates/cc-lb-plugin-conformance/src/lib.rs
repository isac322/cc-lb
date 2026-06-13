//! In-process conformance scaffolding for cc-lb plugin authors.
//!
//! This crate exposes protocol-layer wrappers, plugin-kind verifier entry points,
//! and test fixture helpers for compiled WebAssembly plugins targeting cc-lb. The
//! 0.1 API is synchronous only; async dispatch is intentionally excluded until a
//! later minor release. Error `reason` strings are diagnostics and are not part
//! of the SemVer contract, so callers should match variants instead. Cold CI
//! builds include the Extism/Wasmtime stack and can take several minutes.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;

#[cfg(feature = "dispatch")]
pub mod dispatch;
mod errors;
pub mod fixtures;
pub mod handshake;
pub mod identity;
pub mod prelude;
pub mod self_check;
pub mod verify;

pub use errors::{ExtraInfo, LayerResult, VerifyError, VerifyReport};

pub fn verify_router_plugin(_wasm: &[u8]) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented("verify_router_plugin"))
}

pub fn verify_router_plugin_with_caps(
    _wasm: &[u8],
    _host_capabilities: &BTreeSet<String>,
) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented(
        "verify_router_plugin_with_caps",
    ))
}

pub fn verify_shape_plugin(_wasm: &[u8]) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented("verify_shape_plugin"))
}

pub fn verify_shape_plugin_with_caps(
    _wasm: &[u8],
    _host_capabilities: &BTreeSet<String>,
) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented(
        "verify_shape_plugin_with_caps",
    ))
}

pub fn verify_observability_plugin(_wasm: &[u8]) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented(
        "verify_observability_plugin",
    ))
}

pub fn verify_observability_plugin_with_caps(
    _wasm: &[u8],
    _host_capabilities: &BTreeSet<String>,
) -> Result<VerifyReport, VerifyError> {
    Err(errors::verify_not_implemented(
        "verify_observability_plugin_with_caps",
    ))
}
