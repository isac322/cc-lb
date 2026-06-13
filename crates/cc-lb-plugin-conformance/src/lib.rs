//! In-process conformance scaffolding for cc-lb plugin authors.
//!
//! This crate exposes protocol-layer wrappers, plugin-kind verifier entry points,
//! and test fixture helpers for compiled WebAssembly plugins targeting cc-lb. The
//! 0.1 API is synchronous only; async dispatch is intentionally excluded until a
//! later minor release. Error `reason` strings are diagnostics and are not part
//! of the SemVer contract, so callers should match variants instead. Cold CI
//! builds include the Extism/Wasmtime stack and can take several minutes.

#![forbid(unsafe_code)]

#[cfg(feature = "dispatch")]
pub mod dispatch;
mod errors;
pub mod fixtures;
pub mod handshake;
pub mod identity;
pub mod prelude;
pub mod self_check;
#[cfg(feature = "dispatch")]
pub mod verify;

pub use errors::{ExtraInfo, LayerResult, VerifyError, VerifyReport};
#[cfg(feature = "dispatch")]
pub use verify::{
    verify_observability_plugin, verify_observability_plugin_with_caps, verify_router_plugin,
    verify_router_plugin_with_caps, verify_shape_plugin, verify_shape_plugin_with_caps,
};
