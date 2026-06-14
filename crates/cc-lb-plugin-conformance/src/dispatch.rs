#![cfg(feature = "dispatch")]

use std::collections::BTreeSet;

use cc_lb_plugin_wire::augmented_metadata::AugmentedMetadata;
use cc_lb_plugin_wire::limits::{
    HANDSHAKE_FUEL, HANDSHAKE_WALL_MS, SKIP_HANDSHAKE_IF_FRESH_TTL_SECS,
};
pub use cc_lb_plugin_wire::wire_function::FallbackPolicy;
use cc_lb_plugin_wire::wire_function::WireFunction;
use cc_lb_runtime_protocol::{BuildPluginError, build_plugin};
use thiserror::Error;

use crate::handshake::HandshakeError;

/// Build a temporary plugin session, dispatch one wire call, and return the wire outcome.
///
/// Instantiation and handshake failures are returned as [`RunError`]. Wire-call failures,
/// including traps or panics inside Extism dispatch, are caught by the protocol layer and
/// returned as [`DispatchOutcome::Fallback`] according to the function's fallback policy.
pub fn run<F: WireFunction>(
    wasm: &[u8],
    request: F::Request,
) -> Result<DispatchOutcome<F::Response>, RunError> {
    let plugin = build_plugin(wasm, HANDSHAKE_WALL_MS, HANDSHAKE_FUEL)
        .map_err(RunError::from_build_error)?;
    let metadata = metadata_from_handshake(wasm, &BTreeSet::new()).map_err(RunError::Handshake)?;
    let mut session = PluginSession { plugin, metadata };

    Ok(session.dispatch::<F>(request))
}

/// Reusable in-process plugin session for conformance dispatch checks.
///
/// `PluginSession` is intentionally not cloneable: an Extism plugin instance is mutable
/// execution state and dispatch requires `&mut self`. Panics inside Extism's wire-call path
/// are caught by `cc_lb_runtime_protocol::dispatch::dispatch_wire_call` and surface as
/// [`DispatchOutcome::Fallback`] with the dispatched function's [`FallbackPolicy`]. Such a
/// fallback does not poison the session; later dispatches use the same plugin instance normally.
#[non_exhaustive]
pub struct PluginSession {
    plugin: extism::Plugin,
    metadata: AugmentedMetadata,
}

impl PluginSession {
    /// Instantiate a plugin and run handshake with no host capabilities.
    pub fn new(wasm: &[u8]) -> Result<Self, HandshakeError> {
        Self::new_with_caps(wasm, &BTreeSet::new())
    }

    /// Instantiate a plugin and run handshake with caller-supplied host capabilities.
    pub fn new_with_caps(
        wasm: &[u8],
        host_capabilities: &BTreeSet<String>,
    ) -> Result<Self, HandshakeError> {
        let plugin = build_plugin(wasm, HANDSHAKE_WALL_MS, HANDSHAKE_FUEL)
            .map_err(HandshakeError::from_build_error)?;
        let metadata = metadata_from_handshake(wasm, host_capabilities)?;

        Ok(Self { plugin, metadata })
    }

    /// Dispatch one wire call through the existing plugin instance.
    ///
    /// This mirrors the protocol layer: wire-call failures never return `Result::Err` and
    /// instead become [`DispatchOutcome::Fallback`]. The call is not retried.
    pub fn dispatch<F: WireFunction>(
        &mut self,
        request: F::Request,
    ) -> DispatchOutcome<F::Response> {
        DispatchOutcome::from_protocol(cc_lb_runtime_protocol::dispatch::dispatch_wire_call::<F>(
            &mut self.plugin,
            &self.metadata,
            request,
        ))
    }
}

/// Result of a conformance dispatch call.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DispatchOutcome<R> {
    /// The plugin returned a decoded response.
    Ok(R),
    /// The protocol layer applied the wire function's fallback policy.
    Fallback(FallbackPolicy),
}

impl<R> DispatchOutcome<R> {
    pub(crate) fn from_protocol(
        value: cc_lb_runtime_protocol::dispatch::DispatchOutcome<R>,
    ) -> Self {
        match value {
            cc_lb_runtime_protocol::dispatch::DispatchOutcome::Ok(response) => Self::Ok(response),
            cc_lb_runtime_protocol::dispatch::DispatchOutcome::Fallback(policy) => {
                Self::Fallback(policy)
            }
            _ => unreachable!(),
        }
    }
}

/// Errors that can prevent `dispatch::run` from reaching the wire-call layer.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RunError {
    /// The plugin could not be instantiated for dispatch.
    #[error("plugin instantiation failed: {reason}")]
    Build { reason: String },
    /// The plugin failed handshake or identity checks needed for dispatch metadata.
    #[error("handshake failed: {0}")]
    Handshake(HandshakeError),
}

impl RunError {
    pub(crate) fn from_build_error(value: BuildPluginError) -> Self {
        match value {
            BuildPluginError::Instantiate { reason } => Self::Build { reason },
            _ => unreachable!(),
        }
    }
}

impl HandshakeError {
    pub(crate) fn from_build_error(value: BuildPluginError) -> Self {
        match value {
            BuildPluginError::Instantiate { reason } => Self::Instantiate { reason },
            _ => unreachable!(),
        }
    }
}

fn metadata_from_handshake(
    wasm: &[u8],
    host_capabilities: &BTreeSet<String>,
) -> Result<AugmentedMetadata, HandshakeError> {
    let offer = cc_lb_runtime_protocol::handshake::build_offer(host_capabilities);
    let accept = cc_lb_runtime_protocol::handshake::execute_handshake(wasm, &offer)
        .map_err(HandshakeError::from_protocol)?;
    let identity = cc_lb_runtime_protocol::identity::read_identity(wasm).map_err(|source| {
        HandshakeError::InvalidIdentity {
            field: "custom_section",
            reason: source.to_string(),
        }
    })?;

    Ok(AugmentedMetadata {
        identity,
        negotiated_functions: accept.chosen_versions,
        negotiated_capabilities: accept.required_capabilities,
        handshake_completed_at: 1,
        self_check_passed: true,
        self_check_completed_at: 1,
        expires_at: 1 + SKIP_HANDSHAKE_IF_FRESH_TTL_SECS as i64,
    })
}
