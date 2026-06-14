use thiserror::Error;

use cc_lb_plugin_wire::wire_function::FallbackPolicy;

use crate::{handshake, identity, self_check};

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("identity verification failed: {0}")]
    Identity(#[from] identity::IdentityError),
    #[error("handshake verification failed: {0}")]
    Handshake(#[from] handshake::HandshakeError),
    #[error("self-check verification failed: {0}")]
    SelfCheck(#[from] self_check::SelfCheckError),
    #[error("dispatch verification failed: {0}")]
    Dispatch(#[from] DispatchError),
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub identity: LayerResult,
    pub handshake: LayerResult,
    pub self_check: LayerResult,
    pub dispatch: Vec<LayerResult>,
    pub extras: Vec<ExtraInfo>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LayerResult {
    pub layer: &'static str,
    pub passed: bool,
    pub detail: Option<String>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ExtraInfo {
    pub kind: &'static str,
    pub message: String,
}

#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum DispatchError {
    #[error("{function} dispatch fell back with policy {policy:?}")]
    Fallback {
        function: &'static str,
        policy: FallbackPolicy,
    },
    #[error("{function} response failed conformance check: {reason}")]
    InvalidResponse {
        function: &'static str,
        reason: String,
    },
}
