use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum VerifyError {
    #[error("identity verification failed: {0}")]
    Identity(#[from] IdentityError),
    #[error("handshake verification failed: {0}")]
    Handshake(#[from] HandshakeError),
    #[error("self-check verification failed: {0}")]
    SelfCheck(#[from] SelfCheckError),
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
pub enum IdentityError {
    #[error("{reason}")]
    NotImplemented { reason: String },
}

#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum HandshakeError {
    #[error("{reason}")]
    NotImplemented { reason: String },
}

#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum SelfCheckError {
    #[error("{reason}")]
    NotImplemented { reason: String },
}

#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum DispatchError {
    #[error("{function} is not implemented yet")]
    NotImplemented { function: &'static str },
}

pub(crate) fn verify_not_implemented(function: &'static str) -> VerifyError {
    VerifyError::Dispatch(DispatchError::NotImplemented { function })
}
