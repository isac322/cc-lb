//! Error types shared across plugin boundaries.

use thiserror::Error;

use crate::types::AuthStrategy;

/// Routing failures returned by [`crate::RouterPlugin`].
#[derive(Debug, Error)]
pub enum RouteError {
    /// No route matched the request and principal.
    #[error("no route matched: {reason}")]
    NoRoute {
        /// Redacted mismatch reason.
        reason: String,
    },
    /// The chosen upstream is unavailable or invalid.
    #[error("upstream unavailable: {reason}")]
    UpstreamUnavailable {
        /// Redacted upstream reason.
        reason: String,
    },
    /// Router plugin runtime failed.
    #[error("router runtime error: {reason}")]
    Runtime {
        /// Redacted runtime failure reason.
        reason: String,
    },
}

/// Request shaping failures returned by [`crate::UpstreamDialect`].
#[derive(Debug, Error)]
pub enum DialectError {
    /// The request path or method is unsupported by the dialect.
    #[error("unsupported request: {reason}")]
    UnsupportedRequest {
        /// Redacted unsupported-request reason.
        reason: String,
    },
    /// The selected upstream does not match the dialect.
    #[error("upstream mismatch: {reason}")]
    UpstreamMismatch {
        /// Redacted mismatch reason.
        reason: String,
    },
    /// URL construction failed.
    #[error("invalid upstream url: {source}")]
    InvalidUrl {
        /// URL parser error.
        #[from]
        source: url::ParseError,
    },
}

/// Signing failures returned by [`crate::Signer`] and [`crate::SignerFactory`].
#[derive(Debug, Error)]
pub enum SignerError {
    /// No credentials are configured for the selected upstream.
    #[error("missing credentials: {reason}")]
    MissingCredentials {
        /// Redacted credential reason.
        reason: String,
    },
    /// Credentials exist but cannot be used.
    #[error("invalid credentials: {reason}")]
    InvalidCredentials {
        /// Redacted credential reason.
        reason: String,
    },
    /// Signing failed.
    #[error("signing failed: {reason}")]
    SigningFailed {
        /// Redacted signing failure reason.
        reason: String,
    },
    /// Credential storage is temporarily unavailable.
    #[error("credential storage unavailable: {reason}")]
    StorageUnavailable {
        /// Redacted storage failure reason.
        reason: String,
    },
    /// The factory was asked to build a signer for the wrong strategy.
    #[error("wrong signer strategy: {strategy:?}")]
    WrongStrategy {
        /// Selected auth strategy.
        strategy: AuthStrategy,
    },
}

/// Upstream failures observed after a request has been relayed.
#[derive(Debug, Error)]
pub enum UpstreamError {
    /// Upstream returned an unauthorized response.
    #[error("upstream unauthorized with status {status}")]
    Unauthorized {
        /// HTTP status returned by the upstream.
        status: http::StatusCode,
        /// Redacted upstream response body, when available.
        body: Option<bytes::Bytes>,
    },
    /// Upstream returned a retryable response.
    #[error("upstream retryable status {status}")]
    Retryable {
        /// HTTP status returned by the upstream.
        status: http::StatusCode,
        /// Redacted upstream response body, when available.
        body: Option<bytes::Bytes>,
    },
    /// Upstream returned a non-retryable response.
    #[error("upstream failed with status {status}")]
    Failed {
        /// HTTP status returned by the upstream.
        status: http::StatusCode,
        /// Redacted upstream response body, when available.
        body: Option<bytes::Bytes>,
    },
}

/// Observability hook failures returned by [`crate::ObservabilityHook`].
#[derive(Debug, Error)]
pub enum ObservabilityError {
    /// The bounded observability queue is full.
    #[error("observability queue full")]
    QueueFull,
    /// The observability hook dropped the event.
    #[error("observability event dropped: {reason}")]
    Dropped {
        /// Redacted drop reason.
        reason: String,
    },
}

/// Plugin runtime failures returned by [`crate::PluginRuntime`].
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Plugin manifest cannot be loaded or parsed.
    #[error("invalid plugin manifest: {reason}")]
    InvalidManifest {
        /// Redacted manifest failure reason.
        reason: String,
    },
    /// Plugin artifact cannot be loaded.
    #[error("plugin load failed: {reason}")]
    LoadFailed {
        /// Redacted load failure reason.
        reason: String,
    },
    /// Plugin instantiation failed.
    #[error("plugin instantiation failed: {reason}")]
    InstantiateFailed {
        /// Redacted instantiation failure reason.
        reason: String,
    },
}
