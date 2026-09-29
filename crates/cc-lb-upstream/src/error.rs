use thiserror::Error;

/// Request shaping failures returned by [`crate::UpstreamDialect`].
#[derive(Debug, Error)]
pub enum DialectError {
    /// The request path or method is unsupported by the dialect.
    #[error("unsupported request: {reason}")]
    UnsupportedRequest {
        /// Redacted unsupported-request reason.
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
    /// OAuth access token is expired or within the signer refresh skew window.
    #[error("expired token: {reason}")]
    ExpiredToken {
        /// Redacted expiry reason.
        reason: String,
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
    /// Upstream returned a non-retryable response.
    #[error("upstream failed with status {status}")]
    Failed {
        /// HTTP status returned by the upstream.
        status: http::StatusCode,
        /// Redacted upstream response body, when available.
        body: Option<bytes::Bytes>,
    },
}

/// Response transform failures returned by response-transform hooks.
#[derive(Debug, Error)]
pub enum ResponseTransformError {
    /// Runtime error during response transformation.
    #[error("response transform runtime error: {reason}")]
    Runtime {
        /// Redacted runtime failure reason.
        reason: String,
    },
    /// Trap error from a wasm response transform.
    #[error("response transform trap: {reason}")]
    Trap {
        /// Redacted trap failure reason.
        reason: String,
    },
}
