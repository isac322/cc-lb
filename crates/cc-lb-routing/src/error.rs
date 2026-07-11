use thiserror::Error;

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
