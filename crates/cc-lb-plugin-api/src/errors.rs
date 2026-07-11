//! Error types shared across plugin boundaries.

use thiserror::Error;

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
