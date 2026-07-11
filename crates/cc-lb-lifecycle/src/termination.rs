use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stage payloads: RequestTerminated
// ---------------------------------------------------------------------------

/// Why the request terminated.
///
/// String `error_code` mirrors the pre-existing catalog in
/// `cc_lb_engine::terminal_observer::error_codes` so downstream consumers keep
/// working during the shadow-mode migration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminationReason {
    /// Successful 2xx response.
    Success,
    /// Request was rejected or upstream returned an error. The string
    /// matches an entry in the `error_codes` catalog.
    ErrorCode(String),
    /// `LifecycleContext` was dropped without an explicit termination
    /// signal (usually an in-flight cancellation).
    Dropped,
}

impl TerminationReason {
    /// Static-label form for metrics without payload cardinality.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ErrorCode(_) => "error_code",
            Self::Dropped => "dropped",
        }
    }
}
