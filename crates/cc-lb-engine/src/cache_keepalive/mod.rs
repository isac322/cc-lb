//! Prompt-cache keep-alive scheduler.
//!
//! Owns per-session state that fires synthetic `max_tokens: 0` requests
//! against the same Anthropic upstream that served the original request,
//! so that a session's cache prefix stays warm past the 5 m / 1 h TTL
//! while an agent's tool-execution loop is still in progress.
//!
//! Enabled per principal via `PrincipalRecord::cache_keepalive`. See
//! `cc_lb_storage_api::CacheKeepaliveConfig`.

#[cfg(test)]
mod aead_compat;
mod classifier;
mod dispatcher;
mod lifecycle_glue;
mod metrics;
mod request_snapshot;
mod scheduler;
mod session_key;

pub use classifier::{HeuristicClassifier, TurnDecision};
pub use dispatcher::AnthropicKeepaliveDispatcher;
pub use lifecycle_glue::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer,
};
pub(crate) use lifecycle_glue::{
    LifecycleKeepalive, LifecycleKeepaliveContext, StreamingKeepaliveResponse,
};
pub use metrics::CancelReason;
pub(crate) use metrics::record_cancelled;
pub use request_snapshot::{PersistedRequestSnapshot, RequestSnapshot, SnapshotError};
pub use scheduler::{DispatchOutcome, KeepaliveDispatcher, ScheduleParams};
pub use session_key::SessionKey;
