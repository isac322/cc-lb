mod cache_affinity;
mod subscription;
mod trace;

pub use cache_affinity::{CacheAffinityCandidate, CacheAffinityTrace};
pub use subscription::{CandidateUrgency, SubscriptionPreferenceTrace};
pub use trace::{RoutingTrace, StageDecision, TerminalDecision, TerminalStrategy};

/// Maximum number of stages in a routing trace.
pub const MAX_ROUTING_TRACE_STAGES: usize = 100;
/// Maximum length of a stage name.
pub const MAX_STAGE_NAME_LEN: usize = 256;
/// Maximum length of an error message in internal errors.
pub const MAX_ERROR_MESSAGE_LEN: usize = 1024;
