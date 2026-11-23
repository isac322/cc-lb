//! Pure-value routing trace vocabulary for decision stages and subscription-preference traces.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod subscription;
mod trace;

pub use cc_lb_domain::TerminalStrategy;
pub use subscription::{CandidateUrgency, SubscriptionPreferenceTrace};
pub use trace::{RoutingTrace, StageDecision, TerminalDecision};

/// Maximum number of stages in a routing trace.
pub const MAX_ROUTING_TRACE_STAGES: usize = 100;
/// Maximum length of a stage name.
pub const MAX_STAGE_NAME_LEN: usize = 256;
