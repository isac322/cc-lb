//! Pure-value routing trace vocabulary for decision stages and subscription-preference traces.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod subscription;
mod trace;

pub use cc_lb_domain::TerminalStrategy;
pub use subscription::{CandidateUrgency, SubscriptionPreferenceTrace};
pub use trace::{RoutingTrace, StageDecision, TerminalDecision};
