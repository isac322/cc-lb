//! Shared pure-value domain vocabulary for cc-lb.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod cache;
mod error;
mod identity;
mod plan;
mod quota;
mod routing;
mod upstream;

pub use cache::{
    CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix, CachePricingSummary, CacheScore,
    TtlClass, WarmCacheEntry,
};
pub use error::{InternalError, InternalErrorKind, InternalErrorStage};
pub use identity::{Principal, PrincipalKind, PrincipalKindLite, ReplicaIdentity};
pub use plan::PlanInfo;
pub use quota::{
    RateLimitKind, RateLimitObservation, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, SubscriptionTier,
};
pub use routing::{
    CandidateUrgency, MAX_ERROR_MESSAGE_LEN, MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN,
    RoutingTrace, StageDecision, SubscriptionPreferenceTrace, TerminalDecision, TerminalStrategy,
};
pub use upstream::{
    ANTHROPIC_IDENTITY_HEADERS, BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
    BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, Upstream, UpstreamCandidate, UpstreamKind,
};
