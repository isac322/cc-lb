//! Shared pure-value domain vocabulary for cc-lb.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod cache;
mod credits;
mod error;
mod identity;
mod plan;
mod quota;
mod routing;
mod upstream;

pub use cache::{
    CacheBreakpoint, CacheBreakpointSource, CacheLookbackPrefix, CachePricingSummary, CacheScore,
    HASH_SCHEMA_VERSION, NativeCachePricing, NativeCachePricingComponent,
    NativeCachePricingLifetime, NativeCachePricingProvider, NativeCachePricingRate,
    NativeCachePricingStatus, PrefixBytes, TtlClass, WarmCacheEntry,
};
pub use credits::{
    CreditObservation, CreditObservations, CreditRequestOrder, CreditSource, is_credit_balance,
};
pub use error::{InternalError, InternalErrorKind, InternalErrorStage};
pub use identity::{Principal, PrincipalKind, PrincipalKindLite, ReplicaIdentity};
pub use plan::PlanInfo;
pub use quota::{
    RateLimitKind, RateLimitObservation, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaCredits, SubscriptionQuotaDataState, SubscriptionTier,
};
pub use routing::{
    CandidateUrgency, MAX_ERROR_MESSAGE_LEN, MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN,
    RoutingTrace, StageDecision, SubscriptionPreferenceTrace, TerminalDecision, TerminalStrategy,
};
pub use upstream::{
    ANTHROPIC_IDENTITY_HEADERS, ANTHROPIC_WORKSPACE_ID_HEADER, BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
    BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, CredentialKind, Provider, Upstream, UpstreamCandidate,
    UpstreamIdentity, UpstreamKind,
};
