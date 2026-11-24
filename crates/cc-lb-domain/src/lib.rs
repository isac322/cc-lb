//! Shared pure-value domain vocabulary for cc-lb.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod cache;
mod credits;
mod error;
mod identity;
mod openai_oauth_compat;
mod plan;
mod quota;
mod routing;
mod upstream;
mod upstream_affinity;
/// Vertex AI endpoint and credential configuration types.
pub mod vertex;

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
pub use openai_oauth_compat::{
    CoerceOrReject, DropOrReject, EmulateDropOrReject, EmulateOrReject, EnabledOrDisabled,
    HostedToolHandling, MetadataHandling, OpenaiOauthCompatOverride, OpenaiOauthCompatPolicy,
};
pub use plan::PlanInfo;
pub use quota::{
    RateLimitKind, RateLimitObservation, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaCredits, SubscriptionQuotaDataState, SubscriptionTier,
};
pub use routing::{MAX_ERROR_MESSAGE_LEN, TerminalStrategy};
pub use upstream::{
    ANTHROPIC_IDENTITY_HEADERS, ANTHROPIC_WORKSPACE_ID_HEADER, BUILTIN_SUBSCRIPTION_PREFERENCE_ID,
    BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, CredentialKind, Provider, Upstream, UpstreamCandidate,
    UpstreamIdentity, UpstreamKind,
};
pub use upstream_affinity::UnknownOpenaiReasoningPolicy;
pub use vertex::{
    VERTEX_CREDENTIAL_INVALID_CLIENT, VERTEX_CREDENTIAL_INVALID_GRANT, VertexApiVersion,
    VertexCredentialType, VertexTarget, VertexUpstreamConfig, VertexValidationError,
    origin as vertex_origin, validate_location, validate_project_id,
    vertex_credential_requires_replacement,
};
