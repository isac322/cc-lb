//! Control-plane primitives for admin APIs, dynamic views, audit output, and metadata refresh.
//!
//! This crate owns operator-facing services and subscribers that coordinate
//! stored configuration, API keys, audit records, and runtime status without
//! depending on the data-plane engine crate.

pub mod anthropic_compat;
pub mod anthropic_metadata;
pub mod api_keys;
pub mod audit_payload;
pub mod audit_writer;
pub mod dynamic_view;
pub mod event_bus;
pub mod subscription_metadata_hook;
pub mod traits;

pub use audit_payload::AuditPayload;
pub use audit_writer::{AuditDropped, AuditEntry, AuditWriterSink, spawn_audit_writer};
pub use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin, RoutingContext};
pub use dynamic_view::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot,
};
pub use event_bus::{
    BusReceiver, DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, InMemoryBus, LifecycleBusReceiver,
    RequestEventBus, new_in_memory_bus, record_dashboard_sse_lagged,
};
pub use subscription_metadata_hook::{
    MetadataHookEnqueueError, MetadataHookHandle, MetadataHookRequest, MetadataRefreshEnqueue,
    MetadataRefreshError, MetadataRefreshRecords, fetch_metadata_only, run_metadata_refresh,
    start_subscription_metadata_hook,
};
pub use traits::{
    DynamicViewControl, LimitControl, ManagedKeyControl, NoopSubscriptionQuotaCache,
    PromptCacheObservationCacheLike, PromptCacheObservationEnqueueError,
    PromptCacheObservationInput, PromptCacheObservationSinkLike, PromptCacheThreadUsage,
    RuntimeStatusControl, RuntimeStatusError, SubscriptionQuotaCacheLike,
    SubscriptionQuotaSampleControl,
};

pub trait ReplicaIdentityProvider: Send + Sync {
    fn replica_identity(&self) -> Option<cc_lb_domain::ReplicaIdentity>;
}
