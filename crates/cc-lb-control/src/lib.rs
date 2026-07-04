//! cc-lb-control: admin/control-plane runtime primitives.
//!
//! Currently a skeleton crate; content will be moved from cc-lb-engine
//! in a subsequent restructure batch.

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
pub use dynamic_view::{
    ApplyStatus, DynamicView, DynamicViewBuilder, DynamicViewHolder, UpstreamRateLimitCache,
    UpstreamStatusEntry, UpstreamStatusSnapshot,
};
pub use event_bus::{InMemoryBus, new_in_memory_bus, record_dashboard_sse_lagged};
pub use subscription_metadata_hook::{
    MetadataHookEnqueueError, MetadataHookHandle, MetadataHookRequest, MetadataRefreshEnqueue,
    MetadataRefreshError, MetadataRefreshRecords, fetch_metadata_only, run_metadata_refresh,
    start_subscription_metadata_hook,
};
pub use traits::{
    DynamicViewControl, LimitControl, ManagedKeyControl, NoopSubscriptionQuotaCache,
    PromptCacheObservationCacheLike, PromptCacheObservationEnqueueError,
    PromptCacheObservationSinkLike, RuntimeStatusControl, RuntimeStatusError,
    SubscriptionQuotaCacheLike, SubscriptionQuotaObservationControl,
};
