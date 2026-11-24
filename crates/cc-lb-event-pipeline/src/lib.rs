#![forbid(unsafe_code)]
//! Off-hot-path lifecycle event assembly, subscribers, metrics, and storage fanout.

pub mod lifecycle_api_key_metrics_subscriber;
pub mod lifecycle_cache_hit_miss_subscriber;
pub mod lifecycle_event_assembler;
pub mod lifecycle_event_logger;
pub mod lifecycle_limit_reconcile_subscriber;
pub mod lifecycle_limit_rejection_audit_subscriber;
pub mod lifecycle_rate_limit_header_subscriber;
pub mod lifecycle_routing_tier_subscriber;
pub mod lifecycle_subscription_quota_subscriber;
pub mod metrics_labels;
pub mod pg_notify_fanout;
pub mod request_diagnostics_sink;
pub mod storage_tail_poller;
pub mod subscription_quota_events;
pub mod upstream_rate_limit_events;
pub mod warmup_attempts;

pub use lifecycle_api_key_metrics_subscriber::{
    ApiKeyMetricsSubscriberHandle, spawn_lifecycle_api_key_metrics_subscriber,
};
pub use lifecycle_cache_hit_miss_subscriber::CacheHitMissSubscriberHandle;
pub use lifecycle_event_assembler::{
    RequestEventAssemblerHandle, spawn_request_event_assembler,
    spawn_request_event_assembler_with_diagnostics,
};
pub use lifecycle_event_logger::{LifecycleEventLoggerHandle, spawn_lifecycle_event_logger};
pub use lifecycle_limit_reconcile_subscriber::{
    LimitReconcileSubscriberHandle, spawn_lifecycle_limit_reconcile_subscriber,
};
pub use lifecycle_limit_rejection_audit_subscriber::{
    LimitRejectionAuditSubscriberHandle, spawn_lifecycle_limit_rejection_audit_subscriber,
};
pub use lifecycle_rate_limit_header_subscriber::{
    RateLimitHeaderSubscriberHandle, spawn_lifecycle_rate_limit_header_subscriber,
};
pub use lifecycle_routing_tier_subscriber::RoutingTierSubscriberHandle;
pub use lifecycle_subscription_quota_subscriber::{
    SubscriptionQuotaSubscriberHandle, spawn_lifecycle_subscription_quota_subscriber,
};
pub use metrics_labels::{
    NotifyDropReason, NotifySentOutcome, PartialTrigger, PgListenerReconnectReason, ResetReason,
};
pub use pg_notify_fanout::{
    DEFAULT_PG_NOTIFY_CHANNEL, PARTIAL_NOTIFY_MPSC_CAPACITY, PgListener, PgNotifier, PgNotifyFanout,
};
pub use request_diagnostics_sink::{
    REQUEST_DIAGNOSTICS_QUEUE_CAPACITY, REQUEST_DIAGNOSTICS_SEND_TIMEOUT,
    RequestDiagnosticsDelivery, RequestDiagnosticsOptions, RequestDiagnosticsSink,
    StdoutRequestDiagnosticsSink,
};
pub use storage_tail_poller::StorageTailPoller;
pub use subscription_quota_events::{
    SubscriptionQuotaEnqueueError, SubscriptionQuotaSink, SubscriptionQuotaWriterConfig,
    start_subscription_quota_writer,
};
pub use upstream_rate_limit_events::{
    UpstreamRateLimitEnqueueError, UpstreamRateLimitSink, start_upstream_rate_limit_writer,
};
