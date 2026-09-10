//! RFC-0002 lifecycle-pipeline observability primitives.

pub const LIFECYCLE_BUS_DROPPED_METRIC: &str = "cc_lb_dropped_events_total";

pub mod lifecycle_bus_channel {
    pub const WRITER: &str = "lifecycle_writer_full";
    pub const ASSEMBLER: &str = "lifecycle_assembler_full";
    pub const HOOK_ADAPTER: &str = "lifecycle_hook_adapter_full";
    pub const PRICING: &str = "lifecycle_pricing_full";
    pub const LIMIT_RECONCILE: &str = "lifecycle_limit_reconcile_full";
    pub const CACHE_OBS: &str = "lifecycle_cache_obs_full";
    pub const RATE_LIMIT_HEADER: &str = "lifecycle_rate_limit_header_full";
    pub const SUBSCRIPTION_QUOTA: &str = "lifecycle_subscription_quota_full";
    pub const LIMIT_REJECTION_AUDIT: &str = "lifecycle_limit_rejection_audit_full";
    pub const API_KEY_METRICS: &str = "lifecycle_api_key_metrics_full";
    pub const CACHE_HIT_MISS: &str = "lifecycle_cache_hit_miss_full";
    pub const AGGREGATOR: &str = "lifecycle_aggregator_full";
}

pub fn inc_lifecycle_bus_dropped(channel: &'static str) {
    crate::increment_dropped_events_by(channel, 1);
}
