pub trait EngineMetricsHook: Send + Sync {
    fn record_cache_hit(&self, upstream: &str, model: &str);

    fn record_cache_miss(&self, upstream: &str, model: &str);

    fn record_cache_observation_dropped(&self, reason: &str);

    fn record_dropped_events_by(&self, reason: &str, count: u64);

    fn record_routing_tier_selection(&self, tier: &str, upstream: &str, principal_id: &str);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricsHook;

impl EngineMetricsHook for NoopMetricsHook {
    fn record_cache_hit(&self, _upstream: &str, _model: &str) {}

    fn record_cache_miss(&self, _upstream: &str, _model: &str) {}

    fn record_cache_observation_dropped(&self, _reason: &str) {}

    fn record_dropped_events_by(&self, _reason: &str, _count: u64) {}

    fn record_routing_tier_selection(&self, _tier: &str, _upstream: &str, _principal_id: &str) {}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MetricsCrateHook;

impl EngineMetricsHook for MetricsCrateHook {
    fn record_cache_hit(&self, upstream: &str, model: &str) {
        metrics::counter!(
            "cc_lb_cache_hit_total",
            "upstream" => upstream.to_owned(),
            "model" => model.to_owned()
        )
        .increment(1);
    }

    fn record_cache_miss(&self, upstream: &str, model: &str) {
        metrics::counter!(
            "cc_lb_cache_miss_total",
            "upstream" => upstream.to_owned(),
            "model" => model.to_owned()
        )
        .increment(1);
    }

    fn record_cache_observation_dropped(&self, reason: &str) {
        metrics::counter!(
            "cc_lb_cache_observation_dropped_total",
            "reason" => reason.to_owned()
        )
        .increment(1);
    }

    fn record_dropped_events_by(&self, reason: &str, count: u64) {
        crate::increment_dropped_events_by(reason, count);
    }

    fn record_routing_tier_selection(&self, tier: &str, upstream: &str, principal_id: &str) {
        metrics::counter!(
            "cc_lb_routing_tier_selections_total",
            "tier" => tier.to_owned(),
            "upstream" => upstream.to_owned(),
            "principal_id" => principal_id.to_owned()
        )
        .increment(1);
    }
}
