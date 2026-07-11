use cc_lb_observability::{EngineMetricsHook, NoopMetricsHook};

#[test]
fn noop_metrics_hook_accepts_every_metric_event() {
    // Given a metrics hook that intentionally emits no metrics.
    let hook = NoopMetricsHook;

    // When the engine emits each supported metric event.
    hook.record_cache_hit("upstream", "model");
    hook.record_cache_miss("upstream", "model");
    hook.record_cache_observation_dropped("queue_full");
    hook.record_dropped_events_by("queue_full", 1);
    hook.record_routing_tier_selection("tier", "upstream", "principal");

    // Then each event is accepted without side effects.
}
