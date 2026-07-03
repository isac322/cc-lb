use wasmtime::Engine;

use crate::engine::{HotEngineAllocationStrategy, HotEngineConfig};

pub(crate) fn publish_pool_metrics(engine: &Engine, config: &HotEngineConfig) {
    let (memories_util, instances_util) = match engine.pooling_allocator_metrics() {
        Some(m) => {
            metrics::gauge!("cc_lb_plugin_pool_memories_used").set(m.memories() as f64);
            metrics::gauge!("cc_lb_plugin_pool_core_instances_used").set(m.core_instances() as f64);
            let mem_denom = config.pool_total_memories.max(1) as f64;
            let inst_denom = config.pool_total_core_instances.max(1) as f64;
            let mem_util = (m.memories() as f64) / mem_denom;
            let inst_util = (m.core_instances() as f64) / inst_denom;
            (mem_util.clamp(0.0, 1.0), inst_util.clamp(0.0, 1.0))
        }
        None => {
            metrics::gauge!("cc_lb_plugin_pool_memories_used").set(0.0);
            metrics::gauge!("cc_lb_plugin_pool_core_instances_used").set(0.0);
            (0.0, 0.0)
        }
    };
    let (pool_memories_total, pool_core_instances_total) = match config.allocation_strategy {
        HotEngineAllocationStrategy::OnDemand => (0, 0),
        HotEngineAllocationStrategy::Pooling => {
            (config.pool_total_memories, config.pool_total_core_instances)
        }
    };
    metrics::gauge!("cc_lb_plugin_pool_memories_total").set(pool_memories_total as f64);
    metrics::gauge!("cc_lb_plugin_pool_core_instances_total").set(pool_core_instances_total as f64);
    metrics::gauge!("cc_lb_plugin_memory_max_pages").set(config.memory_max_pages as f64);
    metrics::gauge!("cc_lb_plugin_memory_reservation_bytes")
        .set(config.memory_reservation_bytes as f64);
    metrics::gauge!("cc_lb_plugin_memory_guard_bytes").set(config.memory_guard_bytes as f64);
    metrics::gauge!("cc_lb_plugin_pool_virtual_reservation_bytes")
        .set(config.virtual_memory_reservation_bytes() as f64);
    metrics::gauge!("cc_lb_plugin_pool_memories_utilization_ratio").set(memories_util);
    metrics::gauge!("cc_lb_plugin_pool_core_instances_utilization_ratio").set(instances_util);
}
