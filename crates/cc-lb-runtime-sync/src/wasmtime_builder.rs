use std::sync::Arc;
use std::time::Duration;

use cc_lb_config::{
    ShapeOriginPolicy as ConfigShapeOriginPolicy, WasmtimeAllocationStrategy, WasmtimeConfig,
};
use cc_lb_runtime_wasmtime::policy::{PluginWireBounds, ShapeOriginPolicy};
use cc_lb_runtime_wasmtime::{
    HotEngineAllocationStrategy, HotEngineConfig, WasmtimeRuntime, WasmtimeRuntimeError,
};

/// Default per-call epoch preemption timeout when
/// `runtime.wasmtime.execution_timeout_ms` is unset.
pub const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 1_000;

/// Translate configuration-layer [`WasmtimeConfig`] into runtime [`HotEngineConfig`].
pub fn hot_engine_config_from_wasmtime_config(cfg: &WasmtimeConfig) -> HotEngineConfig {
    let defaults = HotEngineConfig::default();
    HotEngineConfig {
        allocation_strategy: match cfg.allocation_strategy {
            WasmtimeAllocationStrategy::OnDemand => HotEngineAllocationStrategy::OnDemand,
            WasmtimeAllocationStrategy::Pooling => HotEngineAllocationStrategy::Pooling,
        },
        memory_max_pages: cfg.memory_max_pages.unwrap_or(defaults.memory_max_pages),
        memory_reservation_bytes: cfg
            .memory_reservation_bytes
            .unwrap_or(defaults.memory_reservation_bytes),
        memory_guard_bytes: cfg
            .memory_guard_bytes
            .unwrap_or(defaults.memory_guard_bytes),
        pool_total_memories: cfg
            .pool_total_memories
            .unwrap_or(defaults.pool_total_memories),
        pool_total_core_instances: cfg
            .pool_total_core_instances
            .unwrap_or(defaults.pool_total_core_instances),
        shape_origin_policy: match cfg.shape_origin_policy {
            ConfigShapeOriginPolicy::Unrestricted => ShapeOriginPolicy::Unrestricted,
            ConfigShapeOriginPolicy::SelectedUpstreamOrigin => {
                ShapeOriginPolicy::SelectedUpstreamOrigin
            }
        },
        wire_bounds: PluginWireBounds {
            output_body_bytes: cfg.wire_bounds.output_body_bytes,
            max_headers: cfg.wire_bounds.max_headers,
            max_header_value_bytes: cfg.wire_bounds.max_header_value_bytes,
            reason_bytes: cfg.wire_bounds.reason_bytes,
        },
        cookie_redaction: cfg.cookie_redaction,
        ..defaults
    }
}

/// Build an initialized [`WasmtimeRuntime`] honoring all fields of [`WasmtimeConfig`],
/// including `execution_timeout_ms`.
pub fn build_wasmtime_runtime(
    cfg: &WasmtimeConfig,
) -> Result<Arc<WasmtimeRuntime>, WasmtimeRuntimeError> {
    let timeout_ms = cfg
        .execution_timeout_ms
        .unwrap_or(DEFAULT_EXECUTION_TIMEOUT_MS);
    WasmtimeRuntime::new_with_execution_timeout(
        hot_engine_config_from_wasmtime_config(cfg),
        Duration::from_millis(timeout_ms),
    )
    .map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_engine_config_forwards_origin_policy_bounds_and_cookie_redaction() {
        let mut cfg = WasmtimeConfig::default();
        assert_eq!(
            hot_engine_config_from_wasmtime_config(&cfg).shape_origin_policy,
            ShapeOriginPolicy::SelectedUpstreamOrigin
        );
        cfg.allocation_strategy = WasmtimeAllocationStrategy::Pooling;
        cfg.shape_origin_policy = ConfigShapeOriginPolicy::Unrestricted;
        cfg.wire_bounds.max_headers = 32;
        cfg.cookie_redaction = true;
        cfg.execution_timeout_ms = Some(250);

        let hot = hot_engine_config_from_wasmtime_config(&cfg);
        assert_eq!(
            hot.allocation_strategy,
            HotEngineAllocationStrategy::Pooling
        );
        assert_eq!(hot.shape_origin_policy, ShapeOriginPolicy::Unrestricted);
        assert_eq!(hot.wire_bounds.max_headers, 32);
        assert!(hot.cookie_redaction);

        let rt = build_wasmtime_runtime(&WasmtimeConfig {
            execution_timeout_ms: Some(50),
            ..WasmtimeConfig::default()
        })
        .expect("runtime builds with configured timeout");
        assert_eq!(
            rt.config().shape_origin_policy,
            ShapeOriginPolicy::SelectedUpstreamOrigin
        );
    }
}
