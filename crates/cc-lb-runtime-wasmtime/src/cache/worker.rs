use wasmtime::{Instance, Memory, Store, TypedFunc};

use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

use super::HookFn;

pub(super) struct WorkerInstance {
    pub(super) store: Store<HostState>,
    pub(super) memory: Memory,
    pub(super) alloc_fn: TypedFunc<(u32, u32), u32>,
    pub(super) free_fn: TypedFunc<(u32, u32, u32), ()>,
    pub(super) filter_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) shape_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) observe_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) transform_response_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) transform_sse_event_fn: Option<TypedFunc<(u32, u32), u64>>,
}

pub(super) fn build_worker_instance(
    cell: &PluginCell,
    hook: HookFn,
) -> Result<WorkerInstance, WasmtimeRuntimeError> {
    let engine = cell.instance_pre.module().engine();
    let mut store = Store::new(engine, HostState::new(cell.memory_max_pages));
    store.limiter(|state| state.limits());

    let instance: Instance = cell.instance_pre.instantiate(&mut store).map_err(|e| {
        if e.downcast_ref::<wasmtime::PoolConcurrencyLimitError>()
            .is_some()
        {
            WasmtimeRuntimeError::PoolSaturated { resource: "pool" }
        } else {
            WasmtimeRuntimeError::InstantiateFailed(anyhow::Error::from(e))
        }
    })?;

    let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
        WasmtimeRuntimeError::ModuleRejected {
            reason: "module does not export `memory`".into(),
        }
    })?;

    let alloc_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "cc_lb_alloc")
        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
            reason: format!("missing or mistyped `cc_lb_alloc` export: {e}"),
        })?;

    let free_fn = instance
        .get_typed_func::<(u32, u32, u32), ()>(&mut store, "cc_lb_free")
        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
            reason: format!("missing or mistyped `cc_lb_free` export: {e}"),
        })?;

    let (filter_fn, shape_fn, observe_fn, transform_response_fn, transform_sse_event_fn) =
        match hook {
            HookFn::Filter => (
                Some(
                    instance
                        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_filter")
                        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                            reason: format!("missing or mistyped `cc_lb_filter` export: {e}"),
                        })?,
                ),
                None,
                None,
                None,
                None,
            ),
            HookFn::Shape => (
                None,
                Some(
                    instance
                        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_shape")
                        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                            reason: format!("missing or mistyped `cc_lb_shape` export: {e}"),
                        })?,
                ),
                None,
                None,
                None,
            ),
            HookFn::Observe => (
                None,
                None,
                Some(
                    instance
                        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_observe")
                        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                            reason: format!("missing or mistyped `cc_lb_observe` export: {e}"),
                        })?,
                ),
                None,
                None,
            ),
            HookFn::TransformResponse => (
                None,
                None,
                None,
                Some(
                    instance
                        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_transform_response")
                        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                            reason: format!(
                                "missing or mistyped `cc_lb_transform_response` export: {e}"
                            ),
                        })?,
                ),
                None,
            ),
            HookFn::TransformSseEvent => (
                None,
                None,
                None,
                None,
                Some(
                    instance
                        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_transform_sse_event")
                        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
                            reason: format!(
                                "missing or mistyped `cc_lb_transform_sse_event` export: {e}"
                            ),
                        })?,
                ),
            ),
        };

    Ok(WorkerInstance {
        store,
        memory,
        alloc_fn,
        free_fn,
        filter_fn,
        shape_fn,
        observe_fn,
        transform_response_fn,
        transform_sse_event_fn,
    })
}
