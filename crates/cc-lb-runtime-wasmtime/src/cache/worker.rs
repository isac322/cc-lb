use cc_lb_plugin_wire::schema::HookKind;
use wasmtime::{Instance, Memory, Store, TypedFunc};

use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

pub(super) struct WorkerInstance {
    pub(super) store: Store<HostState>,
    pub(super) memory: Memory,
    pub(super) alloc_fn: TypedFunc<(u32, u32), u32>,
    pub(super) filter_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) shape_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) transform_response_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) transform_sse_event_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) transform_realtime_frame_fn: Option<TypedFunc<(u32, u32), u64>>,
    pub(super) shape_envelope_gemini_fn: Option<TypedFunc<(u32, u32), u64>>,
}

pub(super) fn build_worker_instance(
    cell: &PluginCell,
    hook: HookKind,
    export_name: &str,
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

    let hook_fn = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, export_name)
        .map_err(|e| WasmtimeRuntimeError::ModuleRejected {
            reason: format!("missing or mistyped `{export_name}` export: {e}"),
        })?;
    let (
        filter_fn,
        shape_fn,
        transform_response_fn,
        transform_sse_event_fn,
        transform_realtime_frame_fn,
        shape_envelope_gemini_fn,
    ) = match hook {
        HookKind::Filter => (Some(hook_fn), None, None, None, None, None),
        HookKind::Shape => (None, Some(hook_fn), None, None, None, None),
        HookKind::TransformResponse => (None, None, Some(hook_fn), None, None, None),
        HookKind::TransformSseEvent => (None, None, None, Some(hook_fn), None, None),
        HookKind::TransformRealtimeFrame => (None, None, None, None, Some(hook_fn), None),
        HookKind::ShapeEnvelopeGemini => (None, None, None, None, None, Some(hook_fn)),
    };

    Ok(WorkerInstance {
        store,
        memory,
        alloc_fn,
        filter_fn,
        shape_fn,
        transform_response_fn,
        transform_sse_event_fn,
        transform_realtime_frame_fn,
        shape_envelope_gemini_fn,
    })
}
