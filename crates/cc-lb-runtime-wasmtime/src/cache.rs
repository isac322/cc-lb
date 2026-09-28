//! ABI call wrapper for wasmtime plugin hooks.
//!
//! Every wasm call goes through one of [`call_filter_hook`] /
//! [`call_shape_hook`] / the response transform hooks. They all share
//! the same alloc → write → call → read → free flow; only the
//! typed-func name differs.
//!
//! Each call builds a fresh [`Store`] via
//! [`PluginCell::instance_pre`] and drops it on return — no
//! thread_local cache, no version-compare. Cold instantiate against
//! a `PoolingAllocator` + `InstancePre` is on the order of ~10 μs,
//! so the state-leak / version-drift complexity of a cached
//! `WorkerInstance` was never worth its cost (RFC-0001 gap-analysis
//! item #11).
//!
//! See RFC §실행 모델 + §Operational invariants (review consensus).

use std::sync::Arc;
use std::time::Instant;

use crate::cell::PluginCell;
use crate::error::WasmtimeRuntimeError;

mod hooks;
mod worker;

pub use hooks::{
    call_filter_hook, call_shape_hook, call_transform_response_hook, call_transform_sse_event_hook,
};
pub(crate) use hooks::{
    call_filter_hook_scoped, call_shape_hook_scoped, call_transform_response_hook_scoped,
    call_transform_sse_event_hook_scoped,
};
use worker::{WorkerInstance, build_worker_instance};

// RFC-0001 gap-analysis #5. Label values for the `phase` dimension
// on `cc_lb_plugin_trap_total`. Kept bounded so cardinality stays
// small; extend only when a new failure mode is genuinely distinct.
fn trap_phase_label(err: &WasmtimeRuntimeError) -> &'static str {
    match err {
        WasmtimeRuntimeError::GuestTrap { phase, .. } => phase,
        WasmtimeRuntimeError::ModuleRejected { .. } => "reject",
        WasmtimeRuntimeError::InstantiateFailed(_) => "instantiate",
        WasmtimeRuntimeError::ModuleCompile(_) => "compile",
        WasmtimeRuntimeError::EngineInit(_) => "engine_init",
        WasmtimeRuntimeError::ProbeFailed { .. } => "probe",
        WasmtimeRuntimeError::PoolSaturated { .. } => "pool_saturated",
    }
}

/// Default archive alignment for rkyv 0.8 root types.
pub const DEFAULT_ALIGN: u32 = 16;

/// Pick a hook `TypedFunc` out of a [`WorkerInstance`] by internal
/// hook name. The runtime guarantees the chosen variant is populated
/// for the slot kind (via [`crate::inspect::inspect_wasm`]).
#[derive(Clone, Copy)]
enum HookFn {
    Filter,
    Shape,
    TransformResponse,
    TransformSseEvent,
}

impl HookFn {
    fn export_name(self) -> &'static str {
        match self {
            HookFn::Filter => "cc_lb_filter",
            HookFn::Shape => "cc_lb_shape",
            HookFn::TransformResponse => "cc_lb_transform_response",
            HookFn::TransformSseEvent => "cc_lb_transform_sse_event",
        }
    }

    // Short label used as the `hook` dimension on RFC-0001 plugin
    // metrics. Kept distinct from `export_name` so metric label
    // vocabulary doesn't drift when guest export names change.
    fn metric_label(self) -> &'static str {
        match self {
            HookFn::Filter => "filter",
            HookFn::Shape => "shape",
            HookFn::TransformResponse => "transform_response",
            HookFn::TransformSseEvent => "transform_sse_event",
        }
    }
}

fn call_hook_scoped<R, F>(
    cell: &Arc<PluginCell>,
    input: &[u8],
    hook: HookFn,
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    let _store_permit = cell
        .store_budget
        .try_acquire()
        .inspect_err(record_pool_saturation)?;
    let mut wi = build_worker_instance(cell, hook).inspect_err(record_pool_saturation)?;
    execute_call_scoped(&mut wi, cell, input, hook, with_output)
}

fn record_pool_saturation(err: &WasmtimeRuntimeError) {
    if let WasmtimeRuntimeError::PoolSaturated { resource } = err {
        metrics::counter!("cc_lb_plugin_pool_saturation_total", "resource" => *resource)
            .increment(1);
    }
}

fn execute_call_scoped<R, F>(
    wi: &mut WorkerInstance,
    cell: &PluginCell,
    input: &[u8],
    hook: HookFn,
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    let start = Instant::now();
    // Reuse the cell's owned `Arc<str>` label instead of `.to_string()`
    // per emission — SharedString accepts `Arc<str>` directly, avoiding
    // an owned-String heap alloc per hook call.
    let plugin: Arc<str> = Arc::clone(&cell.plugin_name);
    let hook_label = hook.metric_label();

    let result = execute_call_inner_scoped(wi, input, hook, with_output);

    metrics::histogram!(
        "cc_lb_plugin_call_duration_seconds",
        "plugin" => Arc::clone(&plugin),
        "hook" => hook_label,
    )
    .record(start.elapsed().as_secs_f64());

    match &result {
        Ok(_) => {}
        Err(err) => {
            metrics::counter!(
                "cc_lb_plugin_trap_total",
                "plugin" => plugin,
                "hook" => hook_label,
                "phase" => trap_phase_label(err),
            )
            .increment(1);
        }
    }

    result
}

fn execute_call_inner_scoped<R, F>(
    wi: &mut WorkerInstance,
    input: &[u8],
    hook: HookFn,
    with_output: F,
) -> Result<R, WasmtimeRuntimeError>
where
    F: for<'a> FnOnce(&'a [u8]) -> R,
{
    let WorkerInstance {
        store,
        memory,
        alloc_fn,
        free_fn,
        filter_fn,
        shape_fn,
        transform_response_fn,
        transform_sse_event_fn,
    } = wi;

    let hook_fn = match hook {
        HookFn::Filter => filter_fn.as_ref(),
        HookFn::Shape => shape_fn.as_ref(),
        HookFn::TransformResponse => transform_response_fn.as_ref(),
        HookFn::TransformSseEvent => transform_sse_event_fn.as_ref(),
    }
    .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
        reason: format!(
            "plugin does not export `{}` — slot kind mismatch (inspect should have caught this)",
            hook.export_name()
        ),
    })?;

    let input_len: u32 =
        input
            .len()
            .try_into()
            .map_err(|_| WasmtimeRuntimeError::ModuleRejected {
                reason: format!("input too large: {} bytes exceeds u32::MAX", input.len()),
            })?;

    let in_ptr = alloc_fn
        .call(&mut *store, (input_len, DEFAULT_ALIGN))
        .map_err(|e| WasmtimeRuntimeError::GuestTrap {
            phase: "cc_lb_alloc",
            source: anyhow::Error::from(e),
        })?;

    // PDK contract: cc_lb_alloc returns 0 on invalid layout / OOM
    // (`cc-lb-pdk-wasmtime/src/lib.rs::alloc_bytes`). The host MUST
    // treat that as a trap signal — writing to guest address 0 would
    // corrupt the bss instead.
    if in_ptr == 0 {
        return Err(WasmtimeRuntimeError::GuestTrap {
            phase: "cc_lb_alloc",
            source: anyhow::anyhow!("guest returned null pointer for {input_len}-byte allocation"),
        });
    }

    memory
        .write(&mut *store, in_ptr as usize, input)
        .map_err(|e| WasmtimeRuntimeError::GuestTrap {
            phase: "memory.write",
            source: anyhow::Error::from(e),
        })?;

    // PDK contract: the guest helper (`cc_lb_pdk_wasmtime::__private::run_*`)
    // calls `cc_lb_free(in_ptr, in_len, DEFAULT_ALIGN)` as soon as it has
    // owned/borrowed the input bytes — so the host never frees `in_ptr`
    // explicitly.
    let packed = hook_fn
        .call(&mut *store, (in_ptr, input_len))
        .map_err(|e| WasmtimeRuntimeError::GuestTrap {
            phase: hook.export_name(),
            source: anyhow::Error::from(e),
        })?;

    let out_ptr = (packed >> 32) as u32;
    let out_len = (packed & 0xFFFF_FFFF) as u32;

    // Every hook must return a non-empty output buffer. Collapsing
    // (0, 0) into empty bytes here would hide the ABI violation from
    // downstream rkyv decode.
    if out_ptr == 0 || out_len == 0 {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: format!(
                "{}: guest returned invalid (ptr={out_ptr}, len={out_len})",
                hook.export_name()
            ),
        });
    }
    let mem_view = memory.data(&*store);
    let out_end = (out_ptr as usize)
        .checked_add(out_len as usize)
        .ok_or_else(|| WasmtimeRuntimeError::ModuleRejected {
            reason: "guest output ptr+len overflows usize".into(),
        })?;
    if out_end > mem_view.len() {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: format!(
                "guest output [{}..{}] out of bounds (memory size {})",
                out_ptr,
                out_end,
                mem_view.len()
            ),
        });
    }
    let output = with_output(&mem_view[out_ptr as usize..out_end]);

    // Skip `cc_lb_free(out_ptr, out_len, ..)` on the output
    // buffer: the surrounding pure-mode contract drops the whole
    // `Store` on function return, so the pool immediately
    // reclaims every memory page. Calling the guest allocator to
    // "free" bytes that are about to vanish costs a host↔guest
    // transition. `cc_lb_free` for the INPUT
    // buffer is still driven by the guest PDK (see the comment
    // above `hook_fn.call`) — we're only skipping the OUTPUT
    // free because it happens AFTER `hook_fn` returns.
    let _ = free_fn;

    Ok(output)
}
