//! ABI call wrapper for wasmtime plugin hooks.
//!
//! Every wasm call goes through one of [`call_filter_hook`] /
//! [`call_shape_hook`] / [`call_observe_hook`]. They all share the same
//! alloc → write → call → read → free flow; only the typed-func name differs.
//!
//! Each call builds a fresh [`Store`] via
//! [`PluginCell::instance_pre`] and drops it on return — no
//! thread_local cache, no version-compare. Cold instantiate against
//! a `PoolingAllocator` + `InstancePre` is on the order of ~10 μs,
//! so the state-leak / version-drift complexity of a cached
//! `WorkerInstance` was never worth its cost (RFC-0001 gap-analysis
//! item #11).
//!
//! Observe hooks return `(0, 0)` from the guest, so the output alloc/free
//! pair is skipped; only the input buffer is alloc'd and immediately
//! free'd by the guest helper.
//!
//! See RFC §실행 모델 + §Operational invariants (review consensus).

use std::sync::Arc;
use std::time::Instant;

use wasmtime::{Instance, Memory, Store, TypedFunc};

use crate::cell::PluginCell;
use crate::engine::HostState;
use crate::error::WasmtimeRuntimeError;

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

/// One-shot instantiation for a single hook call.
///
/// Owns the `Store` for the duration of the call and drops it on
/// return. Optional hook funcs are populated only when the plugin
/// exports them — a Filter slot has `filter_fn = Some(_)` and all
/// others `None`; a Shape slot has `shape_fn` populated; etc.
/// [`crate::inspect::inspect_wasm`]
/// is the gate that ensures the right `Some(_)`s are present for the
/// slot's kind at load time.
struct WorkerInstance {
    store: Store<HostState>,
    memory: Memory,
    alloc_fn: TypedFunc<(u32, u32), u32>,
    free_fn: TypedFunc<(u32, u32, u32), ()>,
    filter_fn: Option<TypedFunc<(u32, u32), u64>>,
    shape_fn: Option<TypedFunc<(u32, u32), u64>>,
    observe_fn: Option<TypedFunc<(u32, u32), u64>>,
}

fn build_worker_instance(
    cell: &PluginCell,
    hook: HookFn,
) -> Result<WorkerInstance, WasmtimeRuntimeError> {
    let engine = cell.instance_pre.module().engine();
    let mut store = Store::new(engine, HostState);

    let instance: Instance = cell.instance_pre.instantiate(&mut store).map_err(|e| {
        // Distinguish pool-exhaustion from generic instantiate failure so
        // request-path callers can react (backpressure, 503) without
        // stringy downcasting on the anyhow chain — see RFC-0001 #6.
        if e.downcast_ref::<wasmtime::PoolConcurrencyLimitError>()
            .is_some()
        {
            WasmtimeRuntimeError::PoolSaturated {
                resource: "core-instances",
                limit: 0,
            }
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

    // Only look up the export we're about to call. `inspect_wasm`
    // already validated the module carries the export corresponding
    // to its `SlotKind`, so a missing lookup here is a real error
    // (not the historical "probe all hooks and hope one exists").
    let (filter_fn, shape_fn, observe_fn) = match hook {
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
    })
}

/// Pick a hook `TypedFunc` out of a [`WorkerInstance`] by internal
/// hook name. The runtime guarantees the chosen variant is populated
/// for the slot kind (via [`crate::inspect::inspect_wasm`]).
#[derive(Clone, Copy)]
enum HookFn {
    Filter,
    Shape,
    Observe,
}

impl HookFn {
    fn export_name(self) -> &'static str {
        match self {
            HookFn::Filter => "cc_lb_filter",
            HookFn::Shape => "cc_lb_shape",
            HookFn::Observe => "cc_lb_observe",
        }
    }

    // Short label used as the `hook` dimension on RFC-0001 plugin
    // metrics. Kept distinct from `export_name` so metric label
    // vocabulary doesn't drift when guest export names change.
    fn metric_label(self) -> &'static str {
        match self {
            HookFn::Filter => "filter",
            HookFn::Shape => "shape",
            HookFn::Observe => "observe",
        }
    }
}

/// Synchronous filter call. Builds a fresh `Store`, runs the hook,
/// drops the `Store`.
pub fn call_filter_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook(cell, input, HookFn::Filter)
}

/// Synchronous shape call. Input is rkyv-encoded `ShapeRequest`,
/// output rkyv-encoded `ShapeResponse`.
pub fn call_shape_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook(cell, input, HookFn::Shape)
}

/// Synchronous observe call. Guest returns `(0, 0)`; the returned
/// `Vec<u8>` is always empty.
pub fn call_observe_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook(cell, input, HookFn::Observe)
}

fn call_hook(
    cell: &Arc<PluginCell>,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    let mut wi = build_worker_instance(cell, hook).inspect_err(|err| {
        if let WasmtimeRuntimeError::PoolSaturated { resource, .. } = err {
            metrics::counter!("cc_lb_plugin_pool_saturation_total", "resource" => *resource)
                .increment(1);
        }
    })?;
    execute_call(&mut wi, cell, input, hook)
}

fn execute_call(
    wi: &mut WorkerInstance,
    cell: &PluginCell,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    let start = Instant::now();
    // Reuse the cell's owned `Arc<str>` label instead of `.to_string()`
    // per emission — SharedString accepts `Arc<str>` directly, avoiding
    // an owned-String heap alloc per hook call.
    let plugin: Arc<str> = Arc::clone(&cell.plugin_name);
    let hook_label = hook.metric_label();

    let result = execute_call_inner(wi, input, hook);

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

fn execute_call_inner(
    wi: &mut WorkerInstance,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    let WorkerInstance {
        store,
        memory,
        alloc_fn,
        free_fn,
        filter_fn,
        shape_fn,
        observe_fn,
    } = wi;

    let hook_fn = match hook {
        HookFn::Filter => filter_fn.as_ref(),
        HookFn::Shape => shape_fn.as_ref(),
        HookFn::Observe => observe_fn.as_ref(),
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

    // Only the observe hook is allowed to return (0, 0) — side-effect
    // only by contract (`cc-lb-pdk-wasmtime/src/lib.rs::run_observe*`).
    // Filter / shape returning (0, 0) is an ABI
    // violation; collapsing it into empty bytes here would hide the
    // bug from downstream rkyv decode.
    let out_bytes = if matches!(hook, HookFn::Observe) && out_ptr == 0 && out_len == 0 {
        Vec::new()
    } else if out_ptr == 0 || out_len == 0 {
        return Err(WasmtimeRuntimeError::ModuleRejected {
            reason: format!(
                "{}: guest returned invalid (ptr={out_ptr}, len={out_len}); only observe may return (0, 0)",
                hook.export_name()
            ),
        });
    } else {
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
        let bytes = mem_view[out_ptr as usize..out_end].to_vec();

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
        bytes
    };

    Ok(out_bytes)
}
