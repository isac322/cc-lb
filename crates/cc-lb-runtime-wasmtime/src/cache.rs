//! Per-worker thread_local [`WorkerInstance`] cache + ABI call wrapper.
//!
//! The hot path. Every wasm call goes through one of
//! [`call_filter_hook`] / [`call_shape_hook`] /
//! [`call_normalize_error_hook`] / [`call_observe_hook`]. They all
//! share the same alloc → write → call → read → free flow against a
//! single fuel budget; only the typed-func name differs.
//!
//! The cache is rebuilt transactionally on version change
//! (review-consensus invariant: build everything in a local then
//! atomic-swap into the cache). On any trap the entry is discarded
//! so the next call rebuilds a fresh store (review-consensus
//! invariant: drop Store on any trap — no implicit circuit breaker,
//! the rebuild cost is the natural signal).
//!
//! Fuel is set before the alloc / hook / free triple so all three
//! guest calls draw from a single budget. If `cc_lb_free` traps from
//! exhaustion the store is discarded — no risk of accumulating leaked
//! guest memory in a reused store.
//!
//! Observe hooks return `(0, 0)` from the guest, so the alloc/free
//! pair is skipped for them; only the input buffer is alloc'd, the
//! hook runs, the input is free'd.
//!
//! See RFC §실행 모델 + §Operational invariants (review consensus).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use cc_lb_plugin_api::SlotKey;
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
        WasmtimeRuntimeError::PoolSaturated { .. } => "pool_saturated",
    }
}

/// Default archive alignment for rkyv 0.8 root types.
pub const DEFAULT_ALIGN: u32 = 16;

/// Per-worker instantiation, owning its store + typed handles.
///
/// `Empty` is the post-trap / post-eviction state. `Ready` is the
/// steady state. Optional hook funcs are populated only when the
/// plugin exports them — a Filter slot has `filter_fn = Some(_)` and
/// all others `None`; a Shape slot has `shape_fn` and
/// `normalize_error_fn` populated; etc.
///
/// `inspect_wasm` is the gate that ensures the right `Some(_)`s are
/// present for the slot's kind; the dispatch helpers unwrap them
/// with a `ModuleRejected` if a hook the slot kind requires turns out
/// to be missing at instantiate time.
///
/// `Ready` is intentionally much larger than `Empty` — boxing the big
/// variant would add a heap allocation on every hot-path call, so we
/// silence the lint.
#[allow(clippy::large_enum_variant)]
enum WorkerInstance {
    Empty,
    Ready {
        version_id: u64,
        store: Store<HostState>,
        memory: Memory,
        alloc_fn: TypedFunc<(u32, u32), u32>,
        free_fn: TypedFunc<(u32, u32, u32), ()>,
        filter_fn: Option<TypedFunc<(u32, u32), u64>>,
        shape_fn: Option<TypedFunc<(u32, u32), u64>>,
        normalize_error_fn: Option<TypedFunc<(u32, u32), u64>>,
        observe_fn: Option<TypedFunc<(u32, u32), u64>>,
    },
}

impl WorkerInstance {
    fn is_ready_for(&self, version_id: u64) -> bool {
        matches!(self, WorkerInstance::Ready { version_id: v, .. } if *v == version_id)
    }
}

thread_local! {
    static SLOT_INSTANCES: RefCell<HashMap<SlotKey, WorkerInstance>> =
        RefCell::new(HashMap::new());
}

fn build_worker_instance(cell: &PluginCell) -> Result<WorkerInstance, WasmtimeRuntimeError> {
    let engine = cell.instance_pre.module().engine();
    let mut store = Store::new(engine, HostState);

    // RFC-0001 gap-analysis item #13: the pre-instantiate `set_fuel`
    // was redundant because `execute_call` sets the budget again per
    // call. Dropped here so per-call fuel is set exactly at the
    // measurement boundary — see the `set_fuel` inside `execute_call`.

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

    let filter_fn = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_filter")
        .ok();
    let shape_fn = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_shape")
        .ok();
    let normalize_error_fn = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_normalize_error")
        .ok();
    let observe_fn = instance
        .get_typed_func::<(u32, u32), u64>(&mut store, "cc_lb_observe")
        .ok();

    Ok(WorkerInstance::Ready {
        version_id: cell.version_id,
        store,
        memory,
        alloc_fn,
        free_fn,
        filter_fn,
        shape_fn,
        normalize_error_fn,
        observe_fn,
    })
}

/// Pick a hook `TypedFunc` out of a [`WorkerInstance::Ready`] by
/// internal hook name. The runtime guarantees the chosen variant is
/// populated for the slot kind (via [`crate::inspect::inspect_wasm`]).
#[derive(Clone, Copy)]
enum HookFn {
    Filter,
    Shape,
    NormalizeError,
    Observe,
}

impl HookFn {
    fn export_name(self) -> &'static str {
        match self {
            HookFn::Filter => "cc_lb_filter",
            HookFn::Shape => "cc_lb_shape",
            HookFn::NormalizeError => "cc_lb_normalize_error",
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
            HookFn::NormalizeError => "normalize_error",
            HookFn::Observe => "observe",
        }
    }
}

/// Synchronous filter call. Dispatches to the pure or stateful path
/// based on [`PluginCell::pure`].
pub fn call_filter_hook(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_dispatch(slot_key, cell, input, HookFn::Filter)
}

/// Pure-mode filter call: builds a fresh `Store`, runs the hook,
/// drops the `Store`. No thread_local cache, no version-compare.
pub fn call_filter_hook_pure(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_pure(cell, input, HookFn::Filter)
}

/// Stateful filter call: reuses the per-worker `Store` cached for
/// `slot_key` and rebuilds transactionally on `cell.version_id`
/// change.
pub fn call_filter_hook_stateful(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_stateful(slot_key, cell, input, HookFn::Filter)
}

/// Synchronous shape call. Input is rkyv-encoded `ShapeRequest`,
/// output rkyv-encoded `ShapeResponse`.
pub fn call_shape_hook(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_dispatch(slot_key, cell, input, HookFn::Shape)
}

/// Pure-mode shape call. See [`call_filter_hook_pure`].
pub fn call_shape_hook_pure(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_pure(cell, input, HookFn::Shape)
}

/// Stateful shape call. See [`call_filter_hook_stateful`].
pub fn call_shape_hook_stateful(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_stateful(slot_key, cell, input, HookFn::Shape)
}

/// Synchronous normalize_error call. Sibling of [`call_shape_hook`]
/// against the same plugin instance.
pub fn call_normalize_error_hook(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_dispatch(slot_key, cell, input, HookFn::NormalizeError)
}

/// Pure-mode normalize_error call.
pub fn call_normalize_error_hook_pure(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_pure(cell, input, HookFn::NormalizeError)
}

/// Stateful normalize_error call.
pub fn call_normalize_error_hook_stateful(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_stateful(slot_key, cell, input, HookFn::NormalizeError)
}

/// Synchronous observe call. Guest returns `(0, 0)`; the returned
/// `Vec<u8>` is always empty.
pub fn call_observe_hook(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_dispatch(slot_key, cell, input, HookFn::Observe)
}

/// Pure-mode observe call.
pub fn call_observe_hook_pure(
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_pure(cell, input, HookFn::Observe)
}

/// Stateful observe call.
pub fn call_observe_hook_stateful(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    call_hook_stateful(slot_key, cell, input, HookFn::Observe)
}

fn call_hook_dispatch(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    if cell.pure {
        call_hook_pure(cell, input, hook)
    } else {
        call_hook_stateful(slot_key, cell, input, hook)
    }
}

fn call_hook_pure(
    cell: &Arc<PluginCell>,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    let mut wi = build_worker_instance(cell)?;
    execute_call(&mut wi, cell, input, hook)
}

fn call_hook_stateful(
    slot_key: &SlotKey,
    cell: &Arc<PluginCell>,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    SLOT_INSTANCES.with(|cache| {
        let mut cache = cache.borrow_mut();
        let entry = cache
            .entry(slot_key.clone())
            .or_insert(WorkerInstance::Empty);

        if !entry.is_ready_for(cell.version_id) {
            *entry = WorkerInstance::Empty;
            let new_wi = build_worker_instance(cell)?;
            *entry = new_wi;
        }

        let result = execute_call(entry, cell, input, hook);

        if result.is_err() {
            *entry = WorkerInstance::Empty;
        }

        result
    })
}

fn execute_call(
    entry: &mut WorkerInstance,
    cell: &PluginCell,
    input: &[u8],
    hook: HookFn,
) -> Result<Vec<u8>, WasmtimeRuntimeError> {
    let start = Instant::now();
    let plugin = Arc::clone(&cell.plugin_name);
    let hook_label = hook.metric_label();
    let budget = cell.fuel_per_call;

    let result = execute_call_inner(entry, cell, input, hook);

    metrics::histogram!(
        "cc_lb_plugin_call_duration_seconds",
        "plugin" => plugin.to_string(),
        "hook" => hook_label,
    )
    .record(start.elapsed().as_secs_f64());

    match &result {
        Ok((_, remaining_fuel)) => {
            let ratio = if budget > 0 {
                (budget.saturating_sub(*remaining_fuel) as f64) / (budget as f64)
            } else {
                0.0
            };
            metrics::histogram!(
                "cc_lb_plugin_fuel_consumed_ratio",
                "plugin" => plugin.to_string(),
                "hook" => hook_label,
            )
            .record(ratio.clamp(0.0, 1.0));
        }
        Err(err) => {
            metrics::counter!(
                "cc_lb_plugin_trap_total",
                "plugin" => plugin.to_string(),
                "hook" => hook_label,
                "phase" => trap_phase_label(err),
            )
            .increment(1);
        }
    }

    result.map(|(bytes, _)| bytes)
}

fn execute_call_inner(
    entry: &mut WorkerInstance,
    cell: &PluginCell,
    input: &[u8],
    hook: HookFn,
) -> Result<(Vec<u8>, u64), WasmtimeRuntimeError> {
    let WorkerInstance::Ready {
        store,
        memory,
        alloc_fn,
        free_fn,
        filter_fn,
        shape_fn,
        normalize_error_fn,
        observe_fn,
        ..
    } = entry
    else {
        unreachable!("entry must be Ready by call_hook contract");
    };

    let hook_fn = match hook {
        HookFn::Filter => filter_fn.as_ref(),
        HookFn::Shape => shape_fn.as_ref(),
        HookFn::NormalizeError => normalize_error_fn.as_ref(),
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

    store
        .set_fuel(cell.fuel_per_call)
        .map_err(|e| WasmtimeRuntimeError::GuestTrap {
            phase: "set_fuel",
            source: anyhow::Error::from(e),
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
    // explicitly. The single fuel budget set above still covers the
    // guest-side free.
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
    // Filter / shape / normalize_error returning (0, 0) is an ABI
    // violation; collapsing it into empty bytes here would hide the
    // bug from downstream rkyv decode (e.g. normalize_error would
    // silently passthrough as `None`).
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

        free_fn
            .call(&mut *store, (out_ptr, out_len, DEFAULT_ALIGN))
            .map_err(|e| WasmtimeRuntimeError::GuestTrap {
                phase: "cc_lb_free",
                source: anyhow::Error::from(e),
            })?;

        bytes
    };

    // Snapshot remaining fuel AFTER free — the metric wrapper
    // computes consumption against `cell.fuel_per_call`. Guaranteed
    // present because the immediately preceding set_fuel succeeded.
    let remaining_fuel = store.get_fuel().unwrap_or(0);

    Ok((out_bytes, remaining_fuel))
}
