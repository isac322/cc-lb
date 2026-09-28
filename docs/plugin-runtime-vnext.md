# Plugin Runtime vNext — Design Document

Status: Proposal (implementation not started)
Audience: cc-lb core/platform engineers, plugin authors
Prerequisite docs: `docs/runtime-management.md`, `docs/plugin-author-guide.md`
Source audit: `.omo/ultraresearch/20260627-plugin-hotpath-vnext/SYNTHESIS.md`

## 0. Purpose and scope

This document records the design decisions for a new plugin runtime that removes the per-call OS thread/Tokio runtime/spawn_blocking/Mutex/JSON+base64 costs the current Extism wrapper imposes on cc-lb's plugin call hot path, swaps plugins dynamically without a server restart, and guarantees compile-time type safety of function signatures for both plugin authors and the host.

Document scope: architecture decisions + component design + migration plan + unresolved risks.
Out of scope: code PRs, plugin authoring tutorial (`docs/plugin-author-guide.md` updated in Phase 4), measured benchmark numbers (filled in during Phase 3).

## 1. Background

cc-lb plugin calls sit on every request hot path. The current structure pays the following costs on every request:

1. `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` `dispatch_wire_call_sync` and `dispatch_filter_call_sync` spawn a new OS thread per call via `std::thread::Builder::new().spawn(...)`, build a new Tokio runtime inside it via `tokio::runtime::Builder::new_current_thread().build()`, then `block_on(dispatch_*_async)`.
2. Inside async dispatch, `tokio::task::spawn_blocking` drops back onto the blocking pool, takes `Mutex<Plugin>`, and calls `extism::Plugin::call::<String, String>("filter", json)`.
3. The wire is a JSON envelope; the body is base64.
4. `PluginCell { plugin: Mutex<Plugin> }` inside `PluginSlot.current::ArcSwap<PluginCell>` holds one instance per slot. Concurrent calls to the same plugin serialize on the mutex.

Summary: per request, `OS thread creation + Tokio runtime creation + spawn_blocking + Mutex + JSON/base64 serialization` accumulate. The wrapper cost exceeds the plugin's own wasm execution cost.

`benches/extism_sse_overhead` already has an explicit criterion to escalate to wasmtime if batched 32-event p99 exceeds 1ms (`STAY_EXTISM` vs `ESCALATE_WASMTIME`). For product-critical hot-path functions this margin is not enough.

## 2. Goals / non-goals

### Goals

- **G1 thread hop 0**: remove all new OS thread creation, new Tokio runtime creation, and hops to the blocking pool from the filter / shape / sign call hot path.
- **G2 zero-downtime dynamic replacement**: atomically swap plugin code and manifest without a server restart. Preserve the existing `DynamicViewHolder = ArcSwap<DynamicView>` + per-slot `ArcSwap<PluginCell>` + two-phase stage/commit semantics as-is.
- **G3 bidirectional compile-time type safety**: both the plugin author and the host verify the same function signature at compile time. Wire serialization mismatches fail at build or plugin registration time, not at runtime.
- **G4 eliminate payload serialization cost**: remove JSON and base64. Body and headers pass as raw bytes at zero-copy or single-memcpy level.
- **G5 keep the sandbox**: memory isolation, resource limits (memory pages, instruction count), and trap isolation at or above the current Extism level.
- **G6 preserve multi-language plugin potential**: Rust-only for now, but a structure that can extend to polyglot by swapping the wire format.

### Non-goals

- **N1 external BC**: cc-lb has not shipped yet. Binary compatibility with existing Extism plugins is not maintained.
- **N2 wall-clock kill guarantee**: no "kill unconditionally after exactly N ms" guarantee is provided. See §5.5 for the detailed reason.
- **N3 plugin-calls-host-async-I/O model**: no scenario where a plugin on the hot path calls a host async function and yields.
- **N4 migrate everything in a single release**: gradual transition behind a feature gate.

### User-stated constraints (verbatim excerpts, translated)

The constraints in this document derive directly from the following user statements:

- m0027 (problem definition): "Handling this with threads makes no sense. Is there no way to run wasm directly? This is the hot path."
- m0049 (requirements confirmed):
  - "A rewrite is fine — this is a genuinely extreme hot path, so if it isn't optimized the product itself is meaningless."
  - "It hasn't shipped yet, so I'm going to delete all backward compatibility."
  - "Remove all the layer costs like thread pools."
  - "You don't have to use extism, you can rebuild the plugins from scratch, you don't even have to use wasm itself."
  - "It must be swappable dynamically without a server restart, and ideally with no thread hops."
  - "I prefer the approach with as few as possible; if that's impossible, I'll forgive up to a thread pool."
  - "What's also important when developing these plugins... the function signature matters a lot. I'd like a way to develop plugins while preserving type safety of these function signatures."
- m0077 (fuel/thread semantics confirmed): "The fuel issue doesn't matter. That's their own loss." → user-side approval of this document's §5.5 deterministic-fuel policy and no-wall-clock-kill decision.

These quotes are the user-side justification for the §2 goals (G1–G6), non-goals (N1–N4), §4 adoption rationale, and the §5.5 fuel policy.

## 3. Current architecture (what to keep, what to discard)

### Keep

- Trait shapes in `cc-lb-plugin-api/src/traits.rs`: `FilterPlugin`, `UpstreamDialect`, `Signer`, `SignerFactory`, `PluginRuntime`. The surface that callers (`Lifecycle::handle`, `attempt`, `execute_filter_pipeline`) depend on. Minimize signature changes.
- `cc-lb-engine/src/dynamic_view.rs::DynamicViewHolder = ArcSwap<DynamicView>`. Lock-free request path.
- `cc-lb-engine/src/api_keys/principal_view.rs::PrincipalSpecCached` and `resolved_pipeline / resolved_dialect`.
- Per-principal staging in `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`.
- DB polling + revision-hash-based rebuild in `cc-lb-server/src/reconcile.rs`.
- `data/plugins/wasm/cache/{sha256}.wasm` disk cache.
- Inherit only the **semantic intent** of "schema/version agreement between plugin and host" that `crates/cc-lb-runtime-protocol/src/handshake.rs` handled. The current implementation actually executes a JSON `cc_lb_handshake` export; the new ABI is a completely different mechanism that statically inspects the wasm custom section `cc_lb_schema_hash`. Discard the code, keep the intent.

### Discard

- All of `crates/cc-lb-runtime-extism/`.
- `crates/cc-lb-runtime-protocol/` (the JSON envelope in dispatch.rs).
- `crates/cc-lb-plugin-wire/v1, v2, v3` JSON definitions and base64 fields.
- The Extism-PDK-dependent parts of `crates/cc-lb-pdk`.
- Bundled plugin (`plugins/router/*`, `plugins/shape/*`) build artifacts — rebuilt with the new PDK (source logic preserved).

**.wasm modules built with the existing Extism PDK do not run as-is on the new runtime** (direct answer to the m0044 user question): the host import signatures differ (`extism:host/env::input_load_u8` etc. vs this design's `cc_lb_alloc`/`cc_lb_free` self-export model), the wire envelope is completely different (JSON+base64 vs rkyv archive), and the missing schema_hash custom section fails registration-time validation. Because cc-lb has not shipped and there are no external plugins (N1), rebuilding only the bundled plugins suffices.

## 4. Alternatives compared

Result of 8-agent cross-validation and oracle adversarial review consensus (audit in SYNTHESIS.md §"Cross-agent consensus").

| Candidate | thread hop | type safety | zero-downtime swap | sandbox | decision |
|---|---|---|---|---|---|
| Current Extism + JSON | per-call OS thread + runtime + spawn_blocking | runtime only | yes | strong | **discard** |
| **Wasmtime core ABI + cc-lb PDK + rkyv** | **0 (per-worker store)** | **3-layer compile-time + load-time hash** | **yes (existing primitives)** | **strong** | **adopted** |
| Wasmtime Component Model + WIT | 0 possible | compile-time both sides | yes | strong | deferred (canonical ABI list/string copy, current sync path overhead reported) |
| Native dylib (libloading / abi_stable) | 0 | strong (load-time checks) | **no (dlclose + glibc TLS, stated by abi_stable itself)** | none | rejected |
| eBPF (rbpf JIT) | 0 | none (raw byte buffers) | yes (program swap) | strong (verifier) | rejected |
| Embedded scripting (Luau/Rhai/Rune) | 0 | weak (dynamically typed) | yes | varies | rejected |

Rationale details:
- Native dylib: the `abi_stable` docs officially declare "without support for unloading", and glibc `__cxa_thread_atexit_impl` blocks `dlclose` of libraries that used thread-locals. Incompatible with zero-downtime replacement.
- WIT Component Model satisfies G3 most cleanly but the current standard cannot guarantee G4's list/string zero-copy. The WASI 0.3.x lazy-lowering roadmap may resolve it, but as of 2026 production stability and sync path overhead are unverified.
- eBPF/rbpf JIT is a raw-byte-buffer model that collides head-on with G3 (compile-time type-safe signatures). It lacks an assertion mechanism like wasm Rust struct + macro, and plugin authoring ergonomics are very poor. Forcing the eBPF instruction set/verifier constraints on plugin authors is unrealistic.
- Scripting cannot satisfy G3 (no assertable signatures).
- Raw Wasmtime core ABI is the only candidate satisfying all of G1–G6. The downside is that we must write the PDK proc-macro ourselves (§5.2).

**Caution on performance numbers**: figures this document cites — "sub-ms instantiation", "TypedFunc trampoline ~20-100ns", "rkyv vs JSON 30-100x" — are **expectations/indicators** based on external sources, not guarantees in cc-lb's actual payload environment. Final adoption is decided by the cc-lb-payload-specific benchmark in §8.

### 4.1 Incremental improvement proposals reviewed once and rejected (decision history)

Before this vNext rewrite decision, the following incremental proposals were presented once and explicitly rejected by the user at m0049. Recorded with rejection reasons.

| Incremental stage | Content | Rejection reason |
|---|---|---|
| Stage 1 | Add default async methods to `FilterPlugin`, `UpstreamDialect`. Only `ExtismFilterPlugin`/`ExtismDialectPlugin` override to `.await` the existing `dispatch_*_async`. Removes new OS thread + Tokio runtime build. | Not thread hop 0. One thread hop via `spawn_blocking` + blocking pool remains. |
| Stage 2 | Introduce a per-plugin async semaphore before `spawn_blocking`. Convert concurrent calls from mutex waits to async queue waits. | Only changes the mutex to an async queue; the thread hop itself remains. Core cost unresolved. |
| Stage 3 | Measure, then decide between Extism `Pool` or Wasmtime ABI vNext. | "Measure then decide" is incompatible with a product-critical hot path requirement. The user stated "if it isn't optimized the product itself is meaningless." |

The adopted vNext rewrite bypasses all of Stage 1+2+3 and drops thread hops to 0. The user's "a rewrite is fine", "don't have to use extism", "don't even have to use wasm" made this decision possible.

## 5. Adopted design

### 5.1 Execution model — per-worker thread-local instance

**Worker definition and count**:
- "Worker" means an OS thread inside the Tokio multi-thread runtime's **core worker pool** — the fixed-size pool that polls async tasks.
- Separately, Tokio has a **blocking pool** for `spawn_blocking`. This design does not use the blocking pool at all.
- The core worker count is a Tokio runtime setting:
  - Default = `std::thread::available_parallelism()` (usually the CPU core count; reflects cpuset/quota in containers).
  - Explicit: `tokio::runtime::Builder::new_multi_thread().worker_threads(N).build()`.
- cc-lb-server currently starts via `#[tokio::main]` or an explicit `Builder` — unchanged. Operators may set `worker_threads` explicitly for capacity planning (see R2, R3).

```rust
// per-worker thread-local cache
thread_local! {
    static SLOT_INSTANCES: RefCell<HashMap<SlotKey, WorkerInstance>> = RefCell::new(HashMap::new());
}

struct WorkerInstance {
    version_id: u64,
    store: wasmtime::Store<HostState>,
    typed_filter: Option<TypedFunc<(u32, u32), u64>>,
    typed_shape:  Option<TypedFunc<(u32, u32), u64>>,
    // ...
}
```

Call flow:

```text
Lifecycle::handle (Tokio core worker thread)
  ├─ view = dynamic_view.load()             // ArcSwap, lock-free
  ├─ spec  = view.principal_view.get(pid)
  ├─ pipe  = spec.resolved_pipeline()
  └─ for filter in pipe.user_filters:
       ├─ slot   = filter.slot_key()         // (principal, plugin_name)
       ├─ cell   = filter.cell.load()        // Arc<PluginCell>, version_id, InstancePre
       ├─ SLOT_INSTANCES.with(|m| {
       │     let wi = m.borrow_mut().entry(slot).or_insert_with(|| ...);
       │     if wi.version_id != cell.version_id {
       │         // re-instantiate. sub-ms with PoolingAllocator + memory_init_cow
       │         wi.store = Store::new(&engine, host_state.clone());
       │         let instance = cell.instance_pre.instantiate(&mut wi.store)?;
       │         wi.typed_filter = Some(instance.get_typed_func(&mut wi.store, "filter")?);
       │         wi.version_id = cell.version_id;
       │     }
       │     // call. same OS thread, same worker, sync. no blocking pool.
       │     wi.typed_filter.unwrap().call(&mut wi.store, (in_ptr, in_len))
       │  })
       └─ rkyv::access::<ArchivedFilterResponse>(&out_bytes)
```

Key properties:
- **thread hop 0**: no `std::thread::spawn`, `Runtime::Builder::build`, or `spawn_blocking` on the hot path.
- **mutex contention 0**: thread-local means one independent instance per worker per plugin slot. Concurrent calls to the same plugin parallelize naturally across workers.
- **instance reuse**: when the same worker calls the same slot at the same version again, the store and typed_func are reused. Call cost is the `TypedFunc::call` trampoline entry — expected at tens-to-hundreds of ns per Wasmtime docs (verified by measurement in §8).
- **cost on version change**: `InstancePre.instantiate(&mut new_store)`. With Wasmtime PoolingAllocator + `Config::memory_init_cow(true)`, Wasmtime's fast-instantiation docs cite sub-ms as achievable — actual measurement in the cc-lb environment is confirmed by the §8 bench.

### 5.2 PDK — `cc-lb-pdk-wasmtime`

The code a plugin author sees is an ordinary Rust function. Key point: rkyv's `Archived<T>` has a different memory layout from `T` (`Archived<String>` is offset+len, `Archived<Vec<T>>` is relative pointer+len), so `transmute`-ing to `&T` is unsound. The PDK provides safe `*Ref<'_>` view types.

```rust
use cc_lb_pdk::prelude::*;
use cc_lb_plugin_types::v4::{FilterRequestRef, FilterOutput, FilterError, UpstreamId};

#[cc_lb_pdk::plugin(filter)]
pub fn my_filter(req: FilterRequestRef<'_>) -> Result<FilterOutput, FilterError> {
    // FilterRequestRef is macro-generated. Holds &ArchivedFilterRequest internally.
    // Every accessor converts the archived representation to normal Rust types:
    //   req.method()    -> &str
    //   req.path()      -> &str
    //   req.headers()   -> impl Iterator<Item = (&str, &[u8])>
    //   req.body()      -> &[u8]
    //   req.principal() -> PrincipalRef<'_>
    //   req.candidates()-> impl ExactSizeIterator<Item = UpstreamCandidateRef<'_>>

    let kept: Vec<UpstreamId> = req
        .candidates()
        .map(|c| c.upstream_id())
        .collect();

    Ok(FilterOutput {
        kept_upstream_ids: kept,
        reason: "pass-through".into(),
        per_candidate_reasons: vec![],
    })
}
```

What the proc-macro `#[cc_lb_pdk::plugin(filter)]` auto-generates:

1. **Compile-time signature assertion** — verifies the user function's signature exactly matches the per-variant (filter, shape, sign, etc.) definition. A mismatch fails the build.

   ```rust
   const _: () = {
       fn _assert<F>(_: F)
       where F: for<'a> Fn(FilterRequestRef<'a>) -> Result<FilterOutput, FilterError> {}
       _assert(my_filter);
   };
   ```

2. **Guest export function** — safely accesses archived bytes through the `*Ref<'_>` view type:

   ```rust
   #[no_mangle]
   pub extern "C" fn cc_lb_filter(in_ptr: u32, in_len: u32) -> u64 {
       let bytes = unsafe { core::slice::from_raw_parts(in_ptr as *const u8, in_len as usize) };
       // rkyv access returns &ArchivedFilterRequest after linear validation via bytecheck
       let archived: &ArchivedFilterRequest =
           match rkyv::access::<ArchivedFilterRequest, rkyv::rancor::Error>(bytes) {
               Ok(a) => a,
               Err(_) => return pack_err(ErrorCode::InvalidRequest),
           };
       // FilterRequestRef is a thin wrapper over archived. No transmute.
       let req_ref = FilterRequestRef::from_archived(archived);
       match my_filter(req_ref) {
           Ok(out) => emit_output(out),
           Err(e)  => pack_err_user(e),
       }
   }
   ```

   `*Ref<'_>` types are auto-derived by macro in `cc-lb-plugin-types` (a helper derive like `#[derive(ArchiveRef)]`). No `transmute` used.

3. **schema_hash custom section** — embeds a `cc_lb_schema_hash` custom section in the wasm module. Hash input is `(wire version + function variant name + rkyv archive layout fingerprint)`. Only host and guest built with the same cc-lb-plugin-types version produce the same hash. A mismatch is rejected at plugin registration time.

4. **alloc/free helper exports (wasm exports exposed guest → host, not host imports)** — `cc_lb_alloc(len: u32) -> u32`, `cc_lb_free(ptr: u32, len: u32)`. Before the host writes the input buffer into guest linear memory it calls the guest function to reserve the region → `Memory::write` → typed_func call. The guest leaves output in its own memory and returns packed `(out_ptr, out_len)`; the host calls `cc_lb_free(out_ptr, out_len)` right after it finishes processing the archived view.

The host side shares the same Rust types crate (`cc-lb-plugin-types`), so it only needs `TypedFunc<(u32, u32), u64>` and the alloc/free TypedFuncs. **The host-side `Linker::func_wrap` registers zero functions for hot-path plugins** (mechanical enforcement of the no-host-call rule on the sync hot path, §5.6).

Three verification layers:
- L1 (guest compile time): proc-macro `_assert` const block.
- L2 (host compile time): `TypedFunc<Params, Results>` Rust generics enforce core ABI types.
- L3 (load time): `cc_lb_runtime_wasmtime::register_slot` compares the wasm module's `cc_lb_schema_hash` custom section against the expected hash. On mismatch, registration is rejected, no `StagedSlot` is created, and the existing slot is unaffected.

**rkyv safety invariants (both PDK and runtime must uphold)**:
- Never use `access_unchecked`/`from_bytes_unchecked`. Always call `rkyv::access::<T, rancor::Error>(bytes)` to validate via bytecheck on every call.
- `cc_lb_alloc` returns memory at an alignment satisfying the archive root type's max align (`align_of::<ArchivedFilterRequest>()`). The PDK either exports `alloc_aligned(len, align)` or the macro generates a wrapper allocator that fixes the root type's align.
- The `(out_ptr, out_len)` the guest returns must satisfy the same alignment condition.
- While the host holds an `&Archived*` view obtained via `rkyv::access`, it must not call guest `cc_lb_free` during that view's lifetime. Processing order: `access → copy/decode needed values to owned → cc_lb_free(out_ptr, out_len)`.
- While the host holds a guest archived view, **re-entry into the same instance on the same worker (= a new typed_func.call on the same Store)** is forbidden. A new call mutates linear memory and invalidates the archived view. The PDK macro/host helper enforces this via borrow lifetimes so a re-call is impossible while the view scope is open.

### 5.3 Wire format — rkyv zero-copy

Selection rationale: in the bg_e1173fc3 comparison, ~30-100x faster than JSON+base64 (0 guest allocations), and for Rust-to-Rust it also avoids FlatBuffers' builder complexity. If multi-language plugins are needed later, the wire can be swapped to FlatBuffers (the macro's `wire = "..."` parameter).

Representative types:
```rust
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct FilterRequest {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, Vec<u8>)>,   // raw bytes, no base64
    pub body_bytes: Vec<u8>,               // raw bytes
    pub principal: Principal,
    pub candidates: Vec<UpstreamCandidate>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct FilterResponse {
    pub results: Vec<PerCandidateReason>,
}
```

Host send:
```rust
let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&req)?;            // ~1 alloc on host
let in_ptr = call_guest_alloc(&mut store, &alloc_fn, bytes.len())?;  // guest alloc
mem.write(&mut store, in_ptr as usize, &bytes)?;                      // single memcpy
let packed = typed_filter.call(&mut store, (in_ptr, bytes.len() as u32))?;
let (out_ptr, out_len) = unpack(packed);
let out_slice = &mem.data(&store)[out_ptr as usize .. out_ptr as usize + out_len as usize];
let archived  = rkyv::access::<ArchivedFilterResponse, _>(out_slice)?;  // zero-copy, bytecheck
```

Guest receive is exactly §5.2 — a zero-allocation view via `rkyv::access`.

Validation: rkyv `check_bytes` (=`bytecheck`) validates archive structural integrity, alignment, and enum discriminants in a linear pass. The host never calls `access_unchecked`.

### 5.4 Hot-swap — preserve existing primitives

Data structure changes:
```rust
// new PluginCell
pub struct PluginCell {
    pub version_id:    u64,                         // monotonically increasing, worker invalidation trigger
    pub instance_pre:  Arc<InstancePre<HostState>>, // precompiled + pre-linked
    pub schema_hash:   [u8; 32],
    pub fuel_per_call: u64,
    pub memory_max_pages: u32,
}

pub struct PluginSlot {
    pub name:    String,
    pub entry:   RwLock<PluginEntry>,             // manifest, registry metadata
    pub current: ArcSwap<PluginCell>,             // unchanged
}
```

Swap flow:
```text
Reconciler or admin API
  ├─ read wasm bytes from disk cache (data/plugins/wasm/cache/{sha}.wasm)
  ├─ Engine::precompile_module(&engine, &wasm_bytes) → serialized .cwasm
  ├─ Module::deserialize_file(&engine, ...) (unsafe, safe because it's our own build artifact)
  ├─ Linker.instantiate_pre(&module) → InstancePre
  ├─ extract schema_hash custom section, compare with expected → abort on failure
  ├─ self-check call (once) → abort on failure
  ├─ new PluginCell { version_id: prev+1, instance_pre: Arc::new(pre), ... }
  ├─ load into StagedSlot
  └─ commit_staged() → `slot.current.store(Arc::new(cell))` only if every slot succeeded
```

On the next request the worker sees that `cell.version_id` differs from its `WorkerInstance.version_id` → `InstancePre.instantiate(&mut new_store)` → re-extract typed_func. The old `InstancePre`'s `Arc` drops naturally when the worker releases its last reference. No dlclose-style race.

### 5.5 Resource limits and isolation — fuel

- **Memory**: per-store `Memory` page limit + `Config::max_wasm_stack`. Exceeding the limit traps the wasm.
- **Instruction count**: per-store `Store::set_fuel(N)`. The host calls `set_fuel(cell.fuel_per_call)` at the start of every call. Exhaustion → `Trap::OutOfFuel`. Same wasm + same input + same budget always traps at the same instruction. **Instruction-deterministic.**
- **No wall-clock kill (intentional)**: fuel is instruction-count determinism, not time determinism. The same 100M instructions take different time depending on CPU load/cache state. A "kill unconditionally after exactly N ms" guarantee needs one of the following, and we adopt none:
  1. Wasmtime epoch interrupt — an external timer calls `engine.increment_epoch()`. Wasm checks the epoch only at function entry and loop backedges. Because of the known issue where one plugin's timeout traps other plugins when they share an engine, each plugin would need its own engine. Added complexity.
  2. OS signal — bypasses the wasm trap mechanism. No safety guarantee.
  3. Another thread + JoinHandle drop — reintroduces the thread hop we removed.
- **Decision**: fuel determinism suffices. If a plugin exhausts fuel the call fails, `FilterError::Trap` is returned, and `execute_filter_pipeline`'s existing trap branch (which already treats plugin traps as passthrough) handles it. User policy: "the plugin's own responsibility."

### 5.6 Host function policy and guest-internal async

**Async code inside the wasm guest**:
- Writing `async fn` in guest Rust is possible per se. But it only compiles to a state machine inside the guest; **the call never yields to the host**. From the host's view, a `cc_lb_filter` export call is a single synchronous call until return or trap.
- Real async I/O in the guest requires (1) importing host async functions and (2) enabling fiber stack switching via wasmtime async support. This design forbids that on the hot path (N3).
- So a plugin author may use `async`/`await` inside the guest, but the effect equals an ordinary synchronous function call. The premise of short CPU-only logic holds.

**Host function call policy (mechanically enforced at load time)**:
- Hot-path plugins (filter, shape, normalize_error) **may not call host functions**. All inputs arrive in the call's archive; outputs return as an archive. Reason: once a host call enters, the simple sync hot path ends and worker occupancy time becomes unpredictable.
- Enforcement mechanism (register-time validation, not convention):
  1. The hot-path engine's `Linker` is created empty. Zero `func_wrap` registrations.
  2. `Config::wasm_component_model(false)`, no `WasiCtx`, no `wasmtime-wasi` Linker extension.
  3. Inspect `Module::imports()` at registration — if the import set is non-empty, reject registration with `RuntimeError::InstantiateFailed`.
  4. From `Module::exports()`, allow only the hooks declared in the manifest among `cc_lb_filter`/`cc_lb_shape`/... plus `cc_lb_alloc`/`cc_lb_free`. Extra exports are harmless but warn-logged.
  5. The signer engine is separate (§5.7), so its host fns are registered on a separate Linker. Sharing module/InstancePre with the hot-path engine is forbidden.
- Result: no plugin has any way to reach host state, network, filesystem, or clock. Attempts are rejected at instantiate.
- Signer is not on the hot path, so async + host I/O remain allowed as today. Wasmtime async support is enabled only on the signer-dedicated engine (§5.7).

### 5.7 Engine configuration

- **Hot-path engine** (sync): `Config::async_support(false)`, `Config::strategy(Strategy::Cranelift)`, `Config::signals_based_traps(true)`, `Config::memory_reservation(1<<32)`, `Config::memory_guard_size(1<<32)`, `Config::memory_init_cow(true)`, `InstanceAllocationStrategy::Pooling(PoolingAllocationConfig { total_memories: ..., max_memory_size: 32MiB, ... })`. Fuel `Config::consume_fuel(true)`. All workers share this engine. Each worker owns its own Store.
- **Signer engine** (async): `Config::async_support(true)` + epoch interrupt. Separated from the hot-path engine to prevent epoch interference.
- **Engine-specific module/InstancePre**: Wasmtime `Module` and `InstancePre` are bound to the `Engine` that created them. Hot-path and signer must each run **`precompile_module → deserialize → InstancePre` separately** even with the same wasm bytes. `WasmtimeRuntime` holds both engines and keeps separate `Linker`s and InstancePre caches for each.

**PoolingAllocator capacity formula and failure policy**:

Upper bound on concurrently live instances:
```
max_in_flight ≈ Σ_(slot s) ( worker_threads × live_versions(s) )
                + signer engine in-flight (async, separate cap)
                + transient residue (old versions workers haven't invalidated yet after commit)
```

`PoolingAllocationConfig`'s `total_memories`, `total_core_instances`, `total_stacks` (async only) must be comfortably larger than this value. At boot cc-lb-server reserves `worker_threads × (active_slots × 2 + headroom)`; growth beyond that triggers an admin warn-log + Prometheus metric `cc_lb_plugin_pool_saturation_ratio`.

Behavior at cap:
- Instantiate failure during new plugin registration (`stage_slot`) → no `StagedSlot` → whole `commit_staged` aborts → **all existing slots unaffected** (existing cells stay active). The admin API returns `RuntimeError::InstantiateFailed { reason: "pool saturated, increase total_memories or evict unused slots" }`.
- Thread-local re-instantiation failure mid-request on the hot path → only that request returns `FilterError::Runtime`, falling through to `execute_filter_pipeline`'s trap branch (per passthrough or fail policy). No effect on other requests.

## 6. Per-component changes

### 6.1 New crate `cc-lb-runtime-wasmtime`

Location: `crates/cc-lb-runtime-wasmtime/`
Role: implements `cc-lb-plugin-api::PluginRuntime` on wasmtime.

Main types:
```text
WasmtimeRuntime { hot_engine: Engine, signer_engine: Engine, slots: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>, host_state: Arc<HostState> }
PluginSlot      { name, entry: RwLock<PluginEntry>, current: ArcSwap<PluginCell> }
PluginCell      { version_id, instance_pre, schema_hash, fuel_per_call, memory_max_pages }
StagedSlot      { key, entry, slot }
WasmtimeFilterPlugin   impl FilterPlugin
WasmtimeDialectPlugin  impl UpstreamDialect
WasmtimeSignerFactory  impl SignerFactory  (async, separate engine)
```

API:
- `register_slot(manifest) -> Arc<PluginSlot>`
- `stage_slot(principal_id, plugin_name, manifest, hook) -> (Arc<dyn ...>, StagedSlot)`
- `commit_staged(Vec<StagedSlot>)`
- `evict_slot(principal_id, plugin_name)`
- `reload(manifest_ref)`

Method names/signatures match the existing `ExtismRuntime` API so `dynamic_view_builder.rs` works nearly unchanged.

### 6.2 New crate `cc-lb-pdk-wasmtime`

Location: `crates/cc-lb-pdk-wasmtime/`
Contents:
- `cc-lb-pdk-wasmtime-macros` — the `#[cc_lb_pdk::plugin(...)]` proc-macro.
- `cc-lb-pdk-wasmtime` — runtime helpers (`cc_lb_alloc`, archive view abstraction, error helpers).
- `cc-lb-plugin-types` (close to a reorganization of the existing `cc-lb-plugin-api/types.rs`) — host/guest shared types + rkyv derives.

### 6.3 Changed cc-lb code

- `cc-lb-plugin-api/src/traits.rs`: trait signatures unchanged. Except: add `slot_key(&self) -> SlotKey` to `FilterPlugin` to provide the thread_local cache key.
- `cc-lb-engine/src/lifecycle.rs::execute_filter_pipeline`: call code identical. If a plugin returns a trap, the existing branch handles it.
- `cc-lb-engine/src/api_keys/principal_view.rs::RouterPipelineCache`: unchanged.
- `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`: calls `runtime.instantiate_filter_for/...` on `WasmtimeRuntime` (same trait).
- `cc-lb-server/src/reconcile.rs`: unchanged.
- `cc-lb-server/src/tls.rs`: unchanged.
- `Cargo.toml`: add direct `wasmtime = "47"` dependency. Remove `extism = ...` (after the transition ends).

### 6.4 Bundled plugin rebuilds

- `plugins/router/cache-aware` → new PDK, logic unchanged.
- `plugins/shape/subscription-launderer` → new PDK, logic unchanged.

In each plugin's `Cargo.toml`, replace the `extism-pdk` dependency with `cc-lb-pdk-wasmtime`, and replace `#[plugin_fn]` with `#[cc_lb_pdk::plugin(filter)]` etc.

## 7. Migration phases

Per-phase implementation difficulty is the estimate the user requested at m0046. Scale: 1 (trivial) ~ 10 (rewrite-grade).

1. **Phase 0 — new crate skeletons** (difficulty **2/10**)
   - Create `cc-lb-runtime-wasmtime`, `cc-lb-pdk-wasmtime`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-plugin-types`. Gate behind workspace feature `runtime-wasmtime`. wasmtime 47 dependency.
   - An empty `WasmtimeRuntime` implements the `PluginRuntime` trait. All methods panic unimplemented.
   - Just needs to bind the existing trait signatures. Confirm the build passes.

2. **Phase 1 — single filter hook integration** (difficulty **6/10**)
   - Implement the PDK macro's filter variant + rkyv wire types + `*Ref<'_>` view auto-generation macro.
   - Hot-path engine config (`Config::async_support(false)` + PoolingAllocator + CoW + fuel) + per-worker thread_local instance cache.
   - schema_hash custom section read/verify + load-time imports inspection.
   - Rebuild the bundled `cache-aware` plugin with the new PDK.
   - New conformance test (`tests/plugin-lifecycle/wasmtime_filter.rs`) verifies filter calls, hot-swap, version_id increments, fuel trips.
   - The hardest parts are the rkyv view macro design and the alignment-aware alloc helper.

3. **Phase 2 — shape, signer integration** (difficulty **5/10**)
   - shape, normalize_error: repeat the filter pattern.
   - signer: async engine (`Config::async_support(true)`) + epoch interrupt + separate Linker + separate InstancePre cache.

4. **Phase 3 — bench + flip** (difficulty **3/10**)
   - Run `benches/extism_sse_overhead` in the new-runtime variant (`runtime-wasmtime` feature).
   - Confirm non-streaming p50 overhead, p99 overhead, and streaming per-event p50/p99 all improve on or match the current baseline.
   - Flip the default runtime to wasmtime.
   - Little code change; measurement/tuning may take time.

5. **Phase 4 — Extism removal** (difficulty **2/10**)
   - Delete `cc-lb-runtime-extism`, `cc-lb-runtime-protocol`, `cc-lb-plugin-wire/v1,v2,v3`, `cc-lb-pdk(=extism)`, and all JSON/base64 code paths.
   - Remove `extism`, `extism-pdk` dependencies from the workspace.
   - Update docs (`docs/plugin-author-guide.md`, `docs/runtime-management.md`).

**Overall difficulty comparison (preserving the m0046 answer)**:
- Removing only the per-call thread/runtime on the current Extism path: 3-4/10 (similar to part of Phase 1). But Mutex/JSON/base64 costs remain → the rejected incremental path in §4.1.
- This vNext (Phases 0–4 combined): 7/10 — raw Wasmtime + custom ABI vNext design is the heaviest. Extism PDK compatibility cost is 0 (avoided via N1).
- Moving to Wasmtime while keeping Extism compatibility (reimplementing the Extism ABI): 8-9/10. **Not chosen.**

## 8. Verification plan

- **Conformance**: port the existing `crates/cc-lb-plugin-conformance` scenarios to the new PDK + runtime. Filter happy path, trap, fuel trip, invalid archive, schema_hash mismatch, in-flight request consistency during hot-swap.
- **Benchmark**: run `benches/extism_sse_overhead` as-is. Confirm the `tests/load/baseline.json` thresholds (non-streaming p50 overhead <5ms, p99 <20ms, streaming p50 per event <2ms).
- **Property test**: add a wasmtime variant to the lifecycle property tests in `tests/property/`.
- **Loom**: add new PluginCell swap race cases to the dynamic_view race property tests in `tests/loom`.
- **Manual load**: with `tests/load` scenarios, measure worker thread occupancy, memory footprint, p99 latency at 1k RPS on a single node.

## 9. Unresolved risks and decisions needed

- **R1 worker occupancy**: one plugin call holds its worker for the call duration. Even with fuel enforcing an instruction ceiling, a very heavy plugin holding one worker for a long time prevents that worker from processing other queued tasks. SLOs are enforced via fuel_per_call. Recommended initial values: filter 50M, shape 100M (each sub-ms to a few ms under typical Wasmtime throughput assumptions).
- **R2 memory footprint + thread-local eviction**: `worker_threads × active_plugin_slots × memory_max_pages`. 8 workers × 20 slots × 32 pages (2 MiB) = ~320 MiB ceiling. With PoolingAllocator + CoW, actual RSS is much smaller.
   - However, when a slot is evicted (`evict_slot`) or its version swapped, the worker thread-local `SLOT_INSTANCES` map keeps the entry → the old Store leaks.
   - Policy: (a) on each call compare `cell.version_id` and overwrite stale entries with new ones (current §5.1 flow). (b) On slot eviction, a background drain task signals every worker with `worker_drain_for_slot(slot_key)` → the worker removes the thread-local entry at its next await point. (c) Add a periodic LRU sweep as a cc-lb-server background task to GC entries unused for N minutes.

- **R2b principal removal**: when a principal is deleted, all its SlotKeys disappear at the next Reconciler rebuild. The corresponding SlotKey entries in worker thread-locals are also cleaned up by policies (a)/(b)/(c).
- **R3 Wasmtime engine sharing scope**: one hot-path engine shared by all workers. PoolingAllocator's `total_memories` etc. are a hard cap on concurrently live instances. Exceeding the cap in operation fails instantiation. Operational policy decision needed (reconfigure the engine when slot count grows vs provision generous caps).
- **R4 schema evolution**: adding a new field changes schema_hash → all plugins need rebuilds. OK at this stage where backward compat is unwanted, but if wire version negotiation becomes necessary later, the macro must declare `wire_version` and the host must support multi-version dispatch.
- **R5 wasmtime 47 vs Extism 1.30 (wasmtime 43)**: two wasmtime versions coexist during the concurrent-dependency period. Cargo can carry both majors (no rust orphan rule conflict). Cleaned up in Phase 4 when Extism is removed.
- **R6 PDK view type consistency**: if the mapping between `*Ref<'_>` view types and `Archived*` types breaks, wrong memory gets read. Generate both types from the same macro in `cc-lb-plugin-types` to keep a single source. Cross-cargo-version verification is handled by the §5.2 schema_hash custom section. The host calls only `rkyv::access` (not unchecked), validating archive structural integrity via bytecheck on every call.
- **R7 signer engine separation**: operating an async engine and a sync engine in the same process. Resource accounting/memory leak monitoring must be separated.

## 10. References

- Consensus audit: `.omo/ultraresearch/20260627-plugin-hotpath-vnext/SYNTHESIS.md`
- External:
  - Wasmtime 47 Tuning for Fast Instantiation, Fast Execution, Pre-Compiling Wasm.
  - rkyv 0.8 `Archive`, `access`, `bytecheck`.
  - `abi_stable` 0.11 "without support for unloading" statement.
  - glibc `__cxa_thread_atexit_impl` and `dlclose` blocking behavior.
- Internal:
  - `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` (current dispatch cost).
  - `crates/cc-lb-runtime-extism/src/lib.rs` (current ArcSwap + stage/commit).
  - `crates/cc-lb-engine/src/dynamic_view.rs` (DynamicViewHolder).
  - `crates/cc-lb-engine/src/api_keys/principal_view.rs` (PrincipalSpecCached).
  - `crates/cc-lb-server/src/dynamic_view_builder.rs` (build_principal_chains).
  - `benches/extism_sse_overhead/src/main.rs` (1ms threshold).
  - `tests/load/baseline.json` (performance baseline).
