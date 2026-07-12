# RFC-0001: Plugin runtime vNext

> Historical design note: this RFC records the runtime-vnext design process and may reference earlier crate names, schema-hash terminology, and removed hook concepts. For the current plugin author contract, see [the plugin author guide](../plugin-author-guide.md).

- Feature Name: `plugin-runtime-vnext`
- Start Date: 2026-06-27
- Status: Implemented — Phase 4 complete.
- Implementation history is intentionally omitted from the public design note.
- ADR: [docs/adr/0001-plugin-runtime-vnext.md](../adr/0001-plugin-runtime-vnext.md)
- Audit: internal design notes are not included in the public tree.

## Summary

Rewrite cc-lb's plugin call hot path from Extism + JSON to the raw `wasmtime` core ABI plus an in-house PDK. Drive thread hops to zero on the hot path, deliver body/header via rkyv zero-copy, and let plugin authors get compile-time signature enforcement by writing plain typed Rust functions. The existing `ArcSwap`-based zero-downtime dynamic replacement primitives are preserved as-is.

## Motivation

### Cost of the current hot path

The steps that accumulate when a single request invokes a plugin once (see the corresponding runtime call sites in the repository):

1. `std::thread::Builder::new().name(...).spawn(move || ...)` — a new OS thread is spawned per call.
2. Inside that thread, `tokio::runtime::Builder::new_current_thread().enable_time().build()` — a new Tokio runtime is built per call.
3. Enter `runtime.block_on(dispatch_*_async)`.
4. Inside the async dispatch, `tokio::task::spawn_blocking(...)` — hop into the blocking pool.
5. Acquire the mutex on `PluginCell { plugin: Mutex<Plugin> }`. Concurrent calls into the same slot serialize.
6. `serde_json::to_value/to_string` + `_v` envelope key insertion — JSON encoding plus heap allocations.
7. `base64`-encode body/header — the largest CPU cost on big payloads.
8. `extism::Plugin.call::<String, String>(name, input)` — cross into the Extism wasm boundary; the guest reads input bytes one at a time via host imports and JSON-decodes them.
9. The response path is the mirror image of the above.
10. Join the thread, drop the runtime.

`benches/extism_sse_overhead/src/main.rs` already mandates `ESCALATE_WASMTIME` when the 1ms p99 threshold is exceeded. This margin is insufficient for product-critical hot-path functions.

### User-side hard constraints

The constraints this RFC must satisfy are derived directly from the following user statements.

> "It makes no sense to handle this with threads. Isn't there a way to execute wasm directly? This is the hot path." — problem statement.

> "A rewrite is fine. This is genuinely an extreme hot path — if it can't be optimized the product itself is meaningless. We have not shipped yet, so I will throw out all backward compatibility. What we have to do right now is eliminate layer costs like thread pools. ... We don't have to use Extism, we can rebuild the plugins from scratch, we don't even have to use wasm. The only requirements are dynamic plugin swap without a server restart, and ideally zero thread hops. I prefer the path with the fewest hops; if that's truly impossible I'll forgive up to one thread pool hop. Also — for plugin development this matters too: from cc-lb's perspective the plugin takes a defined input, mutates it, and returns a defined output, so the function signature is extremely important. I want plugin authors to enjoy compile-time type safety on that signature." — requirements lock-in.

> User-approved policy: plugin CPU cost belongs to the plugin; the host does not need an instruction-metering kill switch.

Paraphrase:

- **HC-1 Zero thread hops (preferred); one thread-pool hop is the maximum allowed fallback.**
- **HC-2 Plugins must be dynamically swappable without a server restart.**
- **HC-3 Plugin authors must get compile-time type safety on the function signature.**
- **HC-4 cc-lb is pre-launch, so plugin BC, Extism, wasm itself, and the existing plugin build format may all be replaced freely.**
- **HC-5 Plugin CPU cost is the plugin's own responsibility. No wall-clock kill guarantee required.**

## Guide-level explanation

### What the plugin author sees

Just decorate a plain Rust function with one proc-macro:

```rust
use cc_lb_pdk::prelude::*;
use cc_lb_plugin_types::v4::{FilterRequestRef, FilterOutput, FilterError, UpstreamId};

#[cc_lb_pdk::plugin(filter)]
pub fn my_filter(req: FilterRequestRef<'_>) -> Result<FilterOutput, FilterError> {
    // `req` is a safe view over the archived bytes. Accessors return ordinary Rust types:
    //   req.method()      -> &str
    //   req.path()        -> &str
    //   req.headers()     -> impl Iterator<Item = (&str, &[u8])>
    //   req.body()        -> &[u8]
    //   req.principal()   -> PrincipalRef<'_>
    //   req.candidates()  -> impl ExactSizeIterator<Item = UpstreamCandidateRef<'_>>

    let kept: Vec<UpstreamId> = req
        .candidates()
        .filter(|c| c.observed_at_unix_secs() > 0)
        .map(|c| c.upstream_id())
        .collect();

    Ok(FilterOutput {
        kept_upstream_ids: kept,
        reason: "freshness-only".into(),
        per_candidate_reasons: vec![],
    })
}
```

The build artifact is a single wasm32 module. Register it with the cc-lb host and you are done.

### Signature mismatches fail the build

```rust
#[cc_lb_pdk::plugin(filter)]
pub fn my_filter(req: &str) -> u32 { 0 }
//                ^^^^^^^^^^^^^^^^^ error[E0277]: ... `_assert` requires
//                                  `Fn(FilterRequestRef<'_>) -> Result<FilterOutput, FilterError>`
```

The host shares the same `cc-lb-plugin-types` crate, so the same signature is enforced on the host side too. Build OK → registration OK → and on load the `schema_hash` must also match for activation. If any of these three stages disagree, the failure happens at build or registration time, not at runtime.

### Host registration and hot-swap

The existing admin API and reconciler receive plugin manifests as before and run staging → commit. The new `wasmtime`-based runtime implements the same `PluginRuntime` trait without code changes, so `crates/cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains` keeps working with almost no modification.

## Reference-level explanation

### Execution model

```
Lifecycle::handle (async fn on Tokio core worker)
  ├─ view = dynamic_view.load()                      // ArcSwap, lock-free
  ├─ spec = view.principal_view.get(principal_id)
  ├─ pipeline = spec.resolved_pipeline()
  └─ for filter in pipeline.user_filters:
       ├─ cell = filter.cell.load()                  // Arc<PluginCell>
       ├─ SLOT_INSTANCES.with(|m| {
       │     // per-worker thread_local cache
       │     let wi = m.borrow_mut()
       │         .entry(filter.slot_key())
       │         .or_insert_with(|| WorkerInstance::empty());
       │     if wi.version_id != cell.version_id {
       │         wi.store = Store::new(&hot_engine, host_state.clone());
       │         let instance = cell.instance_pre.instantiate(&mut wi.store)?;
       │         wi.typed = Some(instance.get_typed_func(&mut wi.store, "cc_lb_filter")?);
       │         wi.version_id = cell.version_id;
       │     }
        │     // Direct synchronous call. No blocking pool.
        │     wi.typed.unwrap().call(&mut wi.store, (in_ptr, in_len))
       │ })
       └─ rkyv::access::<ArchivedFilterResponse>(&out_bytes)
```

Key points:

- A "worker" is an OS thread inside the Tokio multi-thread runtime's **core worker pool**. The blocking pool used by `spawn_blocking` is separate, and this design never uses it.
- Worker count = `tokio::runtime::Builder::worker_threads(N)` configuration. The default is `std::thread::available_parallelism()` (typically CPU core count, respecting cpuset/quota in containers).
- The thread_local cache holds an instance per `plugin slot × worker` — no mutex, natural parallelism across workers.
- When the same worker re-invokes the same version, the store/typed_func are reused — only the `TypedFunc` trampoline entry cost is paid.
- On version change, `InstancePre::instantiate(&mut new_store)` — `PoolingAllocator` + `Config::memory_init_cow(true)` make sub-ms feasible per the upstream guidance, but the actual numbers in cc-lb's environment will be confirmed by §validation benchmarks.

### Wire format — rkyv zero-copy

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

Host call:

```rust
let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&req)?;
let in_ptr = call_guest_alloc(&mut store, &alloc_fn, bytes.len(), align)?;
mem.write(&mut store, in_ptr as usize, &bytes)?;
let packed = typed_filter.call(&mut store, (in_ptr, bytes.len() as u32))?;
let (out_ptr, out_len) = unpack(packed);
let out_slice = &mem.data(&store)[out_ptr as usize .. out_ptr as usize + out_len as usize];
let archived  = rkyv::access::<ArchivedFilterResponse, _>(out_slice)?;
// ... process the archived view (no re-entrant typed.call while it lives) ...
call_guest_free(&mut store, &free_fn, out_ptr, out_len)?;
```

The guest validates the input via `rkyv::access::<ArchivedFilterRequest, _>(input_bytes)` and then passes the archived view to the user function. The PDK macro seals the `unsafe`.

### Plugin Development Kit (PDK)

What `#[cc_lb_pdk::plugin(filter)]` proc-macro generates:

1. **Compile-time signature assertion** — `const _: () = { fn _assert<F: for<'a> Fn(FilterRequestRef<'a>) -> Result<FilterOutput, FilterError>>(_: F) {} _assert(user_fn); };`. Signature mismatch → build failure.

2. **Guest export function**:
   ```rust
   #[no_mangle]
   pub extern "C" fn cc_lb_filter(in_ptr: u32, in_len: u32) -> u64 {
       let bytes = unsafe { core::slice::from_raw_parts(in_ptr as *const u8, in_len as usize) };
       let archived = match rkyv::access::<ArchivedFilterRequest, rkyv::rancor::Error>(bytes) {
           Ok(a) => a,
           Err(_) => return pack_err(ErrorCode::InvalidRequest),
       };
       let req_ref = FilterRequestRef::from_archived(archived);
       match user_fn(req_ref) {
           Ok(out) => emit_output(out),
           Err(e)  => pack_err_user(e),
       }
   }
   ```

3. **`schema_hash` custom section** — a 32-byte hash of `(wire version + function variant + rkyv archive layout fingerprint)` embedded in the wasm custom section `cc_lb_schema_hash`. Hash algorithm is unresolved (see U-8 below).

4. **`cc_lb_alloc` / `cc_lb_free` guest exports** (not host imports). When the host needs to allocate the input buffer in guest memory, it calls these guest exports. The guest is responsible for returning alignment satisfying the archive root type's maximum alignment.

### rkyv safety invariants

- `access_unchecked` / `from_bytes_unchecked` are forbidden. Always validate via `rkyv::access::<T, rancor::Error>(bytes)` with bytecheck.
- `cc_lb_alloc` must return an alignment satisfying the root type's maximum alignment (e.g. `align_of::<ArchivedFilterRequest>()`).
- The `(out_ptr, out_len)` returned by the guest must also satisfy the same alignment.
- While the host holds an `&Archived*` view obtained via `rkyv::access`, `cc_lb_free` must not be called. Order: access → copy needed values into owned form → `cc_lb_free`.
- A new `typed_func.call` on the same store while an archived view is alive is **forbidden** — linear memory could mutate and invalidate the view. The PDK helper enforces this via borrow lifetimes.

### Hot-swap

New `PluginCell` shape:
```rust
pub struct PluginCell {
    pub version_id:       u64,
    pub instance_pre:     Arc<wasmtime::InstancePre<HostState>>,
    pub schema_hash:      [u8; 32],
    pub memory_max_pages: u32,
}

pub struct PluginSlot {
    pub name:    String,
    pub entry:   RwLock<PluginEntry>,
    pub current: ArcSwap<PluginCell>,  // unchanged
}
```

Swap flow:

```
Reconciler or admin API
  ├─ read wasm bytes from the disk cache (data/plugins/wasm/cache/{sha}.wasm)
  ├─ Engine::precompile_module(&hot_engine, &wasm_bytes) → .cwasm
  ├─ unsafe { Module::deserialize(&hot_engine, &cwasm) }
  ├─ Linker.instantiate_pre(&module) → InstancePre
  ├─ extract the schema_hash custom section and compare with expected → abort on mismatch
  ├─ one self-check call → abort on failure
  ├─ PluginCell { version_id: prev+1, instance_pre: Arc::new(pre), ... }
  ├─ load into StagedSlot
  └─ commit_staged() → slot.current.store(Arc::new(cell)) only when every slot succeeded
```

When a worker's next request observes that `cell.version_id` differs from its `WorkerInstance.version_id`, it calls `InstancePre::instantiate(&mut new_store)` and re-extracts the typed_func. The last `Arc` reference to the old `InstancePre` is dropped naturally. There is no `dlclose`-style race.

### Observability (observe) flush

`PluginCell::Drop` alone cannot flush events buffered in the worker thread_local (`PluginCell` does not own the store or the ring buffer). The correct model:

1. The observability ring buffer is owned by the `WasmtimeObservabilityHook` itself (or a per-worker registry).
2. Record the new `version_id` immediately before `commit_staged()`.
3. A background flush task signals every worker with `worker_drain_for_slot(slot_key, prev_version_id)` (per-worker channel or atomic flag).
4. Each worker drains the prev_version `Store` from its thread_local at the next await point → emit → drop.
5. The old `PluginCell` Arc is allowed to drop only after every drain ack is received.
6. `evict_slot` uses the same procedure but skips the re-instantiation step.

### Host function policy (mechanically enforced at load time)

Hot-path plugins (filter, shape, normalize_error) must have zero host function imports. Enforcement:

1. The hot-path engine's `Linker` is created empty. Zero `func_wrap` calls.
2. `Config::wasm_component_model(false)`, no `WasiCtx` attached, no `wasmtime-wasi` extension.
3. `Module::imports()` is inspected at registration time — a non-empty import set means `RuntimeError::InstantiateFailed`.
4. `Module::exports()` allows only `cc_lb_<hook>` plus `cc_lb_alloc` / `cc_lb_free`. Anything else is warn-logged.
5. The signer/observability engine is separate — modules and `InstancePre` are never shared with the hot-path engine.

Result: host state, network, filesystem, and clock are all blocked. Any attempt is rejected at instantiate.

### Async inside the wasm guest

- Writing `async fn` inside the guest in Rust is allowed. However, that just compiles into a state machine inside the guest; **the call does not yield back to the host**. From the host's perspective, a `cc_lb_<hook>` call is a single synchronous invocation up to return or trap.
- Real async I/O requires (1) host async function imports plus (2) wasmtime async support (fiber stack switching). This is forbidden on the hot path.

### Resource limits

- Per-store `Memory` page limit + `Config::max_wasm_stack`.
- **No wall-clock kill guarantee (intentional)**: time guarantees would require (a) Wasmtime epoch interrupt (known to also trap other plugins when the engine is shared — requires per-plugin engine separation), (b) OS signals (bypass wasm trap mechanics, dangerous), or (c) a separate thread plus `JoinHandle` drop (which re-introduces the very thread hop we removed). None of these are adopted.
- Guest trap → handled by the existing `execute_filter_pipeline` trap branch. The plugin owns this risk (HC-5).

### Engine configuration

- **Hot-path engine** (sync): `Config::async_support(false)`, `Strategy::Cranelift`, `signals_based_traps(true)`, `memory_init_cow(true)`, and configurable allocation strategy. Current implementation defaults to `InstanceAllocationStrategy::OnDemand` with per-store `StoreLimits`; `InstanceAllocationStrategy::Pooling(...)` remains opt-in for deployments that prefer Wasmtime's pool reuse. Shared across all workers, each call owns its store.
- **Signer engine** (async): `async_support(true)` + epoch interrupt. Separated from the hot path to avoid epoch interference.
- **Engine-specific InstancePre**: `Module` and `InstancePre` are bound to the engine that created them. The same wasm bytes must be processed independently through `precompile_module → deserialize → instantiate_pre` for each engine. `WasmtimeRuntime` holds two engines and a separate Linker / InstancePre cache for each.

### PoolingAllocator capacity

```
max_in_flight ≈ Σ_(slot s) ( worker_threads × live_versions(s) )
              + signer engine in-flight
              + transient residue (old versions a worker has not yet invalidated after commit)
```

`PoolingAllocationConfig::total_memories`, `total_core_instances`, and `total_stacks` (async only) must be set sufficiently larger than the above when pooling is enabled. The on-demand default instead gates fresh stores through the same configured core-instance budget. Current observability exposes pool utilization gauges plus `cc_lb_plugin_pool_saturation_total`; the older `cc_lb_plugin_pool_saturation_ratio`/admin warn-log wording is superseded by those metrics and runtime backpressure.

When the cap is reached:
- `stage_slot` instantiate failure → entire `commit_staged` aborts → existing cells are unaffected. The admin API returns `RuntimeError::InstantiateFailed { reason: "pool saturated, increase total_memories or evict unused slots" }`.
- Hot-path thread_local re-instantiation failure → only that request takes `FilterError::Runtime` and falls through the `execute_filter_pipeline` trap branch. Other requests are unaffected.

### Operational invariants (review consensus)

Multi-round review of this RFC (Pro/Con debate) reached agreement that the following four items must be made explicit before merge. The wording below is the converged proposal after both sides made concessions.

**trap cleanup**

If a trap occurs at any of alloc / filter / free, the `Store` is discarded and removed from the worker cache. The next call on the same worker builds a fresh instance. No explicit circuit breaker is introduced — the extra latency of drop+reinstantiate is itself a natural trap signal, and trap metrics give operator visibility. The §"Resource limits" section and the §execution-model pseudocode above must be amended to reflect this invariant during implementation.

**observe drain**

Observability buffers are best-effort in the MVP. After a version turnover, retired worker observe buffers are owned by the worker registry, and two mechanisms operate independently — ① opportunistic drain at the slot's next call, and ② a bounded retention sweep based on age or byte size. Slots that receive no traffic are still reclaimed by the sweep, so memory cannot leak forever. The "wait for all worker acks" model in the §observability flush above is discarded (Tokio multi-thread tasks are not pinned to workers, so the ack-may-never-arrive scenario is a real deadlock). The central `DrainRegistry` structure is not adopted; the worker registry + sweep combination is the simpler design.

**worker monopolization**

A single call holds its Tokio core worker for the entire call duration (see §Drawbacks). The RFC commits to specifying payload size limits plus worker occupancy/trap metrics (`cc_lb_plugin_call_duration_seconds`, `cc_lb_plugin_trap_total`). Instead of real-time preemption, mitigation goes through metrics plus operator-driven plugin disable/rollback.

**transactional WorkerInstance swap**

A new `WorkerInstance` is fully constructed in local variables — new `Store`, `Instance`, memory/export validation, and all `TypedFunc` handles (`cc_lb_<hook>`, `cc_lb_alloc`, `cc_lb_free`). Only after every lookup succeeds is it atomically published into the worker cache. `TypedFunc` handles must never be reused across stores (wasmtime panics when it detects a cross-store call). If any step fails, the previous worker is preserved or the cache entry is evicted. A mixed old/new state is never published. The §execution-model pseudocode above must be amended to reflect this invariant during implementation.

**Items deferred by consensus as over-engineering**

The same review process deferred the following as measured follow-ups:

- Circuit breaker (automatic slot disable + admin alert)
- Central `DrainRegistry` structure + per-version `DrainEpoch`
- active_slot cap, per-slot live_version cap, hot-swap rate limit
- Full Empty/Ready/Invalidated/Draining state machine
- Exhaustive Wasm feature deny-list (explicit policy for Memory64, threads, shared memory, GC, exceptions, function refs, tail call, SIMD)
- `.cwasm` HMAC + Engine config hash integrity binding
- Mandatory `panic="abort"` PDK build-script enforcement
- Removing caller-supplied SHA trust + recomputing on every boot

Under cc-lb's pre-launch state and first-party trusted plugin assumption, baking these in without real measurement data is judged over-engineering. After production measurement they may be reconsidered in a separate RFC.

### Preserved cc-lb primitives

- Host-facing trait shapes — `FilterPlugin`, `UpstreamDialect`, `ObservabilityHook`, `Signer`, and `SignerFactory`. These boundaries were subsequently split across `cc-lb-routing`, `cc-lb-upstream`, and `cc-lb-observability`.
- `cc-lb-engine/src/dynamic_view.rs::DynamicViewHolder = ArcSwap<DynamicView>`.
- `cc-lb-engine/src/api_keys/principal_view.rs::PrincipalSpecCached` and its `resolved_pipeline / resolved_dialect / resolved_hooks`.
- Per-principal staging in `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`.
- DB polling and revision-hash rebuild in `cc-lb-server/src/reconcile.rs`.
- `ArcSwap` config swap in `cc-lb-server/src/reload.rs::ConfigWatcher`.
- The `data/plugins/wasm/cache/{sha256}.wasm` disk cache.
- The **semantic intent of "plugin/host schema and version agreement"** previously handled by `crates/cc-lb-runtime-protocol/src/handshake.rs` — only the intent is inherited. The current implementation actually executes the JSON `cc_lb_handshake` export, whereas the new ABI inspects the wasm custom section `cc_lb_schema_hash` for a fully static comparison — a completely different mechanism. The code is discarded; only the intent is preserved.

### Discarded code

- `crates/cc-lb-runtime-extism/` in full.
- `crates/cc-lb-runtime-protocol/` (JSON envelope dispatch).
- `crates/cc-lb-plugin-wire/v1, v2, v3` JSON definitions and base64 fields.
- The Extism-PDK-dependent parts of `crates/cc-lb-pdk`.
- Bundled plugin build artifacts — the source logic is kept, rebuilt with the new PDK:
  - `plugins/router/cache-aware`
  - `plugins/shape/subscription-launderer` (moved out-of-tree during migration)
  - `plugins/observe/*`
  - In each plugin's `Cargo.toml`, swap the `extism-pdk` dependency for `cc-lb-pdk-wasmtime`, and replace `#[plugin_fn]` with `#[cc_lb_pdk::plugin(<variant>)]`.

`.wasm` modules built with the old Extism PDK will not run on the new runtime as-is (host import signatures, wire envelope, and the missing `schema_hash` all mismatch). Since cc-lb is pre-launch and has no external plugins, this is fine.

### New crates

- **`crates/cc-lb-runtime-wasmtime/`** — wasmtime implementation of `PluginRuntime`.
  ```text
  WasmtimeRuntime { hot_engine: Engine, signer_engine: Engine, slots: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>, host_state: Arc<HostState> }
  PluginSlot      { name, entry: RwLock<PluginEntry>, current: ArcSwap<PluginCell> }
  PluginCell      { version_id, instance_pre, schema_hash, memory_max_pages }
  StagedSlot      { key, entry, slot }
  WasmtimeFilterPlugin   impl FilterPlugin
  WasmtimeDialectPlugin  impl UpstreamDialect
  WasmtimeSignerFactory  impl SignerFactory  (async, separate engine)
  WasmtimeObservabilityHook impl ObservabilityHook
  ```
  Method names and signatures match the existing `ExtismRuntime` so that `dynamic_view_builder.rs` works with almost no modification.

- **`crates/cc-lb-pdk-wasmtime/`** — guest-side PDK helpers + the `#[cc_lb_pdk::plugin(...)]` macro.
- **`crates/cc-lb-pdk-wasmtime-macros/`** — proc-macro split out into its own crate.
- **`crates/cc-lb-plugin-types/`** — types shared between host and guest + rkyv derives + the `*Ref<'_>` view macro.

### Modified cc-lb code

- The host-facing trait module: the trait changes proposed here were superseded by the later split across `cc-lb-routing`, `cc-lb-upstream`, and `cc-lb-observability`.
- `cc-lb-engine/src/lifecycle.rs::execute_filter_pipeline`: call-site identical. Traps handled by the existing branch.
- `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`: only the runtime call goes through `WasmtimeRuntime` (trait identical).
- `cc-lb-server/src/reconcile.rs`, `reload.rs`, `tls.rs`: unchanged.
- `Cargo.toml`: add `wasmtime = "46"` dependency; once the transition is complete, remove `extism = ...`.

## Drawbacks

- **Worker occupancy**: a single plugin call holds its worker thread for the duration of the call. Neighbor task latency can be affected; watch duration/trap metrics and disable or roll back expensive plugins.
- **Memory footprint**: `worker_threads × active_slots × memory_max_pages`. 8 worker × 20 slot × 32 page (2 MiB) = ~320 MiB hard limit. `PoolingAllocator` + CoW makes actual RSS much smaller, but capacity planning is still required.
- **No wall-clock kill guarantee**: see §"Resource limits". A user-approved trade-off, but operational visibility is required.
- **PDK macro implementation burden**: the `*Ref<'_>` view auto-generation, the alignment-aware allocator, and the `schema_hash` custom-section emission must all be written in-house.
- **wasmtime 47 vs Extism 1.30 (wasmtime 43)**: during the transition two wasmtime majors coexist. Cargo can handle it, but build time and image size grow. Cleaned up in Phase 4.

## Rationale and alternatives

Every other candidate violates at least one hard constraint (HC-1 through HC-5).

| Alternative | Thread hop | Type safety | Zero-downtime swap | Sandbox | Rejection reason |
|---|---|---|---|---|---|
| Current Extism + JSON + base64 | per-call OS thread + Tokio runtime + spawn_blocking | runtime only | yes | strong | per-call thread / runtime / spawn_blocking / Mutex / JSON+base64 are all on the hot path. Intrinsically violates HC-1. To be discarded. |
| **Wasmtime core ABI + cc-lb PDK + rkyv** | **0 (per-worker store)** | **3 compile-time layers + load-time hash** | **yes (existing primitives)** | **strong** | **Adopted** |
| Wasmtime Component Model + WIT | possible 0 | both compile-time | yes | strong | Canonical ABI copies list/string on every call. Sync path reports ~3.5x overhead from async machinery. WASI 0.3.x lazy lowering is on the roadmap but unverified in production as of 2026-06. Adoption deferred; future candidate. |
| Native dylib (`libloading` + `abi_stable`) | 0 | strong (load-time) | **impossible** | **none** | `abi_stable` officially declares "without support for unloading". glibc `__cxa_thread_atexit_impl` blocks `dlclose` for TLS-using libraries. Violates HC-2. Sandbox is zero, also violating non-HC security needs. |
| eBPF (`rbpf` JIT) | 0 | none (raw bytes) | yes | strong (verifier) | The raw byte buffer model cannot enforce compile-time typed signatures. Violates HC-3. Plugin author ergonomics also poor. |
| Embedded scripting (Luau via mlua, Rhai, Rune) | 0 | weak (dynamic typed) | yes | varies | All dynamically typed. No compile-time assertion mechanism. Violates HC-3. |
| Incremental Stage 1: async trait default + Extism override | **spawn_blocking 1 hop remains** | runtime only | yes | strong | Violates HC-1 (thread hop is not zero). spawn_blocking hops worker → blocking pool. |
| Incremental Stage 2: per-plugin async semaphore | spawn_blocking 1 hop remains | runtime only | yes | strong | Just moves the mutex to an async queue; the hop itself remains. Violates HC-1. |
| Incremental Stage 3: "measure then decide" (Extism Pool vs Wasmtime) | TBD | runtime only | yes | strong | Incompatible with the user's explicit constraint that "without optimization the product itself is meaningless". |

### Performance number disclaimer

All performance figures cited in this RFC — Wasmtime fast-instantiation's "sub-ms instantiation", `TypedFunc::call` trampoline ~tens to hundreds of ns, rkyv vs JSON+base64 ~30-100x — are **expectations / indicators** based on external sources, not guarantees in cc-lb's actual payload environment. Final adoption is decided by the cc-lb-payload-specific benchmark in §Validation. The default runtime flip happens in Phase 3 based on whether `benches/extism_sse_overhead` plus the `tests/load/baseline.json` thresholds are satisfied.

### Why core ABI instead of the Component Model

WIT / Component Model is the cleanest way to satisfy HC-3 (type safety). However, as of 2026-06:
- The canonical ABI copies list/string every call. cc-lb's hot path involves large body bytes, so copy cost is critical.
- The 2026-standard async machinery reportedly adds ~3.5x overhead even on the sync path. Risk of conflicting with cc-lb's 1ms p99 threshold.
- WASI 0.3.x lazy value lowering may resolve this but lacks production verification.

Core ABI + an in-house PDK delivers the same type safety through a different mechanism (macro + schema_hash) while giving us direct control over zero-copy. If the Component Model matures in the future, a migration would be evaluated in a separate RFC.

## Prior art

- **Wasmtime fast-instantiation docs** — `PoolingAllocationConfig`, `memory_init_cow`, `InstancePre` make sub-ms instantiation feasible.
- **Wasmtime fast-execution docs** — `Strategy::Cranelift`, `signals_based_traps`, `memory_reservation 4GB + guard 4GB` elide bounds checks.
- **rkyv 0.8** — `Archive` derive + `rkyv::access` checked deserialization. bytecheck validates archive structural integrity.
- **abi_stable 0.11** — Rust-to-Rust FFI + load-time layout check. Documents itself as "without support for unloading".
- **glibc `__cxa_thread_atexit_impl`** — the behavior that blocks `dlclose` for TLS-using libraries.
- **Extism 1.30** — `Pool` / `PoolBuilder` provide instance pooling, but the model protects plugin instances with a mutex (different from this RFC's per-worker thread_local model), and the JSON+base64 wire cost remains.
- **Current cc-lb `DynamicViewHolder`** — `ArcSwap<DynamicView>` + per-slot `ArcSwap<PluginCell>` + two-phase stage/commit. Preserved by this RFC.

## Unresolved questions

- **U-1 Worker occupancy thresholds**: conservative alert thresholds are needed separately for filter, shape, and normalize_error. Final values require measurement against cc-lb's actual payloads.
- **U-2 `*Ref<'_>` macro design details**: should `cc-lb-plugin-types` author its own helper derive such as `#[derive(ArchiveRef)]`, or depend on an external macro (e.g. an extension to `rkyv-derive`).
- **U-3 Alignment-aware guest allocator**: how to communicate the archive root type's maximum alignment to the guest allocator. Either export `cc_lb_alloc_aligned(len, align)` or generate per-root-type alloc functions.
- **U-4 `schema_hash` custom section format**: store only the 32-byte BLAKE3 hash, or also include a schema descriptor. The latter aids debugging at the cost of binary size.
- **U-5 Recommended `worker_threads × active_slots`**: an active_slots ceiling that runs stably in 8-core environments. Trade-off between `PoolingAllocationConfig` hard limits and SLO.
- **U-6 `evict_slot` drain timeout**: at what point the background drain task should force-drop if it cannot receive acks from all workers.
- **U-7 wasmtime 47 vs Extism 1.30 (wasmtime 43) coexistence period**: measure the transition's RAM, image size, and build time impact to time Phase 4 correctly.
- **U-8 `schema_hash` algorithm**: **Resolved (Phase 1 W5) — BLAKE3.** Single dependency already in the wasm-runtime chain (planned for content-hashing of cached `.cwasm` artifacts), `const fn`-friendly at proc-macro time, and the 32-byte digest fits the custom-section budget. Lives in the `cc_lb.schema.v1` custom section, computed identically by the macros crate and `cc-lb-runtime-wasmtime::inspect`.

## Future possibilities

- **F-1 Polyglot plugins**: swap the same PDK pattern over a FlatBuffers wire (`#[cc_lb_pdk::plugin(filter, wire = "v4_flatbuffers")]`). Enables Go / AssemblyScript / C++ plugins. Requires additional non-Rust guest SDK work.
- **F-2 Revisit the Component Model**: once WASI 0.3.x lazy lowering stabilizes in production and the sync-path overhead is resolved by measurement, migrate incrementally via a separate RFC.
- **F-3 Direct stream/SSE handling on the hot path**: today SSE relay is handled host-side. Once Component Model `stream<u8>` plus stream splicing mature, plugins could perform stream filtering directly.
- **F-4 Plugin instance hot-pinning**: an admin hint that pre-warms frequently called slots on every worker.
- **F-5 Per-tenant plugin SLO budgets**: dynamically adjust plugin admission or disablement policy based on tenant SLO or pricing policy.

## Migration

Difficulty estimate per phase (1 = trivial, 10 = full rewrite).

1. **Phase 0 — new crate skeletons** (2/10): create 4 crates, `PluginRuntime` trait skeleton, and the workspace `runtime-wasmtime` feature gate. Only the build needs to pass. **Status: complete** (commit `856106b`).
2. **Phase 1 — integrate the filter hook** (6/10): PDK filter variant + rkyv types + view-mode handlers + alignment-aware alloc + hot-path engine + per-worker thread_local cache + `schema_hash` validation + load-time imports inspect + `slot_key` on the `FilterPlugin` trait. Rebuild the bundled `cache-aware` plugin. Author new conformance tests. **Status: complete** — W1 rkyv wire types (`213cb6c`), W2 Engine/HostState (`545e604`), W3 per-worker cache + ABI wrapper + transactional swap (`92de8a3`), W4 PDK macros + dispatch (`55dfd53`), W5 wasmparser inspect + BLAKE3 schema gate (`740d7cb`), W6 `WasmtimeFilterPlugin` adapter (`1b8f5fd`), W7 `cache-aware-wasmtime` plugin + e2e tests (`8162684`), W8 host-side conformance scenarios (`3aaf9c5`). The view-mode variant is exposed via `#[handler(name = "filter", view)]` + `__private::run_filter_view`; the handler takes `&ArchivedFilterRequest` and skips deserialise. The `slot_key` trait method has a `Self::plugin_name` default so existing impls compile unchanged; Extism and wasmtime override to return the principal × plugin pair they were instantiated with.
3. **Phase 2 — shape, observe, signer** (5/10): shape/normalize_error reuse the filter pattern. Observe introduces the background drain task + per-worker ring buffer for the first time. Signer uses the async engine + epoch interrupt + a separate Linker / InstancePre.
4. **Phase 3 — benchmarks + flip** (3/10): run `benches/extism_sse_overhead` against the new runtime variant. Once the `tests/load/baseline.json` thresholds are met (non-streaming p50 overhead < 5ms, p99 < 20ms, streaming p50 per event < 2ms), flip the default runtime.
5. **Phase 4 — remove Extism** (2/10): delete `cc-lb-runtime-extism`, `cc-lb-runtime-protocol`, `cc-lb-plugin-wire/v1,v2,v3`, the Extism part of `cc-lb-pdk`, and all JSON/base64 paths. Drop the `extism` and `extism-pdk` dependencies. Update `docs/plugin-author-guide.md` and `docs/runtime-management.md`.

Comparison (preserving the m0046 estimate):
- Removing only per-call thread/runtime from the existing Extism path: 3-4/10. Mutex/JSON/base64 cost remains → incremental approach rejected.
- This vNext total: 7/10 — raw Wasmtime + in-house ABI vNext is the heaviest of all options.
- Keep the Extism ABI but swap to Wasmtime (build a custom Extism compatibility layer): 8-9/10. Not chosen.

## Validation

- **Conformance**: migrate the existing `crates/cc-lb-plugin-conformance` scenarios to the new PDK + runtime. Filter happy path, trap, invalid archive, `schema_hash` mismatch, and in-flight request consistency during hot-swap.
- **Benchmark**: run `benches/extism_sse_overhead` as-is plus the new runtime variant. Must hit the `tests/load/baseline.json` thresholds.
- **Property tests**: add a wasmtime variant to `tests/property/`.
- **Loom**: add race cases for the new `PluginCell` swap to `tests/loom`.
- **Load**: with `tests/load` at 1k RPS, measure worker thread occupancy, memory footprint, and p99 latency.
- **Load (observe drain sweep)**: prove that across repeated version commit cycles, retired observe buffers do not grow unbounded and are reclaimed by the sweep. Confirm that retired buffers in slots with no traffic are also dropped after the retention timeout. Validates the observe drain invariant from the review consensus.
- **Load (worker tail latency)**: while one intentionally expensive first-party plugin is repeatedly called on the same worker, measure p99/p999 of unrelated requests scheduled on the same worker. Must pass a threshold (e.g. unrelated request p99 increase < 2x) before the Phase 3 default flip is allowed. Validates the worker monopolization invariant from the review consensus.

## Open risks

- **R1 Worker occupancy**: monitor the SLO metric `cc_lb_plugin_call_duration_seconds{plugin,hook,quantile}` and disable or roll back expensive plugins.
- **R2 Memory footprint + thread-local eviction**: after `evict_slot` or a version replacement, the worker thread-local's old Store risks leaking. Policy: (a) on every call, compare version and overwrite stale entries; (b) eviction is signaled by the background drain task to every worker; (c) periodic LRU sweep GCs entries idle for N minutes.
- **R2b Principal removal**: when a principal is removed, all of its SlotKeys disappear in the next reconciler rebuild. Worker thread-local entries are cleaned up via (a)/(b)/(c) above.
- **R3 Runtime store cap**: the capacity formula above, the runtime-wide on-demand store budget, and the Prometheus utilization/saturation metrics.
- **R4 Schema evolution**: adding a new field changes the `schema_hash` → all plugins must be rebuilt. Currently fine (no external plugins). If future wire version negotiation is required, declare a `wire_version` in the macro plus host-side multi-version dispatch.
- **R5 wasmtime 47 ↔ Extism 1.30 (wasmtime 43) coexistence**: binary size and build time grow during the transition. Cleaned up in Phase 4.
- **R6 PDK view type coherence**: if the mapping between `*Ref<'_>` and `Archived*` breaks, you get incorrect memory access. They are co-generated by the same macro to keep a single source of truth. Cross-cargo-version verification is delegated to `schema_hash`.
- **R7 Signer engine separation**: running an async engine and a sync engine in the same process. Resource accounting and memory-leak monitoring must be tracked separately.

## References

- Decision audit: internal design notes are not included in the public tree.
- ADR: [docs/adr/0001-plugin-runtime-vnext.md](../adr/0001-plugin-runtime-vnext.md)
- External:
  - Wasmtime 47 Tuning for Fast Instantiation, Fast Execution, Pre-Compiling Wasm.
  - rkyv 0.8 `Archive`, `access`, `bytecheck`.
  - `abi_stable` 0.11 — "without support for unloading".
  - glibc `__cxa_thread_atexit_impl` and the `dlclose`-blocking behavior.
  - WIT / Component Model — `bytecodealliance/component-docs`.
- Internal:
  - `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` — current dispatch cost.
  - `crates/cc-lb-runtime-extism/src/lib.rs` — current ArcSwap + stage/commit.
  - `crates/cc-lb-engine/src/dynamic_view.rs` — DynamicViewHolder.
  - `crates/cc-lb-engine/src/api_keys/principal_view.rs` — PrincipalSpecCached.
  - `crates/cc-lb-server/src/dynamic_view_builder.rs` — build_principal_chains.
  - `benches/extism_sse_overhead/src/main.rs` — 1ms p99 threshold.
  - `tests/load/baseline.json` — performance baseline.
