# ADR-0001: Plugin runtime vNext — replace Extism with raw Wasmtime + cc-lb PDK

- Status: Proposed
- Date: 2026-06-27
- Supersedes: current Extism-based plugin runtime (`crates/cc-lb-runtime-extism/`)
- RFC: [docs/rfc/0001-plugin-runtime-vnext.md](../rfc/0001-plugin-runtime-vnext.md)

## Context

Plugin calls in cc-lb sit on the hot path of every request. The current Extism wrapper accumulates the following per request:

- `crates/cc-lb-runtime-extism/src/plugin_wrap.rs::dispatch_*_sync` **spawns a new OS thread and creates a new current-thread Tokio runtime**.
- Inside that thread, it re-enters the **blocking pool via `tokio::task::spawn_blocking`**.
- `PluginCell { plugin: Mutex<Plugin> }` serializes concurrent calls to the same slot.
- The wire serializes/deserializes via **JSON envelope + body base64**.

Per-request OS thread spawning, runtime construction, blocking pool hop, mutex waits, and JSON+base64 encoding accumulate so that wrapper overhead dominates the actual wasm execution cost. `benches/extism_sse_overhead` already specifies a 1ms p99 threshold for escalating to Wasmtime.

Since cc-lb is not yet shipped, plugin BC can be discarded and the Extism/wasm/plugin build format can all be replaced freely. However, **zero-downtime dynamic plugin swap** and **a PDK that gives plugin authors compile-time signature type safety** must be preserved or introduced.

## Decision

Rewrite the plugin runtime from scratch on top of the raw `wasmtime` core ABI. Concretely:

1. Plugin execution is a **synchronous `wasmtime::TypedFunc::call`** invocation inside a Tokio core worker thread. No `spawn_blocking`, no additional OS threads, no additional runtime creation. **Zero thread hops**.
2. Plugin instances live in a **per-worker `thread_local!` cache**. `Arc<wasmtime::InstancePre>` sits inside `ArcSwap<PluginCell>`; on `version_id` change each worker re-instantiates into its own `Store`. No mutex.
3. The wire format is **rkyv zero-copy archives**. JSON/base64 are completely discarded. Body/header are raw bytes.
4. Plugin authors write ordinary Rust functions decorated with a proc-macro such as **`#[cc_lb_pdk::plugin(filter)]`**. The macro generates a compile-time signature assertion, a safe `*Ref<'_>` view over archived bytes, and a `schema_hash` custom section. The host shares the same types crate and enforces `TypedFunc<Params, Results>`. **Three compile-time layers + load-time hash verification**.
5. Hot-path plugin execution does **not** use Wasmtime instruction metering. Wall-clock kill is not guaranteed; request-level timeout/backpressure and plugin rollback/disable are the operational controls.
6. Hot-path plugins have **zero host function imports**. The Linker is created empty, and registration is rejected if the module's import set is non-empty. Host state / network / filesystem / clock access is all blocked.
7. The hot-swap primitives (`DynamicViewHolder = ArcSwap<DynamicView>`, per-slot `ArcSwap<PluginCell>`, two-phase `stage_slot`/`commit_staged`, per-principal `SlotKey`, the SHA-256 wasm cache) are preserved as-is. Only the internals of `PluginCell` change.
8. Signer is not on the hot path and is therefore isolated to a **separate async engine** (`Config::async_support(true)` + epoch interrupt). The observability (observe) hook itself uses the same hot-path ABI as the other sync hooks, but plugins only push events into an in-memory ring buffer; actual emission is handled by a **background drain task** that drains worker-by-worker. The hot-path engine and the signer engine never share modules or `InstancePre`.
9. Bundled plugins are rebuilt against the new PDK. Extism, `cc-lb-runtime-protocol`, `cc-lb-plugin-wire/v1,v2,v3`, and all JSON/base64 paths are deleted.

## Consequences

### Positive
- Zero hot-path thread hops. No blocking pool usage. Zero mutex contention.
- JSON+base64 encoding cost eliminated.
- Plugin authors write plain typed Rust functions. Signature mismatches fail at build or registration time.
- Hot-swap semantics are preserved as-is, and ArcSwap's lock-free replacement keeps in-flight requests consistent.
- Host call blocking is enforced at load time rather than by convention.

### Negative
- A single plugin call holds its worker thread for the duration of the call. SLO impact must be monitored and controlled by timeout/backpressure plus plugin rollback/disable.
- Per-instance memory footprint of `worker × slot × memory_max_pages`. Mitigated by `PoolingAllocator` + CoW.
- Worker thread-local entries require eviction/drain/LRU policies. Old `Store`s are not auto-invalidated on version replacement or slot eviction, so a background drain task plus periodic sweep are required (see RFC R2/R2b).
- Wall-clock kill is not guaranteed.
- The PDK must build the rkyv `*Ref<'_>` view-type macro and an alignment-aware allocator from scratch.
- Incompatible with Extism-built plugins (rebuild required). cc-lb has no external plugins because it has not yet shipped.

## Operational invariants (review consensus)

Multi-round review of this ADR (Pro/Con debate and consensus building) reached agreement that the following four operational invariants must be made explicit before the RFC is merged. See [RFC §Operational invariants](../rfc/0001-plugin-runtime-vnext.md#operational-invariants-review-consensus) for the detailed definitions.

- **trap cleanup**: A trap at any guest-call stage causes the `Store` to be discarded and re-instantiated. No explicit circuit breaker is introduced (the drop+reinstantiate latency is a natural signal).
- **observe drain**: Observability buffers are best-effort in the MVP. After a version turnover, retired buffers are owned by the worker registry; two mechanisms operate independently — opportunistic drain on next slot access, and a bounded retention sweep based on age or byte size. Slots that receive no traffic are still reclaimed by the sweep.
- **worker monopolization**: A single plugin call holds its Tokio core worker for the entire call duration. Instead of preemption, mitigation goes through metrics plus operator-initiated plugin disable/rollback.
- **transactional WorkerInstance swap**: A new `WorkerInstance` is fully built in local variables and only then atomically published to the cache. `TypedFunc` handles must never be reused across stores. On failure the previous worker remains active or the cache entry is evicted.

Items the review consensus deferred as over-engineering (under cc-lb's pre-launch state and first-party trusted plugin assumption): circuit breaker (auto slot disable), a central `DrainRegistry`, active_slot / live_version caps, hot-swap rate limit, a full state machine, an exhaustive Wasm feature deny-list, `.cwasm` HMAC integrity, mandatory `panic="abort"` PDK enforcement, and removal of caller-supplied SHA trust. All are measured follow-ups — baking them in without real production measurement data is judged over-engineering.

## Alternatives considered

| Alternative | Rejection reason |
|---|---|
| Wasmtime Component Model + WIT | The canonical ABI copies list/string on every call, and async machinery is reported to add ~3.5x overhead on the sync path. WASI 0.3.x lazy lowering could resolve this but is not production-verified as of 2026-06. |
| Native dylib (`libloading` + `abi_stable`) | `abi_stable` documents itself as "without support for unloading". glibc `__cxa_thread_atexit_impl` blocks `dlclose` for libraries using thread-locals. Incompatible with zero-downtime dynamic replacement. Sandboxing is also zero. |
| eBPF (`rbpf` JIT) | The raw byte buffer model cannot enforce compile-time typed signatures. Very poor plugin author ergonomics. |
| Embedded scripting (Luau via mlua, Rhai, Rune) | All dynamically typed. No mechanism for compile-time signature assertions. |
| Incremental approach: async trait + per-plugin semaphore + measure-then-decide (3 stages) | Stage 1 still leaves a `spawn_blocking` one-hop, Stage 2 merely moves the mutex to an async queue, and Stage 3 "measure then decide" is incompatible with the user's explicit constraint that "without optimization the product itself is meaningless". |

## Out of scope

- Preserving external plugin BC (cc-lb is pre-launch, so this is discarded).
- Strict wall-clock timeout guarantees.
- Host function I/O from hot-path plugins (split off into a separate background flush task).
- Immediate polyglot plugin support (Rust first, with a FlatBuffers swap as future extension — see RFC §future).

## Audit trail

`.omo/ultraresearch/20260627-plugin-hotpath-vnext/SYNTHESIS.md`

## References

- Detailed design: [docs/rfc/0001-plugin-runtime-vnext.md](../rfc/0001-plugin-runtime-vnext.md)
- Current implementation: `crates/cc-lb-runtime-extism/`
- Preserved primitives: `crates/cc-lb-core/src/dynamic_view.rs`, `crates/cc-lb-core/src/api_keys/principal_view.rs`, `crates/cc-lb-server/src/dynamic_view_builder.rs`, `crates/cc-lb-server/src/reconcile.rs`.
