# ADR-0008: Workspace crate reassembly — delete `cc-lb-plugin-api` & `cc-lb-contract`, extract a stable `cc-lb-domain`

- Status: Proposed
- Date: 2026-07-10
- Supersedes: the `cc-lb-plugin-api` published authoring surface and the `cc-lb-contract` shared-vocabulary crate
- Implementation plan: [.omo/plans/crate-reassembly.md](../../.omo/plans/crate-reassembly.md)
- Related: [ADR-0001 plugin runtime vNext](0001-plugin-runtime-vnext.md), [ADR-0002 JSON library strategy](0002-json-library-strategy.md)

## Context

The workspace has 25 `crates/cc-lb-*` core crates (36 workspace members incl. plugins/tests, `fuzz` excluded). Four structural problems accumulated:

1. **`cc-lb-plugin-api` is a published "authoring" crate that plugins do not use.** Guests depend on `cc-lb-pdk-wasmtime` → `cc-lb-plugin-wire`; none import `cc_lb_plugin_api`. Of its 53 exported symbols only 5 cross the wasm wire (`ObserveEvent`, `PerCandidateReason`, `Principal`, `Upstream`, `UpstreamCandidate`); the other 48 are host internals. 13 host crates depend on it, so it is a *host-internal* crate wearing a *published-ABI* hat.

2. **Published semver churn driven by volatile internals.** `FilterOutput` and the routing decision types embed the routing-trace value tree (`SubscriptionPreferenceTrace`, `CacheAffinityTrace`, …). A routing-algorithm tweak therefore forced published-crate major bumps (v5→v6→v7 within three days), even though no plugin author consumed the changed types.

3. **`cc-lb-contract` is a catch-all.** It (a) depends on the soon-deleted `cc-lb-plugin-api` (`crates/cc-lb-contract/src/lib.rs:66`), (b) embeds the runtime event-bus transport (Tokio `broadcast`/`mpsc` in `event_bus.rs`) despite its own doc claiming "no runtime dependency", and (c) mixes a **volatile** event vocabulary (`LifecycleEvent`) with the **stable** persisted `RequestEvent` read-model. Those two depend on routing-trace in *opposite* directions, so they cannot share one crate under a stable←unstable rule.

4. **`cc-lb-engine` is a god-crate that leaks into the control/admin plane.** `cc-lb-admin` and `cc-lb-scheduler` depend on `cc-lb-engine` directly, pulling routing/filter/resilience/relay/`pg_notify` execution they never run, purely to reach a few values and capabilities. There is no enforced stable←unstable dependency direction anywhere in the graph.

The goal: remove `cc-lb-plugin-api`, dismantle the catch-all, decompose enough of `cc-lb-engine` to unhook admin/scheduler, and shape the whole graph so **stable crates never depend on volatile ones** — without changing the wasm wire ABI, the PDK surface, routing behavior, or the storage schema.

## Decision

### 1. Delete two crates, introduce six

Delete `cc-lb-plugin-api` and `cc-lb-contract`. Add six **unpublished** (`publish = false`) crates. Net core count 25 − 2 + 6 = **29** (40 workspace members, `fuzz` excluded).

| New crate | Cadence | Owns | Depends on (cc-lb) |
| --- | --- | --- | --- |
| `cc-lb-domain` | **stable leaf** | Every **pure value type**: nouns/identity (`Principal`, `Upstream`, `UpstreamCandidate`, `UpstreamKind`, `CredentialStrategy`, `GLOBAL_PRINCIPAL`, 4× `BUILTIN_*`, `ANTHROPIC_IDENTITY_HEADERS`), quota/rate values, cache values (`TtlClass` canonical, `CacheScore`, `CachePricingSummary`, breakpoints), the **entire routing-trace tree** (`RoutingTrace`/`StageDecision`/`TerminalDecision`/`SubscriptionPreferenceTrace`/`CandidateUrgency`/`WrhKeySource`/`CacheAffinityTrace`), errors (`InternalError`/`Kind`/`Stage`), `PlanInfo` (moved from control), `ReplicaIdentity` | *(none — dependency-free)* |
| `cc-lb-upstream` | slow SPI | `UpstreamDialect`, `Signer`(+factories), transform hooks, `ShapedRequest`/builder, `SignedRequest`, `RetryDecision` (embeds `Arc<dyn Signer>`), new `DialectShapeContext` | domain |
| `cc-lb-routing` | fast SPI | `FilterPlugin`, `RouterPlugin`, `RouteDecision` (embeds `Arc<dyn UpstreamDialect>`), `PerCandidateReason`, new proxy-only `RoutingContext` view | domain, upstream |
| `cc-lb-quota` | logic | `rate_limit_headers` (`UnifiedQuotaObservation`), `plan_capacity` (`TierKey`, ratios), quota-sample | domain, storage-api |
| `cc-lb-request-log` | **stable** persisted | `RequestEvent` (embeds `domain::RoutingTrace` + `Vec<InternalError>` **directly**), `RequestEventPartial`/`Update`, cache-state records, `HeaderSnapshot`, `CostBreakdown` | domain |
| `cc-lb-lifecycle` | volatile vocab | `LifecycleEvent` (14 variants), `EventId`, stage payloads, `PromptCacheObservationWire`, publisher sink | domain, request-log |

Contract's remaining members are redistributed to their real owners: metrics hook → `cc-lb-observability`; `AuditSink`/`AuditEntry` + `Limit`/`LimitKind`/`KeyStatus` + `PluginSlotKind` → `cc-lb-storage-api`; the Tokio event-bus wiring + `ReplicaIdentityProvider` → `cc-lb-control`.

### 2. Routing-trace lives in the stable leaf, so **no mirror machinery exists**

The routing-trace tree is entirely pure value types (verified: `RoutingTrace` at `crates/cc-lb-plugin-api/src/types.rs:1239` holds only `stages` + `terminal_decision`; `UpstreamCandidate` at :413 aggregates only pure values). Placing the whole tree in `cc-lb-domain` lets the persisted `RequestEvent` embed `domain::RoutingTrace` and `Vec<InternalError>` **directly**. There are **no** `PersistedRoutingTrace`/`PersistedInternalError` mirror types and **no** rich→mirror converter. The published-semver churn (Context §2) is already solved by deleting the *published* `cc-lb-plugin-api`; the residual cost is that a trace-field addition recompiles domain's stable consumers — build coupling, not a semver break.

### 3. Publication stays exactly five crates

Keep `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-pdk-wasmtime`, `cc-lb-runtime-wasmtime`, `cc-lb-plugin-conformance` published. No new published core is created. To keep this legal, `cc-lb-runtime-wasmtime` (published) must depend only on `cc-lb-plugin-wire` among the plugin crates: its **host-side adapters move to `cc-lb-server`** (unpublished), which implements the host traits by wrapping runtime wire-level dispatch. `cc-lb-plugin-conformance` drops its `cc-lb-plugin-api` dep and takes `SlotKey` from `cc-lb-runtime-wasmtime::RuntimeSlotKey`.

### 4. Split the three overloaded concepts

- **`RequestContext`** monolith (dozens of struct-literal construction sites; the exact set is enumerated at implementation) → capability-specific views: proxy-only `RoutingContext` (routing) and a minimal `DialectShapeContext` (upstream) carrying only the fields `UpstreamDialect::shape` reads, so upstream never depends on routing.
- **Slot types** (never merged): `SlotKey` → `cc-lb-runtime-wasmtime::RuntimeSlotKey` (and the unused `FilterPlugin::slot_key` is removed); serialized enum `PluginSlot` → `cc-lb-storage-api::plugin_registry::PluginSlotKind` (serde strings byte-preserved); runtime struct `PluginSlot` → `LoadedPluginSlot`.

### 5. Decompose `cc-lb-engine` only enough to unhook admin & scheduler

`cc-lb-engine` **keeps** its execution logic (routing, filters, resilience, relay, `pg_notify`). Only:

- `rate_limit_headers` + `plan_capacity` move out to `cc-lb-quota`.
- `cc-lb-admin` and `cc-lb-scheduler` **drop their direct `cc-lb-engine` dependency.** admin reaches engine-owned capabilities (route preview, warmup, retained-partial) through **narrow admin-defined port traits implemented by `cc-lb-server`** over engine; it takes values from `cc-lb-domain`/`cc-lb-quota`/`cc-lb-request-log`, and `clock` directly. The dead resilience-status feature (`build_upstream_health` + `Breaker/Bulkhead/Drain` registries) is **deleted, not ported**. scheduler re-points the `anthropic_compat`/`anthropic_metadata` imports — which are only `pub use cc_lb_control::…` re-exports in engine — straight to `cc-lb-control`, and keeps `usage_pruner` locally. The per-request event-runtime workers become `cc-lb-server::event_runtime` modules, not a crate.

### 6. Enforce a stable←unstable acyclic graph

```
leaves: clock · aead · config · plugin-wire[pub] · oauth-protocol · domain
   <  storage-api · request-log · quota · pricing · observability
   <  upstream  <  routing
   <  lifecycle · control
   <  pdk-macros · pdk[pub] · runtime[pub]→wire-only · conformance[pub] · engine · dialect-anthropic · signers
   <  server · admin · scheduler
```

Every edge points toward equal-or-more-stable. The five published crates depend only on published/wire crates.

## Consequences

### Positive
- `cc-lb-plugin-api` disappears as a published surface; routing-algorithm changes can no longer trigger a published semver bump.
- `cc-lb-domain` gives every layer one dependency-free vocabulary; stable persistence (`request-log`, `storage-api`) never points at volatile SPI or engine.
- `cc-lb-admin`/`cc-lb-scheduler` shed the heavy `cc-lb-engine` edge; admin holds only pure values, its read model, quota, storage, and the control-plane view.
- No mirror/converter machinery: `RequestEvent` embeds the real trace type, so there is exactly one definition to maintain.
- The wasm wire ABI, PDK surface, routing behavior, `request_events_v1` JSON, and `PluginSlotKind` serde strings are all unchanged (byte-identical fixtures pin them).

### Negative
- `cc-lb-domain` is a large, widely-depended-on crate; adding a routing-trace field recompiles its stable consumers (build coupling, not a semver break).
- admin's engine-owned capabilities are reached through port indirection (admin trait + server impl) rather than a direct call — one more hop to trace when reading the code.
- The migration is a single large PR touching most of the workspace (20 dependency-ordered todos); it must land atomically to keep the workspace compiling.

## Alternatives considered and rejected

- **A new published "core" crate.** Rejected: no plugin author needs it; would re-introduce the published-churn coupling that motivated the change.
- **One catch-all host-SPI crate** (single `proxy-spi`) — or, conversely, **formal per-layer fragmentation.** Rejected both: chose purpose/cadence crates (`upstream` slow vs `routing` fast) justified by real consumers, not layer aesthetics.
- **A dedicated `cache-types` crate** for `TtlClass`/prompt-cache vocab. Rejected: they are pure values that belong in `cc-lb-domain`; a crate-per-enum is over-fragmentation.
- **Keep routing-trace volatile in `cc-lb-routing` + a `PersistedRoutingTrace` mirror + server converter.** Rejected by decision §2: ~10 duplicate mirror types kept in perpetual sync is a bigger maintenance/`churn` hazard than the build-coupling it avoids; the published-churn pain is already solved by deleting `cc-lb-plugin-api`.
- **An `cc-lb-event-runtime` crate.** Rejected: its only production consumer is `cc-lb-server`, so a crate is a formal split; it becomes `cc-lb-server::event_runtime` modules.
- **Pour warmup/quota/realtime/resilience into `cc-lb-control`.** Rejected: that just renames the god-crate.
- **Fold the 1-consumer firewall crates** (`dialect-anthropic`, `signer-anthropic-key`, `oauth-protocol`). Rejected: they are legitimate isolation boundaries.

## Verification / enforcement

The implementation plan encodes machine-checkable gates:

- **Acyclicity + direction:** `cargo metadata`-derived edges asserted against `.omo/evidence/cadence-labels.json`; no STABLE→UNSTABLE edge.
- **Forbidden edges:** forward-tree helpers `no_dep`/`has_dep` (e.g. `no_dep admin engine` must pass post-migration; it fails on origin/master today, empirically confirmed).
- **Publish legality:** `cargo package` for all five published crates; deleted packages asserted absent via `cargo metadata` node-absence.
- **Byte-compat:** origin/master golden fixtures for `request_events_v1` and `PluginSlotKind`/`KeyStatus` serde strings; a normalized proxy-parity harness diffs the semantic subset across three scenarios (happy `POST /v1/messages`, 401 retry/refresh, filter/limit).
- **No behavior change:** `git diff` gate over migrations, `cc-lb-plugin-wire`, and the PDK crates.

## Post-rebase validation (origin/master `0d75efc0`, 2026-07-10)

This decision was re-validated after rebasing from base `cdf77383` onto `0d75efc0` (8 intervening commits: durable prompt-cache keep-alive, use-it-or-lose-it quota urgency, Fable 5 model, admin live-logs overhaul, scheduler Apalis worker fix). **The decision is unaffected:**

- The workspace **crate set is unchanged** (no manifest added, removed, or renamed).
- `cc-lb-plugin-api`/`cc-lb-contract` gained only **additive fields** on already-classified types (`quota_urgency_*`, `thread_id`, prompt-cache-breakpoint fields) — no new type or trait needs a home, so the Symbol→home partition stands.
- The largest addition, the **`cache_keepalive` subsystem**, lands entirely in existing homes: `cc-lb-engine/src/cache_keepalive/**` + `downstream_stream_drop_guard.rs` are engine execution logic (consumed cross-crate only by `cc-lb-server`, a downward edge); the persisted session record + port already live in `cc-lb-storage-api`; the new `cc-lb-scheduler` keep-alive job depends on `cc-lb-storage-api`, not `cc-lb-engine`.
- **admin's and scheduler's `cc-lb-engine` touch-points are unchanged** (the `feat(admin)` live-logs work is mostly `admin/web` TypeScript), so the admin/scheduler decoupling (§5) is intact.

No topology, dependency-direction, publish, or acceptance-gate change follows from the rebase.

## Status & follow-up

Proposed. Execution is tracked in [.omo/plans/crate-reassembly.md](../../.omo/plans/crate-reassembly.md) (20 todos, single atomic PR). This ADR should move to **Accepted** when that PR merges, and the stale README/plugin-author references to `cc-lb-plugin-api` as a published surface are updated as part of it.
