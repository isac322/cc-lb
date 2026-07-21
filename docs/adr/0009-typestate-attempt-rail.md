# ADR 0009: Typestate Attempt Rail for Unified Proxy and Renewal Accounting

Status: Accepted

## Context

The proxy's upstream request dispatch path previously lacked compile-time enforcement of its lifecycle invariants. It's possible to skip critical steps like quota reservation, request shaping, or request signing. This design also created a silent accounting hole for background cache-keepalive (renewal) traffic. Renewal requests bypassed the billing, quota-checking, and logging systems entirely.

We evaluated shared-dispatcher alternatives (Option 2 and Option 3) to unify these paths. Option 2 proposed a shared dispatcher with dynamic dispatch or conditional branching inside the dispatcher itself. Under Option 3, both proxy and renewal requests would be wrapped in a generic request/response trait or enum. Both alternatives were rejected. They blurred the ownership of reservation and signing capabilities across proxy retries, streaming, warmup, and renewal. Furthermore, they failed to provide compile-time safety, leaving the system vulnerable to bugs where steps are performed out of order or skipped entirely.

## Decision

We adopted a typestate-enforced attempt rail design: `AttemptIntent -> Reserved -> Scoped -> Signed`. This design draws inspiration from type-safe hardware abstraction layers like the embedded-hal crate in the Rust ecosystem. The compiler itself enforces the correct order of operations, making it impossible to skip a step.

### Ownership Model

The `Reserved` state holds the optional quota reservation. It's held across internal 401 retries and streaming responses. Each attempt borrows or moves through `Scoped` and `Signed` states. The `Signed` state is consumed by dispatch. When a response succeeds, the `ResponseAccountingGuard` calls `forget()` to suppress the RAII refund. This handoff occurs only after durable success ownership transfers to the reconciliation subscriber.

### Renewal Durable Exactly-Once Accounting

Background cache-keepalive (renewal) traffic is moved onto this same rail. This ensures renewal traffic is billed, quota-checked, and logged exactly like normal traffic. We rejected any 'renew but don't account' half-state. The per-principal `cache_keepalive.enabled` configuration remains the only off-switch. No new config flag or kill-switch was added.

To prevent data loss under saturation, renewal accounting is a durable exactly-once design. It doesn't rely on the best-effort, drop-on-full event bus. The bus is observability-only. Cost computation in the renewal finalizer runs inline using the same pricing path as the subscriber (`virtual_cost_micros_full` with model, token, and `upstream_kind` fields). It writes the final `RequestEvent` and projections via the awaited `append_request_event_with_projections` method. The event ID is deterministic: `renewal:{session_key_hash}:{generation}`. Only after the durable write and direct `reconcile_by_id` succeed does the finalizer call `forget()`.

### Per-Turn Claim CAS and At-Most-Once Crash Recovery

Before decrypting, reserving, signing, or dispatching, the scheduler atomically claims the turn. This is done via a compare-and-swap (CAS) `UPDATE` query that transitions the session's `enqueue_state` from `enqueued` to `running`. Only the winner of this CAS proceeds.

If a node crashes while a turn is `running`, the session is recovered. The scheduler housekeeper (`ApalisHousekeepingJobHandler::prune_cache_keepalive_sessions`) reclaims stale `running` sessions to terminal (`Stale`) and clears the lease. A recovered turn is never re-dispatched. This at-most-once crash recovery tradeoff is deliberate. Because a crash can occur after the upstream call but before the durable write, re-dispatching could double-call the upstream. Upstream idempotency was rejected as over-engineering for a best-effort keepalive feature.

### ObserveOnly Mode

Sessions with a NULL `accounting_key_id` run in ObserveOnly mode. They still dispatch through the rail and write priced durable rows and projections. However, they don't make a reservation or emit a `LimitDecision`. The key is re-seeded only when the next real proxy request enqueues the session with a live `key_id`.

### Rollback Safety

To ensure rollback-safety, the snapshot AAD remains v1. It's exactly `cache_keepalive_snapshot:v1:{principal_id}:{session_key_hash}:{upstream_id}:{generation}`. The `accounting_key_id` is stored as a plaintext nullable column outside the AAD. This allows pre-deploy and post-deploy snapshots to decrypt identically.

## Verification

We verified the implementation using a test-driven, blackbox-first strategy. All tests pass on the local SQLite backend.

### Verification Commands

To run the full suite of characterization and integration tests, execute:

```bash
cargo test --workspace
```

To run the dedicated renewal accounting end-to-end tests, execute:

```bash
cargo test -p cc-lb-engine --test renewal_accounting_e2e
```

These tests prove that:
1. A renewal cycle bills exactly once, creating a single priced `request_events` row and matching projections.
2. Duplicate delivery attempts are blocked by the claim CAS, billing exactly once.
3. ObserveOnly sessions write priced rows but emit no `LimitDecision`.
4. Saturated event bus channels don't block durable accounting or direct reconciliation.
5. Disabling keepalive mid-cycle refunds the reservation and finalizes the orphan sequence cleanly.
6. Stale running sessions are reclaimed to terminal (`Stale`) by the housekeeper without re-dispatching.
