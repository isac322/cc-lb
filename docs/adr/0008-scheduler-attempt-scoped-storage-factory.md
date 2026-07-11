# ADR 0008: Scheduler Attempt-Scoped Storage Factory

- Status: Accepted
- Date: 2026-07-10
- Plan: [.omo/plans/scheduler-backend-resource-lifetimes.md](../../.omo/plans/scheduler-backend-resource-lifetimes.md)
- Supersedes: none

## Context

During a production incident, an `isac-personal` token refresh job remained pending for 6 hours and 44 minutes. The scheduler's supervisor spawned an adaptive replacement worker after a transient database connection issue. This replacement worker successfully registered heartbeats but never claimed or executed any jobs from the queue. Meanwhile, the cron replacement worker used the exact same SQLite connection pool, recovered successfully, and consumed its scheduled tasks. A full process restart reconstructed the scheduler state from scratch, which immediately cleared the pending job backlog.

The investigation revealed a critical recovery defect distinct from the initial trigger. A transient one-connection pool acquisition timeout triggered the initial worker exit. The recovery defect was that the replacement worker could not poll the queue because it cloned a retained, drained Apalis storage instance.

Apalis storage relies on `Config::clone()`, which shares an underlying `MultiStrategy` backed by `Arc<Mutex<Vec<BoxedPollStrategy>>>`. Calling `poll_strategy()` on the first worker drains this vector completely. Consequently, any subsequent clones of the storage instance have an empty strategy vector. They cannot poll or consume jobs, even though they can still heartbeat. The same one-connection pool remained capable of serving SQL because cron recovered, but this evidence does not prove uninterrupted pool health. This retained storage instance could not serve as a reusable worker prototype because its shared poll-strategy vector was drained.

## Decision

### 1. Retain only durable process-lifetime resources

We will modify `SchedulerBackend` variants to retain only durable, process-lifetime resources. The SQLite backend will hold only the SQLite connection pool and the clock handle. For Postgres, only the connection pool is retained. We will remove any retained Apalis storage or configuration instances from these structures. The queue identities will remain as project-level constants: `ADAPTIVE_QUEUE` and `CRON_QUEUE`.

### 2. Construct fresh Apalis state on every worker attempt

Every worker build attempt must construct a brand-new Apalis storage instance from the durable connection pool. For the SQLite backend, we will call `SqliteStorage::new_in_queue(&pool, ADAPTIVE_QUEUE)` on each attempt. The Postgres backend will call `PostgresStorage::new_with_notify(&pool, &Config::new(ADAPTIVE_QUEUE))` using a fresh `Config` expression each time. Both storage constructors are synchronous and perform no SQL or connection acquisition. In Postgres, the long-lived `PgListener`/`LISTEN` is established only when worker polling begins. The replacement worker swaps attempt-scoped listener state rather than adding steady-state listeners. This ensures that every replacement worker receives a fully populated, undrained poll strategy vector.

### 3. Avoid retained storage on producer and list paths

We will enforce an exhaustive entry-point invariant where all enqueue and list entry points derive from retained pools only. The SQLite enqueue path remains direct SQL. Our Postgres `push_job` method will create and consume one fresh producer storage instance inside the call. Task-builder enqueue paths will remain direct SQL. Both adaptive and cron list methods will each create and consume one fresh non-notify wrapper inside the list call. No producer or list storage instance will escape its operation. This wrapper construction is in-memory only, while push and list operations perform their existing database query and do not establish PgNotify listener state.

### 4. Rename storage structures to backend structures

We will rename `SqliteSchedulerStorage` to `SqliteSchedulerBackend` and `PostgresSchedulerStorage` to `PostgresSchedulerBackend`. These renamed payload structs will own private durable fields. Public constructors are required for in-workspace consumers in `cc-lb-server`, `cc-lb-admin` tests, and integration tests. Apalis-storage factory methods will be `pub(crate)` or narrower and return a new value for immediate operation or attempt use. The returned state must not be stored in any backend, supervisor, or process-lifetime structure. Since the `cc-lb-scheduler` crate is configured with `publish = false`, this internal renaming has no crates.io consumer impact, but there is workspace migration impact.

Our migration obligations require updating every import, enum construction, struct literal, and removed-field access across scheduler, server, and admin tests. Verification must cover clean compilation and test execution for `sqlite-only`, `postgres-only`, and `all-features` across all targets and tests.

### 5. Reject retaining or cloning Apalis Config

We explicitly reject retaining or cloning an Apalis `Config` instance as an immutable blueprint. Cloning a `Config` shares the same drainable `MultiStrategy` and would recreate the exact same bug. Every storage construction must use a brand-new `Config` expression.

### 6. Maintain architectural parity for Postgres

We will apply these same lifetime invariants to the Postgres backend to maintain architectural parity. The observed production failure is SQLite-specific. However, Postgres uses the same `apalis_sql::Config` mechanism, so this parity is preventive rather than evidence of a Postgres incident. Applying this pattern to both backends prevents future recovery defects in Postgres.

## Consequences

### Positive

- Eliminates the adaptive worker recovery defect by ensuring every worker attempt starts with a fresh, undrained poll strategy.
- Simplifies the `SchedulerBackend` variants to hold only durable, process-lifetime resources.
- Maintains strict architectural parity between the SQLite and Postgres scheduler backends.
- Prevents silent failures where a replacement worker heartbeats but never claims jobs.
- Avoids unnecessary connection pool recreation or size tuning.
- Fresh SQLite storage construction does not acquire a new connection or create another pool. Existing polling, heartbeat, enqueue, and list operations still contend through the configured pool.

### Negative/Risks

- Constructing fresh storage instances on every worker attempt adds a small in-memory allocation and pool-handle clone cost.
- A Postgres worker restart reconnects one long-lived listener and executes LISTEN per attempt, rather than per push or list operation.
- Per-push and per-list wrappers add minor in-memory allocation, while the existing database query cost remains.

### Neutral

- The queue names remain as static project-level constants.
- The `cc-lb-scheduler` crate remains unpublished, allowing safe internal refactoring.

## Alternatives considered

| Alternative | Rejection reason |
| --- | --- |
| Propose a minimal SQLite-only patch | This would leave the Postgres backend vulnerable to the same class of recovery defects and break architectural parity. |
| Recreate the connection pool on worker restart | Recreating the pool is expensive, unnecessary, and does not address the root cause of the drained poll strategy. |
| Tune connection pool size or timeouts | Tuning the pool size does not prevent the drained poll strategy bug when a transient timeout eventually occurs. |
| Upgrade the Apalis library version | The pinned rc.8 and rc.9 source has the inspected behavior. Our local ownership correction does not depend on an upgrade, and this ADR makes no claim about uninspected releases. |
| Introduce database schema changes | The defect is entirely in the in-memory resource lifetime management, not the database schema. |
| Implement worker-level retry loops or sleeps | Adding sleeps or retries masks the recovery defect instead of fixing the root cause. |

## Rollout and QA requirements

### Deterministic TDD and QA requirements

We will enforce strict, deterministic testing requirements to verify the fix and prevent regressions:

- **Backend-Parity Contract Test**: We must write a test that verifies the backend-parity contract. This test must use the public constructor and the public `build_adaptive_worker` function to generate workers. It must run against real, isolated SQLite and Postgres databases. The test must use a dispatch closure, a channel barrier, and a bounded `tokio::time::timeout` to coordinate execution. We must start and await the first worker, then generate and run a second worker from the same backend instance. The test must prove that both workers can successfully claim and execute distinct jobs. We explicitly forbid using private fields, private hooks, sleeps, retries, or poll loops for test synchronization.
- **Postgres 18 and Code Coverage Gates**: All-feature tests must pass on Postgres 18, and the `cc-lb-scheduler` crate must maintain a minimum code coverage of 60 percent. These gates are explicitly linked to existing enforced contracts in `.github/workflows/ci.yml`.
- **Quota State-Transition QA**: We will use composed evidence for the quota scenario. The SQLite and Postgres restart-lifetime tests prove replacement-worker consumption. Dedicated engine and admin tests prove subscription-quota writer/storage freshness and the admin quota series behavior. The scenario composes these seams and does not claim one causal end-to-end replacement-to-quota test. We do not claim quota enforcement, and routing eligibility or deprioritization will only be verified where the scenario specifies it.
- **Proxy QA with Throwaway Instance**: We will perform proxy QA using a throwaway instance with isolated storage and off-production ports. This setup will use a deterministic local token endpoint/upstream and a temporary client credential. We will trigger first-worker termination and replacement, then verify that the refreshed credential is used by one minimal proxy request. We will assert route and upstream evidence, followed by credential revocation and complete artifact cleanup. This QA process will perform no production service mutation or paid provider calls.

### Rollout facts

- **No Production Service Mutation**: Neither the implementation nor the QA process may mutate or restart the running production service. Isolated test instances are allowed and required for all verification steps.
- **Root-Cause CI Failures**: Any CI failure during the rollout must be thoroughly investigated and root-caused. We explicitly forbid rerunning failed CI jobs to bypass transient failures, as enforced by our CI flake handling policy.

## Out of scope

- Tuning connection pool sizes, idle timeouts, or acquire timeouts.
- Modifying the database schema or migrations for SQLite or Postgres.
- Upgrading the external Apalis library dependency.
- Implementing any public test hooks or production-visible synchronization primitives.

## Operational invariants (review consensus)

The architecture, performance, and compatibility reviews independently approved the following invariants after one revision round:

- `SchedulerBackend` variants must never retain or cache Apalis storage or configuration instances.
- Every worker build attempt must construct a brand-new Apalis storage instance from the durable connection pool.
- Cloning or sharing an Apalis `Config` instance is strictly prohibited.
- The connection pool must remain as a process-lifetime durable resource.
- A "fresh" storage instance is defined as constructed from the durable pool and a brand-new `Config::new(queue)` expression within the attempt or operation. Cloning previously constructed storage or Config never qualifies.
- No producer or list storage instance may escape its operation.
- No attempt-scoped listener or storage state may be retained.
