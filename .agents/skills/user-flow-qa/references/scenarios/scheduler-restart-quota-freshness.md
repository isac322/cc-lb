# Scheduler Restart and Quota Freshness QA - ADR 0008

Goal: prove, with isolated composed evidence, that a replacement SQLite scheduler
worker still consumes due work and that subscription-quota freshness moves through
the real writer/storage/API seams after the ADR 0008 scheduler refactor.

This scenario has three deliberately separate claims:

- The scheduler test proves the first worker generation cancels and joins, then a
  replacement generation consumes distinct job B from the same SQLite backend.
- The engine test proves the real checkpoint writer advances the latest stored
  sample to `observed_at_unix_millis=35000` while retaining one semantic checkpoint
  at the first sample because only freshness changed.
- The admin test proves stored checkpoints are rendered through the real in-process
  admin router as step data without a fabricated leading zero.

These tests are composed evidence. No single test spans all three seams.

## 0. Environment and preconditions

- Run from the cc-lb workspace root with the repository Rust toolchain and Cargo.
- Run the three commands **serially** because they share Cargo build artifacts.
- Each test owns temporary SQLite storage through `tempfile::TempDir`; no production
  database, credentials, provider, or persistent test artifact is used.
- The admin test calls `cc_lb_admin::router` in process. It does not bind an external
  port or start a live service.
- Save raw command output under `/tmp`; retain only summaries in the task evidence.
- Do not restart or mutate any production service. Do not touch Postgres.

This scenario does **not** inject a database connection loss, drive the admin-web UI,
run a live service, or prove quota enforcement. It proves only replacement-worker
consumption and the composed writer/storage/API freshness transition described above.

## 1. Fixed context and deterministic transitions

### 1.1 Scheduler replacement transition

Given a temporary SQLite scheduler backend and generation 1 consuming job A,
When generation 1 is cancelled and joined and generation 2 starts against the same
backend with distinct delayed job B,
Then generation 2 dispatches job B before the bounded test timeout.

The test also separately asserts generation 1 dispatches job A and joins cleanly.
The bounded timeout is a test failure bound, not a retry or polling loop added by this
scenario.

### 1.2 Writer and storage freshness transition

Given three quota samples at `0`, `31000`, and `35000` milliseconds with identical
semantic quota fields,
When `start_subscription_quota_writer` consumes them into temporary SQLite storage,
Then the latest row equals the sample at `35000`, while the checkpoint collection has
length `1` and remains anchored to the first sample's timestamp and fingerprint.

### 1.3 Storage and API series transition

Given checkpoint utilization values `0.10` at 30 seconds, `0.20` at 75 seconds, and
`0.60` at 180 seconds,
When the in-process admin router serves the header-source 5h series,
Then it returns carried step values at bucket starts `[0, 60, 120, 180, 240]`, every
returned utilization is nonzero, and an upstream without a checkpoint at or before
the requested range returns an empty series instead of an invented zero-valued anchor.

## 2. Exact execution commands and expected assertions

Run exactly in this order.

### 2.1 Scheduler worker lifetime

```bash
cargo test -p cc-lb-scheduler --no-default-features --features sqlite --test worker_restart_lifetime
```

Expected:

- Feature/backend: `--no-default-features --features sqlite`; temporary SQLite.
- Scheduler layer: generation 1 dispatches A, cancels, and joins cleanly.
- Replacement layer: generation 2 executes distinct job B from the same backend.
- Process exits `0`.

### 2.2 Checkpoint writer freshness

```bash
cargo test -p cc-lb-engine --test subscription_quota_checkpoint_writer checkpoint_writer_latest_freshness -- --exact
```

Expected:

- Environment: real quota writer with temporary SQLite storage.
- Writer/storage layer: latest advances to `observed_at_unix_millis=35000`.
- Semantic checkpoint layer: count remains `1`, anchored to the first sample, because
  freshness changed but the semantic fingerprint did not.
- Process exits `0`.

### 2.3 Checkpoint series API shape

```bash
cargo test -p cc-lb-admin --test subscription_quotas subscription_quota_checkpoint_series_returns_steps_without_fabricated_leading_zeroes -- --exact
```

Expected:

- Environment: isolated temporary SQLite plus in-process admin router.
- Storage/API layer: checkpoint steps carry forward as `0.10`, `0.20`, `0.20`, and
  `0.60` over the asserted buckets; no returned bucket has utilization `0.0`.
- No-anchor layer: the upstream with no checkpoint at or before the range returns an
  empty series rather than a fabricated leading zero.
- Process exits `0`.

## 3. Coverage boundaries

| Surface | Status | Evidence boundary |
|---|---|---|
| Replacement scheduler worker | Automated | `worker_restart_lifetime` exercises cancellation/join and generation-2 consumption of distinct job B. |
| Quota writer to SQLite latest/checkpoint state | Automated | `checkpoint_writer_latest_freshness` uses the real writer and storage adapter. |
| SQLite checkpoints to admin API payload | Automated | The exact admin test seeds typed checkpoint records and calls the real router in process. |
| Database connection-loss recovery | Not covered | No command injects or simulates connection loss. |
| Live service or external network | Not covered | No service process or external port is started. |
| Admin-web UI | Not covered | No browser is opened; make no UI claim. |
| Quota enforcement or routing policy | Not covered | No request selection or enforcement path is exercised. |

## 4. Cleanup

- Scheduler, engine, and admin SQLite files live under Rust `TempDir` fixtures and are
  removed automatically when each test fixture drops.
- The admin router is an in-process Tower service; there is no listener to terminate.
- No manual database mutation, production cleanup, external port cleanup, or provider
  cleanup is required.
- Raw logs written to `/tmp` are disposable QA artifacts and are not project evidence.

## 5. Execution verdict

Record the actual result after each serial command. `PASS` requires the exact command
to exit `0` with its assertions intact; otherwise record `FAIL` or `BLOCKED` honestly.

| Command | Layer(s) | Result | Observed assertion |
|---|---|---|---|
| `cc-lb-scheduler` `worker_restart_lifetime` | scheduler/SQLite | PASS | Both tests passed: generation 1 cancelled/joined after A, then generation 2 consumed distinct job B. |
| `cc-lb-engine` `checkpoint_writer_latest_freshness` | writer/storage | PASS | The exact test passed: latest reached `35000` while one first-sample semantic checkpoint remained. |
| `cc-lb-admin` checkpoint-series exact test | storage/API | PASS | The exact test passed: carried checkpoint steps were nonzero and the no-anchor response remained empty. |

Executed serially on 2026-07-11. All three commands exited `0`; temporary
SQLite fixtures dropped automatically, and the in-process router opened no listener.
