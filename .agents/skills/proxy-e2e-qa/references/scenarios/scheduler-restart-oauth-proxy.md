# Scheduler Restart OAuth Proxy QA - ADR 0008

Goal: prove that replacing an adaptive scheduler worker does not break a
client-shaped message request whose sole selected OAuth upstream requires a token
refresh, and that the refreshed Bearer token is the credential sent upstream.

This is an isolated in-process proxy-path scenario. It uses temporary SQLite main
storage, a one-connection in-memory SQLite scheduler pool, an ephemeral local fake
Anthropic listener, and a temporary managed API key. It does not use production
storage, credentials, services, Postgres, a paid provider, or the real Anthropic API.

## 0. Environment and preconditions

- Run from the cc-lb workspace root with the repository Rust toolchain and Cargo.
- Build with `--no-default-features --features sqlite`.
- Save raw command output under `/tmp`; retain summaries only in task evidence.
- Do not restart or mutate a service and do not touch the Postgres container.
- No fixed port is required. The fake upstream binds `127.0.0.1:0`.

The `proxy-e2e-qa` `SKILL.md` has no scenario index table, so this scenario does
not require or add an index row.

## 1. Deterministic scenario

Given one scheduler backend, generation 1 consumes distinct benign OAuth job A
through a bounded mpsc dispatch barrier, then is cancelled and joined cleanly.

Given generation 2 is built from that same backend, exactly one expired OAuth
upstream points at the local fake, and one temporary managed API key belongs to the
request principal.

When an Anthropic-compatible `POST /v1/messages` request supplies that managed key
through `x-api-key`, the signer enqueues and awaits OAuth refresh B. The fake token
endpoint pause and the mpsc channel expose B deterministically; the test uses bounded
timeouts and no sleeps, retries, or polling loops for scheduler synchronization.

Then generation 2 dispatches B, the token generation advances once, refresh history
contains exactly one entry, the client receives HTTP 200, and `MessageScript` records
the refreshed Bearer token on the sole fake upstream. That token differs from the
initial access token. The temporary managed key is revoked and observed as revoked
before generation 2 is cancelled and joined.

## 2. Exact execution command

```bash
cargo test -p cc-lb-server --no-default-features --features sqlite --test oauth_refresh scheduler_restart::replacement_worker_refreshes_selected_oauth_upstream_during_message_request -- --exact
```

## 3. Assertion verdicts

| Assertion | Result | Evidence boundary |
|---|---|---|
| Generation 1 consumes A | PASS | The mpsc barrier receives A before the bounded timeout. |
| Generation 1 cleanup | PASS | Its cancellation token is cancelled and its worker task joins with `Ok`. |
| Replacement worker uses the same backend | PASS | Generation 2 is built with the same `SchedulerBackend` retained by the fixture. |
| Refresh B dispatches | PASS | The generation-2 mpsc barrier receives the sole expired upstream ID. |
| OAuth refresh occurs once | PASS | Token generation equals the initial generation plus one and fake refresh history length equals one. |
| Client-visible response | PASS | The in-process lifecycle returns HTTP 200 for `POST /v1/messages`. |
| Refreshed Bearer reaches selected upstream | PASS | The only `MessageScript` request contains `Authorization: Bearer <refreshed token>`, and the refreshed token differs from the initial token. |
| Temporary key cleanup | PASS | `KeyStore::revoke` succeeds and the stored key status is `Revoked`. |
| Generation 2 cleanup | PASS | Its cancellation token is cancelled and its worker task joins with `Ok`; the lazy refresh token is also cancelled. |

`FAIL` means the exact test exits nonzero because any assertion or bounded wait fails.
`BLOCKED` means the command cannot execute because the local Rust/Cargo environment
or required SQLite build prerequisites are unavailable; no weaker claim substitutes
for this test.

## 4. Coverage boundaries

| Surface | Status | Evidence boundary |
|---|---|---|
| Scheduler worker replacement | Automated | Two adaptive workers are built serially from one SQLite backend. |
| Managed-key proxy authentication | Automated | The generated secret appears only as the client request credential. |
| OAuth refresh and persisted generation | Automated | The real scheduler OAuth handler and temporary main storage are used. |
| Selected upstream signing and dispatch | Automated | The real lifecycle sends the request to the sole local OAuth upstream and the fake records its Authorization header. |
| Production or paid-provider behavior | Not covered | No production service, credential, storage, or external provider is contacted. |
| Postgres parity | Not covered | This scenario is intentionally SQLite-only and does not touch Postgres. |
| Live socket proxy listener | Not covered | The client request drives the in-process lifecycle; only the fake upstream owns an ephemeral listener. |

## 5. Cleanup

- The managed API key is explicitly revoked and its `Revoked` status asserted.
- Both scheduler worker generations are cancelled and joined cleanly.
- The lazy refresh cancellation token is cancelled.
- Main SQLite files are removed when `TempDir` drops.
- The in-memory scheduler pool and ephemeral fake listener drop with the test runtime.
- Raw logs under `/tmp` are disposable QA artifacts and are not committed evidence.

## 6. Execution verdict

Executed on 2026-07-11. The exact command exited `0`; all assertions above are
`PASS`, cleanup completed, and no external or persistent resource was used.
