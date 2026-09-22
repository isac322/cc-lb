## Authentication comes first

An unauthenticated request gets nothing. Not a buffered body, not a JSON parse, not a routing decision, not a quota reservation, not a durable row. An unauthenticated caller must not be able to cost this process more than reading its headers and writing back a rejection. This is the one ordering rule in the request path that is never traded away for convenience.

The rule is enforced by the type system, not by review. `crates/cc-lb-engine/src/authn_rail.rs` defines `Authenticated`, a proof that authentication already succeeded. It has private fields and therefore no public constructor; `authenticate_first` is the only way to obtain one, and it takes headers only — there is no parameter through which a body could be passed. Every API that does work on a request's behalf takes `&Authenticated`, so violating the order is a compile error rather than a review comment.

Rules:
- Authenticate before touching the request body. Buffering, parsing, routing, upstream selection, signing, quota reservation, and durable writes all happen after authentication, never before.
- Obtain `Authenticated` from `authenticate_first` (or `Lifecycle::authenticate`, which wraps it). Never reconstruct, forge, transmute, or clone a proof into existence by another route.
- Keep `&Authenticated` on every function that consumes a request body or dispatches upstream. Removing that parameter to make code compile defeats the entire mechanism.
- Authentication *failure* is not "before authentication". It reached the credential check and was rejected, so it persists exactly one `request_events_v1` row. Only requests that never reach the check — 404 router fallback, 405, drain 503, pre-handler timeout — stay silent.
- The intentionally public endpoints are exactly `/healthz`, `/readyz`, `/admin/health`, `/admin/health/state`, the two admin SPA asset routes, and the metrics listener. Adding to that list requires explicit user approval.
- Keep every admin route behind the `require_admin_auth` `route_layer` in `crates/cc-lb-admin/src/routes.rs`. Never register an admin route outside that layer.

Forbidden shortcuts:
- Dropping the `&Authenticated` parameter from a body-reading or dispatching function.
- Deleting, weakening, or `#[ignore]`-ing the compile-fail proofs in `crates/cc-lb-engine/tests/trybuild/authn_rail/`.
- Adding a public or `#[doc(hidden)]` constructor for `Authenticated` so a caller can skip authentication.
- Moving a body read, JSON parse, or upstream call above the authentication call "just to measure it" or "just for this one path".
- Registering a new unauthenticated route on the proxy or admin listener without explicit user approval.

## SQLite Storage Backend

SQLite is now the default storage backend for local development and CI. The historical redb backend has been removed entirely.

Rules:
- Use SQLite for all local development and testing. It's the default backend, so you don't need to specify extra feature flags for standard builds.
- Keep SQLite database files on a local filesystem. Don't use NFS mounts or other network filesystems, as they don't support SQLite's locking mechanisms reliably.
- Ensure all new migrations and queries are compatible with SQLite's SQL dialect. For example, use `strftime('%s', 'now')` for Unix timestamps.
- Remember that SQLite enforces foreign keys per connection. The connection pool is tuned with `PRAGMA foreign_keys = ON`, WAL mode, and a busy timeout of 5 seconds.
- Don't attempt to configure multi-replica SQLite setups. We don't support Litestream or rqlite.
- Map complex types like arrays using JSON text columns. SQLite doesn't have native array types like Postgres.

## CI flake handling

A CI failure that turns green on retry is not a flake to be waved through. It is a real defect (race, ordering, resource contention, unmanaged shared state) that will eventually hit production. Investigate the failure in the same PR that revealed it.

Rules:
- Never re-run a failed CI job or workflow to force a green result. This covers the GitHub UI "Re-run failed jobs" button, `gh run rerun`, `gh workflow run`, restarting the workflow from a fresh commit with no fix, and every equivalent retry mechanism.
- When CI fails, reproduce locally with the exact CI invocation (features, env vars, database, runner-equivalent concurrency), find the root cause, apply a minimal fix, verify locally, and push the fix in the same PR that revealed the flake.
- If the failure does not reproduce on the first local run, escalate stress before concluding the test is stable: raise thread counts, toggle feature flags, run alongside sibling tests, shrink internal timeouts, or run under `--test-threads=1` to isolate. Do not stop at "not reproducible" without a concrete stress budget.
- Never `#[ignore]`, `#[should_panic]`, delete, or env-gate the failing test to bypass CI. That transfers the risk to production and destroys the signal.
- Never add `sleep`, `tokio::time::sleep`, retry loops, looser assertions, or wider error tolerances to mask timing. Repair the invariant, not the assertion.
- Distinguish infrastructure flake (runner image, sccache races, missing secrets, transient network to `sh.rustup.rs` or crates.io) from test flake. Infrastructure flakes require a workflow, runner-image, or action-level fix and must be surfaced as an issue or CI PR — not silently retried.
- Every flake fix commit body must contain the root cause, the applied fix, and local reproduction proof (e.g. `Verified 20/20 passing runs on <sqlite|postgres|both>`).
- If a session cannot reach the root cause, stop and escalate to a follow-up issue with the failing job URL and observed symptoms. Do not land a band-aid.
- Never add production API (public methods, exported types, enum variants) whose only immediate consumer is a test synchronization or observation hook. Exposing a public `flush`/completion primitive on a production type solely so a test can wait deterministically is flake masking; use a test-scoped observer or wrapper instead.

## JSON library policy

Hot-path JSON *parsing* uses `sonic-rs` (`from_slice`, `from_str`). Everything else — the `Value` AST, `serde_json::Map`, the `json!` macro, `to_vec`/`to_string` serialization — stays on `serde_json`. See `docs/adr/0002-json-library-strategy.md` for the full rationale and the list of migrated sites.

Rules:
- Never swap `serde_json::to_vec` / `to_string` / `to_writer` for the `sonic_rs` equivalents on any path that feeds a hash, cache key, or bytes-on-the-wire comparison. `serde_json` (without `preserve_order`) emits sorted keys; `sonic_rs::Object` is insertion-ordered. Swapping silently changes every cache-affinity routing hash at deploy time. The concrete example lives in `crates/cc-lb-engine/src/prompt_cache_simulator.rs`: `SerializationScratch::serialize` (`serde_json::to_writer`) produces the bytes consumed by `block_digest` and `serialized_prefix`, which derive every v5 prefix key. (Before the schema-5 cutover this rule cited the since-deleted `lifecycle.rs::cache_prefix_hash` / `cache_prefix_hash_and_token_count_v2`.)
- Never migrate parse sites in structs that use `#[serde(flatten)]`. `sonic-rs` rejects them at runtime (upstream issue #114). The only two flatten users today are `crates/cc-lb-storage-api/src/warmup_attempts.rs` and `crates/cc-lb-admin/src/v1/upstreams.rs`.
- Storage adapters (`cc-lb-storage-sqlite`, `cc-lb-storage-postgres`), config (`cc-lb-config`, `schemars`), and the plugin ABI (`cc-lb-plugin-api::types`) MUST stay on `serde_json`. Framework-forced.
- When new hot-path JSON parsing is added (per-request, per-SSE-event), use `sonic_rs::from_slice` / `sonic_rs::from_str`.
- Cache the parsed `Value` at the request boundary and thread it as `Option<&Value>` to helpers instead of re-parsing the same `Bytes`. `lifecycle.rs::parse_body_json` is the reference pattern; the request handler parses once and reuses across `request_cache_metadata_from_value`, `extract_model`, and `reserve_limit` / `LimitRequest::from_value`.

Forbidden shortcuts:
- Clicking "Re-run failed jobs" in the GitHub UI.
- `gh run rerun` or `gh workflow run` as a substitute for a code fix.
- `#[ignore]`, `#[should_panic]`, or conditional-cfg deletion of the failing test.
- `sleep`, retry loops, or weakened assertions to hide a race.
- Silently removing, moving, or renaming the failing test to bypass CI matching.
- Adding a public `flush`/`persist_now`/completion/notification method on a production type whose only immediate consumer is a test — that is masking the missing test seam by contaminating the production API surface.
