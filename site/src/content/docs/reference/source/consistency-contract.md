---
title: "Admin consistency contract source notes"
description: "What each admin response guarantees about persistence, audit, and convergence."
slug: docs/reference/source/consistency-contract
---

## Guarantee classes

Each operation is described with one of three classes, or with a hybrid that names which part falls into which class.

- **Strict sync.** The response is sent only after the state change is committed to storage, the audit row (when the route writes one) is committed, and observers on the serving replica see the change. A 2xx means all three already happened.
- **Async best-effort.** The response means the work was handed to an in-process queue or task. Nothing durable backs it. A crash, a full channel, or a failed write can lose it, and the client is not told.
- **Async guaranteed.** The response means a durable row exists (for example a scheduler job row) that a worker on any replica will pick up, with retries and crash recovery. Delivery is eventual, not immediate.

"Serving replica" means the cc-lb process that handled the admin request. "Peer replica" means any other process that shares the same database.

## Summary

| Subsystem | Operation(s) | Class | Durable at response | Converges later via |
|---|---|---|---|---|
| Admin mutations | 38 mutating admin routes | Strict sync for state and audit, written in two transactions (see below) | Domain state, then audit row | Not applicable |
| Dynamic routing view | Mutations that touch upstreams, principals, keys, or plugins | Strict sync on the serving replica, with rebuilds serialized per process; async on peers | The state behind the view | Postgres `LISTEN`/`NOTIFY` with a 250 ms debounce, then a later successful reconcile pass (60-70 s interval plus jitter, not a deadline) |
| Configuration editor | Draft put, validate, save | Strict sync for storage and file | Draft row, validation report, or the written config file | Never at runtime: a saved file applies on process restart |
| Scheduler | Admin actions that enqueue jobs; cron ticks | Async guaranteed | Job row in the scheduler queue | Worker poll; retry classes; stale-lock recovery |
| Scheduler | Post-claim usage poll after a limit reset | Async best-effort | Nothing | The regular 60 s usage poll tick |
| OAuth token refresh | Lazy refresh on the proxy path | Strict sync for the token; best-effort audit | New token ciphertext and bumped token generation | Peers rebuild through `NOTIFY` or reconcile |
| Price catalog | Catalog fetch and install | Async guaranteed fetch; async best-effort install | Snapshot in storage and the disk cache (fetch job only) | Each replica's 60 s local installer |
| Request events | Proxy request rows | Async best-effort | Nothing at proxy response time | Lifecycle assembler insert with bounded retries |
| Dashboards | Summary and usage reads | Read of a materialized rollup | Not applicable | Usage rollup job every 30 s (3 s jitter); 2 s response cache |
| Subscription quotas | Observations from upstream headers | Async best-effort for the serving replica's cache and for persistence | Nothing at proxy response time | Lifecycle subscriber updates the cache; batched writer (256 rows or 100 ms); Postgres peers hydrate on a single best-effort `NOTIFY`; every reconcile pass also attempts a hydrate from storage, independent of the view hash |

## Admin mutations and audit records

Code: `crates/cc-lb-admin/src/audit.rs`, `crates/cc-lb-admin/src/v1/`, `crates/cc-lb-admin/src/routes.rs`.

The audit write is synchronous. `record_admin_audit` awaits `append_audit` on the storage pool directly. Nothing is buffered, so when a route returns success, its audit row is already committed. When no storage is configured, `record_admin_audit` returns success without writing anything.

State and audit are written in two separate transactions, in this order:

1. The handler commits the domain change, for example `UpstreamStore::update`.
2. The handler awaits `record_admin_audit`.
3. If the route affects routing, the handler rebuilds the dynamic view before responding (see [Dynamic routing view](#dynamic-routing-view)).

Once the handler sees that step 1 committed, it runs everything after it (steps 2 and 3, plus any other writes that follow the commit) as a continuation on a separate task through `complete_after_commit` (`crates/cc-lb-admin/src/v1/post_commit.rs`). The continuation runs independently of request cancellation: a client disconnect drops the request, not the task. A connected request awaits the continuation's result, and the existing failure responses for audit, storage, and rebuild errors are unchanged, so the continuation does not guarantee that the audit row or the rebuild succeeds. Runtime shutdown can cancel the task, and the route then returns 500; a panic in the continuation propagates to the waiting request instead of being converted to 500.

For upstream writes (create, update, enable and disable, API-key and OAuth token writes, refresh completion, clearing the warmup dialect plugin) and the plugin registry label update, both storage backends read the returned record inside the write transaction and commit last. A failure while reloading or decoding that record rolls the write back, so the route returns an error without a committed change, and the continuation starts right after the storage commit.

If step 2 fails, the handler returns `500` with `{"error": "audit_write_failed"}`, but the step 1 change is already committed and is not rolled back. The client gets an error even though the change took effect, and no audit row records it. Check current state with a `GET` before retrying. On "Rebuilt" routes (see the table below), the handler first attempts to rebuild the serving replica's view from committed storage, so routing on that replica follows the committed change when the rebuild succeeds. The 500 body is the same and carries no generation or rebind header; if that rebuild fails, it is logged and the view waits for a later notification or reconcile rebuild. The exceptions:

- `POST /admin/v1/principals/{id}/keys` attempts to compensate. If the audit write fails, `record_issue_audit_or_revoke` tries to revoke the newly issued key; the handler then attempts a view rebuild and returns 500 (`crates/cc-lb-admin/src/v1/keys.rs`). If the revocation also fails, both errors are logged, the key stays active, and the response is the same 500. Check the key's state before retrying.
- `POST /admin/v1/config/save` does the reverse. If the success audit fails, it logs the error and still returns 200, because the file is already written.
- `POST /admin/v1/upstreams/from-oauth-draft` attempts to hard-delete the new upstream if scheduler seeding or metadata storage fails. After attempting the rollback and the PKCE cleanup, the handler attempts a serving-view rebuild for either rollback outcome, then returns 500 `oauth_draft_create_failed`. If the hard delete fails, the upstream stays persisted and the response `detail` contains `rollback delete failed`. That compensation runs before the audit write. An audit failure after it does not roll anything back.

A few other failures after the commit also attempt a serving-view rebuild before returning their original error, because the committed change stays in storage. A failed rebuild is logged and does not change the response:

- `PUT`/`PATCH /admin/v1/upstreams/{id}`, when seeding the warmup bootstrap job fails.
- `POST /admin/v1/upstreams/{id}/oauth/complete`, when seeding the refresh and warmup jobs fails.
- `POST /admin/v1/upstreams/from-oauth-draft`, when deleting the PKCE flow row fails after the upstream and its metadata are stored.
- `DELETE /admin/v1/plugins/registry/{id}`, `PUT`/`DELETE /admin/v1/plugin-chain-entries/{id}`, and `POST /admin/v1/principals/{principal_id}/plugin-chain/reorder` and `/rebalance`, when a storage read that follows the commit fails.

Most handlers audit only the success path. Validation and storage errors return early without an audit row. Three routes audit both the success and the failure of their main operation:

- `POST /admin/v1/upstreams/{id}/limit-resets/claim`, because a failed or timed-out claim may still have used the grant on the provider side.
- `POST /admin/v1/plugins/wasm`, which records failures as `plugin_registry_upload_attempt`.
- `POST /admin/v1/config/save`, which records failures as `config_save_failed`.

These routes still return some errors before the audited operation starts, and those write no operation row: a rejected request body, the upstream lookup failure or 404 for a claim, and the upload rate limit (429) or a failed rate-limit audit query for an upload. The save handler's own authorization check writes no `config_save_failed` row, but through the shipped router a caller without write permission is stopped earlier by `require_admin_auth`, which awaits an `authz_denied` audit row before returning 403 (a failed write there is logged and the 403 still returns).

The plugin registry label update and WASM garbage collection write audit rows like the other mutating routes:

- `PATCH /admin/v1/plugins/registry/{id}` updates the registry label, records `plugin_registry_label_update(id=<registry id>)`, and rebuilds the view.
- `POST /admin/v1/plugins/wasm/gc` deletes unreferenced WASM blobs from storage one at a time and attempts to delete their cache files from disk. Candidates are processed on the request future until one blob is actually deleted. After that first successful deletion, the remaining deletions, cache-file cleanup and summary audit run in the post-commit continuation. If no blob is deleted, all candidates are checked on the request and the final audit runs in the continuation. After the loop it writes one summary row, `plugin_wasm_gc(removed=<count>)`, also when nothing was removed. If a deletion fails partway, the route returns the storage error and writes no audit row for the blobs already removed. It does not rebuild the view.

Rejected authentication is audited synchronously. `require_admin_auth` awaits `record_auth_rejected` before it returns 401. If that write fails, it logs a warning and still returns 401, after a fixed 100 ms delay (`crates/cc-lb-admin/src/auth/mod.rs`). Some read routes also write synchronous audit rows (`audit_query`, `config_editor_read`, `config_draft_read`, `config_history_read`). A row written by `GET /admin/v1/audit` does not appear in that same response.

Optimistic concurrency: principal, upstream, plugin registry, and plugin chain resources return `ETag: W/"<revision>"`. Most of their update and delete routes require `If-Match`, and return `428 if_match_required` when it is missing. A stale revision returns `409 stale_revision` with `current_revision` for principals, upstreams, and OAuth completion, and `412 stale_revision` with `current` for plugin registry and chain entries. For principals, a revision mismatch on a live principal returns the same `stale_revision` body with `current_revision` on SQLite and Postgres. Conflicts on a soft-deleted principal still depend on the operation and backend: repeating a soft delete returns `409 storage_conflict` without a revision on both, while enabling or disabling a deleted principal with an older revision returns `storage_conflict` on SQLite and `stale_revision` on Postgres. Some routes take the revision another way:

- `PUT /admin/v1/principals/{id}/allowed_models` reads `expected_revision` from the JSON body, not `If-Match`.
- `POST /admin/v1/principals/{principal_id}/plugin-chain/reorder` takes an `expected_revision` for each entry in the body.
- `POST /admin/v1/plugins/wasm` never reads `If-Match`. An upload that replaces an existing registry entry of the same name with different bytes is handled in one of two ways. An explicitly confirmed replacement sends the multipart part `confirm_replacement=true` and must also send `replace_registry_id` and `expected_revision`. An automatic replacement, allowed without confirmation when the incoming version is a higher semver than the current one, may omit both parts. When `expected_revision` is omitted, the server uses the revision it just read from storage, so no client-supplied revision is checked and a concurrent change between the client's read and the upload is not detected.
- `POST /admin/v1/upstreams/{id}/oauth/complete` checks the upstream revision captured when the flow was started with `/oauth/start`, stored in the PKCE flow row.
- `POST /admin/v1/principals/{principal_id}/plugin-chain/rebalance` takes no revision at all.
- Configuration draft routes carry `expected_revision` in the request body.

| Routes | State | Audit | Dynamic view on serving replica |
|---|---|---|---|
| `POST /admin/v1/principals`; `PUT`, `PATCH`, `DELETE /admin/v1/principals/{id}`; `POST /admin/v1/principals/{id}/enable`, `/disable`; `PUT /admin/v1/principals/{id}/allowed_models`, `/router-terminal` | Committed | After state | Rebuilt |
| `POST /admin/v1/principals/{id}/keys` | Committed | After state; on failure, revocation of the key is attempted but can fail | Rebuilt |
| `POST /admin/v1/principals/{id}/keys/{key_id}/revoke` | Committed | After state | Rebuilt |
| `POST /admin/v1/upstreams`; `PUT`, `PATCH`, `DELETE /admin/v1/upstreams/{id}`; `POST /admin/v1/upstreams/{id}/enable`, `/disable`; `DELETE /admin/v1/upstreams/{id}/warmup-dialect-plugin` | Committed (an update that turns warmup on for an OAuth upstream also enqueues a warmup job) | After state | Rebuilt |
| `POST /admin/v1/upstreams/from-oauth-draft`; `POST /admin/v1/upstreams/{id}/oauth/complete` | Committed, and refresh/warmup jobs enqueued as applicable; for `from-oauth-draft`, a failed onboarding step triggers a hard delete that can itself fail | After state | Rebuilt |
| `POST /admin/v1/principals/{principal_id}/plugin-chain`, `/reorder`, `/rebalance`; `PUT`, `DELETE /admin/v1/plugin-chain-entries/{id}` | Committed | After state | Rebuilt |
| `DELETE /admin/v1/plugins/registry/{id}`; `POST /admin/v1/plugins/wasm` | Committed | After state (upload audits failures of the upload itself, not its prechecks) | Rebuilt |
| `PATCH /admin/v1/plugins/registry/{id}` | Committed | After state | Rebuilt |
| `POST /admin/v1/plugins/wasm/gc` | Reported blobs deleted from storage; cache-file deletion is best-effort | One summary row after the deletion loop, also when nothing was removed; a deletion error returns before that audit write | Not rebuilt |
| `POST /admin/v1/oauth/draft/start`, `/admin/v1/oauth/draft/complete`; `POST /admin/v1/upstreams/{id}/oauth/start` | PKCE flow row committed | After state | Not rebuilt |
| `PUT /admin/v1/config/draft`; `POST /admin/v1/config/draft/validate` | Draft or validation report committed | After state | Not rebuilt |
| `POST /admin/v1/config/draft/download` | None (returns bytes) | Before the bytes are sent; on audit failure no bytes are sent | Not rebuilt |
| `POST /admin/v1/config/save` | File replaced, draft cleared | After state; on audit failure it still returns 200 | Not rebuilt |
| `POST /admin/v1/upstreams/{id}/subscription-metadata/refresh` | Metadata (and refreshed token, if needed) committed inline | After state | Not rebuilt |
| `POST /admin/v1/upstreams/{id}/warmup/fire-now` | Provider call done inline; the warmup attempt row is written best-effort | After state | Not rebuilt |
| `POST /admin/v1/upstreams/{id}/limit-resets/claim` | Epoch fence settled; provider claim done inline | Success and failure of the claim, after the upstream lookup | Not rebuilt; may spawn a best-effort usage poll |

`POST /admin/v1/router/preview` is a `POST` but changes no state and writes no audit row.

## Dynamic routing view

Code: `crates/cc-lb-admin/src/v1/mod.rs`, `crates/cc-lb-control/src/dynamic_view.rs`, `crates/cc-lb-server/src/notify_listener.rs`, `crates/cc-lb-server/src/reconcile.rs`.

**Serving replica: strict sync, with a deferred escape hatch.** Each "Rebuilt" route in the table above calls `add_dynamic_rebind_headers` after its audit write. That rebuilds the `DynamicView` from storage and installs it with `try_store_if_newer`, which installs a view only when its generation is strictly greater than the resident one. Every rebuild, whether from an admin route, the `NotifyListener`, or the `Reconciler`, reads the resident generation `g` and produces a view with generation `g + 1`.

Rebuilds in one process are serialized. Each of those three paths holds the `DynamicViewHolder` rebuild lock (`lock_rebuild`) from the resident-generation read through the build and the install, so two rebuilds never start from the same generation and an older build cannot replace a newer one. The admin rebuild starts after the mutation and its audit row have committed, so the view it installs was built from storage that includes the mutation. A rebuild waits for any rebuild already in progress, so a queued rebuild can add latency to the admin response. The compare-and-swap in `try_store_if_newer` remains as a defensive check; under the lock it rejects a view only after generation saturation or a publication made outside the lock.

The response then carries one of two headers:

- `x-cc-lb-generation: <n>`: under the normal rebuild protocol (generation not saturated, every publisher holding the lock), `n` is the generation this route's rebuild installed, built after the mutation committed, and `n` or a newer generation is resident. A later conflicting mutation may already have superseded that state. If the defensive compare-and-swap rejected the view (saturation or a publication outside the lock), `n` is the generation resident at that moment and does not prove that this route's view was installed.
- `x-cc-lb-rebind-status: deferred`: the rebuild failed and was logged. The status code is still 2xx because the storage change is committed, but this replica keeps serving the previous view until a later notification or reconcile pass rebuilds it.

`build_dynamic_view` collects the storage revision hash before it reads upstream and principal contents. If a mutation commits while a build is reading contents, for example a write on another replica during a local `Reconciler` or `NotifyListener` build, the installed view can hold pre-mutation contents, but it then also holds the pre-mutation hash, so the hash no longer masks the drift. A later reconcile pass whose storage reads succeed detects the changed hash and rebuilds, even when no notification arrives; a pass whose hash read or build fails leaves the view as it is. On Postgres, the mutation's `NOTIFY` usually triggers an earlier `NotifyListener` rebuild after its 250 ms debounce; that debounce is not a completion bound.

Generation numbers are counted per process, so they cannot be compared across replicas.

**Peer replicas: async.** Neither path is backed by a persistent queue.

- Postgres: upstream, principal, key, plugin registry, and plugin chain writes call `pg_notify` inside the same transaction, so the notification is sent on commit, and a failed `pg_notify` rolls the write back. Each replica's `NotifyListener` waits 250 ms to batch pending events, then rebuilds and installs the result under the same rebuild lock and compare-and-swap. If the database listener connection drops, the notifier reconnects with exponential backoff that starts at 50 ms, doubles, and is capped at 5 s. The delay is not reset after a successful reconnect, so repeated drops in one process keep using the larger delay. Notifications sent while it was disconnected are lost, but after every successful `LISTEN` the notifier publishes a synthetic principal change event, which makes the `NotifyListener` rebuild the view from storage. If that rebuild is installed, it includes upstream, principal, key, and plugin changes committed during the gap. If the in-process broadcast lags, the listener also rebuilds from storage. The only fixed 1 s delay is a single retry when the `NotifyListener` first subscribes to the notifier's in-process channel; if that retry also fails, the listener stops and only the reconcile loop remains.
- Both backends: every replica runs a `Reconciler`. Its interval is 60 s plus a random 0-10 s offset, and each tick waits another random 0-10 s before running. On each pass it hashes storage revisions. When the hash differs from the hash stored in the resident view, it takes the rebuild lock, rereads the resident view and the hash, and rebuilds if they still differ. This is the fallback for missed notifications. The hash covers only upstream revisions, API-key ciphertexts, OAuth token generations, whether an OAuth upstream's stored apply error requires an operator reconnect, principal revisions, plugin registry entries, and plugin chain revisions. A runtime refresh failure that requires a reconnect is a status write that bumps no revision and sends no `NOTIFY`, so a view picks it up on its next rebuild, which the changed hash makes the next successful reconcile pass perform when nothing else rebuilds first. An OAuth reconnect or refresh also bumps no spec revision, but it always advances the token generation, so a reconnect that clears the marker cannot return the hash to a value sampled before the failure. Because every view's hash is collected before its contents, a view built from older versions of those inputs never carries a newer hash, so a later successful pass detects that drift. A failed pass is logged and leaves the view unchanged, and the interval is a schedule, not a repair deadline. It is the only cross-process path for SQLite, whose change notifier never publishes across processes.

## Configuration draft and save

Code: `crates/cc-lb-admin/src/settings.rs`, `crates/cc-lb-admin/src/routes.rs`, `crates/cc-lb-server/src/signal.rs`.

- `PUT /admin/v1/config/draft` checks `expected_revision`, stores the draft in the database, and returns the new revision. A stale revision returns 409. The running configuration does not change.
- `POST /admin/v1/config/draft/validate` stores the validation report for that revision. An invalid draft still returns 200, with the report marked invalid.
- `POST /admin/v1/config/save` requires the revision to be validated and the on-disk fingerprint to match, then rewrites the file with `atomic_replace`. It then clears the draft and appends a history entry. If clearing the draft or writing history fails, it logs the error and still reports success, because the file is the source of truth.

**No runtime convergence.** cc-lb reads its configuration file only at startup. A saved file applies only after the process restarts. `SIGHUP` reloads only the TLS certificate and key, and only when `listener.tls.reload_on_sighup` is enabled. Upstreams, principals, keys, and plugin chains live in the database and follow [Dynamic routing view](#dynamic-routing-view) instead.

## Scheduler jobs

Code: `crates/cc-lb-scheduler/src/worker/backend_api.rs`, `crates/cc-lb-scheduler/src/worker/dispatch.rs`, `crates/cc-lb-scheduler/src/jobs/apalis_housekeeping.rs`, `docs/scheduler.md`.

**Async guaranteed.** `push_adaptive_task` and `push_cron_task` insert a `Pending` job row directly and await the insert. Admin handlers that enqueue jobs await this insert before responding:

- `PUT`/`PATCH /admin/v1/upstreams/{id}` when the update turns warmup on for an OAuth upstream with credentials.
- `POST /admin/v1/upstreams/{id}/oauth/complete`, which enqueues the refresh and warmup jobs.
- `POST /admin/v1/upstreams/from-oauth-draft`, which enqueues the refresh bootstrap job (skipped for `never_refresh` credentials) and, when warmup is on, the warmup bootstrap job.

A duplicate idempotency key returns `Conflict`, which handlers treat as already queued. Any other enqueue error returns an error response, but the domain write is already committed. `from-oauth-draft` is the exception: it attempts to delete the new upstream. After a 2xx, the job row exists (or an earlier row with the same idempotency key does). The row is not guaranteed to still be `Pending` when the client sees the response. Bootstrap jobs are enqueued with a run time of now, and the workers run concurrently with the request, so a worker can claim or even finish the job first.

Execution happens later on whichever replica's worker claims the row:

- Each job has a 60 s execution timeout (`ENTITY_TIMEOUT`, `SINGLETON_TIMEOUT`).
- Retries follow the retry classes in `docs/scheduler.md`.
- `ApalisHousekeepingJob` (hourly by default) moves rows stuck in `Running` past `APALIS_STALE_LOCK_THRESHOLD_SECS` (120 s) back to `Pending` and increments `attempts`, so a job lost to a worker crash runs again after the next housekeeping pass. Stale cache keepalive rows are marked `Killed` instead.

Cron ticks are produced by every replica. Duplicate rows for the same tick collide on the unique `(job_type, idempotency_key)` index, so only one job runs per tick.

**Async best-effort exception.** After a successful limit-reset claim, `trigger_usage_poll` enqueues an extra usage poll from a detached `tokio::spawn` and does not wait for it. If that enqueue fails, it logs a warning, and the regular 60 s `oauth_usage_poll` tick re-observes the upstream instead.

**Inline, not queued.** Fire-now warmup, subscription metadata refresh, and limit-reset claims call the provider during the request. Metadata refresh and limit-reset results are committed before the response. Fire-now is a hybrid: the provider call and the audit row are inline, but the warmup attempt row is written best-effort. `execute_warmup_attempt` logs a failed `insert_warmup_attempt` and returns the computed outcome anyway (`crates/cc-lb-engine/src/warmup_attempts.rs`), so a 2xx fire-now response does not prove the attempt row exists.

`GET /admin/scheduler/status`, `GET /admin/scheduler/failures`, and `GET /admin/v1/upstreams/{id}/warmup` read committed job rows directly.

## OAuth token refresh

Code: `crates/cc-lb-signer-anthropic-oauth/src/lib.rs`, `crates/cc-lb-server/src/refresh.rs`, `crates/cc-lb-storage-postgres/src/adapter/upstreams.rs`.

**Lazy refresh on the proxy path: strict sync for the token.** When signing fails with `ExpiredToken`, `LazyRefreshSigner` takes a per-upstream in-process lock, so one refresh runs per upstream on each replica. It then calls `refresh_one`:

- Credentials marked `never_refresh` (long-lived tokens) fail immediately and are never sent to the token endpoint.
- Otherwise `ApalisLazyRefreshClaimGuard` enqueues an `OAuthRefresh` job keyed by upstream and token expiry. The idempotency key makes concurrent replicas share one job. The caller then polls `read_oauth_token_generation` once a second for up to 60 s, until the generation advances or the job reaches a terminal state. When it sees a terminal state, it re-reads the generation before reporting failure.
- If the wait times out or fails, the caller falls back to an in-process refresh.

The refresh flow commits the new encrypted token in one transaction through `complete_refresh` or `complete_split_refresh`. That transaction increments `oauth_token_generation`, clears `last_apply_error`, and (on Postgres) sends `NOTIFY`. Only then does the signer rebuild from storage and sign the request.

**Audit is best-effort.** The refresh flow's audit append (`emit_audit`) only logs a warning if it fails. Neither the token nor the proxied request is affected.

The admin routes that may refresh a token inline (subscription metadata refresh, limit-reset claim) keep the ordering described in [Admin mutations and audit records](#admin-mutations-and-audit-records). An audit failure after a committed refresh returns 500. `GET /admin/v1/upstreams/{id}/oauth/status` reads storage directly.

## Price catalog

Code: `crates/cc-lb-server/src/app.rs`, `crates/cc-lb-pricing/src/loader.rs`, `crates/cc-lb-scheduler/src/jobs/price_catalog.rs`.

- **Fetch: async guaranteed.** `PriceCatalogRefreshJob` runs on the scheduler every hour by default. It fetches the catalog with a 10 s timeout, skips the write when the fingerprint is unchanged, and otherwise writes the snapshot to storage and the disk cache before upserting the fingerprint. Each failed step returns `Retry` with a 30 s delay. The fetch job does not update the in-memory catalog on any replica.
- **Install: async best-effort, per replica.** At startup, `install_default_fallback_if_uninitialized` installs the built-in snapshot if nothing is loaded yet. A background task then runs `install_latest_local` immediately and every 60 s (`PRICE_CATALOG_LOCAL_INSTALL_INTERVAL`). It installs the stored snapshot when its hash changed, falling back to the disk cache. Failures are logged, and the current catalog stays in place. Within one replica, a new catalog takes effect within about 60 s of the fetch job committing it.
- **Effect on requests.** If a principal has a USD cost limit and the model is not in the catalog, the request is rejected with 503 (`cost limits unavailable for model`). If a request finishes while its model is not in the catalog, its cost stays empty. Rows are not re-priced later.

There is no admin route that triggers a catalog refresh.

## Request events and dashboards

Code: `crates/cc-lb-control/src/event_bus.rs`, `crates/cc-lb-engine/src/lifecycle_event_assembler.rs`, `crates/cc-lb-engine/src/request_diagnostics_sink.rs`, `crates/cc-lb-admin/src/events_routes.rs`, `crates/cc-lb-admin/src/dashboard_routes.rs`, `crates/cc-lb-admin/src/response_cache.rs`.

**Request rows: async best-effort.** The proxy response does not wait for the request event row:

- Lifecycle events go to the assembler over a bounded channel (capacity 4096). When the channel is full, events that carry row data are moved to detached overflow tasks that wait for space, up to 1024 such tasks. Beyond that limit, and for any other event type, the event is dropped and counted as `lifecycle_assembler_full`.
- After `RequestTerminated`, the assembler holds the row for up to 200 ms (`FINALIZATION_GRACE`) for late pricing and cache events, then inserts it.
- A failed insert is retried twice, after 50 ms and 200 ms. After three consecutive failures a circuit breaker opens and later rows get one attempt each. Rows still failing are dropped, logged, and counted as `lifecycle_assembler_storage_error`.
- On shutdown, the assembler drains the channel and any overflow tasks for up to 2 s, then flushes held rows.

Once the insert commits, the assembler publishes `RequestEventUpdate::Final` on the bus.

**Diagnostics line: async, after the insert.** With `observability.request_diagnostics_log` on, the assembler queues one `cc_lb.request_diagnostics.v1` stdout line after the insert commits and before it publishes `RequestEventUpdate::Final`. A dedicated thread writes queued lines; the queue holds 1024 lines. A failed insert produces no line.

- With `request_event_strip_diagnostics` off, the queue offer never waits. A full queue drops the line and counts `request_diagnostics_queue_full`; the row still holds the diagnostic fields, so `cc-lb request-events export-diagnostics` can restore it.
- With `request_event_strip_diagnostics` on, the row is stored without the five diagnostic fields, and the assembler waits up to 500 ms for queue space, which delays the `Final` publish by the same amount. On timeout the line is dropped as `request_diagnostics_strip_send_timeout`, and later offers stop waiting until the queue is at least half empty (`request_diagnostics_strip_queue_full`). These drops, and lines still queued at SIGKILL or OOM, are lost for good.
- Graceful shutdown drains the queue for up to 2 s. If stdout is still stalled at the deadline, shutdown proceeds without joining the writer thread and without logging (logs share the stalled stdout). Every queued line the writer has not started is counted as `request_diagnostics_shutdown_timeout`; the line being written records its own outcome if the write returns before exit, so no line is counted twice.
- The first line per `event_id` matches the stored row. A late terminal event for an already persisted orphan row can produce a second line that the database ignored; consumers keep the first.
- Cache-keepalive renewal rows are inserted by the renewal finalizer, and the assembler emits their line when their lifecycle events arrive. A renewal row whose lifecycle events were dropped (`lifecycle_assembler_full`) or that is finalized after the assembler has stopped has a stored row but no line. Its diagnostic fields are always empty, so `export-diagnostics` restores the count and nothing diagnostic is lost.

**Event reads.** `GET /admin/v1/events/recent` and `GET /admin/v1/events/delta` read committed rows only. They never include rows still held by the assembler. `GET /admin/v1/events/stream` first backfills committed rows after the client's `Last-Event-ID`, querying at most 500 (`BACKFILL_MAX_EVENTS`). The stream's principal, upstream, and model filters are applied to those rows before they are returned. If the backfill returns 500 rows, the stream sends `reset` and ends. With filters active, a client 500 or more rows behind can get fewer than 500 matching rows back and no reset, and rows past the first 500 of the gap are then not replayed; do not infer a reset from the cursor gap alone. After the backfill it streams live bus updates and, on Postgres, updates from `StorageTailPoller`, which polls storage every `event_bus.storage_tail_poll_interval_ms`. A lagging subscriber receives `reset` and the stream closes.

**Dashboards: materialized.** `GET /admin/v1/dashboard/summary` reads only usage rollups. These are updated by the `usage_rollup` job, which runs every 30 s with 3 s jitter by default. On Postgres the rollup only takes events whose transactions are below the current snapshot `xmin`, so a long-open transaction elsewhere delays it.

`GET /admin/v1/dashboard/usage` reads the same rollups. For principal totals it also reads per-principal costs from the request event rows, and caches that result per process for 2 s (`PRINCIPAL_TOTALS_CACHE_TTL`).

## Subscription quota observations

Code: `crates/cc-lb-control/src/event_bus.rs`, `crates/cc-lb-engine/src/lifecycle_subscription_quota_subscriber.rs`, `crates/cc-lb-engine/src/subscription_quota_events.rs`, `crates/cc-lb-storage-postgres/src/adapter/upstream_subscription_quota.rs`, `crates/cc-lb-server/src/notify_listener.rs`, `crates/cc-lb-admin/src/subscription_quotas.rs`.

**Async best-effort, in three stages.** The proxy path does not update the quota cache itself. The event bus hands lifecycle events to a separate subscriber task over a bounded channel (capacity 4096) with a non-blocking send. When that channel is full, the event is dropped and counted as `lifecycle_subscription_quota_full`, and its observation never reaches the cache or storage. The proxy response can finish before the subscriber has processed the events. When the subscriber handles an `UpstreamResponseStarted` event for a 2xx or 429 response, it does three things:

1. **Serving replica cache.** It upserts the observation into the in-process `subscription_quota_cache`. This happens on the subscriber task, independently of the proxy response, so it may land before or after the client receives the response. It is not guaranteed at response time.
2. **Storage, async best-effort.** It enqueues the observation on the writer's bounded channel (`subscription_quota.writer_channel_capacity`, 4096 by default). When that channel is full, the observation is dropped from storage and counted as `subscription_quota_full`. The writer flushes batches of up to 256 records, or every 100 ms. A failed write is logged and the cache keeps the observation.
3. **Peer replicas.** On Postgres, after a batch commits, the writer sends one `NOTIFY` per affected upstream, once, outside the transaction. Peers debounce for 250 ms and then reload those upstreams' latest quota rows from storage. A failed `NOTIFY` is logged and not retried; the committed rows survive, and peers pick them up on their next successful hydrate. Every dynamic view build hydrates the cache for all upstreams: at startup, on a rebind notification for upstreams, principals, keys, or plugins, on the synthetic event after a `LISTEN` reconnect, when the in-process broadcast lags, or on a reconcile pass that finds a changed revision hash. These hydrates replace a cached observation with a stored one whose timestamp is the same or newer. Independently of any build, every `Reconciler` pass also attempts to hydrate the cache from storage for the upstreams in the resident view before it compares revision hashes (`crates/cc-lb-server/src/reconcile.rs`). That periodic hydrate captures a per-slot version for each cached `(upstream, window, source)` observation before the storage read; every replacement of a slot, from any path, bumps its version. A stored observation then replaces the cached one when its timestamp is strictly newer, or when it is equal and the slot's version is unchanged since the capture. An equal-timestamp observation that another replica persisted while its `NOTIFY` was lost is therefore recovered, while a read that raced a local or notified observation cannot restore older content. Empty slots always hydrate. It does not depend on the revision hash, which does not cover quota samples, so committed observations missed by a lost `NOTIFY` reach a peer on its next successful periodic read. The reconcile schedule (60 s plus 0-10 s, then another 0-10 s per pass) is not a recovery deadline, because storage availability and read duration also apply. On SQLite there is no cross-process quota notification; another process's committed rows reach the cache through that periodic read or a dynamic view build. A failed hydrate is logged, the cache keeps its previous values, and the rest of the reconcile pass still runs.

| Read | Source | Shows a new observation |
|---|---|---|
| `GET /admin/v1/subscription-quotas/latest` | In-process cache | On the serving replica, after the subscriber processes the event; never if the event was dropped |
| `GET /admin/v1/subscription-quotas/series` | Storage | After the batch flush |
| `GET /admin/v1/subscription-quotas/aggregate` | Cache for the latest values, storage for history | Latest after the subscriber processes the event; history after the flush |

## Writing tests against this contract

Assert right after a 2xx admin response:

- Domain state through the storage API or a follow-up `GET`, and the audit row through `query_recent_audit`. Both are committed before the response. For example, `crates/cc-lb-admin/tests/v1_principals.rs` checks the audit row right after the response.
- The new `ETag` revision, which can go straight into the next `If-Match`, or into the body `expected_revision` for the routes listed under optimistic concurrency.
- For "Rebuilt" routes, under the normal rebuild protocol, `x-cc-lb-generation` names the generation this route's rebuild installed on the serving replica, built after the mutation committed (see [Dynamic routing view](#dynamic-routing-view)). The view or proxy traffic can then be asserted right after the response, also in a full server where the `NotifyListener` and `Reconciler` run, provided no later conflicting mutation can run in the test. A later rebuild may already have installed a newer generation, so compare the resident generation with `>=` unless the admin route is the only source of rebuilds. `crates/cc-lb-admin/tests/v1_synchronous_rebind.rs` builds `AdminState` with a test `SnapshotRebinder` and no `NotifyListener` or `Reconciler`, sends a single mutation, then checks that the resident generation equals the header and that the view contains the new upstream. `crates/cc-lb-server/tests/dynamic_view_rebuild_serialization.rs` holds another rebuild in progress while the admin request runs and checks the view when the response returns. When the response carries `x-cc-lb-rebind-status: deferred`, do not assert the view.
- Job rows enqueued by the handler: assert that a row with the expected idempotency key exists, not its status. For example, after warmup is turned on, the warmup bootstrap row exists, but a worker may already have claimed or finished it. Assert `Pending` only when the test controls the scheduler worker, for example when no worker is running.

Do not assert right after a response. Wait on the named seam instead:

- **View convergence on peer replicas.** Install a test-scoped tracing layer like `RebindCompletionLayer` in `crates/cc-lb-server/tests/managed_key_multi_instance.rs`, which fires when a `NotifyListener` rebuild finishes, and after each signal probe with proxy traffic until the expected status appears (`wait_for_proxy_status` in the same file). This seam needs the Postgres notifier, because SQLite publishes no change events. Bound the wait with a deadline. To test the reconcile path without a notifier, call `Reconciler::reconcile_once` directly and assert after it returns, as `crates/cc-lb-server/tests/dynamic_view_rebuild_serialization.rs` does.
- **Subscription quota cache.** At the engine level, drive the subscriber directly as the unit tests in `crates/cc-lb-engine/src/lifecycle_subscription_quota_subscriber.rs` do: feed events into `spawn_lifecycle_subscription_quota_subscriber` through a test channel with a recording `SubscriptionQuotaCacheLike` and a `SubscriptionQuotaSink`, close the sender, await `shutdown()` on the handle so queued events drain, then assert the recorded cache entries and the sink receiver. For a peer's view of committed quota rows, write the rows to storage, call `Reconciler::reconcile_once`, and assert the cache after it returns, as `crates/cc-lb-server/tests/reconcile_subscription_quota_hydration.rs` does. A server-level test that asserts `GET /admin/v1/subscription-quotas/latest` on the serving replica after a proxied request still needs a new test-scoped observer that signals when the subscriber has applied the observation.
- **Scheduler job execution.** Inject a dispatch closure that reports each job over a channel, like `dispatching_scheduler_ctx` in `crates/cc-lb-scheduler/tests/worker.rs`, and wait on that channel with a deadline. Do not assert the extra usage poll after a limit-reset claim.
- **Lazy OAuth refresh.** Use `read_oauth_token_generation` and the controllable fixtures in the test module of `crates/cc-lb-server/src/refresh.rs` (`TestOAuthRefreshClaims::wait_for_contention`, `LazyRefreshFixture::hold_refresh_response`, `wait_for_refresh_start`).
- **Price catalog.** Seed it with the test helper and wait with `wait_for_price_catalog` in `tests/integration/managed_api_key_full_flow.rs`.
- **Request event rows.** Subscribe to the bus and wait for `RequestEventUpdate::Final` for the specific request (`crates/cc-lb-engine/tests/lifecycle_request_events.rs`), or install a test-scoped observer. In server-level tests where the exact number of expected rows is known, `wait_for_row_count` in `crates/cc-lb-server/tests/rfc_0002_fix_live_qa.rs` polls storage until the count equals that number and panics after a 10 s deadline. Do not use `settled_row_count` from the same file as a completion signal: it returns once the count stops changing for a few 300 ms reads, or after 15 s, which is a timing heuristic, not proof that a row was written.
- **Dashboards.** Call `rollup_usage_once()` on the storage directly after seeding events (`crates/cc-lb-admin/tests/dashboard_summary.rs`, `crates/cc-lb-admin/tests/dashboard_usage.rs`). On Postgres, call `wait_for_events_visible_for_rollup` first (`crates/cc-lb-storage-conformance/tests/storage_roundtrips_postgres.rs`).
- **Stream tailing on Postgres.** Wait on the `StorageTailPoller` broadcast receiver, as in `crates/cc-lb-engine/tests/storage_tail_poller.rs`.

The `AGENTS.md` CI flake rules apply. Never add `sleep` or retry loops to hide timing, and never add a public `flush`, completion, or notification method to a production type just so a test can wait. If none of the seams above fits, write a test-scoped observer or wrapper.
