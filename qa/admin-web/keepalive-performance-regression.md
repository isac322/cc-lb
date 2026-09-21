# Cache Keepalive Performance Regression/Equivalence QA Contract

<!-- Verification results for historical candidate 9c420 and the origin/master 4ed7cc8c latest-base integration are recorded separately. The PR has not been created yet and CI is pending. -->

- Recorded: 2026-09-15 UTC
- Status: **latest-base `4ed7cc8c` QA 57/57 PASS / browser 92/92 PASS / Rust 724 PASS / Admin Web 692 PASS**
- Scope: backend-only read optimization of the Principal Cache Keepalive summary, list, and detail; preservation of existing Admin Web behavior; fixing the first-page time range of the 24h/7d cursor
- Approved bug fix: fixes the existing behavior where the next page of 24h/7d computed a new `horizon_start_ms` as time elapsed and returned HTTP 400. The new cursor validates request scope with a `horizon` tag and reuses the first-page cutoff.
- Excluded: frontend product code, 5-second polling, AbortSignal propagation, retention policy, restatement, async rollup, cache, freshness policy changes
- Verification interpretation: each row shows `PASS@9c420` historical evidence and `PASS@4ed7cc8c` latest-base evidence together. QA on the latest-base integration is 57 PASS, 0 FAIL, 0 BLOCKED, 0 NOT_RUN. The PR has not been created yet and PR CI is pending before execution. No production deployment was performed.

## 1. Fixed implementation ownership and contract

| Layer | Implementation-owning files/symbols | Verification-owning files |
|---|---|---|
| Storage public API | `crates/cc-lb-storage-api/src/cache_keepalive_sessions/reads.rs`: `CacheKeepaliveSummaryInput { sessions: Vec<CacheKeepaliveSessionListItem>, recent_decisions: u64 }`, required `CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input(&self, principal_id: &str, cutoff_ms: u64)`, required `CacheKeepaliveSessionReadStore::get_cache_keepalive_list_item(&self, principal_id: &str, id: &str)` | Cursor/type unit verification in the same file and both-adapter integration tests. No default full-scan fallback on the new methods. |
| PostgreSQL | `crates/cc-lb-storage-postgres/src/adapter/cache_keepalive_session_reads.rs`: summary input, direct candidate lookup, bounded list query | `crates/cc-lb-storage-postgres/tests/cache_keepalive_session_reads.rs`, integration target `crates/cc-lb-storage-postgres/tests/all.rs` |
| SQLite | `crates/cc-lb-storage-sqlite/src/adapter/cache_keepalive_session_reads.rs`: same contract as PostgreSQL | `crates/cc-lb-storage-sqlite/tests/cache_keepalive_session_reads.rs`, integration target `crates/cc-lb-storage-sqlite/tests/all.rs` |
| Admin route/view | `crates/cc-lb-admin/src/v1/principal_cache_keepalive.rs`: `list_cache_keepalive`, `get_cache_keepalive_detail`; `.../query.rs`: tagged cursor decode/encode and first-page cutoff reuse; `.../view.rs`: summary fold, activity batch, row/detail conversion | `crates/cc-lb-admin/tests/cache_keepalive_contracts.rs`, `.../fixtures.rs`, `crates/cc-lb-admin/src/cache_keepalive_view_tests.rs`, `cache_keepalive_view_activity_tests.rs`, `cache_keepalive_view_status_tests.rs` |
| Admin Web, no product changes | `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts`, `queries.ts`, `usePolledData.ts`, `components/principals/cache-keepalive/*` | Existing component tests and the opt-in-only `crates/cc-lb-admin/web/qa/keepalive-performance-regression.spec.ts`; product code is not changed, and the cursor fix is also verified through real UI pagination. |
| Conditional planner index | Latest-base integration numbers: PostgreSQL `crates/cc-lb-storage-postgres/migrations/0118_*.sql`; SQLite `crates/cc-lb-storage-sqlite/migrations/0086_*.sql` | Added only when raw `EXPLAIN (ANALYZE, BUFFERS)` and SQLite `EXPLAIN QUERY PLAN` prove necessity. Only expression indexes matching `COALESCE(last_message_at_ms, ts * 1000)` and the actual computed order key are allowed. Raw-data backfill, deletion, and column-semantics changes are forbidden. The default collation is not forcibly changed. |

Fixed behavior:

1. Summary sessions are read in the existing global order `last_message_at_ms DESC`, `session_key_hash ASC`, and the existing Rust fold, per-turn integer micro-dollar truncation, pending, unknown pricing, and saturation are reused unchanged. `recent_decisions` is a `u64` count using the same 5-minute inclusive boundary (`>= cutoff_ms`), late-turn anti-join, and `COALESCE`.
2. Detail looks up two candidates — session and visible decision — by principal+bare ID. The candidate with the larger timestamp wins; ties follow the DB default collation ordering of the computed `entry_id`. In the current ASCII namespace, decision comes before session. A decision with a late turn is not a candidate.
3. List preserves default limit 50, maximum 100, exact cursor seek, nullable timestamp fallback, namespace order, unknown-price/nullability, error/reason, and rounding. The new HTTP cursor flattens `horizon: "24h"|"7d"|"all"` into the existing 5 storage cursor fields. A tagged cursor validates principal/horizon/filter and the bounded/all cutoff shape, then reuses the first-page `horizon_start_ms`. An untagged legacy cursor passes existing validation only when it is exact for the current cutoff. Terminal pagination ends at wire `next_cursor == null`, not at an arbitrary 20-page cap or a DOM disabled state.
4. Summary/list/detail were not bound into a single snapshot before either. Normal fixed-fixture results must be exact, and concurrent transitions must converge by the next 5-second poll. Only the list cutoff of the cursor chain is fixed to the first page; each request's summary is recomputed with that request's current `now`. A short read transaction inside one request may be used if the implementation requires it, but the product freshness policy is not strengthened.
5. The side effect where an unrelated past corrupt decision that the narrower query does not read caused the entire request to fail is not a preservation target. Conversely, conversion errors on the requested row, sessions/turns included in the summary, and current page rows keep the existing mapping.
6. `StorageError::InvalidInput { field: "cache_keepalive_pnl", ... }` produced by `dollars_from_micros` is converted by `principal_cache_keepalive::storage_error` to HTTP **400** `invalid_input`. `StorageError::Corrupted` is HTTP **500** `storage_error`.

## 2. Execution environment and evidence format

### 2.1 Repository/Rust commands

In this workspace each crate has `autotests = false` and is consolidated into `tests/all.rs`. Therefore arbitrary `--test cache_keepalive_session_reads` examples are not used. First run the whole actual module to confirm the test target and module path are valid.

```bash
# SQLite storage module
cargo test -p cc-lb-storage-sqlite --test integration \
  'cache_keepalive_session_reads' -- --nocapture

# PostgreSQL storage module. Local-only isolated DB example.
SQLX_OFFLINE=true \
PG_URL='postgres://cclb@127.0.0.1:55439/keepalive_qa' \
cargo test -p cc-lb-storage-postgres --test integration \
  'cache_keepalive_session_reads' -- --nocapture

# Admin HTTP equivalence and cursor modules
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::read_equivalence' -- --nocapture
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::cursor_window' -- --nocapture

# Admin view/economics unit module. The source filename is not the Rust module name.
cargo test -p cc-lb-admin --lib 'cache_keepalive_view::tests' -- --nocapture
```

Single-test reproduction narrows to the full path below only after the whole-module run.

```bash
cargo test -p cc-lb-storage-sqlite --test integration \
  'cache_keepalive_session_reads::<TEST>' -- --exact --nocapture
SQLX_OFFLINE=true PG_URL='postgres://cclb@127.0.0.1:55439/keepalive_qa' \
cargo test -p cc-lb-storage-postgres --test integration \
  'cache_keepalive_session_reads::<TEST>' -- --exact --nocapture
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::read_equivalence::<TEST>' -- --exact --nocapture
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::cursor_window::<TEST>' -- --exact --nocapture
cargo test -p cc-lb-admin --lib \
  'cache_keepalive_view::tests::<TEST>' -- --exact --nocapture
```

Cargo can exit 0 even when the filter selects no test. Therefore reject evidence if the selected test binary prints `running 0 tests` or the final result is `0 passed`. In that case, re-run the whole-module command and copy the printed full test path verbatim into the `--exact` command.

The DSN names the PostgreSQL test launcher accepts are `CI_POSTGRES_URL` or `PG_URL`. A fully CI-compatible environment sets `CI_POSTGRES_URL`, `PG_URL`, `DATABASE_URL`, and `DATABASE_URL_TEST` to the same isolated DSN and sets `SQLX_OFFLINE=true`.

### 2.2 Real compiled server + browser + safe fake upstream

- baseline binary: `$KEEPALIVE_SCRATCH_DIR/bin/cc-lb-baseline`
- The candidate binary is built with the same host toolchain and build profile and placed at `$KEEPALIVE_SCRATCH_DIR/bin/cc-lb-candidate`.
- Real CLI: `cc-lb serve --config <PATH> --data-dir <PATH>`
- The fake upstream binds to loopback only: `cargo run -p fake-anthropic -- --port 19080`
- PostgreSQL uses only an isolated container published on loopback and QA-named databases. The example uses container `cc-lb-keepalive-qa-postgres`, databases `keepalive_baseline`/`keepalive_candidate`, user `cclb`.

After the candidate implementation, Main starts the server once against each isolated DB to apply the current migrations, then stops it. Then the project-owned runner's `prepare` seeds the deterministic fixture, and Main directly owns the lifecycle of the baseline/candidate servers and Vite. The runner does not start or stop daemons. An older runner copy left in scratch outside the repository is only an archive of a past run; subsequent runs use only `crates/cc-lb-admin/web/qa/keepalive-qa-runner.py`. Fixture semantics come from `crates/cc-lb-admin/tests/cache_keepalive_contracts/fixtures.rs`, HTTP server/auth patterns from `crates/cc-lb-admin/tests/admin_test_common.rs`, the browser consumption contract from `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts` and `queries.ts`, and upstream behavior from `tests/fixtures/fake-anthropic/src/main.rs` as sources of truth.

Every `prepare` run receives the same `FIXTURE_ANCHOR_MS`. This value is the display-timestamp basis for fixture rows and does not fix the server's current time. The time anchor of each live request is recorded separately in the evidence's `started_at_utc`. A session's `last_message_at_ms`, `created_at`, `first_scheduled_at`, and `cache_anchor_at` preserve existing past display times, but the lifecycle is `updated_at = anchor`, `expires_at = anchor + 7 days` identically on all DBs. Active rows have `enqueue_state='enqueued'` and `run_at` in the future, so normal housekeeping and the scheduler do not delete verification rows or start jobs. Cleanup verification does not disable app housekeeping; it is performed explicitly via the runner's fixture-ID-scoped `evidence --mutation cleanup` and the existing storage test. SQLite DBs are limited to already-migrated files under `$KEEPALIVE_SCRATCH_DIR`. PostgreSQL seeds only the QA-named DB inside the container via `docker exec ... psql`. The default fixture creates 1,201 recent entries for the UI, 120 past entries that differentiate 24h/7d/all, a separate D=100,000-decision principal, and a separate S/T principal. Therefore UI-07 exceeds 20 pages but does not DOM-scroll all of D=100,000.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
export KEEPALIVE_SCRATCH_DIR="${KEEPALIVE_SCRATCH_DIR:-$REPO_ROOT/target/keepalive-qa}"
RUNNER="$REPO_ROOT/crates/cc-lb-admin/web/qa/keepalive-qa-runner.py"
MANIFEST="$KEEPALIVE_SCRATCH_DIR/fixture-manifest.json"
TOKEN_FILE="$KEEPALIVE_SCRATCH_DIR/admin-token"
FIXTURE_ANCHOR_MS="$(python3 -c 'import time; print(int(time.time() * 1000))')"

python3 "$RUNNER" prepare --engine sqlite --phase baseline \
  --sqlite-db "$KEEPALIVE_SCRATCH_DIR/baseline-sqlite/cc-lb.sqlite" \
  --server-url http://127.0.0.1:54382 --anchor-ms "$FIXTURE_ANCHOR_MS" \
  --manifest "$MANIFEST"
python3 "$RUNNER" prepare --engine sqlite --phase candidate \
  --sqlite-db "$KEEPALIVE_SCRATCH_DIR/candidate-sqlite/cc-lb.sqlite" \
  --server-url http://127.0.0.1:54402 --anchor-ms "$FIXTURE_ANCHOR_MS" \
  --manifest "$MANIFEST"
python3 "$RUNNER" prepare --engine postgres --phase baseline \
  --pg-container cc-lb-keepalive-qa-postgres --pg-database keepalive_baseline \
  --server-url http://127.0.0.1:54392 --anchor-ms "$FIXTURE_ANCHOR_MS" \
  --manifest "$MANIFEST"
python3 "$RUNNER" prepare --engine postgres --phase candidate \
  --pg-container cc-lb-keepalive-qa-postgres --pg-database keepalive_candidate \
  --server-url http://127.0.0.1:54412 --anchor-ms "$FIXTURE_ANCHOR_MS" \
  --manifest "$MANIFEST"

python3 "$RUNNER" compare --engine sqlite \
  --manifest "$MANIFEST" \
  --baseline-url http://127.0.0.1:54382 --candidate-url http://127.0.0.1:54402 \
  --token-file "$TOKEN_FILE" --case all \
  --records "$KEEPALIVE_SCRATCH_DIR/evidence/api-sqlite.jsonl" \
  --summary "$KEEPALIVE_SCRATCH_DIR/evidence/compare-sqlite.json"
python3 "$RUNNER" compare --engine postgres \
  --manifest "$MANIFEST" \
  --baseline-url http://127.0.0.1:54392 --candidate-url http://127.0.0.1:54412 \
  --token-file "$TOKEN_FILE" --case all \
  --records "$KEEPALIVE_SCRATCH_DIR/evidence/api-postgres.jsonl" \
  --summary "$KEEPALIVE_SCRATCH_DIR/evidence/compare-postgres.json"
# Complete both compares above before any browser mutation.
# Immediately before each browser phase, Main stops only that phase's server,
# reseeds with the same FIXTURE_ANCHOR_MS by adding --replace-fixture to the
# matching prepare command above, restarts the server, and confirms
# /admin/health 200.

cd "$REPO_ROOT/crates/cc-lb-admin/web"
KEEPALIVE_SCRATCH_DIR="$KEEPALIVE_SCRATCH_DIR" \
KEEPALIVE_PHASE=baseline KEEPALIVE_ENGINE=sqlite \
KEEPALIVE_BACKEND_URL=http://127.0.0.1:54382 CC_LB_ADMIN_URL=http://127.0.0.1:54382 \
KEEPALIVE_ADMIN_TOKEN_FILE="$TOKEN_FILE" KEEPALIVE_MANIFEST="$MANIFEST" \
KEEPALIVE_EVIDENCE_DIR="$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-baseline" \
bunx playwright test qa/keepalive-performance-regression.spec.ts \
  --config qa/keepalive-e2e.config.ts
# Run the same command for candidate/sqlite (54402), baseline/postgres (54392),
# and candidate/postgres (54412) with their phase/engine/URL/evidence directory.

python3 "$RUNNER" evidence --engine sqlite --phase candidate \
  --manifest "$MANIFEST" --capture-plans \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/api-sqlite.jsonl" \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-candidate/browser-sqlite-candidate.jsonl" \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-candidate/mutations-sqlite-candidate.jsonl" \
  --output-dir "$KEEPALIVE_SCRATCH_DIR/evidence/plans/sqlite-candidate" \
  --report "$KEEPALIVE_SCRATCH_DIR/evidence/report-sqlite-candidate.json"
```

When re-running only the cursor bug fix, use the following exact scope, separate from the performance compare.

```bash
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::cursor_window::cur_01_24h_and_7d_page_chains_survive_clock_advance_with_frozen_start' \
  -- --exact --nocapture
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::cursor_window::cur_02_cursor_scope_principal_and_filter_mismatches_stay_bad_request' \
  -- --exact --nocapture
cargo test -p cc-lb-admin --test integration \
  'cache_keepalive_contracts::cursor_window::cur_03_cursor_anchor_remains_frozen_across_repeated_clock_advances' \
  -- --exact --nocapture
python3 "$RUNNER" compare --engine <sqlite|postgres> --manifest "$MANIFEST" \
  --baseline-url <BASELINE_URL> --candidate-url <CANDIDATE_URL> \
  --token-file "$TOKEN_FILE" --case cursor --records <JSONL> --summary <JSON>
KEEPALIVE_SCRATCH_DIR="$KEEPALIVE_SCRATCH_DIR" \
KEEPALIVE_PHASE=<baseline|candidate> KEEPALIVE_ENGINE=<sqlite|postgres> \
KEEPALIVE_BACKEND_URL=<URL> CC_LB_ADMIN_URL=<URL> \
KEEPALIVE_ADMIN_TOKEN_FILE="$TOKEN_FILE" KEEPALIVE_MANIFEST="$MANIFEST" \
KEEPALIVE_EVIDENCE_DIR="$KEEPALIVE_SCRATCH_DIR/evidence/<engine>-<phase>" \
bunx playwright test qa/keepalive-performance-regression.spec.ts \
  --config qa/keepalive-e2e.config.ts --grep 'CUR-0[1-3]'
```

Baseline's bounded-cursor page 2 HTTP 400 is original bug-reproduction evidence, not a PASS. Only the candidate must satisfy HTTP 200, a frozen cutoff, and the correct horizon tag on page 2 and on repeated time-advance chains. Normal page 2 of the `all` cursor and principal/horizon/filter mismatch 400s must pass on both baseline and candidate.

`qa/keepalive-e2e.config.ts` does not use the mock `globalSetup`/`globalTeardown` of the general E2E suite and selects only `qa/keepalive-performance-regression.spec.ts`. This `qa/` path is outside the default `playwright.config.ts` `testDir: ./e2e`, so it is not collected automatically without the explicit opt-in command. By default it starts real Vite and proxies to the compiled server at `CC_LB_ADMIN_URL`. If Main also manages Vite directly, pass `KEEPALIVE_EXTERNAL_WEB=1` and `KEEPALIVE_WEB_URL`. The config fails before starting if phase, engine, backend, manifest, or the per-phase evidence directory is omitted or empty. A browser pass cannot be produced with a `page.route` JSON mock or synthetic timing. `KEEPALIVE_EVIDENCE_DIR` is placed at `$KEEPALIVE_SCRATCH_DIR/evidence/<engine>-<phase>`, and generated Playwright output stays under the gitignored `target/keepalive-qa` by default. The admin token is read into memory only from a mode-0600 secret file or an explicitly named environment variable, and is never written to argv, stdout, the manifest, or evidence. Immediately after saving, success/failure traces go through the runner's `evidence --sanitize-trace` path, which replaces the raw Bearer token and re-verifies. Because the browser suite mutates the fixture, each DB is restored with `prepare --replace-fixture` immediately before its phase. To re-run `compare` after a browser run, both baseline and candidate must be re-prepared with the same anchor.

`evidence --sanitize-trace` succeeds only when it redacts the trace with a non-empty admin token and re-confirms that no token remains. If there is no token source it exits 2 with `trace redaction verification unavailable` `HarnessError`; if the explicitly named token source's value is an empty string it exits 2 with `trace redaction token must not be empty` `HarnessError`. In both cases it does not print the `sanitized trace` success message.

A probe that changes `document.visibilityState` and `visibilitychange` inside the page verifies the polling hook's DOM visibility branch. An OS-level hidden run — actually covering or minimizing the window or switching to another native tab/app — is a separate observation that also covers browser throttling and background lifecycle. The DOM probe is UI-11 evidence and does not replace PERF-06 native evidence. The historical candidate passed in `evidence-v5/perf06-native.json`. Latest-base passed in `pr-evidence/native-hidden.json` with `native_tab_switch`, 846,006ms total run, 600,002ms hidden steady, 0 new GETs after hidden on all four targets, 5/5 created tabs cleaned up, and `cleanup_failures=[]`.

PERF-06 native evidence is collected by the opt-in CLI `crates/cc-lb-admin/web/qa/keepalive-native-visibility.mjs`, separate from the default Playwright test. Main first reseeds the four datasets with the same fresh fixture anchor and confirms `/admin/health` is 200 on all four servers, then starts raw Chromium separately and owns the loopback CDP endpoint. The CLI does not start a browser; it connects with `chromium.connectOverCDP(endpoint, { noDefaults: true })` and uses only the default context of `browser.contexts()[0]`. The manifest's four `server_url` values — `sqlite:baseline`, `sqlite:candidate`, `postgres:baseline`, `postgres:candidate` — must be distinct loopback origins.

Run from the repository root in argument form.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
KEEPALIVE_SCRATCH_DIR="${KEEPALIVE_SCRATCH_DIR:-$REPO_ROOT/target/keepalive-qa}"
cd "$REPO_ROOT/crates/cc-lb-admin/web"
node qa/keepalive-native-visibility.mjs \
  --manifest "$KEEPALIVE_SCRATCH_DIR/fixture-manifest.json" \
  --scratch-dir "$KEEPALIVE_SCRATCH_DIR" \
  --token-file "$KEEPALIVE_SCRATCH_DIR/admin-token" \
  --cdp-endpoint http://127.0.0.1:59333
```

The same run can also be invoked in environment form.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
export KEEPALIVE_SCRATCH_DIR="${KEEPALIVE_SCRATCH_DIR:-$REPO_ROOT/target/keepalive-qa}"
export KEEPALIVE_MANIFEST="$KEEPALIVE_SCRATCH_DIR/fixture-manifest.json"
export KEEPALIVE_ADMIN_TOKEN_FILE="$KEEPALIVE_SCRATCH_DIR/admin-token"
export KEEPALIVE_CDP_ENDPOINT=http://127.0.0.1:59333
cd "$REPO_ROOT/crates/cc-lb-admin/web"
node qa/keepalive-native-visibility.mjs
```

If the actual manifest filename differs, change only the manifest value in both commands. `KEEPALIVE_NATIVE_VISIBLE_MS` and `KEEPALIVE_NATIVE_HIDDEN_MS` allow only 30,000ms and 600,000ms or more respectively, so the observation time cannot be shortened to pass. The CLI writes the mode-0600 token file value into each origin's local storage, opens the low principal, then opens detail through the same selectors as the existing spec: `cache-keepalive-card` → `Sessions` → `cache-keepalive-sessions-drawer` → `li[data-key]` row button. It observes each of the four tabs in the foreground for 30 seconds, brings a blank control tab to the front, confirms via Node timer and `page.evaluate` polling that all four tabs are actually `hidden`, then performs the 600-second hidden observation and a 30-second restore observation per tab. It closes only the five tabs the test created and does not close the Main-owned browser.

Each run, the CLI creates one new mode-0600 file `$KEEPALIVE_SCRATCH_DIR/evidence/keepalive-native-visibility-<run-id>.json`. This JSON contains `mechanism: "native_tab_switch"`, native visibility event times, the initial in-flight allowance record proven by request start, per-phase GET/status/count, redacted URLs, actual browser timing, before/after CDP `JSHeapUsedSize` samples, new restore response IDs, manual DOM ready/paint frames and four-client match, unchanged `Document.prototype.hidden`/`visibilityState` descriptor verification, and embedded raw JSONL records with unique record IDs. The script does not override `document.hidden`/`visibilityState` or dispatch a synthetic `visibilitychange`. If it cannot produce native hidden it records `FAIL`; if it cannot provide CDP Performance or actual request timing it records `BLOCKED` rather than substituting synthetic values, and exits nonzero. This `.mjs` is outside Playwright test discovery and does not run automatically without the explicit command.

### 2.3 Required fields of every real E2E record

Each API request and browser observation is an independent record. UI paint timing is not copied into API TTFB.

```json
{
  "case_id": "UI-00",
  "run_id": "independent-uuid",
  "phase": "baseline|candidate",
  "engine": "sqlite|postgres",
  "sample": "zero-based sample number or null for browser observations",
  "cache_state": "cold|warm|fixed_snapshot|browser_*|independent_direct|fixture_mutation",
  "runtime_profile": "actual build profile label",
  "started_at_utc": "RFC3339 with milliseconds",
  "ended_at_utc": "RFC3339 with milliseconds",
  "request": {
    "method": "GET",
    "exact_url_or_redacted_sha256": "exact loopback URL, or normalized URL SHA-256 when an identifier is redacted",
    "cursor_in_sha256": "null or SHA-256 of opaque cursor"
  },
  "response": {
    "http_status": 200,
    "canonical_body_sha256": "SHA-256 after deterministic JSON canonicalization",
    "body_bytes": 0,
    "cursor_out_sha256": "null or SHA-256 of next_cursor",
    "cursor_out_original_fields_sha256": "null or SHA-256 of decoded principal_id/horizon_start_ms/filter/last_message_at_ms/entry_id",
    "cursor_out_horizon": "null for baseline/terminal, otherwise 24h|7d|all for candidate",
    "ttfb_ms": 0,
    "wall_ms": 0
  },
  "ui": {
    "observed_at_utc": "independent timestamp",
    "first_paint_ms": 0,
    "stable_paint_ms": 0,
    "visible_row_ids_sha256": "SHA-256 of ordered IDs",
    "screenshot_path": "redacted artifact path"
  },
  "backend": {
    "sql_calls": 0,
    "sql_time_ms": 0,
    "pool_wait_ms": 0,
    "plan_artifact": "EXPLAIN artifact path or null"
  }
}
```

Required evidence means the raw record plus the named assertion output. A screenshot, a disabled DOM button, or a rounded timing summary alone is not evidence.

### 2.4 Current evidence anchors

Latest-base `4ed7cc8c` evidence:

- `RUST-724`: `artifact://704`, 724 passed, 7 pre-existing ignored. Includes keepalive storage, Admin integration, view/economics, and the mapper path; collected from the actual `tests/all.rs` integration target or lib module. Clippy `--all-targets -D warnings` also passed.
- `PR-WEB`: Admin Web 72 test files, 692 tests, build, lint, typecheck all passed. Browser raw records are `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`, 23 cases per dataset, 92/92 PASS total. The matrix is `pr-evidence/verified-browser-matrix.json`.
- `PR-API`: `pr-evidence/{performance,semantics}-{sqlite,postgres}.json` and `api-{performance,semantics}-{sqlite,postgres}.jsonl`. Unchanged responses are exact; only the baseline 400 → candidate 200 on 24h/7d time advance is the approved bug delta.
- `PLAN-v5`: historical `read-plans-reviewed/run-manifest.json` and 124 raw plans. The latest-base query source has the same semantics; only the migration numbers moved to PostgreSQL `0118`, SQLite `0086`. The source path, source SHA, and SQL constant SHA locks remain valid.
- `PR-NATIVE`: `pr-evidence/native-hidden.json` and `native-capture-negative.json`. The native run records `native_tab_switch`, hidden steady 600,002ms, 0 new GETs after hidden on all four targets, before/after DOM match, 5/5 created tabs cleaned up, CDP disconnect, raw browser not terminated, `cleanup_failures=[]`. The negative capture records `FAIL`, exit 1, transport closed, preventing a false PASS.
- `PR-PROXY`: `pr-evidence/proxy-smoke.json`. Latest-base real client → proxy → loopback fake upstream is 200, revoke is 200, a request after revoking the same key is 401. Production calls: 0.

Historical `9c420` evidence group names and materials are retained as the comparison basis:

- `RUST-713`: `artifact://604`, 713 passed, 7 skipped.
- `WEB-v5`: `evidence-v5/reviewed-browser/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`, 92/92 PASS.
- `API-v5`: `comparison-*.json`, `api-*.jsonl`, `concurrent-three-read-comparison.json`.
- `NATIVE-v5`: `perf06-native.json`, `sanitizer-check.json`, `native-capture-negative-restore.json`.
- `PROXY-v4`: `evidence-v4/proxy-smoke.json`.

The actual Rust collection paths are `cache_keepalive_session_reads::*` under `cc-lb-storage-{sqlite,postgres} --test integration`, `cache_keepalive_contracts::{read_equivalence,cursor_window}::*` under `cc-lb-admin --test integration`, and `cache_keepalive_view::{tests,activity_tests,status_tests}::*` under `cc-lb-admin --lib`. `pnl_converter_invalid_input_is_http_400` collects only one public parent test, and the parent verifies the exact private child execution in the same binary. The child is not counted as a separate QA case or extra PASS.

## 3. Storage QA matrix — ST-01~ST-16

Each storage case runs the same logical fixture on PostgreSQL and SQLite. `<TEST>` in the commands is the exact test name written in the row. ST-01~12 and ST-14~16 use the same name on both engines, but ST-13 differs: PostgreSQL `cache_keepalive_session_reads::list_boundaries_preserve_error_contract`, SQLite `cache_keepalive_session_reads::list_boundaries_preserve_invalid_input_contract`.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| ST-01 | Exact session direct lookup. `reads.rs::get_cache_keepalive_list_item`; both adapters | P1 session `sess-100`, active, refresh_count 3; no same-ID decision | Run both engines with `<TEST>=direct_lookup_returns_exact_session` | `Some`, source Session, bare id `sess-100`, active, refresh_count 3; canonical item equals legacy full-list first match | Engine-tagged serialized item and equality assertion | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-02 | Standalone decision direct lookup; adapter row mapper | P1 decision `dec-200`, no turn/session | Both engines, `<TEST>=direct_lookup_returns_visible_decision` | `Some`, source Decision, status/refresh_count null, effective timestamp preserved | Serialized items and PG/SQLite equality | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-03 | Late-turn anti-join. adapter direct candidate SQL | P1 decision `dec-300`; insert same `source_ref_id` turn after first lookup | Both engines, `<TEST>=direct_lookup_hides_decision_after_late_turn` | Before turn: decision; after turn: `None`, unless same bare-ID session exists, then session wins | Before/after rows, mutation UTC, query result hashes | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-04 | Bare-ID collision, unequal timestamps. legacy `ORDER BY last_message_at_ms DESC, entry_id ASC` | Session and visible decision id `clash-400`; run session-newer and decision-newer subcases | Both engines, `<TEST>=direct_lookup_uses_newest_collision_candidate` | Newer effective timestamp wins exactly as legacy list first match | Both subcase candidate timestamps and chosen source | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-05 | Bare-ID collision, equal timestamp, default collation | Same id and exact effective timestamp | Both engines, `<TEST>=direct_lookup_preserves_equal_timestamp_namespace_order` | Chosen row equals each engine's legacy `LIST_SQL` first match; current ASCII namespace expects decision first without adding forced collation | Legacy/new item bytes plus DB collation metadata | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-06 | Tenant isolation in both branches | P2 owns session/decision secret IDs; P1 owns none | Both engines, `<TEST>=direct_lookup_is_principal_scoped` | P1 gets `None`; P2 gets its own rows | Query principal, result, no cross-principal ID in body | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-07 | Narrow summary equals legacy full scan. `CacheKeepaliveSummaryInput`; `summary_for_items` successor | 50 sessions, active/terminal mix; 3 visible recent decisions, 2 late-joined recent decisions, 10,000 old decisions | Both engines, `<TEST>=summary_input_matches_legacy_full_scan` | Ordered sessions exact; `recent_decisions=3`; final four summary values canonical-byte equal at fixed clock | Input row hashes, decision count, legacy/candidate body hashes | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-08 | No joined-decision leakage in recent count | 10 recent joined decisions and 5 recent standalone decisions | Both engines, `<TEST>=summary_recent_decisions_applies_anti_join` | `recent_decisions == 5` | Count query result and fixture cardinalities | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-09 | Full pagination PG/SQLite parity, no cap | Stable mixed dataset >1,000 rows with ties and null timestamps | Both engines, `<TEST>=pagination_reaches_terminal_with_engine_parity`; loop until `next_cursor.is_none()` | Every page ordered ID sequence and cursor payload match; concatenated IDs equal oracle; terminal is wire null, regardless of page count | Per-page cursor-in/out hashes, final row sequence hash, page_count | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-10 | Filter branch work is bounded; adapter list implementation | Dataset has both tables large; run Scheduled and NotTracked | Both engines, `<TEST>=single_source_filters_skip_unrelated_branch` plus planner capture | Scheduled returns session rows only; NotTracked decision rows only; candidate plan/query log shows no unrelated branch work. Do not infer from response alone | SQL statement/plan artifact and exact returned IDs | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-11 | Cursor validation before DB. `CacheKeepaliveSessionListQuery::validate_cursor` | Valid cursor altered by principal, horizon, and filter separately | Both engines, `<TEST>=cursor_mismatch_fails_before_query` | `StorageError::InvalidInput`, field `cache_keepalive_session_cursor`, exact reason; SQL call count 0 | Error debug/JSON and query counter | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-12 | Nullable decision timestamp fallback | Decision with `last_message_at_ms=NULL`, `ts=1_700_000` | Both engines, `<TEST>=nullable_decision_timestamp_uses_ts_millis` | Effective ms `1_700_000_000` in list, detail, summary boundary; same filter/order | Raw DB values and three result hashes | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-13 | Limit/range boundaries | limit 0, 1, 50, 100; direct storage u32::MAX; summary cutoff and list horizon/cursor at u64::MAX | PostgreSQL exact test `cache_keepalive_session_reads::list_boundaries_preserve_error_contract`; SQLite exact test `cache_keepalive_session_reads::list_boundaries_preserve_invalid_input_contract` | 0 empty/no cursor; 1/50/100 K+1 semantics; no overflow/panic. Summary cutoff overflow is `InvalidInput` on both engines. When list horizon/cursor exceeds the i64 range, SQLite preserves the existing `InvalidInput` and PostgreSQL preserves the existing `StorageError::Fatal` with the messages `cache keepalive horizon start cannot be represented as bigint` and `cache keepalive cursor timestamp cannot be represented as bigint` respectively. The candidate must equal each engine's baseline and must not normalize errors cross-engine. HTTP maximum stays 100. | Each input/result, per-engine exact error variant/message, panic-free completion | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-14 | Requested-row corruption mapping | Corrupt config JSON and invalid status on rows selected by direct/list/summary | Both engines, `<TEST>=selected_corruption_is_not_silently_dropped` | Selected mapping error is `StorageError::Corrupted`; unrelated old decision outside narrow read is not required to poison request | Corrupt row key, selected query, exact error variant | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-15 | Arbitrary/noncanonical/colon/Unicode `cursor.entry_id` comparison remains SQL-defined | Valid cursor envelope with entry IDs such as `x`, `session::x`, `decision:é`, `Ω`, and namespace-looking noncanonical text | Both engines, `<TEST>=cursor_entry_id_preserves_legacy_sql_comparison` | Candidate rows/cursor result equal pre-change `LIST_SQL` on that same engine for every value; no parser canonicalization or Rust byte-order substitute | Per-value legacy/candidate ordered IDs and collation metadata | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-16 | DB default collation result contract | Fixture mixing ASCII namespace, colon variants, and non-ASCII IDs on the same DB and same connection/session collation | Both engines, `<TEST>=list_and_detail_match_legacy_on_same_database_collation`; run the legacy query and the candidate query consecutively on the same fixture | On each engine, the legacy and candidate ordered rows, selected detail candidate, and cursor chain are exact. Cross-engine parity of the canonical fixture is also maintained separately. Do not pass on source text grep alone. | DB/connection collation metadata, ordered ID/cursor hashes of both queries, selected candidate hash | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |

## 4. API/semantic QA matrix — SEM-01~SEM-12

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| SEM-01 | Invalid principal route validation. `principal_cache_keepalive.rs::active_principal` | Running Admin app | Exact test `cache_keepalive_contracts::read_equivalence::invalid_principal_id_is_400` | HTTP 400, canonical body `{"error":"invalid_principal_id"}` | Status/body hash | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-02 | Unknown and soft-deleted principal | One absent UUID and one deleted principal | Exact test `cache_keepalive_contracts::read_equivalence::unknown_or_deleted_principal_is_404` | Both HTTP 404 `unknown_principal` | Two request/response records | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-03 | Query parser exact errors. `query.rs::parse_limit/parse_horizon/parse_filter` | Valid principal | Exact test `cache_keepalive_contracts::read_equivalence::invalid_keepalive_query_values_preserve_codes`; issue limit=101/abc, horizon=2h, status=warm, status=renewed&error=true, error=maybe | Respectively `invalid_cache_keepalive_limit`, `invalid_cache_keepalive_horizon`, `invalid_cache_keepalive_status`, `invalid_cache_keepalive_filter`, `invalid_cache_keepalive_error`; all 400 | Five exact URL/status/body hashes | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-04 | Encoded cursor mismatch HTTP mapping | Cursor from P1/24h/all; replay on P2, 7d, renewed | Exact test `cache_keepalive_contracts::read_equivalence::mismatched_cursor_is_400_invalid_input` | HTTP 400 `invalid_input`, field `cache_keepalive_session_cursor`, exact reason from `validate_cursor` | Cursor hash and three response hashes | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-05 | Session and decision bare-ID detail wire | Session `sess-01`; standalone decision `dec-01` | Exact test `cache_keepalive_contracts::read_equivalence::detail_direct_lookup_preserves_session_and_decision_wire` | 200; session has turns/state; decision is not_tracked, attempts null, turns empty; full JSON schema/hash equals fixed legacy oracle | Canonical baseline/candidate bodies | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-06 | Collision resolution at route | Three collision timestamp cases | Exact test `cache_keepalive_contracts::read_equivalence::detail_collision_matches_legacy_first_match` | Session newer -> session; decision newer -> decision; tie -> engine baseline/default collation result | Candidate set and chosen full detail hashes | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-07 | Shadowed decision detail transition | Detail decision exists, then same source turn arrives | Exact test `cache_keepalive_contracts::read_equivalence::detail_hides_late_joined_decision` | Before 200 decision; after mutation 404 `unknown_cache_keepalive_entry`, or same-ID session detail if fixture creates it | Ordered transition records | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-08 | Empty/disabled summary-only response | Empty enabled principal and empty disabled principal | Exact test `cache_keepalive_contracts::read_equivalence::empty_and_disabled_summary_are_zero` | 200; all summary fields zero, `rows=[]`, `next_cursor=null` for limit=0. Disabled UI state is not inferred from this API alone | Both canonical bodies | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-09 | Inclusive five-minute boundary | Fixed clock; session+decision at cutoff, and at cutoff-1ms | Exact test `cache_keepalive_contracts::read_equivalence::summary_five_minute_cutoff_is_inclusive` | cutoff entries included; older-by-1ms excluded; late-joined decisions excluded | Fixed clock, row timestamps, summary count | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-10 | Unknown/partial catalog pricing tolerance | Session turns use absent model and model missing required TTL rate | Exact lib test `cache_keepalive_view::tests::unknown_pricing_stays_null_and_summary_skips_it` plus route test `cache_keepalive_contracts::read_equivalence::detail_direct_lookup_preserves_session_and_decision_wire` | Request succeeds; summary skips session P&L; detail totals remain current zero values and each unpriced turn `pnl:null`; no invented price | Catalog snapshot hash and full JSON | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-11 | Per-turn integer micro-dollar truncation | Multiple deliberately small turns where sum-then-price differs from price-then-sum; include 20,000 tokens × 300,000 micros/M × 3 renewals | Exact lib test `cache_keepalive_view::tests::pricing_preserves_per_turn_integer_truncation` | Each turn computes integer division first; named example spent is 18,000 micros; total equals legacy fold, not naive aggregate | Per-turn integer operands/results | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |
| SEM-12 | Corruption scope and HTTP mapping | Corrupt selected list/detail/session/turn row and unrelated old decision | Exact integration test `cache_keepalive_contracts::read_equivalence::corruption_mapping_tracks_narrow_read_scope` | Selected corruption -> 500 `storage_error`; unrelated old decision outside new query may remain 200 and is documented as intentional scope difference | Selected/unselected row keys and HTTP records | PASS@9c420 [RUST-713, API-v5] / PASS@4ed7cc8c [RUST-724, PR-API] |

## 5. Cursor bug-fix QA matrix — CUR-01~CUR-03

Runner and browser evidence do not compare opaque cursors by deleting them like nonces. The baseline cursor is preserved as the original reference with the existing 5 fields; the candidate cursor is compared for exactness on those 5 decoded fields, then the `horizon` tag is verified separately. The fixture timestamp anchor and the live request's `started_at_utc` are not mixed to hide UTC `now` differences.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| CUR-01 | 24h/7d page chain survives elapsed time. `query.rs::parse_query`, `encode_cursor` | At least 2 pages per horizon, live clock | Exact Axum `cur_01_...` above and browser `--grep 'CUR-01'`; after the UI page 1 response, advance the server second and click `Loading older sessions...` | Baseline's existing 400 is recorded as the accepted bug delta. Candidate page 2 is 200, decoded `horizon_start_ms` equals page 1, and the tag equals the request horizon. Summary is recomputed at each request's current time. | Page 1/2 raw status/body for both horizons, baseline original cursor hash, candidate decoded field hash/tag, UI request URL | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |
| CUR-02 | Valid cursor and principal/horizon/filter scope validation | 24h, 7d, all cursors and P1/P2/filter combinations | Exact Axum `cur_02_...` above, runner `--case cursor`, browser `--grep 'CUR-02'` | The new cursor tag is exact for 24h/7d/all. `all` same-scope page 2 is 200 on baseline/candidate. Different principal, 24h↔7d, and different filter are all 400 `invalid_input`; the existing SQL seek contract for arbitrary `entry_id` stays per ST-15. | Three tag decoded payloads, valid page body, three mismatch status/body/reason | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |
| CUR-03 | First-page anchor remains frozen through repeated advances | At least 5 pages on 24h, server second advanced per page | Exact Axum `cur_03_...` above, runner/browser `--grep 'CUR-03'` | Baseline's first time-advance 400 is the accepted bug delta. Candidate pages 2~5 are all 200 and every emitted cursor's `horizon_start_ms` is exact to page 1. | Per-page request time, cursor-in/out hashes, decoded original-field hashes, frozen anchor | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |

## 6. Admin Web QA matrix — UI-01~UI-12

Unit component commands use `cd crates/cc-lb-admin/web && bun run test --run <FILE> -t '<NAME>'`. Browser commands use the real-server Playwright command from §2.2 with `--grep '<ID>'`.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| UI-01 | Four metric card and value-change flash. `CacheKeepaliveCard`, `MetricTile` | Enabled P1, summary changes once | Component test `src/components/principals/cache-keepalive/__tests__/CacheKeepaliveCard.test.tsx`, name `Given metric change, When rendered, Then flashes ONLY when value changes`; then real browser `--grep 'UI-01'` | Four labels/values format unchanged; changed value flashes; `PAUSE_ANIMATIONS=true` suppresses animation | Test output plus before/after screenshot and independent summary body | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-02 | Toggle and 409 handling. `CacheKeepaliveCard`, mutation hook | Revision conflict fixture | Component test same file `-t 'locks the toggle during another write'`; real browser `--grep 'UI-02'` | Existing switch/lock/toast behavior unchanged. The actual compiled server PATCH returns engine-specific HTTP 409: SQLite `storage_conflict`, PostgreSQL `stale_revision`. Synthetic responses cannot replace either observation. Backend read optimization adds no mutation | Engine-tagged PATCH status/body and UI screenshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-03 | Three horizon transitions. `CacheKeepaliveSessionsDrawer::HorizonToggle`, path builder | Rows distributed across 24h/7d/all | Drawer component test filtered by `-t 'horizon'`; real browser `--grep 'UI-03'` | Exact requests for 24h, 7d, all; Overview label changes; ordered IDs match API | Three exact URLs/body hashes and UI labels | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-04 | Seven filters and error query mapping | At least one row for every base state and orthogonal error | Drawer component test `-t 'filter'`; real browser `--grep 'UI-04'` iterates all 21 horizon×filter combinations | Selected chip `aria-pressed=true`; error uses `error=true`; every API/UI ordered ID list agrees | 21 request records, aria snapshot, ordered ID hashes | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-05 | Row ID/state/error/P&L/attempt ticks | Mixed fixed rows | Drawer component test `-t 'row'`; real browser `--grep 'UI-05'` | Existing text, badge, color class, error border, attempts/max_attempts unchanged | API row and DOM semantic snapshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-06 | FLIP read-before-write and pause behavior | Two rows swap order, then data changes without movement | Existing drawer tests `-t 'reads all FLIP geometry before writes and only animates moved rows'` and `-t 'Given PAUSE_ANIMATIONS=true'`; browser `--grep 'UI-06'` | All geometry reads precede writes; only moved rows animate; pause disables. A fixed frame-rate target is not used as a pass criterion. | Event sequence, trace, screenshot; no frame-rate-only verdict | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-07 | Real cursor pagination through terminal, no 20 cap | Stable dataset requiring >20 pages | Candidate real browser `--grep 'UI-07'`; click `Loading older sessions...` until response `next_cursor` is null; independently direct-fetch every page. Baseline time-advance failure is recorded only under CUR-01/CUR-03. | Candidate has no duplicate/missing IDs; cursor-out hash equals next cursor-in hash; decoded cutoff stays fixed; stop only at wire null; `No more sessions` is presentation confirmation, not sole proof | Every candidate page record, final concatenated ID hash, manifest expected count, page_count >20 | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-08 | Desktop split and mobile detail navigation | One selectable session | Detail/drawer component tests; browser `--grep 'UI-08'` at 1024px and 800px | 1024 split with 440px list; 800 list hidden and `◀ Back`; detail data same | Two viewport screenshots and detail body hash | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-09 | Turn timeline, pending/final P&L | Active multi-turn and terminal multi-turn sessions | `bun run test --run src/components/principals/cache-keepalive/__tests__/SessionDetailPane.test.tsx -t 'turn'`; browser `--grep 'UI-09'` | Active newest turn is pending/live and avoided cost unrealized; terminal newest is final; existing strings/colors/rounded display preserved | Raw detail JSON, semantic DOM, screenshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-10 | Config snapshot and raw record disclosures | Session detail with snapshot and raw fields; decision detail nullables | Detail component test `-t 'Config'` and `-t 'Raw'`; browser `--grep 'UI-10'` | Existing expand/collapse, fields, nullability and raw values unchanged | Detail body and expanded screenshots | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-11 | Visibility pauses polls and preserves each hook's resume behavior | Card+drawer+detail open | Component hook tests plus browser `--grep 'UI-11'`; probe `document.visibilityState`/`visibilitychange` inside the page, hidden 30s then visible | DOM hidden state disables the configured interval. Summary/detail explicitly call `query.refetch()` on hidden→visible through `usePolledData`; sessions only changes its `useInfiniteQuery` interval and may also follow TanStack's existing focus behavior. Record baseline and require candidate to match; do not invent one shared immediate-refetch rule or treat this probe as native OS hiding. | Timestamped DOM visibility events and network log per independent query key | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-12 | Two-principal cache/state isolation | P1 and P2 have disjoint IDs and summary values | Browser `--grep 'UI-12'`; open P1 drawer/detail, navigate to P2, observe two poll intervals | P2 never paints P1 values/rows. Current query functions do **not** pass AbortSignal, so prior P1 network requests may complete; no-cancel is expected and cache-key isolation is the oracle | P1/P2 request timelines, visible IDs per paint, no mixed-principal body | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |

## 7. Performance/evidence QA matrix — PERF-01~PERF-06

No row has an arbitrary absolute latency or fixed frame-rate pass budget. Baseline and candidate run on the same machine, engine, dataset, cache state, concurrency, and build profile. Functional response equality is a prerequisite to considering timing.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| PERF-01 | Summary removes D-scale full materialization | D=100,000 old decisions; fixed small S/T; baseline and candidate binaries | `python3 "$RUNNER" compare --engine <sqlite|postgres> --baseline-url <URL> --candidate-url <URL> --token-file "$TOKEN_FILE" --case PERF-01 --records <JSONL> --summary <JSON>`; 1 cold + 30 warm `?limit=0` requests per engine by default | Fixed-snapshot body hashes exact. Candidate decision work is bounded to one recent visible count, no `list_all` 1,000-row pagination; report relative p50/p95/max, no absolute promise | Raw requests, SQL count/time, plan, rows examined/returned, body hashes | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
| PERF-02 | List first/middle/terminal pages avoid summary full scan and bound materialization | D=100k; stable mixed list >20 pages; fixed S/T | Same `compare` command with `--case PERF-02`; the runner traverses the candidate's UI-size 24h list to wire `next_cursor=null` with no hard cap and records first/page-13/terminal. The baseline original cursor is preserved and the time-advance 400 is separated as the CUR bug delta. | First-page body and the decoded original 5 cursor fields are exact and the candidate tag is 24h. Subsequent candidate page materialization follows the K+1 final-page contract and reads turns only for the page's sessions. Candidate total row count must equal the manifest oracle. | Per-page plan/candidate cursor chain, baseline bug-delta record, baseline/candidate first-page timing | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
| PERF-03 | Detail direct candidate lookup | D=100k; target session, decision, collision, missing ID | Same `compare` command with `--case PERF-03`; 30 warm calls each by default for session, decision, collision, missing | Exact body/status. Candidate has bounded two-candidate lookup and target-only session/turn/upstream work; no list_all pagination | SQL trace/plan, response hashes, relative latency | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
| PERF-04 | Rapid filter switching preserves current no-cancel semantics | Delayed backend responses; five chip changes under 100ms | Browser `--grep 'PERF-04'` | Because frontend is unchanged, preceding requests are not required to show `(canceled)` and may finish. Final selected filter paints only its own query-key data; no stale overwrite | Request lifecycle for all five keys and every UI paint hash | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| PERF-05 | Backend change does not regress frontend render work | Identical 100-row and multi-page bodies replayed from actual candidate server | Browser `--grep 'PERF-05'`; collect baseline/candidate trace around reorder and merge | API improvement is assessed separately. DOM node count, FLIP read/write order, long-task count, stable-paint distribution show no material regression relative to baseline; no fixed frame-rate budget | Browser traces, long tasks, paints, row count, API timing kept separate | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| PERF-06 | Native hidden-tab behavior and memory/network are relative | Four fresh-fixture low-principal tabs: SQLite/PostgreSQL × baseline/candidate, each with card+drawer+detail open | Main starts and owns raw Chromium, then runs §2.2 `node qa/keepalive-native-visibility.mjs` argument or environment command once. The CLI observes each tab visible for at least 30s, fronts one blank control tab while all four remain natively hidden for at least 600s, then restores and observes each tab for at least 30s. The Playwright `--grep 'PERF-06'` page-script probe records `visibility_probe: "page_script"` only and cannot satisfy this row. | Every target reaches real `hidden` without descriptor overrides. Only requests whose recorded start predates that target's hidden event may finish; no new keepalive request starts before restore. Every restore produces a new GET 200 and ready DOM/paint observation. Candidate heap/network results remain relative to the matching baseline; no arbitrary 1MB budget | One native JSON: `mechanism: "native_tab_switch"`, descriptor validation, timestamped native events, per-phase request counts/status/timing/redacted URLs, initial in-flight IDs, CDP V8 heap samples, restore-ready response IDs, four-client DOM match, embedded raw JSONL with unique IDs | PASS@9c420 [WEB-v5, NATIVE-v5] / PASS@4ed7cc8c [PR-WEB, PR-NATIVE] |

## 8. Required transition and end-to-end additions — FLOW-01~FLOW-08

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| FLOW-01 | Generation reactivation/reset. both `cache_keepalive_sessions.rs::replace_from_real_request` | Terminal session generation G, refresh_count >0, error/reason set | Add/run exact storage test `reactivation_increments_generation_and_resets_summary_inputs` on both engines | Same key becomes active generation G+1, refresh_count 0, error/terminal_reason cleared; next summary/list/detail poll reflects replacement | Before/after DB row and three API hashes | PASS@9c420 [RUST-713, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-WEB] |
| FLOW-02 | Late-turn concurrent transition and convergence | Visible decision detail/list, then safe upstream/projection creates turn/session between independent reads | Real server `--grep 'FLOW-02'` on both engines | No stronger atomicity than baseline is required. Each fixed snapshot is valid; decision disappears and session/turn representation converges by next 5s poll without duplicate visible entry | Mutation timestamp, each API start/end, body hashes across at least two polls | PASS@9c420 [RUST-713, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-WEB] |
| FLOW-03 | Cleanup changes retained-session summary | Expired session and stale pending session plus retained control session | Add/run scheduler/storage scoped test and real server `--grep 'FLOW-03'`; invoke existing housekeeping path | Deleted sessions no longer contribute to summary/list/detail; control remains. Decisions are not deleted or redefined | Housekeeping affected count, before/after rows and summaries | PASS@9c420 [RUST-713, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-WEB] |
| FLOW-04 | Catalog null/partial and catalog replacement | Same stored turns under empty/partial catalog, then installed replacement snapshot | Exact lib test `cache_keepalive_view::tests::catalog_absence_and_replacement_reprice_without_storage_change`; real server restart/reload only if supported fixture does so safely | Empty/partial pricing yields null turn P&L and skipped summary contribution; replacement reprices same rows using current catalog, with no financial rollup/cache | Catalog hashes, unchanged storage row hash, before/after response | PASS@9c420 [RUST-713] / PASS@4ed7cc8c [RUST-724] |
| FLOW-05 | Rounding display versus accounting precision | Micros values around half-quantum boundaries, positive and negative | Exact lib test `cache_keepalive_view::tests::pnl_format_rounding_does_not_change_micro_accounting` | Accounting uses exact micros and per-turn truncation; display `format_micros` rounds magnitude by `+ quantum/2`, uses Unicode minus, and does not feed rounded value back | Input micros, formatted strings, exact aggregate micros | PASS@9c420 [RUST-713] / PASS@4ed7cc8c [RUST-724] |
| FLOW-06 | Saturation and converter phase contract | Inputs driving token conversion/sums toward i64 saturation, then total dollar whole part beyond i32 | Exact lib test `cache_keepalive_view::tests::pnl_saturates_then_dollar_converter_returns_invalid_input`; exact route test `cache_keepalive_contracts::read_equivalence::pnl_converter_invalid_input_is_http_400` | Intermediate multiply/add/sub saturate exactly; representable totals return value; API dollar range overflow -> HTTP 400 `invalid_input` field `cache_keepalive_pnl`, not 500 | Integer trace and exact HTTP body | PASS@9c420 [RUST-713] / PASS@4ed7cc8c [RUST-724] |
| FLOW-07 | Two-principal in-flight switch with unchanged no-cancel behavior | P1 response deliberately late; navigate to P2 whose response is fast | Real browser `--grep 'FLOW-07'` | P1 request may complete because AbortSignal is not wired. P2 route never paints P1 card/list/detail; settings draft resets by `principal.id`; selected detail cannot expose P1 data | Route, query-key, request, and paint timeline | PASS@9c420 [RUST-713, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-WEB] |
| FLOW-08 | Full actual-stack equivalence: baseline/candidate × SQLite/PostgreSQL × browser/fake upstream | Same deterministic seed and safe loopback upstream; fixed fixture anchor and separately logged live request clock | Run §2.2 harness for four server/engine combinations; execute all 21 filter/horizon pairs, summary, representative detail/collision/404, CUR-01..03, transition flows, and candidate terminal pagination | Canonical status/body/ordered rows match baseline where the contract is unchanged. For the opaque cursor, compare the decoded original 5 fields and verify the candidate `horizon` tag separately. Only baseline 400/candidate 200 on time-advance pagination is the approved bug delta. | Manifest of binaries/config hashes, fixture/request clock anchors, seed hash, raw records, browser traces, cleanup record | PASS@9c420 [API-v5, WEB-v5, PROXY-v4] / PASS@4ed7cc8c [PR-API, PR-WEB, PR-PROXY] |

## 9. Planner/index decision gate

1. First record PostgreSQL `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` and SQLite `EXPLAIN QUERY PLAN` for summary count, each list branch and cursor position, and detail candidates on D=100k and S/T scales.
2. An index migration is allowed only if the candidate still performs avoidable scan/sort work material to the measured regression. "100% performance resolution without migration" is not a predetermined conclusion.
3. If needed, latest-base migration `0118`/`0086` must index the exact expression used by the query, including `COALESCE(last_message_at_ms, ts * 1000)` and any computed prefixed order key required by the accepted plan. It must not backfill or delete raw rows, change column semantics, or force `COLLATE C`.
4. Repeat ST-09, ST-12, ST-15, ST-16, PERF-01, PERF-02 after any migration. The post-migration response hashes and cursor chain must remain exact.

## 10. Completion accounting

- Original requested IDs: 44 (`ST-01..14`, `SEM-01..12`, `UI-01..12`, `PERF-01..06`)
- Mandatory storage additions: 2 (`ST-15..16`)
- Required transition/E2E additions: 8 (`FLOW-01..08`)
- Approved cursor bug-fix additions: 3 (`CUR-01..03`)
- Total: **57**
- Historical candidate `9c420`: `PASS` **57**, `FAIL` **0**, `BLOCKED` **0**, `NOT_RUN` **0**
- Reviewed browser historical matrix: 4 datasets × 23 cases = **92/92 PASS**
- Latest base `4ed7cc8c`: `PASS` **57**, `FAIL` **0**, `BLOCKED` **0**, `NOT_RUN` **0**. PostgreSQL/SQLite migrations were integrated as `0118`/`0086` respectively and the browser matrix is **92/92 PASS**.
- The current application/QA status and PR CI status are separate. The PR has not been created yet and CI is pending.

A status is PASS only when the §2.3 raw record and §2.4 anchor exist. Do not raise a status on build success, test enumeration, a route visit, a single screenshot, a disabled pagination control, or a synthetic frontend mock alone. Historical evidence is a comparison basis and does not substitute for latest-base PASS or PR CI PASS.
