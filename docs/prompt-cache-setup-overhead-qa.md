# Prompt-cache setup overhead TDD and QA checklist

- Status: Complete
- Written: 2026-09-07
- Design document: `docs/prompt-cache-setup-overhead-remediation.md`

## 1. Verification principles

1. Add new tests first to prove the baseline failure or existing inefficiency.
2. Verify result correctness differentially, using the existing analyzer as the frozen reference.
3. Judge performance regressions with deterministic work counters such as tokenization calls/bytes/tokens, not wall-clock alone.
4. Permanently keep as unit/component/integration tests everything provable without an external provider.
5. Verify the proxy path with real HTTP requests against a fake Anthropic upstream.
6. Production provider calls are not required; if run, classify them as a separate optional calibration.

## 2. TDD order

### A. Pre-fix failure

- [ ] Prove the existing tokenizer call count is 4 on the 4-nested-breakpoint fixture.
- [ ] Prove the current metrics registry has no prompt-cache analysis metrics.
- [ ] Prove with a work counter that cumulative serialized/tokenized bytes grow well beyond the deepest prefix on the 4.4MB production-like fixture.

### B. Exact serialization and key

Permanent unit/property tests:

- [ ] The batch serializer is identical to the existing `PrefixSerializer` bytes at every breakpoint.
- [ ] Implicit 5m and explicit `ttl: "5m"` have the same structural hash but different `TokenPrefixKey`.
- [ ] `cache_control` key order, 5m/1h, and marker movement are reflected in the exact token key.
- [ ] Identical prefixes do not share the token count cache when the downstream credential scope differs.
- [ ] Byte parity is preserved across JSON escapes, CJK, Hangul, emoji/ZWJ, whitespace, long numbers, and base64.
- [ ] Verify 0/1/19/20/21/128/259 blocks and 0/1/3/4/5 breakpoints.

Targets:

- `crates/cc-lb-engine/tests/prompt_cache_byte_oracle.rs`
- `crates/cc-lb-engine/tests/prompt_cache_structural_properties.rs`
- New `crates/cc-lb-engine/tests/prompt_cache_setup_optimization.rs`

### C. Exact incremental token count

Permanent unit/property tests:

- [ ] The fast count at every breakpoint exactly equals the existing full-prefix `encode_ordinary().len()`.
- [ ] Token count error tolerance is 0.
- [ ] Include `}`, `,`, `]`, quote, newline, and trailing whitespace cases that change the BPE boundary.
- [ ] Special-token literal input also produces the same result on the fast path and the full reference.
- [ ] On a test seam with deliberately injected bad offsets/invariants, the fallback produces the same result.
- [ ] The 4-breakpoint work counter is at the level of the deepest prefix plus a small boundary tail, not the sum of 4 prefixes.

### D. Cache and single-flight

Permanent unit/concurrency tests:

- [ ] The first request misses, the second identical request hits, and both results are equal.
- [ ] Two requests with the same structural hash but different exact token keys do not share the cache.
- [ ] The actual leader count for 64 identical concurrent requests is 1.
- [ ] After the leader future is cancelled, the blocking job still caches the result and waiters complete.
- [ ] On leader panic/failure, neither the flight nor the semaphore permit leaks.
- [ ] Recomputation after capacity-exceeded eviction produces identical results.
- [ ] Different keys are not unnecessarily bundled into the same flight.

### E. Async runtime non-blocking

Permanent integration test:

- [ ] Run a 1ms heartbeat and a large 4-breakpoint analysis concurrently on a 2-worker Tokio runtime.
- [ ] The analysis runs on a blocking worker.
- [ ] The heartbeat does not stall for the entire BPE duration.
- [ ] No active tokenizer jobs exceed the semaphore concurrency limit.
- [ ] Queue wait cancellation does not consume a permit.

Use only a wide safety bound for the wall-clock threshold; the primary judgment is heartbeat progress and the active-job counter.

### F. Routing and observation differential

Permanent engine integration tests:

- [ ] Full `V3PromptCacheAnalysis` equality between the reference and optimized paths
- [ ] Cache breakpoints and lookback prefixes equality
- [ ] Cold miss, exact warm hit, N-19 lookback hit, N-20 miss
- [ ] 5m, 1h, 1h→5m, invalid 5m→1h
- [ ] Predicted read/creation tokens and cache value equality
- [ ] Selected upstream and WRH key source equality
- [ ] Buffered/SSE observation records and expiry equality
- [ ] Identical results on provider read/prediction disagreement

### G. Prometheus metrics

Permanent component/integration tests:

- [ ] Metric definitions and `describe_*` registration exist
- [ ] Histogram/counter/gauge labels use only the fixed `stage` and `result`
- [ ] On miss analysis, queue/total/tokenize duration and bytes/tokens increase
- [ ] On the second identical analysis, cache hit increases and tokenized bytes do not
- [ ] On concurrent identical analysis, coalesced increases
- [ ] New metric names appear in the `/metrics` scrape
- [ ] Principal ID, request ID, and prefix hash do not appear in labels

### H. Admin-web timeline

Permanent Vitest/component tests:

- [ ] The timeline total denominator keeps the existing `duration_ms`
- [ ] `limit_reconcile_ms` keeps its existing meaning
- [ ] Existing behavior of partial/live rows is preserved

## 3. Real proxy-path QA matrix

Use an isolated SQLite DB and a local fake Anthropic upstream. Do not modify the shared production DB.

| Case | Request | Checks |
|---|---|---|
| P1 | No cache-control | Existing status/body/header, tokenization work 0 |
| P2 | 4×5m breakpoints cold | Normal dispatch, four exact counts, cache creation score |
| P3 | Repeat same request | Second exact count cache hit, identical upstream bytes |
| P4 | N-19 lookback warm | Same upstream selection and matched index/distance |
| P5 | Only N-20 entry warm | Cache miss routing preserved |
| P6 | 1h→5m mixed TTL | Same read/create segment and selected upstream |
| P7 | Streaming SSE | Same message_start/delta/stop bytes and usage observation |
| P8 | Client disconnect | 499/error classification and flight cleanup |
| P9 | Upstream timeout/429/500 | Same status/error mapping and observation behavior |
| P10 | Concurrent large requests | Event loop progress, executor bound, all responses complete |

For each case, check the applicable items together.

- Client-visible status/body/headers
- Method/path/body/header digest received by the fake upstream
- Routing trace and selected upstream
- Request event cache fields
- Prompt-cache observation record
- Prometheus cache/executor counters

## 4. Browser QA

Use an isolated admin-web and mock request events.

- [ ] Open the final request drawer in Logs.
- [ ] Auth/Route/Setup/TTFB/Body/Limit reconcile display normally.
- [ ] The total displayed time and percentages keep the existing `duration_ms` basis.
- [ ] No clipping/overflow at 375×812, 768×1024, 1280×800.
- [ ] Stage colors and tooltips are correct in light/dark themes.
- [ ] Console errors 0, failed network requests 0.

## 5. Full verification commands

Use focused tests during the change and run the full verification once at the end.

```text
cargo fmt --all -- --check
cargo test -p cc-lb-engine <focused cases>
cargo test -p cc-lb-observability
bun --cwd crates/cc-lb-admin/web test <latency tests>
bun --cwd crates/cc-lb-admin/web run check
cargo nextest run --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

If the same commands as CI exist in a separate workflow, prefer that workflow invocation. Do not hide a CI failure by rerunning; fix the cause in the same PR.

## 6. Pre-PR completion checks

- [x] Design document matches the actual implementation
- [x] Every new test verifies a regressible observable contract
- [x] Temporary benchmarks/fixtures cleaned up
- [x] Proxy QA matrix PASS
- [x] Browser QA PASS
- [x] No secrets or operational dumps in the worktree
- [x] Internal reviewer and security reviewer findings resolved
- [x] Full PR CI pass

## 7. 2026-09-07 isolated QA run record

Used an isolated SQLite DB, a local fake Anthropic upstream, and dynamic proxy/admin/metrics ports. Did not use the shared production DB or provider credentials.

### Proxy path

- Request without cache-control: 200 / `pong`, tokenizer bytes counter delta 0
- 1.44MB, 4×5m cold request: 200 / 0.864s
- Second run of the same request: 200 / 0.104s
- Both response bodies SHA-256 identical
- Both request bodies received by the fake upstream SHA-256 identical to the input file
- SQLite request events: both rows `cache_control_block_count=4`, breakpoint JSON length 4, same cache prefix hash
- Proxy setup: first request 854ms, second 97ms

### Concurrent single-flight

- Ran 8 identical 1.54MB requests concurrently
- 8/8 status 200, body `pong`
- Each wall time 0.576–0.590s
- Metric delta: miss 4, coalesced 28
- Tokenized bytes delta 1,536,611 bytes — one batch's size, not 8× the request

### Prometheus scrape

- `cc_lb_prompt_cache_analysis_duration_seconds{stage=\"queue|tokenize|total\"}` exposed
- `cc_lb_prompt_cache_token_count_cache_total{result=\"hit|miss|coalesced\"}` exposed
- `cc_lb_prompt_cache_tokenized_bytes_total`, `cc_lb_prompt_cache_tokenized_tokens_total`, fallback-prefix counter, worker-failure counter exposed

### Browser/component
- Displayed total kept the existing `duration_ms=4,400ms` basis
- Rendered the persisted `LatencyTimeline` component in a real WKWebView at 1280×800 and 375×812
- Internal pre, Wait, Upstream, Body, Limit reconcile, Unaccounted displayed normally
- Horizontal overflow 0 on both viewports, no clipping/overlap

Also verified the timeline text and computed results in the drawer DOM on the real Logs route. In the automation environment the drawer transition stayed off-screen, so visual capture was done on a dedicated QA page for the same component.

### Final proxy re-verification after security/memory fixes

- Rebuilt the isolated server after applying the downstream credential scope and unstable-tail decode optimizations.
- 0.99MB, 4-breakpoint request: cold 0.734s, exact-cache hit 0.079s
- Fake upstream request length 992,385 bytes, SHA-256 identical to the input
- SQLite events: both rows status 200, breakpoint count 4, 64-character cache prefix hash
- `proxy_setup_ms`: cold 724ms, hit 73ms
- Metrics: miss 4, hit 4, fallback prefixes 0, worker failures 0, inflight final 0
- Confirmed 4 listeners stopped and temporary DB/config/credentials deleted

### Local validation summary

- `cargo fmt --all -- --check`: PASS
- `cc-lb-engine`: unit 449, integration 236 PASS
- `cc-lb-server`: unit 169, integration 118 PASS
- `cc-lb-observability`: all PASS
- Admin web: build, routeTree diff, Biome, typecheck, 555 tests PASS
- CI guard scripts and Prometheus rule syntax PASS
- The local toolchain lacks `cargo-clippy`, `cargo-nextest`, `cargo-llvm-cov`, and `cargo-deny`; those jobs are verified in GitHub CI.
- Workspace `--all-features` on macOS aborted due to the existing Wasm test fixture's Mach-O section constraint. This is not a result for the changed crates or the Linux CI path; GitHub CI makes the final determination.

## 8. PR CI results

All workflows passed on commit `f2144dbc7f4f6f0b68ad2c47900ca9bfe5a27a28` of PR #703.

- Web build/lint/typecheck/Vitest: PASS
- Rust fmt: PASS
- Clippy SQLite: PASS
- Clippy PostgreSQL: PASS
- cargo-deny: PASS
- promtool/dashboard validation: PASS
- nextest + coverage gate: PASS
- real-client E2E: PASS
- crate version guard: PASS
