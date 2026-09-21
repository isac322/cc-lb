# Cache Keepalive Read Performance/Equivalence Verification Results

- Recorded: 2026-09-15 UTC
- Verification target: origin/master `4ed7cc8c` latest-base integration; historical candidate short head `9c420` retained as comparison material
- Current status: latest-base 57/57 QA PASS, browser 92/92 PASS, Rust 724 PASS, Admin Web 692 PASS
- PR/CI status: PR not created, CI pending
- Deployment status: no production deployment

## 1. Conclusion and interpretation scope

The latest-base integration removed the read path that materialized the entire large decision history on every request while preserving the response contract of the Principal Cache Keepalive summary, list, and detail. A `horizon` tag was added to the 24h/7d cursor to fix the first page's time range.

Unchanged responses matched the baseline exactly. The cursor's existing 5 fields were exact, and only a `horizon: "24h"|"7d"|"all"` field was added in the candidate. The only approved behavior difference is that 24h/7d page 2 after time elapsed, which was HTTP 400 on baseline, is fixed to HTTP 200 on the candidate.

Integration on origin/master `4ed7cc8c`, PostgreSQL `0118` and SQLite `0086` migration number adjustment, and Rust/Admin Web/API/browser/native/proxy latest-base verification are complete. Application QA is 57 PASS, 0 FAIL, 0 BLOCKED, 0 NOT_RUN. Because the PR has not been created yet, PR CI is pending, not passed. Historical `9c420` results and artifacts are left as-is in a separate section below.

## 2. Functional equivalence and regression results

### 2.1 57 QA statuses

| Group | ID | Historical result | Latest-base result |
|---|---|---:|---:|
| Storage | ST-01..ST-16 | 16/16 PASS | 16/16 PASS |
| API/semantic | SEM-01..SEM-12 | 12/12 PASS | 12/12 PASS |
| Cursor bug fix | CUR-01..CUR-03 | 3/3 PASS | 3/3 PASS |
| Admin Web | UI-01..UI-12 | 12/12 PASS | 12/12 PASS |
| Performance/evidence | PERF-01..PERF-06 | 6/6 PASS | 6/6 PASS |
| Transition/E2E | FLOW-01..FLOW-08 | 8/8 PASS | 8/8 PASS |
| Total | 57 | 57/57 PASS | 57/57 PASS |

Per-row commands, oracles, evidence anchors, and current status are in `qa/admin-web/keepalive-performance-regression.md`.

### 2.2 Actual test collection paths

Rust tests were collected by each crate's actual target and module path.

- SQLite storage: `cargo test -p cc-lb-storage-sqlite --test integration 'cache_keepalive_session_reads' -- --nocapture`
- PostgreSQL storage: `cargo test -p cc-lb-storage-postgres --test integration 'cache_keepalive_session_reads' -- --nocapture`
- Admin HTTP/cursor: `cargo test -p cc-lb-admin --test integration 'cache_keepalive_contracts::read_equivalence' -- --nocapture` and `cache_keepalive_contracts::cursor_window`
- Admin view: `cargo test -p cc-lb-admin --lib 'cache_keepalive_view::tests' -- --nocapture`
- Browser: `crates/cc-lb-admin/web/qa/keepalive-performance-regression.spec.ts`, an opt-in Playwright config using the real compiled server and Vite proxy

`pnl_converter_invalid_input_is_http_400` is one public parent test that verifies the exact private child execution in the same integration binary. The private child was not counted as a separate test or an extra PASS.

The latest-base full Rust record in `artifact://704` is 724 passed, 7 pre-existing ignored, including the mapper path. Clippy `--all-targets -D warnings` also passed. Admin Web passed 72 test files, 692 tests, and build/lint/typecheck. The latest-base browser record is 23 cases per dataset in `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`, totaling 92/92 PASS. Historical Rust `artifact://604` (713 passed/7 skipped) and historical browser 92/92 are also retained as comparison material.

### 2.3 Response contract

- `FILTERS`: all 21 horizon/filter combinations equal baseline on both engines.
- `SEMANTIC`: all 15 representative semantic requests equal on both engines.
- Semantic decoded original-field SHA: SQLite `c18457839b91d5174db9e011789d2f1597591a3892b8590659ef844b593267e9`, PostgreSQL `b95e3c179690b8e9b526d9818de9478e611295fbd2c84532b2de1229432750a4`.
- Baseline original cursor SHA: SQLite semantic `70ec17f468a2bc310ebd703251ca585e29eb534e86a1e78f6b60fca5889abcc2`, PostgreSQL semantic `6e7b66667331385331460481bc9684ce4475b02b3a4bd04128c889c29f4d5a0d`.
- The candidate cursor chain maintained 5 pages and the `24h`, `7d`, `all` tags were confirmed.
- 24h time advance: baseline 400, candidate 200 on both SQLite/PostgreSQL; frozen `horizon_start_ms` is `1789355647000` and `1789355649000` respectively.
- 7d time advance: baseline 400, candidate 200 on both SQLite/PostgreSQL; frozen `horizon_start_ms` is `1788837247000` and `1788837249000` respectively.
- Corruption of the selected row, the current summary session/turn, and the current page row preserves the existing error mapping. The side effect where an unrelated past corrupt decision that the narrower query no longer reads caused the entire request to fail is not preserved. This is the approved scope difference of `SEM-12`.

### 2.4 Latest-base performance results

The figures below were parsed directly from `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/performance-{sqlite,postgres}.json`. Each percentile row has `n=30`, and p50, p95, max were copied from the file values without rounding. The executed binaries are development debug builds per the snapshot build metadata. The first request is only a separate ordering marker, not cold-cache evidence with OS/DB caches emptied.

#### PERF-01 summary, D=100,000

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 8760.282 | 10746.130 | 11507.444 |
| SQLite | candidate | 30 | 18.784 | 39.110 | 77.071 |
| PostgreSQL | baseline | 30 | 2688.024 | 3641.803 | 4152.987 |
| PostgreSQL | candidate | 30 | 19.469 | 32.352 | 60.326 |

#### PERF-03 direct detail, D=100,000

| Engine | Case | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---|---:|---:|---:|---:|
| SQLite | missing detail | baseline | 30 | 7211.258 | 7262.704 | 7287.787 |
| SQLite | missing detail | candidate | 30 | 1.538 | 1.659 | 1.671 |
| SQLite | collision | baseline | 30 | 7209.370 | 7596.544 | 7928.417 |
| SQLite | collision | candidate | 30 | 1.800 | 2.823 | 5.441 |
| SQLite | decision | baseline | 30 | 7274.190 | 7871.024 | 8762.916 |
| SQLite | decision | candidate | 30 | 1.817 | 1.983 | 3.743 |
| SQLite | session | baseline | 30 | 7509.995 | 10826.688 | 11323.940 |
| SQLite | session | candidate | 30 | 2.192 | 3.421 | 4.936 |
| PostgreSQL | missing detail | baseline | 30 | 2671.627 | 3029.135 | 3486.021 |
| PostgreSQL | missing detail | candidate | 30 | 3.129 | 6.681 | 9.159 |
| PostgreSQL | collision | baseline | 30 | 2713.370 | 3680.839 | 4220.492 |
| PostgreSQL | collision | candidate | 30 | 5.092 | 10.561 | 21.269 |
| PostgreSQL | decision | baseline | 30 | 2759.601 | 3731.117 | 4271.741 |
| PostgreSQL | decision | candidate | 30 | 4.457 | 8.778 | 12.819 |
| PostgreSQL | session | baseline | 30 | 2580.331 | 2721.826 | 2849.919 |
| PostgreSQL | session | candidate | 30 | 6.375 | 9.977 | 10.181 |

#### S/T scale

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 148.647 | 153.362 | 153.732 |
| SQLite | candidate | 30 | 144.371 | 149.169 | 150.956 |
| PostgreSQL | baseline | 30 | 130.351 | 137.863 | 141.215 |
| PostgreSQL | candidate | 30 | 128.114 | 134.717 | 137.216 |

PERF-02 traversed 1,205 rows over 25 pages; the ordered ID SHA is `a0da2c69ead37b119d24b062ace4f162559a28028565a1dae380e7d89f53453a` on both engines. SQLite first baseline/candidate is 8735.984/25.544ms; candidate middle/terminal is 19.069/18.911ms. PostgreSQL first baseline/candidate is 2694.921/21.281ms; candidate middle/terminal is 18.139/20.071ms.

## 3. Historical performance results (`9c420`)

All comparisons ran as development debug binaries using the same fixture and build settings on the same host and engine. The first request was recorded separately, but this was not a cold-cache experiment that forcibly emptied OS/DB caches. These results do not represent the absolute latency of a release binary or the improvement rate across providers or the production runtime as a whole.

The tables in this section are historical `9c420` run values. For latest-base values, use §2.4.

The fixture includes 100,000 old decisions and 100,750 total decisions at D-scale. S/T scale has 2,000 sessions and 5 turns per session, i.e. 10,000 target turns. The manifest's total 10,074 turns also include turns from other fixtures.

### 3.1 PERF-01 summary, D=100,000

Each row is the wall time of `n=30` repeated samples. The first sample was recorded once per phase separately in the raw record and is not included in the percentiles below. The cold/warm labels in the raw record distinguish request order; they are not evidence of an actual DB cache reset.

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 8294.670 | 11526.521 | 12812.671 |
| SQLite | candidate | 30 | 18.306 | 37.281 | 45.702 |
| PostgreSQL | baseline | 30 | 3828.495 | 6260.857 | 7066.382 |
| PostgreSQL | candidate | 30 | 21.611 | 38.391 | 45.549 |

The fixed-snapshot body was exact on both engines.

### 3.2 PERF-02 list pagination

| Engine | Baseline first ms | Candidate first ms | Candidate middle ms | Candidate terminal ms | Rows | Pages |
|---|---:|---:|---:|---:|---:|---:|
| SQLite | 7924.494 | 19.387 | 17.706 | 17.020 | 1205 | 25 |
| PostgreSQL | 4990.123 | 26.028 | 16.987 | 15.288 | 1205 | 25 |

The ordered ID SHA was identical on both engines: `a0da2c69ead37b119d24b062ace4f162559a28028565a1dae380e7d89f53453a`. The candidate traversed 25 pages until wire `next_cursor=null`. Baseline 400 versus candidate 200 on page 2 is the approved cursor bug delta. The only list records where the candidate response hash differs from baseline are due to the new cursor's `horizon` field. The decoded original 5 fields are exact.

### 3.3 PERF-03 direct detail, D=100,000

Warm samples for each detail case and phase are `n=30`.

| Engine | Case | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---|---:|---:|---:|---:|
| SQLite | missing detail | baseline | 30 | 7085.328 | 7123.704 | 7171.206 |
| SQLite | missing detail | candidate | 30 | 1.512 | 1.598 | 1.719 |
| SQLite | collision | baseline | 30 | 7085.069 | 7549.871 | 7645.005 |
| SQLite | collision | candidate | 30 | 1.749 | 1.846 | 1.961 |
| SQLite | decision | baseline | 30 | 7076.632 | 7931.370 | 7934.687 |
| SQLite | decision | candidate | 30 | 1.781 | 1.982 | 2.110 |
| SQLite | session | baseline | 30 | 7902.157 | 8259.935 | 8407.501 |
| SQLite | session | candidate | 30 | 2.282 | 2.739 | 2.771 |
| PostgreSQL | missing detail | baseline | 30 | 3916.862 | 4070.676 | 4396.731 |
| PostgreSQL | missing detail | candidate | 30 | 3.334 | 6.423 | 8.399 |
| PostgreSQL | collision | baseline | 30 | 3918.137 | 4041.009 | 4085.389 |
| PostgreSQL | collision | candidate | 30 | 3.769 | 7.551 | 23.356 |
| PostgreSQL | decision | baseline | 30 | 3917.816 | 4021.281 | 4024.029 |
| PostgreSQL | decision | candidate | 30 | 3.176 | 7.651 | 12.965 |
| PostgreSQL | session | baseline | 30 | 3965.302 | 4628.614 | 4892.519 |
| PostgreSQL | session | candidate | 30 | 7.449 | 12.058 | 13.211 |

### 3.4 S/T scale

S/T scale checks session/turn processing cost separately from the D=100,000 removal effect. No claim is made that the candidate made this path fully constant-time.

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 148.435 | 151.589 | 152.289 |
| SQLite | candidate | 30 | 143.767 | 146.190 | 147.095 |
| PostgreSQL | baseline | 30 | 131.205 | 141.248 | 142.286 |
| PostgreSQL | candidate | 30 | 127.868 | 138.382 | 139.790 |

### 3.5 Three concurrent reads

Summary, all-horizon list, and session detail were requested concurrently at concurrency 3.

| Engine | Phase | Batch wall ms | Summary ms | List ms | Detail ms | HTTP |
|---|---|---:|---:|---:|---:|---|
| SQLite | baseline | 9742.333 | 9632.309 | 9741.547 | 9397.885 | 200/200/200 |
| SQLite | candidate | 27.421 | 25.791 | 26.808 | 2.568 | 200/200/200 |
| PostgreSQL | baseline | 5535.156 | 5397.065 | 5534.599 | 5196.271 | 200/200/200 |
| PostgreSQL | candidate | 25.795 | 18.682 | 25.183 | 5.131 | 200/200/200 |

Summary and detail body SHAs were exact between baseline/candidate. The list raw body SHA differs because of the approved `horizon` addition in the candidate cursor.

## 4. Planner and index judgment

The reviewed plan manifest contains 124 actual artifacts. Each artifact's source path, source file SHA, and SQL constant SHA are non-empty and passed source-lock verification. The plan tree collected nested nodes, not just the root level.

In the PostgreSQL D-scale late-page plan, both the session and decision index conditions include `principal_id`, the horizon lower bound, and the cursor timestamp upper bound in the index range. The representative plan's execution time was 0.135ms. Buffers were 42 shared hit blocks, 0 shared read blocks, 0 physical read blocks. This is a warm-cache hit record; it does not mean 42 blocks were read from disk. SQLite `EXPLAIN QUERY PLAN` and the Python SQLite API do not provide cache hit or physical read block counts, so the same figures were not produced.

Some plans retain a sort over the bounded result. The PostgreSQL D-scale summary session plan also processed 861 actual session rows. Therefore the result is not described as fully constant-time across all sources and all scales.

The proposal to drop existing indexes was rejected. The policy allows only additive indexes; raw row backfill, deletion, column semantics changes, and forced `COLLATE C` are not allowed. In the historical evidence the Cache Keepalive indexes were PostgreSQL `0117` and SQLite `0085`, but because the latest base uses those numbers, the final integration names must be `0118`/`0086`.

The disposable PostgreSQL rehearsal measured the two exact CREATE INDEX statements on 302,250 decision rows and a 103,628,800-byte decision table, i.e. 98.83 MiB. The measured values were 3.357ms and 217.322ms. The observed locks were `ShareLock` and `AccessShareLock` on each target table, all granted. The transaction was rolled back. This result does not claim production deployment time or zero-downtime for live writers.

## 5. Browser, native visibility, sanitizer

The latest-base real-browser matrix passed 23 cases on each of 4 datasets, totaling 92/92. Admin Web product code was not changed; 5-second polling, no AbortSignal propagation, query-key isolation, and drawer/detail behavior were compared against baseline.

Latest-base PERF-06 is not a DOM probe that changes `visibilityState` via page script. It connects to the existing raw Chromium over CDP and brings a blank control tab to the front — the `native_tab_switch` method.

- Status: PASS
- Total duration: 846,006ms
- Hidden steady: 600,002ms
- Targets: SQLite/PostgreSQL × baseline/candidate, 4 total
- New GETs after hidden: 0 on all four targets
- Manual DOM cross-client match: true both before and after
- Embedded raw records: 209, unique record IDs
- Harness-created tabs cleaned up 5/5, Playwright CDP connection closed, raw browser process not terminated
- Cleanup failures: none

Latest-base fault injection recorded `status: FAIL`, `native network evidence capture failed`, Playwright connection closed, raw browser process not terminated, and `cleanup_failures=[]` in `pr-evidence/native-capture-negative.json`. The run exited 1 and did not mistake the transport-closed path for a normal PASS.

The historical v5 sanitizer results are also retained. Missing or empty token sources each failed with exit 2; with a valid token, exit 0, no remaining replacement markers, and result permission `0600` were confirmed.

## 6. Independent review findings and dispositions

| Finding | Disposition |
|---|---|
| Unclear whether the cursor upper bound is an actual index range in planner evidence | Added source-locked deep plan annotation and confirmed the `<= cursor timestamp` index condition for late-page session/decision in the raw plan. |
| Trace sanitizer could treat a missing token as success | Missing/empty token fails closed with exit 2; valid-token redaction and residual-marker checks were separated. |
| DOM visibility probe could be mistaken for native hidden-tab evidence | The DOM probe is limited to UI-11; PERF-06 was separated into a real `native_tab_switch` 600-second run. |
| Documented test target/module path could differ from the actual collection path | Recorded the `tests/all.rs` integration target and the actual module path. |
| Overflow private child could be mistaken for a separate test or omitted | One public parent executes the exact private child and confirms `1 passed`. The child is not counted as a separate PASS. |
| Proposal to drop existing indexes to reduce migration risk | Rejected. The additive policy is kept; only the latest-base numbers were adjusted to `0118`/`0086`. |

Findings on historical source and evidence were incorporated. Re-execution and documentation of the latest-base integration are complete; only PR creation and PR CI are pending.

## 7. Reproduction manifest and raw evidence

The historical worktree short head is `9c420`. The evidence manifest has no full 40-character Git commit, so it is not inferred. The latest integration base is `4ed7cc8c`.

### 7.1 Binary and fixture

| Artifact | SHA-256 |
|---|---|
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-baseline` | `059e3616bd8f5d70d4e431a90907906dbd7e0162df271f7bc71e4c23fbc8dc7e` |
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-candidate` | `b2eac6afd968ba2e50fb283f8e4838320e9663f40db4f36a7c1e61419850dfa8` |
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/fixture-manifest-v5.json` | `5f211ebb11bba581db798eddab58157891a623d760d0e59210fc12505644ae08` |

The manifest SHA used before native and plan capture is `72b0e70627522486fa1d6c4b90c0bbadc1f0e16be742561e9661a62ed8e58b8b`. After the reviewed-browser reseed, the manifest file SHA changed to `5f211e...` in the table above, but the logical fixture SHA is identical across all four datasets: `feb5749814321581d957d8fc8bddd182ddc645e614f4d2d12f1b66b3c03ad341`.

### 7.2 API, planner, native

| Artifact | SHA-256 |
|---|---|
| `evidence-v5/comparison-sqlite.json` | `893581e23c12d83891422a3afc86d6a760ac4c7ed1f5ce0f53b7bc13a23be80d` |
| `evidence-v5/comparison-postgres.json` | `318e45c22b9da1bcfd1acb5dbffef3e5e3ee84c4bbbfbd4a26b883c523abe828` |
| `evidence-v5/api-sqlite.jsonl` | `3cc3f0e68d70cfdcc1bdfeb574266981592bfe8495aed90f0a39d82b411bec57` |
| `evidence-v5/api-postgres.jsonl` | `b71edc387fc686580f27b1e528dbbc7f3e3ab6d279d2054ab3f366c65b425f56` |
| `evidence-v5/concurrent-three-read-comparison.json` | `32fe40182148735095f9d80f79df9d41a57eae5d9b1a97420fb6834509ba3455` |
| `evidence-v5/read-plans-reviewed/run-manifest.json` | `cad9655ad88de8a0d967bb46918743d28beca208d501459378fcb1249e74d2de` |
| `evidence-v5/read-plans-reviewed/index-adoption-summary.json` | `da0e74aa6d97a30604ad5cf556e60a390c6c720dc93ca42b926bf13933b733f7` |
| `evidence-v5/perf06-native.json` | `2b1047a8764de035e0cf6cdc34800f872ab5a0aba4f639f7987277dca4444252` |
| `evidence-v5/sanitizer-check.json` | `8349718040b6a9aacfe3e26acf2c6ca46c1e49f650d4d03ff404fc0df493d6ab` |
| `evidence-v5/native-capture-negative-restore.json` | `eb41c587ecd801914106cb37c3664e8475312705eaee1e668a1ddd4f29a63fd8` |
| `evidence-v5/native-capture-negative.json` | `c11b678f5824444d681d033a9fa0591971716415428475650284cc2498f4b7f0` |
| `evidence-v4/postgres-index-build-rehearsal-large.json` | `ab7aec941be07f521cffc177e1e66a27ff7be7092563a3fa9389a798e546977b` |
| `evidence-v4/proxy-smoke.json` | `8a2705192d0b97774cb65f63db6d05c871c09dcbdcc7df7961a18db591f04e4d` |

### 7.3 Reviewed browser raw records

| Dataset | Cases | Records | SHA-256 |
|---|---:|---:|---|
| `reviewed-browser/sqlite-baseline/browser-sqlite-baseline.jsonl` | 23 | 332 | `cb731e928355b15b1b6547a897300a20bf71701211df598fab15cf9a82c2e8a2` |
| `reviewed-browser/sqlite-candidate/browser-sqlite-candidate.jsonl` | 23 | 336 | `4f97e985d8654b7aba8184b0dfe6b3472160266190f6e8e30f2e3af0dfb4b4a3` |
| `reviewed-browser/postgres-baseline/browser-postgres-baseline.jsonl` | 23 | 333 | `51a2eb191e50944a44387ff07f901608113dc9cc9a561914ad895e5c3f53bfdb` |
| `reviewed-browser/postgres-candidate/browser-postgres-candidate.jsonl` | 23 | 336 | `c290042d4ad6faec3625e6c233c30d26dedb30cdfb8f093d26afe98aa6e43896` |

### 7.4 Latest-base `4ed7cc8c` raw evidence

Latest-base runs are under `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence`. Binary snapshot build metadata records both binaries as development debug builds.

| Artifact | Raw records | SHA-256 |
|---|---:|---|
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-pr-baseline` | — | `beaa4468599f9b9b4117751d978fa16847c7db9dbfeab97faa0cc8b8122206cb` |
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-pr-candidate` | — | `8ebeae8dc1c4fe4a80c5fc26154b78096a7f3c49a052a691ae737650f6593bc1` |
| `pr-evidence/performance-sqlite.json` | summary | `375f8ee36ceae1492ab6e86c1a842d81b560d9d64d5bc4d61fa155803b3583fd` |
| `pr-evidence/api-performance-sqlite.jsonl` | 389 | `1369dff0fb0bdb15c5521d8fa8116daa89bbb777775c1a4bd5ff24e8968f853c` |
| `pr-evidence/performance-postgres.json` | summary | `efff4754ee9bf102385775b4a1dbef88546207b86f974b7dc76367a8e4169495` |
| `pr-evidence/api-performance-postgres.jsonl` | 389 | `b6e3f65ac05989b5c4bdb1651fd93ba7a9e54407a5e97feed8e273a606291726` |
| `pr-evidence/semantics-sqlite.json` | summary | `4e5a3e9f5534fe05abbb61f076295efabd0a1cdc230ffdeb802ed45980eb6cb2` |
| `pr-evidence/api-semantics-sqlite.jsonl` | 93 | `434e3fc2dbb394e0dce361cfee6e3fc4a577e7d906d298f7432f36ef08da8552` |
| `pr-evidence/semantics-postgres.json` | summary | `cbd39ac726bf4d32df892bbaf2649705c73b48e323b81bfda0cd9cd2166d2d1c` |
| `pr-evidence/api-semantics-postgres.jsonl` | 93 | `bbc83873a21106fa8f64c9bfbe0504ff20d5e7ec08d5a366c1643395b883a75f` |
| `pr-evidence/verified-browser-matrix.json` | 4 × 23 cases | `793e3b6fab1daef86b69bfb792d76ed3c2a37460d23ab07897806119f5374a38` |
| `pr-evidence/sqlite-baseline/browser-sqlite-baseline.jsonl` | 333 | `44a8d2ea872088ec6add64a0813ddd4acc2bbe82bda5984b403ec78403b3105f` |
| `pr-evidence/sqlite-candidate/browser-sqlite-candidate.jsonl` | 335 | `1de631553f505372b850a1f4529e9d69efb6ef8064bc1067feb7f6730821874b` |
| `pr-evidence/postgres-baseline/browser-postgres-baseline.jsonl` | 333 | `57638112c02bf5ff13217209f324e0e6eb724c95f1c96a0abae3c971a8a17cb7` |
| `pr-evidence/postgres-candidate/browser-postgres-candidate.jsonl` | 336 | `d6901e510534e7d3abcd9f302269d9e0ea75f367675b3ca8f08814aae59a3b8b` |
| `pr-evidence/native-hidden.json` | 209 embedded | `c0bb8d4ab46402674c64bcab788815d5fc42cb001609d050381c68190120f6ed` |
| `pr-evidence/native-capture-negative.json` | 0 | `5aa77e314b952defe524a61bce119278e6a7e633ca8a0f69a69874b1f4b7a007` |
| `pr-evidence/proxy-smoke.json` | summary | `c237728025b6f2a11fe760f3d3ff2a786bf7c965695f715f3913ce4bae036936` |
| `pr-evidence/credential-scan.json` | 227 files / 92 trace ZIPs | `fa4d7daf0e89b47dd719372cc3bb17821356eadcb4292f8e0d11e8a299b05e53` |

Browser actual-stack manifests use fixture SHA `f7a84dfda4d8b499f235749d3f1becd4a03f5a0bc1aabfcb0d665c07efc27a33` for all four datasets and record the live request clock separately from fixture anchor `1789440613000`.

## 8. Deployment and remaining gates

The latest-base loopback smoke recorded HTTP 200 for a real client → proxy `54471` → fake upstream `19080` request, HTTP 200 for the credential revoke API, and HTTP 401 for a request after revoking the same key. `production_calls=0`. The credential scan confirmed 0 actual fixture credential matches across 227 files and 92 trace ZIPs.

The remaining gates are:

1. Main confirms these two documents as a single source and makes the final commit of only the 23 files in this performance change.
2. Create the PR.
3. Run CI on the created PR and check the result.

The current results include no claim of production deployment, merge, PR creation, or PR CI PASS. Application latest-base verification is PASS, but the PR has not been created and CI is pending.
