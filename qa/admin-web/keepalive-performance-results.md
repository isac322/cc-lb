# Cache Keepalive 읽기 성능·동등성 검증 결과

- 기록일: 2026-09-15 UTC
- 검증 대상: origin/master `4ed7cc8c` latest-base integration; historical candidate short head `9c420`은 비교 자료로 유지
- 현재 상태: latest-base 57/57 QA PASS, browser 92/92 PASS, Rust 724 PASS, Admin Web 692 PASS
- PR/CI 상태: PR 미생성, CI pending
- 배포 상태: 생산 배포 없음

## 1. 결론과 해석 범위

Latest-base integration은 Principal Cache Keepalive summary, list, detail의 응답 계약을 유지하면서 대규모 decision history를 매 요청마다 전부 materialize하던 읽기 경로를 제거했다. 24h/7d cursor에는 첫 페이지의 시간 범위를 고정하는 `horizon` tag를 추가했다.

변경 없는 응답은 baseline과 exact하게 일치했다. Cursor는 기존 5개 필드가 exact했고 candidate에 `horizon: "24h"|"7d"|"all"` 필드만 추가됐다. 승인된 동작 차이는 시간 경과 후 24h/7d page 2가 baseline에서 HTTP 400이던 버그를 candidate에서 HTTP 200으로 수정한 것뿐이다.

Origin/master `4ed7cc8c` 위 통합, PostgreSQL `0118`과 SQLite `0086` migration 번호 조정, Rust/Admin Web/API/browser/native/proxy latest-base 검증은 완료됐다. Application QA는 57 PASS, 0 FAIL, 0 BLOCKED, 0 NOT_RUN이다. PR은 아직 생성되지 않았으므로 PR CI는 통과 상태가 아니라 pending이다. Historical `9c420` 결과와 artifact는 아래 별도 구역에 그대로 남긴다.

## 2. 기능 동등성과 회귀 결과

### 2.1 57개 QA 상태

| 묶음 | ID | Historical 결과 | Latest-base 결과 |
|---|---|---:|---:|
| Storage | ST-01..ST-16 | 16/16 PASS | 16/16 PASS |
| API/semantic | SEM-01..SEM-12 | 12/12 PASS | 12/12 PASS |
| Cursor bug fix | CUR-01..CUR-03 | 3/3 PASS | 3/3 PASS |
| Admin Web | UI-01..UI-12 | 12/12 PASS | 12/12 PASS |
| Performance/evidence | PERF-01..PERF-06 | 6/6 PASS | 6/6 PASS |
| Transition/E2E | FLOW-01..FLOW-08 | 8/8 PASS | 8/8 PASS |
| 합계 | 57 | 57/57 PASS | 57/57 PASS |

행별 명령, oracle, evidence anchor, 현재 상태는 `qa/admin-web/keepalive-performance-regression.md`에 있다.

### 2.2 실제 test 수집 경로

Rust test는 각 crate의 실제 target과 module path로 수집했다.

- SQLite storage: `cargo test -p cc-lb-storage-sqlite --test integration 'cache_keepalive_session_reads' -- --nocapture`
- PostgreSQL storage: `cargo test -p cc-lb-storage-postgres --test integration 'cache_keepalive_session_reads' -- --nocapture`
- Admin HTTP/cursor: `cargo test -p cc-lb-admin --test integration 'cache_keepalive_contracts::read_equivalence' -- --nocapture`와 `cache_keepalive_contracts::cursor_window`
- Admin view: `cargo test -p cc-lb-admin --lib 'cache_keepalive_view::tests' -- --nocapture`
- Browser: `crates/cc-lb-admin/web/qa/keepalive-performance-regression.spec.ts`, 실제 compiled server와 Vite proxy를 사용하는 opt-in Playwright config

`pnl_converter_invalid_input_is_http_400`은 public parent test 하나가 동일 integration binary의 exact private child 실행을 확인한다. Private child를 별도 test나 추가 PASS로 세지 않았다.

Latest-base 전체 Rust 기록은 `artifact://704`의 724 passed, 7 pre-existing ignored이며 mapper 경로를 포함한다. Clippy `--all-targets -D warnings`도 PASS했다. Admin Web은 72 test files, 692 tests와 build/lint/typecheck가 모두 PASS했다. Latest-base browser 기록은 `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`의 dataset별 23 case, 합계 92/92 PASS다. Historical Rust `artifact://604`의 713 passed/7 skipped와 historical browser 92/92도 비교 자료로 유지한다.

### 2.3 응답 계약

- `FILTERS`: 두 엔진 모두 21개 horizon/filter 조합이 baseline과 equal이다.
- `SEMANTIC`: 두 엔진 모두 15개 대표 semantic request가 equal이다.
- Semantic decoded original-field SHA는 SQLite `c18457839b91d5174db9e011789d2f1597591a3892b8590659ef844b593267e9`, PostgreSQL `b95e3c179690b8e9b526d9818de9478e611295fbd2c84532b2de1229432750a4`다.
- Baseline original cursor SHA는 SQLite semantic `70ec17f468a2bc310ebd703251ca585e29eb534e86a1e78f6b60fca5889abcc2`, PostgreSQL semantic `6e7b66667331385331460481bc9684ce4475b02b3a4bd04128c889c29f4d5a0d`다.
- Candidate cursor chain은 5 page를 유지했고 `24h`, `7d`, `all` tag를 확인했다.
- 24h time advance는 SQLite/PostgreSQL 모두 baseline 400, candidate 200이며 frozen `horizon_start_ms`는 각각 `1789355647000`, `1789355649000`이다.
- 7d time advance는 SQLite/PostgreSQL 모두 baseline 400, candidate 200이며 frozen `horizon_start_ms`는 각각 `1788837247000`, `1788837249000`이다.
- Selected row, 현재 summary session/turn, 현재 page row의 corruption은 기존 오류 mapping을 유지한다. 좁은 query가 더 이상 읽지 않는 unrelated 과거 corrupt decision 때문에 전체 요청이 실패하던 부수 효과는 보존하지 않는다. 이것은 `SEM-12`의 승인된 scope 차이다.

### 2.4 Latest-base 성능 결과

아래 수치는 `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/performance-{sqlite,postgres}.json`에서 직접 파싱했다. 각 percentile 행은 `n=30`이며 p50, p95, max를 반올림 없이 파일 값 그대로 옮겼다. 실행 바이너리는 snapshot build metadata가 가리키는 개발용 debug build다. 첫 요청은 별도 순서 표지일 뿐 OS/DB cache를 비운 cold-cache 증거가 아니다.

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

PERF-02는 1,205 rows를 25 pages로 순회했고 ordered ID SHA는 두 엔진 모두 `a0da2c69ead37b119d24b062ace4f162559a28028565a1dae380e7d89f53453a`다. SQLite first baseline/candidate는 8735.984/25.544ms, candidate middle/terminal은 19.069/18.911ms다. PostgreSQL first baseline/candidate는 2694.921/21.281ms, candidate middle/terminal은 18.139/20.071ms다.

## 3. Historical 성능 결과 (`9c420`)

모든 비교는 같은 host와 engine에서 동일 fixture와 빌드 설정을 사용한 개발용 debug 바이너리로 실행했다. 첫 요청은 별도 기록했지만 OS/DB cache를 강제로 비운 cold-cache 실험은 아니다. 이 결과는 release 바이너리의 절대 지연이나 provider·생산 runtime 전체의 개선율을 뜻하지 않는다.

이 절의 표는 historical `9c420` 실행값이다. Latest-base 값은 §2.4를 사용한다.

Fixture는 D-scale에 old decisions 100,000개, 전체 decisions 100,750개를 포함한다. S/T scale은 sessions 2,000개와 session당 turns 5개, 즉 대상 turns 10,000개다. Manifest 전체 turns 10,074개에는 다른 fixture의 turn도 포함된다.

### 3.1 PERF-01 summary, D=100,000

각 행은 반복 sample `n=30`의 wall time이다. 첫 sample은 phase별 1회 별도로 raw record에 남겼으며 아래 percentile에는 포함하지 않았다. Raw record의 cold/warm label은 요청 순서 구분이며 실제 DB cache reset 증거가 아니다.

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 8294.670 | 11526.521 | 12812.671 |
| SQLite | candidate | 30 | 18.306 | 37.281 | 45.702 |
| PostgreSQL | baseline | 30 | 3828.495 | 6260.857 | 7066.382 |
| PostgreSQL | candidate | 30 | 21.611 | 38.391 | 45.549 |

두 엔진 모두 fixed-snapshot body가 exact했다.

### 3.2 PERF-02 list pagination

| Engine | Baseline first ms | Candidate first ms | Candidate middle ms | Candidate terminal ms | Rows | Pages |
|---|---:|---:|---:|---:|---:|---:|
| SQLite | 7924.494 | 19.387 | 17.706 | 17.020 | 1205 | 25 |
| PostgreSQL | 4990.123 | 26.028 | 16.987 | 15.288 | 1205 | 25 |

두 엔진의 ordered ID SHA는 `a0da2c69ead37b119d24b062ace4f162559a28028565a1dae380e7d89f53453a`로 같았다. Candidate는 wire `next_cursor=null`까지 25 page를 순회했다. Page 2의 baseline 400과 candidate 200은 승인된 cursor bug delta다. Candidate response hash가 baseline과 달라지는 list record는 새 cursor의 `horizon` 필드 때문이다. Decoded original 5개 필드는 exact하다.

### 3.3 PERF-03 direct detail, D=100,000

각 detail case와 phase의 warm sample은 `n=30`이다.

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

S/T scale은 D=100,000 제거 효과와 별개로 session/turn 처리 비용을 확인한다. Candidate가 이 경로를 완전한 상수 시간으로 바꿨다는 주장은 하지 않는다.

| Engine | Phase | n | p50 ms | p95 ms | max ms |
|---|---|---:|---:|---:|---:|
| SQLite | baseline | 30 | 148.435 | 151.589 | 152.289 |
| SQLite | candidate | 30 | 143.767 | 146.190 | 147.095 |
| PostgreSQL | baseline | 30 | 131.205 | 141.248 | 142.286 |
| PostgreSQL | candidate | 30 | 127.868 | 138.382 | 139.790 |

### 3.5 세 읽기 동시 요청

Summary, all-horizon list, session detail을 concurrency 3으로 동시에 요청했다.

| Engine | Phase | Batch wall ms | Summary ms | List ms | Detail ms | HTTP |
|---|---|---:|---:|---:|---:|---|
| SQLite | baseline | 9742.333 | 9632.309 | 9741.547 | 9397.885 | 200/200/200 |
| SQLite | candidate | 27.421 | 25.791 | 26.808 | 2.568 | 200/200/200 |
| PostgreSQL | baseline | 5535.156 | 5397.065 | 5534.599 | 5196.271 | 200/200/200 |
| PostgreSQL | candidate | 25.795 | 18.682 | 25.183 | 5.131 | 200/200/200 |

Summary와 detail body SHA는 baseline/candidate가 exact했다. List는 candidate cursor의 승인된 `horizon` 추가 때문에 raw body SHA가 다르다.

## 4. Planner와 index 판단

Reviewed plan manifest에는 실제 124개 artifact가 있다. 각 artifact의 source path, source file SHA, SQL constant SHA가 비어 있지 않고 source lock 검증을 통과했다. Plan tree는 root 한 단계만 보지 않고 중첩 node까지 수집했다.

PostgreSQL D-scale late-page plan에서 session과 decision index condition은 모두 `principal_id`, horizon lower bound, cursor timestamp upper bound를 index range에 포함했다. 대표 plan의 execution time은 0.135ms였다. Buffer는 shared hit 42 blocks, shared read 0 blocks, physical read 0 blocks였다. 이것은 warm-cache hit 기록이며 디스크 read가 42 blocks였다는 뜻이 아니다. SQLite `EXPLAIN QUERY PLAN`과 Python SQLite API는 cache hit와 physical read block 수를 제공하지 않으므로 같은 수치를 만들지 않았다.

일부 plan에는 bounded result에 대한 sort가 남아 있다. PostgreSQL D-scale summary session plan도 실제 861 session row를 처리했다. 따라서 결과를 모든 source와 모든 scale에서 완전한 상수 시간이라고 표현하지 않는다.

기존 index를 drop하는 제안은 거부했다. 정책은 additive index만 허용하며 raw row backfill, 삭제, column 의미 변경, 강제 `COLLATE C`를 허용하지 않는다. Historical evidence에서는 Cache Keepalive index가 PostgreSQL `0117`, SQLite `0085`였지만 최신 base가 그 번호를 사용하므로 최종 통합명은 `0118`/`0086`이어야 한다.

Disposable PostgreSQL rehearsal은 decision rows 302,250개와 decision table 103,628,800 bytes, 즉 98.83 MiB에서 exact CREATE INDEX 두 문장을 측정했다. 측정값은 3.357ms와 217.322ms였다. 관찰 lock은 각 대상 table의 `ShareLock`과 `AccessShareLock`이며 모두 granted였다. Transaction은 rollback했다. 이 결과로 생산 배포 시간이나 live-writer 무중단을 주장하지 않는다.

## 5. Browser, native visibility, sanitizer

Latest-base real-browser matrix는 4개 dataset에서 각각 23 case를 통과했다. 합계 92/92다. Admin Web 제품 코드는 바꾸지 않았고 polling 5초, AbortSignal 미전달, query-key 격리, drawer/detail 동작을 baseline과 비교했다.

Latest-base PERF-06은 page script로 `visibilityState`를 바꾼 DOM probe가 아니다. 기존 raw Chromium에 CDP로 연결하고 blank control tab을 앞으로 가져오는 `native_tab_switch` 방식이다.

- 상태: PASS
- 전체 duration: 846,006ms
- hidden steady: 600,002ms
- target: SQLite/PostgreSQL × baseline/candidate 4개
- hidden 이후 새 GET: 네 target 모두 0건
- manual DOM cross-client match: before와 after 모두 true
- embedded raw records: 209개, record ID unique
- harness가 만든 tab 5/5 cleanup, Playwright CDP connection 종료, raw browser process 미종료
- cleanup failures: 없음

Latest-base fault injection은 `pr-evidence/native-capture-negative.json`에 `status: FAIL`, `native network evidence capture failed`, Playwright connection 종료, raw browser process 미종료, `cleanup_failures=[]`를 기록했다. 실행은 exit 1이었고 transport-closed 경로를 정상 PASS로 오인하지 않았다.

Historical v5 sanitizer 결과도 유지한다. Token source가 없거나 빈 경우 각각 exit 2로 실패했고, 유효 token에서는 exit 0, 치환 marker 잔존 false, 결과 permission `0600`을 확인했다.

## 6. 독립 검토 지적과 처분

| 지적 | 처분 |
|---|---|
| Cursor upper bound가 planner evidence에서 실제 index range인지 불명확 | Source-locked deep plan annotation을 추가하고 late-page session/decision의 `<= cursor timestamp` index condition을 raw plan에서 확인했다. |
| Trace sanitizer가 token 부재를 성공으로 처리할 수 있음 | Missing/empty token을 exit 2로 fail closed하고 valid-token redaction과 잔존 marker 검사를 분리했다. |
| DOM visibility probe를 native hidden-tab 증거로 오인 | DOM probe는 UI-11로 제한하고 PERF-06은 실제 `native_tab_switch` 600초 run으로 분리했다. |
| 문서의 test target/module path가 실제 수집 경로와 다를 수 있음 | `tests/all.rs` integration target과 실제 module path를 기록했다. |
| Overflow private child가 별도 test로 오인되거나 누락될 수 있음 | Public parent 하나가 exact private child를 실행하고 `1 passed`를 확인한다. Child는 별도 PASS로 세지 않는다. |
| 기존 index drop으로 migration 위험을 줄이자는 제안 | 거부했다. Additive policy를 유지하고 latest-base 번호만 `0118`/`0086`으로 조정한다. |

Historical source와 evidence에 대한 지적은 반영됐다. Latest-base 통합본의 재실행과 문서 반영은 완료됐고, PR 생성과 PR CI만 pending이다.

## 7. 재현 manifest와 raw evidence

Historical worktree short head는 `9c420`이다. Evidence manifest에는 full 40-character Git commit이 없으므로 이를 추정하지 않는다. 최신 통합 base는 `4ed7cc8c`다.

### 7.1 Binary와 fixture

| Artifact | SHA-256 |
|---|---|
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-baseline` | `059e3616bd8f5d70d4e431a90907906dbd7e0162df271f7bc71e4c23fbc8dc7e` |
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/cc-lb-candidate` | `b2eac6afd968ba2e50fb283f8e4838320e9663f40db4f36a7c1e61419850dfa8` |
| `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/fixture-manifest-v5.json` | `5f211ebb11bba581db798eddab58157891a623d760d0e59210fc12505644ae08` |

Native와 plan capture 전에 사용한 manifest SHA는 `72b0e70627522486fa1d6c4b90c0bbadc1f0e16be742561e9661a62ed8e58b8b`다. Reviewed browser 재seed 후 manifest file SHA는 위 표의 `5f211e...`로 바뀌었지만 logical fixture SHA는 네 dataset 모두 `feb5749814321581d957d8fc8bddd182ddc645e614f4d2d12f1b66b3c03ad341`로 같다.

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

Latest-base 실행은 `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence` 아래에 있다. Binary snapshot build metadata는 두 바이너리를 개발용 debug build로 기록한다.

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

Browser actual-stack manifests use fixture SHA `f7a84dfda4d8b499f235749d3f1becd4a03f5a0bc1aabfcb0d665c07efc27a33` for all four datasets and record live request clock separately from fixture anchor `1789440613000`.

## 8. 배포 및 남은 gate

Latest-base loopback smoke는 real client → proxy `54471` → fake upstream `19080` 요청 HTTP 200, credential revoke API HTTP 200, 같은 key의 revoke 이후 요청 HTTP 401을 기록했다. `production_calls=0`이다. Credential scan은 227 files와 92 trace ZIPs에서 actual fixture credential match 0건을 확인했다.

남은 gate는 다음과 같다.

1. Main이 이 두 문서를 하나의 source로 확인하고 이번 성능 변경 23개 파일만 final commit한다.
2. PR을 생성한다.
3. 생성된 PR의 CI를 실행하고 결과를 확인한다.

현재 결과에는 production deployment, merge, PR 생성, PR CI PASS 주장이 포함되지 않는다. Application latest-base 검증은 PASS지만 PR은 아직 생성되지 않았고 CI는 pending이다.
