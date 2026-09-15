# Cache Keepalive 성능 회귀·동등성 QA 계약

<!-- historical candidate 9c420과 origin/master 4ed7cc8c latest-base integration의 검증 결과를 구분해 기록한다. PR은 아직 생성되지 않았고 CI는 pending이다. -->

- 기록 기준: 2026-09-15 UTC
- 상태: **latest-base `4ed7cc8c` QA 57/57 PASS / browser 92/92 PASS / Rust 724 PASS / Admin Web 692 PASS**
- 범위: Principal Cache Keepalive summary, list, detail의 backend-only 읽기 최적화, 기존 Admin Web 동작 보존, 24h/7d cursor의 첫 페이지 시간 범위 고정
- 승인된 버그 수정: 24h/7d의 다음 페이지가 시간 경과로 새 `horizon_start_ms`를 계산해 HTTP 400이 되던 기존 동작을 수정한다. 새 cursor는 `horizon` tag로 요청 scope를 검증하고 첫 페이지 cutoff를 재사용한다.
- 제외: 프런트엔드 제품 코드, 5초 polling, AbortSignal 전달, retention 정책, 재무식, async rollup, cache, 최신성 정책 변경
- 검증 해석: 각 행은 `PASS@9c420` historical evidence와 `PASS@4ed7cc8c` latest-base evidence를 함께 표시한다. latest-base integration의 QA는 57 PASS, 0 FAIL, 0 BLOCKED, 0 NOT_RUN이다. PR은 아직 생성되지 않았고 PR CI는 실행 전 pending이다. 생산 배포는 수행하지 않았다.

## 1. 고정 구현 소유권과 계약

| 계층 | 구현 소유 파일·심볼 | 검증 소유 파일 |
|---|---|---|
| Storage public API | `crates/cc-lb-storage-api/src/cache_keepalive_sessions/reads.rs`: `CacheKeepaliveSummaryInput { sessions: Vec<CacheKeepaliveSessionListItem>, recent_decisions: u64 }`, required `CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input(&self, principal_id: &str, cutoff_ms: u64)`, required `CacheKeepaliveSessionReadStore::get_cache_keepalive_list_item(&self, principal_id: &str, id: &str)` | 같은 파일의 cursor/type 단위 검증과 양 adapter 통합 테스트. 새 method에 default full-scan fallback을 두지 않는다. |
| PostgreSQL | `crates/cc-lb-storage-postgres/src/adapter/cache_keepalive_session_reads.rs`: summary input, direct candidate lookup, bounded list query | `crates/cc-lb-storage-postgres/tests/cache_keepalive_session_reads.rs`, 통합 target `crates/cc-lb-storage-postgres/tests/all.rs` |
| SQLite | `crates/cc-lb-storage-sqlite/src/adapter/cache_keepalive_session_reads.rs`: PostgreSQL과 같은 계약 | `crates/cc-lb-storage-sqlite/tests/cache_keepalive_session_reads.rs`, 통합 target `crates/cc-lb-storage-sqlite/tests/all.rs` |
| Admin route/view | `crates/cc-lb-admin/src/v1/principal_cache_keepalive.rs`: `list_cache_keepalive`, `get_cache_keepalive_detail`; `.../query.rs`: tagged cursor decode/encode와 첫 페이지 cutoff 재사용; `.../view.rs`: summary fold, activity batch, row/detail 변환 | `crates/cc-lb-admin/tests/cache_keepalive_contracts.rs`, `.../fixtures.rs`, `crates/cc-lb-admin/src/cache_keepalive_view_tests.rs`, `cache_keepalive_view_activity_tests.rs`, `cache_keepalive_view_status_tests.rs` |
| Admin Web, 제품 무수정 | `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts`, `queries.ts`, `usePolledData.ts`, `components/principals/cache-keepalive/*` | 기존 component tests와 opt-in 전용 `crates/cc-lb-admin/web/qa/keepalive-performance-regression.spec.ts`; 제품 코드는 바꾸지 않고 실제 UI pagination으로 cursor 수정도 검증한다. |
| 조건부 planner index | 최신 base 통합 번호 PostgreSQL `crates/cc-lb-storage-postgres/migrations/0118_*.sql`; SQLite `crates/cc-lb-storage-sqlite/migrations/0086_*.sql` | raw `EXPLAIN (ANALYZE, BUFFERS)`와 SQLite `EXPLAIN QUERY PLAN`이 필요성을 입증할 때만 추가한다. `COALESCE(last_message_at_ms, ts * 1000)`와 실제 computed order key에 맞춘 expression index만 허용한다. raw-data backfill·삭제·컬럼 의미 변경은 금지한다. 기본 collation을 강제 변경하지 않는다. |

고정 동작은 다음과 같다.

1. Summary session은 기존 전역 순서 `last_message_at_ms DESC`, `session_key_hash ASC`로 읽어 기존 Rust fold, turn별 정수 micro-dollar 절삭, pending, unknown pricing, saturation을 그대로 재사용한다. `recent_decisions`는 같은 5분 포함 경계(`>= cutoff_ms`), late-turn anti-join, `COALESCE`를 쓰는 `u64` count다.
2. Detail은 session과 visible decision 두 후보를 principal+bare ID로 조회한다. timestamp가 큰 후보가 우선이며 동률은 computed `entry_id`의 DB 기본 collation 정렬 결과를 따른다. 현재 ASCII namespace에서는 decision이 session보다 먼저다. late turn이 있는 decision은 후보가 아니다.
3. List는 기본 limit 50, 최대 100, exact cursor seek, nullable timestamp fallback, namespace order, unknown-price/nullability, error/reason, rounding을 보존한다. 새 HTTP cursor는 기존 storage cursor 5개 필드에 `horizon: "24h"|"7d"|"all"`을 flatten해 넣는다. Tagged cursor는 principal/horizon/filter와 bounded/all cutoff shape를 검증한 뒤 첫 페이지 `horizon_start_ms`를 재사용한다. tag가 없는 기존 cursor는 현재 cutoff와 exact한 경우에만 기존 validation을 통과한다. terminal pagination은 임의 20-page cap이나 DOM의 disabled 상태가 아니라 wire `next_cursor == null`로 끝난다.
4. Summary/list/detail은 기존에도 한 snapshot으로 묶이지 않았다. 정상 fixed fixture 결과는 exact해야 하고, 동시 전이는 다음 5초 poll에서 수렴해야 한다. cursor chain의 list cutoff만 첫 페이지에 고정하며, 각 요청의 summary는 그 요청의 현재 `now`로 다시 계산한다. 구현상 필요하면 한 요청 내부에 짧은 read transaction을 사용할 수 있으나 제품 최신성 정책은 강화하지 않는다.
5. 좁은 query가 읽지 않는 unrelated 과거 corrupt decision 때문에 전체 요청이 실패하던 부수 효과는 보존 대상이 아니다. 반면 요청 대상 row, summary에 포함되는 session/turn, 현재 page row의 변환 오류는 기존 mapping을 유지한다.
6. `dollars_from_micros`가 만드는 `StorageError::InvalidInput { field: "cache_keepalive_pnl", ... }`은 `principal_cache_keepalive::storage_error`에 의해 HTTP **400** `invalid_input`으로 변환된다. `StorageError::Corrupted`는 HTTP **500** `storage_error`다.

## 2. 실행 환경과 증거 형식

### 2.1 저장소·Rust 명령

이 workspace는 각 crate가 `autotests = false`이고 `tests/all.rs`에 통합되어 있다. 따라서 임의의 `--test cache_keepalive_session_reads` 예시는 사용하지 않는다. 먼저 실제 module 전체를 실행해 test target과 module 경로가 유효한지 확인한다.

```bash
# SQLite storage module
cargo test -p cc-lb-storage-sqlite --test integration \
  'cache_keepalive_session_reads' -- --nocapture

# PostgreSQL storage module. 로컬 전용 격리 DB 예시.
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

단일 test 재현은 module 전체 실행 다음에만 아래 full path로 좁힌다.

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

Cargo는 filter가 한 test도 선택하지 않아도 exit 0일 수 있다. 따라서 선택한 test binary가 `running 0 tests`를 출력하거나 최종 결과가 `0 passed`이면 증거를 거부한다. 이 경우 module 전체 명령을 다시 실행하고 출력된 full test path를 그대로 복사해 `--exact` 명령을 고친다.

PostgreSQL test launcher가 인정하는 DSN 이름은 `CI_POSTGRES_URL` 또는 `PG_URL`이다. 전체 CI 호환 환경은 `CI_POSTGRES_URL`, `PG_URL`, `DATABASE_URL`, `DATABASE_URL_TEST`를 같은 격리 DSN으로 설정하고 `SQLX_OFFLINE=true`를 둔다.

### 2.2 실제 compiled server + browser + safe fake upstream

- baseline binary: `$KEEPALIVE_SCRATCH_DIR/bin/cc-lb-baseline`
- candidate binary는 같은 host toolchain과 build profile로 만들고 `$KEEPALIVE_SCRATCH_DIR/bin/cc-lb-candidate`에 둔다.
- 실제 CLI: `cc-lb serve --config <PATH> --data-dir <PATH>`
- fake upstream은 loopback에만 bind한다: `cargo run -p fake-anthropic -- --port 19080`
- PostgreSQL은 loopback에 publish한 격리 container와 QA 이름 DB만 사용한다. 예시는 container `cc-lb-keepalive-qa-postgres`, databases `keepalive_baseline`/`keepalive_candidate`, user `cclb`를 쓴다.

Candidate 구현 후 Main은 각 격리 DB에 대해 server를 한 번 시작해 현재 migration을 적용하고 종료한다. 그 다음 project-owned runner의 `prepare`로 deterministic fixture를 seed하고, baseline/candidate server와 Vite의 생명주기를 직접 소유한다. Runner는 daemon을 시작하거나 종료하지 않는다. Repository 밖 scratch에 남은 이전 runner 사본은 과거 실행 보관물일 뿐이며, 이후 실행은 `crates/cc-lb-admin/web/qa/keepalive-qa-runner.py`만 사용한다. Fixture 의미는 `crates/cc-lb-admin/tests/cache_keepalive_contracts/fixtures.rs`, HTTP server/auth 패턴은 `crates/cc-lb-admin/tests/admin_test_common.rs`, browser 소비 계약은 `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts`와 `queries.ts`, upstream 동작은 `tests/fixtures/fake-anthropic/src/main.rs`를 source of truth로 삼는다.

모든 `prepare` 실행에는 같은 `FIXTURE_ANCHOR_MS`를 전달한다. 이 값은 fixture row의 표시 timestamp 기준이며 server의 현재 시각을 고정하지 않는다. 각 live request의 시간 anchor는 evidence의 `started_at_utc`에 별도로 기록한다. Session의 `last_message_at_ms`, `created_at`, `first_scheduled_at`, `cache_anchor_at`은 기존 과거 표시 시각을 보존하지만, lifecycle은 모든 DB에서 동일하게 `updated_at = anchor`, `expires_at = anchor + 7 days`로 둔다. Active row는 `enqueue_state='enqueued'`이고 `run_at`은 미래이므로 정상 housekeeping과 scheduler가 검증 row를 삭제하거나 job을 시작하지 않는다. Cleanup 검증은 app housekeeping을 끄지 않고, runner의 fixture-ID 한정 `evidence --mutation cleanup`과 기존 storage test로 명시적으로 수행한다. SQLite DB는 `$KEEPALIVE_SCRATCH_DIR` 아래의 이미 migration된 파일만 허용한다. PostgreSQL은 `docker exec ... psql`로 container 내부의 QA 이름 DB만 seed한다. 기본 fixture는 UI용 최근 1,201 entry와 24h/7d/all 차이를 만드는 과거 120 entry, 별도 D=100,000 decision Principal, 별도 S/T Principal을 만든다. 따라서 UI-07은 20페이지를 넘지만 D=100,000 전체를 DOM scroll하지 않는다.

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
# 위 compare 두 개를 모든 browser mutation보다 먼저 완료한다.
# 각 browser phase 직전에는 Main이 그 phase server만 종료하고, 위의 matching
# prepare 명령에 --replace-fixture를 추가해 같은 FIXTURE_ANCHOR_MS로 재seed한 뒤
# server를 다시 시작하고 /admin/health 200을 확인한다.

cd "$REPO_ROOT/crates/cc-lb-admin/web"
KEEPALIVE_SCRATCH_DIR="$KEEPALIVE_SCRATCH_DIR" \
KEEPALIVE_PHASE=baseline KEEPALIVE_ENGINE=sqlite \
KEEPALIVE_BACKEND_URL=http://127.0.0.1:54382 CC_LB_ADMIN_URL=http://127.0.0.1:54382 \
KEEPALIVE_ADMIN_TOKEN_FILE="$TOKEN_FILE" KEEPALIVE_MANIFEST="$MANIFEST" \
KEEPALIVE_EVIDENCE_DIR="$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-baseline" \
bunx playwright test qa/keepalive-performance-regression.spec.ts \
  --config qa/keepalive-e2e.config.ts
# 같은 명령을 candidate/sqlite(54402), baseline/postgres(54392),
# candidate/postgres(54412)의 phase/engine/URL/evidence directory로 각각 실행한다.

python3 "$RUNNER" evidence --engine sqlite --phase candidate \
  --manifest "$MANIFEST" --capture-plans \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/api-sqlite.jsonl" \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-candidate/browser-sqlite-candidate.jsonl" \
  --jsonl "$KEEPALIVE_SCRATCH_DIR/evidence/sqlite-candidate/mutations-sqlite-candidate.jsonl" \
  --output-dir "$KEEPALIVE_SCRATCH_DIR/evidence/plans/sqlite-candidate" \
  --report "$KEEPALIVE_SCRATCH_DIR/evidence/report-sqlite-candidate.json"
```

Cursor 버그 수정만 재실행할 때는 성능 compare와 분리해 다음 exact scope를 쓴다.

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

Baseline의 bounded cursor page 2 HTTP 400은 원본 버그 재현 evidence이며 PASS가 아니다. Candidate만 page 2와 반복 time advance chain에서 HTTP 200, 고정 cutoff, 올바른 horizon tag를 만족해야 한다. `all` cursor의 정상 page 2와 principal/horizon/filter mismatch 400은 baseline과 candidate 모두 통과해야 한다.

`qa/keepalive-e2e.config.ts`는 일반 E2E의 mock `globalSetup`/`globalTeardown`을 사용하지 않고 `qa/keepalive-performance-regression.spec.ts`만 선택한다. 이 `qa/` 경로는 기본 `playwright.config.ts`의 `testDir: ./e2e` 밖에 있으므로 명시한 opt-in command 없이는 자동 수집되지 않는다. 기본값은 실제 Vite를 시작해 `CC_LB_ADMIN_URL`의 compiled server로 proxy한다. Main이 Vite도 직접 관리하면 `KEEPALIVE_EXTERNAL_WEB=1`과 `KEEPALIVE_WEB_URL`을 준다. Config는 phase, engine, backend, manifest, phase별 evidence directory를 명시하지 않거나 빈 값으로 주면 시작 전에 실패한다. Browser 합격은 `page.route` JSON mock이나 합성 timing으로 만들 수 없다. `KEEPALIVE_EVIDENCE_DIR`는 `$KEEPALIVE_SCRATCH_DIR/evidence/<engine>-<phase>`에 두고, generated Playwright output은 기본적으로 gitignored `target/keepalive-qa` 아래에 남긴다. Admin token은 mode 0600 secret file 또는 명시한 environment variable에서 메모리로만 읽고 argv, stdout, manifest, evidence에 기록하지 않는다. 성공·실패 trace는 저장 직후 runner의 `evidence --sanitize-trace` 경로로 Bearer token 원문을 치환하고 재검사한다. Browser suite는 fixture를 변경하므로 각 phase 직전에 해당 DB를 `prepare --replace-fixture`로 되돌린다. Browser 실행 뒤 `compare`를 다시 실행하려면 baseline과 candidate 양쪽을 모두 같은 anchor로 재prepare해야 한다.

`evidence --sanitize-trace`는 비어 있지 않은 admin token으로 trace를 치환하고 같은 token이 남지 않았음을 다시 확인한 경우에만 성공한다. Token source가 없으면 `trace redaction verification unavailable` `HarnessError`, 명시한 token source의 값이 빈 문자열이면 `trace redaction token must not be empty` `HarnessError`로 exit 2한다. 두 경우 모두 `sanitized trace` 성공 문구를 출력하지 않는다.

`document.visibilityState`와 `visibilitychange`를 page 안에서 바꾸는 probe는 polling hook의 DOM visibility 분기를 검증한다. 창을 실제로 가리거나 최소화하거나 다른 native tab/app으로 전환하는 OS-level hidden run은 브라우저 throttling과 background lifecycle까지 포함하는 별도 관찰이다. DOM probe는 UI-11 증거이며 PERF-06 native 증거를 대신하지 않는다. Historical candidate는 `evidence-v5/perf06-native.json`에서 PASS했다. Latest-base는 `pr-evidence/native-hidden.json`에서 `native_tab_switch`, 846,006ms 전체 실행, 600,002ms hidden steady, 네 target 모두 hidden 이후 새 GET 0건, 생성 tab 5/5 cleanup, `cleanup_failures=[]`로 PASS했다.

PERF-06 native 증거는 기본 Playwright test와 분리된 opt-in CLI `crates/cc-lb-admin/web/qa/keepalive-native-visibility.mjs`가 수집한다. Main은 먼저 네 dataset을 같은 fresh fixture anchor로 다시 seed하고 네 server의 `/admin/health`가 200인지 확인한 뒤, raw Chromium을 별도로 시작해 loopback CDP endpoint를 소유한다. CLI는 browser를 시작하지 않고 `chromium.connectOverCDP(endpoint, { noDefaults: true })`로 연결하며 `browser.contexts()[0]`의 default context만 쓴다. Manifest의 `sqlite:baseline`, `sqlite:candidate`, `postgres:baseline`, `postgres:candidate` `server_url` 네 개가 서로 다른 loopback origin이어야 한다.

저장소 root에서 argument 방식으로 실행한다.

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

같은 실행을 environment 방식으로 호출할 수도 있다.

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
export KEEPALIVE_SCRATCH_DIR="${KEEPALIVE_SCRATCH_DIR:-$REPO_ROOT/target/keepalive-qa}"
export KEEPALIVE_MANIFEST="$KEEPALIVE_SCRATCH_DIR/fixture-manifest.json"
export KEEPALIVE_ADMIN_TOKEN_FILE="$KEEPALIVE_SCRATCH_DIR/admin-token"
export KEEPALIVE_CDP_ENDPOINT=http://127.0.0.1:59333
cd "$REPO_ROOT/crates/cc-lb-admin/web"
node qa/keepalive-native-visibility.mjs
```

실제 manifest 파일명이 다르면 두 명령의 manifest 값만 바꾼다. `KEEPALIVE_NATIVE_VISIBLE_MS`와 `KEEPALIVE_NATIVE_HIDDEN_MS`는 각각 30,000ms와 600,000ms 이상만 허용하므로 관찰 시간을 줄여 합격시킬 수 없다. CLI는 각 origin의 local storage에 mode-0600 token file 값을 넣고 low principal을 연 뒤, 기존 spec과 같은 `cache-keepalive-card` → `Sessions` → `cache-keepalive-sessions-drawer` → `li[data-key]` row button selector로 detail을 연다. 네 tab을 각각 30초 foreground로 관찰하고 blank control tab을 앞으로 가져와 네 tab 모두가 실제 `hidden`인지 Node timer와 `page.evaluate` polling으로 확인한 다음, 600초 hidden 관찰과 tab별 30초 restore 관찰을 수행한다. Test가 만든 다섯 tab만 닫고 Main 소유 browser는 닫지 않는다.

CLI는 실행마다 `$KEEPALIVE_SCRATCH_DIR/evidence/keepalive-native-visibility-<run-id>.json` 하나를 mode 0600으로 새로 만든다. 이 JSON에는 `mechanism: "native_tab_switch"`, native visibility event 시각, request start로 입증된 initial in-flight 허용 기록, phase별 GET/status/count, redacted URL, 실제 browser timing, CDP `JSHeapUsedSize` 전후 sample, 새 restore response ID, manual DOM ready/paint frame와 four-client match, 변경되지 않은 `Document.prototype.hidden`/`visibilityState` descriptor 검증, embedded raw JSONL record와 unique record ID가 포함된다. Script는 `document.hidden`/`visibilityState`를 덮어쓰거나 synthetic `visibilitychange`를 발생시키지 않는다. Native hidden을 만들지 못하면 `FAIL`, CDP Performance 또는 실제 request timing을 제공하지 못하면 합성값으로 대체하지 않고 `BLOCKED`로 기록해 nonzero exit한다. 이 `.mjs`는 Playwright test discovery 밖에 있으므로 명시한 명령 없이 자동 실행되지 않는다.

### 2.3 모든 real E2E record의 필수 필드

각 API request와 browser observation은 독립 record다. UI paint timing을 API TTFB에 복사하지 않는다.

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

### 2.4 현재 evidence anchor

Latest-base `4ed7cc8c` evidence:

- `RUST-724`: `artifact://704`, 724 passed, 7 pre-existing ignored. Keepalive storage, Admin integration, view/economics와 mapper 경로를 포함하며 실제 `tests/all.rs` integration target 또는 lib module에서 수집됐다. Clippy `--all-targets -D warnings`도 PASS했다.
- `PR-WEB`: Admin Web 72 test files, 692 tests, build, lint, typecheck가 PASS했다. Browser raw record는 `/data/tmp/cc-lb-keepalive-qa-lhetgoq3/pr-evidence/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`이고 각 dataset 23 case, 합계 92/92 PASS다. Matrix는 `pr-evidence/verified-browser-matrix.json`이다.
- `PR-API`: `pr-evidence/{performance,semantics}-{sqlite,postgres}.json`과 `api-{performance,semantics}-{sqlite,postgres}.jsonl`. 변경 없는 응답은 exact하고 24h/7d time-advance의 baseline 400 → candidate 200만 승인된 bug delta다.
- `PLAN-v5`: historical `read-plans-reviewed/run-manifest.json`과 124개 raw plan. Latest-base query source의 의미는 같고 migration 번호만 PostgreSQL `0118`, SQLite `0086`으로 이동했다. Source path, source SHA, SQL constant SHA lock은 그대로 유효하다.
- `PR-NATIVE`: `pr-evidence/native-hidden.json`과 `native-capture-negative.json`. Native run은 `native_tab_switch`, hidden steady 600,002ms, 네 target의 hidden 이후 새 GET 0건, before/after DOM match, 생성 tab 5/5 cleanup, CDP disconnect, raw browser 미종료, `cleanup_failures=[]`를 기록한다. Negative capture는 `FAIL`, exit 1, transport closed를 기록해 false PASS를 막는다.
- `PR-PROXY`: `pr-evidence/proxy-smoke.json`. latest-base real client → proxy → loopback fake upstream은 200, revoke는 200, 같은 key의 revoke 이후 요청은 401이다. 생산 호출은 0건이다.

Historical `9c420` evidence group 이름과 자료는 비교 기준으로 유지한다:

- `RUST-713`: `artifact://604`, 713 passed, 7 skipped.
- `WEB-v5`: `evidence-v5/reviewed-browser/{sqlite,postgres}-{baseline,candidate}/browser-*.jsonl`, 92/92 PASS.
- `API-v5`: `comparison-*.json`, `api-*.jsonl`, `concurrent-three-read-comparison.json`.
- `NATIVE-v5`: `perf06-native.json`, `sanitizer-check.json`, `native-capture-negative-restore.json`.
- `PROXY-v4`: `evidence-v4/proxy-smoke.json`.

실제 Rust 수집 경로는 `cc-lb-storage-{sqlite,postgres} --test integration`의 `cache_keepalive_session_reads::*`, `cc-lb-admin --test integration`의 `cache_keepalive_contracts::{read_equivalence,cursor_window}::*`, `cc-lb-admin --lib`의 `cache_keepalive_view::{tests,activity_tests,status_tests}::*`다. `pnl_converter_invalid_input_is_http_400`은 public parent test 하나만 수집하고, parent가 동일 binary의 exact private child 실행을 검증한다. child를 별도 QA case나 추가 PASS로 세지 않는다.

## 3. Storage QA matrix — ST-01~ST-16

각 storage case는 같은 logical fixture를 PostgreSQL과 SQLite에서 실행한다. 명령의 `<TEST>`는 행에 적힌 exact test name이다. ST-01~12와 ST-14~16은 두 엔진이 같은 이름을 쓰지만 ST-13은 PostgreSQL `cache_keepalive_session_reads::list_boundaries_preserve_error_contract`, SQLite `cache_keepalive_session_reads::list_boundaries_preserve_invalid_input_contract`로 다르다.

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
| ST-13 | Limit/range boundaries | limit 0, 1, 50, 100; direct storage u32::MAX; summary cutoff와 list horizon/cursor의 u64::MAX | PostgreSQL exact test `cache_keepalive_session_reads::list_boundaries_preserve_error_contract`; SQLite exact test `cache_keepalive_session_reads::list_boundaries_preserve_invalid_input_contract` | 0 empty/no cursor; 1/50/100 K+1 semantics; no overflow/panic. Summary cutoff overflow는 양 엔진 `InvalidInput`이다. List horizon/cursor가 i64 범위를 넘으면 SQLite는 기존 `InvalidInput`을, PostgreSQL은 기존 `StorageError::Fatal`과 각각 `cache keepalive horizon start cannot be represented as bigint`, `cache keepalive cursor timestamp cannot be represented as bigint` message를 보존한다. Candidate는 엔진별 baseline과 같아야 하며 오류를 cross-engine 정규화하지 않는다. HTTP maximum은 100 그대로다. | 각 input/result, 엔진별 exact error variant/message, panic-free completion | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-14 | Requested-row corruption mapping | Corrupt config JSON and invalid status on rows selected by direct/list/summary | Both engines, `<TEST>=selected_corruption_is_not_silently_dropped` | Selected mapping error is `StorageError::Corrupted`; unrelated old decision outside narrow read is not required to poison request | Corrupt row key, selected query, exact error variant | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-15 | Arbitrary/noncanonical/colon/Unicode `cursor.entry_id` comparison remains SQL-defined | Valid cursor envelope with entry IDs such as `x`, `session::x`, `decision:é`, `Ω`, and namespace-looking noncanonical text | Both engines, `<TEST>=cursor_entry_id_preserves_legacy_sql_comparison` | Candidate rows/cursor result equal pre-change `LIST_SQL` on that same engine for every value; no parser canonicalization or Rust byte-order substitute | Per-value legacy/candidate ordered IDs and collation metadata | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |
| ST-16 | DB default collation result contract | 동일 DB와 동일 connection/session collation에서 ASCII namespace, colon 변형, 비ASCII ID가 섞인 fixture | Both engines, `<TEST>=list_and_detail_match_legacy_on_same_database_collation`; 같은 fixture에서 legacy query와 candidate query를 연속 실행 | 각 엔진에서 legacy와 candidate의 ordered rows, selected detail candidate, cursor chain이 exact하다. canonical fixture의 cross-engine parity도 별도로 유지한다. 소스 text grep만으로 합격시키지 않는다. | DB/connection collation metadata, 양 query의 ordered ID·cursor hashes, selected candidate hash | PASS@9c420 [RUST-713, PLAN-v5] / PASS@4ed7cc8c [RUST-724, PLAN-v5] |

## 4. API·semantic QA matrix — SEM-01~SEM-12

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

Runner와 browser evidence는 opaque cursor를 nonce처럼 삭제해 비교하지 않는다. Baseline cursor는 기존 5개 필드를 가진 원본 reference로 보존하고, candidate cursor는 그 5개 decoded field가 exact한지 비교한 뒤 `horizon` tag를 별도로 검증한다. Fixture timestamp anchor와 live request의 `started_at_utc`를 섞어 UTC `now` 차이를 숨기지 않는다.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| CUR-01 | 24h/7d page chain survives elapsed time. `query.rs::parse_query`, `encode_cursor` | 각 horizon에 2페이지 이상, live clock | 위 exact Axum `cur_01_...`와 browser `--grep 'CUR-01'`; UI page 1 응답 뒤 server second를 넘겨 `Loading older sessions...` 클릭 | Baseline은 기존 400을 accepted bug delta로 기록한다. Candidate page 2는 200이고 decoded `horizon_start_ms`가 page 1과 같으며 tag는 요청 horizon과 같다. Summary는 각 요청 현재 시각으로 재계산한다. | 두 horizon의 page 1/2 raw status/body, baseline 원본 cursor hash, candidate decoded field hash/tag, UI request URL | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |
| CUR-02 | Valid cursor와 principal/horizon/filter scope validation | 24h, 7d, all cursor와 P1/P2/filter 조합 | 위 exact Axum `cur_02_...`, runner `--case cursor`, browser `--grep 'CUR-02'` | 새 cursor tag는 24h/7d/all 모두 정확하다. `all` same-scope page 2는 baseline/candidate 200. 다른 principal, 24h↔7d, 다른 filter는 모두 400 `invalid_input`; arbitrary `entry_id`의 기존 SQL seek 계약은 ST-15 그대로다. | 세 tag decoded payload, valid page body, 세 mismatch status/body/reason | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |
| CUR-03 | First-page anchor remains frozen through repeated advances | 24h에서 최소 5페이지, page마다 server second advance | 위 exact Axum `cur_03_...`, runner/browser `--grep 'CUR-03'` | Baseline 첫 time advance 400은 accepted bug delta. Candidate는 page 2~5 모두 200이고 모든 emitted cursor의 `horizon_start_ms`가 page 1과 exact하다. | Per-page request time, cursor-in/out hashes, decoded original-field hashes, frozen anchor | PASS@9c420 [RUST-713, API-v5, WEB-v5] / PASS@4ed7cc8c [RUST-724, PR-API, PR-WEB] |

## 6. Admin Web QA matrix — UI-01~UI-12

Unit component commands use `cd crates/cc-lb-admin/web && bun run test --run <FILE> -t '<NAME>'`. Browser commands use the real-server Playwright command from §2.2 with `--grep '<ID>'`.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| UI-01 | Four metric card and value-change flash. `CacheKeepaliveCard`, `MetricTile` | Enabled P1, summary changes once | Component test `src/components/principals/cache-keepalive/__tests__/CacheKeepaliveCard.test.tsx`, name `Given metric change, When rendered, Then flashes ONLY when value changes`; then real browser `--grep 'UI-01'` | Four labels/values format unchanged; changed value flashes; `PAUSE_ANIMATIONS=true` suppresses animation | Test output plus before/after screenshot and independent summary body | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-02 | Toggle and 409 handling. `CacheKeepaliveCard`, mutation hook | Revision conflict fixture | Component test same file `-t 'locks the toggle during another write'`; real browser `--grep 'UI-02'` | Existing switch/lock/toast behavior unchanged. The actual compiled server PATCH returns engine-specific HTTP 409: SQLite `storage_conflict`, PostgreSQL `stale_revision`. Synthetic responses cannot replace either observation. Backend read optimization adds no mutation | Engine-tagged PATCH status/body and UI screenshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-03 | Three horizon transitions. `CacheKeepaliveSessionsDrawer::HorizonToggle`, path builder | Rows distributed across 24h/7d/all | Drawer component test filtered by `-t 'horizon'`; real browser `--grep 'UI-03'` | Exact requests for 24h, 7d, all; Overview label changes; ordered IDs match API | Three exact URLs/body hashes and UI labels | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-04 | Seven filters and error query mapping | At least one row for every base state and orthogonal error | Drawer component test `-t 'filter'`; real browser `--grep 'UI-04'` iterates all 21 horizon×filter combinations | Selected chip `aria-pressed=true`; error uses `error=true`; every API/UI ordered ID list agrees | 21 request records, aria snapshot, ordered ID hashes | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-05 | Row ID/state/error/P&L/attempt ticks | Mixed fixed rows | Drawer component test `-t 'row'`; real browser `--grep 'UI-05'` | Existing text, badge, color class, error border, attempts/max_attempts unchanged | API row and DOM semantic snapshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-06 | FLIP read-before-write and pause behavior | Two rows swap order, then data changes without movement | Existing drawer tests `-t 'reads all FLIP geometry before writes and only animates moved rows'` and `-t 'Given PAUSE_ANIMATIONS=true'`; browser `--grep 'UI-06'` | All geometry reads precede writes; only moved rows animate; pause disables. Fixed frame-rate 목표를 합격 기준으로 쓰지 않는다. | Event sequence, trace, screenshot; no frame-rate-only verdict | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-07 | Real cursor pagination through terminal, no 20 cap | Stable dataset requiring >20 pages | Candidate real browser `--grep 'UI-07'`; click `Loading older sessions...` until response `next_cursor` is null; independently direct-fetch every page. Baseline time-advance failure is recorded only under CUR-01/CUR-03. | Candidate has no duplicate/missing IDs; cursor-out hash equals next cursor-in hash; decoded cutoff stays fixed; stop only at wire null; `No more sessions` is presentation confirmation, not sole proof | Every candidate page record, final concatenated ID hash, manifest expected count, page_count >20 | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-08 | Desktop split and mobile detail navigation | One selectable session | Detail/drawer component tests; browser `--grep 'UI-08'` at 1024px and 800px | 1024 split with 440px list; 800 list hidden and `◀ Back`; detail data same | Two viewport screenshots and detail body hash | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-09 | Turn timeline, pending/final P&L | Active multi-turn and terminal multi-turn sessions | `bun run test --run src/components/principals/cache-keepalive/__tests__/SessionDetailPane.test.tsx -t 'turn'`; browser `--grep 'UI-09'` | Active newest turn is pending/live and avoided cost unrealized; terminal newest is final; existing strings/colors/rounded display preserved | Raw detail JSON, semantic DOM, screenshot | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-10 | Config snapshot and raw record disclosures | Session detail with snapshot and raw fields; decision detail nullables | Detail component test `-t 'Config'` and `-t 'Raw'`; browser `--grep 'UI-10'` | Existing expand/collapse, fields, nullability and raw values unchanged | Detail body and expanded screenshots | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-11 | Visibility pauses polls and preserves each hook's resume behavior | Card+drawer+detail open | Component hook tests plus browser `--grep 'UI-11'`; page 안에서 `document.visibilityState`/`visibilitychange` probe로 hidden 30s 후 visible 전환 | DOM hidden state disables the configured interval. Summary/detail explicitly call `query.refetch()` on hidden→visible through `usePolledData`; sessions only changes its `useInfiniteQuery` interval and may also follow TanStack's existing focus behavior. Record baseline and require candidate to match; do not invent one shared immediate-refetch rule or treat this probe as native OS hiding. | Timestamped DOM visibility events and network log per independent query key | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |
| UI-12 | Two-principal cache/state isolation | P1 and P2 have disjoint IDs and summary values | Browser `--grep 'UI-12'`; open P1 drawer/detail, navigate to P2, observe two poll intervals | P2 never paints P1 values/rows. Current query functions do **not** pass AbortSignal, so prior P1 network requests may complete; no-cancel is expected and cache-key isolation is the oracle | P1/P2 request timelines, visible IDs per paint, no mixed-principal body | PASS@9c420 [WEB-v5] / PASS@4ed7cc8c [PR-WEB] |

## 7. Performance/evidence QA matrix — PERF-01~PERF-06

No row has an arbitrary absolute latency or fixed frame-rate pass budget. Baseline and candidate run on the same machine, engine, dataset, cache state, concurrency, and build profile. Functional response equality is a prerequisite to considering timing.

| ID | Requirement / source files + symbols | Given fixture | Exact steps / command | Expected value / oracle | Actual evidence required | Status |
|---|---|---|---|---|---|---|
| PERF-01 | Summary removes D-scale full materialization | D=100,000 old decisions; fixed small S/T; baseline and candidate binaries | `python3 "$RUNNER" compare --engine <sqlite|postgres> --baseline-url <URL> --candidate-url <URL> --token-file "$TOKEN_FILE" --case PERF-01 --records <JSONL> --summary <JSON>`; 1 cold + 기본 30 warm `?limit=0` requests per engine | Fixed-snapshot body hashes exact. Candidate decision work is bounded to one recent visible count, no `list_all` 1,000-row pagination; report relative p50/p95/max, no absolute promise | Raw requests, SQL count/time, plan, rows examined/returned, body hashes | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
| PERF-02 | List first/middle/terminal pages avoid summary full scan and bound materialization | D=100k; stable mixed list >20 pages; fixed S/T | 같은 `compare` 명령에 `--case PERF-02`; runner는 candidate의 UI-size 24h list를 wire `next_cursor=null`까지 하드캡 없이 순회하고 first/page-13/terminal을 기록한다. Baseline 원본 cursor는 보존하고 time-advance 400은 CUR bug delta로 분리한다. | 첫 페이지 body와 decoded 기존 cursor 5개 필드는 exact하고 candidate tag는 24h다. 이후 candidate page materialization은 K+1 final page contract이며 page session만 turn을 읽는다. Candidate total row count는 manifest oracle과 같아야 한다. | Per-page plan/candidate cursor chain, baseline bug-delta record, baseline/candidate first-page timing | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
| PERF-03 | Detail direct candidate lookup | D=100k; target session, decision, collision, missing ID | 같은 `compare` 명령에 `--case PERF-03`; session, decision, collision, missing 각각 기본 30 warm call | Exact body/status. Candidate has bounded two-candidate lookup and target-only session/turn/upstream work; no list_all pagination | SQL trace/plan, response hashes, relative latency | PASS@9c420 [API-v5, PLAN-v5] / PASS@4ed7cc8c [PR-API, PLAN-v5] |
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
| FLOW-08 | Full actual-stack equivalence: baseline/candidate × SQLite/PostgreSQL × browser/fake upstream | Same deterministic seed and safe loopback upstream; fixed fixture anchor and separately logged live request clock | Run §2.2 harness for four server/engine combinations; execute all 21 filter/horizon pairs, summary, representative detail/collision/404, CUR-01..03, transition flows, and candidate terminal pagination | Canonical status/body/ordered rows match baseline where the contract is unchanged. Opaque cursor는 decoded 기존 5개 필드를 비교하고 candidate `horizon` tag를 별도 검증한다. Time-advance pagination의 baseline 400/candidate 200만 승인된 bug delta다. | Manifest of binaries/config hashes, fixture/request clock anchors, seed hash, raw records, browser traces, cleanup record | PASS@9c420 [API-v5, WEB-v5, PROXY-v4] / PASS@4ed7cc8c [PR-API, PR-WEB, PR-PROXY] |

## 9. Planner/index decision gate

1. First record PostgreSQL `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` and SQLite `EXPLAIN QUERY PLAN` for summary count, each list branch and cursor position, and detail candidates on D=100k and S/T scales.
2. An index migration is allowed only if the candidate still performs avoidable scan/sort work material to the measured regression. “Migration 없이 100% 성능 해결”은 사전 결론이 아니다.
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
- Latest base `4ed7cc8c`: `PASS` **57**, `FAIL` **0**, `BLOCKED` **0**, `NOT_RUN` **0**. PostgreSQL/SQLite migration은 각각 `0118`/`0086`으로 통합됐고 browser matrix는 **92/92 PASS**다.
- 현재 application/QA 상태와 PR CI 상태는 별개다. PR은 아직 생성되지 않았고 CI는 pending이다.

상태는 §2.3 raw record와 §2.4 anchor가 있을 때만 PASS다. 빌드 성공, test enumeration, route 방문, screenshot 하나, disabled pagination control, synthetic frontend mock만으로 상태를 올리지 않는다. Historical evidence는 비교 기준이며 latest-base PASS와 PR CI PASS를 대신하지 않는다.
