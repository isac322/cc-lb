# 프롬프트 캐시 공유 저장소 QA 매트릭스 (구현 전)

관련 문서: [prompt-cache-shared-store-decisions.md](prompt-cache-shared-store-decisions.md), [prompt-cache-shared-store-plan.md](prompt-cache-shared-store-plan.md)

이 문서는 PostgreSQL-first 공유 관측 저장소 재설계의 **구현 전 QA 목록**이다. 구현 완료 후 이 목록을 그대로 실행해 결과를 기록한다. 모든 행의 상태는 실행 전까지 `PENDING`이다.

## 1. 검증 대상 계약

구현이 만족해야 하는 관측 가능 계약은 다음과 같다. 우선순위는 **응답 전달/가용성 > 캐시 적중 > 지연**이다.

- 관측을 확보하는 즉시 비동기 저장을 시작한다. 응답 스트리밍과 완료는 DB 커밋과 무관하게 진행된다 — 첫 프레임 전 대기도, 완료 경계 배리어도 없다(P1-A 확정).
- 라우팅은 매 요청마다 공유 저장소에서 후보 키만 일괄 조회한다. pod별 장기 보관 캐시, 관측용 NOTIFY, hydration 경로는 라우팅에서 제거된다.
- 보장의 시작점은 **저장 커밋 성공**이다. 비동기 쓰기 시작은 다음 turn에서의 가시성을 보장하지 않는다.
- 동일 키에 역순으로 도착한 쓰기는 유효 만료를 후퇴시키지 않는다. 만료를 늘리는 쪽이 이길 때 그 관측의 메타데이터가 일관되게 유지된다.
- DB 읽기 실패는 cold cache(관측 없음 확정)와 다르다. 캐시 상태를 알 수 없는 상태로 취급하고, 캐시 친화도 없이 중립 점수로 라우팅을 계속한다(P2-A 확정). 오류는 OTel로 보고한다.
- provider 내부 캐시의 실제 적중은 보장 대상이 아니다. 검증 대상은 cc-lb가 기록하고 읽는 관측 상태다.
- 쓰기 발행은 단일 유한 비블로킹 sink에 `try_send`로 시도한다. 큐 포화나 저장 실패 시 OTel 메트릭/스팬으로 보고하고 요청을 계속 진행한다. 용량 예약, 사전 확인, 요청 거절, 재시도, outbox, 신규 인프라를 두지 않는다(P3 확정). 응답 전달이 캐시 적중보다 우선한다.

- 관측 테이블을 직접 읽는 어드민 화면은 없다. 단, 어드민 라우팅 프리뷰는 엔진의 라우팅 스코어러를 통해 관측을 **간접 소비**하므로 QA-H7로 검증한다.

## 2. 상태 규칙

| 상태 | 의미 |
|---|---|
| `PENDING` | 미실행. 구현 후 실행해 결과를 기록한다. |
| `PASS` | 실행 완료, 모든 Then 단언 충족. 증거 열에 실행 산출물 경로를 기록한다. |
| `FAIL` | 실행 완료, 하나 이상의 Then 단언 불충족. 증거 열에 실패 출력을 기록한다. |
| `BLOCKED` | 실행 불가. §9의 미확인 전제조건과 연결해 사유를 기록한다. 관측되지 않은 blocker를 사전에 단정하지 않는다. |
| `RECORDED` | 실행됐으나 PASS/FAIL 판정 대상이 아닌 기록 행(기준선 재현, 측정·관측 기록). 재실행 없이 산출물을 인용한다. |

실행 증거는 `target/test-evidence/issue-825/` 아래에 상대 경로로 보관한다.

## 3. 확정된 정책

사용자가 다음과 같이 최종 결정했다(2026-09-20). 이전의 OPEN 분기는 폐기한다. Main의 우선순위 감사(피어 리뷰)를 거쳐 설계는 확정 상태다. QA 게이트는 부분 실행됐으며 각 행의 상태는 §7에 기록한다 — 미실행 행이 남아 있으므로 전체 "무회귀"는 주장하지 않는다.

| ID | 쟁점 | 확정 동작 |
|---|---|---|
| **P1** | 완료 경계 | **A. 완전 비동기**: 응답 완료는 DB 커밋과 독립적이다. 스트리밍·비스트리밍 모두 쓰기가 블록/실패 중이어도 정상 완료된다. 다음 turn 가시성은 커밋 완료 여부에 달린다. |
| **P2** | 라우팅 시점 공유 DB 읽기 실패 | **A. fail-open**: 읽기 실패는 cold가 아닌 Unknown으로 처리하고, 캐시 친화도 가중치 없는 중립 점수로 라우팅을 계속한다. 오류는 OTel로 보고한다. |
| **P3** | 쓰기 발행 포화/실패 | **직접 시도 + 실패 보고**: 용량 예약·사전 확인·요청 거절 없이 단일 유한 비블로킹 sink에 `try_send`한다. 실패 시 OTel 메트릭/스팬으로 보고하고 요청을 계속한다. 재시도·outbox·신규 인프라 없음. |

## 4. 공통 환경

- **격리**: 모든 실행은 로컬 격리 자원만 사용한다. SQLite는 테스트별 임시 디렉터리, PostgreSQL은 테스트 전용 데이터베이스/스키마, 포트는 임의 할당 또는 multi-replica 스크립트의 고정 localhost 포트(8888, 8889, 8001, 8002, 8003, 8004, 18888)를 사용한다. 프로덕션 데이터·자격증명·엔드포인트에 쓰지 않는다.
- **자격증명**: 테스트용 더미 키만 사용한다(예: multi-replica 스크립트의 고정 더미 토큰). 실제 provider 키, OAuth 자격, 프로덕션 DB URL을 어떤 산출물에도 기록하지 않는다. 증거 로그는 자격증명을 마스킹한 상태로 저장한다.
- **fake provider 경계**: `tests/fixtures/fake-anthropic`은 계약 충실 mock이다. 이 하네스의 PASS는 cc-lb의 관측·저장·조회 동작을 증명하지만, 실제 유료 provider의 내부 캐시 적중을 증명하지 않는다. provider 실측 검증은 별도 승인이 필요한 범위 밖 작업이다.
- **결정론**: 레이스를 가리는 `sleep` 단언을 쓰지 않는다. 동기화는 테스트 범위 배리어(채널, `tokio::sync::Barrier`, 게이트된 mock store, fake provider의 프레임 제어 헤더)로 한다. 프로덕션 공개 API에 테스트 전용 flush를 추가하지 않는다.
- **시간 제어**: 컴포넌트 테스트는 `cc_lb_clock::TestClock`(`advance_secs`/`advance`)으로 시간을 진행한다. `TestClock`은 in-process 하네스에서만 유효하며, `common::spawn_test_server_*`가 띄우는 별도 프로세스 서버에는 적용되지 않는다. 프로세스 수준 테스트는 DB 행의 절대 시각을 직접 비교한다.
- **측정 단언**: p50/p95/p99 같은 지연 수치는 측정하고 기록한다. 사전에 임의의 임계값(예: 50ms)을 PASS 조건으로 두지 않는다. 0ms 지연, 100% 보장 같은 단언은 금지한다.

## 5. 실행 명령

테스트 타깃 이름은 `Cargo.toml`의 `[[test]]` 등록 기준이다. `cc-lb-server`, `cc-lb-admin`, `cc-lb-storage-postgres`, `tests-multi-replica` 모두 통합 테스트 타깃 이름은 `integration`이며 `tests/all.rs`에 모듈로 등록된다.

```bash
# cc-lb-server 통합 테스트 (SQLite 기본). prompt_cache_live_qa 모듈만 필터하는 예시.
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test -p cc-lb-server --test integration prompt_cache_live_qa -- --nocapture

# cc-lb-admin 통합 테스트 (RequestEvent/rollup 등 관리 API 계약)
CC_LB_ADMIN_SKIP_SPA=1 CC_LB_SKIP_WASM_FIXTURE_BUILD=1 \
  cargo test -p cc-lb-admin --test integration

# PostgreSQL 스토리지 어댑터 라이브 테스트 (postgres feature + CI_POSTGRES_URL 필요)
CI_POSTGRES_URL="postgres://cc_lb:cc_lb@127.0.0.1:5432/cc_lb" \
  cargo test -p cc-lb-storage-postgres --features postgres --test integration -- --test-threads=1

# 2-replica + 공유 PostgreSQL 18 E2E (Docker 필요, DOCKER_HOST=tcp://localhost:2375)
CC_LB_MULTI_REPLICA_E2E=1 \
CC_LB_MULTI_REPLICA_POSTGRES_URL="postgres://cc_lb:cc_lb@127.0.0.1:5432/cc_lb" \
  bash tests/multi-replica/multi-replica-postgres.sh
# 또는 cargo 래퍼:
CC_LB_MULTI_REPLICA_E2E=1 CC_LB_MULTI_REPLICA_POSTGRES_URL="postgres://..." \
  cargo test -p tests-multi-replica --test integration

# Admin Web
cd crates/cc-lb-admin/web
bun install --frozen-lockfile
bun run --shell=bun build
bun run --shell=bun lint
bun run --shell=bun typecheck
bunx --bun vitest run --passWithNoTests --maxWorkers=1 --no-file-parallelism
```

CI 요구사항(`.github/workflows/ci.yml`, `web.yml` 기준):

```bash
cargo fmt --check
bash scripts/lint-audit-redaction.sh
cargo clippy --workspace --all-targets --no-default-features --features sqlite -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --features postgres -- -D warnings
cargo deny check
cargo llvm-cov nextest --workspace \
  --exclude cc-lb-loadgen --exclude cc-lb-stress-suite \
  --all-features --test-threads=4 --no-report
cargo llvm-cov report --lcov --output-path target/coverage.lcov \
  --ignore-filename-regex '^(tests/|fuzz/|examples/|third_party/|tests/fixtures/)'
scripts/coverage-gate.sh target/coverage.lcov
```

## 6. 단언 규칙

- 메트릭이나 로그의 **부재**만으로 동작을 단언하지 않는다. 부재 단언은 항상 관측 가능한 동작 단언(라우팅 결과, DB 행, 응답 본문)과 함께 둔다.
- SQL 소스 텍스트를 단언하지 않는다. 쿼리 동작은 결과와 `EXPLAIN` 출력으로 검증한다.
- 후보 키 개수의 고정 상한을 가정하지 않는다. 일괄 조회 검증은 요청에서 도출된 실제 후보 집합 기준으로 한다.
- `sleep`으로 경쟁 조건을 가리지 않는다. 대기가 필요하면 유한 데드라인이 있는 조건 폴링(기존 `support.rs`의 `wait_for_*` 패턴) 또는 명시적 배리어를 쓴다.
- fake provider의 `x-fake-ttft-ms`, `x-fake-inter-token-ms`, `x-fake-delta-count`, `x-fake-mode` 헤더는 지연을 만드는 수단이지 **결정론적 배리어가 아니다**. "provider 완료를 보류한 채 DB를 단언"하는 시나리오는 테스트 범위의 명시적 게이트(fixture의 스트림 보류 훅 + 저장소 커밋 확인 신호)로 동기화한다. x-fake 지연 헤더는 보조적 스모크에만 사용한다. 이를 위해 프로덕션 공개 API를 추가하지 않는다.

## 7. QA 매트릭스

### A. 기준선 재현

#### QA-A1. #825 기준선 재현 (hydration이 로컬 갱신 만료를 후퇴)

- **상태**: RECORDED
- **Given**: HEAD `917a1003893d87a307f297863a662b6f273e2904`의 실제 SQLite 저장소와 캐시. 동일 관측을 받은 캐시 두 개 중 하나에만 DB 재로딩을 적용.
- **When**: T+0 최초 기록(만료 T+270) → T+40 hit으로 수명 갱신(만료 T+310) → T+60 오래된 DB 기록 재로딩 → T+280 조회.
- **Then**: 재로딩한 캐시는 만료가 T+270으로 후퇴하고 T+280 조회에서 없음으로 판정. 대조군은 T+310 유지. 새 기록→옛 기록 순서의 DB 쓰기도 만료 310→270으로 후퇴.
- **환경**: 격리 로컬 SQLite, 네이티브 실행.
- **증거**: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`.
- **비고**: PostgreSQL에서의 동일 결함은 아직 실행으로 증명되지 않았다. 신구조에는 hydration 경로가 없으므로 이 시나리오의 사후 대응은 §8 매핑과 QA-B7/B8, QA-C1로 옮겨진다.

### B. 쓰기 경로

#### QA-B1. fast-path SSE `message_start` 조기 발행

- **상태**: PASS
- **Given**: 스트리밍 요청. fake provider가 `message_start`에 cache usage를 싣는다. 스트림 보류는 테스트 범위의 명시적 게이트(fixture가 `message_start` 송출 후 최종 프레임을 테스트 신호까지 보류하는 훅)로 고정한다 — `x-fake-*` 지연 헤더는 결정론적 배리어가 아니므로 보조 스모크에만 쓴다.
- **When**: 서버가 `message_start`의 usage를 수신했음을 테스트 배리어로 확인한 뒤(클라이언트 프레임 수신과 무관), 최종 프레임을 계속 보류한 상태에서 저장소를 조회하고 커밋을 확인한다. 이후 게이트를 해제한다.
- **Then**: 스트림이 완료되기 전에 해당 `(upstream_id, canonical_model_id, v3_prefix_key, ttl_class)` 관측 행이 커밋되어 있다. `message_stop`까지 발행을 미루는 기존 동작이면 이 단언은 실패한다.
- **환경**: `cargo test -p cc-lb-server --test integration` 범위의 live QA 하네스, 격리 SQLite.
- **증거**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` (23 pass — production Lifecycle fast-path 조기 발행, 게이트된 store로 최종 프레임 보류 중 커밋 확인), `target/test-evidence/issue-825/observation_failure_isolation.log` (9 pass — 실제 서버 sink 경유 SQLite 커밋을 최종 게이트 프레임 전에 확인).

#### QA-B2. compat SSE 경로 `message_start` 조기 발행

- **상태**: PASS
- **Given**: SSE 이벤트 변환 훅이 있는 dialect(compat 경로)를 통과하는 스트리밍 응답. `relay_response`의 `sse_event_transform_hook` 분기.
- **When**: QA-B1과 동일 — usage 수신 배리어 확인 후 최종 프레임 보류 상태에서 저장소 조회·커밋 확인.
- **Then**: 변환 경로에서도 동일하게 조기 발행된다. 변환 여부와 무관하게 관측 발행 시점이 같다.
- **환경**: compat dialect를 표현하는 테스트 fixture 필요. 해당 fixture가 없으면 §9에 기록하고 BLOCKED가 아니라 fixture 추가 후 실행.
- **증거**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` (compat 경로 조기 발행 포함 23 pass), `target/test-evidence/issue-825/observation_failure_isolation.log` (실제 서버 sink compat 커밋 확인).

#### QA-B3. DB 쓰기 블록 중 첫 프레임 전달

- **상태**: PASS
- **Given**: 쓰기를 게이트로 보류하는 테스트 전용 store(기존 `BlockingStore` 패턴을 `future::pending` 대신 제어 가능한 게이트로 확장). 스트리밍 요청.
- **When**: 관측 쓰기가 게이트에서 블록된 상태로 유지.
- **Then**: 첫 SSE 프레임(`message_start` 또는 첫 delta)이 DB 커밋 없이 클라이언트에 전달된다. "첫 delta 전에 DB 커밋 완료"를 요구하지 않는다. 게이트 해제 후 쓰기가 완료된다.
- **환경**: in-process 하네스(게이트된 store 주입 가능 지점 필요).
- **증거**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` — 게이트된 store 블록 중 첫 프레임 전달 확인. 프레임 수신과 커밋의 순서 기록.

#### QA-B4. abort 전후 발행과 중복 발행 부재

- **상태**: PASS
- **Given**: 스트리밍 요청 두 건. (a) 서버가 `message_start`의 usage를 수신했음을 테스트 배리어로 확인한 뒤(클라이언트가 프레임을 받았는지와 무관) 클라이언트 연결을 강제 종료. (b) `message_start` 도달 전 클라이언트 연결 종료. 발행 횟수는 테스트 전용 관찰 store 래퍼로 계수한다.
- **When**: 각 케이스 후 저장소 조회와 발행 카운터 확인.
- **Then**: (a) 이미 관측된 `message_start` 기반 행은 커밋되어 보존. (b) 관측 전 abort는 발행 없음. DB 행 수(upsert 키 기준 1행)만으로는 중복 발행을 증명할 수 없으므로, 관찰 래퍼가 기록한 수용 발행 배치 수가 정확히 1임을 단언한다 — `message_stop`이나 abort 시점에 두 번째 발행이 없어야 한다. 이것은 **로컬 단일 발행** 계약이다. 분산 exactly-once/at-least-once 보장을 요구하지 않는다.
- **환경**: live QA 하네스, 클라이언트 소켓 강제 close.
- **증거**: `target/test-evidence/issue-825/lifecycle_client_disconnect.log` — before/after abort 케이스 포함 23 pass. 케이스별 DB 행 수와 발행 카운터.

#### QA-B5. 비스트리밍(buffered) 발행

- **상태**: PASS
- **Given**: `stream` 미지정/`false` 요청. `MessageScript`의 `ScriptedMessageResponse`로 usage를 포함한 buffered 응답.
- **When**: 응답 수신 후 저장소 조회, 이어서 동일 prefix의 두 번째 요청.
- **Then**: 응답 완료 후 관측 행이 존재하고, 두 번째 요청의 라우팅이 그 관측을 반영한다. P1-A 확정에 따라 응답 완료는 커밋을 내포하지 않으므로, 두 번째 요청 전에 커밋 완료를 테스트 배리어로 확인한다.
- **증거**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — 실제 2프로세스 PG 환경에서 buffered 발행 확인.

#### QA-B6. 신규 키 쓰기

- **상태**: PASS
- **Given**: 저장소에 없는 새 `(upstream, model, prefix, ttl)` 키의 관측.
- **When**: upsert 실행.
- **Then**: 행이 삽입되고 모든 필드(`expires_at`, `last_observed_at`, `hash_schema_version`, `prefix_content_block_index`, `estimated_prefix_tokens`, `token_estimate_source`)가 기록값과 일치한다.
- **환경**: store 수준 계약 테스트. SQLite와 Postgres 양쪽.
- **증거**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — SQLite+PG 22개 store 테스트 × 20회 = 440 pass.

#### QA-B7. 역순 쓰기 — 오래된 관측이 늦게 도착

- **상태**: PASS
- **Given**: 동일 키. 먼저 만료 T+310의 쓰기를 커밋.
- **When**: 이후 만료 T+270의 오래된 관측 쓰기를 커밋. 반대 방향(옛값 커밋 후 새값 커밋으로 정상 갱신)도 함께 실행한다.
- **Then**: 역순 — `expires_at`은 T+310을 유지하고, 승자(T+310 관측)의 메타데이터가 일관되게 남는다. "만료만 max로 유지하고 메타데이터는 오래된 값으로 덮어쓰기"는 불합격이다. 정순 — 새 쓰기가 옛 행을 정상 갱신한다. T+280 시점 조회에서 키가 유효하다.
- **환경**: store 수준 계약 테스트. SQLite와 Postgres 양쪽. 기준선 QA-A1에서 SQLite는 이 순서로 후퇴했으므로 회귀 방지 행이다.
- **증거**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 역순/정순 모노토닉 병합 440 pass.

#### QA-B8. 동시 쓰기 — 두 pod/두 클라이언트의 겹친 갱신

- **상태**: PASS
- **Given**: 동일 키에 서로 다른 만료를 가진 두 쓰기를 별도 연결에서 동시에 발행. 추가로 동일 만료·다른 메타데이터(동률) 케이스를 준비한다.
- **When**: 두 커밋 완료 후 조회. 동률 케이스도 양쪽 도착 순서로 각각 실행.
- **Then**: 최종 `expires_at`은 둘 중 큰 값이고, 메타데이터는 그 승자 관측의 것과 일관된다. 어느 순서로 커밋돼도 결과가 같다. 동률(같은 `expires_at`)은 `last_observed_at`이 큰 쪽이 승자이며 그 메타데이터가 일관되게 남는다 — 임의의 오래된 메타데이터 덮어쓰기는 불합격이다. 완전히 동일한 관측의 재발행(identity replay)은 멱등이다.
- **환경**: Postgres는 별도 연결 2개로 실행. SQLite는 동일 파일에 대한 순차·동시 쓰기 모두.
- **증거**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 동시 쓰기·동률(coherent tie)·멱등 재발행 포함 440 pass.

#### QA-B9. 발행 포화 시 관측 유실 보고와 요청 계속 (P3 확정)

- **상태**: PASS
- **Given**: 비블로킹 sink의 유한 큐를 고의로 채운 상태.
- **When**: 새 관측을 포함한 요청 디스패치.
- **Then**: 요청은 거절되지 않고 정상 처리된다. `try_send` 실패는 OTel 메트릭/스팬으로 관측 가능하게 기록된다. 용량 예약, 사전 확인, 요청 거절이 없음을 단언한다. 드롭된 관측은 DB에 나타나지 않는다 — 이 유실은 허용 동작이며 보고만 요구된다.
- **환경**: 큐를 채우는 테스트 훅 필요(§9).
- **증거**: `target/test-evidence/issue-825/observation_failure_isolation.log` — queue-full/closed × stream/buffered 모두 HTTP 200 + 실제 카운터/WARN 확인. 단, 큐 계열 케이스는 OTLP export 대상이 아니어서 in-process 카운터/WARN이 증거다(QA-F4 참조).
### C. 읽기 경로

#### QA-C1. 두 프로세스 read-after-commit (NOTIFY/rebind 없이)

- **상태**: PASS
- **Given**: 공유 PostgreSQL에 붙은 cc-lb 프로세스 A와 B. A로 Turn 1 요청을 보내 관측 커밋을 확인.
- **When**: B로 동일 prefix의 Turn 2 요청 전송. B의 로컬 상태는 비어 있다.
- **Then**: B는 공유 DB 조회만으로 A가 기록한 upstream으로 라우팅한다 — 단, 이 기대는 **통제된 동등 조건**(후보들이 health/quota/rate-limit 등 다른 제약에서 동등하게 eligible)에서만 성립한다. warm 관측이 health/quota/rate-limit 제약을 override하는 것은 불합격이다. 관측용 NOTIFY 수신이나 view rebind 없이 성립해야 한다 — 관측 채널(`cclb_prompt_cache_observation_changed`) 제거 후에도 이 시나리오가 통과하는 것이 핵심이다.
- **환경**: `tests/multi-replica/multi-replica-postgres.sh` 확장 또는 동등한 2프로세스 하네스. Docker Postgres 18.
- **증거**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — 실제 2프로세스 PG에서 C1 4/4 pass. B의 `request_events_v1` `upstream_id`와 fake provider 수신 기록.
#### QA-C2. 즉시 다음 turn (P1-A 확정)

- **상태**: RECORDED — `target/test-evidence/issue-825/runtime/RESULTS.tsv` QA-C2: turn2_upstream 기록, commit_seen_at=1789919627 turn2_done=1789919627.
- **Given**: Turn 1 응답 완료 직후, 폴링이나 인위적 대기 없이 Turn 2 전송.
- **When**: 응답 완료와 동시에 다음 요청.
- **Then**: P1-A이므로 Turn 2가 Turn 1 관측을 반영하는 것은 보장이 아니다. 이 행은 동작을 기록한다: 커밋이 완료됐다면 반영, 미완료면 미반영 — 둘 다 허용 동작이다. Turn 1 커밋 시각과 Turn 2 라우팅 결과를 함께 기록해 미커밋 구간의 크기를 관측한다.
- **증거**: Turn 2 라우팅 결과와 Turn 1 커밋 시각.

#### QA-C3. 요청 스냅샷 일관성

- **상태**: PASS
- **Given**: 라우팅 계산 중인 요청. 조회 시점과 선택 시점 사이에 (a) 새 관측 커밋, (b) 기존 행 만료 경과를 각각 주입.
- **When**: 조회 결과를 해당 요청의 선택에 적용.
- **Then**: 한 요청의 라우팅은 그 요청의 조회 결과만 사용하는 일회성 데이터다 — 별도 스냅샷 프레임워크가 아니라 요청 범위 조회 결과다. (a) 조회 후 커밋된 행은 이번 요청에 반영되지 않는다(다음 요청에서 반영). (b) **선택 시점에 만료를 재평가한다** — 조회와 선택 사이에 만료가 경과한 행은 이번 결정에서 제외된다. 요청 간 관측을 재사용하는 warmth 캐시가 없음을 확인한다.
- **환경**: in-process 하네스. `TestClock`으로 만료 경과를 결정론적으로 만든다.
- **증거**: `target/test-evidence/issue-825/lifecycle-regressions.log` (59 pass, 1 existing ignored — 스냅샷 일관성 케이스 포함).

#### QA-C4. 빈 후보 키 집합 — 조회 생략

- **상태**: PASS
- **Given**: 캐시 breakpoint가 없는 요청(후보 키 집합이 비어 있음).
- **When**: 라우팅 실행.
- **Then**: 관측 저장소에 읽기 쿼리를 발행하지 않는다. 라우팅은 정상 완료된다. 단언은 쿼리 카운팅 래퍼 store로 한다(로그 부재 단언 금지).
- **환경**: in-process 하네스, 카운팅 store 래퍼.
- **증거**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 빈 키 조회 생략 케이스 포함 59 pass.

#### QA-C5. 정확 키 한정 조회

- **상태**: PASS
- **Given**: 요청의 후보 `(upstream × model × prefix × ttl)` 키 집합과 무관한 다수의 타 upstream/모델 행이 저장소에 존재.
- **When**: 라우팅 조회 실행.
- **Then**: 조회는 요청에서 도출된 후보의 upstream·model·prefix와 요청이 허용하는 TTL class로 한정된다. 바인딩 없는 무한정 조회(전체 테이블 반환)는 불합격이다. 반환 행은 후보 집합의 부분집합이다. 플래너가 작은 테이블에서 seq scan을 선택하는 것 자체는 실패 사유가 아니다 — 금지 대상은 후보 한정이 없는 쿼리다.
- **환경**: store 수준 + 라우팅 수준. Postgres에서는 `EXPLAIN`으로 인덱스 사용을 확인한다.
- **증거**: `target/test-evidence/issue-825/lifecycle-regressions.log` — 후보 한정 조회 케이스 포함 59 pass. `EXPLAIN` 출력은 `target/test-evidence/issue-825/runtime/measure-lookup/`의 pg-explain/sqlite-plan 참조.

#### QA-C6. 쿼리 플랜과 지연 측정

- **상태**: RECORDED — 측정 완료(임계값 평가가 아닌 기록 행).
- **Given**: 대표적인 카디널리티의 관측 테이블(후보 키 수와 테이블 행 수를 실제 워크로드에 근거해 정한다 — 고정 상한 가정 금지).
- **When**: 라우팅 조회를 반복 실행해 p50/p95/p99를 측정하고 `EXPLAIN (ANALYZE, BUFFERS)`를 채취.
- **Then**: 측정값과 플랜을 기록한다. PASS/FAIL 임계값은 사전에 정하지 않는다 — 캐시 전용의 임의 지연 상한을 두지 않고, 조회 지연과 캐시 적중 이득을 함께 기록해 평가한다. 측정 결과가 새 인덱스/스키마 필요성의 유일한 근거다 — 측정 전에 스키마 변경을 전제하지 않는다.
- **환경**: Postgres 라이브. SQLite에서도 동일 측정을 병행해 비교 기록.
- **증거**: `target/test-evidence/issue-825/runtime/measure-lookup/summary.txt` + pg-explain/sqlite-plan — 200회 반복, live + synthetic 50k 행. pg-live p50=0.040ms p95=0.047ms p99=0.069ms, pg-synth p50=0.167ms p95=0.178ms p99=0.185ms, sqlite-live p50=0.008ms p99=0.012ms, sqlite-synth p50=0.073ms p99=0.090ms. 한계: end-to-end/네트워크/프로덕션 카디널리티 측정이 아니므로 그 범위의 결론은 내지 않는다.


#### QA-D1. 만료 경계 정확성

- **상태**: PASS
- **Given**: `expires_at = T`인 행.
- **When**: `now = T-1`, `now = T`, `now = T+1`에서 각각 조회.
- **Then**: `expires_at > now` 계약 기준으로 `T-1`에서는 반환, `T`와 `T+1`에서는 미반환. 이 경계는 고정 계약이며 구현 결과에 맞춰 조정하지 않는다. grace는 쓰기 시점에 한 번만 차감된다(`lifecycle.rs`의 `prompt_cache_observation_expires_at`) — 조회·재저장 경로에서 grace를 다시 빼는 이중 차감은 불합격이다. 커밋이 지연돼도 저장된 절대 `expires_at`은 관측 시점 기준으로 유지되며, 저장 시점부터 TTL을 다시 세지 않는다.
- **환경**: store 수준, `TestClock` 또는 고정 시각 바인딩.
- **증거**: `target/test-evidence/issue-825/storage-stress-{1..20}.log` — 만료 경계·이중 grace 부재·지연 커밋 TTL 비재설정 포함 440 pass.


#### QA-D2. 키 구성요소 격리

- **상태**: PASS
- **Given**: upstream, canonical model, prefix, TTL class, hash schema version 중 정확히 하나씩만 다른 행들을 시딩.
- **When**: 특정 `(upstream, model, prefix, ttl)` 조합으로 조회·라우팅.
- **Then**: 각 차원이 독립적으로 격리된다. 다른 upstream/모델/prefix/TTL의 warm 행이 이번 요청의 점수에 섞이지 않는다. `hash_schema_version`이 `HASH_SCHEMA_VERSION`과 다른 행은 warmth에서 제외된다.
- **환경**: store 수준 + 라우팅 수준.
- **증거**: `target/test-evidence/issue-825/lifecycle-regressions.log` — schema+TTL 격리 케이스 포함 59 pass. `target/test-evidence/issue-825/storage-stress-{1..20}.log` — store 수준 격리 440 pass.

#### QA-D3. pod/DB 재시작과 TTL 비연장

- **상태**: PASS
- **Given**: 관측 행 커밋 후 (a) cc-lb pod 재시작, (b) Postgres 컨테이너 stop/start.
- **When**: 재시작 완료 후 만료 전 시점에 조회·라우팅.
- **Then**: 커밋된 관측은 재시작 후에도 유효하다. 만료는 절대 시각 기준이며 재시작으로 연장되지 않는다 — 재시작 소요 시간만큼 남은 수명이 줄어든 상태로 읽힌다. 만료가 지난 행은 부활하지 않는다.
- **환경**: multi-replica 하네스 또는 단일 프로세스 + Docker Postgres 재시작.
- **증거**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — pod 재시작 + Postgres 컨테이너 재시작 후 TTL 비연장 확인.

### E. 회귀 — 제거 대상

#### QA-E1. 60초 debounce 제거 — 즉시 재저장

- **상태**: PASS
- **Given**: 관측이 커밋된 키. 컴포넌트 수준에서 `TestClock`을 `advance_secs(40)`으로 40초 진행(60초 debounce 윈도우 미만).
- **When**: 동일 키의 두 번째 관측(hit 갱신) 발생.
- **Then**: 60초를 기다리지 않고 새 쓰기가 발행·커밋된다. 같은 초 안의 `>` 비교 같은 wall-clock 트릭이 아니라, TestClock 진행으로 debounce 윈도우 내임을 보장한 채 쓰기 발생을 단언한다.
- **환경**: in-process 컴포넌트 테스트(`TestClock` 주입 가능 지점). 프로세스 수준에서는 DB 행의 `last_observed_at` 비교로 대체.
- **증거**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` — debounce 제거 후 즉시 재저장(rewrite) 확인.

#### QA-E2. 관측 전용 hydration/NOTIFY 제거 — quota/rate-limit 무영향

- **상태**: PASS
- **Given**: 신구조가 배포된 프로세스.
- **When**: (a) 관측 쓰기 후 피어 pod의 동작 확인. (b) quota/rate-limit 변경 이벤트 발생.
- **Then**: (a) 라우팅은 QA-C1처럼 직접 조회로 동작하고, 관측 hydration 경로(`hydrate_from_store`의 PromptCacheObservation 분기, `cclb_prompt_cache_observation_changed` 채널)는 존재하지 않는다. (b) `UpstreamRateLimit`, `SubscriptionQuota` 등 다른 채널의 hydration/rebind는 기존과 동일하게 동작한다 — 제거 범위는 관측 전용 경로뿐이다.
- **환경**: multi-replica 또는 notify_listener 수준 테스트.
- **증거**: (a) `target/test-evidence/issue-825/runtime/RESULTS.tsv` C1 — 관측 채널 없이 직접 조회 라우팅. (b) `target/test-evidence/issue-825/notify_listener.log` — 5/5 pass, quota/rate-limit hydration이 view rebuild 없이 동작함을 포함.

#### QA-E3. 사망 설정 제거 (clean cutover)

- **상태**: PASS
- **Given**: `refresh_debounce_secs` 등 제거 대상 설정이 포함된 기존 형식의 설정 파일.
- **When**: 설정 로드/검증 실행, 어드민 설정 화면 렌더링.
- **Then**: 제거된 필드는 스키마·파서·어드민 UI(`configEditorModel.ts`의 `prompt_cache_shadow.refresh_debounce_secs` 항목)에 없다. no-op 호환 shim이나 경고 후 무시 옵션을 두지 않는다. 제거된 키를 포함한 구 설정 파일은 `deny_unknown_fields` 계약에 따라 로드가 거절된다.
- **환경**: `cargo test -p cc-lb-server --test integration`(validate 계열) + `bunx --bun vitest run` + 브라우저 QA(QA-H4).
- **증거**: `/tmp/cc-lb-825-removed_nested_fields_and_aliases_are_rejected.log` — 제거된 중첩 필드/별칭을 포함한 구 설정이 `deny_unknown_fields`로 거절됨. `target/test-evidence/issue-825/browser/desktop_prompt_cache_shadow_*.png` — UI에 제거 필드 없음.

### F. 장애·포화 — 캐시 서브시스템 오류가 요청을 실패시키지 않음을 증명

#### QA-F1. 라우팅 시점 DB 읽기 실패 (P2-A 확정)

- **상태**: PASS
- **Given**: 라우팅 조회 시점에 DB 연결 실패/타임아웃을 주입할 수 있는 store 또는 네트워크 차단.
- **When**: 읽기 실패 상태에서 요청 전송.
- **Then**: 요청은 실패하지 않는다. 캐시 친화도 가중치 없는 중립 점수로 라우팅을 계속하고, 읽기 실패는 cold cache(관측 없음 확정)로 기록하지 않는다. 오류는 OTel로 보고된다. "실패를 cold로 해석"하거나 요청을 거절하는 동작은 불합격이다.
- **환경**: 실패 주입 가능한 store 래퍼 또는 `db_unreachable_503.rs`의 컨테이너 제어 패턴.
- **증거**: `target/test-evidence/issue-825/lifecycle-regressions.log` — read-failure 시 Unknown↔cold 구분 케이스 포함 59 pass. 실제 HTTP 수준은 `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` + `fault-exported-spans.log` + `fault-span-summary.json` — 테이블 rename으로 읽기 실패 주입 후 HTTP 200, OTLP lookup Error 자식 스팬 + 성공 200 루트 확인.

#### QA-F2. 쓰기 실패 처리 (P1-A 확정)

- **상태**: PASS
- **Given**: `upsert`가 항상 실패하는 store(기존 `FailingStore` 패턴).
- **When**: 관측 발생 후 응답 완료까지 진행.
- **Then**: 쓰기 실패는 OTel 메트릭/스팬으로 관측 가능하게 기록되고, 스트리밍·비스트리밍 응답 모두 정상 완료된다(기존 `observation_failure_isolation` 계약 유지). 실패한 관측은 커밋되지 않았으므로 다음 조회에 나타나지 않는다 — 보장 시작점은 커밋 성공이다. 재시도나 outbox가 없음을 확인한다.
- **환경**: in-process 하네스.
- **증거**: `target/test-evidence/issue-825/observation_failure_isolation.log` — write-fail × stream/buffered HTTP 200 + 실제 카운터/WARN. `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` — 쓰기 실패 주입 하 HTTP 200 + OTLP 오류 자식 스팬.

#### QA-F3. 포화 시 동작 (P3 확정)

- **상태**: PASS
- **Given**: 비블로킹 sink의 유한 큐를 고갈시킨 상태에서 대량 관측 발생.
- **When**: 포화 상태에서 추가 관측.
- **Then**: QA-B9와 동일 기준. 요청은 거절되지 않고 정상 완료되며, `try_send` 실패는 OTel로 보고된다. 조용한 드롭(보고 없는 유실)은 불합격이다.
- **환경**: QA-B9와 동일.
- **증거**: `target/test-evidence/issue-825/observation_failure_isolation.log` — queue-full/closed × stream/buffered HTTP 200 + 카운터/WARN. 큐 계열은 OTLP export 대상이 아니며 in-process 카운터/WARN이 증거.

#### QA-F4. 캐시 장애 매트릭스 — 스트림/비스트림 응답 성공과 OTel 보고

- **상태**: PASS
- **Given**: 캐시 서브시스템의 각 장애를 개별 주입: (a) 라우팅 읽기 실패, (b) 발행 큐 포화, (c) 큐 닫힘, (d) store 쓰기 실패. 각 장애를 스트리밍과 비스트리밍 요청에 각각 적용.
- **When**: 장애 상태에서 요청을 완료까지 진행.
- **Then**: 모든 조합에서 응답이 정상 완료된다 — 캐시 오류로 요청이 실패하거나 거절되지 않는다. 각 장애는 OTel 자식 operation 오류/카운터로 보고되고, 성공한 HTTP 루트 스팬이 오류로 잘못 표시되지 않는다. provider hit/miss 카운터는 캐시 관측 장애와 무관하게 실제 provider 응답 기준으로 유지된다. 읽기 실패 시 라우팅은 중립 점수로 정상 제약(health/quota/rate-limit)을 그대로 적용한다.
- **환경**: 실패 주입 store 래퍼 + 큐 제어 훅 + OTel/메트릭 캡처.
- **증거**: `target/test-evidence/issue-825/runtime/fault-response-{false,true}.txt` + `fault-metrics-before/after` + `fault-exported-spans.log` + `fault-span-summary.json` — 테이블 rename 읽기/쓰기 실패 하 실제 HTTP 200, 매칭 OTLP lookup Error 자식 스팬, 성공 200 루트. 큐 계열 케이스는 OTLP 미export — `target/test-evidence/issue-825/observation_failure_isolation.log`의 in-process 카운터/WARN이 증거.

### G. 수명주기

#### QA-G1. 종료 드레인 vs 강제 종료

- **상태**: PASS
- **Given**: 발행됐지만 아직 커밋되지 않은 관측이 있는 상태. DB는 정상 응답한다.
- **When**: (a) SIGTERM(graceful drain), (b) SIGKILL, (c) drain 중 DB 장애 또는 종료 데드라인 초과.
- **Then**: (a) 정상 DB에서 큐에 남은 쓰기가 기존 종료 상한(500ms) 안에 커밋 완료된 뒤 프로세스가 종료된다 — "완료를 시도했다"는 것만으로는 불합격이며, 종료 후 DB 조회로 커밋을 확인한다. 동기화는 제어 가능한 테스트 게이트로 하고 wall-clock sleep으로 drain을 가정하지 않는다. (b) SIGKILL은 미커밋 관측 유실을 허용한다. (c) DB 장애/데드라인 초과 시에는 유실이 허용되며 그 한계를 문서에 명시한다. "종료 시 모든 데이터 보존" 같은 단언은 하지 않는다.
- **환경**: 프로세스 수준 테스트. 기존 `drain_*` 테스트 패턴 참조.
- **증거**: `target/test-evidence/issue-825/runtime/RESULTS.tsv` + `runtime/g1/` — PG SHARE lock으로 INSERT 블록을 `pg_stat_activity`로 확인한 뒤: SIGTERM+lock 해제 시 큐 쓰기 커밋(7→9), SIGKILL+lock 유지 시 유실(1→1), SIGTERM+데드라인 만료 후 해제 시 유실(1→1).

### H. 무회귀

#### QA-H1. thread lineage 진단 불변

- **상태**: PASS
- **Given**: thread usage/lineage 진단을 발생시키는 요청 시나리오(기존 `record_thread_usage_from_response` 경로).
- **When**: 변경 전후 동일 시나리오 실행.
- **Then**: thread lineage 관련 진단 출력과 `request_events_v1`의 관련 필드가 변경 전과 동일한 의미를 유지한다.
- **환경**: live QA 하네스.
- **증거**: `target/test-evidence/issue-825/observation_failure_isolation.log` — 10/10 pass. 실제 Lifecycle buffered+streamed → 실제 Tracker 진단 경로(observer bus fixture) 검증: 생성 수 1600→400으로 기존 동등성 유지, 다른 thread 누수 없음, DB 실패 시 track 유실 없음.

#### QA-H2. RequestEvent/rollup 전이 무회귀

- **상태**: PASS
- **Given**: 관측 구조 변경 전후의 동일 요청 시퀀스.
- **When**: `request_events_v1`의 cache 분석 필드(`matched_v3_cache_key`, `lookback_distance`, `predicted_*`, `token_estimate_source` 등)와 usage rollup 조회.
- **Then**: 요청 로그와 rollup의 스키마·의미가 전이 전후로 유지된다. 관측 저장 구조 변경이 이 소비자의 데이터 공급원을 바꾸지 않는다.
- **환경**: `cargo test -p cc-lb-admin --test integration` + live QA 하네스의 `wait_for_lookback_event` 계열 단언.
- **증거**: 기존 계약 패리티 기준(바이트 동일 raw DB 덤프 주장 아님). `target/test-evidence/issue-825/qa825-baseline-rollup.json` — 기준선 conformance 3/3 pass, 현재와 동일한 3개 테스트를 SQLite+PG 양쪽에서 실행. `target/test-evidence/issue-825/usage-rollup.log` — 현재 head 3/3 pass. 기준선+현재 live QA의 `request_events_v1` 캐시 필드 단언(`matched_v3_cache_key`, `lookback_distance`, `predicted_*`, `token_estimate_source`)이 양쪽에서 동일하게 유지됨. `runtime/events-{before,after}.json`, `usage-{before,after}.json` 참조.

#### QA-H3. 기존 prompt_cache_live_qa 시나리오 통과

- **상태**: PASS
- **Given**: 기존 `prompt_cache_live_qa` 모듈 전체(lookback hit, breakpoint 격리, TTL 분기 등).
- **When**: 모듈 전체 실행.
- **Then**: 신구조에서도 기존 시나리오가 통과한다. debounce 제거로 `refresh_debounce_secs = 0` 설정이 무의미해지면 설정을 정리한 뒤 동일 의도로 통과해야 한다.
- **환경**: `cargo test -p cc-lb-server --test integration prompt_cache_live_qa`.
- **증거**: `target/test-evidence/issue-825/qa825-live-stress.json` — 최신 소스 기준 live QA 8케이스 × 20회 = 160 pass(4 test threads, 77.1s). `target/test-evidence/issue-825/qa825-baseline-live.json` — 기준선 917a1003 격리 스크래치 live QA 8/8 pass. 참고: 실행 중 두 가지 테스트 전제조건을 수정 — (1) 구 대기 함수가 독립 observation writer를 기다리지 않던 레이스(turn 2 전 정확히 1개 커밋 행 대기로 수정), (2) negative invalidation 3케이스가 cold 상태에서 공허하게 통과할 수 있던 문제(두 번째 요청 전 정확한 message prefix 커밋 배리어 추가, 기존 단언 유지). 최종 fmt + clippy sqlite/postgres도 통과(`qa825-static-gates.json`).

#### QA-H4. 어드민 설정 화면 브라우저 QA

- **상태**: PASS
- **Given**: 제거된 설정 필드를 포함한 어드민 설정 화면.
- **When**: 브라우저에서 설정 화면을 열고 `prompt_cache_shadow` 섹션 확인, 설정 저장/로드 왕복.
- **Then**: 소비자 관점에서 제거된 필드가 UI에 보이지 않고, 유효한 설정의 저장/로드 왕복이 오류 없이 동작한다. 브라우저 QA 범위는 실제로 제거된 설정 UI 변경뿐이며 새 대시보드 검증을 포함하지 않는다. 단언은 UI의 소비자 가시 동작과 설정 왕복에 두고, `configEditorModel.test.ts`의 섹션 목록 같은 구현 세부를 고정하지 않는다.
- **환경**: `bun run --shell=bun build` 산출물 + 브라우저(frontend QA 절차), vitest.
- **증거**: `target/test-evidence/issue-825/browser/desktop_prompt_cache_shadow_*.png` — 브라우저에서 제거 필드 부재와 설정 왕복 확인.

#### QA-H5. 텔레메트리·요청 로그 유지

- **상태**: PASS
- **Given**: 관측 발생 요청.
- **When**: 메트릭 엔드포인트와 요청 로그 조회.
- **Then**: 쓰기 실패·발행 등 유효한 관측 메트릭과 요청 로그 필드가 유지되거나 신구조에 맞게 이관된다. 로컬 캐시 전용으로 의미가 사라진 메트릭은 제거할 수 있되, 제거한 항목과 사유를 문서에 기록한다. 제거되는 것은 debounce·hydration·관측 NOTIFY 경로뿐이다.
- **환경**: live QA 하네스 + `/metrics` 스크레이프.
- **증거**: 메트릭 인벤토리 비교(HEAD 대비 diff + 런타임 스크레이프). 유지: `cc_lb_cache_observation_dropped_total`, `cc_lb_cache_observation_write_failed_total`, `cc_lb_cache_hit_total`, `cc_lb_cache_miss_total`, `cc_lb_lifecycle_cache_hit_miss_events_total` — `target/test-evidence/issue-825/runtime/observation-metrics.txt`, `metrics-{A,B}-{before,after}.txt`, `fault-metrics-{before,after}.txt`(write_failed{store=\"postgres\"}=2, miss 카운터 실측). 추가: `cc_lb_cache_observation_read_failed_total`(신규 공유 조회 실패 경로, P2-A). 변경: `dropped`의 reason 라벨 — `abort` 제거(발행 지점이 `message_start`로 옮겨져 관측 전 abort 드롭 경로가 소멸, subscriber 삭제와 함께 제거), `channel_closed` 추가(큐 닫힘 드롭을 queue_full과 구분). 제거 사유는 `crates/cc-lb-observability/RUNBOOK.md` diff에 기록됨. 그 외 관측 메트릭 이름의 삭제 없음 — 삭제된 `prompt_cache_observation_cache.rs`는 메트릭을 내지 않았다.

#### QA-H6. 전체 회귀 스위트

- **상태**: PENDING — 현재 head Linux CI 대기 중. 로컬 전체 스위트 2306 pass; 9건 실패 중 PG 의존 4건은 격리 PG 컨테이너 재검증 4/4 pass(근본 원인은 동일 컨테이너 공유), trybuild 1건 pass, 잔여는 사전 존재 네이티브 Mac PDK SIGSEGV + cargo-deny 로컬 미설치. PDK 크래시 증거는 `target/test-evidence/issue-825/pdk-crash.log`(macOS IPS EXC_BAD_ACCESS, copy_to_guest)이며 lldb 디버깅 로그는 타임아웃으로 증거가 아니다. 전체 워크스페이스 통과 주장 금지.
- **Given**: 현재 PR head 커밋(머지 승인은 없으므로 머지된 HEAD가 아니다).
- **When**: §5의 CI 명령 전체 실행(fmt, audit-redaction lint, clippy sqlite/postgres, deny, llvm-cov nextest, coverage gate, web build/lint/typecheck/vitest). 추가로 결정론적 케이스(QA-B6/B7/B8, QA-D1/D2 등 store·라우팅 수준)를 SQLite와 Postgres에서 각각 20회 이상 반복 실행해 불안정성을 점검한다. 실패한 CI job은 원인 수정 없이 재실행으로 통과시키지 않는다.
- **Then**: 전부 통과. Postgres 의존 테스트는 `CI_POSTGRES_URL` 환경에서 실행한다. 반복 실행에서 간헐 실패가 나오면 flake로 넘기지 않고 FAIL로 기록한다.
- **환경**: CI와 동일 조건.
- **증거**: 로컬 스위트 실행 로그 + 격리 PG 재검증 결과(4/4). CI 실행 링크는 확정 후 기록.

#### QA-H7. 어드민 라우팅 프리뷰 무회귀 (async 전환)

- **상태**: PASS
- **Given**: `Lifecycle::preview_route`가 공유 저장소 조회를 위해 async로 전환되고, admin `RoutePreviewPort`/핸들러/서버 어댑터 호출자가 함께 이관된 상태. 커밋된 warm 관측, cold(관측 없음), 만료된 관측, 읽기 실패의 네 가지 저장소 상태를 각각 준비.
- **When**: 각 상태에서 어드민 라우팅 프리뷰 API 호출.
- **Then**: 프리뷰는 기존과 동일한 스코어링 의미와 현행 라우팅 제약(health/quota/rate-limit)을 반영한다 — warm 관측은 프리뷰 점수에 반영되고, cold/만료는 미반영, 읽기 실패는 fail-open 중립 점수 + OTel 오류 보고이며 프리뷰가 503으로 실패하지 않는다. 프리뷰 호출은 upstream을 실제로 호출하지 않는다. async 전환이 프리뷰의 캐시 점수 기능을 조용히 제거하면 불합격이다.
- **환경**: `cargo test -p cc-lb-admin --test integration` + 실패 주입 store.
- **증거**: `target/test-evidence/issue-825/runtime/preview-h7-{warm,cold,expired,unknown}.json` + `preview-{warm,known-cold,recovered,read-failure,inspect}.json` — warm은 warm upstream 승자와 `matched_v3_cache_key` 반영, cold/만료는 미반영, 읽기 실패는 fail-open 중립 + OTel 오류. 프리뷰는 upstream을 호출하지 않는다.

## 8. #825 불변식의 신구조 매핑

원래 #825의 관측 불변식은 "로컬에서 갱신된 만료가 오래된 DB 재로딩으로 후퇴하지 않는다"였다. 신구조에는 hydration과 pod별 장기 캐시가 없으므로, 불변식은 다음처럼 옮겨진다.

| 기존 불변식 (hydration 구조) | 신구조 대응 불변식 | 검증 행 |
|---|---|---|
| 오래된 DB 재로딩이 로컬 갱신 만료를 후퇴시키지 않음 | 동일 키에 역순 도착한 쓰기가 커밋된 만료를 후퇴시키지 않음 | QA-B7, QA-B8 |
| T+280 조회에서 warm으로 판정 | 만료 전 조회에서 커밋된 관측이 반환되고 라우팅에 반영 | QA-D1, QA-C1 |
| 새 원격 기록 반영 정상 | 다른 프로세스가 커밋한 관측을 NOTIFY 없이 조회로 반영 | QA-C1 |
| 실제 만료 정상 | 절대 만료 시각 기준, 재시작으로 연장되지 않음 | QA-D1, QA-D3 |
| upstream/model/prefix/TTL 격리 | 동일 | QA-D2 |

## 9. 미확인 전제조건

실행 전에 확인이 필요한 항목. 관측되지 않은 blocker를 단정하지 않으며, 확인 결과에 따라 행을 조정한다.

| # | 전제 | 확인 방법 | 영향 행 |
|---|---|---|---|
| U1 | compat SSE 경로(`sse_event_transform_hook` 존재 dialect)를 표현하는 테스트 fixture 존재 여부 | fixture/테스트 코드 조사 | QA-B2 |
| U2 | 쓰기를 결정론적으로 보류/해제할 수 있는 게이트된 store 주입 지점 | 구현 중 sink/store 경계 확인 | QA-B3, QA-B9, QA-F3 |
| U3 | 비블로킹 sink 큐를 테스트에서 고갈시키는 훅의 형태 | 구현 중 sink 경계 확인 | QA-B9, QA-F3 |
| U4 | `TestClock`이 프로세스 수준 서버에 주입 불가 — 컴포넌트 테스트 위치 결정 | 구현 중 확인 | QA-E1, QA-C3 |
| U5 | 대표 카디널리티(후보 키 수, 테이블 행 수)의 실측 근거 | 프로덕션 메트릭/로그 조사 또는 합리적 추정치 합의 | QA-C6 |
| U6 | Postgres에서의 #825 동형 결함(역순 upsert 후퇴) 사전 재현 여부 | QA-B7을 변경 전 Postgres 어댑터에 먼저 실행 | QA-B7 기준선 |

## 10. 증거 기록 양식

각 행 실행 후 증거 열에 다음을 기록한다.

```text
실행일시(UTC):
실행자/환경: (로컬|CI, OS, Postgres 버전)
명령: (§5의 정확한 명령)
결과: PASS|FAIL|BLOCKED
산출물: (로그/덤프/스크린샷 경로)
비고: (관측된 예외, 계약 해석 메모)
```

증거는 자격증명을 마스킹한 상태로 `target/test-evidence/` 아래 테스트별 디렉터리에 둔다.
