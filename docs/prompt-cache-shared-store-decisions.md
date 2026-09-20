# 프롬프트 캐시 공유 관측 저장소 — 결정 원장

**상태: 검토 완료 — 최종 설계 확정 (구현·QA 미착수)**

관련 문서: [구현 계획](prompt-cache-shared-store-plan.md) · [QA 계획](prompt-cache-shared-store-qa.md)
기준 커밋: `917a1003893d87a307f297863a662b6f273e2904` (v0.5.0), 이슈 #825
원천 기록: `/tmp/cc-lb-825-planning/session-dialogue.txt` (RECORD 1~117)

이 문서는 세션 전체 대화, 스카우트 패널 검토, 사용자의 우선순위 재검토 지시, 그리고 Main의 통합 감사(DesignPriority·FailurePriority 합의 포함)를 거쳐 확정된 최종 결정 원장이다. **사용자 우선순위: 응답 전달·가용성 > 캐시 히트 > 지연.** 캐시 히트 이득은 정상적인 bounded DB 조회 지연을 정당화하지만, 캐시 하위 시스템 오류가 요청을 거절·실패시키는 것은 허용하지 않는다. 패널 보고서의 과장 표현은 Main이 걸러냈으며 이 원장에 싣지 않는다.

---

## 1. 확정된 사실

### 1.1 결함 #825는 실재하며 근본 원인이 확인됐다

HEAD `917a1003`의 실제 SQLite 저장소와 캐시 코드로 재현했다.

| 시점 | 동작 | 재로딩한 캐시 | 대조군 |
|---|---|---|---|
| T+0 | 최초 관측 기록 | 만료 T+270 | 동일 |
| T+40 | hit로 수명 갱신 | 만료 T+310 | 동일 |
| T+60 | 오래된 DB 행 재로딩(hydration) | **T+270으로 후퇴** | T+310 유지 |
| T+280 | 캐시 조회 | **없음(cold) 판정** | 정상 존재(warm) |

추가로 **새 기록 → 옛 기록 순서의 DB 쓰기**도 만료를 310→270으로 후퇴시켰다. 결함은 두 군데다.

- 메모리 병합이 신선도 비교 없이 엔트리를 통째로 교체: `crates/cc-lb-server/src/prompt_cache_observation_cache.rs`
- DB UPSERT가 동일하게 무조건 덮어쓰기: `crates/cc-lb-storage-{sqlite,postgres}/src/adapter/prompt_cache_observation.rs`
- 60초 저장 생략(debounce)이 DB를 오래된 상태로 방치: `prompt_cache_observation_cache.rs`의 `refresh_debounce_secs` 경로, 설정은 `crates/cc-lb-config/src/types.rs` `PromptCacheShadowConfig`

재현 로그: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`.
**PostgreSQL에서의 수정 전 런타임 재현은 아직 수행하지 않았다** — 같은 UPSERT 코드 경로이므로 동일 결함으로 판단하되, 측정 증거는 SQLite뿐이다.

### 1.2 관리 화면은 관측 테이블을 직접 소비하지 않는다

대시보드의 캐시 토큰·비용은 사용량 집계(`cc-lb-admin/src/dashboard.rs`), 요청별 hit/miss는 요청 로그(`cc-lb-admin/src/events.rs`)에서 읽는다. 관측 저장 구조 변경이 이 화면들의 데이터 공급원을 바꾸지 않는다. 제거되는 설정 항목의 관리 UI 메타데이터(`cc-lb-admin/web/src/lib/configEditorModel.ts`)만 정리한다 — 그 이상의 신규 UI는 범위 밖이다.

### 1.3 grace는 관측 생성 시점에 이미 차감된다 (감사 정정)

`lifecycle.rs:1622–1629`의 `prompt_cache_observation_expires_at`가 `now + ttl − grace`를 계산해 레코드에 저장한다. 조회 측(`prompt_cache_observation_cache.rs:305`)은 `expires_at > now`만 비교한다. 따라서 **읽기 경로에서 `now + grace`를 다시 더하면 안 된다** — grace는 한 번만 적용한다. 지연 커밋이 TTL을 새로 갱신하지도 않는다 — 만료는 관측 시점에 계산된 절대 시각이다.

---

## 2. 최종 결정 통합 (KEEP / CHANGE / REMOVE)

### KEEP — 유지

| 항목 | 내용 |
|---|---|
| PostgreSQL 우선 + SQLite 보존 | 공유 저장소로 기존 PostgreSQL primary 사용. SQLite는 로컬 개발·CI용으로 같은 계약을 유지 |
| 좁은 백엔드 중립 트레이트 | 기존 `PromptCacheObservationStore`를 유지·확장(후보 키 일괄 읽기 + 원자적 단조 쓰기). 향후 Redis 등 다른 백엔드는 같은 동작 계약을 만족해야 하며 자체 배선·검증이 필요하다 — "무수정 교체"를 약속하지 않는다 |
| 기존 스키마·인덱스로 시작 | `prompt_cache_observations` 테이블과 기존 인덱스가 초기에 충분하다고 판단. 신규 스키마·인덱스는 쿼리 플랜 측정 후 필요 시에만 — 측정된 인덱스 증명을 주장하지 않는다 |
| 라우팅 우선순위·건강·quota·필터 | 기존 라우팅 파이프라인의 건강·quota·필터 우선순위를 그대로 유지. 캐시 점수는 그 안의 한 요소일 뿐 |
| thread_usage 진단 텔레메트리 | `thread_usage_score`·`record_thread_usage`·`lineage_counterfactual_from_thread_usage`는 별도 계보의 counterfactual 진단 텔레메트리. warmth 권위가 아니며 분리·보존 |
| quota·rate-limit hydration/NOTIFY | 관측 전용 경로만 제거하고 다른 채널은 유지 |
| 텔레메트리·요청 로그 | 기존 관측 가능성 유지. provider 사용량 메트릭은 영향 없음 |

### CHANGE — 변경

| 항목 | 내용 |
|---|---|
| 라우팅 조회 | pod 로컬 관측 캐시 권위를 폐기하고, 요청의 후보 upstream × prefix 키를 공유 저장소에서 일괄 조회. **정상적인 bounded DB await를 허용** — 캐시 히트 이득이 이 비용을 정당화한다. 결과는 그 요청 안에서만 쓰는 통상적인 임시 조회 결과이며, 별도 스냅샷 프레임워크를 만들지 않는다 |
| 관측 발행 | 첫 유효 provider 사용 정보 확보 즉시(SSE `message_start`, compat SSE 동일 지점, buffered 논스트림은 응답 수신 완료) 직결 싱크에 `try_send`로 비차단 인큐. 사전 검사·대기 없음 |
| 쓰기 경로 | `InMemoryBus` 경유 + subscriber의 이중 큐를 제거하고 `PromptCacheObservationSinkLike` 직결 포트 + 기존 단일 유한 큐로 단순화 |
| DB 갱신 | 원자적 승자 갱신: 동일 키의 유효 만료가 도착 순서로 후퇴하지 않고, 승자 레코드의 메타데이터가 함께 보존된다(오래된 메타데이터 혼합 금지). 동률은 만료 → `last_observed_at` 순으로 결정. 동일 관측의 반복 쓰기는 멱등 |
| 읽기 실패 | 조회 실패 시 OTel로 보고하고 캐시 친화도 없이 중립 라우팅 계속. 실패를 cold와 동일시하지 않고 별도 신호로 남긴다. 요청을 실패시키지 않는다 |
| 쓰기 실패 | 인큐 실패(큐 포화·닫힘)·DB 쓰기 실패를 OTel로 보고하고 요청 처리 계속 |
| 장애 텔레메트리 | 캐시 오류는 자식 operation/span과 기존 카운터로 기록하고, 좁은 읽기 실패 카운터를 추가한다. 성공한 응답의 루트를 오류로 표시하지 않는다 |
| 종료 | 정상 종료 시 기존 `app.rs`의 `SHUTDOWN_TASK_TIMEOUT`(500ms) 안에서 드레인을 시도한다. 종료 프레임워크를 확장하지 않는다 |

### REMOVE — 제거·기각

| 항목 | 사유 |
|---|---|
| 60초 debounce (`refresh_debounce_secs`) | #825의 직접 원인. 설정·스키마·UI 메타데이터·문서에서 완전 삭제, shim 없음 |
| 관측 전용 hydration/NOTIFY | 라우팅이 공유 저장소를 직접 읽으므로 불필요 |
| pod 로컬 warmth 캐시 권위·폴백 | replica-local warmth 폴백은 공유 계약을 깬다 |
| 디스패치 전 용량 예약·수락 검사 | 사용자가 명시 기각 — "최적화 하겠다고 요청을 거부해?" |
| 캐시 실패 시 요청 거절(503) | 가용성 우선순위 위반 |
| 응답 종료 시 커밋 대기(barrier) | 사용자가 독립 완료 선택 |
| 임의의 짧은 타임아웃 값 | 근거 없는 수치를 설계에 박지 않는다 |
| 지금 Redis 구현 | 향후 최적화 후보일 뿐. cold-start 시 전면 miss 위험으로 사용자가 PostgreSQL 우선 지시 |
| 재시도 루프·durable outbox·신규 외부 서비스 | 복잡도 대비 이득 없음, 별도 승인 없이 도입 금지 |
| 분산 exactly-once 주장 | 이 설계는 그런 보장을 제공하지 않으며 주장하지도 않는다 |

---

## 3. 확정된 완료·실패 계약 (사용자 결정 — FINAL)

### P1. 완료 경계 — 독립 완료

응답 종료와 무관하게 백그라운드에서 커밋한다. 응답 종료 시점에 미완료 저장의 커밋을 기다리는 합류 지점을 두지 않는다. 커밋 실패는 OTel로 기록하되 진행 중인 응답을 깨지 않는다. 클라이언트 abort는 별도 제품 질문이 아니다 — 이미 인큐된 유효 사용 증거는 연결 해제만으로 취소하지 않고, 증거 확보 전에는 쓰기를 만들지 않는다.

### P2. 읽기 실패 — 캐시 친화도 없이 계속

조회 실패 시 OTel로 보고하고 캐시 친화도 없이 일반 라우팅 규칙으로 계속한다. 실패는 "관측 없음(cold)"과 다른 상태로 기록한다. 캐시 관측 실패로 요청을 5xx로 거절하지 않는다.

### P3. 쓰기 과부하/실패 — 즉시 시도, 실패 시 OTel 보고 후 계속

사용자 원문: "그냥 시도했다가 에러가 나면 otel에 잘 표기만 하고 넘어가야지 … 응답이 나가는게 cache hit보다 중요해 … 최적화 하겠다고 요청을 거부해?" — 관측 확보 즉시 기존 단일 유한 큐에 비차단 `try_send`를 시도하고, 실패는 OTel로 보고하며 요청을 계속한다.

---

## 4. 폐기된 제안 연대기 (SUPERSEDED)

| 폐기안 | 경위 | 대체 |
|---|---|---|
| pod 로컬 캐시 유지 + 메모리 단조 병합 (RECORD 50 초기 분석안) | RECORD 54에서 사용자가 "pod끼리 무조건 실시간 공유" 요구로 구조 재검토 지시 | 공유 저장소 직접 조회 |
| 60초 debounce 유지 | RECORD 51 사용자 반박; 성능 근거 없는 최적화 | 완전 제거 |
| 첫 응답 이벤트 전 DB 커밋 대기 (RECORD 71~73) | RECORD 74에서 사용자가 세션 순차성을 근거로 반박 | 즉시 비동기 발행 |
| 즉시 Redis 전면 전환 | RECORD 90에서 cold-start 전면 miss 위험과 100ms 허용 판단으로 PostgreSQL 우선 지시 | PostgreSQL 우선 + 좁은 트레이트 |
| etcd/Raft 등 합의 계층 | 오버엔지니어링 | 기존 PostgreSQL 활용 |
| 관리 화면 실시간 관측 소비 가설 | 실사 결과 관측 테이블 직접 소비자 없음 | 죽은 설정 메타데이터 정리만 |
| `last_observed_at` 비교로 신선도 판정 | 관측 시각이 아니라 구독자 처리 시각이라 오판 가능 | DB 원자적 승자 갱신 |
| 디스패치 전 용량 사전 예약 (Main 제안) | 사용자가 우선순위 재검토에서 명시 기각 | 즉시 시도 + OTel 보고 |
| 읽기 경로 `now + grace` 재적용 | 감사 정정 — grace는 생성 시 이미 차감됨(§1.3) | `expires_at > now` 비교만 |

---

## 5. 패널 기여와 Main 정정

| 패널 | 수용한 기여 | Main 정정·각주 |
|---|---|---|
| DecisionAudit | 연대기 원장, 확정/기각/미결정 분리 | 패널 합의가 아닌 사용자 확인 필수로 격 상향 → 사용자 결정으로 종결 |
| ReadArchitecture | 후보 일괄 조회 인터페이스, 동기 라우팅 구역 밖 비동기 fetch | per-lookup `to_owned` 할당 금지; `GREATEST(expires_at)` + `EXCLUDED.*` 메타데이터 덮어쓰기 스케치는 승자 메타데이터 일관성 위반으로 정정; 인덱스 충분성은 측정 과제; 스냅샷 프레임워크는 통상적 임시 조회 결과로 단순화 |
| WriteArchitecture | 직결 싱크 포트, 이중 큐 제거, `CancellationToken` 드레인 | 사전 예약은 사용자가 기각; 소스 검사 기반 타당성이며 런타임 증명 아님; thread_usage는 진단 계보 |
| QADesign | 멀티 레플리카 시나리오, `fake-anthropic` 타이밍 제어 | 외부 실 API·비결정적 sleep 배제; 상세는 QA 문서 |
| DesignPriority·FailurePriority (감사) | 우선순위 기준 전체 결정 재검토 합의, 장애 텔레메트리 계층 | Main이 double-grace·zero-latency·at-least-once 과장을 기각 |

**배제된 과장 표현**: 0ms 지연, 100% 보장, 임의의 50ms 타임아웃, 11분 전제, "Redis 완벽 대체", 인덱스 스캔 보장, DB 오류 시 cold 처리, 종료 시 무조건 전량 보존, 분산 exactly-once.

---

## 6. 명시적 비보장

- **비동기 쓰기 시작 ≠ 다음 턴 전 커밋.** 보장은 성공적 커밋 시점부터 시작한다. 응답 완료 후 즉시 도착한 다음 요청이 아직 미커밋 관측을 못 볼 수 있다 — 사용자가 수용한 간극이다.
- **provider 내부 캐시 상태는 보장하지 않는다.** 공유하는 것은 provider 응답에서 관측한 증거이지 provider 캐시의 현재 상태가 아니다.
- **관측 유실 ≠ provider 캐시 삭제.** 관측을 잃으면 warm 판단 근거를 잃어 miss 가능성이 커질 뿐이다.
- **종료 드레인은 기존 500ms 안의 best-effort다.** 장애·kill 시 유실을 보장하지 않으며, 확장된 종료 프레임워크를 두지 않는다.
- **PostgreSQL 수정 전 런타임 재현은 아직 없다** — QA에서 다룬다.

---

## 7. 증거 경로

- 세션 대화: `/tmp/cc-lb-825-planning/session-dialogue.txt`
- 재현 로그: `/tmp/cc-lb-825-repro-native.log`, `/tmp/cc-lb-825-controls.log`
- 패널 보고: `agent://DecisionAudit`, `agent://ReadArchitecture`, `agent://WriteArchitecture`, `agent://QADesign`
- 주요 소스: `crates/cc-lb-storage-api/src/prompt_cache_observation.rs`, `crates/cc-lb-server/src/prompt_cache_observation_cache.rs`, `crates/cc-lb-server/src/prompt_cache_observation_sink.rs`, `crates/cc-lb-server/src/notify_listener.rs`, `crates/cc-lb-engine/src/sse_relay.rs`, `crates/cc-lb-engine/src/lifecycle.rs`, `crates/cc-lb-config/src/types.rs`
