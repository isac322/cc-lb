# 프롬프트 캐시 공유 관측 저장소 — 구현 계획

**상태: 검토 완료 — 최종 설계 확정 (구현·QA 미착수)**

선행 문서: [결정 원장](prompt-cache-shared-store-decisions.md) · 후속 문서: [QA 계획](prompt-cache-shared-store-qa.md)

이 계획은 결정 원장의 최종 설계를 구현 단위로 옮긴 것이다. **사용자 우선순위: 응답 전달·가용성 > 캐시 히트 > 지연.** 캐시 히트 이득은 정상적인 bounded DB 조회 지연을 정당화하고, 캐시 하위 시스템 오류는 요청을 거절·실패시키지 않는다.

---

## 1. 목표와 비목표

**목표**
- pod 로컬 관측 캐시 권위를 폐기하고, 라우팅이 공유 저장소(PostgreSQL primary)를 직접 일괄 조회하게 한다.
- 첫 유효 provider 관측 시점에 비동기 저장을 시작하고, 첫 스트림 프레임 전 DB 대기를 두지 않는다.
- 60초 debounce와 관측 전용 hydration/NOTIFY를 제거한다.
- DB에서 동일 키의 유효 만료가 도착 순서로 후퇴하지 않게 한다(원자적 승자 갱신). 요청 간 메모리 병합은 제거된다.

**비목표**
- Redis 구현(향후 최적화 후보일 뿐), durable outbox, 재시도 루프, 신규 외부 서비스.
- quota·rate-limit·구독 등 다른 hydration/NOTIFY 채널 변경.
- 관리 화면의 관측 데이터 신규 소비 기능 — 죽은 설정 메타데이터 정리 외 신규 UI 없음.
- PR 머지 — 머지 승인은 이 작업에 포함되지 않는다.

---

## 2. 통합 인터페이스

### 2.1 읽기 — 라우팅 경로

- `crates/cc-lb-storage-api/src/prompt_cache_observation.rs`의 기존 `PromptCacheObservationStore` 트레이트에 **후보 일괄 조회**를 추가한다. 개념 시그니처(최종 명명은 구현 시 확정):
  - `list_active_for_candidates(upstream_ids, canonical_model_id, prefix_keys, not_expired_at_unix_secs) -> Vec<PromptCacheObservationRecord>`
  - 기존 `list_active_for_upstream*`의 per-upstream 반복 호출을 대체한다. 기본 빈 구현이나 조용한 폴백을 두지 않고 각 백엔드가 명시 구현한다.
- `crates/cc-lb-engine/src/lifecycle.rs`: 동기 라우팅 구역(`route_span.enter()`, ~2584) 진입 **전**에 비동기로 1회 조회한다. **정상적인 bounded DB await를 허용한다** — 캐시 히트 이득이 이 비용을 정당화한다.
  - 조회 결과는 그 요청 안에서만 쓰는 통상적인 임시 결과다. 별도 스냅샷 프레임워크를 만들지 않는다.
  - 조회는 조회 시점의 뷰를 담을 뿐 동시 진행 중인 미래 커밋을 반영하지 않는다. 조회 후 결정 시점까지 행이 만료될 수 있으므로 사용 시점에 `expires_at > now`를 다시 확인한다.
  - **grace는 읽기에서 다시 적용하지 않는다.** 만료는 관측 생성 시 `now + ttl − grace`로 이미 계산되어 저장된다(`lifecycle.rs:1622–1629`). 읽기는 `expires_at > now` 비교만 한다 — `now + grace`를 더하는 이중 적용 금지.
  - 락 없는 메모리 탐색이며, 조회당 불필요한 문자열 할당(예: 키 비교용 `to_owned`)을 만들지 않는다.
  - `prefix_keys` 또는 후보 upstream이 비어 있으면 쿼리를 생략한다.
  - **조회 실패 시**: OTel로 보고하고 캐시 친화도 없이 중립 라우팅을 계속한다(P2). 실패를 cold와 동일시하지 않는다.
- `crates/cc-lb-control/src/dynamic_view.rs`·`traits.rs`: 라우팅이 저장소에 접근할 경로를 `DynamicView`에 노출하고, 라우팅 권위로서의 `PromptCacheObservationCacheLike` 사용을 제거한다. 기존 라우팅의 건강·quota·필터 우선순위는 그대로 유지한다.

### 2.2 쓰기 — 관측 발행 경로

- `crates/cc-lb-engine/src/sse_relay.rs` (fast SSE 경로): 현재 `message_start`에서 디코드하고 `message_stop`까지 버퍼하는 구조(387~410)를, **`message_start`에서 첫 유효 관측 확보 즉시 발행**으로 바꾼다. 요청당 1회 발행을 멱등으로 보장한다.
- `crates/cc-lb-engine/src/lifecycle.rs` (compat SSE 경로 ~4022~4470, buffered 논스트림 ~3522~3532): 같은 규칙 — 첫 유효 provider 사용 정보 확보 즉시 비동기 발행. 논스트림은 응답 수신 완료가 첫 확보 지점이다.
- 발행 포트: `PromptCacheObservationSinkLike`(`cc-lb-control/src/traits.rs`)를 `LifecycleContext`/`SseRelay`에 직접 주입하는 **직결 포트**로 정리해 `InMemoryBus` 경유 + `lifecycle_prompt_cache_observation_subscriber.rs`의 이중 큐를 제거한다.
- `crates/cc-lb-server/src/prompt_cache_observation_sink.rs`: 기존 단일 유한 큐의 비동기 writer. 비차단 `try_send` 시도 — 사전 검사·예약·대기 없음. 인큐 실패(큐 포화·닫힘)와 이후 DB 쓰기 실패는 OTel로 보고하고 요청을 계속한다(P3). 재시도·outbox 없음.
- `crates/cc-lb-server/src/app.rs`: 정상 종료 시 기존 `SHUTDOWN_TASK_TIMEOUT`(500ms, `app.rs:91`) 안에서 큐 드레인을 시도한다. 전량 보존을 약속하지 않으며 종료 프레임워크를 확장하지 않는다.
- **abort 불변식**: 이미 인큐된 유효 사용 증거는 클라이언트 연결 해제만으로 취소하지 않고, 증거 확보 전에는 쓰기를 만들지 않는다.

### 2.3 저장소 — 원자적 승자 갱신

- `crates/cc-lb-storage-postgres/src/adapter/prompt_cache_observation.rs`: `ON CONFLICT ... DO UPDATE`를 **승자 일관 갱신**으로 수정한다. 동일 키의 유효 만료가 도착 순서로 후퇴하지 않고, 승자 레코드의 메타데이터가 함께 보존된다 — `expires_at`만 최대로 올리고 나머지 컬럼을 패자 값으로 덮어쓰는 형태는 금지. 동률은 만료 → `last_observed_at` 순으로 결정하고, 동일 관측의 반복 쓰기는 멱등이다. 관측 전용 `pg_notify` 발행을 제거한다.
- `crates/cc-lb-storage-sqlite/src/adapter/prompt_cache_observation.rs`: 동일 계약을 SQLite `UPSERT`로 구현해 로컬/CI 동치성을 유지한다.
- 마이그레이션: 기존 `0072_v3_prompt_cache_observations.sql`(PG)/`0042_v3_prompt_cache_observations.sql`(SQLite) 테이블과 인덱스로 시작한다. **신규 스키마·인덱스는 쿼리 플랜 측정(`EXPLAIN`) 후 필요가 확인될 때만 추가한다** — 측정된 인덱스 증명을 주장하지 않는다.

### 2.4 장애 텔레메트리

- 캐시 하위 시스템 오류(조회 실패·인큐 실패·쓰기 실패)는 자식 operation/span과 기존 카운터로 기록하고, 좁은 읽기 실패 카운터를 추가한다.
- 성공한 응답의 루트 span을 오류로 표시하지 않는다. provider 사용량 메트릭은 영향 없음.

---

## 3. 확정된 완료·실패 계약 (사용자 결정)

| 정책 | 확정 내용 |
|---|---|
| **P1 완료 경계** | 독립 완료. 발행 후 요청 종료 경로에 커밋 대기·합류 지점을 두지 않는다. 커밋 실패는 OTel로 기록하고 진행 중인 응답을 깨지 않는다. |
| **P2 읽기 실패** | 조회 실패를 OTel로 보고하고 캐시 친화도 없이 라우팅을 계속한다. 실패를 cold와 동일시하지 않고 별도 신호로 남긴다. 캐시 관측 실패로 요청을 거절하지 않는다. |
| **P3 쓰기 과부하** | 관측 확보 즉시 기존 단일 유한 큐에 비차단 `try_send`를 시도한다. 인큐 실패·DB 쓰기 실패는 OTel로 보고하고 요청을 계속한다. 사전 예약·수락 검사·요청 거절·재시도·신규 durable 큐 없음. |

---

## 4. 파일·심볼 그룹별 변경 계획

### 4.1 저장소 API / PostgreSQL / SQLite

| 파일 | 변경 |
|---|---|
| `crates/cc-lb-storage-api/src/prompt_cache_observation.rs` | `PromptCacheObservationStore`에 후보 일괄 조회 추가; `upsert_observation` 계약을 원자적 승자 갱신으로 명문화 |
| `crates/cc-lb-storage-postgres/src/adapter/prompt_cache_observation.rs` | `ANY()` 기반 일괄 조회 구현, 승자 일관 upsert, 관측 `pg_notify` 제거 |
| `crates/cc-lb-storage-sqlite/src/adapter/prompt_cache_observation.rs` | `IN` 바인딩 일괄 조회, 동일 승자 일관 upsert |
| `crates/cc-lb-storage-conformance` | 두 백엔드의 일괄 조회·승자 갱신 동치성 검증 추가 |

### 4.2 엔진 — 라우팅 + SSE fast/compat/buffered

| 파일 | 변경 |
|---|---|
| `crates/cc-lb-engine/src/lifecycle.rs` | `route_span` 진입 전 일괄 조회 + 임시 결과 전달; `build_candidates_with_matches`의 warmth 조회를 조회 결과 기반으로 교체; `prompt_cache_observation_context`(~1483)에서 로컬 캐시 의존 제거; compat SSE(~4022~4470)·buffered(~3522) 발행 시점 전진 |
| `crates/cc-lb-engine/src/sse_relay.rs` | `message_start` 즉시 발행, `message_stop` 버퍼 경로 제거, 요청당 1회 멱등 발행 |
| `crates/cc-lb-engine/src/lifecycle_prompt_cache_observation_subscriber.rs` | 직결 포트 전환으로 제거 또는 축소 — 호출자 조사 후 결정 |

### 4.3 컨트롤 — dynamic view / traits / event bus

| 파일 | 변경 |
|---|---|
| `crates/cc-lb-control/src/traits.rs` | `PromptCacheObservationCacheLike`의 라우팅 권위 메서드(`snapshot_for_upstream` 등) 정리; `PromptCacheObservationSinkLike`는 비차단 `try_send` 계약 유지 |
| `crates/cc-lb-control/src/dynamic_view.rs` | `prompt_cache_observation_cache` 필드·`prompt_cache_observation_cache_opt()` 제거 또는 저장소 핸들로 교체; `prompt_cache_observation_sink` 경로 정리 |
| 이벤트 버스 | 관측 전용 `InMemoryBus` 채널·구독자 제거(직결 포트로 대체). 다른 라이프사이클 이벤트 버스는 유지 |

### 4.4 서버 — wiring / sink / cache / notification

| 파일 | 변경 |
|---|---|
| `crates/cc-lb-server/src/prompt_cache_observation_cache.rs` | 라우팅용 로컬 관측 맵·`hydrate_from_store`·debounce 상태 제거. **`thread_usage` 계열은 별도 계보 진단 텔레메트리이므로 분리·보존** — warmth 권위로 취급하지 않는다 |
| `crates/cc-lb-server/src/prompt_cache_observation_sink.rs` | 기존 단일 유한 큐 writer, 비차단 `try_send` + OTel 실패 보고, `CancellationToken` 드레인 |
| `crates/cc-lb-server/src/notify_listener.rs` | `ChangeChannel::PromptCacheObservation` hydrate 분기만 제거. rate-limit·quota 채널과 rebind 경로는 유지 |
| `crates/cc-lb-server/src/dynamic_view_builder.rs` | 관측 캐시 생성·hydrate 호출(~210~234, ~317~324) 제거, 저장소 핸들 배선 |
| `crates/cc-lb-server/src/app.rs` | 싱크 수명주기·기존 500ms 종료 타임아웃 내 드레인 배선 |

### 4.5 설정 / 스키마 / 관리 설정 메타데이터

| 파일 | 변경 |
|---|---|
| `crates/cc-lb-config/src/types.rs` | `PromptCacheShadowConfig.refresh_debounce_secs` 삭제. `max_live_entries_per_partition` 등 로컬 캐시 전용 필드는 호출자 조사 후 함께 정리. **`grace_margin_secs` 정책은 변경하지 않는다** — 만료 계산이 계속 사용하므로 유지 |
| `crates/cc-lb-admin/web/src/lib/configEditorModel.ts` | 삭제된 설정 필드의 UI 메타데이터·설명 정리 |
| 설정 문서·예시 | 죽은 키 제거(clean cutover, shim 없음) |

### 4.6 테스트 / 문서

- `prompt_cache_observation_cache.rs`·`sse_relay.rs`·`lifecycle.rs` 내 기존 테스트를 새 계약에 맞게 수정하거나, 구현 세부를 고정하는 테스트는 삭제한다.
- 회귀 가치가 있는 테스트만 남긴다: DB 승자 일관 갱신, 일괄 조회 필터링, `message_start` 즉시 발행, debounce 제거 후 즉시 반영.
- QA 시나리오는 [QA 계획](prompt-cache-shared-store-qa.md)을 따른다.

---

## 5. 사전 조사 (구현 전 필수)

1. **호출자 전수 조사**: `PromptCacheObservationCacheLike`, `prompt_cache_observation_cache_opt`, `hydrate_from_store`, `PromptCacheObservationSinkLike`, `refresh_debounce_secs`, `PromptCacheObservation` 채널의 모든 참조를 확인한다. **LSP 참조 조회가 필수이며, 소스 grep과 codegraph callers/impact는 보조 수단이다** — codegraph만으로 LSP를 대체하지 않는다. export된 API 변경 전에 소스 전체 호출자를 확정한다.
2. **쿼리 플랜 측정**: 일괄 조회 SQL의 `EXPLAIN`을 실제 스키마에서 확인하고 인덱스 추가 필요 여부를 결정한다.
3. **확정 계약 반영**: §3의 P1~P3 사용자 결정을 인터페이스 계약에 반영한다.

---

## 6. 소유권 경계와 진행 순서

- **Main = 통합 소유자.** 공유 파일(`lifecycle.rs`, `traits.rs`, `dynamic_view.rs`)의 최종 통합과 충돌 해결을 담당한다.
- 병렬 작업 경계: 저장소 어댑터(4.1) / 엔진 발행·조회(4.2) / 서버 wiring(4.4) / 설정·UI(4.5)은 파일이 갈리므로 병렬 가능. `traits.rs`·`dynamic_view.rs` 계약을 먼저 고정한 뒤 착수한다.
- 순서: 사전 조사(§5) → 인터페이스 계약 고정 → 그룹별 구현 → 로컬 스모크/QA/회귀 + 내부 리뷰 → 커밋·PR 생성 → GitHub CI·리뷰를 green까지. **머지는 별도 승인 없이 수행하지 않는다.**

---

## 7. 향후 Redis 대비 설계 원칙

- `PromptCacheObservationStore`의 읽기(후보 키 일괄 조회)·쓰기(원자적 승자 갱신) 계약을 백엔드 중립으로 유지한다 — Redis의 `MGET`/Lua 원자 갱신으로 같은 계약을 구현할 수 있는 형태.
- 단, **"무수정 교체"를 약속하지 않는다.** 미래 백엔드는 같은 동작 계약을 만족해야 하며 자체 배선·설정·검증이 필요하다.
- 이번 변경에서 Redis 전용 개념(키 스키마, TTL 명령)을 인터페이스에 새기지 않는다.

## 8. 명시적 비보장

- 비동기 쓰기 시작이 다음 턴 전 커밋을 뜻하지 않는다. 보장은 성공적 커밋부터다.
- DB 읽기 실패는 cold가 아니며 OTel로 보고 후 중립 라우팅을 계속한다.
- provider 내부 캐시 상태는 보장 범위 밖이다.
- 종료 드레인은 기존 500ms 안의 best-effort다. 장애·kill 시 유실을 보장하지 않는다.
- 분산 exactly-once를 주장하지 않는다.
- PostgreSQL 수정 전 런타임 재현은 아직 없다 — QA에서 다룬다.
