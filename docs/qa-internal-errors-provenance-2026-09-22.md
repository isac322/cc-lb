# Issue #849 사전 구현 QA 문서 — internal_errors provenance

- 작성일: 2026-09-22
- 대상 이슈: [isac322/cc-lb#849](https://github.com/isac322/cc-lb/issues/849) — cc-lb 자체 실패 사유가 폐기됨 (`internal_errors` 6단계 선언, 3개만 실제 생성)
- 기준 HEAD: `1561ee9f371ca74c01c70cca6d81145f6d2ae218` (worktree `isac322/cc-lbs-own-failure-reasons-are-discarded-interna`). 원격 master `26c9d7850c633effd5b8fd6e3fe3001739842c2a`와의 차이는 `composite_signer_dispatch` 테스트뿐이며 런타임 코드는 동일(Main이 gh로 검증) — worktree는 master 대비 테스트 1커밋 차이이며 fast-forward 완료 상태는 아님
- 최신 릴리스: `cc-lb-v0.5.0` (2026-09-20T09:16:15Z 게시, 배포 digest 미확인)
- 최종 검증: 구현 커밋 `1a4ff7bad771cfce83669d0682daf8710b3696a2`의 CI `35725913107`과 publish-check `35725913252`가 통과했다. nextest는 **2,505 PASS / 15 skipped**이며 인증·시도 typestate compile-fail 테스트도 각각 PASS했다. E2E, coverage, fmt, SQLite/PostgreSQL clippy, cargo-deny, promtool, crate-version guard가 통과했고 release-artifact 검사는 조건부 skipped였다. 로컬 strict lint는 `cargo clippy -p cc-lb-server -p cc-lb-engine -p tests-integration --all-targets -- -D warnings`로 통과했다. 로컬 trybuild 링커 제한은 CI에서 검증 완료했다. §10의 PASS는 실제 실행 증거가 있는 항목만 뜻한다. **2026-09-22 사용자 승인으로 QA-ROUTE-03을 N/A 종결하여 최종 집계는 50 PASS / 1 N/A**이며 51개 실행 PASS가 아니다. DOWNGRADE 호환은 사용자 승인으로 제외했다.


---

## 1. 목적과 범위

이 문서는 #849 수정 구현에 앞서 확정하는 QA 체크리스트다. 목적:

1. 수정 후 각 실패 경로가 `request_events_v1.payload.internal_errors`에 올바른 `(stage, kind, message)`를 남기는지 검증할 원자적 케이스를 열거한다.
2. 내부 진단(`internal_errors`)과 업스트림 진단(`upstream_error_type`/`upstream_error_message`)의 상호 배타 규칙을 고정한다.
3. 구현 후 각 항목의 실행 결과를 §10에 기록한다. 조사 당시 `BASELINE_CONFIRMED`는 수정 전 결함 재현이며 수정 후 PASS와 구분한다. 최종 실행 결과와 사용자 승인 N/A 판정은 위 요약과 §10의 상태를 따른다.

비범위: #850(메트릭 레이블) 구현. 이 문서 작성 당시에는 애플리케이션 코드 변경과 빌드/린트/테스트를 보류했고, 이후 사용자 승인으로 구현과 QA·CI를 수행했다.

**역사적 손실은 코드 수정으로 복구되지 않는다.** 32,527건은 이슈 보고자의 프로덕션 집계이며 로컬에서 재검증되지 않았다. 수정은 미래의 실패를 기록할 뿐이며, 백필을 만들지 않는다.

---

## 2. 근거 경계 (Evidence Boundary)

### 2.1 실행 확정 (Main 실서버 베이스라인, HEAD `1561ee9f`)

증거 디렉터리: `/data/tmp/cc-lb-849-repro-ano_sbq8/`

| 증거 파일 | 시나리오 | 관측 결과 |
|---|---|---|
| `auth-reproduction.json` | 무효 API 키 POST | HTTP 401 `authentication_error`/`"invalid api key format"`; 정확히 1개 행, `internal_errors=null`, `upstream_error_*=null`, `dns_ms/connect_ms=null` |
| `expanded-reproduction.json` | 무효 키 ×2, malformed JSON, 유효 JSON(업스트림 없음), 짧은 Content-Length + `shutdown(SHUT_WR)` | 5요청 5개 distinct 행. `invalid_json`(400), `body_read_failed`(400) 모두 `internal_errors=null`. 대조군 `route_no_upstream_after_filter`(503)는 `[{stage:"router_filter",kind:"unavailable",message:"no upstream candidates remain after routing filters"}]` 정상 기록. 404는 추가 행 0 |
| `transport-reproduction.json` | 닫힌 포트로 TCP connect (독립 errno 61 확인) | 502 `upstream_dispatch_failed`, `internal_errors/dns_ms/connect_ms/upstream_error_*` 전부 null |
| `dns-reproduction.json` | `.invalid` 도메인 DNS 실패 | 502 `upstream_dispatch_failed`, 동일하게 전부 null |
| `tls-reproduction.json` | HTTPS→평문 리스너 TLS 핸드셰이크 실패 (fixture 로그가 ClientHello 거부 확인) | 502 `upstream_dispatch_failed`, 동일하게 전부 null — Main 실행 확인 |
| `upstream-reproduction.json` | 업스트림 HTTP 429 `{"error":{"code":"rate_limit_exceeded","message":"isolated upstream rejection"}}` (버퍼 경로) | 응답 그대로 통과, `error_code=upstream_4xx`, `upstream_error_type=null`, `upstream_error_message`=원본 바디, `internal_errors=null` |
| `cleanup.json` | 임시 키 폐기 후 401, 소유 프로세스 3개 종료, 리스너 5개 거절 확인 | 정리 완료 |

실행 관측은 위 7개 범주(auth, invalid_json, body_read, 대조군 route, TCP/DNS/TLS dispatch, upstream 429)에 한정된다. signer/storage/limit 등 나머지는 소스 분석만.

### 2.1a 결함 도입 provenance (Main `git log --follow -S` 독립 검증)

- 빈 소비 암(no-op) 도입: `5f21a25cad919da9a577c46adda7666e0b3f1f99` (PR #388, 2026-07-11T16:40:33+09:00)
- 현재 위치로 이동: `4b659333d27d979b3a57347197453bb75d5d74ea` (PR #401, 2026-07-12T16:30:14+09:00)
- 릴리스 태그 `cc-lb-v0.5.0`의 어셈블러에도 동일 no-op 존재 — **소스 확인만, 릴리스 바이너리 미실행**. 배포 digest 미확인.
- 이전 스카우트 보고의 7월 28/31일 날짜는 오류 — 위 Main 검증값이 정본.

### 2.2 소스 분석만 확정 (실행 미검증)

아래 항목은 코드 리딩으로만 확정됐다. 런타임 관측은 없다.

- `emit_provider_error` 18개 생산자(`lifecycle.rs` 17 + `authn_rail.rs:95` 1) → `LifecycleEvent::ProviderErrorObserved` → 어셈블러 빈 암(`lifecycle_event_assembler.rs:1212`)으로 폐기.
- `InternalErrorStage::{Authn, Signer, Relay}`는 **프로덕션 코드에서** 생성자가 없음(`cc-lb-domain/src/error.rs:6-20`; 테스트/fixture 생성 여부는 별개).
- `signer_failed`, `limit_rejected`, `router_pipeline_unavailable`, `route_not_configured`, `upstream_affinity_unavailable`, `tower_timeout`, `client_closed_request`, `terminal_dropped` 경로의 `internal_errors` 동작 — 소스상 유실/빈 배열로 추정되나 실행 미확인. **`drain_rejected`/`method_not_allowed`/`route_not_found`는 유실이 아니라 auth 게이트에 의한 의도적 무음**(pre-authn 종료는 행을 남기지 않음 — §10.5).
- Transport 에러의 typed downcast 가능성(DNS/TCP/TLS/timeout 구분) — `[SOURCE_HYPOTHESIS]`. `bulkhead.rs:371-376`이 hyper 에러를 `DispatchError::Transport { reason: source.to_string() }`로 문자열화해 소스 체인이 첫 경계에서 소멸.

### 2.3 DesignChallenge 소스 검증 정정 (HEAD `1561ee9f`에서 확인됨)

1. **실패 경로 타이밍은 "라우팅 안 됨"이 아니라 "측정 자체가 안 됨"**: `instrumented_connector.rs:43-51`은 `result.is_ok()` 안에서만 `record_connect`를 호출하고, `dns_cache.rs:190-205`는 `Ok` 암에서만 `record_dns`를 호출한다. 따라서 "디스패치 실패 행에 `connect_ms`가 있어야 한다"는 기준은 이벤트 배선만으로는 달성 불가 — **에러 경로에서의 측정 추가**가 필요하다.
2. **`connect_ms`는 TCP+TLS 합산**: `instrumented_connector.rs:44-48`에서 `connect_ms = total_ms - dns_delta_ms`이며 `total`은 hyper_rustls 커넥터 호출 전체(TLS 핸드셰이크 포함)를 커버한다. TCP-only 타이밍이나 TLS 별도 타이밍을 요구하는 QA 기준은 금지.
3. **0-vs-null 함정**: `read_dns_ms_snapshot()`(`instrumented_connector.rs:62-66`)은 DNS 미기록 시 `unwrap_or(0)`으로 0을 반환한다. DNS 실패 시 `dns_ms`가 unset인 채로 에러 경로 connect를 단순 기록하면 `connect_ms`에 실패한 DNS 시간이 포함된다. QA는 존재 여부가 아니라 **귀속 정확성**을 검증해야 한다.
4. **비식별화 미중앙화**: `lifecycle.rs:5857`과 `:5901`은 플러그인 `error.to_string()`을 `redact_internal_errors` 없이 `internal_errors`에 그대로 push한다. `:2747`, `:6066`만 redact한다. 생산자를 늘리는 수정은 이 기존 구멍을 증폭시키므로 모든 영속 사유에 redaction 단언이 필요하다.
5. **인증 사유의 메트릭 레이블 세분화 금지**: `authn_rail.rs:120-125`는 coarse bucket이 계정 오라클 방지를 위한 의도적 설계임을 문서화한다. 이 제한은 **Prometheus 레이블에만** 적용된다 — 영속 행의 사유는 6번 항목대로 실제 Display 리터럴을 쓴다. 레이블 작업은 #850 소관.
6. **인증 사유의 두 채널 구분**(`authn_rail.rs:120-137`): `key_auth_failure_reason`의 coarse bucket(`"Expired"`, `"Disabled"`, `"Revoked"`, `"PrincipalDisabled"`, `"Unavailable"`, `"InvalidKey"`)은 **Prometheus 메트릭 레이블 전용**의 의도적 anti-oracle 설계다. 영속 행의 `internal_errors.message`는 별개 — `BuiltinAuthError`의 Display는 전부 고정 리터럴(`builtin_authn.rs:31-52`: `"missing x-api-key header"`, `"invalid api key format"`, `"api key not found"`, `"api key signature mismatch"`, `"api key disabled"`, `"api key revoked"`, `"api key expired"`, `"principal not found"`, `"principal disabled"`, `"api key storage unavailable"`)이며 보간된 비밀값이 없고, 클라이언트는 이미 동일 문자열을 401 바디로 받는다(`authn_rail.rs:98-105`). 따라서 **영속 사유는 `error.to_string()`을 redact+truncate 경유로 기록**하고, coarse bucket은 메트릭 레이블에만 유지한다. 영속 행에 coarse bucket만 쓰면 #849가 지적한 "이유를 말하지 않는 라벨" 결함을 재생산한다(DesignChallenge round-5 정정).
7. **`emit_provider_error`는 버스 이벤트 + 옵저버 훅 이중 경로**(`terminal_observer.rs:628-649`, Main 확인): `emit_or_buffer` 후 `ObserveEvent::Error`를 모든 등록 훅에 fan-out한다. 헬퍼를 대체 없이 삭제하면 훅 알림이 회귀한다 — 삭제 범위는 **죽은 버스 variant(`ProviderErrorObserved`)뿐**이며, `ObserveEvent::Error` 알림은 18개 현 사이트(업스트림 3곳 포함) 전부에서 동일 `code`/`message`/`source`와 `AuthCompleted`/터미널 대비 상대 순서를 유지해야 한다 (`authn_rail.rs:94-96`의 순서 포함).

---

## 3. 문제 정의

`internal_errors: Vec<InternalError{stage, kind, message}>` 채널은 typed·redact·영속화되지만, 실패 경로 대부분이 사유를 `emit_provider_error(code, message, source)`라는 별도의 stringly-typed 채널로 보내고, 그 이벤트(`ProviderErrorObserved`)는 어셈블러의 빈 매치 암에서 폐기된다. `route_no_upstream_after_filter`만 두 채널을 모두 호출해 100% 영속되는 유일한 참조 경로다.

프로덕션(`runbear-local`) all-time 기준 사유 유실 32,527행(보고자 집계, 로컬 미검증): `authentication_failed` 31,931 / `upstream_dispatch_failed` 408 / `body_read_failed` 182 / `invalid_json` 5 / `signer_failed` 1.

구조적 원인 4가지 (이슈 본문 §Root cause):

1. 터미널 API에 사유 슬롯이 없음 — `set_terminal(status, &'static str)` (`terminal_observer.rs:392`), `terminate(status, Option<&'static str>)` (`:548`).
2. 에러가 첫 경계에서 `Response`로 접힘 — `attempt`가 `Result<Response<Body>, Box<Response<Body>>>` 반환 (`lifecycle.rs:3924`), 호출자는 `response.status()`만 읽을 수 있음.
3. `internal_errors` 벡터가 `attempt` 시그니처에 이미 있으나(`:3922`) 디스패치 실패 시 push하지 않고 `emit_provider_error`만 호출 (`:4020`).
4. 연결 타이밍이 `UpstreamResponseStarted`에만 존재 — 디스패치 실패 시 응답이 시작되지 않아 `dns_ms`/`connect_ms`가 구조적으로 도달 불가. **추가로 §2.3-1: 실패 경로에서는 측정 자체가 안 된다.**

---

## 4. 기각된 대안

| 대안 | 내용 | 기각 사유 |
|---|---|---|
| A1. 어셈블러에서 `ProviderErrorObserved` 소비 | 빈 암을 채워 `source` 문자열을 `InternalErrorStage`로 역매핑 | stringly-typed 역매핑 + 버스 이벤트 순서 경쟁. `RequestTerminated`가 `partial.internal_errors`를 덮어쓰면(`lifecycle_event_assembler.rs:973`) 선행 이벤트가 지워져 단독으로는 유실을 못 막음. Clean cutover 위반(두 메커니즘 잔존) |
| A2. 분리된 2단계 호출 (`record_internal_error` + `terminate`) | 옵저버에 별도 기록 메서드 제공 | optional second-call hole — `terminate`만 호출하고 기록을 빠뜨리는 실수를 구조적으로 못 막음. 현재 버그의 재발 경로를 그대로 열어둠 |
| A3. `RequestTerminated` 수신 시 무조건 덮어쓰기 | 터미널 스냅샷으로 partial 치환 | 비치명 진단 이력(필터 폴백, shape 폴백) 파괴. 단, **단일 옵저버 소유 모델에서는 터미널 이벤트가 누적 완전 이력을 운반하므로 어셈블러의 스냅샷 치환 자체는 정상** — 중복 append 금지 |
| A4. client close / drop에 synthetic 에러 주입 | `client_closed_request`에 `InternalError{stage:Relay,...}` 강제 기록 | cc-lb 내부 결함이 아닌 이벤트를 내부 결함으로 통계 왜곡 |
| A5. body read 실패를 `Authn`으로 분류 | 바디 파싱 전 단계라는 이유 | 불변식 위반 — `app.rs:2508` 인증 성공 후 `:2522`에서만 바디를 읽음. `body_read_failed`는 절대 `Authn`일 수 없음 |
| A6. transport 에러 문자열 매칭 분류 | `source.to_string().contains("refused")` 등 | 플랫폼/로케일 의존 비결정적. typed downcast/`io::ErrorKind` 필요 |
| A7. 업스트림 4xx/5xx를 `internal_errors`에 기록 | 업스트림 에러도 내부 채널에 | 카테고리 오류 — `upstream_error_*`는 Anthropic 반환 에러 전용. `source="upstream"` 생산자 3곳(`lifecycle.rs:3223,3746,4108`)은 `status.as_str()`(문자열 `"429"`)만 운반해 진단 가치 없음. 마이그레이션이 아니라 **삭제** 대상 |
| A8. 인증 실유 per-variant 메트릭 레이블 | 실패 사유를 메트릭 레이블로 | 계정 오라클 보안 회귀 (`authn_rail.rs:120-125` 문서화된 의도). #850 소관 |

---

## 5. 채택 설계 — 단일 옵저버 소유 typed 터미널 커밋

피어 합의(EventInvestigation·TransportInvestigation·ProvenanceInvestigation) + Main/DesignChallenge 정정을 반영한 채택안:

1. **`ProviderErrorObserved` 버스 variant 완전 삭제.** 18개 생산자(`lifecycle.rs` 17 + `authn_rail.rs` 1) 전부 마이그레이션, alias·deprecated 경로 없음. 단 `emit_provider_error`의 **`ObserveEvent::Error` 훅 fan-out은 동등 수단으로 보존**한다(§2.3-7) — 삭제는 죽은 버스 경로에 한정.
2. **옵저버 소유 누적 진단 + 사유 필수 터미널 메서드.** 알려진 내부 실패로 종료하는 경로는 `InternalError`를 인자로 요구하는 단일 터미널 메서드(가칭 `terminate_failure`)를 사용한다. `error_code`를 받는 제네릭 터미널 API(성공/업스트림/client-close/drop)는 내부 실패 사유를 실수로 유실할 수 없는 형태를 유지한다.
3. **append-only 진단 이력.** 비치명 진단(필터 폴백, shape 폴백)은 누적하고, 최종 치명적 에러는 덮어쓰지 않고 append한다. `RequestTerminated`가 `[prior..., terminal]` 완전 이력을 단 1회 운반. 어셈블러는 스냅샷을 그대로 반영(치환)하되 **동일 스냅샷을 두 번 append하지 않는다**.
4. **`attempt`는 응답 변환 시점까지 typed 실패를 운반.** 소스 체인과 측정된 실패 타이밍을 보존하고, `Response` 변환은 최외곽 단일 지점에서 수행 (RFC 0002 `terminate_dispatch_failed(dispatch_error)` 의도 복원).
5. **진실한 null vs 0.** 미시도 단계는 `null`, 실패한 단계는 실패 시점까지의 실측 ms. 미수행 단계를 0으로 기록 금지. `connect_ms`는 TCP+TLS 합산 의미를 유지하고, DNS 실패 시간이 connect에 섞이지 않도록 귀속을 분리한다(§2.3-3). **실패 타이밍은 기존 터미널 스냅샷(`RequestTerminated`)을 타고 간다 — 신규 `LifecycleEvent` variant를 만들지 않는다**(Main 확정).
6. **auth-first 불변식 유지.** 인증 전 바디 버퍼링 없음, 인증 실패는 즉시 종료.
7. **`upstream_error_*`는 Anthropic 전용 유지** — provider-origin 격리 완전 보존.
8. **클라이언트 응답 불변.** 모든 수정 후 클라이언트 가시 응답(상태/바디)은 베이스라인과 byte-identical해야 한다 — 이 변경이 가장 조용히 깰 수 있는 것이 응답이다.

### 5.1 제안 애플리케이션 파일/심볼 스코프 (승인 대기 — 미승인 상태)

| 파일 | 심볼 | 변경 성격 |
|---|---|---|
| `crates/cc-lb-domain/src/error.rs` | `InternalErrorStage`, `InternalErrorKind` | **`Ingress` stage와 `InvalidInput` kind 추가 확정**(Main/리뷰 합의) — body-read/parse 실패를 `Shape`와 구분. serde `snake_case` 호환 유지. `"storage"` source 귀속은 별도 결정(§11 Q1) |
| `crates/cc-lb-lifecycle/src/event.rs` | `LifecycleEvent::ProviderErrorObserved` | variant 삭제. `kind()` 매핑(`:254`), `event_id()` 매칭(`:233`) 동반 정리. **신규 variant 없음** — 실패 타이밍은 `RequestTerminated` 스냅샷 확장으로 운반 |
| `crates/cc-lb-engine/src/terminal_observer.rs` | `set_terminal`, `terminate`, `set_internal_errors`, `emit_provider_error`, `emit_terminated`, `TerminalClassification`, `error_codes` | 사유 필수 터미널 메서드 추가, `emit_provider_error`의 **버스 발행 부분** 제거(훅 fan-out은 동등 수단으로 보존), 누적 진단 API. `TerminalClassification` 사유 운반은 핸들러 직접 `terminate_failure` 호출로 대체 가능(§11 Q4 해소 방향) |
| `crates/cc-lb-engine/src/lifecycle.rs` | `emit_provider_error` 호출 17곳 전부, `attempt` 반환형, `push_shape_internal_error`, `set_internal_errors` 호출부(`:3721,:5639,:3813`), 디스패치 실패 처리(`:3101-3110,:3196-3204`) | typed 실패 운반 + 터미널 커밋 마이그레이션 |
| `crates/cc-lb-engine/src/bulkhead.rs` | `DispatchError::Transport`, `dispatch_with_client` 에러 매핑(`:371-376`) | `to_string()` 대신 typed source 보존 (tier-2 구분용) |
| `crates/cc-lb-engine/src/instrumented_connector.rs` | `call` 내부 `result.is_ok()` 게이트(`:43-51`), `read_dns_ms_snapshot` | 실패 경로 connect 측정 + DNS 귀속 분리 |
| `crates/cc-lb-engine/src/dns_cache.rs` | `MetricDnsResolver::call` 에러 암(`:201-204`) | 실패 경로 dns 측정 |
| `crates/cc-lb-engine/src/authn_rail.rs` | `reject_unauthenticated`(`:90-119`), `key_auth_failure_reason`(`:125-137`) | coarse 사유를 `InternalError`로 터미널 커밋. 기존 taxonomy 보존 |
| `crates/cc-lb-server/src/app.rs` | `lifecycle_handler` 바디 읽기 실패(`:2551-2562`), `timeout_error`(`:2477-2492`), `TerminalClassification` 소비(`:2792-2795`) | `source`를 터미널 사유로 전달 |
| `crates/cc-lb-engine/src/lifecycle_event_assembler.rs` | `ProviderErrorObserved` 빈 암(`:1212`), `RequestTerminated` 병합(`:973`), 무시 암 정책(`:1100,:1106,:1132`), `_` 와일드카드(`:1220`) | 빈 암 제거/명시적 opt-out 가드(§11 Q5) |
| `crates/cc-lb-observability/src/redaction.rs` | `redact_internal_errors` | 모든 신규 생산 경로가 경유하도록 중앙화(§2.3-4) |
| `crates/cc-lb-engine/src/usage_parser.rs` | `canonical_upstream_error_from_value`(`:414-423`), `bounded_upstream_error`(`:485-499`), `BoundedErrorType::parse`(`:76-79`) | 2차 결함 공유 파서: `error.code`를 `error.type`의 폴백으로 수용, outer `type=="error"` 래퍼 요구 완화(단 **에러 상태 게이트 필수** — §11 Q6), `error_type` 부재 시 message까지 버리는 `error_type?` 조기 탈락 재검토 |
| `crates/cc-lb-engine/src/lifecycle.rs` (스트리밍/버퍼 경로) | `RequestLogUpstreamErrorObserved` 생산자(`:3726,:3734,:5644,:5670`) | 2차 결함: 스트리밍 경로 `:5644`의 `error_type: String::new()` 고정 + 버퍼 경로의 엄격 엔벌로프 — **두 경로가 동일 공유 파서를 쓰도록** 수정(§11 Q6) |

### 5.2 같은 커밋에서 이동/재작성이 필요한 기존 테스트 (DesignChallenge 지적, 소스 확인됨)

- `crates/cc-lb-lifecycle/src/tests.rs:282` — `kind()` 라벨 목록에 `"provider_error_observed"` 포함. variant 삭제 시 목록 갱신. (TerminalFoundation 소유)
- `crates/cc-lb-server/tests/rfc_0002_fix_live_qa.rs` `lqa_2b` — `provider_error_observed` 카운터 0 단언: variant 삭제 후 해당 시리즈가 스크레이프에 나타나지 않으므로 0 단언은 무의미 — **제거됨**(본 문서 소유자가 반영).
- `crates/cc-lb-server/tests/rfc_0002_fix_live_qa.rs` `lqa_6c` — 업스트림 429 시나리오의 `provider_error_observed` 카운터 1 단언을 **영속된 `upstream_error_type`/`upstream_error_message` + `internal_errors` 빈 배열 단언으로 재작성 완료**(`lqa_6c_upstream_rate_limit_error_records_typed_upstream_error`). 다른 카운터로 재지정하지 않음.

---

## 6. 진단 분류 규칙 (상호 배타 + 이력 예외)

### 6.1 상호 배타

- **cc-lb 내부 실패**: `error_code` + `internal_errors`에 기록, `upstream_error_type`/`upstream_error_message`는 `null`.
- **업스트림 반환 실패(4xx/5xx, 스트림 에러)**: `upstream_error_*`에만 기록, `internal_errors`는 `[]`.
- **클라이언트/드롭 계열**(`client_closed_request`, `terminal_dropped`): `internal_errors = []` — 인위적 사유 생성 금지.

`internal_errors`의 정의는 **"cc-lb가 생성한 진단"이지 "cc-lb의 귀책"이 아니다**(DesignChallenge round-3). 클라이언트 기인 거부(무효 키, malformed JSON, cap 초과)도 정당하게 여기에 기록된다. 반대로 client-close/drop은 원인이 불가지이므로 아무것도 만들지 않는다.

### 6.2 이력 예외 (prior-history exception)

`internal_errors`는 단일 항목이 아니라 누적 이력이다. 비치명 진단(라우터 필터 trap/invalid-output 폴백, shape 실패 후 raw passthrough 폴백)이 먼저 쌓이고, 이후 치명적 종료 사유가 append된다. 따라서:

- 한 행의 `internal_errors`에 복수 항목이 있을 수 있으며, 마지막 항목이 터미널 사유다.
- 업스트림 실패로 종료되더라도 **선행 비치명 내부 진단이 있었다면 `internal_errors`는 비어있지 않을 수 있다** — 이것은 §6.1의 예외이며 허용된다. 단 그 항목들은 모두 내부 stage여야 하고, 업스트림 사유 자체가 `internal_errors`에 들어가는 것은 여전히 금지다.
- 터미널 스냅샷은 누적 완전 이력을 운반하므로 어셈블러는 치환하되 동일 내용을 중복 append하지 않는다.

### 6.3 stage/kind 매핑 (안)

| error_code | stage | kind(안) | 비고 |
|---|---|---|---|
| `authentication_failed` | `authn` | `invalid_input` — 단 `BuiltinAuthError::Unavailable`(cc-lb 자체 스토어 다운)은 `unavailable` | message는 **authenticator의 실제 사유** = `error.to_string()`(고정 리터럴, §2.3-6). coarse bucket은 메트릭 레이블 전용 |
| `principal_missing` | `authn` | `unavailable` | |
| `invalid_json` | `ingress` | `invalid_input` | `body_read_failed`와는 `error_code`+message로 구분 |
| `body_too_large` | `ingress` | `invalid_input` | `ParseCompleted::BodyTooLarge`와 일치 |
| `body_read_failed` | `ingress` | `invalid_input` | **Authn 절대 금지** (인증 후에만 바디 읽음). `Ingress` 신설로 `Shape` 모호성 해소(Main/리뷰 합의 — DesignChallenge round-2 지적 해소됨) |
| `limit_rejected` | `router` | `unavailable` | `LimitDecision::Rejected.reason`의 기존 typed 사유를 진단으로 영속(quota 동작 자체는 불변 — §11 Q7 해결) |
| `route_no_upstream_after_filter` | `router_filter` | `unavailable` | 기존 참조 경로 유지 |
| `router_pipeline_unavailable` | `router` | `config_error` | |
| `route_not_configured` | `router` | `config_error`/`unavailable` | |
| `signer_failed` | `signer` | `plugin_error`/`unavailable` | `signer_factory`/`signer` 두 source 모두 |
| `upstream_dispatch_failed` | `relay` | `unavailable`/`timeout` | tier-1: `DispatchError` 문자열 사유. tier-2: typed source 보존으로 DNS/TCP/TLS/timeout 구분(§11 Q9 해결 — in scope) |
| `upstream_affinity_unavailable` | `storage`(신설 확정) | `unavailable` | `"storage"` source 3곳(`:2659,:3747-3754,:5656-5660`) — Q1 해결 |
| `tower_timeout` | `relay` | `timeout` | `Elapsed` 사유 — **post-authn 경로만**(§11 Q8 해결). pre-authn 타임아웃은 행 0 |
| `upstream_4xx`/`upstream_5xx`/`upstream_stream_error`/`upstream_refusal`/`upstream_context_window_exceeded` | — | — | `internal_errors`에 기록하지 않음(선행 비치명 진단 예외 제외) |
| `client_closed_request`/`terminal_dropped`/`route_not_found`/`method_not_allowed`/`drain_rejected` | — | — | `internal_errors` 부재 또는 `[]` 유지(둘은 동등 — §6.1). 단 405/drain은 pre-authn이라 **행 자체가 없음**(§10.5) |

---

## 7. 타이밍 진실성 규칙

| 상황 | `dns_ms` | `connect_ms` | `upstream_ttfb_ms` |
|---|---|---|---|
| DNS 실패 | 실패까지의 실측 ms (측정 추가 필요 — 현재 Err 암에서 미기록) | `null` (TCP 미시도) | `null` |
| TCP 거절 | DNS 성공 실측 ms | 실패까지의 실측 ms (TCP+TLS 구간 의미) | `null` |
| TLS 실패 | DNS 성공 실측 ms | 실패까지의 실측 ms (TCP+TLS 합산 구간 — 분리 금지) | `null` |
| connect timeout | 실측 또는 `null`(미도달) | 실측 timeout까지 ms | `null` |
| 커넥션 재사용/디스패치 전 실패 | `null` | `null` | `null` |
| 미시도 단계 | `null` | `null` | `null` |

- `0`은 "측정값이 실제로 0ms"일 때만 허용. 미기록을 0으로 표현 금지 (`read_dns_ms_snapshot`의 `unwrap_or(0)` 함정 주의 — 현재 0이 "미기록" 센티넬 역할).
- `connect_ms`는 TCP+TLS 합산 의미를 유지한다. TLS 별도 분리 기준 금지.
- 실패 경로 타이밍은 **측정 추가 + `RequestTerminated` 스냅샷 운반**이 필요하다(신규 이벤트 variant 없음 — Main 확정).

---

## 8. 비식별화(redaction) 정책

- `internal_errors`에 영속되는 모든 `message`는 `cc_lb_observability::redact_internal_errors` + `truncate_reason`을 경유해야 한다.
- 현재 `lifecycle.rs:5857`, `:5901`은 raw `error.to_string()`을 미redact push한다 — 수정 시 이 경로들도 중앙 redact를 경유하게 한다.
- 인증 사유는 동적 헤더/토큰 원문 금지, 기존 coarse 라벨만 허용(§6.3).
- 디스패치 사유 문자열에 URL 쿼리·자격증명이 섞일 수 있으므로 동일하게 redact한다.
- QA-SEC-01에서 `Authorization`/API 키 패턴이 포함된 에러가 `[REDACTED]`로 치환되는지 단언한다.

---

## 9. 무시되는 이벤트의 정책 (assembler empty arms)

`merge()`의 빈 암 현황과 채택 정책:

| 이벤트 | 현재 | 정책 |
|---|---|---|
| `ParseCompleted{result:Err}` (`:1100`) | 무시 | 실패 페이로드(`BodyTooLarge{limit_bytes}`, `InvalidJson`)는 터미널 사유로 이미 커밋되므로 **명시적 무시 유지** — 단 opt-out 주석/가드 대상 |
| `AuthCompleted{result:Err}` (`:1106`) | 무시 | `AuthFailure`는 터미널 사유로 커밋되므로 명시적 무시 유지 |
| `LimitDecision{Rejected}` (`:1132`) | 무시 | `reason`/`limit_violation`을 `internal_errors` 사유로 활용할지는 터미널 커밋 측에서 결정; 어셈블러 무시는 명시적 opt-out |
| `ProviderErrorObserved` (`:1212`) | 무시 | **variant 자체 삭제** — 소비 매핑이 아니라 제거가 채택안 |
| `RequestTerminated` (`:1213`) | 무시 | `handle_event` 선행 분기(`:902-1011`)가 처리하므로 정상 no-op |
| `_ => {}` (`:1220`) | 와일드카드 | 신규 variant 추가 시 무음 무시 위험 — 가드 정책(§11 Q5)과 연동 |

**no-op 가드 (정확한 정의)**: `LifecycleEvent` variant가 `merge`에서 빈 암으로 무시될 때 **명시적 opt-out 목록**(variant별 사유 주석 + 집계 테스트/CI lint가 목록과 대조)에 등록돼 있어야 통과하는 구조적 장치. 1차 보증은 `_` 와일드카드 암 제거로 컴파일러 exhaustiveness를 강제하는 것 + opt-out 집계 테스트이며, **CI/lint 정적 가드**(빈 암 검출)를 같은 변경에 포함한다. 텍스트 패턴 매칭만으로는 부족 — 소스 문자열을 핀하는 테스트가 아니라 **영속 출력을 단언하는 행동 테스트**와 병행한다(Main 지시). 이 가드가 #849 실제 메커니즘의 회귀 테스트다.

---

## 10. QA 케이스 매트릭스

상태 값: `NOT_RUN` (미실행) / `BASELINE_CONFIRMED` (Main이 현재 결함 동작을 실행 확인 — 수정 후 기대값과 다름) / `PASS` / `FAIL` / `BLOCKED`.

하니스 매핑(구체 경로):
- `unit` = `cargo test -p cc-lb-engine --lib lifecycle_event_assembler::tests` / `terminal_observer::tests` (인메모리 `mpsc` + `CapturingStore`, DB·네트워크 불필요)
- `int` = `cargo test -p cc-lb-server --test integration rfc_0002_fix_live_qa::` / `proxy_error_fallbacks::` (`autotests=false` — 타깃 이름은 `integration`, 모듈 필터 필요) 및 `cargo test -p tests-integration --test integration terminal_observation::` (`tests/integration/terminal_observation.rs`, `spawn_test_server` 계열은 `crates/cc-lb-server/tests/common.rs`)
- `live` = `/data/tmp/cc-lb-849-repro-ano_sbq8/` 스크래치 서버 방식(`config.toml` + SQLite 시드, proxy 51885/admin 51886/metrics 51887) — Main 베이스라인과 동일 절차
- 공통 빌드 환경(Nix 링커 우회): `RUSTFLAGS='-C linker=/usr/bin/clang -C link-arg=-fuse-ld=/usr/bin/ld' CC=/usr/bin/clang CXX=/usr/bin/clang++ SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk CC_LB_ADMIN_SKIP_SPA=1 SQLX_OFFLINE=true CC_LB_SKIP_WASM_FIXTURE_BUILD=1`

### 10.1 인증/파싱/바디

| ID | 설정 | 행동 | 기대 출력(SQL 페이로드) | 상태 전이 | 하니스 | 상태 |
|---|---|---|---|---|---|---|
| QA-AUTH-01 | 유효하지 않은 형식의 API 키 | POST /v1/messages | 401, `error_code=authentication_failed`, `internal_errors=[{stage:authn, kind:invalid_input, message:"invalid api key format"}]`(authenticator의 실제 사유 — 베이스라인 응답 바디와 동일 문자열), `upstream_error_*=null`, **클라이언트 응답 바디/상태는 베이스라인과 byte-identical** | 없음→terminated(authn 실패) | live/int | **PASS** — live `fixed-auth-parse.json`; `rfc_0002_fix_live_qa::lqa_5b` + `terminal_observation::terminal_authentication_failed` (실행 PASS) |
| QA-AUTH-02 | 만료/비활성/폐기 키 | POST /v1/messages | 401/403, `authentication_failed`, `stage=authn`, `kind=invalid_input`(`Unavailable`은 `unavailable`), message = 실제 Display 리터럴(`"api key expired"`/`"api key disabled"`). **폐기 키는 조회 SQL(`status != 'revoked'`)이 걸러 `"api key revoked"`가 아니라 `"api key not found"`로 귀결 — AUTH-01 경로와 동일** | 동일 | int | **PASS** — live `fixed-auth-states.json` (expired/disabled/revoked 실제 SQL 행); `terminal_observation::terminal_expired_and_disabled_keys_record_authn_reason` (실행 PASS) |
| QA-AUTH-03 | 키는 유효하나 `authenticate_first` 시점에 principal 부재(`BuiltinAuthError::PrincipalMissing`) | POST /v1/messages | **401**, `authentication_failed`, `stage=authn`, `kind=invalid_input`, message=`"principal not found"` | 동일 | int | **PASS** — `terminal_observation::terminal_key_for_missing_principal_records_authn_reason` (실행 PASS) |
| QA-AUTH-03b | 인증 성공 **후** 조회 전에 principal 소실/불일치 | POST /v1/messages | `stage=authn`, `kind=unavailable` 계열 사유 영속 | authn 성공→post-auth principal 실패→terminated | unit | **PASS** — `lifecycle::tests::post_auth_principal_missing_records_authn_unavailable` (engine-lib 실행 PASS) |
| QA-AUTH-04 | 무효 키 + 큰 바디 헤더 | 헤더만 전송, 바디 미전송 | 401이 바디 읽기 전에 반환, 행 1개, `request_body_read_ms` 없거나 null | 인증 선행 불변식 | live | **PASS** — live `fixed-auth-fallback-controls.json`; `terminal_observation::oversized_body_with_bad_credential_is_rejected_without_reading_body` (실행 PASS) |
| QA-AUTH-05 | 인증 스토어 다운(`BuiltinAuthError::Unavailable`) | POST /v1/messages | **503**, `authentication_failed`, `stage=authn`, `kind=unavailable`(invalid_input 아님 — cc-lb 자체 스토어 장애), message=`"api key storage unavailable"` | terminated | int | **PASS** — `proxy_error_fallbacks::key_store_unavailable_persists_typed_authn_reason` (실행 PASS) |
| QA-PARSE-01 | 유효 키, cap 초과 바디 | POST /v1/messages | 413, `body_too_large`, `internal_errors`에 `stage=ingress` 계열 + `limit_bytes` 정보 | authn→parse 실패→terminated | int | **PASS** — live `fixed-timeout-cap.json`; `terminal_observation::body_too_large_after_auth_persists_one_row` (실행 PASS) |
| QA-PARSE-02 | 유효 키, malformed JSON | POST /v1/messages | 400, `invalid_json`, `internal_errors`에 `stage=ingress, kind=invalid_input` 사유, **응답 바디 byte-identical** | 동일 | live/int | **PASS** — live `fixed-auth-parse.json`; `rfc_0002_fix_live_qa::lqa_6f` (실행 PASS) |
| QA-BODY-01 | 유효 키, `Content-Length`보다 짧게 쓰고 `SHUT_WR` | POST /v1/messages | 400, `body_read_failed`, `internal_errors`에 `stage=ingress, kind=invalid_input` + read 원인, **stage≠authn**, **응답 `body read failed: {source}` 형태 유지** | authn→body read 실패→terminated | live | **PASS** — live `fixed-body.json`; `rfc_0002_fix_live_qa::lqa_6g` (실행 PASS — Content-Length 200 < fixture cap 256) |

### 10.2 라우팅/한도/서명

| ID | 설정 | 행동 | 기대 출력 | 상태 전이 | 하니스 | 상태 |
|---|---|---|---|---|---|---|
| QA-ROUTE-01 | 유효 키, **설정된 eligible upstream 0개**(Main 베이스라인과 동일 조건 — `allowed_upstreams=[]`는 "무제한"이지 후보 0이 아님) | POST /v1/messages | 503, `route_no_upstream_after_filter`, `internal_errors=[{router_filter,unavailable,...}]` 100% 유지 | 정상 참조 경로 회귀 방지 | live/int | **PASS** — live `fixed-route-tcp-dns.json`; `terminal_observation::terminal_route_no_upstream_after_filter` (실행 PASS) |
| QA-ROUTE-02 | 라우터 파이프라인 인스턴스화 실패 주입 | POST /v1/messages | 502, `router_pipeline_unavailable`, `stage=router` | terminated | int | **PASS** — `lifecycle::tests::router_pipeline_instantiation_error_records_router_config` (engine-lib 실행 PASS — DI 주입으로 도달 불가 live 모드 커버) |
| QA-ROUTE-03 | 라우트 미구성 upstream 선택 | POST /v1/messages | 502, `route_not_configured`, `stage=router` | terminated | int | **N/A — 2026-09-22 사용자 승인**. 아래 도달 불가 근거에 따라 실행 대상에서 제외하여 종결했다. 실행 PASS 아님. |
| QA-ROUTE-04 | 필터 플러그인 trap/invalid-output 폴백 + 이후 치명적 실패 | POST /v1/messages | `internal_errors`에 선행 비치명 진단 + 터미널 사유 **둘 다** 존재(append 순서) | 누적 이력 보존 | int | **PASS** — `lifecycle::tests::filter_invalid_output_history_precedes_terminal_dispatch_cause` (engine-lib 실행 PASS — 비치명 filter 진단이 터미널 사유 앞에 보존) |
| QA-LIMIT-01 | quota/rate limit 초과 | POST /v1/messages | 429, `limit_rejected`, `internal_errors`에 `stage=router,kind=unavailable` + `LimitDecision::Rejected`의 기존 typed reason 영속(quota 동작 불변) | terminated | int | **PASS** — `terminal_observation::terminal_limit_rejected` (실행 PASS) |
| QA-SIGN-01 | `signer_factory.build` 실패(자격증명 해석 불가) | POST /v1/messages | 502, `signer_failed`, `stage=signer`, redact된 사유 | terminated | int | **PASS** — `lifecycle::tests::signer_factory_failure_records_signer_stage` (engine-lib 실행 PASS) |
| QA-SIGN-02 | `attempt` 내 `sign` 실패 | POST /v1/messages | 502, `signer_failed`, `stage=signer` | terminated | int | **PASS** — `lifecycle::tests::attempt_sign_failure_records_signer_stage` (engine-lib 실행 PASS) |
| QA-STOR-01 | upstream affinity 읽기/바인드 실패(`"storage"` source) | POST /v1/messages | 503, `upstream_affinity_unavailable`, `stage=storage`(신설 확정 — Q1 해결) | terminated | int | **PASS** — `lifecycle::tests::affinity_store_failure_records_storage_stage` (engine-lib 실행 PASS) |

### 10.3 디스패치/릴레이

| ID | 설정 | 행동 | 기대 출력 | 상태 전이 | 하니스 | 상태 |
|---|---|---|---|---|---|---|
| QA-RELAY-01 | 업스트림이 닫힌 포트(**숫자 IP는 DNS를 우회**하므로 `dns_ms=null`이 정답) | POST /v1/messages | 502, `upstream_dispatch_failed`, `internal_errors`에 `stage=relay` + 사유(tier-1). tier-2: `connection_refused` 구분, `dns_ms=null`(IP 리터럴), `connect_ms` 실측(거절까지, TCP+TLS 구간) | terminated | live/int | **PASS** — live `fixed-route-tcp-dns.json`; `terminal_observation::terminal_upstream_dispatch_failed` (실행 PASS) |
| QA-RELAY-02 | `.invalid` 등 미해석 호스트 | POST /v1/messages | 502, `upstream_dispatch_failed`, `stage=relay`, tier-2: DNS 실패 구분, `dns_ms` 실측(실패까지), `connect_ms=null`(TCP 미시도) | terminated | live/int | **PASS** — live `fixed-route-tcp-dns.json`; `terminal_observation::terminal_upstream_dispatch_failed_dns` (실행 PASS) |
| QA-RELAY-03 | HTTPS→평문 리스너 | POST /v1/messages | 502, `upstream_dispatch_failed`, `stage=relay`, tier-2: TLS 실패 구분, `connect_ms` 실측(TLS 포함 구간) | terminated | live/int | **PASS** — live `fixed-tls-upstream.json`; `terminal_observation::terminal_upstream_dispatch_failed_tls` (실행 PASS) |
| QA-RELAY-04 | 연결 타임아웃(비응답 주소) | POST /v1/messages | 502, `upstream_dispatch_failed`, `stage=relay`, `kind=timeout` 또는 구분 가능 사유 | terminated | int | **PASS** — `lifecycle::tests::transport_timeout_records_relay_timeout` (engine-lib 실행 PASS — 결정적 `DispatchError::Transport(TimedOut)` DI 주입으로 typed timeout 분류 검증; 실제 소켓 데드라인 주장 아님) |
| QA-RELAY-05 | bulkhead 큐 포화 | POST /v1/messages | 503, `overloaded_error` 응답, `internal_errors`에 `stage=relay` + `BulkheadFull` 사유 | terminated | int | **PASS** — `terminal_observation::terminal_bulkhead_full` (실행 PASS) |
| QA-RELAY-06 | invalid URI / request build 실패 | POST /v1/messages | 502, `upstream_dispatch_failed`, `stage=relay`, `InvalidUri`/`RequestBuild` 구분 사유 | terminated | int | **PASS** — `lifecycle::tests::invalid_uri_and_request_build_record_relay_config_error` (engine-lib 실행 PASS) |
| QA-RETRY-01 | 첫 시도 401 → signer refresh → 재시도 | POST /v1/messages | 재시도 후 최종 행에 두 시도의 진단이 누적/정리 규칙대로 반영, `UpstreamAttempt{attempt_num:2}` 후 타이밍 리셋 정합 | attempt1 실패 진단→attempt2→terminated | int | **PASS** — `lifecycle::tests::retry_after_401_keeps_provider_error_out_of_internal_errors` (engine-lib 실행 PASS — 재시도 후 provider error가 internal_errors를 오염하지 않음) |

### 10.4 업스트림 반환 에러 (내부 채널 오염 금지)

| ID | 설정 | 행동 | 기대 출력 | 상태 전이 | 하니스 | 상태 |
|---|---|---|---|---|---|---|
| QA-UPSTR-01 | 업스트림 HTTP 429, 비표준 에러 바디(`{"error":{"code":...}}`, outer `type` 없음) | POST /v1/messages | 429 통과, `error_code=upstream_4xx`, `internal_errors` 부재/`[]`. **수정 후 계약**: 파서가 `error.code`를 폴백으로 추출하면 `upstream_error_type="rate_limit_exceeded"` + `upstream_error_message`=추출된 message(`"isolated upstream rejection"`); 파싱 불가 바디는 기존 raw-body 폴백 유지(§11 Q6) | terminated | live | **PASS** — live `fixed-tls-upstream.json`; `terminal_observation::terminal_upstream_4xx_error_code_fallback` (실행 PASS) |
| QA-UPSTR-02 | 업스트림 4xx, canonical 바디(`{"type":"error","error":{"type":...}}`) — **스트리밍 경로** | POST /v1/messages(stream) | `upstream_error_type` 파싱값 기록 — 스트리밍 경로 `:5644`의 `error_type:""` 고정 결함 수정 검증 | terminated | int | **PASS** — live `fixed-tls-upstream.json`; `terminal_observation::terminal_upstream_4xx_streaming_request` (실행 PASS) |
| QA-UPSTR-02b | 업스트림 4xx, `{"error":{"code":...}}` 바디 — **버퍼(비스트리밍) 경로** | POST /v1/messages | 공유 파서가 `error.code`를 폴백으로 읽어 `upstream_error_type` 기록 | terminated | int | **PASS** — live `fixed-tls-upstream.json`; `terminal_observation::terminal_upstream_401_error_code_parsed_and_body_unchanged` (실행 PASS — no-refresh signer 401 경로, 응답 바디 byte-identical 단언 포함) |
| QA-UPSTR-03 | 업스트림 5xx | POST /v1/messages | `error_code=upstream_5xx`, `upstream_error_*` 기록, `internal_errors=[]` | terminated | int | **PASS** — live `fixed-provider-controls.json`; `terminal_observation::terminal_upstream_5xx` (실행 PASS) |
| QA-UPSTR-04 | SSE 스트림 도중 업스트림 에러 | POST /v1/messages(stream) | `error_code=upstream_stream_error`, `upstream_error_*` 기록, `internal_errors=[]` | terminated | int | **PASS** — live `fixed-sse-abnormal.json`; `terminal_observation::terminal_upstream_stream_error` (실행 PASS) |
| QA-UPSTR-05 | abnormal stop(refusal/context window) | POST /v1/messages | `upstream_refusal`/`upstream_context_window_exceeded`, `upstream_error_*`에 stop_reason | terminated | int | **PASS** — live `fixed-sse-abnormal.json`; `terminal_observation::terminal_upstream_refusal` (실행 PASS) |
| QA-UPSTR-06 | **음성 케이스**: 200 성공 응답 바디에 `"error"` 키 포함 | POST /v1/messages | `upstream_error_*` 전부 null — 엔벌로프 게이트 완화가 정상 응답을 오탐하지 않음을 단언(에러 상태 게이트 필수) | 정상 종료 | int | **PASS** — live `fixed-provider-controls.json`; `terminal_observation::terminal_success_body_with_error_key_not_misclassified` (실행 PASS) |

### 10.5 통제 케이스 — 비내부 실패·경계·내구성

| ID | 설정 | 행동 | 기대 출력 | 상태 전이 | 하니스 | 상태 |
|---|---|---|---|---|---|---|
| QA-CTRL-01 | 미등록 경로 | GET /issue849-unregistered (**`/v1/*`는 보호 catchall이라 무키 401 — 라우터 폴백이 아님**) | 404, **행 0** (pre-authn 종료는 무음) | 무음 | live | **PASS** — live `fixed-auth-fallback-controls.json`; `terminal_observation::route_not_found_before_auth_persists_no_row` (실행 PASS) |
| QA-CTRL-02 | 등록 경로에 허용되지 않은 메서드 | DELETE /v1/messages | 405 `method_not_allowed`, **행 0 / `RequestTerminated` 없음**(pre-authn 종료는 무음 — AGENTS 불변식) | 무음 | int | **PASS** — live `fixed-auth-fallback-controls.json`; `terminal_observation::method_not_allowed_before_auth_persists_no_row` (실행 PASS) |
| QA-CTRL-03 | drain 중 요청 | shutdown 중 POST | 503 `drain_rejected`, **행 0 / `RequestTerminated` 없음**(pre-authn 종료는 무음) | 무음 | int | **PASS** — `terminal_observation::drain_rejected_before_auth_persists_no_row` (실행 PASS) |
| QA-CTRL-04 | tower timeout 초과(**post-authn**: 핸들러 진입 후 지연) | 지연 업스트림 | 504 `tower_timeout`, `internal_errors`에 `stage=relay,kind=timeout` 계열(Q8 해결) | terminated | int | **PASS** — live `fixed-timeout-cap.json`; `terminal_observation::terminal_tower_timeout` (실행 PASS) |
| QA-CTRL-05 | 클라이언트가 스트림 도중 연결 종료 | POST 후 disconnect | 499 `client_closed_request`, **선행 비치명 진단 이력은 보존**, 인위적 최종 사유만 생성 금지 | terminated | int | **PASS** — `terminal_observation::terminal_client_closed_mid_stream` + `lifecycle_client_disconnect` 24개 (실행 PASS) |
| QA-CTRL-06 | 옵저버 미종료 drop | finalize 없이 drop | `terminal_dropped`, **선행 진단 이력은 보존**, 인위적 최종 사유만 생성 금지 | drop backstop | unit | **PASS** — `lifecycle_client_disconnect::generic_observer_drop_remains_terminal_dropped` (실행 PASS) |
| QA-CTRL-07 | buffered(비SSE) 정상 응답 | POST /v1/messages | 200, `error_code=null`, `internal_errors` 부재 또는 `[]` (비치명 진단 없을 때) | 정상 종료 | int | **PASS** — live `fixed-provider-controls.json`; `terminal_observation::terminal_success_non_stream` (실행 PASS) |
| QA-CTRL-08 | SSE 정상 스트림 | POST /v1/messages(stream) | 200, `internal_errors` 부재 또는 `[]` | 정상 종료 | int | **PASS** — live `fixed-sse-abnormal.json`; `terminal_observation::terminal_success_stream` (실행 PASS) |
| QA-CTRL-09 | shape 실패→raw passthrough 폴백→정상 완료 | dialect shape 실패 주입 | 최종 행 `internal_errors`에 비치명 `shape` 진단 **보존**(성공 종료여도 이력 유지) | 진단 누적→정상 종료 | int | **PASS** — `lifecycle::tests::shape_fallback_success_keeps_shape_diagnostic` (engine-lib 실행 PASS — 비치명 shape 진단이 성공 종료 행에 보존됨을 직접 단언) |
| QA-CTRL-10 | 터미널 이벤트 후 지연/중복 이벤트 | `finish()` 이후 추가 이벤트 | `finish` 멱등 — 두 번째 `RequestTerminated` 없음, 행 1개 | 중복 방지 | unit | **PASS** — `lifecycle_event_assembler::tests::duplicate_terminal_missing_timings_preserves_first_terminal_timings` + `terminal_observer::tests::record_internal_error_after_finalize_is_ignored` (engine-lib 실행 PASS) |
| QA-CTRL-11 | `RequestStarted` 없이 `RequestTerminated`만 도달 | orphan terminal | `error_code=terminal_without_partial` orphan 행 정책 유지 | orphan 처리 | unit | **PASS** — `lifecycle_event_assembler::tests::terminated_without_partial_writes_orphan_row` + `request_terminated_before_started_writes_orphan_and_increments_metric` + `orphan_terminated_carries_connection_timings_and_internal_errors` (engine-lib 실행 PASS) |
| QA-CTRL-12 | pre-authn 종료(버퍼된 이벤트) | authn 도달 전 종료 | `RequestTerminated` 미발행, 버퍼 폐기 — 행 0 | 무음 | unit | **PASS** — `terminal_observer::tests::pre_authn_termination_publishes_no_events` + `lifecycle::tests::pre_authn_termination_publishes_nothing` (engine-lib 실행 PASS) |
| QA-CTRL-13 | `ObserveEvent::Error` 훅 fan-out 보존 | 실패 경로 실행 | 삭제 후에도 훅이 동일 `code`/`message`/`source` 알림을 `AuthCompleted`/터미널 대비 동일 상대 순서로 수신(18개 사이트 전부: `lifecycle.rs` 17 + `authn_rail.rs` 1) | 훅 회귀 방지 | unit | **PASS** — `terminal_observer::tests::notify_error_hooks_fans_out_without_bus_event` + `lifecycle::tests::error_hooks_preserve_payload_and_order_on_dispatch_failure` + `authn_rail::tests::rejection_reports_authentication_error_and_terminal_to_global_hooks` (engine-lib 실행 PASS — 훅 fan-out·payload·상대 순서를 DI로 검증); 18개 호출 사이트 전수는 SecurityReview의 정적 리뷰로 확인(단일 fan-out unit이 18개 전부를 커버한다는 주장 아님) |
| QA-CTRL-14 | **pre-handler 타임아웃**(authn 도달 전 tower timeout) | 타임아웃이 인증 전에 발화 | **행 0 / `RequestTerminated` 없음** — pre-authn 무음 불변식 | 무음 | int | **PASS** — `app::tests::pre_authn_timeout_emits_no_lifecycle_events` (실행 PASS, `/tmp/cc-lb-849-preauth-timeout-fixed.log` — 실제 middleware timeout이 authn/body-poll 전에 발화해 lifecycle 이벤트 0건) + `terminal_observer::tests::pre_authn_termination_publishes_no_events` + `lifecycle::tests::pre_authn_termination_publishes_nothing` (engine-lib 실행 PASS) |
| QA-CTRL-15 | 업스트림 에러 바디가 malformed/비JSON | POST /v1/messages | `upstream_error_type=null`, `upstream_error_message`=raw 바디 폴백 **유지**(파서 실패 시 기존 동작 보존) | terminated | int | **PASS** — live `fixed-provider-controls.json`; `terminal_observation::terminal_upstream_5xx_malformed_body` (실행 PASS) |
| QA-CTRL-16 | 풀 재사용 연결로 정상 디스패치 | 동일 업스트림 연속 요청 | 성공 행에서 `connect_ms=null`(재사용 시 connect 미수행), `connection_reused=true` | 정상 종료 | int | **PASS** — live `fixed-provider-controls.json`; `terminal_observation::terminal_connection_reused` (실행 PASS) |
| QA-SEC-01 | 에러 메시지에 `Authorization`/API 키 패턴 포함 | 실패 주입 | `internal_errors[].message`가 `[REDACTED]` 치환, 원문 미영속 | redaction | unit/int | **PASS** — `terminal_observer::tests::internal_error_messages_are_redacted_centrally` + `stream_error_classifier_extracts_io_reset_and_redacts_bounded_chain` (engine-lib 실행 PASS — 중앙 redact + bounded chain) |
| QA-COMPAT-01 | (a) 구버전 `internal_errors` JSON이 포함된 기존 행 → 신규 리더; (b) 신규 `ingress`/`storage`/`invalid_input` 값이 포함된 행 → **현재** API 리더 | 역직렬화 양방향 | (a) 기존 행 읽기 성공, `snake_case` 직렬화 유지. (b) **구형 바이너리가 신규 enum variant를 읽으면 실패할 수 있음 — forward/rollback 호환을 약속하지 않음**. deprecated alias나 unknown 무음 폴백 없음. **DOWNGRADE는 사용자가 범위에서 명시 제외** | 호환성 | unit | **PASS** — live `fixed-admin-compat.json`; `domain_compatibility::routing_trace_and_internal_error_match_t0_fixture_bytes_when_reserialized` (T0 fixture byte-exact), `request_event_backward_compat::request_event_deserializes_old_payload_without_routing_trace_and_internal_errors` + `request_event_mixed_old_and_new_fields` + `request_event_with_new_fields_round_trips`, `types::tests::request_event_*_old_*_yields_none`, `cc-lb-lifecycle::request_terminated_preserves_nested_io_timings_and_defaults_legacy_payloads` (전부 실행 PASS); downgrade는 사용자 제외로 범위 밖 |
| QA-API-01 | admin API로 request_events 조회(기존 admin 인증 필요) | GET admin events | `internal_errors`가 API 응답에 전파. 프론트엔드 미러는 **검색한 심볼 범위에서 발견되지 않음**(Main 검색: cc-lb-admin 및 `web/src/lib/api.ts`/`logRows.ts`에 `InternalErrorStage`/`internal_errors` 미검출 — 절대적 부재 주장 아님). UI에 internal_errors 렌더링은 현재 없으며 **신규 구현 대상 아님** — 기존 로그 표시 회귀만 검증(SPA 미변경, `CC_LB_ADMIN_SKIP_SPA=1` 빌드) | API 전파 | int | **PASS** — live `fixed-admin-compat.json`; `terminal_observation::admin_event_detail_propagates_internal_errors` (실행 PASS) |
| QA-GUARD-01 | `merge()`의 `_` 암/빈 암 정책 | 신규 variant 추가 시뮬레이션 | opt-out 미등록 variant가 무시되면 테스트/CI 실패 | 가드 동작 | unit | **PASS** — `lifecycle_event_policy::policy_rejects_wildcards_and_new_or_handled_noops` + `lifecycle_event_assembler_matches_are_explicit_and_documented` (실행 PASS) |

케이스 수: 51 (10.1: 9 — AUTH 6, PARSE 2, BODY 1 / 10.2: 8 — ROUTE 4, LIMIT 1, SIGN 2, STOR 1 / 10.3: 7 — RELAY 6, RETRY 1 / 10.4: 7 — UPSTR 7 / 10.5: 20 — CTRL 16, SEC 1, COMPAT 1, API 1, GUARD 1). 최종 상태: **PASS 50 / N/A 1**. QA-ROUTE-03은 도달 불가 근거를 재검토한 뒤 사용자가 N/A 종결을 명시 승인했다. 51 PASS 주장 아님.

#### QA-ROUTE-03 도달 불가 근거

- `Lifecycle::handle`은 하나의 불변 dynamic-view 스냅샷을 유지한다. `build_candidates_with_matches`는 그 스냅샷의 upstream 레코드에서만 후보를 생성한다.
- `validate_filter_output`은 입력 후보에 없는 upstream ID를 거부한다. `keep_filter_candidates`와 `terminal_candidates`는 기존 후보를 선택할 뿐 새 ID를 만들지 않는다. 따라서 선택된 후보의 ID가 동일 스냅샷에서 사라지는 lookup 실패 조건은 성립하지 않는다.
- `upstream_for_record`는 현재 `AnthropicApiKey`와 `AnthropicOauth` 두 종류를 exhaustive match하며 모두 `Ok`를 반환한다. 변환 실패 분기도 발생하지 않는다.
- 독립 재검토와 Main의 현재 소스 확인으로 위 조건을 확인했다. 제품 불변식을 깨는 테스트 전용 public hook이나 런타임 변경은 추가하지 않았다. 향후 후보 생성·검증 또는 upstream 종류가 바뀌어 분기가 도달 가능해지면 이 QA 항목을 다시 활성화해야 한다.

---

## 11. 미해결 설계 질문 (구현 전 결정 필요)

| ID | 질문 | 쟁점 |
|---|---|---|
| Q1 | ~~`"storage"` source 3곳(`lifecycle.rs:2659,3747-3754,5656-5660`)에 `InternalErrorStage::Storage`를 신설하는가~~ | **해결**: `Storage` stage 신설 확정(Main 확인, 이슈 본문 "coverage for storage" 요구와 일치). 역직렬화 호환 정책은 QA-COMPAT-01 참조 — 무음 폴백 없음, 구형 리더가 신규 variant를 읽으면 실패 가능(롤아웃 주의). enum 추가는 사용자 승인 대상 |
| Q2 | ~~`body_read_failed`/`invalid_json`의 stage를 `Shape`로 재사용하는가, `Ingress`/`Parse` 계열을 신설하는가~~ | **해결**: `InternalErrorStage::Ingress` + `InternalErrorKind::InvalidInput` 추가 확정(Main/리뷰 합의). `Shape`는 진짜 shaping trap 전용으로 유지. 런타임 응답 불변. **enum 추가는 애플리케이션 코드이므로 사용자 명시 승인 필요** |
| Q3 | ~~디스패치 실패 타이밍의 운반 수단~~ | **해결**: 신규 `LifecycleEvent` variant 없이 기존 `RequestTerminated` 터미널 스냅샷 확장으로 운반(Main 확정). **실패 경로 측정 자체**(§2.3-1)가 선행 조건 |
| Q4 | ~~`TerminalClassification`에 사유 슬롯을 추가하는가~~ | **해결 방향**: 미들웨어 마커 경유 대신 핸들러/생산 지점이 `terminate_failure`를 직접 호출하거나 마커에 `InternalError`를 운반 — 구현 시 최종 확정. `TOWER_TIMEOUT`/`BODY_READ_FAILED` 경로가 대상 |
| Q5 | 빈 암 가드의 구체 형태 | **해결 방향**: `_` 와일드카드 제거로 컴파일러 exhaustiveness 강제 + variant별 명시적 opt-out 목록(사유 주석) + **CI 정적 AST 가드**(빈 암/no-op 검출, 같은 변경에 포함) + 영속 출력을 단언하는 행동 테스트 병행. 소스 문자열을 핀하는 테스트는 주 보증이 아님. `ParseCompleted`/`AuthCompleted`/`LimitDecision`의 명시적 무시 사유 문서화 포함 |
| Q6 | ~~2차 결함(`upstream_error_type` 미파싱)의 정확한 경로~~ | **해결**(DesignChallenge round-4, 소스 검증): 두 독립 게이트 — (a) `canonical_upstream_error_from_value`(`usage_parser.rs:414-417`)가 outer `type=="error"` 요구, (b) `bounded_upstream_error`(`:485-499`)가 `error.type`/`error.message`만 읽고 `error.code` 미독, `error_type?` 조기 탈락이 message까지 폐기. 수정 범위 3부: 공유 엔벌로프 파싱(`error.code` 폴백 + outer 래퍼 완화, **에러 상태 게이트 필수**), 스트리밍 raw 폴백(`:5644-5657`), 두 경로 동일 파서 사용. `BoundedErrorType::parse`(`:76-79`)는 길이 캡만 있어 `error.code` 값 수용 가능. **프로덕션 14k/15k 비율은 어느 경로 기여인지 측정된 바 없음 — 비율 주장 금지** |
| Q7 | ~~`limit_rejected`의 사유로 `LimitDecision::Rejected{reason, limit_violation}`를 쓰는가~~ | **해결**: 기존 typed `reason`을 터미널 커밋 시점에 `InternalError{stage:router, kind:unavailable}`로 영속. quota 동작 자체는 불변(Main 확정) |
| Q8 | ~~`tower_timeout`을 `internal_errors`에 기록하는가~~ | **해결**: post-authn `tower_timeout`은 `relay/timeout`으로 기록(Main 확정). pre-authn 타임아웃은 auth 게이트에 의해 행 0(QA-CTRL-14) |
| Q9 | ~~`DispatchError` typed 보존의 범위~~ | **해결**: tier-2 typed source 보존이 in scope(Main 확정). `bulkhead.rs:373`의 `reason: String`을 typed source로 교체해 `io::ErrorKind`/rustls/hyper 구분을 유지. 구체 다운캐스트 매핑은 구현 시 프로브로 확정 |

---

## 12. 피어 합의 기록 (실제 수신 내역)

| 피어 | 수신 내용 | 상태 |
|---|---|---|
| EventInvestigation | 보고서 수신(`agent://EventInvestigation/report`): 단일 터미널 `terminate_error`, append-only 이력, body-read=Shape(Authn 배제), coarse auth 사유 사용, null-vs-0, drop/client-close 분 배제, 훅 fan-out 보존, transport 다운캐스트 추가 조사 | **수신 + 최종 문서 Zero disagreement 확인** — PascalCase taxonomy 채택 + `Ingress` + `InvalidInput` 신설로 auth 경계 명확화. 단, 보고서의 "coarse auth 사유" 제안은 **최종 리터럴 영속 결정(§2.3-6)으로 폐기됨** |
| TransportInvestigation | 합의 보고서 수신(`agent://TransportInvestigation/report`): `terminate_failure` 단일 커밋, `append_diagnostic`, body-read를 `Ingress` 신설 또는 `Router` 전단계로 분류 제안, `[SOURCE_HYPOTHESIS]` 라벨 체계 | **수신 + 최종 문서 전면 동의 확인** — `Ingress` 제안 최종 채택, 훅 fan-out 보존 및 `RequestTerminated` 스냅샷 타이밍 운반 합의 재확인 |
| ProvenanceInvestigation | 보고서 수신(`agent://ProvenanceInvestigation/report`): SHA/릴리스 provenance, Nix 링커 우회 환경변수, CapturingStore 재현 전략, RFC-0002 불변식, redaction 누락 방지, 어셈블러 덮어쓰기 결함 지적(`:973`), serde 호환 | **수신 + 최종 문서 No disagreement 확인** — `:973` 덮어쓰기는 단일 소유자 모델의 authoritative commit으로 재해석 합의. 스카우트의 7월 날짜는 Main의 git 검증(PR #388 2026-07-11 도입, PR #401 2026-07-12 이동)으로 정정 수용 |
| DesignChallenge | 정정 4라운드 수신(IRC): 타이밍 미측정, connect=TCP+TLS, 0-vs-null 함정, redaction 미중앙화(`:5857,:5901`), auth 세분화 금지, Shape 모호성(→Ingress로 해소), `"upstream"` 생산자 삭제, 동일 커밋 테스트 이동 3곳, tier-1/tier-2 분리, `internal_errors`=진단이지 귀책 아님, 훅 fan-out 보존, 2차 결함 양 경로+공유 파서+에러 상태 게이트 | **전부 소스 확인 후 반영** — §2.3, §5.2, §6.1, §6.3, §7, §9, §10, §11. 최종 라운드에서 **full ACK** 수신 |
| Main | 실행 베이스라인 7범주(auth×2, invalid_json, body_read, 대조군, TCP/DNS/TLS, upstream 429) + 정리 증거 수신; auth taxonomy 정정; `:973` 치환 정상 확인; 커넥터 타이밍 의미 정정; no-op 도입 provenance 독립 검증; `emit_provider_error` 훅 fan-out 지적; Ingress/InvalidInput/Storage 확정; 역사적 손실 비복구 명시; **frozen-rubric judge 51/51 분류(33 in_scope/16 other/2 mixed, 51 전부 risk-flagged — 분류이지 진실이 아님) 후 Main이 플래그 전량 판독, 16개 "other"는 직접 이슈 커버리지/회귀 통제로 in-scope 재분류**; 확인된 문서 오류 전량 수정 지시(pre-auth 405/drain 행 0, `allowed_upstreams=[]`=무제한, 숫자 IP DNS 우회, coarse auth 사유 폐기, drop/client-close 이력 보존, Storage/tier-2 확정, raw upstream 메시지 계약, bulk/source-only 상태≠PASS) | **반영됨** |

**미합의/이견**: 없음 — Q1(Storage stage), Q7(limit reason), Q8(tower_timeout), Q9(tier-2 typed source)는 Main 확정으로 해소. body-read stage 이견(Shape vs Ingress)은 **Ingress 채택으로 해소**. DesignChallenge round-5의 영속 auth 사유 정정(실제 Display 리터럴 영속, coarse bucket은 메트릭 전용)도 Main 수용 확인. 남은 열린 항목은 구현 시점 결정 사항(Q4 마커 운반 방식, Q5 가드 구체 형태)과 사용자 승인 대상(enum 추가, 롤아웃/다운그레이드 주의)뿐이다.

---

## 13. 회귀 위험 및 주의

- `route_no_upstream_after_filter`(8,263행 정상 경로)가 수정으로 깨지면 안 됨 — QA-ROUTE-01이 회귀 방지.
- `UpstreamAttempt{attempt_num>1}` 시 어셈블러가 타이밍을 리셋(`:1133-1143`)하므로, 재시도 QA(QA-RETRY-01)는 첫 시도 진단의 보존/정리 규칙을 명시해야 함.
- `pending_events` 버퍼(pre-authn)는 `mark_authn_reached` 또는 `emit_terminated`에서만 드레인 — 터미널 메서드 변경 시 이 순서 보장 유지.
- `carries_request_row`(`event_bus.rs:347-355`)는 `RequestTerminated`/`RequestLogUpstreamErrorObserved`/`StreamCompleted`를 우선 채널로 라우팅 — `RequestTerminated` 필드 확장 시 이 분류 영향 확인.
- 프로덕션 기존 행의 `internal_errors`는 `null` 또는 `[]` — 신규 stage/kind 추가가 기존 행 역직렬화를 깨지 않는지 QA-COMPAT-01로 검증.
- `emit_provider_error` 삭제 시 `ObserveEvent::Error` 훅 알림이 사라지는 회귀 — QA-CTRL-13이 방지.
- 역사적 손실(32,527행)은 복구 불가 — 백필을 만들지 않으며, 수정은 미래 실패의 기록만 보장한다.
