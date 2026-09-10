# Request latency unaccounted 근본 개선 설계

- 상태: 구현 완료, isolated proxy/storage/API/metrics/browser QA 완료 (외부 OTLP collector export 제외)
- 작성일: 2026-09-10
- 대상: proxy request lifecycle, request event/storage/admin API, Logs latency UI, Prometheus/OTLP
- 관련 문서: `docs/request-setup-timing-breakdown.md`
- 실행 QA: `.opencode/skills/user-flow-qa/references/scenarios/request-log-observability.md`의 Cases F–K
- 완료 검증일: 2026-09-10

## 1. 목적과 비목표

이 문서는 운영 request timeline의 `Unaccounted`가 실제 지연 원인을 숨기는 문제를 설명하고, 구현자가 그대로 적용할 계측·저장·API·UI·QA 계약을 정한다.

이번 변경의 목표는 다음과 같다.

1. client request body 수신 시간을 독립 stage로 기록한다.
2. 정상 proxy 요청에서 `duration_ms`와 stage 합의 잔차를 **10ms 이하**로 줄인다.
3. client가 stream을 중단한 499 요청에서도 중단 시점까지의 partial stream 시간을 보존한다.
4. `source_kind="renewal"`을 proxy timeline과 분리해 전체 renewal duration이 `Unaccounted`로 보이지 않게 한다.
5. 기존 row, 혼합 버전 배포, SQLite/PostgreSQL, recent/delta/detail API의 호환성을 유지한다.

이 문서는 성능 최적화 자체나 이미 구현된 setup 8개 stage의 재분해를 제안하지 않는다. 운영에서 확인한 약 0.24~1.23초의 지연은 대부분 client body upload/read 시간이며, 현재 문제는 그 시간을 기록하지 않는 계측 공백이다. Request/response bytes, routing, signing, plugin 실행, retry, timeout, status/body/header 동작은 바꾸지 않는다.

## 2. 결론

`LifecycleContext`의 전체 `duration_ms` clock은 `crates/cc-lb-server/src/app.rs`의 `lifecycle_middleware`에서 시작한다. 그 뒤 `lifecycle_handler`가 `read_request_body`를 await하고, body를 모두 모은 다음에야 `Lifecycle::handle`을 호출한다.

반면 `proxy_setup_ms`의 clock은 `Lifecycle::handle` 진입 후 시작한다. 따라서 다음 구간은 `duration_ms`에는 포함되지만 어떤 request event stage에도 포함되지 않는다.

```text
LifecycleContext::new
  → middleware/handler 진입
  → read_request_body await
  → Lifecycle::handle 진입
```

대형 정상 요청의 header 수신 이후 upstream dispatch 전 시간을 request ID로 대조하면, `read_request_body`가 포함된 이 공백이 기록된 `Unaccounted`의 **99.75~99.98%**를 설명한다. 정상 stream 자체는 `stream_total_ms == upstream_body_ms`로 기록되므로 완료된 stream relay가 누락된 것이 아니다.

별도 문제도 두 가지 확인했다.

- 499 client cancellation은 `DownstreamStreamDropGuard::drop`에서 status/error만 기록하고 relay 시작 이후 경과 시간을 terminal timing으로 저장하지 않는다.
- renewal publisher는 `duration_ms`만 기록하고 proxy/setup/body timing을 모두 `None`으로 발행한다. Proxy 공식을 적용하면 renewal 전체가 `Unaccounted`가 된다.

## 3. 운영 조사 방법

### 3.1 데이터 대조 절차

조사는 다음 순서로 수행했다.

1. 운영 request event에서 `source_kind`, status, `duration_ms`, `proxy_setup_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`, `upstream_body_ms`, `stream_total_ms`, `limit_reconcile_ms`를 request ID별로 조회했다.
2. 정상 proxy, 499, renewal을 분리했다. 서로 다른 source/termination 모델을 한 분포에 섞지 않았다.
3. 현재 admin-web과 같은 방식으로 stage 합과 `Unaccounted`를 계산했다.
4. 같은 request ID의 lifecycle 시작 timestamp, `proxy.read_request_body`, `Lifecycle::handle`, signed-request dispatch timestamp를 trace/log에서 대조했다.
5. header 이후 dispatch 전 wall time에서 이미 기록된 `proxy_setup_ms`, `shape_ms` 등 겹치는 stage를 제외해 계측 공백을 구했다.
6. 원본 request body 크기와 공백의 방향성을 비교했다. 대표 대형 요청은 ingress request와 outbound request bytes도 대조해 body 변형으로 생긴 차이가 아님을 확인했다.
7. Thanos pod CPU/memory/throttling, connection reuse, DNS/connect, affinity resolve, request/response plugin timing을 같은 사건 창에서 확인했다.

현재 UI의 proxy stage 합은 세부 표시는 나누더라도 실질적으로 다음 parent duration을 한 번씩 더한다.

```text
current_accounted =
    proxy_setup_ms
  + shape_ms
  + sign_ms
  + upstream_ttfb_ms
  + upstream_body_ms
  + limit_reconcile_ms

current_unaccounted = max(0, duration_ms - current_accounted)
```

`bulkhead_wait_ms`, `dns_ms`, `connect_ms`, auth/route/limit/setup 세부 stage는 각각 `upstream_ttfb_ms` 또는 `proxy_setup_ms`의 child breakdown이다. Parent와 child를 동시에 더하지 않는다.

### 3.2 정상 proxy 분포

운영 정상 요청의 `Unaccounted` 분포는 집계 창과 event/trace 계산 방식의 경계 차이에 따라 다음 범위였다.

| 분위수 | `Unaccounted` |
| --- | ---: |
| p50 | 약 243~257ms |
| p90 | 약 624~630ms |
| p99 | 약 1.03~1.08s |
| max | 1.225s |

이는 rounding 수준의 잔차가 아니다. 중앙값부터 수백 ms이고 tail은 1초를 넘는다.

### 3.3 대표 request ID 대조

| Request ID | Lifecycle start | Dispatch | Header→dispatch | 기록된 선행 stage 제외 후 공백 | 기록된 `Unaccounted` | 설명 비율 | 원본 body |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `req_server_711` | 07:29:56.849Z | 07:29:57.999188Z | 1,150.188ms | 1,003.188ms (`proxy_setup_ms=138`, `shape_ms=9` 제외) | 1,005ms | 99.820% | 854,336B |
| `req_server_1112` | 07:54:24.595Z | 07:54:25.720519Z | 1,125.519ms | 993.519ms (`proxy_setup_ms=124`, `shape_ms=8` 제외) | 996ms | 99.751% | 917,567B |
| `req_server_860` | 같은 방식으로 대조 | 같은 방식으로 대조 | - | 1,156.763ms | 1,157ms | 99.980% | 대형 request |

`req_server_711`과 `req_server_1112`는 ingress request body와 outbound body가 각각 854,336B, 917,567B로 일치했다. 대형 body일수록 client→server upload/read 구간이 길어졌고, 그 구간이 기존 stage에서 빠져 있었다. 구현 후 `request_body_bytes`를 저장해 이 상관관계를 전체 분포에서 직접 검증할 수 있어야 한다.

완료된 정상 stream의 반례도 확인했다.

- `req_server_960`: `stream_total_ms=22,873`, `upstream_body_ms=22,873`

즉 정상 stream relay는 terminal event에 들어온다. UI는 두 필드를 한 stage로 한 번만 계산해야 하며, 현재 큰 `Unaccounted`의 원인을 stream relay 누락으로 보아서는 안 된다.

## 4. 배제한 원인

다음 항목은 별도 성능 문제의 가능성까지 부정하는 것이 아니라, 이번 수백 ms~1.2s `Unaccounted`의 주원인이 아님을 뜻한다.

### 4.1 CPU와 memory pressure

- Thanos에서 cc-lb pod CPU 최대는 약 0.081 core였고 limit은 2 core였다.
- CPU throttling은 0%였다.
- Memory 최대는 약 163MB였고 limit은 1GiB였다.
- 따라서 CPU saturation, throttling, memory limit 접근이 대표 요청의 약 1초 공백을 설명하지 않는다.
- 더 직접적으로, 공백은 `Lifecycle::handle` 진입 전에 존재하고 body bytes와 함께 증가했다.

### 4.2 DNS와 connect

- 대표 요청은 connection reuse 경로였다.
- DNS/connect는 signed request를 upstream으로 dispatch한 뒤의 `upstream_ttfb_ms` child stage다.
- 조사한 공백은 header 수신 후 **dispatch 전**이므로 실행 순서상 DNS/connect가 원인이 될 수 없다.

### 4.3 Routing/affinity

- Affinity resolve 누적은 44회에 0.102초, 요청당 약 2.3ms였다.
- Routing과 affinity는 `proxy_setup_ms` 안에 포함된다.
- 대표 요청의 `proxy_setup_ms` 자체가 124~138ms인데, 별도 공백은 약 994~1,157ms였다. 이미 parent에 포함된 routing 비용으로 이 공백을 다시 설명할 수 없다.

### 4.4 Plugin과 prompt-cache 분석

- Response plugin `transform_response`는 약 0.35ms였다.
- Request shape/sign은 각각 `shape_ms`, `sign_ms`에 기록된다.
- Prompt-cache parse/queue/structure/serialize/token-key/count/tokenize와 signer 준비는 `proxy_setup_ms` 안의 기존 세부 stage다.
- 대표 요청의 body-read 공백은 이 stage들이 시작되기 전에 발생했다.

따라서 plugin이나 tokenizer 시간을 다시 더하는 방식은 중복 계상이며 근본 해결이 아니다.

## 5. 현재 코드 계측 경계

### 5.1 전체 duration

`crates/cc-lb-server/src/app.rs::lifecycle_middleware`가 `LifecycleContext::new`를 호출한다. `crates/cc-lb-engine/src/terminal_observer.rs`는 이때 `Instant::now()`를 저장하고 `RequestTerminated` 발행 시 `started.elapsed()`를 `duration_ms`로 기록한다.

### 5.2 누락된 request body read

`crates/cc-lb-server/src/app.rs::lifecycle_handler`는 다음 순서로 실행된다.

1. request parts/body 분리
2. `read_request_body` await
3. `Request<Bytes>` 재구성
4. `Lifecycle::handle` 호출

`proxy.read_request_body` tracing span은 이미 있으나 request event field로 전달되지 않는다. Body size도 `ParseInfo.body_bytes`에는 존재하지만 최종 `RequestEvent`의 request-body 전용 필드로 보존되지 않는다. 기존 `RequestEvent.body_bytes`는 response/stream body bytes이므로 재사용하면 안 된다.

### 5.3 `proxy_setup_ms`

`crates/cc-lb-engine/src/lifecycle.rs::Lifecycle::handle`의 local `started`는 handle 진입에서 시작하고, 첫 upstream attempt 직전 `dispatch_started`까지를 `proxy_setup_ms`로 기록한다. 따라서 server body read는 구조적으로 포함되지 않는다.

### 5.4 기존 setup 세부 stage

기존 구현과 `docs/request-setup-timing-breakdown.md`의 계약은 유지한다.

| Field | 현재 경계 |
| --- | --- |
| `json_parse_ms` | `RequestBodyView::new` 내부 `sonic_rs::from_slice` |
| `cache_tokenizer_queue_ms` | prompt-cache executor semaphore 대기 |
| `cache_structure_ms` | block flatten, breakpoint, structural prefix hash/lookback |
| `cache_serialize_ms` | exact token-prefix bytes와 breakpoint offset materialization |
| `cache_token_key_ms` | credential-scoped exact token-prefix BLAKE3 key |
| `cache_count_lookup_ms` | distributed count-cache claim/hit/miss/coalesced wait/retrieval |
| `cache_tokenize_ms` | leader/fallback exact BPE tokenization |
| `prepare_signer_ms` | signer factory build, lookup/decrypt/lazy OAuth refresh |

이 여덟 필드는 `proxy_setup_ms`의 child다. 신규 `request_body_read_ms`는 `proxy_setup_ms` 밖의 선행 sibling이며 `json_parse_ms`와 겹치지 않는다.

### 5.5 정상 response body

- Non-stream path는 response headers 이후 body collection을 `upstream_body_ms`로 저장한다.
- Stream path는 `relay_start`부터 완료까지 `stream_total_ms`를 계산하고 terminal timing의 `upstream_body_ms`에도 같은 값을 넣는다.
- 따라서 정상 stream에서는 두 값이 같을 수 있다. Timeline 합에는 한 번만 포함한다.

### 5.6 499 Drop 경로

`DownstreamStreamDropGuard::drop`은 완료되지 않은 stream이 drop되면 status 499와 `client_closed_request`를 설정하고 termination outcome metric을 기록한다. 그러나 `relay_start`를 소유하지 않고 `set_termination_timings`를 호출하지 않으므로 취소 전까지 이미 흘러간 stream 시간이 보존되지 않는다. 이후 terminal `duration_ms`에는 그 시간이 포함되어 전체 partial relay가 `Unaccounted`로 남는다.

### 5.7 Renewal 경로

`crates/cc-lb-server/src/scheduler_dispatch/cache_keepalive/lifecycle.rs::publish_renewal_lifecycle`는 `source_kind="renewal"`과 `duration_ms`를 발행하지만 다음 필드를 모두 `None`으로 둔다.

- `bulkhead_wait_ms`, `dns_ms`, `connect_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`
- `stream_total_ms`, `upstream_body_ms`, `first_body_chunk_ms`
- `proxy_setup_ms`, setup 세부 stage, `limit_reconcile_ms`

Renewal은 client ingress/body read와 일반 proxy pipeline을 거치지 않는 synthetic source다. Proxy stage 공식을 억지로 채우지 말고 source-kind별 모델로 분리한다.

## 6. 목표 timeline과 불변식

### 6.1 정상 proxy 목표 timeline

```text
Lifecycle duration
├─ Request body read       request_body_read_ms
├─ Proxy setup             proxy_setup_ms
│  ├─ JSON parse           json_parse_ms
│  ├─ Cache queue/analysis cache_*_ms
│  ├─ Auth                 auth_ms
│  ├─ Route                route_ms
│  ├─ Limit reserve        limit_reserve_ms
│  ├─ Prepare signer       prepare_signer_ms
│  └─ Other setup          derived residual inside proxy_setup_ms
├─ Shape                   shape_ms
├─ Sign                    sign_ms
├─ Upstream to headers     upstream_ttfb_ms
│  ├─ Bulkhead wait        bulkhead_wait_ms
│  ├─ DNS                  dns_ms
│  ├─ Connect              connect_ms
│  └─ Provider wait        derived residual inside upstream_ttfb_ms
├─ Response body           one of stream_total_ms / upstream_body_ms
└─ Finalize                finalize_ms
   ├─ Limit reconcile      limit_reconcile_ms
   └─ Other finalize       derived residual inside finalize_ms
```

Parent와 child는 상세 표시에는 함께 나타나도 합산에는 한 번만 포함한다.

정상 proxy의 새 계산식은 다음과 같다.

```text
body_phase_ms =
  if stream_total_ms is present: stream_total_ms
  else: upstream_body_ms ?? 0

accounted_ms =
    request_body_read_ms
  + proxy_setup_ms
  + shape_ms
  + sign_ms
  + upstream_ttfb_ms
  + body_phase_ms
  + finalize_ms

unaccounted_ms = max(0, duration_ms - accounted_ms)
```

신규 계측이 존재하는 정상 proxy final row의 acceptance budget은 다음과 같다.

```text
unaccounted_ms <= 10ms
```

각 `u64` millisecond field의 rounding은 개별 stage마다 바닥/반올림 오차를 만들 수 있다. 10ms budget은 그 오차와 middleware scheduling gap을 흡수하기 위한 것이며, 새로운 수십~수백 ms residual을 허용하는 면책 기준이 아니다.

### 6.2 499 목표 timeline

```text
Lifecycle duration
├─ Request body read       request_body_read_ms
├─ Proxy setup/dispatch    existing proxy stages
├─ Upstream to headers     upstream_ttfb_ms
├─ Partial response body   upstream_body_ms
└─ Finalize/drop           finalize_ms
```

499에서는 기존 `upstream_body_ms`에 `relay_start`부터 downstream cancellation을 guard가 관측한 monotonic instant까지의 partial elapsed를 보존한다. 완료되지 않은 stream에 `stream_total_ms`를 허위로 기록하지 않는다. UI는 complete stream이면 `stream_total_ms`, 그 외에는 `upstream_body_ms`를 body stage로 한 번만 사용한다.

### 6.3 Renewal 목표 timeline

`source_kind="renewal"`에는 proxy 공식을 적용하지 않는다. 이번 변경에서는 이미 존재하는 `duration_ms`를 단일 `Renewal cycle` stage로 표시한다.

```text
Renewal cycle = duration_ms
Renewal unaccounted = 0
```

세부 renewal dispatch/connect/body stage를 새로 추측하거나 proxy field에 합성하지 않는다. 추후 renewal dispatcher가 실제 monotonic boundary를 제공할 때 별도 source-specific stage를 추가할 수 있으나 이번 remediation의 범위가 아니다.

## 7. 신규 필드 계약

아래 내용은 모든 구현 구간이 함께 준수해야 하는 **확정 계약**이다.

| Field | Rust/JSON type | Source | 정확한 의미 | 합산 규칙 |
| --- | --- | --- | --- | --- |
| `request_body_read_ms` | `Option<u64>` / nullable optional number | server `lifecycle_handler` | `read_request_body` 호출 직전부터 성공 또는 read/limit error 반환까지의 monotonic elapsed | `proxy_setup_ms` 앞의 독립 parent stage |
| `request_body_bytes` | `Option<u64>` / nullable optional number | server body collector | 성공적으로 수집해 `Lifecycle::handle`에 전달한 원본 ingress body byte 수 | 시간 합에는 사용하지 않음 |
| `finalize_ms` | `Option<u64>` / nullable optional number | engine terminal path | response body 완료 또는 cancellation 관측 이후부터 `RequestTerminated` 발행 직전까지의 mandatory finalization elapsed | `limit_reconcile_ms`를 포함하는 parent; child와 중복 합산 금지 |

추가 규칙:

- 모든 timing은 monotonic clock으로 측정하고 음수/NaN/Infinity를 만들지 않는다.
- `0`은 측정된 값이다. Missing/null과 구분해 transport/storage/UI에서 보존한다.
- `request_body_bytes`는 `Content-Length` header를 신뢰해 복사하지 않는다. 성공 row는 실제로 수집한 `Bytes::len()`을 기록한다.
- `request_body_bytes`는 기존 response `body_bytes`와 별도다.
- Body-too-large가 `Content-Length`만으로 즉시 거절된 경우 실제 수신 body 크기를 모르는 상태이므로 `request_body_bytes=None`을 허용한다. Stream read 중 cap을 넘긴 경우 구현이 실제 관측 bytes를 안전하게 반환할 수 있을 때만 그 값을 기록한다.
- `finalize_ms`는 `limit_reconcile_ms`를 포함한다. UI는 `Other finalize = max(0, finalize_ms - limit_reconcile_ms)`로 상세 표시한다.
- 정상 완료 stream은 기존처럼 `stream_total_ms`를 보존한다. 499 client cancellation은 `stream_total_ms=None`, `upstream_body_ms=Some(partial_elapsed)`를 원칙으로 한다.

## 8. Lifecycle/event 변경 계약

### 8.1 Server ingress

`crates/cc-lb-server/src/app.rs`에서 다음을 구현한다.

1. `read_request_body` 직전에 monotonic start를 잡는다.
2. 성공 시 elapsed와 실제 `body.len()`을 `LifecycleContext`에 즉시 저장한다.
3. Too-large/read error에서도 elapsed를 먼저 저장한다.
4. Context가 있는 error path는 명시적 terminal status/error와 `finish()`를 사용해 Drop fallback에 의존하지 않는다.
5. 기존 `proxy.read_request_body` span에 `request_body_read_ms`와 성공 시 `http.request.body.size`를 기록한다.
6. Body bytes를 복제하거나 한 번 더 serialize/parse하지 않는다.

### 8.2 Terminal state와 lifecycle event

`crates/cc-lb-engine/src/terminal_observer.rs`와 `crates/cc-lb-lifecycle/src/event.rs`에서 다음을 구현한다.

- `TerminalState`와 `LifecycleEvent::RequestTerminated`에 세 optional field를 추가한다.
- Request body timing은 `Lifecycle::handle` 호출 전 setter로 저장할 수 있어야 한다.
- Non-stream은 upstream body frame loop가 끝난 직후, stream은 마지막 downstream body frame을 확정한 직후 monotonic finalize start를 잡는다.
- Limit/accounting reconcile과 terminal status/error 확정을 끝낸 뒤 `finish()`를 호출하기 직전에 `finalize_ms`를 저장한다. 따라서 `limit_reconcile_ms`는 `finalize_ms` parent 안에 포함되며 합산에는 parent만 사용한다.
- Drop, timeout, early rejection, normal stream/non-stream 모두 이미 완료된 stage를 덮어쓰지 않는다. Setter 재호출이 필요한 경로는 `Some`을 `None`으로 되돌리지 않는다.

### 8.3 499 Drop

`DownstreamStreamDropGuard`는 `relay_start` 또는 동등한 start instant를 소유한다.

1. 정상 `finish()`에서는 기존 complete timing을 유지한다.
2. 미완료 Drop에서 cancellation instant를 한 번 잡는다.
3. `relay_start` 기준 partial elapsed를 기존 `upstream_body_ms`의 missing slot에 먼저 저장한다. 기존 값은 덮어쓰지 않는다.
4. 그 뒤 499와 `client_closed_request`를 설정한다.
5. 기존 stream termination span/metric을 기록한다.
6. cancellation 관측 이후 경과 시간을 `finalize_ms`로 저장하고 `observer.finish()`로 terminal event를 명시적으로 닫는다.
7. 완료되지 않은 stream에는 `stream_total_ms`를 설정하지 않는다.
8. `cc_lb_stream_terminations_total{outcome="client_cancelled"}`의 기존 의미를 유지한다.
9. Cancellation 뒤 upstream/body를 추가로 drain해 elapsed를 부풀리지 않는다.

### 8.4 Assembler

`crates/cc-lb-engine/src/lifecycle_event_assembler.rs`에서 세 신규 필드를 `Partial`에 저장하고 다음 출력에 동일하게 복사한다. 499 partial timing은 기존 `upstream_body_ms` 경로로 보존한다.

- terminal live partial
- final `RequestEvent`
- orphan/finalization fallback 중 이미 받은 timing

`RequestTerminated`가 `StreamCompleted`보다 먼저/나중에 도착하는 event ordering에서도 값이 사라지지 않아야 한다. 499의 partial `upstream_body_ms`를 이후 missing 값으로 덮어쓰지 않는다.

### 8.5 Renewal publisher

Renewal lifecycle payload에 proxy timing을 가짜로 채우지 않는다. `source_kind="renewal"`과 `duration_ms`가 source-specific UI 선택의 기준이다. 세 신규 필드는 renewal row에서 기본적으로 `None`이다.

## 9. RequestEvent, storage, API 변경 계약

### 9.1 Rust DTO

다음 구조에 동일한 optional field를 추가한다.

- `cc_lb_request_log::RequestEvent`
- assembler live `RequestEventPartial` 계열
- `cc_lb_storage_api::RequestEventListItem`
- recent/delta/final/detail response에 쓰이는 admin DTO

Serde 계약은 `#[serde(default, skip_serializing_if = "Option::is_none")]`이다. Detail payload, recent list, final update, terminal live partial이 같은 값을 반환해야 한다.

### 9.2 SQLite

SQLite list hot path는 raw JSON parse 없이 materialized `list_*` column을 읽는다. `0080_request_event_lifecycle_timing_list_fields.sql`에 다음 nullable column을 추가한다.

```text
list_request_body_read_ms INTEGER NULL
list_request_body_bytes   INTEGER NULL
list_finalize_ms           INTEGER NULL
```

Append INSERT/bind, `request_event_list_sql`, `ListRow`, `list_row_to_item`, conformance fixture를 함께 갱신한다. 기존 row backfill을 위해 전체 table을 scan하지 않는다.

### 9.3 PostgreSQL

PostgreSQL은 bounded page의 payload를 `ListPayload`로 한 번 decode한다. `ListPayload`와 `RequestEventListItem` mapping에 세 신규 필드를 추가한다. 기존 JSONB payload 저장 방식과 detail byte contract를 유지하며, 이 remediation만을 위해 별도 materialized column/index를 추가하지 않는다.

### 9.4 Admin API와 web schema

`crates/cc-lb-admin/web/src/lib/api.ts`에서 다음을 모두 갱신한다.

- `RequestEventPartialSchema`
- final event loose schema/transform
- `RequestEvent` TypeScript interface
- fixture와 schema tests

Timing/bytes field는 finite, non-negative, nullable, optional number로 검증한다. Missing, null, 0을 서로 구분한다. API field 이름은 Rust/JSON과 정확히 일치해야 하며 alias를 만들지 않는다.

## 10. UI 변경 계약

### 10.1 Source-kind 분기

`source_kind`를 먼저 확인한다.

- `proxy` 또는 missing legacy proxy: proxy timeline 사용
- `renewal`: `Renewal cycle = duration_ms` 단일 source-specific timeline 사용

Renewal row에는 `Unaccounted 100%`를 표시하지 않는다. Proxy 전용 `Internal pre`, DNS/connect, stream relay 설명도 renewal에 표시하지 않는다.

### 10.2 Proxy stage 계산

`computeStageGroups.ts`와 `LatencyTimeline.tsx`에서 다음 규칙을 적용한다.

1. `Request body read`를 `internal_pre`의 첫 stage로 추가한다.
2. `request_body_bytes`는 stage detail에 사람이 읽을 수 있는 bytes로 함께 표시한다.
3. `proxy_setup_ms` child stage와 parent 합산 규칙은 기존대로 유지한다.
4. Complete stream은 `stream_total_ms`를 body parent로 사용하고 같은 값의 `upstream_body_ms`를 다시 더하지 않는다.
5. 499 incomplete stream은 partial `upstream_body_ms`를 `Partial stream (client cancelled)` stage로 표시한다.
6. `Finalize`는 `finalize_ms` parent를 합에 사용한다. Detail은 `limit_reconcile_ms`와 derived `Other finalize`로 나눌 수 있지만 둘을 parent와 중복 합산하지 않는다.
7. 신규 계측이 모두 있는 정상 proxy에서 residual을 `Unaccounted`로 표시하고 10ms budget을 넘으면 시각적으로 식별 가능하게 한다.
8. 음수 residual은 `max(0, ...)`로 clamp하되, over-accounting을 테스트에서 숨기지 않는다. 개발 계산 helper는 raw signed residual 또는 `accounted_ms`를 함께 노출해 검증할 수 있어야 한다.

### 10.3 Legacy fallback

세 신규 필드가 없는 기존 proxy row는 현재 timeline 계산을 유지한다. 하나 이상의 신규 timing이 있는 mixed row는 present field만 합산하며 missing을 0으로 본다.

기존 renewal row는 `source_kind="renewal"`만으로 새 renewal view를 사용할 수 있으므로 backfill이 필요 없다. `source_kind`조차 없는 매우 오래된 row는 proxy legacy fallback을 유지한다.

### 10.4 설명 문구

UI 설명은 boundary를 정확히 말해야 한다.

- Request body read: client request body를 server가 모두 수신하는 시간이며 JSON parse는 제외
- Partial stream: response headers 이후 client cancellation 관측까지 전달된 partial response body 시간
- Finalize: body 완료/취소 이후 terminal event 발행 전 mandatory accounting/finalization
- Renewal cycle: scheduler가 수행한 cache-keepalive renewal 전체 시간; proxy request breakdown이 아님

## 11. 종료 경로별 불변식

| 경로 | 필수 불변식 |
| --- | --- |
| 정상 non-stream 2xx | `request_body_read_ms`, `request_body_bytes`, `proxy_setup_ms`, `upstream_ttfb_ms`, `upstream_body_ms`, `finalize_ms` 보존; residual ≤10ms |
| 정상 stream 2xx | `stream_total_ms` 보존; `upstream_body_ms`가 같아도 body를 한 번만 합산; residual ≤10ms |
| Upstream 4xx/5xx complete body | 실제 완료한 body stage와 finalize 보존; status/error semantics 불변 |
| Client cancellation 499 | `upstream_body_ms`가 relay start→cancel partial elapsed를 보존; `stream_total_ms`를 완료값처럼 만들지 않음; status 499와 `client_closed_request` 유지 |
| Body too large | 측정한 `request_body_read_ms` 보존; 실제 bytes를 모르면 `request_body_bytes=None`; 413 semantics 불변 |
| Body read error | read elapsed 보존; 명시적 terminal error로 종료; Drop fallback 때문에 unrelated `terminal_dropped`가 되지 않음 |
| Auth/route/limit/signer early error | body read와 완료된 setup stage를 보존; 시작하지 않은 upstream/body stage는 missing |
| Tower timeout | timeout 시점까지 완료된 field를 보존; 존재하지 않는 stage를 합성하지 않음 |
| Renewal success/error | `source_kind="renewal"`; proxy 공식을 사용하지 않음; 단일 renewal duration이 전체 timeline을 설명 |
| Legacy row | 신규 field missing을 허용; 기존 계산/렌더링이 깨지지 않음 |

모든 경로에서 다음 전역 조건을 지킨다.

```text
0 <= each timing
accounted parent stages do not overlap
UI never sums a parent and its child twice
request_body_bytes != response body_bytes
```

## 12. Migration과 backward compatibility

1. 세 신규 필드는 모두 optional이며 historical payload에 default `None`을 사용한다.
2. SQLite migration은 nullable column만 추가하고 historical backfill scan을 하지 않는다.
3. PostgreSQL historical JSONB는 수정하지 않는다.
4. 새 server가 오래된 row를 읽을 수 있어야 한다.
5. 새 admin-web이 old/mixed/new payload를 모두 렌더해야 한다.
6. Missing과 measured zero를 구분한다.
7. Rollout 중 일부 새 row에 field가 부분적으로 없더라도 panic/schema rejection 없이 표시한다.
8. Existing `body_bytes`, `stream_total_ms`, `upstream_body_ms`, setup detail field의 의미를 변경하지 않는다.
9. Alias, deprecated duplicate field, historical value synthesis을 추가하지 않는다.
10. Schema/DTO 변경은 lifecycle → assembler → request log → storage API/backend → admin API/web 순서로 한 번에 cut over한다.

## 13. Prometheus와 OTLP 계약

### 13.1 Prometheus

Request ID, principal, model, body-size raw value를 label로 사용하지 않는다. 고정 vocabulary만 사용한다.

제안 metric:

```text
cc_lb_request_stage_duration_seconds{source_kind,stage,outcome}
cc_lb_request_body_size_bytes{source_kind,outcome}
cc_lb_request_unaccounted_duration_seconds{source_kind,outcome}
cc_lb_request_unaccounted_over_budget_total{source_kind,outcome}
```

고정 label 값:

- `source_kind`: `proxy|renewal|unknown`
- `stage`: `request_body_read|proxy_setup|shape|sign|upstream_ttfb|response_body|finalize|renewal_cycle`
- `outcome`: `success|client_cancelled|error|timeout`

규칙:

- `request_stage_duration_seconds`는 event에 실제 present인 parent stage만 observe한다.
- `response_body`는 complete stream/non-stream 또는 499 partial 중 선택된 값 하나만 observe하며 `outcome`으로 구분한다.
- Renewal은 `renewal_cycle`만 observe하며 proxy stage를 0으로 emit하지 않는다.
- `unaccounted_over_budget_total`은 신규 계측이 완비된 정상 proxy에서 residual >10ms일 때 증가한다. Legacy/mixed row는 budget alert denominator에서 제외한다.
- Body-size histogram bucket은 운영의 854KB/918KB 사례와 body cap을 구분할 수 있어야 한다. Unbounded label은 추가하지 않는다.

### 13.2 OTLP/tracing

실제 span 이름과 경계는 다음과 같다.

- `proxy.read_request_body`: request body collect를 감싸며 `http.request.body.size`, `cc_lb.request_body_read_ms`, `outcome`을 기록한다.
- `proxy.handle`: `Lifecycle::handle` 진입 시 시작한다. `LifecycleContext`가 이 span의 clone을 terminal state에 보관하므로 streaming response를 먼저 반환해도 `RequestTerminated` 발행까지 span을 유지한다. Terminal 직전에 event와 같은 parent timing 값으로 `cc_lb.request.finalize_ms`와 `cc_lb.request.unaccounted_ms`를 기록한다.
- `proxy.finish_response`: buffered response body collect와 mandatory finalization을 감싸며 `cc_lb.response_body_ms`와 `cc_lb.request.finalize_ms`를 기록한다.
- `proxy.response_stream`: downstream stream relay를 감싼다. Complete이면 기존 `stream_total_ms`, client cancellation이면 partial `cc_lb.response_body_ms`와 `stream.outcome="client_cancelled"`를 기록한다.
- `cache_keepalive.renewal_lifecycle`: `RequestStarted`부터 `RequestTerminated`까지 기존 renewal lifecycle event 발행만 감싼다. `cc_lb.source_kind="renewal"`과 `cc_lb.renewal_cycle_ms=duration_ms`를 기록하며 proxy stage를 합성하지 않는다.

`proxy.handle`의 unaccounted 계산은 `duration_ms`에서 present인 `request_body_read_ms`, `proxy_setup_ms`, `shape_ms`, `sign_ms`, `upstream_ttfb_ms`, 선택된 response-body parent, `finalize_ms`를 한 번씩만 뺀다. 합과 차는 saturating 연산을 사용한다. Request body content, credential, request ID의 high-cardinality duplicate attribute를 새로 추가하지 않는다. 기존 request ID correlation attribute는 그대로 사용할 수 있다.

## 14. 구현 결과

아래 5단계는 lifecycle, storage/API, UI, metrics와 회귀 검증까지 구현 완료됐다. 각 단계의 항목은 구현 당시 사용한 계약과 acceptance boundary를 보존한다.

### 단계 1: ingress와 lifecycle contract

- `request_body_read_ms`, `request_body_bytes`를 server에서 측정한다.
- `LifecycleContext` terminal state와 `RequestTerminated`에 세 신규 필드를 추가한다.
- Body error/413 terminal path를 명시적으로 닫는다.

Acceptance:

- Slow chunked upload wall time이 `request_body_read_ms`에 들어간다.
- JSON parse 시간과 겹치지 않는다.
- 854,336B/917,567B fixture의 `request_body_bytes`가 정확하다.

### 단계 2: normal/Drop final timing

- Normal stream/non-stream에 `finalize_ms`를 기록한다.
- Drop guard에서 cancellation 시점의 partial elapsed를 기존 `upstream_body_ms`에 보존한다.
- Parent/child clock boundary를 한 곳에서 정의한다.

Acceptance:

- 정상 proxy residual ≤10ms.
- Mid-stream cancellation의 partial elapsed가 허용 오차 안에서 보존된다.
- Complete stream과 499 partial stream이 동시에 합산되지 않는다.

### 단계 3: event/storage/API propagation

- Assembler, request log, storage API, SQLite/PostgreSQL, admin recent/delta/detail을 갱신한다.
- SQLite nullable materialized list column migration을 추가한다.

Acceptance:

- Lifecycle input, final payload, DB row, recent, delta/final, detail의 세 신규 값이 일치한다.
- Old row와 mixed row가 deserialize/list/render된다.

### 단계 4: source-kind UI

- Proxy 계산을 새 parent stage 모델로 변경한다.
- 499 `Partial stream (client cancelled)`과 renewal `Renewal cycle`을 별도 표시한다.
- Legacy fallback을 유지한다.

Acceptance:

- Renewal에 proxy `Unaccounted 100%`가 나타나지 않는다.
- 499에 cancellation 전 partial body 시간이 나타난다.
- 정상 stream body를 두 번 더하지 않는다.

### 단계 5: metrics/OTLP와 회귀 QA

- Low-cardinality metrics와 span attributes를 추가한다.
- 아래 proxy/UI matrix를 실행한다.

Acceptance:

- Metrics scrape와 in-process span recording이 request event의 source/stage 의미와 일치한다. 외부 OTLP collector export는 local config에 endpoint가 없어 실행하지 않았다.
- 신규 계측이 proxy response semantics와 request bytes를 바꾸지 않는다.

## 15. Proxy-path 및 데이터 QA — PASS

2026-09-10에 fake upstream, dynamic port, 임시 SQLite DB를 사용한 isolated run으로 Cases F–K를 검증했다. 운영 DB는 변경하지 않았다. 임시 디렉터리나 screenshot 경로는 영구 evidence로 약속하지 않으며, 아래에는 재현 가능한 command 범위와 관측값만 기록한다. 과거 Cases A–E 결과는 scenario 문서의 Historical Verdict에 별도로 남긴다.

### 15.1 실행 결과

| 범위 | Verdict | 관측값 |
| --- | --- | --- |
| Slow chunked ingress | PASS | 180,000B final request에서 `request_body_read_ms=2,030`, `duration_ms=2,048`, raw residual `3ms`였다. Upload 지연이 body-read stage에 포함됐다. |
| Exact request bodies | PASS | 854,336B와 917,567B 각각에서 event `request_body_bytes`, upstream capture length, 입력 fixture와 capture의 SHA-256이 일치했다. Raw residual은 각각 `3ms`, `2ms`였다. |
| Normal non-stream | PASS | 선택된 response-body parent와 `finalize_ms`를 한 번씩 합산한 residual이 `2ms`였고 10ms budget 안이었다. |
| Normal complete stream | PASS | `stream_total_ms=9,961`과 `upstream_body_ms=9,961`이 같았으며 body parent는 한 번만 합산됐다. Raw residual은 `1ms`였다. |
| Client-cancelled 499 | PASS | `upstream_body_ms=1,273`, `stream_total_ms` missing, raw residual `2ms`였다. Status/error는 `499`/`client_closed_request`로 유지됐다. |
| Renewal | PASS | `source_kind="renewal"`, `duration_ms=3,210`을 단일 `Renewal cycle`로 사용했고 proxy timing을 합성하지 않았다. |
| Storage/API parity | PASS | SQLite payload와 materialized `list_request_body_read_ms`, `list_request_body_bytes`, `list_finalize_ms`가 일치했다. Recent, delta final, detail과 SQLite가 optional/missing semantics를 포함해 같은 값을 반환했다. |
| Metrics | PASS | Scrape에서 `source_kind`/`stage`/`outcome`의 고정 vocabulary만 사용함을 확인했고 stage 및 unaccounted residual histogram을 확인했다. Request ID, principal, model, raw body size를 신규 label로 사용하지 않았다. |
| OTLP | LIMITED PASS | Span recording tests passed; collector export not exercised because local config has no endpoint. |

### 15.2 Cases F–K verdict

| Case | Required surfaces | Verdict | Evidence |
| --- | --- | --- | --- |
| F — slow chunked ingress and 854,336B parity | Proxy, SQLite, recent/delta/detail, upstream capture | PASS | 180,000B slow chunk: body read `2,030ms`, duration `2,048ms`, residual `3ms`. Separate 854,336B exact-body run: event bytes, capture length, and SHA-256 matched; residual `3ms`. |
| G/H — 917,567B and non-stream accounting | Proxy, SQLite, recent/delta/detail | PASS | Exact 917,567B propagated across payload/materialized/API fields and matched upstream capture length/SHA-256; residual `2ms`. |
| I — complete stream accounting | Proxy, SQLite, recent/delta/detail | PASS | `stream_total_ms=upstream_body_ms=9,961`; body counted once; residual `1ms`. |
| J — 499 partial stream | Proxy, SQLite, recent/delta/detail | PASS | `upstream_body_ms=1,273`, complete `stream_total_ms` absent, residual `2ms`. |
| K — renewal timeline | SQLite, recent/delta/detail | PASS | `duration_ms=3,210` rendered as one source-specific cycle with proxy timing absent. |

### 15.3 Automated validation

| Command scope | Result |
| --- | --- |
| Rust workspace check and clippy, SQLite and PostgreSQL feature configurations | PASS |
| Engine tests | PASS — 508 |
| Server tests | PASS — 176 |
| Lifecycle tests | PASS — 6 |
| Pricing tests | PASS — 35 + 9 |
| Storage SQLite tests | PASS — 12 + 37 |
| Storage PostgreSQL tests | PASS — 11 + 27 |
| Admin event tests | PASS — 24 |
| Admin-web tests | PASS — 71 files, 684 tests |

### 15.4 Proxy path 성능 회귀 검증

현재 head와 `origin/master`를 같은 release build, loopback fake upstream, raw TCP load generator 조건에서 교대로 세 번 비교했다. 각 run은 non-streaming 1,000건(concurrency 4)과 streaming 300건(concurrency 8)을 사용했다.

| 지표 | 현재 head 3-run median | `origin/master` 3-run median | 차이 |
| --- | ---: | ---: | ---: |
| Non-streaming p50 proxy overhead | 0.343ms | 0.347ms | -0.004ms |
| Non-streaming p99 proxy overhead | 0.434ms | 0.627ms | -0.193ms |
| Streaming p50 proxy overhead | 0.867ms | 0.702ms | +0.165ms |
| Streaming p50 per-event overhead | 0.016ms | 0.013ms | +0.003ms |

모든 run은 저장소 budget인 non-streaming p50 5ms 미만, non-streaming p99 20ms 미만, streaming p50 event overhead 5ms 미만을 통과했다. Streaming p99는 loopback host scheduling outlier로 두 revision 모두 run 간 변동이 컸으며 budget 판정 지표가 아니다.

정적 hot-path 검토 결과 request executor에 추가된 작업은 요청당 monotonic clock read 3~4회, uncontended lifecycle-state lock 3~4회, span field record뿐이다. SSE chunk loop에는 추가 작업이 없고 happy path lifecycle event 수도 늘지 않았다. Timing aggregation의 String allocation, map lookup, histogram 기록은 dedicated logger task에서 수행하며 event fanout은 기존 `try_send` drop-and-count 방식이라 request executor를 block하지 않는다. 추가 in-flight 상태는 요청당 약 152B로, 10,000개 long-lived stream 기준 약 1.5MB다. Logger map은 16,384 entries로 제한되며 가장 오래된 active entry를 deterministic하게 제거하고 eviction counter를 기록한다.

따라서 측정 가능한 non-streaming 회귀는 없었고, streaming median 차이는 요청당 0.165ms 및 event당 0.003ms로 budget 대비 충분히 작았다. Proxy path에 유의미한 latency, throughput, memory, backpressure 회귀는 관측되지 않았다.

## 16. UI QA — PASS

실제 isolated Logs route를 1280×800 desktop과 375×812 mobile viewport에서 확인했다.

| Surface | Verdict | 관측값 |
| --- | --- | --- |
| Desktop proxy timeline | PASS | `Request body read`와 `Finalize`가 표시되고 residual이 10ms budget 안에 있었다. |
| Mobile 499 timeline | PASS | `Partial stream (client cancelled)`이 표시됐고 complete stream stage를 합성하지 않았다. |
| Renewal drawer | PASS | `Renewal cycle`이 `3,210ms`로 표시됐으며 `Unaccounted`와 `Internal pre`가 없었다. |
| Layout/runtime | PASS | 검사한 두 viewport에서 horizontal overflow가 없었고 browser console error가 없었다. |

UI 결과는 위 두 viewport와 Cases F–K drawer 경로에 한정한다. 768×1024, 외부 OTLP collector, 별도 배포 환경은 이번 isolated run의 증거로 주장하지 않는다.
## 17. 최종 완료 기준

구현과 2026-09-10 isolated QA는 다음 조건을 충족했다.

1. 정상 proxy final row에서 `request_body_read_ms`와 실제 `request_body_bytes`가 기록된다.
2. 854KB/918KB와 slow-upload fixture에서 body bytes와 body-read duration의 상관관계를 재현한다.
3. 신규 계측이 완비된 정상 stream/non-stream proxy의 `Unaccounted`가 각각 10ms 이하이다.
4. 499는 relay 시작부터 cancellation까지의 partial elapsed를 기존 `upstream_body_ms`에 보존한다.
5. Renewal은 proxy timeline에서 분리되고 전체 duration이 `Renewal cycle`로 설명된다.
6. Lifecycle, assembler, `RequestEvent`, SQLite/PostgreSQL, recent/delta/detail API, TypeScript schema/UI가 동일한 세 신규 필드 계약을 따른다.
7. Parent/child stage가 겹치지 않고 정상 stream body가 중복 합산되지 않는다.
8. Historical/mixed rows에 backfill 없이 backward compatibility가 유지된다.
9. Prometheus가 low-cardinality 계약과 residual histogram 계약을 지켰고 in-process span recording tests가 통과했다. 외부 OTLP collector export는 local config에 endpoint가 없어 실행하지 않았다.
10. Cases F–K의 proxy/storage/API/metrics/UI 경로가 통과했고 request body length/SHA-256 및 기존 status/error semantics가 유지됐다.
