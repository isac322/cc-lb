# Prompt-cache setup overhead 근본 개선 설계

- 상태: Implemented, locally verified, and CI passed
- 작성일: 2026-09-07
- 대상: `cc-lb-engine` proxy hot path와 admin-web latency timeline
- 관련 ADR: ADR 0007, ADR 0008

## 1. 목적

운영 `isac-pi` 요청에서 확인된 초 단위 `proxy_setup_ms`를 제거한다. 최적화와 신규 Prometheus 계측 외에는 proxy의 관측 가능한 동작을 바꾸지 않는다.

다음 값은 변경 전후 완전히 같아야 한다.

- 모든 cache-control breakpoint의 위치, TTL, prefix hash, exact local token count
- 20-content-block lookback 후보와 longest-prefix match
- predicted cache read/creation tokens, cache value, routing winner
- cache observation과 request event에 기록되는 값
- downstream status/body/headers와 upstream으로 전송되는 request bytes
- streaming, cancellation, timeout, retry 및 error mapping

Breakpoint 생략, token 근사, cache score degrade, observation 누락은 허용하지 않는다.

## 2. 운영에서 확인된 문제

2026-09-07 00:00–08:06 UTC의 `isac-pi` 요청 2,219건을 운영 PostgreSQL에서 집계했다.

- 평균 setup: 417.8ms
- p50: 249ms
- p95: 1,430ms
- p99: 3,214ms
- 최대: 10,459ms
- 누적 breakpoint token work와 setup의 Pearson 상관: 0.8675
- token work 300만 이상 구간의 setup p95: 7,178ms

최악 요청은 4.47MB, 182 messages, 4 breakpoints였다. Provider prompt는 366,323 tokens였지만 local estimator는 네 nested prefix를 각각 처음부터 계산해 총 5,704,721 token IDs를 처리했다. Request 수신부터 upstream dispatch까지 10.625초가 걸렸다.

같은 사건 창의 Prometheus는 CPU throttling 0%, pod restart 0, cache observation drop 0, observation write failure 0을 기록했다. PostgreSQL도 ungranted lock 0, active query wait 0이었고 signer name lookup은 0.178ms였다. 병목은 DB나 observation writer가 아니라 dispatch 전에 실행되는 local prompt-cache 분석이었다.

## 3. 현재 실행 지점

Proxy hot path는 `Lifecycle::handle`에서 다음 순서로 실행된다.

1. Request body를 JSON으로 parse한다.
2. `request_cache_metadata_from_value`를 호출한다.
3. `analyze_v3_prompt_cache`가 `tools → system → messages`를 flatten한다.
4. Structural BLAKE3 prefix chain과 lookback 후보를 만든다.
5. 각 breakpoint마다 `PrefixSerializer(blocks[..=index])`를 다시 실행한다.
6. 각 serialized prefix에 `o200k_base::encode_ordinary(...).len()`을 실행한다.
7. 이 작업이 끝난 뒤 auth, routing, limit reservation, signer, dispatch로 진행한다.

Structural hash chain은 이미 전체 요청에 대해 선형이다. 반복 비용은 breakpoint마다 수행되는 cumulative JSON serialization과 BPE tokenization이다.

## 4. 추가로 확인된 정확성 제약

Structural prefix hash와 token-count identity는 같은 key가 아니다.

- Structural block digest는 `cache_control`을 제외한다.
- Tokenization prefix bytes는 `cache_control`을 보존한다.

따라서 implicit 5m marker와 explicit `ttl: "5m"`는 structural hash가 같아도 local token count가 다르다. 직접 재현한 값은 45와 50이었고, 운영 데이터에서도 하나의 structural hash가 둘 이상의 token count를 가진 사례가 4개 확인됐다.

그러므로 기존 `prefix_hash`를 token-count cache key로 사용할 수 없다.

## 5. 선택한 해결 구조

### 5.1 Proxy-local structural hash 유지

기존 schema-5 BLAKE3 chain은 변경하지 않는다. 이 key는 cache affinity, lookback matching, persisted warm observation에 계속 사용한다. Hash schema version도 변경하지 않는다.

### 5.2 Exact token-prefix identity 분리

Token-count cache에는 실제 `PrefixSerializer` bytes의 BLAKE3 digest를 사용한다.

```text
TokenPrefixKey = BLAKE3(
    "cc-lb-token-prefix-v1"
    || downstream_credential_scope
    || exact_serialized_prefix_bytes
)
```

`cache_control`, JSON escaping, source, model을 포함한 실제 tokenization bytes가 같고 downstream credential scope도 같을 때만 같은 key가 된다. Scope는 auth와 같은 우선순위로 `x-api-key`를 사용하고, 없을 때만 `Authorization` header를 one-way BLAKE3로 섞는다. 원문은 저장하거나 노출하지 않는다. Structural invalidator salt나 tokenization input에 없는 path/index metadata는 넣지 않는다.

### 5.3 Exact nested-prefix batch 준비

Breakpoint마다 block tree를 다시 serialize하지 않는다.

1. `{"content_blocks":[` header를 한 번 기록한다.
2. 각 prefix block을 한 번만 deterministic `serde_json`으로 serialize한다.
3. 각 breakpoint block 끝의 byte offset을 기록한다.
4. `],"model":...}` suffix를 결합하면 기존 `PrefixSerializer`와 byte-identical한 prefix가 된다.
5. 각 offset에서 incremental BLAKE3 hasher를 clone해 exact `TokenPrefixKey`를 만든다.

기존 direct serializer와 byte-for-byte equality가 깨지면 최적화 경로를 사용하지 않고 기존 serializer로 fallback한다.

### 5.4 Exact incremental token counting

단순 block별 token count 합산은 BPE boundary 때문에 금지한다. 선택한 fast path는 `tiktoken-rs 0.12`의 ordinary encoder가 반환하는 마지막 불안정 regex piece를 보존한다.

- 이전 breakpoint에서 확정된 stable token count는 재사용한다.
- 새 block bytes와 이전 unstable tail만 tokenization한다.
- 각 breakpoint의 standalone JSON suffix는 unstable tail에 붙여 exact count를 계산한다.
- Incremental encoder 오류나 boundary invariant 불일치 입력은 기존 `encode_ordinary(full_prefix).len()`으로 fallback한다.

모든 fast-path 결과는 frozen reference의 full-prefix count와 differential 비교한다. 오차 허용치는 0이다.

### 5.5 Bounded exact count cache

Process-local bounded cache를 둔다.

- key: `TokenPrefixKey`
- value: exact `u64` local token count
- 기본 capacity: 8,192 entries
- capacity 초과 시 오래된 insertion부터 제거
- eviction은 cache miss와 재계산만 만들며 결과에는 영향을 주지 않는다.

모든 requested breakpoint count는 계속 반환한다. Cache hit은 계산 생략일 뿐 기능 생략이 아니다.

Cache는 downstream credential scope별로 격리한다. 서로 다른 principal/key가 byte-identical prompt를 보내도 cache hit timing을 공유하지 않는다. None-auth mode는 단일 configured principal이므로 `unauthenticated` scope를 사용한다.

### 5.6 Cancellation-safe single-flight

동일 `TokenPrefixKey`의 concurrent miss는 한 번만 계산한다.

- Blocking job은 request future와 독립적으로 완료된다.
- Leader request가 취소돼도 waiter와 cache는 결과를 받는다.
- Panic/failure 시 flight entry와 permit를 정리하고 waiter를 깨운다.
- 실패 결과는 cache하지 않는다.

### 5.7 Bounded blocking executor

`Lifecycle::handle`의 prompt-cache 분석을 Tokio worker에서 직접 실행하지 않는다.

- `Arc<Value>`를 `spawn_blocking` job에 전달한다.
- `tokio::sync::Semaphore`로 동시 CPU job 수를 제한한다.
- 기본 concurrency는 `available_parallelism - 1`, 최솟값 1로 한다.
- Queue에는 timeout이나 approximation을 두지 않는다. 대기는 async이고 기능은 그대로 유지한다.
- Sync API인 `preview_route`와 `parse_request_cache_breakpoints`는 기존 exact sync analyzer를 유지한다.

`Lifecycle::new`와 `new_with_dynamic_view`의 public signature는 변경하지 않는다. Executor와 cache는 내부 private field로 생성한다. 테스트만을 위한 production public API는 추가하지 않는다.

### 5.8 Prometheus 계측

내부 원인 분석은 request timeline이 아니라 Prometheus로 노출한다.

- `cc_lb_prompt_cache_analysis_duration_seconds{stage="queue|total|tokenize"}`
- `cc_lb_prompt_cache_token_count_cache_total{result="hit|miss|coalesced"}`
- `cc_lb_prompt_cache_tokenizer_inflight`
- `cc_lb_prompt_cache_tokenized_bytes_total`
- `cc_lb_prompt_cache_tokenized_tokens_total`
- `cc_lb_prompt_cache_tokenizer_fallback_prefixes_total`
- `cc_lb_prompt_cache_analysis_worker_failed_total`

Label은 고정된 stage/result만 사용한다. Principal, request ID, prefix hash, model은 label로 추가하지 않는다.

### 5.9 Request Timeline 경계

Request event의 `observability_post_ms` 필드와 DB/API compatibility는 유지한다. 다만 admin-web latency timeline과 latency cell에서는 이 post-response 관측 stage를 표시하지 않는다.

- `Setup overhead`는 dispatch를 실제로 지연한 시간이므로 유지한다.
- `Observability post`는 timeline stage에서 제거한다.
- 새 prompt-cache micro timings는 timeline에 추가하지 않는다.
- Timeline total은 기존 `duration_ms`를 유지한다. 이 값은 post-response observation 실행 전에 확정되므로 별도 차감하지 않는다.
- Observation pipeline 상태는 Prometheus에서만 본다.

## 6. 파일별 구현 계획

### Engine

- `crates/cc-lb-engine/src/prompt_cache_simulator.rs`
  - structural analysis와 exact prefix batch preparation 분리
  - frozen reference analyzer 유지
  - optimized analyzer와 work statistics 추가
- `crates/cc-lb-engine/src/tokenizer.rs`
  - exact nested-prefix incremental counter 추가
  - reference path의 기존 thread-local call counter와 별도로 optimized work statistics 추가
- `crates/cc-lb-engine/src/prompt_cache_simulator/optimized.rs`
  - bounded executor, exact cache, single-flight, exact prefix batch 구현
- `crates/cc-lb-engine/src/lifecycle.rs`
  - request JSON을 `Arc<Value>`로 보관
  - proxy hot path에서 async executor 사용
  - sync preview/parser path 유지
- `crates/cc-lb-engine/src/lib.rs`
  - 내부 module만 등록하며 새 production public API는 내보내지 않음

### Observability

- `crates/cc-lb-observability/src/init.rs`
  - metric definitions, descriptions, registration, initial handles 추가

### Admin web

- `computeStageGroups.ts`
  - timeline total은 기존 `duration_ms` 유지
  - internal post는 `limit_reconcile_ms`만 계산
- `LatencyTimeline.tsx`
  - `Observability post` stage 제거
  - stage percentage와 unaccounted는 기존 `duration_ms` 기준 유지
- `LatencyCell.tsx`
  - Observability item 제거, headline/popover percentages는 기존 phase별 denominator 유지

## 7. 실패와 fallback

- Incremental boundary 검증 실패: 해당 request는 기존 full-prefix count path 사용
- Incremental encoder 또는 boundary invariant 실패: 기존 full-prefix count path 사용
- Blocking task join failure: 기존 dropped-events metric과 error trace를 남기고 sync analyzer로 정확히 fallback
- Cache eviction: 정확한 재계산
- Single-flight leader failure: entry 제거 후 waiter가 정확한 재계산

어떤 fallback도 approximation이나 breakpoint 누락을 허용하지 않는다.

## 8. 완료 기준

- Frozen reference와 optimized analyzer의 전체 `V3PromptCacheAnalysis` equality
- 0/1/3/4/5 breakpoint, mixed TTL, lookback 19/20 경계 모두 동일
- Proxy integration에서 selected upstream, request bytes, status/body, event/observation 동일
- 4-breakpoint production-like fixture의 tokenized work가 cumulative 4-pass가 아니라 near-deepest-prefix 수준
- Concurrent identical request에서 actual tokenization job 1회
- 대형 분석 중 Tokio heartbeat가 지속
- Prometheus endpoint에 새 metrics 노출
- Admin-web에서 Observability post가 timeline에 나타나지 않음
- 전체 CI 통과
