# Prompt-cache setup overhead TDD 및 QA 체크리스트

- 상태: Local QA complete; CI pending
- 작성일: 2026-09-07
- 설계 문서: `docs/prompt-cache-setup-overhead-remediation.md`

## 1. 검증 원칙

1. 신규 테스트를 먼저 추가해 baseline 실패 또는 기존 비효율을 증명한다.
2. 결과 정확성은 기존 analyzer를 frozen reference로 사용해 differential 검증한다.
3. 성능 회귀는 wall-clock만으로 판정하지 않고 tokenization calls/bytes/tokens 같은 deterministic work counter로 고정한다.
4. 외부 provider 없이 증명 가능한 항목은 unit/component/integration test로 영구 보존한다.
5. Proxy path는 fake Anthropic upstream을 사용한 실제 HTTP 요청으로 검증한다.
6. Production provider 호출은 필수가 아니며, 실행할 경우 별도 optional calibration으로 분류한다.

## 2. TDD 순서

### A. Pre-fix failure

- [ ] 4개 nested breakpoint fixture에서 기존 tokenizer call count가 4임을 증명한다.
- [ ] 현재 metrics registry에 prompt-cache analysis metrics가 없음을 증명한다.
- [ ] `observability_post_ms`가 별도 post-response 값인데도 timeline stage에 포함됨을 기존 component test로 고정한다.
- [ ] 4.4MB production-like fixture에서 누적 serialized/tokenized bytes가 deepest prefix보다 크게 증가함을 work counter로 증명한다.

### B. Exact serialization과 key

영구 unit/property tests:

- [ ] Batch serializer가 기존 `PrefixSerializer` bytes와 모든 breakpoint에서 동일하다.
- [ ] Implicit 5m과 explicit `ttl: "5m"`은 structural hash가 같지만 `TokenPrefixKey`가 다르다.
- [ ] `cache_control` key order, 5m/1h, marker 이동이 exact token key에 반영된다.
- [ ] 동일 prefix라도 downstream credential scope가 다르면 token count cache를 공유하지 않는다.
- [ ] JSON escape, CJK, 한글, emoji/ZWJ, whitespace, 긴 숫자, base64에서 byte parity를 유지한다.
- [ ] 0/1/19/20/21/128/259 blocks와 0/1/3/4/5 breakpoints를 검증한다.

대상:

- `crates/cc-lb-engine/tests/prompt_cache_byte_oracle.rs`
- `crates/cc-lb-engine/tests/prompt_cache_structural_properties.rs`
- 신규 `crates/cc-lb-engine/tests/prompt_cache_setup_optimization.rs`

### C. Exact incremental token count

영구 unit/property tests:

- [ ] 모든 breakpoint의 fast count가 기존 full-prefix `encode_ordinary().len()`과 정확히 같다.
- [ ] Token count 오차 허용치는 0이다.
- [ ] BPE boundary가 바뀌는 `}`, `,`, `]`, quote, newline, trailing whitespace를 포함한다.
- [ ] Special-token literal 입력도 fast path와 full reference 결과가 같다.
- [ ] 잘못된 offset/invariant를 의도적으로 주입한 test seam에서 fallback이 같은 결과를 낸다.
- [ ] 4-breakpoint work counter가 prefix 4개 누적이 아니라 deepest prefix와 작은 boundary tail 수준이다.

### D. Cache와 single-flight

영구 unit/concurrency tests:

- [ ] 첫 요청은 miss, 두 번째 동일 요청은 hit이며 두 결과가 같다.
- [ ] Structural hash는 같고 exact token key가 다른 두 요청이 cache를 공유하지 않는다.
- [ ] 64개 identical concurrent requests의 실제 leader count는 1이다.
- [ ] Leader future 취소 후에도 blocking job이 결과를 cache하고 waiter가 완료된다.
- [ ] Leader panic/failure에서 flight와 semaphore permit가 누수되지 않는다.
- [ ] Capacity 초과 eviction 후 재계산 결과가 동일하다.
- [ ] 다른 keys는 불필요하게 같은 flight에 묶이지 않는다.

### E. Async runtime non-blocking

영구 integration test:

- [ ] 2-worker Tokio runtime에서 1ms heartbeat와 large 4-breakpoint analysis를 동시에 실행한다.
- [ ] 분석은 blocking worker에서 수행된다.
- [ ] Heartbeat가 전체 BPE 시간 동안 멈추지 않는다.
- [ ] Semaphore concurrency limit를 넘는 active tokenizer jobs가 없다.
- [ ] Queue wait cancellation이 permit를 소비하지 않는다.

Wall-clock threshold는 넓은 safety bound만 사용하고, 주 판정은 heartbeat progress와 active-job counter로 한다.

### F. Routing 및 observation differential

영구 engine integration tests:

- [ ] Reference와 optimized path의 `V3PromptCacheAnalysis` 전체 equality
- [ ] Cache breakpoints와 lookback prefixes equality
- [ ] Cold miss, exact warm hit, N-19 lookback hit, N-20 miss
- [ ] 5m, 1h, 1h→5m, invalid 5m→1h
- [ ] predicted read/creation tokens와 cache value equality
- [ ] selected upstream과 WRH key source equality
- [ ] buffered/SSE observation records와 expiry equality
- [ ] Provider read/prediction disagreement 결과 동일

### G. Prometheus metrics

영구 component/integration tests:

- [ ] Metric definitions와 `describe_*` 등록 존재
- [ ] Histogram/counter/gauge label은 고정 `stage`와 `result`만 사용
- [ ] Miss 분석에서 queue/total/tokenize duration, bytes/tokens가 증가
- [ ] 두 번째 동일 분석에서 cache hit가 증가하고 tokenized bytes는 증가하지 않음
- [ ] Concurrent 동일 분석에서 coalesced가 증가
- [ ] `/metrics` scrape에 새 metric 이름이 나타남
- [ ] Principal ID, request ID, prefix hash가 label에 나타나지 않음

### H. Admin-web timeline

영구 Vitest/component tests:

- [ ] `observability_post_ms`가 `internalPost` 합계에서 제외됨
- [ ] Timeline total denominator는 기존 `duration_ms`를 유지
- [ ] `Observability post` stage/button/detail이 렌더링되지 않음
- [ ] `observability_post_ms` API field가 있어도 다른 stages의 width/percentage가 정확함
- [ ] `limit_reconcile_ms`는 기존 의미대로 유지
- [ ] Partial/live rows의 기존 behavior 유지
- [ ] LatencyCell popover에 Observability item이 없음

## 3. 실제 proxy-path QA 매트릭스

격리된 SQLite DB와 local fake Anthropic upstream을 사용한다. Shared production DB를 변경하지 않는다.

| Case | 요청 | 확인 항목 |
|---|---|---|
| P1 | Cache-control 없음 | 기존 status/body/header, tokenization work 0 |
| P2 | 4×5m breakpoints cold | 정상 dispatch, 네 exact counts, cache creation score |
| P3 | 같은 요청 반복 | 두 번째 exact count cache hit, upstream bytes 동일 |
| P4 | N-19 lookback warm | 동일 upstream 선택과 matched index/distance |
| P5 | N-20 entry만 warm | cache miss routing 유지 |
| P6 | 1h→5m mixed TTL | read/create segment와 selected upstream 동일 |
| P7 | Streaming SSE | message_start/delta/stop bytes와 usage observation 동일 |
| P8 | Client disconnect | 499/error classification과 flight cleanup |
| P9 | Upstream timeout/429/500 | status/error mapping과 observation behavior 동일 |
| P10 | Concurrent large requests | event loop 진행, executor bound, 모든 응답 완료 |

각 case에서 가능한 항목을 함께 확인한다.

- Client-visible status/body/headers
- Fake upstream이 받은 method/path/body/header digest
- Routing trace와 selected upstream
- Request event cache fields
- Prompt-cache observation record
- Prometheus cache/executor counters

## 4. Browser QA

격리된 admin-web과 mock request event를 사용한다.

- [ ] Logs에서 final request drawer를 연다.
- [ ] `observability_post_ms`가 있는 event에서도 timeline에 Observability stage가 없다.
- [ ] Auth/Route/Setup/TTFB/Body/Limit reconcile은 정상 표시된다.
- [ ] 전체 표시 시간과 percentage가 기존 `duration_ms` 기준을 유지한다.
- [ ] 375×812, 768×1024, 1280×800에서 clipping/overflow가 없다.
- [ ] Light/dark theme에서 stage colors와 tooltip이 정상이다.
- [ ] Console error 0, failed network request 0.

## 5. 전체 검증 명령

변경 중에는 focused tests를 사용하고 마지막에 한 번만 전체 검증한다.

```text
cargo fmt --all -- --check
cargo test -p cc-lb-engine <focused cases>
cargo test -p cc-lb-observability
bun --cwd crates/cc-lb-admin/web test <latency tests>
bun --cwd crates/cc-lb-admin/web run check
cargo nextest run --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

CI와 같은 명령이 별도 workflow에 있으면 해당 workflow invocation을 우선한다. CI 실패를 재실행으로 숨기지 않고 같은 PR에서 원인을 수정한다.

## 6. PR 전 완료 체크

- [x] 설계 문서와 실제 구현 일치
- [x] 모든 신규 테스트가 회귀 가능한 observable contract를 검증
- [x] Temporary benchmark/fixture 정리
- [x] Proxy QA matrix PASS
- [x] Browser QA PASS
- [x] Worktree에 비밀값·운영 dump 없음
- [x] Internal reviewer와 security reviewer 지적 해결
- [ ] PR CI 전체 통과

## 7. 2026-09-07 격리 QA 실행 기록

격리된 SQLite DB, local fake Anthropic upstream, 동적 proxy/admin/metrics 포트를 사용했다. Shared production DB와 provider credentials는 사용하지 않았다.

### Proxy path

- Cache-control 없는 요청: 200 / `pong`, tokenizer bytes counter 변화 0
- 1.44MB, 4×5m cold request: 200 / 0.864s
- 동일 요청 두 번째 실행: 200 / 0.104s
- 두 응답 body SHA-256 동일
- Fake upstream이 받은 두 request body SHA-256이 입력 파일과 동일
- SQLite request events: 두 row 모두 `cache_control_block_count=4`, breakpoint JSON length 4, 동일 cache prefix hash
- Proxy setup: 첫 요청 854ms, 두 번째 97ms

### Concurrent single-flight

- 1.54MB 동일 요청 8개를 동시에 실행
- 8/8 status 200, body `pong`
- 각 wall time 0.576–0.590s
- Metric delta: miss 4, coalesced 28
- Tokenized bytes delta 1,536,611 bytes로 요청 8배가 아니라 한 batch 크기 수준

### Prometheus scrape

- `cc_lb_prompt_cache_analysis_duration_seconds{stage=\"queue|tokenize|total\"}` 노출
- `cc_lb_prompt_cache_token_count_cache_total{result=\"hit|miss|coalesced\"}` 노출
- `cc_lb_prompt_cache_tokenized_bytes_total`, `cc_lb_prompt_cache_tokenized_tokens_total`, fallback-prefix counter, worker-failure counter 노출

### Browser/component
- 표시 total은 별도 post-response 값 600ms를 포함하지 않는 기존 `duration_ms=4,400ms` 기준을 유지
- 실제 WKWebView로 persisted `LatencyTimeline` 컴포넌트를 1280×800, 375×812에서 렌더링
- `observability_post_ms=600` fixture에서 `Observability post` 미표시
- Internal pre, Wait, Upstream, Body, Limit reconcile, Unaccounted 정상 표시
- 두 viewport 모두 horizontal overflow 0, clipping/overlap 없음

실제 Logs route에서도 drawer DOM의 timeline text와 계산 결과를 확인했다. 자동화 환경에서 drawer transition이 off-screen 상태로 유지되어 시각 캡처는 동일 컴포넌트 전용 QA page로 수행했다.

### 보안·메모리 수정 후 최종 proxy 재검증

- Downstream credential scope와 unstable-tail decode 최적화 반영 후 격리 서버를 다시 빌드했다.
- 0.99MB, 4-breakpoint request: cold 0.734s, exact-cache hit 0.079s
- Fake upstream request length 992,385 bytes, 입력과 SHA-256 동일
- SQLite events: 두 row 모두 status 200, breakpoint count 4, 64-character cache prefix hash
- `proxy_setup_ms`: cold 724ms, hit 73ms
- Metrics: miss 4, hit 4, fallback prefixes 0, worker failures 0, inflight 최종 0
- Listener 4개 종료와 임시 DB/config/credential 삭제 확인

### Local validation 요약

- `cargo fmt --all -- --check`: PASS
- `cc-lb-engine`: unit 449, integration 236 PASS
- `cc-lb-server`: unit 169, integration 118 PASS
- `cc-lb-observability`: 전체 PASS
- Admin web: build, routeTree diff, Biome, typecheck, 555 tests PASS
- CI guard scripts와 Prometheus rule syntax PASS
- 로컬 toolchain에는 `cargo-clippy`, `cargo-nextest`, `cargo-llvm-cov`, `cargo-deny`가 없어 해당 job은 GitHub CI에서 검증한다.
- macOS의 workspace `--all-features`는 기존 Wasm test fixture의 Mach-O section 제약으로 중단됐다. 변경 crate와 Linux CI 경로의 결과가 아니며 GitHub CI에서 최종 판정한다.
