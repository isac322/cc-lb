# Principal usage totals · polling QA

작성 시점: 구현 전

합의 주체: 6개 독립 조사 에이전트, 2개 비판 리뷰어, 1개 최종 판정 에이전트

구현 중 기술 검토 정정: live-tail component guardrail은 step bucket별로
`event total <= rollup virtual cost`를 검증한다. Event cost를 전체 범위 한 bucket으로
합치면 한 개의 미롤업 tail이 과거의 유효 component까지 제거하므로 값 동일성이 깨진다.
따라서 합의의 최상위 조건인 무동작 변경을 지키기 위해 event-cost bucket granularity는
유지하고, zero-filled dense series materialization만 제거한다.

## 불변 계약

- Principal별 total cost와 component breakdown 값은 기존 full-series collapse와 동일하다.
- UUID 및 legacy/non-UUID principal normalization과 merge 결과를 유지한다.
- Upstream filter 유무가 결과 행 집합과 비용 합계를 바꾸지 않는다.
- Negative·NULL cost 처리와 `component_costs_recorded` 판정을 유지한다.
- SQLite와 PostgreSQL이 동일 결과를 반환한다.
- Live request-event tail을 즉시 반영하며 ETag는 계속 비활성화한다.
- 동일 요청 병합은 응답값·오류·취소 의미를 바꾸지 않는다.

## SQL shape · plan QA

- [x] PostgreSQL no-upstream query에는 nullable upstream predicate가 존재하지 않는다.
- [x] PostgreSQL filtered query는 별도 정적 SQL shape을 사용한다.
- [x] SQLite도 upstream filter 유무에 따라 정적 SQL shape을 선택한다.
- [x] UUID 요청 집합이 비면 UUID branch를 실행하지 않는다.
- [x] Normalization branch는 선택 key 전체를 대상으로 요청당 정확히 한 번 실행되어 padded UUID·legacy·unknown을 함께 처리한다.
- [x] Mixed UUID/non-UUID 입력은 두 결과를 기존 규칙대로 병합한다.
- [x] PostgreSQL prepared generic no-upstream plan이 covering index-only shape이며 heap fetch 0을 달성할 수 있다.
- [x] PostgreSQL filtered plan이 payload/TOAST를 읽지 않고 index-level cost columns를 사용한다.
- [x] NULL, negative, partially recorded component 값의 합계가 기존 SQL과 동일하다.

## Totals projection QA

- [x] `projection=totals`가 dense principal series를 생성하지 않는다.
- [x] Totals 경로는 step별 live-tail guardrail용 sparse bucket만 만들고 zero-filled dense series를 생성하지 않는다.
- [x] 동일 fixture에서 full projection을 collapse한 결과와 totals projection이 필드별로 동일하다.
- [x] Rollup + live event overlap/exclusion 규칙을 유지한다.
- [x] Empty principal selection과 데이터 없는 범위가 기존 empty 결과를 유지한다.
- [x] Upstream-filtered totals도 full projection collapse와 동일하다.

## Coalescing · freshness QA

- [x] 동시에 시작된 동일 query는 하나의 backend 계산만 수행한다.
- [x] Query parameter가 하나라도 다르면 계산을 공유하지 않는다.
- [x] Leader 성공 시 모든 waiter가 동일 결과를 받는다.
- [x] Leader 오류 시 waiter가 매달리지 않고 동일 오류 의미를 받는다.
- [x] Waiter 취소가 leader 계산을 취소하지 않는다.
- [x] 최대 1~3초 microcache 안의 동일 요청은 재사용한다.
- [x] TTL 후에는 반드시 새 계산을 수행한다.
- [x] 새 live request event는 허용된 microcache 상한 뒤 응답에 반영된다.
- [x] ETag는 principal grouping에서 계속 발급하지 않는다.

## API · 브라우저 QA

- [x] `/admin/usage?...group_by=principal&projection=totals` JSON schema와 값이 유지된다.
- [x] Overview Top principals의 total 및 Input/Output/Cache 5m/Cache 1h/Cache read 값이 정확하다.
- [x] 새 request event를 기록한 뒤 허용된 TTL과 5초 poll 안에 카드 값이 증가한다.
- [x] Principal 또는 upstream filter 변경이 다른 query cache와 섞이지 않는다.
- [x] Loading/error/empty 상태가 유지된다.

## 성능 QA

- [x] PostgreSQL scratch에서 optional predicate before와 정적 no-upstream shape after를 같은 데이터로 측정한다.
- [x] `plan_cache_mode=auto` 반복 실행 후 generic plan의 Index Only Scan, Heap Fetches, buffers를 기록한다.
- [x] Filtered path의 plan과 buffers를 별도로 기록한다.
- [x] SQLite와 PostgreSQL에서 full-collapse 대비 sparse direct totals p50·p99를 측정한다.
- [x] 동시 50개 동일 요청에서 backend 계산 횟수와 wall time 감소를 측정한다. 50-way 결과가 10-way 요구를 포함한다.
- [x] 절대시간 임계값은 CI 영구 테스트로 고정하지 않고 측정 보고서에 기록한다.

## 실행 결과

- PostgreSQL 150,000-row scratch, no-upstream generic shape:
  `74.330ms / Bitmap Heap Scan / 7,893 heap blocks` →
  `18.112ms / Index Only Scan / isolated VACUUM에서 heap fetch 0`.
- PostgreSQL filtered UUID:
  `25.846ms / heap access` → `0.330ms / Index Only Scan / heap fetch 0`.
- Fixed normalized no-upstream/filtered: `0.868ms / 0.329ms`, 모두 Index Only Scan.
- SQLite API, 8,000 events: cold p50 `25.468ms` → `17.634ms`,
  hot p50 `73.872ms` → `0.019ms`,
  50-way wall `2,536.515ms` → `21.091ms`.
- PostgreSQL API, 8,000 events: first cold `17.091ms` → `15.731ms`;
  cold p50는 `17.233ms` → `22.009ms`로 이 fixture에서는 개선되지 않았다.
  Hot p50 `15.234ms` → `0.019ms`, 50-way wall `112.036ms` → `25.851ms`.
- 모든 before/after API 응답은 동일한 `12,490` bytes였고 50-way 응답도 byte-equal이었다.
- Cache tests: 50 waiter build 1회, TTL expiry, error fan-out/retry, waiter cancellation,
  leader panic recovery, storage/query-key isolation 통과.
- Admin: unit `49/49`, integration `253/253`.
- Storage conformance: SQLite `78/78`, PostgreSQL `63/63`.
- 독립 SQL 및 cache/totals 리뷰: finding 0건.
- PostgreSQL 영구 plan test는 parallel snapshot에 따라 달라지는 visibility-map 수치를
  고정하지 않고 네 production SQL shape의 Index Only Scan을 검증한다. Heap fetch 0은
  격리된 scratch VACUUM 측정으로 확인했다.

## 종료 게이트

- [x] 관련 Rust unit/integration/conformance 테스트 통과
- [x] SQLite QA 통과
- [x] PostgreSQL QA 통과
- [x] Admin API point-in-time 및 state-transition QA 통과
- [x] Overview frontend는 변경하지 않았으며 API JSON byte parity와 기존 web suite로 비회귀를 확인했다.
- [x] 독립 코드 리뷰 finding 0건
- [ ] GitHub CI 전체 통과
