# Subscription quota range · analysis QA

작성 시점: 구현 전

합의 주체: 6개 독립 조사 에이전트, 2개 비판 리뷰어, 1개 최종 판정 에이전트

## 불변 계약

- 요청 범위 밖의 오래된 anchor는 current-state seed로만 사용한다.
- Bucket 생성은 `floor(since / bucket_secs)`부터 `until`까지 수행한다.
- 1시간/60초의 inclusive 계약은 window당 61개 dense carry-forward point다.
- Anchor-only point도 `sample_count=0`, `observed=true`를 유지한다.
- Left anchor의 정확한 timestamp, source, value는 보존한다.
- Reset, gap, source merge, tie-breaking, downsampling 결과가 기존 요청 범위 의미를 유지한다.
- PostgreSQL과 SQLite가 동일 결과를 반환한다.
- `/analysis`는 요청한 upstream만 storage query에서 읽는다.
- Frontend series freshness 30초와 analysis 120초 poll을 유지하고, 두 query key는 absolute clock 대신 stable range identity를 사용한다.

## 저장소·도메인 QA

- [x] SQLite public series: anchor 1일/30일/180일에서 출력 point 수가 요청 범위에만 의존한다.
- [x] PostgreSQL public series: 같은 입력이 SQLite와 동일한 point·timestamp·source·value를 반환한다.
- [x] Admin slim series: anchor가 `since` 이전이어도 요청 범위 전체 61개 dense point를 반환한다.
- [x] Provider-lots aggregate: anchor age와 무관하게 loop work가 요청 범위에 제한된다.
- [x] Anchor가 없을 때 기존 empty/unknown 결과가 유지된다.
- [x] Anchor가 `since`와 정확히 같을 때 중복 point가 생기지 않는다.
- [x] Anchor가 `since` 직전일 때 첫 bucket이 올바른 carried state를 가진다.
- [x] 범위 안 reset 이후 이전 값이 carry-forward되지 않는다.
- [x] Source가 바뀌는 동일 timestamp tie가 기존 우선순위를 유지한다.
- [x] Long gap과 missing observation semantics가 유지된다.
- [x] Downsampling은 요청 범위 point만 대상으로 하며 첫점·마지막점과 기존 균등 인덱스 선택을 보존한다.

## Analysis QA

- [x] 새 storage API는 non-empty upstream ID 집합을 mandatory SQL predicate로 사용한다.
- [x] PostgreSQL plan이 `(upstream_id, resolution, bucket_start)` 인덱스를 사용할 수 있는 shape다.
- [x] SQLite 쿼리도 upstream filter와 시간 범위를 SQL에서 적용한다.
- [x] 여러 upstream의 rollup을 upstream별 한 번만 partition한다.
- [x] Window별 계산이 원본 row를 deep clone하지 않는다.
- [x] Analysis 응답의 utilization, deficit, observation 수와 source가 기존 의미를 유지한다.
- [x] Backend 상태를 변경한 뒤 다음 analysis refresh에서 값이 갱신된다.

## API · 브라우저 QA

- [x] `/series`, `/analysis`, `/aggregate`의 status와 JSON schema가 유지된다.
- [x] Upstream Detail의 Quota History 선, latest, deficit, loading/error/empty state가 정상이다.
- [x] Mock API browser에서 1h/6h/24h/7d 버튼별 exact request range와 정상 chart render를 확인한다.
- [x] SQLite/PostgreSQL storage·API transition과 browser-layer series 30초/analysis 120초 cadence가 각각 통과한다.
- [x] Virtual clock에서 +60초에는 analysis 요청이 없고 +120초에는 최신 exact bounds로 refetch한다.
- [x] Loading 중 기존 화면, empty 전환, analysis error 중 previous data, 성공 recovery가 정상 표시된다.

## 성능 QA

- [x] Work-count assertion으로 walked bucket 수가 anchor age가 아닌 요청 범위에 제한됨을 증명한다.
- [x] SQLite scratch에서 anchor 1일/30일/180일 p50·p99 및 RSS before/after를 측정한다.
- [x] PostgreSQL scratch에서 같은 데이터의 p50·p99와 buffers를 측정한다.
- [x] `/analysis` filtered query의 rows와 buffers가 전체 rollup 수가 아닌 선택 upstream에 비례한다.
- [x] 절대시간 임계값은 CI 영구 테스트로 고정하지 않고 측정 보고서에 기록한다.

## 실행 결과

- Series 반환 작업량: 180일 anchor, 6 windows에서 `1,555,566` buckets → `366`.
- SQLite series p50/p99: `121.437/129.915ms` → `0.163/0.193ms`.
- PostgreSQL series p50/p99: `133.119/139.993ms` → `0.474/0.692ms`.
- SQLite provider-lots p50/p99: `3.647/3.856ms` → `0.091/0.109ms`.
- PostgreSQL provider-lots p50/p99: `3.114/3.395ms` → `0.218/0.289ms`.
- SQLite peak RSS: `560,758,784` bytes → `13,778,944` bytes.
- PostgreSQL analysis scratch: 전체 upstream `100,000 rows / 29.103ms / 101,087 shared hits` →
  선택 upstream `1,000 rows / 0.937ms / 2,006 shared hits`.
- Storage conformance: SQLite `79/79`, PostgreSQL `64/64`.
- Admin: unit `19/19`, focused integration `15/15`, 전체 `52 + 246` tests 통과.
- Web: typecheck, Biome lint, Vitest `63 files / 573 tests` 통과.
- Playwright: `upstream-quota-analysis-cadence.spec.ts` 6개 browser test 통과.
  1h/6h/24h/7d exact range, delayed loading, empty 전환, analysis error 중 previous data,
  성공 recovery, +60초 series-only refresh, +120초 최신 exact-bounds analysis refresh와
  deficit/caveat DOM 변경을 검증했다. Browser는 mock API를 사용하며 실제 SQLite/PostgreSQL
  storage·API state transition은 별도 Rust conformance/integration tests가 검증한다.
- 독립 리뷰: range와 analysis 두 리뷰 모두 finding 0건.
- GitHub Actions: rebased 기능 head `66be8150fc6c`에서 CI, Web, Publish-check 전부 통과.
  Remote sccache backend 500은 uncached fallback으로, inherited wall-clock heartbeat
  test race는 결정론적 rendezvous로 수정한 상태에서 검증했다.

## 종료 게이트

- [x] 관련 Rust unit/integration/conformance 테스트 통과
- [x] SQLite QA 통과
- [x] PostgreSQL QA 통과
- [x] Web typecheck/test 통과
- [x] Mock API 실제 브라우저 point/state transition과 양 backend storage/API transition을 계층별로 통과
- [x] 독립 코드 리뷰 finding 0건
- [x] GitHub CI 전체 통과
