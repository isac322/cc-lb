# Quota · Principal usage 읽기 경로 확장성 조사

조사일: 2026-09-07

## 범위

Admin UI의 다음 두 읽기 경로를 조사했다.

1. Subscription quota series, analysis, provider-lots aggregate
2. Overview principal usage totals

Prompt-cache tokenization과 관련 관측성은 별도 작업에서 처리하므로 이 문서의 범위가 아니다.

## 조사 방법

6개 독립 조사 에이전트가 서버, 프론트엔드, PostgreSQL, SQLite, 성능 측정, 반례를 나누어 분석했다. 2개 비판 리뷰어가 각 결론을 반박·교차 검토했고 최종 판정 에이전트가 구현 범위와 QA 계약을 확정했다. Scratch SQLite/PostgreSQL benchmark로 핵심 복잡도와 plan shape를 재현했다.

## Quota 결론

운영 `/series`의 442~509ms를 단순 DB 또는 JSON 병목으로 보는 진단은 기각했다. 네트워크/API baseline은 210~230ms이고 해당 DB plan은 12.253ms, 7,786 rows, temp I/O 0이었다.

실제 결함은 series builder가 요청 `since`가 아니라 DB left anchor의 `changed_at`부터 bucket을 생성한다는 점이다. Checkpoint는 장기간 남지만 guardrail은 요청 범위만 검사하므로 작업량이 `O((until-anchor)/bucket_secs)`로 증가한다. Carry-forward가 모든 중간 bucket을 생성하고 downsampling이 요청 범위 이전 데이터까지 대상으로 삼아 visible range 해상도를 잃는다. 같은 문제가 admin/public PostgreSQL·SQLite series와 provider-lots aggregate에 있다.

Scratch public SQLite path에서 최근 1시간/60초/6 windows 요청은 정상 366 buckets여야 하지만 anchor가 1일 전이면 8,646 buckets/4.112ms, 30일 전이면 60,000/133.829ms, 180일 전이면 60,000/749.784ms를 처리했다. Provider-lots 5 windows는 같은 조건에서 0.928/14.318/80.362ms였다.

`/analysis`는 선택 upstream 조건을 SQL에 넣지 않고 모든 upstream/principal/model minute rollup을 읽은 뒤 Rust에서 필터한다. 6개 window가 전체 vector를 반복 스캔하고 matching row를 clone하며, dense bucket을 만든 뒤 `sample_count=0`인 대부분을 버린다. Frontend는 60초마다 바뀌는 timestamp를 query key에 넣어 명목상 120초 poll인 analysis를 실제 60초마다 다시 요청한다.

### 결정

- 각 source stream의 left anchor를 current-state seed로 보존한다.
- 실제 bucket loop는 `floor(since / bucket_secs)`에서 시작한다.
- 요청 범위 전체의 dense carry-forward, reset/gap/source merge, exact anchor timestamp/source/value 계약을 유지한다.
- `/analysis` 전용 upstream-filtered rollup storage API를 추가하고 upstream별로 한 번만 partition해 borrow한다.
- Series와 analysis query identity는 stable `range_secs`를 사용하고, 각 refetch가 시작될 때 최신 exact `since/until`을 계산한다.
- 신규 series 인덱스, 단순 ETag, endpoint mega-merge, response field 축소는 근본 해결책으로 사용하지 않는다.

## Principal usage 결론

PR #698/#700은 payload/TOAST JSON decode를 제거했지만, principal-cost SQL의 `($4::uuid IS NULL OR upstream_id = $4)` optional predicate가 covering index에 없는 `upstream_id`를 참조한다. Custom plan은 `None`을 constant-fold할 수 있지만 prepared generic plan은 heap 접근을 피할 수 없다. `Some` 경로는 모든 plan에서 현재 index-only가 불가능하다.

PostgreSQL 18 scratch 100,000 rows에서 auto generic optional predicate는 Bitmap Heap Scan, 1,640 heap blocks, 37.45ms였고 predicate 없는 정적 shape는 Index Only Scan, heap fetch 0, 11.93ms였다. 절대시간은 운영 수치가 아니지만 generic-plan 전환에 따른 index-only 붕괴는 재현됐다.

`projection=totals`는 응답만 줄인다. 서버는 full rollups, dense principal bucket, bucketed live-event cost를 모두 계산한 뒤 마지막에 collapse한다. Overview 5초 poll은 브라우저당 시간당 720회의 동일 범위 집계를 만들며 cache/coalescing이 없다. Rollup checkpoint ETag는 live tail을 반영하지 않아 사용하지 않는다.

### 결정

- PostgreSQL과 SQLite 모두 upstream filter 유무에 따라 정적 SQL shape을 선택한다.
- Canonical UUID exact branch는 UUID 선택 집합이 있을 때만 실행한다.
- Normalization branch는 선택 key 전체를 대상으로 정확히 한 번 실행해 padded UUID·legacy·unknown을 처리한다.
- PostgreSQL no-upstream generic plan이 covering index-only 형태를 유지하도록 회귀시킨다.
- Filtered path는 upstream-leading covering index로 payload/TOAST 없이 처리한다.
- `projection=totals`는 step별 live-tail guardrail용 sparse bucket만 만들고 zero-filled dense series는 만들지 않는다.
- 동일 principal totals 요청은 in-flight single-flight와 짧은 freshness-safe microcache로 병합한다. ETag는 계속 비활성화한다.

## 완료 조건

각 문제는 독립 PR로 제출한다. 각 PR은 PostgreSQL과 SQLite의 point-in-time 및 state-transition 계약, API 결과, 관련 브라우저 surface, before/after 측정, 독립 리뷰, GitHub CI 전체 통과를 만족해야 한다.
