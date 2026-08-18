# Production Performance Audit Evidence Appendix

이 문서는 `docs/production-performance-audit-2026-08-18.md`의 수치와 판단을 재검증하기 위한 조사 메모다. 운영 데이터는 read-only 세션으로 조회했고, 민감한 payload 원문은 열람하지 않았다.

## 1. 관측 기준

### 운영 상태

- Kubernetes context: `runbear-operation`
- namespace: `llm-proxy`
- cc-lb Deployment image: `ghcr.io/isac322/cc-lb:0.4.2`
- PostgreSQL primary: `llm-proxy-pg-cluster-1`
- PostgreSQL version: 16.15
- database: `cc_lb`
- applied migration max: `105`
- PostgreSQL user tables: 52
- PostgreSQL user indexes: 131
- database size: 약 4.77 GB
- 관측 active data span: 2026-07-23 12:35:33 UTC–2026-08-18 11:57:08 UTC

### read-only 보호

운영 PostgreSQL 확인은 다음 제한을 적용했다.

```sql
SET default_transaction_read_only = on;
SET statement_timeout = '10s';
SET lock_timeout = '1s';
SET idle_in_transaction_session_timeout = '15s';
BEGIN TRANSACTION READ ONLY;
-- SELECT / EXPLAIN만 수행
ROLLBACK;
```

## 2. P0-1 Housekeeping 조사 데이터

### 관련 source

- `crates/cc-lb-scheduler/src/jobs/apalis_housekeeping.rs`
- `crates/cc-lb-scheduler/src/jobs/apalis_housekeeping/tests.rs`
- `crates/cc-lb-storage-postgres/src/adapter/apalis_housekeeping.rs`
- `crates/cc-lb-scheduler/src/worker/cron.rs`
- scheduler metrics/job outcome mapper

### 운영 관측

- `apalis_housekeeping` task는 2026-07-24 이후 연속 실패 상태.
- jobs table에서 housekeeping 관련 반복 실행과 retry 상태가 확인됨.
- 최근 약 30시간 로그에서 FK 오류가 30회 확인됨.
- 실패가 반복되지만 job outcome이 다음 실행을 막지 않아 같은 오류가 scheduler tick마다 재발함.

### 실패 경로

```text
cron tick
  -> singleton apalis job enqueue
  -> apalis_housekeeping handler
  -> stale-lock/cache-session cleanup
  -> stale worker lock_by NULL + worker DELETE transaction
  -> cache job cleanup
  -> next cron tick observes completed cleanup
```

수정 전에는 worker DELETE가 FK 참조 해제보다 먼저 실행됐고, cache job filter가 production queue와 달랐다. 또한 PostgreSQL stale-lock SELECT의 `attempts` decode가 `int4` column과 맞지 않았다.

### 원인별 증거

#### A. FK 참조 해제

`apalis.jobs.lock_by`가 worker를 참조한다. worker 보존 기간은 1시간이고 terminal job 보존 기간은 기본 30일이므로, DELETE 순서만 바꾸면 오래된 worker가 여전히 terminal job에 참조될 수 있다. 수정은 stale worker를 참조하는 jobs의 `lock_by`를 NULL로 바꾼 뒤 worker DELETE를 같은 transaction으로 실행해야 한다.

#### B. cache-keepalive job type mismatch

`CACHE_KEEPALIVE_QUEUE`는 `crates/cc-lb-scheduler/src/worker/mod.rs:37`에서 `"cache_keepalive"`다. PostgreSQL enqueue 함수는 전달받은 `queue`를 `apalis.jobs.job_type`에 bind한다 (`crates/cc-lb-scheduler/src/worker/backend_api.rs:248-254`). 수정 전 housekeeping query는 `job_type = 'adaptive'`로 고정되어 production `cache_keepalive` job을 삭제하지 못했다. fixture도 이 값을 직접 삽입해 mismatch를 가렸다.

#### C. attempts decode

운영 schema의 `apalis.jobs.attempts`는 `integer`/`int4`다. 수정 전 housekeeping stale-lock SELECT는 tuple 마지막 값을 `i64`/`int8`로 매핑했다. SQLx PostgreSQL type compatibility가 strict equality이므로, stale non-keepalive Running row가 반환되면 decode error가 발생한다. Admin failure-listing의 `i32` decode는 별도 경로이며 올바르다.

### 왜 P0인가

- 실패가 일회성이 아니라 매 tick 재현된다.
- 기존 worker DELETE 실패 시 뒤의 cleanup이 도달하지 못했다.
- `cache_keepalive_sessions`와 `apalis.jobs`가 실제로 계속 커졌다.
- 운영 디스크와 autovacuum에 직접 압력을 준다.
### 로컬 production-equivalent repair 측정

Apalis PostgreSQL `1.0.0-rc.8` migration 전체를 적용한 격리된 PostgreSQL 18 database에 다음 상태를 재현했다.

- 2시간 전에 heartbeat가 멈춘 `worker-dead`
- 해당 worker를 `lock_by`로 참조하는 2일 지난 terminal job 1개
- 만료된 `cache_keepalive` job 2개

수정 전 worker DELETE는 FK 오류로 실패했다.

```text
ERROR: update or delete on table "workers" violates foreign key constraint "fk_worker_lock_by" on table "jobs"
```

수정 경로는 `UPDATE lock_by = NULL` 1건, worker DELETE 1건, cache job DELETE 2건을 같은 측정에서 수행했고, 종료 상태는 `stale_worker_refs = 0`, `remaining_cache_jobs = 0`이었다. 이 측정은 production database에 쓰지 않은 로컬 reproduction이며, PostgreSQL regression test는 동일한 FK, queue, `attempts` 경로를 자동화한다.

## 3. P0-2 `cache_keepalive_sessions` 조사 데이터

### relation 통계

| metric | observed |
|---|---:|
| live tuples | 1,028 |
| heap | 728 KB |
| index | 18 MB |
| TOAST | 909 MB |
| total relation | 약 913 MB |
| updates | 66,744 |
| HOT updates | 18,197 |
| HOT ratio | 약 27% |
|

### 저장 구조상 문제

한 row 안에 다음 종류의 값이 함께 존재한다.

- 큰 immutable request/session payload
- heartbeat 또는 lease 시각
- mutable status
- keepalive decision/state

작은 mutable 값만 바뀌어도 대형 TOAST row version이 새로 생성될 수 있다. 변경 column이 index에 포함되면 HOT update도 사용할 수 없다.

### P0-1과의 인과관계

P0-1 실패는 만료 session 정리를 지연시키는 보조 요인이다. 그러나 관측된 48,547회의 non-HOT update가 약 19 KB payload를 반복 기록한 측정만으로도 909 MB TOAST를 설명할 수 있다. 따라서 P0-1 수정과 TOAST repack/rebuild는 별도 측정 대상이며, P0-1 수정 후 TOAST가 자동으로 크게 줄어든다고 가정하지 않는다.

### 판별 방법

P0-1 fix 이후 다음을 비교한다.

1. 만료 row 수와 실제 삭제 row 수
2. `n_dead_tup`, autovacuum count, vacuum timestamp
3. heap/TOAST/index size
4. update rate와 HOT ratio
5. checkpoint/WAL 증가량

cleanup 후에도 TOAST가 유지되면 기존 bloat에 대한 repack/rebuild가 필요하다. update rate가 다시 높으면 row 분리 없이는 재발한다.

## 4. Admin API 측정

정상 사용자 cadence로 주요 endpoint를 3회씩 측정했다. 수치는 network/port-forward baseline을 포함한다.

| 화면/endpoint | 관측 |
|---|---:|
| Overview 15m | 약 284 ms |
| Overview 1h | 약 836 ms |
| Overview 24h | 약 881 ms |
| Overview 7d | 약 1,029 ms |
| Overview 30d | 약 1,080 ms |
| Request Logs list 24h | 약 305.6 ms |
| Request Logs upstream filter | 약 201.1 ms |
| Request Logs model filter | 약 195 ms |
| Request Logs next cursor | 약 310 ms |
| Request Logs principal filter | 약 8.5 ms |
| Request Logs histogram 24h | 약 420.1 ms |
| usage rollup 24h | 약 84.9 ms |
| pool quota history 24h | 약 80.7 ms |
| excluded error rollup | 약 2.1 ms |
| token interval | 약 3.4 ms |
| request event point lookup | 약 0.065 ms |

## 5. PostgreSQL query evidence

### Principal cost enrichment

- 약 20,821 rows를 읽음
- `btrim`/regex normalization 수행
- 1,063.7 ms
- 17,936 shared buffer hits
- 1,047 shared buffer reads
- canonical UUID도 legacy normalization branch를 거침
- v0.4.3 migration 106에 canonical range index와 bounded legacy fallback이 포함됨

### Request Logs pagination

현재 filter와 ordering에 서로 다른 index 경로가 사용된다.

```text
filter: ts / upstream / model / principal
order:  list_ts_ms DESC, list_event_key DESC
```

그 결과 넓은 기간 rows를 먼저 읽고 top-N sort한다. principal exact filter는 선택도가 높아 약 8.5 ms지만 기간 전체 조회와 대부분의 일반 필터는 200–310 ms다.

### Histogram

24h histogram은 raw request events 약 20k rows를 읽고 다음을 수행한다.

- BitmapAnd
- heap fetch
- bucket grouping
- sort
- response serialization

24h 실행시간은 약 420 ms다. rollup 기반 query는 약 84.9 ms다.

### Subscription quota anchor

- `DISTINCT ON` 결과를 위해 약 9,482 rows sort
- 실행시간 약 105.4 ms
- query는 최신 `changed_at_unix_millis DESC`를 요구하지만 기존 index 방향이 일치하지 않는다.

## 6. Storage/index evidence

### 대형 relation

| relation | observed size / signal |
|---|---|
| `request_events_v1` | 약 3.56 GB, 약 137 MB/day 증가 |
| `cache_keepalive_sessions` | 약 913 MB, TOAST 약 909 MB |
| quota checkpoints | 약 79 MB |
| usage rollups | 약 55 MB |
| pool quota history | 약 33 MB |
| keepalive decisions | 약 32 MB |
| `apalis.jobs` | 약 106–107 MB, 약 182k rows |

### 인덱스 후보

`request_events_v1` indexes는 약 522 MB다. scan count가 매우 낮거나 0인 후보가 약 188 MB다.

- `v3_cache_key_ts`: 42 MB, 0 scans
- `cache_state_ts`: 17 MB, 0 scans
- `thread_ts`: 29 MB, 3 scans
- `thread_list_order_idx`: 56 MB, 7 scans
- `source_kind_list_order_idx`: 44 MB, 20 scans

중복 후보:

- quota checkpoint range index와 PK
- `apalis.jobs(id)` 중복 3개
- `apalis.workers(id)` 중복 3개
- UNIQUE column과 일반 index 중복: `wasm_registry_v2(name)`, `upstream_spec_v1(name)`

통계 reset 시점을 알 수 없으므로 scan count만으로 즉시 삭제하지 않는다.

## 7. HOT/autovacuum evidence

### `api_key_usage_writers_v1`

- live rows: 3
- updates: 1,582,470
- HOT updates: 317
- HOT ratio: 약 0.02%
- autovacuum: 26,363
- seq scans: 3,165,030

### `upstream_subscription_quota_latest_v1`

- live rows: 120
- updates: 1,690,135
- HOT updates: 364
- HOT ratio: 약 0.02%
- autovacuum: 14,434

### `prompt_cache_observations`

- 약 346k insert/delete
- frequent analyze/vacuum
- index가 heap보다 약 15배 큼

## 8. Runtime evidence

### cc-lb pod

- CPU: 약 70–114m
- memory: 약 120–134 MiB
- restart: 0
- OOM/eviction/probe failure: 0

### PostgreSQL pod

- CPU limit: 1.5 cores
- 평균 CPU: 약 580–694m
- throttle periods: 24.52%
- cumulative throttle time: 약 5,602.8초
- real-time throttle sample: 8.7–13.2%

### Connection/lock/replication

- connections: 29/100
- idle-in-transaction: 0
- waiting locks: 0
- buffer hit: 97.98%
- replication lag/error: 관측되지 않음
- checkpoint sync 평균 46 ms, 최대 241 ms

## 9. Scheduler/frontend evidence

### Scheduler

- housekeeping 반복 실패
- OAuth refresh idempotency conflict 약 1,950회/32.5h
- pool quota history delete API는 있으나 scheduler 호출 경로가 확인되지 않음

### Frontend

Overview 동시 query/stream:

- summary 5초
- principal usage 5초
- upstreams 30초
- quota aggregate 30초
- pool history 30초
- SSE
- recent events/infinite query

추가 관측:

- hidden tab에서도 120초 grace 동안 polling/SSE 유지
- quota query key에 `nowUnixSecs`가 포함되어 60초마다 cache key churn
- SSE event마다 최대 200-row `RequestEventsTable` 전체 re-render
- Principal detail에서 plugin-chain slot 3개를 header count용으로 조회한 뒤 card에서 재조회
- list endpoint가 full BYTEA payload를 fetch하고 Rust에서 JSON parse
- per-key usage가 historical JSON payload를 scan할 가능성

## 10. 조사 한계와 재검증 명령 계획

현재 확인하지 못한 항목:

- `pg_stat_statements` per-query cumulative history
- 실제 autovacuum/temp/slow-query 발생률
- concurrent multi-user load
- write path contention
- 민감 payload 내부 분포

P0 fix 후 재검증할 항목:

```text
1. housekeeping success ratio and 20 consecutive runs
2. expired cache sessions/jobs deleted count
3. cache_keepalive_sessions heap/index/TOAST size
4. n_dead_tup and autovacuum timestamps
5. apalis.jobs row count and stale lock count
6. request_events principal cost EXPLAIN ANALYZE
7. request cursor EXPLAIN ANALYZE
8. PostgreSQL CPU throttle delta
9. frontend request count per minute
```

문서의 모든 수치는 2026-08-18 당시 운영 v0.4.2/migration 105 관측값이다. 로그 필터 폭증은 별도 작업으로 처리 중이라는 사용자 결정에 따라 이 문서에서는 관측값만 보존한다.
