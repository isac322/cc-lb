# Production Performance Audit

- 조사일: 2026-08-18
- 대상: 운영 cc-lb Admin/API, PostgreSQL, Kubernetes runtime, frontend polling/query 경로
- 조사 방식: read-only 운영 조사, 정상 사용자 cadence의 endpoint 측정, source/query/migration 교차검증
- 조사 당시 배포: `ghcr.io/isac322/cc-lb:0.4.2`
- 조사 당시 PostgreSQL migration: `105`
- 범위: PostgreSQL 52개 테이블, 131개 인덱스, 약 4.77 GB relation 전체, Admin 전체 주요 화면과 API

## 1. 결론

성능 문제는 단일 병목이 아니라 다음 네 가지가 겹쳐 있었다.

1. scheduler housekeeping이 반복 실패하여 만료 데이터와 대형 TOAST가 정리되지 않는다.
2. request log와 histogram이 raw `request_events_v1`를 넓은 기간마다 다시 읽고 정렬한다.
3. 일부 집계 쿼리는 canonical UUID 인덱스를 쓰지 못하고 전체 기간을 정규화한다.
4. frontend가 같은 데이터를 짧은 주기로 중복 polling하고, SSE 이벤트마다 큰 화면을 다시 렌더링한다.

로그 필터 문제는 별도 작업에서 처리 중이라는 운영 결정에 따라 이 문서의 후속 조치 목록에서는 제외한다. 단, 조사 당시에는 TRACE/DEBUG 폭증이 실제로 관측되었다는 사실만 보존한다.

## 2. P0 우선순위

### P0-1. Housekeeping 반복 실패

#### 관측

- `apalis_housekeeping`가 2026-07-24 이후 601회 연속 실패 상태였다.
- 최근 약 30시간 PostgreSQL 로그에서 housekeeping 관련 FK 오류가 30회 관측됐다.
- 정리 대상이 쌓여 다음 공간을 사용했다.
  - `cache_keepalive_sessions`: 약 913 MB
  - `apalis.jobs`: 약 106–107 MB, 약 182,000 rows

#### 근본 원인

검증된 결함은 서로 독립적인 세 가지다.

1. **stale worker를 FK 참조 해제 전에 삭제한다.**
   - `apalis.jobs.lock_by`가 worker를 참조한다.
   - 기존 코드는 worker를 먼저 삭제하므로 참조 중인 worker에서 FK violation이 발생한다.
   - 단순히 DELETE 순서만 바꾸면 충분하지 않다. worker 보존 기간은 1시간이고 terminal job 보존 기간은 기본 30일이므로, 오래된 worker를 참조하는 terminal job이 장기간 남을 수 있다.
   - stale worker의 `lock_by` 참조를 NULL로 바꾸고 worker 삭제를 같은 transaction으로 묶어야 한다.

2. **cache-keepalive cleanup query가 실제 job type과 불일치한다.**
   - `CACHE_KEEPALIVE_QUEUE`는 `crates/cc-lb-scheduler/src/worker/mod.rs:37`에서 `"cache_keepalive"`다.
   - PostgreSQL enqueue는 `queue` 인자를 `apalis.jobs.job_type`에 bind한다 (`crates/cc-lb-scheduler/src/worker/backend_api.rs:248-254`).
   - 그러나 housekeeping cleanup은 `job_type = 'adaptive'`로 필터링했다.
   - 따라서 production enqueue 경로의 `cache_keepalive` job이 cleanup에서 누락됐다.

3. **PostgreSQL `attempts` decoder가 `int4` schema와 불일치한다.**
   - 운영 schema의 `apalis.jobs.attempts`는 `integer`/`int4`다.
   - housekeeping stale-lock query는 이를 `i64`/`int8`로 decode한다.
   - SQLx의 PostgreSQL type compatibility는 strict equality이므로, stale non-keepalive Running row가 처음 반환되는 순간 decode error가 발생한다.

기존 housekeeping fixture는 production queue와 stale-worker FK 참조를 충분히 재현하지 않았고, 이 때문에 세 결함이 모두 테스트에서 가려졌다.

#### 실패가 누적되는 이유

기존 구현은 모든 cleanup statement를 하나의 transaction으로 감싸지 않는다. `reap_stale_locks`만 별도 transaction을 사용한다. 따라서 "전체 housekeeping이 단일 transaction으로 rollback된다"는 설명은 정확하지 않다. 다만 기존 worker DELETE가 첫 단계에서 실패했기 때문에, 그 실행에서는 뒤의 cleanup이 도달하지 못했고 같은 stale 상태가 반복됐다. 수정 구현은 stale job의 worker 참조 해제와 worker DELETE를 하나의 transaction으로 묶는다.

#### 영향

- 만료된 cache keepalive session이 계속 남는다.
- `apalis.jobs`의 cache keepalive row가 계속 증가한다.
- stale worker/job lock이 남는다.
- PostgreSQL stale non-keepalive lock이 남아 housekeeping을 다시 실패시킬 수 있다.
- 반복 update와 vacuum 지연으로 TOAST/WAL 부담이 커진다.

#### 수정 방향

1. stale lock 및 stale cache-keepalive job을 먼저 처리한다.
2. stale worker를 참조하는 job의 `lock_by`를 NULL로 바꾼 뒤 worker DELETE를 transaction으로 실행한다.
3. housekeeping cleanup filter를 실제 `CACHE_KEEPALIVE_QUEUE` 값과 통일한다.
4. PostgreSQL `attempts` decode를 `i32`로 맞춘다.
5. SQLite/PostgreSQL test fixture가 production queue와 stale-worker FK 참조를 재현하도록 한다.
6. 운영에서 read-only dry-run으로 삭제 대상 수와 FK 참조 수를 확인한다.
7. fix 후 scheduler 실행을 retry 없이 관찰한다.

### P0-2. `cache_keepalive_sessions` TOAST 팽창

#### 관측

| 항목 | 값 |
|---|---:|
| live rows | 1,028 |
| heap | 728 KB |
| index | 18 MB |
| TOAST | 909 MB |
| total | 약 913 MB |
| updates | 66,744 |
| HOT updates | 18,197 (약 27%) |

약 1,028개의 live row가 913 MB를 사용하는 비정상적인 상태다. 대부분은 본문 payload가 아니라 TOAST version/bloat와 반복 update의 결과로 해석된다.

#### 근본 원인

이 문제는 housekeeping 실패의 단순한 결과로만 설명되지 않는다.

1. **P0-1은 만료 row 정리를 지연시킨다.**
   - 만료 session row와 dead tuple이 즉시 정리되지 않는다.
   - 이 항은 cleanup 실패가 bloat를 누적시키는 보조 요인이라는 뜻이지, 전체 TOAST 크기의 단일 원인이라는 뜻은 아니다.

2. **큰 immutable payload와 mutable heartbeat/status가 같은 row에 있다.**
   - heartbeat, 상태, lease 같은 작은 값이 바뀔 때도 대형 row version이 새로 기록된다.
   - TOAST 값은 update마다 새 tuple을 만들 수 있다.
   - 변경되는 필드가 index에 포함되면 HOT update도 제한된다.

#### 왜 913 MB가 되는가

관측된 48,547회의 non-HOT update가 약 19 KB payload를 반복 기록했다는 측정은 909 MB TOAST를 설명할 수 있다. 따라서 P0-1 cleanup을 고쳐도 TOAST가 즉시 크게 줄어든다고 가정하면 안 된다. cleanup 후에도 크기가 유지되면 기존 bloat를 repack/rebuild해야 하며, update 패턴이 유지되면 row 분리 없이는 재발한다.

#### 영향

- 디스크 사용량 증가
- TOAST read/write I/O 증가
- update 시 WAL 및 index write 증가
- autovacuum 부담
- cache keepalive worker의 latency 증가
- 장기적으로 PostgreSQL disk pressure 위험

#### 수정 순서

P0-1 수정과 TOAST maintenance는 독립적으로 측정한다. P0-1이 해결되기 전에도 read-only 통계 수집과 maintenance 계획 수립은 가능하다.

1. housekeeping fix 배포
2. 만료 session/job/lock의 실제 삭제량 확인
3. `pg_stat_user_tables`에서 dead tuple과 autovacuum 동작 확인
4. TOAST/heap/index size와 update/HOT 비율 재측정
5. TOAST가 유지되면 maintenance window에서 `pg_repack` 또는 table rebuild
6. immutable payload와 mutable heartbeat/status 분리 검토
7. payload를 별도 table/object로 이동할지 비용 비교

#### P0-1과의 관계

- P0-1: **정리 경로의 FK 참조 해제, job type filter, PostgreSQL decoder 결함**
- P0-2: **반복 update가 주도하는 TOAST/WAL bloat와 cleanup 지연의 보조 효과**

P0-2는 P0-1의 후속 cleanup으로 끝나지 않는다. P0-1 수정 후에도 TOAST가 909 MB 근처에 남을 수 있으며, 그 경우 별도 repack/rebuild가 필요하다.

### P0-3. Housekeeping `attempts` decoder mismatch

운영 schema의 실제 타입은 다음과 같다.

```text
information_schema.columns:
apalis.jobs.attempts = integer / int4
```

housekeeping은 이 column을 `i64`로 decode한다 (`crates/cc-lb-scheduler/src/jobs/apalis_housekeeping.rs:99-105`, `:431-443`). Admin failure listing은 `i32`로 decode한다 (`crates/cc-lb-scheduler/src/admin.rs:276-283`).

이 문제는 현재 worker DELETE FK 오류보다 뒤에서 발생할 수 있어 운영 로그의 주된 FK trigger로 단정할 수 없다. 그러나 FK 순서 문제를 고치면 housekeeping이 stale-lock SELECT까지 진행하고 이 mismatch가 실제 runtime error가 될 수 있다. 별도 regression test와 production-equivalent Postgres decode test가 필요하다.
## 3. P1 우선순위

### P1-1. 운영이 아직 v0.4.2라 query/index fix가 미적용

조사 당시 production은 image `0.4.2`, migration `105`였다. v0.4.3에 다음 개선이 포함되어 있지만 rollout 전에는 운영에 반영되지 않는다.

- Top Principal cost enrichment의 canonical UUID range index
- legacy non-UUID principal fallback의 bounded partial index
- request-event cursor의 snapshot visibility horizon 아래 backward primary-key seek

### P1-2. Top Principal cost enrichment이 약 1초

- 운영 측정: 약 1,063.7 ms
- 약 20,821 rows에 `btrim`/regex 정규화 수행
- buffer: 17,936 hits + 1,047 reads

canonical UUID principal도 legacy normalization 경로를 통과해 넓은 기간을 읽는 것이 원인이었다. v0.4.3 migration 106 적용 후 query plan을 재확인해야 한다.

### P1-3. Request Logs pagination이 200–310 ms

- 24h unfiltered: 약 305.6 ms
- upstream filter: 약 201.1 ms
- model filter: 약 195 ms
- cursor next page: 약 310 ms
- principal filter: 약 8.5 ms

filter는 `ts` index를 사용하지만 ordering은 `(list_ts_ms DESC, list_event_key DESC)`다. filter/order 경로가 달라 넓은 기간 rows를 읽고 top-N sort한다.

### P1-4. 24h histogram이 약 420 ms

raw `request_events_v1` 약 20k rows를 읽고 BitmapAnd/heap fetch/sort한 뒤 histogram을 만든다. 24h 이상은 `usage_rollups_v2` 또는 전용 histogram aggregate로 전환해야 한다.

### P1-5. Overview 장기 범위가 0.84–1.08초

| 범위 | 평균 |
|---|---:|
| 15m | 284 ms |
| 1h | 836 ms |
| 24h | 881 ms |
| 7d | 1,029 ms |
| 30d | 1,080 ms |

port-forward 왕복 baseline이 약 270–300 ms이지만 7d/30d에서 추가 DB 비용이 남는다. ETag와 server-side short TTL cache도 없다.

### P1-6. PostgreSQL CPU CFS throttling

- PostgreSQL CPU limit: 1.5 cores
- 평균 사용량: 약 580–694m
- lifetime throttled periods: 24.52%
- 누적 throttle time: 약 5,602.8초
- 실시간 sample: 8.7–13.2%

평균 CPU가 낮아도 query burst가 limit에 걸린다. query fix 후에도 지속되면 CPU limit 상향을 검토한다.

### P1-7. OAuth refresh scheduler unique violation

- 최근 약 32.5시간: 1,950회
- 매분 같은 idempotency key 재삽입
- `idx_jobs_idempotency_key` conflict

`ON CONFLICT DO NOTHING` 또는 기존 pending/active job 확인이 필요하다.

### P1-8. 중복·저사용 인덱스

확정 중복 후보:

- quota checkpoint range index와 PK
- `apalis.jobs(id)` 3중 중복
- `apalis.workers(id)` 3중 중복
- `wasm_registry_v2(name)` UNIQUE와 일반 index
- `upstream_spec_v1(name)` UNIQUE와 일반 index

`request_events_v1`에서 scan count가 거의 없는 index도 약 188 MB다. v0.4.3 rollout 후 통계와 source call path를 확인하고 `DROP INDEX CONCURRENTLY`를 검토한다.

## 4. P2 우선순위

- `api_key_usage_writers_v1`: 3 rows, 1,582,470 updates, HOT 0.02%
- `upstream_subscription_quota_latest_v1`: 120 rows, 1,690,135 updates, HOT 0.02%
- `prompt_cache_observations`: 약 346k insert/delete와 frequent autovacuum
- subscription quota anchor: 9,482 rows sort, 약 105.4 ms
- frontend Overview 5초 polling과 Upstream 최대 12개 concurrent query
- hidden tab에서도 120초 동안 polling/SSE 지속
- SSE마다 최대 200 rows table 전체 re-render
- Principal detail의 plugin-chain triple query
- request-event list가 full BYTEA payload를 과다 fetch/parse
- per-key usage가 historical JSON payload를 scan할 가능성
- usage rollup writer의 sequential UPSERT
- `request_events_v1` 약 137 MB/day 증가, 약 50 GB/year 추정
- pool quota history retention 호출 누락 가능성
- `work_mem=4MB`, 누적 temp I/O 약 79.5 GB

## 5. 정상으로 확인된 부분

- cc-lb CPU 70–114m
- cc-lb memory 120–134 MiB
- restart/OOM/probe failure 0
- PostgreSQL connections 29/100
- idle-in-transaction 0
- waiting lock 0
- buffer hit ratio 97.98%
- replication과 checkpoint 정상
- event point lookup 약 0.065 ms
- usage rollup 24h 약 84.9 ms
- pool history 24h 약 80.7 ms
- excluded error rollup 약 2.1 ms
- token interval query 약 3.4 ms

## 6. 운영 처리 순서

1. P0-1 housekeeping의 FK 순서와 job type mismatch 수정 및 운영에서 20회 이상 관찰
2. 만료 session/job 정리 후 TOAST/heap/index 재측정
3. 필요할 때만 repack/rebuild
4. v0.4.3 rollout 및 migration 106 적용
5. Top Principal/request cursor query plan 재확인
6. Request Logs list/histogram query 수정
7. OAuth idempotency conflict 수정
8. 중복 인덱스와 low-scan index 정리
9. frontend polling/ETag/virtualization 개선
10. retention, work_mem, CPU limit을 새 query metric 기준으로 재평가

## 7. 조사 한계

- `pg_stat_statements` 미설치
- slow statement, autovacuum, temp, lock logging 비활성
- 운영 안전상 mutation/write 부하 테스트는 수행하지 않음
- 단일 정상 사용자 cadence 중심 측정
- 민감한 request payload 원문은 열람하지 않음
- 본 보고서는 2026-08-18 당시 v0.4.2/migration 105 관측값을 기준으로 함
- 로그 필터 문제는 별도 작업으로 처리 중이라는 사용자 결정에 따라 후속 조치 범위에서 제외함
