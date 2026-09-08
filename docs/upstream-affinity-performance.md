# Upstream affinity 성능 개선

## 범위와 보존 계약

PR #717의 Web Search affinity 성능 감사에서 확인한 문제를 해결한다. 성능 리팩터링은 기능을 보존한다. 이후 사용자가 별도로 승인한 변경은 사용자 설정 TTL과 기본값 90일이다. 이 보존 정책에 따른 만료·정리 외의 라우팅 기능이나 운영 데이터는 변경하지 않는다. 암호문을 저장하지 않고 principal/provider/kind/SHA-256으로 upstream을 찾는 계약은 유지한다.

- 알려진 key가 없으면 503, 서로 다른 알려진 upstream이 섞이면 400을 유지한다.
- 알려진 key와 미등록 key가 섞이면 유일한 기존 upstream으로 보내고 성공 후 미등록 key를 학습한다.
- 저장소 변경은 원자적이다. 활성 매핑의 같은 key를 다른 upstream으로 덮어쓰지 않는다. 만료된 매핑만 새로 검증된 binding으로 교체할 수 있다.
- 활성 같은-target 매핑은 observed_at 최대값과 기존 NULL/최대값 expiry 병합을 유지한다. 설정 TTL은 observed_at 기준의 별도 보존 제한이다.
- opaque 응답을 클라이언트에게 전달하기 전에 매핑 저장을 완료한다. 비동기 fire-and-forget으로 바꾸지 않는다.
- SSE JSON type 우선 판정, multiline/CR framing, 압축, 변환, 오류 전파와 성공 시에만 학습하는 조건을 유지한다.
- 일반 SSE raw-chunk 경로를 유지한다. 불필요한 새 DB 조회를 추가하지 않는다.
- 응답 순회 최적화는 요청에 tools가 없더라도 기존 코드가 학습하던 응답을 놓쳐서는 안 된다.

## 확인한 문제와 근본 원인

| 문제 | 원인 | 개선 방향 |
| --- | --- | --- |
| N-key 쓰기가 N번 순차 await | storage API는 batch이지만 adapter가 row별 SQL로 분해 | parameter 한도 내 multi-row UPSERT, 단일 transaction, 중복 key 정규화와 전체 rollback 보존 |
| SSE 중복 JSON parse/할당 | usage와 affinity가 각각 raw event의 소유권과 파싱 책임 보유 | 이벤트 framing/JSON decoding을 공유하고 관찰자가 같은 parsed value 사용 |
| resolve O(N×M) 비교 | 요청/응답 key membership를 Vec 선형 탐색으로 검증 | borrowed hash membership으로 선형 처리 |
| 일반 buffered JSON AST 순회 | 관찰 필요성과 탐색 진입점 분리 부족 | 기능을 보존하는 응답 구조 판별/단일 순회 재사용; 단순 tools gate로 의미 축소 금지 |
| 무기한 row 증가 | 만료 조회·물리 정리 정책 없이 NULL-expiry mapping을 영속 보존 | 사용자가 승인한 configurable TTL(기본 90일), 기존 NULL 행에도 동일 정책 적용, 요청 밖의 bounded indexed purge |

## 이전 감사 기준값

다음은 이전 감사의 합성/로컬 측정이며 production latency가 아니다. 변경 후 동일 조건 비교로 갱신한다.

- 실제 #713 요청: 2,680,909 bytes, JSON node 2,320개, encrypted_content 19개/총 41,984 bytes.
- SQLite bind: 19 keys 약 0.4–2.8ms, 1,024 keys 약 9–50ms 이상(환경 차이 있음).
- PG row별 쓰기: 한 batch 19개일 때 BEGIN/COMMIT 포함 21개 순차 statement. SSE 이벤트에 나뉘면 각 bind 호출별 transaction이므로 응답 전체를 한 transaction으로 계산하지 않는다.
- SSE 추가 parse: delta 약 1.8µs/event, 1,000 events 약 1.8ms CPU와 임시 문자열 할당.
- resolve: 19-key 약 2µs, 1,024 all-hit 약 1–6.7ms; hash membership 약 0.1ms대.
- SQLite 100k rows: 복합 PK MULTI-INDEX OR, p50 1/19/200 keys = 5.4/50.4/553.9µs. 파일 약 29.4MB.

## 검증 기준

로컬 synthetic upstream과 실제 proxy lifecycle을 사용하며 paid provider/production 쓰기는 하지 않는다.

- SQLite와 PostgreSQL: empty, same-target duplicates, cross-target duplicates, chunk-boundary conflict, rollback, expiry/timestamp merge.
- Proxy POST /v1/messages: 일반 JSON/SSE, Web Search JSON/SSE, 알려진/미등록/혼합 key, 저장 실패, malformed SSE, 압축/transform, client-visible bytes 및 선택 upstream 검증.
- 해당 기존 테스트를 실행하고 불확실한 경계에만 회귀 테스트를 유지한다.
- 0/19/1024 key, SSE delta-heavy stream, DB statement/transaction 비용을 변경 전후 비교한다.
- 독립 리뷰에서 기능 차이를 찾으면 성능 향상보다 계약 보존을 우선한다.

## 보존 비용과 운영 관측

최초 성능 개선에서는 임의 TTL이 오래된 replay를 503으로 바꾸므로 삭제를 보류했다. 이후 사용자가 TTL 설정 기능과 기본값 90일을 승인했다. 이에 따라 오래된 매핑의 만료와 물리 정리를 보존 정책으로 추가한다. 유입량 자체의 상한을 보장하는 용량 제한은 아니며, 현재 정책 범위 안의 매핑은 계속 보존한다.

### 승인된 TTL 구현 계약

```toml
[upstream_affinity]
ttl_days = 90
```

- 기본값은 90일이다. 양수 일 수를 지정하며 0은 허용하지 않는다.
- 환경변수는 `CC_LB_UPSTREAM_AFFINITY__TTL_DAYS`다.
- 변경은 서버 재시작 후 적용한다. 엔진의 조회 정책과 background scheduler의 정리 정책이 같은 startup 설정을 사용한다.
- TTL의 기준은 마지막 성공한 binding의 `observed_at_unix_secs`다. 단순 조회마다 쓰기를 추가하는 sliding TTL은 아니다.
- `now >= observed_at + TTL`이면 만료다. `expires_at_unix_secs`가 별도로 있으면 그 만료 조건도 적용한다.
- 기존 `expires_at = NULL` 행도 동일한 TTL로 조회·정리한다. 대량 backfill UPDATE로 hot path를 막지 않는다.
- 설정을 늘리거나 줄이면 재시작 후 아직 저장된 모든 행에 새 기준이 적용된다. 이미 물리 삭제된 행은 복구하지 않는다.
- 만료된 history의 발급 upstream을 모르면 기존 unknown-affinity 503을 반환한다. 다른 계정으로 추측해서 보내지 않는다.
- purge가 아직 실행되지 않았더라도 만료 행은 라우팅에 사용하지 않는다. 새 성공 응답으로 재학습할 때는 만료된 충돌 행을 atomic하게 교체할 수 있다.
- 물리 정리는 요청 밖 singleton cron에서 기본 600초 간격과 최대 30초 jitter로 실행한다. 한 batch는 최대 1,000행, 한 job은 최대 1,024 batches다. 정책상 만료는 즉시 적용하며 physical purge의 실행을 기다리지 않는다.
- SQLite의 행 삭제는 페이지 재사용을 가능하게 하지만 DB 파일을 즉시 축소하지는 않는다. 요청 경로에서 VACUUM이나 용량 집계를 실행하지 않는다.

- `cc_lb_storage_operation_duration_seconds`: store/operation/status별 resolve/bind 지연. pool 대기와 transaction 대기를 포함한 호출 전체 시간이다.
- `cc_lb_storage_operation_errors_total`: 같은 operation의 오류 수. 이 값만으로 busy와 conflict를 구분하지 않는다.
- `cc_lb_upstream_affinity_batch_keys`: 비어 있지 않은 resolve/bind 호출의 입력 key 수. principal/hash/upstream별 label은 만들지 않는다.
- 별도 pool-wait 계측을 중복 추가하지 않는다. 기존 SQLx pool 사용량과 전체 호출 지연을 함께 확인한다.

row 수와 table/index 크기는 요청이나 metrics scrape 때 조회하지 않는다. 아래 읽기 전용 SQL을 운영 점검 시 별도로 실행한다. 대형 테이블의 정확한 COUNT는 비용이 있으므로 실행 주기를 트래픽과 보존량에 맞춘다.

PostgreSQL의 저비용 용량/행 수 추정:

```sql
SELECT reltuples::bigint AS estimated_rows,
       pg_relation_size(oid) AS table_bytes,
       pg_indexes_size(oid) AS index_bytes,
       pg_total_relation_size(oid) AS total_bytes
FROM pg_class
WHERE oid = 'upstream_affinity_v1'::regclass;
```

`estimated_rows`는 통계 갱신 시점에 의존한다. 정확한 row 수가 필요한 점검에만 `SELECT COUNT(*) FROM upstream_affinity_v1`을 사용한다.

SQLite에서는 `dbstat`를 지원하는 읽기 전용 연결로 table과 그 index page를 합산한다:

```sql
SELECT name, SUM(pgsize) AS bytes
FROM dbstat
WHERE name = 'upstream_affinity_v1'
   OR name IN (
       SELECT name FROM sqlite_schema
       WHERE type = 'index' AND tbl_name = 'upstream_affinity_v1'
   )
GROUP BY name;
```

`dbstat`가 없는 SQLite 빌드에서는 전체 DB의 `page_count × page_size`를 affinity 전용 크기로 오인하지 않는다. 별도 지원 도구로 오프라인 복사본을 점검한다. 보존 회수 정책을 도입하려면 provider의 만료 계약 또는 사용자가 승인한 대화 보존 한도가 먼저 필요하다.

## 구현 결과

### 저장소 쓰기 경계

- 같은 key의 중복 입력은 borrowed-key map으로 정규화한다. 다른 target 충돌을 거부하고, timestamp max/NULL-wins expiry를 유지한다.
- SQLite는 128행/898 bind 이하의 multi-row UPSERT를 실행한다(행당 7개와 TTL 정책 인자 2개). 가변 tail SQL은 statement cache에 보관하지 않고 1행/128행 형태만 재사용하여 다른 저장소 쿼리의 cache를 밀어내지 않는다.
- PostgreSQL은 고정 SQL의 `UNNEST`에 7개 typed array와 정책 인자 2개를 전달한다. 가변 `VALUES` SQL에서 발생하는 tail별 prepare/cache churn을 없앤다. 문자열/해시 데이터는 borrow하고, bounded array buffer는 chunk 간 재사용한다.
- 모든 chunk를 하나의 transaction으로 묶고 affected-row count가 기대와 다르면 전체 rollback한다. TTL 기능은 조회·쓰기·purge API에 현재 시각과 정책을 전달하며, SQLite 0078/PostgreSQL 0113 migration은 observed_at 인덱스만 추가한다. 기존 행의 만료 backfill UPDATE는 하지 않는다.
- source에서 계산한 19-key bind SQL 실행 수는 19+BEGIN/COMMIT에서 1+BEGIN/COMMIT으로 줄었다. 1,024-key는 1,024+2에서 8+2다. 이는 DB wire trace로 센 RTT가 아니며, 서로 다른 SSE 이벤트의 transaction을 합치지는 않는다.

### 공유 SSE 파싱과 입력 의미 보존

- 정상 SSE 이벤트를 `ParsedSseEvent`로 한 번 파싱하고 usage, provider-error, affinity, keepalive가 같은 값을 참조한다.
- 단일 `data:` line은 원본 slice를 직접 파싱한다. 정상 multiline만 합친 문자열을 만든다.
- 기존 관찰자의 비정상 입력 수용도 유지했다. joined JSON이 실패한 예외 경로에서는 legacy line 값을 한 번 수집하여 usage는 순서대로 병합하고, error는 마지막 값, keepalive는 첫 값을 사용한다. affinity는 이 fallback을 쓰지 않고 기존처럼 실패한다.
- Unicode 선행 공백과 뒤쪽 `event: error`도 기존 오류/회계 의미를 보존한다.
- 정상 multiline/CR-only SSE에서 usage/keepalive가 값을 놓치던 기존 파서 간 불일치는 수정했다. affinity는 변경 전부터 multiline 결합을 지원했다.

### 요청/응답 탐색

- 요청 key membership는 borrowed HashSet을 사용해 O(N×M) 비교를 기대 O(N+M)으로 바꿨다. 0-key는 할당·DB·계측 없이 즉시 반환한다.
- JSON 추출기는 composite node를 한 번 방문하며 scalar 재귀와 이미 확인한 Web Search content의 이중 방문을 없앴다.
- buffered 응답의 검사를 request `tools` flag로 끄지 않았다. 그 방식은 unsolicited 응답 학습을 깨뜨리므로 기능 보존 요구와 맞지 않는다. 필요한 검사 자체는 유지하고 순회 비용을 줄였다.

## 변경 전후 실측

기준 revision은 `0ed63d4d97fdca2d689b235b14a2a6cdbbad45ba`다. 로컬 Apple Silicon과 loopback Docker PostgreSQL 18을 사용했다. 동일 release binary에서 원본 adapter 소스를 wrapper에 그대로 포함하고 현재 adapter는 path dependency로 호출했다. before/current 실행 순서를 뒤집은 두 pass의 p50 중앙값을 아래에 기록한다. 운영 지연이나 모든 workload의 속도 향상을 보장하는 수치가 아니다.

| 측정 | 변경 전 p50 | 변경 후 p50 | 비율 |
| --- | ---: | ---: | ---: |
| SQLite 19-key 신규 쓰기 | 0.428ms | 0.206ms | 2.08× |
| SQLite 1,024-key 신규 쓰기 | 11.243ms | 2.447ms | 4.60× |
| SQLite 1,024-key 갱신 | 8.159ms | 1.997ms | 4.09× |
| SQLite 1,024-key 동시 쓰기 | 12.043ms | 3.802ms | 3.17× |
| PostgreSQL 19-key 신규 쓰기 | 11.167ms | 7.171ms | 1.56× |
| PostgreSQL 1,024-key 신규 쓰기 | 112.929ms | 16.377ms | 6.90× |
| PostgreSQL 1,024-key 갱신 | 108.241ms | 16.379ms | 6.61× |
| PostgreSQL 1,024-key 동시 쓰기 | 195.516ms | 38.277ms | 5.11× |

동시 쓰기는 4개 writer의 서로 다른 key workload다. 작은 batch에는 transaction/commit 비용이 남는다. PostgreSQL 19-key 동시 쓰기 p50은 10.861→11.200ms로 개선되지 않았다. read SQL은 변경하지 않았고, 같은 측정의 19-key resolve는 SQLite 74.416→91.459µs, PostgreSQL 574.041→700.583µs였다. 따라서 read나 작은 모든 batch가 빨라졌다고 주장하지 않는다.

빌드가 없는 상태에서 before/current 호출을 교차한 추가 5-round paired 확인의 current/before p50 비율 중앙값은 PostgreSQL 1-key 신규 쓰기 1.004, SQLite 19-key resolve 1.006, PostgreSQL 19-key resolve 1.006이었다. 앞선 큰 차이는 이 측정에서 재현되지 않았다. 계측 비용이 정확히 0이라는 주장이나 모든 환경에서 회귀가 없다는 보장은 아니다.

최종 실제 `usage_parser.rs`와 `upstream_affinity.rs`를 사용하는 CPU 실측(259 SSE events, keepalive 제외)은 19-key stream p50 370.916→152.875µs(2.43×), 1,024-key stream 1,196.583→653.042µs(1.83×)였다. 출력 checksum은 모든 workload에서 동일했다. 네트워크/DB/전체 proxy latency 측정과는 별개다.

Web Search가 없는 pre-parsed buffered 응답의 실제 extractor도 비교했다. 25KB는 7.458→7.375µs, 251KB는 74.292→75.042µs로 차이가 작았고, composite node가 많은 2.52MB 입력은 927.250→853.709µs(1.09×)였다. 검사를 없애지 않은 만큼 대폭 개선이라고 주장하지 않는다.

private Lifecycle matching만 복제한 알고리즘 microbenchmark는 1,024 keys p50 6,631.375→133.500µs였다. 이것은 실제 private 함수나 전체 HTTP 호출 측정이 아니다. 1-key에서는 0.083→0.167µs로 hash 구조 비용이 더 들며, 큰 입력의 이차 시간 문제를 없애기 위한 tradeoff다.

## 성능 리팩터링 단계 검증 (TTL 추가 전)

- 해당 단계 PostgreSQL affinity integration: 1 passed. 실제 PostgreSQL 18에서 array typing, duplicate merging, 129-row chunk 경계, 후속 chunk conflict rollback, expiry와 overflow 오류 계약을 검증했다. 이후 TTL 단계의 세 테스트 실행은 아래에 별도 기록한다.
- 엔진 전체: 464 unit + 248 integration passed, 기존 ignored 각 1개 유지. 그 뒤 추가한 strict multiline fallback regression 1개도 별도 실행해 통과했다.
- SQLite 전체: 12 unit + 34 integration passed.
- Observability 전체: 12 unit + 18 integration passed.
- 해당 4개 crate `cargo fmt --check` 및 `cargo clippy --all-targets -- -D warnings` 통과. 기본 Nix cargo에는 Clippy가 없어 이미 설치된 동일 Rust 1.97.1 toolchain을 사용했다. 새 패키지는 설치하지 않았다.
- 현재 소스로 실제 `cc-lb` server binary를 빌드하고 loopback proxy `POST /v1/messages`를 synthetic A/B upstream과 연결했다. 일반 JSON, JSON/SSE 응답 학습 및 재전송, unknown 503, mixed 400, 미등록 요청의 upstream dispatch 0회를 확인했다.
- ordinary non-Web-Search SSE 응답은 mock의 원본 bytes와 완전히 일치했다.
- SSE stream이 열린 상태에서 opaque 이벤트를 읽고 DB 매핑을 확인한 뒤, 다른 후보가 있는 상태로 즉시 replay해 원 발급 upstream으로 가는 것을 확인했다.
- 실제 `/metrics`에서 bind 4회/resolve 5회의 nonzero duration histogram과 batch histogram, 고정 store/operation/status label을 확인했다. 임시 managed key를 revoke한 뒤 동일 key 요청이 401인 것도 확인했다.
- 독립 storage/engine 리뷰의 발견 사항을 반영했다. PostgreSQL 가변 statement cache 문제를 고정 UNNEST로 해결했고, 기존 비정상 SSE의 오류·usage·keepalive 해석을 보존했다. 최종 blocking finding은 없다.

검증 중 수정한 오류도 남긴다. metric registry의 배열 길이를 갱신하지 않아 첫 컴파일이 실패했고 수정했다. PostgreSQL overflow regression은 기존 helper가 `Fatal`을 반환하는데 `InvalidInput`을 기대해 실패했으므로 테스트 기대만 기존 계약에 맞췄다. HTTP harness는 처음에 현재 signer와 다른 synthetic key를 기대했고, 다음 control은 일반 SSE에 Web Search tool을 잘못 넣어 실패했다. 실제 구현의 조건을 바꾸지 않고 harness의 인증/control 입력을 바로잡은 뒤 최종 matrix를 통과했다.

실행 증거 위치:

- `/tmp/affinity-perf-verification/`: 최종 테스트·Clippy·build 로그와 민감 정보 없는 proxy 증거.
- `/tmp/affinity-bench-results/20260908T115847Z-summary.csv`: 최종 SQLite/PostgreSQL before/current 비교.
- `/tmp/affinity-bench-results/20260908T-final-paired-noise-summary.csv`: 추가 paired 확인.
- `/tmp/affinity-engine-results/20260908T-final3-sse-summary.csv`: 최종 actual-source SSE CPU 비교.
- `/tmp/affinity-engine-results/20260908T-final3-buffered-summary.csv`: buffered extractor CPU 비교.
- `/tmp/affinity-engine-results/20260908T-final2-match-source-replica.csv`: private matcher 알고리즘 복제 실측(전체 Lifecycle 아님).

유료 provider, production DB, 배포는 사용하지 않았다. PostgreSQL은 격리된 local test schema, SQLite는 임시 파일을 사용했다. 최초 성능 리팩터링에서는 임의 삭제를 보류했고, 이후 명시적으로 승인된 TTL 기능으로 보존 기간과 background 정리를 추가했다. 삭제된 오래된 매핑을 복구하거나 입력량과 무관한 절대 디스크 용량 상한을 보장하지는 않는다.

## TTL 추가 후 최종 검증

사용자가 승인한 기본 90일 정책을 적용한 뒤 다시 실행했다. 아래 결과는 위의 TTL 추가 전 성능·검증 결과와 구분한다.

### 테스트와 독립 리뷰

| 범위 | 통과 |
| --- | ---: |
| Config unit / integration | 10 / 35 |
| Engine unit / integration | 465 / 252 |
| Scheduler unit / integration | 39 / 40 |
| Server unit / integration | 174 / 120 |
| Admin unit / integration | 53 / 257 |
| SQLite unit / integration | 12 / 36 |
| PostgreSQL affinity integration (`--ignored`, 실제 로컬 PostgreSQL 18) | 3 |

기존 ignored 항목은 유지했으며 새 bypass는 추가하지 않았다. PostgreSQL 세 테스트는 batch conflict/rollback, retention 경계와 만료 rebind, bounded purge와 동시 갱신 보호를 다룬다. 특히 별도 transaction이 갱신한 행의 lock을 유지한 상태에서 purge가 기다리지 않고 0행을 반환하고, commit 후 새 binding이 보존되는 것을 검증했다.

Config schema를 재생성하고 freshness 검사를 통과했다. 관련 8개 crate에 대해 Clippy `--all-targets -- -D warnings`를 통과했다. 통합 중 기존 server test에서 누락된 `serde_json::Value` import가 컴파일 오류로 발견되어 복구했다.

독립 storage/runtime 리뷰에서 blocking correctness finding은 없었다. 실측에서는 PostgreSQL의 bounded 후보 CTE 뒤 `DELETE ... USING`이 대상 테이블을 순차 스캔할 수 있음을 추가로 발견했다. 이를 동일 statement 안에서 잠근 `ctid` 배열에 대한 DELETE로 바꿨다. 물리 TID를 영속 저장하지 않으며, `FOR UPDATE SKIP LOCKED`와 외부 만료 조건 재검사를 유지한다. 최종 실행 계획은 두 만료 경로 모두 후보 `Limit → LockRows → Index Scan`, 삭제 대상 `Tid Scan`이었다.

추가로 두 purge 문을 full 1,000 / near-empty / empty 후보와 custom / generic prepared plan 조합 12개로 확인했다. 모든 조합에서 후보는 인덱스와 `Limit/LockRows`를 사용했고, 후보·삭제 대상에 Seq Scan이나 Sort가 없었다. full 및 generic plan은 대상 Tid Scan을, near-empty/empty custom plan은 선택적인 해당 만료 인덱스를 사용했다. generic 검증의 GUC는 격리된 테스트 transaction 안에서만 설정하고 rollback했다.

### 실제 HTTP 및 상태 전이 QA

로컬 SQLite를 사용하는 실제 cc-lb와 synthetic A/B upstream에서 18개 HTTP 시나리오를 실행했다. 정상 control은 A를 선택하고, B에서 배운 history는 유효 기간 안에서 B로 유지됐다.

| Given / When | Then |
| --- | --- |
| 기본값 생략, legacy NULL-expiry 행이 89일 전 관찰됨 | HTTP 200, 원 발급 B 선택 |
| 90일 경계 이상 또는 explicit expiry 경과 | 로컬 503 `api_error`, A/B dispatch 증가 없음 |
| 2일 된 행, 현재 90일; 파일을 1일로 reload | current-config에 1일과 restart-required 표시, 실행 중 정책은 90일이라 200 |
| 같은 DB로 1일 설정 재시작 | 같은 행이 503, 조회가 timestamp를 갱신하거나 행을 삭제하지 않음 |
| 90일로 reload 후 재시작, 아직 purge하지 않은 같은 행 | 재시작 전 503, 재시작 후 200으로 복구 |
| 파일은 90일, 환경변수는 1일로 시작 | effective TTL 1일 |
| 기존 recurring 설정으로 로컬 QA의 purge interval을 1초, jitter를 0으로 설정 | 실제 cron producer와 Apalis worker가 `Done`을 기록하고 만료 행 7개를 삭제, fresh 행은 유지 |
| purge 후 오래된 history replay | 503, upstream dispatch 없음 |

정확한 동일 초 경계는 TestClock 기반 테스트로 검증했다. 실제 HTTP QA는 seed 시각 이후의 `>= TTL` 상태를 검증한다. 설정 변경으로 이미 삭제된 행이 복구된다는 주장은 하지 않는다.

새 admin fire-now나 test 전용 production API를 추가하지 않았다. 테스트 키는 revoke하고, synthetic upstream/principal을 정리했으며 credential이 로그에 없는 것도 확인했다. production DB·유료 provider는 사용하지 않았다.

### TTL 포함 성능과 실행 계획

원본 adapter와 현재 TTL adapter를 동일 release binary, 동일 최신 schema/인덱스와 pool에서 비교했다. baseline SQL/loop는 그대로 사용하며 변경된 trait의 연결만 scratch-local interface로 처리했다. 따라서 이 비교는 최신 schema 위에서 adapter 차이를 분리한 것이며, 최초 PR schema 자체의 성능을 재현한 수치는 아니다.

19/1,024-key active workload, before/current 순서를 뒤집은 두 pass(3,184 samples)의 평균 지연 current/baseline 비율:

| backend | 신규 bind 19 / 1,024 | upsert 19 / 1,024 | resolve 19 / 1,024 |
| --- | --- | --- | --- |
| SQLite | 0.761 / 0.219 | 0.820 / 0.386 | 1.001 / 1.090 |
| PostgreSQL | 0.836 / 0.156 | 0.921 / 0.174 | 1.294 / 1.052 |

조회까지 전부 빨라졌다고 주장하지 않는다. 빌드가 없는 상태에서 PostgreSQL 19-key 조회를 추가로 5-round 교차 측정했을 때 평균은 372.19→390.93µs, 중앙값은 365.21→374.13µs였다. round별 비율의 변동이 커 평균 +5.03% 추정은 그 측정만으로 통계적으로 확정되지 않았다.

조회 계획은 SQLite의 composite PK와 PostgreSQL의 pkey index/bitmap 접근을 유지했다. purge는 explicit-expiry/observed-at 인덱스로 후보를 제한한다. PostgreSQL 대상 순차 스캔을 제거한 뒤 1,000행 purge 평균은 explicit-expiry 9.356ms, retention 7.717ms였다(각 3회, production 보장값 아님). SQLite의 같은 batch 측정은 약 4.7–4.9ms였다.

TTL 단계 증거는 `/tmp/affinity-ttl-verification/`에 보존한다. 여기에는 full-suite 요약, 최종 PG 로그, Clippy 로그, 민감 정보 없는 HTTP/cron QA JSON과 before/current·query-plan CSV가 포함된다. 임시 실행 스크립트와 DB는 정리했고, 테스트용 PostgreSQL schema와 프로세스가 남지 않은 것을 확인했다.
