# Current Findings — run anchored at 2026-09-15 UTC

진행 중 관측 결과만 모은다. 완료 보고가 아니며, 어떤 항목도 production 배포·전수 PASS를 주장하지 않는다.

## 1. Controlled cache experiment (keepalive endpoints)

- Verdict: PASS. `cache-experiment/result-summary.json` 기준.
- 측정 범위: 고정된 `principalKeepalive` endpoint(detail/list/summary)와 seeded fixture key만 측정했다. 전체 product query가 아니다.
- 조건 증명: cold cycle 48회, eviction 후 resident page 0 확인, cold-A → warm → cold-B 순서.
- 결과 (median):
  - detail: cold-A TTFB 6.52ms / warm 3.80ms; db_exec 0.135ms → 0.085ms; blks_read 9 → 0.
  - list: cold-A TTFB 23.49ms / warm 20.65ms; db_exec 4.67ms → 6.15ms; blks_read 456 → 0, blks_hit 733 → 3,768.
  - summary: cold-A TTFB 20.35ms / warm 18.24ms; db_exec 4.21ms → 5.54ms; blks_read 453 → 0, blks_hit 689 → 3,721.
- 해석: 이 격리 실험에서 PostgreSQL `shared_blks_read`는 0으로 줄고 buffer hit이 늘었으며, TTFB 중앙값은 list 약 2.8ms, summary 약 2.1ms 감소했다. `shared_blks_read`는 PostgreSQL buffer로 읽어온 블록 수이며 물리 디스크 I/O 횟수가 아니다. 이 결과만으로 운영 병목의 주원인이 캐시인지 판단하지 않는다.
- 한계: mincore는 선택된 relation file만 측정(WAL/metadata/temp 제외); fadvise는 advisory; PostgreSQL restart는 shared_buffers만 초기화하고 storage-controller coldness를 증명하지 않는다.
- pure pool wait: 현재 app metrics/pg_stat_statements로 관측 불가. 0으로 채우지 않고 `null`로 보고했다.
- 측정 귀속: `isolated_serial_pg_stat_statements_delta`, request-ID tracing 아님. 비대상 shared_blks_read 합계 0, 비대상 statement interval 13(api_key_usage_writers_v1 계열). 동일 SQL family를 실행하는 미관측 background task 가능성은 배제하지 못했다.

## 2. Production heavy principal card

- `production-heavy-principal-card.json`: DOM-dispatched UI click 1건에서 최초 요청의 Resource Timing **전체 duration 8,442ms**, HTTP 200과 이후 자동 poll 8회를 관측했다. 이 파일은 해당 요청의 TTFB를 따로 기록하지 않았다.
- 8,442ms를 순수 DB 실행이나 pool queue wait로 분해할 근거는 없다. direct GET 통계와의 관계도 미확정이다.
- Keepalive 484 direct row 중 client deadline(2s×4, 3s×1, 10s×1) 6건은 right-censored lower bound이며 HTTP server FAIL이 아니다.

## 3. SQL/pool correlation 상태

- Server-timing isolated 증거로 canonical RID join은 두 backend에서 측정됐다(SQLite 17 entry, PostgreSQL 19 entry, SQL-bearing 각 6).
- 그러나 pure DB 실행 시간과 pure pool queue wait는 여전히 미측정이다. `sql_elapsed_secs`는 query-stream lifetime, `acquire_total_secs`는 full acquire다.
- PostgreSQL capture는 `action_window_still_open`으로 incomplete다.
- 과거 Camofox native 410 증거는 당시의 capability 한계로 보존한다. 이후 native 기능과 격리 SQL/acquire 연결을 확인했지만, 이 결과가 운영 계측 적용이나 전체 행렬 완료를 뜻하지는 않는다.

## 4. Production native 관측 (overlay)

- Plugins bounded slice: 33 rows / 20 unique item / PASS 15 + PASS_TARGET_ONLY 4 / BLOCKED 4 / NOT_APPLICABLE 5 / SKIPPED_WRITE 5.
- Upstreams bounded slice: 212 rows / 44 unique item / PASS 88 / SKIPPED_WRITE 94 / BLOCKED 20 / NOT_APPLICABLE 10. unmatched item_id 1건(`UI-SRC-0D929B471D7A-name-edit`).
- 두 파일 모두 write 0. 정확한 scope 확인 전이므로 production matrix에 미편입.

## 5. Scope 외 production GET

- `production-read-scope-deviation.json`: local-only 배정 subtask가 production GET 5건 실행(write 0). 별도 기록, 어느 coverage 집합에도 편입하지 않았다.

## 6. Known bug (미수정)

- `clearBaseURL`, `SettingsApply`: 알려진 product bug. 승인되지 않은 runtime 수정 없음.

## 7. Source catalog 상태

- Live source 파일은 approved uncommitted candidate 기준으로 재생성되었다(standard parser 통과). Baseline archive hash는 `input_freeze.archive_member`에 보존.
- 현재 분모: 433 rows / 170 storage operations / 422 production matrix rows / ui_actions 205 / api_endpoints 115.
