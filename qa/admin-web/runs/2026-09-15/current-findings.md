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

- `SettingsApply` 미지원 제공자의 잘못된 버튼 활성화는 2026-09-16 별도 승인한 capability 표시로 수정·검증했다. 실제 Apply 실행 구현과 아래의 기존 지원 모드 응답 계약 문제는 변경하지 않았다.
- `clearBaseURL`: 2026-09-16 명시적 null 제거 계약을 별도로 승인받아 로컬 후보에서 수정·검증했다. 과거 관측은 보존하며 운영에 반영됐다고 주장하지 않는다.

## 7. Source catalog 상태

- Live source 파일은 approved uncommitted candidate 기준으로 재생성되었다(standard parser 통과). Baseline archive hash는 `input_freeze.archive_member`에 보존.
- 현재 분모: 433 rows / 170 storage operations / 422 production matrix rows / ui_actions 205 / api_endpoints 115.

## 8. 머지 보류 후 로컬 추가 검증

- PR #793 머지 보류는 유지한다. 후속 로컬 검증과 승인된 수정은 `80e1180f…`, `91cd5219…` 커밋으로 draft PR에 반영됐고 각 head의 CI를 별도로 확인했다. 이 반영은 머지나 운영 배포가 아니다.
- `post-hold-reassessment/native-source-mapping.json`: 원래 native 관측 245행을 오프라인 대조했다. 244행은 정식 ID, 1행은 이름 편집 후 Escape 취소의 명시적 변형 별칭이다. 기존 실행 상태와 운영 행렬은 바꾸지 않았으며, 최신 분류는 runtime-progress의 후속 overlay에 연결했다.
- `post-hold-reassessment/independent-page-oracle-check.json`: 실제 로컬 PostgreSQL Plugins 탐색에서 사전에 고정한 경로·제목을 DOM에서 별도 추출한 상태와 비교하고 원본 PNG를 확인했다. 종료된 관측 창과 실제 registry SQL/acquire 연결을 검증했다. 과거 열린 window·유실 자료·동일 HTML 해시를 양쪽에 복사한 oracle은 그대로 한계로 남긴다.
- **2026-09-16 로컬 사용량 전이:** `post-hold-usage-status/verification.json`에 저장소·API·UI의 `$1.25 · 2.0K tok` → `$2.00 · 3.0K tok` 변화를 기록했다. 원시 request ingestion이 아닌 직접 시드한 rollup의 표시 경로 검증이다.
- **2026-09-16 로컬 상태 전이:** OAuth credential 없는 upstream의 실제 적용 오류와 danger 도트·오류 title을 확인했다. disable API 뒤 저장소·status API·UI에서 disabled·neutral·title 없음으로 전이했다. 연결 불가능한 URL을 오류 발생 원인으로 간주했던 가설은 기각했다. native tooltip 픽셀은 미검증이다.
- RT 수집 스크립트의 `/admin/v1` 전용 필터가 `/admin/usage`를 누락시킨 문제를 확인했다. 새 same-origin `/admin/` 관측에서 실제 UI 사용량 요청 3건을 각각 SQL 2개·acquire 2개에 연결했다. 과거 누락 구간을 새 측정값으로 채우지 않았다.
- 이 결과는 두 로컬 변형의 기능·상태 전이 검증이며, 운영 전수 실행·순수 DB 시간·순수 pool wait·전체 QA PASS를 의미하지 않는다.

## 9. 이전 Base URL 초기화 수정 — 후속 계약 변경은 §12 참조

- update에서 필드 생략은 유지, URL은 설정, 명시적 null은 override 제거로 구현했다. create/response nullable 표현과 API 키·OAuth 토큰 처리는 유지했다.
- 기존 HTTP 회귀는 수정 전 `String(previous_url) != Null`로 실패했다. 수정 후 SQLite·PostgreSQL conformance, signer 및 실제 Lifecycle→RecordingDispatcher 기본 목적지 검증을 포함한 33개 검사가 모두 통과했다.
- 실제 SettingsCard에서 입력 비우기→저장→독립 GET→새로고침을 수행했다. DB의 base_url NULL, 화면의 `—`, 기본 endpoint 메타데이터를 확인했다. 사용자에게 보이는 입력 동작과 저장된 상태가 이제 일치한다.
- 격리 fixture는 원래 loopback override로 복원하고 서버·브라우저를 종료했다. revision과 audit 기록은 정상적으로 증가했으므로 byte-identical 복원으로 표기하지 않는다. 외부 Anthropic 요청과 운영 변경은 없었다.

## 10. Settings Apply 지원 여부

- 실제 제공자의 `apply_supported`를 draft 응답에 추가했다. true일 때만 기존 검증·revision 조건과 함께 Apply를 활성화하며, false·아직 확인되지 않은 상태에서는 비활성화한다. Save·Validate는 유지한다.
- 지원/미지원 HTTP 경로를 포함한 backend 회귀 23개와 Web 700개가 통과했다. 실제 파일 기반 서버에서 Save·Validate 성공 후에도 Apply가 비활성화됐고, 해당 관측 구간의 서버 Apply 요청은 0건이었다.
- Apply 전용 안내는 공유 aria-live pipeline 밖에 두고 Apply만 설명 대상으로 연결했다. Validate 설명은 그대로 유지했다. 초기 로딩·조회 오류·미확인 상태의 안내도 구분한다.
- 최종 로컬 typecheck의 package-script 실행은 shell의 명령 검색 문제로 tsgo를 찾지 못했다. 설치된 동일 버전 tsgo를 절대경로로 실행한 `-b --noEmit` 검사는 통과했다. 전역 shell 설정이나 의존성을 변경하지 않았다.
- **별도 기존 계약 제한:** 지원 모드 Apply handler는 `{status: applied}`를 반환하지만 client는 `applied_revision` 등을 기대한다. 성공 토스트의 잘못된 revision 표시는 소스 추론이며 이 브라우저 run에서 실행한 결과가 아니다. client의 `expected_revision`도 해당 handler가 추출하지 않는다. history 재조회는 있으나 새 이력 생성은 별도 미검증이다. 이번 capability 승인 범위 밖이므로 고치지 않았다.

## 11. 현재 보고와 과거 관측의 구분

- `runtime-progress.json`과 `.md`는 2026-09-16 후속 overlay를 포함한다. 원래 정규화 1,511행·분류 계수·미해결 135항목은 과거 snapshot으로 보존하며 현재 남은 작업 수로 오인하지 않는다.
- 앞선 보고 snapshot에는 205 actions와 595 variant labels가 있었다. 승인된 안전성 수정 후 현재 source는 205 actions와 597 labels다. label 수는 entity·page·poll을 전개한 실행 분모가 아니며 실행률 계산에 사용하지 않는다.
- Base URL 초기화, file-provider Apply 비활성화, 두 로컬 usage/status 전이는 각각의 제한된 해결 범위를 갖는다. 전체 운영 행렬과 순수 DB/queue wait, 비운영 OAuth 성공 경로, 운영 계측 적용·머지는 여전히 미완료다.
- 사용자가 비운영 OAuth 테스트 계정을 현재 제공할 수 없다고 확인했다. 실제 성공 경로만 외부 전제조건 차단으로 남기고, 이미 검증된 실패·취소·격리 기능 결과와 분리한다. 운영 토큰이나 임의 계정을 대신 사용하지 않는다.

## 12. 승인된 애플리케이션 안전성 수정

- head `19b05570` 재검증에서 실제 회귀를 발견했다. 구버전에서 열어둔 폼의 무편집 저장이 Base URL을 지웠고, 최신 비관리자 기록 250건 뒤의 관리자 작업은 Audit 화면에서 사라졌다. 변경된 범위별 Audit 정렬의 추가 스캔·정렬도 확인했다. 실패 자료는 `application-safety/before/`에 보존했다.
- 사용자의 명시적 승인 후 HTTP `base_url` 생략/null을 유지로 되돌리고 `clear_base_url: true`만 초기화하도록 수정했다. UI는 편집 진입 시 기준값과 비교하고, 초기화 응답의 `base_url === null`이 확인되지 않으면 성공으로 표시하거나 폼을 닫지 않는다. 키·토큰 null 의미와 CAS는 유지한다.
- 최종 바이너리 `f08348b6dcbb821de48ba5d5bf2576168e03ec6c184626d8033a344bdfb4e2b8`로 두 버전 방향을 실제 브라우저에서 검증했다. 구 UI→새 서버의 무편집 저장은 URL/revision을 유지했고, 새 UI의 의도한 초기화는 DB NULL을 저장했다. 새 UI→구 서버의 무시된 초기화(PUT 200 한 건)는 오류·편집 유지로 표시되고 DB URL은 보존됐다.
- Audit UI는 `admin_only=true`를 사용하고 양 DB는 관리자 대상 조건을 LIMIT 전에 적용한다. 일반 API 기본 조회는 비관리자 기록을 계속 반환한다. 실제 UI에서 기존의 빈 목록이 관리자 작업 1건으로 바뀌었다. 브라우저 ResourceTiming에서 query string을 직접 포착하지 못한 한계는 원본 관측에 남겼으며 요청별 성능 상관 증거로 대체하지 않는다.
- 두 DB에 각각 5개의 정렬/부분 인덱스를 추가했다. 6개 조회 결과의 A/B/A 일치, SQLite 임시 정렬 제거, PG 강제 generic actor plan의 부분 인덱스 사용, 10만 합성 행의 실제 migration runner와 concurrent 사전 생성 경로를 확인했다.
- 인덱스 쓰기 비용은 증가했다. `application-safety/after/index-tradeoff-summary.json`에 SQLite autocommit 삽입 및 PG DB 실행/WAL 증가와 측정 한계를 기록했다. 원시 seed의 PG token NULL은 실제 writer와 달라 HTTP 500을 만들었으므로 테스트 데이터만 token 0으로 고쳤다. 이 fixture 오류를 앱 오류로 분류하거나 decoder를 완화하지 않았다.
- **운영 배포 조건:** 큰 운영 PG 테이블에서는 별도 승인된 온라인 사전 인덱스 생성과 정확한 정의·valid/ready 확인이 필요하다. plain startup 생성은 쓰기를 막으며 `lock_timeout`은 생성 시간을 제한하지 않는다. 운영 DDL·배포·머지는 수행하지 않았다.
- 최종 Rust 회귀 112개, Web 706개, 타입 검사·빌드·영향 Rust all-targets/all-features Clippy 및 formatter가 통과했다. Source inventory 433행 검사도 통과했다. 두 독립 리뷰의 실제 수정 요구를 처리했고 원시 증거·정리 결과는 `application-safety/verification.json`과 `index.json`에 연결했다. 이는 세 승인 수정의 격리 검증이지 전체 운영 QA 완료가 아니다.
