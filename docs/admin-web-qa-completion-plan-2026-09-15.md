# Admin Web 전수 QA 잔여 작업 완료 계획

- 작성일: 2026-09-15
- 상태: 진행 중. 전수 완료·운영 개선 완료를 주장하지 않는다.
- 작업 브랜치: `qa/admin-web-exhaustive-completion`
- 구현 기준선: `ef70b347790c46fa9956435a820a51b60c547d04` (Keepalive 성능 개선 PR #791 머지)
- 작업 트리: `/data/tmp/cc-lb-admin-web-qa-completion`
- 원본 보존 위치: `/data/data/orca/workspaces/cc-lb/analyze-and-fix-perf`
- 원래 운영 대상: `https://cc-lb.runbear.io`, Kubernetes context `runbear-operation`
- 실제 운영 배포 commit/image, DB, 측정 시간창은 재확인하여 실행 manifest에 별도로 고정한다.
- 이 문서의 모든 TODO를 끝낼 때까지 goal을 유지한다. 예산 한도를 설정하지 않는다.

## 1. 사용자 요청과 누락 감사

최초 요청은 모든 웹 조회·조작을 UI → API → handler → query로 추적한 QA 목록, 실제 운영 브라우저 실행, 네트워크·브라우저·서버·DB 병목 구분, 재사용 스킬 두 개와 QA 목록의 Git 관리였다. 후속으로 전체 스킬 경로의 `.opencode/skills` → `.agents/skills` 이전도 요청되었다.

앞선 작업은 Keepalive 개선 코드와 로컬 검증·PR/CI는 수행했지만, 다음 일곱 항목을 완료하지 않았다.

| 누락 ID | 남은 의무 | 감사 시점의 근거 | 완료에 필요한 결과 |
|---|---|---|---|
| GAP-01 | 두 스킬·인벤토리·생성기·유효 실행 결과 Git 관리 | 원본에서 미추적. #791 머지 트리에 전체 인벤토리와 두 스킬 없음 | 리뷰·CI를 거친 커밋/PR 및 승인된 머지로 공유 저장소에 반영 |
| GAP-02 | 기존 스킬 전체 경로 이전 반영 | 로컬 이동만 수행. 머지 트리 `.agents/skills` 0개, `.opencode/skills` 36개 파일 | 최신 기준선의 모든 기존 파일을 보존한 이전, 참조 및 발견 검증 |
| GAP-03 | 누락 없는 전체 QA 목록 증명 | 기존 226행은 생성 행 수이며 `pending_full_reconciliation` | 독립 소스 분모, 정방향/역방향 누락 집합 0, 안정 ID 및 실행 단계 |
| GAP-04 | 전체 운영 브라우저 실행 | 주요 화면의 일부 측정과 Keepalive 집중 측정만 존재 | 모든 정의된 대상·변형·페이지·poll cell의 실행 결과와 증거 |
| GAP-05 | 운영 Keepalive 증거 보완과 잘못된 보고 정정 | 과거 pagination 20-page cap, 일부 detail timing 독립성 미입증, 기존 보고서 전수 완료 문구 잔존 | 독립 원시 timestamp, 실제 terminal cursor, 보고서 정정 및 새 측정 |
| GAP-06 | 요청별 모든 계층 상관 측정 | 최종 로컬 API 기록 양 DB 각각 389행의 SQL/pool 필드 null, 별도 plan은 존재 | 브라우저/서버/SQL/pool 증거 연결, 미제공 항목의 실제 계측 또는 구체적 승인·차단 처리 |
| GAP-07 | 통제된 cold/warm 비교 | 첫 요청/후속 요청을 구분했지만 OS/DB cold cache를 통제하지 않음 | 격리 환경의 검증 가능한 cache 상태, 동일 조건의 반복 비교 |

분석·제안 `docs/keepalive-performance-proposal.md`는 사용자의 미커밋 보존 지시를 유지한다. 그 지시를 스킬·QA 산출물 제외로 확대하지 않는다. 과거 로컬 57개 QA·92개 브라우저 PASS는 운영 전수 실행을 대체하지 않는다.

## 2. 권한과 안전 경계

1. 승인된 작업은 문서·프로젝트 소유 스킬·QA 도구·목록·격리 fixture·실행 증거 작성, 조사와 측정, 관련 Git/PR 준비다.
2. **머지는 PR마다 별도 승인받는다.** 승인 요청에는 저장소, PR 번호·제목, 해결하는 일, 주요 파일/동작 변경, 남은 위험, 검증 결과, base/head SHA, merge 방식과 branch 삭제 여부를 설명한다. 승인 전 머지·auto-merge·merge queue를 설정하지 않는다. #791의 과거 승인은 다른 PR에 적용하지 않는다.
3. 새 애플리케이션 runtime 코드·공개 API·제품 동작 변경이 필요하면, 정확한 파일/심볼·변경 동작·필요성·대안을 제시하고 승인받기 전에는 수정하지 않는다. 계측 부족을 메우기 위한 runtime instrumentation도 같은 원칙을 따른다.
4. 운영은 승인된 읽기 측정만 수행한다. 삭제·revocation·설정 변경·외부 호출을 유발하는 동작은 격리 fixture에서 검증한다. 운영의 가역 쓰기도 구체적 대상과 복구 방법을 제시하고 승인받는다.
5. HTTP GET이라도 실제 부작용·갱신·외부 과금 가능성을 소스로 확인한다. method만으로 안전하다고 분류하지 않는다.
6. 운영 DB cache flush, 운영 PostgreSQL 재시작, 전역 통계 reset, 운영 대량 부하, 실제 자격증명 복제 및 비밀값을 포함한 dump를 금지한다.
7. 운영 읽기 요청은 낮은 동시성으로 실행하며 부하·오류·대기 상황에 따라 일시 중단한다. 중단은 BLOCKED로 기록하고 완료로 처리하지 않는다.
8. 운영 데이터가 계속 변하면 관측 시간창과 분모 변경을 기록한다. 불안정한 전체 집합을 고정된 fixture로 검증한 결과와 운영 결과를 합쳐 위장하지 않는다.
9. 기존 원본 작업 트리는 보존한다. 최신 기준선의 기존 스킬을 이전하고 원본 신규 두 스킬 및 QA 자산만 선별 반영한다. 오래된 앱 diff·migration 번호·미커밋 분석을 통째로 복사하지 않는다.
10. 원시 request ID·시각·증거는 보존하되 token, cookie, 개인정보, 요청 payload 비밀값은 기록하지 않는다. Git에는 검증된 비식별 증거와 재현 절차만 반영한다.

## 3. 산출물과 단일 소유권

- 이 계획 문서: Main 소유. 요청·TODO·승인·상태·최종 증거의 진행 기록.
- `.agents/skills/web-qa-inventory-extractor/SKILL.md`: 정적 UI/API/SQL 전수 추출과 독립 역대조 절차. 실행 스킬과 역할을 중복하지 않는다.
- `.agents/skills/web-performance-qa/SKILL.md`: 검증된 목록의 브라우저 실행·계측·위험 격리·증거 연결 절차.
- `scripts/generate-qa-inventory.mjs`: 기존 생성기를 재사용한다. 단순히 하드코딩된 이전 목록을 출력한 것으로 현재 소스 조사 성공을 주장하지 않는다.
- `qa/admin-web/api-query-inventory.yaml` 및 `.md`: 같은 안정 ID, 같은 원자 요청, 실행 방법과 source evidence.
- `qa/admin-web/runs/`: run별 manifest, 소스 분모/실행 행렬, PASS/FAIL/BLOCKED ledger, 비식별 원시 증거 및 결과 보고. 기존 원시 기록을 새 기록으로 덮어쓰지 않는다.
- 기존 `.agents/skills/user-flow-qa` 시나리오와 `web/e2e`, `web/qa` 실행 자산을 우선 재사용한다. 불필요한 별도 테스트 프레임워크를 만들지 않는다.
- 소스 조사자는 소스와 의도만 기록한다. 브라우저 결과·계측값·PASS를 만들어내지 않는다. 브라우저 실행자는 실제 수행한 cell만 기록한다. Main이 두 증거를 연결한다.

## 4. 행렬과 증거 계약

### 4.1 정적 목록

각 UI action은 안정 ID, 화면/컴포넌트, 현재 source 파일·심볼·위치, 선행조건, 정확한 클릭/입력/스크롤 단계, 상태/필터 변형, 기대 화면, 관련 원자 요청 ID를 갖는다. 원자 요청은 method/path/query/body/관련 header, client callsite, handler, authorization, storage trait와 SQLite/PostgreSQL query 또는 명시적 no-query/cache 근거를 갖는다. UI 도달 불가와 client-only는 사유를 기록한다.

동적 경로는 실제 wire contract로 검증한다. 여러 method/endpoint를 `A & B` 한 행에 합치지 않는다. 임의 page size, selector가 겹치는 All 버튼, 존재하지 않는 modal, 이전 버전 endpoint를 쓰지 않는다.

### 4.2 실행 범위

- 엔티티별 항목은 실제 API로 Principal/Upstream/Plugin/대상 행의 분모를 확인한다.
- 각 적용 가능한 상태·필터·페이지·poll cycle을 실제 실행 키로 펼친다. 적용되지 않는 차원은 사유와 cardinality 1을 기록한다.
- 계속 추가되는 운영 이력은 측정 anchor와 cursor 범위로 한정하고 변화량을 기록한다. 무제한 미래 데이터까지 끝났다고 주장하지 않는다.
- pagination 종료는 원본 wire `next_cursor == null`과 마지막 응답 증거로만 확인한다. page 수 hard cap을 terminal로 바꾸지 않는다.
- 쓰기/삭제·오류/빈 상태는 목적에 맞는 격리 fixture에서 상태전이 전후를 검증한다. 운영과 격리 실행의 환경 표시는 별도다.

### 4.3 요청별 원시 기록

각 record에 `run_id`, `execution_key`, `environment`, `source_commit`, `deployed_image`, UTC 및 monotonic 시작/종료 시각, method/비식별 URL/입력 hash, status, response 크기와 의미 hash를 기록한다. UI action과 direct request는 서로 다른 request ID와 시각을 갖는다. 쓰기 요청을 단순히 두 번 재생하지 말고 격리 fixture의 동일 시작 상태를 복원한 독립 실행으로 비교한다.

브라우저 click/request/TTFB/download/state/render 시점을 보존한다. server handler 및 SQL·pool wait는 같은 correlation으로 연결한다. cache-served/no-DB 경로는 실제 관측 또는 소스 근거로 `not_applicable`과 이유를 남긴다. 측정되지 않은 값은 0 또는 다른 계층의 값으로 채우지 않는다. null은 완료가 아니라 구체적 미계측/차단 사유와 연결한다.

### 4.4 완료 판정

- 정적 완료: source→inventory와 inventory→source의 미매핑 집합 각각 0, 중복 ID 0, 잘못된 path/method/source 0.
- 실행 완료: 실제 기대 cell 집합과 고유 ledger key 집합이 exact하게 같다.
- 수식: `expected cells = unique keys = PASS + FAIL + BLOCKED`.
- 최종 PASS: FAIL 0, BLOCKED 0, 필수 증거 누락 0, 위험 동작의 복구 확인.
- 단순 행 수 일치, 대표 entity, screenshot만 존재, API 200, 단위 테스트 통과는 전수 완료가 아니다.
- 승인·접근·필수 계측이 막히면 TODO를 완료하지 않는다. 차단 원인과 해소에 필요한 구체적 승인을 사용자에게 요청한다.

## 5. 상세 TODO와 완료 조건

체크박스는 실행 증거가 있을 때만 갱신한다. TODO 도구와 아래 86개 항목의 이름을 동일하게 유지한다. 발견된 추가 의무는 원래 항목을 지우지 않고 새 ID로 추가한다.

### 문서와 기준선

- [x] PLAN-01 남은 작업 문서와 완료 조건 작성
  - GAP-01~07, 원문 권한 경계, 모든 TODO, 단일 소유자와 증거 계약을 이 문서에 기록하고 목록 일치를 검증한다.
- [x] PLAN-02 원본 보존과 최신 기준 작업트리 준비
  - #791이 포함된 기준선과 별도 브랜치를 고정한다. 원본 분석/스킬/QA 자료를 보존하고 승인된 자산만 선별 이관한다.
- [x] PLAN-03 운영 배포 인증 관측 접근 기준선 확정
  - 실제 image/commit, Pod/DB backend, 접근 계정/권한, WARP, metrics/logs/traces/DB read-only 경로를 확인한다. 비밀값 없이 manifest를 만든다.

### 스킬과 자산

- [x] ASSET-01 최신 기존 스킬 전체 경로 이전
  - 최신 `.opencode/skills`의 모든 파일을 내용 보존하여 이전한다. 신규 두 스킬 추가와 구분하고 관련 참조를 갱신한다.
- [x] ASSET-02 두 QA 스킬 역할과 실행 계약 정리
  - 전수 추출/역대조와 실제 실행/계측의 책임을 분리한다. 위험 구분, 변동 분모, 독립 timing, cold-cache 의미와 false-PASS 금지를 명확히 한다.
- [x] ASSET-03 인벤토리 생성기와 산출물 Git 관리 준비
  - 기존 생성기·안정 ID를 이어받고 current-source 검증 데이터를 반영한다. YAML/Markdown 생성 재현성과 일치를 확인한다.
- [x] ASSET-04 기존 운영 보고서의 과장 판정 정정
  - 기존 원시 기록은 보존하고 20-page cap/detail timing 증거 한계를 명시한다. 기존 `runtime-reconciled`/`10/10` 주장을 철회하며 새 run과 연결한다.
- [x] ASSET-05 새 경로 스킬 발견과 재실행 검증
  - 새 세션/정상 discovery에서 `.agents/skills`를 찾고 각 스킬의 문서화된 명령/경로를 실행할 수 있음을 확인한다. 현재 세션의 stale `skill://` 매핑은 새 경로를 찾았다고 거짓 판정하지 않는다.

### 소스 전수 목록

- [x] INV-01 Overview 공통 인증 탐색 동작 전수 대조
  - KPI/차트/시간창/사용량/Principal drill-down, auth gate, command palette, navigation, mount/poll/refresh 요청을 모두 추적한다.
- [x] INV-02 Upstreams Warmup 동작과 요청 전수 대조
  - 목록/상세/설정/enable-disable/metadata·quota/refresh/warmup/인증/키 등 현재 존재하는 모든 조건부 UI와 요청을 추적한다.
- [x] INV-03 Principals Router Keepalive 동작 전수 대조
  - 생성/편집/키/권한/limit/route 설정과 Keepalive 카드·sheet·필터·상세·poll·cursor 동작을 추적한다.
- [x] INV-04 Logs SSE 필터 상세 동작 전수 대조
  - live/history, session/group, 모든 필터·시간창·페이지, SSE lifecycle, row/detail/payload 전개 요청을 추적한다.
- [x] INV-05 Plugins 업로드 참조 삭제 동작 전수 대조
  - 목록/상세/upload/replace/apply/reference/GC/delete와 현재 UI의 조건부 경로를 추적한다.
- [x] INV-06 Settings Audit 동작과 요청 전수 대조
  - config draft/schema/validation/apply/history와 Audit 검색/시간창/필터/페이지/상세를 현재 source 기준으로 추적한다.
- [x] INV-07 Admin API 역추적과 양 저장소 SQL 매핑
  - 모든 등록 method/path를 UI 요청과 역대조한다. handler→trait→실제 SQL/메모리/cache를 추적하고 runtime 부작용도 분류한다.
- [x] INV-08 소스 인벤토리 양방향 누락 집합 검증
  - 서로 독립적인 source 분모와 inventory를 집합 대조한다. 미매핑·중복·깨진 참조가 0임을 기계적으로 확인한다.

- [x] INV-09 운영 전용 Credentials Status 소스와 차이 대조
  - 실제 운영 `e56d029e`와 기준선 `ef70b347` 사이의 UI/API 차이를 별도 기록한다. 운영에 남아 있는 Credentials/Status 화면, 이전 인증·config·API 계약을 누락하지 않고 최신 전용 기능의 운영 적용 불가 상태를 명시한다.
### 계측 준비

- [ ] MEASURE-01 운영 엔티티 분모와 실행 행렬 고정
  - 실제 목록과 cursor page traversal, 조회 anchor, 상태/필터/poll 조합을 기록한다. 변동 데이터 규칙과 운영 부하 중단 조건을 설정한다.
- [ ] MEASURE-02 안전한 쓰기 삭제 격리 fixture 준비
  - 실제 app + SQLite 기본 환경 및 필요한 PostgreSQL 환경을 구성한다. 외부 서비스는 안전한 시험 연결을 사용하고 실제 자격증명을 복제하지 않는다. 정상/빈/오류/권한 상태 및 복구 경로를 준비한다.
  - 2026-09-16 사용자가 실제 OAuth 성공 검증용 비운영 테스트 계정이 현재 없다고 확인했다. 해당 성공·메타데이터 조회 경로는 외부 전제조건 차단으로 유지한다. 운영 토큰 복제, 임의 계정 선택, 가짜 성공 응답으로 대체하지 않으며 계정 부재를 다른 격리 검증의 실패로 합산하지 않는다.
- [x] MEASURE-03 요청별 서버 SQL pool 계측 방법 확정
  - 기존 관측 기능으로 요청별 SQL 횟수/시간/pool 대기를 얻을 수 있는지 확인한다. 부족하면 필요한 정확한 instrumentation 범위를 승인 요청한다. 별도 EXPLAIN만으로 실제 요청 시간을 채우지 않는다.
- [x] MEASURE-04 브라우저 독립 원시 기록 수집기 검증
  - 실제 클릭과 network capture를 연결하고 원시 시각·response hash·render 결과를 수집한다. direct fetch 복사값 및 누락값을 성공으로 판정하지 않는 음성 검증을 수행한다.

- [x] MEASURE-05 Admin 요청에 한정한 SQL 로그 필터 검증
  - 격리 SQLite·PostgreSQL에서 `info,[admin.request]=debug`를 시험했다. 각 3개 Admin 조회의 SQL·acquire 이벤트를 유지하면서 Admin span 밖 SQL DEBUG는 0건이었다. 같은 소규모 broad-filter 대조에서는 각각 99·144건이었다. 일반 INFO와 Proxy 401 제어 결과는 유지했다.
  - 이 설정은 Admin span 내부의 모든 DEBUG를 허용하므로 SQL-only 필터나 모든 endpoint의 비밀 안전성 검증이 아니다. 운영 설정·앱 코드·provider 계정은 변경하지 않았으며 로그량·성능 수치를 운영 전체로 외삽하지 않는다.

### 전수 실측

- [ ] RUN-01 Overview 공통 인증 탐색 브라우저 실측
  - INV-01의 모든 적용 가능한 cell을 실행하고 환경·원자 요청·UI 증거를 기록한다.
- [ ] RUN-02 Upstreams Warmup 브라우저 전수 실측
  - 모든 대상 Upstream과 variant를 실행한다. 부작용 있는 동작은 격리 환경에서 검증한다.
- [ ] RUN-03 Principals Router 브라우저 전수 실측
  - 모든 대상 Principal의 일반 관리/라우팅 UI와 관련 상태전이를 실행한다. 키 발급/폐기는 격리 fixture만 사용한다.
- [ ] RUN-04 Keepalive 계정 필터 상세 폴링 전수 실측
  - 모든 active Principal, 3 horizons×7 filters, 카드/설정 sheet/list/detail, 동시 세 poll stream 각각 최소 두 cycle을 검증한다.
- [ ] RUN-05 Keepalive 모든 cursor 실제 terminal 검증
  - 적용 가능한 각 조합을 실제 null cursor까지 순회한다. hard cap, 중간 오류, 변동 분모는 별도 차단 사유이며 terminal로 기록하지 않는다.
- [ ] RUN-06 Logs SSE 필터 상세 브라우저 전수 실측
  - 로그와 SSE의 모든 정의된 변형을 실행하고 시간창·cursor·필터·렌더 상태의 일치를 확인한다.
- [ ] RUN-07 Plugins 업로드 참조 삭제 브라우저 실측
  - 운영 read cell과 격리 lifecycle mutation cell을 구분해 실행하고 참조/삭제 상태전이를 검증한다.
- [ ] RUN-08 Settings Audit 브라우저 전수 실측
  - 운영 조회 및 격리 설정 적용/이력/감사 상태전이를 검증한다.
- [ ] RUN-09 쓰기 삭제 상태전이와 복구 결과 검증
  - 모든 mutation cell이 storage→API→UI 변화와 복구를 증명하는지 대조한다. 실행하지 못한 동작을 목록에서 제외하지 않는다.
- [ ] RUN-10 모든 원자 요청의 서버 DB 상관 분석
  - 각 실행을 실제 handler·SQL·pool 관측과 연결한다. 화면/전송/서버/DB 병목 및 미관측 원인을 개별 cell에 기록한다.

- [ ] RUN-11 운영 전용 Credentials Status 브라우저 실측
  - INV-09에서 확인한 운영 전용 화면과 동작도 동일한 실제 브라우저·원시 증거 기준으로 실행한다. 최신 코드에서 제거되었다는 이유로 운영 분모에서 빼지 않는다.
### 캐시 비교

- [x] CACHE-01 격리 cold warm 캐시 통제 절차 검증
  - 운영이 아닌 격리 환경에서 app cache, DB buffer, OS page cache의 통제 범위를 명시하고 cache 상태를 관측으로 입증한다. 첫 요청을 cold라고 이름만 바꾸지 않는다.
- [x] CACHE-02 통제된 캐시 조건별 성능 비교 기록
  - 같은 build/engine/dataset/동시성에서 cold와 warm 조건을 각각 반복 측정한다. API/SQL/pool/response/브라우저 지표를 분리하고 제한을 명시한다.

### 검증과 전달

- [ ] VERIFY-01 기능 누락 실패 증거 결함 수정 재검증
  - QA 자산의 실제 결함은 수정하고 재실행한다. 앱 결함은 범위를 제시해 승인받은 후 수정한다. 실패를 삭제·재명명·완화하여 통과시키지 않는다.
- [ ] VERIFY-02 원시 증거 보안과 실행 행렬 완전성 검증
  - 기대 key와 actual key를 exact 대조하고 missing/duplicate/secret/필수 null을 검사한다. 독립 검토자가 분모와 PASS 주장을 반증 시도한다.
- [ ] VERIFY-03 최종 성능 병목 결과와 문서 동기화
  - 화면/계정/variant별 결과, 오류와 개선점, SQL 상관 근거, cache 상태, 남은 한계를 작성한다. 스킬/목록/계획/실행 보고의 상태가 모순되지 않게 한다.
- [x] VERIFY-04 전체 변경 독립 리뷰와 저장소 게이트 검증
  - 스킬 이전·QA 도구·데이터·문서 전체 diff를 독립 리뷰한다. 변경 범위에 맞는 formatter/lint/typecheck/tests와 실제 실행을 최종 한 번 수행한다.
- [x] VERIFY-05 문서 스킬 인벤토리 증거 커밋 PR 생성
  - 명시된 자산과 검증된 비식별 증거만 커밋한다. 분석 제안·비밀값·무관한 원본 앱 변경을 제외하고 PR에 포함/제외 범위를 설명한다.
- [x] VERIFY-06 현재 PR head CI 통과와 리뷰 완료
  - 현재 head의 CI/필수 리뷰를 확인한다. 실패는 무수정 rerun하지 않고 원인을 수정해 검증한다.
- [x] VERIFY-07 PR별 변경 설명 후 사용자 머지 승인 요청
  - PR마다 §2의 최종 내용·검증·head/base·방식·위험을 설명하고 사용자 승인을 기록한다. 대기 중에는 가능한 독립 작업을 진행한다.
- [ ] VERIFY-08 승인된 PR만 머지하고 산출물 반영 확인
  - 승인된 동일 head/대상/옵션으로만 머지한다. 변경되면 다시 설명하고 승인받는다. 공유 저장소의 파일/스킬 경로/보고서 반영을 확인한 뒤 goal 완료를 판정한다.

- [x] VERIFY-09 YAML 직렬화 결함 수정 및 파서 검증
  - 빈 문자열 등 특수 mapping key를 안전하게 직렬화한다. 생성 YAML을 표준 파서로 읽고 원본 key/value가 보존되는지 검증한다. 생성물 바이트 일치와 문법·의미 유효성을 별도로 확인한다.

### Audit 오류 수정 — 사용자 추가 승인

- [x] AUDIT-01 승인된 최신 Audit 조회 계약과 영향 범위 고정
  - 실제 263행 fixture에서 오래된 200개를 먼저 제한해 최신 config_export가 API/UI에서 누락되는 결함을 수정한다. 기존 AuditStore의 append-order 조회는 보존한다. 새 최근 조회는 ts 내림차순과 삽입 ID/seq 내림차순 tie-break 후 LIMIT을 적용하며 All/Principal/Actor 범위를 지원한다. HTTP 필터 검증·since/after/until·limit 상한과 감사 기록 부작용은 유지한다.
- [x] AUDIT-02 기존 순서를 보존하는 양 DB 최신 조회 구현
  - storage-api에 별도 query_recent_audit와 borrowed AuditQueryScope를 추가하고 SQLite/PostgreSQL에서 실제 제한 조회한다. 기존 query_audit/query_audit_by_actor와 conformance append 순서는 바꾸지 않는다. 모든 trait 구현·관련 mock도 갱신한다. 불필요한 새 migration·실시간 설정 변경·상시 계측은 포함하지 않는다.
- [x] AUDIT-03 Audit 화면 연결과 최신 기록 회귀 검증
  - Admin Audit 두 alias를 새 조회에 연결하고 최신 200개 초과, 같은 ts의 결정적 순서, 지연 삽입된 과거 ts, Principal/Actor 필터, 빈/0limit/역전시간창, 새 export 발생 후 UI 갱신을 검증한다. 원래 오래된 순 저장소 조회의 기존 테스트는 유지한다. 실제 브라우저 pre-fix FAIL 증거와 post-fix 검증을 분리한다.
- [x] AUDIT-04 수정된 Audit 계약과 QA 목록 및 증거 갱신
  - 코드 변경 뒤 source catalog/hash/SQL/실행 명세와 결과를 갱신한다. 이전 ef70 실행 증거와 새 candidate의 소스·binary를 혼동하지 않는다. 수정 PR/머지는 기존 개별 승인 절차를 따른다.

### 추가 승인 오류 수정

- [x] FIX-01 Upstream 이름 변경 중복 제출 수정 검증
  - InlineNameEditor의 Enter/blur 중복 제출을 실제 한 요청으로 수렴시킨다. 정상 blur 저장과 Escape 취소는 유지하며 API 성공 뒤 stale revision 409가 덮어쓰지 않도록 실제 브라우저로 검증한다.
- [x] FIX-02 저장된 Base URL 응답 누락 수정 검증
  - Upstreams/OAuth 조회·변경 응답과 프런트 schema에 저장된 base_url을 일관되게 반환한다. secret 원문이나 저장되지 않은 api_key_env 출처를 복원·노출하지 않는다. 저장→새 조회→페이지 새로고침 상태전이를 검증한다.
- [x] FIX-03 선택적 API 키 라벨 계약 수정 검증
  - 선택적 라벨의 omitted/empty 값을 양 DB 발급 경로에서 허용하고 실제 잘못된 입력을 400으로 매핑한다. 키 생성·해시·인증 알고리즘과 중복발급 방지는 보존한다. 발급·조회·proxy 사용·폐기의 실제 경로 및 음성 입력을 검증한다.
- [x] FIX-04 Principal 변경 후 Router revision 동기화 검증
  - spec_revision을 바꾸는 관련 mutation 후 Router의 실제 서버 revision/전략 값을 갱신하고 갱신 중 stale 저장을 막는다. 진짜 동시 수정에 대한 409 보호는 유지한다.
- [x] FIX-05 Plugins 삭제 완료 흐름과 GC 안내 수정 검증
  - 삭제된 대상의 참조 refetch/Not Found 오류가 성공을 덮지 않게 캐시·현재 선택 화면을 정리한다. GC는 고아 blob만 정리한다는 실제 동작에 맞춰 안내·활성 조건을 수정하며 등록된 플러그인을 추가 삭제하지 않는다.

### 승인된 격리 계측

- [x] OBS-01 Admin 전용 요청 식별과 span 구현
  - `app.rs::admin_router`와 private middleware에 서버 생성 bounded ID, `x-request-id`, 정제된 request span과 응답 생성까지의 handler 시간을 연결한다. Proxy 처리·인증·본문·SSE 동작은 바꾸지 않는다.
- [x] OBS-02 양 저장소 acquire 전체시간 관측 활성화
  - SQLite `open_sqlite`와 PostgreSQL `open_postgres_pool`에서 SQLx 일반 acquire 로그를 DEBUG로 활성화할 수 있게 한다. pool 크기·timeout·SQL 결과는 유지하며 기존 전이 의존성 `log`를 직접 선언한다.
- [x] OBS-03 격리 요청 SQL acquire 상관 검증
  - 두 DB의 실제 Admin 요청에서 응답 ID와 동일 span의 SQL·acquire 이벤트를 연결한다. 식별자 충돌·클라이언트 ID 반사·handler/stream 시간 혼동을 방지한다.
- [x] OBS-04 계측 비밀 비노출과 오버헤드 검증
  - 격리 로그에 credential·cookie·본문·원시 URL을 노출하지 않는지 확인하고, 기존 후보와 계측 후보를 같은 조건에서 비교한다. Proxy 제어 요청도 비교한다. 운영 설정 변경·배포는 별도 승인 전에는 하지 않는다.

- [x] OBS-05 Server-Timing UI 요청 식별 연결 구현
  - 추가 승인된 `Server-Timing`의 `rid.description`에 기존 서버 ID만 전달한다. 시간·본문·인증정보를 추가 노출하지 않는다. 수집기는 캐시 ID 재사용·뒤늦은 모호성·상충하는 ID를 보수적으로 처리한다.
- [x] OBS-06 실제 브라우저 SQL 상관 연결 검증
  - 각 DB에서 실제 UI가 보낸 요청을 Resource Timing ID로 서버 head/SQL/acquire 로그에 연결한다. 독립 fetch로 UI 요청을 대체하지 않고, 빠진 로그·본문·순수 wait를 다른 값으로 채우지 않는다.

### 차단 항목 재평가

- [x] REASSESS-01 운영 관측 행과 소스 ID 오프라인 대조
  - 이미 보존한 Plugins·Upstreams native 관측을 실제 source ID와 대조한다. 별도 관측 이름을 정식 ID로 연결할 근거가 없는 행은 미해결로 남기며 새 운영 요청은 보내지 않는다.
- [x] REASSESS-02 외부 승인 없이 가능한 잔여 검증 분리
  - 남은 항목을 외부 승인·credential 필요, 운영 계측 적용 필요, 기존 자료로 가능한 대조, 승인된 격리 환경에서 가능한 검증으로 나눈다. 머지 보류를 다른 작업의 취소로 확대하지 않는다.

- [x] REASSESS-03 PostgreSQL 브라우저 관측 창 종료 증거 검증
  - 기존 raw snapshot의 `end=null`과 `ui_oracle=null`은 유지하고, 승인된 로컬 PostgreSQL 환경에서 유효한 `endAction` 호출 뒤 종료된 window와 실제 UI·RID·SQL 증거를 새 파일로 보존한다. 운영 요청이나 코드 변경은 하지 않는다.
  - 새 raw window의 종료 시각과 서버 요청 3건 연결을 확인했다. 다만 기대·관측 해시를 같은 HTML에서 만든 한계가 있어 독립 UI oracle PASS로는 인정하지 않는다.
- [x] REASSESS-04 독립 UI 오라클과 원본 화면 증거 검증
  - 클릭 전에 최소 기대 상태를 고정하고, 실제 DOM에서 별도로 추출한 상태와 비교한다. 스크린샷 원본 bytes와 두 상태 객체를 먼저 보존한 뒤 해시를 계산한다. 동일 관측을 양쪽에 복사한 해시 비교는 통과 근거로 사용하지 않는다.
  - 클릭 전에 고정한 `/plugins` 경로와 `Plugins` 제목을 실제 DOM에서 별도 추출한 상태와 비교했다. 원본 PNG를 보존하고 Main이 화면과 해시를 확인했다. 종료된 관측 창의 registry GET을 실제 PostgreSQL query/acquire 이벤트 3개씩에 연결했다. 이 한 페이지 탐색 검증은 운영 전수 QA 완료를 뜻하지 않는다.

### 격리 잔여 시나리오

- [x] LOCAL-01 Upstream 사용량 표시의 실데이터 경로 검증
  - 미관측된 cost/token 표시가 어떤 실제 집계·DTO·UI 필드에 연결되는지 확인한다. 외부 호출 없는 격리 fixture 생성 경로가 확인될 때만 실제 상태 전이를 검증한다.
  - 새 SQLite fixture의 실제 rollup 저장소와 Admin API, 브라우저에서 `$1.25 · 2.0K tok` → `$2.00 · 3.0K tok` 자연 poll 전이를 확인했다. 수정한 캡처에서 실제 `/admin/usage` UI 요청 3건을 각각 SQL·acquire 이벤트에 연결했다. request ingestion 경로를 검증한 것은 아니다.
- [x] LOCAL-02 Upstream 오류 상태 표시의 생성 경로 검증
  - 상태 표시의 실제 생성 경로와 UI 계약을 확인한다. 연결되지 않는 URL만 설정해서 오류 상태라고 가정하지 않고, 존재하지 않는 UI 변형도 통과·실패로 만들지 않는다.
  - 실제 생성 경로는 연결 실패가 아닌 OAuth credential 검증 오류였다. 새 격리 upstream의 `error`·danger 도트·오류 title을 확인하고 실제 disable API 이후 저장소·status API·브라우저가 `disabled`·neutral·title 없음으로 바뀌는 것을 확인했다. native tooltip 픽셀 표시는 검증하지 않았다.

### 승인된 Base URL 초기화

- [x] BASEURL-01 명시적 null 초기화 계약과 호출부 범위 확정
  - 2026-09-16 사용자가 `명시적 null로 제거 승인`을 선택했다. update에서 생략은 유지, null은 override 제거, URL은 설정이다. create/response의 nullable 표현과 key/OAuth/warmup 필드 계약은 유지한다.
- [x] BASEURL-02 API와 양 저장소의 삼상태 갱신 구현
  - `UpstreamUpdate.base_url`의 외부 Option은 변경 여부, 내부 Option은 nullable 값을 뜻한다. 기존 Serde 삼상태 관례와 양 DB의 명시적 presence 조건을 사용하고 모든 실제 caller/mock을 이관한다. 새 migration·의존성·runtime test hook은 추가하지 않는다.
- [x] BASEURL-03 생략 설정 초기화 및 충돌 회귀 검증
  - 기존 HTTP 회귀에 생략·명시적 null·복원·stale revision을 추가했고 수정 전 실제 실패를 보존했다. 양 DB conformance와 해당 회귀를 수정 후 검증하며 키·토큰의 비노출과 충돌 보호를 유지한다.
- [x] BASEURL-04 실제 UI 저장 재조회와 프록시 목적지 검증
  - 실제 SettingsCard에서 비우기→저장→새 GET→새로고침 뒤 기본값 상태를 확인한다. 실제 proxy 경로는 기존 RecordingDispatcher seam에서 custom override와 제거 후 기본 목적지를 비교한다. 유료/외부 Anthropic 요청은 보내지 않는다.
  - 수정 전 HTTP 회귀가 기존 URL 반환으로 실패했고, 수정 후 SQLite·PostgreSQL conformance와 signer·실제 Lifecycle dispatch 회귀를 포함한 33/33 검사가 통과했다. 실제 UI 비우기·저장·독립 GET·새로고침 후 DB NULL과 기본 endpoint 표시를 확인하고 원래 loopback override를 복원했다. 외부 provider 요청은 하지 않았다.

- [x] BASEURL-05 초기화 수정 후 카탈로그 문서 최종 검증
  - formatter, 영향 Rust target의 all-features Clippy, source inventory check와 표준 YAML 파싱을 확인했다. 두 DB의 실제 SQL 및 bind 정보와 UI·API 삼상태 계약을 함께 갱신했다.
- [x] BASEURL-06 Base URL 수정 커밋과 PR CI 검증
  - 추가 승인 수정과 격리 증거를 별도 커밋으로 기존 draft PR에 반영하고 새 head의 CI를 확인한다. 이전 head의 통과를 새 head에 적용하지 않으며 머지 보류는 유지한다.
  - 후속 커밋 `80e1180f0bedd2054450fb68e57444e035c59ef2`를 draft PR #793에 반영했다. 해당 head의 Rust·Web·publish-check 및 상태 검사 11개 성공, release-artifact 조건부 제외 1개를 확인했다. 사용자 머지 보류는 유지하며 ready 전환·머지·배포는 하지 않았다.

### 승인된 Settings 지원 표시

- [x] SETTINGS-01 설정 제공자 capability와 소비자 범위 확정
  - 2026-09-16 사용자가 지원 여부 표시 수정을 승인했다. 실제 Apply 구현이 있는 InMemoryCurrentConfig와 TestReloader만 지원을 선언하고, ConfigWatcher·Config·기본 제공자는 미지원이다.
- [x] SETTINGS-02 API 지원 여부와 Apply 비활성화 구현
  - CurrentConfig의 명시적 capability를 draft 응답의 필수 `apply_supported`로 전달한다. UI는 값이 정확히 true일 때만 기존 검증·revision 조건과 함께 Apply를 허용하고, false·미확인에는 비활성화 이유를 표시한다.
- [x] SETTINGS-03 지원 미지원 및 상태 전이 회귀 검증
  - 수정 전 미지원인데 Apply가 활성화되는 실패를 재현했다. 미지원 제공자의 저장·검증 유지와 직접 Apply 501, 지원 제공자의 기존 200, UI의 false→true→unknown 전이를 검증한다.
- [x] SETTINGS-04 실제 브라우저 안내와 요청 차단 검증
  - 실제 파일 기반 서버에서 유효한 draft의 Save·Validate 후에도 Apply가 비활성화되고 안내가 보이며 Apply 요청이 발생하지 않는지 확인한다. 사전 기대 상태·독립 DOM·원본 PNG·서버 로그를 보존한다.
  - 실제 파일 제공자에서 Save 1건·Validate 1건 성공 후에도 Apply가 비활성화됐고 서버 로그의 Apply 요청은 0건이었다. 최종 문구·Apply 전용 접근성 설명·Validate 설명 분리와 변경 컨트롤이 보이는 원본 PNG를 확인했다.
- [x] SETTINGS-05 설정 capability 문서 인벤토리 및 PR 검증
  - API·UI 계약과 정확한 source 참조를 갱신하고 영향 게이트 및 독립 리뷰를 수행한다. 새 head CI를 확인하되 기존 머지 보류를 해제하거나 운영에 적용하지 않는다.
  - 커밋 `91cd5219bd9047d1339ff40e9aec11142867a36e`를 draft PR #793에 반영했다. 해당 head의 Rust·Web·publish-check 및 상태 검사 11개 성공, release-artifact 조건부 제외 1개를 확인했다. 기존 지원 모드의 응답 revision·요청 CAS·이력 생성 제한은 별도 미검증 사항으로 남겼으며 머지·운영 반영은 하지 않았다.

### 애플리케이션 안전성 재검증

- [x] SAFETY-01 앱 변경을 목적별로 분류하고 영향 범위 확정
  - head `19b05570`의 소스 파일 27개 중 2개는 cfg(test) 내부 변경이고 25개는 런타임 변경이다. QA 도구와 의도한 기능 수정·관측 변경을 구분했다.
- [x] SAFETY-02 독립 리뷰로 의도하지 않은 동작 변경 점검
  - 계측·백엔드/저장소·프런트엔드를 독립 검토했다. 근거가 부족했던 Plugins 이중 삭제 추정은 알림/탐색 순서를 대조한 뒤 철회했다.
- [x] SAFETY-03 누락된 동작 비교를 실행하고 안전성 근거 보고
  - 현재 head 빌드로 Rust 104개·웹 53개 검사, SQLite/PG HTTP 22쌍(44요청), 성공한 합성 키 등록의 로그 비노출 검사를 수행했다. Audit 빈 화면과 구버전에서 열린 Base URL 폼의 변경 없는 저장에 의한 DB NULL을 실제 브라우저로 재현했다. Audit 범위별 추가 정렬/스캔을 두 DB의 실행 계획으로 확인했다. 운영 영향 없음.
- [x] SAFETY-04 수정 승인 후 앱 회귀 세 항목 재검증
  - 2026-09-16 사용자가 아래 세 수정 범위를 명시적으로 승인했다. 기존 CI/리뷰 완료 체크는 당시 검사 이력이며 새로 발견된 안전성 결함의 해결을 뜻하지 않는다.

### 승인된 앱 안전성 수정

- [x] SAFETYFIX-01 Base URL 명시적 초기화 계약과 폼 수정
  - HTTP의 생략·null은 유지, URL은 설정, `clear_base_url: true`만 초기화로 구분한다. URL과 초기화가 동시에 전달되면 변경 전에 거절한다. 기존 BASEURL-01~06의 null 초기화는 과거 검증 이력으로 보존하고 현재 계약은 이 항목으로 대체한다. 자격증명 의미와 revision 보호를 유지한다.
- [x] SAFETYFIX-02 Audit 관리자 필터를 조회 제한 전에 적용
  - API 기본값은 전체 기록을 유지하고 UI는 `admin_only=true`를 명시한다. 양 DB에서 관리자 대상 조건을 LIMIT 전에 적용하며 principal/actor/time 범위와 최신 순서를 유지한다.
- [x] SAFETYFIX-03 Audit 정렬 인덱스와 쓰기 비용 검증
  - 새 SQLite/PG 마이그레이션으로 실제 필터·정렬을 지원한다. 기존 마이그레이션을 고치거나 운영 DB에 실행하지 않는다. 조회 계획과 추가 쓰기 비용을 격리 데이터로 검증한다.
  - 최종 인덱스는 두 DB 각각 5개이며 actor 정렬 인덱스는 `actor_authority IS NOT NULL` 부분 인덱스다. 6개 조회 형태의 A/B/A 결과가 같았고 SQLite 임시 정렬 제거와 PostgreSQL generic actor plan의 부분 인덱스 사용을 확인했다. 10만 합성 행의 실제 앱 migration runner에서 직접 생성 및 별도 concurrent 사전 생성 경로 모두 성공하고 행 수·정의·valid/ready 상태가 보존됐다.
  - 쓰기 비용은 0이 아니다. 합성 데이터의 SQLite 2천 autocommit 삽입은 평균 기준 대비 비관리자 약 33%, 관리자 약 126% 증가했다. 실제 writer처럼 token 0을 저장한 PostgreSQL 1만 행 INSERT의 DB 실행 시간은 약 73%·151%, WAL은 약 27%·116% 증가했다. PG 값은 commit/client/pool 시간을 제외한 실행·WAL 수치이며 운영 지연·처리량으로 외삽하지 않는다. token NULL을 넣었던 초기 SQL 전용 fixture 비용은 최종 비용 근거로 사용하지 않는다.
- [x] SAFETYFIX-04 공용 API 타입과 모든 소비자 통합
  - 새 초기화 필드와 Audit 조회 옵션의 타입·호출부·mock·카탈로그를 함께 이관한다. 과거 실행 증거는 현재 계약으로 덮어쓰지 않는다.
- [x] SAFETYFIX-05 기존 실패 재현과 양 DB 회귀 검증
  - 구버전에서 열린 폼을 유지한 배포 전환 후 무편집 저장, 현재 UI의 명시적 초기화, 429 기록 뒤의 관리자 작업 조회, 양 DB 결과/정렬/쓰기 비용, 권한·CAS·자격증명 보존을 확인한다.
  - 최종 바이너리 `f08348b6dcbb821de48ba5d5bf2576168e03ec6c184626d8033a344bdfb4e2b8`에서 old UI→new backend 무편집 저장은 URL/revision을 그대로 유지했고, current UI의 의도적 초기화는 DB NULL을 저장했다. new UI→old backend에서 무시된 초기화(PUT 200 한 건)는 오류·편집 유지로 표시되며 성공을 가장하지 않았다. Audit의 숨겨졌던 관리자 작업은 실제 UI에 표시됐다.
  - Rust 112개·Web 706개·타입 검사·빌드·영향 target Clippy 및 formatter를 확인했다. 실제 PG HTTP에서도 보존/명시적 초기화/400 충돌/409 CAS/복원 및 두 Audit 모드를 확인했다. 표준 YAML 파싱과 inventory 생성·check는 433행으로 통과했다. 원시 증거의 한계와 초기 PG fixture 오류(token NULL)를 별도 기록했다.
- [x] SAFETYFIX-06 독립 리뷰와 안전성 보고 및 PR 갱신
  - 세 결함의 수정과 재검증을 독립 리뷰한다. 필요한 저장소 게이트와 새 head CI를 확인하되 머지·운영 배포·운영 DB 변경은 하지 않는다.
  - 앱 수정 커밋 `e993e2393b6a3b5774afd0f85a9a881fdb2f0955`를 draft PR #793에 반영했다. 해당 commit의 CI·Web·publish-check 및 상태 검사 11개 성공, release-artifact 조건부 제외 1개를 확인했다. `application-safety/ci-source-commit.json`에 exact-head 근거를 고정했다. 이후 문서 갱신의 CI는 그 head에서 별도로 확인하며, 머지와 운영 반영은 계속 보류한다.

## 6. 진행 및 증거 기록

| 시점 | 항목 | 실제 수행/증거 | 남은 조건 |
|---|---|---|---|
| 2026-09-15 시작 | 전체 | 누락 감사 후 사용자로부터 남은 작업 문서화·지속 실행 지시 수신. 예산 제한 없는 goal 생성. 별도 최신 기준 작업트리 생성. | 아래 TODO 증거 기반 실행 |
| 2026-09-15 | PLAN-01, PLAN-02 | 문서 TODO 40개와 고유 이름 40개 확인. `ef70b347` 기준 별도 worktree 생성. 기존 QA 자산 5개만 선별 복사하고 원본 보존. 예산 필드 없이 active goal 생성 확인. | 운영 기준선 및 후속 검증 진행 중 |
| 2026-09-15 | ASSET-01, ASSET-02 | 최신 기준선 스킬 36개 원본과 이동 후 byte 비교 불일치 0, 기존 경로 제거 확인. path+SHA manifest `e7e0bb343a8e1d874926bd3937c5c67e9e7f884c69d99d6c8f3b05364a1bffb0`. 신규 두 스킬의 정적 추출/실행 책임 분리. | Git 반영·최종 게이트는 별도 TODO 유지 |
| 2026-09-15 | ASSET-04, ASSET-05 진행 | 과거 보고서 전수 완료 주장 철회 및 별도 review JSON 작성, 원시 JSON byte 동일 보존. 새 `omp --cwd=... --skills=... --no-session -p` 세션이 두 `skill://` URI를 `.agents/skills` 실제 경로로 읽음. 단독 `omp read`의 empty registry 실패와 구분. | 새 운영 run 연결 및 생성기 재실행 검증 남음 |
| 2026-09-15 | PLAN-03, INV-09, RUN-11 진행 | 운영 v0.4.9 `e56d029e`/PG16 migration114와 기준선 `ef70b347`/migration118 차이 확인. Admin/Web 변경57파일과 운영 전용 화면을 위한 TODO2개 추가. | 운영·최신 소스 행렬을 별도 검증 |
| 2026-09-15 | PLAN-03 완료 | Camofox 실제 UI와 GET에서 Principal22/Upstream9/Plugin2 확인. 보호된 조회 접근 성공. legacy 배포의 auth/session404를 최신 identity 권한 증거로 오인하지 않음. `production-preflight.json` 및 독립 K8s/Thanos/Tempo 조사에 버전·DB·관측 한계 기록. | 요청별 SQL/pool 수집과 전수 실행은 별도 TODO |
| 2026-09-15 | MEASURE-03 범위 결정 | 새 상시 앱 계측 승인 요청에 사용자가 목적을 질문했으며 승인은 주어지지 않았다. 기존 관측으로 전수 QA를 먼저 진행하고 실제 특정 요청의 증거 부족을 확인한 뒤 최소 보완 방법을 판단한다. | runtime 변경 없음. 누락 계측은 null과 원인을 기록 |
| 2026-09-15 | CACHE-01 부분 검증 | 네트워크 없는 일회용 Linux 컨테이너의 소유64MiB파일에서 mincore16384pages→파일별evict0→inspect0→warm16384를 실제 관측. 전역cache flush와 운영 변경 없음. | 실제 DB buffer/app cache 조건과 성능 비교는 아직 미검증 |
| 2026-09-15 | ASSET-03~05 완료 | generator/--check 모두433행(205UI/149request/115API) 생성·일치 검증. 별도 scratch의 new file·기존 source 변경·MD불일치가 각각 실패하고 복구 후 성공함을 실험. native skill discovery와 과거 보고서 철회·새 IN_PROGRESS run 교차참조 완료. | 실제 실행/최종 독립 검토/커밋은 별도 TODO 유지 |
| 2026-09-15 | MEASURE-02 진행 | SQLite·PG16 실제앱 각각 prepare→populated snapshot→reset→restart→proof 수행. fixture hash/엔티티수 보존,각12개200+의도401확인,plugin참조2 확인. OAuth시작URL을loopback으로제한. | 실제browser mutation전수와 외부OAuth성공 조건은 미완료 |
| 2026-09-15 | INV-01~06, INV-09 완료 | 영역별 독립 소스 조사와 통합 교정으로 UI205action/149원자요청·93sourcefile을 고정하고현재hash/route/요청매핑검사통과. 운영과다른57파일 대조및운영전용10UI/13API명세를deployed-delta에보존. 기존가짜modal/경로는교정. | API/SQL 의미 독립 검토와 최종 양방향 판정은 INV-07/08에서 별도 진행 |
| 2026-09-15 | MEASURE-04 완료 | recorder1.0.5 실제 Chromium click/독립GET/원래Promise·Response identity/동일URL병렬/503/abort/SSEno-clone/privacy/overflow/미완료fetch drain후settlement 보존 검증. 최신raw SHA `d491f9de898a4dddadc1da8d034edd92f3e9fbe701c499e352c73907d9aa28be`. | Camofox native410장애와ResourceTiming candidate/서버correlation부재는별도한계이며운영전수PASS를뜻하지않음 |
| 2026-09-15 | INV-07, INV-08 완료 | 115API 등록↔목록·149UI요청 매핑 미해결0. read70개를4개독립범위(10/21/19/20)로검토해발견한20개 의미오류를교정하고각검토자가해소확인. slim-checkpoint/usage-interval 누락SQL과upstream전체scan오매핑수정. 최종168고유operation·양DB SQL 근거를검증하고generator/--check다시통과. | source-semantic-review.json 참조. 실제운영실행/계측의완료는별도이며전체diff최종리뷰도남음 |
| 2026-09-15 | CACHE-01, CACHE-02 완료 | 현재ef70앱+격리PG16,같은합성100k-decision데이터·concurrency1에서summary/list/detail각 cold-A8/warm30/cold-B8 총138요청. 48cold회모두소유relation341파일/37684pageevict후mincore0,요청직전targetOS/PGbuffer0 실증. 각endpoint46회200·canonicalhash단일값 직접검증. | `runs/2026-09-15/cache-experiment` 참조. PostgreSQL target-relation cache만의통제실험;순수poolwait미관측,protocolrequest-ID추적/하드웨어cold/운영전체개선/SQLiteAPI cache비교주장없음 |
| 2026-09-15 | CACHE-01/02 재현 증거 보완 대기 | 위 실측·48회 조건·138응답 자체는 확인했지만 실행자가 당시 helper 원본/hash를 보존하지 않았음을 후속확인. 현재helper는실험후안전성수정으로달라졌으므로현재hash를당시코드로기록하지않는다. 원래도구작성·수정기록에서당시버전을정확복구중이며체크박스를완료에서보류로정정한다. | 실측값변조·추정복구없음. 재현스크립트와당시helper provenance확보후완료판정 |
| 2026-09-15 | CACHE-01/02 재현 증거 보완 완료 | 후속두수정의기록을역적용해965행/36278byte의당시helper를복구했고기존tool snapshot D210과일치. 복구SHA `6b96b33f848db01c0a293325480cb480fb0635295c27a17330a3d85f2ebcaf7b`,재현스크립트·protocol과함께보존. | reproducibility.json에사후복구이며실험당시fullSHA를기록한것이아님을명시. 실측값/원시조건증거변경없음 |
| 2026-09-15 | AUDIT-01~03 완료 | 새최근조회의양DB·HTTP회귀32개PASS,실제후보UI15/15 PASS·36network기록. 최신200개/동일ts/late-old삽입/필터/newexport조회상태전이확인. ef70baseline과후보binary·patchSHA를분리보존. | approvedcandidate evidence는 `local-settings/audit-candidate`; SettingsApply501은변경없음 |
| 2026-09-15 | 전체검증 환경 결함 처리 | 영향crate1512개실행중1511PASS/1trybuild링커실패/9skip. trybuild가RUSTFLAGS를제거해사용자Cargo설정의ld64.lld가AppleSDK를해석못한것을확인. 글로벌설정변경없이scratch CARGO_HOME의nativeclang과기존캐시/bin링크를사용해해당실제typestate test 재실행1/1 PASS. | 테스트삭제/약화/무수정retry아님. 최종다섯수정통합후전체gate다시검증할예정 |
| 2026-09-15 | 운영 실행 차단 기록 | 부분분모와74개공통관측·484개Keepalive direct 기록을보존. 짧은client deadline은serverfailure가아닌censored lower bound로교정. heavycard한번실제DOM UI요청은200/8442ms로관측하고poll후이동·탭종료. | 전체UIlist조합·cursor미완료. Camofox native410/isolated-world관측한계·운영부하안전중단을완료로세지않음 |
| 2026-09-15 | FIX-01~05 검증 | 후보 binary SHA `f564dcf57a4c2b914b2c690b8dee4e1c68f4fa472ae680bbe599d250dcd28f65`: Upstreams 필수 6/6, Principals 10/10, Plugins 단순 삭제·cascade 삭제·실제 orphan GC 세 흐름 PASS. 원래 환경변수명 복원은 승인 범위 밖의 기존 write-only 계약으로 분리한다. | 격리 후보 검증이며 운영 반영이나 운영 전수 PASS를 뜻하지 않는다. |
| 2026-09-15 | 후보 증거 보존 | 원시 JSON/JSONL·provenance·후속 회귀 로그 17개를 `qa/admin-web/runs/2026-09-15/approved-fixes/index.json`의 경로와 SHA로 보존했다. | 자격증명·브라우저 프로필·검토하지 않은 스크린샷은 제외했다. 기존 baseline FAIL은 후보 PASS로 덮어쓰지 않는다. |
| 2026-09-15 | 응답 회귀 테스트 수정 | 최초 전체 게이트 1,519/1,520 PASS 뒤 승인된 `base_url` 추가를 거부하던 assertion을 수정했다. 같은 테스트와 warmup 이력 4개 필드 비노출 검증을 보존했고, focused 9/9 및 수정된 전체 게이트 1,520/1,520 PASS(exit 0)를 확인했다. | 제외된 9개는 실행된 것으로 세지 않는다. 이 결과는 이후 추가된 계측 구현 전 후보의 회귀 증거이며 운영 전수 측정 PASS가 아니다. |
| 2026-09-15 | 독립 리뷰 | Audit·FIX-01~05·응답 테스트 수정은 correct 판정. `after`는 하한 배타 필터이며 전진 cursor를 보장하지 않는다. legacy 빈 label 문자열과 v1 null 표현은 각각 보존한다. | 조건부 Audit 정렬의 인덱스 비용과 현재 도달 경로가 확인되지 않은 DB check 오류 메시지 위험은 미측정 사항으로 남긴다. migration·추가 API 변경 없음. |
| 2026-09-15 | YAML 직렬화 결함 | `wire_semantics`의 빈 문자열 키가 인용되지 않는 결함을 확인했다. 기존 `--check`의 바이트 일치만으로 파싱 성공을 주장할 수 없다. | 카탈로그·generator 수정 후 표준 파싱 검증 필요. 운영 엔티티·cursor 분모, native pointer, 요청별 SQL/pool 증거, 비운영 OAuth 성공 자격증명도 미완료다. |
| 2026-09-15 | 격리 계측 검증 | `instrumentation/index.json`에 19개 파일 보존. 후보 `132e831a…`: 두 DB 12개 실제 요청의 ID·SQL·acquire 연결, 민감한 body/cookie/header/query 표식 비노출 2건, A/B/A 300개 GET, Proxy 401·request-id 제어 12건, 관련 library tests 191/191 PASS. | 빈 목록의 loopback 왕복 비교이며 순수 CPU 비용이나 운영 성능을 뜻하지 않는다. 순수 DB 실행·pool wait는 미측정이다. |
| 2026-09-15 | 브라우저 연결 확장 | strict-CSP mock에서 Server-Timing ID를 Camofox 격리 영역에서 읽고 서버 ID 두 개와 일치시켰다. 사용자 추가 승인으로 기존 ID만 담는 헤더를 구현했다. 새 후보 `db564601…`에서 두 DB UI 요청 14개의 ID를 관찰했다. | 이전 PG 서버의 제한된 Hub tail로 전체 로그를 회수하지 못했으므로 그 7개 요청을 SQL 연결 완료로 세지 않는다. 전체 로그를 private 파일에 보존하도록 수집 경로를 고친 뒤 새 관측을 진행한다. |
| 2026-09-15 | 수집기 과장 방지 | 수집기 1.0.7에서 뒤늦은 동일 ID 후보·상충 ID·약한 기존 연결이 잘못 유지되는 세 경우를 재현하고 수정 후 같은 재현을 통과했다. | 실제 브라우저 검증과 캐시 ID 재사용 처리는 별도 확인한다. native click 한 호출이 두 요청을 낸 도구 현상도 기록하며 UI action과 HTTP request를 1:1로 가정하지 않는다. |
| 2026-09-15 | Canonical 수집기 연결 검증 | 수정된 수집기 1.0.7에서 보존한 SQLite 17개, PostgreSQL 19개 Resource Timing ID를 관측 전부터 저장한 private 서버 로그에 모두 연결했다. 각 DB에서 6개 SQL-bearing entry를 확인했다. `qa/admin-web/runs/2026-09-15/server-timing/index.json`에 성공·실패·제한 증거를 분리 보존했다. | SQLite는 sequence 106 이후의 별도 window이며 앞선 103개 drain 복구가 아니다. PG raw snapshot의 action window는 열려 있어 전체 UI oracle 완료가 아니다. 순수 DB/queue 시간과 전체 운영 행렬은 계속 미완료다. |
| 2026-09-15 | 최종 코드 게이트 | Rust format, Web lint/typecheck, 72 files·699 Web tests, 현재 inventory check, recorder syntax 모두 PASS. Server/SQLite library tests는 Server-Timing 추가 후에도 191/191 PASS. | 테스트/도구 통과가 운영 전체 실행이나 머지 승인을 뜻하지 않는다. |
| 2026-09-15 | PR #793 생성 및 CI | `qa/admin-web-exhaustive-completion`의 `7c8b6168957bff5050d6482ae7d3c137ee7efe31`을 푸시하고 `master` 대상 draft PR을 생성했다. Rust·Web·publish-check 모두 성공했고 상태 검사 포함 11개 성공, release-artifact 조건부 검사 1개 제외를 확인했다. | 이 결과는 해당 head에 한정한다. 이후 로컬 QA 기록은 별도 변경이며 자동으로 같은 CI 증거를 적용하지 않는다. |
| 2026-09-15 | 머지 보류 | 저장소·PR·base/head·squash/admin 방식·브랜치 유지·미완료 범위를 제시한 뒤 사용자가 `머지 보류`를 선택했다. | Draft 해제·머지·자동 머지·브랜치 삭제·운영 배포는 실행하지 않는다. 별도 승인 없는 나머지 QA 작업은 가능한 범위만 수행한다. |
| 2026-09-16 | 격리 사용량·오류 상태 전이 | `post-hold-usage-status/index.json`의 22개 파일에 초기·전이 원시 DOM/PNG/API/SQL seed 및 UI 요청 상관 증거를 보존했다. 기존 미관측 `UPSTREAM-004`, `UPSTREAM-006`에 대한 새 로컬 기능 증거이며 과거 행은 덮어쓰지 않았다. | `/admin/v1` 전용 RT 필터가 `/admin/usage`를 빠뜨린 수집 결함을 수정해 새 관측으로 검증했다. 운영 요청·앱 코드·PR 변경은 없고 해당 fixture 포트·브라우저 탭은 종료했다. |

## 7. 승인 및 차단 기록

| 대상 | 요청할 구체적 변경/작업 | 승인 상태 | 실행 상태 |
|---|---|---|---|
| PR #793 머지 | head `7c8b6168957bff5050d6482ae7d3c137ee7efe31`, base `master`, squash/admin, 브랜치 삭제 없음으로 최종 요청 | 사용자 `머지 보류` 선택 | Draft 유지. 머지·ready 전환·auto-merge·queue·브랜치 삭제 금지. 새 명시적 승인 필요 |
| 격리 Admin 요청 계측 | Admin 전용 ID/span, 양 DB acquire 로그, 필요한 직접 의존성 선언 및 격리 검증 | 사용자 `격리 계측 구현 승인` 선택 | 명시한 세 runtime 파일과 지원 metadata만 변경. Proxy 처리·운영 설정·배포는 제외 |
| 운영 쓰기 또는 운영 설정 변경 | 필요 시 대상·부작용·복구 방법별 요청 | 미승인 | 변경 없음 |
| 최신 Audit 기록 누락 수정 | Admin Audit용 최근 제한 조회 추가, 기존 append-order 계약 유지, 양 DB/handler/회귀 검증 | 사용자 ask에서 `Audit 오류 수정 승인` 선택 | 위 AUDIT-01~04 범위만 구현 가능. 머지·운영 반영 미승인 |
| Settings Apply 지원 표시 | provider capability를 draft API에 노출하고 UI에서 명시적 true만 Apply 허용; 미지원·로딩·오류·unknown 안내 구분 | 2026-09-16 사용자 `지원 여부 표시 수정 승인` 선택 | API/700 Web·23 backend 회귀와 실제 file-provider UI 검증 완료. 기존 apply/reload 실행·startup-fixed 정책은 변경하지 않음 |
| 추가 다섯 앱 오류 수정 | FIX-01~05: 중복 이름 저장/Base URL/선택 라벨/Router revision/Plugins 삭제·GC 표시 | 사용자 다중 선택으로 다섯 항목 모두 승인 | 해당 UI/API/양 DB/회귀 범위만 수정. 별도 계측·Settings Apply·배포·머지 미승인 |
| Server-Timing ID 전달 | `app.rs`에서 기존 x-request-id와 동일한 ID만 rid.description으로 전달하고 QA 수집기에 연결 | 사용자 `격리 환경 추가 승인` 선택 | 새 시간값·본문·credential 노출 없음. 운영 설정·배포는 별도 승인 |
| Base URL 명시적 null 제거 | Upstream update의 생략/명시적 null/URL을 삼상태로 구분하고 양 DB·기존 호출부·UI·프록시 목적지 검증 | 2026-09-16 사용자 `명시적 null로 제거 승인` 선택 | 격리 구현·검증 완료. 키·토큰 null 계약과 운영 데이터·머지 보류는 유지 |
| 앱 안전성 세 항목 수정 | 명시적 `clear_base_url`, LIMIT 전 `admin_only`, Audit 필터·정렬 인덱스와 읽기/쓰기 검증 | 2026-09-16 사용자 `세 문제 수정 승인` 선택 | 이 범위의 앱/API/새 migration 및 격리 검증만 승인. 기존 null 초기화 HTTP 계약을 대체. 머지·운영 배포·운영 DB 실행은 미승인 |
| 대용량 운영 PG 인덱스 준비 | 운영 배포 전 동일한 5개 인덱스를 별도 승인된 온라인 작업으로 생성하고 정의·valid/ready 확인 | 미승인 | 직접 startup 생성은 쓰기를 막을 수 있으므로 사전 준비와 승인 전 배포 차단. 격리 리허설만 수행 |

문서의 TODO가 모두 체크되고, 실제 증거의 필수 항목이 모두 충족되고, 승인 필요한 작업까지 완료되기 전에는 goal을 완료 처리하지 않는다.

### 7.1 요청 상관 계측: 확인된 원인과 승인 경계

읽기 전용 조사 후 사용자가 격리 계측 구현을 명시 승인했다. 승인된 앱 변경은 `cc-lb-server/src/app.rs`의 Admin 전용 middleware, `cc-lb-server/src/storage_factory.rs::open_postgres_pool`, `cc-lb-storage-sqlite/src/lib.rs::open_sqlite`이다. 필요한 기존 `log` 직접 의존성 선언과 검증을 포함하되 버전 인상은 하지 않는다. 운영 로그 설정 변경·배포, Proxy 처리 변경은 승인 범위 밖이다.

- `crates/cc-lb-server/src/app.rs::admin_router`와 `crates/cc-lb-admin/src/routes.rs::build_router`에는 Admin 요청용 식별 span과 응답 correlation header가 없다. Proxy의 기존 `request_id_middleware`는 별도 경로이므로 수정 대상으로 삼지 않는다.
- 최소 HTTP 제안은 Admin 경로에만 서버 생성 bounded ID와 `x-request-id` 응답 헤더, 정제된 method·route template·status·handler duration을 연결하는 것이다. 비신뢰 클라이언트 ID를 무조건 반사하거나 헤더·본문·쿠키·원시 URL을 기록하지 않는다.
- SQLite는 `crates/cc-lb-storage-sqlite/src/lib.rs::open_sqlite`, PostgreSQL은 `crates/cc-lb-server/src/storage_factory.rs::open_postgres_pool`에서 pool을 구성한다. 일반 acquire 로그는 SQLx 기본값이 off이므로 로그 필터만 바꿔서는 활성화되지 않는다.
- 저장소 SQLx 0.9.0의 query 로그는 현재 span을 사용한다. SQLite worker도 명령과 함께 span을 받아 진입한다. 다만 `QueryLogger.elapsed`에는 행 스트리밍·소비·백프레셔 시간이 포함될 수 있으므로 순수 DB 실행 시간으로 기록하지 않는다.
- SQLx의 `acquired_after_secs`는 세마포어 대기 외에 ping·연결 생성·인증·hook·재시도 등을 포함하는 **acquire 전체 시간**이다. 순수 `pool_wait_ms`를 분리하는 기본 hook은 확인되지 않았다. acquire 값을 pool wait에 복사하지 않는다.
- 위 HTTP span과 pool 옵션은 격리 구현·검증만 승인되었다. 실제 운영 로그 필터와 배포는 별도 사용자 승인 전에는 실행하지 않는다. 격리 요청에서 상관 ID·driver 시간·acquire 시간·비밀 비노출을 먼저 증명해야 하며, 순수 DB 실행과 pool wait가 여전히 없으면 해당 필드는 계속 차단 상태다.
- 계측 CPU·메모리·로그량 비용은 아직 측정하지 않았다. 무시할 수 있는 비용이라고 가정하거나 전체 요청의 귀속이 이미 증명됐다고 보고하지 않는다.

### 7.2 Audit 인덱스의 운영 배포 조건

PostgreSQL의 plain `CREATE INDEX`는 생성하는 동안 해당 테이블의 쓰기를 막는다. 새 migration의 `lock_timeout = '1s'`는 잠금 획득 대기만 제한하며 생성 중 잠금 보유 시간을 제한하지 않는다. statement timeout도 성공이나 무중단을 보장하지 않는다.

따라서 대용량 운영 `audit_log_v1`에서는 새 앱 배포보다 먼저, 별도로 승인받은 작업에서 새 migration과 동일한 인덱스 이름·키 순서·조건으로 `CREATE INDEX CONCURRENTLY`를 한 문장씩 transaction 밖에서 실행해야 한다. 앱 migration 경로 안에서 concurrent 생성을 실행하거나 timeout을 해제하지 않는다. 생성 후 대상 테이블, 전체 정의, `indisvalid`, `indisready`를 확인한다. 이름만 같은 잘못된/invalid 인덱스는 `IF NOT EXISTS`만으로 검증되지 않는다.

올바른 사전 생성이 확인되면 앱의 새 migration은 인덱스 생성을 건너뛴다. 이 경로도 짧은 잠금과 migration 기록 작업까지 없어진다는 뜻은 아니다. 격리 10만 행 리허설은 운영 무중단 증명이나 운영 DDL 승인으로 간주하지 않는다. SQLite 검증은 별도 writer가 없는 격리 startup에서 수행했으며 인덱스 생성이 다른 writer에 영향을 주지 않는다고 주장하지 않는다.
