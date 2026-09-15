# 프론트엔드 상태 관리 감사와 수정 계획

- 감사일: 2026-09-07
- 기준: `isac322/fix-flickering`의 폴링 플리커링 수정이 적용된 소스.
- 사용자 요청: 병렬 조사 → 독립 교차검증 → 결과 보존 → 기존 플리커링과 분리된 PR에서 수정.
- 감사 중 저장소 변경 없음: 전후 Git diff와 untracked 내용 SHA-256 일치.

## 기존 플리커링 수정과의 관계

기반 변경은 조회 중 성공 데이터를 스켈레톤/빈 배열로 교체하던 경로와 절대시간 query key의 교체를 수정했다. 이번 변경은 편집 대상 식별자, 공유 설정, SSE 수명주기, 캐시 무효화, 렌더링 비용 및 검증 누락을 다룬다. 요청 상세 `partial → final` 타임라인은 같은 데이터 보존 원칙이 누락된 별도 전환이다. 같은 파일을 공유하므로 기반 PR 위의 stacked PR로 차이를 분리한다.

## 확정 및 조건부 확정 원장

경로는 별도 표기가 없으면 `crates/cc-lb-admin/web/src/` 기준이다. 아래는 수정 전 관찰이며, 수정 완료 판정은 검증 결과 절에서 별도로 기록한다.

| ID | 우선순위 | 근거와 사용자 영향 | 수정 방향 |
|---|---|---|---|
| S01 | P1 | `routes/principals.tsx` PrincipalDetail에 identity key가 없어 Allowed Models/Default Limits/inline slot 초안이 A에서 B로 이동. 실제 React probe에서 B id/revision과 A 모델 초안을 함께 제출 | principal identity에 편집 subtree와 mutation 수명주기 귀속 |
| S02 | P2 | `lib/useLiveEventStream.ts` stale detector가 최초 connect를 캡처. A→B 필터 변경 뒤 50초 정체를 주입하면 연결 URL이 A→B→A | 최신 filter/connect를 읽는 안정적인 effect 경계 |
| S03 | P2 | `RequestEventDrawer.tsx` 및 `latency/LatencyTimeline.tsx`: partial→final에서 표시 중 타임라인이 상세 조회 중 전체 skeleton으로 전환. probe에서 skeleton 24개 확인 | 표시 가능한 데이터 유지, 미도착 detail만 점진 보강 |
| S04 | P2 | `lib/locale.ts`, `lib/theme.ts`: 각 hook instance의 독립 useState. setter 이후 동시에 mounted된 인스턴스들이 dark/light, ko-KR/en-US, Asia/Seoul/UTC로 분리 | 외부 store 구독 또는 provider를 이용한 단일 상태 |
| S05 | P2 | `routes/settings.tsx` ConfigDraftSection textarea는 빈 state, 서버 draft와 미연결. metadata만 서버 revision 표시 | 초기/새 draft 로드, 미저장 초안과 서버 revision 분리 |
| S06 | P2 | `lib/queries.ts` cascade plugin delete는 registry/reference만 invalidate. 서버 chain/warmup 참조 제거 및 revision 변경과 캐시 불일치 | 실제 영향받는 plugin-chain/upstreams 캐시 무효화 |
| S07 | P2 | `routes/audit.tsx` Upstream/Route/Status 필터를 backend `AuditQuery`가 무시. 활성 필터 수만 바뀜 | 지원 가능한 실제 데이터 기준으로 필터 계약 구현 |
| S08 | P2 조건부 | CacheKeepaliveSettingsDrawer의 `[open, principal]` effect. 외부 actor가 같은 principal을 변경하고 reconnect/refetch가 도착하면 미저장 77이 서버 30으로 덮임 | open/id별 초안 초기화와 편집 시작 revision 보존 |
| S09 | P2 | SessionDetailPane 오류 분기만 Back header 누락. ≤960px에서 목록도 숨겨져 drawer 전체 종료 외 복귀 경로 없음 | loading/success/error 공통 복귀 경로 |
| S10 | P2 조건부 | CommandPalette navigate 후 100ms DOM click. 기존 production dist에 실제 lazy route import 확인; cold route가 늦으면 동작 누락. 느린 브라우저 재현은 감사에서 미실행 | URL search로 action intent 전달하고 mount 후 소비 |
| S11 | P1 | Settings Rotate token은 활성 버튼이나 toast mock만 표시, API 호출 없음 | 지원되지 않는 회전을 성공으로 표시하지 않으며 실제 운영 방식에 맞춘 UI |
| P01 | P2 | usePolledData spread가 tracked query props를 전부 읽음. 동일 데이터 refetch: data-only 알림 0, spread 알림 2; data ref는 동일 | tracked result 보존, visibility status를 native query status와 혼합하지 않기 |
| P02 | P2 | RequestEventsTable 200행 중 한 행 변경 또는 동일 props의 parent render에도 getRequestOutcome 200회. unchanged row DOM은 유지됨 | 안정적인 table/row 경계, 부분 갱신별 작업 제한 |
| P03 | P2 | heartbeat-only에서 version/events 불변인데 추가 render 1회. UI 미사용 activity/cursor state가 원인 | 내부 transport 상태는 ref, 사용자 표시 state만 publish |
| P04 | P2 | SSE effect deps의 visible 때문에 grace=false인 hide/show에도 연결 1→2→3 생성 | 실제 pause 여부 전이에만 연결 재구성 |
| P05 | P2 | Logs rows와 pageRows가 같은 live snapshot을 두 번 merge/sort | 사용자 표시·내보내기·필터용 파생 데이터의 중복 정렬 제거 |
| P06 | P2 | 커서 페이지에서 필터 변경 시 새 필터+이전 cursor 요청 뒤 초기 cursor 요청. 늦은 응답 화면 오염은 방어됨 | query 구독 전 filter별 pagination identity 분리 |
| P07 | P2 조건부 | CacheKeepaliveSessionsDrawer 및 principals FLIP 루프에서 rect read/style write 교차 | 전체 rect read 후 style write 수행 |
| P08 | P3 조건부 | TimeRangeStrip 매 draw 치수 대입, 인라인 callback에 따른 listener 재등록, window mousemove에서 불필요한 rect 조회 | 치수 변경 때만 backing size 설정, 안정 callback 및 drag 전역 listener 범위 |
| T01 | P2 | 기본 Vitest 수집에서 usePolledData.test.tsx, useLiveEventStream.test.tsx, liveMergeSessions.test.ts 누락. 명시 실행도 No test files found | 환경별 수집 경로 정리와 실제 상태 전이 검증 |

## 검증됐지만 낮은 우선순위인 부수 관찰

- usePolledData의 `status`가 native pending/error/success를 hidden/live로 덮고, PolledDataResult intersection은 never로 붕괴한다. 현재 production status 소비자 없음. P01과 함께 정리한다.
- upsert reducer의 eviction은 고정 cap(500+500) 범위 선형 스캔이며 무제한 O(N²)가 아니다. cap 미만도 스캔하므로 불필요 작업은 있으나 새로운 heap/list 자료구조가 필요하다는 주장은 채택하지 않는다.
- liveFlashIds가 Map 앞 20개를 선택하여 신규 행이 아니라 오래된 행이 강조 대상이 된다. P02/P05 작업에서 같은 live render 계약으로 처리한다.
- RelativeTime absolute formatting은 매 render에 계산된다. localStorage/자동 timezone 초기화는 매 render가 아니라 mount 시에만 일어난다.
- Hint와 native title 이중 tooltip, plugin 삭제 후 URL 잔류, plugin 내부 raw anchor, session config/raw 접힘 상태 유지: 별도의 저우선 UX 관찰. 핵심 결함 수정에 불필요한 전면 개편 근거로 사용하지 않는다.

## 기각·축소한 주장과 반례

- 세 setState가 세 render를 만든다: React automatic batching을 무시한 주장. 실제 heartbeat render만 측정.
- SSE zombie connection, reconnect timer leak: generation guard/cleanup이 방어. 반면 stale closure와 visibility churn은 별도 재현됨.
- Principal modal이 열린 채 sidebar 클릭으로 secret/삭제 대상 혼입: modal backdrop 때문에 해당 직접 조작 불가. inline editor 문제만 확정.
- PluginDetail/RequestDetail/SettingsCard에 key가 없으므로 항상 identity 누출: 실제 unmount 경로 및 상위 DetailView key가 방어하는 경우 제외.
- 상대시간 queryFn의 Date.now 자체가 잘못된 cache key: rolling resource의 의도된 계약이므로 기존 수정 유지.
- Logs next cursor 무한루프, Clear 뒤 과거 histogram 유지가 반드시 결함: 반례와 의도된 독립 view 상태가 있어 기각.
- Fable legend 상수화: 의도된 계약. 미사용 import만으로 Recharts bundle 증가: tree shaking 분석 없이 확정 불가.
- getBoundingClientRect마다 강제 reflow/GPU texture 파기: 과장. 실제 layout 비용/FPS는 감사에서 측정하지 않음.
- CommandPalette 5초 polling: 실제 30초, 같은 query key는 dedupe되며 visibility gating 적용. 상시 query를 없애는 것은 사전 로딩 UX tradeoff라 자동 변경하지 않음.
- TimeRangeBounds committed prop 동기화 자체는 정상. 일반 background polling만으로 keepalive draft가 덮인다는 주장은 structural sharing과 비폴링 usePrincipals 때문에 기각.

## 검증 근거

현재 소스의 임시 복사본에서 실행한 React/Vitest probe(저장소 수정 없음):

1. Principal A 초안이 B id/revision으로 제출되는 경로.
2. 동일 principal 외부 revision 도착 시 keepalive draft 77→30.
3. stale SSE URL A→B→A, grace 이내 연결 수 1→2→3, heartbeat-only render 1.
4. 두 preference instance의 불일치.
5. 200행 table의 단일 변경/동일 props에도 outcome 계산 200회.
6. partial→final timeline skeleton 24개.
7. 실제 QueryObserver 동일-data 알림 data-only 0/spread 2.
8. 기본 runner가 상태 테스트 세 파일을 수집하지 않음.

부하의 ms/FPS와 느린 production chunk navigation은 감사에서 측정하지 않았다. 구현 후 실서비스 브라우저 전이 및 작업 횟수 검증으로 보완한다.

## 충돌 없는 소유권과 단계

- 공통 Query 소유자: `queries.ts`, `usePolledData.ts`; tracked result, cascade cache, pagination query 계약 담당.
- SSE 소유자: `useLiveEventStream.ts`, `upsertReducer.ts` 및 직접 테스트. route 수정 금지.
- Preferences 소유자: `locale.ts`, `theme.ts`, 필요 최소 shared store 및 테스트. public hook 이름/반환 필드는 유지.
- Principal 소유자: `principals.tsx`와 route 테스트. identity draft, 해당 파일 FLIP, URL action=new 소비.
- Settings 소유자: `settings.tsx`와 관련 route 테스트. server draft, 잘못된 rotation UI.
- Keepalive 소유자: 세션/설정 drawer와 detail pane, 해당 테스트. principal route 수정 금지.
- Table/detail 소유자: RequestEventsTable, RequestEventDrawer/LatencyTimeline, 해당 테스트. route callbacks는 Logs 소유자에게 계약 전달.
- Logs/Overview 소유자: `logs.tsx`, `index.tsx`, `logRows.ts`와 route 테스트. 중복 sort, pagination identity, flash id.
- Canvas 소유자: TimeRangeStrip와 테스트. parent callback API 유지.
- Audit 소유자: audit route, query filter DTO 및 backend/storage 필요 범위. 공통 queries.ts 변경은 Query 소유자만 수행.
- Navigation 소유자: CommandPalette, upstream route action, plugin upload action. Principal/Settings route는 각 소유자가 URL action 계약을 소비.
- Runner 소유자: Vitest config와 누락 테스트. SSE/Query 테스트는 각 소유자가 수정하고 runner만 수집 규칙을 소유.

모든 하위 에이전트는 빌드·테스트·포맷을 실행하지 않는다. 통합 소유자만 병렬 편집 종료 후 포맷/타입/동작/실브라우저 검증을 실행한다. 실패는 원인과 파일 소유자를 지정해 수정 후 재검증한다. PR 생성은 승인된 저장소의 규칙을 따르며 merge는 요청되지 않았다.

## 구현 및 검증 결과

기반 플리커링 변경은 PR #705 (`isac322/fix-flickering`, commit `6dbb0dcc`)에 분리했다. 본 문서는 후속 `isac322/frontend-state-boundaries` 브랜치에 먼저 커밋했고, 파일별 단일 소유자로 병렬 구현했다.

- S01–S11, P01–P08, T01: 구현 및 관련 회귀 검증 완료. S07은 잘못 복제된 request-log 필터를 제거하고 실제 Audit API의 principal/since/until 계약을 사용한다. S11은 지원하지 않는 회전을 새로 구현하지 않고 정확한 환경변수/서비스 재시작 안내로 교정한다.
- 독립 리뷰에서 추가 확인한 empty-cursor backfill 정지, pause 중 실패 시간 누적, 초기 draft 조회 오류의 영구 loading, 폐기된 concurrent render의 pagination checkpoint, upload modal 전환 중 mutation callback 유실도 수정했다.
- 실제 브라우저 검증에서 Audit `request_id` 중복에 따른 ghost row를 발견했다. API 13행이 화면 17행으로 남던 결함을 composite+occurrence identity로 수정했으며, 이벤트 삭제/dedupe 없이 필터 전환 뒤 API와 화면 행 수가 일치함을 확인했다.
- 최종 로컬 gates: `bun run typecheck`, `bun run lint`, `bun run build` 통과. `bun run test --run`: 70개 파일, 633개 테스트 통과. 기존 Biome schema 버전 안내 1건은 오류가 아니며 이 작업에서 의존성을 변경하지 않았다.
- 실제 SQLite/admin API/browser: Principal A/B 초안 격리, 실제 409 `storage_conflict`와 dirty draft 보존, 설정 초안 저장, locale/timezone/Toaster 동기화, 모바일 session error/Back 통과.
- production preview/browser: lazy chunk를 350ms 이상 보류한 command action, Logs cursor/filter/Canvas DPR·drag·polling 및 실제 DB→API→UI 변경, Audit principal/시간 필터와 중복 ID 행 보존 통과.
- request partial→final은 실제 compiled component browser harness와 실제 backend detail 응답으로 검증했다. backend in-process partial bus를 외부 SQLite 쓰기로 발행할 수 없어 proxy/SSE full-stack 검증으로 표현하지 않는다.
- 독립 counterproof: 동일 payload query render 0회, 변경 payload 1회; 200행 table의 동일 props 계산 0회, 한 행 변경 계산 1회(나머지 199행 DOM 유지); heartbeat/cursor 추가 render 0회; grace 이내 hide/show 연결 1개 유지, close 0회.
- 원문 실행 로그 및 화면 증거는 격리 QA 산출물로 보존했고, 재현 절차는 `.agents/skills/user-flow-qa/references/scenarios/frontend-state-boundaries.md`에 정리했다.
