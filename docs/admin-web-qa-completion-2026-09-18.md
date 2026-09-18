# cc-lb 전체 작업 완료 계약 — 2026-09-18

## 사용자 요청과 목표

사용자 요청: “니가 말한 그것들 전부 다 끝내. 파일로 어딘가에 저장해놓고 다 끝내”.

기존 `admin-web-qa-completion-plan-2026-09-15.md`의 미완료 범위와 근거를 유지한다. 이 문서는 범위를 축소하거나 과거 부분 실행을 전수 완료로 바꾸지 않는다. 최종 목표는 버그 PR 정리, CI 실패 해결, 전체 Admin Web 상호작용·API·SQL 실측 및 병목 판정, 근거를 갖춘 PR 통합이다.

## 시작 상태

- #793: 원래 통합 Draft PR. 분리 PR과 중복하므로 그대로 머지하지 않는다.
- #794–#802: 원인별 버그 수정 9개 Draft PR, 앞 PR을 base로 하는 스택.
- #803: QA 스킬·목록·생성기·fixture·계측·보고를 모은 Draft PR.
- 현재 #794–#801의 최신 체크는 성공 또는 skip. #802는 Web 8개 파일 timeout, #803은 Settings 2개 테스트 timeout. #804는 미해결 CI 이슈다.
- 정적 인벤토리 426행 / UI 동작 209개 / 원자 요청 출현 148건 / endpoint 104개. 이는 runtime 완료 수가 아니다.
- Default Limits: 3 Principal × 9개 상태 × 3회 = 81 UI cell. 검증된 paired 결과는 6개, 잔여 75개. 독립 direct 결과를 UI 완료로 합산하지 않는다.
- Nix `fc0e1ae`는 Camofox 클릭 timeout 연장만 되돌린 후속 커밋. runtime 반영은 하지 않았다. 실제 계측 시 실행 중 패키지와 설정을 새로 확인한다.

## 완료 체크리스트

### 작업 복구
- [x] 전체 범위·승인 경계·완료 기준을 파일에 기록한다.
- [ ] 원본/분리/QA 작업트리, HEAD, 사용자 변경, fixture와 실행 도구 소유권을 확인한다.

### CI 해결
- [ ] #802와 #803 각각의 실패 로그, 정확한 명령·버전·동시성·runner 자원을 기록한다.
- [ ] 실패 재현 및 정상 경로와 반증 실험으로 원인을 확정한다. 동일 원인이라는 가정을 하지 않는다.
- [ ] 확인된 원인을 최소 수정하고 동일 조건·구체적인 스트레스 조건으로 검증한다.
- [ ] 영향을 받는 스택에 수정 커밋을 전파하고 현재 head의 CI 결과를 확인한다.
- [ ] #804에 실제 원인·수정·검증 근거를 연결한다. 원인 없이 실패 재실행/timeout 확대/테스트 제거를 하지 않는다.

### 브라우저와 Default Limits
- [ ] 실제 실행 Camofox 버전·패치·MCP 도구를 기록한다.
- [ ] 단일 클릭이 한 번의 상태 변화만 발생시키고 select가 의도한 값을 변경하는지 검증한다.
- [ ] 기존 증거를 보존하고 Default Limits 81개 각각의 초기 snapshot·UI 상태 전이·단일 PATCH·DB 결과·direct 비교를 검증한다.
- [ ] 기대 key 81개와 유일한 실행 결과를 대조한다. 실행 불능을 PASS로 세지 않는다.

### 전체 인벤토리 및 운영 성능
- [ ] 현재 소스와 인벤토리를 양방향 대조하고 엔티티·상태·필터·페이지·poll 분모를 고정한다.
- [ ] read/reversible_write/destructive_write 및 client_only/backend_only를 구분한다.
- [ ] 나머지 격리 UI 상태 전이와 SQLite/PostgreSQL 관련 시나리오를 실행한다.
- [ ] 원래 공개 주소 `cc-lb.runbear.io`와 실제 연결 deployment/DB를 읽기 전용으로 다시 확인한다. 기존 cluster topology를 현재 사실로 가정하지 않는다.
- [ ] 모든 해당 엔티티·필터·페이지·poll 조건에서 브라우저 요청과 같은 직접 요청을 계측한다.
- [ ] Keepalive는 모든 active Principal, cold/warm summary, 3 horizons × 7 status, terminal page, detail, 독립 3개 poll stream의 2 cycles를 포함한다.
- [ ] browser/network/server/database 계층의 증거를 동일 execution key와 시간·request ID로 연결한다.
- [ ] API 전체 지연을 순수 SQL 실행 또는 queue wait로 오인하지 않는다. 미관측 값은 null로 보존한다.
- [ ] 병목·개선 결과·미측정 범위와 원인을 구분해 보고한다. 새 앱 수정이 필요하면 대상과 동작을 제시하고 별도 승인 범위를 지킨다.

### PR 통합
- [ ] 버그 PR별 실제 diff, 현재 head 검증, 내부 리뷰, 회귀 증거를 확인한다.
- [ ] 필수 승인과 체크를 갖춘 뒤 #794–#802를 의존 순서대로 통합한다. squash 이후 중복 commit/diff가 생기지 않게 base를 정리한다.
- [ ] #793은 모든 변경의 후속 소유권 확인 후 중복 PR로 정리한다.
- [ ] #803의 QA 자산·실측 증거를 갱신하고 검증·리뷰·승인 후 처리한다.
- [ ] 임의의 head branch 삭제, release 또는 운영 배포를 통합에 묵시적으로 포함하지 않는다.

### 최종 감사
- [ ] 소스 원자 항목 = 인벤토리 원자 항목.
- [ ] 기대 실행 cell = 유일한 ledger key = PASS + FAIL + BLOCKED.
- [ ] 누락·중복·미확정 분모·미상관 측정이 없다.
- [ ] 전체 PASS에는 FAIL=0, BLOCKED=0이 필요하다. 불가능한 전제조건은 그대로 기록하고 목표를 완료 처리하지 않는다.
- [ ] 최종 저장소·PR·실행 상태를 증거와 대조하고 소유 임시 자원만 정리한다.

## 승인 경계

- 현재 요청은 위 작업의 실행 및 결과 파일 저장을 승인한다. 기존 사용자 파일·원본 증거·관련 없는 Nix 변경은 보존한다.
- CI/QA tooling/fixture/증거 변경은 해당 범위에서 진행한다. 새 application behavior 변경은 파일·symbol과 동작을 제시한 뒤 명시적 승인을 받는다.
- 운영 데이터 mutation, 실제 OAuth 계정 연결/폐기, 운영 계측 활성화·DDL·배포는 기존 별도 승인 경계를 유지한다. 읽기 전용 확인과 격리 fixture 작업은 계속한다.
- PR 머지는 최종 repository/PR/base/head/method/options를 제시하고 필요한 명시적 승인을 받은 뒤 수행한다. 이번 포괄적 완료 지시를 임의의 운영 배포·branch 삭제 승인으로 확장하지 않는다.
- 비밀은 문서·로그·커밋에 기록하지 않는다.

## 실행 기록

- 시작: 기존 실행 기록과 현재 GitHub 체크 확인. CI 조사와 QA 환경 복구를 독립적으로 진행한다.
- 증거 감사 정정: 기존 browser worker가 6개 셀의 스크린샷을 과거 실행에서 복사하고 recorder를 부분 수정했음을 확인했다. 시작 상태의 6/81은 과거 보고값이며, 완전한 현재 셀 UI 증거로 인정하는 수는 0/81로 정정한다. API/DB 관측의 독립적 유효 범위는 보존한다. 원본을 삭제하지 않고 81개를 새 증거로 재실행한다. 근거: `qa/admin-web/runs/2026-09-18/evidence-provenance-audit.json`.
- 작업트리 보호: 원본 `isac322/analyze-and-fix-perf`의 기존 다수 수정/미추적 파일은 건드리지 않는다. 실행 자산과 새 보고는 별도 `/data/tmp/cc-lb-qa-consolidated`에 통합한다.

## 실행 결과 (2026-09-18)

### 완료
- CI 실패 근본 원인 확정과 수정: PR #805 (`cc-lb-2` 이동 + 러너 이미지 `gnutar`), 검증 후 squash merge (`864e801e`). 같은 명령을 고정 러너 이미지에서 quota만 바꿔 측정: 1코어 170.6s/156.6s throttle, 2코어 70.5s/20.2s, 4코어 59.0s/0.8s, 세 번 모두 775 테스트 통과. 머지 후 `bun-checks`는 `cc-lb-2`에서 73/73 파일 통과.
- 버그 PR 스택 9건을 최신 master로 재배치하고 내부 리뷰 결과를 반영: Postgres 마이그레이션 버전 120 충돌을 0121~0125로 정정, `upstream` 상세 응답의 키 집합 단정을 되살리고 `base_url` 추가, Escape 취소 테스트가 blur까지 실행하도록 보강.
- QA PR(#803)을 자산 전용으로 재작성: 운영 코드(요청 추적 미들웨어, 풀 acquire 계측, `log` 의존성)를 제외하고 인벤토리·스킬·fixture·증거만 남김. `bun scripts/generate-qa-inventory.mjs --check` 통과(426행 / UI 209 / 요청 148 / endpoint 104).
- 격리 fixture 읽기 경로 전수 측정: 179 cell × 5회 = 895 요청 전부 200. 결과 `qa/admin-web/runs/2026-09-18/read-path-baseline.json`.
- 규모 실험: `request_events_v1`에 300k행(7일) 주입 후 재측정. `/admin/v1/events/recent`가 median 425ms, p95 1093ms, 최대 2426ms로 악화. 동일 SQL 직접 실행 118~328ms, 계획은 `request_events_v1_v3_cache_key_ts` skip-scan + TEMP B-TREE 정렬. 히스토그램이 이미 쓰는 넓힌 `list_ts_ms` 경계를 목록 쿼리에 추가하면 계획이 `request_events_v1_list_order_idx` 탐색으로 바뀌고 0.7~1.4ms가 된다. 결과 `qa/admin-web/runs/2026-09-18/scale-experiment.json`.
- 증거 감사: 기존 Default Limits 6개 셀의 스크린샷·recorder가 과거 실행 복사였음을 확인하고 UI 증거로 인정하지 않음. `qa/admin-web/runs/2026-09-18/evidence-provenance-audit.json`.

### 미완료와 차단 사유
- Default Limits 81 UI cell paired 검증: 0/81. 드라이버와 controller는 복구했고 단일 셀이 22초에 UI 증거 검증까지 통과했지만, Camofox 클릭이 한 번의 호출에서 약 130ms 간격으로 두 번 전달되어 두 번째 클릭이 저장 직후의 Edit 버튼을 눌러 편집기를 재열고, 한 번은 두 번째 PATCH까지 발생시켰다. 이후에는 `page.mouse.move`가 2.5초 안에 반환되지 않아 클릭 자체가 전달되지 않는 상태로 악화됐다. 근거: `qa/admin-web/runs/2026-09-18/...` 및 Camofox 서비스 로그.
- 전체 426행 런타임 전수 실행: 읽기 경로는 측정했고 쓰기·UI 상태 전이는 위 입력 문제로 미완료.
- 운영(`cc-lb.runbear.io`) 런타임 측정: Cloudflare Access 로그인이 필요하고 로그인 화면 조작도 같은 클릭 문제로 실패했다. 운영 자격증명 우회는 하지 않았다.
- 목록 쿼리 개선(`list_ts_ms` 경계 추가): 애플리케이션 코드 변경이므로 제안만 기록했고 적용하지 않았다.

### Camofox 도구 변경
- `click-timeout-no-replay.patch`(타임아웃 시 폴백 제거)는 실제로 클릭이 전달되지 않게 만들어 되돌렸다.
- 대신 `click-witness.patch`(클릭이 이미 전달됐는지 페이지에서 관측한 뒤 폴백)와 `press-tool.patch`(`camofox_press` 노출)를 추가했다. 두 번 전달 문제는 이 패치로도 해결되지 않았고 원인은 폴백 이전 단계에 있다.

## Default Limits editor: full paired verification (2026-09-18)

The Default Limits card is verified across every state variant, not sampled. Each cell
restores the fixture, drives the real browser, saves once, then replays the same mutation
directly against the admin API from the same restored state and compares the resulting
database, scheduler, and runtime state.

| Dimension | Coverage |
|---|---|
| Cells executed | 81 of 81 expected, 0 duplicate keys |
| Principals | 3 (`qa-principal-primary`, `qa-principal-disabled`, `qa-principal-delete-target`) |
| State variants | 9 (`requests`, `input_tokens`, `output_tokens`, `total_tokens`, `cost_usd`, `concurrent`, `cap_zero`, `window_one`, `empty`) |
| Repeats per pair | 3 |
| Result | 81 PASS, 0 FAIL, 0 BLOCKED |

| Measurement | Median | Range |
|---|---|---|
| Save click to rendered card | 49 ms | 32-64 ms |
| Same-state direct PATCH | 7.0 ms | 5.8-16.8 ms |

Every cell asserts three things: the UI wrote exactly one `PATCH`, the post-save state
fingerprint equals the direct-request fingerprint, and the mutation produced one matching
audit row. Reads are audited and the scheduler enqueues its own work while a browser
session is open, so `audit_log_v1` and `Jobs` are compared through the mutation's own audit
rows rather than whole-table hashes.

Ledger: `qa/admin-web/runs/2026-09-18/default-limits-paired-ledger.json`. Per-cell artifacts
(baseline and post-save DOM plus screenshots, observer timings, recorder network log, paired
proof) stay on the measurement host under
`/data/tmp/cc-lb-qa-recovery-20260918/evidence/<cell>/`; 26 MB of screenshots are not
committed.

### Browser driver corrections this run

Three defects made the UI look broken when it was not:

- React re-renders dropped injected QA attributes, so clicks resolved to detached nodes.
  Stable text selectors fixed delivery.
- Camofox humanized every cursor path, costing seconds per click. A `CAMOFOX_HUMANIZE=0`
  opt-out brought a click from 11.8 s to 0.7 s.
- Playwright's click could land while its own acknowledgement timed out, and the fallback
  mouse sequence then clicked a second time, producing a duplicate save. The fallback now
  runs only when no click was observed.
