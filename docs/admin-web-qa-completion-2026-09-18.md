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
