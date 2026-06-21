# BDD OoS-Manual Scenarios (M0 deliverable, plan v3)

- 작성일: 2026-06-18
- 작성 근거: plan v3 §7.3 "OoS-manual 처리 (정확 ID 목록 게이트)"
- M1 진입 차단 조건: 본 표가 비어 있거나 불완전하면 M0 불통과
- Invariant: `converted + OoS-manual + blocked == 321` (모든 마일스톤 게이트)

## OoS-Manual 판정 기준

다음 조건 중 **모두 만족** 시 OoS-manual:
1. 자동화된 단언이 시나리오의 핵심 가치를 보존하지 못함 (예: "사람이 보기에 자연스러워야 한다")
2. 자동 단언으로 대체하면 시나리오 의도가 왜곡됨
3. 다른 보조 트랙 (visual-qa / playwright / 수동 운영자 점검) 으로 대체 가능

자동화 가능 여부가 모호한 시나리오 (예: F8.2 "운영자 대시보드에 사유가 표시된다") 는 **자동화 가능** 으로 분류 — JSON API 응답 + DOM 표현 단언으로 커버 가능하므로 OoS 아님.

## 현재 OoS-Manual 목록 (2건)

| ID | Writer | Persona | v5.2 한글 제목 (출처) | 자동화 불가 사유 | 대체 트랙 | 검증 책임 |
|---|---|---|---|---|---|---|
| F4.1c | W1 | Alice | 한 팀 줄을 누르면 그 팀의 자세한 보기로 이어진다 | "한 팀 줄을 누르면" = 사람의 클릭 + UI 페이지 전환 시각 확인. 시각적 흐름 자체가 의도. JSON API 응답 단언으로 대체하면 시나리오 의도 (사용자 UX) 가 왜곡됨 | playwright + visual-qa 트랙 (frontend-fanout-qa skill) | Frontend 팀 v6 라운드 |
| F4.11b | W1 | Alice | 두 표식의 의미가 사용자 인지 도움말로 함께 안내된다 | "사용자 인지 도움말" = 툴팁/배지의 사람 가독성. DOM 단언으로 표식 존재 여부 + tooltip 텍스트 비교는 가능하나 "인지 도움" 의 의도는 사람 검수 필요 | playwright DOM 단언 (자동) + visual-qa 검수 (수동) 의 조합. **자동 단언 부분은 별도 시나리오로 분리해 자동화 트랙으로 유지** | Frontend 팀 v6 라운드 |

## 자동화 가능 분류 (참고용)

다음은 표면적으로는 "사람이 본다" / "한 화면에서 보인다" 류 표현이지만 **자동화 가능** 으로 분류된 시나리오 (참고): F1.1a (활성 + 첫 키 한 화면 = JSON 응답 두 필드 단언), F4.1a/b (대시보드 = JSON API), F4.10 (사용량 그래프 = 응답 시리즈 단언), F2.5 (키 목록 화면 = 응답 본문에 secret 미포함 단언), F5.8 (자격증명 상태 한 화면 = JSON status field).

## M1 게이트 의무 사항

M0 → M1 진입 시점에 본 표가 다음을 만족해야 한다:
1. 모든 OoS row 가 `id` + `자동화 불가 사유` + `대체 트랙` + `검증 책임` 4 컬럼 모두 채워져 있을 것
2. OoS row 수 + 자동화 가능 row 수 + blocked row 수 == 321 (invariant)
3. 본 표가 마지막으로 갱신된 시점이 M1 진입 시점 7일 이내일 것 (stale 방지)

위 3 조건 중 하나라도 위반 시 M1 진입 차단.

## 갱신 정책

- M0~M5 진행 중 새로운 OoS 후보 발견 시 즉시 본 표에 추가 (commit 메시지에 "OoS-manual: F<x.y> reason=..." 명시)
- 자동화 가능으로 재분류 시 row 제거 + 매핑 표 (`bdd-test-conversion-map.md`) row 의 `Status` 컬럼 갱신
