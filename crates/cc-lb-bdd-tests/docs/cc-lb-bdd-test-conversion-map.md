# BDD Scenario → Rust Test Function Mapping (M0 deliverable for plan v3)

- 시나리오 총수: 321 (v5.2)
- 자동 추출 일자: 2026-06-18
- 매핑 규칙: 시나리오 ID `Fn.m[suffix]` → 함수명 `fn_m[suffix]` (lowercase, dot→underscore). Backend matrix는 `_sqlite` / `_postgres` suffix 가 매크로 expansion 으로 자동 부여.
- 영문 source 결정 (v3 §0): step text/title/doc/panic 메시지는 영문. 본 표의 "v5.2 한글 제목" 컬럼은 영문 번역의 출처 보존용. 영문 step text 번역은 M1 산출.
- Fast 컬럼: PR 게이트 (~30건 후보, 33개 표시). nextest filter `test(/^fast_/)` 가 대상.
- Status 컬럼: 초기값 `pending`. M1 부터 `RED→GREEN` / `STABLE` / `OoS-manual` / `blocked-<reason>` 로 갱신.
- Persona 컬럼: Feature 단위 기본값. Multi-persona 시나리오 (예: Alice→Bob hand-off) 는 M1 에서 행 단위 재지정.

## 합계

| Writer | Scenarios | Features |
|---|---:|---:|
| W1 (team/traffic/dashboard/cache/health) | 98 | 7 |
| W2 (credential/incident) | 66 | 7 |
| W3 (policy/plugin) | 63 | 6 |
| W4 (platform/audit/lifecycle/backend/cost/secret) | 94 | 7 |
| **TOTAL** | **321** | **27** |

Invariant (every milestone gate): `converted + OoS-manual + blocked == 321`.

## 매핑 표 (321 행)

| Writer | ID | Feature | Persona (default) | fn_name | Fast? | Backend | Status | v5.2 한글 제목 (출처) |
|---|---|---|---|---|:---:|---|---|---|
| W1 | F1.1a | F1 | Alice | `f1_1a` | ✓ | both | pending | 새 팀 등록은 활성 상태와 첫 키를 같은 화면에서 한 번에 마무리한다 |
| W1 | F1.1b | F1 | Alice | `f1_1b` | ✓ | both | pending | 새 팀 등록은 누가 언제 만들었는지를 감사 기록에 남긴다 |
| W1 | F1.2 | F1 | Alice | `f1_2` |  | both | pending | 발급된 키는 등록 직후 한 번만 보여진다 |
| W1 | F1.3 | F1 | Alice | `f1_3` |  | both | pending | 다른 운영자가 그 사이 같은 팀을 바꿨다면 두 번째 저장은 멈춰진다 |
| W1 | F1.4 | F1 | Alice | `f1_4` | ✓ | both | pending | 비활성화된 팀의 호출은 받아들여지지 않는다 |
| W1 | F1.5 | F1 | Alice | `f1_5` | ✓ | both | pending | 팀에 허용된 모델 범위를 벗어나는 호출은 거부된다 |
| W1 | F1.7 | F1 | Alice | `f1_7` | ✓ | both | pending | 팀을 삭제해도 그 팀이 했던 일의 감사 흔적은 사라지지 않는다 |
| W1 | F1.8 | F1 | Alice | `f1_8` |  | both | pending | 식별자에 금지 문자나 예약된 머리말이 들어가면 등록이 거부된다 |
| W1 | F1.10 | F1 | Alice | `f1_10` |  | both | pending | 비활성 팀을 다시 활성으로 되돌리면 같은 비밀로 곧 받아들여진다 |
| W1 | F1.11 | F1 | Alice | `f1_11` |  | both | pending | 비활성화 직전에 시작된 호출은 마무리되고 새 호출만 거부된다 |
| W1 | F2.1 | F2 | Alice | `f2_1` | ✓ | both | pending | 새 키 발급 시 비밀은 단 한 번만 보여진다 |
| W1 | F2.2 | F2 | Alice | `f2_2` |  | both | pending | 키 회전 시 새 키와 옛 키가 짧은 겹침 기간을 갖는다 |
| W1 | F2.3 | F2 | Alice | `f2_3` | ✓ | both | pending | 키 회수 즉시 다음 호출부터 거부된다 |
| W1 | F2.4a | F2 | Alice | `f2_4a` |  | both | pending | 정해진 시점에 자동으로 만료되는 키는 그 시점에 끊긴다 |
| W1 | F2.4b | F2 | Alice | `f2_4b` |  | both | pending | 만료 임박 알림이 운영자에게 미리 도착한다 |
| W1 | F2.5 | F2 | Alice | `f2_5` | ✓ | both | pending | 키 목록 화면에는 비밀이 결코 함께 보이지 않는다 |
| W1 | F2.6 | F2 | Alice | `f2_6` |  | both | pending | 키 보유자 이름과 메모를 운영자가 나중에 고칠 수 있다 |
| W1 | F2.8a | F2 | Alice | `f2_8a` |  | both | pending | 사용량 보고는 키 보유자 단위로 호출 수와 거부 사유를 나눠 보여준다 |
| W1 | F2.8b | F2 | Alice | `f2_8b` |  | both | pending | 보유자 이름을 고치면 옛 사용량도 새 이름으로 통일되어 보인다 |
| W1 | F2.10 | F2 | Alice | `f2_10` |  | both | pending | 한 보유자가 동시에 가질 수 있는 활성 키 수가 정해져 있다 |
| W1 | F2.11 | F2 | Alice | `f2_11` |  | both | pending | 키의 마지막 몇 자리를 다시 들여다본 사실도 감사 대상이 된다 |
| W1 | F2.12a | F2 | Alice | `f2_12a` |  | both | pending | 키 보유자가 사람인지 머신인지가 목록 화면에서 한눈에 구분된다 |
| W1 | F2.12b | F2 | Alice | `f2_12b` |  | both | pending | 사용량 보고에서도 사람/머신 단위가 따로 합산되어 보인다 |
| W1 | F2.13a | F2 | Alice | `f2_13a` |  | both | pending | 키 상태는 활성→일시중지→회수의 순서로 다뤄진다 |
| W1 | F2.13b | F2 | Alice | `f2_13b` |  | both | pending | 키 상태 전이마다 누가 언제 바꿨는지가 감사 기록에 남는다 |
| W1 | F2.14 | F2 | Alice | `f2_14` |  | both | pending | 회전을 지원하지 않는 저장 백엔드에서 키 회전을 시도하면 친절히 거부된다 (신규 v5) |
| W1 | F3.1 | F3 | Bob | `f3_1` | ✓ | both | pending | 정상 키로 정상 모델을 부르면 응답이 그대로 도착한다 |
| W1 | F3.2 | F3 | Bob | `f3_2` |  | both | pending | 스트리밍 응답이 끊김 없이 끝까지 흐른다 |
| W1 | F3.3 | F3 | Bob | `f3_3` | ✓ | both | pending | 알 수 없는 키로 보낸 호출은 친절히 거부된다 |
| W1 | F3.4 | F3 | Bob | `f3_4` | ✓ | both | pending | 비활성 팀의 키로 보낸 호출은 거부된다 |
| W1 | F3.5a | F3 | Bob | `f3_5a` |  | both | pending | 자기 팀에 허용되지 않은 모델을 부르면 그 호출이 거부된다 |
| W1 | F3.5b | F3 | Bob | `f3_5b` |  | both | pending | 허용되지 않은 모델 거부 메시지는 Claude 형식의 오류 봉투로 전달된다 |
| W1 | F3.5c | F3 | Bob | `f3_5c` |  | both | pending | 허용되지 않은 모델 거부가 사용량 보고와 감사 묶음에 함께 더해진다 |
| W1 | F3.8 | F3 | Bob | `f3_8` |  | both | pending | 응답 도중에 cc-lb 안에서 갑작스런 문제가 생기면 Claude 형식의 오류로 마감된다 |
| W1 | F3.9 | F3 | Bob | `f3_9` |  | both | pending | Bob은 파일을 올리고 받고 지우는 흐름을 cc-lb를 통해 마무리한다 |
| W1 | F3.11a | F3 | Bob | `f3_11a` |  | both | pending | 알 수 없는 경로나 알 수 없는 동작으로 보낸 호출은 Claude 형식의 오류 봉투로 돌아 |
| W1 | F3.11b | F3 | Bob | `f3_11b` |  | both | pending | 알 수 없는 경로/동작의 거부가 사용량 보고의 거부 사유 종류별에 더해진다 |
| W1 | F3.12 | F3 | Bob | `f3_12` |  | both | pending | 한도로 거부될 때 "언제 다시 시도하면 되는지"가 같은 응답에 들어 있다 |
| W1 | F3.13 | F3 | Bob | `f3_13` |  | both | pending | 응답 본문이 미리 정한 상한을 넘기면 친절히 끊긴다 |
| W1 | F3.14 | F3 | Bob | `f3_14` |  | both | pending | 등록된 통로 외의 외부 호스트로 곧장 나가려 하면 막힌다 |
| W1 | F3.15 | F3 | Bob | `f3_15` |  | both | pending | 한 단계만 머무는 전달 표식들은 응답 경계에서 정리된다 |
| W1 | F3.16 | F3 | Bob | `f3_16` |  | both | pending | 격리 시험 환경에서 인증을 임시로 끈 사실은 분명히 표시된다 |
| W1 | F4.1a | F4 | Alice | `f4_1a` |  | both | pending | 어제 가장 많이 쓴 팀이 비용 순서로 한눈에 보인다 |
| W1 | F4.1b | F4 | Alice | `f4_1b` |  | both | pending | 같은 표에 호출 수 대 거부 비율이 함께 보인다 |
| W1 | F4.1c | F4 | Alice | `f4_1c` |  | both | OoS-manual (visual assertion) | 한 팀 줄을 누르면 그 팀의 자세한 보기로 이어진다 |
| W1 | F4.2 | F4 | Alice | `f4_2` |  | both | pending | 기간·팀·키 보유자·모델 중 골라서 좁혀 본다 |
| W1 | F4.3 | F4 | Alice | `f4_3` |  | both | pending | 한 달 비용이 정해진 한도에 가까워지면 같은 화면에서 미리 보인다 |
| W1 | F4.4a | F4 | Alice | `f4_4a` |  | both | pending | 모델별 호출 점유율과 비용 점유율이 같은 화면에서 한눈에 보인다 |
| W1 | F4.4b | F4 | Alice | `f4_4b` |  | both | pending | 모델 점유율 표에서 한 모델을 누르면 시간대별 흐름으로 이어진다 |
| W1 | F4.5 | F4 | Alice | `f4_5` |  | both | pending | 시간대별 사용 흐름이 끊김 없이 이어진다 |
| W1 | F4.6 | F4 | Alice | `f4_6` |  | both | pending | 거부 사유가 종류별로 한눈에 보인다 |
| W1 | F4.7 | F4 | Alice | `f4_7` |  | both | pending | 보고 화면을 표 형식으로 내려받을 수 있다 |
| W1 | F4.8 | F4 | Alice | `f4_8` |  | both | pending | 실시간 흐름이 끊겼다는 사실이 같은 화면에서 분명히 보인다 |
| W1 | F4.10 | F4 | Alice | `f4_10` |  | both | pending | 로그 페이지에서 종류·팀·보유자를 골라 좁혀 본다 |
| W1 | F4.11a | F4 | Alice | `f4_11a` |  | both | pending | "0건"과 "아직 모름"이 같은 표 안에서 분명히 구분된다 |
| W1 | F4.11b | F4 | Alice | `f4_11b` |  | both | OoS-manual (visual assertion) | 두 표식의 의미가 사용자 인지 도움말로 함께 안내된다 |
| W1 | F4.11c | F4 | Alice | `f4_11c` |  | both | pending | 같은 구분이 시간대별 흐름 그림 위에도 그대로 표시된다 |
| W1 | F4.12 | F4 | Alice | `f4_12` |  | both | pending | 한 호출의 단계별 머문 시간을 자세히 본다 |
| W1 | F6.1 | F6 | Alice | `f6_1` |  | both | pending | 분당 한도를 넘으면 그 팀의 다음 호출이 잠시 거부된다 |
| W1 | F6.2a | F6 | Alice | `f6_2a` |  | both | pending | 일일 한도가 차면 그날의 나머지 호출이 거부되고 다음 날 같은 시각에 다시 비워진다 |
| W1 | F6.2b | F6 | Alice | `f6_2b` |  | both | pending | 일일 한도 거부 안내에는 한도가 다시 비워지는 시점이 들어 있다 |
| W1 | F6.3 | F6 | Alice | `f6_3` |  | both | pending | 월 비용 한도가 가까워지면 새 호출이 미리 거부된다 |
| W1 | F6.4 | F6 | Alice | `f6_4` |  | both | pending | 허용 모델 묶음 밖으로 부르면 거부된다 |
| W1 | F6.5 | F6 | Alice | `f6_5` |  | both | pending | 한도 편집 직후의 호출에 새 한도가 곧 반영된다 |
| W1 | F6.8 | F6 | Alice | `f6_8` |  | both | pending | 한도 위반이 거부 사유 종류별 보고에서 따로 보인다 |
| W1 | F6.9 | F6 | Alice | `f6_9` |  | both | pending | 외부 호스트 허용 목록 밖의 곳으로는 cc-lb가 나가지 않는다 |
| W1 | F6.10a | F6 | Alice | `f6_10a` |  | both | pending | 특정 경로의 본문 크기 한도가 그 경로의 호출에 적용된다 |
| W1 | F6.10b | F6 | Alice | `f6_10b` |  | both | pending | 한 경로의 본문 한도 변경이 다른 경로의 본문 한도에 영향을 주지 않는다 |
| W1 | F6.12 | F6 | Alice | `f6_12` |  | both | pending | 한도 위반으로 잠시 거부되던 팀이 시간이 지나 회복한다 |
| W1 | F6.13 | F6 | Alice | `f6_13` |  | both | pending | 일일 한도를 다 쓴 팀은 다음 날 같은 시각에 자동으로 다시 받아들여진다 |
| W1 | F6.14a | F6 | Alice | `f6_14a` |  | both | pending | 한 팀의 분당 한도 위반이 다른 팀의 호출 받아들임에 영향을 주지 않는다 |
| W1 | F6.14b | F6 | Alice | `f6_14b` |  | both | pending | 한 팀의 폭주가 다른 팀의 회복 시점을 늦추지 않는다 |
| W1 | F6.15 | F6 | Alice | `f6_15` |  | both | pending | 동시에 처리 중인 호출 수에 따로 한도를 정할 수 있다 |
| W1 | F6.16 | F6 | Alice | `f6_16` |  | both | pending | 한도가 "팀"인지 "보유자"인지 "키"인지에 따라 적용 단위가 달라진다 |
| W1 | F6.17 | F6 | Alice | `f6_17` |  | both | pending | 한 키에만 더 작은 한도를 따로 정해 둘 수 있다 |
| W1 | F19.1 | F19 | Alice | `f19_1` |  | both | pending | 같은 의미의 호출이 두 번째에는 같은 곳으로 모인다 |
| W1 | F19.2 | F19 | Alice | `f19_2` |  | both | pending | 캐시 적중률이 같은 화면에서 한눈에 보인다 |
| W1 | F19.3a | F19 | Alice | `f19_3a` |  | both | pending | 캐시 적중으로 아낀 비용이 비용 보고에 따로 표시된다 |
| W1 | F19.3b | F19 | Alice | `f19_3b` |  | both | pending | "캐시 없이라면 들었을 비용"이 가상 비용 라벨로 함께 보인다 |
| W1 | F19.3c | F19 | Alice | `f19_3c` |  | both | pending | 실제 비용과 가상 비용이 시간대별 흐름으로 비교되어 보인다 |
| W1 | F19.4 | F19 | Alice | `f19_4` |  | both | pending | 캐시 친화가 끊기는 경우에는 새로 처리된다 |
| W1 | F19.5 | F19 | Alice | `f19_5` |  | both | pending | 캐시 친화는 팀 경계를 넘지 않는다 |
| W1 | F19.6a | F19 | Alice | `f19_6a` |  | both | pending | 캐시 친화가 작동하는 동안에도 한도와 모델 묶음 위반은 그대로 거부된다 |
| W1 | F19.6b | F19 | Alice | `f19_6b` |  | both | pending | 캐시 적중률은 거부된 호출을 빼고 계산된다 |
| W1 | F19.7 | F19 | Alice | `f19_7` |  | both | pending | 짧게 유지되는 캐시와 길게 유지되는 캐시가 등급으로 분리되어 보인다 |
| W1 | F19.8 | F19 | Alice | `f19_8` |  | both | pending | 비활성 팀의 호출은 캐시 적중을 통해서도 받아들여지지 않는다 |
| W1 | F19.9 | F19 | Alice | `f19_9` |  | both | pending | 한 호출의 캐시 상태가 분명한 분류로 한눈에 보인다 |
| W1 | F19.10 | F19 | Alice | `f19_10` |  | both | pending | 캐시 적중인데 비용 단위가 처음과 어긋난 경우가 따로 표시된다 |
| W1 | F26.1 | F26 | Charlie | `f26_1` | ✓ | both | pending | 살아 있음과 처리 가능이 분리되어 보인다 |
| W1 | F26.2a | F26 | Charlie | `f26_2a` |  | both | pending | 처리 가능 상태가 회복 불가일 때는 계속 "준비 안 됨"으로 답한다 |
| W1 | F26.2b | F26 | Charlie | `f26_2b` |  | both | pending | 처리 가능이 "준비 안 됨"인 동안에도 살아 있음 신호는 분리되어 유지된다 |
| W1 | F26.2c | F26 | Charlie | `f26_2c` |  | both | pending | 무대 뒤의 자동 복귀 시도가 운영 화면에 표식으로 보인다 |
| W1 | F26.3 | F26 | Charlie | `f26_3` |  | both | pending | 살아 있음 본문에 버전·가동 시간·빌드 표식이 함께 보인다 |
| W1 | F26.4 | F26 | Charlie | `f26_4` |  | both | pending | 알림 구독이 끊겼다가 다시 붙으면 그 사이의 변경분을 따라잡는다 |
| W1 | F26.5a | F26 | Charlie | `f26_5a` |  | both | pending | cc-lb가 재시작했음을 분명한 표식으로 알린다 |
| W1 | F26.5b | F26 | Charlie | `f26_5b` |  | both | pending | 재시작 표식이 시간과 함께 감사 기록에 남는다 |
| W1 | F26.6 | F26 | Charlie | `f26_6` |  | both | pending | 지금 진행 중인 호출 수가 같은 화면에서 게이지로 보인다 |
| W1 | F26.7 | F26 | Charlie | `f26_7` |  | both | pending | 점진적 종료 중에는 살아 있음과 처리 가능이 따로 답한다 |
| W2 | F5.1 | F5 | Charlie | `f5_1` | ✓ | both | pending | 만료가 임박한 자격증명을 cc-lb이 자동으로 갱신한다 |
| W2 | F5.2 | F5 | Charlie | `f5_2` |  | both | pending | 자동 갱신이 반복 실패하면 운영자에게 알린다 (v4 F5.2 split — 알림 규칙) |
| W2 | F5.3 | F5 | Charlie | `f5_3` |  | both | pending | 자동 갱신이 반복 실패하면 cc-lb은 백오프 간격을 늘린다 (v4 F5.2 split — |
| W2 | F5.4 | F5 | Charlie | `f5_4` | ✓ | both | pending | 잘못된 자격증명은 등록 단계에서 거부된다 |
| W2 | F5.5 | F5 | Charlie | `f5_5` | ✓ | both | pending | 자격증명을 회수하면 그 자격증명을 쓰던 모든 호출이 즉시 멈춘다 |
| W2 | F5.6 | F5 | Charlie | `f5_6` |  | both | pending | 회수된 자격증명에 대한 감사 기록은 그대로 남는다 |
| W2 | F5.7 | F5 | Charlie | `f5_7` |  | both | pending | 자격증명 보호 키의 권한이 흐트러지면 운영자가 알아챈다 (신규 P3) |
| W2 | F5.8 | F5 | Charlie | `f5_8` |  | both | pending | 자격증명의 상태가 한 화면에서 사람이 읽을 수 있게 보인다 (신규 P3) |
| W2 | F5.9 | F5 | Charlie | `f5_9` |  | both | pending | 같은 자격증명을 두 명이 동시에 고치려 하면 한쪽만 받아들여진다 (신규 P3) |
| W2 | F7.1 | F7 | Charlie | `f7_1` | ✓ | both | pending | 비상 차단을 켜면 모든 호출이 거부된다 |
| W2 | F7.2 | F7 | Charlie | `f7_2` | ✓ | both | pending | 비상 차단을 풀면 정상 상태로 돌아온다 |
| W2 | F7.3 | F7 | Charlie | `f7_3` |  | both | pending | cc-lb이 재시작되어도 비상 차단 상태는 유지된다 |
| W2 | F7.4 | F7 | Charlie | `f7_4` |  | both | pending | 비상 차단 중에도 관리 화면과 대시보드는 작동한다 |
| W2 | F7.5 | F7 | Charlie | `f7_5` | ✓ | both | pending | 차단으로 거부된 응답에는 "운영자가 일시 차단" 의미가 명시된다 |
| W2 | F7.6 | F7 | Charlie | `f7_6` |  | both | pending | 발동과 해제는 두 단계 확인을 요구한다 |
| W2 | F7.7 | F7 | Charlie | `f7_7` |  | both | pending | 비상 차단의 발동·해제 사유가 감사 대상이 된다 (신규 P3) |
| W2 | F8.1 | F8 | Charlie | `f8_1` |  | both | pending | 위로가 일시적으로 느려지면 사용자에게 지연을 알린다 (v4 F8.1 split — 사용자  |
| W2 | F8.2 | F8 | Charlie | `f8_2` |  | both | pending | 위로가 느려지면 운영자 대시보드에 사유가 표시된다 (v4 F8.1 split — 운영자 가 |
| W2 | F8.3 | F8 | Charlie | `f8_3` |  | both | pending | Anthropic의 시간당 한도 초과 응답은 사용자에게 그대로 전달된다 |
| W2 | F8.4 | F8 | Charlie | `f8_4` |  | both | pending | 같은 자격증명이 연속으로 실패하면 잠시 그 자격증명만 막는다 |
| W2 | F8.5 | F8 | Charlie | `f8_5` |  | both | pending | 잠시 막아둔 동안의 새 호출은 빠르게 거부된다 |
| W2 | F8.6 | F8 | Charlie | `f8_6` |  | both | pending | Anthropic이 5xx로 답하면 사용자에게 일시 장애를 안내한다 |
| W2 | F8.7 | F8 | Charlie | `f8_7` | ✓ | both | pending | 한 위로가 죽으면 다른 위로로 자동 우회한다 |
| W2 | F8.8 | F8 | Charlie | `f8_8` |  | both | pending | 모든 위로가 동시에 죽으면 일관된 응답을 돌려준다 |
| W2 | F8.9 | F8 | Charlie | `f8_9` |  | both | pending | 라우팅 추적이 너무 길어지면 잘림 표시와 함께 보인다 |
| W2 | F8.10 | F8 | Charlie | `f8_10` |  | both | pending | backpressure가 걸리면 새 호출은 친절히 거부된다 |
| W2 | F8.11 | F8 | Charlie | `f8_11` |  | both | pending | 위로 별로 동시 호출 격벽이 따로 적용된다 (신규 P3) |
| W2 | F8.12 | F8 | Charlie | `f8_12` |  | both | pending | 안전한 호출만 자동으로 다시 시도된다 (신규 P3) |
| W2 | F8.13 | F8 | Charlie | `f8_13` |  | both | pending | Anthropic이 보낸 한도 안내 헤더가 사용자에게 그대로 전달된다 (신규 P3) |
| W2 | F10.1 | F10 | Alice | `f10_1` | ✓ | both | pending | 운영자가 브라우저로 Anthropic에 동의한다 |
| W2 | F10.2 | F10 | Alice | `f10_2` |  | both | pending | 되돌아옴은 cc-lb이 검증한 뒤에만 받아들인다 |
| W2 | F10.3 | F10 | Alice | `f10_3` |  | both | pending | 동의가 끝나면 자격증명이 등록되고 활성으로 표시된다 |
| W2 | F10.4 | F10 | Alice | `f10_4` |  | both | pending | 잘못된 되돌아옴 주소는 거부된다 |
| W2 | F10.5 | F10 | Alice | `f10_5` |  | both | pending | 동의 세션을 가리키는 표식이 빠지거나 바뀌면 거부된다 |
| W2 | F10.6 | F10 | Alice | `f10_6` |  | both | pending | 운영자가 동의를 취소하면 자격증명은 만들어지지 않는다 |
| W2 | F10.7 | F10 | Alice | `f10_7` |  | both | pending | 두 운영자가 동시에 OAuth 동의를 진행해도 서로 섞이지 않는다 (신규 P3) |
| W2 | F10.8 | F10 | Alice | `f10_8` |  | both | pending | 동의 세션은 일정 시간이 지나면 만료된다 (신규 P3) |
| W2 | F11A.1 | F11A | Charlie | `f11a_1` | ✓ | both | pending | 5시간 한도의 현재 사용량이 보인다 |
| W2 | F11A.2 | F11A | Charlie | `f11a_2` |  | both | pending | 7일 한도의 현재 사용량이 보인다 |
| W2 | F11A.3 | F11A | Charlie | `f11a_3` |  | both | pending | 기본 한도와 초과 한도가 서로 구분되어 보인다 (v4 F11.14 split — 구분 표시 |
| W2 | F11A.4 | F11A | Charlie | `f11a_4` |  | both | pending | 초과 한도에 들어선 사실이 운영자에게 명시된다 (v4 F11.14 split — 진입 상태 |
| W2 | F11A.5 | F11A | Charlie | `f11a_5` |  | both | pending | 사용량이 80%에 임박하면 화면에서 경고로 보인다 |
| W2 | F11A.6 | F11A | Charlie | `f11a_6` |  | both | pending | 운영자가 한도 메타 정보를 즉시 새로고침한다 |
| W2 | F11A.7 | F11A | Charlie | `f11a_7` |  | both | pending | 한도 합치기 방식을 운영자가 고른다 |
| W2 | F11A.8 | F11A | Charlie | `f11a_8` |  | both | pending | 5시간 창 안에서도 시간대별 사용량이 따로 보인다 (신규 P3) |
| W2 | F11A.9 | F11A | Charlie | `f11a_9` |  | both | pending | 자격증명에 붙은 사용 제한 조건이 사람이 읽을 수 있게 보인다 (신규 P3) |
| W2 | F11A.10 | F11A | Charlie | `f11a_10` |  | both | pending | 한도가 모자라면 부족분이 운영자에게 보인다 (신규 P3) |
| W2 | F11A.11 | F11A | Charlie | `f11a_11` |  | both | pending | 자격증명을 지워도 마지막으로 본 한도 값은 잠시 남는다 (신규 P3) |
| W2 | F11B.1 | F11B | Charlie | `f11b_1` |  | both | pending | Anthropic 한도가 활성 상태로 유지되도록 주기적으로 알린다 |
| W2 | F11B.2 | F11B | Charlie | `f11b_2` |  | both | pending | 미리 데움 호출은 사용량·비용에 반영되지 않는다 |
| W2 | F11B.3 | F11B | Charlie | `f11b_3` |  | both | pending | Anthropic이 자주 보내지 말라고 하면 폴링 간격을 늘린다 |
| W2 | F11B.4 | F11B | Charlie | `f11b_4` |  | both | pending | 여러 복제 노드 중 한 곳만 미리 데움을 수행한다 |
| W2 | F11B.5 | F11B | Charlie | `f11b_5` |  | both | pending | 미리 데움이 실패하면 백오프를 두고 다시 시도한다 |
| W2 | F11B.6 | F11B | Charlie | `f11b_6` |  | both | pending | 미리 데움 상태를 운영자가 화면에서 본다 |
| W2 | F11B.7 | F11B | Charlie | `f11b_7` |  | both | pending | 끊긴 동안의 변경은 다시 붙은 뒤 reconciler가 따라잡는다 |
| W2 | F11B.8 | F11B | Charlie | `f11b_8` |  | both | pending | 비상 차단 중에는 미리 데움이 멈춘다 (신규 P3) |
| W2 | F11B.9 | F11B | Charlie | `f11b_9` |  | both | pending | 미리 데움 대상 위로와 다음 시도 시각을 운영자가 본다 (신규 P3) |
| W2 | F11B.10 | F11B | Charlie | `f11b_10` |  | both | pending | 미리 데움은 OAuth 자격증명에서만 동작한다는 사실이 운영자에게 보인다 (신규 P3) |
| W2 | F11B.11 | F11B | Charlie | `f11b_11` |  | both | pending | Anthropic 위로 주소 변동이 운영자에게 보이고 호출은 끊기지 않는다 (신규 P3) |
| W2 | F11C.1 | F11C | Charlie | `f11c_1` |  | both | pending | 호환성 캐시가 한 시간 주기로 자동 갱신된다 |
| W2 | F11C.2 | F11C | Charlie | `f11c_2` |  | both | pending | 조직 메타가 한도 차이를 추적할 수 있게 보관된다 |
| W2 | F11C.3 | F11C | Charlie | `f11c_3` |  | both | pending | 운영자가 구독 메타를 직접 새로고침한다 |
| W2 | F11C.4 | F11C | Charlie | `f11c_4` |  | both | pending | 프로세스 재시작 표식은 한도 spike로 오해되지 않는다 |
| W2 | F11C.5 | F11C | Charlie | `f11c_5` |  | both | pending | 호환성 캐시 갱신이 실패하면 옛 값을 그대로 유지한다 (v4 F11.26 split — 옛 |
| W2 | F11C.6 | F11C | Charlie | `f11c_6` |  | both | pending | 호환성 캐시의 마지막 성공 갱신 시각이 운영자에게 보인다 (v4 F11.26 split — |
| W2 | F11C.7 | F11C | Charlie | `f11c_7` |  | both | pending | 마지막 시도 시각과 마지막 성공 시각이 따로 보인다 (신규 P3) |
| W3 | F9.1 | F9 | Alice | `f9_1` | ✓ | both | pending | 팀에 정책을 부착하면 다음 호출부터 즉시 적용된다 |
| W3 | F9.2 | F9 | Alice | `f9_2` |  | both | pending | 정책을 떼면 그 후 호출에만 영향이 간다 |
| W3 | F9.3 | F9 | Alice | `f9_3` | ✓ | both | pending | 형식이 잘못된 정책은 저장 단계에서 거부된다 |
| W3 | F9.4 | F9 | Alice | `f9_4` |  | both | pending | 정책 변경은 cc-lb 재시작 없이 다음 호출에 반영된다 |
| W3 | F9.5 | F9 | Alice | `f9_5` |  | both | pending | 한 팀의 정책 변경은 다른 팀의 호출에 영향을 주지 않는다 |
| W3 | F9.6 | F9 | Alice | `f9_6` |  | both | pending | 전역 규칙이 먼저, 팀 규칙이 나중에 적용된다 |
| W3 | F9.8 | F9 | Alice | `f9_8` |  | both | pending | 운영자는 정책을 부착하기 전 미리 흐름을 검증할 수 있다 |
| W3 | F12.1 | F12 | Bob | `f12_1` | ✓ | both | pending | 같은 인장의 플러그인을 두 번 올리면 두 번째는 거부된다 |
| W3 | F12.2 | F12 | Bob | `f12_2` |  | both | pending | 플러그인은 라벨과 버전으로 구분되어 관리된다 |
| W3 | F12.3 | F12 | Bob | `f12_3` |  | both | pending | 운영자가 플러그인 줄의 순서를 바꾸면 다음 호출부터 새 순서로 동작한다 |
| W3 | F12.4 | F12 | Bob | `f12_4` | ✓ | both | pending | 어디선가 쓰이는 플러그인은 삭제할 수 없다 |
| W3 | F12.5 | F12 | Bob | `f12_5` |  | both | pending | 한 플러그인 줄에 넣을 수 있는 플러그인 수를 넘기면 거부된다 |
| W3 | F12.6 | F12 | Bob | `f12_6` |  | both | pending | 한 분에 올릴 수 있는 플러그인 수는 정해져 있다 |
| W3 | F12.7 | F12 | Bob | `f12_7` |  | both | pending | 현재 cc-lb은 응답을 다듬는 슬롯의 플러그인만 받는다 |
| W3 | F12.8 | F12 | Bob | `f12_8` |  | both | pending | 인장이 검증되지 않는 플러그인은 등록 단계에서 거부된다 |
| W3 | F12.9 | F12 | Bob | `f12_9` |  | both | pending | 한 플러그인이 자기 한도 안에서 자원을 다 써도 다른 플러그인은 영향을 받지 않는다 |
| W3 | F12.10a | F12 | Bob | `f12_10a` |  | both | pending | 플러그인 줄 변경의 준비 단계에서는 옛 묶음이 그대로 쓰인다 (v5 분리) |
| W3 | F12.10b | F12 | Bob | `f12_10b` |  | both | pending | 적용 시점에 새 묶음으로 한꺼번에 바뀐다 (v5 분리) |
| W3 | F12.10c | F12 | Bob | `f12_10c` |  | both | pending | 적용이 끝나면 더 이상 쓰이지 않는 옛 부착이 보관소에서 정리된다 (v5 분리) |
| W3 | F12.11 | F12 | Bob | `f12_11` |  | both | pending | 운영자는 새 플러그인을 플러그인 줄 안에서 위치를 지정해 끼울 수 있다 |
| W3 | F12.12 | F12 | Bob | `f12_12` |  | both | pending | 적용 전 사전 점검에서 슬롯 종류가 안 맞으면 변경이 거부된다 |
| W3 | F12.13 | F12 | Bob | `f12_13` |  | both | pending | 어디에도 부착되지 않은 플러그인 줄 항목은 별도 목록으로 보인다 |
| W3 | F12.14 | F12 | Bob | `f12_14` |  | both | pending | 운영자는 플러그인 줄 안의 항목을 한 번에 다시 정렬할 수 있다 |
| W3 | F12.15 | F12 | Bob | `f12_15` |  | both | pending | 정원이 하나로 정해진 슬롯에는 두 번째 부착이 거부된다 |
| W3 | F21.1 | F21 | Alice | `f21_1` |  | both | pending | 호출 한 건이 누구의 어떤 모델 호출인지 별도로 셈해진다 |
| W3 | F21.2 | F21 | Alice | `f21_2` |  | both | pending | 한 호출이 시작과 끝, 또는 오류로 끝났음이 한 번씩 기록된다 |
| W3 | F21.3 | F21 | Alice | `f21_3` |  | both | pending | 스트리밍 응답의 부분 전송 횟수가 운영 대시보드에 모인다 |
| W3 | F21.4 | F21 | Alice | `f21_4` |  | both | pending | 인증 실패는 사유 종류별로 셈해진다 |
| W3 | F21.5 | F21 | Alice | `f21_5` |  | both | pending | 백프레셔로 호출이 떨궈지면 그 사실이 운영 대시보드에 남는다 |
| W3 | F21.7 | F21 | Alice | `f21_7` |  | both | pending | 응답을 받은 사람도 그 호출의 식별자를 본다 |
| W3 | F21.8 | F21 | Alice | `f21_8` |  | both | pending | 운영자가 정한 외부 관찰 도구로 같은 사건이 전송된다 |
| W3 | F21.9 | F21 | Alice | `f21_9` |  | both | pending | 한 호출의 식별자는 감사 기록·운영 로그·외부 추적·응답까지 동일하다 |
| W3 | F21.11 | F21 | Alice | `f21_11` |  | both | pending | 한 호출의 단계별 소요 시간이 사용량 보고에 함께 들어간다 |
| W3 | F21.12 | F21 | Alice | `f21_12` |  | both | pending | 외부 관찰 도구로의 사건 전송이 실패해도 호출 처리에는 영향이 없다 |
| W3 | F21.13 | F21 | Alice | `f21_13` |  | both | pending | 관찰 사건 큐가 차면 떨궈진 묶음이 별도로 보인다 |
| W3 | F21.14 | F21 | Alice | `f21_14` |  | both | pending | 한 팀의 관찰 사슬이 자원을 다 써도 다른 팀의 관찰 사슬은 영향을 받지 않는다 |
| W3 | F25.1 | F25 | Bob | `f25_1` | ✓ | both | pending | cc-lb이 지원하는 플러그인 형식 안에서 만든 플러그인은 받아들여진다 |
| W3 | F25.2 | F25 | Bob | `f25_2` |  | both | pending | cc-lb이 지원하지 않는 플러그인 형식은 거부된다 |
| W3 | F25.3 | F25 | Bob | `f25_3` |  | both | pending | 같은 내용의 플러그인은 같은 인장을 가진다 |
| W3 | F25.5 | F25 | Bob | `f25_5` |  | both | pending | 사전 검사를 통과하지 못한 플러그인은 등록 단계에서 막힌다 |
| W3 | F25.7 | F25 | Bob | `f25_7` |  | both | pending | 작성자는 함수별로 실패 시 동작을 미리 정해 둘 수 있다 |
| W3 | F25.8 | F25 | Bob | `f25_8` |  | both | pending | cc-lb이 지원하는 플러그인 형식이 여러 세대일 때 가장 잘 맞는 세대로 합의된다 |
| W3 | F25.9 | F25 | Bob | `f25_9` |  | both | pending | 작성자는 cc-lb이 제공하는 보조 기능을 통해서만 외부와 통신한다 |
| W3 | F25.11 | F25 | Bob | `f25_11` |  | both | pending | 등록된 플러그인은 자기 이름·버전·할 수 있는 일을 운영자에게 보인다 |
| W3 | F25.12 | F25 | Bob | `f25_12` |  | both | pending | 등록된 플러그인이 많아도 cc-lb 재시작 시 부팅을 지연시키지 않는다 |
| W3 | F25.13 | F25 | Bob | `f25_13` |  | both | pending | 플러그인의 본 단계가 정해진 시간을 넘기면 호출은 대체 동작으로 마무리된다 |
| W3 | F25.14 | F25 | Bob | `f25_14` |  | both | pending | cc-lb 경계에서 비밀은 플러그인에 닿기 전 가려져 전달된다 |
| W3 | F25.15 | F25 | Bob | `f25_15` |  | both | pending | 플러그인이 더 낮은 세대로 강제 합의를 요청하면 거부된다 |
| W3 | F25.16 | F25 | Bob | `f25_16` |  | both | pending | 능력 선언에서 슬롯이 요구하는 능력이 빠진 플러그인은 거부된다 |
| W3 | F25.17 | F25 | Bob | `f25_17` |  | both | pending | cc-lb이 받는 형식 세대 범위 바깥의 플러그인은 거부된다 |
| W3 | F27.3 | F27 | Bob | `f27_3` |  | both | pending | 짧은 시간 안의 플러그인 업로드 폭주는 잠시 멈춰진다 |
| W3 | F27.4 | F27 | Bob | `f27_4` |  | both | pending | 비상 차단은 두 단계 확인 후에만 효력이 생긴다 |
| W3 | F27.5 | F27 | Bob | `f27_5` |  | both | pending | 관리자 세션이 일정 시간을 넘기면 위험 동작 전 다시 신원 확인을 요구한다 |
| W3 | F27.6 | F27 | Bob | `f27_6` |  | both | pending | 다른 출처에서 시작된 관리자 요청은 의도치 않게 실행되지 않는다 |
| W3 | F27.7 | F27 | Bob | `f27_7` |  | both | pending | 관리자 토큰 값은 운영 화면 어디에서도 평문으로 보이지 않는다 |
| W3 | F29.1 | F29 | Charlie | `f29_1` |  | both | pending | 갑작스러운 충돌 메시지에도 비밀은 가려진다 |
| W3 | F29.2 | F29 | Charlie | `f29_2` |  | both | pending | 운영자가 의도적으로 결함을 주입해 회복력을 시험한다 |
| W3 | F29.3a | F29 | Charlie | `f29_3a` |  | both | pending | 외부 연결이 갑자기 끊겨도 정해진 대체 동작으로 호출이 마무리된다 (v5.2 분리) |
| W3 | F29.3b | F29 | Charlie | `f29_3b` |  | both | pending | 외부 연결 끊김으로 인한 대체 동작은 같은 호출 식별자로 운영 로그와 감사 기록에 남는다  |
| W3 | F29.4 | F29 | Charlie | `f29_4` |  | both | pending | 한도 엔진이 차갑게 다시 시작해도 진행 중이던 계산은 이어진다 |
| W3 | F29.5 | F29 | Charlie | `f29_5` |  | both | pending | 결함 주입을 한 팀에만 한정하면 다른 팀의 호출은 영향을 받지 않는다 |
| W3 | F29.6 | F29 | Charlie | `f29_6` |  | both | pending | 결함 주입을 켜고 끄는 동작 자체가 감사 기록에 남는다 |
| W3 | F29.7 | F29 | Charlie | `f29_7` |  | both | pending | 결함 주입은 미리 정해진 지점에서만 일어난다 |
| W4 | F13.1 | F13 | Dana | `f13_1` | ✓ | both | pending | 감사관이 한 운영자의 한 분기 변경을 시간순으로 본다 |
| W4 | F13.2 | F13 | Dana | `f13_2` |  | both | pending | 감사 기록에는 비밀 정보가 한 줄도 들어가 있지 않다 |
| W4 | F13.3 | F13 | Dana | `f13_3` |  | both | pending | 한 번 적힌 감사 기록은 어떤 운영자도 지우거나 고칠 수 없다 |
| W4 | F13.4 | F13 | Dana | `f13_4` |  | both | pending | 감사관이 시간 창과 페이지 단위로 좁혀 본다 |
| W4 | F13.5 | F13 | Dana | `f13_5` |  | both | pending | 보존 기간이 지난 기록만 정확히 정리된다 |
| W4 | F13.6 | F13 | Dana | `f13_6` |  | both | pending | 한 호출의 시작·끝·오류가 같은 추적 표식으로 묶인다 |
| W4 | F13.7 | F13 | Dana | `f13_7` |  | both | pending | 감사 기록을 외부 감사 시스템으로 내보낸다 |
| W4 | F13.8 | F13 | Dana | `f13_8` |  | both | pending | 한 호출의 추적 표식이 audit, 운영 로그, 응답에 모두 같이 찍힌다 |
| W4 | F13.9 | F13 | Dana | `f13_9` |  | both | pending | 내보낸 감사 파일에 변조 증명이 함께 들어 있다 (신규 P3) |
| W4 | F13.10 | F13 | Dana | `f13_10` |  | both | pending | 감사관이 한 호출의 모든 관련 기록을 교차 표로 본다 (신규 P3) |
| W4 | F13.11 | F13 | Dana | `f13_11` |  | both | pending | 비용과 한도 위반이 같은 호출 줄에 함께 보인다 (신규 P3, split 1/2) |
| W4 | F13.12 | F13 | Dana | `f13_12` |  | both | pending | 비용과 한도 위반이 같은 호출 단위로 분기 보고서에 합산된다 (신규 P3, split 2/ |
| W4 | F14.1 | F14 | Alice | `f14_1` | ✓ | both | pending | 운영자가 초안을 저장해도 실제 동작에는 아직 영향이 없다 |
| W4 | F14.2 | F14 | Alice | `f14_2` |  | both | pending | 운영자가 초안을 검증하면 적용 전에 문제가 드러난다 |
| W4 | F14.3 | F14 | Alice | `f14_3` | ✓ | both | pending | 잘못된 설정은 저장 단계에서 거부된다 |
| W4 | F14.4 | F14 | Alice | `f14_4` | ✓ | both | pending | 검증을 통과한 초안만 적용된다 (split 1/2) |
| W4 | F14.5 | F14 | Alice | `f14_5` |  | both | pending | 적용된 설정 변경이 적용 이력에 한 줄로 기록된다 (split 2/2) |
| W4 | F14.6 | F14 | Alice | `f14_6` |  | both | pending | 운영자가 적용 이력을 시간순으로 본다 |
| W4 | F14.7 | F14 | Alice | `f14_7` |  | both | pending | 운영자가 이전 버전 설정으로 되돌린다 |
| W4 | F14.8 | F14 | Alice | `f14_8` |  | both | pending | 재시작 없이 적용 가능한 항목은 재시작 없이 설정 적용된다 |
| W4 | F14.9 | F14 | Alice | `f14_9` |  | both | pending | 재시작이 필요한 항목은 적용 전에 그 사실이 표시된다 |
| W4 | F14.10 | F14 | Alice | `f14_10` |  | both | pending | 운영자가 적용된 설정을 파일로 내려받는다 |
| W4 | F14.11 | F14 | Alice | `f14_11` |  | both | pending | bootstrap 설정은 첫 부팅에 한 번만 반영된다 |
| W4 | F14.12 | F14 | Alice | `f14_12` |  | both | pending | 적용 안 된 초안은 정해진 기간 뒤 자동으로 비워진다 (신규 P3) |
| W4 | F14.13 | F14 | Alice | `f14_13` |  | both | pending | 재시작이 필요한 항목은 어느 항목인지 항목별로 명확히 표시된다 (신규 P3) |
| W4 | F14.14 | F14 | Alice | `f14_14` |  | both | pending | 디스크 위 설정 파일이 바뀌면 cc-lb가 그 변경을 알아챈다 (신규 P3) |
| W4 | F15.1 | F15 | Charlie | `f15_1` |  | both | pending | 종료 신호 후에는 새 호출을 안 받는다 |
| W4 | F15.2 | F15 | Charlie | `f15_2` |  | both | pending | 종료 중에도 이미 진행 중 호출은 마지막까지 처리된다 |
| W4 | F15.3 | F15 | Charlie | `f15_3` |  | both | pending | 종료 대기 시간이 초과되면 강제로 끝내고 보고한다 |
| W4 | F15.4 | F15 | Charlie | `f15_4` |  | both | pending | 새 인증서 적용해도 진행 중 스트림 안 끊김 |
| W4 | F15.5 | F15 | Charlie | `f15_5` |  | both | pending | 잘못된 인증서는 적용 전에 거부된다 (split 1/2) |
| W4 | F15.6 | F15 | Charlie | `f15_6` |  | both | pending | 잘못된 인증서 거부 사유가 운영자에게 한 줄로 안내된다 (split 2/2) |
| W4 | F15.7 | F15 | Charlie | `f15_7` |  | both | pending | 인증서 만료가 가까워지면 운영자에게 미리 알린다 |
| W4 | F15.8 | F15 | Charlie | `f15_8` |  | both | pending | 임시 디버그 로깅이 정해진 시간 뒤 자동으로 꺼진다 |
| W4 | F15.9 | F15 | Charlie | `f15_9` |  | both | pending | 종료 신호의 종류에 따라 종료 의미가 분리된다 (신규 P3) |
| W4 | F15.10 | F15 | Charlie | `f15_10` |  | both | pending | 디버그 로깅은 정해진 운영 신호로만 켜진다 (신규 P3) |
| W4 | F15.11 | F15 | Charlie | `f15_11` |  | both | pending | 운영 전용 소켓은 외부 호출자의 접근을 거부한다 (신규 P3, split 1/3) |
| W4 | F15.12 | F15 | Charlie | `f15_12` |  | both | pending | 운영 전용 소켓은 같은 머신 안 운영자의 관리 호출을 정상 수신한다 (신규 P3, spli |
| W4 | F15.13 | F15 | Charlie | `f15_13` |  | both | pending | 외부 호출용 포트와 운영 전용 소켓은 종료 진행 중에도 분리된 상태로 동작한다 (신규 P3 |
| W4 | F15.14 | F15 | Charlie | `f15_14` |  | both | pending | 복제 노드가 종료 완료된 사실이 다른 복제 노드에 표식으로 남는다 (신규 P3) |
| W4 | F17.1 | F17 | Charlie | `f17_1` | ✓ | both | pending | 한 복제 노드의 변경이 다른 복제 노드에 즉시 알려진다 |
| W4 | F17.2 | F17 | Charlie | `f17_2` |  | both | pending | 여러 복제 노드 중 단 하나만 작업을 수행한다 — 자격증명 미리 데움 |
| W4 | F17.3 | F17 | Charlie | `f17_3` |  | both | pending | 작업을 수행하던 복제 노드가 사라지면 다른 복제 노드가 인계받는다 |
| W4 | F17.4 | F17 | Charlie | `f17_4` |  | both | pending | 설정 변경은 모든 복제 노드가 같은 시점에 같은 값을 본다 |
| W4 | F17.5 | F17 | Charlie | `f17_5` |  | both | pending | 같은 줄을 동시에 고치려 하면 한쪽만 성공한다 |
| W4 | F17.6 | F17 | Charlie | `f17_6` |  | both | pending | 두 복제 노드가 동시에 단일 수행권을 들었다고 믿어도 한쪽만 작업을 계속한다 (신규 P3, |
| W4 | F17.7 | F17 | Charlie | `f17_7` |  | both | pending | 단일 수행권 자기 점검 결과가 운영 로그와 지표에 남는다 (신규 P3, split 2/2) |
| W4 | F17.8 | F17 | Charlie | `f17_8` |  | both | pending | 운영자가 어느 복제 노드가 살아 있는지 한 화면에서 본다 (신규 P3) |
| W4 | F17.9 | F17 | Charlie | `f17_9` |  | both | pending | 복제 노드 식별자가 손상되면 안전하게 새 식별자가 발급된다 (신규 P3, split 1/3 |
| W4 | F17.10 | F17 | Charlie | `f17_10` |  | both | pending | 옛 식별자로 묶여 있던 작업은 다른 복제 노드로 인계되거나 만료된다 (신규 P3, spli |
| W4 | F17.11 | F17 | Charlie | `f17_11` |  | both | pending | 옛 식별자와 새 식별자가 같은 호출에 동시에 쓰이지 않는다 (신규 P3, split 3/3 |
| W4 | F17.12 | F17 | Charlie | `f17_12` |  | both | pending | 두 저장 백엔드에서 같은 운영자 시나리오가 같은 결과를 낸다 (F22-merged) |
| W4 | F17.13 | F17 | Charlie | `f17_13` |  | both | pending | 두 저장 백엔드의 일관성 시나리오가 모두 통과한다 (F22-merged) |
| W4 | F17.14 | F17 | Charlie | `f17_14` |  | both | pending | 저장 백엔드 종류를 잘못 바꾸면 부팅이 명확히 거부된다 (F22-merged) |
| W4 | F17.15 | F17 | Charlie | `f17_15` |  | both | pending | 두 저장 백엔드에서 같은 운영자 동작이 같은 수와 같은 종류의 감사 줄을 남긴다 (F22- |
| W4 | F17.16 | F17 | Charlie | `f17_16` |  | both | pending | 두 저장 백엔드 모두에서 감사 기록에 비밀이 평문으로 보이지 않는다 (F22-merged, |
| W4 | F17.17 | F17 | Charlie | `f17_17` |  | both | pending | 다운그레이드 이동은 부팅 단계에서 거부된다 (F22-merged) |
| W4 | F17.18 | F17 | Charlie | `f17_18` |  | both | pending | 두 운영자가 같은 줄을 동시에 고치는 상황이 두 저장 백엔드 모두에서 같은 결정을 낸다 ( |
| W4 | F18.1 | F18 | Dana | `f18_1` |  | both | pending | 운영자가 모델별 현재 가격을 한 화면에서 본다 |
| W4 | F18.2 | F18 | Dana | `f18_2` | ✓ | both | pending | 호출당 비용이 카탈로그 가격으로 정확히 계산된다 |
| W4 | F18.3 | F18 | Dana | `f18_3` |  | both | pending | 분기 비용 보고서가 팀별로 정확히 합산된다 |
| W4 | F18.4 | F18 | Dana | `f18_4` |  | both | pending | 가격이 바뀌면 그 시점 이후 새 호출부터 새 가격이 적용된다 |
| W4 | F18.5 | F18 | Dana | `f18_5` |  | both | pending | 캐시가 들어맞은 호출은 비용 절감이 별도 항목으로 보인다 |
| W4 | F18.6 | F18 | Dana | `f18_6` |  | both | pending | 가격 변경 이력이 시간순으로 보존된다 (split 1/2) |
| W4 | F18.7 | F18 | Dana | `f18_7` |  | both | pending | 과거 호출 비용 계산이 가격 이력의 그 시점 단가와 정합한다 (split 2/2) |
| W4 | F18.8 | F18 | Dana | `f18_8` |  | both | pending | 캐시를 처음 만든 호출과 캐시를 다시 읽은 호출이 단가가 따로 계산된다 (신규 P3, sp |
| W4 | F18.9 | F18 | Dana | `f18_9` |  | both | pending | 캐시 생성·재사용 단가의 합산이 분기 보고서 총합과 한 토큰도 안 어긋난다 (신규 P3,  |
| W4 | F18.10 | F18 | Dana | `f18_10` |  | both | pending | 토큰 수를 정확히 못 셀 때는 추정 표시와 함께 보인다 (신규 P3, split 1/2) |
| W4 | F18.11 | F18 | Dana | `f18_11` |  | both | pending | 추정으로 보고된 호출은 사유 기록과 별도 조회가 따로 제공된다 (신규 P3, split 2 |
| W4 | F18.12 | F18 | Dana | `f18_12` |  | both | pending | 위로 이름이 바뀌어도 이전 사용량은 그대로 보인다 (F23-merged) |
| W4 | F18.13 | F18 | Dana | `f18_13` |  | both | pending | 위로를 삭제해도 그 이전 사용량은 보존된다 (F23-merged) |
| W4 | F18.14 | F18 | Dana | `f18_14` |  | both | pending | 두 위로를 하나로 합치면 사용량이 정확히 합산된다 (F23-merged) |
| W4 | F18.15 | F18 | Dana | `f18_15` |  | both | pending | 사용량 보고서에 위로 이름 변경이 함께 표시된다 (F23-merged) |
| W4 | F20.1 | F20 | Dana | `f20_1` | ✓ | both | pending | 저장된 모든 비밀은 평문으로 디스크에 남아 있지 않다 |
| W4 | F20.2 | F20 | Dana | `f20_2` |  | both | pending | 변조하면 즉시 들통난다 |
| W4 | F20.3 | F20 | Dana | `f20_3` |  | both | pending | 마스터 키가 사라지면 비밀은 영영 복구되지 않는다 — 의도된 동작 |
| W4 | F20.4 | F20 | Dana | `f20_4` |  | both | pending | 잘못된 마스터 키로는 cc-lb가 부팅 자체를 거부한다 (split 1/2) |
| W4 | F20.5 | F20 | Dana | `f20_5` |  | both | pending | 잘못된 마스터 키 부팅 시도에서 어떤 비밀도 평문으로 한 번도 읽히지 않는다 (split  |
| W4 | F20.6 | F20 | Dana | `f20_6` |  | both | pending | 키 회전 후에도 이전에 저장된 비밀은 그대로 읽힌다 |
| W4 | F20.7 | F20 | Dana | `f20_7` |  | both | pending | 마스터 키 파일의 권한이 너무 느슨하면 부팅이 거부된다 |
| W4 | F20.8 | F20 | Dana | `f20_8` |  | both | pending | 비밀이 알려진 자리 패턴을 따르는 값은 종류별로 가려진 표시로 보인다 |
| W4 | F20.9 | F20 | Dana | `f20_9` |  | both | pending | 비정상 종료 메시지에도 비밀 정보는 가려진 표시로만 나온다 |
| W4 | F20.10 | F20 | Dana | `f20_10` |  | both | pending | 마스터 키 회전이 진행 중 호출을 끊지 않는다 (신규 P3) |
| W4 | F20.11 | F20 | Dana | `f20_11` |  | both | pending | 비정상 종료 추적의 변수 값에도 비밀이 가려진 표시로만 나온다 (신규 P3) |
| W4 | F20.12 | F20 | Dana | `f20_12` |  | both | pending | 호출자에게 돌아가는 응답 헤더에서 비밀이 알려진 자리 패턴을 따르는 값은 가려진 표시로만  |
| W4 | F20.13 | F20 | Dana | `f20_13` |  | both | pending | 같은 비밀이 다른 위로에 저장돼 있어도 서로 풀어 쓸 수 없다 (신규 P3) |
| W4 | F24.1 | F24 | Alice | `f24_1` |  | both | pending | 외부 가격 소스가 정상이면 새 가격을 가져온다 |
| W4 | F24.2 | F24 | Alice | `f24_2` |  | both | pending | 외부 가격 소스가 일시 장애여도 마지막 가격이 그대로 쓰인다 |
| W4 | F24.3 | F24 | Alice | `f24_3` |  | both | pending | 외부 가격 소스가 오래 죽어 있으면 운영자에게 알린다 |
| W4 | F24.4 | F24 | Alice | `f24_4` |  | both | pending | 가격 소스에 세 번 실패하면 디스크에 남겨 둔 가격으로, 그것도 없으면 비용 보고를 보류한 |
| W4 | F24.5 | F24 | Alice | `f24_5` |  | both | pending | 가격 카탈로그가 통째로 망가지면 그 사실을 안전하게 알린다 |
| W4 | F24.6 | F24 | Alice | `f24_6` |  | both | pending | 운영자가 가격 카탈로그의 마지막 갱신 시각을 본다 |
| W4 | F24.7 | F24 | Alice | `f24_7` |  | both | pending | 가격 카탈로그의 출처 증명이 맞지 않으면 새 가격을 쓰지 않는다 (신규 P3) |
| W4 | F24.8 | F24 | Alice | `f24_8` |  | both | pending | 카탈로그에 없는 모델 호출은 정해진 폴백 방식으로 보고된다 (신규 P3) |
