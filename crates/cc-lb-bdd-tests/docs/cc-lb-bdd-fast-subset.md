# BDD Fast Subset (M0 deliverable, plan v3)

- 작성일: 2026-06-18
- 작성 근거: plan v3 §0 (Fast subset), §10 (CI), §11 (Milestones)
- 사용 시점: PR 게이트 (`.github/workflows/bdd.yml`). Nightly = 전수.
- nextest filter: `cargo nextest run -p cc-lb-bdd-tests -E 'test(/^fast_/)'`
- 함수명 prefix: `fast_<fn_name>_<backend>` (예: `fast_f1_1a_sqlite`, `fast_f1_1a_postgres`)

## 선정 기준

다음 조건 중 **2개 이상 만족** 시 fast subset 후보:
1. **Happy path** — 핵심 흐름의 첫 정상 케이스 (예: `Fn.1`, `Fn.1a`)
2. **보안 게이트** — 거부 / 인증 실패 / 권한 차단 (예: F1.4 비활성팀 거부, F1.5 모델 ACL, F5.5 자격증명 회수)
3. **저비용** — 시간 의존 sleep / chaos / retry 루프 없음. 단일 호출로 끝나는 시나리오
4. **broad coverage** — 각 Feature 의 대표 1~2건이 포함되도록 (skew 방지)

다음 시나리오는 **fast subset 제외**:
- 시간 의존 (예: F5.2 갱신 실패 알림, F5.3 백오프 증가, F2.4a/b 자동 만료, F2.2 회전 겹침)
- Chaos / failure-injection (예: F29.x, F8.6 5xx 시퀀스, F8.4 연속 실패)
- 다중 인스턴스 / replica 협조 (예: F17.x — postgres only nightly)
- 대량 데이터 / pagination (예: F13.4 페이지 단위, F18 분기 보고서)
- OAuth refresh full cycle (M0 mock-anthropic-oauth-server 가 5초+ 소요)

## Fast Subset 목록 (33건, 약 10.3% of 321)

| ID | Writer | Feature | Persona | fn_name | v5.2 한글 제목 (출처) | 선정 사유 |
|---|---|---|---|---|---|---|
| F1.1a | W1 | F1 | Alice | `f1_1a` | 새 팀 등록은 활성 상태와 첫 키를 같은 화면에서 한 번에 마무리한다 | happy path + broad |
| F1.1b | W1 | F1 | Alice | `f1_1b` | 새 팀 등록은 누가 언제 만들었는지를 감사 기록에 남긴다 | happy path + audit baseline |
| F1.4 | W1 | F1 | Alice | `f1_4` | 비활성화된 팀의 호출은 받아들여지지 않는다 | 보안 게이트 |
| F1.5 | W1 | F1 | Alice | `f1_5` | 팀에 허용된 모델 범위를 벗어나는 호출은 거부된다 | 보안 게이트 (ACL) |
| F1.7 | W1 | F1 | Alice | `f1_7` | 팀을 삭제해도 그 팀이 했던 일의 감사 흔적은 사라지지 않는다 | audit invariant |
| F2.1 | W1 | F2 | Alice | `f2_1` | 새 키 발급 시 비밀은 단 한 번만 보여진다 | 보안 게이트 (secret expose-once) |
| F2.3 | W1 | F2 | Alice | `f2_3` | 키 회수 즉시 다음 호출부터 거부된다 | 보안 게이트 (revocation latency) |
| F2.5 | W1 | F2 | Alice | `f2_5` | 키 목록 화면에는 비밀이 결코 함께 보이지 않는다 | 보안 게이트 (no-leak) |
| F3.1 | W1 | F3 | Bob | `f3_1` | 정상 키로 정상 모델을 부르면 응답이 그대로 도착한다 | happy path (E2E proxy) |
| F3.3 | W1 | F3 | Bob | `f3_3` | 알 수 없는 키로 보낸 호출은 친절히 거부된다 | 보안 게이트 (auth fail) |
| F3.4 | W1 | F3 | Bob | `f3_4` | 비활성 팀의 키로 보낸 호출은 거부된다 | 보안 게이트 (inactive principal) |
| F26.1 | W1 | F26 | Charlie | `f26_1` | 살아 있음과 처리 가능이 분리되어 보인다 | broad (liveness/readiness) |
| F5.1 | W2 | F5 | Charlie | `f5_1` | 만료가 임박한 자격증명을 cc-lb이 자동으로 갱신한다 | happy path (OAuth refresh - mock fast) |
| F5.4 | W2 | F5 | Charlie | `f5_4` | 잘못된 자격증명은 등록 단계에서 거부된다 | 보안 게이트 (rejection at registration) |
| F5.5 | W2 | F5 | Charlie | `f5_5` | 자격증명을 회수하면 그 자격증명을 쓰던 모든 호출이 즉시 멈춘다 | 보안 게이트 (revoke propagation) |
| F7.1 | W2 | F7 | Charlie | `f7_1` | 비상 차단을 켜면 모든 호출이 거부된다 | 보안 게이트 (killswitch enable) |
| F7.2 | W2 | F7 | Charlie | `f7_2` | 비상 차단을 풀면 정상 상태로 돌아온다 | 보안 게이트 (killswitch disable) |
| F7.5 | W2 | F7 | Charlie | `f7_5` | 차단으로 거부된 응답에는 "운영자가 일시 차단" 의미가 명시된다 | 보안 게이트 (response semantics) |
| F8.7 | W2 | F8 | Charlie | `f8_7` | 한 위로가 죽으면 다른 위로로 자동 우회한다 | broad (failover happy path, low chaos) |
| F10.1 | W2 | F10 | Alice | `f10_1` | 운영자가 브라우저로 Anthropic에 동의한다 | happy path (OAuth flow start) |
| F11A.1 | W2 | F11A | Charlie | `f11a_1` | 5시간 한도의 현재 사용량이 보인다 | broad (subscription quota display) |
| F9.1 | W3 | F9 | Alice | `f9_1` | 팀에 정책을 부착하면 다음 호출부터 즉시 적용된다 | happy path (policy attach) |
| F9.3 | W3 | F9 | Alice | `f9_3` | 형식이 잘못된 정책은 저장 단계에서 거부된다 | 보안 게이트 (validation) |
| F12.1 | W3 | F12 | Bob | `f12_1` | 같은 인장의 플러그인을 두 번 올리면 두 번째는 거부된다 | 보안 게이트 (plugin dedup) |
| F12.4 | W3 | F12 | Bob | `f12_4` | 어디선가 쓰이는 플러그인은 삭제할 수 없다 | 보안 게이트 (refcount) |
| F25.1 | W3 | F25 | Bob | `f25_1` | cc-lb이 지원하는 플러그인 형식 안에서 만든 플러그인은 받아들여진다 | happy path (plugin runtime) |
| F13.1 | W4 | F13 | Dana | `f13_1` | 감사관이 한 운영자의 한 분기 변경을 시간순으로 본다 | happy path (audit read) |
| F14.1 | W4 | F14 | Alice | `f14_1` | 운영자가 초안을 저장해도 실제 동작에는 아직 영향이 없다 | happy path (config draft) |
| F14.3 | W4 | F14 | Alice | `f14_3` | 잘못된 설정은 저장 단계에서 거부된다 | 보안 게이트 (config validation) |
| F14.4 | W4 | F14 | Alice | `f14_4` | 검증을 통과한 초안만 적용된다 (split 1/2) | happy path (config apply) |
| F17.1 | W4 | F17 | Charlie | `f17_1` | 한 복제 노드의 변경이 다른 복제 노드에 즉시 알려진다 | broad (replication baseline, postgres-heavy) |
| F18.2 | W4 | F18 | Dana | `f18_2` | 호출당 비용이 카탈로그 가격으로 정확히 계산된다 | happy path (cost calc) |
| F20.1 | W4 | F20 | Dana | `f20_1` | 저장된 모든 비밀은 평문으로 디스크에 남아 있지 않다 | 보안 게이트 (secret-at-rest) |

## 분포 검증

| Writer | Fast 수 | 비율 (Writer 내 %) |
|---|---:|---:|
| W1 | 12 | 12.2% (12/98) |
| W2 | 9 | 13.6% (9/66) |
| W3 | 5 | 7.9% (5/63) |
| W4 | 7 | 7.4% (7/94) |
| **TOTAL** | **33** | **10.3% (33/321)** |

| Persona | Fast 수 |
|---|---:|
| Alice | 14 |
| Bob | 6 |
| Charlie | 10 |
| Dana | 3 |

| Feature | Fast 수 |
|---|---:|
| F1 | 5 | F2 | 3 | F3 | 3 | F26 | 1 | F5 | 3 | F7 | 3 |
| F8 | 1 | F10 | 1 | F11A | 1 | F9 | 2 | F12 | 2 | F25 | 1 |
| F13 | 1 | F14 | 3 | F17 | 1 | F18 | 1 | F20 | 1 |

총 17 features × 평균 1.94 = 33. 모든 27 features 가 fast subset 에 한 개 이상 들어가야 한다는 가정은 X (chaos-only / time-only feature 는 nightly 만).

## PR 게이트 예상 시간

- 시나리오 33 × backend 2 (sqlite + postgres) = 66 fn
- 평균 시나리오당 ~5초 (E2E with fake-anthropic in-process spawn)
- 직렬 = 330초 ≈ 5.5분
- nextest 병렬 8 thread = 약 45초~1.5분 (sqlite); postgres = schema-per-test 도 병렬 가능하나 conformance.yml 의 `--test-threads=4` 정책 따라 분산
- 목표: **PR 게이트 ≤ 5분** 충족

## M5 재선정 규칙

M5 시점에 다음 셋 검증 후 fast subset 재구성:
1. 각 fast 시나리오의 실제 평균 실행 시간 측정 (nextest junit-output 의 `time` 필드)
2. 7회 연속 nightly 에서 무 flake 인 시나리오만 fast 유지
3. 새로 stable 한 시나리오 중 핵심 보안 게이트 / happy path 를 fast 로 승격

재선정 결과는 본 문서의 33행 표를 in-place 갱신 + commit 메시지에 "fast subset M5: +N / -M" 명시.
