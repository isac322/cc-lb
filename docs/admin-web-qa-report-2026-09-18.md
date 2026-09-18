# cc-lb 2026-09-18 실행 보고

## 요약

| 항목 | 상태 |
|---|---|
| Web CI timeout 근본 원인과 수정 | 완료 (PR #805 머지, `864e801e`) |
| 버그 PR 스택 9건 재배치와 리뷰 반영 | 완료, CI 통과 후 머지 대기 |
| QA 자산 PR 재작성(운영 코드 분리) | 완료, CI 통과 후 머지 대기 |
| 격리 fixture 읽기 경로 전수 측정 | 완료 (179 cell × 5회, 전부 200) |
| 규모 실험과 병목 근거 | 완료 (300k 이벤트, 원인 SQL 계획 확인) |
| Default Limits 81 UI paired 검증 | 차단 (0/81) |
| 전체 426행 런타임 전수 실행 | 부분 완료 (읽기 경로만) |
| 운영 `cc-lb.runbear.io` 런타임 측정 | 차단 (Access 로그인 조작 불가) |
| 목록 쿼리 개선 적용 | 미적용 (애플리케이션 변경, 승인 필요) |

## 1. CI

`bun-checks`가 `cc-lb-1`(cpu quota 1)에서 실행됐다. vitest는 단일 worker지만 bun 런타임·GC·jsdom이 1코어를 넘겨 요구해 대부분의 스케줄링 주기가 throttle되고, 개별 테스트가 5000ms 예산을 넘겼다.

고정 러너 이미지와 실제 CI 명령으로 quota만 바꿔 측정:

| quota | vitest wall | throttled | 결과 |
|---|---|---|---|
| 1 core | 170.6s | 156.6s | 775 통과 |
| 2 cores | 70.5s | 20.2s | 775 통과 |
| 4 cores | 59.0s | 0.8s | 775 통과 |

클러스터 실측: #802 러너는 `rpi4`에서 throttled periods +7342/14193, #803은 `rock5bp`에서 +3986/5461.

적용: web 작업을 `cc-lb-2`로 이동, 러너 이미지에 `gnutar` 추가(busybox tar가 `actions/cache`의 `-P`를 거부해 캐시가 매번 실패했다). 머지 후 `bun-checks`는 `cc-lb-2`에서 73/73 파일 통과, 검증 단계 4분 35초.

## 2. 읽기 경로 측정 (격리 fixture)

179 cell을 5회씩, 총 895 요청 모두 200. 분모는 API로 확인: principal 3, upstream 4, plugin 0, API key 1. Keepalive는 3 horizon × 7 status × 3 principal = 63 cell을 포함한다.

데이터가 적은 상태에서는 모든 endpoint가 warm median 1~5ms였다.

## 3. 규모 실험과 병목

`request_events_v1`에 7일 범위 300k행을 주입하고 재측정했다.

| endpoint | 주입 전 warm median | 주입 후 median | p95 | 최대 |
|---|---|---|---|---|
| `/admin/v1/events/recent` | 0.94ms | 425.85ms | 1093ms | 2426ms |
| `/admin/v1/events/histogram` | 1.03ms | 229.13ms | 395ms | 395ms |
| `/admin/v1/audit` | 1.76ms | 2.21ms | 5.53ms | 8.37ms |
| `/admin/v1/dashboard/summary` | 1.96ms | 2.48ms | 3.54ms | 9.0ms |

원인은 목록 쿼리 계획이다. `request_event_list_sql.rs`의 조건은 `ts` 범위인데 이 테이블에는 `ts` 선행 인덱스가 없다. SQLite는 `request_events_v1_v3_cache_key_ts` skip-scan을 고른 뒤 `ORDER BY list_ts_ms DESC, list_event_key DESC, id DESC`를 TEMP B-TREE로 정렬한다. 같은 쿼리를 직접 실행하면 118~328ms다.

히스토그램 쿼리가 이미 쓰는 넓힌 `list_ts_ms` 경계를 목록 쿼리에 추가하면 계획이 `request_events_v1_list_order_idx` 탐색으로 바뀌고 같은 100행이 0.7~1.4ms에 반환된다. 이는 애플리케이션 변경이라 적용하지 않고 제안으로 남겼다.

한계: SQLite fixture이며 운영은 PostgreSQL이다. 주입 데이터는 균일 분포 합성이고 측정 장비는 워크스테이션이다.

## 4. Default Limits UI 검증 차단

controller와 드라이버를 복구해 한 셀이 22초에 UI 증거 검증(단일 trusted Save, 단일 PATCH, revision+1, DOM/스크린샷 해시)을 통과했다. 그러나 Camofox 클릭 한 번이 페이지에 약 130ms 간격으로 두 번 도달한다. 두 번째 클릭은 저장 직후 같은 좌표의 Edit 버튼을 눌러 편집기를 다시 열었고, 한 번은 두 번째 PATCH까지 만들어 revision이 예상보다 증가했다.

서버 로그는 `locator.click` 3초 타임아웃 후 `mouse sequence dispatched` 한 번만 기록한다. 즉 중복은 폴백 호출 횟수가 아니라 그 이전 단계에서 발생한다. 이후 `page.mouse.move`가 2.5초 내에 반환되지 않는 상태로 악화되어 클릭 자체가 전달되지 않았다.

조치: 잘못된 `click-timeout-no-replay` 패치를 제거하고, 클릭이 이미 전달됐는지 관측한 뒤 폴백하는 `click-witness`와 키보드 활성화용 `camofox_press`를 추가했다(`/etc/nix-darwin` `cc8fee0`). 중복은 여전히 재현되므로 81셀은 0/81로 남긴다.

또한 기존 6개 셀의 "완료" 증거는 과거 스크린샷 복사와 recorder 템플릿 수정이었음을 확인해 UI 증거로 인정하지 않았다.

## 5. 운영 측정 차단

`cc-lb.runbear.io`는 Cloudflare Access 로그인을 요구한다. Google 계정 선택 화면까지 도달했지만 선택 클릭이 위 입력 문제로 실패했다. 다른 계정이나 우회 자격증명은 사용하지 않았다.

## 6. 남은 작업

1. Camofox 중복 입력의 원인을 Camoufox 입력 파이프라인까지 추적하거나, `camofox_press` 기반 키보드 경로로 UI QA를 재개한다(현재 MCP 래퍼가 구버전이라 재연결이 필요하다).
2. 81 UI cell과 쓰기 경로 상태 전이를 실행한다.
3. 운영 접근을 확보해 실제 부하에서 같은 측정을 반복한다.
4. 목록 쿼리 개선을 승인받아 적용하고, PostgreSQL에서도 계획을 확인한다.
