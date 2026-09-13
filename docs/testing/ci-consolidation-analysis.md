# CI 통합 및 프로덕션 동작 복원 분석

작성일: 2026-09-13

## Goal

테스트 피라미드가 추가한 검증력을 유지하면서 PR CI의 중복 빌드와 job 시작 오버헤드를 제거한다. 같은 목적과 실행 환경을 가진 검사는 한 job의 독립된 step으로 합치고, 실행 환경이나 실패 의미가 다른 검사는 분리한다. 동시에 테스트 seam 때문에 달라진 `App::start`의 production readiness/listener bind 순서를 `origin/master`와 동일하게 복원한다.

완료 조건은 다음과 같다.

1. `App::start`의 production 관측 동작이 `origin/master`와 같아야 한다: `wait_for_ready()` 완료 후 proxy listener와 admin listener를 순서대로 bind한다.
2. 테스트 전용 held-listener seam은 유지하되 production 경로의 bind 시점과 오류 순서를 바꾸지 않는다.
3. T1~T5 기능 테스트, PostgreSQL contract, coverage gate, real-client/process E2E의 필수 검증 범위를 줄이지 않는다.
4. TX는 기능 정확성 테스트와 분리된 비기능 검증으로 유지하되 모든 PR에서 20분짜리 별도 job으로 실행하지 않는다.
5. 동일 목적의 중복 job은 통합하고, 서로 다른 환경·실패 의미·외부 의존성을 가진 job은 분리한다.
6. job 통합 뒤에도 각 검사는 이름이 있는 별도 step으로 남아 실패 원인을 바로 식별할 수 있어야 한다.

## 측정 결과

비교 기준:

- master CI run: `34507320831`
- 현재 HEAD CI run: `34695692908`

| 항목 | master | 현재 HEAD | 변화 |
|---|---:|---:|---:|
| CI critical path | 15분 47초 | 22분 31초 | +6분 44초 (+42.7%) |
| `nextest-cov` | 6분 18초 | 10분 47초 | +4분 29초 (+71.2%) |
| `rust-fast` | 없음 | 3분 34초 | 신규 |
| `tx` | 없음 | 20분 11초 | 신규 |
| `e2e` | 4분 3초 | 3분 51초 | 유사 |

현재 PR은 `ci.yml`에 `rust-fast`와 `tx`를 새 job으로 추가했고, `web.yml`에 `playwright`를 새 job으로 추가했다. Rust 테스트 수와 coverage는 증가했지만, PR마다 별도 Rust toolchain 설치·checkout·cache restore·workspace compile이 반복된다.

## Job별 결정

| Job | 목적 | 다른 job과의 중복 | 결정 |
|---|---|---|---|
| `fmt` | 포맷, 저장소 정적 guard | `cargo-deny`, `promtool`과 checkout 및 짧은 정적 검증 성격 공유 | `static-checks`로 통합 |
| `cargo-deny` | dependency/license/advisory 검사 | 짧은 T0 정적 검사 | `static-checks`의 별도 step으로 통합 |
| `promtool` | alert/dashboard 정적 검사 | 짧은 T0 정적 검사 | `static-checks`의 별도 step으로 통합 |
| `clippy (sqlite/postgres)` | 동일 lint를 feature 조합별 실행 | toolchain, cache, 대부분의 compile artifact가 서로 중복되고 `rust-fast`와도 Rust build를 중복 | matrix와 `rust-fast`를 하나의 `rust-checks` job으로 통합 |
| `rust-fast` | T1/T2 빠른 실행과 tier lint | T1/T2는 `nextest-cov`가 다시 실행하지만 tier lint가 동일 test binary 목록을 필요로 함 | 삭제하지 않고 `rust-checks`에서 clippy와 checkout, toolchain, cache, build artifact 공유 |
| `nextest-cov` | T1~T5, PostgreSQL, Wasmtime, coverage gate | 필수 기능 및 coverage의 기준 job | 독립 유지 |
| `tx` | 성능·지연·loom 등 비기능 검증 | 기능 correctness job과 목적이 다름 | PR 필수 job에서 제외하고 schedule/수동 실행으로 유지 |
| `e2e` | 실제 client, OS process, signal, TLS | Node client 설치와 process semantics가 고유 | 독립 유지 |
| `guard-crate-versions` | PR version 정책 | event 조건과 release job dependency가 고유 | 독립 유지 |
| `release-pr-artifacts` | release PR 전용 image/chart 검증 | Docker/Helm과 release 조건이 고유 | 독립 유지 |
| Web `bun-checks` | build/lint/typecheck/Vitest | 브라우저를 요구하지 않음 | 독립 유지 |
| Web `playwright` | Chromium UI behavior | runner와 browser dependency 및 실패 의미가 다름 | 독립 유지 |
| Scheduled PostgreSQL jobs | stress와 multi-process failover | 동일 DB setup이지만 assertion 경계와 장애 격리가 다름 | 분리 유지 |

## 예상 PR CI 구조

일반 PR에서 실행되는 Rust 관련 핵심 job은 다음으로 줄인다.

1. `static-checks`
   - `cargo fmt --check`
   - audit redaction lint
   - integration consolidation guard
   - workspace scaffold guard
   - `cargo deny check`
   - alert rule 및 Grafana dashboard 검증
2. `rust-checks`
   - SQLite feature clippy
   - PostgreSQL feature clippy
   - `--profile fast`
   - strict tier lint
3. `nextest-cov`
   - TX를 제외한 T1~T5 전체
   - PostgreSQL contract
   - Wasmtime
   - package별 coverage gate
4. `e2e`
   - 실제 client matrix
   - process-level T5

`tx`는 nightly schedule과 `workflow_dispatch`에서 계속 실행한다. 테스트 코드는 삭제하지 않고 PR critical path와 runner startup에서만 제외한다.

일반 PR에서 실제 runner를 할당하는 Rust CI job 수는 현재 구조 기준 10개(`clippy` matrix 2개 포함)에서 5개로 줄어든다. `release-pr-artifacts`와 `tx`는 일반 PR에서 runner를 할당하지 않는다. 현재 측정값을 그대로 대입하면 PR critical path는 20분 11초인 TX가 빠지고 10분 47초인 `nextest-cov`가 다시 지배한다. 실제 통합 후 시간은 다음 CI 실행에서 측정해야 한다.

## 프로덕션 동작 문제

수정 전 `App::start`는 listener를 먼저 bind한 뒤 `start_with_listeners` 안에서 readiness를 기다렸다. `origin/master`는 readiness를 기다린 뒤 listener를 bind했다. 이 차이는 ready 이전 TCP connect 결과와 `EADDRINUSE` 발생 시점을 바꿀 수 있었다.

복원 방식:

- `App::start`에서 `server_state.wait_for_ready().await`를 먼저 수행한다.
- 그 다음 proxy와 admin listener를 기존 순서로 bind한다.
- 실제 serve 본문은 private helper로 이동한다.
- 테스트가 사용하는 `start_with_listeners`는 자체적으로 readiness를 기다린 뒤 같은 private helper를 호출한다.
- production `start`와 test seam이 같은 serve 본문을 공유하되, production bind 시점은 `origin/master`와 동일하게 유지한다.

## 검증 기준

- production `App::start` 순서가 `wait_for_ready → proxy bind → admin bind`인지 코드와 회귀 시나리오로 확인한다.
- held-listener 기반 T4 테스트가 계속 실행되는지 확인한다.
- `nextest-cov`가 기존 T1~T5 및 PostgreSQL 범위를 그대로 포함하는지 filter를 확인한다.
- `tx__` 테스트가 schedule/수동 경로에서 계속 열거되고 실행되는지 확인한다.
- 통합된 `clippy` 두 feature command와 모든 `static-checks` command를 실행한다.
- 실제 proxy 대표 경로를 실행해 listener seam 변경이 요청/응답 동작을 바꾸지 않았는지 확인한다.

## 적용 및 검증 결과

- `App::start`를 `wait_for_ready → proxy bind → admin bind → serve` 순서로 복원했다.
- held-listener seam은 `start_with_listeners → wait_for_ready → serve`를 유지하며 production과 동일한 private serve 본문을 공유한다.
- 일반 PR의 Rust 관련 runner 할당은 10개에서 5개로 줄었다.
- `tx__` 테스트는 삭제하지 않았고 `ci.yml`의 nightly schedule과 `workflow_dispatch`에서 계속 실행한다.
- `nextest-cov`, package coverage gate, real-client matrix, process E2E 명령은 유지했다.
- `actionlint .github/workflows/ci.yml`: PASS.
- `cargo check -p cc-lb-server --all-features`: PASS.
- T4 `t4__parity_characterization_golden`, `t4__proxy_nonstream_happy`: 2/2 PASS.
- T5 `t5__process_boot_default_config`: 1/1 PASS.
- 통합 `rust-checks` 명령:
  - SQLite clippy: PASS.
  - PostgreSQL clippy: PASS.
  - fast profile: 1,782/1,782 PASS, 526 skipped.
  - strict tier lint: PASS.
- 로컬 static 검증에서 fmt, audit-redaction, integration consolidation, workspace scaffold, Prometheus rule syntax은 PASS했다.
- `cargo-deny`는 로컬에 설치되어 있지 않아 실행하지 못했다. CI의 checksum 검증 설치 단계와 `cargo deny check` 명령은 기존 job에서 그대로 이동했다.
- Grafana validator는 로컬에서 alert-name 추출이 비어 실패했지만, 누락으로 표시된 alert 7개는 모두 `deploy/alerts/live-tail.yml`에 존재한다. 동일 HEAD의 Linux CI에서는 기존 `promtool` job이 PASS했으며, 통합 job도 Linux runner를 사용한다.
