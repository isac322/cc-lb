# 테스트 실행과 tier 운영

cc-lb의 Rust 테스트는 검증 경계에 따라 T1, T2, T3, T4, T5, TX로 나눈다. tier는 파일 위치나 현재 의존성이 아니라, 테스트의 주장을 깨뜨릴 수 있는 최소 production 경계로 결정한다. 자세한 판정 기준은 `.opencode/skills/test-pyramid-tiers/SKILL.md`를 따른다.

## 로컬 실행

모든 명령은 저장소 루트에서 실행한다. 표준 workspace 명령은 load, stress, multi-replica 전용 package를 제외한다.

```bash
# T1 + T2: 순수 로직과 in-memory component
cargo nextest run \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --exclude tests-multi-replica \
  --all-features \
  --profile fast

# T3: SQLite, Wasmtime, protocol contract
cargo nextest run \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --exclude tests-multi-replica \
  --all-features \
  --profile contract

# T4: in-process App + loopback socket
cargo nextest run \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --exclude tests-multi-replica \
  --all-features \
  --profile integration

# T5: process-level acceptance tests
cargo nextest run -p cc-lb-server -p tests-integration \
  --all-features \
  --profile e2e

# TX: 성능, 지연, 메모리, loom 등 비기능 계약
cargo nextest run \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --exclude tests-multi-replica \
  --all-features \
  --profile tx
```

PostgreSQL이 필요한 `t3_postgres__`와 일부 `tx__` 테스트는 `CI_POSTGRES_URL`이 없으면 실패한다. 테스트를 건너뛰지 않는다.

```bash
export CI_POSTGRES_URL='postgres://postgres:testpw@localhost:5432/cc_lb_test'
export DATABASE_URL="$CI_POSTGRES_URL"
export DATABASE_URL_TEST="$CI_POSTGRES_URL"
export SQLX_OFFLINE=true

cargo nextest run \
  --workspace \
  --exclude cc-lb-loadgen \
  --exclude cc-lb-stress-suite \
  --exclude tests-multi-replica \
  --all-features \
  --profile contract-postgres
```

## 정적 게이트

```bash
cargo fmt --check
bash scripts/check-integration-test-consolidation.sh
bash scripts/check-test-tiers.sh
```

`check-test-tiers.sh`는 다음을 함께 검사한다.

- tier 표식과 디렉터리 일치
- real sleep, retry, silent skip, `#[ignore]`, ambient env 변경 등 금지 패턴
- T4/T5 테스트와 `upper-tier-registry.toml`의 1:1 대응
- 이동·병합·삭제 ledger의 대상 및 guardian 유효성
- 닫힌 migration ledger의 최종 테스트 수 하한
- `cc-lb-testkit` 의존성 경계

## CI 대응

| CI job | 실행 범위 |
|---|---|
| `static-checks` | fmt, 저장소 guard, dependency policy, alert/dashboard 정적 검증 |
| `rust-checks` | SQLite/PostgreSQL clippy, `--profile fast`, strict tier lint |
| `nextest-cov` | TX를 제외한 workspace 전체, PostgreSQL 포함, package별 coverage gate |
| `e2e` | process 및 real-client E2E와 `--profile e2e` |
| `tx` | schedule/수동 실행 전용 `--profile tx`, PostgreSQL service 포함 |

`nextest-cov`는 기본 nextest filter를 사용하므로 T1부터 T5까지 실행한다. `rust-checks`는 clippy와 빠른 T1/T2 피드백이 같은 checkout, toolchain, cache, build artifact를 공유하게 한다. TX는 기본 filter와 PR 필수 job에서 제외하며 nightly schedule 또는 `workflow_dispatch`에서 실행한다.

## 새 테스트를 추가할 때

1. 실패 시 드러나는 최소 동작 주장을 한 문장으로 적는다.
2. 그 주장을 깨뜨릴 수 있는 최소 production 경계로 tier를 정한다.
3. T2에서 storage fake를 추가하면 같은 PR에서 shared conformance 시나리오를 추가한다.
4. T4/T5이면 `docs/testing/upper-tier-registry.toml`에 정확히 한 항목을 등록한다.
5. 기존 테스트를 이동, 병합, 분할, 삭제하면 `docs/testing/migration-ledger.toml`에 기록한다.
6. `cargo nextest list`에서 leaf test 이름에 tier segment가 정확히 하나만 나타나는지 확인한다.
7. 관련 profile과 strict tier lint를 실행한다.

## 운영 파일

- `.config/nextest.toml`: profile filter와 직렬 test group
- `.opencode/skills/test-pyramid-tiers/SKILL.md`: tier 판정 및 hard rule
- `docs/testing/classification.csv`: 전수 분류 결과
- `docs/testing/upper-tier-registry.toml`: T4/T5 대표 시나리오
- `docs/testing/migration-ledger.toml`: 이동·병합·분할·삭제 및 추가 test identity
- `docs/testing/test-count-baseline.txt`: 마이그레이션 시작 시 수집된 test count
