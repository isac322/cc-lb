# cc-lb BDD v5.2 → Rust 실행 가능 테스트 변환 계획 (v3.2, Momus v3.1 APPROVED-WITH-EDITS 의 B3 PARTIAL 해소)

- 작성일: 2026-06-18
- 변환 대상: 4 파일 / 27 features / 321 scenarios (v5.2)
- **시나리오 텍스트 언어 (v3 결정)**: **영문**. v5.2 본문은 사람이 읽으라고 한글로 작성된 보고서. 실제 테스트 코드의 step text · scenario title · doc comment 는 **영문**. 페르소나 이름 (Alice/Bob/Charlie/Dana) 과 도메인 어휘 (upstream / warmup / replica / callback / readyz / drain / sweep / lease / token / quota) 는 영문 그대로 유지. 한글 본문 → 영문 step text 번역은 M0 산출물 추가
- 대상 워크스페이스: `/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf` (`opencode/crisp-wolf` 브랜치)
- v3 변경 요약 (v2 → v3):
  1. **시나리오 source 언어를 영문으로 확정.** v2 의 "한국어 도메인 어휘" 표기 폐기. 한글은 사람용 보고서 (v5.2 markdown) 에만 남고, Rust 코드 산출물은 step / title / doc / jargon-rewrite 게이트 전부 영문 기준
  2. **BDD 프레임워크 재조사 + 비교 매트릭스** (§3 전면 재작성). cucumber-rs 0.23 / hand-rolled `bdd_scenario!` 매크로 / gauge / gherkin-rs 4개 후보 정량 비교. **결론은 유지 (hand-rolled 매크로 채택)** — 단 거부 근거가 v2 의 "Korean step matching 불안정" 에서 v3 의 "nextest 외부 시나리오-단위 필터링 불가 + dependency cost + ID-based mapping 우위" 로 교체
  3. §4.2 시나리오 메타: 한글 doc → 영문 doc (한글 원본 참조용 마크다운 경로 + 라인만 보존)
  4. §4.3 한국어 함수명 미사용 사유 → 영문 source 채택에 따라 자연 해소 (§4.3 폐지, 본 §0 에 흡수)
  5. §7.1 persona bootstrap: 페르소나 영문 이름 유지, 페르소나 설명도 영문 한 줄로
  6. §12 jargon-rewrite 게이트 의미 반전: v2 = "영문 인프라 어휘를 한글 도메인 어휘로 치환했는지 확인" → v3 = "step text 안에 **Rust 구현 jargon (Vec<u8>, HashMap, tokio::spawn, sqlx::query!, Arc, Mutex 등)** 이 침투했는지 검출 + 차단". 도메인 어휘 (upstream/warmup/replica/callback/readyz) 는 모두 허용
  7. §13 R-jargon-rewrite 리스크: 영문 source 기준으로 재정의. 검출 그렙 패턴 명시
  8. (v2 의 Momus blocking gap 10건은 v2 에서 이미 해소 → v3 에서도 모두 그대로 유지)
- v3.1 변경 요약 (v3 → v3.1, Momus v3 REJECT 의 3개 new blocking 해소):
  1. **§1.3 stale 한글 hard constraint 갱신** — v3 §0 영문 결정과 충돌하던 "시나리오 한국어 제목·페르소나·도메인 어휘 보존" 줄을 영문 source 정책으로 교체. 한글 원본은 `///` doc backlink 으로만 보존, panic / assert / 식별자 등장 금지
  2. **§12.1 forbidden jargon list 대폭 확장** — Momus 지적 누락 항목 (`JoinHandle`, `JoinSet`, `select!`, `join!`, `mpsc`, `oneshot`, `broadcast`, `Semaphore`, `Runtime`, `block_on`, `Duration`, `Instant`, `sleep`, `serde_json::Value`, `Uuid`) 포함 + `Notify`, `Stream`, `Cow`, `OnceCell`, `OnceLock`, `Lazy`, `task::yield_now`, `spawn_blocking`, `Transaction`, `PgPool`, `SqlitePool`, `Migrator`, `tower::`, `hyper::`, `http::`, `Json(`, `Response::`, `Request::`, `Method::`, `Uri`, `Deserialize`, `Serialize`, `json!`, `thiserror::`, ` ?;`, `bail!`, `ensure!`, `Err(`, `pub fn`, `pub async fn`, `pub struct`, `pub enum`, `impl `, `where `, `'a`, `'static` 추가
  3. **§3.5 매크로 `description =` string literal 인자 추가 + §12.1 lint 대상 명시** — Momus 지적 "lint extractable 보장 안 됨". 매크로에 `description` string literal 의무화 → grep 대상이 closure body 아닌 4개 명시 텍스트 (title literal + description literal + 생성 fn doc + panic literal) 로 한정. lint 구현은 `syn` crate 으로 named-argument 추출 (cargo expand 의존성 회피)
  4. §13 R3 row 보강 (위 #3 의 의무화 + lint 비대상 명시 + PR checklist 항목 명시)
- v3.2 변경 요약 (v3.1 → v3.2, Momus v3.1 의 B3 PARTIAL 해소):
  1. §3.5 매크로 시그니처에서 `description =` 을 "optional attributes" 블록 → "required attributes" 블록으로 이동. 누락 시 컴파일 에러 + Momus B3 rationale 명시.
  2. §12.1 Lint 대상 텍스트 목록의 `description` 항목 "선택적" → "**필수 인자**" 로 표기 정정.
- 본 계획 목적: **계획만**. 코드 작성은 별도 라운드.
- 컨텍스트 인벤토리: [cc-lb-bdd-inventory-coverage-v5.2.md](/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md) (1326 인벤토리 대비 COVERED 79.3%)

---

## 0. 결정 요약 (TL;DR)

| 영역 | 결정 | 거부한 대안 |
|---|---|---|
| Gherkin 실행기 | **신규 `bdd_scenario!` 매크로** (hand-rolled, `cc-lb-bdd-tests` crate 안에 `macro_rules!` 로 정의). 비교 매트릭스는 §3.2 참조 | cucumber-rs 0.23 / gauge / gherkin-rs / 시나리오당 `#[test]` 직작성 |
| 시나리오 → Rust 함수 | ID 규칙 `f<n>_<m>[_suffix]` + fast 후보는 `fast_f<n>_<m>` prefix. **영문 제목·step·doc**, 매크로 `persona=` 강제 인자 (`Alice`/`Bob`/`Charlie`/`Dana`) | 한국어 함수명 (rust-analyzer / nextest 필터 / git grep 마찰), 한글 step text (영문 source 결정으로 자동 폐기) |
| 백엔드 매트릭스 | sqlite + postgres 양쪽 모두 (conformance 패턴 그대로 재사용) | sqlite-only |
| Anthropic 업스트림 | **fake-anthropic 의 `MessageScript` 가 1순위 도구** (응답 FIFO + 헤더 inject + `with_delay` + recorded request 회수 + `wait_for_requests` 비동기 대기). SSE streaming + mid-response drop 등 MessageScript 표현 불가 case 한정으로 fake-anthropic 라이브러리 보강 PR 1건 (§5.2) | 별도 `ScenarioInjector` trait, wiremock-only |
| OAuth | mock-anthropic-oauth-server 재사용 | 신규 mock 작성 |
| DB 격리 | sqlite tempfile-per-test + postgres schema-per-test | DB 공유 + transaction rollback |
| Postgres URL env | **`CI_POSTGRES_URL` 단일 정본** (postgres.yml 일치). 로컬 단축 alias 만 `DATABASE_URL` 허용, 코드/스크립트는 항상 `CI_POSTGRES_URL` 우선 | `DATABASE_URL` 사용 |
| 페르소나 | bootstrap helper 4개 (`alice()`, `bob()`, `charlie()`, `dana()`) | 인라인 setup |
| Crate 정수 | 4개 top-level integration test binary (writer 1개당 1개): `tests/w1.rs`, `tests/w2.rs`, `tests/w3.rs`, `tests/w4.rs`. 각 binary 는 `mod w<n>_<feature>;` 로 하위 feature 모듈 포함 (cargo 자동 발견 OK) | 27 binary (feature 1개당 1개), nested dir (자동발견 X) |
| Fast subset | 함수명 prefix `fast_` (예: `fast_f1_1a`). nextest filter `test(/^fast_/)` | `@fast` 태그 (Rust 식별자 `@` 불가) |
| CI | 신규 `.github/workflows/bdd.yml` + `bdd-nightly.yml` | ci.yml 확장 |
| PR vs nightly | PR=`fast_` prefix 함수만 ≤5분, nightly=전수 ≤30분 | PR 전수 |
| No-real-API 게이트 | (a) 빌드시 `grep -r api.anthropic.com tests/` 검사, (b) 통합 픽스처에 loopback-only HTTP wrapper, 위반 시 즉시 panic | 신뢰 기반 |
| Captures retention | `tests/__captures__/` 는 **gitignore**. CI 실패 시에만 `actions/upload-artifact` 업로드 | git commit |
| 진척 단위 | M0~M5 마일스톤. 모든 마일스톤 게이트에 invariant `converted + OoS-manual + blocked = 321` 강제 | feature 단위 빅뱅 |

---

## 1. 목표·제약 (계약)

### 1.1 목표
1. v5.2 321 시나리오를 **개별 실행 가능 Rust 테스트**로 변환한다.
2. 변환된 테스트는 **로컬에서 실행** (single command) 후 **CI에서 자동 실행** 되어야 한다.
3. 각 시나리오는 실패 시 **시나리오 ID + 페르소나 + 실패 step + binary observable** 을 출력한다.
4. **sqlite (기본) + postgres (conformance)** 양쪽 백엔드에서 동일 시나리오가 통과해야 한다 (단, 명시적 OoS-manual 항목 제외).
5. **모든 마일스톤 종료 시점에 321 invariant 가 성립**: `converted + OoS-manual + blocked = 321`.

### 1.2 비-목표 (Non-Goal)
- 1326 인벤토리의 NC 82건·PARTIAL 95건 보강 (v6 안건)
- 실제 Anthropic API 회귀
- 부하·soak·24h 안정성 (`tests/soak/soak.sh`, `.github/workflows/soak.yml`)
- Web Dashboard UI 시각 회귀 (Playwright/visual-qa 별도 트랙)
- 일부 페르소나-가시성 "사람이 본다" 시나리오는 명시적 OoS-manual 처리 (정확 ID 목록 §7.3 / M0 산출물)

### 1.3 하드 제약
- 실제 Anthropic API 호출 금지. 모든 업스트림 응답은 fake-anthropic 또는 wiremock으로 생성.
- §13 no-real-API 검출 게이트가 빌드/CI 모두에서 강제.
- 시나리오 step text·title·doc comment·panic 메시지는 **영문**. 영문 도메인 어휘 (`upstream`, `warmup`, `callback`, `replica`, `readyz`, `drain`, `sweep`, `lease`, `token`, `quota`, `principal`, `audit`, `signer`, `dialect`, `plugin`, `chain`, `terminal`, `filter`, `shape`, `observability`) 는 step text 와 doc 에서 그대로 사용. 한글 원본 (v5.2 markdown) 은 `/// Original (Korean human-readable report): <path>#L<from>-L<to>` 형태의 backlink doc comment 로만 보존. 한글이 panic/assert 메시지 또는 Rust 식별자에 직접 등장하는 것은 금지.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo nextest run` 전부 통과.

---

## 2. 현재 자산 인벤토리 (재사용 대상)

본 변환은 새 인프라 최소화. 검증된 자산:

| 자산 | 경로 | 재사용 방식 |
|---|---|---|
| `ConformanceBackend` trait | [crates/cc-lb-storage-conformance/src/harness.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/crates/cc-lb-storage-conformance/src/harness.rs) | trait 구조 모방 → `BddBackend` trait 신설 |
| `scenario!` / `plugin_registry_scenario!` 매크로 | 각 scenarios 파일 (anthropic_compatibility_kv_store.rs:22, organization_metadata_store.rs:19, ...) | **재사용 X**. 패턴만 참고하여 cc-lb-bdd-tests 안에 신규 `bdd_scenario!` 정의 |
| `MessageScript` + `ScriptedMessageResponse` + `RecordedMessageRequest` | [tests/fixtures/fake-anthropic/src/routes.rs:48-156](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/tests/fixtures/fake-anthropic/src/routes.rs) | **메인 도구**. `push_response`, `pop_response`, `requests()`, `request_count()`, `wait_for_requests(expected, timeout)`, `with_delay`, `with_header` 그대로 사용 |
| fake-anthropic library form | `fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse, RecordedMessageRequest, app}` (lib.rs:8) | in-process spawn |
| mock-anthropic-oauth-server | tests/fixtures/mock-anthropic-oauth-server | OAuth (F5, F10) |
| wiremock 0.6 | workspace dep | 보조 업스트림 (LiteLLM 가격 카탈로그 — F24) |
| 기존 E2E 패턴 | [pipeline_e2e.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/crates/cc-lb-server/tests/pipeline_e2e.rs) | 5 fake-anthropic + sqlite + Lifecycle 부팅 사슬 |
| Postgres CI 패턴 | [.github/workflows/postgres.yml](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/postgres.yml) | postgres:18 service + `CI_POSTGRES_URL` + `--test-threads=4` |
| Sqlite CI 패턴 | [.github/workflows/ci.yml](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/ci.yml) | nextest + sccache + `oracle4-cc-lb` runner + features matrix `[sqlite, postgres]` |

### 2.1 자산 갭 (만들어야 할 것)

1. **`bdd_scenario!` 매크로** (cc-lb-bdd-tests 안에 로컬 정의). 페르소나·`fast_` prefix·sqlite+postgres 자동 생성.
2. **fake-anthropic 라이브러리 보강 PR** (좁은 범위):
   - SSE streaming response 출력 옵션 (현 `into_response()` 는 `Json(body)` 전용)
   - Mid-response connection drop 옵션
   - 위 2 옵션이 `ScriptedMessageResponse` 의 enum 변형으로 추가 (하위 호환)
3. **페르소나 bootstrap helper**.
4. **시나리오 ID ↔ 함수 매핑 표** (321행, M0 산출). 매 마일스톤마다 invariant 강제.
5. **No-real-API 검출 픽스처** (loopback-only `reqwest::Client` wrapper + `cargo test no_real_anthropic_in_fixtures`).

---

## 3. 결정 1: Gherkin 실행기 (영문 source 전제하 재비교, v3)

### 3.1 선택: `cc-lb-bdd-tests` crate 안 신규 `bdd_scenario!` 매크로 (hand-rolled, `macro_rules!`)

### 3.2 비교 매트릭스 (4 후보 × 13 축)

조사 일자: 2026-06-18. 출처:
- cucumber-rs 0.23.0 `Cargo.toml` (`gherkin = 0.16`, `cucumber-codegen = 0.23.0`, `cucumber-expressions = 0.5`, `inventory = 0.3`, `clap = 4.3`, +9 transitive)
- cucumber-rs README + Tags 문서 + IntelliJ libtest 통합 문서 (cucumber-rs/cucumber main branch book)
- 실제 사용 예: `leptos-rs/leptos` examples/*/e2e/, `eclipse-kuksa/kuksa-databroker`, `hit-box/hitbox`, `mandrean/cw-optimizoor`, `boxabirds/maw`
- gauge 공식 사이트 (gauge.org) 지원 언어 매트릭스
- 본 워크스페이스 `crates/cc-lb-storage-conformance/src/scenarios/*.rs` 의 file-local `scenario!` `macro_rules!` 정의 8건 (`anthropic_compatibility_kv_store.rs:22`, `organization_metadata_store.rs:19`, `upstream_rate_limit_store.rs:23`, `upstream_subscription_metadata_store.rs:20`, `upstream_subscription_quota_store.rs:34`, `scenarios/upstream_store.rs:84`, `plugin_registry_store.rs` 의 `plugin_registry_scenario!`)

| # | 평가 축 | cucumber-rs 0.23 | hand-rolled `bdd_scenario!` (v3 선택) | gauge | gherkin-rs (parser only) |
|---|---|---|---|---|---|
| 1 | Rust 네이티브 지원 | ✓ | ✓ | ✗ JS/C#/Java/Py/Ruby only | ✓ parser, runner 직접 |
| 2 | 영문 `.feature` 파일 파싱 | ✓ (`.feature` 자동) | ✗ Rust DSL only | n/a | ✓ parser API |
| 3 | step text → fn 바인딩 | `#[given/when/then(expr=...)]` + cucumber-expressions + `inventory` linker collect | 매크로 인자로 명시 (id, persona, title, given/when/then 클로저) | n/a | 직접 |
| 4 | Backend matrix (sqlite/pg) | `Scenario Outline + Examples` 또는 `World` 안 backend field | 매크로 attribute `backend = both` (기본) / `sqlite_only` / `postgres_only` → fn 2개 자동 expand | n/a | 직접 |
| 5 | nextest **외부** 시나리오 단위 필터 | **✗** binary 1개만 노출. `--tags @fast` 는 cucumber-rs **내부** CLI / `CUCUMBER_FILTER_TAGS` env. `harness = false` 강제 | **✓** 각 시나리오가 native `#[tokio::test]` 함수. `cargo nextest -E 'test(/^fast_/)'`, `test(=f1_1a_sqlite)` 그대로 동작 | n/a | 직접 (다만 직접 구현 시 nextest 친화 layout 가능) |
| 6 | Fast subset 메커니즘 | tag `@fast` + cucumber 내부 필터 (nextest 와 무관) | 매크로 attribute `fast = true` → 생성 fn 이름에 `fast_` prefix 부여 | n/a | 직접 |
| 7 | 단일 시나리오 디버그 (nextest filter 1건) | binary 단위 + `--name f1_1a` (cucumber 내부 regex). nextest 가 보는 건 binary 1개 | `cargo nextest run -E 'test(=f1_1a_sqlite)'` 그대로 동작 | n/a | 가능 |
| 8 | 컴파일 타임 (321 sc, M5 시점) | step fn 등록은 `inventory` 사용 → 분량 자체 영향 적음. 단 `cucumber-codegen` proc-macro + 13+ transitive dep 추가 → 초기 빌드 +60~120s, 워크스페이스 sccache 캐싱 후 +5~10s | macro_rules expansion 321 fn (sqlite/postgres 분리 시 642 fn). 분산 빌드 단위 4개 (W1~W4 binary) → 각 binary ~160 fn. 외부 의존 0 → 초기 빌드 영향 미미 | n/a | 의존 |
| 9 | `MessageScript` / fake-anthropic 통합 | `World` 안에 `Arc<MessageScript>` field, hooks 에서 in-process spawn. World 가 글로벌이므로 시나리오간 격리는 `World::default()` 매 시나리오 호출 의존 | 매크로 본체에서 `let bdd_ctx = BddCtx::spawn_fake_anthropic().await;` 1줄. 시나리오마다 fresh instance 보장 | n/a | 가능 |
| 10 | Persona 보존 (영문 source) | step text 자체에 "Alice", "Bob" 그대로 적힘 (영문이라 자연스러움). `World` 가 persona context 보유 | 매크로 강제 인자 `persona = Alice` → 누락 시 컴파일 에러. 생성 fn 의 panic message 에 자동 prefix | n/a | 직접 |
| 11 | Conformance `scenario!` 패턴 정합 | World/Cucumber runner 자체 시스템. ConformanceBackend trait 호출은 World 내부에서 가능하지만 **다른 패러다임 공존** | 워크스페이스의 8건 file-local `scenario!` `macro_rules!` 와 **동일 mental model** (생성 fn → backend matrix → conformance trait 호출) | n/a | 가능 |
| 12 | 외부 의존성 추가 | 13+ transitive deps (`gherkin 0.16`, `cucumber-codegen`, `cucumber-expressions`, `inventory`, `clap 4.3`, `globwalk`, `ref-cast`, `sealed`, `smart-default`, `derive_more`, `humantime`, ...) | 0 (workspace 내장) | n/a | gherkin 1개 |
| 13 | 출력 / CI 친화 | `--format=json` (libtest 호환 IntelliJ Rust 한정). JUnit XML / Cucumber JSON 지원. nextest 의 junit/json output 과는 **다른 stream** (cucumber binary 가 그대로 stdout 점유) | nextest 의 표준 libtest 출력 + junit-output 그대로 활용. `cargo nextest run --message-format=json` 도 정상 | n/a | 직접 |

### 3.3 결정 (영문 source 전제) 및 거부 근거

**채택: hand-rolled `bdd_scenario!` 매크로.**

영문 source 가 cucumber-rs 의 일부 약점 (한국어 step 매칭 불안정 등) 을 자동으로 해소함에도 채택을 뒤집지 않는 이유:

- **축 5 (nextest 외부 필터링) 가 결정적**. v2 §0 의 "Fast subset 함수 prefix + `cargo nextest -E 'test(/^fast_/)'`" 는 우리 CI 의 1차 게이트. cucumber-rs 는 `harness = false` 강제 → nextest 는 binary 1개만 본다. fast subset / 단일 시나리오 격리 / retry 전략 모두 cucumber 내부 CLI (`--tags`, `--name`) 에 의존. **외부 도구 (nextest filter expression DSL) 와 동등하게 통합 불가**.
- **축 11 (conformance 패턴 정합)**. 워크스페이스에 이미 file-local `scenario!` `macro_rules!` 가 8개 동일 패턴 (id → backend matrix fn 2개 expand → conformance trait 호출). 새 `bdd_scenario!` 가 같은 mental model 을 유지 → 신규 매크로 도입 비용 = 0 학습 곡선. cucumber-rs 는 `World` / step attr / `cucumber-expressions` 라는 **다른 패러다임** 도입 비용.
- **축 12 (외부 의존성)**. cucumber-rs 도입 시 `cucumber-codegen` proc-macro + `inventory` linker collect + `cucumber-expressions` regex DSL + 9개 transitive dep 추가. 우리 워크스페이스의 sqlite/postgres feature matrix + extism runtime + axum + sqlx 와 dependency tree 충돌 가능성. hand-rolled = 0 추가 의존.
- **축 3 (step → fn 바인딩)**. 영문 source 에서 cucumber-expressions 의 fuzzy step matching 이 가치를 발휘하는 시점은 **자연어 변동성이 높을 때**. 우리 321 sc 는 이미 ID 기반 (F1.1a → f1_1a) + 명시 매크로 인자. fuzzy matching 대신 명시적 ID 매핑이 더 강력 (step text "Alice creates a team" 과 "Alice creates a team named 'X'" 의 regex 충돌 회피).

**거부**:
- **cucumber-rs 0.23**: 축 5/11/12 모두 비용. 영문 source 결정 후에도 결론 불변.
- **gauge**: Rust 미지원 (지원 언어: JS/C#/Java/Py/Ruby). 폴리글랏 fan-out 으로 우회 가능하나 본 워크스페이스 ergonomics 와 무관 + CI 부담만 증가.
- **gherkin-rs (parser only)**: cucumber-rs 의 subset. runner 자체 구현 부담을 가져오는데 cucumber-rs 보다 이점 없음.
- **시나리오당 `#[test]` 직작성**: 보일러플레이트 폭증 (321 fn × 2 backend × persona setup × MessageScript spawn = ~2000 줄 중복). 매크로 1개로 0 줄.

### 3.4 conformance `scenario!` 와의 관계 (정정)
- 워크스페이스의 `scenario!` 와 `plugin_registry_scenario!` 는 **각 conformance 파일에서 로컬 정의된 helper 매크로** (anthropic_compatibility_kv_store.rs:22, organization_metadata_store.rs:19, upstream_rate_limit_store.rs:23, upstream_subscription_metadata_store.rs:20, upstream_subscription_quota_store.rs:34, scenarios/upstream_store.rs:84, plugin_registry_store.rs).
- 재사용 가능한 public API 가 아니므로 **확장 대상 아님**. 패턴(시그니처·sqlite/postgres 매트릭스 생성·error path)만 모방하여 cc-lb-bdd-tests 안에 신규 정의.

### 3.5 신규 매크로 시그니처 (안, 영문 source)

```rust
// crates/cc-lb-bdd-tests/src/macros.rs (M0 deliverable)
//
// usage:
//   bdd_scenario!(
//     id = "F1.1a",
//     fn_name = f1_1a,                 // ASCII identifier (no non-ascii)
//     persona = Alice,                 // required attribute; compile error if missing
//     title = "Alice registers a new principal and sees active state + first key on one screen",
//     given = |ctx| async move { ctx.alice().await },
//     when  = |ctx, alice| async move { alice.create_principal("team-x").await },
//     then  = |ctx, _alice, result: PrincipalCreateResult| async move {
//       ctx.assert(result.is_active,
//         "[F1.1a · Alice] active flag missing. expected=true, actual={}",
//         result.is_active);
//       ctx.assert(result.first_key.is_some(),
//         "[F1.1a · Alice] first key missing. expected=Some, actual=None");
//     },
//   );
//
// required attributes (in addition to id/fn_name/persona/title/given/when/then above):
//   description = "Alice creates principal 'team-x' via admin POST. \
//                  Verify response carries active=true and first_key=Some. \
//                  Audit row written with actor=admin, action=PrincipalCreate.",
//                         // ↑ Natural-language Given/When/Then narrative as a string literal.
//                         //   THIS IS REQUIRED, not optional. Missing description = compile error.
//                         //   This is the primary scenario-text input to the §12.1 jargon-lint
//                         //   (along with `title =`). Closure bodies are excluded from lint.
//                         //   Multi-line literals concatenated with `\` continuation are OK.
//                         //   Rationale: guarantees grep-extractability (Momus v3 B3).
//
// optional attributes:
//   fast = true,           // → fn name gets prefix `fast_f1_1a`, picked up by nextest -E 'test(/^fast_/)'
//   backend = sqlite_only, // → skip postgres matrix (e.g. F17.x multi-replica scenarios)
//   oos_manual = "human visual assertion; promote to visual-qa track", // OoS-manual: compiles but #[ignore] + reason doc auto-attached
```

Macro body:
- `sqlite` feature active → `#[tokio::test] async fn <fn_name>_sqlite()` generated
- `postgres` feature active && `backend != sqlite_only` → `#[tokio::test] async fn <fn_name>_postgres()` generated
- `fast = true` → both functions emitted as `fast_<fn_name>_sqlite()` / `fast_<fn_name>_postgres()`
- failure → `panic!("[{id} · {persona}] {step}: {observable_failed}")` (English message format, no Korean characters in panic text — Korean is for human report only)

---

## 4. 결정 2: 시나리오 → Rust 함수 매핑 규칙

### 4.1 ID 규칙

| Gherkin ID | Rust 함수 명 (sqlite) | Rust 함수 명 (postgres) | 위치 | Fast? |
|---|---|---|---|---|
| F1.1a | `f1_1a_sqlite` (`fast_f1_1a_sqlite`) | `f1_1a_postgres` (`fast_f1_1a_postgres`) | `crates/cc-lb-bdd-tests/tests/w1/f1_principal_create.rs` | YES |
| F11A.9 | `f11a_9_sqlite` | `f11a_9_postgres` | `tests/w2/f11a_warmup_target.rs` | NO |
| F11B.4 | `f11b_4_sqlite` | `f11b_4_postgres` | `tests/w2/f11b_warmup_execution.rs` | NO |
| F25.13 | `f25_13_sqlite` | `f25_13_postgres` | `tests/w3/f25_plugin_runtime.rs` | NO |

규칙:
- 점 → 밑줄 (`F1.1a` → `f1_1a`)
- 영문 suffix 그대로 (a/b/c/d)
- 백엔드별 함수 2개 자동 생성 (매크로가 처리)
- Fast subset 진입 시 추가 prefix `fast_`
- 파일 분할: feature 단위 (`f<N>_<short_name>.rs`)
- 디렉터리 분할: writer 단위 (`tests/w1/`, ..., `tests/w4/`)
- **cargo 자동 발견 보장**: §8.1 layout 참조

### 4.2 시나리오 메타 보존 (영문, v3)

```rust
/// # F1.1a — Alice (operator) registers a new principal and sees active state + first key on one screen
///
/// **Given** Alice is logged in to the cc-lb operator console
/// **When** she registers a new principal named "team-x"
/// **Then** the result screen shows the active flag and the first key together
///
/// Persona: Alice (operator)
/// Original (Korean human-readable report): cc-lb-true-bdd-1-team-traffic-v5.2.md L42-L58
bdd_scenario!(
    id = "F1.1a",
    fn_name = f1_1a,
    persona = Alice,
    fast = true,
    title = "Alice registers a new principal and sees active state + first key on one screen",
    ...
);
```

- Doc comment, title, step text 모두 **영문**
- 원본 한글 보고서 경로 + 라인 번호는 backlink 형태로 doc 에 보존 (`Original ... L42-L58`)
- panic / assert 메시지도 영문 (예: `"[F1.1a · Alice] active flag missing. expected=true, actual=false"`)

### 4.3 식별자 정책 (영문 source 결정으로 §v2.4.3 자연 해소)
v2 까지의 "한국어 함수명 미사용 사유" 절은 영문 source 결정 시점에서 자동 해소되어 폐지. 추가 정책 없음 — Rust 식별자는 ASCII 만 사용, step text 와 doc 도 영문.

---

## 5. 결정 3: Anthropic Upstream 모킹 전략

### 5.1 1차 도구: fake-anthropic `MessageScript` (예상 적용 ~270건)

cc-lb-bdd-tests 의 `BddCtx::with_upstream` 가 fake-anthropic 을 in-process spawn 하고 `MessageScript` 핸들을 반환.

`MessageScript` 가 모델 가능한 것 (M0 검증 완료):
- 상태 코드 / 본문 / 응답 헤더 (rate-limit, x-anthropic-* 헤더 inject)
- 응답 지연 (`with_delay(Duration)`)
- 다중 응답 FIFO 큐 (예: 첫 요청 200, 두번째 429, 세번째 200)
- Recorded request 회수 (`requests()`, `request_count()`)
- 비동기 대기 (`wait_for_requests(expected, timeout)`)
- 에러 응답 형식 (`error(status, type, msg)`)

`MessageScript` 적용 시나리오 예 (대표):
- **F1.1b** 키 발급 후 호출 — 응답 200, body recorded request 의 `body_json.principal_id` 검증
- **F3.10** rate-limit 헤더 노출 — 응답 헤더 inject + Bob 의 호출 결과 검증
- **F5.2** OAuth 갱신 반복 실패 알림 — 응답 401 × N 후 200, mock-anthropic-oauth-server 와 조합
- **F6.10a** 경로별 본문 한도 — 정상 응답 + recorded body 검증
- **F11A.9** 미리 데움 대상 — recorded request 수로 sweep 검증
- **F18.2** 호출당 비용 계산 — 응답 body 의 `usage.input_tokens` 강제 후 storage row 검증

### 5.2 2차 도구: fake-anthropic 보강 PR (M0 산출, 좁은 범위)

`MessageScript` 가 못 하는 case 만 보강. 보강 범위:

1. **SSE streaming response** — 현 `ScriptedMessageResponse::into_response()` 가 `Json(body).into_response()` 만 출력. SSE 출력 안 함.
   - 변경 안: `ScriptedMessageResponse` 에 `body_kind: ResponseKind` 필드 추가 (enum `Json(Value)` / `Sse(Vec<SseEvent>)`).
   - 영향 받는 시나리오: F3 응답 흐름의 streaming 검증 (~10건), F4 대시보드 SSE 구독 (~5건).
2. **Mid-response connection drop** — 응답 시작 후 연결 끊기.
   - 변경 안: `ResponseKind::DropAfterBytes(Vec<u8>, usize)` 변형 추가.
   - 영향 받는 시나리오: F3.8 (응답 도중 cc-lb 문제), F8.6 (위로 일관 응답 도중 죽음) (~6건).
3. **Conditional response** — request body 기반 응답 분기 (현 `pop_response` 는 FIFO 만).
   - 변경 안: `MessageScript::push_conditional(predicate: Fn(&RecordedMessageRequest) -> Option<ScriptedMessageResponse>)`.
   - 영향 받는 시나리오: F6 ACL 모델별 응답 분기 (~8건), F12 플러그인 줄 순서별 응답 (~10건).

보강 후 영향 받는 기존 사용처: `cc-lb-server/tests/pipeline_e2e.rs`, `tests/integration/managed_api_key_full_flow.rs` 등. enum 추가는 **하위 호환** (`#[non_exhaustive]` 적용).

이 PR 은 M0 게이트의 일부. 별도 plan 파일로 분기 가능 (`.omo/plans/fake-anthropic-bdd-extensions.md`).

### 5.3 3차 도구: 보조 업스트림 (wiremock + mock-anthropic-oauth-server, ~21건)

- LiteLLM 가격 카탈로그 (F18, F24): `wiremock::MockServer` (이미 [managed_api_key_full_flow.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/tests/integration/managed_api_key_full_flow.rs) 검증)
- OAuth (F5, F10): mock-anthropic-oauth-server in-process spawn

### 5.4 거부한 대안
- 실제 API VCR record/replay — rate limit·credential·재현성·CI 비용
- `ScenarioInjector` 신규 trait — MessageScript 가 이미 동일 역할 (Momus 검증). 중복 의존성 추가

---

## 6. 결정 4: DB 픽스처 (기존 패턴 그대로 + env 표준화)

### 6.1 sqlite
- per-test: `tempfile::TempDir` → `<dir>/bdd-<scenario_id>.sqlite`
- 종료 시 RAII 자동 정리

### 6.2 postgres
- per-test schema: `CREATE SCHEMA bdd_<scenario_id>_<uuid>`
- 풀 `search_path` 고정
- teardown: `DROP SCHEMA IF EXISTS <name> CASCADE`

### 6.3 환경변수 정책 (표준화)
- **정본 env: `CI_POSTGRES_URL`** ([postgres.yml:52](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/postgres.yml) 일치)
- 로컬 사용자 단축 alias: `DATABASE_URL` 또한 인식, 단 코드 내부 lookup 순서는 항상 `CI_POSTGRES_URL` 우선, fallback `DATABASE_URL`
- 둘 다 미설정 → postgres 매트릭스 자동 skip (panic 아님, `eprintln!` + early return)

`cc-lb-bdd-tests/src/backends/postgres.rs` 내 lookup:
```rust
let url = std::env::var("CI_POSTGRES_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .ok();
let Some(url) = url else {
    eprintln!("postgres bdd 매트릭스 skip: CI_POSTGRES_URL 또는 DATABASE_URL 미설정");
    return Ok(());
};
```

### 6.4 마이그레이션
`MetaStore::initialize(&storage, BackendKind::{Sqlite|Postgres})` (conformance 동일). `sqlx::migrate!` 가 내부 동작.

### 6.5 격리 확인 게이트
proxy-e2e-qa 스킬 cleanup 게이트 — 2 시나리오 동시 실행 시 sqlite 파일·postgres schema 분리 + leak 없이 정리.

---

## 7. 결정 5: 페르소나·시나리오 분류·OoS 처리

### 7.1 페르소나 bootstrap helper (영문 source, v3)

```rust
// crates/cc-lb-bdd-tests/src/personas.rs
impl BddCtx {
    /// Alice — operator. Admin token; full operator privileges (principals, keys, dashboards, killswitch).
    pub async fn alice(&self) -> OperatorClient { /* admin token + operator role */ }
    /// Bob — developer / plugin author. Per-principal API key; can call upstream, upload plugins.
    pub async fn bob(&self) -> DeveloperClient { /* api key + developer role */ }
    /// Charlie — SRE. Admin token; incident response, drain, multi-replica, backend parity, warmup leases.
    pub async fn charlie(&self) -> SreClient    { /* admin + sre role */ }
    /// Dana — auditor. Read-only token; audit log access, redaction verification, no mutation.
    pub async fn dana(&self) -> AuditorClient   { /* read-only + auditor role */ }
}
```

- Persona names: English (Alice / Bob / Charlie / Dana). Same as v5.2 markdown.
- Persona role doc: English one-liner per helper.
- Macro `persona = Alice|Bob|Charlie|Dana` is a required attribute; compile error if missing.
- panic / assert messages use English persona name (`[F1.1a · Alice] ...`), never Korean.

### 7.2 시나리오 레벨 분류 (L0/L1/L2)

| 레벨 | 정의 | 부팅 비용 | 추정 건수 |
|---|---|---|---|
| **L0** Storage | storage trait 직접 호출, server 부팅 X | <50ms | ~30 |
| **L1** Server | cc-lb-server in-process + fake-anthropic in-process | ~500ms | ~200 |
| **L2** Full-stack | cc-lb-server + fake-anthropic + admin API + dashboard read | ~1s | ~91 |
| **합계** | | | **321** |

M0 단계에서 시나리오별 1차 부여. 매핑 표에 컬럼 추가.

### 7.3 OoS-manual 처리 (정확 ID 목록 게이트)

**v1 의 "예상 5~10건" 표현 제거.** M0 산출물로 **정확 ID 목록** 작성. 표 미완 시 M1 진입 차단 (M0 게이트의 일부).

OoS-manual 후보 (M0 에서 확정 후 ID 명시):
- W1 F4.1c "한 팀 줄 누르면 자세한 보기" 의 "사람이 본다" 단언 부분 — 자동화 가능 부분은 변환 (응답 JSON 검증), "본다" 만 OoS-manual 분기
- W4 키스루크·테마·로케일 (frontend-fanout-qa 트랙)

M0 산출물 `bdd-oos-manual.md` 양식:
```
| id | reason | replacement |
|---|---|---|
| F4.1c (manual 부분) | 사용자 눈 검증, JSON 응답만으로 단언 불가 | visual-qa 스킬, M4 후 |
| F4.11b (manual 부분) | tooltip 표시 텍스트, accessible name 만 자동 검증 가능 | visual-qa 스킬 |
```

표 row 마다 (a) 정확 시나리오 ID, (b) 자동화 불가 사유 한 줄, (c) 대체 검증 트랙 명시. 빠진 row 가 있으면 M1 게이트 차단.

**M0 종료 시 강제 invariant**:
```
converted_count + oos_manual_count + blocked_count == 321
```
- `converted`: 매핑 표에 `RED→GREEN` 또는 `STABLE` 상태
- `oos_manual`: OoS-manual 표에 정확 ID 있음 + 사유 + 대체 트랙
- `blocked`: 시나리오 정의 결손 등 v6 회귀 필요한 케이스 + 정확 ID + 사유

위 합이 321 미달 또는 초과 시 M0 미완 (게이트 차단).

---

## 8. 신규 crate 구조 (cargo 자동 발견 가능 layout)

### 8.1 디렉터리 layout

```
crates/cc-lb-bdd-tests/
├── Cargo.toml          # [features] sqlite (default), postgres
├── src/
│   ├── lib.rs          # 공개 fixture / ctx
│   ├── macros.rs       # bdd_scenario! 매크로 (cc-lb-bdd-tests 안 로컬 정의)
│   ├── ctx.rs          # BddCtx (admin client + 업스트림 핸들 + storage)
│   ├── personas.rs     # alice/bob/charlie/dana
│   ├── upstream.rs     # fake-anthropic spawn helper + MessageScript wrapper
│   ├── upstream_safety.rs # loopback-only HTTP wrapper (§13 R11)
│   └── backends/
│       ├── sqlite.rs   # BddBackend impl
│       └── postgres.rs # BddBackend impl (feature-gated)
└── tests/
    ├── w1.rs           # top-level integration binary 1
    ├── w1/
    │   ├── mod.rs      # pub mod f1_principal_create; ... pub mod f26_health;
    │   ├── f1_principal_create.rs
    │   ├── f2_api_key.rs
    │   ├── f3_dispatch.rs
    │   ├── f4_dashboard.rs
    │   ├── f6_quota.rs
    │   ├── f19_prompt_cache.rs
    │   └── f26_health.rs
    ├── w2.rs           # top-level binary 2
    ├── w2/
    │   ├── mod.rs
    │   ├── f5_oauth_refresh.rs
    │   ├── f7_killswitch.rs
    │   ├── f8_upstream_outage.rs
    │   ├── f10_oauth_consent.rs
    │   ├── f11a_warmup_target.rs
    │   ├── f11b_warmup_execution.rs
    │   └── f11c_warmup_observability.rs
    ├── w3.rs           # top-level binary 3
    ├── w3/
    │   ├── mod.rs
    │   ├── f9_policy.rs
    │   ├── f12_plugin_registry.rs
    │   ├── f21_observability.rs
    │   ├── f25_plugin_runtime.rs
    │   ├── f27_admin_meta.rs
    │   └── f29_chaos.rs
    ├── w4.rs           # top-level binary 4
    ├── w4/
    │   ├── mod.rs
    │   ├── f13_retention.rs
    │   ├── f14_config_apply.rs
    │   ├── f15_lifecycle.rs
    │   ├── f17_multi_replica.rs
    │   ├── f18_cost_catalog.rs
    │   ├── f20_secret_redaction.rs
    │   └── f24_pricing_provenance.rs
    └── no_real_anthropic.rs # §13 R11 빌드시 grep + 런타임 wrapper 게이트
```

각 `tests/w<n>.rs` 내용 (예시):
```rust
mod w1;
// bdd_scenario! 매크로가 #[tokio::test] async fn 들을 모듈 안에 생성
// w1.rs 자체는 cargo 가 자동 발견하는 top-level integration test binary
```

각 `tests/w<n>/mod.rs`:
```rust
pub mod f1_principal_create;
pub mod f2_api_key;
// ...
```

이 layout 으로 cargo 가 **4개 test binary** (`w1`, `w2`, `w3`, `w4`) 를 자동 발견. nextest 도 동일하게 인식.

### 8.2 `Cargo.toml` (feature-gated optional dependency 패턴)

```toml
[package]
name = "cc-lb-bdd-tests"
version.workspace = true
edition.workspace = true
publish = false

[features]
default = ["sqlite"]
sqlite = ["dep:cc-lb-storage-sqlite"]
postgres = ["dep:cc-lb-storage-postgres"]

# 정규 dependency 로 둠 (dev-dependencies + optional 조합은 cargo feature gate 와 부정합)
[dependencies]
tokio = { workspace = true, features = ["full"] }
async-trait = { workspace = true }
anyhow = { workspace = true }
reqwest = { workspace = true }
tempfile = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
fake-anthropic = { path = "../../tests/fixtures/fake-anthropic" }
mock-anthropic-oauth-server = { path = "../../tests/fixtures/mock-anthropic-oauth-server" }
cc-lb-server = { path = "../cc-lb-server" }
cc-lb-storage-api = { path = "../cc-lb-storage-api" }
cc-lb-storage-sqlite = { path = "../cc-lb-storage-sqlite", optional = true }
cc-lb-storage-postgres = { path = "../cc-lb-storage-postgres", optional = true }

[dev-dependencies]
wiremock = { workspace = true }
```

특히 `cc-lb-storage-sqlite` / `cc-lb-storage-postgres` 는 `[dependencies]` 내 `optional = true` + `[features]` 의 `dep:` 매핑으로 control (v1 의 `[dev-dependencies] optional = true` 패턴은 cargo feature gate 와 부정합이라 제거).

`publish = false` 라 publish 부담 없음.

---

## 9. 로컬 실행 절차

### 9.1 sqlite (default)

```sh
cargo nextest run -p cc-lb-bdd-tests
```

### 9.2 postgres

```sh
# 로컬 postgres 이미 떠있다고 가정
export CI_POSTGRES_URL=postgres://postgres:testpw@localhost:5432/cc_lb_test
cargo nextest run -p cc-lb-bdd-tests --no-default-features --features postgres -- --test-threads=4
```

`CI_POSTGRES_URL` (또는 fallback `DATABASE_URL`) 미설정 → postgres 매트릭스 자동 skip.

### 9.3 단일 시나리오 디버그

각 시나리오는 `<fn_name>_sqlite` 또는 `<fn_name>_postgres` 로 변환됨 (또는 fast subset prefix `fast_<fn_name>_*`).

```sh
# F1.1a 의 sqlite 버전만:
cargo nextest run -p cc-lb-bdd-tests -E 'test(=f1_1a_sqlite)'

# F1.1a 의 sqlite + postgres 양쪽 모두 (정확 일치):
cargo nextest run -p cc-lb-bdd-tests -E 'test(/^f1_1a_(sqlite|postgres)$/)'

# Fast subset 전체:
cargo nextest run -p cc-lb-bdd-tests -E 'test(/^fast_/)'
```

(v1 의 `test(=f1_1a) and test(=f1_1a_sqlite)` 은 논리적으로 불가능, 삭제됨.)

### 9.4 환경변수
- `CC_LB_BDD_LOG=trace` — tracing 활성
- `CC_LB_BDD_KEEP_TEMPFILE=1` — 실패 시 sqlite·postgres schema 보존
- `CC_LB_TEST_READY_TIMEOUT_SECS` — 기존 변수
- `CI_POSTGRES_URL` (or `DATABASE_URL`) — §6.3

---

## 10. CI 실행 계획

### 10.1 신규 workflow: `.github/workflows/bdd.yml` (PR 게이트)

```yaml
name: bdd

on:
  pull_request:
    paths:
      - 'crates/cc-lb-bdd-tests/**'
      - 'tests/fixtures/fake-anthropic/**'
      - 'tests/fixtures/mock-anthropic-oauth-server/**'
      - 'crates/cc-lb-server/**'
      - 'crates/cc-lb-storage-*/**'
      - '.github/workflows/bdd.yml'
  push:
    branches: [master]
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

env:
  CC_LB_ADMIN_SKIP_SPA: "1"

jobs:
  bdd-sqlite-fast:
    name: BDD sqlite (fast_ subset, PR gate)
    runs-on: oracle4-cc-lb
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
        with: { rust-cache-key: bdd-sqlite }
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features sqlite \
            -E 'test(/^fast_/)'
      - name: Upload failure captures
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-sqlite-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 14

  bdd-postgres-fast:
    name: BDD postgres (fast_ subset, PR gate)
    runs-on: oracle4-cc-lb
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: testpw
          POSTGRES_DB: cc_lb_test
        ports: ['5432:5432']
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      CI_POSTGRES_URL: postgres://postgres:testpw@localhost:5432/cc_lb_test
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
        with: { rust-cache-key: bdd-postgres }
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features postgres \
            -E 'test(/^fast_/)' -- --test-threads=4
      - name: Upload failure captures
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-postgres-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 14
```

### 10.2 신규 workflow: `.github/workflows/bdd-nightly.yml` (전수)

```yaml
name: bdd-nightly

on:
  schedule:
    - cron: '0 18 * * *'  # KST 03:00
  workflow_dispatch:

jobs:
  bdd-full-sqlite:
    runs-on: oracle4-cc-lb
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features sqlite
      - name: Upload captures on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-nightly-sqlite-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 30

  bdd-full-postgres:
    runs-on: oracle4-cc-lb
    timeout-minutes: 60
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: testpw
          POSTGRES_DB: cc_lb_test
        ports: ['5432:5432']
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      CI_POSTGRES_URL: postgres://postgres:testpw@localhost:5432/cc_lb_test
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features postgres \
            -- --test-threads=4
      - name: Upload captures on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-nightly-postgres-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 30
```

### 10.3 Fast subset 정책 (정정)
- 함수명 prefix `fast_` 강제 — 매크로 인자 `fast = true` 시 자동 부여
- nextest filter `test(/^fast_/)` (Rust 식별자 호환)
- 선정 기준: 각 feature 당 happy 1 + 핵심 edge 1 + 회귀 risk 높은 1
- 목표 PR 게이트 wall-clock < 5분
- 선정 산출물: `/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md` (M0 산출, 매핑 표의 `fast` 컬럼과 일치)

### 10.4 분기별 빈도

| 트리거 | 슈트 | 백엔드 | 예상 시간 |
|---|---|---|---|
| PR `paths` match | `fast_` prefix | sqlite + postgres | ≤5분 |
| `push master` | `fast_` prefix | sqlite + postgres | ≤5분 |
| nightly cron | 전수 | sqlite + postgres | ≤30분 |
| `workflow_dispatch` 수동 | 옵션 (기본 `fast_`) | 매트릭스 선택 가능 | 가변 |

### 10.5 CI runner / 자원
- `oracle4-cc-lb` 자체호스트 runner
- postgres:18 service container
- sccache 활성

### 10.6 비밀 (Secrets)
- **0건**. Anthropic 실 API 키 절대 미투입. DB 비밀 (`POSTGRES_PASSWORD=testpw`) 은 service container 한정.

---

## 11. 마일스톤 (M0 → M5)

각 마일스톤 종료 시 다음 invariant 강제:
- `converted + OoS-manual + blocked == 321` (반드시 등호)
- 모든 변경 파일 `cargo fmt --check` 통과
- 모든 변경 crate `cargo clippy -- -D warnings` 통과
- §13 no-real-API 게이트 통과
- 새 시나리오는 RED→GREEN 증거 보관 (§12)

### M0 — Harness scaffold + W1 F1 시범 + OoS 표 완성 (1주)

**산출물**:
- `crates/cc-lb-bdd-tests/` crate 생성 (§8 layout)
- `BddCtx`, `BddBackend` trait, `bdd_scenario!` 매크로
- `personas::{alice,bob,charlie,dana}`
- fake-anthropic 보강 PR (§5.2): SSE + drop_after_bytes + push_conditional (별도 plan 분기 가능)
- W1 F1 10 scenarios 변환
- 시나리오 매핑 표 v1 (`/home/bhyoo/cc-lb-bdd/bdd-test-conversion-map.md` 321행)
- OoS-manual 정확 ID 목록 (`/home/bhyoo/cc-lb-bdd/bdd-oos-manual.md`)
- Fast subset 선정 v1 (`/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md`)
- §13 no-real-API 게이트 (loopback wrapper + grep test)

**검증 게이트 (binary)**:
- [ ] `cargo nextest run -p cc-lb-bdd-tests` sqlite 통과 (F1 10건)
- [ ] `CI_POSTGRES_URL=... cargo nextest run -p cc-lb-bdd-tests --no-default-features --features postgres -- --test-threads=4` 통과
- [ ] `cargo clippy -p cc-lb-bdd-tests --no-default-features --features sqlite -- -D warnings` 0 warning
- [ ] `cargo clippy -p cc-lb-bdd-tests --no-default-features --features postgres -- -D warnings` 0 warning
- [ ] 매핑 표 invariant: converted=10, OoS-manual=확정, blocked=확정, 합=321 (W1 F1 외는 `blocked-pending-conversion`)
- [ ] OoS-manual 표 모든 row 가 (id, reason, replacement) 채워짐
- [ ] no_real_anthropic test 통과
- [ ] 실패 주입 1회 (F1 시나리오 단언 인위적 깨뜨림) → 출력에 시나리오 ID + 페르소나 + 단계 + observable 포함

### M1 — W1 잔여 88 scenarios + CI sqlite 게이트 (1주)

**산출물**:
- W1 F2/F3/F4/F6/F19/F26 변환 (88 scenarios)
- `.github/workflows/bdd.yml` sqlite job 활성
- 매핑 표 W1 행 채움 (98 행)

**검증 게이트**:
- [ ] PR 시범 1건 → bdd.yml sqlite job 통과
- [ ] PR 게이트 wall-clock ≤ 5분
- [ ] invariant: converted ≥ 98 (W1 전수) + OoS-manual + blocked == 321

### M2 — W2 66 scenarios + CI postgres 게이트 (1.5주)

**산출물**:
- W2 F5/F7/F8/F10/F11A/F11B/F11C 변환 (66 scenarios)
- OAuth 시나리오 (F5/F10) mock-anthropic-oauth-server 사용 검증
- `.github/workflows/bdd.yml` postgres job 활성
- 매핑 표 W2 행 채움

**검증 게이트**:
- [ ] postgres job 통과 wall-clock ≤ 7분
- [ ] OAuth refresh 시나리오는 `tokio::time::pause()` + `advance()` 사용 (실시간 의존 금지)
- [ ] invariant: converted ≥ 164 + OoS-manual + blocked == 321

### M3 — W3 63 scenarios + fake-anthropic 보강 활용 (1.5주)

**산출물**:
- W3 F9/F12/F21/F25/F27/F29 변환 (63 scenarios)
- F29 (chaos): `ScriptedMessageResponse::error` 와 fake-anthropic 보강 (drop_after_bytes / push_conditional) 활용
- `plugin-handshake-spike` wasm 자산 재사용 (ci.yml 이 이미 빌드)

**검증 게이트**:
- [ ] F29 chaos 시나리오 cc-lb 회복 < 5초 단언
- [ ] 동일 시나리오 100회 반복 flake rate ≤ 1% (자동 retry 금지)
- [ ] invariant: converted ≥ 227 + OoS-manual + blocked == 321

### M4 — W4 94 scenarios + nightly 전수 (2주)

**산출물**:
- W4 F13/F14/F15/F17/F18/F20/F24 변환 (94 scenarios)
- F17 (multi-replica) sqlite 2 인스턴스 동시 부팅
- `.github/workflows/bdd-nightly.yml` 활성

**검증 게이트**:
- [ ] nightly 1주 무중단 통과
- [ ] nightly wall-clock < 30분
- [ ] invariant: converted == 321 - OoS-manual - blocked (W4 전수 변환)

### M5 — 마무리·튜닝 + Stabilization (1주)

**산출물**:
- nextest 분할 최적화
- 페르소나 fixture 캐시 검증
- 실패 진단 메시지 표준화
- `fast_` subset 재선정 (PR 시간 5분 유지)
- [cc-lb-bdd-inventory-coverage-v5.2.md](/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md) 에 BDD 함수명 컬럼 추가

**검증 게이트**:
- [ ] PR 게이트 시간 ≤ 5분 (3회 연속)
- [ ] **모든 converted 시나리오가 sqlite + postgres 양쪽에서 7회 연속 nightly 통과**
- [ ] 매핑 표 invariant 최종: converted + OoS-manual + blocked == 321
- [ ] 1326 inventory 매핑 표에 BDD 함수명 컬럼 100% 채움 (converted 시나리오 한정)

---

## 12. 검증 게이트 (모든 마일스톤 공통, Proxy-E2E-QA 스킬 기준)

각 시나리오 변환은 다음 2개 증거를 보관해야 done:

| 증거 | 출처 | 형식 | 보관 |
|---|---|---|---|
| RED→GREEN 증거 | `cargo nextest run -E 'test(=<fn>)'` 단언 깨진 상태 + 고친 상태 | nextest 출력 텍스트 | PR description 또는 PR comment 본문 (commit message 미사용) |
| Surface artifact | L0=storage row dump, L1=admin API 응답 + recorded request 로그, L2=dashboard JSON 응답 | JSON 파일 `tests/__captures__/<scenario_id>.json` | **gitignore** (commit X). CI 실패 시에만 `actions/upload-artifact` 업로드, 14일/30일 보관 |

규칙:
- 위 2 증거 모두 capture 안 했으면 그 시나리오는 done 아님
- 매핑 표 status 컬럼: `RED→GREEN` (1차 통과), `STABLE` (1주 nightly 무 flake), `OoS-manual`, `blocked-<reason>`
- captures 디렉터리는 `crates/cc-lb-bdd-tests/.gitignore` 에 추가
- CI workflow 가 `if: failure()` 조건으로 captures 디렉터리를 artifact 업로드

### 12.1 Step text jargon-rewrite 게이트 (v3, 의미 반전)

영문 source 결정에 따라 v2 의 "영문 인프라 어휘 → 한글 도메인 어휘 치환" gate 는 폐기. v3 에서는 **step text 안에 Rust 구현 jargon 이 침투했는지 검출** 하는 방향으로 의미가 반전된다.

- **허용 어휘 (domain language, 그대로 사용)**: `upstream`, `warmup`, `replica`, `callback`, `readyz`, `drain`, `sweep`, `lease`, `token`, `quota`, `principal`, `audit`, `signer`, `dialect`, `plugin`, `chain`, `terminal`, `filter`, `shape`, `observability`
- **금지 어휘 (Rust implementation jargon, lint 대상 텍스트에서 사용 금지)**:
  - 타입: `Vec`, `HashMap`, `BTreeMap`, `HashSet`, `BTreeSet`, `Arc`, `Rc`, `Mutex`, `RwLock`, `Option`, `Result`, `Box<dyn>`, `dyn Trait`, `Cow`, `String`, `&str`, `Cell`, `RefCell`, `OnceCell`, `OnceLock`, `Lazy`, `OnceLazy`
  - async/runtime: `tokio::spawn`, `tokio::time`, `tokio::sync`, `tokio::task`, `tokio::runtime`, `async fn`, `await`, `Pin<Box>`, `Future`, `Stream`, `JoinHandle`, `JoinSet`, `select!`, `join!`, `mpsc`, `oneshot`, `broadcast`, `Semaphore`, `Notify`, `Runtime`, `block_on`, `Duration`, `Instant`, `sleep`, `timeout(`, `task::yield_now`, `spawn_blocking`
  - DB / SQL: `sqlx::`, `sqlx::query`, `sqlx::query_as`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `WHERE`, `transaction`, `pool`, `connection`, `Transaction`, `PgPool`, `SqlitePool`, `Migrator`, `migrate!`
  - HTTP / wire: `axum::`, `tower::`, `reqwest::`, `hyper::`, `http::`, `StatusCode::`, `StatusCode::OK`, `HeaderMap`, `HeaderValue`, `Body`, `Bytes`, `Json(`, `Response::`, `Request::`, `Method::`, `Uri`
  - serde / JSON: `serde::`, `serde_json::`, `serde_json::Value`, `serde_yaml::`, `Deserialize`, `Serialize`, `json!`
  - UUID / IDs: `Uuid`, `uuid::`, `Uuid::new_v4`, `Uuid::parse_str`, `UlidGenerator`
  - 에러 처리: `unwrap()`, `expect(`, `unwrap_or`, `unwrap_err`, `panic!`, `anyhow::`, `thiserror::`, ` ?`, ` ?;` (Rust 의 `?` operator), `bail!`, `ensure!`, `Err(`
  - 모듈 시그니처: `pub fn`, `pub async fn`, `pub struct`, `pub enum`, `impl ` (followed by trait name), `where ` clause, lifetime parameters (`'a`, `'static` in text)
- **Lint 대상 텍스트** (grep extractable, 매크로 시그니처에서 보장):
  - `bdd_scenario!` 매크로의 `title = "<string literal>"` 인자값
  - `bdd_scenario!` 매크로의 `description = "<string literal>"` 인자값 (M0 에서 신설, **필수 인자**; given/when/then 의 자연어 서술용. 누락 시 컴파일 에러)
  - 매크로가 expand 한 생성 fn 의 doc comment (`///` 줄)
  - panic / assert 메시지 안의 영문 step text 부분 (예: `"[F1.1a · Alice] active flag missing. expected=true, actual=false"` 에서 `"active flag missing"` 부분 — formatter argument 가 아닌 literal 부분만)
- **Lint 비대상** (Rust 의도된 사용, 검사 안 함):
  - 매크로의 `given = |...| { ... }` 등 closure body — Rust 구현 코드라 모든 jargon 허용
  - 매크로의 `fn_name = f1_1a` 등 식별자 인자
  - `Cargo.toml` 의 dependencies
  - `tests/__captures__/*.json` 의 capture 데이터
- **검출 게이트**: `cc-lb-bdd-tests` crate 의 빌드 단계에 lint script (`scripts/bdd-jargon-lint.sh`) 가 위 4개 lint 대상 텍스트 (title 인자값 + description 인자값 + 생성 fn doc comment + panic literal) 만 추출 → 금지 어휘 regex grep → 발견 시 빌드 실패. 추출은 (a) `cargo expand` 출력 파싱 (CI 안정성) 또는 (b) source 단의 `bdd_scenario!\(` macro invocation 의 named-argument parser (Rust syntax tree 사용, `syn` crate) 중 (b) 선택 (CI 의 `cargo expand` 의존성 회피).
- 정책 위반 예시:
  - ❌ "Given the upstream pool has 3 connections" (`pool`, `connection` 은 구현 jargon)
  - ✅ "Given there are 3 active upstreams"
  - ❌ "When Alice spawns a tokio task to drain" (`tokio task` 는 구현 jargon)
  - ✅ "When Alice initiates a drain"
- 페르소나 ↔ 도메인 어휘는 모두 허용 (Alice/Bob/Charlie/Dana, upstream/warmup/...).

---

## 13. 리스크 + 대응

| # | 리스크 | 감지 신호 | 대응 (구체) |
|---|---|---|---|
| R1 | MessageScript 표현 부족 | M2~M3 변환 도중 시나리오 표현 불가 | §5.2 보강 PR 의 enum 변형 확장 (별 plan) |
| R2 | postgres CI 동시성 부족 | postgres job 시간 > 10분 | `--test-threads` 4 → 8 증대, 또는 nightly 만 postgres 전수 |
| R3 | step text 안에 Rust 구현 jargon 침투 (Vec/HashMap/tokio/sqlx/JoinHandle/select! 등 — §12.1 금지 어휘 목록) | 빌드 단계 lint (`scripts/bdd-jargon-lint.sh`) 가 §12.1 의 "Lint 대상 텍스트" 4종 (title literal + description literal + 생성 fn doc comment + panic literal) 만 추출하고, 도메인 어휘만 통과시키며 구현 jargon 발견 시 빌드 실패 | (a) 매크로 시그니처에 `description = "..."` string literal arg 의무화 (§3.5) — closure body 가 아닌 명시적 string 으로 step 자연어 서술, grep 가능. (b) lint 구현은 `syn` crate 으로 `bdd_scenario!\(...\)` macro invocation 의 named-argument literal 추출 (cargo expand 의존성 회피). (c) PR review 체크리스트 row "step text uses domain language only" + (d) §12.1 의 lint 비대상 (closure body / Cargo.toml / captures) 명시 |
| R4 | 페르소나 권한 사전 부여 누락 | F1/F2 시나리오 403 | `personas::*` helper bootstrap 보장, M0 게이트에서 권한 매트릭스 검증 표 산출 |
| R5 | Anthropic SSE 형식 변경 | nightly 실패 클러스터 | fake-anthropic 의 SSE 모듈에 contract test (`ScriptedMessageResponse::Sse` 검증, M0 동반) |
| R6 | 시나리오 의도 불명확 | M2~M4 변환 도중 "given 이 모호" | 매핑 표의 `blocked-ambiguous` 상태로 분류 + v6 회귀 큐 |
| R7 | CI 자원 (oracle4-cc-lb) 포화 | nightly 30분 초과 3회 | sccache 강화 + nextest archive 빌드 분리 (`cargo nextest archive` → `run --archive-file`) |
| R8 | OoS 시나리오 누락 분류 | 자동화 불가 시나리오를 자동화 시도 | M0 OoS-manual 표 row 필수 + M1 게이트가 표 완성도 검사 |
| R9 | 페르소나 fixture 의존 폭증 | M3 setup 시간 증가 ≥ 2초/시나리오 | fixture 캐시 (`once_cell::sync::OnceLock` + `Arc<Bootstrap>`) |
| R10 | flake (시간 의존 시나리오) | M3 chaos / M2 OAuth refresh 간헐 실패 | 강제: 시간 진행 필요 시 `tokio::time::pause()` + `advance()` 사용 (자동 retry 금지). flake 발견 시 즉시 매핑 표 status `RED→GREEN` 로 강등 후 재조사 |
| **R11** | **실제 Anthropic API 호출 유출** | tests/픽스처에 `api.anthropic.com` 문자열 또는 비-loopback host 등장 | (a) 빌드시 `tests/no_real_anthropic.rs` 가 `grep -r 'api\.anthropic\.com' tests/` 수행, 발견 시 panic. (b) `upstream_safety::LoopbackOnlyClient` 가 reqwest middleware 로 비-loopback host 차단, 위반 시 panic. (c) CI workflow env 에 Anthropic 키 secret 절대 미투입 (rg `secrets\.ANTHROPIC` quality.yml 가 검사) |

---

## 14. 산출물 위치

| 산출물 | 위치 | 생성 시점 | retention |
|---|---|---|---|
| 본 계획 (정본) | `/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.omo/plans/bdd-test-conversion.md` | 현 turn | 영구 |
| 본 계획 (웹 가시) | `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-test-conversion-plan.md` | 현 turn | 영구 |
| 매핑 표 (321 행) | `/home/bhyoo/cc-lb-bdd/bdd-test-conversion-map.md` | M0 산출, 매 M 갱신 | 영구 |
| OoS-manual 표 | `/home/bhyoo/cc-lb-bdd/bdd-oos-manual.md` | M0 산출 (M1 진입 게이트) | 영구 |
| Fast subset 선정 | `/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md` | M0 산출, M5 재선정 | 영구 |
| crate 코드 | `crates/cc-lb-bdd-tests/` | M0~M4 | 영구 |
| 신규 workflow | `.github/workflows/bdd.yml`, `bdd-nightly.yml` | M1, M4 | 영구 |
| fake-anthropic 보강 plan | `.omo/plans/fake-anthropic-bdd-extensions.md` | M0 동반 | 영구 |
| RED→GREEN 증거 | PR description / PR comment 본문 (commit message 미사용) | 시나리오 별 | PR history |
| Surface artifact (captures) | `crates/cc-lb-bdd-tests/tests/__captures__/<scenario_id>.json` | 시나리오 실행 시 | **gitignore**. CI 실패 시 artifact 업로드 (PR 14일, nightly 30일) |
| 1326 inventory 매핑 갱신 | `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md` (BDD 함수명 컬럼 추가) | M5 | 영구 |

---

## 15. 본 계획 외 (명시 Non-Goal 재확인)

- v6 라운드의 NC 82건·PARTIAL 95건 보강
- 1326 inventory 보정 (변환 도중 발견되는 정의 결손 → `blocked-pending-v6` 분류)
- visual-qa / Playwright 시각 회귀 (frontend-fanout-qa)
- 보안 침투 (security-research)
- 실 Anthropic API 회귀 (전혀 다른 트랙)
- soak / 부하 (soak.yml)

---

## 16. 본 계획 수락 게이트

이 계획이 다음 4 항목을 만족할 때 코드 라운드 M0 착수:

1. 사용자가 본 v2 문서를 검토하고 명시 승인 ("OK / 시작")
2. **Momus reviewer 가 본 v2 의 모든 blocking gap 해소를 확인한 APPROVED-WITH-EDITS 또는 APPROVED 회신** (v1 의 "unconditional approval" 표현은 부적합, blocking gap 해소 확인 기준으로 정정)
3. `.omo/plans/bdd-test-conversion.md` 와 `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-test-conversion-plan.md` 가 동일 (현 turn 에 sync 완료)
4. 사이드바·INDEX 에서 본 계획 접근 가능 (현 turn 에 갱신 완료)

위 4 만족 → 별도 M0 작업 plan 발행 → 코드 라운드 시작.
