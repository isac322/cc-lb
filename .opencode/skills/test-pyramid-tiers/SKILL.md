---
name: test-pyramid-tiers
description: cc-lb 테스트를 작성·이동·리뷰·분류할 때 반드시 사용. 임의의 테스트가 어느 층(T0~T5/TX)에 속해야 하는지 판정하는 기준, 표기법, 하드 룰, registry/ledger 형식, DI seam 규칙을 정의한다. Trigger: 새 테스트 작성, 테스트 이동/병합/삭제, flaky 테스트 조사, "어느 층", "test tier", "테스트 피라미드", "t2__/t3__/t4__ 접두어", CI 테스트 job 변경.
---

# cc-lb 테스트 층(tier) 분류 기준 — 확정본 v1

확정 경로: 초안 v0 → 1차 토론(3인: PyramidPurist/ProxyPragmatist/DeterminismEngineer, R1+R2) → 2차 Fable 5.1 패널(3인: Guardian/Maintainer/Adversary, R1+R2, D1~D15 투표) → Main 최종 결정. 잔여 불일치 5건의 결정은 §9에 기록.

## 1. 원칙

1. **층은 claim으로 정한다.** 테스트가 실패했을 때 한 문장으로 말할 수 있는 최소 주장(claim)과, 그 주장을 반례로 깨뜨릴 수 있는 **최소 production 경계**가 층을 정한다. 파일 위치·디렉터리·현재 사용하는 의존성·주석은 층을 바꾸지 않는다.
2. **side effect는 승격 사유가 아니라 위반 사유다.** 주장은 T2인데 실 SQLite를 쓰면 그 테스트는 "T3"가 아니라 `T2 / FAIL(concrete-sqlite)`다. "현재층"이라는 개념은 없다.
3. **아래로 밀어라, 단 손실을 기록하라.** 상위층 테스트를 하위층으로 내릴 때 fake가 재현하지 못하는 실패(`loss`)를 적고, 그 실패를 잡는 상위층 대표(`guardian`)가 registry에 있어야만 내릴 수 있다.
4. **P0는 위아래 모두 가진다.** P0 불변식은 하위층(T1/T2/T3)의 exhaustive 검증과 실 HTTP 경계 T4의 대표 1건을 모두 가진다. 같은 (journey, class)의 상위 테스트는 1개만.
5. **결정성은 전 층 공통 하드 룰이다.** real sleep·retry-until-green·silent skip·`#[ignore]`·전역 recorder·실 시계는 어느 층에서도 허용되지 않는다(T3~T5 fixture 내부의 scripted delay만 예외). 포트 핸드오프는 production이 bound 주소를 보고하지 않는 `t5__process_*`에서만 영구 예외로 허용하며, nextest process test-group 직렬화와 재시도 금지를 동반한다.
6. **비율은 대시보드다.** 층별 개수/비율은 CI gate가 읽지 않는다. hard gate는 금지 패턴 0건, T4/T5 registry 1:1, migration ledger와 테스트 수 하한, `cc-lb-testkit` 의존 경계다.

## 2. 층 정의

| 층 | 이름 | claim의 최소 경계 | 허용 의존성 | 금지 | 표식(nextest 경로 segment) | CI job |
|---|---|---|---|---|---|---|
| T0 | Static | 컴파일·린트·스키마·deny·tier lint | 컴파일러/스크립트 | — | (테스트 아님) | `static-checks` + `rust-checks` |
| T1 | Unit | 단일 함수·모듈·타입의 값/전이/정책. production trait의 test double을 **주입하지 않음** | 인메모리 값, `cc_lb_clock::TestClock`, 고정 seed/UUID, proptest, insta, `#[tokio::test(start_paused = true)]`+`advance` | 실 I/O 일체, `tokio::spawn`(bounded channel+join으로 관찰되는 crate-private spawner 제외), multi_thread flavor | 없음(기본값) — inline `#[cfg(test)] mod tests` 또는 `tests/t1/` | `rust-checks` (PR 필수) |
| T2 | Component | production trait의 **fake/in-memory** 구현을 주입해 2개 이상 모듈/크레이트의 wiring을 검증 | T1 + `tower::ServiceExt::oneshot`, in-memory trait fake(§6 fake 규칙), bounded channel, test-scoped `RequestEventBus`/recorder, `TestClock` | concrete SQLite/Postgres/FS/Wasmtime/socket/subprocess | `t2__` — `tests/t2/` 또는 inline `#[cfg(test)] mod t2__<name>` | `rust-checks` |
| T3 | Contract | 실 어댑터/프로토콜 구현 하나가 계약을 지킨다. subtype은 **파일 위치로 유도**: `t3-shared`=`cc-lb-storage-conformance/src/scenarios/*`(같은 symbol을 sqlite·postgres·fake runner가 모두 실행), `t3-native`=`crates/cc-lb-storage-{sqlite,postgres}/tests/*`, `cc-lb-runtime-wasmtime/tests/*`(backend 고유 의미론: query plan, MVCC, NOTIFY, advisory lock, FS cache, WASM trap), `t3-protocol`=signer↔mock OAuth, dialect↔응답 샘플, `WasmtimeFilterPlugin`↔ABI | 실 SQLite(tempfile)/Postgres(UUID schema)/Wasmtime; `t3-protocol`에서만 **held** `127.0.0.1:0` listener(같은 함수에서 직접 serve/accept) | 실 sleep/retry, `Option<Fixture>` skip, 전체 `App` 조립, port handoff | `t3__`(sqlite/wasm/protocol), `t3_postgres__`(Postgres 필요) | `nextest-cov`(PR 필수). 로컬 분리 실행은 `contract`/`contract-postgres` |
| T4 | Integration (in-process) | 한 프로세스에서 실 `App`(composition root) 조립 + loopback socket + fake-anthropic(in-process) + SQLite로 **(journey, terminal class)** 1건 | localhost socket, 실 hyper/rustls, SQLite, 실 wall clock(존재/순서 단정만) | subprocess, browser, docker, `elapsed < N` 단정, fixed sleep, blind retry, port handoff | `t4__` — `tests/integration/t4/` | `nextest-cov`(PR 필수). 로컬 분리 실행은 `integration` — registry 등록 필수 |
| T5 | E2E / Acceptance | OS process/실 클라이언트/브라우저/멀티 프로세스 의미가 assertion의 명사. subtype 위치 유도: `t5-process`=`tests/e2e/t5/process/`(argv/env/config, signal, exit code, restart 복구, multi-replica), `t5-ui-mocked`=`web/e2e/ui-mocked/`(실 브라우저+`page.route`; backend 증거 아님), `t5-full`=`web/e2e/full/`, `tests/real-client/*` | 무엇이든. production이 bound 주소를 보고하면 child가 `:0` bind 후 startup log로 보고; 그렇지 않으면 직렬 process test에 한해 `reserve_addr` 영구 예외 | fixed sleep, retry, silent skip | `t5__` | `nextest-cov` + `e2e`(경로별 PR 또는 nightly) — registry 등록 필수 |
| TX | Non-functional | throughput/latency/p99/memory/burst 수량/loom schedule/fuzz/bench | 무엇이든 | `#[ignore]`(filter 제외로 대체) | `tx__` 또는 별도 package(`tests/load`, `tests/loom`, `tests/stress-suite`, `fuzz`, benches) | `tx`(nightly schedule/수동 실행, PostgreSQL service 포함), 비율 분모와 기본 filter에서 제외 |

프론트엔드: Vitest project `lib`=T1, `components`=T2, API 스키마 양측 검증(Rust `schemars` 산출물 ↔ TS zod)=T3, Playwright `ui-mocked`=T5-ui-mocked, `full`=T5-full. Rust 분모와 합산하지 않는다.

## 3. 판정 절차 (테스트 함수 1개당)

표기(한 줄):
```
<nextest path> | intended=T<n> | conformity=PASS | FAIL(<code>,...) | target=T<n> | DELETE duplicate-of=<registry key 또는 path> | SPLIT[T<a>:<claim>, T<b>:<claim>]
```

- **Q0.** assertion이 throughput/latency/p99/memory/burst 수량/loom schedule/fuzz인가? → 예: `intended=TX`, 종료. 기능 불변식 proptest는 아니오.
- **Q1.** 실패 메시지 한 문장으로 **최소 claim**을 적는다. 서로 다른 경계를 요구하는 claim이 둘 이상이면 `SPLIT`.
- **Q2.** claim을 한 층 아래 fake로 바꿔도 production에서 가능한 실패 원인이 보존되는가? 보존되면 한 층 내린다. 반복. 멈춘 층이 `intended`.
  - 값/전이/정책(헤더 규칙, 가격, 쿼터 산술, 라우팅 점수, SSE 프레임 파싱, 상태기계) → T1
  - production trait fake를 주입해야 드러나는 wiring(요청→예약→라우팅→서명 헤더, 이벤트→assembler→store 호출) → T2
  - 실 어댑터/프로토콜 구현의 의미론(SQL, MVCC, NOTIFY, WASM ABI, OAuth PKCE 교환) → T3
  - 실 hyper/TCP/TLS framing, 커넥션 재사용, half-close, 취소 전파, composition root wiring → T4
  - OS process/argv/signal/exit/restart/multi-process/브라우저 → T5
- **Q3.** 같은 claim이 하위층에 이미 있는가? 있으면 `DELETE duplicate-of=<key>`. 상위층(T4/T5) 후보는 `journey signature = (entry surface, production boundary 순서, terminal class)`가 registry에 이미 있으면 DELETE. HTTP status/body/header/chunk 크기만 다른 변형은 signature가 같다(T1/T2 table로 흡수).
- **Q4.** §4 하드 룰 위반 여부 → `conformity=PASS` 또는 `FAIL(<code>,...)`.
- **Q5.** `target`: 기본은 `intended`. FAIL이면 §5 seam/fake 도입 후 재작성 대상. 상위→하위 이동 시 ledger에 `loss=[...]`, `guardian=<registry key>` 기록(§7).
- **Q6.** 위치: private 접근 필요 → inline(T1 `mod tests`, T2 `mod t2__*`). 아니면 `tests/tN/`. T3 이상 inline 금지. 테스트용 production `pub` 추가 금지(AGENTS.md).

## 4. 하드 룰 (기계 검증: `scripts/check-test-tiers.sh`)

검사 범위: `cargo nextest list`가 열거하는 모든 테스트 바이너리의 전체 소스(helper, `tests/common`, `tests/*_support`, fixture 크레이트 포함). helper에 숨긴 위반은 본문 위반과 동일.

reason code(닫힌 목록 21개): `concrete-sqlite`, `concrete-postgres`, `concrete-fs`, `wasmtime`, `socket`, `subprocess`, `real-sleep`, `real-clock`, `rand`, `env`, `global-recorder`, `port-handoff`, `retry`, `silent-skip`, `ignore`, `uncontrolled-spawn`, `multi-thread`, `raw-sql`, `no-seam`, `weakened-assertion`(리뷰 판정), `elapsed-assert`.

공통(전 층, 예외 없음):
- `ignore`: `#\[ignore\b`
- `silent-skip`: `eprintln!\(\s*"skip`, `return\s+Ok\(\(\)\)\s*;?\s*//.*skip`, `Option<.*Fixture>` 반환 fixture, `CI_POSTGRES_URL|DATABASE_URL_TEST|PG_URL`을 읽는 `env::var`가 postgres fixture 모듈 1곳 외 등장
- `retry`: `SERVER_START_ATTEMPTS`, `attempts?\s*<`, `for _ in 0..\d+\s*\{[^}]*(sleep|try)`, `loop\s*\{[^}]*sleep`
- `port-handoff`: `reserve_addr|free_addr`, `TcpListener::bind\([^)]*:0[^)]*\)[^;]*\.local_addr\(\)[^;]*;\s*drop`
- `env`: `env::(set_var|remove_var)`, `std::env::var` (child `Command::env`는 허용)
- `global-recorder`: `install_prometheus|set_global_recorder|install_recorder` (T4 `t4__parity_metrics_*` 1건만 `tier-allow`)
- `real-sleep`: `std::thread::sleep`
- `elapsed-assert`: `elapsed\(\)\s*[<>]`, `p99|p50` 단정 (TX 전용)

T1/T2 `fast` 프로파일(공통 + 아래):
- `concrete-*`/`wasmtime`/`socket`/`subprocess`: `(std|tokio)::fs::`, `tempfile\b`, `sqlx::`, `open_sqlite|SqliteStorage|SqlitePool|PgPool|PostgresStorage`, `wasmtime::`, `TcpListener|TcpStream|UdpSocket`, `wiremock|MockServer`, `process::Command|CARGO_BIN_EXE`
- `real-clock`: `SystemClock|SystemTime::now|Instant::now|Utc::now` (테스트 코드에서. `TestClock` 사용)
- `rand`: `rand::|make_rng|Uuid::new_v4` (고정 seed/ID 사용)
- `real-sleep`: `tokio::time::(sleep|interval|timeout)`가 같은 테스트 함수에 `start_paused = true` 없이 등장
- `multi-thread`: `flavor\s*=\s*"multi_thread"`
- `uncontrolled-spawn`: `tokio::spawn`이 `JoinHandle` await/abort 또는 bounded channel ack 없이 사용 (T2는 test-scoped spawner/`JoinSet` 허용)

T3(공통 + 아래):
- `subprocess`: `process::Command|CARGO_BIN_EXE|spawn_test_server`
- `socket`: `TcpListener::bind`가 protocol adapter SUT 밖에서 등장, 또는 bind한 listener를 같은 함수에서 serve/accept하지 않음
- `real-clock`: 어댑터 생성자에 `SystemClock` 주입(→`TestClock`)
- `retry`: `emit_until_received|startup_delay` 류 반복 emit (NOTIFY는 listener-ready ack → 1회 emit → bounded receive)

T4(공통 + 아래):
- `subprocess`, `browser|docker|Playwright`
- 완료 조건은 response/ack/commit; `tokio::time::timeout`은 hang 방지 상한으로만(결과 단정 금지)

T5(공통): `Option<Env>` silent skip, fixed sleep, blind startup retry 금지. child는 RAII kill/wait.

TX: 기본 filter에서 제외한다. `cargo nextest run --workspace --all-features --profile tx`로 명시 실행하며, PostgreSQL TX는 `CI_POSTGRES_URL`이 없으면 실패한다. `#[ignore]`는 0건이어야 한다.

production 코드 lint(DI seam 결과 유지): `crates/**/src/**`에서 `SystemTime::now\(` 는 `cc-lb-clock::SystemClock` 1곳만; `rand::make_rng\(|thread_rng\(` 는 composition root(`cc-lb-server/src/{app,bootstrap}.rs`)만; `std::env::var` 는 `cc-lb-server` 기동 코드만.

예외 형식(유일): `// tier-allow(<code>): <reason> until=YYYY-MM-DD` — 만료 시 CI 실패. 영구 예외(until 없음)는 두 가지뿐: `tier-allow(multi-thread): os-thread claim`(OS 스레드 병렬성 자체가 검증 대상), `tier-allow(port-handoff): t5 process`(`t5__process_*` + nextest `test-group = 'process'` 직렬 + 재시도 없음). `ignore`, `silent-skip`, `retry`, `env`는 예외 자체가 존재하지 않는다.

## 5. 표식과 배치

- 진실은 nextest 테스트 경로의 tier segment 하나: `t2__|t3__|t3_postgres__|t4__|t5__|tx__`. 없으면 T1. 경로에 segment가 2개 이상이면 lint 실패.
- 디렉터리는 lint로 일치 검사: `crates/<c>/tests/t2/*.rs` 안의 모듈은 `t2__`로 시작해야 하고 그 역도 성립. `tests/all.rs` 단일 바이너리 유지(`autotests=false`), `scripts/check-integration-test-consolidation.sh`는 재귀 디렉터리까지 검사하도록 확장.
- nextest filter는 `test(/(^|::)t3__/)`처럼 앵커를 써서 inline `mod t2__wiring`도 잡는다.
- 선언 주석(`// tier:`)은 도입하지 않는다.
`tN__`의 연속 밑줄은 Rust의 `non_snake_case` lint를 유발하므로 integration test root(`tests/all.rs`) 또는 해당 `#[cfg(test)]` module에 `#[allow(non_snake_case)]`를 둔다. production module 전체에는 적용하지 않는다.

## 6. fake 규칙 (T2)

- fake는 **recording/scripted** 구현이어야 한다: 입력을 기록하고 미리 지정된 응답을 돌려준다. 내부에 정책 로직을 재구현하지 않는다.
- storage-api trait을 대체하는 fake(`cc-lb-storage-api` 33 trait)는 `cc-lb-storage-conformance` shared 시나리오를 **세 번째 backend(`t3__fake_*` runner)**로 통과해야 T2에서 사용할 수 있다. 시나리오가 없는 trait의 fake를 도입하는 PR은 **같은 PR에서** 해당 trait의 최소 shared 시나리오(roundtrip + 경계 1개)를 추가한다(2차 패널 Maintainer안 채택: 인메모리 구현 부재 trait이 20개 이상이라 선행 일괄 작성은 마이그레이션을 직렬화함). conformance 실패는 fake 수정 대상이지 시나리오 완화 대상이 아니다.
- 비-storage fake(`UpstreamDispatch`, `Signer`, `RouterPlugin`, `UpstreamDialect`, `EngineMetricsHook`, `RequestEventBus`)는 crate-local(`crates/cc-lb-engine/tests/common`, `storage_support`)에 두고, 3개 이상 크레이트에서 동일 정의가 중복될 때만 `cc-lb-testkit`(foundation-only)으로 추출한다.
- `cc-lb-testkit` 의존 allowlist(`scripts/check-test-tiers.sh`로 검사): `cc-lb-aead, cc-lb-clock, cc-lb-domain, cc-lb-routing, cc-lb-storage-api, cc-lb-request-log, async-trait, bytes, http, http-body-util, tower, tokio, tokio-util, metrics, metrics-util, serde_json, sha2, uuid`. `cc-lb-aead`와 `tokio-util`은 각각 `UpstreamStore`와 `RuntimeChangeNotifier`의 공개 trait 시그니처가 직접 노출하며, `sha2`는 `PriceCatalogCache`의 payload-hash 계약을 구현하는 데 필요하다. engine/admin/server/control/sqlx/wasmtime/reqwest/hyper-util 의존 금지. helper 추가 PR은 ≥3개 crate-local 중복 정의 삭제를 동반한다.

## 7. 상위층 registry와 마이그레이션 ledger

`docs/testing/upper-tier-registry.toml` — `t4__`/`t5__` 테스트는 항목과 1:1:
```toml
[[entry]]
key = "proxy-messages-stream/malformed-chunk-or-eof"
journey = "proxy-messages-stream"      # entry surface + boundary 순서 (closed list, 아래)
class = "malformed-chunk-or-eof"        # terminal class (closed enum, 아래)
test = "tests-integration::integration::t4__proxy_stream_malformed_chunk_maps_to_upstream_stream_error"
p0 = ["I6-sse-boundary"]                # 담당 P0 불변식 id
why_no_fake = "hyper 청크 디코더가 커널 경계에서 EOF를 만드는 경로는 tower fake로 재현 불가"
```
journey(closed list): `proxy-messages-nonstream`, `proxy-messages-stream`, `proxy-count-tokens`, `proxy-models`, `admin-crud`, `admin-sse`, `metrics-scrape`, `process-boot`, `process-cli`, `process-reload`, `process-drain`, `multi-replica`.
class(closed enum): `happy`, `connect-failure`, `timeout-before-headers`, `timeout-mid-body`, `malformed-chunk-or-eof`, `half-close-rst`, `downstream-cancel`, `401-refresh-replay`, `tls-handshake`, `signal-drain`, `restart-recovery`, `multi-replica-notify`, `parity-composition-root`. (`hop-by-hop-rewire`, `body-byte-preservation`은 class가 아니라 `happy` 테스트에 붙는 assertion이다.) 새 journey/class는 registry PR로만 추가하며 `why_no_fake`, 결정적 fixture, 대표 테스트 1개를 동반.

`docs/testing/migration-ledger.toml` — 이동/병합/삭제되는 모든 테스트 함수 1행:
```toml
[[move]]
old = "cc-lb-server::rfc_0002_fix_live_qa::lqa_2a_upstream_401_maps_to_terminal_row"
action = "MOVE"                          # MOVE | DELETE | SPLIT | MERGE
new = ["cc-lb-engine::t2__terminal_mapping::status_table"]
duplicate_of = ""                        # DELETE일 때 registry key 또는 path
loss = ["hyper 401 body drain 후 재dispatch 경로"]
guardian = "proxy-messages-nonstream/401-refresh-replay"
```
규칙: `loss ≠ []`인데 guardian이 registry에 없으면 이동 금지(원본을 `FAIL`로 유지). `MERGE`는 원본 함수마다 같은 `new` 대상의 별도 행으로 기록하고 `asserted`에 보존한 assertion 식을 나열한다. ledger에 없는 테스트 함수 수 감소는 마이그레이션 기간 CI 실패. 재작성된 assertion은 원본과 같거나 더 정확해야 하며(`assert_eq!`→`||`/범위 predicate, assert 수 감소는 `FAIL(weakened-assertion)`).

## 8. DI seam 도입 규칙 (production 로직 불변 보증)

1. seam 변경은 생성자·시그니처·필드·빌더 추가와 기존 본문의 1:1 이동/호출 치환만 허용한다. production `#[cfg(test)]` 분기와 테스트 전용 public flush/observer API는 금지한다.
2. 리팩터 전에 `build_app_with_path`로 production composition root를 조립하는 `t4__parity_characterization_golden`을 먼저 녹색으로 만들고, seam 변경 뒤에도 status·headers·body·`RequestEvent`·audit·quota 스냅샷 diff 0을 유지한다. 이 golden은 영구 회귀 테스트다.
3. wall clock seam은 기존 `ClockHandle`을 전달하고 `SystemTime::now()` 호출만 `clock.now()`로 치환한다. 기존 u64 millisecond 절단·오류 fallback 의미론을 유지한다.
4. RNG는 새 trait을 만들지 않는다. Lifecycle의 기존 `with_terminal_rng_seed`를 유지하고, chaos/reconcile에는 같은 `StdRng::from_seed` 빌더 패턴만 추가한다. `random_range` 표현식과 production 기본 RNG를 바꾸지 않는다.
5. monotonic 시간은 런타임 안에서만 `tokio::time::Instant`로 치환한다. 동기 `Drop`/`spawn_blocking` 경로는 `std::time::Instant`를 유지하고 근거 있는 `tier-allow(real-clock)`를 사용한다.
6. 실 listener seam은 `App::start`의 serve 본문을 `App::start_with_listeners`로 이동하고, production `start`가 기존 bind 순서로 listener를 만든 뒤 위임하게 한다. T4는 held listener를 직접 전달한다.
7. 기본 wiring이 real 구현을 고르는지는 별도 테스트 전용 composition API가 아니라 영구 characterization golden과 T5 process canary가 증명한다.

## 9. 잔여 불일치에 대한 Main 결정

1. **fake conformance 강도** — Maintainer안: 같은 PR에서 시나리오 추가, storage-api trait 한정. (근거: `local://arch-storage-admin-control.md` §3.1 — 인메모리 구현 부재 trait 20개 이상.)
2. **`live_qa_1_default_boot`** — `t5__process_boot_default_config`(T5-process canary 1건). P0 happy journey는 T4 `lqa_1a` 후계가 담당. `e2e` job은 `crates/cc-lb-server/**`, `tests/**` 경로 변경 PR에서 실행.
3. **`SystemClock` in T1~T4 테스트 코드** — `FAIL(real-clock)`. 일괄 치환 PR 1건으로 해소(개별 tier-allow 누적 금지).
4. **registry 축** — journey와 class를 분리한 2필드(§7). hop-by-hop/body-byte는 assertion.
5. **connect-failure fixture** — T4 대표는 held listener가 accept 직후 close(EOF-before-headers). ECONNREFUSED/RESET/EOF의 정확 `io::ErrorKind`→`error_code` 매핑은 T2 table이 담당하며 손실은 ledger에 기록한다.
6. **characterization golden 수명** — `build_app_with_path` 조립 경로의 유일한 전체 payload 단정이므로 마이그레이션 후에도 영구 유지한다.
7. **`with_terminal_rng_seed`** — 제거하거나 새 RNG trait으로 감싸지 않는다. 기존 빌더를 그대로 재사용한다.

## 10. 캘리브레이션 (합의된 판정 예시)

| 샘플 | 판정 |
|---|---|
| `cc-lb-server/tests/rfc_0002_fix_live_qa.rs` (21) | dominant `intended=T2 / FAIL(subprocess, concrete-sqlite, socket, real-sleep, retry, raw-sql)`. T4 잔존: `lqa_1a`(proxy-messages-nonstream/happy), `lqa_1b`(proxy-messages-stream/happy), `lqa_4a`(admin-sse/happy). T5: `live_qa_1_default_boot`→`t5__process_boot_default_config`. TX: `lqa_3a_bus_drop`, `lqa_6e_admin_sse_keeps_up`. 나머지 16 → `t2__` table(in-process `App` router oneshot + fake upstream tower service + in-memory `RequestEventStore` + assembler ack). |
| `cc-lb-engine/src/lifecycle.rs` inline | dominant T1 PASS. `build_candidates_cache_score` `FAIL(real-clock)`→`TestClock`. p99 latency 테스트 `#[ignore]`→`tx__`로 이동. |
| `cc-lb-engine/tests/response_transform_paths.rs` (42) | dominant T2. SQLite 사용 13건 `FAIL(concrete-sqlite, real-sleep, real-clock)` → in-memory `RequestEventStore` + assembler ack. |
| `cc-lb-admin/tests/v1_plugins.rs` (32) | dominant `T2 / FAIL(concrete-sqlite)` (helper `config_admin_common::temp_storage`가 `open_sqlite`). `registry_delete_when_unreferenced_deletes_blob_and_cache_file` → `SPLIT[T2: route→store 호출, T3-native(FS): 캐시 파일 삭제]`. `chain_rebalance_emits_chain_audit` `FAIL(real-sleep)`→observed audit sink ack. |
| `cc-lb-storage-sqlite/tests/request_events_cursor.rs` (7) | T3-native(`EXPLAIN QUERY PLAN` 단정) PASS 단 `Arc::new(SystemClock)`→`TestClock`(`FAIL(real-clock)` 일괄 치환). |
| `cc-lb-storage-conformance/tests/storage_roundtrips_sqlite.rs` + scenarios | T3-shared PASS. `runtime_change_notifier`: `FAIL(retry, silent-skip, real-sleep)` → listener-ready ack→1회 emit→bounded receive; latency budget은 `SPLIT[T3: delivery, TX: budget]`. |
| `tests/integration/terminal_observation.rs` (11) | dominant T2 (`FAIL(socket, concrete-sqlite, env, port-handoff, real-sleep)`). T4 잔존(registry): `terminal_success_non_stream`(happy), `terminal_success_stream`/`ChunkedSseMock`(malformed-chunk-or-eof), `terminal_upstream_dispatch_failed`(connect-failure, accept-then-close로 재작성), `terminal_tower_timeout`(timeout-before-headers). `terminal_upstream_4xx/5xx/auth/limit/body-size` → `t2__terminal_mapping::status_table`. |
| `cc-lb-scheduler/tests/oauth_refresh.rs` (6) | dominant T2 (`FAIL(rand)`: `Uuid::new_v4`→고정 ID). deleted/missing/non-OAuth/disabled eligibility 4건 → `SPLIT`하여 `refresh_eligibility(record)->Eligibility` 순수 함수 T1(추출은 본문 이동만, 로직 불변). |
| `cc-lb-engine/src/usage_parser.rs` inline | T1 PASS. |
| `tests/property/tests/subscription_preference.rs` | T1 PASS(proptest). `t1__` 접두어 불필요. |
