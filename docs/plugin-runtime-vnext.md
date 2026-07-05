# Plugin Runtime vNext — 설계 문서

상태: Proposal (구현 미시작)
대상 독자: cc-lb 코어/플랫폼 엔지니어, plugin 작성자
선행 문서: `docs/runtime-management.md`, `docs/plugin-author-guide.md`
근거 audit: `.omo/ultraresearch/20260627-plugin-hotpath-vnext/SYNTHESIS.md`

## 0. 목적과 범위

cc-lb의 plugin 호출 핫패스에서 현재 Extism wrapper가 부과하는 per-call OS thread/Tokio runtime/spawn_blocking/Mutex/JSON+base64 비용을 제거하고, 서버 재시작 없이 plugin을 동적으로 교체하며, plugin 작성자와 호스트 양쪽에서 함수 시그니처의 타입 안전성을 컴파일 단계부터 보장하는 새 plugin runtime의 설계 결정을 기록한다.

문서 범위: 아키텍처 결정 + 컴포넌트 설계 + 마이그레이션 계획 + 미해결 위험.
범위 밖: 코드 PR, plugin 저작 튜토리얼(Phase 4에 `docs/plugin-author-guide.md` 갱신), 실측 벤치 수치(Phase 3에서 채움).

## 1. 배경

cc-lb의 plugin 호출은 모든 요청 핫패스에 있다. 현재 구조는 다음 비용을 매 요청마다 지불한다.

1. `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` `dispatch_wire_call_sync`, `dispatch_filter_call_sync`가 호출마다 `std::thread::Builder::new().spawn(...)`로 새 OS thread를 만들고, 그 안에서 `tokio::runtime::Builder::new_current_thread().build()`로 새 Tokio runtime을 만든 뒤 `block_on(dispatch_*_async)` 한다.
2. async dispatch 내부에서 `tokio::task::spawn_blocking`으로 blocking pool에 다시 떨어뜨려 `Mutex<Plugin>`을 잡고 `extism::Plugin::call::<String, String>("filter", json)` 호출.
3. wire는 JSON envelope, body는 base64.
4. `PluginSlot.current::ArcSwap<PluginCell>` 안의 `PluginCell { plugin: Mutex<Plugin> }`은 한 slot에 한 인스턴스. 동일 plugin의 동시 호출은 mutex로 직렬.

요약: 요청당 `OS thread 생성 + Tokio runtime 생성 + spawn_blocking + Mutex + JSON/base64 직렬화`가 누적된다. plugin 자체의 wasm 실행 비용보다 wrapper 비용이 큰 상황.

`benches/extism_sse_overhead`는 batched 32-event p99가 1ms를 넘으면 wasmtime으로 escalate하도록 이미 명시된 기준이 있다(`STAY_EXTISM` vs `ESCALATE_WASMTIME`). 핫패스 product-critical 함수에서는 이 마진이 충분치 않다.

## 2. 목표 / 비목표

### 목표

- **G1 thread hop 0**: filter / shape / observe / sign 호출 핫패스에서 새 OS thread 생성, 새 Tokio runtime 생성, blocking pool로의 hop을 전부 없앤다.
- **G2 무중단 동적 교체**: server 재시작 없이 plugin 코드와 manifest를 atomically 교체. 기존 `DynamicViewHolder = ArcSwap<DynamicView>` + per-slot `ArcSwap<PluginCell>` + two-phase stage/commit 의미를 그대로 보존.
- **G3 양방향 컴파일타임 타입 안전**: plugin 작성자와 호스트 양쪽이 같은 함수 시그니처를 컴파일 단계에서 검증. wire 직렬화 mismatch는 런타임이 아니라 빌드 또는 plugin 등록 시점에 실패.
- **G4 페이로드 직렬화 비용 제거**: JSON, base64 제거. body와 헤더는 raw bytes로 zero-copy 또는 single memcpy 수준으로 전달.
- **G5 sandbox 유지**: 메모리 격리, 자원 한계(메모리 페이지, 명령 수), trap 격리는 현재 Extism 수준 이상.
- **G6 다언어 plugin 가능성 보존**: 우선은 Rust-only, 그러나 wire 포맷 교체로 polyglot 확장 가능한 구조.

### 비목표

- **N1 외부 BC**: cc-lb는 아직 배포되지 않았다. 기존 Extism plugin 바이너리 호환은 유지하지 않는다.
- **N2 wall-clock kill 보장**: "정확히 N ms 지나면 무조건 죽인다"는 보장은 제공하지 않는다. 자세한 이유는 §5.5.
- **N3 host async I/O를 plugin이 부르는 모델**: 핫패스에서 plugin이 host async function을 호출해 yield하는 시나리오는 도입하지 않는다.
- **N4 단일 release에 모두 마이그레이션**: feature gate로 점진 전환.

### 사용자가 명시한 제약 (원문 발췌)

이 문서의 제약 조건은 다음 사용자 발화에서 직접 유도된다.

- m0027 (문제 정의): "thread로 처리하는게 말이 안돼. wasm을 바로 실행할 수 있는 방법은 없어? 이게 hot path야."
- m0049 (요구사항 확정):
  - "rewrite여도 상관없고, 이게 진짜 엄청나게 hot path라서 최적화 한 되면 제품 자체가 의미가 없어."
  - "아직 배포된 게 아니라서 하위 호환성은 다 지워버리려고 돼."
  - "thread pool 같은 레이어 비용을 다 없애는 거야."
  - "extism을 안 써도 되고, 플러그인 다시 처음부터 빌드해도 되고, wasm 자체를 안 써도 돼."
  - "서버 재시작 없이 동적으로 갈아끼울 수 있어야 하고, thread hop이 없었으면 좋겠어."
  - "최대한 없는 방식을 선호하고, 만약 그렇게는 불가능하다면 thread pool까지는 용서해줄게."
  - "이 플러그인 개발할 때에도 중요한 게... 함수 시그니처가 아주 중요해. 플러그인 개발할 때 이 함수 시그니처의 타입 안정성을 지키면서 개발할 수 있는 방식이 있었으면 좋겠어."
- m0077 (fuel/스레드 의미 확정): "fuel 문제는 상관없어. 그건 자기가 손해 보는 거야." → 본 문서 §5.5의 fuel deterministic 정책 + wall-clock kill 미보장 결정의 사용자 측 승인.

이 인용은 §2 목표(G1~G6), 비목표(N1~N4), §4 채택 근거, §5.5 fuel 정책의 사용자 측 정당성 근거다.

## 3. 현재 아키텍처 (보존할 부분, 폐기할 부분)

### 보존

- `cc-lb-plugin-api/src/traits.rs`의 trait 모양: `FilterPlugin`, `UpstreamDialect`, `ObservabilityHook`, `Signer`, `SignerFactory`, `PluginRuntime`. 호출자(`Lifecycle::handle`, `attempt`, `execute_filter_pipeline`)가 의존하는 표면. 시그니처 변경 최소화.
- `cc-lb-engine/src/dynamic_view.rs::DynamicViewHolder = ArcSwap<DynamicView>`. lock-free request path.
- `cc-lb-engine/src/api_keys/principal_view.rs::PrincipalSpecCached`와 `resolved_pipeline / resolved_dialect / resolved_hooks`.
- `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`의 per-principal staging.
- `cc-lb-server/src/reconcile.rs`의 DB polling + revision-hash 기반 rebuild.
- `cc-lb-server/src/reload.rs::ConfigWatcher`의 ArcSwap 기반 config swap.
- `data/plugins/wasm/cache/{sha256}.wasm` 디스크 캐시.
- `crates/cc-lb-runtime-protocol/src/handshake.rs`가 담당하던 "plugin과 호스트의 스키마/버전 합의" **의도(semantic intent)**만 계승. 현 구현은 JSON `cc_lb_handshake` export를 실제로 실행하는 방식이고, 새 ABI는 wasm custom section `cc_lb_schema_hash`를 inspect하는 정적 비교로 완전히 다른 메커니즘. 코드는 폐기, 의도만 보존.

### 폐기

- `crates/cc-lb-runtime-extism/` 전체.
- `crates/cc-lb-runtime-protocol/` (dispatch.rs의 JSON envelope).
- `crates/cc-lb-plugin-wire/v1, v2, v3` JSON 정의와 base64 필드.
- `crates/cc-lb-pdk` 중 Extism PDK 의존 부분.
- 동봉 plugin (`plugins/router/*`, `plugins/shape/*`) 빌드 산출물 — 새 PDK로 재빌드 (소스 로직은 유지).

**기존 Extism PDK로 빌드된 .wasm 모듈은 새 runtime에서 그대로 동작하지 않는다** (m0044 사용자 질문 직답): 호스트 import 시그니처가 다르고(`extism:host/env::input_load_u8` 등 vs 본 설계의 `cc_lb_alloc/cc_lb_free` 자체 export 모델), wire envelope이 JSON+base64 vs rkyv archive로 완전히 다르며, schema_hash custom section이 없어 등록 시점 검증을 통과하지 못한다. cc-lb는 아직 배포되지 않아 외부 plugin도 없으므로(N1) 동봉 plugin만 재빌드하면 충분.

## 4. 대안 비교

8개 에이전트 교차 검증과 oracle adversarial review 합의 결과(audit는 SYNTHESIS.md §"Cross-agent consensus").

| 후보 | thread hop | 타입 안전 | 무중단 교체 | sandbox | 결정 |
|---|---|---|---|---|---|
| 현 Extism + JSON | per-call OS thread + runtime + spawn_blocking | runtime only | 됨 | 강 | **폐기** |
| **Wasmtime core ABI + cc-lb PDK + rkyv** | **0 (per-worker store)** | **3계층 컴파일타임 + 로드타임 hash** | **됨 (기존 primitive)** | **강** | **채택** |
| Wasmtime Component Model + WIT | 0 가능 | 양쪽 컴파일타임 | 됨 | 강 | 보류 (canonical ABI list/string copy, 현재 sync path overhead 보고됨) |
| Native dylib (libloading / abi_stable) | 0 | 강 (load-time 검사) | **불가 (dlclose + glibc TLS, abi_stable 자체 명시)** | 없음 | 거절 |
| eBPF (rbpf JIT) | 0 | 없음 (raw byte buffers) | 됨 (program swap) | 강 (verifier) | 거절 |
| Embedded scripting (Luau/Rhai/Rune) | 0 | 약 (dynamic typed) | 됨 | 다양 | 거절 |

근거 디테일:
- 네이티브 dylib는 `abi_stable` 문서가 "without support for unloading"을 공식 선언, glibc `__cxa_thread_atexit_impl`이 thread-local 사용한 라이브러리의 `dlclose`를 차단. 무중단 교체와 호환 불가.
- WIT Component Model은 G3를 가장 깔끔하게 만족하지만 G4의 list/string zero-copy를 현재 표준에서 보장 못 함. WASI 0.3.x lazy lowering 로드맵이 해결할 가능성 있으나 2026년 현재 production 안정성과 sync path overhead가 검증되지 않음.
- eBPF/rbpf JIT는 raw byte buffer를 다루는 모델이라 G3(컴파일타임 타입 안전 시그니처)와 정면 충돌. wasm Rust struct + macro 같은 단언 메커니즘이 없고, plugin 작성 ergonomics도 매우 낮음. plugin 작성자에게 eBPF instruction set/verifier 제약을 강제하는 것은 비현실적.
- Scripting은 G3 불가 (단언 가능한 시그니처 부재).
- Raw Wasmtime core ABI는 모든 G1~G6를 만족시키는 유일한 후보. 단점은 PDK proc-macro를 자체 작성해야 하는 것 (§5.2).

**성능 수치 주의**: 본 문서가 인용하는 "sub-ms instantiation", "TypedFunc trampoline ~20-100ns", "rkyv vs JSON 30-100x" 등은 외부 자료 기반 **기대치/지표**이며 cc-lb 실제 payload 환경에서의 보장이 아니다. 최종 채택 확정은 §8의 cc-lb-payload-specific benchmark가 결정한다.

### 4.1 한 번 검토 후 거절된 점진 개선안 (의사결정 이력)

본 vNext rewrite 결정 전에 다음 점진 개선안이 한 차례 제시되었고, m0049에서 사용자가 명시적으로 거절했다. 거절 사유와 함께 기록.

| 점진안 단계 | 내용 | 거절 사유 |
|---|---|---|
| Stage 1 | `FilterPlugin`, `UpstreamDialect`에 default async method 추가. `ExtismFilterPlugin`/`ExtismDialectPlugin`만 override해서 기존 `dispatch_*_async`를 `.await`. 새 OS thread + Tokio runtime build 제거. | thread hop 0이 아님. `spawn_blocking`+blocking pool 통한 thread hop 1단계는 여전히 남음. |
| Stage 2 | `spawn_blocking` 앞에 per-plugin async semaphore 도입. 동시 호출을 mutex 대기 대신 async queue 대기로 전환. | mutex가 async queue로 바뀔 뿐 thread hop 자체는 그대로. 본질적 비용 미해결. |
| Stage 3 | 측정 후 Extism `Pool` 또는 Wasmtime ABI vNext 결정. | "측정 후 결정" 방식은 product-critical hot path 요구와 양립 불가. 사용자는 "최적화 안 되면 제품 자체가 의미 없어"라고 명시. |

채택된 vNext rewrite는 Stage 1+2+3을 모두 우회하여 thread hop을 0으로 떨어뜨린다. 사용자의 "rewrite여도 상관없고", "extism 안 써도 되고", "wasm 자체를 안 써도 돼"가 이 결정을 가능하게 했다.

## 5. 채택안 설계

### 5.1 실행 모델 — per-worker thread-local instance

**Worker 정의 및 수**:
- "Worker"는 Tokio 멀티 스레드 runtime의 **core worker pool** 안에 있는 OS thread를 가리킨다. async task를 poll하는 고정 크기 풀.
- 별개로 Tokio는 `spawn_blocking`용 **blocking pool**을 따로 갖는다. 본 설계는 blocking pool을 일절 사용하지 않는다.
- core worker 수는 Tokio runtime 설정값:
  - 기본값 = `std::thread::available_parallelism()` (보통 CPU 코어 수, 컨테이너면 cpuset/quota 반영).
  - 명시: `tokio::runtime::Builder::new_multi_thread().worker_threads(N).build()`.
- cc-lb-server는 현재 `#[tokio::main]` 또는 명시적 `Builder`로 시작 — 변경 없음. 운영자는 `worker_threads`를 명시해 capacity planning에 활용 가능 (R2, R3 참조).

```rust
// per-worker thread-local cache
thread_local! {
    static SLOT_INSTANCES: RefCell<HashMap<SlotKey, WorkerInstance>> = RefCell::new(HashMap::new());
}

struct WorkerInstance {
    version_id: u64,
    store: wasmtime::Store<HostState>,
    typed_filter: Option<TypedFunc<(u32, u32), u64>>,
    typed_shape:  Option<TypedFunc<(u32, u32), u64>>,
    // ...
}
```

호출 흐름:

```text
Lifecycle::handle (Tokio core worker thread)
  ├─ view = dynamic_view.load()             // ArcSwap, lock-free
  ├─ spec  = view.principal_view.get(pid)
  ├─ pipe  = spec.resolved_pipeline()
  └─ for filter in pipe.user_filters:
       ├─ slot   = filter.slot_key()         // (principal, plugin_name)
       ├─ cell   = filter.cell.load()        // Arc<PluginCell>, version_id, InstancePre
       ├─ SLOT_INSTANCES.with(|m| {
       │     let wi = m.borrow_mut().entry(slot).or_insert_with(|| ...);
       │     if wi.version_id != cell.version_id {
       │         // 재인스턴스화. PoolingAllocator + memory_init_cow로 sub-ms
       │         wi.store = Store::new(&engine, host_state.clone());
       │         let instance = cell.instance_pre.instantiate(&mut wi.store)?;
       │         wi.typed_filter = Some(instance.get_typed_func(&mut wi.store, "filter")?);
       │         wi.version_id = cell.version_id;
       │     }
       │     // 호출. 같은 OS thread, 같은 worker, sync. blocking pool 미사용.
       │     wi.typed_filter.unwrap().call(&mut wi.store, (in_ptr, in_len))
       │  })
       └─ rkyv::access::<ArchivedFilterResponse>(&out_bytes)
```

핵심 속성:
- **thread hop 0**: 핫패스에 `std::thread::spawn`, `Runtime::Builder::build`, `spawn_blocking` 없음.
- **mutex contention 0**: thread_local이라 plugin slot당 worker 수만큼 독립 인스턴스. 같은 plugin 동시 호출 시에도 워커끼리는 자연 병렬.
- **인스턴스 재사용**: 같은 worker가 같은 version의 같은 slot을 다시 호출하면 store와 typed_func 재사용. 호출 비용은 `TypedFunc::call`의 trampoline 진입 — Wasmtime 문서 기준 수십~수백 ns 수준으로 기대(실측 §8에서 검증).
- **version 변경 시 비용**: `InstancePre.instantiate(&mut new_store)`. Wasmtime PoolingAllocator + `Config::memory_init_cow(true)` 적용 시 Wasmtime fast-instantiation 문서가 sub-ms를 가능치로 제시 — cc-lb 환경에서의 실측은 §8 벤치로 확정.

### 5.2 PDK — `cc-lb-pdk-wasmtime`

Plugin 작성자가 보는 코드는 평범한 Rust 함수. 핵심: rkyv의 `Archived<T>`는 `T`와 메모리 레이아웃이 다르므로(`Archived<String>`은 offset+len, `Archived<Vec<T>>`는 relative pointer+len) `transmute`로 `&T`를 만들면 unsound. PDK는 안전한 `*Ref<'_>` view 타입을 제공한다.

```rust
use cc_lb_pdk::prelude::*;
use cc_lb_plugin_types::v4::{FilterRequestRef, FilterOutput, FilterError, UpstreamId};

#[cc_lb_pdk::plugin(filter)]
pub fn my_filter(req: FilterRequestRef<'_>) -> Result<FilterOutput, FilterError> {
    // FilterRequestRef는 macro 생성. 내부에 &ArchivedFilterRequest를 보유.
    // 모든 접근자가 archived 표현을 정상 Rust 타입으로 변환:
    //   req.method()    -> &str
    //   req.path()      -> &str
    //   req.headers()   -> impl Iterator<Item = (&str, &[u8])>
    //   req.body()      -> &[u8]
    //   req.principal() -> PrincipalRef<'_>
    //   req.candidates()-> impl ExactSizeIterator<Item = UpstreamCandidateRef<'_>>

    let kept: Vec<UpstreamId> = req
        .candidates()
        .map(|c| c.upstream_id())
        .collect();

    Ok(FilterOutput {
        kept_upstream_ids: kept,
        reason: "pass-through".into(),
        per_candidate_reasons: vec![],
    })
}
```

proc-macro `#[cc_lb_pdk::plugin(filter)]`가 자동 생성하는 것:

1. **컴파일타임 시그니처 단언** — 사용자 함수의 시그니처가 macro variant(filter, shape, observe, sign 등)별 정의와 정확히 일치하는지 확인. 어긋나면 빌드 실패.

   ```rust
   const _: () = {
       fn _assert<F>(_: F)
       where F: for<'a> Fn(FilterRequestRef<'a>) -> Result<FilterOutput, FilterError> {}
       _assert(my_filter);
   };
   ```

2. **게스트 export 함수** — `*Ref<'_>` view 타입을 통해 안전하게 archived bytes에 접근:

   ```rust
   #[no_mangle]
   pub extern "C" fn cc_lb_filter(in_ptr: u32, in_len: u32) -> u64 {
       let bytes = unsafe { core::slice::from_raw_parts(in_ptr as *const u8, in_len as usize) };
       // rkyv access는 bytecheck로 linear 검증 후 &ArchivedFilterRequest 반환
       let archived: &ArchivedFilterRequest =
           match rkyv::access::<ArchivedFilterRequest, rkyv::rancor::Error>(bytes) {
               Ok(a) => a,
               Err(_) => return pack_err(ErrorCode::InvalidRequest),
           };
       // FilterRequestRef는 archived 위에 얇은 wrapper. transmute 없음.
       let req_ref = FilterRequestRef::from_archived(archived);
       match my_filter(req_ref) {
           Ok(out) => emit_output(out),
           Err(e)  => pack_err_user(e),
       }
   }
   ```

   `*Ref<'_>` 타입은 `cc-lb-plugin-types`에서 macro로 자동 도출(`#[derive(ArchiveRef)]` 같은 보조 derive). `transmute` 사용 없음.

3. **schema_hash custom section** — wasm 모듈에 `cc_lb_schema_hash` custom section을 임베드. 해시 입력은 `(wire 버전 + 함수 variant 이름 + rkyv archive 레이아웃 fingerprint)`. 동일 cc-lb-plugin-types 버전으로 빌드된 호스트와 게스트만 같은 해시. mismatch는 plugin 등록 시점에 거부.

4. **alloc/free 헬퍼 export (guest → host로 노출되는 wasm export, host import 아님)** — `cc_lb_alloc(len: u32) -> u32`, `cc_lb_free(ptr: u32, len: u32)`. 호스트가 input buffer를 게스트 linear memory에 기록하기 전에 게스트 함수를 호출해 영역 확보 → `Memory::write` → typed_func 호출. 출력은 게스트가 자기 메모리에 두고 packed `(out_ptr, out_len)`을 반환, 호스트가 archived view 처리 종료 직후 `cc_lb_free(out_ptr, out_len)` 호출.

호스트 사이드는 같은 Rust 타입 crate(`cc-lb-plugin-types`)를 공유하므로 `TypedFunc<(u32, u32), u64>`와 alloc/free TypedFunc만 알면 끝. **host 사이드 `Linker::func_wrap`는 핫패스 plugin에서는 단 하나도 등록하지 않음** (sync 핫패스에서 host call 금지의 기계적 강제, §5.6).

검증 3계층:
- L1 (게스트 컴파일타임): proc-macro `_assert` const block.
- L2 (호스트 컴파일타임): `TypedFunc<Params, Results>` Rust 제네릭이 코어 ABI 타입 강제.
- L3 (로드타임): `cc_lb_runtime_wasmtime::register_slot`가 wasm 모듈의 `cc_lb_schema_hash` custom section을 expected hash와 비교. mismatch면 등록 거부, `StagedSlot` 미생성, 기존 slot 무영향.

**rkyv 안전 불변조건 (PDK와 runtime이 모두 준수해야 함)**:
- `access_unchecked`/`from_bytes_unchecked` 사용 금지. 항상 `rkyv::access::<T, rancor::Error>(bytes)`를 호출해 bytecheck로 매 호출 검증.
- `cc_lb_alloc`은 archive root 타입의 최대 align(`align_of::<ArchivedFilterRequest>()`)을 만족하는 alignment로 메모리를 반환. PDK는 `alloc_aligned(len, align)` 형태로 export하거나, root 타입의 align을 고정한 wrapper allocator를 macro가 생성.
- 게스트가 반환하는 `(out_ptr, out_len)`도 같은 alignment 조건을 만족해야 함.
- 호스트가 `rkyv::access`로 얻은 `&Archived*` view는 그 view의 lifetime 동안 게스트 `cc_lb_free`를 호출하지 않는다. 처리 순서: `access → 필요한 값 owned로 복사/decode → cc_lb_free(out_ptr, out_len)`.
- 게스트 archived view를 호스트가 들고 있는 동안 같은 worker의 같은 인스턴스로 **재진입(=같은 Store에 새 typed_func.call)** 금지. 새 호출은 linear memory를 변형시켜 archived view를 invalidate함. PDK 매크로/host helper가 view scope를 닫지 않으면 재호출 못 하도록 borrow lifetime으로 강제.

### 5.3 Wire 포맷 — rkyv zero-copy

선택 근거: bg_e1173fc3 비교에서 JSON+base64 대비 ~30-100x 빠르고 (게스트 0 allocation), Rust-to-Rust 한정이면 FlatBuffers의 builder 복잡성도 회피. 다언어 plugin 필요 시 wire를 FlatBuffers로 swap 가능(macro의 `wire = "..."` 파라미터).

대표 타입:
```rust
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct FilterRequest {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, Vec<u8>)>,   // raw bytes, no base64
    pub body_bytes: Vec<u8>,               // raw bytes
    pub principal: Principal,
    pub candidates: Vec<UpstreamCandidate>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct FilterResponse {
    pub results: Vec<PerCandidateReason>,
}
```

호스트 보내기:
```rust
let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&req)?;            // ~1 alloc on host
let in_ptr = call_guest_alloc(&mut store, &alloc_fn, bytes.len())?;  // guest alloc
mem.write(&mut store, in_ptr as usize, &bytes)?;                      // single memcpy
let packed = typed_filter.call(&mut store, (in_ptr, bytes.len() as u32))?;
let (out_ptr, out_len) = unpack(packed);
let out_slice = &mem.data(&store)[out_ptr as usize .. out_ptr as usize + out_len as usize];
let archived  = rkyv::access::<ArchivedFilterResponse, _>(out_slice)?;  // zero-copy, bytecheck
```

게스트 받기는 §5.2 그대로 — `rkyv::access`로 zero-allocation view.

검증: rkyv `check_bytes` (=`bytecheck`)가 linear pass로 archive 구조 정합성, 정렬, enum discriminant를 모두 검증. 호스트는 `access_unchecked`를 절대 호출하지 않는다.

### 5.4 Hot-swap — 기존 primitive 보존

데이터 구조 변경:
```rust
// 새 PluginCell
pub struct PluginCell {
    pub version_id:    u64,                       // 단조 증가, 워커 invalidation 트리거
    pub instance_pre:  Arc<InstancePre<HostState>>, // precompiled + pre-linked
    pub schema_hash:   [u8; 32],
    pub fuel_per_call: u64,
    pub memory_max_pages: u32,
}

pub struct PluginSlot {
    pub name:    String,
    pub entry:   RwLock<PluginEntry>,             // manifest, registry metadata
    pub current: ArcSwap<PluginCell>,             // 그대로
}
```

교체 흐름:
```text
Reconciler 또는 admin API
  ├─ wasm bytes를 disk cache에서 read (data/plugins/wasm/cache/{sha}.wasm)
  ├─ Engine::precompile_module(&engine, &wasm_bytes) → 직렬화된 .cwasm
  ├─ Module::deserialize_file(&engine, ...) (unsafe, 자체 빌드 산출물이라 안전)
  ├─ Linker.instantiate_pre(&module) → InstancePre
  ├─ schema_hash custom section 추출, expected와 비교 → 실패 시 abort
  ├─ self-check 호출 (1회) → 실패 시 abort
  ├─ 새 PluginCell { version_id: prev+1, instance_pre: Arc::new(pre), ... }
  ├─ StagedSlot에 적재
  └─ commit_staged() → 모든 slot이 성공한 경우에만 `slot.current.store(Arc::new(cell))`
```

워커는 다음 요청 때 `cell.version_id`가 자기 `WorkerInstance.version_id`와 다른 것을 발견 → `InstancePre.instantiate(&mut new_store)` → typed_func 재추출. 이전 `InstancePre`의 `Arc`는 그 워커가 마지막으로 들고 있던 참조를 놓으면 자연 drop. dlclose 같은 race 없음.

**관측성 flush 책임 (PluginCell이 소유 아님)**:
`PluginCell`은 `InstancePre`만 들고 있고 per-worker `Store`나 ring buffer를 소유하지 않는다. 따라서 ArcSwap 교체 시 old `PluginCell::drop`만으로는 워커 thread-local에 남은 buffered observability event를 flush할 수 없다(설계 오류였음 — Oracle 검토에서 지적).

올바른 flush 모델:
- 관측성 buffer는 **`WasmtimeObservabilityHook` 본체**(`Arc<dyn ObservabilityHook>`로 `DynamicView`가 보유) 또는 **per-worker registry**가 소유한다.
- Slot eviction/version 교체 시 drain 순서:
  1. `commit_staged()` 직전에 새 `version_id`를 기록.
  2. 별도 background flush task가 모든 worker thread를 한 바퀴 돌며 `worker_drain_for_slot(slot_key, prev_version_id)`를 invoke (per-worker channel 또는 atomic flag로 cooperative 요청).
  3. 각 워커가 다음 await point에서 자기 thread-local 안의 prev_version `Store`를 drain → emit → drop.
  4. drain 완료 ack를 모두 받고 나서 old `PluginCell` Arc를 마지막 참조에서 떨어지게 함.
- Slot 완전 제거 시(`evict_slot`): 위와 동일하되 새 instantiate 없이 thread-local entry 삭제.
- 이 모델 없이 단순 `Drop`만 깔면 SSE batched events가 누락될 수 있음.

### 5.5 자원 한계와 격리 — fuel

- **메모리**: per-store `Memory` 페이지 한계 + `Config::max_wasm_stack`. 한도 초과 시 wasm trap.
- **명령 수**: per-store `Store::set_fuel(N)`. 매 호출 시작 시 호스트가 `set_fuel(cell.fuel_per_call)`. 소진 시 `Trap::OutOfFuel`. 같은 wasm + 같은 input + 같은 budget이면 항상 같은 명령에서 trap. **instruction-deterministic.**
- **wall-clock kill 미보장 (의도적)**: fuel은 명령 수 결정론이지 시간 결정론이 아니다. 같은 1억 명령이라도 CPU 부하/캐시 상태에 따라 시간 차이가 발생. "정확히 N ms 후 무조건 죽인다"는 보장은 다음 중 하나가 필요한데 셋 다 채택하지 않는다:
  1. Wasmtime epoch interrupt — 외부 timer가 `engine.increment_epoch()`. wasm은 함수 entry와 loop backedge에서만 epoch 체크. plugin engine 공유 시 한 plugin timeout이 다른 plugin도 trap시키는 알려진 현상 때문에 plugin마다 engine 분리 필요. 추가 복잡도.
  2. OS signal — wasm trap 메커니즘 우회. 안전 보장 없음.
  3. 다른 스레드 + JoinHandle drop — 우리가 없앤 thread hop을 다시 들이는 것.
- **결정**: fuel 결정론으로 충분. plugin이 fuel을 소진하면 그 호출은 실패하고 `FilterError::Trap`이 반환되며, `execute_filter_pipeline`의 기존 trap 분기(현재도 plugin trap을 passthrough로 다룸)가 처리. 사용자 정책: "plugin 자기 책임".

### 5.6 Host functions 정책 및 게스트 내부 async

**Wasm 게스트 내부 async 코드**:
- 게스트 Rust에서 `async fn`을 작성하는 것 자체는 가능. 그러나 그건 게스트 안에서 state machine으로 컴파일될 뿐이고, **호출이 호스트로 yield되지 않는다**. 호스트 관점에서 `cc_lb_filter` export 호출은 return 또는 trap까지 단일 동기 호출.
- 진짜 async I/O를 게스트에서 하려면 (1) host async function을 import하고 (2) wasmtime async support로 fiber stack switching을 켜야 함. 본 설계는 핫패스에서 이를 금지(N3).
- 따라서 plugin 작성자가 게스트 안에서 `async`/`await`를 써도 무방하지만, 효과는 평범한 동기 함수 호출과 같다. CPU만 쓰는 짧은 로직이라는 전제가 유지.

**Host function 호출 정책 (load-time 기계적 강제)**:
- 핫패스 plugin (filter, shape, normalize_error)에서 **host function 호출 금지**. 모든 입력은 호출 시 archive에 담겨 들어오고, 출력은 archive로 돌아간다. 이유: host call이 들어가면 단순 sync 핫패스가 끝나고 worker 점유 시간이 예측 불가능해짐.
- enforce 메커니즘 (convention 아니라 register-time validation):
  1. 핫패스 engine의 `Linker`는 빈 상태로 생성. `func_wrap` 등록 0건.
  2. `Config::wasm_component_model(false)`, `WasiCtx` 미장착, `wasmtime-wasi` Linker extension 미적용.
  3. `Module::imports()`를 register 시점에 inspect — import set이 비어 있지 않으면 `RuntimeError::InstantiateFailed`로 등록 거부.
  4. `Module::exports()`에서 `cc_lb_filter`/`cc_lb_shape`/... 중 manifest가 선언한 hook과 `cc_lb_alloc`/`cc_lb_free`만 허용. 추가 export는 무해하지만 warn-log.
  5. signer/observability용 engine은 별도(§5.7)이므로 별도 Linker에 그쪽 host fn만 등록. 핫패스 engine과 module/InstancePre 공유 금지.
- 결과: 어떤 plugin도 host state, network, filesystem, clock에 접근할 방법이 없음. 시도하면 instantiate 거부.
- observability hook(observe)은 background flush 모델로 분리. plugin은 in-memory ring buffer에 event push만 하고, host가 별도 task에서 주기적으로 drain. 현 `sse_batch.rs` 의도와 동일하나 wire가 rkyv.
- signer는 핫패스가 아니므로 기존처럼 async + host I/O 허용. Wasmtime의 async support는 signer 전용 별도 engine(§5.7)에서만 활성화.

### 5.7 Engine 구성

- **핫패스 engine** (sync): `Config::async_support(false)`, `Config::strategy(Strategy::Cranelift)`, `Config::signals_based_traps(true)`, `Config::memory_reservation(1<<32)`, `Config::memory_guard_size(1<<32)`, `Config::memory_init_cow(true)`, `InstanceAllocationStrategy::Pooling(PoolingAllocationConfig { total_memories: ..., max_memory_size: 32MiB, ... })`. fuel `Config::consume_fuel(true)`. 이 engine을 모든 worker가 공유. 각 worker는 자기 Store를 소유.
- **signer engine** (async): `Config::async_support(true)` + epoch interrupt. 핫패스 engine과 분리해서 epoch 간섭 방지.
- **engine-specific module/InstancePre**: Wasmtime의 `Module`과 `InstancePre`는 자신을 만든 `Engine`에만 종속. 핫패스용과 signer용은 같은 wasm 바이트를 사용하더라도 **각자 별도로 `precompile_module → deserialize → InstancePre`** 해야 한다. `WasmtimeRuntime`은 두 engine을 함께 들고 각각의 `Linker`와 InstancePre 캐시를 분리해 보유.

**PoolingAllocator capacity 산식 및 실패 정책**:

동시 활성 인스턴스 수 상한:
```
max_in_flight ≈ Σ_(slot s) ( worker_threads × live_versions(s) )
                + signer engine in-flight (async, 별도 cap)
                + 일시적 잔여 (commit 후 워커가 아직 invalidate 못한 old version)
```

`PoolingAllocationConfig`의 `total_memories`, `total_core_instances`, `total_stacks`(async일 때만)는 이 값보다 충분히 커야 함. cc-lb-server는 boot 시 `worker_threads × (active_slots × 2 + headroom)`을 잡고, 그 이상 증가하면 admin warn-log + Prometheus metric `cc_lb_plugin_pool_saturation_ratio`.

cap 도달 시 동작:
- 새 plugin 등록(`stage_slot`)에서 instantiate 실패 → `StagedSlot` 미생성 → `commit_staged` 전체 abort → **기존 slot 전부 무영향**(기존 cell이 그대로 active 유지). admin API는 `RuntimeError::InstantiateFailed { reason: "pool saturated, increase total_memories or evict unused slots" }` 반환.
- 핫패스 요청 중 thread-local 재인스턴스화 실패 → 해당 요청만 `FilterError::Runtime` 반환, `execute_filter_pipeline`의 trap 분기로 fallthrough (passthrough policy 또는 fail 정책에 따름). 다른 요청에 영향 없음.

## 6. 컴포넌트별 변경 사항

### 6.1 새 crate `cc-lb-runtime-wasmtime`

위치: `crates/cc-lb-runtime-wasmtime/`
역할: `cc-lb-plugin-api::PluginRuntime`을 wasmtime 기반으로 구현.

주요 타입:
```text
WasmtimeRuntime { hot_engine: Engine, signer_engine: Engine, slots: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>, host_state: Arc<HostState> }
PluginSlot      { name, entry: RwLock<PluginEntry>, current: ArcSwap<PluginCell> }
PluginCell      { version_id, instance_pre, schema_hash, fuel_per_call, memory_max_pages }
StagedSlot      { key, entry, slot }
WasmtimeFilterPlugin   impl FilterPlugin
WasmtimeDialectPlugin  impl UpstreamDialect
WasmtimeSignerFactory  impl SignerFactory  (async, 별도 engine)
WasmtimeObservabilityHook impl ObservabilityHook
```

API:
- `register_slot(manifest) -> Arc<PluginSlot>`
- `stage_slot(principal_id, plugin_name, manifest, hook) -> (Arc<dyn ...>, StagedSlot)`
- `commit_staged(Vec<StagedSlot>)`
- `evict_slot(principal_id, plugin_name)`
- `reload(manifest_ref)`

기존 `ExtismRuntime` API와 메서드 명/시그니처 동일하게 맞춰 `dynamic_view_builder.rs`가 거의 변경 없이 동작.

### 6.2 새 crate `cc-lb-pdk-wasmtime`

위치: `crates/cc-lb-pdk-wasmtime/`
구성:
- `cc-lb-pdk-wasmtime-macros` — `#[cc_lb_pdk::plugin(...)]` proc-macro.
- `cc-lb-pdk-wasmtime` — runtime 헬퍼 (`cc_lb_alloc`, archive view 추상화, error helpers).
- `cc-lb-plugin-types` (이미 있음에 가까운 `cc-lb-plugin-api/types.rs` 재정렬) — host/guest 공유 타입 + rkyv derive.

### 6.3 변경되는 cc-lb 코드

- `cc-lb-plugin-api/src/traits.rs`: trait 시그니처 그대로. 단, `FilterPlugin`에 `slot_key(&self) -> SlotKey` 추가하여 thread_local 캐시 키 제공.
- `cc-lb-engine/src/lifecycle.rs::execute_filter_pipeline`: 호출 코드는 동일. plugin이 trap을 돌려주면 기존 분기로 처리.
- `cc-lb-engine/src/api_keys/principal_view.rs::RouterPipelineCache`: 그대로.
- `cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains`: `runtime.instantiate_filter_for/...`를 `WasmtimeRuntime`로 호출 (trait 동일).
- `cc-lb-server/src/reconcile.rs`: 변경 없음.
- `cc-lb-server/src/reload.rs::ConfigWatcher`: 변경 없음.
- `cc-lb-server/src/tls.rs`: 변경 없음.
- `Cargo.toml`: `wasmtime = "47"` 직접 의존 추가. `extism = ...` 제거(transition 종료 후).

### 6.4 동봉 plugin 재빌드

- `plugins/router/cache-aware` → 새 PDK, 로직 그대로.
- `plugins/shape/subscription-launderer` → 새 PDK, 로직 그대로.
- `plugins/observe/*` → 새 PDK.

각 plugin의 `Cargo.toml`에서 `extism-pdk` 의존을 `cc-lb-pdk-wasmtime`으로 교체, `#[plugin_fn]`을 `#[cc_lb_pdk::plugin(filter)]` 등으로 교체.

## 7. 마이그레이션 단계

각 Phase의 구현 난이도는 m0046에서 사용자가 요청한 추정치다. 척도: 1(단순) ~ 10(rewrite급).

1. **Phase 0 — 새 crate 골격** (난이도 **2/10**)
   - `cc-lb-runtime-wasmtime`, `cc-lb-pdk-wasmtime`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-plugin-types` 생성. workspace feature `runtime-wasmtime`로 gate. wasmtime 47 의존.
   - 빈 `WasmtimeRuntime`가 `PluginRuntime` trait를 구현. 모든 메서드는 unimplemented panic.
   - 기존 trait 시그니처 그대로 묶기만 하면 됨. 빌드 통과만 확인.

2. **Phase 1 — filter 단일 hook 통합** (난이도 **6/10**)
   - PDK macro의 filter variant 구현 + rkyv wire 타입 + `*Ref<'_>` view 자동 생성 macro.
   - 핫패스 engine 구성(`Config::async_support(false)` + PoolingAllocator + CoW + fuel) + per-worker thread_local instance 캐시.
   - schema_hash custom section read/verify + load-time imports inspect.
   - 동봉 `cache-aware` plugin을 새 PDK로 재빌드.
   - 새 conformance test (`tests/plugin-lifecycle/wasmtime_filter.rs`)에서 filter 호출, hot-swap, version_id 증가, fuel trip을 검증.
   - 가장 까다로운 부분은 rkyv view macro 설계와 alignment-aware alloc 헬퍼.

3. **Phase 2 — shape, observability, signer 통합** (난이도 **5/10**)
   - shape, normalize_error: filter와 같은 패턴 반복.
   - observe: §5.4 background drain task + per-worker ring buffer 신규 구현.
   - signer: async engine(`Config::async_support(true)`) + epoch interrupt + 별도 Linker + 별도 InstancePre 캐시.

4. **Phase 3 — 벤치 + flip** (난이도 **3/10**)
   - `benches/extism_sse_overhead`를 새 런타임 변형으로 실행 (`runtime-wasmtime` feature).
   - non-streaming p50 overhead, p99 overhead, streaming per-event p50/p99 모두 현재 baseline 대비 개선 또는 동등 확인.
   - default runtime을 wasmtime으로 flip.
   - 코드 변경 자체는 적음, 측정/조정이 시간 들 수 있음.

5. **Phase 4 — Extism 제거** (난이도 **2/10**)
   - `cc-lb-runtime-extism`, `cc-lb-runtime-protocol`, `cc-lb-plugin-wire/v1,v2,v3`, `cc-lb-pdk(=extism)`, JSON/base64 코드 경로 모두 삭제.
   - workspace에서 `extism`, `extism-pdk` 의존 제거.
   - 문서 (`docs/plugin-author-guide.md`, `docs/runtime-management.md`) 갱신.

**전체 난이도 비교 (m0046 답변 보존)**:
- 현 Extism 경로에서 per-call thread/runtime만 제거: 3-4/10 (Phase 1 일부와 유사). 그러나 Mutex/JSON/base64 비용은 남음 → §4.1 거절된 점진안.
- 본 vNext (위 Phase 0~4 합계): 7/10 — raw Wasmtime + 자체 ABI vNext 설계가 가장 무거움. Extism PDK 호환 유지 비용은 0(N1로 회피).
- Extism 호환을 유지하면서 Wasmtime로 갈아타기(Extism ABI 재구현): 8-9/10. **선택 안 함.**

## 8. 검증 계획

- **Conformance**: 기존 `crates/cc-lb-plugin-conformance` 시나리오를 새 PDK + runtime으로 옮긴다. filter happy path, trap, fuel trip, invalid archive, schema_hash mismatch, hot-swap 중 in-flight 요청 일관성.
- **Benchmark**: `benches/extism_sse_overhead` 그대로 실행. `tests/load/baseline.json`의 threshold (non-streaming p50 overhead <5ms, p99 <20ms, streaming p50 per event <2ms) 만족 확인.
- **Property test**: `tests/property/`의 lifecycle property test에 wasmtime variant 추가.
- **Loom**: `tests/loom`의 dynamic_view race property test에 새 PluginCell 교체 race 케이스 추가.
- **수동 부하**: `tests/load` 시나리오로 단일 노드 1k RPS에서 worker thread 점유율, 메모리 풋프린트, p99 latency 측정.

## 9. 미해결 위험과 결정 필요 사항

- **R1 worker 점유**: 한 plugin 호출이 그 worker를 호출 시간 동안 잡는다. fuel로 명령 상한을 강제하더라도 매우 무거운 plugin이 worker 하나를 오래 잡으면 그 worker가 큐에 쌓인 다른 task를 못 처리. SLO는 fuel_per_call로 강제. 권장 초기값: filter 50M, shape 100M (Wasmtime 일반 throughput 가정 시 각 sub-ms~수 ms).
- **R2 메모리 풋프린트 + thread-local eviction**: `worker_threads × active_plugin_slots × memory_max_pages`. 8 worker × 20 slot × 32 page (2 MiB) = ~320 MiB 상한. PoolingAllocator + CoW면 actual RSS 훨씬 작음.
   - 그러나 slot이 evict되거나(`evict_slot`) version이 교체되더라도 worker thread-local `SLOT_INSTANCES` map은 그대로 남는다 → old Store가 leak.
   - 정책: (a) 매 호출 시 `cell.version_id`와 비교해 stale entry는 새 entry로 덮어씀 (현 §5.1 흐름). (b) slot eviction은 §5.4의 background drain task가 모든 worker에 `worker_drain_for_slot(slot_key)` 신호 → 워커가 다음 await point에서 thread-local entry 제거. (c) 주기적 LRU sweep을 cc-lb-server background task로 추가하여 N분간 미사용 entry를 GC.

- **R2b principal 제거**: principal 삭제 시 그 principal의 모든 SlotKey가 다음 Reconciler rebuild에서 사라짐. 워커 thread-local의 해당 SlotKey entry도 (a)/(b)/(c) 정책으로 정리.
- **R3 Wasmtime engine 공유 범위**: 핫패스 engine 1개를 모든 worker가 공유. PoolingAllocator의 `total_memories` 등이 동시 활성 instance 수의 hard cap. 운영 중 cap 초과 시 instantiate 실패. 운영 정책 결정 필요 (slot 수 증가 시 engine 재구성 vs cap 넉넉히 잡기).
- **R4 schema 진화**: 새 필드 추가 시 schema_hash 변경 → 모든 plugin 재빌드 필요. Backward-compat 원치 않는 단계이므로 OK이나, 미래에 wire version 협상이 필요해지면 macro에 `wire_version` 명시 + 호스트가 multi-version dispatch를 가질 수 있어야 함.
- **R5 wasmtime 47 vs Extism 1.30 (wasmtime 43)**: 동시 의존 시기에 두 wasmtime이 공존. cargo가 두 major를 동시에 들고 가도 빌드는 됨(rust orphan rule 충돌 없음). Phase 4에서 Extism 제거하며 정리.
- **R6 PDK view 타입 정합성**: `*Ref<'_>` view 타입과 `Archived*` 타입의 매핑이 깨지면 잘못된 메모리를 읽을 수 있다. `cc-lb-plugin-types`에서 두 타입을 같은 macro로 동시 생성해 단일 소스로 유지. cross-cargo-version 검증은 §5.2의 schema_hash custom section이 담당. 호스트는 `rkyv::access`(unchecked가 아님)만 호출해 bytecheck로 archive 구조 정합성을 매 호출마다 검증.
- **R7 signer engine 분리**: async engine과 sync engine 두 개를 같은 프로세스에서 운용. 자원 책정/메모리 누수 모니터링 분리 필요.

## 10. 참고

- 합의 audit: `.omo/ultraresearch/20260627-plugin-hotpath-vnext/SYNTHESIS.md`
- 외부:
  - Wasmtime 47 Tuning for Fast Instantiation, Fast Execution, Pre-Compiling Wasm.
  - rkyv 0.8 `Archive`, `access`, `bytecheck`.
  - `abi_stable` 0.11 "without support for unloading" 명시.
  - glibc `__cxa_thread_atexit_impl`와 `dlclose` 차단 동작.
- 내부:
  - `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` (현행 dispatch 비용).
  - `crates/cc-lb-runtime-extism/src/lib.rs` (현행 ArcSwap + stage/commit).
  - `crates/cc-lb-engine/src/dynamic_view.rs` (DynamicViewHolder).
  - `crates/cc-lb-engine/src/api_keys/principal_view.rs` (PrincipalSpecCached).
  - `crates/cc-lb-server/src/dynamic_view_builder.rs` (build_principal_chains).
  - `benches/extism_sse_overhead/src/main.rs` (1ms threshold).
  - `tests/load/baseline.json` (성능 baseline).
