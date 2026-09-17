# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, a wasmtime + rkyv plugin runtime, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`. The implementation plan lives in [.omo/plans/anthropic-proxy.md](./.omo/plans/anthropic-proxy.md).

## Quick start

1. Set the admin bootstrap token: `export CC_LB_BOOTSTRAP_ADMIN_TOKEN=$(uuidgen)`
2. Optionally seed initial state via `bootstrap.toml` in your data_dir
3. Run `cc-lb-server serve --config cc-lb.toml`
4. Open the dashboard at `http://localhost:<admin_port>/`
5. Add upstreams, principals, and plugin chains via the dashboard

See [docs/runtime-management.md](docs/runtime-management.md) for the full API and architecture.

## Operator Guides

- [Upstream warm-up](./docs/upstream-warmup.md): keep Anthropic 5h windows ticking
- [Distributed Scheduler](./docs/scheduler.md): topology, retry classes, metrics, and runbook

### 0.4.9 호환 릴리스

이 브랜치는 PostgreSQL 운영 0.4.9에서 관리 키의 legacy kind 컬럼을 제거하기 전에 배포할 중간 버전이다. 일반 최신 릴리스나 운영 배포 승인을 의미하지 않는다.

- 기존 키의 인증·조회·상태 변경·폐기는 유지한다. 새 키 발급은 secret 생성이나 저장 전에 거절하며 Admin API는 `503`과 `key_issuance_paused`를 반환한다. 신규 키가 필요한 회전도 제한된다. 이 제한은 호환 버전을 사용하는 동안 지속되며 최종 버전으로 전환해야 해제된다.
- PostgreSQL startup은 migration 114까지만 실행한다. 123까지의 원본 manifest를 포함하되, 이미 적용된 후속 migration도 버전·체크섬을 검증한다. 알 수 없는 버전, 변경된 체크섬, 실패 이력은 startup 오류다. migration을 실행한 것처럼 기록하거나 검증을 건너뛰지 않는다.
- migration 연결은 pool 설정을 적용받은 뒤 pool에서 분리한다. 검증·실행 오류 후 잠금을 가진 연결이 pool에 반환되지 않도록 닫는다.
- SQLite의 startup migration 범위는 바꾸지 않았다. SQLite 키 어댑터의 컬럼 제거 전후 동작은 별도 fixture로 검증하며, PostgreSQL의 후속 migration 이력 허용 정책을 SQLite에 적용했다고 주장하지 않는다.

운영 전환 순서는 별도 승인을 전제로 한다.

1. 컬럼을 남긴 상태에서 모든 0.4.9 replica를 이 호환 버전으로 교체하고 실제 proxy 요청을 확인한다.
2. 구버전 replica가 더 이상 요청을 처리하지 않는 것을 확인한 뒤, 별도로 검토·승인한 스키마/인덱스 준비와 최종 버전 전환을 수행한다.
3. 컬럼 제거 후 롤백 대상은 원래 0.4.9가 아니라 검증된 호환 버전이다. 후속 manifest가 달라지거나 123을 넘으면 이 호환 버전을 그대로 재사용하지 않는다.

새 키 발급을 유지하려고 가짜 kind 기본값을 넣거나, `_sqlx_migrations`를 수동 수정하거나, 0117을 구버전 replica가 남아 있는 동안 실행하지 않는다. 인덱스의 쓰기 비용과 startup 잠금, 기타 pending migration의 부작용은 별도의 운영 전환 검토 대상이다.

### Stream diagnostics

HTTP 200 means response headers were sent, not that the response body completed. Use `cc_lb_stream_terminations_total{outcome,cause}` to distinguish `completed`, `upstream_error`, `proxy_error`, and `client_cancelled`. Causes are bounded categories such as `unexpected_eof`, `h2_cancel`, and `affinity_error`; request IDs and error messages are never metric labels.

With OTLP enabled, `proxy.response_stream` spans remain open until the response body ends or is dropped. Transport failures record a bounded, redacted `error.chain` and, when available, `error.io.kind`, `error.io.os_error`, and `error.h2.reason`. These describe the failed response path, not necessarily which party caused the disconnect.

Streaming `RequestFinished` hooks and the `stream latency breakdown` log run on a dedicated worker with a fixed 4,096-entry queue, so a slow completion callback cannot delay the final downstream body bytes. Per-chunk hooks remain inline. Queue overflow and closure increment `cc_lb_dropped_events_total` with bounded `stream_completion_observer_*` reasons; callback panic accounting and recovery apply to unwind builds, while the release profile aborts native Rust panics and production Wasm traps return errors instead. Mandatory lifecycle events, accounting, affinity decisions, and stream termination metrics remain inline. Graceful server shutdown prioritizes durable writer flushes, then waits up to the existing cleanup budget for remaining optional observations before telemetry teardown; a timeout increments `stream_completion_observer_shutdown_timeout` and leaves the worker detached.

## Plugin authors

Plugins are wasm modules authored with the published `cc-lb-pdk-wasmtime`; bundled guest plugins depend only on that PDK, which re-exports the guest-facing wire API from `cc-lb-plugin-wire`. The wire-only `cc-lb-runtime-wasmtime` runtime compiles each upload via wasmtime 46, validates imports, required plugin and hook metadata, per-hook wire versions, per-hook BLAKE3 layout fingerprints, and an upload-time runtime probe before dispatching calls. Local execution defaults to on-demand allocation with fresh per-call `Store`s, per-store `StoreLimits`, and a process-wide store budget; operators can opt into Wasmtime pooling through `[runtime.wasmtime] allocation_strategy = "pooling"`.

The slots a plugin may target:

- **filter**: return a `FilterResponse` deciding which upstream candidates to keep. The V1 filter request exposes the requested `service_tier`.
- **shape**: unified slot that transforms the incoming request into an upstream-bound `ShapedRequest`, and transforms downstream responses (both buffered and SSE). A shape plugin must implement request shaping, buffered response transform, and SSE event transform, with explicit no-op handlers for unneeded response hooks.
- **observe**: receive lifecycle events; side-effect only.

The published crates.io set is exactly 5 crates: `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-pdk-wasmtime`, `cc-lb-runtime-wasmtime`, and `cc-lb-plugin-conformance`. The six unpublished domain crates are `cc-lb-domain`, `cc-lb-upstream`, `cc-lb-routing`, `cc-lb-quota`, `cc-lb-request-log`, and `cc-lb-lifecycle`. Start with [docs/plugin-author-guide.md](./docs/plugin-author-guide.md), then use the crate READMEs for focused API notes: [`cc-lb-plugin-wire`](./crates/cc-lb-plugin-wire/README.md), [`cc-lb-pdk-wasmtime`](./crates/cc-lb-pdk-wasmtime/README.md), [`cc-lb-pdk-wasmtime-macros`](./crates/cc-lb-pdk-wasmtime-macros/README.md), and [`cc-lb-plugin-conformance`](./crates/cc-lb-plugin-conformance/README.md). Runtime design background is in the historical [RFC-0001](./docs/rfc/0001-plugin-runtime-vnext.md).

Upload flow: build the plugin to `wasm32-unknown-unknown`, then POST the artifact + `slot_kind=filter|shape|observe` to `POST /admin/v1/plugins/wasm`. The host runs `admit_wasm`, persists the SHA-256, plugin metadata name/version, plugin description, plugin usage, and per-hook metadata, then triggers a dynamic-view rebind. Re-uploading the same plugin name and SHA is a successful noop; uploading the same metadata name with a higher semantic version replaces the existing registry row in place; same/lower version replacements require an explicit confirmation retry. Registry reference counts include both plugin-chain bindings and upstream warmup dialect plugin bindings.
