- 2026-05-23T09:00:17.085203+00:00: updated fake-anthropic last-request header recording.
  - Changed files: tests/fixtures/fake-anthropic/src/routes.rs, tests/fixtures/fake-anthropic/tests/whitelist_headers.rs
  - Tests: cargo test -p fake-anthropic record_whitelist_headers -- --exact --nocapture; cargo test -p fake-anthropic; cargo clippy -p fake-anthropic --all-targets -- -D warnings; cargo fmt --all
  - Gotchas: /__last_request now preserves x_api_key and adds a headers object with exactly x-organization-uuid and x-trusted-device-token; header lookup is case-insensitive while JSON keys stay lower-case; rust-analyzer is not installed so lsp_diagnostics could not run.
2026-05-23T18:11:32+09:00
- Changed files: `crates/cc-lb-server/tests/api_wildcard_forwarding.rs`, `crates/cc-lb-server/src/app.rs`, `crates/cc-lb-server/src/builtins.rs`
- Commands run: `cargo fmt`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test api_wildcard_forwarding`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test multi_route_dispatch`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test admin_separate_listener`, `CC_LB_ADMIN_SKIP_SPA=1 cargo clippy -p cc-lb-server --all-targets -- -D warnings`
- Gotchas: `cc-lb-admin` build script needs `CC_LB_ADMIN_SKIP_SPA=1` in this environment; anonymous `/api/event_logging/batch` initially failed at downstream authn, so a minimal built-in auth exception was required to satisfy the forward-as-is requirement.
2026-05-23T18:26:52+09:00
- Changed files: `crates/cc-lb-server/src/app.rs`, `crates/cc-lb-server/tests/files_content_explicit_route.rs`, `.omo/notepads/cc-lb-p0-routing-fixes/learnings.md`
- Commands run: `cargo fmt --all`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test files_content_explicit_route`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test multi_route_dispatch`, `CC_LB_ADMIN_SKIP_SPA=1 cargo clippy -p cc-lb-server --all-targets -- -D warnings`
- Gotchas: route-registration coverage needed a test-only route path list because the existing `/v1/{*path}` wildcard would otherwise make `/v1/files/abc123/content` pass before the explicit route was registered; `rust-analyzer` is not installed, so `lsp_diagnostics` reported it unavailable.

2026-05-23T00:00:00+00:00
- Changed files: `crates/cc-lb-core/src/hop_by_hop.rs`, `crates/cc-lb-server/tests/header_preservation_contract.rs`, `.omo/evidence/task-4-headers-forward.txt`, `.omo/evidence/task-4-hop-by-hop.txt`, `.omo/evidence/task-4-contract-comment.txt`
- Commands run: `cargo fmt --all`, `CC_LB_ADMIN_SKIP_SPA=1 cargo test -p cc-lb-server --test header_preservation_contract`, `cargo test -p cc-lb-core --test lifecycle_response_header_passthrough`, `cargo doc --no-deps -p cc-lb-core`, `CC_LB_ADMIN_SKIP_SPA=1 cargo clippy -p cc-lb-core -p cc-lb-server --all-targets -- -D warnings`
- Gotchas: fake-anthropic only records x_api_key plus the selected lower-case headers, so Connection had to be verified with a separate recording upstream while keeping the fixture whitelist unchanged; rust-analyzer is still unavailable.
