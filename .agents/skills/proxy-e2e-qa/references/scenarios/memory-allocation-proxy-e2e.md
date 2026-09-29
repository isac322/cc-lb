# Memory Allocation Root Fixes Proxy E2E QA Scenario

Goal: prove that the proxy path correctly handles prompt-cache hit/miss routing, Wasmtime filter/shape plugin execution (including header stripping, signer credential override, origin mismatch fail-open, and filter fail-closed), and proxy body limits/compression (including non-streaming/SSE byte identity, gzip/br/zstd decoding, body-cap 413 pre-collection, and per-path caps).

This is an isolated in-process proxy-path scenario. It uses temporary SQLite main storage, a fresh disposable SQLite database, an ephemeral local fake Anthropic listener, and a temporary managed API key. No production storage, credentials, services, Postgres, paid providers, or real Anthropic APIs are involved.

## 0. Environment and preconditions

- Run from the cc-lb workspace root with the repository Rust toolchain and Cargo.
- Build with `--no-default-features --features sqlite`.
- Save raw command output under `/tmp`; retain summaries only in task evidence.
- Do not restart or mutate a service and do not touch the Postgres container.
- Fixed isolated ports are required: 53251 (proxy), 53252 (admin), 53253 (metrics), 53261 (primary fixture), and 53262 (foreign-origin fixture for C3.2).
- Initialize the isolated environment and common variables:
  ```bash
  export QA_ROOT="$(mktemp -d /tmp/cclb-memory-qa.XXXXXX)"
  export QA_DB="$QA_ROOT/storage.sqlite"
  export QA_PROXY='http://127.0.0.1:53251'
  export QA_ADMIN='http://127.0.0.1:53252'
  export QA_METRICS='http://127.0.0.1:53253'
  export QA_FIXTURE='http://127.0.0.1:53261'
  export QA_ADMIN_TOKEN='qa-admin-token'
  export QA_API_KEY='qa-disposable-managed-key'
  export QA_PRINCIPAL_ID='11111111-1111-4111-8111-111111111111'
  export QA_UPSTREAM_ID='22222222-2222-4222-8222-222222222222'
  export QA_UPSTREAM_NAME='qa-upstream'
  ```

The `proxy-e2e-qa` `SKILL.md` has no scenario index table, so this scenario does not require or add an index row.

## 1. Deterministic scenario steps

### C2, Prompt-Cache Hit/Miss Routing and Token Accounting

- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **C2.1 Non-streaming miss then hit preserves routing, body identity, and token accounting:**
  ```bash
  cp "$QA_ROOT/fixtures/c2-cacheable.json" "$QA_ROOT/c2-cacheable.json"
  for RID in c2-miss-0001 c2-hit-0002; do
    curl --fail-with-body -sS -o "$QA_ROOT/$RID.json" -w '%{http_code}\n' \
      -X POST "$QA_PROXY/v1/messages" \
      -H "x-api-key: $QA_API_KEY" \
      -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
      -H "x-request-id: $RID" --data-binary "@$QA_ROOT/c2-cacheable.json"
    curl -fsS "$QA_FIXTURE/captures/$RID" -o "$QA_ROOT/$RID-capture.json"
  done
  jq -e '.usage.cache_creation_input_tokens == 128 and .usage.cache_read_input_tokens == 0' "$QA_ROOT/c2-miss-0001.json"
  jq -e '.usage.cache_read_input_tokens == 128' "$QA_ROOT/c2-hit-0002.json"
  jq -e --arg sha "$(sha256sum "$QA_ROOT/c2-cacheable.json" | cut -d' ' -f1)" '.upstream == "cache-miss" and .body_sha256 == $sha' "$QA_ROOT/c2-miss-0001-capture.json"
  jq -e --arg sha "$(sha256sum "$QA_ROOT/c2-cacheable.json" | cut -d' ' -f1)" '.upstream == "cache-hit" and .body_sha256 == $sha' "$QA_ROOT/c2-hit-0002-capture.json"
  ```

- **C2.2 Streaming cache hit preserves SSE bytes while accounting cache-read tokens:**
  ```bash
  jq '.stream = true' "$QA_ROOT/c2-cacheable.json" >"$QA_ROOT/c2-cacheable-stream.json"
  curl --http1.1 --raw -NfsS -D "$QA_ROOT/c2-stream.headers" -o "$QA_ROOT/c2-stream.out" \
    -X POST "$QA_PROXY/v1/messages" \
    -H "x-api-key: $QA_API_KEY" -H 'anthropic-version: 2023-06-01' \
    -H 'content-type: application/json' -H 'x-request-id: c2-stream-0003' \
    --data-binary "@$QA_ROOT/c2-cacheable-stream.json"
  cmp "$QA_ROOT/fixtures/c2-hit.sse" "$QA_ROOT/c2-stream.out"
  curl -fsS "$QA_FIXTURE/captures/c2-stream-0003" -o "$QA_ROOT/c2-stream-capture.json"
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/v1/usage?range=1h&step=1m&group_by=upstream&upstream_id=$QA_UPSTREAM_ID" \
    -o "$QA_ROOT/c2-usage.json"
  jq -e '.upstream == "cache-hit"' "$QA_ROOT/c2-stream-capture.json"
  jq -e '[.series[].buckets[].cache_read_input_tokens] | add >= 128' "$QA_ROOT/c2-usage.json"
  ```

### C3, Wasmtime Filter/Shape Plugin Behavior

- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **C3.1 Shape output strips protected headers; the trusted signer overrides a spoofed credential:**
  ```bash
  SHAPE_ID="$(curl -fsS -X POST -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    -F "bytes=@$QA_ROOT/fixtures/shape-strip.wasm" \
    -F 'original_filename=shape-strip.wasm' \
    "$QA_ADMIN/admin/v1/plugins/wasm" | jq -er '.id')"
  curl -fsS -X POST -H "Authorization: Bearer $QA_ADMIN_TOKEN" -H 'content-type: application/json' \
    -d "{\"slot\":\"shape\",\"wasm_registry_id\":\"$SHAPE_ID\",\"position\":\"first\"}" \
    "$QA_ADMIN/admin/v1/principals/$QA_PRINCIPAL_ID/plugin-chain" \
    -o "$QA_ROOT/c3-shape-chain.json"
  curl --fail-with-body -sS -o "$QA_ROOT/c3-shape-response.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c3-shape-0001' --data-binary "@$QA_ROOT/message.json"
  curl -fsS "$QA_FIXTURE/captures/c3-shape-0001" -o "$QA_ROOT/c3-shape-capture.json"
  jq -e '.headers["x-qa-keep"] == "shaped"
    and (.headers.connection | not)
    and (.headers["x-api-key"] | not)
    and (.headers["x-anthropic-qa"] | not)
    and (.authorization_equals_real_signer_token == true)
    and (.authorization_equals_spoofed_plugin_value == false)' "$QA_ROOT/c3-shape-capture.json"
  ```

- **C3.2 Origin-policy rejection fails open to raw pass-through, not a foreign origin:**
  ```bash
  curl --fail-with-body -sS -o "$QA_ROOT/c3-origin-response.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c3-origin-0002' --data-binary "@$QA_ROOT/message.json"
  curl -fsS "$QA_FIXTURE/captures/c3-origin-0002" -o "$QA_ROOT/c3-origin-capture.json"
  jq -e '.upstream == "selected" and .path == "/v1/messages" and (.headers["x-qa-keep"] | not)' "$QA_ROOT/c3-origin-capture.json"
  ```

- **C3.3 Filter plugin that rejects every candidate fails closed:**
  ```bash
  FILTER_ID="$(curl -fsS -X POST -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    -F "bytes=@$QA_ROOT/fixtures/filter-reject-all.wasm" \
    -F 'original_filename=filter-reject-all.wasm' \
    "$QA_ADMIN/admin/v1/plugins/wasm" | jq -er '.id')"
  curl -fsS -X POST -H "Authorization: Bearer $QA_ADMIN_TOKEN" -H 'content-type: application/json' \
    -d "{\"slot\":\"router\",\"wasm_registry_id\":\"$FILTER_ID\",\"position\":\"first\"}" \
    "$QA_ADMIN/admin/v1/principals/$QA_PRINCIPAL_ID/plugin-chain" \
    -o "$QA_ROOT/c3-filter-chain.json"
  curl -sS -D "$QA_ROOT/c3-filter.headers" -o "$QA_ROOT/c3-filter.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c3-filter-0003' --data-binary "@$QA_ROOT/message.json"
  jq -e '.type == "error" and .error.type == "route_no_upstream_after_filter"' "$QA_ROOT/c3-filter.json"
  test "$(curl -fsS "$QA_FIXTURE/captures/c3-filter-0003" | jq -r '.present')" = false
  ```

### C5, Proxy Body Limits, Non-Streaming, SSE, and Compressed Streams

- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **C5.1 Non-streaming proxy response stays byte-identical:**
  ```bash
  curl --fail-with-body -sS -D "$QA_ROOT/c5-nonstream.headers" -o "$QA_ROOT/c5-nonstream.out" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c5-nonstream-0001' --data-binary "@$QA_ROOT/message.json"
  cmp "$QA_ROOT/fixtures/nonstream-response.json" "$QA_ROOT/c5-nonstream.out"
  ```

- **C5.2 Identity SSE stream remains byte-identical end to end:**
  ```bash
  curl --http1.1 --raw -NfsS -D "$QA_ROOT/c5-sse.headers" -o "$QA_ROOT/c5-sse.out" \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c5-sse-0002' --data-binary "@$QA_ROOT/fixtures/stream-message.json"
  cmp "$QA_ROOT/fixtures/identity.sse" "$QA_ROOT/c5-sse.out"
  rg -qi '^content-type: text/event-stream' "$QA_ROOT/c5-sse.headers"
  ```

- **C5.3 gzip, br, and zstd SSE decoding observes usage without rewriting client bytes:**
  ```bash
  for ENCODING in gzip br zstd; do
    RID="c5-$ENCODING-0003"
    curl --http1.1 --raw -NfsS -D "$QA_ROOT/$ENCODING.headers" -o "$QA_ROOT/$ENCODING.out" \
      -X POST "$QA_PROXY/v1/messages?fixture_encoding=$ENCODING" \
      -H "x-api-key: $QA_API_KEY" -H 'anthropic-version: 2023-06-01' \
      -H 'content-type: application/json' -H "x-request-id: $RID" \
      --data-binary "@$QA_ROOT/fixtures/stream-message.json"
    cmp "$QA_ROOT/fixtures/$ENCODING.sse" "$QA_ROOT/$ENCODING.out"
    rg -qi "^content-encoding: $ENCODING" "$QA_ROOT/$ENCODING.headers"
  done
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/usage?range=1h&step=1m&group_by=upstream&upstream_id=$QA_UPSTREAM_ID" \
    -o "$QA_ROOT/c5-compressed-usage.json"
  jq -e '[.series[].buckets[].output_tokens] | add >= 24' "$QA_ROOT/c5-compressed-usage.json"
  ```

- **C5.4 Content-Length and chunked oversize bodies are rejected before collection:**
  ```bash
  dd if=/dev/zero of="$QA_ROOT/oversize-1025.bin" bs=1025 count=1 status=none
  curl --http1.1 -sS -D "$QA_ROOT/c5-length.headers" -o "$QA_ROOT/c5-length.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c5-length-0004' --data-binary "@$QA_ROOT/oversize-1025.bin"
  curl --http1.1 -sS -D "$QA_ROOT/c5-chunked.headers" -o "$QA_ROOT/c5-chunked.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'transfer-encoding: chunked' -H 'x-request-id: c5-chunked-0005' \
    --data-binary "@$QA_ROOT/oversize-1025.bin"
  jq -e '.type == "error" and .error.type == "body_too_large" and .error.message == "request body exceeds configured cap"' "$QA_ROOT/c5-length.json"
  cmp "$QA_ROOT/c5-length.json" "$QA_ROOT/c5-chunked.json"
  ```

- **C5.5 Per-path body caps distinguish `/v1/messages` from `/v1/files`:**
  ```bash
  dd if=/dev/zero of="$QA_ROOT/2048.bin" bs=2048 count=1 status=none
  curl --http1.1 -sS -o "$QA_ROOT/c5-message-2048.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c5-message-cap-0006' --data-binary "@$QA_ROOT/2048.bin"
  curl --http1.1 --fail-with-body -sS -o "$QA_ROOT/c5-file-2048.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/files" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/octet-stream' \
    -H 'x-request-id: c5-files-cap-0007' --data-binary "@$QA_ROOT/2048.bin"
  jq -e '.error.type == "body_too_large"' "$QA_ROOT/c5-message-2048.json"
  curl -fsS "$QA_FIXTURE/captures/c5-files-cap-0007" -o "$QA_ROOT/c5-file-capture.json"
  jq -e '.path == "/v1/files" and .body_sha256 == "'"$(sha256sum "$QA_ROOT/2048.bin" | cut -d' ' -f1)"'"' "$QA_ROOT/c5-file-capture.json"
  ```

## 2. Assertion verdicts

| Assertion | Result | Evidence boundary |
|---|---|---|
| C2.1 Non-streaming miss then hit | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C2.2 Streaming cache hit | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.1 Shape output strips and overrides | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.2 Origin-policy fail-open | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.3 Filter fail-closed | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.1 Non-streaming byte-identity | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.2 Identity SSE stream byte-identity | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.3 Compressed SSE decoding | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.4 Oversize body pre-collection rejection | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.5 Per-path body caps | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |