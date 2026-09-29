# Price Catalog Refresh and Priced Usage QA Scenario

Goal: functionally test the price-catalog refresh and priced usage features end to end, spanning storage, the HTTP admin API, and routing, such that a zero-context agent can reproduce identical results from this file alone.

Two equally-weighted halves:
- **Part A, Point-in-time** (§3): given a fixed state, is each layer correct?
- **Part B, State-transition** (§4): when backend data changes, does every layer change the way it should?

---

## 0. Environment and preconditions

- Service: local `cc-lb.service`; admin API + embedded SPA at `http://127.0.0.1:52252` (proxy `:52251`, metrics `:52253`).
- Auth: `localStorage['cc-lb-admin-token']` = env `CC_LB_ADMIN_TOKEN`. Header: `Authorization: Bearer $TOKEN`.
- API: `curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:52252<path>`.
- DB read: `sqlite3 -readonly "file:$QA_DB?mode=ro" "<SQL>"` against the isolated fixture database from §0.1.

### 0.1 Isolated test instance (REQUIRED for state-transition mutations, never mutate shared prod DB)
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

The isolated config is part of the fixture, not an optional convenience. It must:
- bind only `53251` (proxy), `53252` (admin + embedded admin-web), and `53253` (metrics);
- create `$QA_PRINCIPAL_ID`, its disposable `$QA_API_KEY`, and `$QA_UPSTREAM_ID` pointing at `$QA_FIXTURE`;
- set `messages_body_cap_bytes=1024` and `files_body_cap_bytes=4096` in the lifecycle fixture passed to the isolated server;
- make the fixture expose `GET $QA_FIXTURE/captures/<request-id>` as JSON with `upstream`, `method`, `path`, `headers`, and `body_sha256`; and `POST $QA_FIXTURE/catalog` / `GET $QA_FIXTURE/catalog` for its local price document and revision;
- remove `$QA_ROOT` and revoke the disposable managed key after the run.

Use the client-shaped request below unless an item supplies another one. All response/capture files stay under `$QA_ROOT`; do not print secrets or raw captures into task evidence.

```bash
cat >"$QA_ROOT/message.json" <<'JSON'
{"model":"qa-price-model","max_tokens":24,"messages":[{"role":"user","content":"reply with exactly: qa"}]}
JSON
```

## 1. Context data

The fixture catalog at `$QA_FIXTURE/catalog` starts with `qa-price-model` at `input_cost_per_token=0.000001` and `output_cost_per_token=0.000001`. The fake non-streaming Anthropic response reports exactly `usage.input_tokens=100` and `usage.output_tokens=20`. Therefore, one completed request must add exactly `virtual_cost_micros=120` to `/admin/v1/usage`.

## 2. Deterministic state mutation, THE ENGINE

- **M1 unchanged refresh:** Re-reading an unchanged catalog leaves fixture revision `1` and the same usage price.
- **M2 changed catalog:** Replace the catalog with `catalog-v2.json`, whose same model has `input_cost_per_token=0.000002` and `output_cost_per_token=0.000003`. Wait for the built-in catalog refresh cadence and confirm the local catalog control reports revision `2`.

## 3. Part A, Point-in-time cases

### 3.1 Point-in-time and unchanged refresh preserve price and usage (C4.1)
- **Source and context:** documented for isolated execution; no operational evidence is retained here.
- **Live invocation:**
  ```bash
  curl -fsS -X POST "$QA_FIXTURE/catalog" -H 'content-type: application/json' \
    --data-binary "@$QA_ROOT/fixtures/catalog-v1.json" -o "$QA_ROOT/c4-catalog-v1.json"
  curl --fail-with-body -sS -o "$QA_ROOT/c4-v1-response.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c4-v1-0001' --data-binary "@$QA_ROOT/message.json"
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/v1/usage?range=1h&step=minute&group_by=upstream&upstream_id=$QA_UPSTREAM_ID" \
    -o "$QA_ROOT/c4-usage-v1.json"
  curl -fsS "$QA_FIXTURE/catalog" -o "$QA_ROOT/c4-catalog-unchanged.json"
  jq -e '.observed == true and ([.series[].buckets[].virtual_cost_micros] | add == 120)' "$QA_ROOT/c4-usage-v1.json"
  jq -e '.revision == 1 and .served_document == "catalog-v1"' "$QA_ROOT/c4-catalog-unchanged.json"
  ```
- **Expected observable result:** The proxied request and `/admin/v1/usage` both return `200`. Usage is observed and the only bucket total is exactly `virtual_cost_micros:120`. Re-reading an unchanged catalog leaves fixture revision `1` and the same usage price. No changed or missing fallback is invented.

## 4. Part B, State-transition cases

### 4.1 Changed catalog changes only subsequent priced usage (C4.2)
- **Fixture mutation:** Replace the catalog with `catalog-v2.json`, whose same model has `input_cost_per_token=0.000002` and `output_cost_per_token=0.000003`. Allow the built-in catalog refresh worker to run and confirm the local catalog control reports revision `2`.
- **Live invocation:**
  ```bash
  curl -fsS -X POST "$QA_FIXTURE/catalog" -H 'content-type: application/json' \
    --data-binary "@$QA_ROOT/fixtures/catalog-v2.json" -o "$QA_ROOT/c4-catalog-v2.json"
  sleep "$QA_PRICE_CATALOG_WAIT_SECS"
  curl -fsS "$QA_FIXTURE/catalog" -o "$QA_ROOT/c4-catalog-after-refresh.json"
  curl --fail-with-body -sS -o "$QA_ROOT/c4-v2-response.json" -w '%{http_code}\n' \
    -X POST "$QA_PROXY/v1/messages" -H "x-api-key: $QA_API_KEY" \
    -H 'anthropic-version: 2023-06-01' -H 'content-type: application/json' \
    -H 'x-request-id: c4-v2-0002' --data-binary "@$QA_ROOT/message.json"
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/v1/usage?range=1h&step=minute&group_by=upstream&upstream_id=$QA_UPSTREAM_ID" \
    -o "$QA_ROOT/c4-usage-v2.json"
  jq -e '.revision == 2 and .served_document == "catalog-v2"' "$QA_ROOT/c4-catalog-after-refresh.json"
  jq -e '.observed == true and ([.series[].buckets[].virtual_cost_micros] | add == 380)' "$QA_ROOT/c4-usage-v2.json"
  ```
- **Expected observable result:** The control shows revision `2`. The second client request is `200`. Aggregated `virtual_cost_micros` is exactly `380` (`120` for the v1 request plus `260` for the v2 request). This proves the changed catalog affects subsequent usage while the first recorded request retains its original correct price.

## 5. Automated-test coverage map

- Price catalog local installer: `crates/cc-lb-server/src/app.rs::spawn_price_catalog_local_installer`
- Price catalog loader: `crates/cc-lb-pricing/src/loader.rs`
- Price catalog parser: `crates/cc-lb-pricing/src/loader.rs::parse_litellm_json`

## 6. Verdict table

| Case | Layer(s) | Result | Evidence |
|------|----------|--------|----------|
| C4.1 Point-in-time | API, storage |  |  |
| C4.2 State transition | API, storage |  |  |
