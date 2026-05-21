# Anthropic Pro OAuth Extism Plugin

`plugin-oauth-anthropic-pro` authenticates Claude Pro/Max style Anthropic OAuth bearer tokens for `cc-lb`.

The plugin is intentionally small. It identifies the cc-lb principal and returns `signer_factory_ref = "anthropic-oauth"`; it does not perform OAuth refresh or talk to Anthropic.

Build from the workspace root:

```sh
cargo build --target wasm32-wasip1 --release -p plugin-oauth-anthropic-pro
```

The wasm artifact is written to:

```text
target/wasm32-wasip1/release/plugin_oauth_anthropic_pro.wasm
```

The Extism config value `config` is a JSON string:

```json
{
  "client_id": "oauth-client-id",
  "principals": {
    "alice": {
      "refresh_token_storage_key": "alice:anthropic_oauth",
      "token_prefix": "sk-ant-oat01-MOCK-alice"
    }
  }
}
```

`client_id` is the Anthropic OAuth client id used by the admin enrollment flow.

Each principal maps to the redb OAuth credential key used by the host signer.

`token_prefix` is optional. When present, the presented `Authorization: Bearer ...` token must start with that prefix.

When no principal has `token_prefix`, cc-lb must inject `cc-lb-principal-id` from an internal trusted mapping before the plugin runs.

Ambiguous mappings are rejected.

Successful authentication returns principal kind `subscription_bearer`.

The returned principal claim `refresh_token_storage_key` documents the storage row, for example `alice:anthropic_oauth`.

Default quotas are:

- `requests_per_window = 5000`
- `input_tokens_per_window = 5000000`
- `output_tokens_per_window = 1000000`
- `window_seconds = 60`

Enrollment is out-of-plugin and uses admin REST:

```sh
curl -X POST http://127.0.0.1:9090/admin/oauth/start \
  -H 'Authorization: Bearer test-admin' \
  -H 'Content-Type: application/json' \
  -d '{"principal_id":"alice","provider":"anthropic_oauth"}'
```

The response contains `authorize_url` and `state_token`.

Open or programmatically GET `authorize_url` against the configured issuer.

The OAuth server redirects with an authorization `code`.

Complete enrollment with:

```sh
curl -X POST http://127.0.0.1:9090/admin/oauth/complete \
  -H 'Authorization: Bearer test-admin' \
  -H 'Content-Type: application/json' \
  -d '{"state_token":"...","code":"..."}'
```

The admin handler exchanges the code through the Anthropic OAuth signer helper and persists credentials via `cc-lb-storage-redb`.

At request time, the host `anthropic-oauth` signer factory loads the credential row, refreshes if needed, and signs upstream requests with an OAuth bearer token.

We do NOT store raw access tokens in plugin output or config.

We do NOT store raw access tokens for operators to manage.

Only refresh credentials are persisted through cc-lb storage, AEAD-encrypted in redb with the configured master key.

See `tests/integration/oauth-plugin.toml` for a local mock-OAuth smoke configuration.
