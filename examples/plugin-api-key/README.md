# Static API Key Extism Plugin

`plugin-api-key` is a small Extism authentication plugin for `cc-lb`. It reads a
static API-key map from Extism config, authenticates downstream requests by
`x-api-key` or `Authorization: Bearer ...`, and returns an `api_key` principal
plus signer state for the built-in Anthropic-style key signer behavior.

Build the wasm artifact from the workspace root:

```sh
cargo build --target wasm32-wasip1 --release -p plugin-api-key
```

The artifact is written to:

```text
target/wasm32-wasip1/release/plugin_api_key.wasm
```

The plugin config value `keys` is a JSON string, because Extism config values
are strings. The JSON maps `principal_id` to API key:

```toml
[plugins.authn_plugin]
name = "plugin-api-key"
wasm_path = "target/wasm32-wasip1/release/plugin_api_key.wasm"

[plugins.authn_plugin.config]
keys = '{"alice":"sk-ant-alice"}'
```

When the key `sk-ant-alice` is presented, the plugin returns principal `alice`,
`kind = "api_key"`, and signer factory reference `anthropic-key`. Header lookup
is case-insensitive, and `x-api-key` is preferred over `Authorization`.

Default quotas returned by the plugin are:

- `requests_per_window = 1000`
- `input_tokens_per_window = 1000000`
- `output_tokens_per_window = 200000`
- `window_seconds = 60`

Unknown keys are rejected by default when a static map is configured. A gated
SHA-256 principal fallback is available only when `allow_sha256_fallback` is set
to `"true"`; see `.omo/notepads/anthropic-proxy/pending-questions.md` for the
T33 plan contradiction this resolves.

See `tests/integration/api-key-plugin.toml` for a complete local smoke config.
