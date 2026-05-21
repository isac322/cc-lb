# Internal Key Mapping Extism Plugin

`plugin-internal-key-mapping` lets an organization issue opaque `cc-lb` tokens to developers and map those tokens to organization-owned Anthropic credentials at the proxy.

Developers send `Authorization: Bearer ck-internal-...`.

The plugin never forwards that internal token upstream.

Instead, it authenticates the token, returns an `internal_key` principal, and asks the host `anthropic-key` signer to load the real credential from encrypted redb storage.

This is useful when you want local or team tokens that broker access to Anthropic without exposing the real `sk-ant-...` value.

Build from the workspace root:

```sh
cargo build --target wasm32-wasip1 --release -p plugin-internal-key-mapping
```

The wasm artifact is written to:

```text
target/wasm32-wasip1/release/plugin_internal_key_mapping.wasm
```

The Extism config value `tokens` is a JSON string.

It maps each internal token to a principal and a real credential storage row:

```json
{
  "ck-internal-alice-xxxx": {
    "principal_id": "alice",
    "real_credential_kind": "anthropic_api_key",
    "real_credential_storage_key": "alice:real_anthropic_api_key"
  }
}
```

Supported `real_credential_kind` values are:

- `anthropic_api_key` maps to signer factory `anthropic-key`
- `anthropic_oauth` maps to signer factory `anthropic-oauth`
- `aws_sigv4` maps to signer factory `aws-sigv4`
- `gcp_oauth` maps to signer factory `gcp-oauth`

The T35 local integration config is:

```text
tests/integration/internal-key.toml
```

It points the upstream at the fake Anthropic fixture, enables redb storage, and configures this authn plugin with a sample `ck-internal-alice-X` mapping.

Before running the proxy, seed the encrypted redb row with the helper binary:

```sh
cargo run -p cc-lb-storage-redb --example seed_real_anthropic_key -- \
  /tmp/cc-lb-internal-key.redb \
  0000000000000000000000000000000000000000000000000000000000000000 \
  alice:real_anthropic_api_key \
  sk-ant-real-aaaa
```

At request time, send:

```sh
curl -X POST http://127.0.0.1:8080/v1/messages \
  -H 'Authorization: Bearer ck-internal-alice-X' \
  -d '{"model":"c","messages":[{"role":"user","content":"hi"}],"max_tokens":10}'
```

The upstream request is signed with `x-api-key: sk-ant-real-aaaa` from storage.

Default quotas are `1000` requests, `1000000` input tokens, `200000` output tokens, and a `60` second window.
