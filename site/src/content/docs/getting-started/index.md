---
title: Run cc-lb
description: Build or run cc-lb, configure authenticated runtime state, and reach the first request.
slug: docs/getting-started
---

cc-lb puts a self-hosted Anthropic-compatible endpoint in front of pooled API-key and OAuth upstreams. Choose the binary path for a source build or the container path for the published image; both use the same admin and proxy contracts.

## Choose a runtime

| Path | Use it when | Artifact |
| --- | --- | --- |
| [Build the binary](#build-the-binary) | You own the host toolchain and want a local release build. | `target/release/cc-lb` |
| [Run the published container](#run-the-published-container) | You want the packaged server with no Rust checkout on the host. | `ghcr.io/isac322/cc-lb:1.0.0` |

Both paths need an Anthropic API key or Anthropic OAuth account for an upstream, an admin token, a master key for encrypted credentials, and durable storage.

## Build the binary

### Prerequisites

- A Linux host with Git, [rustup](https://rustup.rs/), and Bun 1.3.14 or newer.
- Rust 1.98.1 from the repository's `rust-toolchain.toml`.
- The `wasm32-unknown-unknown` Rust target for bundled Wasmtime fixtures.
- `openssl` for generating local secrets.

Build the server and its bundled admin app from the repository root:

```bash
rustup target add wasm32-unknown-unknown
cargo build --release -p cc-lb-server
```

The executable is `./target/release/cc-lb`. The build includes the admin web app and bundled Wasm fixtures; set `CC_LB_SKIP_WASM_FIXTURE_BUILD=1` only when you deliberately do not need those fixtures.

Create the local secrets and storage directory:

```bash
export CC_LB_MASTER_KEY="$(openssl rand -hex 32)"
export CC_LB_ADMIN_TOKEN="$(openssl rand -hex 24)"
export ANTHROPIC_API_KEY="replace-with-your-upstream-secret"
mkdir -p ./data
```

Keep the values outside source control. Create `cc-lb.toml`:

```toml
[runtime]
data_dir = "./data/runtime"

[listener]
proxy_addr = "127.0.0.1:8080"
admin_addr = "127.0.0.1:9090"
metrics_addr = "127.0.0.1:9091"

[storage]
kind = "sqlite"
path = "./data/storage.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[[admin.auth.providers]]
kind = "static_token"
id = "local"
token_env = "CC_LB_ADMIN_TOKEN"
```

Validate the configuration, then start the server:

```bash
./target/release/cc-lb config validate --config cc-lb.toml
./target/release/cc-lb serve --config cc-lb.toml
```

The default local endpoints are the admin dashboard and API at `http://127.0.0.1:9090/`, the proxy at `http://127.0.0.1:8080/`, and metrics at `http://127.0.0.1:9091/`.

## Run the published container

The published image is the immutable server release image. Prepare a config file and an environment file on the host.

`.env`:

```dotenv
CC_LB_MASTER_KEY=replace-with-a-64-character-hex-key
CC_LB_ADMIN_TOKEN=replace-with-a-long-random-admin-token
ANTHROPIC_API_KEY=replace-with-your-upstream-secret
```

`cc-lb.container.toml`:

```toml
[runtime]
data_dir = "/var/lib/cc-lb/data"

[listener]
proxy_addr = "0.0.0.0:8080"
admin_addr = "0.0.0.0:9090"
metrics_addr = "0.0.0.0:9091"

[storage]
kind = "sqlite"
path = "/var/lib/cc-lb/storage.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[[admin.auth.providers]]
kind = "static_token"
id = "container"
token_env = "CC_LB_ADMIN_TOKEN"
```

Create a writable host data directory and run the image as the current host user so SQLite and runtime state can persist through the bind mount:

```bash
mkdir -p ./data

docker run --rm --name cc-lb \
  --user "$(id -u):$(id -g)" \
  --env-file .env \
  -p 8080:8080 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v "$PWD/cc-lb.container.toml:/etc/cc-lb/cc-lb.toml:ro" \
  -v "$PWD/data:/var/lib/cc-lb" \
  ghcr.io/isac322/cc-lb:1.0.0
```

The container exposes the proxy, admin, and metrics listeners. Keep the admin and metrics ports on a private network when the container is not running only on a local host.

## Continue with runtime setup

Use the [install and configure guide](/docs/getting-started/install/) to create an upstream, principal, and proxy key. Then send a standard Messages API request through the proxy.
