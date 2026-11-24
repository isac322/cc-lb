---
title: Run cc-lb
description: Build or run cc-lb, configure authenticated runtime state, and reach the first request.
slug: docs/getting-started
---

cc-lb puts an operator-managed endpoint in front of pooled Anthropic, OpenAI, and Gemini API-key and OAuth upstreams. Choose the binary path for a source build or the container path for the published image; both use the same admin and proxy contracts.

## Choose a runtime

| Path | Use it when | Artifact |
| --- | --- | --- |
| [Build the binary](#build-the-binary) | You own the host toolchain and want a local release build. | `target/release/cc-lb` |
| [Run the published container](#run-the-published-container) | You want the packaged server with no Rust checkout on the host. | `ghcr.io/isac322/cc-lb:1.0.1` |
| [Split control-plane & data-plane](#split-control-plane-and-data-plane-deployment) | You scale proxy pods independently from a PostgreSQL-backed admin control plane. | `cc-lb-proxy` + `cc-lb-admin` (`--target proxy`, `--target admin`) |

Both paths need an upstream, an admin token, a master key for encrypted credentials, and durable storage. The upstream may be Anthropic (`anthropic_api_key` or `anthropic_oauth`), OpenAI (`openai_api_key` or `openai_oauth`), Gemini (`gemini_api_key` or `gemini_oauth`), or Vertex (`vertex_api_key` or `vertex_credential`). See [upstream concepts](/docs/concepts/upstreams/) for credential paths and warm-up limits.

Shared native routes cover Anthropic `POST /v1/messages` and `POST /v1/messages/count_tokens`, OpenAI `POST /v1/responses` and `GET /v1/responses` WebSocket, and Gemini `POST /v1beta/models/{model}:generateContent`, `:streamGenerateContent`, and `:countTokens`. The [runtime endpoint reference](/docs/reference/source/runtime-management/#proxy-capability-boundary) lists supported provider-prefixed routes and credential restrictions.

cc-lb does not translate between providers or act as a universal multi-provider gateway. Vertex AI Gemini is supported through scoped API-key and credential upstreams; Express and regional API-key targets support generate/stream/countTokens only because Google rejects API keys for cache, catalog, and Live, while regional credentials add approved cached-content create/use, publisher model-catalog list/get, and Live routes through Vertex. The catalog is a minimal publisher projection, not entitlement-aware; explicit cached-content billing remains Vertex-controlled; use least-privilege service accounts. Bedrock remains unsupported.

## Build the binary

### Prerequisites

- A Linux host with Git, [rustup](https://rustup.rs/), and Bun 1.4.4 or newer.
- Rust 1.99.1 from the repository's `rust-toolchain.toml`.
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
  ghcr.io/isac322/cc-lb:1.0.1
```

The container exposes the proxy, admin, and metrics listeners. Keep the admin and metrics ports on a private network when the container is not running only on a local host.

## Split control-plane and data-plane deployment

For PostgreSQL-backed deployments that scale proxy pods separately from the admin control plane, cc-lb also builds as two focused binaries (plus Docker targets `proxy` and `admin`):

| Binary / Docker target | Purpose &amp; roles | Listeners &amp; routes | Compile isolation |
| --- | --- | --- | --- |
| `cc-lb-proxy` (`--target proxy`) | Data-plane proxy and optional scheduler worker pool. Supports `--role proxy-worker` (default), `--role proxy` (or `--no-worker`, stateless proxy only), `--role worker`, plus `config validate`. | `proxy_addr` (default `:8080`, includes `/healthz` and `/readyz`) and `metrics_addr` (default `:9091`) | Excludes `cc-lb-admin` and `rust-embed` / SPA web assets |
| `cc-lb-admin` (`--target admin`) | PostgreSQL DB-only control plane (`cc-lb-admin-server` package). Enqueues jobs without running worker loops; streams live-tail updates via Postgres `LISTEN`/`NOTIFY` and storage polling. | `admin_addr` (default `:9090`, includes `/admin/health` and optional SPA via `embed-spa`) and `metrics_addr` (default `:9091`) | Excludes proxy engine, routing evaluator, event pipeline, and proxy listeners |

Build the PostgreSQL split binaries from source or via Docker:

```bash
cargo build --release -p cc-lb-proxy --no-default-features --features postgres
cargo build --release -p cc-lb-admin-server --no-default-features --features postgres,embed-spa

# Or container targets:
docker buildx build --target proxy --build-arg FEATURES=postgres -t cc-lb-proxy .
docker buildx build --target admin --build-arg FEATURES=postgres -t cc-lb-admin .
```

### Split environment, ports, and config contract

- **Environment variables**: set `CC_LB_MASTER_KEY` (64-character hex) on every `cc-lb-proxy` and `cc-lb-admin` process so all nodes decrypt credentials consistently; set `CC_LB_ADMIN_TOKEN` on `cc-lb-admin`.
- **Shared configuration**: mount the shared PostgreSQL config read-only (for example, [`deploy/compose/cc-lb.split.toml`](https://github.com/isac322/cc-lb/blob/master/deploy/compose/cc-lb.split.toml) at `/etc/cc-lb/cc-lb.toml:ro`). Because `cc-lb-admin` cannot rewrite proxy config files across separate filesystems, `GET /admin/v1/config/editor` reports `file.mode = "read_only"` and `POST /admin/v1/config/save` returns `409 file_not_writable`, while draft editing, validation, and `POST /admin/v1/config/draft/download` (`200 OK` TOML) stay enabled.
- **Compose example**: run [`deploy/compose/docker-compose.split.yml`](https://github.com/isac322/cc-lb/blob/master/deploy/compose/docker-compose.split.yml) (`proxy-a` `:8080` / metrics `:9091` with `--role proxy-worker`, `proxy-b` `:8081` / metrics `:9092` with `--role proxy`, and `admin` `:9090` / metrics `:9093`); see [`deploy/compose/README.md`](https://github.com/isac322/cc-lb/blob/master/deploy/compose/README.md).

## Continue with runtime setup

The [install and configure guide](/docs/getting-started/install/) uses an Anthropic API-key example to create an upstream, principal, and proxy key, then send a Messages API request. For OpenAI or Gemini, use the provider-specific credential and route contracts in the [runtime management reference](https://github.com/isac322/cc-lb/blob/master/docs/runtime-management.md) and the [README supported scope](https://github.com/isac322/cc-lb#supported-scope).
