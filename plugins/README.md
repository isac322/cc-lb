# Plugins

This directory contains WebAssembly (WASM) plugins for `cc-lb`, compiled to the `wasm32-wasip1` target. Plugins are **not** included in the default workspace build; they are built separately and loaded at runtime via the Extism plugin boundary.

## Plugin Categories

Plugins are organized by trait category from the `cc-lb-plugin-api`. Each category corresponds to a distinct extension point in the load balancer:

| Category | Purpose | Trait |
|----------|---------|-------|
| `router` | Request routing and principal selection | `cc-lb-plugin-api::traits::router::Router` |
| `dialect` | Protocol adaptation and message transformation | `cc-lb-plugin-api::traits::dialect::Dialect` |
| `signer` | Cryptographic signing and credential injection | `cc-lb-plugin-api::traits::signer::Signer` |
| `observability` | Logging, tracing, and metrics collection | `cc-lb-plugin-api::traits::observability::Observability` |

## Directory Structure

Each plugin lives in its own crate under its category:

```
plugins/
├── router/
│   ├── round-robin/
│   │   ├── Cargo.toml
│   │   └── src/
│   └── ...
├── dialect/
├── signer/
├── observability/
└── README.md (this file)
```

## Crate Naming Convention

All plugin crates follow the naming pattern:

```
cc-lb-<category>-<name>
```

**Examples:**
- `cc-lb-router-round-robin` (in `plugins/router/round-robin/`)
- `cc-lb-dialect-openai-compat` (in `plugins/dialect/openai-compat/`)
- `cc-lb-signer-jwt-bearer` (in `plugins/signer/jwt-bearer/`)

## Building Plugins

Plugins compile to the `wasm32-wasip1` target and are **excluded** from the workspace's default build. To build a specific plugin:

```bash
cd plugins/router/round-robin
cargo build --target wasm32-wasip1 --release
```

The compiled WASM module will be available at `target/wasm32-wasip1/release/cc_lb_router_round_robin.wasm`.

## Reference

For plugin API design and trait definitions, see the `cc-lb-plugin-api` crate (in `crates/`).
