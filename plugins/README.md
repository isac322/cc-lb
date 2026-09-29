# Plugins

WebAssembly plugins built against `cc-lb-pdk-wasmtime`. The plugin author reference (PDK macros, hooks, wire versions, admission, and upload via `/admin/v1/plugins/wasm`) lives in **[docs/plugin-author-guide.md](../docs/plugin-author-guide.md)**.

```
plugins/
├── router/
│   └── cache-aware-wasmtime/            # cache-affinity filter plugin
└── test-fixtures/                       # plugins used by host tests
    ├── wasmtime-filter-service-tier/
    └── wasmtime-shape-passthrough/
```

Build a plugin with:

```bash
rustup target add wasm32-unknown-unknown
cargo build -p cache-aware-wasmtime --target wasm32-unknown-unknown --release
```
