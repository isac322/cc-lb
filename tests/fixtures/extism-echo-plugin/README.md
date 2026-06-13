# extism-echo-plugin

Tiny cc-lb fixture that exports `filter` (echoing its input) and `cc_lb_handshake` (advertising the `filter` wire function) so it can pass the admin upload pipeline.

Build it with:

```bash
cargo build -p extism-echo-plugin --target wasm32-unknown-unknown --release
```

The wasm artifact is written to `target/wasm32-unknown-unknown/release/extism_echo_plugin.wasm`.
