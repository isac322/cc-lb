# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, Extism-based plugin boundaries, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`. The implementation plan lives in [.omo/plans/anthropic-proxy.md](./.omo/plans/anthropic-proxy.md).
