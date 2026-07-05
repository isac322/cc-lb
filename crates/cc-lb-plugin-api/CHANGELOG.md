# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-api-v0.5.0...cc-lb-plugin-api-v0.6.0) - 2026-07-05

### Other

- *(routing)* drop unused round-robin/least-connections terminal strategies ([#319](https://github.com/isac322/cc-lb/pull/319))

## [0.4.0](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-api-v0.3.0...cc-lb-plugin-api-v0.4.0) - 2026-07-03

### Added

- *(plugin)* [**breaking**] wire v1 baseline + hook versioning + description contract + admission probe ([#287](https://github.com/isac322/cc-lb/pull/287))

### Removed

- *(plugin)* [**breaking**] drop `UpstreamDialect::normalize_error` — all in-tree implementations returned `None`; Bedrock, the only intended consumer, is not supported. Downstream dialects must migrate.
- *(plugin)* [**breaking**] drop `PluginRuntime` trait and `RuntimeError::HandshakeFailed` variant — handshake replaced with the 2-stage admission gate (static `inspect_wasm` + runtime ABI probe) in `cc-lb-runtime-wasmtime`.

### Other

- *(plugin)* compress plugin versioning surfaces back to v1 baseline ([#275](https://github.com/isac322/cc-lb/pull/275))

## [0.2.1](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-api-v0.2.0...cc-lb-plugin-api-v0.2.1) - 2026-06-26

### Added

- *(router)* add builtin subscription preference filter ([#176](https://github.com/isac322/cc-lb/pull/176))

## [0.2.0](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-api-v0.1.1...cc-lb-plugin-api-v0.2.0) - 2026-06-18

### Added

- *(core)* capture Anthropic unified rate-limit extras with numeric threshold ([#143](https://github.com/isac322/cc-lb/pull/143))
