# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/isac322/cc-lb/compare/cc-lb-pdk-wasmtime-macros-v0.1.2...cc-lb-pdk-wasmtime-macros-v0.2.0) - 2026-10-06

### Added

- *(site)* add operator documentation and GitHub support links ([#904](https://github.com/isac322/cc-lb/pull/904))

### Other

- [**breaking**] remove legacy compatibility code, fallbacks, and dead schema ([#890](https://github.com/isac322/cc-lb/pull/890))
- *(plugins)* [**breaking**] remove the observability hook plugin slot ([#885](https://github.com/isac322/cc-lb/pull/885))
- bump toolchain and dependencies to latest ([#728](https://github.com/isac322/cc-lb/pull/728))
- *(deps)* bump the cargo-workspace group across 1 directory with 18 updates ([#492](https://github.com/isac322/cc-lb/pull/492))

### Breaking

- Remove `#[derive(WireSchema)]`. `cc-lb-plugin-wire` generates the fingerprint of every hook wire type, and `#[cc_lb_plugin]` embeds it.

## [0.1.2](https://github.com/isac322/cc-lb/compare/cc-lb-pdk-wasmtime-macros-v0.1.1...cc-lb-pdk-wasmtime-macros-v0.1.2) - 2026-07-15

### Added

- add service-tier pricing support ([#455](https://github.com/isac322/cc-lb/pull/455))

### Other

- fold filter service tier into wire v1 ([#475](https://github.com/isac322/cc-lb/pull/475))

## [0.1.1](https://github.com/isac322/cc-lb/compare/cc-lb-pdk-wasmtime-macros-v0.1.0...cc-lb-pdk-wasmtime-macros-v0.1.1) - 2026-07-09

### Added

- *(plugin)* add response transform hooks ([#373](https://github.com/isac322/cc-lb/pull/373))
