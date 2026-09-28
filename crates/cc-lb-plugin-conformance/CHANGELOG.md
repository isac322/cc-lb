# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- Remove `conformance_engine_config` (and its prelude re-export). `ConformanceSuite` now defaults to `HotEngineConfig::default()`, which raises the plugin memory ceiling from 1024 pages (64 MiB) to 2048 pages (128 MiB) to match production.

## [0.2.4](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-conformance-v0.2.3...cc-lb-plugin-conformance-v0.2.4) - 2026-07-15

### Added

- add service-tier pricing support ([#455](https://github.com/isac322/cc-lb/pull/455))

### Other

- fold filter service tier into wire v1 ([#475](https://github.com/isac322/cc-lb/pull/475))

## [0.2.3](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-conformance-v0.2.2...cc-lb-plugin-conformance-v0.2.3) - 2026-07-12

### Other

- workspace crate reassembly — delete cc-lb-plugin-api & cc-lb-contract, extract cc-lb-domain + 5 SPI/vocab crates (ADR-0008) ([#401](https://github.com/isac322/cc-lb/pull/401))

## [0.2.2](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-conformance-v0.2.1...cc-lb-plugin-conformance-v0.2.2) - 2026-07-09

### Added

- *(plugin)* add response transform hooks ([#373](https://github.com/isac322/cc-lb/pull/373))
- *(routing)* add v9 subscription-preference cache-loss gate ([#364](https://github.com/isac322/cc-lb/pull/364))

### Fixed

- *(routing)* key subscription-preference WRH on thread_id instead of request_id ([#322](https://github.com/isac322/cc-lb/pull/322))

## [0.2.1](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-conformance-v0.2.0...cc-lb-plugin-conformance-v0.2.1) - 2026-07-04

### Other

- updated the following local packages: cc-lb-runtime-wasmtime
