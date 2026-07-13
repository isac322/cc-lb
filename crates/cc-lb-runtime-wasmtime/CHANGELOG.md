# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-wasmtime-v0.1.3...cc-lb-runtime-wasmtime-v0.1.4) - 2026-07-13

### Other

- update Cargo.toml dependencies

## [0.1.3](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-wasmtime-v0.1.2...cc-lb-runtime-wasmtime-v0.1.3) - 2026-07-12

### Other

- workspace crate reassembly — delete cc-lb-plugin-api & cc-lb-contract, extract cc-lb-domain + 5 SPI/vocab crates (ADR-0008) ([#401](https://github.com/isac322/cc-lb/pull/401))

## [0.1.2](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-wasmtime-v0.1.1...cc-lb-runtime-wasmtime-v0.1.2) - 2026-07-09

### Added

- *(plugin)* add response transform hooks ([#373](https://github.com/isac322/cc-lb/pull/373))
- *(routing)* add v9 subscription-preference cache-loss gate ([#364](https://github.com/isac322/cc-lb/pull/364))
- *(observability)* surface subscription-preference tier + WRH urgency in RoutingTrace ([#324](https://github.com/isac322/cc-lb/pull/324))

### Fixed

- *(routing)* cache-weighted subscription-preference exponential boost — delete cache_affinity from principal chain ([#340](https://github.com/isac322/cc-lb/pull/340)) ([#350](https://github.com/isac322/cc-lb/pull/350))
- *(routing)* key subscription-preference WRH on thread_id instead of request_id ([#322](https://github.com/isac322/cc-lb/pull/322))

### Other

- *(workspace)* Direction Y crate restructure — split cc-lb-core into engine/contract/control ([#317](https://github.com/isac322/cc-lb/pull/317))
- *(routing)* rewrite subscription-preference filter with weighted-rendezvous selection ([#312](https://github.com/isac322/cc-lb/pull/312))

## [0.1.1](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-wasmtime-v0.1.0...cc-lb-runtime-wasmtime-v0.1.1) - 2026-07-04

### Added

- make wasmtime allocation strategy configurable ([#294](https://github.com/isac322/cc-lb/pull/294))
