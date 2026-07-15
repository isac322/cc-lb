# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

<<<<<<< Updated upstream
=======
## [0.1.4](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-wasmtime-v0.1.3...cc-lb-runtime-wasmtime-v0.1.4) - 2026-07-15

### Added

- add service-tier pricing and filter wire v2 ([#455](https://github.com/isac322/cc-lb/pull/455))

### Other

- fold filter service tier into wire v1 ([#475](https://github.com/isac322/cc-lb/pull/475))
- Add slot-agnostic Plugins dashboard ([#468](https://github.com/isac322/cc-lb/pull/468))
- *(storage)* optimize request event and quota queries ([#466](https://github.com/isac322/cc-lb/pull/466))
- harden reproducible build inputs ([#464](https://github.com/isac322/cc-lb/pull/464))
- *(wasm)* cache compiled modules by content hash to stop redundant cranelift recompilation ([#448](https://github.com/isac322/cc-lb/pull/448))
- reduce allocator churn across quota, prompt-cache, wasmtime, price-catalog, and HTTP paths ([#423](https://github.com/isac322/cc-lb/pull/423))
- consolidate integration-test targets into per-crate harnesses ([#427](https://github.com/isac322/cc-lb/pull/427))

### Added

- Add wire-version-aware runtime probes for Filter V1 and V2, including requested service-tier delivery in the V2 sample.

>>>>>>> Stashed changes
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
