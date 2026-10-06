# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-wire-v0.8.0...cc-lb-plugin-wire-v0.9.0) - 2026-10-06

### Added

- *(site)* add operator documentation and GitHub support links ([#904](https://github.com/isac322/cc-lb/pull/904))

### Other

- [**breaking**] remove legacy compatibility code, fallbacks, and dead schema ([#890](https://github.com/isac322/cc-lb/pull/890))
- *(plugins)* [**breaking**] remove the observability hook plugin slot ([#885](https://github.com/isac322/cc-lb/pull/885))
- *(credentials)* remove legacy credential stack ([#778](https://github.com/isac322/cc-lb/pull/778))

### Breaking

- `HookMetadata.mode` is required when deserializing `cc_lb.plugin.v1` metadata; metadata written before the `mode` key existed is rejected. `HookMode` no longer implements `Default`.

## [0.6.1](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-wire-v0.6.0...cc-lb-plugin-wire-v0.6.1) - 2026-07-12

### Other

- workspace crate reassembly — delete cc-lb-plugin-api & cc-lb-contract, extract cc-lb-domain + 5 SPI/vocab crates (ADR-0008) ([#401](https://github.com/isac322/cc-lb/pull/401))
