# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- `HookMetadata.mode` is required when deserializing `cc_lb.plugin.v1` metadata; metadata written before the `mode` key existed is rejected. `HookMode` no longer implements `Default`.

## [0.6.1](https://github.com/isac322/cc-lb/compare/cc-lb-plugin-wire-v0.6.0...cc-lb-plugin-wire-v0.6.1) - 2026-07-12

### Other

- workspace crate reassembly — delete cc-lb-plugin-api & cc-lb-contract, extract cc-lb-domain + 5 SPI/vocab crates (ADR-0008) ([#401](https://github.com/isac322/cc-lb/pull/401))
