# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-protocol-v0.2.2...cc-lb-runtime-protocol-v0.3.0) - 2026-07-01

### Other

- inject Clock trait for all wall-clock reads workspace-wide ([#183](https://github.com/isac322/cc-lb/pull/183))

## [0.2.2](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-protocol-v0.2.1...cc-lb-runtime-protocol-v0.2.2) - 2026-06-26

### Other

- updated the following local packages: cc-lb-plugin-api

## [0.2.1](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-protocol-v0.2.0...cc-lb-runtime-protocol-v0.2.1) - 2026-06-18

### Other

- updated the following local packages: cc-lb-plugin-api

## [0.2.0](https://github.com/isac322/cc-lb/compare/cc-lb-runtime-protocol-v0.1.0...cc-lb-runtime-protocol-v0.2.0) - 2026-06-16

### Added

- *(plugin-registry)* negotiate wire_version end-to-end and add cc-lb doctor ([#128](https://github.com/isac322/cc-lb/pull/128))

### Fixed

- *(release-plz)* decouple public crates from workspace version ([#132](https://github.com/isac322/cc-lb/pull/132))
- enforce supported_slots across view, admin, and runtime self-check ([#125](https://github.com/isac322/cc-lb/pull/125))
