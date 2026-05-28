# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **Per-principal plugin overrides**: Config authors can now configure custom plugins for individual principals.
- Added `router_plugin` and `observability_hooks` fields to `PrincipalSpec` in the configuration.
- Global `PluginsConfig` remains the default fallback chain when per-principal fields are omitted.
- Validation, preflight checks, and hot-reload behavior now operate on an all-or-nothing basis.
- Admin `/status` JSON response now includes a `principals` map showing active overrides with redacted configuration hashes.
- Backward compatibility is fully preserved: zero-principal-plugin configurations remain unchanged, producing a byte-identical observe stream.
