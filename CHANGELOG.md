# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **BREAKING**: Upstreams, principals, and plugin chains are now managed via the database instead of TOML configuration sections. All administrative operations go through the dashboard or the `/admin/v1/*` REST API. See [docs/runtime-management.md](docs/runtime-management.md) for migration and API reference.
- **BREAKING**: Dropped support for wire v1 and v2 router plugins. All router plugins must now implement the wire v3 filter contract.
- **BREAKING**: Removed the legacy single-router reference plugin from the codebase.
- **Behavior Change**: Removed the automatic fallback to the first candidate when a router plugin returns no decision. The proxy now fails closed if the filter pipeline empties the candidate list.
- **Behavior Change**: Shape plugin trap errors are now forwarded raw to the client instead of being swallowed or mapped to generic errors.

### Added

- **Router Pipeline**: Principals can now configure multiple router plugins in an ordered pipeline.
- **Terminal Strategy**: Added support for configuring a terminal selection strategy per principal. This strategy selects the final upstream from the filtered candidates.
- **Per-principal plugin overrides**: Config authors can now configure custom plugins for individual principals.
- Validation, preflight checks, and hot-reload behavior now operate on an all-or-nothing basis.
- Admin `/status` JSON response now includes a `principals` map showing active overrides with redacted configuration hashes.
- Backward compatibility is fully preserved: zero-principal-plugin configurations remain unchanged, producing a byte-identical observe stream.
