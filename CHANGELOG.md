# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **BREAKING**: Removed the `redb` storage backend entirely. SQLite is now the default storage backend for local development and CI. The `StorageConfig::Redb` and `BackendKind::Redb` configuration options are no longer supported. If you have existing local development data in a `redb` database, use the `cc-lb-redb-migrate` CLI tool to migrate your data to SQLite.
- **BREAKING**: Upstreams, principals, and plugin chains are now managed via the database instead of TOML configuration sections. All administrative operations go through the dashboard or the `/admin/v1/*` REST API. See [docs/runtime-management.md](docs/runtime-management.md) for migration and API reference.

### Added

- **SQLite storage backend**: Added a new SQLite storage backend as the default database for local development and CI.
- **Per-principal plugin overrides**: Config authors can now configure custom plugins for individual principals.
- Validation, preflight checks, and hot-reload behavior now operate on an all-or-nothing basis.
- Admin `/status` JSON response now includes a `principals` map showing active overrides with redacted configuration hashes.
- Backward compatibility is fully preserved: zero-principal-plugin configurations remain unchanged, producing a byte-identical observe stream.
