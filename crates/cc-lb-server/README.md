# cc-lb-server

Anthropic-compatible multi-principal reverse proxy server. Supports both redb (default) and postgres backends for managed API key storage and policy enforcement.

## Postgres deployment

### Minimum postgres version

Tested against postgres:17-alpine; minimum postgres version is postgres 17. Migrations in `crates/cc-lb-storage-postgres/migrations/` target postgres 17+ features and semantics.

### Connection string format

Connection strings follow the libpq URI format:
```
postgres://user:password@host:port/database
```

Example:
```
postgres://cclb:cclb@localhost:5432/cclb
```

### Required migrations

Migrations are auto-applied on server startup via `sqlx::migrate!` macro. All migrations in `crates/cc-lb-storage-postgres/migrations/` execute in deterministic order, including:
- Schema setup (meta, killswitch, audit log, request events)
- Quota tracking and principal limit states
- OAuth credential storage
- API key tables (opaque AEAD ciphertext + managed key records)
- Configuration versioning
- Managed API key tables (0013_managed_api_keys, 0014_managed_api_key_index) for multi-instance support

No manual migration step is required; the server will initialize the database on first run.

### Feature flag requirement

Postgres support requires the `postgres` feature flag. Build with:
```
cargo build --features postgres
```

The default build uses the redb backend:
```
cargo build  # equivalent to: cargo build --features redb
```

To build with both backends available (redb default at runtime, postgres via config):
```
cargo build --all-features
```

### Retry behavior

The postgres adapter implements exponential backoff for transient connection failures:
- Attempt 1: immediate
- Attempt 2: after 50ms
- Attempt 3: after 200ms
- Attempt 4: after 500ms

Total worst-case latency before failure: 750ms (50 + 200 + 500).

Retry logic is defined in `crates/cc-lb-storage-postgres/src/adapter/retry.rs`. Transient errors (connection pool exhaustion, I/O errors, TLS errors) are retried. Logical errors (constraint violations, row not found, etc.) are returned immediately without retry.

After all retry attempts are exhausted, the server returns HTTP 503 Service Unavailable with a `Retry-After: 1` header, allowing clients to implement appropriate backoff.

### Multi-instance support

Managed API keys are stored in postgres and shared across server instances. A key issued on instance A authenticates requests to instance B, provided both instances connect to the same postgres database. This enables horizontal scaling and failover scenarios without key synchronization overhead.

See `crates/cc-lb-storage-conformance/tests/managed_keys_postgres.rs` (Task 15) for conformance verification of multi-instance key sharing and concurrent issue scenarios.
