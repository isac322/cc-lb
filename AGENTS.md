## SQLite Storage Backend

SQLite is now the default storage backend for local development and CI. The historical redb backend has been removed entirely.

Rules:
- Use SQLite for all local development and testing. It's the default backend, so you don't need to specify extra feature flags for standard builds.
- Keep SQLite database files on a local filesystem. Don't use NFS mounts or other network filesystems, as they don't support SQLite's locking mechanisms reliably.
- Ensure all new migrations and queries are compatible with SQLite's SQL dialect. For example, use `strftime('%s', 'now')` for Unix timestamps.
- Remember that SQLite enforces foreign keys per connection. The connection pool is tuned with `PRAGMA foreign_keys = ON`, WAL mode, and a busy timeout of 5 seconds.
- Don't attempt to configure multi-replica SQLite setups. We don't support Litestream or rqlite.
- Map complex types like arrays using JSON text columns. SQLite doesn't have native array types like Postgres.
