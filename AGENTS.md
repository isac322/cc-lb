## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

## SQLite Storage Backend

SQLite is now the default storage backend for local development and CI. The historical redb backend has been removed entirely.

Rules:
- Use SQLite for all local development and testing. It's the default backend, so you don't need to specify extra feature flags for standard builds.
- Keep SQLite database files on a local filesystem. Don't use NFS mounts or other network filesystems, as they don't support SQLite's locking mechanisms reliably.
- Ensure all new migrations and queries are compatible with SQLite's SQL dialect. For example, use `strftime('%s', 'now')` for Unix timestamps.
- Remember that SQLite enforces foreign keys per connection. The connection pool is tuned with `PRAGMA foreign_keys = ON`, WAL mode, and a busy timeout of 5 seconds.
- Don't attempt to configure multi-replica SQLite setups. We don't support Litestream or rqlite.
- Map complex types like arrays using JSON text columns. SQLite doesn't have native array types like Postgres.
