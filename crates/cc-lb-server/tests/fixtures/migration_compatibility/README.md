# Historical migration compatibility fixture

`cc-lb-v2.1.0-postgres.sql` is a plain-text PostgreSQL dump generated from the released `cc-lb-v2.1.0` artifact before the `apalis-postgres` 1.0.0 upgrade.

- Release tag: `cc-lb-v2.1.0`
- Release commit: `b39cb6b1b7fcd11dbf3f887f687a2555210eca25`
- Container image: `ghcr.io/isac322/cc-lb:2.1.0`
- Image digest: `sha256:77b7627ef99903408af77290829c79247c588c1e52b1aa640ea2e7866ffd4382`
- Historical scheduler artifact: `apalis-postgres 1.0.0-rc.8` (Cargo.lock checksum `947ad881f4102c5a4d6bb4f117ed38c1b4fe15868631facbc858aab57e3ad38d`)
- PostgreSQL server: `15.19` (`postgres:15.19`, image digest `sha256:e2f427e086310a0ec75ac050a875521887d1f82c9843a3a3ad90ce9bbbc0dbdc`)
- Dump tool: `pg_dump 15.19 (Debian 15.19-1.pgdg13+2)`
- Generation command: `pg_dump -U postgres -d cc_lb_legacy --format=plain --column-inserts --no-owner --no-privileges --no-comments`
- Fixture SHA-256: `002101d1969052862eb06405436b4ada528256eeba9eb7acb1df8feec740b60e`

The database was initialized by running the released image against PostgreSQL. The scheduler ledger is the pre-1.0 `cc_lb_scheduler._sqlx_migrations` table, and the dump retains one pending `apalis.jobs` row with JSONB metadata and an idempotency key. The compatibility test restores this dump into a disposable database and invokes the current shared startup path without pre-seeding a fresh database.
