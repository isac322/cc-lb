# PostgreSQL Migration Rules

## Scope

These rules apply to every file in this migration directory. They supplement the repository-level `AGENTS.md` for PostgreSQL migrations only.
Existing merged migrations are historical artifacts and may predate these rules. NEVER edit them to make them compliant; apply these rules to new migrations and follow-up versions.

## Required design

- Migrations run automatically during application startup while holding a database-wide advisory lock. The total time for the entire pending migration set MUST be shorter than the configured pool `statement_timeout`. With the default 30-second timeout, rehearse for a total lock-hold target of 15 seconds or less and an absolute duration below 30 seconds. Migration SQL MUST NOT raise or disable `statement_timeout`—including raising it to 90 seconds—to bypass this constraint. The 120-second startup probe is only a secondary ceiling, not the migration budget.
- Production-scale data rewrites and backfills MUST default to an explicitly approved, separately operated online job.
- A production-scale rewrite or backfill MAY run during startup only as row-count-limited, individually committed, resumable batches. Each batch MUST cap the number of rows changed, commit independently, resume safely after interruption or retry, and use a durable marker or data predicate to skip completed work. A `WHERE` clause, date range, or time filter alone is not a bound; for example, one `UPDATE` covering 10 days is prohibited regardless of the filter.
- `-- no-transaction` only changes how sqlx wraps the migration. It MUST NOT be treated as proof that statements or PostgreSQL routines can commit autonomously.
- Test startup backfills through the actual `sqlx::migrate!` runner against a real PostgreSQL DSN. The test MUST exercise transaction control, interrupt execution, retry the same migration, prove that completed batches are not repeated, and verify the final state. Preserve logs or equivalent evidence that the assertions ran against PostgreSQL; a skipped or ignored test is not a passing result.
- Every startup backfill MUST have a fast no-op guard for empty and already-completed data. The guard MUST run before any batch transaction so these paths do not repeat transaction or fsync work. Measure no-op wall time through the actual `sqlx::migrate!` runner with a persistent-state fixture.
- PostgreSQL `PROCEDURE` or `DO` blocks that execute `COMMIT` MUST NOT be used unless an actual `sqlx::migrate!` runner test against PostgreSQL proves that transaction control succeeds and provides evidence that its assertions ran without being skipped. SQLx may send the entire migration as one multi-statement query, causing routine-level `COMMIT` to fail with `invalid transaction termination`.
- `statement_timeout` limits each statement, not total migration duration. It is not evidence that the pending migration set fits the advisory-lock budget.
- A migration that reads, rewrites, or indexes an existing production-scale table MUST be rehearsed with production-scale row and byte volume. Measure total elapsed time, the slowest statement, and lock impact. `CREATE INDEX CONCURRENTLY` MUST NOT run through the ordinary sqlx migration transaction or advisory-lock path, including a `-- no-transaction` migration; move concurrent index work to an explicitly approved, separately operated online path.
- A merged or shipped migration is immutable. A migration recorded in a shared or otherwise non-disposable database is also immutable. NEVER edit, reorder, rename, or replace such a migration; an applied checksum mismatch MUST NOT be repaired by editing its SQL. Preserve the checksum and add a new, higher-version migration for every follow-up fix. Before merge or shipment, a corrected migration that exists only in disposable local fixtures MAY be rerun only by resetting those fixture databases.
- Destructive SQL and SQL against a production database MUST NEVER be executed without explicit approval.

## Release checklist

Before release, the migration author MUST:

- [ ] Confirm the total advisory-lock hold time for the entire pending migration set is shorter than the configured pool `statement_timeout`. With the default 30-second timeout, record a rehearsal result of 15 seconds or less and confirm the absolute duration is below 30 seconds.
- [ ] Confirm no migration SQL raises or disables `statement_timeout`, including a raise to 90 seconds; treat the 120-second startup probe only as a secondary ceiling.
- [ ] For a production-scale rewrite or backfill, confirm that an explicitly approved, separately operated online job is used by default.
- [ ] If startup execution is chosen, confirm every batch is row-count-limited, commits independently, resumes after interruption or retry, and skips completed work through a durable marker or data predicate. Reject a single filtered statement, including a 10-day `UPDATE`, as not bounded.
- [ ] For a migration that reads, rewrites, or indexes an existing production-scale table, rehearse with production-scale row and byte volume and record total elapsed time, the slowest statement, and observed lock impact.
- [ ] Run each startup backfill through the actual `sqlx::migrate!` runner against a real PostgreSQL DSN. Preserve logs or equivalent evidence that assertions for transaction control, interruption, retry, completed-batch skipping, and final state executed; skipped or ignored tests do not pass.
- [ ] Measure empty and already-completed no-op wall time through the actual `sqlx::migrate!` runner with a persistent-state fixture, and confirm these paths perform no unnecessary batch transactions or fsync-producing commits.
- [ ] Do not rely on `-- no-transaction` as proof of autonomous commits. If a PostgreSQL `PROCEDURE` or `DO` block executes `COMMIT`, preserve evidence from an unskipped actual-runner PostgreSQL test that proves it succeeds.
- [ ] For large index work, confirm that an ordinary index build fits the advisory-lock budget. Confirm that `CREATE INDEX CONCURRENTLY` is absent from the sqlx migration path and, when needed, runs only through an explicitly approved separate online operator path.
- [ ] Check `_sqlx_migrations` on each shared or non-disposable target database before rollout; do not infer applied state from application version alone.
- [ ] Confirm no merged, shipped, shared-database, or non-disposable-database migration changed checksum. Do not edit applied SQL to repair a checksum mismatch. For an unreleased migration used only in disposable local fixtures, reset those databases before rerunning corrected SQL.
- [ ] Obtain explicit approval before any destructive or production SQL execution.
