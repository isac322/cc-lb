# cc-lb True BDD — Platform / Audit v5.2

- Date: 2026-06-18
- Author: Writer 4 (Platform/Audit)
- Scope: 7 features / 94 scenarios (F13, F14, F15, F17-merged, F18-merged, F20, F24)
- Inputs
  - `~/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.md` (v5: 7 features / 89 scenarios)
  - `~/cc-lb-bdd/cc-lb-bdd-verify-C-scenario-quality.md` (§4 cross-file consistency rules)
  - `~/cc-lb-bdd/cc-lb-bdd-final-report-v5.md` (§3 sample table)
  - `~/cc-lb-bdd/bdd-research.md` (§A–§H, 18 authorities)
- v5 → v5.2: 89 + 3 additional multi-rule splits (F14.4, F15.11, F17.9) = **94**
- Personas: Alice (Operator), Bob (Developer/Plugin Author), Charlie (SRE), Dana (Auditor) — 4 personas only
- Principles
  - One scenario = one business rule (Cucumber.io §F.2)
  - Declarative Given/When/Then, domain vocabulary only (Cucumber.io §E)
  - "encrypted form / immediately detectable if tampered" (AEAD substitute)
  - "once written, no one can delete or modify it" (append-only substitute)
  - "secret information appears only as a redacted marker" (redaction substitute)
  - "a change on one replica is immediately propagated to all other replicas" (LISTEN/NOTIFY substitute)
  - "only one replica among many performs the task" (lease holder substitute)
  - "apply configuration without restart" (hot reload substitute)
  - "wait for in-flight calls to complete, accept no new ones" (drain substitute)
  - "applying a new certificate does not interrupt in-flight streams" (TLS reload substitute)
  - "same behavior when switching from one storage backend to another" (backend parity substitute)
  - "secrets follow a known positional pattern" (byte-prefix regex substitute, §E compliant)
  - "ready / not-ready" (readyz state vocabulary — readyz substitute)
  - "upstream" (upstream substitute — consistent with W1/W2/W3)

## v5.2 Change Summary (vs v5)

**1. Multi-rule splits added (3 cases, +5 scenarios)**
- F14.4 → F14.4a/b: applying a validated draft (functional) vs recording apply history (audit)
- F15.11 → F15.11a/b/c: external rejection (security) vs internal normal operation (functional) vs verifying separation (invariant)
- F17.9 → F17.9a/b/c: issuing a new identifier (functional) vs handing over old tasks (functional) vs preventing simultaneous use of the same identifier (invariant)

**2. Cross-file vocab consistency fix (1 case, FAIL → PASS)**
- Remaining English `upstream` occurrences throughout W4 → unified to `upstream` (W1/W2/W3 already use `upstream`)
- Applied locations: F13.10, F18 description, F18.12–F18.15 (F23-merged, 4 cases), F20.12, F20.16, Open Questions
- Total substitutions: **23** (`upstream` unified across W4)
- Code-level identifiers (e.g., `upstream_id` column name) are preserved — no such identifiers present in this file

**3. Final distribution (v5.2)**: F13(12) + F14(14) + F15(14) + F17(18) + F18(15) + F20(13) + F24(8) = **94 scenarios / 7 features**

## v5 Change Summary (vs v4) — no changes, record preserved

**1. Multi-rule splits (8 cases, +8 scenarios)**
- F13.11 → F13.11a/b: showing call lines (functional) vs quarterly report aggregation (metrics)
- F15.5 → F15.5a/b: rejecting a certificate (functional) vs reporting the rejection reason (metrics/audit)
- F17.6 → F17.6a/b: resolving split-brain (functional) vs recording self-check (audit + metrics)
- F17.12 (old) → F17.13/F17.14: same count and kind of audit lines (metrics) vs no secret plaintext (audit redaction)
- F18.6 → F18.6a/b: showing price history chronologically (functional) vs matching call costs (metrics)
- F18.7 → F18.7a/b: separating cache creation/reuse unit prices (functional) vs quarterly aggregation match (metrics)
- F18.8 → F18.8a/b: showing estimated cost (functional) vs recording estimation reason and separate query (audit + metrics)
- F20.4 → F20.4a/b: rejecting boot (functional) vs guaranteeing plaintext is never read (metrics)

**2. Jargon cleanup**
- `SQLite` / `Postgres` → "one storage backend" / "another storage backend"
- `replica` → `replica`
- `readyz` → `readyz`
- `access_token` / `refresh_token` / `API key` (audit/log context) → "token"

**3. Crypto-invariant vocabulary generalization (3 cases)**
- F20.1: "short window" / "plaintext portion" → unified to "known positional pattern" vocabulary
- F20.7: "known secret shape" → generalized to "secrets follow a known positional pattern"
- F20.11: response header secret masking unified to the same "known positional pattern" vocabulary

**4. F17-merged / F18-merged single-rule verification**
- New P3 scenarios in both groups re-verified against "functional behavior + metrics value + audit record" triple-combination criterion
- Combined cases included in the 8 splits above

**5. 4-persona constraint**
- Eve, who appeared in v4 for conflict comparison, → generalized to "a second operator" domain vocabulary (only Alice/Bob/Charlie/Dana named explicitly)

---

## Feature F13 — Auditor audits a quarter using immutable audit logs

During a quarterly audit, Dana directly verifies who changed what and when, and that the records have not been modified by anyone since. The same guarantees hold when exporting to an external audit system.

### Scenario: Auditor views one operator's changes in a quarter in chronological order
- Given operator Alice made multiple changes to team and limit settings during the past quarter
- And Dana is logged into the cc-lb console with auditor privileges
- When Dana opens the change log scoped to "operator Alice, Q1"
- Then all changes Alice made during that quarter appear on one screen in the order they occurred
- And each line shows what was changed, when it was changed, and which call unit it occurred in

### Scenario: Audit records contain no secret information on any line
- Given operator Alice registered new credentials
- And the entire registration process was captured in the audit log
- When Dana browses the full audit log for that quarter
- Then no line shows a plaintext token
- And the fields that would contain secret information appear only as redacted markers

### Scenario: An audit record that has been written cannot be deleted or modified by any operator
- Given operator Alice's model limit change from yesterday is recorded in the audit log
- And operator Alice holds the highest-privilege admin token
- When Alice attempts to delete that line or modify its content
- Then all attempts are rejected
- And each attempt itself is recorded as a new line in the audit log

### Scenario: Auditor narrows the view by time window and page
- Given one hundred thousand audit lines have accumulated over a quarter
- When Dana narrows the time window to "March 10, 14:00–16:00" and pages through the results
- Then only lines within that time window appear, one page at a time
- And no line appears twice across page turns, and no line is missing

### Scenario: Only records past the retention period are purged
- Given the audit log retention period is set to 1 year
- And lines from more than 1 year ago and lines from less than 1 year ago are both stored
- When the automatic purge job completes one cycle
- Then only lines older than 1 year are removed
- And lines within 1 year remain intact
- And the count and time range of purged lines are recorded as a single new audit line
- And when the retention period is updated to a new value, the new period takes effect from the next purge cycle

### Scenario: The start, end, and error of a single call are grouped under the same trace identifier
- Given Bob's call went through cc-lb toward Anthropic and ended in an error
- When Dana opens the audit log using that call's trace identifier
- Then the call start, intermediate steps, and error termination all appear as individual lines grouped under the same trace identifier
- And no lines from other calls are mixed in

### Scenario: Auditor exports audit logs to an external audit system
- Given an external audit system requests one month of audit logs
- When Dana exports that period using an auditor-only token
- Then every line of the exported file matches the cc-lb original line for line
- And the fields that would contain secret information contain only redacted markers

### Scenario: A single call's trace identifier appears identically in the audit log, operational log, and response
- Given a call is processed through cc-lb
- When Dana simultaneously views the audit log, operational log, and the response headers returned to the caller, all using that call's trace identifier
- Then the same trace identifier appears identically in all three places
- And the same trace identifier is never used for any other call

### Scenario: The exported audit file includes a tamper-proof integrity proof (new P3)
- Given Dana exports one month of audit logs to an external audit system
- When Dana receives that file along with an integrity proof generated by cc-lb
- Then the proof is linked line by line to the original cc-lb lines for the same period
- And if anyone modifies any single line of the external file, the proof immediately reveals it
- And the detection is recorded as a new line in cc-lb's audit log

### Scenario: Auditor views all related records for a single call in a cross-reference table (new P3)
- Given a single call passed through limit evaluation, cost calculation, cache decision, and upstream routing
- When Dana opens the cross-reference view using that call's trace identifier
- Then the limit, cost, cache, routing, and audit lines all appear on the same timeline grouped under the same trace identifier
- And no line is mixed with another call

### Scenario: Cost and limit violation appear together on the same call line (new P3, split 1/2)
- Given Bob's call was rejected for exceeding the limit
- And cost was incurred for the tokens used up to the point of rejection
- When Dana opens that call's line
- Then the rejection reason and the cost up to that point appear together on the same line

### Scenario: Cost and limit violations are aggregated together by call unit in the quarterly report (new P3, split 2/2)
- Given Bob's multiple calls were rejected during a quarter for exceeding the limit
- And cost was incurred up to the point of rejection for each call
- When Dana opens the quarterly report
- Then the cost and violation count of the rejected calls are aggregated together grouped by call unit

---

## Feature F14 — Operator safely manages configuration drafts, validation, apply, and history

Changes to live configuration are first saved as a draft and must pass validation before taking effect. Invalid configurations are blocked before apply, and the apply history can always be rolled back.

### Scenario: Saving a draft does not yet affect live behavior
- Given Alice creates a draft with a changed limit setting
- When Alice saves that draft
- Then the draft appears in the console with a "Draft" label
- And call processing behavior has not changed yet

### Scenario: Validating a draft reveals problems before apply
- Given Alice has a saved draft
- When Alice runs validation on that draft
- Then format errors, conflicts, and safety rule violations are all shown on one screen
- And the impact on which calls will be affected is shown alongside

### Scenario: An invalid configuration is rejected at the save step
- Given Alice enters a configuration with a format error
- When Alice attempts to save the draft
- Then the save is rejected
- And the location and nature of the error is indicated on a single line
- And when too many lines have changed to fit on one screen at once, only a portion is shown and that fact is indicated

### Scenario: Only a draft that passes validation is applied (split 1/2)
- Given a draft that has passed all validations
- When Alice clicks Apply for that draft
- Then the new configuration takes effect immediately starting from the next call

### Scenario: An applied configuration change is recorded as a single line in the apply history (split 2/2)
- Given Alice has completed applying a validated draft
- When Alice opens the apply history screen immediately after
- Then that apply appears as a single line in the apply history
- And who applied it, when, and which items were applied are all shown on the same line

### Scenario: Operator views apply history in chronological order
- Given configuration has been applied multiple times over the past two months
- When Alice opens the apply history screen
- Then who applied what configuration and when is shown in reverse chronological order
- And each line allows the full configuration at that point in time to be reopened

### Scenario: Operator rolls back to a previous version of the configuration
- Given Alice's configuration applied yesterday has a problem
- When Alice selects the previous version applied the day before yesterday and clicks Rollback
- Then that version is re-applied exactly as a new apply
- And the rollback itself is recorded as a new line in the apply history

### Scenario: Items applicable without restart are applied without restart
- Given there is a draft that changes items such as limits, routing, and the price catalog
- When Alice applies that draft
- Then cc-lb uses the new values starting from the next call without restarting
- And in-flight calls are not interrupted

### Scenario: Items requiring restart are indicated before apply
- Given there is a draft that changes items requiring restart, such as the listening port or authentication method
- When Alice runs validation
- Then the validation result includes a line reading "this item requires a restart"
- And Alice can only proceed with apply after seeing that warning

### Scenario: Operator downloads the applied configuration as a file
- Given the full current applied configuration exists
- When Alice clicks Export Configuration
- Then it is downloaded as a single file
- And the fields that would contain secret information contain only redacted markers

### Scenario: The bootstrap configuration is applied only once on first boot
- Given bootstrap.toml is placed in the installation directory
- And cc-lb read that file during first boot and left a single line in the apply history
- When cc-lb boots a second time
- Then bootstrap.toml is not processed again
- And the same bootstrap line does not appear twice in the apply history
- And a marker indicating it has already been processed once remains in the same directory so subsequent boots make the same decision

### Scenario: An unapplied draft is automatically cleared after the defined period (new P3)
- Given Alice saved a draft several days ago but never applied it
- When the defined retention period passes
- Then the draft is automatically cleared
- And the fact that it was cleared is recorded as a single line in the operational log
- And starting a new draft in the same slot does not mix old draft content into the new draft

### Scenario: Items requiring restart are clearly indicated per item (new P3)
- Given Alice changed the listening port and a limit setting together in one draft
- When Alice runs validation
- Then each item individually shows either "requires restart" or "immediately applicable"
- And when Apply is clicked, immediately applicable items use the new values starting from the next call, while items requiring restart are applied on the next boot

### Scenario: When a configuration file on disk changes, cc-lb detects the change (new P3)
- Given Charlie directly modifies an external configuration file
- When that file is written back to disk
- Then cc-lb detects the change and sends it to the validation step
- And if validation passes, the new values are used starting from the next call without restarting
- And if validation fails, the old values continue to be used and the failure reason is recorded as a single line in the operational log

---

## Feature F15 — Graceful shutdown and certificate renewal do not interrupt in-flight calls

When an operator or SRE brings down a replica or renews a certificate, in-flight calls are processed through to the end. New calls are directed to other replicas.

### Scenario: After a shutdown signal, no new calls are accepted
- Given a replica is in normal operation
- When Charlie sends a graceful shutdown signal to that replica
- Then new calls arriving after that point are immediately directed to other replicas
- And that replica's readyz changes to the "not-ready" state

### Scenario: In-flight calls continue to be processed through to the end during shutdown
- Given two calls on a replica are streaming their response bodies
- When Charlie sends a graceful shutdown signal to that replica
- Then the replica waits for in-flight calls to complete, accept no new ones
- And both calls receive all tokens through to the end normally

### Scenario: When the drain timeout expires, calls are forcibly terminated and reported
- Given a replica is in the middle of graceful shutdown
- And some calls do not complete within the defined drain timeout
- When that timeout expires
- Then the remaining calls are forcibly terminated
- And the count and reason for force-terminated calls are recorded as a single line in the operational log
- And the same count is visible in operational metrics

### Scenario: Applying a new certificate does not interrupt in-flight streams
- Given a replica is processing a call that streams the response token by token
- When Charlie applies a new TLS certificate
- Then the new certificate is used starting from the next new connection
- And already in-flight streams continue uninterrupted through to the last token

### Scenario: An invalid certificate is rejected before apply (split 1/2)
- Given Charlie has a new TLS certificate file
- And the certificate chain is invalid or the certificate has expired
- When Charlie attempts to apply that certificate
- Then the apply is rejected
- And the previous certificate continues to be used as-is

### Scenario: The rejection reason for an invalid certificate is indicated to the operator on a single line (split 2/2)
- Given Charlie attempted to apply a certificate with an invalid chain or an expired certificate and was rejected
- When Charlie views the rejection result
- Then what was wrong is indicated on a single line
- And the same reason is also recorded as a single line in the operational log

### Scenario: Operator is notified in advance as the certificate approaches expiry
- Given the time remaining until the applied certificate expires has dropped to or below the defined threshold in days
- When cc-lb performs its check at the moment that threshold is first reached
- Then an expiry warning is recorded as a single line in the operational log
- And the same warning is also visible in operational metrics

### Scenario: Temporary debug logging automatically turns off after a defined time
- Given the SRE wants to temporarily enable debug logging on a replica
- When Charlie sends a debug logging signal to that replica
- Then debug logging is enabled for a limited duration only
- And after that time elapses, it automatically reverts to the normal log level

### Scenario: The type of shutdown signal determines the shutdown semantics (new P3)
- Given a replica is in normal operation
- When Charlie compares the result of sending a "graceful shutdown" signal versus an "emergency shutdown" signal
- Then for graceful shutdown, in-flight calls are processed through to the end and readyz changes to "not-ready" first before the replica stops
- And for emergency shutdown, in-flight calls are immediately force-terminated and that fact is recorded as a single line in the operational log
- And for both shutdowns, the reason and signal type are recorded together in the shutdown marker

### Scenario: Debug logging can only be enabled via the designated operational signal (new P3)
- Given debug logging cannot be enabled via the console or a configuration file
- When Charlie attempts to enable debug logging only via an operational signal
- Then debug logging is enabled only on the replica that received that signal
- And the log level of other replicas remains unchanged
- And the fact that it was enabled is recorded as a single line in the operational log

### Scenario: External callers are denied access to the operations-only socket (new P3, split 1/3)
- Given cc-lb has a separate external call port and an operations-only socket
- When an external caller attempts to reach the operations-only socket
- Then the attempt is rejected
- And the rejection is recorded as a single line in the operational log

### Scenario: The operations-only socket normally receives admin calls from operators on the same machine (new P3, split 2/3)
- Given cc-lb has a separate external call port and an operations-only socket
- When Charlie on the same machine sends a graceful shutdown signal via the operations-only socket
- Then the signal is received normally
- And the graceful shutdown triggered by that signal starts immediately

### Scenario: The external call port and the operations-only socket operate independently during shutdown (new P3, split 3/3)
- Given cc-lb has a separate external call port and an operations-only socket and is in normal operation
- And Charlie has sent a graceful shutdown signal via the operations-only socket
- When in-flight calls on the external call port are being processed through to the end during shutdown
- Then the two listening endpoints operate in an independent state
- And the processing on the external call port does not affect the signal flow on the operations-only socket

### Scenario: The completion of a replica's shutdown is left as a marker for other replicas (new P3)
- Given replica A has completed graceful shutdown
- When replicas B and C view the same operational information
- Then the time and reason for A's shutdown completion are visible as a shutdown marker
- And when a new binary starts up in the same slot, the old marker and the new boot are not mixed on the same line

---

## Feature F17 — cc-lb operates across multiple replicas and multiple storage backends

Even with multiple replicas running simultaneously, a change by one operator is immediately propagated to all replicas, and the same task is performed by only one replica. Switching from one storage backend to another yields the same behavior from the operator's perspective.

### Scenario: A change on one replica is immediately propagated to other replicas
- Given two replicas A and B are running
- When Alice applies a new limit setting on A
- Then B's current limit display immediately changes to the new value
- And the next call arriving on any replica is evaluated with the same limit

### Scenario: Only one replica among many performs the task — credential warmup
- Given three replicas are running and credential warmup is scheduled for the same time
- When that time arrives
- Then only one of the three replicas performs that task
- And the other two replicas skip that task

### Scenario: When the replica performing the task disappears, another replica takes over
- Given replica A is performing a credential warmup task
- When replica A stops unexpectedly
- Then within the defined expiry time, another replica takes over that task
- And the same task is not performed twice in duplicate

### Scenario: All replicas see the same value at the same moment when a configuration change is applied
- Given Alice applies a routing policy once
- When calls arrive at all three replicas simultaneously immediately after that point
- Then all three replicas make the same routing decision

### Scenario: When two operators try to edit the same line simultaneously, only one succeeds
- Given Alice on console A and a second operator on console B are editing the same limit line simultaneously
- When both changes are saved at nearly the same time
- Then only one of the two changes succeeds
- And the losing side is shown a single line reading "another operator edited this first, please reload"

### Scenario: Even if two replicas simultaneously believe they hold the exclusive execution right, only one continues the task (new P3, split 1/2)
- Given during a brief interruption in the notification channel, two replicas simultaneously believe they hold the exclusive execution right for the same task
- When cc-lb performs its periodic self-check
- Then only one of the two replicas continues the task and the other immediately steps down
- And the same task is never executed all the way through twice

### Scenario: The result of the exclusive execution right self-check is recorded in the operational log and metrics (new P3, split 2/2)
- Given cc-lb has resolved a simultaneous exclusive execution right conflict between two replicas through a self-check
- When Charlie views the operational log and operational metrics at that point in time
- Then the self-check result is recorded as one line each in the operational log and operational metrics
- And which replica stepped down is shown alongside

### Scenario: Operator sees which replicas are alive on a single screen (new P3)
- Given three replicas are running
- When Charlie opens the replica status screen
- Then each replica's identifier, last heartbeat time, and the number of calls it is receiving appear in a single table
- And if a replica has not sent a heartbeat within the defined time, that row changes to "no response"

### Scenario: When a replica's identifier is corrupted, a new identifier is safely issued (new P3, split 1/3)
- Given the identifier file of a replica is corrupted and cannot be read
- When that replica reboots
- Then cc-lb safely obtains a new identifier
- And the fact that a new identifier was issued is recorded as a single line in the operational log

### Scenario: Tasks bound to the old identifier are handed over to another replica or expire (new P3, split 2/3)
- Given a replica's identifier was corrupted and it rebooted with a new identifier
- And tasks bound to the old identifier remain
- When the defined expiry time passes
- Then those tasks are handed over to another replica or expire
- And the same task is not performed all the way through twice

### Scenario: The old identifier and the new identifier are not used for the same call simultaneously (new P3, split 3/3)
- Given a replica's identifier was corrupted and it rebooted with a new identifier
- When new calls and tasks to be handed over are processed simultaneously after that point
- Then no call is bound to both the old and new identifiers simultaneously
- And in the operational log, the old identifier appears only in task handover and expiry flows, and the new identifier appears only in new call flows

### Scenario: The same operator scenario produces the same result across both storage backends (F22-merged)
- Given Alice performed team creation, key issuance, and limit setting in sequence on one storage backend
- When the same scenario is run in the same order on another storage backend
- Then both environments succeed at the same steps or are rejected at the same steps
- And the resulting team, key, and limits are semantically equivalent

### Scenario: The consistency scenarios for both storage backends all pass (F22-merged)
- Given a set of backend consistency scenarios is defined
- When that set is run in sequence on both storage backends
- Then all the same scenarios pass in both environments

### Scenario: Attempting to change storage backend type incorrectly results in a clear boot rejection (F22-merged)
- Given cc-lb was operating with one storage backend
- When an operator attempts to boot with a different storage backend against the same data directory
- Then the boot is rejected
- And the rejection message shows both the stored type and the attempted type

### Scenario: The same operator action leaves the same count and kind of audit lines across both storage backends (F22-merged, split 1/2)
- Given Alice performs the same operator action on one storage backend, then performs the same action on another storage backend
- When Dana views the audit logs for both environments with the same time window
- Then the same kind and count of audit lines remain in both environments

### Scenario: Audit logs contain no secret in plaintext in either storage backend (F22-merged, split 2/2)
- Given Alice performs operator actions involving credentials on both storage backends
- When Dana views the audit logs for both environments with the same time window
- Then no line in either environment shows secret information in plaintext
- And the fields that would contain secret information contain only redacted markers

### Scenario: A downgrade migration is rejected at the boot stage (F22-merged)
- Given the schema version written in the data directory is a newer version
- When an operator attempts to boot a cc-lb with a lower schema version
- Then the boot is rejected
- And which version was being downgraded from and to is shown alongside

### Scenario: Two operators editing the same line simultaneously produces the same decision across both storage backends (new P3)
- Given Alice on console A and a second operator on console B are editing the same limit line simultaneously
- When the same conflict scenario is triggered on both storage backends
- Then in both environments, only one of the two succeeds
- And the losing side receives the same reason message in the same form
- And the audit lines in both environments remain with the same count and same meaning

---

## Feature F18 — Cost and usage are reported accurately

Operators view the model price catalog, and per-call cost is calculated from that catalog. Even when an upstream is renamed, removed, or merged, prior usage is preserved as-is and cumulative totals are accurate. (F23 absorbed)

### Scenario: Operator views the current price for each model on a single screen
- Given the price catalog has been updated to the latest
- When Alice opens the cost catalog screen
- Then model name, input token unit price, output token unit price, and currency appear in a single table

### Scenario: Per-call cost is calculated accurately from the catalog prices
- Given a model's input and output token unit prices are recorded in the catalog
- And a call used 1,000 input tokens and 500 output tokens
- When Alice views that call's cost
- Then the reported cost matches input token unit price x 1,000 + output token unit price x 500

### Scenario: The quarterly cost report is accurately aggregated by team
- Given calls from multiple teams occurred during a quarter
- When Alice opens the quarterly cost report
- Then each team's cost total matches the sum of costs for calls belonging to that team

### Scenario: When prices change, the new price applies to new calls after that point
- Given an operator has applied a new unit price for a model
- When new calls are processed after that point
- Then those new calls' costs are calculated using the new unit price
- And calls that started immediately before that point are calculated using the old unit price

### Scenario: A call served by cache shows cost savings as a separate line item
- Given a call is processed with a cache hit
- When Alice opens that call's cost report
- Then the original cost and the saved amount appear separately
- And the same value is added to the team's cumulative savings total

### Scenario: Price change history is preserved in chronological order (split 1/2)
- Given a model's unit price changed twice during the past quarter
- When Alice opens that model's price history
- Then the time of each change and the unit price at that point appear in chronological order

### Scenario: Historical call cost calculations are consistent with the unit price at that time in the price history (split 2/2)
- Given a model's unit price changed twice during the past quarter
- And multiple calls were processed in between
- When Dana views each call's cost alongside the price history
- Then each call's cost calculation matches the unit price that was in effect at the time of that call, with no discrepancy down to a single token

### Scenario: The call that first creates a cache and the call that reads the cache again are priced separately (new P3, split 1/2)
- Given the catalog records separate unit prices for cache creation and cache reuse
- When one call first creates a cache and the next call reads the same cache again
- Then the first call's cost is calculated using the cache creation unit price and the next call's cost using the cache reuse unit price, separately

### Scenario: The aggregation of cache creation and reuse unit prices matches the quarterly report total with no discrepancy (new P3, split 2/2)
- Given both cache creation calls and cache reuse calls occurred during a quarter
- When Alice opens that model's quarterly report
- Then the sum of cache creation unit price totals and cache reuse unit price totals matches that model's total cost with no discrepancy down to a single token

### Scenario: When token count cannot be calculated precisely, it is shown with an estimated indicator (new P3, split 1/2)
- Given cc-lb does not precisely know how to count tokens for a model
- When that model's call cost is reported
- Then the cost appears with an estimated indicator

### Scenario: A call reported as estimated provides a reason record and a separate query (new P3, split 2/2)
- Given a call's cost was reported as estimated
- When Dana follows that call's trace identifier
- Then the same call's trace identifier has a line reading "estimated cost" as the reason
- And Alice can filter to view only calls processed as estimated separately

### Scenario: When an upstream is renamed, prior usage remains visible (F23-merged)
- Given upstream "anthropic-prod" has one month of usage accumulated
- When Alice renames that upstream to "anthropic-2026"
- Then the same one month of usage is visible under the new name with the same values
- And the total has no discrepancy down to a single token

### Scenario: When an upstream is deleted, its prior usage is preserved (F23-merged)
- Given upstream "legacy-x" that is no longer in use has usage remaining
- When Alice deletes "legacy-x"
- Then that upstream's historical usage is still visible in the quarterly report
- And in the cost report, that usage is aggregated with the same values

### Scenario: When two upstreams are merged into one, usage is accurately aggregated (F23-merged)
- Given upstream "anthropic-a" and upstream "anthropic-b" each have usage accumulated
- When Alice merges both into a new upstream "anthropic"
- Then the new upstream's usage equals the sum of the two old upstreams' usage
- And no single call is counted twice

### Scenario: The usage report shows upstream name changes alongside (F23-merged)
- Given an upstream's name changed once during a quarter
- When Alice opens the usage report for that quarter
- Then the old name and the new name appear grouped as the same upstream
- And when the name changed is shown alongside on a single line

---

## Feature F20 — Auditor verifies that stored secrets are not in plaintext and cannot be tampered

Dana directly verifies that all secret information (credentials, tokens) held by cc-lb is not visible in plaintext on disk, and that any tampering is immediately detectable.

### Scenario: All stored secrets do not remain in plaintext on disk
- Given tokens and credentials are stored in cc-lb
- When Dana reads any position and any width of cc-lb's storage files
- Then no plaintext of any secret is found
- And everything stored is in encrypted form, and nowhere does any secret appear in plaintext following a known positional pattern

### Scenario: Tampering is immediately detectable
- Given one stored credential exists
- And someone slightly modifies one byte of that file
- When cc-lb attempts to read and use that credential again
- Then the read is rejected
- And the tampering attempt is recorded as a single line in the audit log

### Scenario: If the master key is lost, secrets can never be recovered — intended behavior
- Given an operator has permanently lost the master key file
- When cc-lb is rebooted
- Then the stored secrets cannot be recovered in plaintext by any means
- And cc-lb clearly reports that fact and refuses to operate normally

### Scenario: cc-lb refuses to boot with an incorrect master key (split 1/2)
- Given an operator has configured an incorrect master key file
- When cc-lb boots
- Then the boot is rejected
- And the rejection reason is indicated on a single line

### Scenario: During a boot attempt with an incorrect master key, no secret is ever read in plaintext (split 2/2)
- Given an operator has configured an incorrect master key file
- When cc-lb attempts to boot
- Then at no stage of the boot process is any secret read in plaintext
- And no plaintext trace following a known positional pattern ever remains on disk or in a memory dump

### Scenario: Secrets stored before a key rotation can still be read after the rotation
- Given a master key rotation has occurred once
- And credentials stored before the rotation exist
- When cc-lb reads and uses those credentials again
- Then they are read normally under the new key scheme and used in calls
- And plaintext is never written to disk again

### Scenario: If the master key file has permissions that are too permissive, boot is rejected
- Given the master key file is placed with permissions that allow other users on the same machine to read it
- When cc-lb boots
- Then the boot is rejected
- And the rejection reason is indicated on a single line

### Scenario: Values following a known positional pattern are shown as redacted markers by type
- Given a value following a known positional pattern for secrets is about to be written into the log accidentally
- And that pattern includes model provider tokens, cloud access tokens, and authorization tokens
- When that log line is written
- Then regardless of type, it is changed to a redacted marker before being stored
- And no plaintext remains on any line

### Scenario: In abnormal termination messages, secret information appears only as a redacted marker
- Given an unexpected abnormal termination occurs during processing
- And a credential value is about to be included in the termination message accidentally
- When that message is recorded in the operational log and audit log
- Then only a redacted marker appears in the secret field
- And plaintext does not leak anywhere

### Scenario: Master key rotation does not interrupt in-flight calls (new P3)
- Given cc-lb is processing calls normally
- When Charlie rotates the master key to a new value
- Then not a single in-flight call is interrupted
- And calls arriving immediately after the rotation are also processed normally
- And secrets stored with the old key and secrets re-stored with the new key are both readable for a period
- And the completion of the rotation is recorded as a single line in the audit log

### Scenario: Even variable values in abnormal termination traces appear only as redacted markers (new P3)
- Given a credential variable value is about to be included in an abnormal termination trace accidentally
- When that trace is written to the operational log and audit log
- Then only a redacted marker appears in the variable value field
- And the secret is never leaked in plaintext

### Scenario: In response headers returned to callers, values following a known positional pattern appear only as redacted markers (new P3)
- Given cc-lb receives response headers from an upstream and passes them to the caller
- And a value following a known positional pattern for secrets is accidentally included in those headers
- When that response reaches the caller
- Then that header value is changed to a redacted marker
- And the same fact is recorded as a single line in the audit log

### Scenario: The same secret stored in different upstreams cannot be decrypted using the other upstream's context (new P3)
- Given the same plaintext secret is stored separately in upstream X and upstream Y
- When Dana attempts to read X's storage file in Y's environment
- Then the read is rejected
- And the encrypted form of the two upstreams is different from each other
- And tampering with one upstream does not affect the reading of the other upstream

---

## Feature F24 — The price catalog operates even during external dependency failures

The price catalog depends on an external price source (LiteLLM). Even if the external source is temporarily down or has been down for a long time, operators see it operating safely in the defined manner.

### Scenario: When the external price source is healthy, new prices are fetched
- Given the LiteLLM price source is healthy
- When the price catalog refresh cycle arrives
- Then cc-lb receives new prices from the external source
- And costs for the next call are calculated using the new prices

### Scenario: When the external price source is temporarily down, the last prices are used as-is
- Given the price catalog has been successfully refreshed once before
- And the next refresh attempt fails temporarily
- When new calls arrive after that point
- Then costs are calculated using the last successfully received prices
- And the operator sees a single line reading "price source temporarily unavailable"

### Scenario: When the external price source has been down for a long time, the operator is notified
- Given the price source has been unresponsive beyond the defined threshold time
- When cc-lb performs its check at the moment that threshold is first reached
- Then a "price source unresponsive for extended period" warning is recorded as a single line in the operational log
- And the same warning is also visible in operational metrics

### Scenario: After three consecutive price source failures, the disk-cached prices are used; if those are also unavailable, cost reporting is deferred
- Given the price source call has failed three times in a row
- And cc-lb has a copy of the last prices written to disk
- When new calls arrive after that point
- Then costs are calculated using the disk-cached prices
- And when there is no disk copy either, cost reporting is marked as deferred

### Scenario: If the price catalog is completely corrupted, that fact is safely reported
- Given the price catalog file is corrupted and cannot be read
- When cc-lb attempts to reload that catalog
- Then no call's cost is calculated as zero
- And cost reporting is marked as deferred and the operator is notified on a single line

### Scenario: Operator views the last update time of the price catalog
- Given the catalog was updated at a point in time
- When Alice opens the catalog status
- Then "last updated: [time]" and the update result appear on a single line
- And when the update is too old, a separate indicator is shown alongside

### Scenario: If the price catalog's provenance proof does not match, the new prices are not used (new P3)
- Given a new price catalog has been received from an external source
- And the catalog's provenance proof does not match the issuer cc-lb trusts
- When cc-lb attempts to apply that catalog
- Then the apply is rejected
- And the previously trusted prices continue to be used
- And the rejection reason and the expected issuer are recorded as a single line in the operational log

### Scenario: Calls for a model not in the catalog are reported using the defined fallback method (new P3)
- Given Bob's call uses a model name not recorded in the catalog
- When that call's cost is calculated
- Then it is reported using one of the defined fallback methods (closest model group rate or deferred indicator)
- And that fallback decision is recorded as a single line on that call's trace identifier
- And the operator can filter to view only calls processed via fallback separately

---

## Open Questions (W4 scope, v5.2)

1. **F17-merged 18 scenarios — exceeds Cucumber.io recommended 5–15 (worsening)** — The semantic units are clear with the split-brain self-check separation (F17.6a/b), backend audit separation (F17.13/F17.14), and identifier corruption 3-way split (F17.9a/b/c), but the scenario count within a single feature is 18. Consider splitting "replica consistency" and "storage backend parity" into separate features in the next round — priority escalated as of v5.2.
2. **F18-merged 15 scenarios — same problem** — Three groups are growing: cost calculation accuracy (F18.1–F18.7b), estimation (F18.8a/b), and upstream name preservation (F23-merged, 4 cases). Consider splitting F18 / F18-supplement in the next round.
3. **F20.1 / F20.7 generalized vocabulary consistency** — "secrets follow a known positional pattern" is stakeholder-readable vocabulary, but unit/property tests must own the responsibility of locking in the actual regex and byte-length invariants. That responsibility split should be explicitly stated in the test contract.
4. **F15.5a/b split recognizes rejection reason indication as an independent rule** — The same pattern (rejection + reason indication) exists in F14.3 and F24.7. Decide whether to apply the same split consistently in the next round.
5. **F17.6a/b split is a model for the "functional behavior vs audit/metrics record" pattern** — The same pattern exists in F13 (multiple audit combinations), F15.3 (force termination + metrics), and F17.7 (heartbeat), and should be applied consistently in the next round.
6. **(v5.2 new) Multi-listen-endpoint scenario split (F15.11a/b/c) is a candidate for the "security / functional / invariant 3-axis" pattern** — Locking external rejection (security), internal normal operation (functional), and separation behavior (invariant) in three scenarios is worth considering for consistent application to other dual-channel behaviors in F15 (streams during certificate renewal, processing during shutdown).
7. **(v5.2 new) Identifier corruption 3-way split (F17.9a/b/c) is a candidate for the "issuance / handover / concurrency-prevention 3-axis" pattern** — Locking identifier issuance (functional), old task handover (functional), and simultaneous use prevention (invariant) in three scenarios is worth considering for application to exclusive execution rights and replica synchronization scenarios in F17.

---

## Change Tracking Table (v5 → v5.2)

| v5 ID | v5.2 ID | Status | Notes |
|---|---|---|---|
| F14.4 | F14.4a, F14.4b | **split** | applying a validated draft (functional) vs recording apply history (audit) |
| F15.11 | F15.11a, F15.11b, F15.11c | **split (3-way)** | external rejection (security) vs internal normal operation (functional) vs verifying separation (invariant) |
| F17.9 | F17.9a, F17.9b, F17.9c | **split (3-way)** | issuing a new identifier (functional) vs handing over old tasks (functional) vs preventing simultaneous use of the same identifier (invariant) |
| F13.10 | F13.10 | vocab-fix | `upstream routing` — vocabulary unified across W4 |
| F18 description | F18 description | vocab-fix | `upstream name changed` — vocabulary unified across W4 |
| F18.12 (F23-merged) | F18.12 (F23-merged) | vocab-fix | scenario title + Given/When/Then 4 occurrences: `upstream` unified across W4 |
| F18.13 (F23-merged) | F18.13 (F23-merged) | vocab-fix | scenario title + Given/Then 3 occurrences: `upstream` unified across W4 |
| F18.14 (F23-merged) | F18.14 (F23-merged) | vocab-fix | scenario title + Given/When/Then 5 occurrences: `upstream` unified across W4 |
| F18.15 (F23-merged) | F18.15 (F23-merged) | vocab-fix | scenario title + Given/Then 3 occurrences: `upstream` unified across W4 |
| F20.12 | F20.12 | vocab-fix | 1 occurrence of `upstream` in Given — unified across W4 |
| F20.16 | F20.16 | vocab-fix | 5 occurrences in Given/Then/And: `upstream` unified across W4 |
| Open Questions | Open Questions | vocab-fix | `upstream name preservation` unified across W4, +2 new items (6, 7) |

**Final distribution (v5.2)**: F13(12) + F14(14) + F15(14) + F17(18) + F18(15) + F20(13) + F24(8) = **94 scenarios / 7 features**

**Split count (v5 → v5.2)**: 3 (F14.4, F15.11, F17.9)
**Cumulative split count (v4 → v5.2)**: 11 (v5: 8 + v5.2: 3)
**Cross-file vocab fixes**: 1 (`upstream` unified across W4, total 23 substitutions)
**Cumulative scenario count**: v4 81 → v5 89 (+8) → v5.2 94 (+5)

---

## Change Tracking Table (v4 → v5) — record preserved

| v4 ID | v5 ID | Status | Notes |
|---|---|---|---|
| F13.1 | F13.1 | keep | — |
| F13.2 | F13.2 | jargon-rewrite | access_token/refresh_token/API key → "token" |
| F13.3 | F13.3 | keep | "admin token" → "admin token" |
| F13.4 | F13.4 | keep | — |
| F13.5 | F13.5 | keep | — |
| F13.6 | F13.6 | keep | — |
| F13.7 | F13.7 | keep | — |
| F13.8 | F13.8 | keep | — |
| F13.9 | F13.9 | keep | — |
| F13.10 | F13.10 | keep | — |
| F13.11 | F13.11a, F13.11b | **split** | showing call lines (functional) vs quarterly aggregation (metrics) |
| F14.1–F14.13 | F14.1–F14.13 | keep | — |
| F15.1 | F15.1 | jargon-rewrite | replica → replica, readyz → readyz |
| F15.2 | F15.2 | jargon-rewrite | replica → replica |
| F15.3 | F15.3 | jargon-rewrite | replica → replica |
| F15.4 | F15.4 | jargon-rewrite | replica → replica |
| F15.5 | F15.5a, F15.5b | **split** | certificate rejection (functional) vs rejection reason indication (metrics) |
| F15.6 | F15.6 | keep | — |
| F15.7 | F15.7 | jargon-rewrite | replica → replica |
| F15.8 | F15.8 | jargon-rewrite | replica → replica, readyz → readyz |
| F15.9 | F15.9 | jargon-rewrite | replica → replica |
| F15.10 | F15.10 | keep | — |
| F15.11 | F15.11 | jargon-rewrite | replica → replica |
| F17.1 | F17.1 | jargon-rewrite | replica → replica |
| F17.2 | F17.2 | jargon-rewrite | replica → replica |
| F17.3 | F17.3 | jargon-rewrite | replica → replica |
| F17.4 | F17.4 | jargon-rewrite | replica → replica |
| F17.5 | F17.5 | persona | Eve → "a second operator" |
| F17.6 | F17.6a, F17.6b | **split** | resolving split-brain (functional) vs recording self-check (audit + metrics) |
| F17.7 | F17.7 | jargon-rewrite | replica → replica |
| F17.8 | F17.8 | jargon-rewrite | replica → replica |
| F17.9 | F17.9 | jargon-rewrite | SQLite/Postgres → storage backend |
| F17.10 | F17.10 | jargon-rewrite | SQLite/Postgres → storage backend |
| F17.11 | F17.11 | jargon-rewrite | SQLite/Postgres → storage backend |
| F17.12 | F17.12, F17.13 | **split** | audit line count and kind (metrics) vs no secret plaintext (audit redaction) |
| F17.13 | F17.14 | jargon-rewrite | renumber + downgrade rejection |
| F17.14 | F17.15 | jargon-rewrite + persona | SQLite/Postgres → storage backend, Eve → "a second operator" |
| — | F17.16 | (none new) | — |
| F18.1–F18.5 | F18.1–F18.5 | keep | — |
| F18.6 | F18.6a, F18.6b | **split** | price history chronological (functional) vs call cost match (metrics) |
| F18.7 | F18.7a, F18.7b | **split** | cache unit price separation (functional) vs quarterly aggregation match (metrics) |
| F18.8 | F18.8a, F18.8b | **split** | estimated indicator (functional) vs estimation reason record and separate query (audit + metrics) |
| F18.9–F18.12 | F18.12–F18.15 | renumber | F23-merged as-is |
| F20.1 | F20.1 | crypto-rewrite | unified "known positional pattern" vocabulary |
| F20.2–F20.3 | F20.2–F20.3 | keep | — |
| F20.4 | F20.4a, F20.4b | **split** | boot rejection (functional) vs plaintext never read (metrics) |
| F20.5–F20.6 | F20.6–F20.7 | keep+renumber | — |
| F20.7 | F20.8 | crypto-rewrite | "known secret shape" → "secrets follow a known positional pattern" |
| F20.8 | F20.9 | keep+renumber | — |
| F20.9 | F20.10 | keep+renumber | — |
| F20.10 | F20.11 | keep+renumber | — |
| F20.11 | F20.12 | crypto-rewrite | response headers — unified to "secrets follow a known positional pattern" |
| F20.12 | F20.13 | keep+renumber | — |
| F24.1–F24.8 | F24.1–F24.8 | keep | — |

(End of v5.2 — 7 features, 94 scenarios)
