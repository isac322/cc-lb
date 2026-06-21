# cc-lb True BDD — v5.2 Writer 1 (Team / Traffic / Dashboard / Cache / Health)

## Scope
- F1, F2, F3, F4, F6, F19, F26 (no merge impact)
- Date: 2026-06-18
- Author: v5.2 Writer 1
- Personas: Alice (operator), Bob (developer), Charlie (SRE), Dana (auditor)
- Source: `cc-lb-true-bdd-1-team-traffic-v5.md` (87 scenarios) + `cc-lb-bdd-final-report-v5.md` §3 PARTIAL reason table
- Vocabulary rule: No HTTP/DB/Rust/byte/env-var exposure. Domain language only.
- Changes from v5 (v5.2 changelog):
  - **6 multi-rule scenarios split (6 → 17, +11):** PARTIAL scenarios remaining from v5 that contained multiple rules were split to enforce the one scenario = one rule principle.
    - F3.5 → F3.5a (unauthorized model rejected) + F3.5b (rejection message format) + F3.5c (audit-bundle report added)
    - F4.1 → F4.1a (yesterday's top teams by cost) + F4.1b (call-to-rejection ratio) + F4.1c (drill-down navigation)
    - F4.11 → F4.11a ("zero calls" vs "not yet known" table distinction) + F4.11b (user comprehension tooltip) + F4.11c (time-period chart)
    - F6.10 → F6.10a (per-path body size limit applied) + F6.10b (other path isolation)
    - F19.3 → F19.3a (cache-hit cost savings) + F19.3b (hypothetical cost label) + F19.3c (time-period comparison)
    - F26.2 → F26.2a ("not ready" maintained when unrecoverable) + F26.2b (liveness signal separated) + F26.2c (auto-reconnect attempt indicator)
  - **v5 changelog retained:** The 11 → 22 items split in the v4 → v5 phase and the new F2.14 are unchanged.
  - **Retained:** IDs that were not split remain as-is. The 11 IDs dropped in v3 are not backfilled.
- Total scenarios: 87 → **98**
- Feature count: 7 (unchanged)

---

## Feature F1: Operator onboards a new team onto cc-lb

Alice accepts a new team into cc-lb and brings that team to a state where it can make its first call. This feature covers registration, identifier validation, concurrent-edit protection, inactive/active recovery, and audit preservation after deletion.

### Scenario F1.1a: New team registration finishes with active status and first key on the same screen in one step

```gherkin
Given Alice is logged in as an operator
And the team name is not yet registered in cc-lb
When Alice enters the new team name and usage limit and submits the registration
Then the team is created in active status
And the team's first client key is shown exactly once on the same screen
```

### Scenario F1.1b: New team registration leaves an audit record of who created it and when

```gherkin
Given Alice is logged in as an operator
And the team name is not yet registered in cc-lb
When Alice enters the new team name and usage limit and submits the registration
Then the audit log records who created which team, when, and that the first key was issued
```

### Scenario F1.2: The issued key is shown exactly once immediately after registration

```gherkin
Given Alice has registered a new team and is viewing the first key issuance screen
When Alice navigates away from that screen and then opens the same team again
Then the full secret key is not shown again
And only the last few characters and the issuance time are shown instead
And if Alice tries to view the secret again, she is informed that a new key must be issued
```

### Scenario F1.3: If another operator changed the same team in the meantime, the second save is stopped

```gherkin
Given Alice and another operator have both opened the limit-editing screen for the same team at the same time
And the other operator finished saving first
When Alice tries to save her own changes
Then Alice's save is gracefully rejected with a message that someone else changed it in the meantime
And Alice can only retry her changes after viewing the latest values again
```

### Scenario F1.4: Calls from a deactivated team are not accepted

```gherkin
Given Alice has set a team to inactive status
When Bob, who holds that team's key, calls Claude through cc-lb
Then that call is gracefully rejected
And Bob receives only the message that the team is temporarily paused
And the rejection is visible in the usage report broken down by rejection reason type
```

### Scenario F1.5: Calls that exceed the model set allowed for the team are rejected

```gherkin
Given Alice has allowed only a specific model bundle for a team
When Bob on that team calls with a model outside the allowed bundle
Then that call is gracefully rejected
And Bob receives only the message that the model cannot be used by this team
And the same fact appears in Alice's rejection-reason report by type
```

### Scenario F1.7: Deleting a team does not erase the audit trail of what that team did

```gherkin
Given a team already has multiple call records and key issuance/revocation history
When Alice permanently deletes that team
Then the team's active/inactive record disappears
And the audit events left by that team remain queryable as "deleted team X"
And Dana can review the same events in chronological order during a quarterly audit
```

### Scenario F1.8: Registration is rejected if the identifier contains forbidden characters or a reserved prefix

```gherkin
Given Alice is trying to register a new team
When Alice enters a name containing invisible characters or a system-reserved prefix
Then the registration is gracefully rejected
And Alice receives only the message that this name is system-reserved or contains invisible characters
And the rejection does not affect the usage report
```

### Scenario F1.10: Restoring a deactivated team to active status lets the same secrets be accepted again immediately

```gherkin
Given Alice has set a team to inactive status
When Alice restores that team to active status
Then the team's keys are accepted again with the same secrets
And Bob's code can send calls again without receiving a new key or taking any extra steps
And who changed the status and when — both the deactivation and reactivation — appear as a pair in the audit log
```

### Scenario F1.11: Calls in progress just before deactivation are completed; only new calls are rejected

```gherkin
Given multiple calls are in progress using that team's key
When Alice sets the team to inactive status
Then calls already in progress are completed with normal responses
And new calls after that point are gracefully rejected
And the two groups appear separated as "before deactivation / after deactivation" in the rejection-reason report by type
```

---

## Feature F2: Operator issues, displays, and revokes client keys

Alice creates keys for a team, shows them only as much as necessary, and revokes them when a risk signal appears. This feature covers one-time-display of keys, rotation, revocation, expiry, owner display, state transitions, per-holder limits, access auditing, and a notice when rotation is unsupported.

### Scenario F2.1: When a new key is issued, the secret is shown exactly once

```gherkin
Given Alice has opened the key issuance screen for a team
When Alice issues a new key
Then the full secret key is shown exactly once right there
And reopening the same screen no longer shows that secret
And only the last few characters and the holder name remain
```

### Scenario F2.2: When a key is rotated, the new key and the old key have a short overlap period

```gherkin
Given a team already has one active key
When Alice initiates rotation of that key
Then a new key is issued and shown exactly once right there
And the old key can be used alongside it for a short preset period
And when that period ends, the old key is no longer accepted automatically
```

### Scenario F2.3: A revoked key is rejected starting from the very next call

```gherkin
Given Bob is making normal calls with his key
When Alice revokes that key
Then all calls after that point are gracefully rejected
And Bob receives only the message that the key is no longer accepted
And the revocation — including who revoked it and when — is recorded in the audit log
```

### Scenario F2.4a: A key with a set expiry time is cut off at that time

```gherkin
Given Alice has set an expiry time on a key
When that expiry time passes
Then calls coming in on that key are gracefully rejected
And "expired key" is visible by type in the rejection-reason report
```

### Scenario F2.4b: An upcoming-expiry notification reaches the operator in advance

```gherkin
Given Alice has set an expiry time on a key
When the expiry time is approaching
Then Alice receives a notification at a preset time that "this key is about to expire"
And who received that notification and when is recorded in the audit log
```

### Scenario F2.5: The key list screen never shows the secret alongside the key

```gherkin
Given Alice has multiple keys for a team
When Alice opens the key list for that team
Then each key shows only the holder name, issuance time, and last-used time
And the secret is not visible on any row
And no search or sort action exposes the secret
```

### Scenario F2.6: The operator can later edit the key holder name and note

```gherkin
Given Alice initially entered the holder name incorrectly for an issued key
When Alice edits the holder name and note on that key
Then the new name is reflected in the list immediately
And the old name and new name — along with who changed them and when — are recorded in the audit log
And the secret for that key is not shown again
```

### Scenario F2.8a: The usage report breaks down call count and rejection reasons per key holder

```gherkin
Given a team has multiple keys with different holders
When the usage report for that team is viewed
Then call count and rejection reasons are shown separately for each holder
And past usage from revoked keys is still shown under that holder's name
```

### Scenario F2.8b: Editing a holder name unifies past usage under the new name

```gherkin
Given past usage has accumulated under a holder's key
When Alice edits that holder's name
Then the holder's past usage and new usage are shown unified under the single new name
And the old name no longer appears separately on the same report screen
```

### Scenario F2.10: There is a set limit on how many active keys one holder can have at the same time

```gherkin
Given Alice has set a limit on "the number of active keys one person can hold at the same time" for a team
When a new key is about to be issued while that holder already has that many keys
Then the issuance is gracefully rejected
And Alice receives only the message that this holder has reached the active key limit
And a new key can only be issued again after one old key is revoked
```

### Scenario F2.11: Viewing the last few characters of a key is itself subject to auditing

```gherkin
Given Alice views the last few characters of a key
When Dana reviews the same record during a quarterly audit
Then the audit log contains a separate entry for who viewed the last few characters of which key and when
And the full secret does not appear anywhere in that record
And Dana can reconstruct the viewing history in chronological order from that record alone
```

### Scenario F2.12a: Whether a key holder is a person or a machine is clearly visible at a glance on the list screen

```gherkin
Given a team has a mix of person-holder keys and machine-holder keys
When Alice opens the key list for that team
Then each row clearly shows whether the holder is a "person" or a "machine"
And Alice can immediately open a filtered view showing only one type
```

### Scenario F2.12b: The usage report also shows person and machine totals separately

```gherkin
Given a team has a mix of person-holder keys and machine-holder keys
When Alice views the usage report for that team
Then the person-holder total and machine-holder total are shown separately in the same report
And the ratio of the two totals is shown together on the time-period trend
```

### Scenario F2.13a: A key's state is managed in the order active → suspended → revoked

```gherkin
Given Alice has a key in active status
When Alice changes that key to suspended for a while and then permanently revokes it
Then the key state is clearly visible as "active → suspended → revoked"
And while suspended, the same rejection as revocation occurs, but it can be restored to active
```

### Scenario F2.13b: Who changed the key state and when is recorded in the audit log for each transition

```gherkin
Given Alice has changed a key through active → suspended → revoked
When Dana reviews the trail of that key during a quarterly audit
Then all three state transitions appear in the audit log with who changed them and when
And Dana can reconstruct the flow of state transitions in chronological order from that record alone
```

### Scenario F2.14: Attempting key rotation on a storage backend that does not support rotation is gracefully rejected (new in v5)

```gherkin
Given the cc-lb storage backend type does not support key rotation
And Alice tries to rotate a team's key from the operations screen
When Alice's rotation request reaches cc-lb
Then the rotation is gracefully rejected
And Alice receives the message "rotation is not supported on the current storage backend type; use a different flow" delivered as a Claude-format error envelope
And the same rejection — including who attempted it and when — is recorded in the audit log
```

---

## Feature F3: Developer calls Claude through cc-lb

Bob calls cc-lb as if it were a standard Anthropic service. This feature covers normal calls, streaming, invalid keys, file flow, error envelopes for unknown paths, response size caps, enforcement of the registered tunnel for external hosts, forwarding-header cleanup, and authentication-disabled mode.

### Scenario F3.1: Calling a normal model with a valid key delivers the response unchanged

```gherkin
Given Bob holds an active key for his team
When Bob makes one call using a model allowed for his team
Then Claude's response is delivered to Bob unchanged
And that call is counted as one success in the usage report
And from Bob's code's perspective, there is no visible sign that cc-lb was in the middle
```

### Scenario F3.2: A streaming response flows through to the end without interruption

```gherkin
Given Bob sends a call with streaming enabled
When Claude is sending responses piece by piece with cc-lb in the middle
Then Bob receives the pieces in the exact order they were received
And the end of the last piece is clearly signaled
And that call is counted as one streaming success in the usage report
```

### Scenario F3.3: A call sent with an unknown key is gracefully rejected

```gherkin
Given a call comes in with a secret that has never been issued as any key
When cc-lb tries to identify the owner of that call
Then the call is gracefully rejected
And the rejection message is delivered as an error envelope in the format Anthropic would use
And that rejection is added to the "by rejection reason type" section of the usage report
```

### Scenario F3.4: A call sent with a key from an inactive team is rejected

```gherkin
Given Alice has set a team to inactive status
When Bob sends a call using that team's key
Then the call is gracefully rejected
And Bob receives a Claude-format error envelope
And the same fact is added to the rejection-reason-by-type report
```

### Scenario F3.5a: Calling a model not allowed for the team results in that call being rejected

```gherkin
Given Bob's team allows only a specific model bundle
When Bob calls with a model outside the allowed bundle
Then that call is gracefully rejected
```

### Scenario F3.5b: The rejection message for a disallowed model is delivered as a Claude-format error envelope

```gherkin
Given Bob has called with a model not allowed for his team and is being rejected
When that rejection message arrives for Bob
Then Bob receives only the message "this model cannot be used by this team" delivered as a Claude-format error envelope
And the same envelope follows the same formatting rules as error envelopes for other rejection reasons
```

### Scenario F3.5c: The disallowed-model rejection is added to both the usage report and the audit bundle

```gherkin
Given Bob called with a model outside the allowed bundle and was rejected
When Alice views the rejection-reason-by-type report
Then that rejection appears counted once under the "disallowed model" type
And the same fact is preserved in the audit bundle so Dana can review it during a quarterly audit
```

### Scenario F3.8: If a sudden problem occurs inside cc-lb during a response, it is closed with a Claude-format error

```gherkin
Given Bob's streaming call is flowing normally up to the midpoint
When a sudden problem occurs inside cc-lb
Then the stream is closed with a Claude-format error envelope
And Bob receives the reason "the response was interrupted midway" in the same format
And "interrupted midway" is added by type to the usage report
```

### Scenario F3.9: Bob completes the flow of uploading, downloading, and deleting a file through cc-lb

```gherkin
Given Bob's team has access to the file feature
When Bob uploads a file through cc-lb, retrieves the same file, and finally deletes it
Then each step responds in the exact format Anthropic would use
And the same file identifier is used consistently across all three steps
And upload, download, and delete are each added by type to the usage report
```

### Scenario F3.11a: A call sent to an unknown path or with an unknown operation returns a Claude-format error envelope

```gherkin
Given Bob accidentally calls with a non-existent path or an invalid operation
When cc-lb receives that call
Then Bob receives a Claude-format error envelope
And the envelope contains the message "this path/operation is not recognized"
```

### Scenario F3.11b: The rejection for an unknown path/operation is added to the usage report's rejection-reason-by-type section

```gherkin
Given Bob accidentally calls with a non-existent path or an invalid operation
When cc-lb rejects that call
Then the same rejection is added once to the "by rejection reason type" section of the usage report
And Alice can view the trend of rejections of the same type by time period
```

### Scenario F3.12: When rejected due to a limit, "when to retry" is included in the same response

```gherkin
Given Bob's team is temporarily being rejected for exceeding the per-minute limit
When Bob's code sends the same call one more time at that point
Then that call is also gracefully rejected
And the response includes "approximately when to retry" in the units set by the operator
And Bob's code can naturally retry after waiting until that time
```

### Scenario F3.13: If the response body exceeds a preset cap, it is gracefully cut off

```gherkin
Given Bob sends a call likely to produce a very large response
When cc-lb starts delivering Claude's response to Bob
Then the moment the response body reaches the preset cap, it is gracefully cut off
And Bob receives the message that the response was cut off midway because it reached the cap
And the same fact is added once under the "cap exceeded" type to the usage report
```

### Scenario F3.14: Attempts to go directly to an external host not in the registered tunnel are blocked

```gherkin
Given Charlie has set that only the registered Anthropic tunnel in cc-lb can exit to the outside
When a call tries to exit directly to an external host without going through the registered tunnel
Then that call is gracefully rejected
And the caller receives only the message "only the registered tunnel may be used to exit"
And the same fact appears separately in the rejection-reason-by-type report
```

### Scenario F3.15: Hop-by-hop forwarding headers are cleaned up at the response boundary

```gherkin
Given Claude sends a response for a call
When cc-lb delivers that response to Bob
Then the "hop-by-hop" forwarding headers are cleaned up at that boundary
And only meaningful response information remains when delivered to Bob
And the same cleanup happens equally to both normal responses and error envelopes
```

### Scenario F3.16: The fact that authentication is temporarily disabled in an isolated test environment is clearly marked

```gherkin
Given Charlie has temporarily disabled authentication in cc-lb in an isolated test environment
When Bob calls without a key in that environment
Then cc-lb accepts that call but a marker that "authentication is currently disabled" is recorded in the audit log alongside it
And the same marker is clearly visible as a banner at the top of the operations screen
And the same setting cannot take effect in the production environment
```

---

## Feature F4: Operator views the daily usage and cost dashboard

Alice views the previous day's top teams by usage, costs, distance from limits, and real-time trends — all on one screen every day. This feature covers daily/weekly/monthly views, period/team/holder/model filters, real-time stream, log filters, distinction between zero values and unmeasured values, and per-step latency detail views.

### Scenario F4.1a: Yesterday's top teams by usage are visible at a glance sorted by cost

```gherkin
Given Alice opens the dashboard every morning
When the teams that had calls yesterday are organized into a table
Then the top teams are shown at the top in descending order by cost
And the same order is maintained even after the screen is refreshed
```

### Scenario F4.1b: The same table also shows the call count to rejection ratio

```gherkin
Given Alice is viewing the table of yesterday's top teams by usage
When each row of the same table is rendered
Then each team's call count and rejection ratio are shown together on the same row
And the units between call count and rejection ratio are clearly distinguished so there is no confusion
```

### Scenario F4.1c: Clicking a team row navigates to that team's detail view

```gherkin
Given Alice is viewing the table of yesterday's top teams by usage
When Alice clicks a team's row
Then it navigates directly to that team's detail view screen
And from that screen, Alice can navigate directly to another team
```

### Scenario F4.2: Filter by selecting from period, team, key holder, or model

```gherkin
Given Alice is viewing a table on the dashboard
When Alice selects and filters by period, team, key holder, or model
Then the entire table is reorganized to match those conditions
And cost, call count, and rejection ratio are recalculated with the same conditions
And the same conditions remain in the URL bar so reopening it shows the same screen
```

### Scenario F4.3: When monthly cost approaches the set limit, it is shown on the same screen in advance

```gherkin
Given a team has a monthly cost limit set
When accumulated cost exceeds a certain percentage of the limit
Then a notice that "this team is approaching its limit" appears at the top of the dashboard
And Alice can click that notice to go directly to the limit editing screen
And the same fact is shown before a rejection occurs
```

### Scenario F4.4a: Model-by-model call share and cost share are visible at a glance on the same screen

```gherkin
Given Alice is viewing the dashboard for a team
When that team uses a mix of models
Then the call share and cost share for each model are shown together
And the most expensive model appears at the top of the cost share
```

### Scenario F4.4b: Clicking a model in the model share table navigates to the time-period trend for that model

```gherkin
Given Alice is viewing the model share table
When Alice clicks a model row
Then it navigates to the time-period call trend screen for that model
And from that screen, Alice can navigate directly to another model
```

### Scenario F4.5: The time-period usage trend continues without gaps

```gherkin
Given Alice is viewing a team's daily trend
When calls came in by time period
Then the time-period bars continue without gaps
And time periods with zero calls are clearly marked as "0"
And clicking a bar navigates to the detailed log for that time period
```

### Scenario F4.6: Rejection reasons are visible at a glance by type

```gherkin
Given a team had multiple types of rejections yesterday
When Alice opens the rejection view for that team
Then rejection reasons are grouped and shown by type
And the ratio and time-period trend for each type are shown together
And the largest type appears at the top
```

### Scenario F4.7: The report screen can be downloaded in table format

```gherkin
Given Alice has a filtered report screen
When Alice clicks "Download"
Then the same content visible on the screen is produced as a table-format file
And no secrets or full key values appear anywhere in that file
And Dana can open the same file again during a quarterly audit
```

### Scenario F4.8: The fact that the real-time stream has been interrupted is clearly visible on the same screen

```gherkin
Given Alice has the real-time stream enabled and is viewing the dashboard
When the real-time stream is interrupted for some reason
Then a marker that "the real-time stream has been interrupted" is clearly visible on the screen
And the accumulated report numbers remain accurate during that time
And when the real-time stream reconnects, the same marker disappears
```

### Scenario F4.10: On the log page, filter by selecting type, team, or holder

```gherkin
Given Alice is viewing the cc-lb log page
When Alice selects and filters by type, team, or holder
Then the same page is reorganized to match those conditions
And no secrets or full key values appear on any row
And the same conditions remain in the URL bar so reopening it shows the same screen
```

### Scenario F4.11a: "Zero calls" and "not yet known" are clearly distinguished within the same table

```gherkin
Given Alice is viewing a team's usage report
When some time periods had zero calls and other time periods have not yet finished measuring
Then "zero calls" and "not yet known" are clearly distinguished within the same table
And even if the two markers appear on the same row, they are rendered with different marker shapes
```

### Scenario F4.11b: The meaning of the two markers is explained together as a user-comprehension tooltip

```gherkin
Given Alice is viewing a row in a team's usage report table where "zero calls" and "not yet known" are mixed
When Alice tries to learn the meaning of the two markers
Then a tooltip explaining the difference between the two markers is shown within the same table
And Alice can recognize both at a glance without confusion
```

### Scenario F4.11c: The same distinction is also shown on the time-period trend chart

```gherkin
Given Alice is viewing a team's time-period usage trend chart
When some time periods had zero calls and other time periods have not yet finished measuring
Then the "zero calls" time periods and "not yet known" time periods appear with different markers on the same trend chart
And the meaning of the two markers is explained in the legend beside the chart
```

### Scenario F4.12: Per-step time spent for a single call is viewed in detail

```gherkin
Given Alice is looking at one slow call from a team in detail
When Alice opens the per-step detail view for that call
Then the time spent at each stage — such as "key recognition, limit check, external call, response delivery" — is shown separately
And the stage that took the longest is highlighted on the same screen
And Alice can immediately identify which stage was slow from that highlight
```

---

## Feature F6: Per-team budget, model ACL, and rate limits are enforced

The limits Alice has set actually work inside cc-lb. This feature covers per-minute limits, daily limits, monthly cost limits, model bundles, external host allowlists, per-path body size, concurrency limits, limit application scope, per-key overrides, recovery, and inter-team isolation.

### Scenario F6.1: Exceeding the per-minute limit causes the team's next calls to be temporarily rejected

```gherkin
Given a team has a set per-minute limit
When the team's keys exceed the limit within a short time
Then the next call after exceeding the limit is gracefully rejected
And Bob receives only the message "please retry shortly"
And the same fact is added to the rejection-reason-by-type report
```

### Scenario F6.2a: When the daily limit is exhausted, the remaining calls that day are rejected and the limit resets at the same time the next day

```gherkin
Given a team has a set daily limit
When the team uses up the daily limit
Then the team's calls are gracefully rejected for the rest of that day
And the limit resets automatically at the same time the next day
```

### Scenario F6.2b: The daily-limit rejection notice includes when the limit will reset

```gherkin
Given a team is being rejected because it has exhausted the daily limit
When Bob's call hits that rejection
Then the response notice includes "when the limit will reset" in the units set by the operator
And Bob's code can naturally retry after waiting until that time
```

### Scenario F6.3: When the monthly cost limit is near, new calls are rejected in advance

```gherkin
Given a team has a set monthly cost limit
When accumulated cost is nearly at the limit
Then the team's new calls are gracefully rejected
And Bob receives only the message "this team's monthly limit is nearly exhausted"
And the same fact is visible on the dashboard before a rejection occurs
```

### Scenario F6.4: Calling outside the allowed model bundle is rejected

```gherkin
Given Alice has allowed only a specific model bundle for a team
When Bob on that team calls with a model outside the bundle
Then that call is gracefully rejected
And "disallowed model" is added to the rejection-reason-by-type report
And the same rejection continues until Alice adds that model to the bundle
```

### Scenario F6.5: A new limit takes effect immediately for calls right after it is edited

```gherkin
Given Alice has increased the per-minute limit for a team
When Bob sends a call immediately after that change is saved
Then the call is accepted under the new limit
And the old limit is not applied even briefly alongside it
And the change — including who made it and when — is recorded in the audit log
```

### Scenario F6.8: Limit violations are shown separately in the rejection-reason-by-type report

```gherkin
Given multiple types of rejections occurred for a team
When Alice views the rejection-reason-by-type report
Then per-minute violations, daily violations, monthly violations, and disallowed models are shown separately by type
And the ratio and time-period trend for each type are also shown together
And the type with the most violations appears at the top
```

### Scenario F6.9: cc-lb does not exit to hosts outside the external host allowlist

```gherkin
Given Charlie has set the external host allowlist
When a call tries to exit to an external host outside the allowlist
Then cc-lb blocks that external call
And Bob receives only the message "this external host cannot be exited to"
And the same fact is recorded in the audit log together with the external host name
```

### Scenario F6.10a: A per-path body size limit is applied to calls on that path

```gherkin
Given Alice has set a smaller body size limit for a specific path
When Bob sends a call on that path with a body exceeding the limit
Then that call is gracefully rejected
And Bob receives only the message "the body size limit for this path was exceeded"
```

### Scenario F6.10b: Changing the body size limit for one path does not affect the body size limit for other paths

```gherkin
Given Alice has set a smaller body size limit only for a specific path
When Bob sends a body of the same size on a different path
Then that call's body size limit is maintained without being affected
And the acceptance on other paths is shown as separate from the limit change on the one path
```

### Scenario F6.12: A team temporarily rejected for a limit violation recovers when time passes

```gherkin
Given a team is temporarily being rejected for exceeding the per-minute limit
When the preset recovery time passes
Then that team's calls start being accepted again
And Bob's code can send calls again with the same key without any extra steps
And the recovery event is shown on the rejection-reason-by-type report trend
```

### Scenario F6.13: A team that has exhausted its daily limit is automatically accepted again at the same time the next day

```gherkin
Given a team is being rejected because it has exhausted its daily limit
When the same time the next day passes
Then the daily limit is automatically reset
And Bob's code can send calls again without any extra steps
And the recovery event is shown on the rejection-reason-by-type report trend
```

### Scenario F6.14a: One team's per-minute limit violation does not affect another team's call acceptance

```gherkin
Given two teams each have their own separate limits within the same cc-lb
When one team is being rejected for exceeding its per-minute limit
Then the other team's calls are accepted without being affected
And both teams' limit headroom and rejection trends are shown separately on screen
```

### Scenario F6.14b: One team's burst does not delay the other team's recovery time

```gherkin
Given two teams each have their own separate limits within the same cc-lb
When one team greatly exceeds its per-minute limit
Then the other team's recovery time is maintained exactly as preset
And both teams' recovery trends are shown separately in the rejection-reason-by-type report
```

### Scenario F6.15: A separate limit can be set on the number of concurrently in-progress calls

```gherkin
Given a team has a set limit on "the number of concurrently in-progress calls"
When a new call comes in while the team's in-progress calls have reached that number
Then the new call is gracefully rejected
And Bob receives only the message "the concurrent call limit has been reached"
And once one in-progress call finishes, the next call is accepted immediately
```

### Scenario F6.16: The application scope changes depending on whether the limit is set to "team", "holder", or "key"

```gherkin
Given Alice can choose the application scope of a limit from "team", "holder", or "key"
When Alice sets that limit to the holder scope
Then when one holder exceeds the limit, only that holder's keys are rejected
And other holders on the same team send calls without being affected
And which scope the limit is applied to is clearly visible on the same screen
```

### Scenario F6.17: A smaller limit can be set separately for just one key

```gherkin
Given the default limit for a team is set
When Alice sets a smaller limit separately for just one key within that team
Then that key's calls follow the smaller limit first
And the other keys in the same team follow the team default limit as before
And the per-key limit setting — including who set it and when — is recorded in the audit log
```

---

## Feature F19: Cache-affinity routing actually reduces cost

The same semantic call costs less the second time. This feature covers cache-affinity routing, cache hit rate, cost savings, cache tier separation, bypass blocking for inactive teams, cache state classification, and token-mismatch visibility.

### Scenario F19.1: The same semantic call is routed to the same place the second time

```gherkin
Given Bob's team makes two calls with the same input
When the second call reaches cc-lb
Then it is routed to the same processing path as the first call
And even if another team's call comes in between the two calls, the affinity is maintained
And the same fact is visible in the "cache-affinity trend" section of the usage report
```

### Scenario F19.2: The cache hit rate is visible at a glance on the same screen

```gherkin
Given Alice is viewing the dashboard for a team
When that team is using cache-affinity routing
Then the "cache hit rate" is shown by time period
And the higher the hit rate, the greater the cost savings visible on the same screen
And when the hit rate drops, that point is clearly visible on the trend
```

### Scenario F19.3a: The cost saved by cache hits is shown separately in the cost report

```gherkin
Given a team frequently repeats the same input
When the accumulated cost for that day is calculated
Then "cost saved by cache hits" is shown separately in the cost report
And that number is clearly visible alongside that day's accumulated cost
```

### Scenario F19.3b: "What the cost would have been without cache" is shown together as a hypothetical cost label

```gherkin
Given Alice is viewing the cost report for a team
When cache hits occurred that day
Then "what the cost would have been without cache" is shown together as a hypothetical cost label in the same report
And the hypothetical cost figure is clearly distinguished from the actual cost incurred
```

### Scenario F19.3c: Actual cost and hypothetical cost are compared on the same time-period trend

```gherkin
Given Alice is viewing the cost report for a team
When cache hits and misses occurred mixed by time period that day
Then "cost saved by cache hits" and "what the cost would have been without cache" are plotted together on the same time-period trend
And it is visible at a glance which time period had the greatest difference between the two figures
```

### Scenario F19.4: When cache affinity is broken, the call is processed fresh

```gherkin
Given a team's cache-affinity routing is broken for some reason
When the same semantic call comes in again
Then that call is processed fresh and receives a normal response
And the fact that the cache did not hit is not directly visible to Bob
And the same fact is visible to the operator as a drop in the cache hit rate
```

### Scenario F19.5: Cache affinity does not cross team boundaries

```gherkin
Given two teams happen to use the same input
When both teams' calls come into cc-lb
Then one team's cache hit does not flow to the other team's calls
And cost savings and hit rate are calculated separately per team
And the two teams' calls also appear separated in the audit log
```

### Scenario F19.6a: Even while cache affinity is working, limit and model bundle violations are still rejected

```gherkin
Given a team is making good use of cache-affinity routing
When the same team violates the per-minute limit or model bundle
Then the call is gracefully rejected regardless of whether the cache hit
And it is added once to the rejection-reason-by-type report normally
```

### Scenario F19.6b: The cache hit rate is calculated excluding rejected calls

```gherkin
Given cache-affinity routing and limit rejections both occurred for a team
When Alice views that team's cache hit rate
Then rejected calls are not included in the hit rate calculation
And the rejected call count and hit rate are shown separately on the same screen
```

### Scenario F19.7: Short-lived cache and long-lived cache are shown separated by tier

```gherkin
Given a team uses both short-lived inputs and long-lived inputs
When Alice opens the cache view for that team
Then the short tier and long tier are shown separately on the same screen
And the hit rate and cost savings for each tier are calculated and shown separately
And the meaning of the tiers is explained in domain vocabulary that the operator can understand
```

### Scenario F19.8: Calls from an inactive team are not accepted even through a cache hit

```gherkin
Given Alice has set a team to inactive status
When a call coming in on that team's key has an input that matches the cache-affinity routing well
Then that call is gracefully rejected regardless of whether the cache hits
And the cache does not become a bypass route for an inactive team
And the same fact appears normally in the rejection-reason-by-type report
```

### Scenario F19.9: The cache state of a single call is visible at a glance with a clear classification

```gherkin
Given Alice is viewing the cache detail view for a team
When some calls arrived fresh, some reached an already-accumulated input, and some partially reused
Then each call shows a clear state such as "hit, miss, partial-hit, create, skip, unknown"
And the ratio of each state is visible at a glance on the same screen
And Alice can filter to one state and view only that group of calls separately
```

### Scenario F19.10: Cache hits where the cost unit mismatches the original are marked separately

```gherkin
Given Alice is viewing the cache view for a team
When the same semantic call hit the cache but the cost-calculation unit mismatches the original call
Then "cache token mismatch" is clearly marked beside that call
And Alice also views how the degree of mismatch trends by time period
And the impact of that mismatch on the cost savings figure is shown together on the same screen
```

---

## Feature F26: Operator/SRE views cc-lb liveness and readiness separately

Charlie views whether cc-lb is alive and whether it is ready to accept work as separate signals. This feature covers the separation of the two signals, permanent failure, body display, reconnection tracking, restart markers, in-progress call gauge, and separate behavior during drain.

### Scenario F26.1: Liveness and readiness are shown separately

```gherkin
Given Charlie is viewing the status of cc-lb
When Charlie asks "is it alive?" and "is it ready to accept work?" separately
Then "is it alive?" answers yes if cc-lb itself is operating
And "is it ready to accept work?" answers yes only if all external dependencies are ready
And the two answers are seen separated without influencing each other
```

### Scenario F26.2a: When readiness is unrecoverable, it continues to answer "not ready"

```gherkin
Given the external dependency that cc-lb relies on is permanently unreachable
When Charlie asks "is it ready to accept work?" again
Then the answer continues to be "not ready"
And that answer does not change on the surface until the external dependency becomes reachable again
```

### Scenario F26.2b: Even while readiness is "not ready", the liveness signal remains separate and maintained

```gherkin
Given cc-lb's readiness answer continues to be "not ready"
When Charlie asks "is it alive?" again in the meantime
Then the answer continues to be "alive"
And "alive" and "not ready" are seen separated on the same screen without influencing each other
```

### Scenario F26.2c: The background auto-reconnect attempts are visible as a marker on the operations screen

```gherkin
Given cc-lb's readiness answer continues to be "not ready"
When cc-lb repeatedly attempts automatic reconnection to the external dependency in the background
Then a marker that "auto-reconnect is being attempted" is clearly visible on the same screen
And the "not ready" answer on the surface does not change until the auto-reconnect succeeds
```

### Scenario F26.3: The liveness body shows version, uptime, and build marker together

```gherkin
Given Charlie is viewing the detailed answer to "is it alive?"
When the body of that answer arrives
Then the body shows cc-lb's version, uptime, and build marker together
And the same markers change clearly between before and after a restart
And Charlie can tell which version is running just from the markers
```

### Scenario F26.4: When notification subscription is interrupted and reconnects, the changes in between are caught up

```gherkin
Given cc-lb's notification subscription is briefly interrupted
When that subscription reconnects
Then configuration and limit changes that occurred during the interruption are reflected via periodic catch-up
And Charlie can clearly see the interruption and the reconnection on the same trend
And the accumulated report numbers remain accurate during the interruption
```

### Scenario F26.5a: cc-lb announces clearly with a marker that it has restarted

```gherkin
Given cc-lb restarts for some reason
When Charlie views the same cc-lb again
Then a marker clearly showing that a restart occurred is visible
And that marker clearly marks the boundary between the old uptime and the new uptime
```

### Scenario F26.5b: The restart marker remains in the audit log with a timestamp

```gherkin
Given cc-lb restarts for some reason
When Dana reviews the same trail during a quarterly audit
Then the same restart marker remains in the audit log with a timestamp
And Dana can reconstruct the restart flow in chronological order from that record alone
```

### Scenario F26.6: The current number of in-progress calls is visible as a gauge on the same screen

```gherkin
Given Charlie is viewing cc-lb's readiness status screen
When the number of in-progress calls rises and falls over time
Then a gauge showing "the current number of in-progress calls" is visible on the same screen
And it is visible at a glance how close that number is to the concurrency limit
And the moment the gauge reaches zero is clearly marked
```

### Scenario F26.7: During graceful shutdown, liveness and readiness answer separately

```gherkin
Given cc-lb has begun graceful shutdown
When Charlie asks "is it alive?" and "is it ready to accept work?" separately
Then "is it alive?" continues to answer yes during that time
And "is it ready to accept work?" changes to "not ready" soon
And because of that separation, the external load balancer has time to block only new calls while letting in-progress calls finish
```
