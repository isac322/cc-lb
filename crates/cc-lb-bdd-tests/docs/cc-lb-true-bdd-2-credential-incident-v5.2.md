# cc-lb True BDD — Writer 2: Credential & Incident (v5.2)

- Date: 2026-06-18
- Writer: 2 of 4 (Credential & Incident — Anthropic credentials, OAuth, subscription freshness, incident response)
- Input: v4 62 scenarios + `~/cc-lb-bdd/cc-lb-bdd-verify-C-scenario-quality.md` + `~/cc-lb-bdd/cc-lb-bdd-v3-final-consensus.md` Open Question #3
- Change summary (v5 → v5.2): 66 unchanged; only 5 occurrences of the English word "replica" in body text replaced with "replica node". cross-file 5/5 PASS achieved.
- Change summary (v4 → v5): v4 62 → v5 66 (multi-rule split +4; consolidated F11 27 → F11A/F11B/F11C separated)
- Features: 7 — F5, F7, F8, F10, F11A, F11B, F11C (5 → 7 by applying consensus OQ#3 separation)
- Key changes (v5 → v5.2):
  - Vocabulary consistency finalized (resolving the 1 remaining verify-C §4 cross-file consistency item): the English word "replica" in the Charlie persona description and in 5 occurrences in the F11B.4 body text was replaced with the domain term "replica node", matching W4. cross-file 5/5 PASS achieved.
  - Scope of change: word substitution in 5 body occurrences only. Scenario IDs, count, feature structure, and multi-rule split results are all unchanged.
- Key changes (v4 → v5):
  - **F11 split (consensus OQ#3 applied)**: consolidated v4 F11 27 → F11A "Subscription Quota Visibility" 11 + F11B "Subscription Quota Freshness" 11 + F11C "Organization Metadata and Compatibility Cache" 7. Now within Cucumber.io's recommended 5-15 scenarios per feature.
  - **Vocabulary consistency cleanup (verify-C §4 applied)**: batch substitution of `upstream` (English) → `wiro` (Korean), `warmup` (English) → `miri-deum` (Korean), `callback` (English) in the OAuth screen flow → `doedol-aom` (Korean). OAuth preserved as a standard protocol proper noun.
  - **Multi-rule Then split (verify-C §5 applied)**: v4 F5.2 (notification + backoff) → v5 F5.2 + F5.3, v4 F8.1 (user UX + operator dashboard) → v5 F8.1 + F8.2, v4 F11.14 (base/overage distinction + overage entry explicit) → v5 F11A.3 + F11A.4, v4 F11.26 (retain stale value + last success time) → v5 F11C.5 + F11C.6.
  - **F11 internal scenario renumbering**: F11.1-F11.11 (warmup) → F11B.1-F11B.11; F11.12-F11.21 (5h/7d visibility) → F11A.1-F11A.11 (incorporating F11.14 split); F11.22-F11.27 (organization metadata and compatibility cache) → F11C.1-F11C.7 (incorporating F11.26 split).
- Personas
  - **Alice** — Operator. Oversees teams, credentials, subscription quotas, and cache freshness.
  - **Bob** — Developer / plugin author. Calls Claude through cc-lb.
  - **Charlie** — SRE. Handles incidents, restarts, warmup, and multi-replica node operations.
  - **Dana** — Auditor. Reviews audit records, secret protection, and redaction.

---

## Feature F5: The operator manages Anthropic credentials securely

The operator (Alice) registers, rotates, and revokes the credentials used to call Anthropic, and must be able to stop all traffic immediately when an incident occurs. The state of credentials and any permission changes to the protection key are visible to the operator in a single view.

### Scenario F5.1: cc-lb automatically rotates a credential that is close to expiry

- Given a credential registered by Alice has entered the 30-minute window before its expiry time
- When cc-lb enters the next rotation cycle
- Then cc-lb fetches and stores a new credential without operator intervention
- And none of Bob's calls are interrupted
- And the rotation is recorded in the audit log

### Scenario F5.2: cc-lb notifies the operator when automatic rotation fails repeatedly (v4 F5.2 split — notification rule)

- Given cc-lb has failed to auto-rotate the same credential for the configured number of consecutive attempts
- When the next rotation attempt also fails
- Then Alice's notification channel receives a message indicating this credential requires manual intervention

### Scenario F5.3: cc-lb increases the backoff interval when automatic rotation fails repeatedly (v4 F5.2 split — backoff rule)

- Given cc-lb has failed to auto-rotate the same credential for the configured number of consecutive attempts
- When the next rotation attempt also fails
- Then cc-lb increases the backoff interval to avoid hammering Anthropic with infinite retries

### Scenario F5.4: A malformed credential is rejected at registration time

- Given Alice has entered a credential that is malformed or fails the authorization check
- When registration is attempted
- Then cc-lb rejects the credential without storing it and returns a clear explanation
- And this credential is not added to Bob's call path

### Scenario F5.5: Revoking a credential immediately stops all calls that used it

- Given Bob's calls are in progress using credential K
- When Alice revokes K from the management screen
- Then new calls can no longer use K from the moment it is revoked
- And calls already in progress do not proceed to the next step using K
- And Bob receives a message indicating the administrator has revoked this credential

### Scenario F5.6: Audit records for a revoked credential remain intact

- Given Alice has revoked credential K
- When Dana looks up all calls that occurred under K in the audit view one month later
- Then the plaintext of K does not appear anywhere
- And the timestamp, team, and outcome of calls made under K are visible with no sign of tampering

### Scenario F5.7: The operator is notified when the permission on the credential protection key drifts (new P3)

- Given Charlie has placed the master protection key that guards cc-lb credentials in a safe location
- When the access permission on that protection key changes from its normal state
- Then cc-lb detects the permission change on the next inspection and notifies Alice and Dana in a single line
- And the path to register new credentials is blocked with a clear message until the permission is restored

### Scenario F5.8: The state of a credential is displayed in a human-readable form in a single view (new P3)

- Given Alice opens the detail view for credential K
- When the view renders
- Then the state of K is displayed in a human-readable form as one of: "found and healthy", "not visible", "shows signs of tampering", or "expired"
- And Alice receives guidance on what to do next within the same view

### Scenario F5.9: When two people try to edit the same credential simultaneously, only one is accepted (new P3)

- Given Alice and another operator modify the same credential K at nearly the same time
- When both changes arrive at cc-lb
- Then only the change that arrives first is applied
- And the party that arrives second receives a message indicating someone else changed it first, along with guidance to re-read and retry

---

## Feature F7: The operator activates the emergency killswitch

The operator (Alice) must be able to stop all outbound calls from cc-lb at once when an incident occurs. A two-step confirmation guards against accidental activation, and the reason for each activation and deactivation is recorded in the audit log.

### Scenario F7.1: Activating the emergency killswitch causes all calls to be rejected

- Given Alice has the authority to activate the emergency killswitch
- When Alice activates the emergency killswitch
- Then no call from Bob reaches Anthropic after that point
- And Bob receives a message indicating the operator has temporarily blocked all traffic

### Scenario F7.2: Deactivating the emergency killswitch restores normal operation

- Given the emergency killswitch is active
- When Alice deactivates the killswitch after completing the two-step confirmation
- Then Bob's calls reach Anthropic again normally after that point
- And the deactivation is recorded in the audit log

### Scenario F7.3: The emergency killswitch state persists across a cc-lb restart

- Given while the emergency killswitch is active, Charlie replaces the cc-lb process with a new binary
- When cc-lb becomes ready again
- Then the emergency killswitch remains active
- And Bob's first call after restart is also rejected

### Scenario F7.4: The management screen and dashboard remain operational during an emergency killswitch

- Given the emergency killswitch is active
- When Alice accesses the dashboard and views usage and the credential list
- Then the screen opens normally
- And Alice can deactivate the killswitch and revoke credentials

### Scenario F7.5: Responses rejected by the killswitch clearly indicate an operator-imposed block

- Given the emergency killswitch is active
- When Bob sends a call to cc-lb
- Then the response contains a human-readable message indicating the operator has temporarily blocked all traffic
- And Bob can distinguish that this is an operator decision, not an Anthropic outage

### Scenario F7.6: Activation and deactivation require a two-step confirmation

- Given Alice presses the emergency killswitch button for the first time
- When only a single click is attempted
- Then cc-lb requires a second confirmation asking "are you sure you want to activate the killswitch in this environment?"
- And the killswitch is not activated without the second confirmation
- And the same rule applies at the time of deactivation

### Scenario F7.7: The reason for each killswitch activation and deactivation is subject to audit (new P3)

- Given Alice intends to activate or deactivate the emergency killswitch
- When Alice enters a human-readable explanation of the reason for the decision during the two-step confirmation
- Then cc-lb records the reason, the decision-maker, and the timestamp in the audit log
- And Dana can trace why the killswitch was activated or deactivated using that same reason one month later

---

## Feature F8: The SRE responds to Anthropic outages

The SRE (Charlie) keeps cc-lb stable while giving users a clear explanation when an Anthropic upstream slows down or fails. Credentials and upstreams each have their own bulkhead, and rate-limit guidance from the upstream is passed through to users as-is.

### Scenario F8.1: When the upstream slows temporarily, users are notified of the delay (v4 F8.1 split — user UX rule)

- Given Anthropic is responding more slowly than usual
- When Bob's call exceeds the configured time limit
- Then cc-lb does not drop the call and sends the user a message indicating the upstream is slow and to wait a moment

### Scenario F8.2: When the upstream slows, the reason is shown on the operator dashboard (v4 F8.1 split — operator visibility rule)

- Given Anthropic is responding more slowly than usual
- When Bob's call exceeds the configured time limit
- Then Charlie's dashboard shows the reason as "upstream delay"

### Scenario F8.3: Anthropic's rate-limit-exceeded response is passed through to users as-is

- Given Anthropic signals that this credential has exhausted its quota
- When Bob's call uses that credential
- Then cc-lb passes this information through to Bob with the same meaning and without modification
- And calls using other credentials are not affected

### Scenario F8.4: When the same credential fails consecutively, that credential alone is temporarily blocked

- Given calls going through credential K are failing consecutively
- When the failure count exceeds the configured threshold
- Then cc-lb temporarily refuses new calls going out through K
- And calls using other credentials proceed normally

### Scenario F8.5: New calls during the temporary block are rejected quickly

- Given credential K is currently blocked by the circuit breaker
- When Bob sends a call using the same credential
- Then cc-lb immediately returns a message indicating the credential is temporarily blocked, without sending the call upstream
- And Bob receives guidance on when to retry

### Scenario F8.6: When Anthropic responds with 5xx, users are informed of the temporary outage

- Given Anthropic signals a temporary outage on its side
- When Bob's call is in progress
- Then cc-lb delivers a human-readable message indicating there is a temporary outage on the upstream
- And guidance indicating it is safe to retry the same call shortly is included

### Scenario F8.7: When one upstream goes down, traffic is automatically rerouted to another upstream

- Given Charlie has registered multiple upstreams for the same credential
- When one of those upstreams stops responding
- Then new calls flow to the other upstreams that are still alive
- And Bob's calls receive normal responses without noticing the disruption

### Scenario F8.8: When all upstreams go down simultaneously, a consistent response is returned

- Given all upstreams for the same credential have stopped responding simultaneously
- When Bob's call arrives
- Then cc-lb sends users a single message indicating the entire upstream is currently unreachable
- And the same message is sent consistently throughout the incident

### Scenario F8.9: When the routing trace becomes too long, it is shown with a truncation indicator

- Given Charlie wants to know how a call was routed — which credential, upstream, and circuit breaker state were used
- When Charlie opens the cc-lb screen with that call's trace identifier
- Then the major steps of the routing decision are visible at a glance
- And when the path exceeds the display limit and further detail cannot be shown, the fact that the trace was truncated here is indicated in a human-readable way

### Scenario F8.10: When backpressure is applied, new calls are rejected gracefully

- Given cc-lb is about to exceed the number of concurrent calls it can accept
- When one more new call arrives
- Then cc-lb does not accept the call and returns a graceful message indicating there is no capacity at this time
- And calls already in progress are not affected

### Scenario F8.11: Each upstream has its own concurrent call bulkhead (new P3)

- Given Charlie has registered two upstreams for the same credential
- When the concurrent calls going to one upstream reach that upstream's bulkhead limit
- Then new calls going to that upstream are rejected gracefully
- And calls using the other upstream proceed normally

### Scenario F8.12: Only idempotent calls are retried automatically (new P3)

- Given Bob's call has failed due to a temporary upstream outage
- When cc-lb decides whether to resend the call
- Then only calls classified as safe to retry with the same outcome are retried automatically
- And calls that cannot be classified as safe to retry are not retried and their result is passed to the user as-is

### Scenario F8.13: Rate-limit guidance headers from Anthropic are passed through to users as-is (new P3)

- Given Anthropic includes a header in its response indicating how long to wait before the next attempt
- When cc-lb forwards that response to Bob
- Then the rate-limit guidance header is passed through unmodified and untruncated
- And Bob reads the same guidance in his client and determines when to make the next attempt

---

## Feature F10: The operator registers credentials via Anthropic OAuth

The operator (Alice) registers credentials by consenting to Anthropic in a browser. Concurrent consent flows, session expiry, and cancellation are all handled safely. OAuth is preserved as a standard protocol name; the flow of returning to cc-lb after consent is called a "callback".

### Scenario F10.1: The operator consents to Anthropic through a browser

- Given Alice begins creating a new OAuth credential
- When Alice navigates her browser to the address cc-lb provides and consents with Anthropic
- Then when consent is complete, the flow returns to cc-lb
- And no one other than Alice who started the session can take over this consent flow

### Scenario F10.2: The callback is accepted only after cc-lb validates it

- Given Alice finishes consent and the callback arrives at cc-lb
- When cc-lb inspects the returned information
- Then cc-lb accepts only callbacks that match a session it issued
- And callbacks that fail the check do not result in a credential being created

### Scenario F10.3: When consent completes, the credential is registered and marked active

- Given Alice has completed consent successfully
- When cc-lb stores the received credential
- Then the new credential appears in the list as active
- And it is immediately available for use in Bob's calls at that same moment

### Scenario F10.4: An invalid callback address is rejected

- Given Alice attempts to return via a callback address that is not registered with cc-lb
- When cc-lb inspects the address
- Then cc-lb rejects the request with a clear explanation and does not create a credential
- And this attempt is recorded in the audit log

### Scenario F10.5: A missing or tampered session marker is rejected

- Given cc-lb issued a temporary marker when Alice started the consent flow
- When the marker in the returning callback is absent or shows signs of tampering
- Then cc-lb does not create a credential
- And Alice receives a message indicating she should restart the consent flow

### Scenario F10.6: If the operator cancels consent, no credential is created

- Given Alice selected "deny" on the Anthropic consent screen
- When the callback arrives at cc-lb
- Then no credential is created
- And Alice sees a message indicating the credential was not registered because consent was not granted

### Scenario F10.7: Two operators conducting OAuth consent simultaneously do not interfere with each other (new P3)

- Given Alice and another operator have each started an OAuth consent flow in their own browsers at nearly the same time
- When the callbacks from both arrive at cc-lb at nearly the same time
- Then each person's consent session is matched only to the callback that person initiated
- And one person's consent cannot capture the other person's credential

### Scenario F10.8: A consent session expires after a set period of time (new P3)

- Given Alice has started an OAuth consent flow and cc-lb has issued a temporary marker
- When the callback does not arrive within the configured time
- Then that marker is no longer valid
- And a callback that arrives late with that marker is rejected, and Alice sees a message indicating she should restart the consent flow

---

## Feature F11A: Subscription Quota Visibility (5h/7d quota screen)

The operator (Alice) views the current usage, quota, proximity warning, and aggregation mode for each credential's 5-hour window and 7-day window in a single screen. The screen can be refreshed immediately to pull in console changes. (Split from v4 F11.12-F11.21 by applying consensus OQ#3. v4 F11.14 was multi-rule and is split into F11A.3 + F11A.4.)

### Scenario F11A.1: Current usage against the 5-hour quota is visible

- Given Alice opens the detail view for active credential K
- When the screen renders
- Then how much K has used in the current 5-hour window is shown with a number
- And the window's start time and end time are shown alongside

### Scenario F11A.2: Current usage against the 7-day quota is visible

- Given Alice opens the detail view for active credential K
- When the screen renders
- Then how much K has used in the current 7-day window is shown with a number
- And the window's start time and next renewal time are shown alongside

### Scenario F11A.3: The base quota and overage quota are displayed separately (v4 F11.14 split — distinction display rule)

- Given credential K has both a base quota and an overage quota assigned
- When Alice views the detail for K
- Then the usage against the base quota and the usage against the overage quota are displayed separately

### Scenario F11A.4: The fact that the overage quota has been entered is explicitly shown to the operator (v4 F11.14 split — entry state rule)

- Given credential K has exceeded the base quota and entered the overage quota
- When Alice views the detail for K
- Then the fact that the overage quota has been entered is stated explicitly in a human-readable form
- And Alice sees how much of the overage quota remains within the same screen

### Scenario F11A.5: A warning is shown on screen when usage approaches 80%

- Given usage in credential K's 5-hour window approaches 80%
- When Alice views the dashboard
- Then K's row uses a distinct highlight to convey that the quota is approaching its limit
- And the same notification also arrives once on the notification channel

### Scenario F11A.6: The operator refreshes quota metadata immediately

- Given Alice has just changed the quota in the Anthropic console
- When Alice presses "Refresh now" on credential K's screen
- Then cc-lb fetches the metadata again immediately without waiting for the next automatic cycle
- And the new quota is reflected on the screen immediately

### Scenario F11A.7: The operator chooses the quota aggregation mode

- Given Alice's team has multiple credentials in the same organization
- When Alice selects one of "view per credential" or "view aggregated by organization"
- Then the usage and quota display on screen is redrawn consistently according to the selected mode
- And Alice can switch to the other mode and view it again at any time

### Scenario F11A.8: Usage by time slot within the 5-hour window is shown separately (new P3)

- Given Alice opens the 5-hour quota detail for credential K
- When the usage bar is expanded
- Then usage broken down by time slot within the 5-hour window is shown alongside
- And Alice can see at a glance which time slots had the highest call volume within the same screen

### Scenario F11A.9: Usage restrictions attached to a credential are displayed in a human-readable form (new P3)

- Given Anthropic communicates usage restrictions alongside credential K
- When Alice views the detail for K
- Then those restrictions are presented as human-readable guidance
- And Alice sees within the same screen which calls this credential can and cannot be used for

### Scenario F11A.10: When the quota is exhausted, the shortfall is shown to the operator (new P3)

- Given credential K has exhausted its quota within the quota window
- When Alice views the analytics window detail for K
- Then the shortfall is shown as a human-readable number
- And Alice can see at a glance how much quota is currently short

### Scenario F11A.11: The last known quota value is temporarily retained after a credential is deleted (new P3)

- Given Alice deletes credential K from the screen
- When Alice immediately registers a new credential K' in the same slot
- Then the last quota value K showed is temporarily preserved
- And Alice can compare K's last state and K's new state within the same screen

---

## Feature F11B: Subscription Quota Freshness (warmup)

cc-lb periodically sends small signals to keep the Anthropic subscription's 5-hour window active (these signals are called "warmup"). The SRE (Charlie) views the warmup schedule, retries, isolation, OAuth exclusivity, and upstream address changes in a single screen. (Split from v4 F11.1-F11.11 by applying consensus OQ#3.)

### Scenario F11B.1: Anthropic is signaled periodically to keep the quota active

- Given cc-lb has an active credential
- When the configured cycle arrives
- Then cc-lb sends a small signal to Anthropic to keep the quota active
- And the next cycle time is determined by adjusting from the start time of the 5-hour window
- And the 5-hour window remains alive even without any calls from Bob

### Scenario F11B.2: Warmup calls are not counted toward usage or cost

- Given cc-lb is sending small signals for warmup
- When Alice views the usage and cost for the same time window
- Then warmup signals are not added to either the usage figure or the cost report
- And only calls actually sent by Bob are counted as usage

### Scenario F11B.3: When Anthropic signals to back off, the polling interval is increased

- Given Anthropic responds indicating that requests for this credential should be sent less frequently
- When cc-lb determines the next warmup time
- Then the polling interval increases automatically
- And when Anthropic's guidance returns to normal, the interval returns to normal as well

### Scenario F11B.4: Only one of multiple replica nodes performs warmup

- Given Charlie has cc-lb running as three replica nodes
- When the warmup cycle arrives
- Then only one of the three replica nodes sends the warmup signal for that credential
- And no other replica node sends the same signal again during the same cycle

### Scenario F11B.5: When warmup fails, the next attempt uses a backoff interval

- Given the previous warmup attempt failed
- When cc-lb determines the next attempt
- Then the next attempt does not happen immediately and a backoff interval is applied
- And when a successful response is returned, the backoff shrinks back to the normal interval

### Scenario F11B.6: The operator views warmup status on screen

- Given Alice opens the operations screen
- When she expands a credential row
- Then "last successful warmup time", "next attempt time", and "end time of the current quota window" are shown together
- And Alice can determine at a glance that warmup is currently alive

### Scenario F11B.7: Changes that occurred while the subscription was disconnected are caught up by the reconciler after reconnection

- Given while cc-lb's notification subscription was briefly disconnected, Alice revoked a credential and registered a new upstream
- When the subscription reconnects
- Then the periodic reconciler sweeps through the changes that occurred during the disconnection and applies them to cc-lb's current state
- And no further calls go out using the revoked credential, and the new upstream is included in routing

### Scenario F11B.8: Warmup is suspended during an emergency killswitch (new P3)

- Given cc-lb's emergency killswitch is active
- When the warmup cycle arrives
- Then cc-lb does not send the warmup signal
- And after the killswitch is deactivated, warmup resumes as normal from the next cycle

### Scenario F11B.9: The operator views the warmup target upstream and next attempt time (new P3)

- Given Alice opens the warmup detail screen
- When she expands the multiple upstreams bound to a credential
- Then the next warmup attempt time and last result for each upstream are shown together
- And Alice can read within the same screen that even for the same credential, the schedule may differ per upstream

### Scenario F11B.10: The operator is shown that warmup applies only to OAuth credentials (new P3)

- Given Alice has a credential registered by a method other than OAuth
- When Alice views the warmup status of that credential
- Then the screen shows in a human-readable form that warmup does not apply to this credential
- And Alice is informed within the same screen that an OAuth credential is required to receive warmup

### Scenario F11B.11: Upstream address changes at Anthropic are shown to the operator and calls are not interrupted (new P3)

- Given cc-lb re-checks the domain for the Anthropic upstream on its own cycle
- When the upstream address changes to a different value from normal
- Then Bob's calls in progress are not interrupted
- And Charlie's screen displays a single line indicating the upstream address has been updated to a new value

---

## Feature F11C: Organization Metadata and Compatibility Cache Freshness

cc-lb periodically refreshes organization metadata (pricing tier, billing type) and the compatibility cache, and shows refresh failures, restarts, and freshness timestamps to the operator as-is. (Split from v4 F11.22-F11.27 by applying consensus OQ#3. v4 F11.26 was multi-rule and is split into F11C.5 + F11C.6.)

### Scenario F11C.1: The compatibility cache is automatically refreshed on a one-hour cycle

- Given cc-lb is running and the compatibility cache is populated
- When the configured one-hour cycle arrives
- Then cc-lb replaces the cache with new values
- And none of Bob's calls are interrupted between refreshes

### Scenario F11C.2: Organization metadata is stored in a way that allows quota differences to be traced

- Given Alice's organization has two credentials with different pricing tiers or billing types
- When Alice opens the dashboard
- Then each credential's pricing tier and billing type are shown in a human-readable form
- And Alice can see at a glance that the quota difference comes from the tier

### Scenario F11C.3: The operator manually refreshes subscription metadata

- Given Alice has just changed the billing type in the Anthropic console
- When Alice presses "Refresh subscription metadata" on the cc-lb screen
- Then cc-lb fetches the subscription metadata again without waiting for the next automatic cycle
- And the new information is reflected on the screen immediately

### Scenario F11C.4: Process restart markers are not mistaken for quota spikes

- Given Charlie has replaced the cc-lb process with a new binary
- When cc-lb starts up again and recalculates usage
- Then the restart marker is not drawn as a spike on the quota usage graph but is shown as a distinct indicator
- And Alice can read from the graph that there was a restart, not a sudden increase in quota usage

### Scenario F11C.5: When a compatibility cache refresh fails, the previous value is retained (v4 F11.26 split — stale value retention rule)

- Given cc-lb receives the compatibility cache on a one-hour cycle
- When one of the refresh attempts fails
- Then cc-lb retains the previous value it received

### Scenario F11C.6: The time of the last successful compatibility cache refresh is shown to the operator (v4 F11.26 split — freshness display rule)

- Given a compatibility cache refresh for cc-lb has failed once
- When Alice views the compatibility cache detail
- Then the time of the last successful refresh is displayed in a human-readable form

### Scenario F11C.7: The last attempt time and last success time are shown separately (new P3)

- Given Alice opens the subscription metadata detail
- When the screen renders
- Then "last attempt time" and "last success time" are shown separately
- And Alice can see at a glance how fresh the currently displayed values are

---

Completed: 2026-06-18
Writer: 2 of 4 (Credential & Incident)
Features: 7 — F5, F7, F8, F10, F11A (visibility), F11B (freshness), F11C (organization metadata and compatibility cache)
Scenarios: 66 (9 + 7 + 13 + 8 + 11 + 11 + 7)
References: `~/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v4.md`, `~/cc-lb-bdd/cc-lb-bdd-verify-C-scenario-quality.md`, `~/cc-lb-bdd/cc-lb-bdd-v3-final-consensus.md`

---

## Change Log (v4 → v5)

### 1) F11 Split (consensus Open Question #3 applied)

The consolidated v4 F11 (27 scenarios) exceeded Cucumber.io's recommended 5-15 scenarios per feature by nearly double. Split into three as follows:

| v5 feature | Name | v4 source range | Scenario count | Operational concern |
|---|---|---|---:|---|
| **F11A** | Subscription Quota Visibility | v4 F11.12-F11.21 + split | 11 | visibility side: 5h/7d quota, usage, proximity warning, aggregation mode, time slot breakdown, etc. |
| **F11B** | Subscription Quota Freshness | v4 F11.1-F11.11 | 11 | signal-sending side: cycle, isolation, retries, OAuth exclusivity, upstream address changes, etc. |
| **F11C** | Organization Metadata and Compatibility Cache Freshness | v4 F11.22-F11.27 + split | 7 | metadata freshness: one-hour cycle, restart markers, stale value retention, freshness timestamps, etc. |

All three features fall within the recommended 5-15 range.

### 2) Multi-rule Then Split (verify-C §5 applied)

Based on the diagnosis that 53.3% of 30 sampled verify-C scenarios were multi-rule, the 4 scenarios in v4 W2 that most clearly combined two rules are split:

| v4 ID | Two bundled rules | v5 split result |
|---|---|---|
| F5.2 | (a) notification delivered to operator (b) backoff applied | F5.2 (notification) + F5.3 (backoff) |
| F8.1 | (a) delay notice to user (b) reason shown on operator dashboard | F8.1 (user UX) + F8.2 (operator visibility) |
| F11.14 | (a) base/overage distinction display (b) overage entry state made explicit | F11A.3 (distinction display) + F11A.4 (entry state) |
| F11.26 | (a) retain previous value (b) display last success time | F11C.5 (stale value retention) + F11C.6 (freshness display) |

Total +4 scenarios. Other scenarios suspected of being multi-rule (F5.1 rotation+audit, F7.2 deactivation+audit, F10.4 rejection+audit, etc.) were judged to have a single-line audit companion attached to the same operator concern, and their split was deferred (verify-C §7 fix #4 "audit sibling separation" recommended for the next round).

### 3) Vocabulary Consistency Cleanup (verify-C §4 applied)

| v4 English vocabulary | Frequency | v5 Korean vocabulary | Notes |
|---|---:|---|---|
| `upstream` | 18 (feature description 1 + F8.7×4 + F8.8×2 + F8.11×5 + F11B.9×4 + body distribution) | `wiro` | F8.1 had already used "wiro". Unified throughout. |
| `warmup` | 25+ (Charlie persona description + F8 intro + F11B.1-F11B.10 body overall) | `miri-deum` | Recommended by verify-C. Applied across the entire F11B feature. |
| `callback` | 9 (F10.2×3 + F10.4×2 + F10.5×1 + F10.7×1 + F10.8×2) | `doedol-aom` | Applied uniformly in the F10 OAuth flow. OAuth preserved as a standard protocol name. |
| `OAuth` | n/a | `OAuth` (preserved) | Preserved as a standard protocol name. |
| `access_token` / `refresh_token` / `API key` | n/a (not in W2) | "token / refresh token / API key" | Does not occur in this W2. W4 document's responsibility. |

Total vocabulary replacements: **52** (upstream 18 + warmup 25 + callback 9).

### 4) Personas

Four personas only: Alice (Operator), Bob (Developer), Charlie (SRE), Dana (Auditor). Unchanged from v4.

### 5) v4 → v5 Scenario Mapping

```
v4 F5.1 → v5 F5.1
v4 F5.2 → v5 F5.2 (notification) + F5.3 (backoff)        [SPLIT]
v4 F5.3 → v5 F5.4
v4 F5.4 → v5 F5.5
v4 F5.5 → v5 F5.6
v4 F5.6 → v5 F5.7
v4 F5.7 → v5 F5.8
v4 F5.8 → v5 F5.9

v4 F7.1-F7.7 → v5 F7.1-F7.7 (no change)

v4 F8.1 → v5 F8.1 (user UX) + F8.2 (operator visibility)  [SPLIT]
v4 F8.2 → v5 F8.3
v4 F8.3 → v5 F8.4
v4 F8.4 → v5 F8.5
v4 F8.5 → v5 F8.6
v4 F8.6 → v5 F8.7 (upstream → "wiro" substitution)
v4 F8.7 → v5 F8.8 (upstream → "wiro" substitution)
v4 F8.8 → v5 F8.9 (upstream → "wiro" substitution)
v4 F8.9 → v5 F8.10
v4 F8.10 → v5 F8.11 (upstream → "wiro" substitution, 4×)
v4 F8.11 → v5 F8.12
v4 F8.12 → v5 F8.13

v4 F10.1 → v5 F10.1
v4 F10.2 → v5 F10.2 (callback → "doedol-aom" substitution, 3×)
v4 F10.3 → v5 F10.3
v4 F10.4 → v5 F10.4 (callback → "doedol-aom" substitution, 2×)
v4 F10.5 → v5 F10.5 (callback → "doedol-aom" substitution, 1×)
v4 F10.6 → v5 F10.6
v4 F10.7 → v5 F10.7 (callback → "doedol-aom" substitution, 1×)
v4 F10.8 → v5 F10.8 (callback → "doedol-aom" substitution, 2×)

v4 F11.1 → v5 F11B.1 (warmup → "miri-deum" substitution)
v4 F11.2 → v5 F11B.2 (warmup → "miri-deum" substitution)
v4 F11.3 → v5 F11B.3 (warmup → "miri-deum" substitution)
v4 F11.4 → v5 F11B.4 (warmup → "miri-deum" substitution)
v4 F11.5 → v5 F11B.5 (warmup → "miri-deum" substitution)
v4 F11.6 → v5 F11B.6 (warmup → "miri-deum" substitution)
v4 F11.7 → v5 F11B.7 (upstream → "wiro" substitution)
v4 F11.8 → v5 F11B.8 (warmup → "miri-deum" substitution, 4×)
v4 F11.9 → v5 F11B.9 (warmup and upstream batch substitution)
v4 F11.10 → v5 F11B.10 (warmup → "miri-deum" substitution)
v4 F11.11 → v5 F11B.11 (upstream / "wijjok" → "wiro" batch substitution)

v4 F11.12 → v5 F11A.1
v4 F11.13 → v5 F11A.2
v4 F11.14 → v5 F11A.3 (distinction) + F11A.4 (entry)        [SPLIT]
v4 F11.15 → v5 F11A.5
v4 F11.16 → v5 F11A.6
v4 F11.17 → v5 F11A.7
v4 F11.18 → v5 F11A.8
v4 F11.19 → v5 F11A.9
v4 F11.20 → v5 F11A.10
v4 F11.21 → v5 F11A.11

v4 F11.22 → v5 F11C.1
v4 F11.23 → v5 F11C.2
v4 F11.24 → v5 F11C.3
v4 F11.25 → v5 F11C.4
v4 F11.26 → v5 F11C.5 (stale value retention) + F11C.6 (freshness display)   [SPLIT]
v4 F11.27 → v5 F11C.7
```

### 6) Count Changes

| Category | v4 | Split / substitution result | v5 |
|---|---:|---|---:|
| F5 | 8 | +1 (F5.2 split) | 9 |
| F7 | 7 | no change | 7 |
| F8 | 12 | +1 (F8.1 split) | 13 |
| F10 | 8 | no change (callback substitution only) | 8 |
| F11A | n/a (part of v4 F11) | F11.12-F11.21 + F11.14 split | 11 |
| F11B | n/a (part of v4 F11) | F11.1-F11.11 | 11 |
| F11C | n/a (part of v4 F11) | F11.22-F11.27 + F11.26 split | 7 |
| **Total** | **62** | **+4** | **66** |

Features: 5 → 7 (single F11 split into F11A/F11B/F11C).

### 7) Next Round Items (v5 → v6 preview)

- Consider full application of verify-C §7 fix #4 "behavior + audit sibling separation" — F5.1, F7.2, F10.4 are candidates.
- Stakeholder review of whether the F11A/F11B/F11C split affects operator screen navigation.
- Consensus Open Question #2 (F8.9 reframe) was already resolved in v4 and is retained in the v5 body as-is.
- The consensus count discrepancy note (W2 +21 claim vs. actual +19) is maintained in this v5 with the explicit +19 baseline.
