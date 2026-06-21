# BDD Scenario → Rust Test Function Mapping (M0 deliverable for plan v3)

- Total scenarios: 321 (v5.2)
- Auto-extraction date: 2026-06-18
- Mapping rules: Scenario ID `Fn.m[suffix]` maps to function name `fn_m[suffix]` (lowercase, dot to underscore). The backend matrix automatically appends `_sqlite` or `_postgres` suffixes via macro expansion.
- English source decision (v3 §0): Step text, titles, docs, and panic messages are in English. This table now uses the English v5.2 scenario titles.
- Fast column: PR gate (around 30 candidates, 33 marked). The target is the nextest filter `test(/^fast_/)`.
- Status column: Initial value is `pending`. It updates to `RED→GREEN`, `STABLE`, `OoS-manual`, or `blocked-<reason>` starting from M1.
- Persona column: Default value is set per feature. Multi-persona scenarios, such as Alice to Bob hand-off, are reassigned per row in M1.

## Totals

| Writer | Scenarios | Features |
|---|---:|---:|
| W1 (team/traffic/dashboard/cache/health) | 98 | 7 |
| W2 (credential/incident) | 66 | 7 |
| W3 (policy/plugin) | 63 | 6 |
| W4 (platform/audit/lifecycle/backend/cost/secret) | 94 | 7 |
| **TOTAL** | **321** | **27** |

Invariant (every milestone gate): `converted + OoS-manual + blocked == 321`.

## Mapping Table (321 rows)

| Writer | ID | Feature | Persona (default) | fn_name | Fast? | Backend | Status | v5.2 title |
|---|---|---|---|---|:---:|---|---|---|
| W1 | F1.1a | F1 | Alice | `f1_1a` | ✓ | both | pending | New team registration finishes active state and the first key on one screen |
| W1 | F1.1b | F1 | Alice | `f1_1b` | ✓ | both | pending | New team registration records who created it and when in audit |
| W1 | F1.2 | F1 | Alice | `f1_2` |  | both | pending | An issued key is shown only once right after registration |
| W1 | F1.3 | F1 | Alice | `f1_3` |  | both | pending | A second save stops if another operator changed the same team meanwhile |
| W1 | F1.4 | F1 | Alice | `f1_4` | ✓ | both | pending | Calls from an inactive team are not accepted |
| W1 | F1.5 | F1 | Alice | `f1_5` | ✓ | both | pending | Calls outside the model range allowed for a team are rejected |
| W1 | F1.7 | F1 | Alice | `f1_7` | ✓ | both | pending | Deleting a team does not erase audit traces of what that team did |
| W1 | F1.8 | F1 | Alice | `f1_8` |  | both | pending | Registration is rejected when an identifier contains forbidden characters or a reserved prefix |
| W1 | F1.10 | F1 | Alice | `f1_10` |  | both | pending | When an inactive team is restored to active, the same secrets are soon accepted again |
| W1 | F1.11 | F1 | Alice | `f1_11` |  | both | pending | Calls started just before deactivation finish, and only new calls are rejected |
| W1 | F2.1 | F2 | Alice | `f2_1` | ✓ | both | pending | When a new key is issued, the secret is shown only once |
| W1 | F2.2 | F2 | Alice | `f2_2` |  | both | pending | During key rotation, the new key and old key have a short overlap period |
| W1 | F2.3 | F2 | Alice | `f2_3` | ✓ | both | pending | Immediately after key revocation, the next calls are rejected |
| W1 | F2.4a | F2 | Alice | `f2_4a` |  | both | pending | A key that automatically expires at a set time stops at that time |
| W1 | F2.4b | F2 | Alice | `f2_4b` |  | both | pending | An upcoming-expiration notification reaches the operator in advance |
| W1 | F2.5 | F2 | Alice | `f2_5` | ✓ | both | pending | Secrets are never shown on the key list screen |
| W1 | F2.6 | F2 | Alice | `f2_6` |  | both | pending | An operator can later edit a key holder name and memo |
| W1 | F2.8a | F2 | Alice | `f2_8a` |  | both | pending | Usage reporting separates call counts and rejection reasons by key holder |
| W1 | F2.8b | F2 | Alice | `f2_8b` |  | both | pending | When a holder name is edited, old usage is unified under the new name |
| W1 | F2.10 | F2 | Alice | `f2_10` |  | both | pending | The number of active keys one holder may have at the same time is limited |
| W1 | F2.11 | F2 | Alice | `f2_11` |  | both | pending | Looking again at the last few characters of a key is also audited |
| W1 | F2.12a | F2 | Alice | `f2_12a` |  | both | pending | The list screen clearly distinguishes whether a key holder is a person or a machine |
| W1 | F2.12b | F2 | Alice | `f2_12b` |  | both | pending | Usage reporting also shows separate totals for person and machine units |
| W1 | F2.13a | F2 | Alice | `f2_13a` |  | both | pending | Key state is handled in active→paused→revoked order |
| W1 | F2.13b | F2 | Alice | `f2_13b` |  | both | pending | Every key state transition records who changed it and when in audit |
| W1 | F2.14 | F2 | Alice | `f2_14` |  | both | pending | Trying key rotation on a storage backend that does not support rotation is politely rejected (new in v5) |
| W1 | F3.1 | F3 | Bob | `f3_1` | ✓ | both | pending | Calling a normal model with a normal key returns the response unchanged |
| W1 | F3.2 | F3 | Bob | `f3_2` |  | both | pending | A streaming response flows to the end without interruption |
| W1 | F3.3 | F3 | Bob | `f3_3` | ✓ | both | pending | A call sent with an unknown key is politely rejected |
| W1 | F3.4 | F3 | Bob | `f3_4` | ✓ | both | pending | A call sent with a key from an inactive team is rejected |
| W1 | F3.5a | F3 | Bob | `f3_5a` |  | both | pending | Calling a model not allowed for the caller's team rejects the call |
| W1 | F3.5b | F3 | Bob | `f3_5b` |  | both | pending | A disallowed-model rejection message is delivered as a Claude-shaped error envelope |
| W1 | F3.5c | F3 | Bob | `f3_5c` |  | both | pending | A disallowed-model rejection is added to both usage reporting and the audit bundle |
| W1 | F3.8 | F3 | Bob | `f3_8` |  | both | pending | If a sudden problem occurs inside cc-lb during a response, it closes with a Claude-shaped error |
| W1 | F3.9 | F3 | Bob | `f3_9` |  | both | pending | Bob completes the upload, download, and delete file flow through cc-lb |
| W1 | F3.11a | F3 | Bob | `f3_11a` |  | both | pending | A call sent to an unknown path or unknown action returns a Claude-shaped error envelope |
| W1 | F3.11b | F3 | Bob | `f3_11b` |  | both | pending | Rejection of an unknown path or action is added to usage reporting by rejection reason type |
| W1 | F3.12 | F3 | Bob | `f3_12` |  | both | pending | When rejected by a limit, the same response includes when to try again |
| W1 | F3.13 | F3 | Bob | `f3_13` |  | both | pending | If the response body exceeds a predefined ceiling, it is politely cut off |
| W1 | F3.14 | F3 | Bob | `f3_14` |  | both | pending | Attempts to go directly to an external host outside registered paths are blocked |
| W1 | F3.15 | F3 | Bob | `f3_15` |  | both | pending | Single-hop forwarding markers are cleaned up at the response boundary |
| W1 | F3.16 | F3 | Bob | `f3_16` |  | both | pending | Temporarily turning off authentication in an isolated test environment is clearly marked |
| W1 | F4.1a | F4 | Alice | `f4_1a` |  | both | pending | Yesterday's highest-usage teams are visible at a glance in cost order |
| W1 | F4.1b | F4 | Alice | `f4_1b` |  | both | pending | The same table shows call count and rejection ratio together |
| W1 | F4.1c | F4 | Alice | `f4_1c` |  | both | OoS-manual (visual assertion) | Pressing a team row leads to that team's detail view |
| W1 | F4.2 | F4 | Alice | `f4_2` |  | both | pending | Alice narrows the view by period, team, key holder, or model |
| W1 | F4.3 | F4 | Alice | `f4_3` |  | both | pending | When monthly cost approaches a configured limit, it appears early on the same screen |
| W1 | F4.4a | F4 | Alice | `f4_4a` |  | both | pending | Call share and cost share by model are visible at a glance on the same screen |
| W1 | F4.4b | F4 | Alice | `f4_4b` |  | both | pending | Pressing a model in the model share table leads to its hourly flow |
| W1 | F4.5 | F4 | Alice | `f4_5` |  | both | pending | Hourly usage flow continues without gaps |
| W1 | F4.6 | F4 | Alice | `f4_6` |  | both | pending | Rejection reasons are visible at a glance by type |
| W1 | F4.7 | F4 | Alice | `f4_7` |  | both | pending | A report screen can be downloaded in table format |
| W1 | F4.8 | F4 | Alice | `f4_8` |  | both | pending | The fact that the live flow was interrupted is clearly visible on the same screen |
| W1 | F4.10 | F4 | Alice | `f4_10` |  | both | pending | Alice narrows the log page by type, team, or holder |
| W1 | F4.11a | F4 | Alice | `f4_11a` |  | both | pending | "0 calls" and "not known yet" are clearly distinguished in the same table |
| W1 | F4.11b | F4 | Alice | `f4_11b` |  | both | OoS-manual (visual assertion) | The meaning of the two markers is explained with user-facing help |
| W1 | F4.11c | F4 | Alice | `f4_11c` |  | both | pending | The same distinction is also shown on the hourly flow chart |
| W1 | F4.12 | F4 | Alice | `f4_12` |  | both | pending | Alice views the per-stage dwell time of one call in detail |
| W1 | F6.1 | F6 | Alice | `f6_1` |  | both | pending | When the per-minute limit is exceeded, the team's next call is temporarily rejected |
| W1 | F6.2a | F6 | Alice | `f6_2a` |  | both | pending | When the daily limit fills, the rest of that day's calls are rejected and it resets at the same time next day |
| W1 | F6.2b | F6 | Alice | `f6_2b` |  | both | pending | Daily-limit rejection guidance includes when the limit will clear again |
| W1 | F6.3 | F6 | Alice | `f6_3` |  | both | pending | When the monthly cost limit approaches, new calls are rejected early |
| W1 | F6.4 | F6 | Alice | `f6_4` |  | both | pending | Calling outside the allowed model bundle is rejected |
| W1 | F6.5 | F6 | Alice | `f6_5` |  | both | pending | A call immediately after a limit edit soon reflects the new limit |
| W1 | F6.8 | F6 | Alice | `f6_8` |  | both | pending | Limit violations appear separately in reporting by rejection reason type |
| W1 | F6.9 | F6 | Alice | `f6_9` |  | both | pending | cc-lb does not go to external hosts outside the allow list |
| W1 | F6.10a | F6 | Alice | `f6_10a` |  | both | pending | The body-size limit for a specific path applies to calls on that path |
| W1 | F6.10b | F6 | Alice | `f6_10b` |  | both | pending | Changing the body limit for one path does not affect the body limit for another path |
| W1 | F6.12 | F6 | Alice | `f6_12` |  | both | pending | A team temporarily rejected for a limit violation recovers after time passes |
| W1 | F6.13 | F6 | Alice | `f6_13` |  | both | pending | A team that used up its daily limit is automatically accepted again at the same time next day |
| W1 | F6.14a | F6 | Alice | `f6_14a` |  | both | pending | One team's per-minute limit violation does not affect another team's call acceptance |
| W1 | F6.14b | F6 | Alice | `f6_14b` |  | both | pending | One team's surge does not delay another team's recovery point |
| W1 | F6.15 | F6 | Alice | `f6_15` |  | both | pending | A separate limit can be set on the number of calls being processed at the same time |
| W1 | F6.16 | F6 | Alice | `f6_16` |  | both | pending | The enforcement unit changes depending on whether the limit targets team, holder, or key |
| W1 | F6.17 | F6 | Alice | `f6_17` |  | both | pending | A smaller limit can be set separately for only one key |
| W1 | F19.1 | F19 | Alice | `f19_1` |  | both | pending | Calls with the same meaning converge to the same place on the second call |
| W1 | F19.2 | F19 | Alice | `f19_2` |  | both | pending | Cache hit rate is visible at a glance on the same screen |
| W1 | F19.3a | F19 | Alice | `f19_3a` |  | both | pending | Cost saved through cache hits is shown separately in cost reporting |
| W1 | F19.3b | F19 | Alice | `f19_3b` |  | both | pending | "Cost without cache" is also shown with a virtual cost label |
| W1 | F19.3c | F19 | Alice | `f19_3c` |  | both | pending | Actual cost and virtual cost are compared as an hourly flow |
| W1 | F19.4 | F19 | Alice | `f19_4` |  | both | pending | When cache affinity breaks, the call is processed as new |
| W1 | F19.5 | F19 | Alice | `f19_5` |  | both | pending | Cache affinity does not cross team boundaries |
| W1 | F19.6a | F19 | Alice | `f19_6a` |  | both | pending | Even while cache affinity works, limit and model bundle violations are still rejected |
| W1 | F19.6b | F19 | Alice | `f19_6b` |  | both | pending | Cache hit rate is calculated excluding rejected calls |
| W1 | F19.7 | F19 | Alice | `f19_7` |  | both | pending | Short-lived cache and long-lived cache appear separated by tier |
| W1 | F19.8 | F19 | Alice | `f19_8` |  | both | pending | Calls from an inactive team are not accepted even through a cache hit |
| W1 | F19.9 | F19 | Alice | `f19_9` |  | both | pending | One call's cache state is visible at a glance with clear classification |
| W1 | F19.10 | F19 | Alice | `f19_10` |  | both | pending | When a cache hit has a cost unit mismatch from the first call, it is marked separately |
| W1 | F26.1 | F26 | Charlie | `f26_1` | ✓ | both | pending | Liveness and readiness to process are shown separately |
| W1 | F26.2a | F26 | Charlie | `f26_2a` |  | both | pending | When readiness to process cannot recover, it keeps answering "not ready" |
| W1 | F26.2b | F26 | Charlie | `f26_2b` |  | both | pending | While readiness is "not ready", the liveness signal remains separate |
| W1 | F26.2c | F26 | Charlie | `f26_2c` |  | both | pending | Automatic recovery attempts behind the scenes appear as a marker on the operator screen |
| W1 | F26.3 | F26 | Charlie | `f26_3` |  | both | pending | The liveness body shows version, uptime, and build marker together |
| W1 | F26.4 | F26 | Charlie | `f26_4` |  | both | pending | When a notification subscription disconnects and reconnects, it catches up with changes from the gap |
| W1 | F26.5a | F26 | Charlie | `f26_5a` |  | both | pending | cc-lb announces that it restarted with a clear marker |
| W1 | F26.5b | F26 | Charlie | `f26_5b` |  | both | pending | The restart marker remains in audit with time |
| W1 | F26.6 | F26 | Charlie | `f26_6` |  | both | pending | The current number of in-progress calls appears as a gauge on the same screen |
| W1 | F26.7 | F26 | Charlie | `f26_7` |  | both | pending | During graceful shutdown, liveness and readiness answer separately |
| W2 | F5.1 | F5 | Charlie | `f5_1` | ✓ | both | pending | cc-lb automatically rotates a credential that is close to expiry |
| W2 | F5.2 | F5 | Charlie | `f5_2` |  | both | pending | cc-lb notifies the operator when automatic rotation fails repeatedly (v4 F5.2 split — notification rule) |
| W2 | F5.3 | F5 | Charlie | `f5_3` |  | both | pending | cc-lb increases the backoff interval when automatic rotation fails repeatedly (v4 F5.2 split — backoff rule) |
| W2 | F5.4 | F5 | Charlie | `f5_4` | ✓ | both | pending | A malformed credential is rejected at registration time |
| W2 | F5.5 | F5 | Charlie | `f5_5` | ✓ | both | pending | Revoking a credential immediately stops all calls that used it |
| W2 | F5.6 | F5 | Charlie | `f5_6` |  | both | pending | Audit records for a revoked credential remain intact |
| W2 | F5.7 | F5 | Charlie | `f5_7` |  | both | pending | The operator is notified when the permission on the credential protection key drifts (new P3) |
| W2 | F5.8 | F5 | Charlie | `f5_8` |  | both | pending | The state of a credential is displayed in a human-readable form in a single view (new P3) |
| W2 | F5.9 | F5 | Charlie | `f5_9` |  | both | pending | When two people try to edit the same credential simultaneously, only one is accepted (new P3) |
| W2 | F7.1 | F7 | Charlie | `f7_1` | ✓ | both | pending | Activating the emergency killswitch causes all calls to be rejected |
| W2 | F7.2 | F7 | Charlie | `f7_2` | ✓ | both | pending | Deactivating the emergency killswitch restores normal operation |
| W2 | F7.3 | F7 | Charlie | `f7_3` |  | both | pending | The emergency killswitch state persists across a cc-lb restart |
| W2 | F7.4 | F7 | Charlie | `f7_4` |  | both | pending | The management screen and dashboard remain operational during an emergency killswitch |
| W2 | F7.5 | F7 | Charlie | `f7_5` | ✓ | both | pending | Responses rejected by the killswitch clearly indicate an operator-imposed block |
| W2 | F7.6 | F7 | Charlie | `f7_6` |  | both | pending | Activation and deactivation require a two-step confirmation |
| W2 | F7.7 | F7 | Charlie | `f7_7` |  | both | pending | The reason for each killswitch activation and deactivation is subject to audit (new P3) |
| W2 | F8.1 | F8 | Charlie | `f8_1` |  | both | pending | When the upstream slows temporarily, users are notified of the delay (v4 F8.1 split — user UX rule) |
| W2 | F8.2 | F8 | Charlie | `f8_2` |  | both | pending | When the upstream slows, the reason is shown on the operator dashboard (v4 F8.1 split — operator visibility rule) |
| W2 | F8.3 | F8 | Charlie | `f8_3` |  | both | pending | Anthropic's rate-limit-exceeded response is passed through to users as-is |
| W2 | F8.4 | F8 | Charlie | `f8_4` |  | both | pending | When the same credential fails consecutively, that credential alone is temporarily blocked |
| W2 | F8.5 | F8 | Charlie | `f8_5` |  | both | pending | New calls during the temporary block are rejected quickly |
| W2 | F8.6 | F8 | Charlie | `f8_6` |  | both | pending | When Anthropic responds with 5xx, users are informed of the temporary outage |
| W2 | F8.7 | F8 | Charlie | `f8_7` | ✓ | both | pending | When one upstream goes down, traffic is automatically rerouted to another upstream |
| W2 | F8.8 | F8 | Charlie | `f8_8` |  | both | pending | When all upstreams go down simultaneously, a consistent response is returned |
| W2 | F8.9 | F8 | Charlie | `f8_9` |  | both | pending | When the routing trace becomes too long, it is shown with a truncation indicator |
| W2 | F8.10 | F8 | Charlie | `f8_10` |  | both | pending | When backpressure is applied, new calls are rejected gracefully |
| W2 | F8.11 | F8 | Charlie | `f8_11` |  | both | pending | Each upstream has its own concurrent call bulkhead (new P3) |
| W2 | F8.12 | F8 | Charlie | `f8_12` |  | both | pending | Only idempotent calls are retried automatically (new P3) |
| W2 | F8.13 | F8 | Charlie | `f8_13` |  | both | pending | Rate-limit guidance headers from Anthropic are passed through to users as-is (new P3) |
| W2 | F10.1 | F10 | Alice | `f10_1` | ✓ | both | pending | The operator consents to Anthropic through a browser |
| W2 | F10.2 | F10 | Alice | `f10_2` |  | both | pending | The callback is accepted only after cc-lb validates it |
| W2 | F10.3 | F10 | Alice | `f10_3` |  | both | pending | When consent completes, the credential is registered and marked active |
| W2 | F10.4 | F10 | Alice | `f10_4` |  | both | pending | An invalid callback address is rejected |
| W2 | F10.5 | F10 | Alice | `f10_5` |  | both | pending | A missing or tampered session marker is rejected |
| W2 | F10.6 | F10 | Alice | `f10_6` |  | both | pending | If the operator cancels consent, no credential is created |
| W2 | F10.7 | F10 | Alice | `f10_7` |  | both | pending | Two operators conducting OAuth consent simultaneously do not interfere with each other (new P3) |
| W2 | F10.8 | F10 | Alice | `f10_8` |  | both | pending | A consent session expires after a set period of time (new P3) |
| W2 | F11A.1 | F11A | Charlie | `f11a_1` | ✓ | both | pending | Current usage against the 5-hour quota is visible |
| W2 | F11A.2 | F11A | Charlie | `f11a_2` |  | both | pending | Current usage against the 7-day quota is visible |
| W2 | F11A.3 | F11A | Charlie | `f11a_3` |  | both | pending | The base quota and overage quota are displayed separately (v4 F11.14 split — distinction display rule) |
| W2 | F11A.4 | F11A | Charlie | `f11a_4` |  | both | pending | The fact that the overage quota has been entered is explicitly shown to the operator (v4 F11.14 split — entry state rule) |
| W2 | F11A.5 | F11A | Charlie | `f11a_5` |  | both | pending | A warning is shown on screen when usage approaches 80% |
| W2 | F11A.6 | F11A | Charlie | `f11a_6` |  | both | pending | The operator refreshes quota metadata immediately |
| W2 | F11A.7 | F11A | Charlie | `f11a_7` |  | both | pending | The operator chooses the quota aggregation mode |
| W2 | F11A.8 | F11A | Charlie | `f11a_8` |  | both | pending | Usage by time slot within the 5-hour window is shown separately (new P3) |
| W2 | F11A.9 | F11A | Charlie | `f11a_9` |  | both | pending | Usage restrictions attached to a credential are displayed in a human-readable form (new P3) |
| W2 | F11A.10 | F11A | Charlie | `f11a_10` |  | both | pending | When the quota is exhausted, the shortfall is shown to the operator (new P3) |
| W2 | F11A.11 | F11A | Charlie | `f11a_11` |  | both | pending | The last known quota value is temporarily retained after a credential is deleted (new P3) |
| W2 | F11B.1 | F11B | Charlie | `f11b_1` |  | both | pending | Anthropic is signaled periodically to keep the quota active |
| W2 | F11B.2 | F11B | Charlie | `f11b_2` |  | both | pending | Warmup calls are not counted toward usage or cost |
| W2 | F11B.3 | F11B | Charlie | `f11b_3` |  | both | pending | When Anthropic signals to back off, the polling interval is increased |
| W2 | F11B.4 | F11B | Charlie | `f11b_4` |  | both | pending | Only one of multiple replica nodes performs warmup |
| W2 | F11B.5 | F11B | Charlie | `f11b_5` |  | both | pending | When warmup fails, the next attempt uses a backoff interval |
| W2 | F11B.6 | F11B | Charlie | `f11b_6` |  | both | pending | The operator views warmup status on screen |
| W2 | F11B.7 | F11B | Charlie | `f11b_7` |  | both | pending | Changes that occurred while the subscription was disconnected are caught up by the reconciler after reconnection |
| W2 | F11B.8 | F11B | Charlie | `f11b_8` |  | both | pending | Warmup is suspended during an emergency killswitch (new P3) |
| W2 | F11B.9 | F11B | Charlie | `f11b_9` |  | both | pending | The operator views the warmup target upstream and next attempt time (new P3) |
| W2 | F11B.10 | F11B | Charlie | `f11b_10` |  | both | pending | The operator is shown that warmup applies only to OAuth credentials (new P3) |
| W2 | F11B.11 | F11B | Charlie | `f11b_11` |  | both | pending | Upstream address changes at Anthropic are shown to the operator and calls are not interrupted (new P3) |
| W2 | F11C.1 | F11C | Charlie | `f11c_1` |  | both | pending | The compatibility cache is automatically refreshed on a one-hour cycle |
| W2 | F11C.2 | F11C | Charlie | `f11c_2` |  | both | pending | Organization metadata is stored in a way that allows quota differences to be traced |
| W2 | F11C.3 | F11C | Charlie | `f11c_3` |  | both | pending | The operator manually refreshes subscription metadata |
| W2 | F11C.4 | F11C | Charlie | `f11c_4` |  | both | pending | Process restart markers are not mistaken for quota spikes |
| W2 | F11C.5 | F11C | Charlie | `f11c_5` |  | both | pending | When a compatibility cache refresh fails, the previous value is retained (v4 F11.26 split — stale value retention rule) |
| W2 | F11C.6 | F11C | Charlie | `f11c_6` |  | both | pending | The time of the last successful compatibility cache refresh is shown to the operator (v4 F11.26 split — freshness display rule) |
| W2 | F11C.7 | F11C | Charlie | `f11c_7` |  | both | pending | The last attempt time and last success time are shown separately (new P3) |
| W3 | F9.1 | F9 | Alice | `f9_1` | ✓ | both | pending | Attaching a policy to a team applies it immediately to subsequent calls |
| W3 | F9.2 | F9 | Alice | `f9_2` |  | both | pending | Detaching a policy only affects subsequent calls |
| W3 | F9.3 | F9 | Alice | `f9_3` | ✓ | both | pending | Malformed policies are rejected during the save phase |
| W3 | F9.4 | F9 | Alice | `f9_4` |  | both | pending | Policy changes apply to subsequent calls without restarting cc-lb |
| W3 | F9.5 | F9 | Alice | `f9_5` |  | both | pending | Policy changes for one team do not affect calls from other teams |
| W3 | F9.6 | F9 | Alice | `f9_6` |  | both | pending | Global rules apply first, followed by team rules |
| W3 | F9.8 | F9 | Alice | `f9_8` |  | both | pending | Operators can validate the flow before attaching a policy |
| W3 | F12.1 | F12 | Bob | `f12_1` | ✓ | both | pending | Uploading a plugin with the same signature twice rejects the second upload |
| W3 | F12.2 | F12 | Bob | `f12_2` |  | both | pending | Plugins are managed and distinguished by label and version |
| W3 | F12.3 | F12 | Bob | `f12_3` |  | both | pending | Reordering the plugin queue applies the new order to subsequent calls |
| W3 | F12.4 | F12 | Bob | `f12_4` | ✓ | both | pending | Plugins currently in use cannot be deleted |
| W3 | F12.5 | F12 | Bob | `f12_5` |  | both | pending | Exceeding the maximum number of plugins in a plugin queue is rejected |
| W3 | F12.6 | F12 | Bob | `f12_6` |  | both | pending | The number of plugins that can be uploaded per minute is limited |
| W3 | F12.7 | F12 | Bob | `f12_7` |  | both | pending | Currently cc-lb only accepts plugins for response shaping slots |
| W3 | F12.8 | F12 | Bob | `f12_8` |  | both | pending | Plugins with unverified signatures are rejected during registration |
| W3 | F12.9 | F12 | Bob | `f12_9` |  | both | pending | A plugin exhausting its resource limit does not affect other plugins |
| W3 | F12.10a | F12 | Bob | `f12_10a` |  | both | pending | The old rule set is used during the preparation phase of a plugin queue change |
| W3 | F12.10b | F12 | Bob | `f12_10b` |  | both | pending | The new rule set is applied all at once at the application point |
| W3 | F12.10c | F12 | Bob | `f12_10c` |  | both | pending | Unused old attachments are cleaned up from the repository after application |
| W3 | F12.11 | F12 | Bob | `f12_11` |  | both | pending | Operators can insert a new plugin at a specified position in the plugin queue |
| W3 | F12.12 | F12 | Bob | `f12_12` |  | both | pending | Changes are rejected if slot types do not match during pre-application checks |
| W3 | F12.13 | F12 | Bob | `f12_13` |  | both | pending | Unattached plugin queue items are displayed in a separate list |
| W3 | F12.14 | F12 | Bob | `f12_14` |  | both | pending | Operators can reorder all items in the plugin queue at once |
| W3 | F12.15 | F12 | Bob | `f12_15` |  | both | pending | Attaching a second plugin to a single-capacity slot is rejected |
| W3 | F21.1 | F21 | Alice | `f21_1` |  | both | pending | Each call is counted separately by principal and model |
| W3 | F21.2 | F21 | Alice | `f21_2` |  | both | pending | The start, completion, or error of a call is recorded exactly once |
| W3 | F21.3 | F21 | Alice | `f21_3` |  | both | pending | The number of partial transmissions for streaming responses is aggregated on the operator dashboard |
| W3 | F21.4 | F21 | Alice | `f21_4` |  | both | pending | Authentication failures are counted by failure reason |
| W3 | F21.5 | F21 | Alice | `f21_5` |  | both | pending | Calls dropped due to backpressure are recorded on the operator dashboard |
| W3 | F21.7 | F21 | Alice | `f21_7` |  | both | pending | The recipient of the response also sees the call identifier |
| W3 | F21.8 | F21 | Alice | `f21_8` |  | both | pending | The same event is transmitted to an external observability tool configured by the operator |
| W3 | F21.9 | F21 | Alice | `f21_9` |  | both | pending | The call identifier is identical across audit logs, operator logs, external traces, and responses |
| W3 | F21.11 | F21 | Alice | `f21_11` |  | both | pending | The duration of each call phase is included in the usage report |
| W3 | F21.12 | F21 | Alice | `f21_12` |  | both | pending | Failure to transmit events to an external observability tool does not affect call processing |
| W3 | F21.13 | F21 | Alice | `f21_13` |  | both | pending | Dropped batches are displayed separately when the observability event queue is full |
| W3 | F21.14 | F21 | Alice | `f21_14` |  | both | pending | One team's observability chain exhausting its resources does not affect other teams' observability chains |
| W3 | F25.1 | F25 | Bob | `f25_1` | ✓ | both | pending | Plugins built within the plugin format supported by cc-lb are accepted |
| W3 | F25.2 | F25 | Bob | `f25_2` |  | both | pending | Plugins with unsupported formats are rejected |
| W3 | F25.3 | F25 | Bob | `f25_3` |  | both | pending | Plugins with identical content share the same signature |
| W3 | F25.5 | F25 | Bob | `f25_5` |  | both | pending | Plugins failing pre-checks are blocked during registration |
| W3 | F25.7 | F25 | Bob | `f25_7` |  | both | pending | Authors can predefine fallback behaviors for each function on failure |
| W3 | F25.8 | F25 | Bob | `f25_8` |  | both | pending | The most compatible generation is negotiated when cc-lb supports multiple plugin format generations |
| W3 | F25.9 | F25 | Bob | `f25_9` |  | both | pending | Authors communicate with the external environment only through auxiliary functions provided by cc-lb |
| W3 | F25.11 | F25 | Bob | `f25_11` |  | both | pending | Registered plugins display their name, version, and capabilities to the operator |
| W3 | F25.12 | F25 | Bob | `f25_12` |  | both | pending | Having many registered plugins does not delay booting during cc-lb restart |
| W3 | F25.13 | F25 | Bob | `f25_13` |  | both | pending | If the core phase of a plugin exceeds the designated time, the call completes with a fallback behavior |
| W3 | F25.14 | F25 | Bob | `f25_14` |  | both | pending | Secrets are masked at the cc-lb boundary before being passed to plugins |
| W3 | F25.15 | F25 | Bob | `f25_15` |  | both | pending | Plugins requesting negotiation for a lower generation than supported are rejected |
| W3 | F25.16 | F25 | Bob | `f25_16` |  | both | pending | Plugins missing capabilities required by a slot are rejected |
| W3 | F25.17 | F25 | Bob | `f25_17` |  | both | pending | Plugins with format generations outside the range accepted by cc-lb are rejected |
| W3 | F27.3 | F27 | Bob | `f27_3` |  | both | pending | Rapid plugin upload spikes within a short timeframe are temporarily throttled |
| W3 | F27.4 | F27 | Bob | `f27_4` |  | both | pending | Emergency shutdown takes effect only after two-step verification |
| W3 | F27.5 | F27 | Bob | `f27_5` |  | both | pending | Admin sessions require re-authentication before high-risk operations after a certain period of inactivity |
| W3 | F27.6 | F27 | Bob | `f27_6` |  | both | pending | Admin requests originating from other sources are not executed unintentionally |
| W3 | F27.7 | F27 | Bob | `f27_7` |  | both | pending | Admin token values are never displayed in plaintext anywhere on the operator screen |
| W3 | F29.1 | F29 | Charlie | `f29_1` |  | both | pending | Secrets are masked even in sudden crash messages |
| W3 | F29.2 | F29 | Charlie | `f29_2` |  | both | pending | Operators intentionally inject faults to test resilience |
| W3 | F29.3a | F29 | Charlie | `f29_3a` |  | both | pending | Calls complete with a designated fallback behavior even if the external connection is suddenly lost |
| W3 | F29.3b | F29 | Charlie | `f29_3b` |  | both | pending | Fallback behavior due to external connection loss is recorded in operator logs and audit logs with the same call identifier |
| W3 | F29.4 | F29 | Charlie | `f29_4` |  | both | pending | Ongoing tracking continues even if the limit engine undergoes a cold restart |
| W3 | F29.5 | F29 | Charlie | `f29_5` |  | both | pending | Limiting fault injection to a single team does not affect calls from other teams |
| W3 | F29.6 | F29 | Charlie | `f29_6` |  | both | pending | Enabling and disabling fault injection is recorded in the audit log |
| W3 | F29.7 | F29 | Charlie | `f29_7` |  | both | pending | Fault injection occurs only at predefined points |
| W4 | F13.1 | F13 | Dana | `f13_1` | ✓ | both | pending | The auditor views one operator's branch changes in chronological order |
| W4 | F13.2 | F13 | Dana | `f13_2` |  | both | pending | The audit record contains no secret information on any line |
| W4 | F13.3 | F13 | Dana | `f13_3` |  | both | pending | No operator can delete or edit an audit record once it is written |
| W4 | F13.4 | F13 | Dana | `f13_4` |  | both | pending | The auditor narrows by time window and page |
| W4 | F13.5 | F13 | Dana | `f13_5` |  | both | pending | Only records past the retention period are swept precisely |
| W4 | F13.6 | F13 | Dana | `f13_6` |  | both | pending | The start, end, and error of one call are grouped by the same trace marker |
| W4 | F13.7 | F13 | Dana | `f13_7` |  | both | pending | Audit records are exported to an external audit system |
| W4 | F13.8 | F13 | Dana | `f13_8` |  | both | pending | One call's trace marker is printed in audit, operation logs, and the response |
| W4 | F13.9 | F13 | Dana | `f13_9` |  | both | pending | The exported audit file includes tamper evidence (new P3) |
| W4 | F13.10 | F13 | Dana | `f13_10` |  | both | pending | The auditor views all related records for one call in a cross table (new P3) |
| W4 | F13.11 | F13 | Dana | `f13_11` |  | both | pending | Cost and quota violation appear together on the same call line (new P3, split 1/2) |
| W4 | F13.12 | F13 | Dana | `f13_12` |  | both | pending | Cost and quota violations are aggregated into the branch report by the same call unit (new P3, split 2/2) |
| W4 | F14.1 | F14 | Alice | `f14_1` | ✓ | both | pending | Saving a draft does not yet affect actual behavior |
| W4 | F14.2 | F14 | Alice | `f14_2` |  | both | pending | Validating a draft exposes problems before apply |
| W4 | F14.3 | F14 | Alice | `f14_3` | ✓ | both | pending | Invalid configuration is rejected at save time |
| W4 | F14.4 | F14 | Alice | `f14_4` | ✓ | both | pending | Only a draft that passed validation is applied (split 1/2) |
| W4 | F14.5 | F14 | Alice | `f14_5` |  | both | pending | An applied configuration change is recorded as one line in apply history (split 2/2) |
| W4 | F14.6 | F14 | Alice | `f14_6` |  | both | pending | The operator views apply history in time order |
| W4 | F14.7 | F14 | Alice | `f14_7` |  | both | pending | The operator rolls back to a previous configuration version |
| W4 | F14.8 | F14 | Alice | `f14_8` |  | both | pending | Items that can apply without restart apply without restart |
| W4 | F14.9 | F14 | Alice | `f14_9` |  | both | pending | Items that require restart are marked before apply |
| W4 | F14.10 | F14 | Alice | `f14_10` |  | both | pending | The operator downloads the applied configuration as a file |
| W4 | F14.11 | F14 | Alice | `f14_11` |  | both | pending | Bootstrap configuration is reflected only once on first boot |
| W4 | F14.12 | F14 | Alice | `f14_12` |  | both | pending | Unapplied drafts are automatically cleared after the configured period (new P3) |
| W4 | F14.13 | F14 | Alice | `f14_13` |  | both | pending | Items requiring restart are clearly marked item by item (new P3) |
| W4 | F14.14 | F14 | Alice | `f14_14` |  | both | pending | cc-lb notices when the configuration file on disk changes (new P3) |
| W4 | F15.1 | F15 | Charlie | `f15_1` |  | both | pending | New calls are not accepted after the shutdown signal |
| W4 | F15.2 | F15 | Charlie | `f15_2` |  | both | pending | Calls already in progress complete even during drain |
| W4 | F15.3 | F15 | Charlie | `f15_3` |  | both | pending | When the drain wait time expires, remaining calls are forced to end and reported |
| W4 | F15.4 | F15 | Charlie | `f15_4` |  | both | pending | Ongoing streams are not disconnected when a new certificate applies |
| W4 | F15.5 | F15 | Charlie | `f15_5` |  | both | pending | Invalid certificates are rejected before apply (split 1/2) |
| W4 | F15.6 | F15 | Charlie | `f15_6` |  | both | pending | The reason an invalid certificate was rejected is shown to the operator on one line (split 2/2) |
| W4 | F15.7 | F15 | Charlie | `f15_7` |  | both | pending | Operators are notified before certificate expiration is near |
| W4 | F15.8 | F15 | Charlie | `f15_8` |  | both | pending | Temporary debug logging turns off automatically after the configured time |
| W4 | F15.9 | F15 | Charlie | `f15_9` |  | both | pending | Shutdown semantics are separated by shutdown signal kind (new P3) |
| W4 | F15.10 | F15 | Charlie | `f15_10` |  | both | pending | Debug logging is enabled only by the configured operation signal (new P3) |
| W4 | F15.11 | F15 | Charlie | `f15_11` |  | both | pending | The operation-only socket rejects access from external callers (new P3, split 1/3) |
| W4 | F15.12 | F15 | Charlie | `f15_12` |  | both | pending | The operation-only socket normally receives management calls from an operator on the same machine (new P3, split 2/3) |
| W4 | F15.13 | F15 | Charlie | `f15_13` |  | both | pending | The external caller port and operation-only socket stay separated during drain (new P3, split 3/3) |
| W4 | F15.14 | F15 | Charlie | `f15_14` |  | both | pending | Other replicas retain a marker that one replica completed shutdown (new P3) |
| W4 | F17.1 | F17 | Charlie | `f17_1` | ✓ | both | pending | A change on one replica is immediately announced to another replica |
| W4 | F17.2 | F17 | Charlie | `f17_2` |  | both | pending | Only one replica among many performs the work - credential warmup |
| W4 | F17.3 | F17 | Charlie | `f17_3` |  | both | pending | If the replica performing work disappears, another replica takes over |
| W4 | F17.4 | F17 | Charlie | `f17_4` |  | both | pending | All replicas see the same value at the same time for a configuration change |
| W4 | F17.5 | F17 | Charlie | `f17_5` |  | both | pending | If two operators try to edit the same line at the same time, only one succeeds |
| W4 | F17.6 | F17 | Charlie | `f17_6` |  | both | pending | If two replicas both believe they hold the same lease, only one continues the work (new P3, split 1/2) |
| W4 | F17.7 | F17 | Charlie | `f17_7` |  | both | pending | Lease self-check results remain in operation logs and metrics (new P3, split 2/2) |
| W4 | F17.8 | F17 | Charlie | `f17_8` |  | both | pending | The operator sees which replicas are alive on one screen (new P3) |
| W4 | F17.9 | F17 | Charlie | `f17_9` |  | both | pending | If a replica identifier is damaged, a new identifier is issued safely (new P3, split 1/3) |
| W4 | F17.10 | F17 | Charlie | `f17_10` |  | both | pending | Work tied to the old identifier is handed off to another replica or expires (new P3, split 2/3) |
| W4 | F17.11 | F17 | Charlie | `f17_11` |  | both | pending | The old identifier and new identifier are not used at the same time for the same call (new P3, split 3/3) |
| W4 | F17.12 | F17 | Charlie | `f17_12` |  | both | pending | The same operator scenario produces the same result on two storage backends (F22-merged) |
| W4 | F17.13 | F17 | Charlie | `f17_13` |  | both | pending | All consistency scenarios pass on both storage backends (F22-merged) |
| W4 | F17.14 | F17 | Charlie | `f17_14` |  | both | pending | Boot is clearly rejected if the storage backend kind is changed incorrectly (F22-merged) |
| W4 | F17.15 | F17 | Charlie | `f17_15` |  | both | pending | The same operator action leaves the same number and kinds of audit lines on both storage backends (F22-merged, split 1/2) |
| W4 | F17.16 | F17 | Charlie | `f17_16` |  | both | pending | Secrets are not visible in plaintext in audit records on either storage backend (F22-merged, split 2/2) |
| W4 | F17.17 | F17 | Charlie | `f17_17` |  | both | pending | Downgrade migration is rejected at boot (F22-merged) |
| W4 | F17.18 | F17 | Charlie | `f17_18` |  | both | pending | Concurrent edits of the same line by two operators make the same decision on both storage backends (new P3) |
| W4 | F18.1 | F18 | Dana | `f18_1` |  | both | pending | The operator views current prices by model on one screen |
| W4 | F18.2 | F18 | Dana | `f18_2` | ✓ | both | pending | Per-call cost is calculated exactly from catalog prices |
| W4 | F18.3 | F18 | Dana | `f18_3` |  | both | pending | The branch cost report aggregates accurately by team |
| W4 | F18.4 | F18 | Dana | `f18_4` |  | both | pending | When a price changes, the new price applies starting with new calls after that point |
| W4 | F18.5 | F18 | Dana | `f18_5` |  | both | pending | A cache-hit call shows savings as a separate item |
| W4 | F18.6 | F18 | Dana | `f18_6` |  | both | pending | Price change history is preserved in time order (split 1/2) |
| W4 | F18.7 | F18 | Dana | `f18_7` |  | both | pending | Historical call cost calculation matches the price history unit price at that point (split 2/2) |
| W4 | F18.8 | F18 | Dana | `f18_8` |  | both | pending | The call that first creates a cache and the call that rereads the cache are priced separately (new P3, split 1/2) |
| W4 | F18.9 | F18 | Dana | `f18_9` |  | both | pending | The aggregate of cache creation and reuse unit prices differs by not even one token from the branch report total (new P3, split 2/2) |
| W4 | F18.10 | F18 | Dana | `f18_10` |  | both | pending | When token count cannot be exact, it is shown with an estimated marker (new P3, split 1/2) |
| W4 | F18.11 | F18 | Dana | `f18_11` |  | both | pending | Calls reported as estimates provide a reason record and separate query (new P3, split 2/2) |
| W4 | F18.12 | F18 | Dana | `f18_12` |  | both | pending | Previous usage remains visible after an upstream is renamed (F23-merged) |
| W4 | F18.13 | F18 | Dana | `f18_13` |  | both | pending | Previous usage is preserved after an upstream is deleted (F23-merged) |
| W4 | F18.14 | F18 | Dana | `f18_14` |  | both | pending | Usage is aggregated accurately when two upstreams are merged into one (F23-merged) |
| W4 | F18.15 | F18 | Dana | `f18_15` |  | both | pending | The usage report also shows upstream renames (F23-merged) |
| W4 | F20.1 | F20 | Dana | `f20_1` | ✓ | both | pending | No stored secret remains as plaintext on disk |
| W4 | F20.2 | F20 | Dana | `f20_2` |  | both | pending | Tampering is detected immediately |
| W4 | F20.3 | F20 | Dana | `f20_3` |  | both | pending | If the master key disappears, secrets can never be recovered - intended behavior |
| W4 | F20.4 | F20 | Dana | `f20_4` |  | both | pending | cc-lb rejects boot itself with an invalid master key (split 1/2) |
| W4 | F20.5 | F20 | Dana | `f20_5` |  | both | pending | No secret is ever read as plaintext during an invalid-master-key boot attempt (split 2/2) |
| W4 | F20.6 | F20 | Dana | `f20_6` |  | both | pending | Secrets stored before key rotation are still read after key rotation |
| W4 | F20.7 | F20 | Dana | `f20_7` |  | both | pending | Boot is rejected if the master key file permissions are too loose |
| W4 | F20.8 | F20 | Dana | `f20_8` |  | both | pending | Values following a known-position pattern for secrets are shown as redacted markers by kind |
| W4 | F20.9 | F20 | Dana | `f20_9` |  | both | pending | Secret information appears only as redacted markers even in abnormal termination messages |
| W4 | F20.10 | F20 | Dana | `f20_10` |  | both | pending | Master key rotation does not disconnect ongoing calls (new P3) |
| W4 | F20.11 | F20 | Dana | `f20_11` |  | both | pending | Variable values in abnormal termination traces also show secrets only as redacted markers (new P3) |
| W4 | F20.12 | F20 | Dana | `f20_12` |  | both | pending | Values following a known-position pattern for secrets are shown only as redacted markers in response headers returned to the caller (new P3) |
| W4 | F20.13 | F20 | Dana | `f20_13` |  | both | pending | The same secret stored on different upstreams cannot be decrypted for each other (new P3) |
| W4 | F24.1 | F24 | Alice | `f24_1` |  | both | pending | New prices are fetched when the external pricing source is healthy |
| W4 | F24.2 | F24 | Alice | `f24_2` |  | both | pending | The last prices are still used when the external pricing source has a temporary failure |
| W4 | F24.3 | F24 | Alice | `f24_3` |  | both | pending | The operator is alerted when the external pricing source stays down for a long time |
| W4 | F24.4 | F24 | Alice | `f24_4` |  | both | pending | After three pricing source failures, disk-stored prices are used, and if those are also absent, cost reporting is held |
| W4 | F24.5 | F24 | Alice | `f24_5` |  | both | pending | If the whole pricing catalog is damaged, that fact is reported safely |
| W4 | F24.6 | F24 | Alice | `f24_6` |  | both | pending | The operator views the last refresh time of the pricing catalog |
| W4 | F24.7 | F24 | Alice | `f24_7` |  | both | pending | New prices are not used if the pricing catalog provenance proof does not match (new P3) |
| W4 | F24.8 | F24 | Alice | `f24_8` |  | both | pending | Calls to models absent from the catalog are reported using the configured fallback method (new P3) |
