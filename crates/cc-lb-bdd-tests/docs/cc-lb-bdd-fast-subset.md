# BDD Fast Subset (M0 deliverable, plan v3)

- Date: 2026-06-18
- Basis: plan v3 §0 (Fast subset), §10 (CI), §11 (Milestones)
- Usage: PR gate (`.github/workflows/bdd.yml`). Nightly = full run.
- nextest filter: `cargo nextest run -p cc-lb-bdd-tests -E 'test(/^fast_/)'`
- Function name prefix: `fast_<fn_name>_<backend>` (e.g., `fast_f1_1a_sqlite`, `fast_f1_1a_postgres`)

## Selection Criteria

A scenario is a fast subset candidate if it meets **two or more** of the following conditions:
1. **Happy path**: first normal case of a core flow (e.g., `Fn.1`, `Fn.1a`)
2. **Security gate**: rejection, authentication failure, or permission blocking (e.g., F1.4 inactive team rejection, F1.5 model ACL, F5.5 credential revocation)
3. **Low cost**: no time-dependent sleep, chaos, or retry loops. Scenarios that complete in a single call
4. **Broad coverage**: includes 1 or 2 representative scenarios from each feature to prevent skew

The following scenarios are **excluded from the fast subset**:
- Time-dependent (e.g., F5.2 renewal failure notification, F5.3 backoff increase, F2.4a/b automatic expiration, F2.2 rotation overlap)
- Chaos or failure-injection (e.g., F29.x, F8.6 5xx sequence, F8.4 consecutive failures)
- Multi-instance or replica coordination (e.g., F17.x, postgres only nightly)
- Large data or pagination (e.g., F13.4 page-by-page, F18 quarterly report)
- OAuth refresh full cycle (M0 mock-anthropic-oauth-server takes 5s+)

## Fast Subset List (33 scenarios, about 10.3% of 321)

| ID | Writer | Feature | Persona | fn_name | v5.2 title | Selection Reason |
|---|---|---|---|---|---|---|
| F1.1a | W1 | F1 | Alice | `f1_1a` | New team registration finishes active state and the first key on one screen | happy path + broad |
| F1.1b | W1 | F1 | Alice | `f1_1b` | New team registration records who created it and when in audit | happy path + audit baseline |
| F1.4 | W1 | F1 | Alice | `f1_4` | Calls from an inactive team are not accepted | security gate |
| F1.5 | W1 | F1 | Alice | `f1_5` | Calls outside the model range allowed for a team are rejected | security gate (ACL) |
| F1.7 | W1 | F1 | Alice | `f1_7` | Deleting a team does not erase audit traces of what that team did | audit invariant |
| F2.1 | W1 | F2 | Alice | `f2_1` | When a new key is issued, the secret is shown only once | security gate (secret expose-once) |
| F2.3 | W1 | F2 | Alice | `f2_3` | Immediately after key revocation, the next calls are rejected | security gate (revocation latency) |
| F2.5 | W1 | F2 | Alice | `f2_5` | Secrets are never shown on the key list screen | security gate (no-leak) |
| F3.1 | W1 | F3 | Bob | `f3_1` | Calling a normal model with a normal key returns the response unchanged | happy path (E2E proxy) |
| F3.3 | W1 | F3 | Bob | `f3_3` | A call sent with an unknown key is politely rejected | security gate (auth fail) |
| F3.4 | W1 | F3 | Bob | `f3_4` | A call sent with a key from an inactive team is rejected | security gate (inactive principal) |
| F26.1 | W1 | F26 | Charlie | `f26_1` | Liveness and readiness to process are shown separately | broad (liveness/readiness) |
| F5.1 | W2 | F5 | Charlie | `f5_1` | cc-lb automatically rotates a credential that is close to expiry | happy path (OAuth refresh - mock fast) |
| F5.4 | W2 | F5 | Charlie | `f5_4` | A malformed credential is rejected at registration time | security gate (rejection at registration) |
| F5.5 | W2 | F5 | Charlie | `f5_5` | Revoking a credential immediately stops all calls that used it | security gate (revoke propagation) |
| F7.1 | W2 | F7 | Charlie | `f7_1` | Activating the emergency killswitch causes all calls to be rejected | security gate (killswitch enable) |
| F7.2 | W2 | F7 | Charlie | `f7_2` | Deactivating the emergency killswitch restores normal operation | security gate (killswitch disable) |
| F7.5 | W2 | F7 | Charlie | `f7_5` | Responses rejected by the killswitch clearly indicate an operator-imposed block | security gate (response semantics) |
| F8.7 | W2 | F8 | Charlie | `f8_7` | When one upstream goes down, traffic is automatically rerouted to another upstream | broad (failover happy path, low chaos) |
| F10.1 | W2 | F10 | Alice | `f10_1` | The operator consents to Anthropic through a browser | happy path (OAuth flow start) |
| F11A.1 | W2 | F11A | Charlie | `f11a_1` | Current usage against the 5-hour quota is visible | broad (subscription quota display) |
| F9.1 | W3 | F9 | Alice | `f9_1` | Attaching a policy to a team applies it immediately to subsequent calls | happy path (policy attach) |
| F9.3 | W3 | F9 | Alice | `f9_3` | Malformed policies are rejected during the save phase | security gate (validation) |
| F12.1 | W3 | F12 | Bob | `f12_1` | Uploading a plugin with the same signature twice rejects the second upload | security gate (plugin dedup) |
| F12.4 | W3 | F12 | Bob | `f12_4` | Plugins currently in use cannot be deleted | security gate (refcount) |
| F25.1 | W3 | F25 | Bob | `f25_1` | Plugins built within the plugin format supported by cc-lb are accepted | happy path (plugin runtime) |
| F13.1 | W4 | F13 | Dana | `f13_1` | The auditor views one operator's branch changes in chronological order | happy path (audit read) |
| F14.1 | W4 | F14 | Alice | `f14_1` | Saving a draft does not yet affect actual behavior | happy path (config draft) |
| F14.3 | W4 | F14 | Alice | `f14_3` | Invalid configuration is rejected at save time | security gate (config validation) |
| F14.4 | W4 | F14 | Alice | `f14_4` | Only a draft that passed validation is applied (split 1/2) | happy path (config apply) |
| F17.1 | W4 | F17 | Charlie | `f17_1` | A change on one replica is immediately announced to another replica | broad (replication baseline, postgres-heavy) |
| F18.2 | W4 | F18 | Dana | `f18_2` | Per-call cost is calculated exactly from catalog prices | happy path (cost calc) |
| F20.1 | W4 | F20 | Dana | `f20_1` | No stored secret remains as plaintext on disk | security gate (secret-at-rest) |

## Distribution Verification

| Writer | Fast Count | Ratio (% within Writer) |
|---|---:|---:|
| W1 | 12 | 12.2% (12/98) |
| W2 | 9 | 13.6% (9/66) |
| W3 | 5 | 7.9% (5/63) |
| W4 | 7 | 7.4% (7/94) |
| **TOTAL** | **33** | **10.3% (33/321)** |

| Persona | Fast Count |
|---|---:|
| Alice | 14 |
| Bob | 6 |
| Charlie | 10 |
| Dana | 3 |

| Feature | Fast Count |
|---|---:|
| F1 | 5 | F2 | 3 | F3 | 3 | F26 | 1 | F5 | 3 | F7 | 3 |
| F8 | 1 | F10 | 1 | F11A | 1 | F9 | 2 | F12 | 2 | F25 | 1 |
| F13 | 1 | F14 | 3 | F17 | 1 | F18 | 1 | F20 | 1 |

Total of 17 features × average of 1.94 = 33. There's no assumption that all 27 features must be included in the fast subset (chaos-only or time-only features are nightly only).

## Estimated PR Gate Time

- 33 scenarios × 2 backends (sqlite + postgres) = 66 fn
- Average of ~5s per scenario (E2E with fake-anthropic in-process spawn)
- Serial = 330s ≈ 5.5 minutes
- nextest parallel 8 threads = about 45s to 1.5 minutes (sqlite). For postgres, schema-per-test can also run in parallel, but it's distributed according to the `--test-threads=4` policy in `conformance.yml`.
- Goal: satisfies **PR gate ≤ 5 minutes**

## M5 Reselection Rules

At M5, reconfigure the fast subset after verifying the following three:
1. Measure the actual average execution time of each fast scenario (the `time` field in nextest junit-output)
2. Keep only scenarios that are flake-free in 7 consecutive nightly runs as fast
3. Promote core security gates or happy paths among newly stable scenarios to fast

The reselection results will update the 33-row table in this document in-place, and specify "fast subset M5: +N / -M" in the commit message.
