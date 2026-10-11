# Prompt-cache keep-alive operator guide

Feature-gated per principal. When enabled, cc-lb tracks sessions that carry
an Anthropic prompt-cache `cache_control` breakpoint, classifies whether the
agent is still executing or waiting for the user, and enqueues durable
`CacheKeepaliveJob` work through Apalis to fire a synthetic `max_tokens: 0`
request before the cache TTL expires.

Design references:
- [RFC-0003](./rfc/0003-prompt-cache-keepalive.md) — full architecture.
- [ADR 0004](./adr/0004-cache-keepalive-scheduler.md) — scheduler / dispatcher / classifier decisions.

## Scope and provider gating

v1 fires keep-alives only against **direct Anthropic OAuth upstreams**.
`AnthropicApiKey` is disabled until storage-backed keep-alive signing lands;
downstream proxy keys are never replayed to Anthropic. All other upstream kinds
are skipped at fire time and the session is cancelled with `reason=upstream_gone`:

- **Bedrock, Vertex:** `max_tokens: 0` behavior is unverified.
- **OpenRouter:** schema requires `max_tokens >= 1`.

Nothing runs - no snapshot, no schedule - for principals whose
`cache_keepalive.enabled` is `false` or unset, for principals whose
`prompt_cache_mode` is `one_hour_no_keepalive` (see
[One-hour cache mode](#one-hour-cache-mode)), or for requests that carry
no `cache_control` breakpoint.

## One-hour cache mode

`prompt_cache_mode` is a per-principal enum carried on the principal record and
managed through the existing `PATCH /admin/v1/principals/{id}` contract with
`If-Match`. The admin UI exposes it as the **Use one-hour cache** switch.

| Mode | Cache controls sent upstream | Keep-alives |
|---|---|---|
| `preserve_client` (default) | exactly as the caller sent them | operate normally |
| `one_hour_no_keepalive` | recognized `ephemeral` controls promoted to `ttl: "1h"` | prohibited; stored config dormant |

Details that operators need:

- The mode rewrites only locations the prompt-cache contract already reads: the
  top-level automatic `cache_control`, tool entries, `system` content blocks,
  and `messages[].content[]` blocks. Nested JSON - tool input schemas, tool
  arguments, user payloads - is never touched, and a request without cache
  controls gains none. It is a duration override, not an automatic-caching
  feature.
- Only markers the provider contract already defines are promoted: an
  `ephemeral` control with no `ttl`, `ttl: "5m"`, or `ttl: "1h"`. A control the
  contract would reject - an unknown or non-string `ttl`, a non-`ephemeral`
  type - is left exactly as the caller sent it, so the mode overrides a
  duration instead of repairing an invalid request.
- Anthropic Messages requests are affected. Other protocols and the
  count-token endpoints are unchanged.
- The stored `cache_keepalive` configuration is kept, not deleted, when the mode
  is enabled. The UI shows it as disabled by one-hour cache mode. Returning to
  `preserve_client` restores the saved configuration and keeps serving the
  client's own TTLs.
- Entering `one_hour_no_keepalive` retires the principal's active keep-alive
  sessions (`status=terminal`, `reason=cancelled`) in the same storage
  transaction as the mode write. Already-queued jobs become no-ops through the
  existing generation and status fences; no scheduler job payload is rewritten.
- A renewal that is already running when the update lands cannot be recalled.
  Its usage is finalized and recorded, and it is not rescheduled.
- The durable enqueue boundary re-reads the principal's mode inside its own
  transaction, so a request admitted under the previous mode cannot recreate a
  session after the update cancelled it.

```bash
export CC_LB_ADMIN_TOKEN=<admin token>
export PRINCIPAL_ID=<principal uuid>

curl -sS -X PATCH \
     -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
     -H "If-Match: \"<current revision>\"" \
     -H "content-type: application/json" \
     "http://${ADMIN_ADDR}/admin/v1/principals/${PRINCIPAL_ID}" \
     -d '{"prompt_cache_mode":"one_hour_no_keepalive"}' \
  | jq '.prompt_cache_mode'
```

Send `"preserve_client"` with the current revision to restore client policy;
the previously saved keep-alive configuration becomes effective again. The
update takes effect for newly admitted traffic when the updated principal view
is published, and requests already in flight finish under the policy they
started with. The admin UI exposes the same write as the **Use one-hour cache**
switch and disables the keep-alive controls while it is on. The API contract is
documented in
[docs/runtime-management.md](./runtime-management.md#principals-api).

## Scheduler and storage model

Apalis is the only timer/worker transport for GA. The durable session fence is
the `cache_keepalive_sessions` table, not a second scheduler. Each eligible
real response writes a new generation and enqueues a lightweight
`CacheKeepaliveJob` with idempotency key
`cache_keepalive:<session_key_hash>:<generation>`. A worker checks the stored
generation before dispatch and again through conditional post-dispatch updates,
so old queued/running work becomes a stale no-op.

Scheduling is anchored to cache creation / response begin:

- Streaming responses use the Anthropic `message_start` event timestamp.
- Non-streaming responses use the first upstream response/body event when
  available, with request dispatch time as the fallback.
- `run_at = cache_anchor_at + ttl - lead_time`; with the default 5-minute TTL
  and 30-second lead, the job fires 270 seconds after `message_start`, not 270
  seconds after final response completion.

Successful keepalive hits preserve the original duration anchor, increment
`refresh_count`, record the synthetic hit's new response-begin/cache anchor,
and enqueue the next generation. New real requests reset `refresh_count` and
`first_scheduled_at`.

Prompt snapshots are stored only as AEAD ciphertext in
`cache_keepalive_sessions.encrypted_payload`. The AAD binds principal id,
session hash, upstream id, generation, and the cache-keepalive snapshot version
so ciphertext cannot be replayed across sessions or generations. Downstream
`Authorization` and `x-api-key` headers are never persisted; the worker signs at
fire time from storage-backed upstream credentials.

SQLite is supported for local development, CI, and bounded single-node use.
Production multi-replica / HA deployments should use Postgres so Apalis claims
and the generation fence are visible to every replica. Redis is a no-go for this
feature until a separate design proves a safe SQL fence / signing / retention
boundary.

## Configuration

Attached to each principal via the admin API (see
[docs/runtime-management.md](./runtime-management.md)). All fields have
defaults; every field is optional except `enabled`.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | required | Master switch on this principal. |
| `refresh_lead_time_5m_secs` | `30` | Fire 30 s before the 5-minute TTL expires. Schedule = 4m30s after `message_start` / response begin. |
| `refresh_lead_time_1h_secs` | `300` | Fire 5 minutes before the 1-hour TTL expires. Schedule = 55m after `message_start` / response begin. |
| `max_refreshes_per_session` | `12` | Hard cap on keep-alive fires per session. |
| `max_total_duration_secs` | `14400` (4 h) | Wall-clock cap from first schedule. |
| `snapshot_max_bytes` | `524288` (512 KiB) | Shaped body larger than this is not tracked. |
| `classifier.extra_wait_for_user_tools` | `[]` | Extra tool names to treat as "wait for user" on top of the built-in list. |
| `classifier.treat_end_turn_as_ambiguous` | `false` | Treat `stop_reason: end_turn` as ambiguous. Because the LLM judge is reserved for a future release, ambiguous decisions currently fail closed as user-turn. |
| `classifier.llm_judge` | `null` | Reserved for a future small-LLM judge. The admin API rejects non-null values in this release. |

The built-in wait-for-user tool allow-list matches Claude Code, Cline,
Roo, Aider, Continue, OpenCode, OpenHands, and Cursor:
`AskUserQuestion`, `ExitPlanMode`, `ask_followup_question`, `ask_question`,
`cursor_ask_question`, `question`, `attempt_completion`,
`submit_and_exit`, `finish`, `plan_mode_respond`, `act_mode_respond`,
`report_bug`.

## Rollout phases

1. **Phase 0 — default off.** Ship the code with every principal's
   `cache_keepalive` unset. Behavior unchanged for all existing traffic.
2. **Phase 1 — internal opt-in.** Enable on one staging principal with
   `max_refreshes_per_session = 2`, `max_total_duration_secs = 600`.
   Confirm the metric counters below move and no unexpected 400s appear
   in the upstream error logs.
3. **Phase 2 — production canary.** Enable on one production principal
   with defaults. Watch `cc_lb_cache_keepalive_fired_total{result="miss"}`
   and the `cc_lb_cache_keepalive_cancelled_total{reason=…}` distribution
   for one week. False-positive fires (miss + AgentInTurn) cost roughly
   the input-token price of one prefix per fire.
4. **Phase 3 — general availability.** Enable on remaining principals as
   operators opt in. GA assumes durable SQLite/Postgres storage is enabled;
   the old no-DB-persistence and cross-replica non-goals apply only to the
   pre-Apalis prototype. Keep an eye on
   `cc_lb_cache_keepalive_active_sessions` per principal for capacity planning.

## Metrics

Prometheus surface:

- `cc_lb_cache_keepalive_scheduled_total{principal_id, ttl}` — a keep-alive
  fire was queued. `ttl` is `"5m"` or `"1h"`.
- `cc_lb_cache_keepalive_fired_total{principal_id, ttl, result}` — a
  keep-alive fire was dispatched. `result` is `"hit"` (cache read
  observed), `"miss"` (no cache read), `"error"` (transport / 4xx / 5xx),
  `"stale"`, `"noop"`, or `"decrypt_failed"`.
- `cc_lb_cache_keepalive_cancelled_total{principal_id, reason}` — a
  session was torn down. `reason` values:
  - `user_turn_detected` — the classifier decided the user is now up.
  - `snapshot_too_large` — shaped body exceeded `snapshot_max_bytes`.
- `cc_lb_cache_keepalive_classifier_decisions_total{decision, source}` —
  every classifier verdict. `decision` is `"user_turn"`, `"agent_in_turn"`,
  or `"ambiguous"`. `source` is always `"heuristic"`. Not labelled by
  `principal_id` — aggregate over the whole process.
- `cc_lb_cache_keepalive_active_sessions{principal_id}` — gauge of
  currently-scheduled sessions.

Interpretation shortcuts:

- Steady growth in `active_sessions` without matching
  `cancelled_total{reason=user_turn_detected}` = the classifier is over-firing.
  Add tool names to `extra_wait_for_user_tools` or lower
  `max_refreshes_per_session`.
- `fired_total{result=miss}` spiking = requests are being evicted between
  the last read and the keep-alive fire. Verify the principal's traffic
  fits within the configured `refresh_lead_time_5m_secs` /
  `refresh_lead_time_1h_secs` (fires must land inside the TTL window,
  not after).
- `fired_total{result=error}` spiking = an unsupported upstream kind,
  upstream outage, or provider error. Investigate the upstream error log.
- `cancelled_total{reason=snapshot_too_large}` = a principal's shaped
  request bodies are above 512 KiB. Raise `snapshot_max_bytes` on that
  principal, or accept that these sessions are not tracked.

## Runbook: enable for a principal

```
export CC_LB_ADMIN_TOKEN=<admin token>
export PRINCIPAL_ID=<principal uuid>

curl -sS -X PATCH \
     -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
     -H "If-Match: \"<current revision>\"" \
     -H "content-type: application/json" \
     "http://${ADMIN_ADDR}/admin/v1/principals/${PRINCIPAL_ID}" \
     -d '{
           "cache_keepalive": {
             "enabled": true,
             "max_refreshes_per_session": 2,
             "max_total_duration_secs": 600
           }
         }' \
  | jq '.cache_keepalive'
```

The response echoes the merged config (defaults filled in). Confirm
`enabled: true` and the fields you set. The change is picked up on the
next request from that principal — no server restart required.

## Runbook: disable for a principal

```
export CC_LB_ADMIN_TOKEN=<admin token>
export PRINCIPAL_ID=<principal uuid>

curl -sS -X PATCH \
     -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
     -H "If-Match: \"<current revision>\"" \
     -H "content-type: application/json" \
     "http://${ADMIN_ADDR}/admin/v1/principals/${PRINCIPAL_ID}" \
     -d '{"cache_keepalive": null}' \
  | jq '.cache_keepalive'
```

`null` clears the config entirely. New real requests from this principal
stop tracking immediately. Sessions that were already scheduled before
the clear will still fire until they naturally exit via
`max_refreshes_per_session`, `max_total_duration_secs`, an
upstream/miss error, or a subsequent real request classified as
`UserTurn`. The
`cc_lb_cache_keepalive_active_sessions{principal_id=...}` gauge drains
proportionally as sessions exit; expect it to reach zero within
`max_total_duration_secs` in the worst case, and much faster in
practice.

Cleanup removes expired sessions, terminal keepalive Apalis rows, and stale
old `Pending` keepalive rows. It never deletes `Queued` or `Running` rows;
generation idempotency and worker pre/post checks make old active work safe
when it eventually runs. `CacheKeepaliveJob` rows are enqueued with
`max_attempts=1`, and business outcomes such as miss, stale, decrypt failure,
unsupported provider, and dispatch error are recorded as terminal/noop results
rather than retried by Apalis.

## Runbook: session isn't being kept warm

Symptom: operator expects prefix cache reads for a specific principal +
thread, but every request re-writes the prefix.

Checks, in order:

1. Config enabled on the principal? Query the admin API and confirm
   `.cache_keepalive.enabled == true`.
2. Requests carry a `cache_control` breakpoint? Inspect a captured
   request body. If absent, cc-lb does not track the session and this
   is expected behavior.
3. Is the session identifiable? Confirm one of
   `x-claude-code-session-id`, `x-claude-session-id`,
   `x-session-affinity`, or `x-session-id` is present. If absent, cc-lb
   falls back to `cache_prefix_hash` and per-request affinity may fail
   to stick.
4. `cc_lb_cache_keepalive_active_sessions{principal_id=<id>}` shows the
   session? If zero, either the classifier is deciding `user_turn` on
   every response (check
   `cc_lb_cache_keepalive_classifier_decisions_total{decision="user_turn"}`
   spike) or the snapshot capture is failing (check
   `cc_lb_cache_keepalive_cancelled_total{reason="snapshot_too_large"}`).
5. `cc_lb_cache_keepalive_fired_total{result="miss"}` present? A miss
   means the fire reached Anthropic but Anthropic did not report a
   cache read. Common cause: the shape plugin has changed and the
   snapshot no longer matches the cache key. Re-establish the session
   with a fresh real request.
6. `cc_lb_cache_keepalive_cancelled_total{reason="upstream_gone"}`?
   Almost always means the upstream is not a direct Anthropic upstream.
   Bedrock / Vertex / OpenRouter are provider-gated in v1.

If none of the above explains it, capture a request and response body
alongside the admin-config output and file an issue.
