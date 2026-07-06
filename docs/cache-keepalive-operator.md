# Prompt-cache keep-alive operator guide

Feature-gated per principal. When enabled, cc-lb tracks sessions that carry
an Anthropic prompt-cache `cache_control` breakpoint, classifies whether the
agent is still executing or waiting for the user, and periodically fires a
synthetic `max_tokens: 0` request to keep the cache prefix warm before its
TTL expires.

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

Nothing runs — no snapshot, no schedule — for principals whose
`cache_keepalive.enabled` is `false` or unset, or for requests that carry
no `cache_control` breakpoint.

## Configuration

Attached to each principal via the admin API (see
[docs/runtime-management.md](./runtime-management.md)). All fields have
defaults; every field is optional except `enabled`.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | required | Master switch on this principal. |
| `refresh_lead_time_5m_secs` | `30` | Fire 30 s before the 5-minute TTL expires. Schedule = 4m30s after last real request. |
| `refresh_lead_time_1h_secs` | `300` | Fire 5 minutes before the 1-hour TTL expires. Schedule = 55m after last real request. |
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
   operators opt in. Keep an eye on `cc_lb_cache_keepalive_active_sessions`
   per principal for capacity planning.

## Metrics

Prometheus surface:

- `cc_lb_cache_keepalive_scheduled_total{principal_id, ttl}` — a keep-alive
  fire was queued. `ttl` is `"5m"` or `"1h"`.
- `cc_lb_cache_keepalive_fired_total{principal_id, ttl, result}` — a
  keep-alive fire was dispatched. `result` is `"hit"` (cache read
  observed), `"miss"` (no cache read), or `"error"` (transport / 4xx / 5xx).
- `cc_lb_cache_keepalive_cancelled_total{principal_id, reason}` — a
  session was torn down. `reason` values:
  - `new_request` — a fresh real request superseded the pending
    keep-alive.
  - `no_cache_control` — the tracked request no longer carries a
    `cache_control` breakpoint.
  - `max_refreshes` — hit `max_refreshes_per_session`.
  - `max_duration` — hit `max_total_duration_secs`.
  - `user_turn_detected` — the classifier decided the user is now up.
  - `snapshot_too_large` — shaped body exceeded `snapshot_max_bytes`.
  - `upstream_gone` — dispatch returned an error, unreachable upstream,
    or a provider-gated upstream kind.
  - `shutdown` — the server drained on `SIGTERM`.
  - `process_cap_exceeded` — the process-wide active-session hard cap
    (10 000 sessions) was hit; new sessions are refused until existing
    ones drain.
- `cc_lb_cache_keepalive_classifier_decisions_total{decision, source}` —
  every classifier verdict. `decision` is `"user_turn"`, `"agent_in_turn"`,
  or `"ambiguous"`. `source` is currently `"heuristic"`; `"llm"` is reserved
  for the future judge implementation. Not labelled by `principal_id` —
  aggregate over the whole process.
- `cc_lb_cache_keepalive_llm_latency_seconds` — reserved histogram for the
  future LLM judge. It has zero samples in this release because non-null
  `classifier.llm_judge` is rejected by the admin API.
- `cc_lb_cache_keepalive_active_sessions{principal_id}` — gauge of
  currently-scheduled sessions.

Interpretation shortcuts:

- Steady growth in `active_sessions` without matching
  `cancelled_total{reason=new_request}` = the classifier is over-firing.
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
