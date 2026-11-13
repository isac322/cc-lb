# Prompt-cache keep-alive operator guide

Feature-gated per principal. cc-lb enqueues durable `CacheKeepaliveJob` work
through Apalis before an eligible cache prefix expires. Anthropic requests
use cache-control breakpoints and turn classification with zero-output
renewals. Opted-in OpenAI Responses requests use their native cache lifetime
and usage evidence with a bounded output budget.

Design references:
- [RFC-0003](./rfc/0003-prompt-cache-keepalive.md) — full architecture.
- [ADR 0004](./adr/0004-cache-keepalive-scheduler.md) — scheduler / dispatcher / classifier decisions.

## Scope and provider gating

Keep-alive fires against direct Anthropic OAuth upstreams and opted-in
OpenAI Responses requests for GPT-5.6 and later. Anthropic API-key upstreams
remain unsupported; downstream proxy keys are never replayed to Anthropic.
An existing, enabled upstream of any unsupported kind terminates the session
with `terminal_reason=unsupported_provider`.

OpenAI Responses uses the documented `prompt_cache_options.ttl` field. It
accepts `"30m"` and `"12h"`; when the field is omitted, a supported request
defaults to `"30m"`. A `prompt_cache_breakpoint.ttl` overrides that default
for the selected breakpoint, while a breakpoint without `ttl` inherits the
request lifetime. Supported explicit blocks include `input_text`, `input_image`,
and `input_file` in `function_call_output.output` and
`custom_tool_call_output.output` arrays, as well as message content. Explicit
mode without a real input breakpoint is not eligible. Keys embedded in metadata,
tool schemas, or JSON text inside tool results do not declare a lifetime.
Keep-alive selects the last supported explicit content breakpoint, or the latest
eligible implicit message ending when no explicit breakpoint exists. It retains
that boundary's lifetime and provenance and replays the full captured input.
This does not model every mixed-lifetime boundary or identify which prefix
produced the provider's aggregate cached-token count.

Enable both `cache_keepalive.enabled` and
`cache_keepalive.classifier.openai_responses_enabled` on the principal.
Positive native cached-token evidence must meet the 1,024-token minimum.
Renewals also require a refresh budget that meets the existing cost threshold
(at least 13 refreshes for GPT-5.6 with 30m lifetime). A selected `"12h"`
prefix records a not-tracked decision without a renewal job: reads must not
restart its 12-hour clock.

Legacy models keep their separate `prompt_cache_retention` semantics
(`"in_memory"` or `"24h"`). That maximum-retention policy does not select a
modern minimum TTL. `"30m"` in the legacy field is invalid provider syntax.
Responses renewals retain the cache options and prefix and use
`max_output_tokens: 16` with `stream: false`. They can consume paid output
tokens and do not guarantee a cache hit or cost saving.

A missing, disabled, or deleted upstream instead produces
`terminal_reason=dispatch_error` with `error="upstream gone"`.

For an enabled Anthropic principal, no snapshot or schedule is created for
requests with no `cache_control` breakpoint, except that a response-derived
compaction boundary may retire an already-active matching generation without
creating a new snapshot or schedule. Disabled or unset principals remain
entirely out of keep-alive processing. Ordinary no-breakpoint Anthropic
responses remain outside keep-alive completion sequencing.

## Response compaction boundaries

When a completed response reports `stop_reason: "compaction"` or contains a
top-level `compaction` block, the matching keep-alive generation is retired
through the durable fence. cc-lb does not schedule the pre-compaction request
snapshot, including when the response continues with a client tool use. This
response-derived boundary does not depend on optional gateway hint headers.
A later main response built from the adopted summary can establish a new
generation. Plain `pause_turn` and ordinary tool use without a compaction block
retain their existing behavior.

## Scheduler and storage model

Apalis is the only timer/worker transport for GA. The durable session fence is
the `cache_keepalive_sessions` table, not a second scheduler. Each eligible
real response writes a new generation and enqueues a lightweight
`CacheKeepaliveJob` with idempotency key
`cache_keepalive:<session_key_hash>:<generation>`. A worker checks the stored
generation before dispatch and again through conditional post-dispatch updates,
so old queued/running work becomes a stale no-op.

Completed renewal records use `keepalive-v2:<session_key_hash>:<cycle_id>:<generation>`
as their source identity. The version prefix separates new writes from retained
legacy identities, including sessions whose old keys contain colons or have no
cycle UUID. Turns, request events, and renewal evidence share this identity, so
purging and recreating a session cannot overwrite retained spend.

An authenticated, non-auxiliary, keep-alive-eligible real request retires the
matching active generation before upstream dispatch. Its queued renewal cannot
fire while that real request waits for response headers or streams its response.
The session appears as **In use** (`in_request`), with renewals paused.
This state remains tracked and visible even without retained renewal history,
but does not count toward **Renewing now** or match Scheduled/Renewed filters.

The request marker expires at the deadline derived from the server's configured
request timeout. Without a recorded terminal upstream response by then, the
read model shows Expired with the neutral reason “Request did not complete
before its deadline”.
This does not claim a known abort or error. Legacy `new_request` cancellations
without a request-start marker retain the `cancelled` state, shown as Returned.

A completed eligible response classified as `AgentInTurn` can establish a new
generation. A final cancellation records its actual completion reason. If the
real request fails, cc-lb does not resume the old snapshot. Completion-sequence
fencing prevents an older completion from replacing a newer request.

Ordering metadata is retained only while its session exists or a request-owner
protection window remains open. Issuance records the last issuance time and
protection deadline in the existing ticket write. The window is the configured
upstream total timeout plus one additional timeout as a safety margin.
Housekeeping may remove orphan metadata after the latest protection deadline.
Payload and evidence retention remain unchanged.

Each ticket also carries its own deadline and namespace epoch. Expired tickets
cannot mutate keep-alive state, and a recreated namespace receives a new epoch.
A delayed callback therefore cannot claim a later owner even if its numeric
ticket sequence repeats after cleanup.
Malformed or unrecognized metadata is retained and cannot authorize a leased
completion; housekeeping does not recycle an identity it cannot validate.

New writers store leased ownership under `keepalive_order_v2:<session>`.
The old `keepalive_order:<session>` key remains a numeric legacy counter.
The first allocation in a new namespace reads its legacy issued floor, but
does not import its applied owner or epoch. New writers never rewrite that
legacy key; later allocations in an existing v2 namespace ignore legacy changes.
Housekeeping handles both namespaces with their respective retention rules.

Rolling ownership upgrades are supported from the latest published 2.2.x line
and current master. A provisional marker temporarily stores its protection
deadline plus one in raw `first_scheduled_at`, so those versions' ticketed
replacement fence declines rather than reviving it. New readers use the real
start in `running_since`; finalization and reactivation restore a real
first-scheduled anchor. Active renewal-duration limits are unchanged.
An old enqueue declined by this fence is a no-op, not a failed client request.
A later old-node cache anchor can resume legacy scheduling after protection
expires. These comparisons use the existing UTC lease clock; keep pod clocks
synchronized.

Rolling directly from 2.1.x or older can transiently revive an in-request marker
during the overlap because those writers use different replacement predicates.
Drain and stop those old pods before starting new writers, or upgrade to 2.2.x
first. Normal client requests still succeed, but ignoring this recommendation
can schedule unnecessary renewals from an older snapshot.

If failover changes or removes a threadless request's eligible cache identity,
completion retires the original marker with `session_identity_changed` using
its original owner ticket. A final eligible identity receives a normal fresh
completion, never a ticket borrowed from the original identity. Without a
final eligible identity, completion only retires the original owner.
An unscoped main request also retires the generic subagent owner captured at
start, independently of whether a newer request superseded its primary owner.
Neither retirement re-observes or replaces a newer owner.

Completed requests that cannot produce a safe renewal snapshot finalize their
captured primary and sibling owners with `completed_not_renewable`: “Request
completed, renewal not possible”. This includes configuration changes,
missing or invalid snapshot metadata, raw-passthrough shaping fallbacks,
undecodable completed responses, and responses rejected by affinity binding.
Captured ownership survives even when no safe snapshot was captured. Fully
collected HTTP error responses and protocol-terminal provider errors, refusals,
or context-window stops also finalize as `completed_not_renewable`.
A streamed stop hint alone is not terminal proof. Transport failures, incomplete
streams, and aborts before a terminal response still use the neutral deadline
fallback. Dropping an unconsumed completion context only logs at debug level;
that diagnostic Drop never invents completion or writes a final state.

For stream EOF to establish completion, decoding must finish and no partial
event may remain. An already received native terminal or error event is
independent proof, even if a later proxy step fails. Fully received buffered
responses rejected by body limits or response transformation also finalize
their captured owners without renewal.

If SSE response transformation fails open before emitting output, keep-alive
continues observing the original raw events, including terminal events split
across chunks. Eligible responses retain their renewal snapshot. Transformed
output alone never establishes upstream terminal proof.

The proxy waits for the request-start fence before dispatch. Storage errors
are logged and do not fail the proxy request; a queued renewal can still run
if the fence cannot be persisted.

Successful real-request cancellations persist a nullable `cancel_reason`:
`new_request`, `user_turn_detected`, `ambiguous_turn`, `retention_recorded`,
`renewal_not_beneficial`, `session_identity_changed`, `completed_not_renewable`,
`snapshot_too_large`, `cached_prefix_too_small`, or
`zero_token_renewal_unsupported`. Older rows
remain null; show the Returned badge without a separate cancellation box or a
guessed cause.

Scheduling is anchored to cache creation / response begin:

- Streaming responses use the Anthropic `message_start` event timestamp.
- Non-streaming responses use the first upstream response/body event when
  available, with request dispatch time as the fallback.
- `run_at = cache_anchor_at + ttl - lead_time`; with the default 5-minute TTL
  and 30-second lead, the job fires 270 seconds after `message_start`, not 270
  seconds after final response completion.
- For mixed Anthropic breakpoints, scheduling uses the shortest protected TTL;
  a 1-hour system prefix plus a 5-minute message prefix renews on the 5-minute cadence.

The initial schedule subtracts the existing cache anchor age. After a
cache-hit renewal, the next schedule uses the full configured `ttl - lead_time`
interval from the synthetic response's cache anchor; it does not reuse the
initial shortened delay. Session caps retain the original duration anchor.
Rescheduling increments `refresh_count`. The final successful renewal that
reaches the refresh cap also increments it. That final hit advances the durable generation
and is immediately terminalized without enqueuing another renewal. Its turn
and evidence retain the generation that actually dispatched. New real requests
reset `refresh_count` and `first_scheduled_at`.

Both caps are checked before provider dispatch. A renewal scheduled at or after
`first_scheduled_at + max_total_duration_secs` is terminal `max_duration`
without an HTTP request. A late worker also stops when the current time reaches
that deadline. The send gate stays attached through signing and bulkhead
admission, and is checked again after capacity is acquired without bypassing
the circuit breaker. Once HTTP starts, crossing the cap does not cancel the
request or discard its paid usage. The post-response cap checks remain in force.

Anthropic rescheduling requires positive cache reads and zero cache creation.
A renewal that reads one prefix but rebuilds another is terminal `cache_miss`,
even when its read count is positive. Its entire paid usage and spend remain
recorded. This uses no deployment-tuned token tolerance: explicit snapshot
continuations add no cache-controlled content, and automatic-cache snapshots
replay the captured input unchanged.

An eligible successful read-only renewal attempts observation publication
through the existing bounded, nonblocking best-effort sink. The shared decoder
uses the exact transformed body sent to the provider, its canonical model, and
its eligible resolved explicit or automatic breakpoints. The snapshot's
scheduling TTL does not identify the deepest observed breakpoint. Publication
is attempted before finalization, rescheduling, or cap handling, including the
final successful renewal when a session cap prevents another fire. Misses,
errors, unsupported providers, and ambiguous usage produce no new keepalive
observations.

Read-only evidence requires a numeric positive `cache_read_input_tokens` and
numeric zero creation: flat `cache_creation_input_tokens: 0`, both nested
`cache_creation.ephemeral_5m_input_tokens: 0` and
`cache_creation.ephemeral_1h_input_tokens: 0`, or the complete suffix pair
`cache_creation_input_tokens_5m: 0` and `cache_creation_input_tokens_1h: 0`.
Every supplied representation must contain complete, valid unsigned counters,
and flat, nested, and suffix totals must agree. Suffix fields are evidence-only;
numeric accounting remains unchanged. Missing or null values cannot establish
new evidence. For SSE, omitted or null fields preserve the corresponding prior
evidence components; complete numeric flat, nested, or suffix snapshots replace
the prior representation and may change evidence state. Partial, malformed, or
contradictory snapshots invalidate evidence; partial nested or suffix SSE
snapshots cannot be combined into zero evidence. In ordinary response
attribution, positive creation disables only the
new no-predicted-match inferred-hit branch; existing creation/write and
predicted-match behavior remain unchanged.

The dispatcher captures an immutable absolute observation-expiry anchor
immediately before the actual signed HTTP dispatch. This is separate from the
response-begin/cache anchors used for scheduling above; it is never finalizer
time or a scheduler reconstruction of `now - cache_anchor_age`. Existing
scheduler anchors are unchanged; ordinary response observations retain their
existing dispatch-based expiry anchor.

Publication does not wait for durable commit. A full or closed sink or a
storage failure does not fail the renewal, and publication does not change
accounting, status, authorization, generation fencing, caps, scheduling, or
failure behavior. It uses the existing observation store without a new schema,
API DTO, retry, outbox, or flush. It does not verify live provider cache state
or guarantee provider acceptance or a provider cache hit.

Prompt snapshots are stored only as AEAD ciphertext in
`cache_keepalive_sessions.encrypted_payload`. The AAD binds principal id,
session hash, upstream id, generation, and the cache-keepalive snapshot version
so ciphertext cannot be replayed across sessions or generations. Downstream
`Authorization` and `x-api-key` headers are never persisted; the worker signs at
fire time from storage-backed upstream credentials.

The keep-alive snapshot is trimmed to the last explicit `cache_control`
breakpoint when top-level automatic caching is not present. This removes
uncached trailing messages while preserving the matching user `tool_result`
message after an assistant `tool_use`, and keeps a minimal user continuation
when the breakpoint ends on an assistant turn. If only `system` or `tools`
contains an explicit breakpoint, the request history is reduced to a minimal
user turn.

Snapshot capture, trimming, renewal shaping, and persistence preserve the
shaped request's JSON object key order. Renewal shaping changes `max_tokens`
and removes `stream`; it does not sort prompt objects. This wire-body rule is
separate from cc-lb's canonical, sorted-key serialization for routing hashes.
Preserving order does not guarantee a provider cache hit or a particular cost.

SQLite is supported for local development, CI, and bounded single-node use.
Production multi-replica / HA deployments should use Postgres so Apalis claims
and the generation fence are visible to every replica. Redis is a no-go for this
feature until a separate design proves a safe SQL fence / signing / retention
boundary.

## Renewal usage accounting

Successful renewals use the same cache-creation TTL split as regular Anthropic
usage accounting. When either nested
`usage.cache_creation.ephemeral_5m_input_tokens` or
`ephemeral_1h_input_tokens` is positive, those counters determine the total
and both TTL buckets, even if the flat total disagrees. A positive flat total
with an absent, empty, or all-zero nested breakdown falls back to the 5-minute
bucket; it does not imply a 1-hour write.

Request events and keep-alive turns retain that split for local cost accounting.
The change applies to future renewals and does not rewrite historical rows.
These are cc-lb usage records and cost estimates; the provider determines its invoice.
## Read model, retained history, and economics

The admin read model separates current live sessions from retained history.
Retained rows and evidence are historical accounting data; they are not a
lifetime total and do not imply that a session is still active.

- `total_spent` is the exact persisted renewal spend from retained turn rows.
  It includes misses and remains available after session reset or purge.
- Avoided savings are credited at most once per cycle, to the latest qualifying
  renewal before the first matching positive-cache-read follow-up within its TTL.
  The follow-up must use the same principal, upstream, and model, and match either
  a candidate prefix hash or the keep-alive session key. A session-key match also
  requires a positive renewal cache read and a follow-up cache read at least as
  large as the renewal's. Follow-up evidence uses the routed serving cache
  breakpoints.
  Evidence retention uses that same full TTL window, including long-retention
  renewals; housekeeping does not shorten it to five minutes.
  Avoided value is `renewal tokens × (cache-creation price − cache-read price)`:
  the follow-up still pays for its cache read.
- Missing, legacy, malformed, or ambiguous evidence is reported as
  `unknown`. Avoided savings and net savings are nullable when the evidence or
  pricing is incomplete; the UI must not display a guessed zero.
- A valid renewal with no proven follow-up has no avoided credit. It remains
  retained spend, and may be shown as pending while its live TTL window is
  open.
- Summary-only requests use SQL aggregates over retained turns and evidence.
  They do not load every session, turn, or encrypted snapshot into Rust.
  List/detail requests load only the rows needed for their page or selected
  session. When keep-alive is disabled, the Admin Web card does not poll the
  summary endpoint.

The internal base states remain `renewed`, `scheduled`, `in_request`, `capped`,
`expired`, `cancelled`, `failed`, and `not_tracked`. The admin list uses filled
badges: scheduled and renewed appear as **Warm** in the live/accent tone;
in_request as **In use** in neutral; cancelled as **Returned** in green;
capped as **At limit** in neutral; expired as **Expired** in neutral;
failed as **Failed** in danger; and not_tracked as **Not tracked** in neutral.
Rows with an error use danger regardless of their base state.
In use explains that real traffic has paused renewal. The `in_request` API
status filter selects that state; `visible` and `tracked` include it. The
`renewed` filter selects only active renewal sessions, not requests currently
in flight. Internal states, filter names and API values are unchanged.
Sessions in the `cancelled` state show Returned only when the selected horizon
contains a renewal turn. Otherwise they show Not tracked. List filters, group
counts, and the selected session's detail state use the same horizon.
Reason text explains why renewals ended without a cancellation prefix, such as
“A new request arrived” or “The agent is waiting for the user”. Legacy rows with
no recorded cause use “Renewals ended” in the API; the detail pane omits the
separate reason box.

The sessions drawer keeps its title, summary and toolbar fixed. From `md`,
the list and detail scroll independently, with a sticky detail header; phones
show one pane at a time with a Back to sessions button. The summary is a
label-over-value Net / Saved / Spent strip, with a qualification below only when
needed. The range control sits to its right, or above it on phones, and uses
All time for the unbounded range. There is no scope note or repeated Showing
label. Partial lists show “X of Y sessions shown” and Load more sessions;
complete lists omit the count footer.

The detail rail covers all cycles inside that horizon without a duplicate
Renewals table. Hover, focus or tap a pulse for hit/miss, its full timestamp,
time since the last request and renewal, spent amount, per-renewal net and
follow-up evidence. Click to pin the details; Escape or an outside click
closes them. Miss marks use the warning tone. Estimated per-renewal spend on
legacy synthesized rails carries `≈`. Unmatched records remain visible as
Additional evidence. Sessions without a rail, including 12h sessions, retain
a cycle-grouped renewal list; legacy records without an owned cycle remain
unknown. `total_renewals_current_cycle` and pending status describe only the
current cycle, so a newly replaced cycle can show zero current renewals while
retaining earlier cycles in its history.

The detail header uses `follow_up_outcomes` to count observed follow-ups,
pending gaps, closed gaps with no follow-up, and unknown gaps separately.
Each rail gap contributes exactly once, including an empty waiting gap as
pending. These counts preserve mixed historical outcomes; the existing
`evidence_status` keeps its conservative semantics for list and filter consumers.

Renewal scheduling and pending follow-up evidence are separate. An In use
cycle remains inactive and has no Next renewal, but a paid renewal can still
be waiting for follow-up while its TTL window is open. That window ends at
the last renewal's anchor plus its TTL, not the request marker's deadline or
the first renewal's timestamp. Matching follow-up evidence resolves the cycle
as observed. Pausing renewals, or reaching an incomplete-request deadline,
does not by itself turn the cycle into a closed no-follow-up loss.
An active scheduling row does not extend an expired paid renewal's follow-up
window either; an overdue scheduled job is not evidence of a live cache.

Errors are orthogonal: a terminal dispatch/decrypt/provider error does not
become a live `scheduled` or `renewed` row, and `error` is displayed alongside the
terminal base state. An expired request marker projects as Expired without an
invented error. Its deadline explanation remains visible alongside retained
renewal history; without renewal history in the selected horizon, normal
cancelled-session visibility rules hide it.
Completion uses the request's original durable start ticket to resolve its own
marker. Cache-anchor timestamps do not grant ownership of a newer request's marker.


Renewal request events retain the originating `thread_id` for thread-scoped
sessions, including agent-scoped sessions, so thread-filtered event queries
include those renewals. Prefix-only sessions have no thread attribution.

## Payload format upgrades

The framed payload format stores the captured body as raw bytes before optional
compression. Existing legacy and v2 JSON payloads remain readable and migrate
once on their next successful renewal. Later renewals reuse the migrated
ciphertext.

An existing snapshot retains the key order of its stored body; an upgrade cannot
recover ordering already lost by an older writer. A new eligible real request
captures a fresh snapshot in that request's shaped-body order.

When crossing an incompatible payload-format boundary, drain and stop old
workers and request-serving replicas before starting new writers. Binaries
that cannot read the framed format must not overlap its writers; do not
downgrade while active sessions contain unsupported payloads. Ownership-only
rollouts between payload-compatible versions follow the support policy above.

## Configuration

Attached to each principal via the admin API (see
[docs/runtime-management.md](./runtime-management.md)). All fields have
defaults; every field is optional except `enabled`.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | required | Master switch on this principal. |
| `refresh_lead_time_5m_secs` | `30` | Fire 30 s before an Anthropic 5-minute or OpenAI 30-minute cache TTL expires (4m30s after Anthropic `message_start`, or 29m30s into an OpenAI 30m window). |
| `refresh_lead_time_1h_secs` | `300` | Fire 5 minutes before an Anthropic 1-hour cache TTL expires (55m after `message_start`). OpenAI 12-hour retention is recorded with this snapshot metadata without periodic renewal. |
| `max_refreshes_per_session` | `4` | Hard cap on keep-alive fires per session. OpenAI 30-minute keep-alive requires raising this to at least `13` for GPT-5.6. |
| `max_total_duration_secs` | `14400` (4 h) | Wall-clock cap from first schedule. |
| `snapshot_max_bytes` | `8388608` (8 MiB) | Shaped and trimmed body larger than this is not tracked. |
| `min_cache_tokens` | `10000` (10,000 tokens) | Minimum cached input tokens (`cache_read_input_tokens + cache_creation_input_tokens`) required before an `AgentInTurn` response schedules renewals. |
| `classifier.extra_wait_for_user_tools` | `[]` | Extra tool names to treat as "wait for user" on top of the built-in list. |
| `classifier.treat_end_turn_as_ambiguous` | `false` | Treat `stop_reason: end_turn` as ambiguous. Because the LLM judge is reserved for a future release, ambiguous decisions currently fail closed as user-turn. |
| `classifier.llm_judge` | `null` | Reserved for a future small-LLM judge. The admin API rejects non-null values in this release. |

The built-in wait-for-user tool allow-list matches Claude Code, Oh My Pi,
Cline, Roo, Aider, Continue, OpenCode, OpenHands, and Cursor:
`AskUserQuestion`, `ExitPlanMode`, `StructuredOutput`, `SubagentHandback`,
`EndConversation`, `ask`,
`ask_followup_question`, `ask_question`, `cursor_ask_question`,
`question`, `attempt_completion`, `submit_and_exit`, `finish`,
`plan_mode_respond`, `act_mode_respond`, `report_bug`.

Tool-name matching is case-sensitive and uses the client-facing response after
any public response transform, in both SSE and buffered JSON paths. Provider
usage accounting, error detection, and affinity extraction use the original
upstream response.

## Rollout phases

1. **Phase 0 — default off.** Ship the code with every principal's
   `cache_keepalive` unset. Behavior unchanged for all existing traffic.
2. **Phase 1 — internal opt-in.** Enable on one staging principal with
   `max_refreshes_per_session = 2`, `max_total_duration_secs = 600`.
   Confirm classifier decisions and durable renewal records appear, and check
   the upstream error logs for unexpected 400s.
3. **Phase 2: production canary.** Enable on one production principal
   with defaults. Review renewal outcomes, usage, and cost estimates in the
   principal's Cache keepalive view and renewal request events for one week.
   Use the cancellation counter below for its documented completion-path
   reasons, not as a count of all terminal sessions.
4. **Phase 3: general availability.** Enable on remaining principals as
   operators opt in. GA assumes durable SQLite/Postgres storage is enabled;
   the old no-DB-persistence and cross-replica non-goals apply only to the
   pre-Apalis prototype. Use durable session records for capacity planning.

## Metrics

The keep-alive Prometheus surface includes these counters:

- `cc_lb_cache_keepalive_cancel_errors_total` counts cancellation fences that
  failed with a storage error. A fence that finds no active generation is the
  normal case and is not counted. This is separate from `cancelled_total`.
- `cc_lb_cache_keepalive_dispatch_skipped_total{reason}` counts claimed
  renewals skipped before provider dispatch because durable authorization no
  longer held or its storage check failed.
- `cc_lb_cache_keepalive_follow_up_rejected_total{reason}` counts follow-up
  prefix candidates rejected by the off-path evidence writer. Reasons are
  `no_matching_renewal_prefix` (within the retained candidate window),
  `outside_ttl`, and `not_first_in_cycle`. No additional request-path query
  is made. Request diagnostics retain at most 32 candidate hashes, with their
  breakpoint/content-block positions, for investigating missing attribution.
- `cc_lb_cache_keepalive_cancelled_total{principal_id, reason}` counts the
  latest real-response completion's cancellation paths listed below.
  - `user_turn_detected`: a response-derived compaction boundary with no
    current cache breakpoint. Ordinary `UserTurn` and `Ambiguous` classifier
    decisions do not increment this cancellation counter.
  - `session_identity_changed`: failover changed or removed the final eligible
    cache identity, or a completed unscoped main request retired its captured
    generic subagent owner.
  - `completed_not_renewable`: a completed request could not produce a safe
    renewal snapshot; its captured owner was finalized without rescheduling.
  - `snapshot_too_large`: the captured, trimmed shaped body exceeded the
    principal's `snapshot_max_bytes`.
  - `cached_prefix_too_small`: an `AgentInTurn` response reported fewer than
    `min_cache_tokens` cached input tokens.
  - `zero_token_renewal_unsupported`: the shaped request uses enabled
    thinking, `output_config.format`, or a forced `tool_choice`, which a
    `max_tokens: 0` renewal cannot send.
- `cc_lb_cache_keepalive_classifier_decisions_total{decision, source}` counts
  heuristic classifier invocations. `decision` is `"user_turn"`,
  `"agent_in_turn"`, or `"ambiguous"`; `source` is always `"heuristic"`.
  There is no `principal_id` label, so the counter aggregates the whole process.
  Normal `UserTurn` and `Ambiguous` decisions appear here even though their
  cancellation paths do not increment `cancelled_total`.

Counters may be absent from `/metrics` until their first increment. There is
no keep-alive scheduling counter, fire-result counter, or active-session gauge.
Use the principal's Cache keepalive view or
`GET /admin/v1/principals/{principal_id}/cache-keepalive` for durable session
activity, and renewal request events for completed usage and cost estimates.

For classifier tuning, compare `user_turn`, `agent_in_turn`, and `ambiguous`
decisions with actual response tool names before changing
`extra_wait_for_user_tools`. These aggregate counters cannot identify a
particular principal's misclassification. For `snapshot_too_large`, compare the
trimmed body size with that principal's configured cap (8 MiB by default)
before raising `snapshot_max_bytes`.

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

`null` clears the config entirely. Setting `enabled: false` also disables
tracking. Once the serving principal view reflects the update, new real
requests stop tracking and queued sessions are cancelled when the worker
claims them, before upstream dispatch. Queued rows need not disappear
immediately; their durable session ends with `terminal_reason=cancelled`.

Disabling does not abort a renewal already dispatched upstream. If it completes
as a cache hit or miss, cc-lb retains its request event, keep-alive turn, usage,
and cost accounting, reconciles the reservation once, then marks the session
cancelled without enqueueing another job. `Error` and `UnsupportedProvider`
outcomes retain the RAII reservation-refund path.

Cleanup removes expired sessions, terminal keepalive Apalis rows, and stale
old `Pending` keepalive rows. It never deletes `Queued` or `Running` rows;
generation idempotency and worker pre/post checks make old active work safe
when it eventually runs. `CacheKeepaliveJob` rows are enqueued with
`max_attempts=1`, and business outcomes such as miss, stale, decrypt failure,
unsupported provider, and dispatch error are recorded as terminal/noop results
rather than retried by Apalis.

## Runbook: Anthropic session isn't being kept warm

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
   `x-hermes-session-id`, `x-aside-provider-session-id`,
   `x-aside-session-id`, `x-session-affinity`, `x-opencode-session-id`,
   `x-opencode-session`, `x-session-id`, `session_id`, `session-id`, or
   `conversation_id` (or body session metadata) is present. If absent,
   cc-lb falls back to `cache_prefix_hash` and per-request affinity may
   fail to stick. Subagent requests (`x-claude-code-agent-id` or
   `request_kind = "subagent"`) are scoped under their own agent key so
   they do not overwrite or cancel the parent session's schedule, and
   one-shot auxiliary requests (`recap`, `compaction`, `session_title`,
   `auto_thinking`, `notification`, `side`, `look_at`) do not mutate
   keep-alive state.

   Claude Code can label side requests with
   `x-claude-code-request-class: auxiliary`. cc-lb excludes these requests
   before issuing or claiming keep-alive completion state, observing or
   changing a generation, enqueueing, cancellation, or recording completion
   and `not_tracked` bookkeeping. The exclusion applies even when the
   reported kind remains `unknown`, `subagent`, `main`, or `advisor`.
   Legitimate `unknown` and `subagent` turns without the explicit auxiliary
   class retain their existing eligibility and agent-scope isolation.

   The [Claude Code gateway hint contract](https://code.claude.com/docs/en/llm-gateway-protocol#gateway-hint-headers)
   describes these hints as optional per-request metadata. Claude Code
   v2.1.273 and later can send them; for a custom `ANTHROPIC_BASE_URL`,
   clients enable them with `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1` because
   they are off by default. cc-lb does not promise to consume every hint.
   Clients that omit the request-class hint retain the existing body
   classification behavior.
4. Does the response report at least `.min_cache_tokens` cached input tokens?
   A completed `AgentInTurn` response below that threshold is cancelled with
   `reason="cached_prefix_too_small"` and is not scheduled.
   A request (after any shape plugin) that uses enabled thinking,
   `output_config.format`, or a forced `tool_choice` is not tracked, and an
   active session is cancelled with `reason="zero_token_renewal_unsupported"`:
   Anthropic rejects those fields on a `max_tokens: 0` renewal.
5. Inspect the principal's Cache keepalive view for its latest decision and
   session status. Normal `UserTurn` and `Ambiguous` decisions stop renewal;
   the classifier counter is process-wide, not a per-session diagnostic.
   Check `snapshot_too_large` and the configured size cap if capture failed.
6. Does the latest durable renewal turn report a miss? Inspect both
   `cache_read_input_tokens` and the cache-creation total and TTL buckets.
   Anthropic renewals stop on zero cache reads or on partial prefix rebuilding:
   positive cache reads together with positive cache creation are also a miss.
   Compare the shaped request's prompt blocks and breakpoints with the
   snapshot, and check whether the fire landed within the TTL window.
   A fresh real request can establish a new snapshot; no local counter
   proves the provider cache is warm.
7. Inspect the session's terminal reason and stored error.
   `unsupported_provider` means an existing, enabled upstream is not a
   supported direct Anthropic OAuth kind. `dispatch_error` with
   `error="upstream gone"` means the upstream is missing, disabled, or deleted.
   Other `dispatch_error` messages describe reservation, transport, or provider
   failures; inspect the stored error and upstream logs.

If none of the above explains it, capture a request and response body
alongside the admin-config output and file an issue.

## OpenAI regression checks

The strict loopback suite covers the Responses HTTP listener, native cached-token
evidence, encrypted SQLite snapshots, admin scheduling, streaming completion,
and a scheduler renewal. Run it after building the server:

```sh
cargo test -p cc-lb-server --test integration openai_cache_keepalive::
CC_LB_BIN="$PWD/target/release/cc-lb" uv run tests/real-client/openai_keepalive_sdk.py
```

The SDK check uses the pinned OpenAI Python client with a local provider fixture.
It does not call OpenAI or establish live cache hits, machine locality, or savings.
