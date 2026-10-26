#!/usr/bin/env node
// Parent-run offline oracle. Reads only pinned scratch artifacts; stdout only, no APIs/credentials/DBs.
// No sequence/nearest-time matching. Response ID -> real turn span -> local request ID -> durable event.
import { readFile, readdir } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const BASE = process.env.CC_LB_QA_WORK_DIR ?? path.dirname(new URL(import.meta.url).pathname);
const hash = (value) => createHash('sha256').update(value).digest('hex');
async function hashFile(file) {
  const state = createHash('sha256');
  for await (const chunk of createReadStream(file)) state.update(chunk);
  return state.digest('hex');
}
const finite = (v) => Number.isSafeInteger(v) && v >= 0;
const must = (ok, code) => { if (!ok) throw new Error(`INVALID:${code}`); };
const verdict = (checks) => checks.some((c) => c.verdict === 'FAIL') ? 'FAIL' : checks.some((c) => c.verdict !== 'PASS') ? 'INCOMPLETE' : 'PASS';
const check = (name, ok, missing = false) => ({ name, verdict: ok ? 'PASS' : missing ? 'INCOMPLETE' : 'FAIL' });
const json = async (file) => { try { return JSON.parse(await readFile(file, 'utf8')); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } };

// Bookkeeping validation only: this does not prove that transport hooks ran or that a
// provider invoice/funding balance equals catalog list-price accounting.
export function validateOutboundBudgetMetadata(snapshot, { expectedCase, expectedSliceCapMicros } = {}) {
  must(snapshot?.schema === 'outbound-budget-v1', 'outbound_budget_schema_missing');
  must(typeof snapshot.case === 'string' && (!expectedCase || snapshot.case === expectedCase), 'outbound_budget_case_mismatch');
  must([3000000, 6000000].includes(snapshot.slice_usd_cap_micros), 'explicit_original_or_new_slice_cap_required');
  if (expectedSliceCapMicros !== undefined) must(snapshot.slice_usd_cap_micros === expectedSliceCapMicros, 'outbound_budget_selected_cap_mismatch');
  must(snapshot.campaign_usd_cap_micros === 15000000, 'outbound_global_cap_changed');
  must(snapshot.unsafe_integer_cost_evidence === false && snapshot.bound_breach === false, 'unsafe_or_breached_outbound_cost');
  must(/^[0-9a-f]{64}$/.test(snapshot.catalog_sha256 ?? ''), 'outbound_catalog_pin_missing');
  must(snapshot.pricing_basis === 'catalog_list_price_model_not_provider_guarantee' && snapshot.real_paid_funding_or_invoice_state === 'UNVERIFIED', 'catalog_price_not_invoice_or_credit_proof');
  must(finite(snapshot.attempts) && finite(snapshot.case_forward_cap) && snapshot.attempts <= snapshot.case_forward_cap, 'outbound_attempt_cap_invalid');
  must(Array.isArray(snapshot.records) && snapshot.records.length === snapshot.attempts, 'outbound_record_count_mismatch');
  let spent = 0n; let inFlight = 0n; let unknown = 0n;
  const ids = new Set();
  for (const record of snapshot.records) {
    must(/^[0-9a-f]{64}$/.test(record.id_sha256 ?? '') && !ids.has(record.id_sha256), 'outbound_record_identity_invalid');
    ids.add(record.id_sha256);
    must(finite(record.reserved_bound_micros) && finite(record.charged_micros) && record.bound_breach === false, 'outbound_record_cost_invalid');
    must(finite(record.max_input_tokens) && record.max_input_tokens > 0 && finite(record.max_output_tokens) && finite(record.catalog_max_output_tokens) && record.max_output_tokens <= record.catalog_max_output_tokens, 'outbound_hard_token_bounds_invalid');
    const reserved = BigInt(record.reserved_bound_micros);
    const charged = BigInt(record.charged_micros);
    must(charged <= reserved, 'outbound_charge_exceeds_reserved_bound');
    if (record.status === 'SETTLED') spent += charged;
    else if (record.status === 'IN_FLIGHT') inFlight += reserved;
    else if (record.status === 'UNKNOWN') unknown += reserved;
    else must(false, 'outbound_unknown_record_status');
  }
  const total = spent + inFlight + unknown;
  must(total <= BigInt(Number.MAX_SAFE_INTEGER), 'outbound_aggregate_unsafe_integer');
  const numberEquals = (field, value) => finite(snapshot[field]) && BigInt(snapshot[field]) === value;
  must(numberEquals('spent_micros', spent) && numberEquals('in_flight_hold_micros', inFlight) && numberEquals('unknown_hold_micros', unknown) && numberEquals('total_held_micros', total), 'outbound_aggregate_mismatch');
  must(snapshot.total_held_micros_exact === total.toString(), 'outbound_exact_total_mismatch');
  must(snapshot.unresolved === (inFlight > 0n || unknown > 0n || snapshot.records.some((record) => record.status !== 'SETTLED')), 'outbound_unresolved_state_mismatch');
  must(finite(snapshot.campaign_before_micros) && BigInt(snapshot.campaign_before_micros) + total <= 15000000n && total <= BigInt(snapshot.slice_usd_cap_micros), 'outbound_global_or_slice_cap_exceeded');
  return { verdict: 'PASS_LEDGER_SHAPE_ONLY', selected_slice_cap_micros: snapshot.slice_usd_cap_micros, conservative_held_micros: Number(total), unresolved: snapshot.unresolved, admission_ready: snapshot.blocked === false, transport_hook_execution_proven: false, provider_invoice_or_funding_proven: false };
}
function options(argv) {
  const o = { preflight: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--preflight') { o.preflight = true; continue; }
    must(['--epoch', '--expected-sha'].includes(argv[i]) && argv[i + 1], 'invalid_argument'); o[argv[i].slice(2)] = argv[++i];
  }
  o.epoch = path.resolve(o.epoch ?? '');
  must(path.dirname(o.epoch) === BASE && /^fixed-[0-9a-f]{12}-core3(?:-[a-z0-9-]{1,16})?$/.test(path.basename(o.epoch)), 'new_core3_epoch_required');
  must(/^[0-9a-f]{64}$/.test(o['expected-sha'] ?? ''), 'expected_sha_required');
  return o;
}
export const WS_REQUIRED_CASES = Object.freeze([
  { id: 'one_create_one_terminal_event', contract: 'Every admitted response.create including generate:false owns exactly one lifecycle and terminal request event. No handshake-only counting.' },
  { id: 'lazy_first_create_model_owner', contract: 'Upgrade does not pin model/upstream. Actual first create model and owned chain determine routing.' },
  { id: 'quota_reject_before_provider_create', contract: 'Exhaust requests/output/cost/concurrent per-turn quota independently. Rejected create produces one terminal rejection event and zero upstream creates.' },
  { id: 'settle_provider_usage_exactly_once', contract: 'Provider-reported input/cache/output usage settles once; OpenAI output includes reasoning already.' },
  { id: 'unknown_previous_response_owner', contract: 'Unknown previous_response_id never invents affinity, never forwards, one failure event.' },
  { id: 'known_owner_fail_closed', contract: 'Observed response ID owner remains fixed across model/candidate changes; unavailable owner never dispatches to another owner.' },
  { id: 'same_lane_fifo_named_parallel', contract: 'Default/same-stream creates obey FIFO; named stream lanes may run in parallel. No blanket concurrent-create rejection.' },
  { id: 'steer_automatic_successor', contract: 'Each provider-triggered successor turn gets its own identity, admission/accounting and terminal event; wire model/stream preserved.' },
  { id: 'disconnect_cancel_release', contract: 'Close/disconnect actual socket mid-turn, release reservations/concurrency promptly, one cancellation event, fresh follow-up admitted without TTL expiry. Do NOT invent response.cancel.' },
  { id: 'reconnect_replay_owner', contract: 'Native reconnect/full replay remains native-observed; response ownership and committed per-turn usage/events survive without cross-owner fallback.' },
  { id: 'auth_failure_exactly_one_event', contract: 'Auth failure records exactly one event and makes zero provider attempts.' },
  { id: 'silent_prechecks', contract: '404/405/drain pre-auth prechecks remain silent with zero lifecycle request events and zero provider attempts.' },
]);

async function loadEvents(epoch) {
  const dir = path.join(epoch, 'codex/evidence');
  let names; try { names = await readdir(dir); } catch (e) { if (e.code === 'ENOENT') return []; throw e; }
  const byId = new Map();
  for (const name of names.filter((n) => /^events-.+\.jsonl$/.test(n))) {
    const lines = (await readFile(path.join(dir, name), 'utf8')).split('\n').filter(Boolean);
    for (const line of lines) {
      const e = JSON.parse(line); if (!e.request_id) continue;
      const prior = byId.get(e.request_id);
      must(!prior || JSON.stringify(prior) === JSON.stringify(e), 'same_request_id_conflicting_durable_rows');
      byId.set(e.request_id, e);
    }
  }
  return [...byId.values()];
}
async function preflight(opts, manifest) {
  must(manifest?.binary?.expected_sha256 === opts['expected-sha'], 'manifest_binary_sha_mismatch');
  const contract = await json(path.join(opts.epoch, 'contract.json'));
  must(contract?.artifact?.expected_sha256 === opts['expected-sha'], 'contract_sha_mismatch');
  must(await hashFile(contract.artifact.path) === opts['expected-sha'], 'current_binary_changed');
  for (const [rel, pin] of Object.entries(manifest.files ?? {})) {
    const file = path.resolve(opts.epoch, rel); must(file.startsWith(`${opts.epoch}/`), 'pinned_file_escapes_epoch');
    must(hash(await readFile(file)) === pin.sha256, `pinned_file_changed:${rel}`);
  }
  must(contract.paid_bounds?.generation_attempts_per_slice === 16 && contract.paid_attempt_overrides?.codex?.max_generation_attempts === 32, 'original_slice_caps_changed');
  must(contract.paid_bounds?.usd_ceiling_per_slice === 3 && contract.paid_bounds?.usd_ceiling_campaign === 15, 'usd_caps_changed');
  const slices = [];
  const reservations = new Map();
  for (const name of await readdir(opts.epoch)) {
    if (!/^approved-core3-(?:subscription|api-key)-.+-reservation\.json$/.test(name)) continue;
    const record = await json(path.join(opts.epoch, name));
    must(record && typeof record.slice === 'string' && !reservations.has(record.slice), 'ambiguous_or_duplicate_slice_reservation');
    const cap = record.slice_cap_micros ?? 3000000;
    must([3000000, 6000000].includes(cap) && Number.isFinite(record.charge_usd) && record.charge_usd >= 0 && record.charge_usd <= cap / 1e6, 'recorded_reservation_cap_invalid');
    reservations.set(record.slice, { record, cap });
  }
  for (const entry of await readdir(opts.epoch, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const dir = path.join(opts.epoch, entry.name);
    const b = await json(path.join(dir, 'budget.json'));
    const d = await json(path.join(dir, 'driver-budget.json'));
    const s = await json(path.join(dir, 'summary.json'));
    const outbound = await json(path.join(dir, 'outbound-budget.json'));
    const reservation = reservations.get(entry.name);
    if (!b && !d && !s && !outbound && !reservation) continue;
    const shape = outbound ? validateOutboundBudgetMetadata(outbound, { expectedSliceCapMicros: reservation?.cap }) : null;
    const cap = outbound?.slice_usd_cap_micros ?? reservation?.cap ?? 3000000;
    const legacyUsd = Math.max(Number(b?.costUsd) || 0, Number(d?.usd) || 0);
    const cancelled = s?.out_of_band_provider_cost?.conservative_upper_bound_usd;
    const cancelReserve = Number.isFinite(cancelled) && cancelled > 0 ? cancelled : 0;
    const heldUsd = Math.max(legacyUsd + cancelReserve, (shape?.conservative_held_micros ?? 0) / 1e6, reservation?.record.charge_usd ?? 0);
    const unknown = b?.costKnown === false && Number(b?.attempts) > 0;
    const fullSliceHeld = reservation?.record.charge_usd === cap / 1e6;
    if (entry.name !== 'prior-campaign-baseline') must(heldUsd <= cap / 1e6, `recorded_slice_cap_exceeded:${entry.name}`);
    slices.push({ slice: entry.name, conservative_held_usd: heldUsd, recorded_creation_cap_micros: cap, unbounded_unknown: unknown && !fullSliceHeld, outbound_ledger: shape, complete_financial_dispatch_authorization: false });
    reservations.delete(entry.name);
  }
  for (const [name, { record, cap }] of reservations) slices.push({ slice: name, conservative_held_usd: record.charge_usd, recorded_creation_cap_micros: cap, orphan_reservation_preserved: true, unbounded_unknown: false, complete_financial_dispatch_authorization: false });
  must(slices.some((slice) => slice.slice === 'prior-campaign-baseline'), 'prior_campaign_baseline_missing');
  const epochExposure = slices.reduce((sum, slice) => sum + slice.conservative_held_usd, 0);
  must(Number.isFinite(epochExposure) && epochExposure <= 15, 'epoch_conservative_cap_exceeded');
  return { verdict: slices.some((slice) => slice.unbounded_unknown) ? 'PASS_SOURCE_PREFLIGHT_FINANCES_PENDING' : 'PASS_SOURCE_PREFLIGHT_ONLY', binary_sha256: opts['expected-sha'], pinned_files: Object.keys(manifest.files).length, epoch_including_frozen_prior_usd: epochExposure, slices, caps: { common_forwards: 16, codex_forwards: 32, original_creation_slice_usd: 3, explicit_new_creation_slice_usd: 6, global_usd: 15 }, whole_root_live_finance_verified: false, authorizes_paid_dispatch: false, paid_dispatch_requirement: 'Only guarded entrypoint with fresh canonical whole-root financial snapshot, reviewed transport hooks and per-forward durable reservation authorizes a paid start.', runtime_calls: 0, paid_calls: 0 };
}

export function wsProof(summary, events, capture) {
  if (!summary) return { verdict: 'NOT_RUN', why: 'new-epoch native Codex summary absent' };
  const checks = [];
  const spans = Array.isArray(capture?.spans) ? capture.spans : [];
  const wsTurns = (summary.scenarios ?? []).filter((s) => s.scenario === 'ws' || s.scenario === 'ws_reconnect').flatMap((s) => (s.turns ?? []).map((t) => ({ scenario: s.scenario, ...t })));
  checks.push(check('native_ws_turns_executed', wsTurns.length > 0, wsTurns.length === 0));
  checks.push(check('all_three_original_native_scenarios_present', ['http', 'ws', 'ws_reconnect'].every((name) => summary.scenarios?.some((s) => s.scenario === name && s.verdict === 'PASS')), !summary.scenarios?.length));
  checks.push(check('collector_received_spans_without_decode_or_drop_errors', Boolean(capture?.stats?.acceptedSpans > 0 && capture.stats.decodeErrors === 0 && capture.stats.rejectedSpans === 0 && capture.stats.oversizedRequests === 0), !capture));
  const joined = [];
  const usedRequestIds = new Set();
  for (const turn of wsTurns) {
    const requests = turn.requests.filter((q) => q.transport === 'ws');
    checks.push(check(`${turn.label}:event_count_per_create`, turn.cc_lb_events?.count === requests.length && !turn.cc_lb_events?.timed_out && !turn.cc_lb_events?.invalidated, !turn.cc_lb_events));
    for (const q of requests) {
      const responses = turn.responses.filter((r) => r.attempt === q.attempt && ['response.completed', 'response.incomplete', 'response.failed'].includes(r.type));
      const r = responses[0];
      checks.push(check(`${turn.label}:${q.attempt}:one_provider_terminal`, responses.length === 1, !r));
      if (!r?.response_id) { checks.push(check(`${turn.label}:${q.attempt}:provider_response_id`, false, true)); continue; }
      const matches = spans.filter((s) => s.name === 'proxy.responses_websocket.turn' && s.resource?.serviceName === 'cc-lb' && s.attributes?.['gen_ai.response.id'] === r.response_id && typeof s.attributes?.['cc_lb.request.id'] === 'string');
      const requestIds = [...new Set(matches.map((s) => s.attributes['cc_lb.request.id']))];
      checks.push(check(`${turn.label}:${q.attempt}:trusted_span_bridge`, requestIds.length === 1, matches.length === 0));
      if (requestIds.length !== 1) continue;
      const requestId = requestIds[0];
      const rows = events.filter((e) => e.request_id === requestId);
      const event = rows[0];
      checks.push(check(`${turn.label}:${q.attempt}:one_durable_event`, rows.length === 1, !event));
      checks.push(check(`${turn.label}:${q.attempt}:unique_turn_identity`, !usedRequestIds.has(requestId)));
      usedRequestIds.add(requestId);
      const u = r.usage;
      const cache = u?.cached_tokens ?? 0;
      const expected = u ? { input_tokens: Math.max(0, u.input_tokens - cache), cache_read_input_tokens: cache, output_tokens: u.output_tokens, ...(u.reasoning_tokens != null ? { thinking_tokens: u.reasoning_tokens } : {}) } : null;
      checks.push(check(`${turn.label}:${q.attempt}:usage_matches_raw_without_reasoning_double_add`, Boolean(expected && event && Object.entries(expected).every(([k, v]) => (event[k] ?? 0) === v)), !u || !event));
      checks.push(check(`${turn.label}:${q.attempt}:principal_and_owner`, Boolean(event && event.principal_id === summary.principal_id && event.upstream_id === summary.upstream_id), !event));
      checks.push(check(`${turn.label}:${q.attempt}:key_identity`, Boolean(event && summary.key_id && (event.key_id ?? event.api_key_id ?? event.client_key_id) === summary.key_id), !event || !summary.key_id));
      checks.push(check(`${turn.label}:${q.attempt}:per_turn_status_not_handshake`, Boolean(event && event.status >= 200 && event.status < 300 && event.status !== 101), !event));
      checks.push(check(`${turn.label}:${q.attempt}:priced_turn`, finite(event?.cost_usd_micros), !event || event.cost_usd_micros == null));
      const observedStreams = [...new Set(matches.map((s) => s.attributes.stream_id ?? s.attributes['cc_lb.responses.stream_id'] ?? s.attributes['gen_ai.request.stream_id']).filter((v) => v !== undefined && v !== null && v !== ''))];
      checks.push(check(`${turn.label}:${q.attempt}:stream_not_ambiguous`, observedStreams.length <= 1));
      joined.push({ scenario: turn.scenario, turn: turn.turn, attempt: q.attempt, provider_response_id_sha256: hash(r.response_id), request_id: requestId, stream_id: observedStreams[0] ?? null, default_lane: observedStreams.length === 0, generate_false: q.generate === false, usage_expected: expected, event_cost_usd_micros: event?.cost_usd_micros ?? null });
    }
  }
  return { verdict: verdict(checks), checks, turns: joined, joining: 'raw provider response_id -> actual OTel gen_ai.response.id + cc_lb.request.id -> durable request_id', no_sequence_join: true, no_fabricated_stream_id: true, limitation: 'One native upstream proves its owner consistency, not cross-account isolation or negative quota/owner fencing. Those remain separate required fixtures.' };
}

export function geminiExpectedUsage(raw) {
  must(raw && finite(raw.promptTokenCount) && finite(raw.candidatesTokenCount), 'raw_gemini_required_usage_missing');
  must(raw.cachedContentTokenCount == null || finite(raw.cachedContentTokenCount), 'raw_gemini_cache_invalid');
  must(raw.thoughtsTokenCount == null || finite(raw.thoughtsTokenCount), 'raw_gemini_thoughts_invalid');
  const cached = raw.cachedContentTokenCount ?? 0;
  const thoughts = raw.thoughtsTokenCount ?? 0;
  return { input_tokens: Math.max(0, raw.promptTokenCount - cached), cache_read_input_tokens: cached, output_tokens: raw.candidatesTokenCount + thoughts, thinking_tokens: thoughts };
}
function component(tokens, perToken) {
  must(finite(tokens) && Number.isFinite(perToken) && perToken >= 0, 'catalog_component_invalid');
  const price = Math.round(perToken * 1e12); must(Number.isSafeInteger(price), 'catalog_price_precision_unrepresentable');
  const value = BigInt(tokens) * BigInt(price) / 1000000n;
  must(value <= BigInt(Number.MAX_SAFE_INTEGER), 'cost_too_large_for_safe_evidence'); return Number(value);
}
async function geminiProof(epoch) {
  const s = await json(path.join(epoch, 'gemini-accounting-control/summary.json'));
  if (!s) return { verdict: 'NOT_RUN', why: 'raw Gemini positive-thought control absent' };
  const raw = s.response?.usageMetadata;
  if (!raw) return { verdict: 'INCOMPLETE', why: 'raw vendor usageMetadata absent; normalized output is not an oracle' };
  const expected = geminiExpectedUsage(raw); const e = s.observability?.event;
  const catalogFile = path.resolve(s.catalog?.path ?? '');
  must(catalogFile.startsWith(`${epoch}/`), 'catalog_outside_new_epoch');
  const catalogBytes = await readFile(catalogFile); must(hash(catalogBytes) === s.catalog?.sha256, 'raw_control_catalog_changed');
  const catalog = JSON.parse(catalogBytes); const price = catalog[s.catalog.key];
  must(price?.litellm_provider === 'gemini', 'non_native_gemini_catalog');
  const costs = {
    input_micros: component(expected.input_tokens, price.input_cost_per_token),
    output_micros: component(expected.output_tokens, price.output_cost_per_token),
    cache_read_micros: expected.cache_read_input_tokens === 0 ? 0 : component(expected.cache_read_input_tokens, price.cache_read_input_token_cost),
  };
  const total = costs.input_micros + costs.output_micros + costs.cache_read_micros;
  const checks = [
    check('positive_candidates_and_thoughts', raw.candidatesTokenCount > 0 && raw.thoughtsTokenCount > 0, !(raw.thoughtsTokenCount > 0)),
    check('raw_vendor_fields_preserved', s.source === 'RAW' && s.CLIcoverage === false && s.paidPostInvocations === 1),
    check('raw_usage_exact_settlement', Boolean(e && Object.entries(expected).every(([k, v]) => e[k] === v)), !e),
    check('component_floor_cost_exact', e?.cost_usd_micros === total, !e || e.cost_usd_micros == null),
    check('one_request_event_trusted_request_id', s.observability?.matchedEventCount === 1 && s.observability?.committedEventCount === 1 && e?.request_id === s.expectedTrace?.requestId),
    check('real_provider_response_id_present', /^[0-9a-f]{64}$/.test(s.response?.providerResponseIdSha256 ?? '')),
  ];
  return { verdict: verdict(checks), checks, provider_raw_usageMetadata: raw, expected_from_raw: expected, expected_cost_components: costs, expected_total_micros: total, observed_total_micros: e?.cost_usd_micros ?? null, rule: 'output=candidates+thoughts exactly once; thinking is a subset, not another priced component; per-component integer floor' };
}

async function main(opts) {
  const manifest = await json(path.join(opts.epoch, 'fixed-epoch.json'));
  const pre = await preflight(opts, manifest);
  if (opts.preflight) { console.log(JSON.stringify(pre)); return; }
  const codex = await json(path.join(opts.epoch, 'codex/summary.json'));
  const events = await loadEvents(opts.epoch);
  const capture = await json(path.join(opts.epoch, 'approved-core3-spans.json'));
  const ws = wsProof(codex, events, capture);
  const gemini = await geminiProof(opts.epoch);
  const remaining = WS_REQUIRED_CASES.filter((c) => !['one_create_one_terminal_event', 'settle_provider_usage_exactly_once', 'reconnect_replay_owner'].includes(c.id)).map((c) => ({ ...c, verdict: 'NOT_RUN', required_evidence: 'Parent-run regression output with exact test name and current source/binary digest; raw provider-bound cases need actual relay dispatch/span/durable-row artifacts. No inferred PASS from native success.' }));
  const trace = await json(path.join(opts.epoch, 'approved-core3-application-trace-proof.json'));
  const anthropic = await json(path.join(opts.epoch, 'anthropic-regression/summary.json'));
  const output = { kind: 'approved_core3_strict_offline_oracle', binary_sha256: opts['expected-sha'], preflight: pre, ws, gemini, application_trace: trace ? { verdict: trace.verdict, expected_requests: trace.expectedRequestCount, proven_requests: trace.provenRequestCount } : { verdict: 'NOT_RUN' }, anthropic: anthropic ? { verdict: anthropic.verdict, checks: anthropic.checks } : { verdict: 'NOT_RUN' }, required_ws_negative_and_multiplex_cases: remaining, subscription: { verdict: 'BLOCKED', reason: 'metadata contract only; actual safe access descriptor/availability not read, live subscription wire not exercised' }, overall: 'PARTIAL', paid_calls_executed_by_oracle: 0, no_database_or_secret_reads: true };
  console.log(JSON.stringify(output, null, 2));
  if ([ws.verdict, gemini.verdict].some((v) => v === 'FAIL')) process.exitCode = 1;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main(options(process.argv.slice(2))).catch((error) => { console.error(error.message); process.exitCode = 2; });
