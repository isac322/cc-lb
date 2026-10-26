#!/usr/bin/env node
// Self-contained unit and contract tests for the multi-provider real-CLI E2E QA harness.
// Runs offline with zero provider calls and zero external /tmp dependencies.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { createHash, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createOutboundBudgetGuard } from './approved-core3-subscription-budget-v30.mjs';
import { geminiExpectedUsage, wsProof } from './approved-core3-oracle.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const UNIT_ROOT = path.join(os.tmpdir(), `cc-lb-provider-e2e-selftest-${randomUUID()}`);
const MODEL = 'gpt-5.2';
const CODEX_MODEL = 'gpt-6.1-sol';
const SOURCE = 'synthetic-cap30-budget-regression-only';
const CITE = 'synthetic-prewarm-policy-regression-not-provider-proof';

const BASE_ENTRY = Object.freeze({
  litellm_provider: 'openai',
  max_input_tokens: 272000,
  max_output_tokens: 128000,
  input_cost_per_token: 1.75e-6,
  output_cost_per_token: 1.4e-5,
  cache_read_input_token_cost: 1.75e-7,
  input_cost_per_token_priority: 3.5e-6,
  output_cost_per_token_priority: 2.8e-5,
  cache_read_input_token_cost_priority: 3.5e-7,
});

const GPT61_SOL_ENTRY = Object.freeze({
  litellm_provider: 'openai',
  max_input_tokens: 922000,
  max_output_tokens: 128000,
  input_cost_per_token: 2e-6,
  output_cost_per_token: 1.4e-5,
  cache_read_input_token_cost: 2e-7,
  cache_read_input_token_cost_above_272k_tokens: 4e-7,
  cache_creation_input_token_cost_above_12hr: 1e-5,
  cache_creation_input_token_cost_above_12hr_above_272k_tokens: 5e-6,
  input_cost_per_token_priority: 3.5e-6,
  output_cost_per_token_priority: 2.8e-5,
  cache_read_input_token_cost_priority: 3.5e-7,
  cache_creation_input_token_cost_above_12hr_priority: 2e-5,
  cache_creation_input_token_cost_above_12hr_above_272k_tokens_ultrafast: 1.5e-5,
});

const BASE_POLICY = Object.freeze({
  generate_false_output_zero: true,
  cache_write: 'included_in_input',
  eligible_models: [MODEL],
  citations: [CITE],
  supported_prompt_cache_retention: ['in_memory'],
});

const sha = (value) => createHash('sha256').update(value).digest('hex');
let fixtureNumber = 0;

before(() => {
  mkdirSync(UNIT_ROOT, { recursive: true, mode: 0o700 });
});

after(() => {
  rmSync(UNIT_ROOT, { recursive: true, force: true });
});

function fixture({
  policy = undefined,
  sliceUsd = 6,
  campaignPriorMicros = 0,
  outstandingMaxOutboundMicros = 0,
} = {}) {
  const epoch = path.join(UNIT_ROOT, `synthetic-epoch-${++fixtureNumber}`);
  const name = 'codex-ws-subscription';
  const root = path.join(epoch, name);
  mkdirSync(root, { recursive: true, mode: 0o700 });
  const eligibility = {
    protocol: 'openai-responses',
    eligible_tiers: ['priority'],
    omitted_tier: 'priority',
    citations: [CITE],
  };
  if (policy !== undefined) eligibility.responses_ws_prewarm = policy;
  const ctx = {
    root,
    upstreamId: 'synthetic-prewarm-upstream',
    costCatalog: { prices: { [MODEL]: { key: MODEL } } },
  };
  const plan = {
    case: 'codex-ws',
    name,
    epoch,
    caps: { case_forwards: 12, slice_usd: sliceUsd },
  };
  const guard = createOutboundBudgetGuard({
    ctx,
    plan,
    catalog: { [MODEL]: { ...BASE_ENTRY } },
    models: [MODEL],
    campaignBudgetBefore: { conservativePriorMicros: campaignPriorMicros, outstandingMaxOutboundMicros },
    pricingEligibility: { [MODEL]: eligibility },
  });
  const body = { model: MODEL, input: 'SYNTHETIC_PREWARM_PAYLOAD_NOT_PERSISTED', type: 'response.create' };
  return { guard, body, model: MODEL, budgetFile: path.join(root, 'outbound-budget.json') };
}

function reserveWs(f, id, body) {
  return f.guard.reserve({ id, model: f.model, body, source: SOURCE, transport: 'responses-ws' });
}

function disk(f) {
  assert.equal(existsSync(f.budgetFile), true);
  return JSON.parse(readFileSync(f.budgetFile, 'utf8'));
}

test('missing prewarm policy reserves bounded output cap equal to ordinary create without max_output_tokens', () => {
  const ordinary = fixture();
  const ordinaryBody = { ...ordinary.body };
  const ordinaryResult = reserveWs(ordinary, 'ordinary-create', ordinaryBody);
  assert.equal(ordinaryResult.ok, true);

  const warm = fixture();
  const warmBody = { ...warm.body, generate: false };
  const beforeHash = sha(JSON.stringify(warmBody));
  const warmResult = reserveWs(warm, 'missing-policy-warmup', warmBody);
  assert.deepEqual(warmResult, { ok: true, reason: null, bound_micros: ordinaryResult.bound_micros });
  assert.equal(warm.guard.snapshot().records[0].prewarm, true);
  assert.equal(warm.guard.snapshot().records[0].effective_output_bound_tokens, 16384);
  assert.equal(warm.guard.snapshot().records[0].max_output_tokens, 16384);
  assert.equal(sha(JSON.stringify(warmBody)), beforeHash);
  assert.equal(disk(warm).in_flight_hold_micros, ordinaryResult.bound_micros);
});

test('missing policy rejects explicit retention and unsupported cache option without weakening checks', () => {
  const retention = fixture();
  const retentionResult = reserveWs(retention, 'missing-policy-retention', {
    ...retention.body,
    generate: false,
    prompt_cache_retention: 'in_memory',
  });
  assert.deepEqual(retentionResult, {
    ok: false,
    reason: 'prewarm_cache_retention_not_documented',
    bound_micros: null,
  });
  assert.equal(retention.guard.snapshot().attempts, 0);
  assert.equal(existsSync(retention.budgetFile), false);

  const options = fixture();
  const optionsResult = reserveWs(options, 'unsupported-cache-option', {
    ...options.body,
    generate: false,
    prompt_cache_options: { ttl: '12h' },
  });
  assert.deepEqual(optionsResult, {
    ok: false,
    reason: 'prompt_cache_options_pricing_not_supported',
    bound_micros: null,
  });
  assert.equal(options.guard.snapshot().attempts, 0);
  assert.equal(existsSync(options.budgetFile), false);
});

test('ordinary and missing-policy warmup reservations are independent and cap before dispatch', () => {
  const f = fixture();
  assert.equal(reserveWs(f, 'ordinary-first', { ...f.body, max_output_tokens: 128000 }).ok, true);
  const second = reserveWs(f, 'full-bound-warmup-second', {
    ...f.body,
    generate: false,
    max_output_tokens: 128000,
  });
  assert.deepEqual(second, { ok: false, reason: 'full_outbound_bound_exceeds_slice', bound_micros: 4536000 });
  const snapshot = f.guard.snapshot();
  assert.equal(snapshot.attempts, 1);
  assert.equal(snapshot.records.length, 1);
  assert.equal(snapshot.total_held_micros, 4536000);
  assert.equal(snapshot.in_flight_hold_micros, 4536000);
});

test('missing-policy warmup with absent usage keeps UNKNOWN hold after sealing and rejects future admission', () => {
  const f = fixture();
  assert.equal(reserveWs(f, 'no-usage-warmup', { ...f.body, generate: false }).ok, true);
  const settled = f.guard.settle({ id: 'no-usage-warmup', format: 'openai', providerStatus: 200 });
  assert.equal(settled.ok, false);
  const beforeSeal = f.guard.snapshot();
  assert.equal(beforeSeal.records[0].status, 'UNKNOWN');
  const sealed = f.guard.seal();
  assert.equal(sealed.sealed, true);
  assert.equal(sealed.seal_persisted, true);
  assert.equal(sealed.records[0].status, 'UNKNOWN');
  const afterSeal = reserveWs(f, 'after-seal-new-request', { ...f.body });
  assert.deepEqual(afterSeal, { ok: false, reason: 'outbound_budget_sealed', bound_micros: null });
});

test('unknown tier remains rejected for missing-policy warmup', () => {
  const f = fixture();
  const result = reserveWs(f, 'unknown-tier-warmup', { ...f.body, generate: false, service_tier: 'fast' });
  assert.deepEqual(result, { ok: false, reason: 'wire_service_tier_not_eligible', bound_micros: null });
  assert.equal(f.guard.snapshot().attempts, 0);
  assert.equal(existsSync(f.budgetFile), false);
});

test('exact documented policy alone enables output-zero optimization while retaining input hold', () => {
  const f = fixture({ policy: BASE_POLICY });
  const body = { ...f.body, generate: false, prompt_cache_retention: 'in_memory' };
  const beforeHash = sha(JSON.stringify(body));
  const result = reserveWs(f, 'documented-zero-warmup', body);
  assert.deepEqual(result, { ok: true, reason: null, bound_micros: 229376 });
  assert.equal(f.guard.snapshot().records[0].prewarm, true);
  assert.equal(f.guard.snapshot().records[0].effective_output_bound_tokens, 0);
  assert.equal(f.guard.snapshot().records[0].max_input_tokens, 272000);
  assert.equal(f.guard.snapshot().records[0].max_output_tokens, 0);
  assert.equal(sha(JSON.stringify(body)), beforeHash);
  assert.equal(disk(f).in_flight_hold_micros, 229376);
} );

test('cap30 library preserves slice choices 3 and 6 but rejects 7', () => {
  assert.equal(fixture({ sliceUsd: 3 }).guard.preflight().ok, true);
  assert.equal(fixture({ sliceUsd: 6 }).guard.preflight().ok, true);
  const invalid = fixture({ sliceUsd: 7 });
  assert.equal(invalid.guard.preflight().ok, false);
  assert.equal(invalid.guard.preflight().reason, 'explicit_supported_slice_usd_cap_required');
});

test('cap30 exact actual-bound boundary admits and one micro over rejects without persistence', () => {
  const probe = fixture();
  const bound = reserveWs(probe, 'bound-probe', { ...probe.body }).bound_micros;
  assert.equal(Number.isSafeInteger(bound), true);
  const exact = fixture({ campaignPriorMicros: 30_000_000 - bound - 1, outstandingMaxOutboundMicros: 1 });
  assert.deepEqual(reserveWs(exact, 'exact-cap30', { ...exact.body }), {
    ok: true,
    reason: null,
    bound_micros: bound,
  });
  const over = fixture({ campaignPriorMicros: 30_000_000 + 1 - bound, outstandingMaxOutboundMicros: 0 });
  assert.deepEqual(reserveWs(over, 'over-cap30', { ...over.body }), {
    ok: false,
    reason: 'full_outbound_bound_exceeds_campaign',
    bound_micros: bound,
  });
  assert.equal(over.guard.snapshot().attempts, 0);
  assert.equal(existsSync(over.budgetFile), false);
});

test('cap30 setup rejects prior campaign exposure above thirty', () => {
  const f = fixture({ campaignPriorMicros: 31_000_000 });
  assert.equal(f.guard.preflight().ok, false);
  assert.equal(f.guard.preflight().reason, 'campaign_ceiling_exceeded');
});

test('gpt-6.1-sol catalog rates with _above_12hr and _ultrafast suffixes parse and settle WS service_tier auto', () => {
  const contract = JSON.parse(readFileSync(path.join(HERE, 'approved-core3-cap30-contract.json'), 'utf8'));
  const epoch = path.join(UNIT_ROOT, `synthetic-epoch-${++fixtureNumber}`);
  const root = path.join(epoch, 'codex');
  mkdirSync(root, { recursive: true, mode: 0o700 });
  const eligibility = {
    protocol: 'openai-responses',
    eligible_tiers: ['default'],
    omitted_tier: 'default',
    citations: contract.api_key_pricing_eligibility.models[MODEL].citations,
  };
  const guard = createOutboundBudgetGuard({
    ctx: {
      root,
      upstreamId: 'synthetic-codex-upstream',
      costCatalog: { prices: { [CODEX_MODEL]: { key: CODEX_MODEL } } },
    },
    plan: { case: 'api-key-codex', name: 'codex', epoch, caps: { case_forwards: 32, slice_usd: 6 } },
    catalog: { [CODEX_MODEL]: { ...GPT61_SOL_ENTRY } },
    models: [CODEX_MODEL],
    campaignBudgetBefore: { conservativePriorMicros: 0, outstandingMaxOutboundMicros: 0 },
    pricingEligibility: { [CODEX_MODEL]: eligibility },
  });
  assert.equal(guard.preflight().ok, true);

  const reserved = guard.reserve({
    id: 'ws:ws:1',
    model: CODEX_MODEL,
    body: { type: 'response.create', model: CODEX_MODEL, generate: false, input: [] },
    source: SOURCE,
    transport: 'responses-ws',
  });
  assert.equal(reserved.ok, true);
  assert.equal(reserved.bound_micros <= 1_000_000, true);

  const settled = guard.settle({
    id: 'ws:ws:1',
    usage: {
      input_tokens: 9314,
      cached_tokens: 0,
      output_tokens: 0,
      reasoning_tokens: 0,
      total_tokens: 9314,
    },
    format: 'openai',
    providerStatus: 200,
    serviceTier: 'auto',
  });
  assert.equal(settled.ok, true);
  const settledRecord = guard.snapshot().records[0];
  assert.equal(settledRecord.status, 'SETTLED');
  assert.equal(settledRecord.observed_response_tier, 'default');
  assert.equal(settledRecord.charged_micros, 18628);
});

test('oracle wsProof joins WS turns and accepts omitted thinking_tokens when reasoning_tokens is 0', () => {
  const summary = {
    principal_id: 'principal-1',
    upstream_id: 'upstream-1',
    key_id: 'key-1',
    scenarios: [
      { scenario: 'http', verdict: 'PASS', turns: [] },
      {
        scenario: 'ws',
        verdict: 'PASS',
        turns: [
          {
            turn: 1,
            label: 'codex-ws-turn1',
            cc_lb_events: { count: 1, timed_out: false, invalidated: false },
            requests: [{ transport: 'ws', attempt: 'ws:ws:1', generate: false }],
            responses: [
              {
                attempt: 'ws:ws:1',
                type: 'response.completed',
                response_id: 'resp_test_1',
                usage: {
                  input_tokens: 9314,
                  cached_tokens: 0,
                  output_tokens: 0,
                  reasoning_tokens: 0,
                  total_tokens: 9314,
                },
              },
            ],
          },
        ],
      },
      { scenario: 'ws_reconnect', verdict: 'PASS', turns: [] },
    ],
  };
  const events = [
    {
      request_id: 'req_core_1',
      principal_id: 'principal-1',
      upstream_id: 'upstream-1',
      key_id: 'key-1',
      status: 200,
      input_tokens: 9314,
      cache_read_input_tokens: 0,
      output_tokens: 0,
      thinking_tokens: null,
      cost_usd_micros: 18628,
    },
  ];
  const capture = {
    stats: { acceptedSpans: 1, decodeErrors: 0, rejectedSpans: 0, oversizedRequests: 0 },
    spans: [
      {
        name: 'proxy.responses_websocket.turn',
        resource: { serviceName: 'cc-lb' },
        attributes: {
          'gen_ai.response.id': 'resp_test_1',
          'cc_lb.request.id': 'req_core_1',
        },
      },
    ],
  };
  const proof = wsProof(summary, events, capture);
  assert.equal(proof.verdict, 'PASS');
  assert.equal(proof.turns.length, 1);
  assert.equal(
    proof.checks.find((c) => c.name.endsWith(':usage_matches_raw_without_reasoning_double_add'))?.verdict,
    'PASS',
  );
});

test('geminiExpectedUsage sums candidatesTokenCount and thoughtsTokenCount into output_tokens', () => {
  const expected = geminiExpectedUsage({
    promptTokenCount: 1000,
    cachedContentTokenCount: 200,
    candidatesTokenCount: 79,
    thoughtsTokenCount: 123,
  });
  assert.deepEqual(expected, {
    input_tokens: 800,
    cache_read_input_tokens: 200,
    output_tokens: 202,
    thinking_tokens: 123,
  });
});

test('Object.create projection preserves non-enumerable _adminToken while overriding upstreams', () => {
  const ctx = {
    adminUrl: 'http://127.0.0.1:19131',
    upstreams: [{ host: '127.0.0.1:21130' }],
  };
  Object.defineProperty(ctx, '_adminToken', { value: 'secret-admin-token', enumerable: false });
  const projected = Object.create(ctx, {
    upstreams: {
      value: [{ host: 'generativelanguage.googleapis.com' }],
      enumerable: true,
    },
  });
  assert.equal(projected._adminToken, 'secret-admin-token');
  assert.equal(projected.upstreams[0].host, 'generativelanguage.googleapis.com');
  assert.equal(Object.keys(projected).includes('_adminToken'), false);
});
