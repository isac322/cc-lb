#!/usr/bin/env node
// Self-contained unit and contract tests for the multi-provider real-CLI E2E QA harness.
// Runs offline with zero provider calls and zero external /tmp dependencies.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import net from 'node:net';
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

function findFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close((err) => (err ? reject(err) : resolve(port)));
    });
    srv.on('error', reject);
  });
}

test('live cc-lb binary E2E across all provider upstreams verifies 5-area backend extensions', async (t) => {
  const repoRoot = path.resolve(HERE, '../../..');
  const ccLbBin = process.env.CC_LB_BIN ?? path.join(repoRoot, 'target/debug/cc-lb');
  if (!existsSync(ccLbBin)) {
    t.skip(`cc-lb binary not built at ${ccLbBin}`);
    return;
  }

  const mockRequests = [];
  const mockServer = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const bodyText = Buffer.concat(chunks).toString('utf8');
      mockRequests.push({
        method: req.method,
        url: req.url,
        headers: { ...req.headers },
        bodyText,
      });

      if (req.method === 'GET' && req.url === '/v1/models') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ object: 'list', data: [{ id: 'gpt-5.2' }] }));
        return;
      }
      if (req.method === 'GET' && req.url === '/v1beta/models') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ models: [{ name: 'models/gemini-2.5-pro' }] }));
        return;
      }
      if (req.method === 'POST' && req.url.includes(':countTokens')) {
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'x-goog-request-id': 'goog-req-count-1',
        });
        res.end(JSON.stringify({ totalTokens: 42 }));
        return;
      }
      if (req.method === 'POST' && req.url.startsWith('/v1/responses')) {
        let parsed = {};
        try {
          parsed = JSON.parse(bodyText);
        } catch {}
        if (parsed.input === 'trigger_http200_failed') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(
            JSON.stringify({
              id: 'resp_fail_200',
              object: 'response',
              status: 'failed',
              service_tier: 'priority',
              error: { code: 'server_error', message: 'synthetic upstream failure on 200' },
              usage: { input_tokens: 12, output_tokens: 3, total_tokens: 15 },
            }),
          );
          return;
        }
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'ratelimit-limit': '100;w=60',
          'ratelimit-remaining': '99',
          'ratelimit-reset': '60',
        });
        res.end(
          JSON.stringify({
            id: `resp_ok_${mockRequests.length}`,
            object: 'response',
            status: 'completed',
            model: parsed.model ?? 'gpt-5.2',
            service_tier: 'priority',
            output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'ok' }] }],
            usage: {
              input_tokens: 100,
              input_tokens_details: { cached_tokens: 20, cache_write_tokens: 10 },
              output_tokens: 5,
              total_tokens: 105,
            },
          }),
        );
        return;
      }
      if (req.method === 'POST' && req.url.includes(':generateContent')) {
        let parsedGm = {};
        try {
          parsedGm = JSON.parse(bodyText);
        } catch {}
        const promptText = JSON.stringify(parsedGm);
        if (promptText.includes('trigger_gemini_429_retry_info')) {
          res.writeHead(429, {
            'Content-Type': 'application/json',
            'x-goog-request-id': 'goog-req-429-retryinfo',
          });
          res.end(
            JSON.stringify({
              error: {
                code: 429,
                message: 'Quota exceeded for metric',
                status: 'RESOURCE_EXHAUSTED',
                details: [
                  {
                    '@type': 'type.googleapis.com/google.rpc.RetryInfo',
                    retryDelay: '9s',
                  },
                ],
              },
            }),
          );
          return;
        }
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'x-goog-request-id': 'goog-req-gen-1',
          'retry-after': '15',
        });
        res.end(
          JSON.stringify({
            candidates: [{ content: { role: 'model', parts: [{ text: 'gemini-ok' }] }, finishReason: 'STOP' }],
            usageMetadata: {
              promptTokenCount: 100,
              cachedContentTokenCount: 20,
              cacheCreationTokenCount: 10,
              candidatesTokenCount: 6,
              totalTokenCount: 106,
            },
          }),
        );
        return;
      }
      if (req.method === 'POST' && req.url.includes('streamGenerateContent')) {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'x-goog-request-id': 'goog-req-stream-1',
        });
        const frame = JSON.stringify({
          response: {
            candidates: [{ content: { role: 'model', parts: [{ text: 'code-assist-stream-ok' }] }, finishReason: 'STOP' }],
            usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 4, totalTokenCount: 14 },
          },
        });
        res.end(`data: ${frame}\n\n`);
        return;
      }
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'unhandled mock route', url: req.url }));
    });
  });

  await new Promise((resolve) => mockServer.listen(0, '127.0.0.1', resolve));
  const mockPort = mockServer.address().port;
  const mockBaseUrl = `http://127.0.0.1:${mockPort}`;

  const [proxyPort, adminPort, metricsPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
    findFreePort(),
  ]);
  const runDir = path.join(UNIT_ROOT, 'live-e2e-5areas');
  mkdirSync(path.join(runDir, 'data'), { recursive: true, mode: 0o700 });
  const sqlitePath = path.join(runDir, 'cc-lb.sqlite');
  const configPath = path.join(runDir, 'cc-lb.toml');
  const adminToken = randomBytes(32).toString('hex');
  const masterKeyHex = randomBytes(32).toString('hex');

  writeFileSync(
    configPath,
    [
      '[runtime]',
      `data_dir = ${JSON.stringify(path.join(runDir, 'data'))}`,
      '',
      '[listener]',
      `proxy_addr = "127.0.0.1:${proxyPort}"`,
      `admin_addr = "127.0.0.1:${adminPort}"`,
      `metrics_addr = "127.0.0.1:${metricsPort}"`,
      '',
      '[storage]',
      'kind = "sqlite"',
      `path = ${JSON.stringify(sqlitePath)}`,
      '',
      '[aead]',
      'key_env = "CC_LB_MASTER_KEY"',
      '',
      '[[admin.auth.providers]]',
      'kind = "static_token"',
      'id = "local"',
      'token_env = "CC_LB_ADMIN_TOKEN"',
      '',
    ].join('\n'),
    { mode: 0o600 },
  );

  const proc = spawn(ccLbBin, ['serve', '--config', configPath], {
    cwd: runDir,
    env: {
      ...process.env,
      CC_LB_ADMIN_TOKEN: adminToken,
      CC_LB_MASTER_KEY: masterKeyHex,
      RUST_LOG: 'info',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const cleanup = async () => {
    try {
      proc.kill('SIGTERM');
    } catch {}
    await new Promise((r) => mockServer.close(() => r()));
  };
  t.after(cleanup);

  // Wait for /admin/health
  let ready = false;
  for (let i = 0; i < 60; i += 1) {
    try {
      const r = await fetch(`http://127.0.0.1:${adminPort}/admin/health`);
      if (r.status === 200) {
        ready = true;
        break;
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 150));
  }
  assert.equal(ready, true, 'cc-lb /admin/health must reach 200');

  const adminCall = async (method, routePath, body, extraHeaders = {}) => {
    const res = await fetch(`http://127.0.0.1:${adminPort}${routePath}`, {
      method,
      headers: {
        Authorization: `Bearer ${adminToken}`,
        ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
        ...extraHeaders,
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {}
    return { status: res.status, headers: res.headers, json, text };
  };

  // 1. Create Principal + API Key
  const pRes = await adminCall('POST', '/admin/v1/principals', {
    name: 'e2e-5areas-principal',
    kind: 'human',
    allowed_models: [],
    allowed_upstreams: [],
    default_limits: [],
  });
  assert.equal(pRes.status, 201, `create principal failed: ${pRes.text}`);
  const principalId = pRes.json.id;

  const kRes = await adminCall('POST', `/admin/v1/principals/${principalId}/keys`, {
    label: 'e2e-5areas-key',
  });
  assert.equal(kRes.status, 201, `create key failed: ${kRes.text}`);
  const clientKey = kRes.json.plaintext_key;
  const keyId = kRes.json.id;

  // 2. Add OpenAI API Key, Gemini API Key, OpenAI OAuth, and Gemini OAuth upstreams
  const oaKeyUp = await adminCall('POST', '/admin/v1/upstreams', {
    name: 'e2e-openai-api-key',
    kind: 'openai_api_key',
    base_url: mockBaseUrl,
    api_key_value: 'sk-openai-mock-secret',
    warmup_enabled: false,
  });
  assert.equal(oaKeyUp.status, 201, `create openai_api_key failed: ${oaKeyUp.text}`);
  const oaKeyUpstreamId = oaKeyUp.json.id;

  const gmKeyUp = await adminCall('POST', '/admin/v1/upstreams', {
    name: 'e2e-gemini-api-key',
    kind: 'gemini_api_key',
    base_url: mockBaseUrl,
    api_key_value: 'AIzaSyMockGeminiSecretKey123456',
    warmup_enabled: false,
  });
  assert.equal(gmKeyUp.status, 201, `create gemini_api_key failed: ${gmKeyUp.text}`);
  const gmKeyUpstreamId = gmKeyUp.json.id;
  assert.equal(
    oaKeyUp.headers.get('cache-control')?.includes('no-store'),
    true,
    'openai_api_key create response must set Cache-Control: no-store',
  );
  assert.equal(
    gmKeyUp.headers.get('cache-control')?.includes('no-store'),
    true,
    'gemini_api_key create response must set Cache-Control: no-store',
  );
  const invalidWarmupUp = await adminCall('POST', '/admin/v1/upstreams', {
    name: 'e2e-invalid-openai-warmup',
    kind: 'openai_api_key',
    base_url: mockBaseUrl,
    api_key_value: 'sk-openai-warmup-reject',
    warmup_enabled: true,
  });
  assert.equal(invalidWarmupUp.status, 400, 'non-AnthropicOauth warmup_enabled:true must be rejected with 400');
  assert.equal(invalidWarmupUp.json?.error, 'warmup_unsupported_kind');


  const nowSec = Math.floor(Date.now() / 1000);
  const oaOauthUp = await adminCall('POST', '/admin/v1/upstreams', {
    name: 'e2e-openai-oauth',
    kind: 'openai_oauth',
    base_url: mockBaseUrl,
    enabled: false,
    warmup_enabled: false,
    oauth_tokens: {
      access_token: 'oa-oauth-mock-token',
      refresh_token: '',
      expires_at_unix_secs: nowSec + 3600,
      scopes: [],
      never_refresh: true,
      account_id: 'acct-mock-1',
    },
  });
  assert.equal(oaOauthUp.status, 201, `create openai_oauth failed: ${oaOauthUp.text}`);
  const oaOauthUpstreamId = oaOauthUp.json.id;
  const setUpstreamEnabled = async (id, enabled) => {
    const cur = await adminCall('GET', `/admin/v1/upstreams/${id}`);
    const etag = cur.headers.get('etag') ?? `"${cur.json.revision}"`;
    const res = await adminCall('PATCH', `/admin/v1/upstreams/${id}`, { enabled }, { 'If-Match': etag });
    assert.equal(res.status, 200, `setUpstreamEnabled(${id}, ${enabled}) failed: ${res.text}`);
  };
  await setUpstreamEnabled(oaOauthUpstreamId, false);

  const gmOauthUp = await adminCall('POST', '/admin/v1/upstreams', {
    name: 'e2e-gemini-oauth',
    kind: 'gemini_oauth',
    base_url: mockBaseUrl,
    project_id: 'proj-mock-1',
    enabled: false,
    warmup_enabled: false,
    oauth_tokens: {
      access_token: 'gm-oauth-mock-token',
      refresh_token: '',
      expires_at_unix_secs: nowSec + 3600,
      scopes: [],
      never_refresh: true,
      project_id: 'proj-mock-1',
    },
  });
  assert.equal(gmOauthUp.status, 201, `create gemini_oauth failed: ${gmOauthUp.text}`);
  const gmOauthUpstreamId = gmOauthUp.json.id;
  await setUpstreamEnabled(gmOauthUpstreamId, false);

  // 3. Verify D1 (Preflight & Recheck & Access projection)
  const oaCandidatePreflight = await adminCall('POST', '/admin/v1/upstreams/preflight', {
    kind: 'openai_api_key',
    api_key_value: 'sk-openai-candidate-probe',
    base_url: mockBaseUrl,
  });
  assert.equal(oaCandidatePreflight.status, 200, `openai candidate preflight failed: ${oaCandidatePreflight.text}`);
  assert.equal(oaCandidatePreflight.json.verdict, 'ok');

  const gmCandidatePreflight = await adminCall('POST', '/admin/v1/upstreams/preflight', {
    kind: 'gemini_api_key',
    api_key_value: 'AIzaSyCandidateGeminiProbe123456',
    base_url: mockBaseUrl,
  });
  assert.equal(gmCandidatePreflight.status, 200, `gemini candidate preflight failed: ${gmCandidatePreflight.text}`);
  assert.equal(gmCandidatePreflight.json.verdict, 'ok');

  const oaRecheck = await adminCall('POST', `/admin/v1/upstreams/${oaKeyUpstreamId}/preflight`);
  assert.equal(oaRecheck.status, 200, `openai recheck failed: ${oaRecheck.text}`);
  assert.equal(oaRecheck.json.verdict, 'ok');
  const oaGetAfterRecheck = await adminCall('GET', `/admin/v1/upstreams/${oaKeyUpstreamId}`);
  assert.equal(oaGetAfterRecheck.status, 200);
  assert.equal(oaGetAfterRecheck.json.api_key_access?.verified, true);

  const gmRecheck = await adminCall('POST', `/admin/v1/upstreams/${gmKeyUpstreamId}/preflight`);
  assert.equal(gmRecheck.status, 200, `gemini recheck failed: ${gmRecheck.text}`);
  assert.equal(gmRecheck.json.verdict, 'ok');
  const gmGetAfterRecheck = await adminCall('GET', `/admin/v1/upstreams/${gmKeyUpstreamId}`);
  assert.equal(gmGetAfterRecheck.status, 200);
  assert.equal(gmGetAfterRecheck.json.api_key_access?.verified, true);

  // 4. Verify D2 (OAuth status for openai_oauth and gemini_oauth)
  const oaOauthStatus = await adminCall('GET', `/admin/v1/upstreams/${oaOauthUpstreamId}/oauth/status`);
  assert.equal(oaOauthStatus.status, 200);
  assert.equal(oaOauthStatus.json.status, 'active');
  assert.equal(oaOauthStatus.json.has_credentials, true);
  assert.equal(oaOauthStatus.json.mode, 'long_lived_365d');
  assert.equal(oaOauthStatus.json.can_refresh, false);

  const gmOauthStatus = await adminCall('GET', `/admin/v1/upstreams/${gmOauthUpstreamId}/oauth/status`);
  assert.equal(gmOauthStatus.status, 200);
  assert.equal(gmOauthStatus.json.status, 'active');
  assert.equal(gmOauthStatus.json.has_credentials, true);
  assert.equal(gmOauthStatus.json.mode, 'long_lived_365d');
  assert.equal(gmOauthStatus.json.can_refresh, false);

  // 5. Verify B1 & A2 (Upload Wasm shape passthrough plugin, attach to principal, verify scrubbing & multi-provider execution)
  const wasmPath = path.join(repoRoot, 'target/wasm32-unknown-unknown/release/wasmtime_shape_passthrough.wasm');
  if (existsSync(wasmPath)) {
    const wasmBytes = readFileSync(wasmPath);
    const form = new FormData();
    form.append('bytes', new Blob([wasmBytes], { type: 'application/wasm' }), 'wasmtime_shape_passthrough.wasm');
    form.append('original_filename', 'wasmtime_shape_passthrough.wasm');
    const uploadRes = await fetch(`http://127.0.0.1:${adminPort}/admin/v1/plugins/wasm`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${adminToken}` },
      body: form,
    });
    const uploadJson = await uploadRes.json();
    assert.equal(uploadRes.status, 201, `wasm upload failed: ${JSON.stringify(uploadJson)}`);

    const chainRes = await adminCall('POST', `/admin/v1/principals/${principalId}/plugin-chain`, {
      slot: 'shape',
      wasm_registry_id: uploadJson.id,
    });
    assert.equal(chainRes.status, 201, `insert shape chain failed: ${chainRes.text}`);
  }

  // Wait briefly for dynamic view rebuild
  await new Promise((r) => setTimeout(r, 300));

  // Send OpenAI /v1/responses with cross-provider header + ?key= query secret
  const oaProxyRes = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses?key=leak_query_secret&keep=1`, {
    method: 'POST',
    headers: {
      'x-goog-api-key': clientKey,
      'Content-Type': 'application/json',
      'api-key': 'leak-downstream-api-key',
    },
    body: JSON.stringify({ model: 'gpt-5.2', input: 'hello' }),
  });
  assert.equal(oaProxyRes.status, 200);
  const lastOaMock = mockRequests.at(-1);
  assert.equal(lastOaMock.url.includes('leak_query_secret'), false, 'query key= must be scrubbed before plugin/upstream');
  assert.equal(lastOaMock.url.includes('keep=1'), true, 'non-secret query parameter must be preserved');
  assert.equal(lastOaMock.headers['x-goog-api-key'], undefined, 'cross-provider x-goog-api-key must be stripped');
  assert.equal(lastOaMock.headers['api-key'], undefined, 'api-key must be stripped');
  assert.equal(lastOaMock.headers['authorization'], 'Bearer sk-openai-mock-secret');

  // Send OpenAI HTTP 200 status=failed response (C2)
  const oaFailed200Res = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${clientKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ model: 'gpt-5.2', input: 'trigger_http200_failed' }),
  });
  assert.equal(oaFailed200Res.status, 200);
  const oaFailed200Json = await oaFailed200Res.json();
  assert.equal(oaFailed200Json.status, 'failed');

  // Send Gemini :generateContent and :countTokens (E1, C5, C7)
  const gmGenRes = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:generateContent?key=leak_gm_query&foo=bar`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
        'api-key': 'leak-generic-api-key',
      },
      body: JSON.stringify({ contents: [{ role: 'user', parts: [{ text: 'hi gemini' }] }] }),
    },
  );
  assert.equal(gmGenRes.status, 200);
  const lastGmMock = mockRequests.at(-1);
  assert.equal(lastGmMock.url.includes('leak_gm_query'), false, 'Gemini query key= must be scrubbed');
  assert.equal(lastGmMock.headers['api-key'], undefined, 'api-key header must be stripped');
  assert.equal(lastGmMock.headers['x-goog-api-key'], 'AIzaSyMockGeminiSecretKey123456');

  const gmCountRes = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:countTokens`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ contents: [{ role: 'user', parts: [{ text: 'count me' }] }] }),
    },
  );
  assert.equal(gmCountRes.status, 200, 'Gemini :countTokens must succeed through proxy');
  const gmCountJson = await gmCountRes.json();
  assert.equal(gmCountJson.totalTokens, 42);

  // 6. Verify B2 (Gemini OAuth SSE envelope unwrapping composed with active shape/transform dialect)
  await setUpstreamEnabled(gmKeyUpstreamId, false);
  await setUpstreamEnabled(gmOauthUpstreamId, true);
  await new Promise((r) => setTimeout(r, 300));

  const gmOauthStreamRes = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:streamGenerateContent`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ contents: [{ role: 'user', parts: [{ text: 'stream code assist' }] }] }),
    },
  );
  assert.equal(gmOauthStreamRes.status, 200);
  const gmOauthStreamText = await gmOauthStreamRes.text();
  assert.equal(
    gmOauthStreamText.includes('"code-assist-stream-ok"'),
    true,
    `Gemini OAuth SSE must unwrap response envelope: ${gmOauthStreamText}`,
  );
  assert.equal(
    gmOauthStreamText.includes('"response":{'),
    false,
    `Outer Code Assist response envelope must be unwrapped: ${gmOauthStreamText}`,
  );
  // Verify Gemini OAuth :countTokens wrapping into /v1internal:countTokens with {"request":{"model":"models/...", ...}}
  const gmOauthCountRes = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:countTokens?key=leak_oauth_count_key&keep_count=1`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        generateContentRequest: {
          contents: [{ role: 'user', parts: [{ text: 'count via gemini oauth' }] }],
        },
      }),
    },
  );
  assert.equal(gmOauthCountRes.status, 200, `Gemini OAuth :countTokens failed: ${await gmOauthCountRes.clone().text()}`);
  const gmOauthCountJson = await gmOauthCountRes.json();
  assert.equal(gmOauthCountJson.totalTokens, 42);
  const lastGmOauthCountMock = mockRequests.at(-1);
  assert.equal(
    lastGmOauthCountMock.url.startsWith('/v1internal:countTokens'),
    true,
    `Gemini OAuth :countTokens must rewrite path to /v1internal:countTokens, got: ${lastGmOauthCountMock.url}`,
  );
  assert.equal(
    lastGmOauthCountMock.url.includes('leak_oauth_count_key'),
    false,
    'Gemini OAuth :countTokens query key= must be scrubbed',
  );
  assert.equal(
    lastGmOauthCountMock.url.includes('keep_count=1'),
    true,
    'Gemini OAuth :countTokens non-secret query parameter must be preserved',
  );
  const wrappedCountBody = JSON.parse(lastGmOauthCountMock.bodyText);
  assert.equal(wrappedCountBody.model, undefined, 'v1internal:countTokens must not have top-level model');
  assert.equal(wrappedCountBody.project, undefined, 'v1internal:countTokens must not have top-level project');
  assert.equal(wrappedCountBody.request?.model, 'models/gemini-2.5-pro');

  // Verify built-in SubscriptionPreferenceFilter OAuth-first selection and API-key fallback on OpenAI pool
  await setUpstreamEnabled(oaKeyUpstreamId, true);
  await setUpstreamEnabled(oaOauthUpstreamId, true);
  await new Promise((r) => setTimeout(r, 300));

  const oaSubPrefRes = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${clientKey}`,
      'Content-Type': 'application/json',
      session_id: 'codex-e2e-session-123',
    },
    body: JSON.stringify({ model: 'gpt-5.2', input: 'prefer oauth over api key' }),
  });
  assert.equal(oaSubPrefRes.status, 200);
  const lastOaSubPrefMock = mockRequests.at(-1);
  assert.equal(
    lastOaSubPrefMock.headers['authorization'],
    'Bearer oa-oauth-mock-token',
    'SubscriptionPreferenceFilter must select OpenAI OAuth upstream over OpenAI API key when both are enabled',
  );

  await setUpstreamEnabled(oaOauthUpstreamId, false);
  await new Promise((r) => setTimeout(r, 300));
  const oaFallbackRes = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${clientKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ model: 'gpt-5.2', input: 'fallback to api key' }),
  });
  assert.equal(oaFallbackRes.status, 200);
  assert.equal(
    mockRequests.at(-1).headers['authorization'],
    'Bearer sk-openai-mock-secret',
    'SubscriptionPreferenceFilter must fall back to OpenAI API key when OAuth upstream is disabled',
  );

  // Trigger Gemini 429 with JSON RetryInfo.retryDelay (without HTTP Retry-After header)
  await setUpstreamEnabled(gmOauthUpstreamId, false);
  await setUpstreamEnabled(gmKeyUpstreamId, true);
  await new Promise((r) => setTimeout(r, 300));
  const gm429RetryInfoRes = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:generateContent`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        contents: [{ role: 'user', parts: [{ text: 'trigger_gemini_429_retry_info' }] }],
      }),
    },
  );
  assert.equal(gm429RetryInfoRes.status, 429);


  // Re-enable gmKeyUp and disable gmOauthUp
  await setUpstreamEnabled(gmOauthUpstreamId, false);
  await setUpstreamEnabled(gmKeyUpstreamId, true);

  // 7. Verify C1 & E2 (Provider-aware local error formatting & proportional input token estimate)
  const curPrincipal = await adminCall('GET', `/admin/v1/principals/${principalId}`);
  const pEtag = curPrincipal.headers.get('etag') ?? `"${curPrincipal.json.revision}"`;
  const limitPatch = await adminCall(
    'PATCH',
    `/admin/v1/principals/${principalId}`,
    {
      default_limits: [
        {
          kind: 'input_tokens',
          window_secs: 300,
          cap_micros: 5000,
        },
      ],
    },
    { 'If-Match': pEtag },
  );
  assert.equal(limitPatch.status, 200, `patch principal default_limits failed: ${limitPatch.text}`);
  await new Promise((r) => setTimeout(r, 300));

  // Large OpenAI input (>24,000 bytes => >6,000 estimated input tokens > 5,000 cap)
  const oaLimitReject = await fetch(`http://127.0.0.1:${proxyPort}/v1/responses`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${clientKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ model: 'gpt-5.2', input: 'x'.repeat(24000) }),
  });
  assert.equal(oaLimitReject.status, 429, 'Large OpenAI input must exceed 5,000 input token limit');
  const oaLimitJson = await oaLimitReject.json();
  assert.equal(oaLimitJson.type, undefined, 'OpenAI local error must not have outer Anthropic type:"error"');
  assert.equal(oaLimitJson.error?.type, 'rate_limit_error');
  assert.equal(oaLimitJson.error?.code, 'rate_limit_error');
  assert.equal(oaLimitJson.error?.limit_kind, 'input_tokens');

  // Large Gemini contents (>24,000 bytes => >6,000 estimated input tokens > 5,000 cap)
  const gmLimitReject = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:generateContent`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        contents: [{ role: 'user', parts: [{ text: 'y'.repeat(24000) }] }],
      }),
    },
  );
  assert.equal(gmLimitReject.status, 429, 'Large Gemini contents must exceed 5,000 input token limit');
  const gmLimitJson = await gmLimitReject.json();
  assert.equal(gmLimitJson.type, undefined, 'Gemini local error must not have outer Anthropic type:"error"');
  assert.equal(gmLimitJson.error?.code, 429);
  assert.equal(gmLimitJson.error?.status, 'RESOURCE_EXHAUSTED');
  assert.equal(gmLimitJson.error?.limit_kind, 'input_tokens');
  // Large wrapped Gemini countTokens generateContentRequest (>24,000 bytes => >6,000 estimated input tokens > 5,000 cap)
  const gmWrappedCountLimitReject = await fetch(
    `http://127.0.0.1:${proxyPort}/v1beta/models/gemini-2.5-pro:countTokens`,
    {
      method: 'POST',
      headers: {
        'x-goog-api-key': clientKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        generateContentRequest: {
          contents: [{ role: 'user', parts: [{ text: 'z'.repeat(24000) }] }],
        },
      }),
    },
  );
  assert.equal(
    gmWrappedCountLimitReject.status,
    429,
    'Large wrapped generateContentRequest in :countTokens must exceed 5,000 input token limit',
  );


  // 8. Verify C5 & D3 (Event kinds in /admin/v1/events/delta & local API-key usage in /admin/v1/upstreams/{id}/usage)
  await new Promise((r) => setTimeout(r, 800));

  const oaEvents = await adminCall('GET', '/admin/v1/events/delta?since_cursor=0&event_kind=openai_responses');
  assert.equal(oaEvents.status, 200, `oaEvents failed: ${oaEvents.text}`);
  assert.equal(oaEvents.json.events.length >= 2, true, 'openai_responses events must be recorded');
  assert.equal(
    oaEvents.json.events.some((e) => e.service_tier === 'priority'),
    true,
    'top-level OpenAI service_tier must be recorded on HTTP events',
  );
  assert.equal(
    oaEvents.json.events.some((e) => e.error_code === 'upstream_stream_error'),
    true,
    'HTTP 200 response.failed must be recorded with error_code=upstream_stream_error',
  );
  assert.equal(
    oaEvents.json.events.some(
      (e) =>
        e.input_tokens === 70 &&
        e.cache_read_input_tokens === 20 &&
        e.cache_creation_input_tokens === 10,
    ),
    true,
    'OpenAI usage must subtract both cached_tokens (20) and cache_write_tokens (10) from gross input_tokens (100 -> 70)',
  );
  assert.equal(
    oaEvents.json.events.some((e) => e.observed_session_id === 'codex-e2e-session-123'),
    true,
    'Codex session_id header must be captured in observed_session_id',
  );

  const gmGenEvents = await adminCall('GET', '/admin/v1/events/delta?since_cursor=0&event_kind=gemini_generate_content');
  assert.equal(gmGenEvents.status, 200, `gmGenEvents failed: ${gmGenEvents.text}`);
  assert.equal(gmGenEvents.json.events.length >= 2, true, 'gemini_generate_content events must be recorded');
  assert.equal(
    gmGenEvents.json.events.some(
      (e) =>
        e.input_tokens === 70 &&
        e.cache_read_input_tokens === 20 &&
        e.cache_creation_input_tokens === 10,
    ),
    true,
    'Gemini usage must subtract both cachedContentTokenCount (20) and cacheCreationTokenCount (10) from promptTokenCount (100 -> 70)',
  );

  const gmCountEvents = await adminCall('GET', '/admin/v1/events/delta?since_cursor=0&event_kind=gemini_count_tokens');
  assert.equal(gmCountEvents.status, 200, `gmCountEvents failed: ${gmCountEvents.text}`);
  assert.equal(gmCountEvents.json.events.length >= 1, true, 'gemini_count_tokens events must be recorded');

  const oaUsage = await adminCall('GET', `/admin/v1/upstreams/${oaKeyUpstreamId}/usage?range=24h`);
  assert.equal(oaUsage.status, 200, `openai_api_key usage failed: ${oaUsage.text}`);
  assert.equal(oaUsage.json.source, 'estimated');

  const gmUsage = await adminCall('GET', `/admin/v1/upstreams/${gmKeyUpstreamId}/usage?range=24h`);
  assert.equal(gmUsage.status, 200, `gemini_api_key usage failed: ${gmUsage.text}`);
  assert.equal(gmUsage.json.source, 'estimated');
});
