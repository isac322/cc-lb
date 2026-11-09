#!/usr/bin/env node
// Offline server-contract probe; not native CLI coverage or real-provider cache-hit evidence.
// Node >= 20 or Bun; no packages, database CLI, build, real credentials, or network services required.
//   CC_LB_BIN=/absolute/path/to/prebuilt/cc-lb node tests/real-client/provider-e2e/run-gemini-cache-loopback.mjs --baseline
//   CC_LB_BIN=/absolute/path/to/prebuilt/cc-lb bun tests/real-client/provider-e2e/run-gemini-cache-loopback.mjs
// --baseline expects the pre-fix create=404 and unbound generate/count=503 behavior.
// Default (or --new) expects v1beta create, owner pinning, rejection gates, and restart persistence.
// Optional CC_LB_BASELINE_BIN=/absolute/path/to/old/cc-lb seeds populated SQLite
// through public APIs, then upgrades that same database with CC_LB_BIN before new cases.
// --baseline ignores CC_LB_BASELINE_BIN and uses CC_LB_BIN alone as before.
// QA: healthy uncached generation; no-dispatch auth/unsupported/unbound requests; exact create
// bytes/values and provider credentials; generate/count owner A with B eligible; disabled-owner
// fail-closed behavior; malformed/missing/expired metadata and provider errors never bind;
// duplicate name from B never retargets A; the same SQLite database survives a server restart.
// Owner-epoch mutations cover credentials, base URL, project/access, and an in-flight create
// held across an authenticated admin commit. Both logical provider endpoints share32102.
// All four listeners are loopback: proxy32100/admin32101/fake32102/metrics32103.
// Only synthetic keys are used. Configuration and SQLite are fresh in a private scratch directory.
// Public authenticated admin APIs are the ONLY provisioning/mutation interface (no SQL/bind forging).
// The independent fake-provider JSONL journal is the dispatch oracle, never proxy telemetry.
// Cleanup is an explicit final step: owned children, listeners, and scratch are removed even on failure.
// An outside-repository cleanup ledger is retained as a receipt (empty resources on clean exit).

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { accessSync, appendFileSync, constants, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';

const args = process.argv.slice(2);
assert(args.every((arg) => arg === '--baseline' || arg === '--new'), 'Only --baseline or --new is supported');
assert(!(args.includes('--baseline') && args.includes('--new')), 'Choose one mode');
const baseline = args.includes('--baseline');
const binary = process.env.CC_LB_BIN;
assert(binary && path.isAbsolute(binary), 'CC_LB_BIN must name an absolute, prebuilt executable');
accessSync(binary, constants.X_OK);
const baselineBinary = baseline ? undefined : process.env.CC_LB_BASELINE_BIN;
if (baselineBinary !== undefined) {
  assert(path.isAbsolute(baselineBinary), 'CC_LB_BASELINE_BIN must name an absolute, prebuilt executable');
  accessSync(baselineBinary, constants.X_OK);
}

const PORT = Object.freeze({ proxy: 32100, admin: 32101, provider: 32102, metrics: 32103 });
const origin = (port) => `http://127.0.0.1:${port}`;
// Native Gemini URL shaping replaces base paths, so use Host to distinguish the
// second endpoint without allocating another port. Startup verifies localhost reachability.
const secondaryProviderOrigin = `http://localhost:${PORT.provider}`;
const MODEL = 'models/gemini-2.5-flash';
const CREATE = '/v1beta/cachedContents';
const GENERATE = '/v1beta/models/gemini-2.5-flash:generateContent';
const COUNT = '/v1beta/models/gemini-2.5-flash:countTokens';
const contents = [{ role: 'user', parts: [{ text: 'Offline loopback cache contract probe.' }] }];
const createBody = () => ({ model: MODEL, contents, ttl: '3600s' });
const generationBody = (name) => ({ contents, ...(name ? { cachedContent: name } : {}) });
const countBody = (name) => ({ generateContentRequest: { model: MODEL, ...generationBody(name) } });
const providerKeys = {
  A: 'AIzaSySyntheticLoopbackA00000000000000000',
  B: 'AIzaSySyntheticLoopbackB00000000000000000',
  C: 'AIzaSySyntheticLoopbackC00000000000000000',
};
const adminToken = randomBytes(32).toString('hex');
const masterKey = randomBytes(32).toString('hex');
let clientKey = '';
let isolationClientKey = '';
const runId = randomUUID();
const ledgerPath = path.join(os.tmpdir(), `cc-lb-gemini-cache-${runId}.cleanup.json`);
const ledger = { run_id: runId, created_at: new Date().toISOString(), resources: [], cleaned: [] };
const resources = new Map();
const children = new Map();
const logs = [];
let scratch;
let serverChild;
let plan;
let fatalProviderError;
let cleaning;
const journal = [];
let passed = 0;

function redact(text) {
  let safe = String(text);
  for (const secret of [adminToken, masterKey, clientKey, isolationClientKey, ...Object.values(providerKeys)]) {
    if (secret) safe = safe.split(secret).join('[redacted]');
  }
  return safe.replace(/cclb_[A-Za-z0-9_-]+/g, '[redacted-client-key]');
}
function saveLedger() {
  writeFileSync(ledgerPath, `${JSON.stringify(ledger, null, 2)}\n`, { mode: 0o600 });
}
function record(id, kind, identifier, deleteCommand) {
  const entry = { id, kind, identifier, delete_command: deleteCommand, created_at: new Date().toISOString() };
  ledger.resources.push(entry);
  saveLedger();
}
function retired(id) {
  const entry = ledger.resources.find((item) => item.id === id);
  if (!entry) return;
  ledger.resources = ledger.resources.filter((item) => item.id !== id);
  ledger.cleaned.push({ ...entry, cleaned_at: new Date().toISOString() });
  saveLedger();
}
function check(condition, message) {
  assert(condition, message);
}
function status(result, expected, label) {
  assert.equal(result.status, expected, `${label}: expected HTTP ${expected}, received ${result.status}; ${redact(result.text).slice(0, 900)}`);
}
function noDispatch(before, label) {
  assert.equal(journal.length, before, `${label}: unexpected provider dispatch ${JSON.stringify(journal.slice(before))}`);
}
function exactly(before, owner, method, route, label) {
  const entries = journal.slice(before);
  assert.equal(entries.length, 1, `${label}: expected exactly one provider dispatch, got ${JSON.stringify(entries)}`);
  assert.equal(entries[0].owner, owner, `${label}: wrong credential owner`);
  assert.equal(entries[0].method, method, `${label}: wrong provider method`);
  assert.equal(entries[0].path, route, `${label}: provider path was rewritten`);
  return entries[0];
}
async function test(label, action) {
  if (fatalProviderError) throw fatalProviderError;
  try {
    const detail = await action();
    if (fatalProviderError) throw fatalProviderError;
    passed += 1;
    console.log(`PASS ${label}${detail ? `: ${detail}` : ''}`);
  } catch (error) {
    throw new Error(`FAIL ${label}: ${redact(error.message)}`, { cause: error });
  }
}
async function request(port, method, route, body, headers = {}) {
  const response = await fetch(`${origin(port)}${route}`, {
    method,
    headers: { ...(body === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    redirect: 'error',
    signal: AbortSignal.timeout(8_000),
  });
  const bytes = Buffer.from(await response.arrayBuffer());
  const text = bytes.toString('utf8');
  let json;
  try { json = JSON.parse(text); } catch { /* Non-JSON responses are intentional negative fixtures. */ }
  return { status: response.status, headers: response.headers, bytes, text, json };
}
const admin = (method, route, body, headers = {}) => request(PORT.admin, method, route, body, { authorization: `Bearer ${adminToken}`, ...headers });
const proxy = (method, route, body, key = clientKey) => request(PORT.proxy, method, `/gemini${route}`, body, key === null ? {} : { 'x-goog-api-key': key });

function authBeforeBody(route, key) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const req = http.request({
      host: '127.0.0.1', port: PORT.proxy, method: 'POST', path: `/gemini${route}`, agent: false,
      headers: {
        'content-type': 'application/json', 'content-length': '10485760',
        ...(key === null ? {} : { 'x-goog-api-key': key }),
      },
    });
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      req.destroy();
      if (error) reject(error);
      else resolve(result);
    };
    const timer = setTimeout(() => finish(new Error(`Auth-before-body timed out for ${route}: no request body was sent`)), 4_000);
    req.once('error', (error) => finish(error));
    req.once('response', (response) => {
      const chunks = [];
      response.on('data', (chunk) => chunks.push(chunk));
      response.once('error', (error) => finish(error));
      response.once('end', () => finish(null, { status: response.statusCode, text: Buffer.concat(chunks).toString('utf8') }));
    });
    // Deliberately never req.write/end: authentication must complete before body aggregation.
    req.flushHeaders();
  });
}

async function listen(server, port, id) {
  check(!cleaning, 'Probe is shutting down; refusing to create a listener');
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen({ port, host: '127.0.0.1', exclusive: true }, resolve);
  });
  resources.set(id, server);
  record(id, 'owned_loopback_listener', { address: `127.0.0.1:${port}`, owner_pid: process.pid }, `close in-process listener ${id}`);
}
async function closeListener(id) {
  const owned = resources.get(id);
  if (!owned) return;
  await new Promise((resolve, reject) => {
    owned.close((error) => error ? reject(error) : resolve());
    owned.closeAllConnections?.();
  });
  resources.delete(id);
  retired(id);
}
async function reservePorts() {
  for (const [role, port] of Object.entries(PORT)) {
    if (role === 'provider') continue;
    await listen(net.createServer(), port, `reservation-${role}`);
  }
}
function createdResponse(name) {
  return {
    name,
    model: MODEL,
    createTime: new Date().toISOString(),
    expireTime: new Date(Date.now() + 3_600_000).toISOString(),
    usageMetadata: { totalTokenCount: 21 },
    displayName: 'loopback synthetic cache',
  };
}
function startProvider(port, listenerId) {
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, origin(port));
      if (req.method === 'GET' && url.pathname === '/prices.json') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{}');
        return;
      }
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const raw = Buffer.concat(chunks).toString('utf8');
      let body;
      try { body = JSON.parse(raw); } catch { body = null; }
      const owner = Object.keys(providerKeys).find((key) => req.headers['x-goog-api-key'] === providerKeys[key]) ?? 'UNKNOWN';
      const logicalListener = req.headers.host === `localhost:${PORT.provider}` ? 'fake-provider-secondary' : listenerId;
      const entry = { seq: journal.length + 1, listener: logicalListener, owner, method: req.method, path: url.pathname, raw_path: req.url, query: url.search, body };
      journal.push(entry);
      appendFileSync(path.join(scratch, 'provider-journal.jsonl'), `${JSON.stringify(entry)}\n`, { mode: 0o600 });
      check(owner !== 'UNKNOWN', 'Fake provider received an unknown credential (values intentionally omitted)');
      check(
        ![clientKey, isolationClientKey].some((key) => key && req.headers['x-goog-api-key'] === key),
        'Managed client key leaked to provider',
      );
      check(!req.headers.authorization && !url.searchParams.has('key'), 'Unexpected auth transport at fake provider');
      if (req.method === 'POST' && url.pathname === CREATE) {
        check(plan, 'Unexpected unplanned create request');
        const current = plan;
        plan = undefined;
        if (current.hold) {
          current.hold.arrived();
          await current.hold.released;
        }
        res.writeHead(current.status ?? 200, { 'content-type': current.contentType ?? 'application/json', 'x-loopback-provider': owner });
        res.end(current.bytes);
        return;
      }
      if (req.method === 'POST' && url.pathname === COUNT) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ totalTokens: 21 }));
        return;
      }
      if (req.method === 'POST' && url.pathname === GENERATE) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          candidates: [{ content: { role: 'model', parts: [{ text: `synthetic-owner-${owner}` }] }, finishReason: 'STOP' }],
          usageMetadata: { promptTokenCount: 21, candidatesTokenCount: 1, totalTokenCount: 22 },
          modelVersion: 'gemini-2.5-flash',
        }));
        return;
      }
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { code: 404, status: 'NOT_FOUND', message: 'Synthetic provider has no such operation' } }));
    } catch (error) {
      fatalProviderError = error;
      if (!res.headersSent) res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'loopback fixture rejected request' }));
    }
  });
  return listen(server, port, listenerId);
}
function configure() {
  for (const dir of ['home', 'data', 'tmp']) mkdirSync(path.join(scratch, dir), { mode: 0o700 });
  writeFileSync(path.join(scratch, 'prices.json'), '{}\n', { mode: 0o600 });
  writeFileSync(path.join(scratch, 'cc-lb.toml'), [
    '[runtime]', `data_dir = ${JSON.stringify(path.join(scratch, 'data'))}`, '',
    '[listener]', `proxy_addr = "127.0.0.1:${PORT.proxy}"`, `admin_addr = "127.0.0.1:${PORT.admin}"`, `metrics_addr = "127.0.0.1:${PORT.metrics}"`, '',
    '[storage]', 'kind = "sqlite"', `path = ${JSON.stringify(path.join(scratch, 'storage.sqlite'))}`, '',
    '[price_catalog]', `url = "${origin(PORT.provider)}/prices.json"`, `cache_path = ${JSON.stringify(path.join(scratch, 'prices.json'))}`, '',
    '[aead]', 'key_env = "CC_LB_MASTER_KEY"', '',
    '[[admin.auth.providers]]', 'kind = "static_token"', 'id = "loopback"', 'token_env = "CC_LB_ADMIN_TOKEN"', '',
  ].join('\n'), { mode: 0o600 });
}
function logExcerpt() {
  return redact(logs.join('')).slice(-8_000);
}
async function startServer(binaryOverride = binary) {
  for (const role of ['proxy', 'admin', 'metrics']) await closeListener(`reservation-${role}`);
  check(!cleaning, 'Probe is shutting down; refusing to create a child');
  const child = spawn(binaryOverride, ['serve', '--config', path.join(scratch, 'cc-lb.toml')], {
    cwd: scratch,
    // Do not inherit real credentials, proxies, telemetry endpoints, or existing cc-lb configuration.
    env: {
      PATH: process.env.PATH ?? '', HOME: path.join(scratch, 'home'), TMPDIR: path.join(scratch, 'tmp'),
      XDG_CACHE_HOME: path.join(scratch, 'home'), XDG_CONFIG_HOME: path.join(scratch, 'home'),
      CC_LB_ADMIN_TOKEN: adminToken, CC_LB_MASTER_KEY: masterKey, RUST_LOG: 'info',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const id = `cc-lb-${child.pid ?? randomUUID()}`;
  const state = { child, id, error: null, done: null, exited: false };
  state.done = new Promise((resolve) => {
    child.once('error', (error) => { state.error = error; state.exited = true; resolve(); });
    child.once('exit', (code, signal) => { state.exited = true; state.exit = { code, signal }; resolve(); });
  });
  if (child.pid) {
    children.set(id, state);
    record(id, 'owned_child', { pid: child.pid, binary: binaryOverride, cwd: scratch, config: path.join(scratch, 'cc-lb.toml') }, `kill -TERM ${child.pid}; wait for owned child; kill -KILL only if still alive`);
  }
  for (const stream of [child.stdout, child.stderr]) {
    stream.on('data', (chunk) => {
      logs.push(redact(chunk.toString('utf8')));
      while (logs.length > 100) logs.shift();
    });
  }
  serverChild = state;
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (state.exited) throw new Error(`cc-lb exited during startup: ${state.error?.message ?? JSON.stringify(state.exit)}\n${logExcerpt()}`);
    try {
      const health = await request(PORT.admin, 'GET', '/admin/health');
      if (health.status === 200 && !state.exited) {
        check(existsSync(path.join(scratch, 'storage.sqlite')), 'Server did not initialize the fresh SQLite database');
        return;
      }
    } catch (error) {
      if (state.exited) throw new Error(`cc-lb startup failed: ${error.message}\n${logExcerpt()}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50)); // Bounded readiness, never an assertion sleep.
  }
  throw new Error(`cc-lb readiness timed out on loopback admin32101\n${logExcerpt()}`);
}
async function stopServer(state = serverChild) {
  if (!state) return;
  if (!state.exited) state.child.kill('SIGTERM');
  let timer;
  const exited = await Promise.race([
    state.done.then(() => true),
    new Promise((resolve) => { timer = setTimeout(() => resolve(false), 5_000); }),
  ]);
  clearTimeout(timer);
  if (!exited && !state.exited) {
    state.child.kill('SIGKILL');
    await state.done;
  }
  children.delete(state.id);
  retired(state.id);
  if (serverChild === state) serverChild = undefined;
}
async function awaitSoleOwner(principalId, upstreamId) {
  const deadline = Date.now() + 5_000;
  let last;
  while (Date.now() < deadline) {
    last = await admin('POST', '/admin/v1/router/preview', { principal_id: principalId, provider: 'gemini', path: GENERATE, body: generationBody() });
    if (last.status === 200 && last.json?.winner?.upstream_id === upstreamId) return;
    await new Promise((resolve) => setTimeout(resolve, 25)); // Dynamic-view readiness, no provider dispatch.
  }
  throw new Error(`Dynamic routing view did not expose the sole eligible owner: HTTP ${last?.status}; ${redact(last?.text).slice(0, 900)}`);
}
async function setEnabled(id, enabled) {
  const current = await admin('GET', `/admin/v1/upstreams/${id}`);
  status(current, 200, 'GET upstream');
  const etag = current.headers.get('etag') ?? `W/"${current.json.revision}"`;
  const updated = await admin('PATCH', `/admin/v1/upstreams/${id}`, { enabled }, { 'if-match': etag });
  status(updated, 200, `PATCH upstream enabled=${enabled}`);
  assert.equal(updated.json.enabled, enabled, 'Admin did not apply enabled state');
}
async function patchUpstream(id, patch, label) {
  const current = await admin('GET', `/admin/v1/upstreams/${id}`);
  status(current, 200, `${label}: GET upstream`);
  const revision = current.json?.spec_revision ?? current.json?.revision;
  const etag = current.headers.get('etag') ?? `W/"${revision}"`;
  const updated = await admin('PATCH', `/admin/v1/upstreams/${id}`, patch, { 'if-match': etag });
  status(updated, 200, label);
  assert.equal(updated.json.id, id, `${label}: update returned another upstream`);
  return updated;
}
async function setup() {
  const p = await admin('POST', '/admin/v1/principals', { name: 'gemini-loopback', kind: 'machine', allowed_models: ['gemini-2.5-flash'], allowed_upstreams: [], default_limits: [] });
  status(p, 201, 'Create principal');
  const k = await admin('POST', `/admin/v1/principals/${p.json.id}/keys`, { label: 'loopback-only' });
  status(k, 201, 'Issue client key');
  check(typeof k.json?.plaintext_key === 'string' && k.json.plaintext_key.length > 0, 'Admin did not return managed client key');
  clientKey = k.json.plaintext_key; // Memory only: never printed or written.
  const upstreams = {};
  for (const owner of ['A', 'B']) {
    const result = await admin('POST', '/admin/v1/upstreams', {
      name: `loopback-${owner}`, kind: 'gemini_api_key', base_url: origin(PORT.provider),
      api_key_value: providerKeys[owner], skip_preflight: true, warmup_enabled: false,
    });
    status(result, 201, `Create upstream ${owner}`);
    upstreams[owner] = result.json.id;
  }
  // enabled is not a create field; use the actual revision-guarded update schema.
  await setEnabled(upstreams.B, false);
  await awaitSoleOwner(p.json.id, upstreams.A);
  noDispatch(0, 'Admin setup with skip_preflight');
  return { principal: p.json.id, ...upstreams };
}
async function unavailable(name, label, key = clientKey) {
  for (const [route, body] of [[GENERATE, generationBody(name)], [COUNT, countBody(name)]]) {
    const before = journal.length;
    const result = await proxy('POST', route, body, key);
    status(result, 503, label);
    assert.equal(result.json?.error?.status, 'UNAVAILABLE', `${label}: missing provider-shaped unavailable status`);
    noDispatch(before, label);
  }
}
async function ownerReuse(name, label, expectedOwner = 'A', expectedListener = undefined) {
  for (const [route, body] of [[GENERATE, generationBody(name)], [COUNT, countBody(name)]]) {
    const before = journal.length;
    const result = await proxy('POST', route, body);
    status(result, 200, label);
    const entry = exactly(before, expectedOwner, 'POST', route, label);
    if (expectedListener) assert.equal(entry.listener, expectedListener, `${label}: wrong provider listener`);
    assert.deepEqual(entry.body, body, `${label}: request body changed`);
  }
}
function prepareResponse(body, options = {}) {
  const bytes = options.raw === undefined ? Buffer.from(`  ${JSON.stringify(body, null, 2)}\n`) : Buffer.from(options.raw);
  plan = { bytes, status: options.status ?? 200, contentType: options.contentType ?? 'application/json', hold: options.hold };
  return bytes;
}
function holdCreateResponse() {
  let arrived;
  let release;
  const reached = new Promise((resolve) => { arrived = resolve; });
  const released = new Promise((resolve) => { release = resolve; });
  return { arrived, release, reached, released };
}
async function within(promise, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function commonCases() {
  await test('healthy no-cache generate', async () => {
    const before = journal.length;
    status(await proxy('POST', GENERATE, generationBody()), 200, 'Healthy generation');
    exactly(before, 'A', 'POST', GENERATE, 'Healthy generation');
    return '200; journal=A exactly once';
  });
  await test('unsupported methods and API scopes stay local', async () => {
    const routes = [
      ['GET', CREATE], ['PUT', CREATE], ['PATCH', CREATE], ['DELETE', CREATE],
      ['GET', `${CREATE}/anything`], ['POST', `${CREATE}/anything`],
      ['PATCH', `${CREATE}/anything`], ['DELETE', `${CREATE}/anything`],
      ['POST', '/v1/cachedContents'], ['POST', '/v1alpha/cachedContents'],
      ['POST', '/v1beta/projects/offline/locations/test/cachedContents'],
    ];
    for (const [method, route] of routes) {
      const before = journal.length;
      status(await proxy(method, route, ['GET', 'DELETE'].includes(method) ? undefined : createBody()), 404, `${method} ${route}`);
      noDispatch(before, `${method} ${route}`);
    }
    for (const model of ['models/gemini-4-flash', 'models/gpt-5', 'models/claude-sonnet-4']) {
      const before = journal.length;
      const result = await proxy('POST', CREATE, { ...createBody(), model });
      status(result, 404, `Unsupported create model ${model}`);
      if (!baseline) check(result.text.includes('operation_not_supported'), 'Unsupported model must report operation_not_supported');
      noDispatch(before, `Unsupported create model ${model}`);
    }
    return 'all404; zero provider dispatches';
  });
  await test('invalid and absent auth do not dispatch', async () => {
    // Pre-fix create is unsupported; generation still exercises its actual authentication path.
    for (const route of baseline ? [GENERATE] : [CREATE, GENERATE]) {
      for (const key of [null, 'synthetic-invalid-client-key']) {
        const before = journal.length;
        status(await proxy('POST', route, route === CREATE ? createBody() : generationBody(), key), 401, 'Invalid auth');
        noDispatch(before, 'Invalid auth');
      }
    }
    return '401; zero provider dispatches';
  });
  if (!baseline) await test('create authentication precedes request-body reads', async () => {
    for (const key of [null, 'synthetic-invalid-client-key']) {
      const before = journal.length;
      status(await authBeforeBody(CREATE, key), 401, 'Header-only create auth');
      noDispatch(before, 'Header-only create auth');
    }
    return 'header-only POST declared10MiB, no body sent:401 within4s; zero dispatches';
  });
  await test('unbound generate and wrapped count fail closed', async () => {
    await unavailable('cachedContents/unbound-loopback', 'Unbound resource');
    return 'both503 UNAVAILABLE; zero dispatches';
  });
}
async function baselineCases() {
  await test('baseline exact cachedContents create is unsupported', async () => {
    const before = journal.length;
    status(await proxy('POST', CREATE, createBody()), 404, 'Baseline create');
    noDispatch(before, 'Baseline create');
    return '404; zero provider dispatches';
  });
}
async function newCases(ids) {
  const name = 'cachedContents/loopback-owned-by-A';
  await test('create pass-through establishes A as owner', async () => {
    const before = journal.length;
    const object = createdResponse(name);
    const expectedBytes = prepareResponse(object);
    const result = await proxy('POST', CREATE, createBody());
    status(result, 200, 'Create resource');
    assert.deepEqual(result.bytes, expectedBytes, 'Create response bytes changed');
    assert.deepEqual(result.json, object, 'Create response values changed');
    assert.deepEqual(exactly(before, 'A', 'POST', CREATE, 'Create resource').body, createBody(), 'Create request body changed');
    check(!plan, 'Fake create fixture was not consumed');
    return '200; exact response bytes/values; journal=A';
  });
  await test('client stream flag cannot bypass durable create binding', async () => {
    const streamedName = 'cachedContents/client-stream-flag';
    const body = { ...createBody(), stream: true };
    const before = journal.length;
    prepareResponse(createdResponse(streamedName));
    status(await proxy('POST', CREATE, body), 200, 'Create with client stream flag');
    assert.deepEqual(exactly(before, 'A', 'POST', CREATE, 'Stream-flag create').body, body);
    await ownerReuse(streamedName, 'Stream-flag resource');
    return 'create200; body unchanged; generate/count200 on durable owner';
  });
  await test('another principal cannot reuse a known owner cache', async () => {
    const beforeProvisioning = journal.length;
    const other = await admin('POST', '/admin/v1/principals', {
      name: 'gemini-loopback-isolated-principal-B',
      kind: 'machine',
      allowed_models: ['*'],
      allowed_upstreams: [],
      default_limits: [],
    });
    status(other, 201, 'Create isolated principal B');
    assert.notEqual(other.json.id, ids.principal, 'Isolation fixture reused the original principal');
    const issued = await admin('POST', `/admin/v1/principals/${other.json.id}/keys`, { label: 'loopback-isolation-only' });
    status(issued, 201, 'Issue isolated principal B client key');
    check(typeof issued.json?.plaintext_key === 'string' && issued.json.plaintext_key.length > 0, 'Admin did not return isolated principal client key');
    isolationClientKey = issued.json.plaintext_key; // Memory only; included in every output redaction.
    check(isolationClientKey !== clientKey, 'Isolation fixture reused the original client key');
    await awaitSoleOwner(other.json.id, ids.A);
    noDispatch(beforeProvisioning, 'Isolated principal provisioning');
    await unavailable(name, 'Principal B must not inherit principal A cache ownership', isolationClientKey);
    await ownerReuse(name, 'Original principal A retains its known cache');
    return 'principal B generate/count503 with zero dispatch; original principal A generate/count200 on owner A';
  });
  await setEnabled(ids.B, true);
  await test('generate and count reuse A with B enabled', async () => {
    await ownerReuse(name, 'Owned resource');
    return 'both200; independent journal=A/A, never B';
  });
  await setEnabled(ids.A, false);
  await awaitSoleOwner(ids.principal, ids.B);
  await test('disabled A never fails over its cache to B', async () => {
    await unavailable(name, 'Disabled owner');
    return 'both503; zero B requests';
  });
  await test('duplicate name from B conflicts without retargeting A', async () => {
    const before = journal.length;
    prepareResponse(createdResponse(name));
    const result = await proxy('POST', CREATE, createBody());
    status(result, 503, 'Cross-owner duplicate');
    exactly(before, 'B', 'POST', CREATE, 'Cross-owner duplicate');
    await unavailable(name, 'Duplicate must not retarget disabled A');
    return 'B create503; resource still503 while A disabled; no reuse dispatched';
  });
  await setEnabled(ids.A, true);
  await test('original A binding survives conflicting B response', async () => {
    await ownerReuse(name, 'Preserved original owner');
    return 'generate/count200 on A';
  });
  // Make B the sole creator for negatives, leaving the positive A binding untouched.
  await setEnabled(ids.A, false);
  await awaitSoleOwner(ids.principal, ids.B);
  const malformed = [
    ['missing name', (object) => { delete object.name; }],
    ['empty name', (object) => { object.name = ''; }],
    ['wrong name prefix', (object) => { object.name = 'models/not-a-cache'; }],
    ['nested name', (object) => { object.name = 'cachedContents/nested/invalid'; }],
    ['non-string name', (object) => { object.name = 17; }],
    ['missing expireTime', (object) => { delete object.expireTime; }],
    ['invalid expireTime', (object) => { object.expireTime = 'not-an-rfc3339-time'; }],
    ['expired expireTime', (object) => { object.expireTime = '2000-01-01T00:00:00Z'; }],
    ['non-JSON response', null],
    ['SSE response', null],
  ];
  for (const [index, [label, mutate]] of malformed.entries()) {
    await test(`${label} is not advertised or bound`, async () => {
      const candidate = `cachedContents/rejected-${index}`;
      const object = createdResponse(candidate);
      mutate?.(object);
      const reference = typeof object.name === 'string' && object.name ? object.name : candidate;
      const before = journal.length;
      const responseOptions = label === 'SSE response'
        ? { raw: `data: ${JSON.stringify(object)}\n\n`, contentType: 'text/event-stream' }
        : mutate ? {} : { raw: '<not-json>', contentType: 'text/plain' };
      prepareResponse(object, responseOptions);
      status(await proxy('POST', CREATE, createBody()), 503, label);
      exactly(before, 'B', 'POST', CREATE, label);
      await unavailable(reference, `${label} must not bind`);
      if (reference !== candidate) await unavailable(candidate, `${label} must not bind canonical candidate`);
      return 'create503; generate/count503; journal has only rejected create';
    });
  }
  await test('provider403 does not establish a binding', async () => {
    const rejectedName = 'cachedContents/provider-error-never-bound';
    const before = journal.length;
    // Include plausible metadata even in the error payload to detect accidental status-agnostic binding.
    prepareResponse({ ...createdResponse(rejectedName), error: { code: 403, status: 'PERMISSION_DENIED', message: 'Synthetic denied create' } }, { status: 403 });
    status(await proxy('POST', CREATE, createBody()), 403, 'Provider error');
    exactly(before, 'B', 'POST', CREATE, 'Provider error');
    await unavailable(rejectedName, 'Provider error must not bind');
    return '403 pass-through; generate/count503 without dispatch';
  });
  await test('owner binding persists across server restart', async () => {
    await stopServer();
    await reservePorts();
    await startServer();
    await awaitSoleOwner(ids.principal, ids.B);
    await unavailable(name, 'Restarted disabled owner');
    await setEnabled(ids.A, true);
    await ownerReuse(name, 'Restarted persisted owner');
    return 'same SQLite/key; disabled A503, reenabled A200; no retarget to B';
  });
  await setEnabled(ids.B, false);
  await awaitSoleOwner(ids.principal, ids.A);
  let previousName = name;
  const mutationCases = [
    {
      label: 'Gemini API-key secret owner epoch mutation',
      patch: { api_key_value: providerKeys.C, skip_preflight: true },
      listener: 'fake-provider',
      newName: 'cachedContents/epoch-after-secret',
    },
    {
      label: 'Gemini base URL owner epoch mutation',
      patch: { base_url: secondaryProviderOrigin, skip_preflight: true },
      listener: 'fake-provider-secondary',
      newName: 'cachedContents/epoch-after-base-url',
    },
    {
      // Gemini API-key access is represented by project_id; workspace_id is
      // an Anthropic-only field and is intentionally not sent for this kind.
      label: 'Gemini API-key project access owner epoch mutation',
      patch: { project_id: 'loopback-gemini-project-after-epoch', skip_preflight: true },
      projectId: 'loopback-gemini-project-after-epoch',
      listener: 'fake-provider-secondary',
      newName: 'cachedContents/epoch-after-project-access',
    },
  ];
  for (const mutation of mutationCases) {
    await test(`${mutation.label} invalidates old cache and rebinds new cache`, async () => {
      const beforeMutation = journal.length;
      const updated = await patchUpstream(ids.A, mutation.patch, mutation.label);
      check(!updated.text.includes(providerKeys.C), `${mutation.label}: response exposed the rotated secret`);
      noDispatch(beforeMutation, `${mutation.label} admin update`);
      if (mutation.projectId) {
        assert.equal(
          updated.json?.api_key_access?.project_id,
          mutation.projectId,
          `${mutation.label}: project access snapshot did not change`,
        );
      }
      await awaitSoleOwner(ids.principal, ids.A);
      await unavailable(previousName, `${mutation.label} old cache`);

      prepareResponse(createdResponse(mutation.newName));
      const beforeCreate = journal.length;
      const created = await proxy('POST', CREATE, createBody());
      status(created, 200, `${mutation.label} new cache create`);
      const createEntry = exactly(beforeCreate, 'C', 'POST', CREATE, `${mutation.label} new cache create`);
      assert.equal(createEntry.listener, mutation.listener, `${mutation.label}: new cache used stale provider URL`);
      check(!plan, `${mutation.label}: fake create fixture was not consumed`);
      await ownerReuse(mutation.newName, `${mutation.label} new cache reuse`, 'C', mutation.listener);
      previousName = mutation.newName;
      return 'old cache503 with zero dispatch; new cache created/reused on the new owner epoch';
    });
  }
  await test('in-flight create cannot bind after its owner epoch changes', async () => {
    const staleName = 'cachedContents/epoch-stale-in-flight-create';
    const freshName = 'cachedContents/epoch-after-in-flight-mutation';
    const hold = holdCreateResponse();
    prepareResponse(createdResponse(staleName), { hold });
    const beforeCreate = journal.length;
    let createSettled = false;
    // Observe rejection immediately as data so a failure before finally cannot
    // leave an unhandled fetch rejection or a provider response gate held open.
    const pending = proxy('POST', CREATE, createBody()).then(
      (response) => { createSettled = true; return { response }; },
      (error) => { createSettled = true; return { error }; },
    );
    try {
      await within(hold.reached, 4_000, 'Held create arrival at fake provider');
      const entry = exactly(beforeCreate, 'C', 'POST', CREATE, 'Held create before mutation');
      assert.equal(entry.listener, 'fake-provider-secondary', 'Held create used stale base URL');
      check(!createSettled, 'Create must remain in flight while the provider response is held');
      const beforeMutation = journal.length;
      const projectId = 'loopback-gemini-project-after-in-flight-mutation';
      const updated = await patchUpstream(ids.A, { project_id: projectId, skip_preflight: true }, 'In-flight project access mutation');
      assert.equal(updated.json?.api_key_access?.project_id, projectId, 'In-flight mutation did not commit project access');
      noDispatch(beforeMutation, 'In-flight admin mutation while provider response is held');
      await awaitSoleOwner(ids.principal, ids.A);
      check(!createSettled, 'Create completed before the held provider response was released');
      hold.release();
      const outcome = await pending;
      if (outcome.error) throw outcome.error;
      status(outcome.response, 503, 'Stale create response after owner epoch changed');
      noDispatch(beforeMutation, 'Stale create response must not trigger another provider dispatch');
      await unavailable(staleName, 'Stale in-flight create must not bind');
      await unavailable(previousName, 'In-flight mutation invalidates previous epoch cache');

      prepareResponse(createdResponse(freshName));
      const beforeFresh = journal.length;
      status(await proxy('POST', CREATE, createBody()), 200, 'Fresh create after in-flight mutation');
      const freshEntry = exactly(beforeFresh, 'C', 'POST', CREATE, 'Fresh create after in-flight mutation');
      assert.equal(freshEntry.listener, 'fake-provider-secondary', 'Fresh create used stale base URL');
      check(!plan, 'Fresh create after in-flight mutation did not consume its provider fixture');
      await ownerReuse(freshName, 'Fresh cache after in-flight mutation', 'C', 'fake-provider-secondary');
      return 'admin commit precedes provider response release; stale create503, unbound reuse503/no dispatch; fresh cache200';
    } finally {
      hold.release();
      await pending;
    }
  });
}
async function cleanup() {
  if (cleaning) return cleaning;
  cleaning = (async () => {
    const errors = [];
    for (const state of [...children.values()]) {
      try { await stopServer(state); } catch (error) { errors.push(error); }
    }
    for (const id of [...resources.keys()]) {
      try { await closeListener(id); } catch (error) { errors.push(error); }
    }
    // Only this mkdtemp result is removable, and never while an owned child is still using it.
    if (scratch && children.size === 0 && resources.size === 0) {
      try {
        check(ledger.resources.some((entry) => entry.id === 'scratch' && entry.identifier.path === scratch), 'Scratch ownership ledger mismatch');
        rmSync(scratch, { recursive: true, force: true });
        retired('scratch');
      } catch (error) { errors.push(error); }
    }
    ledger.finished_at = new Date().toISOString();
    saveLedger();
    if (errors.length) throw new AggregateError(errors, 'Owned resource cleanup failed; inspect cleanup ledger');
  })();
  return cleaning;
}
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    console.error(`FAIL interrupted by ${signal}`);
    cleanup().then(() => process.exit(signal === 'SIGINT' ? 130 : 143), (error) => {
      console.error(redact(error.message));
      process.exit(1);
    });
  });
}
async function main() {
  saveLedger();
  console.log(`Cleanup ledger: ${ledgerPath}`);
  try {
    scratch = mkdtempSync(path.join(os.tmpdir(), 'cc-lb-gemini-cache-'));
    record('scratch', 'owned_scratch_directory', { path: scratch, owner_pid: process.pid }, `rm -rf -- ${JSON.stringify(scratch)} (only after owned children exit)`);
    configure();
    await reservePorts();
    await startProvider(PORT.provider, 'fake-provider');
    if (!baseline) {
      const readiness = await fetch(`${secondaryProviderOrigin}/prices.json`, { signal: AbortSignal.timeout(4_000), redirect: 'error' });
      assert.equal(readiness.status, 200, 'localhost provider endpoint must reach the owned IPv4 listener');
      await readiness.arrayBuffer();
    }
    await startServer(baselineBinary ?? binary);
    const ids = await setup();
    if (baselineBinary) {
      await test('populated SQLite upgrade retains principal key and upstreams', async () => {
        const beforeOldGeneration = journal.length;
        status(await proxy('POST', GENERATE, generationBody()), 200, 'Old binary healthy generation');
        exactly(beforeOldGeneration, 'A', 'POST', GENERATE, 'Old binary populated-database control');
        const retained = [];
        for (const owner of ['A', 'B']) {
          const existing = await admin('GET', `/admin/v1/upstreams/${ids[owner]}`);
          status(existing, 200, `Read old binary upstream ${owner}`);
          const { id, name, kind, base_url, enabled } = existing.json;
          retained.push({ id, name, kind, base_url, enabled });
        }
        await stopServer();
        await reservePorts();
        await startServer();
        const beforeRetainedReads = journal.length;
        for (const expected of retained) {
          const existing = await admin('GET', `/admin/v1/upstreams/${expected.id}`);
          status(existing, 200, 'Read retained upstream after upgrade');
          const { id, name, kind, base_url, enabled } = existing.json;
          assert.deepEqual({ id, name, kind, base_url, enabled }, expected, 'Upgrade changed existing upstream identity or configuration');
        }
        await awaitSoleOwner(ids.principal, ids.A);
        noDispatch(beforeRetainedReads, 'Upgrade retained configuration reads');
        const beforeNewGeneration = journal.length;
        status(await proxy('POST', GENERATE, generationBody()), 200, 'New binary retained-principal/key generation');
        exactly(beforeNewGeneration, 'A', 'POST', GENERATE, 'Upgraded binary uses retained principal key and upstream A');
        return 'old binary populated/served SQLite; new binary reopened same database/config/keys and retained both upstreams';
      });
    }
    if (baseline) await baselineCases();
    await commonCases();
    if (!baseline) await newCases(ids);
  } finally {
    await cleanup();
  }
  console.log(`PASS ${baseline ? 'baseline' : 'new'}: ${passed} cases; owned children/listeners/scratch cleaned; no real provider calls`);
}
main().catch((error) => {
  console.error(redact(error.message));
  if (logs.length) console.error(`--- cc-lb log excerpt ---\n${logExcerpt()}`);
  process.exitCode = 1;
});
