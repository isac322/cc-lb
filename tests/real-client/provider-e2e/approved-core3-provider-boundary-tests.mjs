// Source-only preparation. Import and --plan never bind, spawn, read credentials,
// generate certificates, import the relay, or mutate files/environment.
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import http from 'node:http';
import https from 'node:https';
import { Writable } from 'node:stream';
import net from 'node:net';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';

const SELF = fileURLToPath(import.meta.url);
const PREFIX = 'approved-core3-provider-boundary-test-';
const MODEL = 'synthetic-local-model';
const HOLD = 12345;
const ROOT = '/tmp/cc-lb-actual-cli-e2e-20261020';
const NAMES = Object.freeze([
  'http-wire-retry', 'http-refusal', 'http-error', 'http-missing', 'http-capture',
  'http-abort', 'http-cancel', 'http-relay-close', 'http-guard-error',
  'ws-text', 'ws-binary', 'ws-lanes', 'ws-zero', 'ws-missing', 'ws-zero-mixed',
  'ws-zero-generation', 'ws-error', 'ws-cancel', 'ws-close', 'ws-provider-close',
  'ws-capture', 'ws-ambiguous', 'ws-wrong-lane', 'ws-uncreated-id',
  'negative-valid', 'negative-general400', 'negative-retry2', 'negative-fake-handle',
  'negative-proof-kind', 'negative-proof-providerAttempts', 'negative-proof-status',
  'negative-proof-errorStatus', 'negative-proof-providerEvents', 'negative-proof-costKnown',
  'negative-proof-attemptsDelta', 'negative-proof-costDeltaMicros', 'negative-proof-drops',
  'negative-proof-failoverDelta', 'negative-proof-event', 'negative-event-request_id',
  'negative-event-status', 'negative-event-error_code', 'negative-event-upstream_id',
  'negative-event-cost_usd_micros', 'negative-event-input_tokens', 'negative-event-output_tokens',
  'tools-refused-distinct-fields', 'tools-namespace-projection', 'tools-credential-labels',
  'tools-bound-rows', 'tools-bound-depth',
  'tools-input-lite-http', 'tools-input-lite-ws', 'tools-input-root-and-typed',
  'tools-input-refused', 'tools-input-malformed', 'tools-input-ignore-untyped',
  'http-dispatch-request-known', 'http-dispatch-request-secret', 'http-dispatch-request-getter',
  'http-dispatch-end-known', 'http-dispatch-end-secret', 'http-dispatch-end-object',
  'http-ipv4-only-san-success', 'http-ipv6-only-san-success', 'http-ip-dns-only-san-rejected',
  'bind-collision',
]);

// Private, closure-owned diagnostics only; never copy fields from an Error.
const dispatchFailureAudits = new WeakMap();

const blocked = reason => { throw new Error(reason.startsWith('BLOCKED:') ? reason : `BLOCKED:${reason}`); };
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const bytes = value => Buffer.from(JSON.stringify(value));
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const safeError = error => {
  // Never stringify arbitrary errors, OpenSSL output, request headers, or bodies.
  if (error?.code === 'ERR_ASSERTION') return 'ASSERTION_FAILED';
  const message = String(error?.message ?? '');
  return /^BLOCKED:[a-z0-9_:-]+$/i.test(message) ? message : 'BLOCKED:local_fixture_or_runner_failure';
};

/** Pure metadata; the same exact plan must be supplied to --run. */
export function providerBoundaryPlan({ root = ROOT, evidenceDir = root, runId = 'local', basePort = 24100, openssl = '/usr/bin/openssl', runtime = process.execPath } = {}) {
  if (!path.isAbsolute(root) || !path.isAbsolute(evidenceDir) || !path.isAbsolute(openssl) || !path.isAbsolute(runtime)) blocked('absolute_plan_paths_required');
  const rel = path.relative(root, evidenceDir);
  if (rel === '..' || rel.startsWith(`..${path.sep}`) || path.isAbsolute(rel)) blocked('evidence_must_be_within_existing_root');
  if (!/^[a-zA-Z0-9_-]{1,60}$/.test(runId)) blocked('safe_explicit_run_id_required');
  if (!Number.isSafeInteger(basePort) || basePort <= 1024 || basePort + NAMES.length + 4 >= 65536) blocked('explicit_unprivileged_port_range_required');
  const stem = path.join(evidenceDir, `${PREFIX}${runId}`);
  const files = { certificate: `${stem}.crt`, privateKey: `${stem}.key`, ipCertificate: `${stem}.ip.crt`, ipPrivateKey: `${stem}.ip.key`, result: `${stem}.result.json` };
  const relayFile = path.join(path.dirname(SELF), 'approved-core3-provider-boundary.mjs');
  const fixturePort = basePort;
  const collisionPort = basePort + NAMES.length + 1;
  const ipFixturePort = basePort + NAMES.length + 2;
  const ipv6FixturePort = basePort + NAMES.length + 3;
  const identityFixturePort = basePort + NAMES.length + 4;
  const listeners = [
    { role: 'local-tls-fixture', id: `localhost:${fixturePort}/tcp`, port: fixturePort },
    { role: 'ip-only-san-v4-fixture', id: `127.0.0.1:${ipFixturePort}/tcp`, port: ipFixturePort },
    { role: 'ip-only-san-v6-fixture', id: `[::1]:${ipv6FixturePort}/tcp`, port: ipv6FixturePort },
    { role: 'ip-url-dns-only-san-fixture', id: `127.0.0.1:${identityFixturePort}/tcp`, port: identityFixturePort },
    ...NAMES.filter(name => name !== 'bind-collision').map(name => ({ role: name, id: `127.0.0.1:${basePort + 1 + NAMES.indexOf(name)}/tcp`, port: basePort + 1 + NAMES.indexOf(name) })),
    { role: 'bind-collision-existing-owner', id: `127.0.0.1:${collisionPort}/tcp`, port: collisionPort },
    { role: 'bind-collision-attempt', id: `127.0.0.1:${collisionPort}/tcp`, port: collisionPort },
  ];
  // An empty explicit configuration avoids default openssl.cnf/RANDFILE
  // behavior, including unplanned HOME/.rnd writes. No environment override.
  const opensslArgs = ['req', '-config', '/dev/null', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', files.privateKey, '-out', files.certificate, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'];
  const ipOpensslArgs = ['req', '-config', '/dev/null', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', files.ipPrivateKey, '-out', files.ipCertificate, '-days', '1', '-subj', '/CN=synthetic-ip-only', '-addext', 'subjectAltName=IP:127.0.0.1,IP:::1'];
  const args = ['--root', root, '--evidence', evidenceDir, '--id', runId, '--port', String(basePort), '--openssl', openssl, '--runtime', runtime];
  return {
    schema: 'synthetic-local-provider-boundary-unit-v2', root, evidenceDir, runId, basePort,
    openssl, runtime, relayFile, files, fixturePort, ipFixturePort, ipv6FixturePort, identityFixturePort, collisionPort, listeners,
    scenarioCount: NAMES.length,
    fixtureCounts: { tlsListeners: 4, certificatePairs: 2, relaySuccessfulListeners: NAMES.length - 1, collisionExistingListeners: 1, collisionFailedBindAttempts: 1 },
    ledgerRecord: { kind: 'qa-plan', id: `${PREFIX}${runId}:plan` },
    processes: [
      { id: `${PREFIX}${runId}:openssl`, command: [openssl, ...opensslArgs] },
      { id: `${PREFIX}${runId}:openssl-ip`, command: [openssl, ...ipOpensslArgs] },
      { id: `${PREFIX}${runId}:child`, command: [runtime, SELF, '--child', ...args] },
    ],
    parentCommands: { plan: [runtime, SELF, '--plan', ...args], run: [runtime, SELF, '--run', ...args], runRequirement: 'Parent must inject __APPROVED_CORE3_QA_CONTEXT and __APPROVED_CORE3_QA_RECORD_RESOURCE, or call runProviderBoundaryTests({ctx,recordResource,plan}) directly' },
    prerequisites: ['Existing root and evidence directories', 'Bun runtime required by actual relay', 'OpenSSL req supporting -config /dev/null, explicit -subj and -addext without a default configuration', 'Parent-provided global QA recordResource callback', 'Frozen relay tlsFixture:{ca:PEM} localhost-only option and synthetic_fixture_only metadata'],
    resources: 'Exact names/commands above; PIDs recorded immediately after spawn; listener records before bind; no new directories; no automatic file cleanup',
    tls: 'Explicit ca per fixture HTTPS client and relay tlsFixture only; rejectUnauthorized:true and localhost hostname verification; environment and system trust untouched',
    environment: 'Child/OpenSSL receive allowlisted PATH/SystemRoot only; no provider credentials, inherited NODE_OPTIONS, TLS overrides, or stdin',
    guard: 'Explicit UNIT mock; calls only; no financial/catalog/invoice/native PASS claim',
    coverage: NAMES,
    actualVendorCalls: 0,
    nativePrewarmModelsFeatures: 'UNCHANGED',
    additionalToolsScope: 'SYNTHETIC_LITERAL_SHAPE_ONLY; no model, price, prewarm-policy, hosted-fee eligibility, or native coverage expansion',
    fileMode: '0600 from initial creation; private key/cert/result retained in global ledger',
  };
}

async function existingDirectory(value) {
  const info = await fs.stat(value).catch(() => null);
  if (!info?.isDirectory()) blocked('existing_root_and_evidence_directories_required');
}

function environment() {
  // Do not inherit process.env. This is a fresh per-process allowlist, not an
  // environment mutation; no TLS trust-related environment variables are used.
  return Object.fromEntries(['PATH', 'SystemRoot', 'SYSTEMROOT'].filter(key => typeof process.env[key] === 'string').map(key => [key, process.env[key]]));
}

function guardMock({ refuse = false, throwSettle = false } = {}) {
  const calls = { reserve: [], settle: [], settleNonGeneration: [], seal: [] };
  let sealed = false;
  return {
    scope: 'UNIT_MOCK_ONLY', calls,
    seal() { sealed = true; calls.seal.push({ sealed: true }); return { ok: true, sealed: true, seal_persisted: true }; },
    reserve(input) { calls.reserve.push(structuredClone(input)); return { ok: !refuse && !sealed, bound_micros: HOLD }; },
    settle(input) { calls.settle.push(structuredClone(input)); if (throwSettle && input.usage !== null) throw new Error('unit-settlement-error'); return { ok: true }; },
    settleNonGeneration(input) { calls.settleNonGeneration.push(structuredClone(input)); return { ok: true }; },
  };
}

async function eventually(predicate, name) {
  const deadline = Date.now() + 8000;
  while (!predicate()) { if (Date.now() > deadline) blocked(`timeout_${name}`); await delay(5); }
}

function frame(payload, { opcode = 1, fin = true, masked = false } = {}) {
  const data = Buffer.from(payload);
  let header;
  if (data.length < 126) header = Buffer.from([(fin ? 128 : 0) | opcode, (masked ? 128 : 0) | data.length]);
  else if (data.length <= 65535) { header = Buffer.alloc(4); header[0] = (fin ? 128 : 0) | opcode; header[1] = (masked ? 128 : 0) | 126; header.writeUInt16BE(data.length, 2); }
  else { header = Buffer.alloc(10); header[0] = (fin ? 128 : 0) | opcode; header[1] = (masked ? 128 : 0) | 127; header.writeBigUInt64BE(BigInt(data.length), 2); }
  if (!masked) return Buffer.concat([header, data]);
  const mask = Buffer.from([17, 34, 51, 68]);
  for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
  return Buffer.concat([header, mask, data]);
}

function frameReader(onFrame, onMessage) {
  let buffered = Buffer.alloc(0), parts = [];
  return chunk => {
    buffered = Buffer.concat([buffered, chunk]);
    while (buffered.length >= 2) {
      let size = buffered[1] & 127, offset = 2;
      if (size === 126) { if (buffered.length < 4) return; size = buffered.readUInt16BE(2); offset = 4; }
      if (size === 127) { if (buffered.length < 10) return; const wide = buffered.readBigUInt64BE(2); assert.ok(wide <= 32n * 1024n * 1024n); size = Number(wide); offset = 10; }
      const maskOffset = offset, masked = !!(buffered[1] & 128);
      if (masked) offset += 4;
      if (buffered.length < offset + size) return;
      const wire = Buffer.from(buffered.subarray(0, offset + size));
      const payload = Buffer.from(buffered.subarray(offset, offset + size));
      const opcode = buffered[0] & 15, fin = !!(buffered[0] & 128);
      if (masked) for (let i = 0; i < payload.length; i++) payload[i] ^= buffered[maskOffset + i % 4];
      buffered = buffered.subarray(offset + size);
      onFrame(wire, opcode);
      if (opcode >= 8) continue;
      parts.push(payload);
      if (fin) { onMessage(Buffer.concat(parts)); parts = []; }
    }
  };
}

async function bind(server, host, port) {
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen({ host, port, exclusive: true }, () => { server.removeListener('error', reject); resolve(); }); });
}

async function localFixture(plan, record, { host = 'localhost', role = 'local-tls-fixture' } = {}) {
  const ca = await fs.readFile(plan.files.certificate);
  const key = await fs.readFile(plan.files.privateKey);
  const sse = Buffer.from('event: response.created\r\ndata: {"type":"response.created","response":{"id":"http-created","status":"in_progress"}}\r\n\r\nevent: response.completed\r\ndata: {"type":"response.completed","response":{"id":"http-created","status":"completed","service_tier":"unit-lane","usage":{"input_tokens":3,"output_tokens":2,"input_tokens_details":{"cached_tokens":1}}}}\r\n\r\ndata: [DONE]\r\n\r\n');
  const gzip = gzipSync(sse);
  const requests = [], sessions = new Map(), sockets = new Set();
  const server = https.createServer({ ca, cert: ca, key, minVersion: 'TLSv1.2' }, (req, res) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('error', () => {});
    req.once('end', () => {
      const which = req.headers['x-fixture-case'];
      requests.push({ url: req.url, method: req.method, headers: req.headers, rawHeaders: [...req.rawHeaders], body: Buffer.concat(chunks), which });
      if (which === 'retry' && requests.filter(item => item.which === 'retry').length === 1) { res.writeHead(503); res.end('temporary-local'); return; }
      if (which === 'negative') { res.writeHead(400, { 'content-type': 'application/json' }); res.end('{"error":{"status":"INVALID_ARGUMENT"}}'); return; }
      if (which === 'error') { res.writeHead(500); res.end('local-error'); return; }
      if (which === 'hold') { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write('data: partial\n\n'); return; }
      if (which === 'abort') { res.writeHead(200, { 'content-length': '99999' }); res.write('partial'); setImmediate(() => res.destroy()); return; }
      if (which === 'missing') { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{"id":"missing","object":"response","status":"completed"}'); return; }
      if (which === 'capture') { res.writeHead(200, { 'content-type': 'application/json' }); res.end(bytes({ id: 'large', object: 'response', status: 'completed', padding: 'x'.repeat(2 * 1024 * 1024), usage: { input_tokens: 1, output_tokens: 1 } })); return; }
      res.writeHead(200, { 'content-type': 'text/event-stream', 'content-encoding': 'gzip', 'content-length': gzip.length });
      res.write(gzip.subarray(0, 11)); res.end(gzip.subarray(11));
    });
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('error', () => {}); socket.once('close', () => sockets.delete(socket)); });
  server.on('tlsClientError', () => {});
  server.on('upgrade', (req, socket, head) => {
    const name = new URL(req.url, 'https://localhost').searchParams.get('case');
    const accept = createHash('sha1').update(String(req.headers['sec-websocket-key']) + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
    const session = { frames: [], messages: [], sent: [], socket, send(wire) { this.sent.push(Buffer.from(wire)); socket.write(wire); } };
    sessions.set(name, session);
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    const read = frameReader((wire, opcode) => { session.frames.push(wire); if (opcode === 8) socket.end(frame(Buffer.from([3, 232]), { opcode: 8 })); }, message => session.messages.push(message));
    socket.on('data', read);
    if (head.length) read(head);
  });
  const listenerHost = host === '::1' ? '[::1]' : host;
  await record({ kind: 'listener', id: `${listenerHost}:${plan.fixturePort}/tcp`, role, pid: process.pid, delete: 'Normal fixture lifecycle: server.close and owned sockets.destroy; retained ledger' });
  try { await bind(server, host, plan.fixturePort); }
  catch { blocked('exact_fixture_port_unavailable_existing_owner_untouched'); }
  return { ca, requests, sessions, gzip, async close() { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); } };
}

function httpCall(port, rawPath, body, which, { cancel = false, headers = {} } = {}) {
  let request;
  const promise = new Promise((resolve, reject) => {
    request = http.request({ hostname: '127.0.0.1', port, method: 'POST', path: rawPath, headers: { ...headers, 'content-type': 'application/json', 'content-length': body.length, 'x-fixture-case': which } }, response => {
      const chunks = [];
      response.on('data', chunk => { chunks.push(chunk); if (cancel) { response.destroy(); request.destroy(); } });
      response.once('end', () => resolve({ status: response.statusCode, body: Buffer.concat(chunks) }));
      response.once('error', reject);
      response.once('aborted', () => reject(new Error('expected-local-abort')));
    });
    request.setTimeout(8000, () => request.destroy(new Error('local-timeout')));
    request.once('error', reject);
    request.end(body);
  });
  return { promise, abort: () => request.destroy() };
}

async function wsConnect(port, name) {
  const socket = net.createConnection({ host: '127.0.0.1', port });
  const inbound = [];
  let upgrade = Buffer.alloc(0), upgraded = false, closed = false, failure = false;
  socket.on('error', () => { failure = true; });
  socket.once('close', () => { closed = true; });
  socket.on('data', chunk => {
    if (upgraded) { inbound.push(Buffer.from(chunk)); return; }
    upgrade = Buffer.concat([upgrade, chunk]);
    const end = upgrade.indexOf('\r\n\r\n');
    if (end >= 0) { assert.match(upgrade.subarray(0, end).toString(), /^HTTP\/1\.1 101 /); upgraded = true; if (end + 4 < upgrade.length) inbound.push(Buffer.from(upgrade.subarray(end + 4))); }
  });
  await eventually(() => socket.readyState === 'open' || failure, 'ws_connect');
  if (failure) blocked('local_ws_connect_failed');
  socket.write(`GET /v1/responses?case=${name} HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: c3ludGhldGljbG9jYWwxMg==\r\n\r\n`);
  await eventually(() => upgraded || closed || failure, 'ws_upgrade');
  if (!upgraded) blocked('strict_tls_ws_fixture_upgrade_failed');
  return { socket, inbound, isClosed: () => closed, close() { socket.destroy(); } };
}

function fullHold(relay, guard, expected = guard.calls.reserve.length) {
  const metadata = relay.metadata();
  assert.equal(metadata.settled.length, 0);
  assert.equal(metadata.outstanding.length, expected);
  for (const item of metadata.outstanding) { assert.equal(item.state, 'UNKNOWN'); assert.equal(item.hold_policy, 'FULL_HOLD'); assert.equal(item.bound_micros, HOLD); }
  assert.equal(guard.calls.settleNonGeneration.length, 0);
  for (const call of guard.calls.settle) assert.equal(call.usage, null);
  assert.equal(new Set(guard.calls.settle.map(item => item.id)).size, expected);
}

// Expectations deliberately describe complete public metadata shapes. These
// constructors do not call or reimplement the relay's traversal/filter helpers.
function expectedToolEntry(location, depth, type, name, { sourcePath = '$.tools', sourceKind = 'ROOT_TOOLS', typePresent = type !== null, namePresent = name !== null, typeOmitted = false, nameOmitted = false, nestedPresent = false, nestedArray = false } = {}) {
  return {
    path: location, depth, source_path: sourcePath, source_kind: sourceKind, object: true, type, name,
    type_present: typePresent, name_present: namePresent,
    type_value_omitted: typeOmitted, name_value_omitted: nameOmitted,
    nested_tools_present: nestedPresent, nested_tools_array: nestedArray,
  };
}

function liteAdditionalToolsInput() {
  // Literal type/name shapes from the approved metadata-only shape summary.
  // No native source, schema, credential, model choice, or pricing is copied.
  return { role: 'developer', type: 'additional_tools', tools: [
    { type: 'namespace', name: 'functions', tools: [
      { type: 'custom', name: 'exec' }, { type: 'function', name: 'wait' },
      { type: 'function', name: 'request_user_input' }, { type: 'function', name: 'request_user_input_async' },
    ] },
    { type: 'namespace', name: 'clock', tools: [{ type: 'function', name: 'sleep' }] },
    { type: 'namespace', name: 'collaboration', tools: [
      { type: 'function', name: 'followup_task' }, { type: 'function', name: 'interrupt_agent' },
      { type: 'function', name: 'list_agents' }, { type: 'function', name: 'send_message' },
      { type: 'function', name: 'spawn_agent' }, { type: 'function', name: 'wait_agent' },
    ] },
  ] };
}

function expectedLiteAdditionalToolEntries(containerPath) {
  const rows = [
    ['[0]', 0, 'namespace', 'functions', true],
    ['[0].tools[0]', 1, 'custom', 'exec', false],
    ['[0].tools[1]', 1, 'function', 'wait', false],
    ['[0].tools[2]', 1, 'function', 'request_user_input', false],
    ['[0].tools[3]', 1, 'function', 'request_user_input_async', false],
    ['[1]', 0, 'namespace', 'clock', true],
    ['[1].tools[0]', 1, 'function', 'sleep', false],
    ['[2]', 0, 'namespace', 'collaboration', true],
    ['[2].tools[0]', 1, 'function', 'followup_task', false],
    ['[2].tools[1]', 1, 'function', 'interrupt_agent', false],
    ['[2].tools[2]', 1, 'function', 'list_agents', false],
    ['[2].tools[3]', 1, 'function', 'send_message', false],
    ['[2].tools[4]', 1, 'function', 'spawn_agent', false],
    ['[2].tools[5]', 1, 'function', 'wait_agent', false],
  ];
  return rows.map(([suffix, depth, type, name, nested]) => expectedToolEntry(`${containerPath}${suffix}`, depth, type, name, { sourcePath: containerPath, sourceKind: 'TYPED_ADDITIONAL_TOOLS', nestedPresent: nested, nestedArray: nested }));
}

function expectedRootToolContainer(shape, toolsCount = null) {
  return { source_path: '$.tools', source_kind: 'ROOT_TOOLS', shape, tools_count: toolsCount };
}

function expectedTypedToolContainer(index, { shape = 'ARRAY', toolsCount = 3, rolePresent = true, role = 'developer', roleMatches = true } = {}) {
  return {
    source_path: `$.input[${index}].tools`, source_kind: 'TYPED_ADDITIONAL_TOOLS', shape, tools_count: toolsCount,
    item_path: `$.input[${index}]`, item_type: 'additional_tools',
    role_present: rolePresent, role, role_matches_source_contract: roleMatches,
  };
}

function assertToolObservation(relay, guard, entries, {
  admitted = true, truncated = false, rootPresent = true, rootArray = true,
  rootShape = 'ARRAY', containers, unknown = false, unknownShapes = [],
  inputArrayPresent = true, additionalItemsObserved = 0,
} = {}) {
  assert.equal(guard.calls.reserve.length, 1);
  const metadata = relay.metadata();
  const observations = metadata.receipts.filter(item => item.kind === 'generation_admission_tools_observed');
  const outcomes = metadata.receipts.filter(item => item.kind === 'generation_admission');
  assert.equal(observations.length, 1);
  assert.equal(outcomes.length, 1);
  const admissionId = hash(guard.calls.reserve[0].id);
  const common = { synthetic_fixture_only: true, source_scope: 'synthetic-provider-boundary-fixture' };
  const expectedContainers = containers ?? [expectedRootToolContainer(rootShape, rootArray ? guard.calls.reserve[0].body.tools.length : null)];
  assert.deepEqual(observations[0], {
    ...common, kind: 'generation_admission_tools_observed',
    admission_id_sha256: admissionId, reservation_id_sha256: null,
    provider_forward_attempted: false,
    observation_scope: 'DECODED_NATIVE_BODY_BEFORE_BUDGET_ADMISSION',
    observed_tools: {
      container_path: '$.tools', present: rootPresent, array: rootArray, entries, truncated,
      containers: expectedContainers, unknown, unknown_shapes: unknownShapes,
      additional_tools_input_array_present: inputArrayPresent,
      additional_tools_items_observed: additionalItemsObserved,
      interpretation: 'OBSERVED_FIELDS_ONLY_NOT_HOSTED_TOOL_CLASSIFICATION',
      label_policy: 'BOUNDED_IDENTIFIER_ONLY_CREDENTIAL_SHAPED_VALUES_OMITTED',
      pricing_authority: 'SHARED_GUARD_ACTUAL_UNMODIFIED_BODY',
    },
  });
  assert.deepEqual(outcomes[0], {
    ...common, kind: 'generation_admission', decision: admitted ? 'DURABLY_RESERVED' : 'BLOCKED',
    admission_id_sha256: admissionId, reservation_id_sha256: admitted ? admissionId : null,
    reason: admitted ? null : 'durable_reservation_refused',
  });
  assert.ok(metadata.receipts.indexOf(observations[0]) < metadata.receipts.indexOf(outcomes[0]));
  assert.equal(metadata.counters.actualgeneration_writeattempts, admitted ? 1 : 0);
  return metadata;
}

function assertNoToolCanaries(metadata, markers) {
  // In addition to exact projection-object equality, scan EVERY metadata key
  // and string leaf for raw markers and common independent marker digests.
  // Whole request-body hashing is expected; per-label hashes are not allowed.
  const forbidden = new Set(markers.flatMap(marker => [
    marker,
    ...['sha256', 'sha1'].flatMap(algorithm => [
      createHash(algorithm).update(marker).digest('hex'),
      createHash(algorithm).update(marker).digest('base64'),
      createHash(algorithm).update(marker).digest('base64url'),
      createHash(algorithm).update(JSON.stringify(marker)).digest('hex'),
    ]),
  ]));
  const matches = [];
  function visit(value, location) {
    if (typeof value === 'string') {
      if ([...forbidden].some(marker => value.includes(marker))) matches.push(location);
    } else if (value !== null && typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) {
        if ([...forbidden].some(marker => key.includes(marker))) matches.push(`${location}.[key]`);
        visit(child, `${location}.${key}`);
      }
    }
  }
  visit(metadata, '$');
  // Report paths only, never the dummy auth values or arbitrary metadata.
  assert.deepEqual(matches, [], 'metadata must contain neither canaries nor independent canary hashes');
}

async function testSuite(plan, record) {
  if (typeof Bun === 'undefined') blocked('bun_runtime_required_by_actual_relay');
  const fixture = await localFixture(plan, record);
  let imported;
  try {
    imported = await import(pathToFileURL(plan.relayFile).href);
    if (typeof imported.createProviderBoundaryRelay !== 'function') blocked('actual_relay_factory_required');
  } catch {
    await fixture.close();
    blocked('frozen_actual_relay_import_or_factory_prerequisite_missing');
  }
  const factory = imported.createProviderBoundaryRelay;
  const results = [];
  const target = `https://localhost:${plan.fixturePort}/v1/responses`;
  const ctx = { root: plan.root, evidenceDir: plan.evidenceDir, upstreamId: 'synthetic-unit-upstream' };
  const relayRecord = (_ctx, resource) => record(resource);
  async function withRelay(name, callback, { protocol = 'openai-responses', guard = guardMock(), targetBaseUrl = target, boundaryFixture = fixture, expectedHost = 'localhost' } = {}) {
    const listener = plan.listeners.find(item => item.role === name);
    assert.ok(listener);
    const relay = await factory({ ctx, port: listener.port, targetBaseUrl, protocol, models: [MODEL], recordResource: relayRecord, getGuard: () => guard, tlsFixture: { ca: boundaryFixture.ca.toString('utf8') } });
    try {
      const meta = relay.metadata();
      if (meta.synthetic_fixture_only !== true) blocked('relay_explicit_ca_synthetic_fixture_contract_required');
      assert.equal(meta.source_original_url.host, expectedHost);
      assert.equal(meta.tls_trust_scope, 'EXPLICIT_LOCAL_FIXTURE_CA');
      await callback(relay, guard);
      results.push({ name, status: 'PASS_SYNTHETIC_LOCAL_UNIT', actualVendorCalls: 0 });
    } catch (error) {
      const wrapped = new Error(`BLOCKED:case_${name.replaceAll('-', '_')}:${safeError(error).replace(/^BLOCKED:/, '')}`);
      const audit = dispatchFailureAudits.get(error);
      if (audit) dispatchFailureAudits.set(wrapped, audit);
      throw wrapped;
    } finally { await relay.close(); }
  }
  const requestBody = Buffer.from(' {"model":"synthetic-local-model", "input":["\\u0041",{"raw":"%2F+"}]} \n');
  try {
    // Explicit client CA, certificate validation and hostname verification. No
    // trust environment variable or global HTTPS agent is changed.
    await new Promise((resolve, reject) => {
      const req = https.request({ hostname: 'localhost', port: plan.fixturePort, servername: 'localhost', rejectUnauthorized: true, ca: fixture.ca, method: 'GET', path: '/fixture-readiness', agent: false }, res => { res.resume(); res.once('end', resolve); });
      req.once('error', () => reject(new Error('BLOCKED:local_ca_or_hostname_verification_failed'))); req.end();
    });
    await withRelay('http-wire-retry', async (relay, guard) => {
      const rawPath = '/literal/%2e%2e/prefix/responses?dup=a&dup=b&slash=%2F&plus=+&empty=';
      const start = fixture.requests.length;
      const first = await httpCall(relay.port, rawPath, requestBody, 'retry').promise;
      assert.equal(first.status, 503);
      const second = await httpCall(relay.port, rawPath, requestBody, 'retry').promise;
      assert.equal(second.status, 200); assert.deepEqual(second.body, fixture.gzip);
      const forwarded = fixture.requests.slice(start);
      assert.equal(forwarded.length, 2);
      for (const item of forwarded) { assert.equal(item.url, rawPath); assert.equal(item.method, 'POST'); assert.deepEqual(item.body, requestBody); }
      assert.equal(guard.calls.reserve.length, 2); assert.notEqual(guard.calls.reserve[0].id, guard.calls.reserve[1].id);
      assert.deepEqual(guard.calls.reserve[0].body, guard.calls.reserve[1].body);
      const unknown = guard.calls.settle.filter(item => item.usage === null);
      const settled = guard.calls.settle.filter(item => item.usage !== null);
      assert.equal(unknown.length, 1); assert.equal(unknown[0].providerStatus, 503);
      assert.equal(settled.length, 1); assert.deepEqual(settled[0].usage, { input_tokens: 3, output_tokens: 2, input_tokens_details: { cached_tokens: 1 } });
      assert.equal(settled[0].serviceTier, 'unit-lane');
      const meta = relay.metadata();
      assert.equal(meta.attempts[1].provider_response_id_sha256, hash('http-created'));
      assert.ok(meta.tls_receipts.every(item => item.certificate_authorized === true && item.hostname_verified === true));
    });
    await withRelay('http-refusal', async (relay, guard) => {
      const before = fixture.requests.length;
      assert.equal((await httpCall(relay.port, '/v1/responses', requestBody, 'success').promise).status, 503);
      assert.equal(guard.calls.reserve.length, 1); assert.equal(guard.calls.settle.length, 0);
      assert.equal(fixture.requests.length, before); assert.equal(relay.metadata().counters.actual_boundary_write_attempts, 0);
    }, { guard: guardMock({ refuse: true }) });

    // The child alone replaces the request boundary for these exact fixture
    // options. The closed adapter exercises the actual relay catch, not TLS or
    // provider behavior; the nonfault wire/TLS cases above remain unmodified.
    const dispatchFaults = [
      { name: 'http-dispatch-request-known', phase: 'HTTPS_REQUEST', shape: 'known', code: 'ERR_INVALID_ARG_VALUE' },
      { name: 'http-dispatch-request-secret', phase: 'HTTPS_REQUEST', shape: 'secret', code: 'UNKNOWN' },
      { name: 'http-dispatch-request-getter', phase: 'HTTPS_REQUEST', shape: 'getter', code: 'UNKNOWN' },
      { name: 'http-dispatch-end-known', phase: 'UPSTREAM_END', shape: 'known', code: 'ERR_STREAM_DESTROYED' },
      { name: 'http-dispatch-end-secret', phase: 'UPSTREAM_END', shape: 'secret', code: 'UNKNOWN' },
      { name: 'http-dispatch-end-object', phase: 'UPSTREAM_END', shape: 'object', code: 'UNKNOWN' },
    ];
    for (const fault of dispatchFaults) {
      await withRelay(fault.name, async (relay, guard) => {
        const sentinel = `UNIT_DISPATCH_SECRET_${fault.name.replaceAll('-', '_')}`;
        const injected = new Error(sentinel);
        injected.stack = sentinel;
        const unsafeCode = { toString() { throw new Error(sentinel); }, [Symbol.toPrimitive]() { throw new Error(sentinel); } };
        if (fault.shape === 'getter') Object.defineProperty(injected, 'code', { get() { throw new Error(sentinel); } });
        else injected.code = fault.shape === 'known' ? fault.code : fault.shape === 'object' ? unsafeCode : sentinel;
        const rawPath = `/v1/responses?dispatch-unit=${fault.name}`;
        const before = fixture.requests.length;
        const originalRequest = https.request;
        let intercepted = 0, ended = 0, destroyed = 0;
        let realDestroyImplCalls = 0, closeEvents = 0, firstCloseTransitions = 0, firstCloseDestroyCalls = null;
        let bodyMatches = null, finallyObserved = false, checkpoint = 'SETUP', beforeAssert = null;
        let outcome;
        const boundedInteger = value => Number.isSafeInteger(value) && value >= 0 && value <= 65535 ? value : null;
        const allowedPhases = new Set(['READ_BODY', 'ADMISSION', 'REQUEST_OPTIONS', 'HTTPS_REQUEST', 'REQUEST_HANDLERS', 'UPSTREAM_TRAILERS', 'UPSTREAM_END']);
        const allowedCodes = new Set([
          'ERR_INVALID_ARG_TYPE', 'ERR_INVALID_ARG_VALUE', 'ERR_INVALID_HTTP_TOKEN', 'ERR_HTTP_INVALID_HEADER_VALUE',
          'ERR_INVALID_PROTOCOL', 'ERR_INVALID_URL', 'ERR_HTTP_HEADERS_SENT', 'ERR_STREAM_WRITE_AFTER_END',
          'ERR_STREAM_DESTROYED', 'ERR_TLS_CERT_ALTNAME_INVALID', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT',
          'EPIPE', 'ENOTFOUND', 'UNKNOWN',
        ]);
        const snapshot = () => {
          const metadata = relay.metadata();
          const diagnostic = metadata.receipts.filter(item => item.kind === 'http_dispatch_failure');
          const first = diagnostic[0];
          return {
            intercepted_calls: boundedInteger(intercepted), end_calls: boundedInteger(ended),
            destroy_calls: boundedInteger(destroyed), close_events: boundedInteger(closeEvents),
            destroyed_state: closedRequest.destroyed,
            real_destroy_impl_calls: boundedInteger(realDestroyImplCalls),
            first_close_transitions: boundedInteger(firstCloseTransitions),
            first_close_destroy_calls: boundedInteger(firstCloseDestroyCalls),
            body_matches: bodyMatches, status_is_503: outcome ? outcome.status === 503 : null,
            generic_body_matches: outcome ? outcome.body.equals(bytes({ error: { type: 'provider_boundary_blocked', message: 'BLOCKED:provider_boundary_blocked' } })) : null,
            fixture_forward_delta: boundedInteger(fixture.requests.length - before),
            reserve_count: boundedInteger(guard.calls.reserve.length), settle_count: boundedInteger(guard.calls.settle.length),
            settle_null_count: boundedInteger(guard.calls.settle.filter(item => item.usage === null).length),
            settle_exact_once: guard.calls.settle.length === 1 && guard.calls.settle[0].usage === null,
            unknown_full_hold: metadata.settled.length === 0 && metadata.outstanding.length === 1 &&
              metadata.outstanding[0].state === 'UNKNOWN' && metadata.outstanding[0].hold_policy === 'FULL_HOLD' &&
              metadata.outstanding[0].bound_micros === HOLD,
            diagnostic_count: boundedInteger(diagnostic.length),
            phase: first && allowedPhases.has(first.phase) ? first.phase : null,
            error_code: first && allowedCodes.has(first.error_code) ? first.error_code : null,
            boundary_write_attempts: boundedInteger(metadata.counters.actual_boundary_write_attempts),
            generation_write_attempts: boundedInteger(metadata.counters.actualgeneration_writeattempts),
            provider_response_receipts: boundedInteger(metadata.counters.provider_response_receipts),
            attempt_count: boundedInteger(metadata.attempts.length), tls_receipt_count: boundedInteger(metadata.tls_receipts.length),
            canary_absent: !JSON.stringify({ metadata, response: outcome ? outcome.body.toString('utf8') : null, results }).includes(sentinel),
          };
        };
        const beforeAssertion = value => { checkpoint = value; beforeAssert = snapshot(); };
        // Use Node's actual destroy lifecycle; only the closed end boundary
        // injects the synchronous fault. Never simulate destruction/close.
        const closedRequest = new Writable({
          emitClose: true,
          destroy(error, callback) { realDestroyImplCalls++; callback(error); },
        });
        const closeObserved = new Promise(resolve => closedRequest.once('close', resolve));
        closedRequest.on('close', () => {
          closeEvents++;
          if (firstCloseTransitions === 0) { firstCloseTransitions++; firstCloseDestroyCalls = destroyed; }
        });
        closedRequest.end = body => {
          ended++; bodyMatches = Buffer.isBuffer(body) && body.equals(requestBody);
          beforeAssertion('END_BODY_MATCH');
          assert.deepEqual(body, requestBody);
          throw injected;
        };
        closedRequest.destroy = function (...args) {
          destroyed++;
          return Reflect.apply(Writable.prototype.destroy, this, args);
        };
        try { // Preserve fault assertions; observe standard Writable destruction.
        try {
          https.request = function (...args) {
            const options = args[0];
            const exactFixture = options?.protocol === 'https:' && options.hostname === 'localhost' &&
              String(options.port) === String(plan.fixturePort) && options.servername === 'localhost' &&
              options.method === 'POST' && options.path === rawPath && options.rejectUnauthorized === true &&
              options.ca === fixture.ca.toString('utf8') && Array.isArray(options.headers) &&
              options.headers.some((value, index) => index % 2 === 0 && value.toLowerCase() === 'x-fixture-case' && options.headers[index + 1] === fault.name);
            if (!exactFixture) return Reflect.apply(originalRequest, this, args);
            intercepted++;
            if (fault.phase === 'HTTPS_REQUEST') throw injected;
            return closedRequest;
          };
          outcome = await httpCall(relay.port, rawPath, requestBody, fault.name).promise;
        } finally { https.request = originalRequest; }
        beforeAssertion('REQUEST_RESTORED');
        assert.equal(https.request, originalRequest);
        beforeAssertion('STATUS_503');
        assert.equal(outcome.status, 503);
        beforeAssertion('GENERIC_RESPONSE_BODY');
        assert.deepEqual(outcome.body, bytes({ error: { type: 'provider_boundary_blocked', message: 'BLOCKED:provider_boundary_blocked' } }));
        beforeAssertion('INTERCEPTED_CALLS');
        assert.equal(intercepted, 1);
        beforeAssertion('END_CALLS');
        assert.equal(ended, fault.phase === 'UPSTREAM_END' ? 1 : 0);
        if (fault.phase === 'UPSTREAM_END') {
          beforeAssertion('STANDARD_CLOSE_EVENT');
          await closeObserved;
        }
        beforeAssertion('DESTROYED_STATE');
        assert.equal(closedRequest.destroyed, fault.phase === 'UPSTREAM_END');
        beforeAssertion('DESTROY_IMPL_EFFECT');
        assert.equal(realDestroyImplCalls, fault.phase === 'UPSTREAM_END' ? 1 : 0);
        beforeAssertion('CLOSE_EVENTS');
        assert.equal(closeEvents, fault.phase === 'UPSTREAM_END' ? 1 : 0);
        beforeAssertion('FIRST_CLOSE_TRANSITIONS');
        assert.equal(firstCloseTransitions, fault.phase === 'UPSTREAM_END' ? 1 : 0);
        beforeAssertion('FIXTURE_FORWARD_DELTA');
        assert.equal(fixture.requests.length, before);
        beforeAssertion('RESERVE_COUNT');
        assert.equal(guard.calls.reserve.length, 1);
        beforeAssertion('SETTLE_COUNT');
        assert.equal(guard.calls.settle.length, 1);
        beforeAssertion('UNKNOWN_FULL_HOLD');
        fullHold(relay, guard, 1);
        const metadata = relay.metadata();
        const reservationId = hash(guard.calls.reserve[0].id);
        const diagnostic = metadata.receipts.filter(item => item.kind === 'http_dispatch_failure');
        beforeAssertion('SAFE_DIAGNOSTIC');
        assert.deepEqual(diagnostic, [{
          synthetic_fixture_only: true, source_scope: 'synthetic-provider-boundary-fixture',
          kind: 'http_dispatch_failure', phase: fault.phase, error_code: fault.code,
          reservation_id_sha256: reservationId,
        }]);
        const blockedReceipt = metadata.receipts.find(item => item.kind === 'http' && item.decision === 'BLOCKED');
        beforeAssertion('BLOCKED_RECEIPT');
        assert.ok(blockedReceipt);
        beforeAssertion('DIAGNOSTIC_ORDER');
        assert.ok(metadata.receipts.indexOf(diagnostic[0]) < metadata.receipts.indexOf(blockedReceipt));
        beforeAssertion('BOUNDARY_WRITE_ATTEMPTS');
        assert.equal(metadata.counters.actual_boundary_write_attempts, 1);
        beforeAssertion('GENERATION_WRITE_ATTEMPTS');
        assert.equal(metadata.counters.actualgeneration_writeattempts, 1);
        beforeAssertion('PROVIDER_RESPONSE_RECEIPTS');
        assert.equal(metadata.counters.provider_response_receipts, 0);
        beforeAssertion('ATTEMPT_COUNT');
        assert.equal(metadata.attempts.length, 1);
        beforeAssertion('ATTEMPT_RESERVATION_JOIN');
        assert.equal(metadata.attempts[0].reservation_id_sha256, reservationId);
        beforeAssertion('TLS_RECEIPT_COUNT');
        assert.equal(metadata.tls_receipts.length, fault.phase === 'UPSTREAM_END' ? 1 : 0);
        beforeAssertion('TLS_NOT_CONNECTED');
        assert.ok(metadata.tls_receipts.every(item => item.secure_connect_observed === false));
        beforeAssertion('OUTSTANDING_RESERVATION_JOIN');
        assert.equal(metadata.outstanding[0].reservation_id_sha256, reservationId);
        beforeAssertion('UNKNOWN_REASON');
        assert.equal(metadata.outstanding[0].reason, 'http_forward_failed_or_interrupted_full_hold');
        beforeAssertion('CAPTURE_RELEASED');
        assert.equal(metadata.capture_bytes_held, 0);
        beforeAssertion('CANARY_ABSENT');
        assertNoToolCanaries({ metadata, outcome, results }, [sentinel]);
        } catch (error) {
          if (error !== null && (typeof error === 'object' || typeof error === 'function')) {
            dispatchFailureAudits.set(error, () => ({
              schema: 'synthetic-dispatch-failure-audit-v1', scope: 'UNIT_ONLY_CLOSED_REQUEST_ADAPTER',
              case_name: fault.name, checkpoint, finally_observed: finallyObserved,
              before_assert: beforeAssert, after_finally: snapshot(),
            }));
          }
          throw error;
        } finally { finallyObserved = true; }
      });
    }
    for (const [name, which] of [['http-error', 'error'], ['http-missing', 'missing'], ['http-capture', 'capture'], ['http-abort', 'abort'], ['http-cancel', 'hold']]) {
      await withRelay(name, async (relay, guard) => {
        await httpCall(relay.port, '/v1/responses', requestBody, which, { cancel: name === 'http-cancel' }).promise.catch(error => { if (!['http-abort', 'http-cancel'].includes(name)) throw error; });
        await eventually(() => relay.metadata().outstanding[0]?.state === 'UNKNOWN', name);
        fullHold(relay, guard, 1);
        assert.equal(relay.metadata().capture_bytes_held, 0);
      });
    }
    await withRelay('http-relay-close', async (relay, guard) => {
      const pending = httpCall(relay.port, '/v1/responses', requestBody, 'hold');
      const outcome = pending.promise.catch(() => null);
      await eventually(() => fixture.requests.at(-1)?.which === 'hold' && guard.calls.reserve.length === 1, 'http_close_dispatch');
      await relay.close(); await outcome; fullHold(relay, guard, 1);
    });
    await withRelay('http-guard-error', async (relay, guard) => {
      await httpCall(relay.port, '/v1/responses', requestBody, 'success').promise;
      assert.equal(guard.calls.settle.filter(item => item.usage !== null).length, 1);
      assert.equal(guard.calls.settle.filter(item => item.usage === null).length, 1);
      assert.equal(relay.metadata().outstanding[0].state, 'UNKNOWN');
    }, { guard: guardMock({ throwSettle: true }) });

    async function wsCase(name, callback) {
      await withRelay(name, async (relay, guard) => {
        const client = await wsConnect(relay.port, name);
        try { await eventually(() => fixture.sessions.has(name), 'fixture_ws_session'); await callback(relay, guard, client, fixture.sessions.get(name)); }
        finally { client.close(); }
      });
    }
    async function submit(guard, client, session, value, { binary = false, fragmented = false } = {}) {
      const before = guard.calls.reserve.length;
      const payload = bytes(value);
      const wire = fragmented ? [frame(payload.subarray(0, 9), { masked: true, fin: false, opcode: binary ? 2 : 1 }), frame(payload.subarray(9), { masked: true, opcode: 0 })] : [frame(payload, { masked: true, opcode: binary ? 2 : 1 })];
      const frameStart = session.frames.length;
      for (const part of wire) client.socket.write(part);
      await eventually(() => session.frames.length >= frameStart + wire.length && guard.calls.reserve.length === before + 1, 'ws_reserved_then_forwarded');
      assert.deepEqual(session.frames.slice(frameStart, frameStart + wire.length), wire);
      assert.deepEqual(session.messages.at(-1), payload);
      assert.deepEqual(guard.calls.reserve.at(-1).body, value);
      return wire;
    }
    function event(session, value, { fragmented = false, opcode = 1 } = {}) {
      const payload = bytes(value);
      if (fragmented) {
        session.send(frame(payload.subarray(0, 13), { opcode, fin: false }));
        session.send(frame(payload.subarray(13), { opcode: 0 }));
      } else session.send(frame(payload, { opcode }));
    }
    async function created(session, client, id, lane, options) {
      const value = { type: 'response.created', ...(lane === undefined ? {} : { stream_id: lane }), response: { id, model: MODEL, status: 'in_progress' } };
      event(session, value, options);
      await eventually(() => Buffer.concat(client.inbound).length >= Buffer.concat(session.sent).length, 'created_forwarded');
      // Provider byte forwarding happens before private association. Drain the
      // next event-loop turn before submitting another independent request.
      await delay(0);
    }
    async function completed(session, client, id, usage, lane) {
      event(session, { type: 'response.completed', ...(lane === undefined ? {} : { stream_id: lane }), response: { id, model: MODEL, status: 'completed', ...(usage === undefined ? {} : { usage }), service_tier: 'ws-unit-tier' } });
      await eventually(() => Buffer.concat(client.inbound).length >= Buffer.concat(session.sent).length, 'completed_forwarded'); await delay(0);
    }
    const create = (lane, generate = false) => ({ type: 'response.create', model: MODEL, stream_id: lane, generate, input: [] });
    const positive = { input_tokens: 4, output_tokens: 2, input_tokens_details: { cached_tokens: 1 }, output_tokens_details: { reasoning_tokens: 1 } };
    for (const name of ['ws-text', 'ws-binary']) await wsCase(name, async (relay, guard, client, session) => {
      const wire = await submit(guard, client, session, create('lane-wire'), { binary: name === 'ws-binary', fragmented: name === 'ws-text' });
      assert.equal(guard.calls.settle.length, 0); assert.equal(relay.metadata().outstanding.length, 1);
      assert.equal(relay.metadata().outstanding[0].bound_micros, HOLD);
      const attempt = relay.metadata().attempts[0]; assert.equal(attempt.generate_false, true); assert.equal(attempt.wire_sha256, hash(Buffer.concat(wire)));
      await created(session, client, 'wire-id', 'lane-wire', { fragmented: name === 'ws-text', opcode: name === 'ws-binary' ? 2 : 1 });
      await completed(session, client, 'wire-id', positive);
      await eventually(() => guard.calls.settle.some(call => call.usage !== null), 'wire_usage');
      assert.deepEqual(guard.calls.settle[0].usage, positive); assert.equal(guard.calls.settle[0].id, guard.calls.reserve[0].id);
      assert.equal(guard.calls.settle[0].serviceTier, 'ws-unit-tier');
      assert.deepEqual(Buffer.concat(client.inbound), Buffer.concat(session.sent));
      assert.ok(relay.metadata().receipts.some(item => item.provider_response_id_sha256 === hash('wire-id')));
    });
    await wsCase('ws-lanes', async (relay, guard, client, session) => {
      await submit(guard, client, session, create('lane-a')); await created(session, client, 'id-a', 'lane-a');
      await submit(guard, client, session, create('lane-b', true)); await created(session, client, 'id-b', 'lane-b');
      assert.equal(guard.calls.reserve.length, 2);
      assert.equal(guard.calls.settle.length, 0);
      assert.equal(relay.metadata().outstanding.length, 2);
      assert.ok(relay.metadata().outstanding.every(item => item.bound_micros === HOLD));
      assert.deepEqual(relay.metadata().attempts.map(item => item.generate_false), [true, false]);
      await completed(session, client, 'id-b', { input_tokens: 8, output_tokens: 3 });
      await completed(session, client, 'id-a', positive);
      await eventually(() => relay.metadata().settled.length === 2, 'lane_settlement');
      assert.equal(guard.calls.settle[0].id, guard.calls.reserve[1].id); assert.equal(guard.calls.settle[1].id, guard.calls.reserve[0].id);
      assert.equal(relay.metadata().attempts[0].stream_lane_sha256, hash('lane-a'));
      assert.equal(relay.metadata().attempts[1].stream_lane_sha256, hash('lane-b'));
      assert.notEqual(guard.calls.reserve[0].id, guard.calls.reserve[1].id);
    });
    for (const name of ['ws-zero', 'ws-missing', 'ws-zero-mixed', 'ws-zero-generation']) await wsCase(name, async (relay, guard, client, session) => {
      await submit(guard, client, session, create('zero-lane', name === 'ws-zero-generation'));
      await created(session, client, 'zero-id', 'zero-lane');
      const usage = name === 'ws-missing' ? undefined : { input_tokens: 0, output_tokens: 0, input_tokens_details: { cached_tokens: name === 'ws-zero-mixed' ? 1 : 0 } };
      await completed(session, client, 'zero-id', usage);
      await eventually(() => guard.calls.settle.length === 1, 'zero_usage_observed');
      if (name === 'ws-zero') { assert.deepEqual(guard.calls.settle[0].usage, usage); assert.equal(relay.metadata().settled.length, 1); }
      else fullHold(relay, guard, 1);
    });
    for (const name of ['ws-error', 'ws-cancel', 'ws-close', 'ws-provider-close', 'ws-capture']) await wsCase(name, async (relay, guard, client, session) => {
      await submit(guard, client, session, create('unknown-lane'));
      if (name === 'ws-error') event(session, { type: 'error', error: { message: 'synthetic' } });
      if (name === 'ws-cancel') client.socket.write(frame(bytes({ type: 'response.cancel' }), { masked: true }));
      if (name === 'ws-close') client.socket.write(frame(Buffer.from([3, 232]), { masked: true, opcode: 8 }));
      if (name === 'ws-provider-close') session.send(frame(Buffer.from([3, 232]), { opcode: 8 }));
      if (name === 'ws-capture') {
        const large = frame(Buffer.alloc(2 * 1024 * 1024, 120), { opcode: 2 }); session.send(large);
        await eventually(() => Buffer.concat(client.inbound).length >= large.length, 'capture_wire_forwarded');
        assert.deepEqual(Buffer.concat(client.inbound), large);
        assert.ok(relay.metadata().receipts.some(item => item.kind === 'responses_ws_opaque_accounting'));
        await relay.close();
      }
      await eventually(() => relay.metadata().outstanding[0]?.state === 'UNKNOWN', 'ws_unknown');
      fullHold(relay, guard, 1);
      if (name === 'ws-cancel') { assert.equal(guard.calls.reserve.length, 1); assert.equal(session.messages.length, 1); }
    });
    for (const name of ['ws-ambiguous', 'ws-wrong-lane', 'ws-uncreated-id']) await wsCase(name, async (relay, guard, client, session) => {
      await submit(guard, client, session, create('ambiguous-lane', true));
      if (name === 'ws-ambiguous') await submit(guard, client, session, create('ambiguous-lane', true));
      if (name !== 'ws-uncreated-id') await created(session, client, 'ambiguous-id', 'ambiguous-lane');
      await completed(session, client, name === 'ws-uncreated-id' ? 'uncreated' : 'ambiguous-id', positive, name === 'ws-wrong-lane' ? 'other-lane' : 'ambiguous-lane');
      await relay.close(); fullHold(relay, guard);
    });

    const negativeBody = bytes({ contents: 'invalid-not-array', generationConfig: { maxOutputTokens: 8 }, max_tokens: 4 });
    const proof = () => ({ kind: 'pinned_negative_quota_probe', providerAttempts: 1, status: 400, errorStatus: 'INVALID_ARGUMENT', providerEvents: 1, costKnown: true, attemptsDelta: 1, costDeltaMicros: 0, drops: 0, failoverDelta: 0, event: { request_id: 'synthetic-local-request', status: 400, error_code: 'upstream_4xx', upstream_id: ctx.upstreamId, cost_usd_micros: 0, input_tokens: 0, output_tokens: 0 } });
    async function negativeRequest(relay) { const response = await httpCall(relay.port, `/v1beta/models/${MODEL}:generateContent`, negativeBody, 'negative').promise; assert.equal(response.status, 400); }
    for (const name of NAMES.filter(name => name.startsWith('negative-'))) await withRelay(name, async (relay, guard) => {
      if (name === 'negative-general400') { await negativeRequest(relay); fullHold(relay, guard, 1); return; }
      if (name === 'negative-fake-handle') { assert.equal(relay.settlePinnedNegativeProbe({}, proof()).ok, false); assert.equal(guard.calls.reserve.length, 0); assert.equal(guard.calls.settleNonGeneration.length, 0); return; }
      const handle = relay.beginPinnedNegativeProbe({ model: MODEL, maxOutputTokens: 4 });
      await negativeRequest(relay);
      if (name === 'negative-retry2') await negativeRequest(relay);
      const evidence = proof();
      if (name.startsWith('negative-proof-')) delete evidence[name.slice('negative-proof-'.length)];
      if (name.startsWith('negative-event-')) delete evidence.event[name.slice('negative-event-'.length)];
      // Even a forged one-attempt proof cannot release two actual forwards.
      const outcome = relay.settlePinnedNegativeProbe(handle, evidence);
      assert.equal(outcome.ok, name === 'negative-valid');
      assert.equal(relay.settlePinnedNegativeProbe(handle, proof()).ok, false, 'handle must be consumed even when evidence is refused');
      if (name === 'negative-valid') {
        assert.equal(guard.calls.settleNonGeneration.length, 1);
        assert.equal(guard.calls.settleNonGeneration[0].id, guard.calls.reserve[0].id);
        assert.deepEqual(guard.calls.settleNonGeneration[0].proof, evidence);
        assert.equal(relay.metadata().settled[0].settlement_kind, 'PINNED_NON_GENERATION_PROOF');
      } else fullHold(relay, guard);
    }, { protocol: 'google-generate-content' });

    async function forwardToolPayload(relay, guard, payload, { rawPath = '/v1/responses', headers = {} } = {}) {
      const body = Buffer.from(` \n${JSON.stringify(payload, null, 2)}\n`);
      const start = fixture.requests.length;
      const response = await httpCall(relay.port, rawPath, body, 'tools-unit', { headers }).promise;
      assert.equal(response.status, 200);
      assert.deepEqual(response.body, fixture.gzip);
      assert.equal(fixture.requests.length, start + 1);
      const forwarded = fixture.requests[start];
      assert.equal(forwarded.url, rawPath);
      assert.deepEqual(forwarded.body, body, 'metadata projection must never redact or reserialize the original request');
      assert.deepEqual(JSON.parse(forwarded.body.toString('utf8')), payload);
      assert.equal(guard.calls.reserve.length, 1);
      assert.deepEqual(guard.calls.reserve[0].body, payload, 'UNIT guard must receive the complete unmodified billing body');
      for (const [key, value] of Object.entries(headers)) assert.equal(forwarded.headers[key.toLowerCase()], value);
      return forwarded;
    }
    await withRelay('tools-refused-distinct-fields', async (relay, guard) => {
      const payload = { model: MODEL, input: [], tools: [{ type: 'function', name: 'web_search' }, { type: 'web_search' }] };
      const start = fixture.requests.length;
      assert.equal((await httpCall(relay.port, '/v1/responses', bytes(payload), 'tools-refused').promise).status, 503);
      assert.equal(fixture.requests.length, start);
      assert.deepEqual(guard.calls.reserve[0].body, payload);
      assert.equal(guard.calls.settle.length, 0);
      assert.equal(guard.calls.settleNonGeneration.length, 0);
      const metadata = assertToolObservation(relay, guard, [
        expectedToolEntry('$.tools[0]', 0, 'function', 'web_search'),
        expectedToolEntry('$.tools[1]', 0, 'web_search', null),
      ], { admitted: false });
      assert.equal(metadata.counters.actual_boundary_write_attempts, 0);
      assert.deepEqual(metadata.attempts, []);
      assert.deepEqual(metadata.tls_receipts, []);
      assert.deepEqual(metadata.outstanding, []);
      assert.deepEqual(metadata.settled, []);
    }, { guard: guardMock({ refuse: true }) });
    await withRelay('tools-namespace-projection', async (relay, guard) => {
      const marker = {
        schema: 'UNIT_SCHEMA_ONLY_CANARY', description: 'UNIT_DESCRIPTION_ONLY_CANARY',
        arguments: 'UNIT_ARGUMENTS_ONLY_CANARY', prompt: 'UNIT_PROMPT_ONLY_CANARY',
        header: 'UNIT_HEADER_ONLY_CANARY', auth: 'UNIT_AUTH_ONLY_CANARY', query: 'UNIT_QUERY_ONLY_CANARY',
      };
      const payload = {
        model: MODEL, input: marker.prompt, prompt: marker.prompt,
        tools: [
          { type: 'function', name: 'web_search', description: marker.description, arguments: { value: marker.arguments },
            parameters: { type: 'object', properties: { value: { const: marker.schema } }, tools: [{ type: 'function', name: marker.schema }] } },
          { type: 'namespace', name: 'local_group', description: marker.description, tools: [
            { type: 'function', name: 'local_one', arguments: { value: marker.arguments } },
            { type: 'web_search', description: marker.description },
            { type: 'namespace', name: 'inner_group', tools: [{ type: 'function', name: 'deep_local', description: marker.description, prompt: marker.prompt }] },
          ] },
          null,
          { type: { value: marker.schema }, name: { value: marker.arguments }, tools: { type: 'function', name: marker.schema } },
          { name: 'name_only' },
          { type: 'function', name: 'parameter_only', parameters: { tools: [{ type: 'function', name: marker.schema }] } },
        ],
      };
      await forwardToolPayload(relay, guard, payload, {
        rawPath: `/v1/responses?dummy=${marker.query}`,
        headers: { 'x-unit-private-header': marker.header, authorization: `Bearer ${marker.auth}` },
      });
      const metadata = assertToolObservation(relay, guard, [
        expectedToolEntry('$.tools[0]', 0, 'function', 'web_search'),
        expectedToolEntry('$.tools[1]', 0, 'namespace', 'local_group', { nestedPresent: true, nestedArray: true }),
        expectedToolEntry('$.tools[1].tools[0]', 1, 'function', 'local_one'),
        expectedToolEntry('$.tools[1].tools[1]', 1, 'web_search', null),
        expectedToolEntry('$.tools[1].tools[2]', 1, 'namespace', 'inner_group', { nestedPresent: true, nestedArray: true }),
        expectedToolEntry('$.tools[1].tools[2].tools[0]', 2, 'function', 'deep_local'),
        { path: '$.tools[2]', depth: 0, source_path: '$.tools', source_kind: 'ROOT_TOOLS', object: false },
        expectedToolEntry('$.tools[3]', 0, null, null, { typePresent: true, namePresent: true, typeOmitted: true, nameOmitted: true, nestedPresent: true }),
        expectedToolEntry('$.tools[4]', 0, null, 'name_only'),
        expectedToolEntry('$.tools[5]', 0, 'function', 'parameter_only'),
      ], { inputArrayPresent: false, unknown: true, unknownShapes: [{ path: '$.tools[2]', reason: 'TOOL_ENTRY_NOT_OBJECT' }] });
      assertNoToolCanaries(metadata, Object.values(marker));
    });
    await withRelay('tools-credential-labels', async (relay, guard) => {
      // All values are invented local canaries, never credentials read from
      // sources, environment, grants, or a provider.
      const omittedNames = [
        'sk-unit_fixture_dummy', 'sk_unit_fixture_dummy', 'AIzaUnitFixtureDummy',
        'ya29.unit_fixture_dummy', 'ghp_unit_fixture_dummy', 'github_pat_unit_fixture_dummy',
        'xoxb-unit-fixture-dummy', 'glpat-unit-fixture-dummy', 'hf_unit_fixture_dummy',
        'AKIAUNITFIXTUREDUMMY', 'ASIAUNITFIXTUREDUMMY', 'eyJunit_fixture_dummy',
        'bearer_unit_fixture_dummy', 'a'.repeat(32), '1'.repeat(12),
        'unit dummy invalid identifier', 'unit_long_'.repeat(16),
      ];
      const omittedType = 'sk-unit_type_dummy';
      const payload = { model: MODEL, input: [], tools: [
        ...omittedNames.map(name => ({ type: 'function', name })),
        { type: omittedType, name: 'safe_local_name' },
      ] };
      await forwardToolPayload(relay, guard, payload);
      const metadata = assertToolObservation(relay, guard, [
        ...omittedNames.map((_name, index) => expectedToolEntry(`$.tools[${index}]`, 0, 'function', null, { namePresent: true, nameOmitted: true })),
        expectedToolEntry(`$.tools[${omittedNames.length}]`, 0, null, 'safe_local_name', { typePresent: true, typeOmitted: true }),
      ]);
      assertNoToolCanaries(metadata, [...omittedNames, omittedType]);
    });
    await withRelay('tools-bound-rows', async (relay, guard) => {
      const description = 'UNIT_BOUND_ROWS_DESCRIPTION_CANARY';
      const payload = { model: MODEL, input: [], tools: Array.from({ length: 260 }, (_value, index) => ({ type: 'function', name: `row_${index}`, description })) };
      const forwarded = await forwardToolPayload(relay, guard, payload);
      const metadata = assertToolObservation(relay, guard, Array.from({ length: 256 }, (_value, index) => expectedToolEntry(`$.tools[${index}]`, 0, 'function', `row_${index}`)), {
        truncated: true, unknown: true, unknownShapes: [{ path: '$.tools', reason: 'TOOL_METADATA_ENTRY_LIMIT' }],
      });
      assert.equal(JSON.parse(forwarded.body.toString('utf8')).tools.length, 260);
      assert.equal(guard.calls.reserve[0].body.tools.length, 260);
      assert.deepEqual(guard.calls.reserve[0].body.tools[259], { type: 'function', name: 'row_259', description });
      assertNoToolCanaries(metadata, [description, 'row_256', 'row_257', 'row_258', 'row_259']);
    });
    await withRelay('tools-bound-depth', async (relay, guard) => {
      const description = 'UNIT_BOUND_DEPTH_DESCRIPTION_CANARY';
      let chain = { type: 'function', name: 'depth_18', description };
      for (let depth = 17; depth >= 0; depth--) chain = { type: 'namespace', name: `depth_${depth}`, description, tools: [chain] };
      const payload = { model: MODEL, input: [], tools: [chain] };
      const forwarded = await forwardToolPayload(relay, guard, payload);
      const expected = Array.from({ length: 17 }, (_value, depth) => expectedToolEntry(`$.tools[0]${'.tools[0]'.repeat(depth)}`, depth, 'namespace', `depth_${depth}`, { nestedPresent: true, nestedArray: true }));
      const metadata = assertToolObservation(relay, guard, expected, {
        truncated: true, unknown: true,
        unknownShapes: [{ path: `$.tools[0]${'.tools[0]'.repeat(16)}.tools`, reason: 'NAMESPACE_DEPTH_LIMIT' }],
      });
      let node = JSON.parse(forwarded.body.toString('utf8')).tools[0];
      const forwardedNames = [];
      for (;;) {
        forwardedNames.push(node.name);
        if (!Object.hasOwn(node, 'tools')) break;
        assert.equal(node.tools.length, 1);
        node = node.tools[0];
      }
      assert.deepEqual(forwardedNames, Array.from({ length: 19 }, (_value, depth) => `depth_${depth}`));
      assert.equal(node.type, 'function');
      assert.equal(node.description, description);
      assertNoToolCanaries(metadata, [description, 'depth_17', 'depth_18']);
    });

    const typedOnlyObservation = {
      rootPresent: false, rootArray: false, rootShape: 'ABSENT',
      containers: [expectedRootToolContainer('ABSENT'), expectedTypedToolContainer(0)],
      additionalItemsObserved: 1,
    };
    await withRelay('tools-input-lite-http', async (relay, guard) => {
      const payload = { model: MODEL, input: [liteAdditionalToolsInput()] };
      const forwarded = await forwardToolPayload(relay, guard, payload);
      assert.equal(Object.hasOwn(JSON.parse(forwarded.body.toString('utf8')), 'tools'), false);
      assert.equal(Object.hasOwn(guard.calls.reserve[0].body, 'tools'), false);
      assertToolObservation(relay, guard, expectedLiteAdditionalToolEntries('$.input[0].tools'), typedOnlyObservation);
    });
    await wsCase('tools-input-lite-ws', async (relay, guard, client, session) => {
      const payload = { type: 'response.create', model: MODEL, stream_id: 'typed-unit-lane', generate: false, input: [liteAdditionalToolsInput()] };
      const wire = await submit(guard, client, session, payload, { fragmented: true });
      assert.equal(Object.hasOwn(JSON.parse(session.messages[0].toString('utf8')), 'tools'), false);
      assert.equal(Object.hasOwn(guard.calls.reserve[0].body, 'tools'), false);
      assert.deepEqual(session.frames, wire);
      assertToolObservation(relay, guard, expectedLiteAdditionalToolEntries('$.input[0].tools'), typedOnlyObservation);
      assert.equal(guard.calls.settle.length, 0);
      assert.equal(relay.metadata().outstanding[0].bound_micros, HOLD);
      await created(session, client, 'typed-unit-response', 'typed-unit-lane');
      await completed(session, client, 'typed-unit-response', positive);
      await eventually(() => relay.metadata().settled.length === 1, 'typed_unit_ws_settlement');
      assert.equal(guard.calls.settle[0].id, guard.calls.reserve[0].id);
      assert.deepEqual(guard.calls.settle[0].usage, positive);
      assert.deepEqual(Buffer.concat(client.inbound), Buffer.concat(session.sent));
    });
    await withRelay('tools-input-root-and-typed', async (relay, guard) => {
      const ignoredUserContent = 'UNIT_DUAL_SOURCE_USER_CONTENT_CANARY';
      const payload = {
        model: MODEL,
        tools: [{ type: 'function', name: 'root_local_function' }, { type: 'custom', name: 'root_local_custom' }],
        input: [
          liteAdditionalToolsInput(),
          { type: 'message', role: 'user', content: ignoredUserContent },
          { type: 'additional_tools', role: 'developer', tools: [{ type: 'custom', name: 'typed_exec' }] },
        ],
      };
      const forwarded = await forwardToolPayload(relay, guard, payload);
      const original = JSON.parse(forwarded.body.toString('utf8'));
      assert.equal(original.tools.length, 2);
      assert.equal(original.input[0].tools.length, 3);
      assert.equal(original.input[2].tools.length, 1);
      const metadata = assertToolObservation(relay, guard, [
        expectedToolEntry('$.tools[0]', 0, 'function', 'root_local_function'),
        expectedToolEntry('$.tools[1]', 0, 'custom', 'root_local_custom'),
        ...expectedLiteAdditionalToolEntries('$.input[0].tools'),
        expectedToolEntry('$.input[2].tools[0]', 0, 'custom', 'typed_exec', { sourcePath: '$.input[2].tools', sourceKind: 'TYPED_ADDITIONAL_TOOLS' }),
      ], {
        containers: [expectedRootToolContainer('ARRAY', 2), expectedTypedToolContainer(0), expectedTypedToolContainer(2, { toolsCount: 1 })],
        additionalItemsObserved: 2,
      });
      assertNoToolCanaries(metadata, [ignoredUserContent]);
    });
    await withRelay('tools-input-refused', async (relay, guard) => {
      const payload = { model: MODEL, input: [liteAdditionalToolsInput()] };
      const before = fixture.requests.length;
      assert.equal((await httpCall(relay.port, '/v1/responses', bytes(payload), 'typed-unit-refused').promise).status, 503);
      assert.equal(fixture.requests.length, before);
      assert.deepEqual(guard.calls.reserve[0].body, payload);
      assert.equal(Object.hasOwn(guard.calls.reserve[0].body, 'tools'), false);
      assert.equal(guard.calls.settle.length, 0);
      assert.equal(guard.calls.settleNonGeneration.length, 0);
      const metadata = assertToolObservation(relay, guard, expectedLiteAdditionalToolEntries('$.input[0].tools'), { ...typedOnlyObservation, admitted: false });
      assert.equal(metadata.counters.actual_boundary_write_attempts, 0);
      assert.deepEqual(metadata.attempts, []);
      assert.deepEqual(metadata.tls_receipts, []);
      assert.deepEqual(metadata.outstanding, []);
    }, { guard: guardMock({ refuse: true }) });
    await withRelay('tools-input-malformed', async (relay, guard) => {
      const marker = {
        nonArray: 'UNIT_TYPED_NON_ARRAY_CANARY', object: 'UNIT_TYPED_OBJECT_CANARY',
        missingRoleTool: 'UNIT_MISSING_ROLE_TOOL_CANARY', userRoleTool: 'UNIT_USER_ROLE_TOOL_CANARY',
        unsupportedRole: 'UNIT_UNSUPPORTED_ROLE_CANARY', unsupportedRoleTool: 'UNIT_UNSUPPORTED_ROLE_TOOL_CANARY',
        nullRoleTool: 'UNIT_NULL_ROLE_TOOL_CANARY', roleObject: 'UNIT_ROLE_OBJECT_CANARY',
      };
      const payload = { model: MODEL, input: [
        { type: 'additional_tools', role: 'developer' },
        { type: 'additional_tools', role: 'developer', tools: null },
        { type: 'additional_tools', role: 'developer', tools: marker.nonArray },
        { type: 'additional_tools', role: 'developer', tools: { type: 'function', name: marker.object } },
        { type: 'additional_tools', tools: [{ type: 'function', name: marker.missingRoleTool }] },
        { type: 'additional_tools', role: 'user', tools: [{ type: 'function', name: marker.userRoleTool }] },
        { type: 'additional_tools', role: marker.unsupportedRole, tools: [{ type: 'function', name: marker.unsupportedRoleTool }] },
        { type: 'additional_tools', role: null, tools: [{ type: 'function', name: marker.nullRoleTool }] },
        { type: 'additional_tools', role: 'developer', tools: [null, 42] },
        { type: 'additional_tools', role: 'assistant', tools: [] },
        { type: 'additional_tools', role: 'tool', tools: [] },
        { type: 'additional_tools', role: 'system', tools: [] },
        { type: 'additional_tools', role: { value: marker.roleObject }, tools: [] },
        { type: 'additional_tools', role: 'developer', tools: [] },
      ] };
      const forwarded = await forwardToolPayload(relay, guard, payload);
      assert.equal(Object.hasOwn(JSON.parse(forwarded.body.toString('utf8')), 'tools'), false);
      const metadata = assertToolObservation(relay, guard, [
        { source_path: '$.input[8].tools', source_kind: 'TYPED_ADDITIONAL_TOOLS', path: '$.input[8].tools[0]', depth: 0, object: false },
        { source_path: '$.input[8].tools', source_kind: 'TYPED_ADDITIONAL_TOOLS', path: '$.input[8].tools[1]', depth: 0, object: false },
      ], {
        rootPresent: false, rootArray: false, rootShape: 'ABSENT', unknown: true, additionalItemsObserved: 14,
        containers: [
          expectedRootToolContainer('ABSENT'),
          expectedTypedToolContainer(0, { shape: 'ABSENT', toolsCount: null }),
          expectedTypedToolContainer(1, { shape: 'NULL', toolsCount: null }),
          expectedTypedToolContainer(2, { shape: 'INVALID_NON_ARRAY', toolsCount: null }),
          expectedTypedToolContainer(3, { shape: 'INVALID_NON_ARRAY', toolsCount: null }),
          expectedTypedToolContainer(4, { toolsCount: 1, rolePresent: false, role: null, roleMatches: false }),
          expectedTypedToolContainer(5, { toolsCount: 1, role: 'user', roleMatches: false }),
          expectedTypedToolContainer(6, { toolsCount: 1, role: null, roleMatches: false }),
          expectedTypedToolContainer(7, { toolsCount: 1, role: null, roleMatches: false }),
          expectedTypedToolContainer(8, { toolsCount: 2 }),
          expectedTypedToolContainer(9, { toolsCount: 0, role: 'assistant', roleMatches: false }),
          expectedTypedToolContainer(10, { toolsCount: 0, role: 'tool', roleMatches: false }),
          expectedTypedToolContainer(11, { toolsCount: 0, role: 'system', roleMatches: false }),
          expectedTypedToolContainer(12, { toolsCount: 0, role: null, roleMatches: false }),
          expectedTypedToolContainer(13, { toolsCount: 0 }),
        ],
        unknownShapes: [
          { path: '$.input[0].tools', reason: 'ADDITIONAL_TOOLS_ABSENT' },
          { path: '$.input[1].tools', reason: 'ADDITIONAL_TOOLS_NULL' },
          { path: '$.input[2].tools', reason: 'ADDITIONAL_TOOLS_INVALID_NON_ARRAY' },
          { path: '$.input[3].tools', reason: 'ADDITIONAL_TOOLS_INVALID_NON_ARRAY' },
          { path: '$.input[4]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[5]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[6]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[7]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[8].tools[0]', reason: 'TOOL_ENTRY_NOT_OBJECT' },
          { path: '$.input[8].tools[1]', reason: 'TOOL_ENTRY_NOT_OBJECT' },
          { path: '$.input[9]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[10]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[11]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
          { path: '$.input[12]', reason: 'ADDITIONAL_TOOLS_ROLE_MISSING_OR_UNSUPPORTED' },
        ],
      });
      assertNoToolCanaries(metadata, Object.values(marker));
    });
    await withRelay('tools-input-ignore-untyped', async (relay, guard) => {
      const marker = {
        functionTools: 'UNIT_FUNCTION_TOOLS_DECOY', schema: 'UNIT_SCHEMA_TYPED_DECOY',
        shadowNamespace: 'UNIT_SHADOW_NAMESPACE_DECOY', stringInput: 'UNIT_STRING_TYPED_DECOY',
        userContent: 'UNIT_USER_CONTENT_TYPED_DECOY', messageTools: 'UNIT_MESSAGE_TOOLS_DECOY',
        shadowTag: 'UNIT_SHADOW_TYPED_TAG_DECOY', userText: 'UNIT_USER_TEXT_TYPED_DECOY',
      };
      const decoy = name => ({ type: 'additional_tools', role: 'developer', tools: [{ type: 'function', name }] });
      const payload = { model: MODEL, tools: [
        { type: 'function', name: 'real_root_function', tools: [{ type: 'function', name: marker.functionTools }],
          parameters: { tools: [{ type: 'function', name: marker.schema }], input: [decoy(marker.schema)] } },
        { type: 'namespace_shadow', name: 'not_a_namespace', tools: [{ type: 'function', name: marker.shadowNamespace }] },
      ], input: [
        JSON.stringify(decoy(marker.stringInput)),
        { type: 'message', role: 'user', content: JSON.stringify(decoy(marker.userContent)) },
        { type: 'message', role: 'developer', tools: [{ type: 'function', name: marker.messageTools }] },
        { type: 'additional_tools_shadow', role: 'developer', tools: [{ type: 'function', name: marker.shadowTag }] },
        { type: 'message', role: 'user', content: [{ type: 'input_text', text: JSON.stringify(decoy(marker.userText)) }] },
      ] };
      await forwardToolPayload(relay, guard, payload);
      const metadata = assertToolObservation(relay, guard, [
        expectedToolEntry('$.tools[0]', 0, 'function', 'real_root_function', { nestedPresent: true, nestedArray: true }),
        expectedToolEntry('$.tools[1]', 0, 'namespace_shadow', 'not_a_namespace', { nestedPresent: true, nestedArray: true }),
      ]);
      assertNoToolCanaries(metadata, Object.values(marker));
    });

    const occupied = http.createServer((_req, res) => res.end('original-local-listener'));
    await record({ kind: 'listener', id: `127.0.0.1:${plan.collisionPort}/tcp`, role: 'bind-collision-existing-owner', pid: process.pid, delete: 'Normal test lifecycle: close this exact owned server instance only' });
    await bind(occupied, '127.0.0.1', plan.collisionPort);
    try {
      await assert.rejects(factory({ ctx, port: plan.collisionPort, targetBaseUrl: target, protocol: 'openai-responses', models: [MODEL], recordResource: relayRecord, getGuard: () => guardMock(), tlsFixture: { ca: fixture.ca.toString('utf8') } }), /BLOCKED:exact_owned_port_bind_failed_existing_owner_untouched/);
      assert.equal(occupied.listening, true);
      await new Promise((resolve, reject) => { http.get({ host: '127.0.0.1', port: plan.collisionPort, path: '/' }, res => { const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.once('end', () => { try { assert.equal(Buffer.concat(chunks).toString(), 'original-local-listener'); resolve(); } catch (error) { reject(error); } }); }).once('error', reject); });
      results.push({ name: 'bind-collision', status: 'PASS_SYNTHETIC_LOCAL_UNIT', actualVendorCalls: 0 });
    } finally { await new Promise(resolve => occupied.close(resolve)); }

    // These real TLS cases follow all original 64 scenarios. A constructor
    // rejection cannot satisfy the identity-negative case.
    const ipFixtures = [];
    try {
      const ipFiles = { certificate: plan.files.ipCertificate, privateKey: plan.files.ipPrivateKey };
      const ip4 = await localFixture({ ...plan, fixturePort: plan.ipFixturePort, files: ipFiles }, record, { host: '127.0.0.1', role: 'ip-only-san-v4-fixture' });
      ipFixtures.push(ip4);
      const ip6 = await localFixture({ ...plan, fixturePort: plan.ipv6FixturePort, files: ipFiles }, record, { host: '::1', role: 'ip-only-san-v6-fixture' });
      ipFixtures.push(ip6);
      const dnsOnly = await localFixture({ ...plan, fixturePort: plan.identityFixturePort }, record, { host: '127.0.0.1', role: 'ip-url-dns-only-san-fixture' });
      ipFixtures.push(dnsOnly);
      const ipCases = [
        { name: 'http-ipv4-only-san-success', host: '127.0.0.1', urlHost: '127.0.0.1', port: plan.ipFixturePort, fixture: ip4, identityRejected: false },
        { name: 'http-ipv6-only-san-success', host: '::1', urlHost: '[::1]', port: plan.ipv6FixturePort, fixture: ip6, identityRejected: false },
        { name: 'http-ip-dns-only-san-rejected', host: '127.0.0.1', urlHost: '127.0.0.1', port: plan.identityFixturePort, fixture: dnsOnly, identityRejected: true },
      ];
      const allowedIpCodes = new Set(['ERR_INVALID_ARG_VALUE', 'ERR_TLS_CERT_ALTNAME_INVALID']);
      const ipErrorCode = error => {
        let code;
        try { code = error?.code; } catch { return 'UNKNOWN'; }
        return typeof code === 'string' && allowedIpCodes.has(code) ? code : 'UNKNOWN';
      };
      for (const scenario of ipCases) {
        await withRelay(scenario.name, async (relay, guard) => {
          const marker = `UNIT_IP_TLS_PRIVATE_HEADER_${scenario.name.replaceAll('-', '_')}`;
          const rawPath = '/literal/ip/responses?dup=a&dup=b&slash=%2F&plus=+&empty=';
          const authority = `${scenario.urlHost}:${scenario.port}`;
          const expectedHeaders = [
            'Host', authority, 'Connection', 'close', 'X-Raw-Case', 'a%2F+',
            'X-Raw-List', 'left,right', 'X-Private-Canary', marker,
            'content-type', 'application/json', 'content-length', String(requestBody.length),
            'x-fixture-case', scenario.name,
          ];
          const before = scenario.fixture.requests.length;
          const originalRequest = https.request;
          let constructorCalls = 0, constructorReturned = 0, socketCount = 0, secureCount = 0;
          let constructorCode = null, transportCode = null, observedHeaders = null, outcome;
          let checkpoint = 'IP_SETUP', beforeAssert = null, finallyObserved = false;
          const snapshot = () => {
            const metadata = relay.metadata();
            const tls = metadata.tls_receipts[0];
            const dispatch = metadata.receipts.find(item => item.kind === 'http_dispatch_failure');
            const phases = ['READ_BODY', 'ADMISSION', 'REQUEST_OPTIONS', 'HTTPS_REQUEST', 'REQUEST_HANDLERS', 'UPSTREAM_TRAILERS', 'UPSTREAM_END'];
            const boundedInteger = value => Number.isSafeInteger(value) && value >= 0 && value <= 65535 ? value : null;
            const generic = outcome?.status === 503 && [
              bytes({ error: { type: 'provider_boundary_blocked', message: 'BLOCKED:provider_boundary_blocked' } }),
              bytes({ error: { type: 'provider_boundary_blocked', message: 'BLOCKED:provider_tls_or_transport_failure_full_hold' } }),
            ].some(value => outcome.body.equals(value));
            return {
              constructor_calls: boundedInteger(constructorCalls), constructor_returned_count: boundedInteger(constructorReturned),
              constructor_error_code: constructorCode, tls_error_code: transportCode,
              socket_count: boundedInteger(socketCount), secure_connect_count: boundedInteger(secureCount),
              observer_restored: https.request === originalRequest,
              status_is_200: outcome ? outcome.status === 200 : null, status_is_503: outcome ? outcome.status === 503 : null,
              generic_503_matches: outcome ? generic : null,
              tls_receipt_count: boundedInteger(metadata.tls_receipts.length),
              certificate_authorized: tls ? tls.certificate_authorized === true : null,
              hostname_verified: tls ? tls.hostname_verified === true : null,
              fixture_forward_delta: boundedInteger(scenario.fixture.requests.length - before),
              reserve_count: boundedInteger(guard.calls.reserve.length), settle_count: boundedInteger(guard.calls.settle.length),
              null_settle_count: boundedInteger(guard.calls.settle.filter(item => item.usage === null).length),
              positive_settle_count: boundedInteger(guard.calls.settle.filter(item => item.usage !== null).length),
              unknown_full_hold: metadata.outstanding.length === 1 && metadata.outstanding[0].state === 'UNKNOWN' &&
                metadata.outstanding[0].hold_policy === 'FULL_HOLD' && metadata.outstanding[0].bound_micros === HOLD,
              settled_count: boundedInteger(metadata.settled.length),
              boundary_write_attempts: boundedInteger(metadata.counters.actual_boundary_write_attempts),
              generation_write_attempts: boundedInteger(metadata.counters.actualgeneration_writeattempts),
              provider_response_receipts: boundedInteger(metadata.counters.provider_response_receipts),
              complete_response_observed: metadata.receipts.some(item => item.kind === 'generation_https_response' && item.complete === true),
              dispatch_phase: phases.includes(dispatch?.phase) ? dispatch.phase : null,
              canary_absent: !JSON.stringify(metadata).includes(marker) && !(outcome?.body.includes(Buffer.from(marker)) ?? false),
            };
          };
          const beforeAssertion = value => { checkpoint = value; beforeAssert = snapshot(); };
          try {
            try {
              https.request = function (...args) {
                const options = args[0];
                const exactFixture = options?.protocol === 'https:' && options.hostname === scenario.host &&
                  String(options.port) === String(scenario.port) && options.method === 'POST' && options.path === rawPath &&
                  options.ca === scenario.fixture.ca.toString('utf8') && Array.isArray(options.headers) &&
                  options.headers.some((value, index) => index % 2 === 0 && value.toLowerCase() === 'x-fixture-case' && options.headers[index + 1] === scenario.name);
                if (!exactFixture) return Reflect.apply(originalRequest, this, args);
                constructorCalls++; observedHeaders = [...options.headers];
                let request;
                try { request = Reflect.apply(originalRequest, this, args); constructorReturned++; }
                catch (error) { constructorCode = ipErrorCode(error); throw error; }
                request.once('socket', socket => { socketCount++; socket.once('secureConnect', () => { secureCount++; }); });
                request.once('error', error => { transportCode = ipErrorCode(error); });
                return request;
              };
              outcome = await httpCall(relay.port, rawPath, requestBody, scenario.name, {
                headers: { Host: 'downstream.invalid', Connection: 'close', 'X-Raw-Case': 'a%2F+', 'X-Raw-List': 'left,right', 'X-Private-Canary': marker },
              }).promise;
            } finally { https.request = originalRequest; }
            beforeAssertion('IP_OBSERVER_RESTORED');
            assert.equal(https.request, originalRequest);
            if (!scenario.identityRejected) {
              beforeAssertion('IP_EXPECT_STATUS_200');
              assert.equal(outcome.status, 200);
            }
            beforeAssertion('IP_CONSTRUCTOR_RETURNED');
            assert.equal(constructorCalls, 1); assert.equal(constructorReturned, 1); assert.equal(constructorCode, null);
            const metadata = relay.metadata();
            beforeAssertion('IP_ONE_TLS_RECEIPT');
            assert.equal(metadata.tls_receipts.length, 1); assert.equal(socketCount, 1);
            beforeAssertion('IP_EXACT_RESERVATION_AND_ATTEMPT');
            assert.equal(guard.calls.reserve.length, 1);
            assert.equal(metadata.counters.actual_boundary_write_attempts, 1);
            assert.equal(metadata.counters.actualgeneration_writeattempts, 1);
            assert.equal(metadata.attempts.length, 1);
            assert.equal(metadata.attempts[0].reservation_id_sha256, hash(guard.calls.reserve[0].id));
            beforeAssertion('IP_RAW_HEADER_OPTIONS');
            assert.deepEqual(observedHeaders, expectedHeaders);
            if (scenario.identityRejected) {
              beforeAssertion('IP_TLS_IDENTITY_ERROR');
              assert.equal(transportCode, 'ERR_TLS_CERT_ALTNAME_INVALID');
              assert.equal(secureCount, 0); assert.equal(metadata.tls_receipts[0].secure_connect_observed, false);
              assert.equal(metadata.tls_receipts[0].transport_error, true);
              beforeAssertion('IP_EXPECT_GENERIC_TLS_503');
              assert.equal(outcome.status, 503);
              assert.deepEqual(outcome.body, bytes({ error: { type: 'provider_boundary_blocked', message: 'BLOCKED:provider_tls_or_transport_failure_full_hold' } }));
              beforeAssertion('IP_IDENTITY_NO_HTTP_FORWARD');
              assert.equal(scenario.fixture.requests.length, before);
              assert.equal(metadata.counters.provider_response_receipts, 0);
              beforeAssertion('IP_IDENTITY_UNKNOWN_EXACT_NULL_SETTLEMENT');
              assert.equal(guard.calls.settle.length, 1); fullHold(relay, guard, 1);
            } else {
              beforeAssertion('IP_TLS_CERTIFICATE_AND_IDENTITY');
              assert.equal(transportCode, null); assert.equal(secureCount, 1);
              assert.equal(metadata.tls_receipts[0].secure_connect_observed, true);
              assert.equal(metadata.tls_receipts[0].certificate_authorized, true);
              assert.equal(metadata.tls_receipts[0].hostname_verified, true);
              assert.equal(metadata.tls_receipts[0].status, 200);
              beforeAssertion('IP_RAW_WIRE_EQUALITY');
              const forwarded = scenario.fixture.requests.slice(before);
              assert.equal(forwarded.length, 1);
              beforeAssertion('IP_RAW_HTTP_METHOD');
              assert.equal(forwarded[0].method, 'POST');
              beforeAssertion('IP_RAW_PATH_QUERY');
              assert.equal(forwarded[0].url, rawPath);
              beforeAssertion('IP_RAW_HEADERS');
              assert.deepEqual(forwarded[0].rawHeaders, expectedHeaders);
              beforeAssertion('IP_RAW_BODY');
              assert.deepEqual(forwarded[0].body, requestBody);
              beforeAssertion('IP_RAW_RESPONSE_BYTES');
              assert.deepEqual(outcome.body, scenario.fixture.gzip);
              beforeAssertion('IP_COMPLETE_POSITIVE_FINANCIAL_SETTLEMENT');
              assert.equal(guard.calls.settle.length, 1);
              assert.equal(guard.calls.settle[0].id, guard.calls.reserve[0].id);
              assert.deepEqual(guard.calls.settle[0].usage, { input_tokens: 3, output_tokens: 2, input_tokens_details: { cached_tokens: 1 } });
              assert.equal(guard.calls.settle[0].providerStatus, 200); assert.equal(guard.calls.settle[0].serviceTier, 'unit-lane');
              assert.equal(guard.calls.settleNonGeneration.length, 0);
              assert.equal(metadata.outstanding.length, 0); assert.equal(metadata.settled.length, 1);
              assert.equal(metadata.counters.provider_response_receipts, 1);
              assert.ok(metadata.receipts.some(item => item.kind === 'generation_https_response' && item.complete === true && item.settlement === 'SETTLED'));
            }
            beforeAssertion('IP_NO_SETTLEMENT_FAILURE_OR_CAPTURE_LEAK');
            assert.equal(metadata.counters.settlement_failures, 0); assert.equal(metadata.capture_bytes_held, 0);
            beforeAssertion('IP_CANARY_ABSENT');
            assertNoToolCanaries({ metadata, outcome }, [marker]);
          } catch (error) {
            if (error !== null && (typeof error === 'object' || typeof error === 'function')) {
              dispatchFailureAudits.set(error, () => ({
                schema: 'synthetic-ip-tls-failure-audit-v1', scope: 'ACTUAL_RELAY_LOCAL_TLS_UNIT_ONLY',
                case_name: scenario.name, checkpoint, finally_observed: finallyObserved,
                before_assert: beforeAssert, after_finally: snapshot(),
              }));
            }
            throw error;
          } finally { finallyObserved = true; }
        }, { targetBaseUrl: `https://${scenario.urlHost}:${scenario.port}/v1/responses`, boundaryFixture: scenario.fixture, expectedHost: scenario.urlHost });
      }
    } finally { for (const ownedFixture of ipFixtures.reverse()) await ownedFixture.close(); }
    assert.equal(results.length, NAMES.length);
    return { schema: plan.schema, ok: true, synthetic_fixture_only: true, actualVendorCalls: 0, budgetGuard: 'UNIT_MOCK_ONLY', financialCatalogInvoiceNativePass: 'NOT_CLAIMED', cases: results };
  } finally { await fixture.close(); }
}

async function createExclusiveFile(record, pathname, role) {
  await record({ kind: 'file', id: pathname, role, mode: '0600', delete: 'RETAINED; explicit cleanup approval and ownership matching required' });
  // wx supplies O_CREAT|O_EXCL; initial permissions are at most 0600 before
  // OpenSSL opens/truncates these existing owned files. Never pre-touch broadly.
  try { const handle = await fs.open(pathname, 'wx', 0o600); await handle.close(); }
  catch { blocked('planned_retained_file_exists_or_unwritable_choose_new_run_id'); }
}

async function processRun(command, args, id, record, { ipc = false, onMessage } = {}) {
  // Intent is durable before spawn; the actual PID follows immediately. No
  // process output is captured or logged, especially OpenSSL diagnostics.
  await record({ kind: 'process-intent', id, command: [command, ...args], delete: 'Normal finite process exit; no port-owner kill' });
  const child = spawn(command, args, { env: environment(), stdio: ipc ? ['ignore', 'ignore', 'ignore', 'ipc'] : 'ignore', ...(ipc ? {} : { timeout: 30000, killSignal: 'SIGTERM' }) });
  let exitCode, exitSignal, spawnError;
  const exited = new Promise(resolve => {
    child.once('error', error => { spawnError = error; resolve(); });
    child.once('exit', (code, signal) => { exitCode = code; exitSignal = signal; resolve(); });
  });
  let ready = false, result = null;
  child.on('message', message => {
    if (message?.type === 'ready') ready = true;
    if (message?.type === 'result') result = message.result;
    if (onMessage) void onMessage(child, message);
  });
  await record({ kind: 'process', id, pid: child.pid ?? null, command: [command, ...args], created: new Date().toISOString(), delete: 'Normal finite process exit; retained ledger' });
  if (ipc) {
    await eventually(() => ready || spawnError || exitCode !== undefined || exitSignal != null, 'child_ready');
    if (ready) child.send({ type: 'go' });
  }
  await exited;
  if (spawnError?.code === 'ENOENT') blocked(ipc ? 'child_runtime_executable_missing' : 'openssl_executable_missing');
  if (!ipc && exitSignal) blocked('openssl_local_certificate_generation_timeout_or_signal');
  if (spawnError || exitCode !== 0 || exitSignal) blocked(ipc ? 'child_runtime_or_local_fixture_prerequisite_failed' : 'openssl_req_addext_localhost_certificate_generation_failed');
  if (ipc && !result) blocked('child_result_missing');
  return result;
}

/** Future parent --run only. Global QA ledger is injected, never discovered. */
export async function runProviderBoundaryTests({ ctx, recordResource, plan = providerBoundaryPlan() } = {}) {
  if (!ctx || typeof recordResource !== 'function' || ctx.root !== plan.root || ctx.evidenceDir !== plan.evidenceDir) blocked('matching_parent_context_and_global_qa_ledger_required');
  await existingDirectory(plan.root); await existingDirectory(plan.evidenceDir);
  const rootReal = await fs.realpath(plan.root), evidenceReal = await fs.realpath(plan.evidenceDir);
  const evidenceRelative = path.relative(rootReal, evidenceReal);
  if (evidenceRelative === '..' || evidenceRelative.startsWith(`..${path.sep}`) || path.isAbsolute(evidenceRelative)) blocked('evidence_symlink_escapes_approved_root');
  // The exact metadata plan is retained before any fixture resource creation.
  // Parent must display/approve this plan before calling; run has no stdin.
  const rebuilt = providerBoundaryPlan(plan);
  assert.deepEqual(plan, rebuilt, 'run requires an unmodified exact plan');
  const record = resource => recordResource({ ...ctx, root: null }, { ...resource, created: new Date().toISOString() });
  await record({ kind: 'qa-plan', id: `${PREFIX}${plan.runId}:plan`, plan, delete: 'Retained QA ledger evidence; no automatic cleanup' });
  await createExclusiveFile(record, plan.files.certificate, 'synthetic-local-certificate');
  await createExclusiveFile(record, plan.files.privateKey, 'synthetic-local-private-key');
  await createExclusiveFile(record, plan.files.ipCertificate, 'synthetic-ip-only-certificate');
  await createExclusiveFile(record, plan.files.ipPrivateKey, 'synthetic-ip-only-private-key');
  for (const openssl of plan.processes.filter(item => item.id.endsWith(':openssl') || item.id.endsWith(':openssl-ip'))) {
    await processRun(openssl.command[0], openssl.command.slice(1), openssl.id, record);
  }
  for (const file of [plan.files.certificate, plan.files.privateKey, plan.files.ipCertificate, plan.files.ipPrivateKey]) {
    await fs.chmod(file, 0o600);
    if ((await fs.stat(file)).size === 0) blocked('openssl_certificate_or_key_missing');
  }
  const child = plan.processes.find(item => item.id.endsWith(':child'));
  const result = await processRun(child.command[0], child.command.slice(1), child.id, record, {
    ipc: true,
    async onMessage(process, message) {
      if (message?.type !== 'record') return;
      try {
        const item = message.resource;
        if (item.kind !== 'listener' || !plan.listeners.some(listener => listener.id === item.id)) blocked('child_unplanned_listener_resource');
        await record(item); process.send({ type: 'record-ack', id: message.id });
      } catch { process.send({ type: 'record-error', id: message.id }); }
    },
  });
  await createExclusiveFile(record, plan.files.result, 'synthetic-unit-result');
  await fs.writeFile(plan.files.result, JSON.stringify(result, null, 2), { mode: 0o600 });
  if (!result.ok) blocked(result.reason || 'synthetic_local_regression_failed');
  return { resultPath: plan.files.result, result, retained: Object.values(plan.files) };
}

async function childMain(plan) {
  if (typeof process.send !== 'function') blocked('parent_child_ipc_required');
  let resolveGo;
  const go = new Promise(resolve => { resolveGo = resolve; });
  const pending = new Map();
  process.on('message', message => {
    if (message?.type === 'go') resolveGo();
    const waiter = pending.get(message?.id);
    if (waiter && message.type === 'record-ack') { pending.delete(message.id); waiter.resolve(); }
    if (waiter && message.type === 'record-error') { pending.delete(message.id); waiter.reject(new Error('BLOCKED:global_qa_ledger_registration_failed')); }
  });
  const record = resource => new Promise((resolve, reject) => {
    const id = randomUUID(); pending.set(id, { resolve, reject }); process.send({ type: 'record', id, resource });
  });
  process.send({ type: 'ready' }); await go;
  let result;
  try { result = await testSuite(plan, record); }
  catch (error) {
    const audit = dispatchFailureAudits.get(error);
    result = { ok: false, synthetic_fixture_only: true, actualVendorCalls: 0, reason: safeError(error), ...(audit ? { dispatch_failure_audit: audit() } : {}) };
  }
  await new Promise(resolve => process.send({ type: 'result', result }, resolve));
  process.disconnect();
}

function cliPlan(argv) {
  const read = key => { const index = argv.indexOf(key); return index < 0 ? undefined : argv[index + 1]; };
  return providerBoundaryPlan({ root: read('--root'), evidenceDir: read('--evidence') ?? read('--root'), runId: read('--id'), basePort: read('--port') === undefined ? undefined : Number(read('--port')), openssl: read('--openssl'), runtime: read('--runtime') });
}

async function main() {
  const argv = process.argv.slice(2), plan = cliPlan(argv);
  if (argv.includes('--plan')) { process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`); return; }
  if (argv.includes('--child')) { await childMain(plan); return; }
  if (!argv.includes('--run')) blocked('use_plan_or_parent_only_run');
  const result = await runProviderBoundaryTests({ ctx: globalThis.__APPROVED_CORE3_QA_CONTEXT, recordResource: globalThis.__APPROVED_CORE3_QA_RECORD_RESOURCE, plan });
  process.stdout.write(`${JSON.stringify({ resultPath: result.resultPath, scope: 'SYNTHETIC_LOCAL_UNIT_ONLY', retained: result.retained })}\n`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === SELF) main().catch(error => { process.stderr.write(`${safeError(error)}\n`); process.exitCode = 1; });
