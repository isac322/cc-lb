import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import { connect as connectHttp2 } from 'node:http2';
import { connect as connectTcp } from 'node:net';
import { mkdtemp, mkdir, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  assertTerminalOrder, buildAgyLaunch, buildSandboxProfile, classifyConsumerFailure, classifyDenialSignals, classifyDeniedOperations, createResourceLedger, encodeReplayFrames, websocketReplayFrames,
  evaluateHarnessOwner, matchesSyntheticGatewayAuth, observeDecodedConsumerOutput, preflightIsolation, runBoundedProcess,
  startReplayServer, validateReplayFixture,
} from './recorder-replay.mjs';
import { runHermeticFixture } from './run.mjs';

async function workspace(label) {
  const parent = process.env.CC_LB_QA_OUTPUT ?? tmpdir();
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const scratchDir = await mkdtemp(path.join(parent, `qh-${label}-`));
  const ledger = createResourceLedger(scratchDir);
  await ledger({ kind: 'scratch-directory', identifier: scratchDir, delete_command: `rm -rf -- '${scratchDir.replaceAll("'", "'\\''")}'`, state: 'retained' });
  return { scratchDir, ledger };
}

function fixture(frames = [{ kind: 'json', data: { safeUnknown: { zero: 0, no: false }, message: 'synthetic-ok' } }]) {
  return {
    id: 'qh-synthetic-case', provenance: 'synthetic', client: { name: 'test', version: '1', source_revision: null, binary_sha256: null },
    surfaceIds: ['synthetic:unit'], scenario: 'Local harness test; never runtime/provider proof',
    request: { method: 'POST', path: '/replay?x=1&x=2', headers: [['content-type', 'application/json']], body: { safe: true } },
    response: { status: 200, headers: [['content-type', 'application/json'], ['x-qh-order', 'first'], ['x-qh-order', 'second']], frames, termination: 'eof' },
    expected: { provider_success: false },
  };
}

function http(replay, value, body = JSON.stringify(value.request.body)) {
  return new Promise((resolve, reject) => {
    // Raw header arrays disable Node's automatic Host header. Keep fixture order
    // and duplicates, supplying only the mandatory authority when it is absent.
    const headers = value.request.headers.some(([name]) => name.toLowerCase() === 'host')
      ? value.request.headers : [['Host', new URL(replay.origin).host], ...value.request.headers];
    const child = request(`${replay.origin}${value.request.path}`, { method: value.request.method, headers: headers.flat() }, (response) => {
      const chunks = []; let settled = false;
      const finish = (complete) => { if (!settled) { settled = true; resolve({ status: response.statusCode, headers: response.rawHeaders, complete, bytes: Buffer.concat(chunks) }); } };
      response.on('data', (chunk) => chunks.push(chunk)); response.on('end', () => finish(true)); response.on('aborted', () => finish(false)); response.on('error', () => finish(false));
    });
    child.on('error', reject); child.end(body);
  });
}

test('QH-WIRE-002 / QH-PROTO-001: unknown fields, duplicate headers and query order survive synthetic replay', async () => {
  const owned = await workspace('order'); const value = fixture();
  let seen;
  const replay = await startReplayServer({ ...owned, fixture: value, authorize: (request) => { seen = request; return true; } });
  try {
    const result = await http(replay, value);
    assert.equal(result.status, 200);
    assert.deepEqual(seen.headers.filter(([name]) => name.toLowerCase() === 'host').map(([, value]) => value), [new URL(replay.origin).host]);
    assert.deepEqual(JSON.parse(result.bytes), value.response.frames[0].data);
    const pairs = result.headers.flatMap((name, i, raw) => i % 2 === 0 && name.toLowerCase() === 'x-qh-order' ? [raw[i + 1]] : []);
    assert.deepEqual(pairs, ['first', 'second']);
    assert.deepEqual([...new URL(seen.path, replay.origin).searchParams], [['x', '1'], ['x', '2']]);
    assert.deepEqual(value.response.frames[0].data.safeUnknown, { zero: 0, no: false });
  } finally { await replay.close(); }
});

test('QH-WIRE-002 / QH-SSE-001: sanitized SSE retains multiline, UTF8 and terminal order across fragments', async () => {
  const owned = await workspace('sse');
  const value = fixture([
    { kind: 'sse', data: ': synthetic comment\r\nid: synthetic-event\r\nevent: response.created\r\ndata: {"type":"response.created",\r\ndata: "response":{"id":"resp_synthetic"}}\r\n\r\n', chunkSizes: [1, 7, 1, 3] },
    { kind: 'sse', data: 'data: {"type":"response.output_text.delta","delta":"합성 UTF8"}\n\n', chunkSizes: [1, 2, 5] },
    { kind: 'sse', data: 'data: {"type":"response.completed","response":{"id":"resp_synthetic"}}\n\n' },
  ]);
  value.response.headers = [['content-type', 'text/event-stream']];
  const replay = await startReplayServer({ ...owned, fixture: value });
  try {
    const result = await http(replay, value);
    assert.equal(result.complete, true);
    assert.match(result.bytes.toString(), /합성 UTF8/);
    assert(result.bytes.indexOf('response.created') < result.bytes.indexOf('response.completed'));
    assert.match(result.bytes.toString(), /: synthetic comment/);
  } finally { await replay.close(); }
});

test('QH-WIRE-003 / QH-ERR-001: malformed and partial frames, provider error, cancel and post-terminal mutation stay distinct', async () => {
  const owned = await workspace('negatives');
  const malformed = fixture([{ kind: 'sse', data: 'data: {not-json}\n\n' }]);
  const provider = fixture([{ kind: 'json', data: { error: { code: 'provider_unavailable', type: 'provider-error' } } }]); provider.response.status = 503;
  for (const [i, value] of [malformed, provider].entries()) {
    const replay = await startReplayServer({ fixture: value, scratchDir: path.join(owned.scratchDir, String(i)), ledger: owned.ledger });
    try { const result = await http(replay, value); assert.equal(result.status, value.response.status); assert(result.bytes.length > 0); }
    finally { await replay.close(); }
  }
  for (const termination of ['abrupt-eof', 'client-cancel', 'connection-reset']) {
    const value = fixture([{ kind: 'sse', data: 'data: {"type":"response.output_text.delta","delta":"partial' }]);
    value.response.termination = termination;
    const replay = await startReplayServer({ fixture: value, scratchDir: path.join(owned.scratchDir, termination), ledger: owned.ledger });
    try { const result = await http(replay, value); assert.equal(result.complete, false); }
    finally { await replay.close(); }
  }
  assert.throws(() => assertTerminalOrder([
    { kind: 'sse', data: 'data: {"type":"response.completed"}\n\n' },
    { kind: 'sse', data: 'data: {"type":"response.output_text.delta","delta":"after"}\n\n' },
  ]), /QHARNESS_POST_TERMINAL_DATA/);
});

test('QH-WIRE-004 / QH-AUTH-001: unauthorized headers receive 401 before sending body or invoking postauth hooks', async () => {
  const owned = await workspace('auth'); const value = fixture();
  const calls = { owner: 0, quota: 0, signer: 0, provider: 0 };
  const hooks = Object.fromEntries(Object.keys(calls).map((key) => [key, () => { calls[key]++; }]));
  const replay = await startReplayServer({ ...owned, fixture: value, authorize: () => false, hooks });
  const socket = connectTcp(replay.port, '127.0.0.1');
  try {
    const status = await new Promise((resolve, reject) => {
      socket.setTimeout(2000, () => reject(new Error('header-only response deadline')));
      socket.once('error', reject);
      socket.once('connect', () => socket.write(`POST ${value.request.path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 999999\r\nExpect: 100-continue\r\n\r\n`));
      socket.once('data', (chunk) => resolve(chunk.toString('ascii').split('\r\n')[0]));
    });
    assert.match(status, /^HTTP\/1\.1 401/);
    assert.equal(replay.stats.bodyBytesRead, 0); assert.equal(replay.stats.bodyReadCalls, 0);
    assert.deepEqual(calls, { owner: 0, quota: 0, signer: 0, provider: 0 });
    assert.deepEqual([replay.stats.ownerLookups, replay.stats.quotaLookups, replay.stats.signerCalls, replay.stats.providerCalls], [0, 0, 0, 0]);
  } finally { socket.destroy(); await replay.close(); }
});

test('QH-WIRE-005 / QH-OWNER-001: owner negatives are explicitly harness-only, not runtime evidence', () => {
  const state = { principal: 'synthetic-p1', provider: 'gemini', kind: 'session', owner: 'synthetic-o1', epoch: '1' };
  assert.equal(evaluateHarnessOwner(state, state).allowed, true);
  for (const key of Object.keys(state)) assert.equal(evaluateHarnessOwner({ ...state, [key]: 'synthetic-other' }, state).allowed, false);
  const unavailable = evaluateHarnessOwner(state, { ...state, available: false });
  assert.equal(unavailable.allowed, false); assert.equal(unavailable.runtimeAcceptance, 'NOT-RUN');
  assert.equal(evaluateHarnessOwner({ requestId: 'arbitrary', project: 'arbitrary' }, state).allowed, false);
});

test('QH-RED-007: planted unknown fixture secret rejects before listener and cannot appear in errors', async () => {
  const owned = await workspace('leak'); const sentinel = 'QHARNESS_PLANTED_UNKNOWN_SECRET_1324';
  const value = fixture([{ kind: 'json', data: { unknown: sentinel } }]);
  await assert.rejects(startReplayServer({ ...owned, fixture: value, sentinels: [sentinel] }), (error) => !error.message.includes(sentinel));
});

test('QH-RED-005: captured opaque request/stdout content is not persisted', async () => {
  const owned = await workspace('capture'); const value = fixture();
  const replay = await startReplayServer({ ...owned, fixture: value });
  const sentinel = 'QHARNESS_CAPTURED_UNKNOWN_PRIVATE_CONTENT_1324';
  try { await http(replay, value, JSON.stringify({ futureUnknown: sentinel, authorization: `Bearer ${sentinel}` })); }
  finally { await replay.close(); }
  assert(!(await readFile(path.join(owned.scratchDir, 'wire.jsonl'), 'utf8')).includes(sentinel));
});

test('QH-WIRE-002 / QH-GRPC-001: generated gRPC prefix, payload and trailers cross an actual HTTP2 stream', async () => {
  const owned = await workspace('grpc');
  const payload = Buffer.from([0, 0, 0, 0, 2, 10, 0]);
  const value = fixture([{ kind: 'grpc', data: payload.toString('base64'), encoding: 'base64', syntheticContent: true }]);
  value.response.headers = [['content-type', 'application/grpc']]; value.response.trailers = [['grpc-status', '0']];
  const replay = await startReplayServer({ ...owned, fixture: value }); const session = connectHttp2(replay.origin);
  try {
    const response = await new Promise((resolve, reject) => {
      const stream = session.request({ ':method': 'POST', ':path': value.request.path }); const chunks = []; let status;
      stream.on('trailers', (trailers) => { status = trailers['grpc-status']; });
      stream.on('data', (chunk) => chunks.push(chunk)); stream.on('end', () => resolve({ bytes: Buffer.concat(chunks), status })); stream.on('error', reject); stream.end('{}');
    });
    assert.deepEqual(response.bytes, payload); assert.equal(response.status, '0');
    assert.deepEqual(encodeReplayFrames(value), payload);
  } finally { session.destroy(); await replay.close(); }
});

test('QH-WIRE-002 / QH-WS-001: actual websocket text and generated binary preserve payload order', async () => {
  const owned = await workspace('ws'); const value = fixture([
    { kind: 'ws-text', data: { type: 'response.created', safeUnknown: false } },
    { kind: 'ws-binary', data: 'CgA=', encoding: 'base64', syntheticContent: true },
  ]);
  value.request.method = 'GET'; value.response.status = 101; value.response.headers = [];
  const replay = await startReplayServer({ ...owned, fixture: value });
  const client = new WebSocket(`${replay.origin.replace('http:', 'ws:')}${value.request.path}`); client.binaryType = 'arraybuffer';
  try {
    const messages = await new Promise((resolve, reject) => {
      const chunks = []; const timer = setTimeout(() => reject(new Error('websocket deadline')), 2000);
      client.addEventListener('open', () => client.send('{"type":"response.create"}'));
      client.addEventListener('error', () => { clearTimeout(timer); reject(new Error('websocket error')); });
      client.addEventListener('message', (event) => { chunks.push(event.data); if (chunks.length === 2) { clearTimeout(timer); resolve(chunks); } });
    });
    assert.equal(JSON.parse(messages[0]).safeUnknown, false); assert.deepEqual(Buffer.from(messages[1]), Buffer.from([10, 0]));
  } finally { client.close(); await replay.close(); }
});

test('QH-EVD-001 / QH-WIRE-001: request echo and fixture self-report cannot satisfy decoded consumer output', () => {
  const marker = 'QHARNESS_UNPROMPTED_MARKER';
  const echo = Buffer.from(JSON.stringify({ type: 'request', marker, success: true }));
  assert.deepEqual(observeDecodedConsumerOutput({ client: 'codex', stdout: echo, marker }), { markerObserved: false, completed: false });
  const decoded = Buffer.from(`${JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: marker } })}\n${JSON.stringify({ type: 'turn.completed' })}\n`);
  assert.deepEqual(observeDecodedConsumerOutput({ client: 'codex', stdout: decoded, marker }), { markerObserved: true, completed: true });
  assert.deepEqual(observeDecodedConsumerOutput({ client: 'agy', stdout: Buffer.from(JSON.stringify({ event: 'result', result: { status: 'SUCCESS', response: marker } })), marker }), { markerObserved: true, completed: true });
});

test('QH-ISO-003: missing enforceable sandbox returns BLOCKED, never a skipped PASS', async () => {
  assert.deepEqual(await preflightIsolation({ platform: 'unsupported', scratchDir: '/unused', recorderPort: 1 }), { status: 'BLOCKED', reason: 'QHARNESS_OS_SANDBOX_UNAVAILABLE', spawnedClient: false });
});

test('QH-ISO-001/002: exact-port profile denies original auth/keychain and explicit planted stand-in', () => {
  const profile = buildSandboxProfile({ scratchDir: '/private/tmp/synthetic', originalHome: '/synthetic/original-home', deniedProbe: '/private/tmp/synthetic/denied', recorderPort: 12345 });
  assert.match(profile, /localhost:12345/); assert(!profile.includes('localhost:*'));
  assert.match(profile, /deny network\*/); assert.match(profile, /\/synthetic\/original-home/); assert.match(profile, /\/Library\/Keychains/); assert.match(profile, /com.apple.securityd/);
});

test('QH-ISO-004 / QH-LIFE-001: timeout reaps child descendants and closes owned open streams', async () => {
  const owned = await workspace('teardown'); const entries = [];
  const ledger = async (entry) => { entries.push(entry); await owned.ledger(entry); };
  const result = await runBoundedProcess(process.execPath, ['-e', 'require("node:child_process").spawn(process.execPath,["-e","setInterval(()=>{},1000)"],{stdio:"inherit"});setInterval(()=>{},1000)'], { env: { PATH: '/usr/bin:/bin', HOME: owned.scratchDir }, cwd: owned.scratchDir, scratchDir: owned.scratchDir, ledger, deadlineMs: 100 });
  assert.equal(result.timedOut, true); assert(entries.some((entry) => entry.kind === 'process-group' && entry.state === 'active')); assert(entries.some((entry) => entry.kind === 'process-group' && entry.state === 'removed'));
  const deadline = Date.now() + 1500;
  let gone = false;
  while (Date.now() < deadline) {
    try { process.kill(-result.pid, 0); } catch (error) { if (error.code === 'ESRCH') { gone = true; break; } throw error; }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.equal(gone, true, 'owned process group must disappear within teardown bound');
  const replay = await startReplayServer({ ...owned, ledger, fixture: fixture() });
  const socket = connectTcp(replay.port, '127.0.0.1');
  await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
  const disconnected = new Promise((resolve) => socket.once('close', resolve));
  await replay.close(); await disconnected;
  assert(entries.some((entry) => entry.kind === 'listener' && entry.state === 'removed'));
});

test('QH-WIRE-003: omitted request/response fields fail before replay', () => {
  const value = fixture(); delete value.response;
  assert.throws(() => validateReplayFixture(value), /QHARNESS_INVALID_FIXTURE/);
});

test('QH-EVD-001: successful replay cannot hide a rejected evidence sink during close', async () => {
  const owned = await workspace('sink'); const value = fixture();
  const replay = await startReplayServer({ ...owned, fixture: value, sentinels: ['captured'] });
  try {
    assert.equal((await http(replay, value)).status, 200);
    await assert.rejects(replay.close(), { code: 'QHARNESS_WIRE_PERSISTENCE_REJECTED' });
  } finally { await replay.close(); }
});

test('QH-EVD-001: safe startup diagnostics retain category but never stderr content', () => {
  const result = { stdout: Buffer.alloc(0), stderr: Buffer.from('unknown flag: QHARNESS_PRIVATE_DIAGNOSTIC'), exitCode: 1 };
  assert.equal(classifyConsumerFailure(result), 'QHARNESS_CLIENT_ARGUMENT_REJECTED');
  assert.equal(classifyConsumerFailure({ ...result, stderr: Buffer.from('Authentication required') }), 'QHARNESS_SYNTHETIC_AUTH_MODE_INACTIVE');
  assert.equal(classifyConsumerFailure({ ...result, stderr: Buffer.from('operation not permitted') }), 'QHARNESS_CLIENT_OPERATION_DENIED');
});

test('QH-WIRE-002: signed synthetic target maps to owned loopback without contacting its authority', async () => {
  const owned = await workspace('signed'); const value = fixture([]);
  value.request.method = 'PUT'; value.request.path = 'https://blob.synthetic.invalid/upload?sig=synthetic-signature&part=one&part=two';
  value.response.status = 204;
  const result = await runHermeticFixture({ ...owned, fixture: value });
  assert.equal(result.status, 'PASS'); assert.match(result.target, /^https:\/\/blob\.synthetic\.invalid\/upload/);
  assert(!result.target.includes('synthetic-signature')); assert.equal(result.providerAcceptance, 'NOT-RUN');
});

test('QH-EVD-001: mixed redaction carriers and descriptor candidates never become protocol acceptance', async () => {
  const owned = await workspace('candidate');
  const mixed = fixture([{ kind: 'json', data: {} }, { kind: 'ws-text', data: {} }]);
  const result = await runHermeticFixture({ ...owned, fixture: mixed });
  assert.equal(result.status, 'DEFERRED'); assert.equal(result.classification, 'redaction-carrier-only');
  assert.equal(result.providerAcceptance, 'NOT-RUN'); assert(result.nextProof);
  await assert.rejects(startReplayServer({ ...owned, fixture: mixed }), { code: 'QHARNESS_MIXED_TRANSPORT_CARRIER' });
  const candidate = fixture(); candidate.provenance = 'descriptor-only';
  const replayed = await runHermeticFixture({ ...owned, fixture: candidate });
  assert.equal(replayed.status, 'DEFERRED'); assert.equal(replayed.replayFidelity, 'PASS'); assert.equal(replayed.disposition, 'deferred-reachability');
});

test('QH-EVD-001: successful consumers have no warning-derived failure reason; denied operations expose only closed categories', () => {
  assert.equal(classifyConsumerFailure({ stdout: Buffer.alloc(0), stderr: Buffer.from('invalid settings warning'), exitCode: 0 }), null);
  const stdout = Buffer.alloc(0);
  assert.deepEqual(classifyDeniedOperations({ stdout, stderr: Buffer.from('listen unix /QHARNESS_PRIVATE: bind: operation not permitted') }), [
    { category: 'network', operation: 'listen-unix', scope: 'unspecified' },
  ]);
  assert.deepEqual(classifyDeniedOperations({ stdout, stderr: Buffer.from('mkdir /synthetic/scratch/private: permission denied'), scratchDir: '/synthetic/scratch' }), [
    { category: 'filesystem', operation: 'mkdir', scope: 'owned-scratch' },
  ]);
  assert.deepEqual(classifyDeniedOperations({ stdout, stderr: Buffer.from('com.apple.securityd operation not permitted') }), [
    { category: 'keychain', operation: 'keychain-access', scope: 'unspecified' },
  ]);
});

test('QH-ISO-001/002: denied-line discriminator separates bind from file/auth hypotheses without retaining text', () => {
  const signals = classifyDenialSignals({
    stderr: Buffer.from('listen tcp 127.0.0.1:0: bind: operation not permitted\nmkdir /private/tmp/synthetic/test: permission denied'),
    stdout: Buffer.alloc(0), scratchDir: '/private/tmp/synthetic', originalHome: '/synthetic/original-home',
  });
  assert.deepEqual(signals, {
    listenOrBind: true, dialOrConnect: false, createOrWrite: true, readOrStat: false,
    execOrSpawn: false, keychainMentioned: false, scratchMentioned: true, originalHomeMentioned: false,
    executableMentions: [], nestedSandboxMentioned: false, sandboxApplyMentioned: false, errnos: ['EPERM', 'EACCES'],
    executableWithinOriginalHome: false, executableWithinNixStore: false,
    listenEndpoints: [{ transport: 'tcp', tcpLoopback: true, port: null, ephemeralRequested: true, unixWithinScratch: false }],
  });
});

test('QH-ISO-001/002: nested Seatbelt diagnostics expose executable whitelist and sandbox_apply without raw stderr', () => {
  const signals = classifyDenialSignals({
    stderr: Buffer.from('failed to start sandbox-exec: sandbox_apply: Operation not permitted'),
    stdout: Buffer.alloc(0),
  });
  assert.deepEqual(signals.executableMentions, ['sandbox-exec']);
  assert.equal(signals.nestedSandboxMentioned, true);
  assert.equal(signals.sandboxApplyMentioned, true);
  assert.deepEqual(signals.errnos, ['EPERM']);
  assert.deepEqual(classifyDeniedOperations({ stderr: Buffer.from('failed to start sandbox-exec: sandbox_apply: Operation not permitted'), stdout: Buffer.alloc(0) }), [
    { category: 'sandbox', operation: 'sandbox-apply', scope: 'unspecified' },
  ]);
  assert(!JSON.stringify(signals).includes('failed to start'));
});

test('QH-ISO-001: unknown denied executable exposes only other and storage-scope booleans', () => {
  const signals = classifyDenialSignals({
    stderr: Buffer.from('fork/exec /nix/store/synthetic-package/bin/private-helper: operation not permitted'),
    stdout: Buffer.alloc(0), originalHome: '/synthetic/original-home',
  });
  assert.deepEqual(signals.executableMentions, ['other']);
  assert.equal(signals.executableWithinNixStore, true);
  assert.equal(signals.executableWithinOriginalHome, false);
  assert(!JSON.stringify(signals).includes('private-helper'));
  assert(!JSON.stringify(signals).includes('/nix/store/'));
});

test('QH-ISO-001/002: denied listener endpoints reveal loopback ports but never arbitrary hostnames or unix paths', () => {
  const signals = classifyDenialSignals({
    stderr: Buffer.from('listen tcp 127.0.0.1:43210: bind: operation not permitted\nlisten tcp private-sensitive-host:4444: operation not permitted\nlisten unix /synthetic/scratch/private.sock: permission denied'),
    stdout: Buffer.alloc(0), scratchDir: '/synthetic/scratch',
  });
  assert.deepEqual(signals.listenEndpoints, [
    { transport: 'tcp', tcpLoopback: true, port: 43210, ephemeralRequested: false, unixWithinScratch: false },
    { transport: 'tcp', tcpLoopback: false, port: null, ephemeralRequested: false, unixWithinScratch: false },
    { transport: 'unix', tcpLoopback: false, port: null, ephemeralRequested: false, unixWithinScratch: true },
  ]);
  assert(!JSON.stringify(signals).includes('private-sensitive-host'));
  assert(!JSON.stringify(signals).includes('private.sock'));
});

test('QH-ISO-002: agy local-listener grant permits loopback bind/inbound, never arbitrary outbound', () => {
  const inputs = { scratchDir: '/private/tmp/synthetic', originalHome: '/synthetic/original-home', deniedProbe: '/private/tmp/synthetic/denied', recorderPort: 12345 };
  assert(!buildSandboxProfile(inputs).includes('allow network-bind'));
  assert(!buildSandboxProfile(inputs).includes('allow network-inbound'));
  const profile = buildSandboxProfile({ ...inputs, allowEphemeralBind: true });
  assert(profile.includes('(allow network-bind (local ip "localhost:*"))'));
  assert(profile.includes('(allow network-outbound (remote ip "localhost:12345"))'));
  assert(profile.includes('(allow network-inbound (local ip "localhost:*"))'));
  assert(!profile.includes('(allow network-outbound (remote ip "localhost:*"))'));
  assert(profile.includes('com.apple.securityd'));
  assert(profile.includes('/synthetic/original-home'));
});

test('QH-WIRE-002: physical websocket continuation and FIN bits survive replay encoding', () => {
  const encoded = websocketReplayFrames([
    { kind: 'ws-text', data: '{"safe":', final: false, messageId: 'synthetic-fragment', chunkSizes: [1, 2] },
    { kind: 'ws-continuation', data: 'true}', opcode: 0, final: true, messageId: 'synthetic-fragment', chunkSizes: [2, 1] },
  ]);
  assert.equal(encoded.length, 2);
  assert.equal(encoded[0].bytes[0], 0x01);
  assert.equal(encoded[1].bytes[0], 0x80);
  assert.deepEqual(encoded.map((frame) => frame.chunkSizes), [[1, 2], [2, 1]]);
  assert.equal(encoded.filter((frame) => frame.opcode < 8 && frame.fin).length, 1);
});

test('QH-WIRE-002: sanitized physical websocket fragments decode as one real WebSocket message', async () => {
  const owned = await workspace('ws-fragments');
  const value = fixture([
    { kind: 'ws-text', data: '{"safe":', final: false, messageId: 'synthetic-fragment', chunkSizes: [1, 2] },
    { kind: 'ws-continuation', data: 'true}', opcode: 0, final: true, messageId: 'synthetic-fragment', chunkSizes: [2, 1] },
  ]);
  value.request.method = 'GET'; value.response.status = 101; value.response.headers = [];
  const result = await runHermeticFixture({ ...owned, fixture: value });
  assert.equal(result.status, 'PASS');
  assert.equal(result.bytesMatch, true);
  assert.equal(result.consumerAcceptance, 'NOT-RUN');
});

test('QH-WIRE-001: gateway Bearer recipe is explicit, marker-free, and disjoint from native API-key settings', () => {
  const consumer = { mode: 'agy-print', transport: 'sse', marker: 'AGY159_SYNTHETIC_GATEWAY_BEARER_OK', authMode: 'gateway-bearer', model: 'gemini-2.0-flash', modelSource: 'AGY_LLM_GATEWAY_MODELS' };
  const launch = buildAgyLaunch({ consumer, origin: 'http://127.0.0.1:12345' });
  assert.equal(launch.settings, null);
  assert.equal(launch.authMode, 'gateway-bearer');
  assert.deepEqual(launch.environment, {
    AGY_CLI_HIDE_LOGO: '1', AGY_CLI_HIDE_ACCOUNT_INFO: '1',
    AGY_LLM_GATEWAY_URL: 'http://127.0.0.1:12345',
    AGY_LLM_GATEWAY_API_KEY: 'synthetic-gateway-api-key',
    AGY_LLM_GATEWAY_MODELS: 'gemini-2.0-flash,gemini-2.5-pro',
  });
  assert(!launch.argv.includes('--model'));
  assert(!launch.argv.includes('--settings'));
  assert.deepEqual(launch.argv.slice(-2), ['--output-format', 'json']);
  assert(!JSON.stringify(launch).includes(consumer.marker));
  const native = buildAgyLaunch({ consumer: { mode: 'agy-print', transport: 'sse' }, origin: 'http://127.0.0.1:12345' });
  assert.equal(native.environment.GEMINI_API_KEY, 'qharness-synthetic-local-only');
  assert.equal(native.environment.GOOGLE_GEMINI_BASE_URL, 'http://127.0.0.1:12345');
  assert.equal(native.settings.security.auth.selectedType, 'gemini-api-key');
  assert(!('AGY_LLM_GATEWAY_URL' in native.environment));
  assert.throws(() => buildAgyLaunch({ consumer: { ...consumer, authMode: 'unknown' }, origin: 'http://127.0.0.1:12345' }), { code: 'QHARNESS_AGY_AUTH_MODE_UNSUPPORTED' });
  assert.throws(() => buildAgyLaunch({ consumer: { ...consumer, modelSource: 'unknown' }, origin: 'http://127.0.0.1:12345' }), { code: 'QHARNESS_AGY_GATEWAY_MODEL_CONTRACT' });
  assert.throws(() => buildAgyLaunch({ consumer, origin: 'https://provider.invalid' }), { code: 'QHARNESS_AGY_REPLAY_ORIGIN_INVALID' });
});

test('QH-WIRE-001: gateway consumer proof requires Bearer-only wire auth and decoded standalone SUCCESS JSON', () => {
  const headers = [['authorization', 'Bearer synthetic-gateway-api-key'], ['content-type', 'application/json']];
  assert.equal(matchesSyntheticGatewayAuth(headers), true);
  assert.equal(matchesSyntheticGatewayAuth([...headers, ['x-goog-api-key', 'synthetic-other']]), false);
  assert.equal(matchesSyntheticGatewayAuth([...headers, headers[0]]), false);
  assert.equal(matchesSyntheticGatewayAuth([['authorization', 'Bearer synthetic-wrong-key']]), false);
  const marker = 'AGY159_SYNTHETIC_GATEWAY_BEARER_OK';
  assert.deepEqual(observeDecodedConsumerOutput({ client: 'agy', stdout: Buffer.from(JSON.stringify({ status: 'SUCCESS', response: marker }, null, 2)), marker }), { markerObserved: true, completed: true });
  assert.deepEqual(observeDecodedConsumerOutput({ client: 'agy', stdout: Buffer.from(JSON.stringify({ status: 'ERROR', response: marker })), marker }), { markerObserved: false, completed: false });
});
