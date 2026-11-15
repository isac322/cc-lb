#!/usr/bin/env node
import { request as httpRequest } from 'node:http';
import { connect as connectHttp2 } from 'node:http2';
import { mkdir, mkdtemp, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { fixtures as codexFixtures } from '../codex169/fixtures.mjs';
import { fixtures as agyFixtures } from '../agy159/fixtures.mjs';
import { createRedactionContext, sanitizeRecord, assertSafeArtifact } from './redaction.mjs';
import { classifyFixtureTransport, createResourceLedger, encodeReplayFrames, isInterruptedTermination, localReplayPath, startReplayServer, runPinnedConsumer, preflightIsolation, validateReplayFixture, websocketReplayFrames } from './recorder-replay.mjs';

const ALL_FIXTURES = [...codexFixtures, ...agyFixtures];

function argumentsOf(argv) {
  const result = { mode: 'fixtures', scratch: process.env.CC_LB_QA_OUTPUT, fixture: null, client: null };
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i]; const value = argv[i + 1];
    if (!['--mode', '--scratch', '--fixture', '--client'].includes(key) || !value || value.startsWith('--')) throw new Error('QHARNESS_INVALID_ARGUMENT');
    result[key.slice(2)] = value;
  }
  if (!['fixtures', 'consumers', 'isolation'].includes(result.mode) || !result.scratch || (result.client && !['codex', 'agy'].includes(result.client))) throw new Error('QHARNESS_INVALID_ARGUMENT');
  return result;
}

async function observeHttp(fixture, replay) {
  return new Promise((resolve) => {
    const isGrpc = fixture.response.frames.some((frame) => ['grpc', 'grpc-message'].includes(frame.kind));
    const client = isGrpc ? connectHttp2(replay.origin) : null;
    const chunks = []; let settled = false; let stream;
    const timer = setTimeout(() => finish(false, true), 5000);
    function finish(complete, timedOut = false) {
      if (settled) return; settled = true; clearTimeout(timer);
      stream?.destroy(); client?.destroy();
      resolve({ complete, timedOut, bytes: Buffer.concat(chunks) });
    }
    if (client) {
      client.on('error', () => finish(false));
      stream = client.request({ ':method': fixture.request.method, ':path': localReplayPath(fixture.request.path) });
      stream.on('data', (chunk) => chunks.push(chunk)); stream.on('end', () => finish(true)); stream.on('error', () => finish(false)); stream.on('aborted', () => finish(false));
    } else {
      const headers = fixture.request.headers.some(([name]) => name.toLowerCase() === 'host')
        ? fixture.request.headers : [['Host', `127.0.0.1:${replay.port}`], ...fixture.request.headers];
      stream = httpRequest({ hostname: '127.0.0.1', port: replay.port, path: localReplayPath(fixture.request.path), method: fixture.request.method, headers: headers.flat() }, (response) => {
        response.on('data', (chunk) => chunks.push(chunk)); response.on('end', () => finish(true)); response.on('aborted', () => finish(false)); response.on('error', () => finish(false));
      });
      stream.on('error', () => finish(false));
    }
    const body = fixture.request.body;
    stream.end(body === undefined || body === null ? undefined : typeof body === 'string' ? body : JSON.stringify(body));
  });
}

async function observeWs(fixture, replay) {
  return new Promise((resolve) => {
    const client = new WebSocket(`${replay.origin.replace(/^http/, 'ws')}${localReplayPath(fixture.request.path)}`);
    const chunks = []; let settled = false;
    const expectedFrames = websocketReplayFrames(fixture.response.frames).filter((frame) => frame.opcode < 8 && frame.fin).length;
    const timer = setTimeout(() => finish(false, true), 5000);
    function finish(complete, timedOut = false) {
      if (settled) return; settled = true; clearTimeout(timer); client.close();
      resolve({ complete, timedOut, bytes: Buffer.concat(chunks) });
    }
    client.binaryType = 'arraybuffer';
    client.addEventListener('open', () => client.send(JSON.stringify(fixture.request.body ?? { type: 'response.create' })));
    client.addEventListener('message', (event) => {
      chunks.push(typeof event.data === 'string' ? Buffer.from(event.data) : Buffer.from(event.data));
      if (chunks.length === expectedFrames) finish(true);
    });
    client.addEventListener('error', () => finish(false));
    client.addEventListener('close', () => finish(chunks.length === expectedFrames));
  });
}

export async function runHermeticFixture({ fixture, scratchDir, ledger }) {
  validateReplayFixture(fixture);
  const transport = classifyFixtureTransport(fixture);
  if (!transport.replayable) return { fixtureId: fixture.id, ...transport, status: 'DEFERRED', fixtureEvidence: fixture.provenance, consumerAcceptance: 'NOT-RUN', providerAcceptance: 'NOT-RUN', runtimeAcceptance: 'NOT-RUN', fullClientAcceptance: false };
  const replay = await startReplayServer({ fixture, scratchDir, ledger });
  try {
    const sanitized = replay.fixture;
    const websocket = sanitized.response.frames.some((frame) => frame.kind.startsWith('ws-'));
    const observation = websocket ? await observeWs(sanitized, replay) : await observeHttp(sanitized, replay);
    const termination = typeof sanitized.response.termination === 'string' ? sanitized.response.termination : sanitized.response.termination?.kind;
    const interrupted = isInterruptedTermination(termination) || termination === 'close:1006';
    const expected = encodeReplayFrames(sanitized, { websocket });
    const bytesMatch = observation.bytes.equals(expected);
    const fidelityPassed = !observation.timedOut && (bytesMatch || (interrupted && !observation.complete && observation.bytes.length > 0 && expected.subarray(0, observation.bytes.length).equals(observation.bytes)));
    return {
      fixtureId: fixture.id,
      classification: 'synthetic-replay-fidelity-only',
      status: !fidelityPassed ? 'FAIL' : fixture.provenance === 'descriptor-only' ? 'DEFERRED' : 'PASS',
      replayFidelity: fidelityPassed ? 'PASS' : 'FAIL',
      fixtureEvidence: fixture.provenance,
      disposition: fixture.provenance === 'descriptor-only' ? 'deferred-reachability' : 'synthetic-replay-only',
      nextProof: fixture.provenance === 'descriptor-only' ? 'Feature owner must demonstrate the pinned consumer action and decoder for this descriptor candidate.' : null,
      target: sanitized.request.path,
      bytesMatch, interrupted, complete: observation.complete,
      consumerAcceptance: 'NOT-RUN', providerAcceptance: 'NOT-RUN', runtimeAcceptance: 'NOT-RUN', fullClientAcceptance: false,
    };
  } finally { await replay.close(); }
}

export async function main(argv = process.argv.slice(2)) {
  const args = argumentsOf(argv);
  const selected = ALL_FIXTURES.filter((fixture) => (!args.fixture || fixture.id === args.fixture) && (!args.client || fixture.client.name === args.client));
  if (!selected.length) throw new Error('QHARNESS_UNKNOWN_FIXTURE');
  await mkdir(args.scratch, { recursive: true, mode: 0o700 });
  const root = await mkdtemp(path.join(args.scratch, 'recorder-run-'));
  const ledger = createResourceLedger(root);
  await ledger({ kind: 'scratch-directory', identifier: root, delete_command: `rm -rf -- '${root.replaceAll("'", "'\\''")}'`, state: 'retained' });
  const cases = [];
  for (const fixture of selected) {
    if (args.mode === 'consumers' && !fixture.expected?.consumer) continue;
    const scratchDir = path.join(root, fixture.id);
    if (args.mode === 'consumers') cases.push(await runPinnedConsumer({ fixture, scratchDir, ledger }));
    else if (args.mode === 'fixtures') cases.push(await runHermeticFixture({ fixture, scratchDir, ledger }));
    else {
      const replay = await startReplayServer({ fixture, scratchDir, ledger });
      try { cases.push({ fixtureId: fixture.id, ...await preflightIsolation({ scratchDir, recorderPort: replay.port, ledger, allowEphemeralBind: fixture.client.name === 'agy' }), providerAcceptance: 'NOT-RUN' }); }
      finally { await replay.close(); }
      break;
    }
  }
  if (!cases.length) throw new Error('QHARNESS_NO_SUPPORTED_CONSUMER_CASES');
  const summary = {
    issue: 1324, mode: args.mode, cases,
    status: cases.some((entry) => entry.status === 'FAIL') ? 'FAIL' : cases.some((entry) => entry.status === 'BLOCKED') ? 'BLOCKED' : 'PASS',
    deferredCases: cases.filter((entry) => entry.status === 'DEFERRED').length,
    scope: 'harness-checks-only',
    providerAcceptance: 'NOT-RUN', runtimeAcceptance: 'NOT-RUN', fullClientAcceptance: false,
  };
  const safe = sanitizeRecord({ provenance: 'synthetic', ...summary }, createRedactionContext({ scenarioId: 'run-summary', policyVersion: 'qharness-v1' }));
  const bytes = Buffer.from(`${JSON.stringify(safe)}\n`); assertSafeArtifact(bytes, { sentinels: [], allowedSyntheticTokens: [] });
  await writeFile(path.join(root, 'summary.json'), bytes, { mode: 0o600 });
  process.stdout.write(bytes);
  return summary;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().then((summary) => { if (summary.status !== 'PASS') process.exitCode = 1; }).catch(() => {
    process.stderr.write('QHARNESS_RUN_FAILED\n'); process.exitCode = 1;
  });
}
