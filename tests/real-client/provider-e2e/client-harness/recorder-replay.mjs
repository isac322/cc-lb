import { createServer as createHttpServer } from 'node:http';
import { createServer as createHttp2Server } from 'node:http2';
import { createServer as createTcpServer } from 'node:net';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { appendFile, chmod, mkdir, readFile, realpath, writeFile } from 'node:fs/promises';
import { existsSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { createRedactionContext, sanitizeRecord, assertSafeArtifact } from './redaction.mjs';

const MAX_BYTES = 2 * 1024 * 1024;
const MAX_REQUESTS = 32;
const SAFE_ERROR = 'QHARNESS_OPERATION_FAILED';
const PINS = Object.freeze({
  codex: { version: '0.169.0', revision: 'd32063f96c2dff1dc96af839b3952abb6acfeda4', sha256: '434603625edf4fd28af9c123a8b43ed569ef8a3c6a771eed8b656c04ed230b84' },
  agy: { version: '1.5.9', sha256: 'fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a' },
});
const pairHeaders = (raw = []) => Array.from({ length: raw.length / 2 }, (_, i) => [raw[i * 2], raw[i * 2 + 1]]);
const fail = (code) => Object.assign(new Error(code), { code });
const terminalKind = (value) => typeof value === 'string' ? value : value?.kind ?? 'complete';
const shellQuote = (value) => `'${String(value).replaceAll("'", "'\\''")}'`;
export const isInterruptedTermination = (value) => ['partial', 'cancel', 'reset', 'abort', 'abrupt-eof', 'client-cancel', 'connection-reset'].includes(terminalKind(value));

export function createResourceLedger(scratchDir, ledgerPath = process.env.CC_LB_QA_LEDGER) {
  const context = createRedactionContext({ scenarioId: 'resource-ledger', policyVersion: 'qharness-v1' });
  return async (entry) => {
    const file = ledgerPath ?? path.join(scratchDir, 'resources.jsonl');
    await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
    await safeWrite(file, { provenance: 'synthetic', created_utc: new Date().toISOString(), ...entry }, context, [], true);
  };
}

export function evaluateHarnessOwner(actual, bound) {
  const match = bound?.available !== false && ['principal', 'provider', 'kind', 'owner', 'epoch'].every((key) =>
    typeof actual?.[key] === 'string' && actual[key] === bound?.[key]);
  return { allowed: match, classification: 'harness-owner-model-only', runtimeAcceptance: 'NOT-RUN' };
}

export function observeDecodedConsumerOutput({ client, stdout, marker }) {
  let markerObserved = false;
  let completed = false;
  const text = stdout.toString('utf8').trim();
  let events;
  try { events = [JSON.parse(text)]; }
  catch { events = text.split('\n').flatMap((line) => { try { return [JSON.parse(line)]; } catch { return []; } }); }
  for (const event of events) {
    if (!event || typeof event !== 'object') continue;
    if (client === 'codex') {
      if (event.type === 'item.completed' && event.item?.type === 'agent_message' && typeof event.item.text === 'string') markerObserved ||= event.item.text.includes(marker);
      if (event.type === 'turn.completed') completed = true;
    } else if (client === 'agy') {
      const result = event.event === 'result' ? event.result : event;
      if (result?.status === 'SUCCESS' && typeof result.response === 'string') {
        markerObserved ||= result.response.includes(marker);
        completed = true;
      }
    }
  }
  return { markerObserved, completed };
}

export function classifyConsumerFailure({ stderr, stdout, exitCode, timedOut, overflow, error }) {
  if (error) return error;
  if (timedOut) return 'QHARNESS_CLIENT_DEADLINE';
  if (overflow) return 'QHARNESS_CLIENT_OUTPUT_LIMIT';
  if (exitCode === 0) return null;
  const message = `${stderr.toString('utf8')}\n${stdout.toString('utf8')}`;
  if (/unknown (?:flag|option)|flag provided but not defined|unexpected argument|unrecognized (?:option|argument)|invalid value .*--/i.test(message)) return 'QHARNESS_CLIENT_ARGUMENT_REJECTED';
  if (/Authentication required|Please visit the URL to log in|not logged in|login required/i.test(message)) return 'QHARNESS_SYNTHETIC_AUTH_MODE_INACTIVE';
  if (/operation not permitted|permission denied|EPERM|EACCES/i.test(message)) return 'QHARNESS_CLIENT_OPERATION_DENIED';
  if (/invalid stream chunk|stream closed before|SSE|failed to parse|JSON parse|invalid character/i.test(message)) return 'QHARNESS_CLIENT_STREAM_REJECTED';
  if (/invalid.*(?:config|settings)|failed.*(?:config|settings)|model.*(?:not found|unsupported|unknown)/i.test(message)) return 'QHARNESS_CLIENT_CONFIGURATION_REJECTED';
  return exitCode === 0 ? null : 'QHARNESS_CLIENT_EXIT_FAILURE';
}

export function classifyDeniedOperations({ stderr, stdout, scratchDir, originalHome = homedir() }) {
  const diagnostics = [];
  for (const line of `${stderr.toString('utf8')}\n${stdout.toString('utf8')}`.split('\n')) {
    if (!/operation not permitted|permission denied|EPERM|EACCES/i.test(line)) continue;
    let category = 'unknown';
    let operation = 'unknown';
    if (/sandbox_apply|sandbox_init|sandbox_init_with_parameters/i.test(line)) { category = 'sandbox'; operation = 'sandbox-apply'; }
    else if (/keychain|securityd|SecItem|security framework/i.test(line)) { category = 'keychain'; operation = 'keychain-access'; }
    else if (/\b(?:posix_spawn|execve|fork|spawn)\b/i.test(line)) { category = 'process'; operation = 'spawn'; }
    else if (/\b(?:dial|connect|listen|bind|socket|lookup|getsockopt|setsockopt)\b/i.test(line)) {
      category = 'network'; operation = line.match(/\b(dial|connect|listen|bind|socket|lookup|getsockopt|setsockopt)\b/i)[1].toLowerCase();
      if (/\bunix\b/i.test(line)) operation += '-unix';
    } else if (/\b(?:open|mkdir|stat|lstat|readlink|read|write|rename|unlink|chmod|chown|access|create|getwd|getcwd)\b/i.test(line)) {
      category = 'filesystem'; operation = line.match(/\b(open|mkdir|stat|lstat|readlink|read|write|rename|unlink|chmod|chown|access|create|getwd|getcwd)\b/i)[1].toLowerCase();
    } else if (/rlimit|thread|semaphore|shared memory|mmap|mach|sysctl|resource/i.test(line)) { category = 'resource'; operation = 'resource-access'; }
    const scope = scratchDir && line.includes(scratchDir) ? 'owned-scratch'
      : originalHome && line.includes(originalHome) ? 'original-home'
        : /\/Library\/Keychains/i.test(line) ? 'system-keychain' : 'unspecified';
    const diagnostic = { category, operation, scope };
    if (!diagnostics.some((value) => JSON.stringify(value) === JSON.stringify(diagnostic))) diagnostics.push(diagnostic);
    if (diagnostics.length === 12) break;
  }
  return diagnostics;
}

export function classifyListenEndpoints({ stderr, stdout, scratchDir }) {
  const endpoints = [];
  for (const line of `${stderr.toString('utf8')}\n${stdout.toString('utf8')}`.split('\n')) {
    if (!/operation not permitted|permission denied|EPERM|EACCES/i.test(line)) continue;
    for (const match of line.matchAll(/\b(?:listen|bind)\s+(tcp[46]?|udp[46]?|unix(?:gram|packet)?)\s+([^\s"'\\]+)/gi)) {
      const transport = match[1].toLowerCase().startsWith('unix') ? 'unix' : match[1].toLowerCase().startsWith('tcp') ? 'tcp' : 'udp';
      const target = match[2].replace(/[,:;]+$/, '');
      const address = target.match(/^\[([^\]]+)\]:(\d+)$/) ?? target.match(/^(.*):(\d+)$/);
      const host = address?.[1] ?? '';
      const number = address ? Number(address[2]) : null;
      const validPort = Number.isInteger(number) && number >= 0 && number <= 65535;
      const ipv4Loopback = /^127(?:\.\d{1,3}){3}$/.test(host) && host.split('.').every((value) => Number(value) <= 255);
      const tcpLoopback = transport === 'tcp' && (ipv4Loopback || host === '::1' || host === 'localhost');
      const roots = scratchDir ? [scratchDir, scratchDir.replace(/^\/private\/tmp(?=\/)/, '/tmp')] : [];
      const endpoint = {
        transport, tcpLoopback,
        port: tcpLoopback && validPort && number !== 0 ? number : null,
        ephemeralRequested: validPort && number === 0,
        unixWithinScratch: transport === 'unix' && roots.some((root) => target.startsWith(`${root}/`)),
      };
      if (!endpoints.some((item) => JSON.stringify(item) === JSON.stringify(endpoint))) endpoints.push(endpoint);
    }
  }
  return endpoints;
}

export function classifyDenialSignals({ stderr, stdout, scratchDir, originalHome = homedir() }) {
  const denied = `${stderr.toString('utf8')}\n${stdout.toString('utf8')}`.split('\n')
    .filter((line) => /operation not permitted|permission denied|EPERM|EACCES/i.test(line)).join('\n');
  const executableMentions = [];
  let executableWithinOriginalHome = false;
  let executableWithinNixStore = false;
  for (const match of denied.matchAll(/(?:^|[/\s"'(])(sandbox-exec|security|sh|bash)(?=[:\s"')]|$)/gi)) {
    const name = match[1].toLowerCase();
    if (!executableMentions.includes(name)) executableMentions.push(name);
  }
  for (const match of denied.matchAll(/(?:^|\s)(?:fork\/exec|execve|posix_spawn|exec|failed to start|launch process)\s*[:(]?\s*["']?([^\s"'(),:]+)/gim)) {
    const candidate = match[1];
    const name = path.basename(candidate);
    executableWithinOriginalHome ||= Boolean(originalHome && candidate.startsWith(`${originalHome}/`));
    executableWithinNixStore ||= candidate.startsWith('/nix/store/');
    if (!['sandbox-exec', 'security', 'sh', 'bash'].includes(name) && !executableMentions.includes('other')) executableMentions.push('other');
  }
  const sandboxApplyMentioned = /sandbox_apply|sandbox_init|sandbox_init_with_parameters/i.test(denied);
  const errnos = [];
  if (/operation not permitted|\bEPERM\b/i.test(denied)) errnos.push('EPERM');
  if (/permission denied|\bEACCES\b/i.test(denied)) errnos.push('EACCES');
  return {
    listenOrBind: /\b(?:listen|bind)\b/i.test(denied),
    dialOrConnect: /\b(?:dial|connect|lookup|getsockopt|setsockopt)\b/i.test(denied),
    createOrWrite: /\b(?:mkdir|create|write|rename|unlink|chmod|chown)\b/i.test(denied),
    readOrStat: /\b(?:open|read|stat|lstat|readlink|access|getwd|getcwd)\b/i.test(denied),
    execOrSpawn: /\b(?:exec|execve|posix_spawn|fork|spawn)\b|failed to start|launch process/i.test(denied),
    keychainMentioned: /keychain|securityd|SecItem|security framework/i.test(denied),
    scratchMentioned: Boolean(scratchDir && (denied.includes(scratchDir) || denied.includes(scratchDir.replace(/^\/private\/tmp(?=\/)/, '/tmp')))),
    originalHomeMentioned: Boolean(originalHome && denied.includes(originalHome)),
    executableMentions,
    executableWithinOriginalHome,
    executableWithinNixStore,
    nestedSandboxMentioned: /sandbox-exec|sandbox_apply|sandbox_init|seatbelt/i.test(denied),
    sandboxApplyMentioned,
    errnos,
    listenEndpoints: classifyListenEndpoints({ stderr, stdout, scratchDir }),
  };
}

export function assertTerminalOrder(frames) {
  let terminal = false;
  for (const frame of frames) {
    if (!['sse', 'ws-text'].includes(frame.kind)) continue;
    const values = typeof frame.data === 'object' ? [frame.data] : frame.kind === 'sse'
      ? String(frame.data).split(/\r?\n\r?\n/).map((block) => block.split(/\r?\n/).filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart()).join('\n'))
      : [frame.data];
    for (let value of values) {
      if (typeof value === 'string') { try { value = JSON.parse(value); } catch { continue; } }
      if (!value || typeof value.type !== 'string') continue;
      if (terminal) throw fail('QHARNESS_POST_TERMINAL_DATA');
      if (['response.completed', 'response.failed', 'response.incomplete', 'error'].includes(value.type)) terminal = true;
    }
  }
}

export function localReplayPath(target) {
  if (typeof target !== 'string' || (!target.startsWith('/') && !/^https?:\/\//.test(target)) || target.startsWith('//')) throw fail('QHARNESS_INVALID_TARGET');
  const url = new URL(target, 'http://recorder.invalid');
  return `${url.pathname}${url.search}`;
}

export function classifyFixtureTransport(fixture) {
  const families = new Set(fixture.response.frames.map((frame) => frame.kind.startsWith('ws-') ? 'ws'
    : frame.kind.startsWith('grpc') ? 'grpc' : frame.kind === 'audio' ? 'audio' : 'http'));
  return families.size > 1
    ? { replayable: false, classification: 'redaction-carrier-only', reason: 'QHARNESS_MIXED_TRANSPORT_CARRIER', nextProof: 'Run redaction assertions separately for each carrier; this is not a protocol stream.' }
    : { replayable: true, family: [...families][0] ?? 'http' };
}

export function validateReplayFixture(fixture) {
  if (!fixture || !/^[A-Za-z0-9][A-Za-z0-9._:-]+$/.test(fixture.id ?? '') ||
      !['synthetic', 'descriptor-only'].includes(fixture.provenance) ||
      !fixture.request || typeof fixture.request.path !== 'string' || (!fixture.request.path.startsWith('/') && !/^https?:\/\//.test(fixture.request.path)) || fixture.request.path.startsWith('//') ||
      !fixture.response || !Number.isInteger(fixture.response.status) || fixture.response.status < 100 || fixture.response.status > 599 ||
      !Array.isArray(fixture.response.frames) || !Array.isArray(fixture.request.headers) || !Array.isArray(fixture.response.headers)) {
    throw fail('QHARNESS_INVALID_FIXTURE');
  }
  for (const pairs of [fixture.request.headers, fixture.response.headers]) {
    if (pairs.some((pair) => !Array.isArray(pair) || pair.length !== 2 || pair.some((v) => typeof v !== 'string' || /[\r\n]/.test(v)))) throw fail('QHARNESS_INVALID_HEADERS');
  }
  if (fixture.response.frames.length > 4096 || Buffer.byteLength(JSON.stringify(fixture)) > MAX_BYTES) throw fail('QHARNESS_FIXTURE_TOO_LARGE');
  assertTerminalOrder(fixture.response.frames);
  localReplayPath(fixture.request.path);
  return fixture;
}

async function safeWrite(file, record, context, sentinels = [], append = false) {
  let safe;
  try { safe = sanitizeRecord(record, context); } catch { throw fail('QHARNESS_RECORD_REDACTION_FAILED'); }
  const bytes = Buffer.from(`${JSON.stringify(safe)}\n`);
  try { assertSafeArtifact(bytes, { sentinels, allowedSyntheticTokens: [] }); } catch { throw fail('QHARNESS_ARTIFACT_REJECTED'); }
  try {
    if (append) await appendFile(file, bytes, { mode: 0o600 });
    else await writeFile(file, bytes, { mode: 0o600 });
    await chmod(file, 0o600);
  } catch { throw fail('QHARNESS_ARTIFACT_WRITE_FAILED'); }
  return safe;
}

async function resource(ledger, scratchDir, entry) {
  const record = { provenance: 'synthetic', created_utc: new Date().toISOString(), ...entry };
  if (ledger) await ledger(record);
  else await safeWrite(path.join(scratchDir, 'resources.jsonl'), record,
    createRedactionContext({ scenarioId: 'resources', policyVersion: 'qharness-v1' }), [], true);
}

function frameBytes(frame) {
  const data = frame.data;
  let bytes;
  if (typeof data === 'string') bytes = Buffer.from(data, frame.encoding === 'base64' ? 'base64' : 'utf8');
  else if (Array.isArray(data) && data.every((v) => Number.isInteger(v) && v >= 0 && v <= 255)) bytes = Buffer.from(data);
  else if (data && typeof data === 'object' && Array.isArray(data.bytes)) bytes = Buffer.from(data.bytes);
  else bytes = Buffer.from(JSON.stringify(data ?? null));
  if (frame.kind === 'grpc-message') {
    const prefix = Buffer.alloc(5);
    prefix[0] = frame.compressed ? 1 : 0;
    prefix.writeUInt32BE(bytes.length, 1);
    return Buffer.concat([prefix, bytes]);
  }
  return bytes;
}

export function encodeReplayFrames(fixture, { websocket = false } = {}) {
  return Buffer.concat(fixture.response.frames
    .filter((frame) => !websocket || !['ws-ping', 'ws-pong', 'ws-close'].includes(frame.kind))
    .map(frameBytes));
}

function websocketFrame(opcode, bytes, fin = true) {
  const first = (fin ? 0x80 : 0) | opcode;
  let header;
  if (bytes.length < 126) header = Buffer.from([first, bytes.length]);
  else if (bytes.length <= 65535) {
    header = Buffer.alloc(4); header[0] = first; header[1] = 126; header.writeUInt16BE(bytes.length, 2);
  } else {
    header = Buffer.alloc(10); header[0] = first; header[1] = 127; header.writeBigUInt64BE(BigInt(bytes.length), 2);
  }
  return Buffer.concat([header, bytes]);
}

export function websocketReplayFrames(frames) {
  let fragmented = false;
  return frames.map((frame) => {
    const controlOpcode = frame.kind === 'ws-ping' ? 9 : frame.kind === 'ws-pong' ? 10 : frame.kind === 'ws-close' ? 8 : null;
    const opcode = frame.opcode ?? controlOpcode ?? (frame.kind === 'ws-continuation' || frame.continuation === true || fragmented ? 0 : frame.kind === 'ws-binary' ? 2 : 1);
    if (![0, 1, 2, 8, 9, 10].includes(opcode)) throw fail('QHARNESS_INVALID_WS_OPCODE');
    const fin = frame.fin ?? frame.final ?? !(frame.more === true || frame.complete === false);
    if (opcode < 8) fragmented = !fin;
    return { opcode, fin, bytes: websocketFrame(opcode, frameBytes(frame), fin), chunkSizes: frame.chunkSizes ?? frame.chunk_sizes };
  });
}

function writeFragments(stream, bytes, sizes) {
  if (!Array.isArray(sizes) || !sizes.length) { stream.write(bytes); return; }
  let offset = 0;
  for (const size of sizes) {
    if (!Number.isInteger(size) || size < 1) throw fail('QHARNESS_INVALID_FRAGMENT');
    if (offset >= bytes.length) break;
    stream.write(bytes.subarray(offset, offset + size)); offset += size;
  }
  if (offset < bytes.length) stream.write(bytes.subarray(offset));
}

/** Owned replay endpoint only: this module never makes a provider/upstream request. */
export async function startReplayServer({ fixture, scratchDir, ledger, authorize = () => true, hooks = {}, sentinels = [] }) {
  validateReplayFixture(fixture);
  if (!classifyFixtureTransport(fixture).replayable) throw fail('QHARNESS_MIXED_TRANSPORT_CARRIER');
  await mkdir(scratchDir, { recursive: true, mode: 0o700 });
  const context = createRedactionContext({ scenarioId: fixture.id, policyVersion: 'qharness-v1' });
  // Every payload is authored synthetic data, even when its schema provenance is descriptor-only.
  // Preserve the original provenance in the retained fixture, not a false consumer/live claim.
  const safeFixture = sanitizeRecord({ provenance: 'synthetic', fixture }, context).fixture;
  if (!safeFixture) throw fail('QHARNESS_SANITIZED_FIXTURE_UNAVAILABLE');
  assertSafeArtifact(Buffer.from(JSON.stringify(safeFixture)), { sentinels, allowedSyntheticTokens: [] });
  const observations = [];
  const sockets = new Set();
  const sessions = new Set();
  let requests = 0;
  let closed = false;
  let writes = Promise.resolve();
  const stats = { bodyBytesRead: 0, bodyReadCalls: 0, ownerLookups: 0, quotaLookups: 0, signerCalls: 0, providerCalls: 0, requestTargets: [] };
  const observe = (record) => {
    const safe = sanitizeRecord({ provenance: 'captured', ...record }, context);
    observations.push(safe);
    writes = writes.then(() => safeWrite(path.join(scratchDir, 'wire.jsonl'), { provenance: 'captured', ...record }, context, sentinels, true));
    // Surface write errors via close(), without an unhandled rejection during request delivery.
    writes.catch(() => {});
  };
  const isGrpc = safeFixture.response.frames.some((frame) => frame.kind === 'grpc-message' || frame.kind === 'grpc');
  const server = isGrpc ? createHttp2Server() : createHttpServer();
  server.setTimeout(10_000, (connection) => connection.destroy());
  server.on('connection', (socket) => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  if (isGrpc) server.on('session', (session) => { sessions.add(session); session.on('close', () => sessions.delete(session)); session.on('error', () => {}); });
  server.on('clientError', (_error, socket) => { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); });
  const admit = async (req) => {
    const authorized = await authorize({ method: req.method, path: req.url, headers: pairHeaders(req.rawHeaders), stats });
    if (!authorized) return 401;
    requests += 1;
    stats.requestTargets.push(sanitizeRecord({ provenance: 'captured', method: req.method, path: req.url }, context));
    if (requests > MAX_REQUESTS) return 429;
    if (req.method !== safeFixture.request.method || req.url !== localReplayPath(safeFixture.request.path)) return 404;
    for (const [name, counter] of [['owner', 'ownerLookups'], ['quota', 'quotaLookups'], ['signer', 'signerCalls'], ['provider', 'providerCalls']]) {
      if (hooks[name]) { stats[counter] += 1; await hooks[name](); }
    }
    return 0;
  };
  const respond = async (req, res) => {
    try {
      const rejection = await admit(req);
      if (rejection) {
        observe({ event: 'local-rejection', status: rejection, body_read: false, harness_only: true });
        res.writeHead(rejection, { 'content-length': '0', ...(isGrpc ? {} : { connection: 'close' }) }); res.end(); return;
      }
      if (!isGrpc && req.headers.expect?.toLowerCase() === '100-continue') res.writeContinue();
      const chunks = []; let bytes = 0;
      stats.bodyReadCalls += 1;
      for await (const chunk of req) {
        bytes += chunk.length; stats.bodyBytesRead += chunk.length;
        if (bytes > MAX_BYTES) throw fail('QHARNESS_REQUEST_TOO_LARGE');
        chunks.push(chunk);
      }
      const body = Buffer.concat(chunks);
      let json;
      try { json = JSON.parse(body.toString('utf8')); } catch { json = null; }
      const url = new URL(req.url, 'http://recorder.invalid');
      observe({ event: 'request', method: req.method, path: url.pathname, headers: pairHeaders(req.rawHeaders), query: [...url.searchParams], body: json ?? { kind: 'metadata-only', bytes }, harness_only: true });
      res.writeHead(safeFixture.response.status, isGrpc ? Object.fromEntries(safeFixture.response.headers) : safeFixture.response.headers.flat());
      for (const frame of safeFixture.response.frames) writeFragments(res, frameBytes(frame), frame.chunkSizes ?? frame.chunk_sizes);
      const termination = terminalKind(safeFixture.response.termination);
      observe({ event: 'replay-termination', termination, frame_count: safeFixture.response.frames.length, provider_verified: false });
      if (isInterruptedTermination(termination)) { res.flushHeaders?.(); setImmediate(() => res.destroy()); }
      else {
        if (safeFixture.response.trailers) res.addTrailers(Object.fromEntries(safeFixture.response.trailers));
        res.end();
      }
    } catch {
      observe({ event: 'recorder-error', code: SAFE_ERROR });
      if (!res.headersSent) res.writeHead(400, { 'content-length': '0' });
      res.end();
    }
  };
  server.on('request', (req, res) => { void respond(req, res); });
  if (!isGrpc) server.on('checkContinue', (req, res) => { void respond(req, res); });
  if (!isGrpc) server.on('upgrade', async (req, socket, head) => {
    socket.on('error', () => {});
    try {
      const rejection = await admit(req);
      const key = req.headers['sec-websocket-key'];
      if (rejection || typeof key !== 'string' || req.headers['sec-websocket-version'] !== '13') {
        socket.end(`HTTP/1.1 ${rejection || 400} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`); return;
      }
      const accept = createHash('sha1').update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest('base64');
      socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
      observe({ event: 'ws-open', headers: pairHeaders(req.rawHeaders), provider_verified: false });
      let buffered = head; let fragments = []; let messageOpcode = null; let messages = 0;
      const replay = () => {
        for (const frame of websocketReplayFrames(safeFixture.response.frames)) {
          writeFragments(socket, frame.bytes, frame.chunkSizes);
        }
        const termination = terminalKind(safeFixture.response.termination);
        observe({ event: 'replay-termination', termination, frame_count: safeFixture.response.frames.length, provider_verified: false });
        if (isInterruptedTermination(termination) || termination === 'close:1006') socket.destroy();
        else if (['close', 'ws-close', 'eof', 'complete', 'completed'].includes(termination) || termination.startsWith('close:')) {
          const code = safeFixture.response.close_code ?? (termination.startsWith('close:') ? Number(termination.slice(6)) : 1000);
          const payload = Buffer.alloc(2); payload.writeUInt16BE(code);
          socket.end(websocketFrame(8, payload));
        }
      };
      const drain = () => {
        try {
          while (buffered.length >= 2) {
            const fin = Boolean(buffered[0] & 0x80); const opcode = buffered[0] & 15; const masked = Boolean(buffered[1] & 0x80);
            let length = buffered[1] & 127; let offset = 2;
            if (!masked || (buffered[0] & 0x70)) throw fail('QHARNESS_WS_BAD_FRAME');
            if (length === 126) { if (buffered.length < 4) return; length = buffered.readUInt16BE(2); offset = 4; }
            else if (length === 127) { if (buffered.length < 10) return; const n = buffered.readBigUInt64BE(2); if (n > BigInt(MAX_BYTES)) throw fail('QHARNESS_WS_TOO_LARGE'); length = Number(n); offset = 10; }
            if (length > MAX_BYTES || (opcode >= 8 && (!fin || length > 125))) throw fail('QHARNESS_WS_BAD_FRAME');
            if (buffered.length < offset + 4 + length) return;
            const mask = buffered.subarray(offset, offset + 4); offset += 4;
            const payload = Buffer.from(buffered.subarray(offset, offset + length)); buffered = buffered.subarray(offset + length);
            for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
            if (opcode === 8) {
              if (!socket.writableEnded) socket.end(websocketFrame(8, payload));
              else socket.destroy();
              return;
            }
            if (opcode === 9) { socket.write(websocketFrame(10, payload)); continue; }
            if (opcode === 10) continue;
            if (![0, 1, 2].includes(opcode) || (opcode === 0 && messageOpcode === null) || (opcode !== 0 && messageOpcode !== null)) throw fail('QHARNESS_WS_BAD_FRAME');
            if (opcode !== 0) messageOpcode = opcode;
            fragments.push(payload);
            if (fragments.reduce((n, part) => n + part.length, 0) > MAX_BYTES) throw fail('QHARNESS_WS_TOO_LARGE');
            if (!fin) continue;
            const message = Buffer.concat(fragments); fragments = []; messageOpcode = null;
            if (++messages > MAX_REQUESTS) throw fail('QHARNESS_TOO_MANY_MESSAGES');
            let json; try { json = JSON.parse(message.toString('utf8')); } catch { json = null; }
            observe({ event: 'ws-message', body: json ?? { kind: 'metadata-only', bytes: message.length } });
            replay();
          }
        } catch { observe({ event: 'recorder-error', code: 'QHARNESS_WS_BAD_FRAME' }); socket.destroy(); }
      };
      socket.on('data', (chunk) => { buffered = Buffer.concat([buffered, chunk]); drain(); });
      drain();
    } catch { socket.destroy(); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const port = server.address().port;
  const listener = { kind: 'listener', identifier: `127.0.0.1:${port}`, owner_pid: process.pid, close_identity: `startReplayServer:${fixture.id}:${process.pid}:${port}`, delete_command: 'await replay.close()', cleanup_language: 'javascript-in-owning-process' };
  const closeServer = async () => {
    for (const session of sessions) session.destroy();
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve) => server.close(resolve));
  };
  try { await resource(ledger, scratchDir, { ...listener, state: 'active' }); }
  catch {
    await closeServer();
    try { await resource(ledger, scratchDir, { ...listener, state: 'removed' }); } catch { /* Registration error remains the primary safe failure. */ }
    throw fail('QHARNESS_LISTENER_REGISTRATION_FAILED');
  }
  return {
    origin: `http://127.0.0.1:${port}`, port, fixture: safeFixture, observations, stats,
    async close() {
      if (closed) return; closed = true;
      await closeServer();
      let failure;
      try { await resource(ledger, scratchDir, { ...listener, state: 'removed' }); }
      catch { failure = fail('QHARNESS_CLEANUP_LEDGER_FAILED'); }
      try { await writes; }
      catch { failure ??= fail('QHARNESS_WIRE_PERSISTENCE_REJECTED'); }
      if (failure) throw failure;
    },
  };
}

export function buildSandboxProfile({ scratchDir, originalHome, recorderPort, deniedProbe, allowEphemeralBind = false }) {
  if (!Number.isInteger(recorderPort) || recorderPort < 1 || recorderPort > 65535) throw fail('QHARNESS_INVALID_PORT');
  const q = (s) => JSON.stringify(s);
  // Seatbelt rejects port 0 syntactically. OS-selected ephemeral listeners need
  // wildcard local bind/inbound permission; outbound stays recorder-port-only.
  const bind = allowEphemeralBind ? '(allow network-bind (local ip "localhost:*"))\n(allow network-inbound (local ip "localhost:*"))\n' : '';
  return `(version 1)\n(allow default)\n(deny network*)\n(allow network-outbound (remote ip "localhost:${recorderPort}"))\n${bind}(deny file-read* (subpath ${q(originalHome)}) (subpath "/Library/Keychains") (subpath ${q(deniedProbe)}))\n(deny mach-lookup (global-name "com.apple.securityd") (global-name "com.apple.securityd.xpc"))\n(deny file-write*)\n(allow file-write* (subpath ${q(scratchDir)}) (literal "/dev/null"))\n`;
}

export function parseOwnedListenerSnapshot(raw, processGroup) {
  if (!Number.isInteger(processGroup) || processGroup < 1 || Buffer.byteLength(raw) > MAX_BYTES) throw fail('QHARNESS_LISTENER_AUDIT_INVALID');
  let pid; let group;
  const listeners = [];
  for (const line of raw.split('\n')) {
    if (/^p\d+$/.test(line)) { pid = Number(line.slice(1)); group = null; }
    else if (/^g\d+$/.test(line)) group = Number(line.slice(1));
    else if (line.startsWith('n')) {
      const address = line.slice(1).match(/^(127\.0\.0\.1|\[::1\]):(\d+)$/);
      if (!pid || group !== processGroup || !address || Number(address[2]) < 1 || Number(address[2]) > 65535) throw fail('QHARNESS_OWNED_LISTENER_SCOPE_REJECTED');
      listeners.push({ owner_pid: pid, owner_process_group: group, address: address[1] === '[::1]' ? '::1' : address[1], port: Number(address[2]) });
    }
  }
  return listeners;
}

/** Ownership evidence only. Polling cannot prove absence of transient listeners. */
export function createOwnedListenerAudit({ scratchDir, ledger, maxDurationMs = 35_000 }) {
  if (!Number.isFinite(maxDurationMs) || maxDurationMs < 1 || maxDurationMs > 120_000) return { status: 'BLOCKED', reason: 'QHARNESS_LISTENER_AUDIT_DEADLINE_INVALID' };
  const executable = findExecutable('lsof') ?? (existsSync('/usr/sbin/lsof') ? '/usr/sbin/lsof' : null);
  if (!executable) return { status: 'BLOCKED', reason: 'QHARNESS_LISTENER_INSPECTOR_UNAVAILABLE' };
  const seen = new Map();
  let stopped = false; let running; let failure = null; let group;
  const snapshot = () => ({
    status: failure ? 'FAIL' : 'OBSERVED', reason: failure, processGroup: group,
    sampled: true, intervalMs: 50, continuousIngressEnforcement: false,
    listeners: [...seen.values()],
  });
  return {
    status: 'READY',
    snapshot,
    start({ pid, terminate }) {
      group = pid;
      const deadline = Date.now() + maxDurationMs;
      running = (async () => {
        while (!stopped) {
          if (Date.now() > deadline) throw fail('QHARNESS_LISTENER_AUDIT_DEADLINE');
          const result = await runBoundedProcess(executable, ['-nP', '-a', '-g', String(pid), '-iTCP', '-sTCP:LISTEN', '-F', 'pgn'], {
            env: { PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: scratchDir }, cwd: scratchDir, scratchDir, ledger, deadlineMs: 1500,
          });
          if (result.error || result.timedOut || (result.exitCode !== 0 && !(result.exitCode === 1 && result.stdout.length === 0 && result.stderr.length === 0))) throw fail('QHARNESS_LISTENER_INSPECTION_FAILED');
          const listeners = parseOwnedListenerSnapshot(result.stdout.toString('utf8'), pid);
          const now = new Date().toISOString();
          for (const listener of listeners) {
            const key = `${listener.owner_pid}:${listener.address}:${listener.port}`;
            const prior = seen.get(key);
            if (prior) { prior.last_seen_utc = now; continue; }
            const entry = { ...listener, first_seen_utc: now, last_seen_utc: now };
            seen.set(key, entry);
            await resource(ledger, scratchDir, { kind: 'owned-listener-observation', identifier: key, ...entry, sampled: true, delete_command: `kill -TERM -- -${pid}`, state: 'active' });
          }
          if (!stopped) await new Promise((resolve) => setTimeout(resolve, 50));
        }
      })().catch((error) => {
        failure = error.code?.startsWith('QHARNESS_') ? error.code : 'QHARNESS_LISTENER_AUDIT_FAILED';
        stopped = true; terminate();
      });
    },
    async stop({ processGroupGone = false } = {}) {
      stopped = true;
      await running;
      const closedUpperBound = new Date().toISOString();
      for (const [key, entry] of seen) await resource(ledger, scratchDir, {
        kind: 'owned-listener-observation', identifier: key, ...entry, sampled: true,
        ...(processGroupGone ? { closed_upper_bound_utc: closedUpperBound } : { observation_ended_utc: closedUpperBound }),
        delete_command: `kill -TERM -- -${group}`, state: processGroupGone ? 'removed' : 'unresolved',
      });
      return snapshot();
    },
  };
}

async function waitForOwnedProcessGroup(group, probe, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try { probe(group); }
    catch (error) {
      if (error.code === 'ESRCH') return { status: 'gone', reason: null };
      return { status: 'unresolved', reason: ['EPERM', 'EACCES'].includes(error.code) ? 'probe-denied' : 'probe-failed' };
    }
    if (Date.now() >= deadline) return { status: 'unresolved', reason: 'cleanup-deadline' };
    await new Promise((resolve) => setTimeout(resolve, Math.min(10, deadline - Date.now())));
  }
}

export function runBoundedProcess(command, args, { env, cwd, scratchDir = cwd, ledger, listenerAudit, deadlineMs = 20_000, maxBytes = MAX_BYTES, groupProbe = (group) => process.kill(-group, 0), groupCleanupTimeoutMs = 1000 }) {
  if (typeof groupProbe !== 'function' || !Number.isFinite(groupCleanupTimeoutMs) || groupCleanupTimeoutMs < 1 || groupCleanupTimeoutMs > 3000) throw fail('QHARNESS_PROCESS_GROUP_CLEANUP_BOUND_INVALID');
  return new Promise((resolve) => {
    const child = spawn(command, args, { env, cwd, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    const out = []; const err = []; let bytes = 0; let timedOut = false; let overflow = false; let finished = false;
    const kill = (signal) => { if (child.pid) { try { process.kill(-child.pid, signal); } catch {} } };
    const entry = { kind: 'process-group', identifier: String(child.pid), owner_pid: process.pid, command, argv: args, started_utc: new Date().toISOString(), delete_command: `kill -TERM -- -${child.pid}` };
    const registration = child.pid ? resource(ledger, scratchDir, { ...entry, state: 'active' }) : Promise.resolve();
    registration.catch(() => kill('SIGKILL'));
    if (listenerAudit && child.pid) listenerAudit.start({ pid: child.pid, terminate: () => kill('SIGTERM') });
    const timer = setTimeout(() => { timedOut = true; kill('SIGTERM'); }, deadlineMs);
    const hardTimer = setTimeout(() => kill('SIGKILL'), deadlineMs + 1000);
    const reap = setTimeout(() => { void finish(null, 'SIGKILL', 'QHARNESS_REAP_DEADLINE'); }, deadlineMs + 3000);
    const collect = (destination) => (chunk) => {
      bytes += chunk.length;
      if (bytes > maxBytes) { overflow = true; kill('SIGTERM'); }
      else destination.push(chunk);
    };
    child.stdout.on('data', collect(out)); child.stderr.on('data', collect(err));
    async function finish(exitCode, signal, error = null) {
      if (finished) return; finished = true;
      clearTimeout(timer); clearTimeout(hardTimer); clearTimeout(reap);
      kill('SIGKILL'); child.stdout.destroy(); child.stderr.destroy();
      let cleanupError = null;
      // Registration can persist the active entry and then reject. Settle it
      // independently so it cannot prevent either cleanup or the final record.
      try { await registration; } catch { error ??= 'QHARNESS_RESOURCE_LEDGER_FAILED'; }
      const processGroupCleanup = child.pid
        ? await waitForOwnedProcessGroup(child.pid, groupProbe, groupCleanupTimeoutMs)
        : { status: 'not-created', reason: null };
      if (processGroupCleanup.status === 'unresolved') cleanupError = 'QHARNESS_PROCESS_GROUP_CLEANUP_UNRESOLVED';
      let audit;
      if (listenerAudit) {
        try { audit = await listenerAudit.stop({ processGroupGone: processGroupCleanup.status === 'gone' }); if (audit.status === 'FAIL') error ??= audit.reason; }
        catch { error ??= 'QHARNESS_LISTENER_AUDIT_FAILED'; }
      }
      if (child.pid) {
        try {
          await resource(ledger, scratchDir, { ...entry, state: processGroupCleanup.status === 'gone' ? 'removed' : 'unresolved', cleanup_reason: processGroupCleanup.reason });
        } catch { cleanupError ??= 'QHARNESS_RESOURCE_LEDGER_FAILED'; }
      }
      error ??= cleanupError;
      resolve({ pid: child.pid ?? null, exitCode, signal, timedOut, overflow, error, cleanupError, processGroupCleanup, listenerAudit: audit, stdout: Buffer.concat(out), stderr: Buffer.concat(err) });
    }
    child.on('error', () => { void finish(null, null, 'QHARNESS_SPAWN_FAILED'); });
    child.on('exit', () => kill('SIGKILL'));
    child.on('close', (code, signal) => { void finish(code, signal); });
  });
}

export async function preflightIsolation({ scratchDir, recorderPort, ledger, platform = process.platform, allowEphemeralBind = false }) {
  if (platform !== 'darwin' || !existsSync('/usr/bin/sandbox-exec')) return { status: 'BLOCKED', reason: 'QHARNESS_OS_SANDBOX_UNAVAILABLE', spawnedClient: false };
  await mkdir(scratchDir, { recursive: true, mode: 0o700 });
  scratchDir = await realpath(scratchDir);
  const deniedProbe = path.join(scratchDir, 'auth-store-denial-probe');
  await mkdir(deniedProbe, { recursive: true, mode: 0o700 });
  await writeFile(path.join(deniedProbe, 'auth.json'), 'QHARNESS_PLANTED_AUTH_SENTINEL', { mode: 0o600 });
  const profile = buildSandboxProfile({ scratchDir, originalHome: homedir(), recorderPort, deniedProbe, allowEphemeralBind });
  const profilePath = path.join(scratchDir, 'consumer.sb');
  await writeFile(profilePath, profile, { mode: 0o600 });
  const program = `const fs=require('node:fs');const net=require('node:net');const connect=(host,port)=>new Promise(r=>{const s=net.connect({host,port});s.setTimeout(1500);s.once('connect',()=>{s.destroy();r('CONNECTED')});s.once('error',e=>r(e.code));s.once('timeout',()=>{s.destroy();r('TIMEOUT')})});(async()=>{let auth;try{fs.readFileSync(process.argv[1]);auth='READABLE'}catch(e){auth=e.code}const allowed=await connect('127.0.0.1',Number(process.argv[2]));const foreign=await connect('203.0.113.1',443);console.log(JSON.stringify({auth,allowed,foreign}));})()`;
  const result = await runBoundedProcess('/usr/bin/sandbox-exec', ['-f', profilePath, process.execPath, '-e', program, path.join(deniedProbe, 'auth.json'), String(recorderPort)], {
    env: { PATH: '/usr/bin:/bin', HOME: scratchDir, TMPDIR: scratchDir }, cwd: scratchDir, scratchDir, ledger, deadlineMs: 6000,
  });
  let proof; try { proof = JSON.parse(result.stdout.toString('utf8')); } catch { proof = null; }
  const denied = (value) => ['EPERM', 'EACCES'].includes(value);
  const preflightProof = {
    processExitedZero: result.exitCode === 0, timedOut: result.timedOut, resultDecoded: Boolean(proof),
    recorderAllowed: proof?.allowed === 'CONNECTED', authStoreDenied: denied(proof?.auth), externalDenied: denied(proof?.foreign),
    policySyntaxRejected: /syntax error|parse error|invalid port|unbound variable|unrecognized.*(?:filter|operation)|illegal.*(?:profile|port)/i.test(result.stderr.toString('utf8')),
    denialSignals: classifyDenialSignals({ ...result, scratchDir }),
  };
  if (result.exitCode !== 0 || result.timedOut || !proof || proof.allowed !== 'CONNECTED' || !denied(proof.auth) || !denied(proof.foreign)) {
    return { status: 'BLOCKED', reason: preflightProof.policySyntaxRejected ? 'QHARNESS_SANDBOX_PROFILE_REJECTED' : 'QHARNESS_SANDBOX_PREFLIGHT_FAILED', spawnedClient: false, preflightProof };
  }
  let bindProof;
  if (allowEphemeralBind) {
    // The registered child process group owns every probe socket. It closes each
    // listener before emitting only a safe loopback port observation.
    const bindProgram = `const net=require('node:net');const bind=host=>new Promise(resolve=>{const s=net.createServer(c=>c.destroy());s.once('error',e=>resolve({status:e.code}));s.listen(0,host,()=>{const port=s.address().port;const created_utc=new Date().toISOString();s.close(()=>resolve({status:'BOUND',port,created_utc}))})});(async()=>{const loopback=await bind('127.0.0.1');const nonloopback=await bind('0.0.0.0');console.log(JSON.stringify({loopback,nonloopback}))})()`;
    const bound = await runBoundedProcess('/usr/bin/sandbox-exec', ['-f', profilePath, process.execPath, '-e', bindProgram], {
      env: { PATH: '/usr/bin:/bin', HOME: scratchDir, TMPDIR: scratchDir }, cwd: scratchDir, scratchDir, ledger, deadlineMs: 4000,
    });
    let observed; try { observed = JSON.parse(bound.stdout.toString('utf8')); } catch { observed = null; }
    const loopbackAllowed = observed?.loopback?.status === 'BOUND' && Number.isInteger(observed.loopback.port) && observed.loopback.port > 0 && observed.loopback.port <= 65535;
    const nonLoopbackDenied = denied(observed?.nonloopback?.status);
    if (loopbackAllowed) {
      await resource(ledger, scratchDir, { kind: 'child-listener-probe', identifier: `127.0.0.1:${observed.loopback.port}`, owner_process_group: bound.pid, created_utc: observed.loopback.created_utc, delete_command: `kill -TERM -- -${bound.pid}`, state: 'removed' });
    }
    if (bound.exitCode !== 0 || bound.timedOut || bound.error || !loopbackAllowed || !nonLoopbackDenied) {
      return { status: 'BLOCKED', reason: 'QHARNESS_EPHEMERAL_BIND_SCOPE_NOT_ENFORCED', spawnedClient: false, preflightProof, bindProof: { processExitedZero: bound.exitCode === 0, timedOut: bound.timedOut, resultDecoded: Boolean(observed), loopbackAllowed, loopbackDenied: denied(observed?.loopback?.status), nonLoopbackDenied, nonLoopbackAllowed: observed?.nonloopback?.status === 'BOUND', denialSignals: classifyDenialSignals({ ...bound, scratchDir }) } };
    }
    bindProof = { ephemeralLoopbackBindAllowed: true, nonLoopbackBindDenied: true, observedProbePort: observed.loopback.port };
  }
  // A forbidden *loopback* destination must also be denied, not merely external egress.
  const canary = createTcpServer((socket) => socket.destroy());
  await new Promise((resolve, reject) => { canary.once('error', reject); canary.listen(0, '127.0.0.1', resolve); });
  const canaryPort = canary.address().port;
  const canaryResource = { kind: 'listener', identifier: `127.0.0.1:${canaryPort}`, owner_pid: process.pid, close_identity: `preflightIsolation:canary:${process.pid}:${canaryPort}`, delete_command: 'await new Promise(resolve => canary.close(resolve))', cleanup_language: 'javascript-in-owning-process' };
  let other;
  let canaryFailure;
  try {
    try { await resource(ledger, scratchDir, { ...canaryResource, state: 'active' }); }
    catch { throw fail('QHARNESS_CANARY_REGISTRATION_FAILED'); }
    const probe = await runBoundedProcess('/usr/bin/sandbox-exec', ['-f', profilePath, process.execPath, '-e', `const n=require('node:net');const s=n.connect({host:'127.0.0.1',port:${canaryPort}});s.on('connect',()=>{s.destroy();process.exit(2)});s.on('error',e=>process.exit(['EPERM','EACCES'].includes(e.code)?0:3));s.setTimeout(1500,()=>{s.destroy();process.exit(4)})`], { env: { PATH: '/usr/bin:/bin', HOME: scratchDir }, cwd: scratchDir, scratchDir, ledger, deadlineMs: 3000 });
    other = probe.exitCode === 0 && !probe.timedOut;
  } catch (error) {
    canaryFailure = error.code?.startsWith('QHARNESS_') ? error : fail('QHARNESS_CANARY_PROBE_FAILED');
  } finally {
    await new Promise((resolve) => canary.close(resolve));
    try { await resource(ledger, scratchDir, { ...canaryResource, state: 'removed' }); }
    catch { canaryFailure ??= fail('QHARNESS_CLEANUP_LEDGER_FAILED'); }
  }
  if (canaryFailure) throw canaryFailure;
  return other ? { status: 'READY', profilePath, proof: { authStoreDenied: true, externalDenied: true, otherLoopbackDenied: true, recorderAllowed: true, ...bindProof } } : { status: 'BLOCKED', reason: 'QHARNESS_LOOPBACK_SCOPE_NOT_ENFORCED', spawnedClient: false, preflightProof: { ...preflightProof, otherLoopbackDenied: false } };
}

function findExecutable(name) {
  for (const directory of (process.env.PATH ?? '').split(path.delimiter)) {
    const candidate = path.join(directory, name);
    if (existsSync(candidate)) return realpathSync(candidate);
  }
  return null;
}

export function buildAgyLaunch({ consumer, origin }) {
  const url = new URL(origin);
  if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.username || url.password || url.pathname !== '/' || url.search || url.hash) throw fail('QHARNESS_AGY_REPLAY_ORIGIN_INVALID');
  if (consumer.mode !== 'agy-print' || consumer.transport !== 'sse' || (consumer.authMode !== undefined && consumer.authMode !== 'gateway-bearer')) throw fail('QHARNESS_AGY_AUTH_MODE_UNSUPPORTED');
  const prompt = 'Reply with the supplied response text. Do not use tools.';
  if (consumer.authMode === 'gateway-bearer') {
    if (consumer.model !== 'gemini-2.0-flash' || consumer.modelSource !== 'AGY_LLM_GATEWAY_MODELS') throw fail('QHARNESS_AGY_GATEWAY_MODEL_CONTRACT');
    return {
      authMode: 'gateway-bearer', settings: null,
      environment: {
        AGY_CLI_HIDE_LOGO: '1', AGY_CLI_HIDE_ACCOUNT_INFO: '1',
        AGY_LLM_GATEWAY_URL: url.origin,
        AGY_LLM_GATEWAY_API_KEY: 'synthetic-gateway-api-key',
        AGY_LLM_GATEWAY_MODELS: 'gemini-2.0-flash,gemini-2.5-pro',
      },
      argv: ['--print', prompt, '--output-format', 'json'],
    };
  }
  return {
    authMode: 'native-api-key',
    settings: { security: { auth: { selectedType: 'gemini-api-key' } }, telemetry: { enabled: false }, privacy: { usageStatisticsEnabled: false } },
    environment: { GEMINI_API_KEY: 'qharness-synthetic-local-only', GOOGLE_GEMINI_BASE_URL: url.origin, GOOGLE_GENAI_USE_VERTEXAI: 'false' },
    argv: ['--print', prompt, '--model', 'gemini-3.1-pro-low', '--settings', '{"modelProvider":"gemini"}', '--output-format', 'stream-json', '--print-timeout', '15s'],
  };
}

export function matchesSyntheticGatewayAuth(headers) {
  const authorization = headers.filter(([name]) => name.toLowerCase() === 'authorization');
  return authorization.length === 1 && authorization[0][1] === 'Bearer synthetic-gateway-api-key'
    && !headers.some(([name]) => ['x-api-key', 'x-goog-api-key', 'api-key'].includes(name.toLowerCase()));
}

export async function runPinnedConsumer({ fixture, executable, scratchDir, ledger, deadlineMs = 30_000 }) {
  validateReplayFixture(fixture);
  const consumer = fixture.expected?.consumer;
  const pin = PINS[fixture.client?.name];
  const base = { fixtureId: fixture.id, classification: 'synthetic-consumer-replay', providerAcceptance: 'NOT-RUN', runtimeAcceptance: 'NOT-RUN', fullClientAcceptance: false };
  if (!consumer || !pin || fixture.client.version !== pin.version || fixture.client.binary_sha256 !== pin.sha256 || (pin.revision && fixture.client.source_revision !== pin.revision)) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_CONSUMER_PIN_REQUIRED' };
  executable ??= findExecutable(fixture.client.name);
  if (!executable) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_CLIENT_UNAVAILABLE' };
  const bytes = await readFile(executable);
  if (createHash('sha256').update(bytes).digest('hex') !== pin.sha256) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_CLIENT_HASH_MISMATCH' };
  if (!['sse', 'ws'].includes(consumer.transport) || !['codex-exec', 'agy-print'].includes(consumer.mode) || typeof consumer.marker !== 'string' || !/^[A-Z0-9_]{8,100}$/.test(consumer.marker)) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_CONSUMER_ACTION_UNSUPPORTED' };
  if ((fixture.client.name === 'codex') !== (consumer.mode === 'codex-exec') || (fixture.client.name === 'agy' && consumer.transport !== 'sse')) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_CONSUMER_ACTION_MISMATCH' };
  if (fixture.client.name === 'agy' && (consumer.authMode !== undefined && consumer.authMode !== 'gateway-bearer')) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_AGY_AUTH_MODE_UNSUPPORTED' };
  if (consumer.authMode === 'gateway-bearer' && (consumer.model !== 'gemini-2.0-flash' || consumer.modelSource !== 'AGY_LLM_GATEWAY_MODELS')) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_AGY_GATEWAY_MODEL_CONTRACT' };
  await mkdir(scratchDir, { recursive: true, mode: 0o700 });
  scratchDir = await realpath(scratchDir);
  const home = path.join(scratchDir, 'home'); const tmp = path.join(scratchDir, 'tmp'); const work = path.join(scratchDir, 'work');
  for (const directory of [home, tmp, work, path.join(home, '.config'), path.join(home, '.cache'), path.join(home, '.local/share'), path.join(home, '.local/state')]) await mkdir(directory, { recursive: true, mode: 0o700 });
  await resource(ledger, scratchDir, { kind: 'scratch-directory', identifier: scratchDir, delete_command: `rm -rf -- ${shellQuote(scratchDir)}`, state: 'retained' });
  const databasePath = path.join(scratchDir, 'harness.sqlite3');
  const python = findExecutable('python3');
  if (!python) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_SQLITE_PYTHON_UNAVAILABLE' };
  const database = await runBoundedProcess(python, ['-c', 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute("CREATE TABLE IF NOT EXISTS harness_run (fixture_id TEXT NOT NULL)"); c.execute("INSERT INTO harness_run VALUES (?)",(sys.argv[2],)); c.commit(); c.close()', databasePath, fixture.id], { cwd: work, env: { PATH: '/usr/bin:/bin', HOME: home }, scratchDir, ledger, deadlineMs: 5000 });
  if (database.exitCode !== 0) return { ...base, status: 'BLOCKED', reason: 'QHARNESS_SQLITE_CREATION_FAILED' };
  const fakeKey = 'qharness-synthetic-local-only';
  const gateway = consumer.authMode === 'gateway-bearer';
  let gatewayAccepted = 0; let gatewayRejected = 0;
  const replay = await startReplayServer({ fixture, scratchDir, ledger, authorize: gateway ? ({ headers }) => {
    const accepted = matchesSyntheticGatewayAuth(headers);
    if (accepted) gatewayAccepted++; else gatewayRejected++;
    return accepted;
  } : undefined });
  let result;
  try {
    const isolation = await preflightIsolation({ scratchDir, recorderPort: replay.port, ledger, allowEphemeralBind: fixture.client.name === 'agy' });
    if (isolation.status !== 'READY') return { ...base, ...isolation };
    const listenerAudit = fixture.client.name === 'agy' ? createOwnedListenerAudit({ scratchDir, ledger, maxDurationMs: deadlineMs + 2000 }) : null;
    if (listenerAudit?.status === 'BLOCKED') return { ...base, status: 'BLOCKED', reason: listenerAudit.reason, spawnedClient: false };
    const environment = {
      PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, TMPDIR: tmp, LANG: 'C.UTF-8', TERM: 'dumb', NO_COLOR: '1',
      XDG_CONFIG_HOME: path.join(home, '.config'), XDG_CACHE_HOME: path.join(home, '.cache'), XDG_DATA_HOME: path.join(home, '.local/share'), XDG_STATE_HOME: path.join(home, '.local/state'),
    };
    let argv;
    if (fixture.client.name === 'codex') {
      const codexHome = path.join(home, '.codex'); await mkdir(codexHome, { mode: 0o700 });
      const url = new URL(fixture.request.path, replay.origin); const baseUrl = `${replay.origin}${url.pathname.replace(/\/responses\/?$/, '')}`;
      const config = `model_provider="qharness"\nmodel="gpt-5.2"\napproval_policy="never"\nsandbox_mode="read-only"\ncheck_for_update_on_startup=false\ncli_auth_credentials_store="file"\nmcp_oauth_credentials_store="file"\n[features]\nplugins=false\nremote_plugin=false\napps=false\n[analytics]\nenabled=false\n[model_providers.qharness]\nname="qharness"\nbase_url=${JSON.stringify(baseUrl)}\nwire_api="responses"\nenv_key="QHARNESS_LOCAL_KEY"\nsupports_websockets=${consumer.transport === 'ws'}\nrequires_openai_auth=false\nrequest_max_retries=0\nstream_max_retries=0\n`;
      await writeFile(path.join(codexHome, 'config.toml'), config, { mode: 0o600 });
      environment.CODEX_HOME = codexHome; environment.QHARNESS_LOCAL_KEY = fakeKey;
      argv = ['exec', '--json', '--skip-git-repo-check', '--', 'Reply with the supplied response text. Do not use tools.'];
    } else {
      const launch = buildAgyLaunch({ consumer, origin: replay.origin });
      if (launch.settings) {
        const geminiHome = path.join(home, '.gemini'); await mkdir(geminiHome, { mode: 0o700 });
        await writeFile(path.join(geminiHome, 'settings.json'), JSON.stringify(launch.settings), { mode: 0o600 });
      }
      Object.assign(environment, launch.environment);
      argv = launch.argv;
    }
    result = await runBoundedProcess('/usr/bin/sandbox-exec', ['-f', isolation.profilePath, executable, ...argv], { env: environment, cwd: work, scratchDir, ledger, listenerAudit, deadlineMs });
    // Only decoded, completed consumer output counts. Request/debug echo cannot satisfy this.
    // Raw stdout/stderr remain in memory and are never returned or persisted.
    const decoded = observeDecodedConsumerOutput({ client: fixture.client.name, stdout: result.stdout, marker: consumer.marker });
    const gatewayAuthVerified = gateway ? gatewayAccepted > 0 && gatewayRejected === 0 : null;
    const listenerObserved = !listenerAudit || result.listenerAudit?.listeners.length > 0;
    const passed = decoded.markerObserved && decoded.completed && result.exitCode === 0 && !result.timedOut && !result.overflow && !result.error && (!gateway || gatewayAuthVerified) && listenerObserved;
    const failure = passed ? null : classifyConsumerFailure(result) ?? (!listenerObserved ? 'QHARNESS_OWNED_LISTENER_NOT_OBSERVED' : gateway && !gatewayAuthVerified ? 'QHARNESS_GATEWAY_AUTH_NOT_VERIFIED' : !decoded.markerObserved ? 'QHARNESS_CONSUMER_MARKER_NOT_OBSERVED' : 'QHARNESS_CONSUMER_TURN_INCOMPLETE');
    const summary = { ...base, client: fixture.client, status: passed ? 'PASS' : 'FAIL', ...decoded, reason: failure, gatewayAuthVerified, ownedListenerAudit: result.listenerAudit, deniedOperations: passed ? [] : classifyDeniedOperations({ ...result, scratchDir }), denialSignals: passed ? null : classifyDenialSignals({ ...result, scratchDir }), exitCode: result.exitCode, signal: result.signal, timedOut: result.timedOut, outputOverflow: result.overflow, requestsObserved: replay.observations.length, requestTargets: replay.stats.requestTargets, isolation: isolation.proof, databaseCreated: true };
    // Completion includes durable safe evidence and teardown, not only a decoded answer.
    try { await replay.close(); }
    catch (error) { summary.status = 'FAIL'; summary.reason = error.code ?? 'QHARNESS_REPLAY_CLOSE_FAILED'; }
    await safeWrite(path.join(scratchDir, 'consumer-result.json'), { provenance: 'synthetic', ...summary }, createRedactionContext({ scenarioId: fixture.id, policyVersion: 'qharness-v1' }));
    return summary;
  } finally { await replay.close(); }
}
