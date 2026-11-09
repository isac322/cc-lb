#!/usr/bin/env node
// External prerequisites: CC_LB_BIN, Node >=22.13 (node:sqlite) or Bun, and an
// external directory containing node_modules/openai and node_modules/@google/genai.
// node tests/real-client/provider-e2e/image-admission-sdk.mjs \
//   --sdk-modules /absolute/scratch/sdk --ledger /absolute/scratch/resources.jsonl
// Optional native CLI capture/replay: add --cli. On macOS both clients and their
// descendants run under sandbox-exec with network denied except localhost.
// Elsewhere a namespace with no non-loopback interfaces is required.
// --cli-only implies --cli and skips the eight SDK cases for focused CLI replay QA.
// Linux example (requires unshare and ip):
// unshare --user --map-root-user --net sh -c 'ip link set lo up; exec node "$@"' \
//   sh tests/real-client/provider-e2e/image-admission-sdk.mjs \
//   --sdk-modules /absolute/scratch/sdk --ledger /absolute/scratch/resources.jsonl --cli
// Overrides: SDK_NODE_MODULES=/scratch/sdk/node_modules GEMINI_CLI=/scratch/gemini
// CODEX_BIN=/absolute/codex, or --codex-bin /... --gemini-bin /....
// Artifacts are retained. No repository package installs or real credentials.
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { createRequire, syncBuiltinESMExports } from 'node:module';
import net from 'node:net';
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, openSync, closeSync, readFileSync, realpathSync, statSync, writeFileSync, accessSync, constants } from 'node:fs';
import http from 'node:http';
import * as zlib from 'node:zlib';
import { networkInterfaces, tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
const { deflateSync, gunzipSync, zstdDecompressSync: nodeZstdDecompress } = zlib;

const args = {
  timeout: 90,
  cli: false,
  cliOnly: false,
  'sdk-modules': process.env.SDK_NODE_MODULES,
  'codex-bin': process.env.CODEX_BIN,
  'gemini-bin': process.env.GEMINI_CLI,
};
for (let i = 2; i < process.argv.length; i++) {
  const flag = process.argv[i];
  if (flag === '--cli') args.cli = true;
  else if (flag === '--cli-only') { args.cli = true; args.cliOnly = true; }
  else {
    assert(['--sdk-modules', '--ledger', '--cc-lb-bin', '--timeout', '--codex-bin', '--gemini-bin'].includes(flag), `Unknown argument: ${flag}`);
    assert(process.argv[i + 1], `Missing value for ${flag}`);
    args[flag.slice(2)] = process.argv[++i];
  }
}
assert(args.ledger && args['sdk-modules'], '--ledger and --sdk-modules are required');
const repository = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const ledger = resolve(args.ledger);
assert(!inside(repository, ledger) && existsSync(dirname(ledger)), '--ledger must have an existing parent outside repository');
const binary = realpathSync(args['cc-lb-bin'] ?? process.env.CC_LB_BIN ?? '');
accessSync(binary, constants.X_OK);
const sdkModulePath = realpathSync(args['sdk-modules']);
const sdkRoot = sdkModulePath.endsWith(`${sep}node_modules`) ? dirname(sdkModulePath) : sdkModulePath;
assert(!inside(repository, sdkRoot), 'SDK modules must be installed outside repository');
const sdkRequire = createRequire(join(sdkRoot, 'package.json'));
const timeout = Number(args.timeout) * 1000;
assert(Number.isFinite(timeout) && timeout > 0, '--timeout must be positive seconds');
const MODELS = { openai: 'gpt-4.1', gemini: 'gemini-2.5-flash' };
const nativeFetch = globalThis.fetch.bind(globalThis);
const resources = [];
const summary = { verdict: 'FAIL', scope: args.cliOnly ? 'native CLI capture/replay only; SDK cases NOT_RUN' : 'SDK controls/images and optional native CLI capture/replay', cases: [], cli: [], runtime: process.version, bun: process.versions.bun ?? null };
let scratch;
let db;
let activeWire;
let providerError;
let responseCounter = 0;
const providerCalls = [];

function inside(root, path) {
  const value = relative(root, path);
  return value === '' || (value !== '..' && !value.startsWith(`..${sep}`) && !isAbsolute(value));
}
function quote(value) { return `'${String(value).replaceAll("'", "'\\''")}'`; }
function record(kind, id, deleteCommand, details = {}) {
  appendFileSync(ledger, `${JSON.stringify({ created_at: new Date().toISOString(), kind, id: String(id), delete: deleteCommand, ...details })}\n`, { mode: 0o600 });
}
function sha(value) { return createHash('sha256').update(value).digest('hex'); }
function save(name, value) { writeFileSync(join(scratch, name), value, { mode: 0o600 }); }
function loopback(url) {
  const parsed = new URL(url);
  assert(parsed.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(parsed.hostname), `Non-loopback URL refused: ${parsed.origin}`);
  return parsed;
}
async function guardedFetch(input, init) {
  const request = new Request(input, init);
  loopback(request.url);
  const body = request.method === 'POST' ? Buffer.from(await request.clone().arrayBuffer()) : null;
  if (activeWire && body) activeWire.push({ path: new URL(request.url).pathname, body, sha256: sha(body) });
  return nativeFetch(request, { redirect: 'error', signal: AbortSignal.timeout(timeout) });
}
// SDKs cannot silently use a default remote endpoint or proxy. The fetch guard is
// the wire observer; the socket guard also covers Node HTTP transports.
const socketConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...values) {
  const normalized = Array.isArray(values[0]) ? values[0] : values;
  const options = normalized[0];
  const host = typeof options === 'object' && options !== null ? options.host ?? options.hostname : typeof normalized[1] === 'string' ? normalized[1] : undefined;
  assert(host === '127.0.0.1' || host === '::1', `Non-loopback socket refused: ${host ?? 'implicit host'}`);
  return socketConnect.apply(this, values);
};
syncBuiltinESMExports();
globalThis.fetch = guardedFetch;

function packageVersion(name) {
  let current = dirname(sdkRequire.resolve(name));
  for (;;) {
    const manifest = join(current, 'package.json');
    if (existsSync(manifest)) {
      const value = JSON.parse(readFileSync(manifest, 'utf8'));
      if (value.name === name) return value.version;
    }
    const parent = dirname(current);
    assert(parent !== current, `Cannot locate installed ${name} version`);
    current = parent;
  }
}
function catalog() {
  return Object.fromEntries(Object.entries(MODELS).map(([provider, model]) => [model, {
    litellm_provider: provider, mode: 'chat', max_input_tokens: 131072,
    max_output_tokens: 1024, max_tokens: 131072, input_cost_per_token: 0.000002,
    output_cost_per_token: 0.00001, cache_read_input_token_cost: 0.0000001,
  }]));
}
function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
function pngChunk(kind, payload) {
  const type = Buffer.from(kind);
  const length = Buffer.alloc(4); length.writeUInt32BE(payload.length);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(Buffer.concat([type, payload])));
  return Buffer.concat([length, type, payload, crc]);
}
function image() {
  const width = 1024; const height = 768;
  const rows = Buffer.alloc(height * (width * 3 + 1));
  let state = 0x1182cafe;
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width * 3; x++) {
      state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
      rows[y * (width * 3 + 1) + x + 1] = state & 255;
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0); ihdr.writeUInt32BE(height, 4); ihdr[8] = 8; ihdr[9] = 2;
  const png = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), pngChunk('IHDR', ihdr), pngChunk('IDAT', deflateSync(rows, { level: 6 })), pngChunk('IEND', Buffer.alloc(0))]);
  const data = png.toString('base64');
  assert(data.length > 3000000, `Expected >3m Base64 chars, got ${data.length}`);
  save('high-entropy-1024x768.png', png);
  summary.image = { width, height, color: 'RGB8', compression: 'zlib level 6', png_bytes: png.length, base64_chars: data.length, sha256: sha(png) };
  return data;
}
async function listen(label, handler) {
  const server = http.createServer(handler);
  server.requestTimeout = timeout;
  await new Promise((yes, no) => { server.once('error', no); server.listen(0, '127.0.0.1', yes); });
  const address = `127.0.0.1:${server.address().port}`;
  record('listener', address, `owned HTTP server.close/closeAllConnections (pid ${process.pid})`, { label, pid: process.pid });
  resources.push(async () => {
    server.closeAllConnections();
    await new Promise((yes, no) => server.close(error => error ? no(error) : yes()));
    record('closed-listener', address, 'none; socket closed', { label });
  });
  return { server, url: `http://${address}` };
}
async function readBody(request) {
  const chunks = []; let size = 0;
  for await (const chunk of request) {
    size += chunk.length; assert(size <= 32 * 1024 * 1024, 'Fixture body exceeds 32 MiB'); chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks);
  const encoding = request.headers['content-encoding'];
  if (encoding === 'gzip') return gunzipSync(raw);
  if (encoding === 'zstd') {
    if (process.versions.bun) return Buffer.from(Bun.zstdDecompressSync(raw));
    assert(nodeZstdDecompress, 'Codex zstd requires Node >=23.8 or Bun');
    return nodeZstdDecompress(raw);
  }
  assert(!encoding || encoding === 'identity', `Unsupported capture encoding: ${encoding}`);
  return raw;
}
function json(response, value, status = 200) {
  response.writeHead(status, { 'Content-Type': 'application/json', Connection: 'close' });
  response.end(JSON.stringify(value));
}
function responsePayload(protocol, model, id) {
  if (protocol === 'chat') return { id, object: 'chat.completion', created: 1700000000, model, choices: [{ index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' }], usage: { prompt_tokens: 1500, completion_tokens: 1, total_tokens: 1501, prompt_tokens_details: { cached_tokens: 0 } } };
  if (protocol === 'gemini') return { responseId: id, modelVersion: model, candidates: [{ content: { role: 'model', parts: [{ text: 'ok' }] }, index: 0, finishReason: 'STOP' }], usageMetadata: { promptTokenCount: 1500, candidatesTokenCount: 1, totalTokenCount: 1501 } };
  return { id, object: 'response', created_at: 1700000000, status: 'completed', model, error: null, incomplete_details: null, output: [{ type: 'message', id: `msg_${id}`, role: 'assistant', status: 'completed', content: [{ type: 'output_text', text: 'ok', annotations: [] }] }], usage: { input_tokens: 1500, input_tokens_details: { cached_tokens: 0 }, output_tokens: 1, output_tokens_details: { reasoning_tokens: 0 }, total_tokens: 1501 } };
}
function hasImage(body, protocol) {
  if (protocol === 'gemini') return (body.generateContentRequest?.contents ?? body.contents)?.some(content => content.parts?.some(part => (part.inlineData?.data || part.inline_data?.data) && (part.inlineData?.mimeType || part.inline_data?.mime_type)?.startsWith('image/')));
  const messages = protocol === 'chat' ? body.messages : body.input;
  return Array.isArray(messages) && messages.some(message => Array.isArray(message.content) && message.content.some(part => part.type === 'input_image' || part.type === 'image_url'));
}
async function fixture(request, response) {
  try {
    const path = new URL(request.url, 'http://127.0.0.1').pathname;
    if (request.method === 'GET' && path === '/catalog.json') return json(response, catalog());
    assert(request.method === 'POST', `Unexpected fixture method/path: ${request.method} ${path}`);
    const raw = await readBody(request);
    const body = JSON.parse(raw);
    const protocol = path === '/v1/responses' ? 'responses' : path === '/v1/chat/completions' ? 'chat' : path.startsWith(`/v1beta/models/${MODELS.gemini}:`) ? 'gemini' : null;
    assert(protocol, `Unexpected provider path ${path}`);
    const provider = protocol === 'gemini' ? 'gemini' : 'openai';
    assert.equal(provider === 'openai' ? request.headers.authorization : request.headers['x-goog-api-key'], provider === 'openai' ? 'Bearer sk-loopback-image-fixture' : 'AIza-loopback-image-fixture');
    assert.equal(provider === 'gemini' ? MODELS.gemini : body.model, MODELS[provider]);
    const id = `resp_image_${++responseCounter}`;
    const call = { path, protocol, provider, id, sha256: sha(raw), bytes: raw.length, image: Boolean(hasImage(body, protocol)) };
    providerCalls.push(call);
    save(`provider-${responseCounter}.json`, raw);
    const payload = responsePayload(protocol, MODELS[provider], id);
    const stream = body.stream || path.endsWith(':streamGenerateContent');
    if (!stream) return json(response, payload);
    response.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'close' });
    if (protocol === 'responses') {
      const events = [{ type: 'response.created', sequence_number: 0, response: { ...payload, status: 'in_progress', output: [], usage: null } }, { type: 'response.output_text.delta', sequence_number: 1, item_id: `msg_${id}`, output_index: 0, content_index: 0, delta: 'ok', logprobs: [] }, { type: 'response.completed', sequence_number: 2, response: payload }];
      for (const event of events) response.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
    } else if (protocol === 'gemini') response.write(`data: ${JSON.stringify(payload)}\n\n`);
    else throw new Error('Streaming Chat not requested by this harness');
    response.end();
  } catch (error) { providerError = error; json(response, { error: String(error) }, 400); }
}
async function waitFor(description, check, child) {
  const deadline = Date.now() + timeout; let last;
  while (Date.now() < deadline) {
    assert(!child || child.exitCode === null, `Server exited during ${description}`);
    if (providerError) throw providerError;
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      const code = error.code ?? error.cause?.code;
      const transient = ['SQLITE_BUSY', 'SQLITE_LOCKED', 'ECONNREFUSED'].includes(code) || [5, 6].includes(error.errcode);
      if (!transient) throw error;
      last = error;
    }
    await new Promise(yes => setTimeout(yes, 100));
  }
  throw new Error(`Timed out waiting for ${description}: ${last ?? 'condition never observed'}`);
}
function childProcess(command, argv, environment, name) {
  const log = openSync(join(scratch, `${name}.log`), 'w', 0o600);
  const child = spawn(command, argv, { cwd: scratch, env: environment, stdio: ['ignore', log, log] });
  closeSync(log);
  const done = new Promise((yes, no) => { child.once('error', no); child.once('close', (code, signal) => yes({ code, signal })); });
  // Register cleanup before any readiness/version request can fail.
  resources.push(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
    let timer;
    try {
      await Promise.race([done, new Promise((_, no) => { timer = setTimeout(() => no(new Error(`${name} shutdown timeout`)), 20000); })]);
    } catch { child.kill('SIGKILL'); await done.catch(() => {}); }
    finally { clearTimeout(timer); }
    if (child.pid) record('closed-process', child.pid, 'none; process reaped', { name });
  });
  if (child.pid) record('process', child.pid, `kill -TERM ${child.pid} # confirm command still matches ${quote(command)} ${argv.map(quote).join(' ')}`, { name, command: [command, ...argv] });
  return { child, done };
}
async function boundedChild(command, argv, environment, name) {
  const running = childProcess(command, argv, environment, name);
  let timer;
  try {
    return await Promise.race([running.done, new Promise((_, no) => { timer = setTimeout(() => { running.child.kill('SIGTERM'); no(new Error(`${name} timed out`)); }, timeout); })]);
  } finally { clearTimeout(timer); }
}
function findExecutable(name, override) {
  const candidates = override ? [resolve(override)] : (process.env.PATH ?? '').split(':').map(directory => join(directory, name));
  for (const candidate of candidates) {
    try { accessSync(candidate, constants.X_OK); if (statSync(candidate).isFile()) return realpathSync(candidate); } catch {}
  }
  return null;
}
async function openEvidence(path) {
  if (process.versions.bun) {
    const { Database } = await import('bun:sqlite');
    return new Database(path, { readonly: true });
  }
  const { DatabaseSync } = await import('node:sqlite');
  return new DatabaseSync(path, { readOnly: true });
}
function evidence(key, principal, upstream, model) {
  const events = db.prepare('SELECT request_id, key_id, principal_id, upstream_id, model, list_status AS status, input_tokens, cache_read_input_tokens, output_tokens FROM request_events_v1 WHERE key_id = ?').all(key);
  const buckets = db.prepare('SELECT bucket_width_secs AS width, SUM(requests) AS requests, SUM(input_tokens) AS input, SUM(output_tokens) AS output FROM api_key_usage_buckets_v1 WHERE key_id = ? GROUP BY bucket_width_secs').all(key);
  if (events.length === 0 || buckets.length === 0) return null;
  assert.equal(events.length, 1, 'Exactly one durable terminal event');
  // A 3600-second limit persists in 60-second buckets (durable_bucket_width).
  assert.deepEqual(buckets.map(bucket => bucket.width), [60], 'The configured one-hour input limit has one durable bucket width');
  const event = events[0];
  for (const [field, value] of Object.entries({ key_id: key, principal_id: principal, upstream_id: upstream, model, status: 200, input_tokens: 1500, cache_read_input_tokens: 0, output_tokens: 1 })) assert.equal(event[field], value, `Durable event ${field}`);
  for (const bucket of buckets) assert.deepEqual({ ...bucket }, { width: bucket.width, requests: 1, input: 1500, output: 1 }, 'Durable input limit settlement must use provider usage, not estimated image quantity');
  return { event: { ...event }, buckets: buckets.map(bucket => ({ ...bucket })) };
}

async function main() {
  assert(!inside(repository, realpathSync(tmpdir())), 'TMPDIR must be outside repository');
  scratch = mkdtempSync(join(tmpdir(), 'cc-lb-image-admission-'));
  const owner = randomBytes(16).toString('hex');
  record('scratch-directory', scratch, `rm -rf -- ${quote(scratch)}`, { owner, inode: statSync(scratch).ino });
  save('.image-harness-owner', owner);
  console.log(`Resource ledger: ${ledger}; retained scratch: ${scratch}`);
  const baseEnvironment = { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: scratch, TMPDIR: scratch, NO_PROXY: '127.0.0.1,localhost,::1', LANG: 'C.UTF-8', TERM: 'dumb', NO_COLOR: '1' };
  // Import after guards, with no real ambient SDK credentials or proxy settings.
  for (const name of Object.keys(process.env)) if (/(?:KEY|TOKEN|SECRET|CREDENTIAL|PROXY|GOOGLE|VERTEX|OPENAI|GEMINI|AZURE|AWS)/i.test(name)) delete process.env[name];
  process.env.HOME = scratch;
  summary.sdk_versions = { openai: packageVersion('openai'), '@google/genai': packageVersion('@google/genai') };
  summary.binary_sha256 = sha(readFileSync(binary));
  const { default: OpenAI } = await import(pathToFileURL(sdkRequire.resolve('openai')).href);
  const { GoogleGenAI } = await import(pathToFileURL(sdkRequire.resolve('@google/genai')).href);
  const data = image();
  const provider = await listen('provider/catalog', fixture);
  const claims = [];
  for (const label of ['proxy', 'admin', 'metrics']) claims.push(await listen(`port-claim-${label}`, (_, response) => json(response, {}, 503)));
  const ports = claims.map(claim => claim.server.address().port);
  const proxy = `http://127.0.0.1:${ports[0]}`;
  const adminBase = `http://127.0.0.1:${ports[1]}`;
  const token = randomBytes(32).toString('hex');
  save('prices.json', JSON.stringify(catalog()));
  save('config.toml', `[runtime]\ndata_dir = ${JSON.stringify(join(scratch, 'data'))}\n[listener]\nproxy_addr = "127.0.0.1:${ports[0]}"\nadmin_addr = "127.0.0.1:${ports[1]}"\nmetrics_addr = "127.0.0.1:${ports[2]}"\n[storage]\nkind = "sqlite"\npath = ${JSON.stringify(join(scratch, 'storage.sqlite'))}\n[aead]\nkey_env = "CC_LB_MASTER_KEY"\n[price_catalog]\ncache_path = ${JSON.stringify(join(scratch, 'prices.json'))}\nurl = "${provider.url}/catalog.json"\n[observability]\ntracing_level = "info"\n[[admin.auth.providers]]\nkind = "static_token"\nid = "image-fixture"\ntoken_env = "CC_LB_ADMIN_TOKEN"\n`);
  // Last-created resources are exactly the three reservation listeners.
  for (let i = 0; i < claims.length; i++) await resources.pop()();
  const running = childProcess(binary, ['serve', '--config', join(scratch, 'config.toml')], { ...baseEnvironment, CC_LB_MASTER_KEY: randomBytes(32).toString('hex'), CC_LB_ADMIN_TOKEN: token }, 'server');
  await waitFor('admin health and installed frozen catalog', async () => {
    const response = await guardedFetch(`${adminBase}/admin/health`);
    return response.ok && readFileSync(join(scratch, 'server.log'), 'utf8').includes('installed latest local LiteLLM price catalog');
  }, running.child);
  db = await openEvidence(join(scratch, 'storage.sqlite'));
  async function admin(path, body) {
    const response = await guardedFetch(adminBase + path, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const text = await response.text(); assert(response.ok, `Admin ${path}: ${response.status} ${text}`); return JSON.parse(text);
  }
  const upstreams = {};
  for (const providerName of ['openai', 'gemini']) {
    const upstream = await admin('/admin/v1/upstreams', { name: `image-${providerName}`, kind: `${providerName}_api_key`, base_url: provider.url, api_key_value: providerName === 'openai' ? 'sk-loopback-image-fixture' : 'AIza-loopback-image-fixture', skip_preflight: true, warmup_enabled: false });
    upstreams[providerName] = upstream.id;
  }
  async function managedCase(label, providerName, expectImage, invoke) {
    const model = MODELS[providerName];
    const principal = await admin('/admin/v1/principals', { name: label, kind: 'machine', allowed_models: [model], allowed_upstreams: [upstreams[providerName]], default_limits: [{ kind: 'input_tokens', window_secs: 3600, cap_micros: 100000 }] });
    const key = await admin(`/admin/v1/principals/${principal.id}/keys`, { label });
    const before = providerCalls.length;
    activeWire = [];
    let result; let wire;
    try { result = await invoke(key.plaintext_key); wire = activeWire; } finally { activeWire = null; }
    assert.equal(providerCalls.length, before + 1, `${label}: must reach provider exactly once`);
    const call = providerCalls.at(-1);
    assert.equal(call.image, expectImage, `${label}: provider image presence`);
    assert.equal(call.provider, providerName);
    assert.equal(wire.length, 1, `${label}: exactly one client POST`);
    assert.equal(call.sha256, wire[0].sha256, `${label}: provider body must match SDK/native request bytes unchanged`);
    if (result?.id) assert.equal(result.id, call.id, `${label}: SDK response must originate at provider`);
    const durable = await waitFor(`${label}: one terminal event and durable 1500/1 bucket`, () => evidence(key.key_id, principal.id, upstreams[providerName], model), running.child);
    const item = { case: label, verdict: 'PASS', input_limit: 100000, wire: { ...call, client_sha256: wire[0].sha256 }, ...durable };
    summary.cases.push(item);
    console.log(`PASS ${label}: unchanged wire; provider usage 1500/1 durably settled`);
    return item;
  }
  for (const withImage of args.cliOnly ? [] : [false, true]) {
    const label = withImage ? 'image' : 'healthy-text-control';
    await managedCase(`openai-responses-${label}`, 'openai', withImage, async key => {
      const client = new OpenAI({ apiKey: key, baseURL: `${proxy}/openai/v1`, maxRetries: 0, fetch: guardedFetch });
      return client.responses.create({ model: MODELS.openai, max_output_tokens: 16, input: [{ role: 'user', content: [{ type: 'input_text', text: `image-admission ${label}: say ok` }, ...(withImage ? [{ type: 'input_image', image_url: `data:image/png;base64,${data}`, detail: 'high' }] : [])] }] });
    });
    await managedCase(`openai-chat-${label}`, 'openai', withImage, async key => {
      const client = new OpenAI({ apiKey: key, baseURL: `${proxy}/openai/v1`, maxRetries: 0, fetch: guardedFetch });
      return client.chat.completions.create({ model: MODELS.openai, max_tokens: 16, messages: [{ role: 'user', content: [{ type: 'text', text: `image-admission ${label}: say ok` }, ...(withImage ? [{ type: 'image_url', image_url: { url: `data:image/png;base64,${data}`, detail: 'high' } }] : [])] }] });
    });
    for (const stream of [false, true]) {
      await managedCase(`gemini-${stream ? 'stream' : 'generate'}-${label}`, 'gemini', withImage, async key => {
        const client = new GoogleGenAI({ apiKey: key, httpOptions: { baseUrl: `${proxy}/gemini`, apiVersion: 'v1beta' } });
        const request = { model: MODELS.gemini, contents: [{ role: 'user', parts: [{ text: `image-admission ${label}: say ok` }, ...(withImage ? [{ inlineData: { mimeType: 'image/png', data } }] : [])] }], config: { maxOutputTokens: 16 } };
        let response;
        if (stream) { for await (const chunk of await client.models.generateContentStream(request)) response = chunk; }
        else response = await client.models.generateContent(request);
        assert.equal(response.text, 'ok');
        return { id: response.responseId };
      });
    }
  }
  await captureCli({ baseEnvironment, managedCase, proxy });
  assert(!providerError, String(providerError));
  summary.verdict = 'PASS';
}

async function captureCli({ baseEnvironment, managedCase, proxy }) {
  const isolated = Object.values(networkInterfaces()).flat().every(address => address.internal);
  const sandbox = process.platform === 'darwin' ? findExecutable('sandbox-exec', '/usr/bin/sandbox-exec') : null;
  const sandboxProfile = join(scratch, 'cli-loopback.sb');
  if (sandbox) save('cli-loopback.sb', '(version 1)\n(allow default)\n(deny network*)\n(allow network-outbound (remote ip "localhost:*"))\n(allow network-inbound (local ip "localhost:*"))\n');
  for (const name of ['codex', 'gemini']) {
    const executable = findExecutable(name, args[`${name}-bin`]);
    if (!args.cli || !executable || (!isolated && !sandbox)) {
      summary.cli.push({ client: name, verdict: 'NOT_RUN', reason: !args.cli ? '--cli not requested' : !executable ? 'CLI not installed' : 'requires macOS sandbox-exec or an isolated loopback-only network namespace; no CLI process started' });
      continue;
    }
    const home = join(scratch, `${name}-home`);
    mkdirSync(home, { mode: 0o700 });
    const environment = { ...baseEnvironment, HOME: home, XDG_CONFIG_HOME: join(home, '.config'), XDG_CACHE_HOME: join(home, '.cache'), XDG_DATA_HOME: join(home, '.local/share') };
    const runCli = (argv, label) => boundedChild(sandbox ?? executable, sandbox ? ['-f', sandboxProfile, executable, ...argv] : argv, environment, label);
    await runCli(['--version'], `${name}-version`);
    const version = readFileSync(join(scratch, `${name}-version.log`), 'utf8').trim();
    let captured;
    const captures = [];
    const capture = await listen(`${name}-native-wire-capture`, async (request, response) => {
      try {
        const path = new URL(request.url, 'http://127.0.0.1').pathname;
        if (request.method !== 'POST') return json(response, { models: [] });
        const raw = await readBody(request); const body = JSON.parse(raw);
        const protocol = name === 'codex' ? 'responses' : 'gemini';
        const evidence = { path, sha256: sha(raw), protocol, bytes: raw.length, image: Boolean(hasImage(body, protocol)), operation: path.endsWith(':countTokens') ? 'countTokens' : 'generation' };
        captures.push(evidence);
        save(`${name}-native-request-${captures.length}.json`, raw);
        save(`${name}-native-captures.json`, JSON.stringify(captures, null, 2));
        if (evidence.image && evidence.operation === 'generation') {
          captured ??= { ...evidence, body, raw };
          save(`${name}-native-request.json`, raw);
        }
        if (path.endsWith(':countTokens')) return json(response, { totalTokens: 100 });
        json(response, { error: { code: 400, status: 'INVALID_ARGUMENT', message: 'Controlled image wire capture stop; not inference success' } }, 400);
      } catch (error) { providerError = error; json(response, { error: String(error) }, 400); }
    });
    let argv;
    if (name === 'codex') {
      const codexHome = join(home, '.codex'); mkdirSync(codexHome);
      writeFileSync(join(codexHome, 'config.toml'), `model_provider = "image_loopback"\nmodel = "${MODELS.openai}"\napproval_policy = "never"\nsandbox_mode = "read-only"\ncheck_for_update_on_startup = false\ncli_auth_credentials_store = "file"\nmcp_oauth_credentials_store = "file"\n[features]\nplugins = false\nremote_plugin = false\napps = false\n[analytics]\nenabled = false\n[model_providers.image_loopback]\nname = "image-loopback"\nbase_url = "${capture.url}/v1"\nwire_api = "responses"\nenv_key = "IMAGE_LOOPBACK_KEY"\nsupports_websockets = false\nrequires_openai_auth = false\nrequest_max_retries = 0\nstream_max_retries = 0\n`, { mode: 0o600 });
      environment.CODEX_HOME = codexHome;
      environment.IMAGE_LOOPBACK_KEY = 'sk-loopback-codex-capture';
      // --image accepts multiple values; -- prevents it swallowing the prompt.
      argv = ['exec', '--skip-git-repo-check', '--image', join(scratch, 'high-entropy-1024x768.png'), '--', 'Describe the attached image in one word. Do not use tools.'];
    } else {
      mkdirSync(join(home, '.gemini'), { recursive: true, mode: 0o700 });
      writeFileSync(join(home, '.gemini/settings.json'), JSON.stringify({ security: { auth: { selectedType: 'gemini-api-key' } }, telemetry: { enabled: false }, privacy: { usageStatisticsEnabled: false }, general: { enableAutoUpdate: false } }), { mode: 0o600 });
      environment.GEMINI_API_KEY = 'AIza-loopback-gemini-cli-capture';
      environment.GOOGLE_GEMINI_BASE_URL = capture.url;
      environment.GOOGLE_GENAI_USE_VERTEXAI = 'false';
      argv = ['--skip-trust', '--model', MODELS.gemini, '--prompt', `Describe @${join(scratch, 'high-entropy-1024x768.png')} in one word. Do not use tools.`, '--output-format', 'json'];
    }
    const exit = await runCli(argv, `${name}-capture`);
    assert(captured, `${name} installed but native image shape was not captured; inspect ${name}-capture.log`);
    summary.cli.push({ client: name, version, verdict: 'WIRE_SHAPE_ONLY', isolation: sandbox ? 'macOS sandbox-exec: deny network except localhost' : 'loopback-only network namespace', exit, captures, path: captured.path, native_sha256: captured.sha256, bytes: captured.raw.length, note: 'Actual CLI image builder; controlled 400 after capture, not successful CLI inference; countTokens captures are wire evidence only' });
    const providerName = name === 'codex' ? 'openai' : 'gemini';
    // Replay the actual client-produced bytes, not a fabricated equivalent shape.
    const result = await managedCase(`${name}-captured-native-replay`, providerName, true, async key => {
      const response = await guardedFetch(`${proxy}/${providerName}${captured.path}`, { method: 'POST', headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json', ...(name === 'gemini' ? { 'x-goog-api-key': key } : {}) }, body: captured.raw });
      const body = await response.text();
      assert(response.ok, `${name} captured replay rejected: ${response.status} ${body.slice(0, 1000)}`);
      return {};
    });
    summary.cli.at(-1).replay = result.case;
  }
}

try {
  await main();
} catch (error) {
  summary.error = error.stack ?? String(error);
  process.exitCode = 1;
} finally {
  db?.close();
  for (const cleanup of resources.reverse()) {
    try { await cleanup(); } catch (error) { summary.cleanup_error = String(error); summary.verdict = 'FAIL'; process.exitCode = 1; }
  }
  if (scratch) save('summary.json', `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify(summary, null, 2));
  if (scratch) console.log(`Retained evidence: ${scratch}; resource ledger: ${ledger}`);
}
