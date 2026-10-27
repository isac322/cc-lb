// Shared runtime for actual-CLI + real-provider cc-lb QA slices.
// Scope: scratch-only. Fresh SQLite per slice, API-key upstreams only, no OAuth,
// no cleanup (resources are recorded in the parent ledger and retained).
import { spawn, execFile } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { createReadStream, existsSync, openSync, closeSync } from 'node:fs';
import { mkdir, readFile, writeFile, appendFile, readdir, stat, rmdir, chmod, rename } from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const ROOT = path.dirname(fileURLToPath(import.meta.url));
export const LEDGER_PATH = path.join(ROOT, 'ledger.json');
export const BINARY_PATH = path.resolve(ROOT, '../../../target/debug/cc-lb');
export const EXPECTED_BINARY_SHA256 = '6a2e215ff96a33f58e1f221ba45dc7225cc2dce18be8c4375c0d72d72bef5ecd';
export const CREDENTIALS_PATH = path.join(ROOT, 'credentials.json');

// Numerical bounds (parent safety review). Drivers MUST respect these; the
// runtime enforces what it can (server limits, budget gate, run timeouts).
export const CLIENT_LIMITS = Object.freeze({
  maxTurnsPerSession: 2,
  turnTimeoutSecs: 120,
  maxGenerationAttempts: 16,
  maxOutputTokens: 2000,
  sliceCostUsd: 3,
  campaignCostUsd: 15, // contract C655: 4 CLI slices + 1 fault slice, each $3 / 16 generations
});

// cc_lb_storage_api::principal::Limit = {kind, window_secs, cap_micros}.
// kind ∈ requests|input_tokens|output_tokens|total_tokens|cost_usd|concurrent.
// requests cap is a count; cost_usd cap is USD micros. Window must be <= 604800.
// cost_usd fails closed (RejectReason::CostUnavailable) when the price catalog
// cannot price the model, so it is the server-side "safe block", never silent.
// NOTE: limit engine admits cap/10 (min 1) until durable usage is refreshed
// (cold allowance); this can only reject earlier, never overspend.
export const SERVER_LIMIT_WINDOW_SECS = 86_400;
// Hard ceiling for per-slice attempt overrides. Codex alone may go to 64
// (Main-approved); every other slice stays <= 32. USD caps never change.
export const MAX_ATTEMPTS_OVERRIDE = 32;
export const MAX_ATTEMPTS_OVERRIDE_BY_SLICE = Object.freeze({ codex: 64 });

export function sliceClientLimits(opts = {}, sliceName) {
  const n = opts.maxGenerationAttempts;
  if (n === undefined || n === CLIENT_LIMITS.maxGenerationAttempts) return CLIENT_LIMITS;
  const ceiling = MAX_ATTEMPTS_OVERRIDE_BY_SLICE[sliceName] ?? MAX_ATTEMPTS_OVERRIDE;
  if (!Number.isInteger(n) || n < 1 || n > ceiling) {
    throw new Error(`maxGenerationAttempts override for slice '${sliceName}' must be an integer 1..${ceiling}`);
  }
  // Only the attempt count may change; USD slice/campaign caps stay fixed.
  return Object.freeze({ ...CLIENT_LIMITS, maxGenerationAttempts: n });
}

export function serverLimitSpec(limits = CLIENT_LIMITS) {
  return [
    { kind: 'requests', window_secs: SERVER_LIMIT_WINDOW_SECS, cap_micros: limits.maxGenerationAttempts },
    { kind: 'cost_usd', window_secs: SERVER_LIMIT_WINDOW_SECS, cap_micros: Math.round(CLIENT_LIMITS.sliceCostUsd * 1_000_000) },
  ];
}

function sameLimits(a = [], b = []) {
  const key = (l) => `${l.kind}|${l.window_secs}|${l.cap_micros}`;
  return JSON.stringify(a.map(key).sort()) === JSON.stringify(b.map(key).sort());
}

const CRITICAL_DROP_REASONS = ['lifecycle_assembler_full', 'storage_error', 'subscription_quota_full'];
const DROP_METRIC_RE = /(drop|_full|lagged|storage_error|overflow|queue_full)/i;
const SECRET_KEY_RE = /(api[_-]?key(?!_id)|token|secret|authorization|password|plaintext|master)/i;

const secrets = new Set();
const credentialCache = new Map();

function remember(secret) {
  if (typeof secret === 'string' && secret.length >= 8) secrets.add(secret);
  return secret;
}

export function redactText(text) {
  let out = String(text);
  for (const s of secrets) out = out.split(s).join('[REDACTED]');
  return out;
}

export function redact(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return redactText(value);
  if (Array.isArray(value)) return value.map(redact);
  if (typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = SECRET_KEY_RE.test(k) && typeof v === 'string' && v.length > 0 ? '[REDACTED]' : redact(v);
    }
    return out;
  }
  return value;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowIso() {
  return new Date().toISOString();
}

async function writePrivate(file, content) {
  await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  await writeFile(file, content, { mode: 0o600 });
  await chmod(file, 0o600);
}

async function writeEvidence(file, content) {
  await mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  await writeFile(file, redactText(content), { mode: 0o600 });
}

function execFileText(cmd, args, { timeoutMs = 30_000, env } = {}) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout: timeoutMs, maxBuffer: 1 << 20, env: env ?? process.env }, (error, stdout, stderr) => {
      if (error) {
        const reason = redactText(String(stderr || error.message)).slice(0, 500);
        reject(new Error(`${path.basename(cmd)} failed: ${reason}`));
        return;
      }
      resolve(String(stdout));
    });
  });
}

export async function sha256File(file) {
  const hash = createHash('sha256');
  await new Promise((resolve, reject) => {
    createReadStream(file).on('data', (c) => hash.update(c)).on('end', resolve).on('error', reject);
  });
  return hash.digest('hex');
}

function artifactFailure(code, detail, cause) {
  const error = new Error(`BLOCKED:${code}: ${detail}`, cause ? { cause } : undefined);
  error.code = code;
  return error;
}

function runtimeArtifact(value) {
  if (value === undefined || value === null) throw artifactFailure('ARTIFACT_REQUIRED', 'an explicit runtime artifact is required');
  if (typeof value !== 'object' || Array.isArray(value) || typeof value.path !== 'string'
      || !path.isAbsolute(value.path) || value.path.includes('\0') || !/^[0-9a-f]{64}$/.test(value.expected_sha256 ?? '')) {
    throw artifactFailure('ARTIFACT_INVALID', 'artifact must contain an absolute path and SHA-256');
  }
  return Object.freeze({ path: value.path, expected_sha256: value.expected_sha256 });
}

async function verifyRuntimeArtifact(artifact) {
  let actual;
  try { actual = await sha256File(artifact.path); }
  catch (cause) { throw artifactFailure('ARTIFACT_UNREADABLE', 'the selected executable cannot be read', cause); }
  if (actual !== artifact.expected_sha256) throw artifactFailure('ARTIFACT_SHA_MISMATCH', 'the selected executable changed or does not match its contract');
  return actual;
}

// ---------------------------------------------------------------- ledger

async function withLedgerLock(fn) {
  const lock = `${LEDGER_PATH}.lock`;
  const deadline = Date.now() + 15_000;
  for (;;) {
    try {
      await mkdir(lock);
      break;
    } catch (error) {
      if (error.code !== 'EEXIST') throw error;
      if (Date.now() > deadline) throw new Error(`ledger lock busy: ${lock}`);
      await sleep(50 + Math.floor(Math.random() * 100));
    }
  }
  try {
    return await fn();
  } finally {
    await rmdir(lock).catch(() => {});
  }
}

export async function recordResource(ctx, entry) {
  const record = {
    created_at: nowIso(),
    status: 'created',
    owner_slice: ctx?.name ?? null,
    ...redact(entry),
  };
  if (!record.kind || !record.id || !record.delete) {
    throw new Error('recordResource requires kind, id, delete');
  }
  await withLedgerLock(async () => {
    const ledger = JSON.parse(await readFile(LEDGER_PATH, 'utf8'));
    ledger.entries = Array.isArray(ledger.entries) ? ledger.entries : [];
    ledger.entries.push(record);
    await writeFile(LEDGER_PATH, `${JSON.stringify(ledger, null, 2)}\n`);
  });
  if (ctx?.root) {
    await appendFile(path.join(ctx.root, 'resources.jsonl'), `${JSON.stringify(record)}\n`, { mode: 0o600 });
  }
  return record;
}

// ---------------------------------------------------------- credentials

async function loadDescriptor() {
  if (!existsSync(CREDENTIALS_PATH)) throw new Error(`BLOCKED: credential descriptor missing at ${CREDENTIALS_PATH}`);
  return JSON.parse(await readFile(CREDENTIALS_PATH, 'utf8'));
}

function parseEnvFile(text, key) {
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const m = line.match(/^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
    if (!m || m[1] !== key) continue;
    let v = m[2].trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    else v = v.replace(/\s+#.*$/, '');
    return v;
  }
  return null;
}

// Typed credential error. `code` is stable; message never contains secrets.
export class CredentialError extends Error {
  constructor(code, message) {
    super(`BLOCKED[${code}]: ${redactText(message)}`);
    this.name = 'CredentialError';
    this.code = code;
  }
}

function runPiped(cmd, args, input, { timeoutMs = 30_000, env } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['pipe', 'pipe', 'pipe'], env: env ?? { PATH: process.env.PATH ?? '/usr/bin:/bin' } });
    const out = [];
    const err = [];
    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs);
    child.stdout.on('data', (c) => out.push(c));
    child.stderr.on('data', (c) => err.push(c));
    child.on('error', (e) => { clearTimeout(timer); reject(e); });
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code, stdout: Buffer.concat(out).toString('utf8'), stderr: Buffer.concat(err).toString('utf8') });
    });
    child.stdin.end(input ?? '');
  });
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const IDENT_RE = /^[a-z_][a-z0-9_]*$/;

// Decrypt script executed by a native Node (Bun's node:crypto lacks chacha20-poly1305).
// Input JSON on stdin; plaintext on stdout pipe only.
const CHACHA_DECRYPT_SCRIPT = `
const c=require('crypto');let b='';process.stdin.on('data',d=>b+=d).on('end',()=>{
try{const i=JSON.parse(b);const m=Buffer.from(i.master,'hex');const x=Buffer.from(i.cipher,'hex');
const n=x.subarray(0,12),t=x.subarray(x.length-16),ct=x.subarray(12,x.length-16);
const d=c.createDecipheriv('chacha20-poly1305',m,n,{authTagLength:16});d.setAAD(Buffer.from(i.aadHex,'hex'));d.setAuthTag(t);
process.stdout.write(Buffer.concat([d.update(ct),d.final()]).toString('utf8'));}
catch(e){process.stderr.write(String(e&&e.code||'DECRYPT_FAILED'));process.exit(3);}});`;

let nativeNodeCache;
async function resolveNativeNode() {
  if (nativeNodeCache !== undefined) return nativeNodeCache;
  const candidates = [];
  if (!globalThis.Bun && process.execPath) candidates.push(process.execPath);
  for (const dir of (process.env.PATH ?? '').split(':')) if (dir) candidates.push(path.join(dir, 'node'));
  for (const bin of [...new Set(candidates)]) {
    if (!existsSync(bin)) continue;
    try {
      const r = await runPiped(bin, ['-e', "process.stdout.write(String(!process.versions.bun && require('crypto').getCiphers().includes('chacha20-poly1305')))"], '', { timeoutMs: 10_000 });
      if (r.code === 0 && r.stdout.trim() === 'true') { nativeNodeCache = bin; return bin; }
    } catch { /* try next */ }
  }
  nativeNodeCache = null;
  return null;
}

async function decryptChaCha(masterHex, cipherHex, aadHex) {
  const nodeBin = await resolveNativeNode();
  if (!nodeBin) throw new CredentialError('DECRYPT_RUNTIME_UNAVAILABLE', 'no native Node with chacha20-poly1305 found on PATH (Bun crypto lacks it)');
  const r = await runPiped(nodeBin, ['-e', CHACHA_DECRYPT_SCRIPT], JSON.stringify({ master: masterHex, cipher: cipherHex, aadHex }));
  if (r.code !== 0) throw new CredentialError('DECRYPT_FAILED', `native node decrypt failed (${r.stderr.trim().slice(0, 80) || r.code})`);
  return r.stdout;
}

async function retrieveSqliteAead(source) {
  const { path: dbPath, table, column, upstreamId, masterEnvFile, masterEnvKey } = source;
  if (!UUID_RE.test(upstreamId ?? '')) throw new CredentialError('SOURCE_INVALID', 'sqliteAead upstreamId must be a UUID');
  if (!IDENT_RE.test(table ?? '') || !IDENT_RE.test(column ?? '')) throw new CredentialError('SOURCE_INVALID', 'sqliteAead table/column invalid');
  if (table !== 'upstream_api_key_secret_v1') throw new CredentialError('SOURCE_INVALID', 'sqliteAead may only read upstream_api_key_secret_v1 (API-key rows, never OAuth)');
  if (!existsSync(dbPath)) throw new CredentialError('SOURCE_UNAVAILABLE', `sqlite source missing: ${dbPath}`);
  let masterHex;
  try {
    masterHex = parseEnvFile(await readFile(masterEnvFile, 'utf8'), masterEnvKey);
  } catch {
    throw new CredentialError('SOURCE_UNAVAILABLE', `master env file unreadable: ${masterEnvFile}`);
  }
  if (!/^[0-9a-f]{64}$/i.test(masterHex ?? '')) throw new CredentialError('SOURCE_INVALID', 'master key is not 64 hex chars');
  remember(masterHex);
  const sql = `SELECT hex(${column}) AS cipher FROM ${table} WHERE upstream_id='${upstreamId}';`;
  const r = await runPiped('sqlite3', ['-readonly', '-json', `file:${dbPath}?mode=ro`, sql], '', { timeoutMs: 15_000 });
  if (r.code !== 0) throw new CredentialError('SOURCE_UNAVAILABLE', `sqlite3 read failed: ${r.stderr.trim().slice(0, 200)}`);
  let rows;
  try { rows = r.stdout.trim() ? JSON.parse(r.stdout) : []; } catch { throw new CredentialError('SOURCE_INVALID', 'sqlite3 output not JSON'); }
  if (rows.length !== 1 || typeof rows[0].cipher !== 'string' || rows[0].cipher.length < 2 * (12 + 16 + 1)) {
    throw new CredentialError('SOURCE_UNAVAILABLE', `expected exactly one API-key ciphertext row for ${upstreamId}, got ${rows.length}`);
  }
  // AAD = raw 16-byte UUID (verified against cc-lb storage), not the hyphenated UTF-8 id.
  return decryptChaCha(masterHex, rows[0].cipher, upstreamId.replaceAll('-', '').toLowerCase());
}

async function retrieveSecret(source) {
  switch (source?.type) {
    case 'sqliteAead':
      return retrieveSqliteAead(source);
    case 'envFile':
    case 'envfile': {
      const text = await readFile(source.path, 'utf8');
      return parseEnvFile(text, source.key);
    }
    case 'env': {
      return process.env[source.key] ?? null;
    }
    case 'gcloudApiKey': {
      let keyName = source.keyName;
      if (!keyName.includes('/')) {
        const listed = await execFileText('gcloud', [
          'services', 'api-keys', 'list', `--project=${source.project}`,
          `--filter=displayName=${keyName}`, '--format=value(name)',
        ]);
        const names = listed.split('\n').map((s) => s.trim()).filter(Boolean);
        if (names.length !== 1) throw new Error(`BLOCKED: gcloud key '${keyName}' resolved to ${names.length} keys`);
        keyName = names[0];
      }
      const out = await execFileText('gcloud', [
        'services', 'api-keys', 'get-key-string', keyName, `--project=${source.project}`, '--format=value(keyString)',
      ]);
      return out.trim() || null;
    }
    case 'opItem': {
      const out = await execFileText('op', ['item', 'get', source.item, '--fields', source.field, '--reveal']);
      return out.trim() || null;
    }
    default:
      throw new CredentialError('SOURCE_UNSUPPORTED', `unsupported credential source type ${source?.type}`);
  }
}

// Returns {kind, baseUrl, apiKey} with apiKey only in memory. API-key kinds only.
export async function getCredential(provider) {
  if (credentialCache.has(provider)) return credentialCache.get(provider);
  const descriptor = (await loadDescriptor())[provider];
  if (!descriptor) throw new CredentialError('DESCRIPTOR_MISSING', `no credential descriptor for provider ${provider}`);
  if (!/_api_key$/.test(descriptor.kind)) throw new CredentialError('NOT_API_KEY', `${provider} kind ${descriptor.kind} is not API-key only`);
  let apiKey;
  try {
    apiKey = await retrieveSecret(descriptor.source);
  } catch (error) {
    if (error instanceof CredentialError) throw error;
    throw new CredentialError('SOURCE_UNAVAILABLE', `${provider} credential retrieval failed: ${error.message}`);
  }
  apiKey = typeof apiKey === 'string' ? apiKey.trim() : apiKey;
  if (!apiKey) throw new CredentialError('SOURCE_EMPTY', `${provider} credential source returned no value`);
  remember(apiKey);
  const cred = Object.freeze({ kind: descriptor.kind, baseUrl: descriptor.baseUrl, apiKey });
  credentialCache.set(provider, cred);
  return cred;
}

// ------------------------------------------------------------ helpers

async function portInUse(port) {
  return new Promise((resolve) => {
    const socket = net.connect({ host: '127.0.0.1', port });
    socket.setTimeout(500);
    socket.once('connect', () => { socket.destroy(); resolve(true); });
    socket.once('timeout', () => { socket.destroy(); resolve(false); });
    socket.once('error', () => resolve(false));
  });
}

async function httpRequest(url, { method = 'GET', headers = {}, body, timeoutMs = 15_000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const init = { method, headers: { ...headers }, signal: controller.signal };
    if (body !== undefined) {
      init.body = typeof body === 'string' ? body : JSON.stringify(body);
      if (!Object.keys(init.headers).some((h) => h.toLowerCase() === 'content-type')) {
        init.headers['content-type'] = 'application/json';
      }
    }
    const res = await fetch(url, init);
    const text = await res.text();
    let parsed = text;
    try { parsed = text ? JSON.parse(text) : null; } catch { /* keep text */ }
    return { status: res.status, headers: Object.fromEntries(res.headers.entries()), body: parsed };
  } finally {
    clearTimeout(timer);
  }
}

export async function admin(ctx, apiPath, method = 'GET', body, headers = {}) {
  return httpRequest(`${ctx.adminUrl}${apiPath}`, {
    method,
    body,
    headers: { authorization: `Bearer ${ctx._adminToken}`, ...headers },
  });
}

function expectStatus(res, ok, what) {
  if (!ok.includes(res.status)) {
    throw new Error(`${what} failed: HTTP ${res.status} ${redactText(JSON.stringify(res.body)).slice(0, 400)}`);
  }
  return res.body;
}

function tomlString(s) {
  return JSON.stringify(String(s));
}

function renderConfig({ runtimeDir, ports, otlpEndpoint }) {
  return [
    '[runtime]',
    `data_dir = ${tomlString(path.join(runtimeDir, 'data'))}`,
    '',
    '[listener]',
    `proxy_addr = "127.0.0.1:${ports[0]}"`,
    `admin_addr = "127.0.0.1:${ports[1]}"`,
    `metrics_addr = "127.0.0.1:${ports[2]}"`,
    '',
    '[storage]',
    'kind = "sqlite"',
    `path = ${tomlString(path.join(runtimeDir, 'storage.sqlite'))}`,
    '',
    '[price_catalog]',
    `cache_path = ${tomlString(path.join(runtimeDir, 'price-catalog.json'))}`,
    '',
    '[aead]',
    'key_env = "CC_LB_MASTER_KEY"',
    '',
    '[observability]',
    'tracing_level = "info"',
    'request_diagnostics_log = true',
    ...(otlpEndpoint === undefined ? [] : [`otlp_endpoint = ${tomlString(otlpEndpoint)}`]),
    '',
    '[[admin.auth.providers]]',
    'kind = "static_token"',
    'id = "local"',
    'token_env = "CC_LB_ADMIN_TOKEN"',
    '',
  ].join('\n');
}

function serverEnv(runtimeDir, masterKey, adminToken, { otlpEndpoint } = {}) {
  return {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    HOME: path.join(runtimeDir, 'home'),
    TMPDIR: path.join(runtimeDir, 'tmp'),
    XDG_CACHE_HOME: path.join(runtimeDir, 'home', '.cache'),
    XDG_CONFIG_HOME: path.join(runtimeDir, 'home', '.config'),
    XDG_DATA_HOME: path.join(runtimeDir, 'home', '.local', 'share'),
    RUST_LOG: 'info',
    CC_LB_MASTER_KEY: masterKey,
    CC_LB_ADMIN_TOKEN: adminToken,
    ...(otlpEndpoint === undefined ? {} : { CC_LB_OBSERVABILITY__OTLP_ENDPOINT: otlpEndpoint }),
  };
}

async function tailLog(file, bytes = 4000) {
  try {
    const text = await readFile(file, 'utf8');
    return redactText(text.slice(-bytes));
  } catch {
    return '';
  }
}

// Bootstrap liveness: /healthz 200 (proxy) + /admin/health 200. A fresh DB has
// no principal/upstream, so /readyz is 503 by design until the admin seed runs.
async function waitLive(ctx, isAlive, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  let last = 'not attempted';
  while (Date.now() < deadline) {
    if (!isAlive()) throw new Error(`cc-lb pid ${ctx.pid} exited before live:\n${await tailLog(ctx.logPath)}`);
    try {
      const live = await httpRequest(`${ctx.proxyUrl}/healthz`, { timeoutMs: 2_000 });
      const adminHealth = await httpRequest(`${ctx.adminUrl}/admin/health`, { timeoutMs: 2_000 });
      if (live.status === 200 && adminHealth.status === 200) return { healthz: live.body, adminHealth: adminHealth.body };
      last = `healthz=${live.status} admin=${adminHealth.status}`;
    } catch (error) {
      last = error.message;
    }
    await sleep(250);
  }
  throw new Error(`cc-lb not live within ${deadlineMs}ms (${last}):\n${await tailLog(ctx.logPath)}`);
}

// Post-seed readiness: /readyz must be exactly 200; 503 is never success.
async function waitReady(ctx, isAlive, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  let last = 'not attempted';
  while (Date.now() < deadline) {
    if (!isAlive()) throw new Error(`cc-lb pid ${ctx.pid} exited before ready:\n${await tailLog(ctx.logPath)}`);
    try {
      const ready = await httpRequest(`${ctx.proxyUrl}/readyz`, { timeoutMs: 2_000 });
      if (ready.status === 200) return ready.body;
      last = `readyz=${ready.status} reason=${ready.body?.reason ?? 'n/a'}`;
    } catch (error) {
      last = error.message;
    }
    await sleep(250);
  }
  throw new Error(`cc-lb not ready within ${deadlineMs}ms after admin seed (${last}):\n${await tailLog(ctx.logPath)}`);
}

function pidAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; }
}

async function processCommand(pid) {
  const r = await runPiped('ps', ['-p', String(pid), '-o', 'command='], '', { timeoutMs: 5_000 });
  return r.code === 0 ? r.stdout.trim() : null;
}

function listOf(body, key) {
  if (Array.isArray(body)) return body;
  return Array.isArray(body?.[key]) ? body[key] : [];
}

// cc-lb pricing bootstrap (verified in source): the in-process installer
// (app.rs spawn_price_catalog_local_installer) runs at startup and every 60s and
// installs ONLY from the DB snapshot or, when the DB has none, the
// price_catalog.cache_path file (loader.rs install_latest_local). The network
// fetch is the `price_catalog_refresh` scheduler job (every 3600s), so a fresh
// DB has no LiteLLM prices for up to an hour. The harness preloads the cache
// file from the SAME official default feed the config uses (price_catalog.url is
// left at its default), records provenance, and then requires the server's own
// install log line. No rates are invented.
export const PRICE_CATALOG_URL = 'https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json';
const PRICE_INSTALL_LOG_LINE = 'installed latest local LiteLLM price catalog';

async function ensurePriceCatalogCache(ctx) {
  const provenancePath = path.join(ctx.runtimeDir, 'price-catalog.provenance.json');
  if (existsSync(ctx.priceCatalogPath)) {
    const bytes = await readFile(ctx.priceCatalogPath);
    JSON.parse(bytes.toString('utf8'));
    return (await readJson(provenancePath)) ?? { source: 'preexisting_cache_file_unknown_provenance', path: ctx.priceCatalogPath,
      sha256: createHash('sha256').update(bytes).digest('hex'), bytes: bytes.length };
  }
  const res = await fetch(PRICE_CATALOG_URL, { signal: AbortSignal.timeout(30_000) });
  if (res.status !== 200) throw new Error(`BLOCKED: official price catalog fetch HTTP ${res.status}`);
  const bytes = Buffer.from(await res.arrayBuffer());
  let catalog;
  try { catalog = JSON.parse(bytes.toString('utf8')); } catch { throw new Error('BLOCKED: official price catalog is not JSON'); }
  if (!catalog || typeof catalog !== 'object' || Object.keys(catalog).length < 100) {
    throw new Error('BLOCKED: official price catalog unexpectedly small');
  }
  const tmp = `${ctx.priceCatalogPath}.harness.tmp`;
  await writeFile(tmp, bytes, { mode: 0o600 });
  await rename(tmp, ctx.priceCatalogPath);
  const provenance = {
    source: 'official_default_price_catalog_url',
    url: PRICE_CATALOG_URL,
    fetchedAt: nowIso(),
    httpStatus: res.status,
    etag: res.headers.get('etag'),
    lastModified: res.headers.get('last-modified'),
    sha256: createHash('sha256').update(bytes).digest('hex'),
    bytes: bytes.length,
    entries: Object.keys(catalog).length,
    path: ctx.priceCatalogPath,
  };
  await writeEvidence(provenancePath, JSON.stringify(provenance, null, 2));
  return provenance;
}

async function logContains(file, needle) {
  try { return (await readFile(file, 'utf8')).includes(needle); } catch { return false; }
}

// Exact native LiteLLM key per provider. Vertex/bare or other prefixed/suffixed
// keys are NOT treated as the native model (they may price a different product).
const NATIVE_CATALOG = Object.freeze({
  openai: { key: (m) => m, litellmProvider: 'openai' },
  gemini: { key: (m) => `gemini/${m}`, litellmProvider: 'gemini' },
  // Native Anthropic Messages: bare model id with litellm_provider 'anthropic'
  // (Bedrock/Vertex 'anthropic.*' / 'vertex_ai/*' keys are not native).
  anthropic: { key: (m) => m, litellmProvider: 'anthropic' },
});

function tierPrices(entry, base) {
  const values = Object.entries(entry)
    .filter(([k, v]) => typeof v === 'number' && (k === base || k.startsWith(`${base}_`)) && !k.includes('batches'))
    .map(([, v]) => v);
  return { base: entry[base], max: values.length ? Math.max(...values) : entry[base] };
}

// Priced = every pricedModel has the exact native LiteLLM entry with input and
// output per-token prices in the cache file AND the server logged installing it.
async function inspectPriceCatalog(ctx, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  const native = NATIVE_CATALOG[ctx.provider];
  let fileResult = null;
  let installed = false;
  while (Date.now() < deadline) {
    if (!fileResult && existsSync(ctx.priceCatalogPath)) {
      try {
        const catalog = JSON.parse(await readFile(ctx.priceCatalogPath, 'utf8'));
        const prices = {};
        const missing = [];
        for (const model of ctx.pricedModels) {
          const key = native?.key(model);
          const entry = key ? catalog[key] : undefined;
          if (entry && entry.litellm_provider === native.litellmProvider
            && typeof entry.input_cost_per_token === 'number' && typeof entry.output_cost_per_token === 'number') {
            const input = tierPrices(entry, 'input_cost_per_token');
            const output = tierPrices(entry, 'output_cost_per_token');
            prices[model] = { key, input: input.base, output: output.base, inputMax: input.max, outputMax: output.max };
          } else {
            missing.push({ model, expectedKey: key ?? null });
          }
        }
        fileResult = { entries: Object.keys(catalog).length, prices, missing, filePriced: missing.length === 0 };
      } catch {
        // partially written; retry
      }
    }
    installed = await logContains(ctx.logPath, PRICE_INSTALL_LOG_LINE);
    if (fileResult && installed) break;
    await sleep(1_000);
  }
  return {
    available: Boolean(fileResult),
    installedByServer: installed,
    path: ctx.priceCatalogPath,
    provider: ctx.provider,
    models: ctx.pricedModels,
    provenance: ctx.priceCatalogProvenance ?? null,
    ...(fileResult ?? {}),
    modelPriced: Boolean(fileResult?.filePriced && installed),
  };
}

function hostOf(url) {
  try { return new URL(url).host; } catch { return null; }
}

// --------------------------------------------------------- prepareSlice

// opts: {name, ports:[proxy,admin,metrics], provider, model, pricedModels?,
//        artifact: {path, expected_sha256} (required, never inferred from legacy constants),
//        upstreams?: [{provider, name?, baseUrl?}] (default one upstream for provider),
//        serverLimits?: boolean (default true), resume?: boolean,
//        liveDeadlineMs?, readyDeadlineMs?, catalogDeadlineMs?, otlpEndpoint?: string}
// resume:true = explicit recovery of THIS slice's owned, still-running runtime:
// verifies parent-ledger PID ownership, process command line, binary sha and the
// exact rendered config, reuses persisted runtime.env secrets, never restarts,
// deletes or recreates the DB, and seeds only missing rows (matched by name).
export async function prepareSlice(opts) {
  const { name, ports, provider, model, otlpEndpoint } = opts;
  const artifact = runtimeArtifact(
    opts.artifact ?? { path: BINARY_PATH, expected_sha256: EXPECTED_BINARY_SHA256 },
  );
  if (!/^[a-z0-9][a-z0-9-]*$/.test(name ?? '')) throw new Error('prepareSlice: invalid name');
  if (!Array.isArray(ports) || ports.length !== 3) throw new Error('prepareSlice: ports must be [proxy, admin, metrics]');
  if (!provider || !model) throw new Error('prepareSlice: provider and model required');
  if (otlpEndpoint !== undefined && (typeof otlpEndpoint !== 'string' || !otlpEndpoint.trim())) {
    throw new Error('prepareSlice: otlpEndpoint must be a non-empty string when provided');
  }
  const resume = opts.resume === true;

  const root = path.join(ROOT, name);
  const runtimeDir = path.join(root, 'runtime');
  const hasDb = existsSync(path.join(runtimeDir, 'storage.sqlite'));
  if (hasDb && !resume) {
    throw new Error(`prepareSlice: ${runtimeDir}/storage.sqlite exists; pass resume:true to recover the owned runtime, or choose a new slice name`);
  }
  if (resume && !existsSync(path.join(runtimeDir, 'cc-lb.pid'))) {
    throw new Error(`prepareSlice: resume requested but ${runtimeDir}/cc-lb.pid is missing`);
  }
  if (!resume) {
    for (const port of ports) {
      if (await portInUse(port)) throw new Error(`prepareSlice: port ${port} already in use; not touching existing service`);
    }
  }

  const binarySha256 = await verifyRuntimeArtifact(artifact);
  if (resume) {
    let snapshot;
    try { snapshot = JSON.parse(await readFile(path.join(root, 'slice-ledger.json'), 'utf8')); }
    catch (cause) { throw artifactFailure('ARTIFACT_RESUME_MISMATCH', 'the owned runtime binary identity is unavailable', cause); }
    if (snapshot.binary?.path !== artifact.path || snapshot.binary?.sha256 !== artifact.expected_sha256) {
      throw artifactFailure('ARTIFACT_RESUME_MISMATCH', 'resume requires the original owned runtime artifact');
    }
  }

  const upstreamSpecs = (opts.upstreams?.length ? opts.upstreams : [{ provider }]).map((u, i) => ({
    provider: u.provider,
    name: u.name ?? `${name}-${u.provider}-${i}`,
    baseUrl: u.baseUrl,
  }));
  // Resolve every credential before spawning/seeding anything (missing => BLOCKED).
  const creds = {};
  for (const u of upstreamSpecs) creds[u.provider] = await getCredential(u.provider);

  const preexistingRoot = existsSync(root);
  const ctx = {
    name,
    root,
    runtimeDir,
    provider,
    model,
    ports,
    proxyUrl: `http://127.0.0.1:${ports[0]}`,
    adminUrl: `http://127.0.0.1:${ports[1]}`,
    metricsUrl: `http://127.0.0.1:${ports[2]}/metrics`,
    configPath: path.join(runtimeDir, 'cc-lb.toml'),
    storagePath: path.join(runtimeDir, 'storage.sqlite'),
    portRoles: { proxy: ports[0], admin: ports[1], metrics: ports[2] },
    pricedModels: [...new Set([model, ...(opts.pricedModels ?? [])])],
    logPath: path.join(runtimeDir, 'cc-lb.log'),
    pidPath: path.join(runtimeDir, 'cc-lb.pid'),
    priceCatalogPath: path.join(runtimeDir, 'price-catalog.json'),
    clientHome: path.join(root, 'home'),
    clientKeyPath: path.join(root, 'client', 'key'),
    evidenceDir: path.join(root, 'evidence'),
    binaryPath: artifact.path,
    binarySha256,
    expectedBinarySha256: artifact.expected_sha256,
    ledgerPath: LEDGER_PATH,
    limits: { client: sliceClientLimits({ maxGenerationAttempts: opts.limitOverrides?.maxGenerationAttempts ?? opts.maxGenerationAttempts }, name), server: [] },
    blocked: [],
    upstreams: [],
    recovered: resume ? {} : null,
  };
  const expectedConfig = renderConfig({ runtimeDir, ports, otlpEndpoint });
  const expectedCommand = `${ctx.binaryPath} serve --config ${ctx.configPath}`;
  let isAlive;

  if (!resume) {
    for (const dir of [root, runtimeDir, path.join(runtimeDir, 'data'), path.join(runtimeDir, 'home'),
      path.join(runtimeDir, 'tmp'), path.join(root, 'client'), ctx.clientHome, ctx.evidenceDir]) {
      await mkdir(dir, { recursive: true, mode: 0o700 });
    }
    const masterKey = remember(randomBytes(32).toString('hex'));
    const adminToken = remember(randomBytes(32).toString('hex'));
    Object.defineProperty(ctx, '_adminToken', { value: adminToken, enumerable: false });

    if (!preexistingRoot) {
      await recordResource(ctx, {
        kind: 'scratch_directory', id: root,
        delete: `After explicit cleanup signal and ownership verification: rm -rf ${root}`,
        note: 'Slice root: fresh SQLite, cc-lb runtime, client home, evidence.',
      });
    }

    await writePrivate(path.join(runtimeDir, 'runtime.env'), `CC_LB_MASTER_KEY=${masterKey}\nCC_LB_ADMIN_TOKEN=${adminToken}\n`);
    await writePrivate(ctx.configPath, expectedConfig);

    const env = serverEnv(runtimeDir, masterKey, adminToken, { otlpEndpoint });
    await verifyRuntimeArtifact(artifact);
    await execFileText(ctx.binaryPath, ['config', 'validate', '--config', ctx.configPath], { env, timeoutMs: 30_000 });

    ctx.priceCatalogProvenance = await ensurePriceCatalogCache(ctx);

    await verifyRuntimeArtifact(artifact);
    const logFd = openSync(ctx.logPath, 'a', 0o600);
    const child = spawn(ctx.binaryPath, ['serve', '--config', ctx.configPath], {
      cwd: runtimeDir,
      env,
      detached: true,
      stdio: ['ignore', logFd, logFd],
    });
    closeSync(logFd);
    child.unref();
    if (!child.pid) throw new Error('cc-lb spawn failed (no pid)');
    ctx.pid = child.pid;
    child.on('exit', (code) => { ctx.exitCode = code; });
    isAlive = () => child.exitCode === null && ctx.exitCode === undefined;
    await writePrivate(ctx.pidPath, `${child.pid}\n`);
    await recordResource(ctx, {
      kind: 'process', id: `cc-lb pid ${child.pid} (${name}, ports ${ports.join('/')})`,
      pid: child.pid, config: ctx.configPath,
      binary: { path: ctx.binaryPath, sha256: ctx.binarySha256, expected: ctx.expectedBinarySha256 },
      delete: `After explicit cleanup signal: verify 'ps -p ${child.pid} -o command=' shows ${expectedCommand}, then kill -TERM ${child.pid}`,
    });
    await recordResource(ctx, {
      kind: 'sqlite_database', id: ctx.storagePath,
      delete: `Removed with slice root after cleanup signal (rm -rf ${root}); stop pid ${child.pid} first`,
      note: 'Fresh QA SQLite; API-key upstream rows only, no OAuth.',
    });
  } else {
    // ---- explicit recovery of an owned running runtime (no restart, no delete)
    const pid = Number((await readFile(ctx.pidPath, 'utf8')).trim());
    if (!Number.isInteger(pid) || pid <= 1) throw new Error('resume: invalid pid file');
    const ledger = JSON.parse(await readFile(LEDGER_PATH, 'utf8'));
    const owned = (ledger.entries ?? []).find((e) => e.kind === 'process' && e.pid === pid && e.owner_slice === name
      && e.config === ctx.configPath);
    if (!owned) throw new Error(`resume: pid ${pid} is not a ledger-owned cc-lb process for slice ${name}`);
    if (!pidAlive(pid)) throw new Error(`resume: owned pid ${pid} is not running; refusing to restart silently`);
    const command = await processCommand(pid);
    if (command !== expectedCommand) throw new Error(`resume: pid ${pid} command mismatch (${redactText(command ?? 'unreadable')})`);
    const configText = await readFile(ctx.configPath, 'utf8');
    if (configText !== expectedConfig) throw new Error('resume: on-disk config differs from the rendered config for these ports');
    const envText = await readFile(path.join(runtimeDir, 'runtime.env'), 'utf8');
    const masterKey = parseEnvFile(envText, 'CC_LB_MASTER_KEY');
    const adminToken = parseEnvFile(envText, 'CC_LB_ADMIN_TOKEN');
    if (!/^[0-9a-f]{64}$/.test(masterKey ?? '') || !/^[0-9a-f]{64}$/.test(adminToken ?? '')) {
      throw new Error('resume: runtime.env secrets missing or malformed');
    }
    remember(masterKey);
    remember(adminToken);
    Object.defineProperty(ctx, '_adminToken', { value: adminToken, enumerable: false });
    for (const dir of [path.join(root, 'client'), ctx.clientHome, ctx.evidenceDir]) {
      await mkdir(dir, { recursive: true, mode: 0o700 });
    }
    ctx.pid = pid;
    isAlive = () => pidAlive(pid);
    ctx.recovered = {
      at: nowIso(), pid, ledgerEntryCreatedAt: owned.created_at, command: 'verified', config: 'verified', binarySha256: 'verified',
      configSha256: createHash('sha256').update(configText).digest('hex'),
    };
    // Prove the persisted admin token is the one this process accepts.
    const probe = await admin(ctx, '/admin/v1/principals');
    if (probe.status !== 200) throw new Error(`resume: persisted admin token rejected by pid ${pid} (HTTP ${probe.status})`);
    // Running server picks the cache file up on its next 60s installer tick.
    ctx.priceCatalogProvenance = await ensurePriceCatalogCache(ctx);
  }

  await snapshotSliceLedger(ctx);
  ctx.health = await waitLive(ctx, isAlive, opts.liveDeadlineMs ?? 30_000);

  // ---- admin seed (API key only); on resume reuse rows matched by exact name.
  const existingUpstreams = resume
    ? listOf(expectStatus(await admin(ctx, '/admin/v1/upstreams'), [200], 'list upstreams'), 'upstreams')
    : [];
  for (const u of upstreamSpecs) {
    const cred = creds[u.provider];
    const baseUrl = u.baseUrl ?? cred.baseUrl;
    const prior = existingUpstreams.find((e) => e.name === u.name);
    let id;
    if (prior) {
      if (prior.kind !== cred.kind) throw new Error(`resume: upstream ${u.name} kind ${prior.kind} != ${cred.kind}`);
      id = prior.id;
      ctx.recovered.upstreams = [...(ctx.recovered.upstreams ?? []), id];
    } else {
      const created = expectStatus(await admin(ctx, '/admin/v1/upstreams', 'POST', {
        name: u.name, kind: cred.kind, api_key_value: cred.apiKey, base_url: baseUrl,
        skip_preflight: true, warmup_enabled: false,
      }), [200, 201], `create upstream ${u.name}`);
      id = created.id;
      await recordResource(ctx, {
        kind: 'qa_sqlite_row_upstream', id: `${id} (${u.name}) in ${ctx.storagePath}`,
        delete: 'Contained in slice QA SQLite; removed with slice root after cleanup signal',
      });
    }
    ctx.upstreams.push({ id, name: u.name, provider: u.provider, kind: cred.kind, baseUrl, host: hostOf(baseUrl) });
  }
  ctx.upstreamId = ctx.upstreams[0].id;
  await snapshotSliceLedger(ctx);

  ctx.costCatalog = await inspectPriceCatalog(ctx, opts.catalogDeadlineMs ?? 90_000);
  if (!ctx.costCatalog.modelPriced) {
    ctx.blocked.push(`cost_catalog_unavailable:installed=${ctx.costCatalog.installedByServer},models=${ctx.pricedModels.join(',')}`);
  }

  ctx.limits.server = opts.serverLimits === false ? [] : serverLimitSpec(ctx.limits.client);
  const principalName = `${name}-principal`;
  const priorPrincipal = resume
    ? listOf(expectStatus(await admin(ctx, '/admin/v1/principals?limit=200'), [200], 'list principals'), 'principals')
      .find((p) => p.name === principalName)
    : undefined;
  if (priorPrincipal) {
    ctx.principalId = priorPrincipal.id;
    ctx.recovered.principalId = priorPrincipal.id;
    // Verify reused principal limits; reconcile ONLY this slice's own QA principal
    // via a single If-Match PATCH (no refund/reset; usage history is retained).
    const got = await admin(ctx, `/admin/v1/principals/${priorPrincipal.id}`);
    const current = expectStatus(got, [200], 'get principal');
    const before = current.default_limits ?? [];
    if (!sameLimits(before, ctx.limits.server)) {
      const etag = got.headers.etag;
      if (!etag) throw new Error('resume: principal GET returned no ETag; refusing unconditional limit update');
      expectStatus(await admin(ctx, `/admin/v1/principals/${priorPrincipal.id}`, 'PATCH',
        { default_limits: ctx.limits.server }, { 'if-match': etag }), [200], 'update principal default_limits');
      const after = expectStatus(await admin(ctx, `/admin/v1/principals/${priorPrincipal.id}`), [200], 'verify principal');
      if (!sameLimits(after.default_limits ?? [], ctx.limits.server)) throw new Error('resume: principal limits did not apply');
      ctx.recovered.principalLimits = { before, after: after.default_limits, updatedAt: nowIso() };
    } else {
      ctx.recovered.principalLimits = { verified: before };
    }
  } else {
    const principal = expectStatus(await admin(ctx, '/admin/v1/principals', 'POST', {
      name: principalName, kind: 'machine', allowed_models: [],
      allowed_upstreams: ctx.upstreams.map((u) => u.id),
      default_limits: ctx.limits.server,
    }), [200, 201], 'create principal');
    ctx.principalId = principal.id;
    await recordResource(ctx, {
      kind: 'qa_sqlite_row_principal', id: `${principal.id} in ${ctx.storagePath}`,
      delete: 'Contained in slice QA SQLite; removed with slice root after cleanup signal',
    });
  }

  // Client key: reuse the 0600 key file on resume only if its key is still active.
  let reusedKey = false;
  if (resume && existsSync(ctx.clientKeyPath)) {
    const plaintext = (await readFile(ctx.clientKeyPath, 'utf8')).trim();
    const keys = listOf((await admin(ctx, `/admin/v1/principals/${ctx.principalId}/keys?status=active`)).body, 'keys');
    const match = keys.find((k) => plaintext.endsWith(k.last_4) && plaintext.includes(k.key_id));
    if (plaintext && match) {
      ctx.clientKey = remember(plaintext);
      ctx.keyId = match.key_id;
      ctx.recovered.keyId = match.key_id;
      reusedKey = true;
    }
  }
  if (!reusedKey) {
    const issued = expectStatus(await admin(ctx, `/admin/v1/principals/${ctx.principalId}/keys`, 'POST', {
      label: `${name}-client`,
    }), [200, 201], 'issue client key');
    ctx.keyId = issued.key_id;
    ctx.clientKey = remember(issued.plaintext_key);
    await writePrivate(ctx.clientKeyPath, `${ctx.clientKey}\n`);
    await recordResource(ctx, {
      kind: 'local_proxy_key_file', id: ctx.clientKeyPath, key_id: ctx.keyId,
      delete: `Removed with slice root after cleanup signal (rm -rf ${root})`,
    });
  }

  ctx.ready = await waitReady(ctx, isAlive, opts.readyDeadlineMs ?? 30_000);
  ctx.clientEnvBase = () => clientEnvBase(ctx);
  await snapshotSliceLedger(ctx);
  await writeEvidence(path.join(root, 'prepare.json'), JSON.stringify(publicCtx(ctx), null, 2));
  return ctx;
}

// Per-slice ledger snapshot (ports, PID, homes, rows) written immediately.
async function snapshotSliceLedger(ctx) {
  await writeEvidence(path.join(ctx.root, 'slice-ledger.json'), JSON.stringify(redact({
    slice: ctx.name,
    updatedAt: nowIso(),
    parentLedger: LEDGER_PATH,
    portRoles: ctx.portRoles,
    pid: ctx.pid ?? null,
    binary: { path: ctx.binaryPath, sha256: ctx.binarySha256, expected: ctx.expectedBinarySha256 },
    paths: {
      root: ctx.root, runtimeDir: ctx.runtimeDir, config: ctx.configPath, storage: ctx.storagePath,
      log: ctx.logPath, pidFile: ctx.pidPath, priceCatalog: ctx.priceCatalogPath,
      clientHome: ctx.clientHome, clientKeyFile: ctx.clientKeyPath, evidence: ctx.evidenceDir,
    },
    upstreams: ctx.upstreams,
    principalId: ctx.principalId ?? null,
    keyId: ctx.keyId ?? null,
    cleanup: 'Retained until explicit user cleanup signal; verify pid command line before kill -TERM, then rm -rf root.',
  }), null, 2));
}

function publicCtx(ctx) {
  return redact({
    slice: ctx.name,
    provider: ctx.provider,
    model: ctx.model,
    binary: { path: ctx.binaryPath, sha256: ctx.binarySha256, expected: ctx.expectedBinarySha256, match: ctx.binarySha256 === ctx.expectedBinarySha256 },
    pid: ctx.pid,
    proxyUrl: ctx.proxyUrl,
    adminUrl: ctx.adminUrl,
    metricsUrl: ctx.metricsUrl,
    principalId: ctx.principalId,
    keyId: ctx.keyId,
    upstreams: ctx.upstreams,
    limits: ctx.limits,
    costCatalog: ctx.costCatalog,
    blocked: ctx.blocked,
    health: ctx.health,
    ready: ctx.ready,
    recovered: ctx.recovered,
  });
}

// Fresh client environment: no inherited auth/keyring/provider variables.
export function clientEnvBase(ctx) {
  const home = ctx.clientHome;
  return {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    HOME: home,
    USER: process.env.USER ?? 'qa',
    LANG: 'C.UTF-8',
    TERM: 'dumb',
    NO_COLOR: '1',
    TMPDIR: path.join(home, 'tmp'),
    XDG_CONFIG_HOME: path.join(home, '.config'),
    XDG_DATA_HOME: path.join(home, '.local', 'share'),
    XDG_CACHE_HOME: path.join(home, '.cache'),
    XDG_STATE_HOME: path.join(home, '.local', 'state'),
  };
}

// --------------------------------------------------------- observability

function parsePrometheus(text) {
  const samples = {};
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+(\S+)/);
    if (!m) continue;
    const v = Number(m[2]);
    if (Number.isFinite(v)) samples[m[1]] = v;
  }
  return samples;
}

function isDropSample(key) {
  return DROP_METRIC_RE.test(key) || CRITICAL_DROP_REASONS.some((r) => key.includes(r));
}

export async function metricsSnapshot(ctx, label) {
  const res = await httpRequest(ctx.metricsUrl, { timeoutMs: 10_000 });
  if (res.status !== 200) throw new Error(`metrics scrape failed: HTTP ${res.status}`);
  const text = typeof res.body === 'string' ? res.body : JSON.stringify(res.body);
  const counters = parsePrometheus(text);
  const dropCounters = Object.fromEntries(Object.entries(counters).filter(([k]) => isDropSample(k)));
  if (label) await writeEvidence(path.join(ctx.evidenceDir, `metrics-${label}.prom`), text);
  return { at: nowIso(), counters, dropCounters };
}

export function compareDropCounters(before, after) {
  const increased = [];
  for (const [key, value] of Object.entries(after.dropCounters)) {
    const prev = before.dropCounters[key] ?? 0;
    if (value > prev) {
      increased.push({ key, before: prev, after: value, critical: CRITICAL_DROP_REASONS.some((r) => key.includes(r)) });
    }
  }
  return { increased, criticalIncreased: increased.filter((i) => i.critical) };
}

// Current committed request-event cursor via SSE (first cursor/heartbeat id).
async function currentEventCursorViaStream(ctx, deadlineMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deadlineMs);
  try {
    const res = await fetch(`${ctx.adminUrl}/admin/v1/events/stream`, {
      headers: { authorization: `Bearer ${ctx._adminToken}`, accept: 'text/event-stream' },
      signal: controller.signal,
    });
    if (res.status !== 200 || !res.body) return null;
    const decoder = new TextDecoder();
    let buf = '';
    for await (const chunk of res.body) {
      buf += decoder.decode(chunk, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const block = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const event = block.match(/^event:\s*(.*)$/m)?.[1]?.trim();
        const id = block.match(/^id:\s*(.*)$/m)?.[1]?.trim();
        if ((event === 'cursor' || event === 'heartbeat') && id && /^\d+$/.test(id)) return Number(id);
        if (event === 'reset') return null;
      }
    }
    return null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

async function deltaPage(ctx, sinceCursor, filters = {}) {
  const qs = new URLSearchParams({ since_cursor: String(sinceCursor), limit: '500', ...filters });
  const res = await admin(ctx, `/admin/v1/events/delta?${qs}`);
  return expectStatus(res, [200], 'events delta');
}

async function drainDelta(ctx, sinceCursor, filters, maxPages = 50) {
  const events = [];
  let cursor = sinceCursor;
  for (let i = 0; i < maxPages; i += 1) {
    const page = await deltaPage(ctx, cursor, filters);
    events.push(...(page.events ?? []));
    cursor = page.next_cursor ?? cursor;
    if (page.exhausted || (page.events ?? []).length === 0) return { events, cursor, exhausted: true };
  }
  return { events, cursor, exhausted: false };
}

// Capture BEFORE launching a CLI: committed event cursor + metrics.
export async function observabilityBaseline(ctx, label = 'baseline') {
  let eventCursor = await currentEventCursorViaStream(ctx, 10_000);
  let cursorSource = 'events_stream';
  if (eventCursor === null) {
    eventCursor = (await drainDelta(ctx, 0, {})).cursor;
    cursorSource = 'events_delta_drain';
  }
  const metrics = await metricsSnapshot(ctx, label);
  const baseline = { label, at: nowIso(), eventCursor, cursorSource, metrics };
  await writeEvidence(path.join(ctx.evidenceDir, `observability-${label}.json`),
    JSON.stringify({ ...baseline, metrics: { at: metrics.at, dropCounters: metrics.dropCounters } }, null, 2));
  return baseline;
}

// Bounded wait for committed request events after a baseline cursor.
// Completes when every expectedRequestIds is present (if given) and at least
// minCount distinct request_ids are seen; otherwise returns timedOut=true.
export async function waitForRequestEvents(ctx, {
  baseline, expectedRequestIds = [], minCount = 1, deadlineMs = 30_000, label, principalOnly = true,
} = {}) {
  if (!baseline) throw new Error('waitForRequestEvents requires baseline from observabilityBaseline');
  const filters = principalOnly && ctx.principalId ? { principal_id: ctx.principalId } : {};
  const deadline = Date.now() + deadlineMs;
  const byId = new Map();
  let cursor = baseline.eventCursor;
  let done = false;
  for (;;) {
    const page = await drainDelta(ctx, cursor, filters);
    cursor = page.cursor;
    for (const e of page.events) byId.set(e.request_id || `${e.ts}-${byId.size}`, e);
    const ids = new Set(byId.keys());
    done = expectedRequestIds.every((id) => ids.has(id)) && ids.size >= minCount;
    if (done || Date.now() >= deadline) break;
    await sleep(500);
  }
  const events = [...byId.values()];
  const missing = expectedRequestIds.filter((id) => !byId.has(id));
  const after = await metricsSnapshot(ctx, label ? `${label}-after` : undefined);
  const drops = compareDropCounters(baseline.metrics, after);
  if (label) {
    await writeEvidence(path.join(ctx.evidenceDir, `events-${label}.jsonl`),
      events.map((e) => JSON.stringify(redact(e))).join('\n') + (events.length ? '\n' : ''));
    await writeEvidence(path.join(ctx.evidenceDir, `observability-${label}.json`), JSON.stringify({
      baselineCursor: baseline.eventCursor, finalCursor: cursor, eventCount: events.length,
      timedOut: !done, missing, dropIncreases: drops.increased, invalidated: drops.increased.length > 0,
    }, null, 2));
  }
  // Any increase in an event-loss/drop counter invalidates the run (not PASS).
  return {
    events, finalCursor: cursor, timedOut: !done, missing,
    dropIncreases: drops.increased, criticalDropIncreases: drops.criticalIncreased,
    invalidated: drops.increased.length > 0,
  };
}

// ------------------------------------------------------------- budget

async function readJson(file) {
  try { return JSON.parse(await readFile(file, 'utf8')); } catch { return null; }
}

// Attempts = committed provider-bound events (upstream_id present) for this
// principal, EXCLUDING local limit rejections (error_code 'limit_rejected'):
// cc-lb selects an upstream before reserve, so those carry upstream_id but were
// never dispatched. Provider 4xx/404/429 and transport failures still count.
export async function budgetStatus(ctx) {
  const { events } = await drainDelta(ctx, 0, ctx.principalId ? { principal_id: ctx.principalId } : {});
  const byId = new Map();
  for (const e of events) byId.set(e.request_id || `${e.ts}-${byId.size}`, e);
  const all = [...byId.values()];
  const localLimitRejections = all.filter((e) => e.error_code === 'limit_rejected');
  const attemptsEvents = all.filter((e) => e.upstream_id && e.error_code !== 'limit_rejected');
  const costMicros = attemptsEvents.reduce((sum, e) => sum + (Number(e.cost_usd_micros) || 0), 0);
  // Unknown cost = a committed provider attempt without cost_usd_micros that may
  // have generated: 2xx, client-cancelled 499, or any attempt reporting tokens.
  // A provider rejection with no tokens is a known $0. Catalog availability is a
  // separate pre-run gate (ctx.blocked) and never marks zero attempts unknown.
  const unpriced = attemptsEvents.filter((e) => (e.cost_usd_micros === null || e.cost_usd_micros === undefined)
    && ((e.status >= 200 && e.status < 300) || e.status === 499
      || Number(e.output_tokens) > 0 || Number(e.input_tokens) > 0));
  const sliceCostUsd = costMicros / 1_000_000;

  const status = {
    slice: ctx.name,
    at: nowIso(),
    attempts: attemptsEvents.length,
    costUsd: sliceCostUsd,
    costKnown: unpriced.length === 0,
    unpricedRequestIds: unpriced.map((e) => e.request_id),
    localLimitRejections: localLimitRejections.length,
    catalogPriced: ctx.costCatalog?.modelPriced === true,
    note: 'Only committed request events are counted; in-flight requests are invisible until committed (use waitForRequestEvents after each turn).',
  };
  await writeEvidence(path.join(ctx.root, 'budget.json'), JSON.stringify(status, null, 2));

  let campaignCostUsd = 0;
  const campaignUnknownSlices = [];
  for (const entry of await readdir(ROOT, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const b = await readJson(path.join(ROOT, entry.name, 'budget.json'));
    if (!b) continue;
    campaignCostUsd += Number(b.costUsd) || 0;
    // Unknown only with evidence of unpriced generation. Legacy files without
    // unpricedRequestIds count as unknown unless they record zero attempts.
    const unknownIds = Array.isArray(b.unpricedRequestIds) ? b.unpricedRequestIds : null;
    const unknown = unknownIds ? unknownIds.length > 0 : (b.costKnown === false && Number(b.attempts) !== 0);
    if (unknown && entry.name !== ctx.name) {
      campaignUnknownSlices.push({ slice: entry.name, attempts: b.attempts ?? null, unpricedRequestIds: unknownIds ?? 'legacy_unknown', at: b.at ?? null });
    }
  }

  const reasons = [...ctx.blocked];
  const lim = ctx.limits?.client ?? CLIENT_LIMITS;
  if (status.attempts >= lim.maxGenerationAttempts) reasons.push(`attempts_exhausted:${status.attempts}/${lim.maxGenerationAttempts}`);
  if (sliceCostUsd >= CLIENT_LIMITS.sliceCostUsd) reasons.push(`slice_cost_cap:${sliceCostUsd.toFixed(4)}`);
  if (campaignCostUsd >= CLIENT_LIMITS.campaignCostUsd) reasons.push(`campaign_cost_cap:${campaignCostUsd.toFixed(4)}`);
  if (!status.costKnown) reasons.push('cost_unknown');
  if (campaignUnknownSlices.length > 0) {
    reasons.push(`campaign_cost_unknown_in_other_slice:${campaignUnknownSlices.map((s) => s.slice).join(',')}`);
  }
  return { ...status, campaignCostUsd, campaignUnknownSlices, blocked: reasons.length > 0, reasons: [...new Set(reasons)] };
}

// Throws BLOCKED before a paid CLI turn when any bound would be crossed.
export async function assertBudget(ctx) {
  const b = await budgetStatus(ctx);
  if (b.blocked) {
    // Budget exhaustion is BLOCKED (verdict), never an application FAIL.
    const error = new Error(`BLOCKED[BUDGET]: ${b.reasons.join(', ')}`);
    error.code = 'BUDGET_BLOCKED';
    error.verdict = 'BLOCKED';
    error.budget = b;
    throw error;
  }
  return b;
}

// ------------------------------------------- Gemini CLI quota readiness (C)

// cc-lb limit estimate input (lifecycle.rs DEFAULT_MAX_INPUT_ESTIMATE).
const CC_LB_MAX_INPUT_ESTIMATE = 4000;
const LOCAL_COST_429_MESSAGE = 'cost/window cap exceeded';
const GOOGLE_API_HOST = 'generativelanguage.googleapis.com';

function quotaBlocked(code, message, detail) {
  const error = new Error(`BLOCKED[${code}]: ${redactText(message)}`);
  error.code = code;
  error.verdict = 'BLOCKED';
  error.detail = detail;
  return error;
}

function metricSum(counters, name, labelFilter) {
  let sum = 0;
  let seen = false;
  for (const [key, value] of Object.entries(counters)) {
    if (key !== name && !key.startsWith(`${name}{`)) continue;
    if (labelFilter && !labelFilter(key)) continue;
    sum += value;
    seen = true;
  }
  return { sum, seen };
}

function isGoogleEnvelope(body) {
  return Boolean(body && typeof body === 'object' && body.error && typeof body.error === 'object'
    && typeof body.error.code === 'number' && typeof body.error.status === 'string');
}

// Negative readiness probe (contract C, labelled outside CLI coverage):
// same client key + same real Google upstream, body with top-level max_tokens
// (cc-lb limit estimate precedence: max_tokens > max_output_tokens >
// generationConfig.maxOutputTokens, lifecycle.rs:7286-7297), output cap 8 for
// Google, and an invalid `contents`. While the key is cold the cost estimate
// exceeds cap/10 → LOCAL 429 'cost/window cap exceeded' (key registered, nothing
// committed). Once warm the request is dispatched and Google rejects it with
// 400 INVALID_ARGUMENT before generation. ONLY that provider 400, matched by a
// committed upstream_4xx event with $0 cost, proves the full $3 USD cap is live.
// Bounded: <= maxAttempts local probes within deadlineMs; stops at the first
// non-local response. Backoff waits are never treated as proof.
export async function ensureGeminiCliQuotaReady(ctx, {
  model = 'gemini-3.1-pro-preview', maxOutputTokens = 65535, deadlineMs = 30_000, maxAttempts = 8,
} = {}) {
  const result = {
    label: 'negative_quota_readiness_probe', outsideCliCoverage: true, model, maxOutputTokens,
    startedAt: nowIso(), localProbes: [], providerResponse: null, providerAttempts: 0, verdict: null, checks: {},
  };
  const evidencePath = path.join(ctx.evidenceDir, 'gemini-quota-readiness.json');
  const done = async (verdict, error) => {
    result.verdict = verdict;
    result.finishedAt = nowIso();
    if (error) result.error = { code: error.code, message: error.message, detail: error.detail ?? null };
    await writeEvidence(evidencePath, JSON.stringify(redact(result), null, 2));
    if (error) throw error;
    return result;
  };

  // ---- preconditions (no network beyond local admin/metrics)
  if (ctx.provider !== 'gemini') return done('BLOCKED', quotaBlocked('PROBE_PRECONDITION', `slice provider ${ctx.provider} is not gemini`));
  const upstream = ctx.upstreams.find((u) => u.id === ctx.upstreamId);
  if (ctx.upstreams.length !== 1 || upstream?.host !== GOOGLE_API_HOST || upstream.kind !== 'gemini_api_key') {
    return done('BLOCKED', quotaBlocked('PROBE_PRECONDITION', 'probe requires exactly one real gemini_api_key upstream at generativelanguage.googleapis.com'));
  }
  if (!ctx.costCatalog?.modelPriced) return done('BLOCKED', quotaBlocked('PROBE_PRICING', 'price catalog not installed/priced for slice models'));
  const price = ctx.costCatalog.prices?.[model];
  if (!price) return done('BLOCKED', quotaBlocked('PROBE_PRICING', `no exact native catalog price for ${model}`));
  const costLimit = ctx.limits.server.find((l) => l.kind === 'cost_usd');
  if (!costLimit) return done('BLOCKED', quotaBlocked('PROBE_PRECONDITION', 'principal has no server cost_usd limit'));
  const warmCapUsd = costLimit.cap_micros / 1_000_000;
  const coldCapUsd = Math.max(1, Math.floor(costLimit.cap_micros / 10)) / 1_000_000;
  const estimateMinUsd = CC_LB_MAX_INPUT_ESTIMATE * price.input + maxOutputTokens * price.output;
  const estimateMaxUsd = CC_LB_MAX_INPUT_ESTIMATE * price.inputMax + maxOutputTokens * price.outputMax;
  const before = await budgetStatus(ctx);
  const remainingWarmUsd = warmCapUsd - before.costUsd;
  result.checks.pricing = { key: price.key, estimateMinUsd, estimateMaxUsd, coldCapUsd, warmCapUsd, remainingWarmUsd };
  // Hold-size proof (source, verified at binary sha 50d43f…): LimitRequest::from_value
  // takes top-level max_tokens before max_output_tokens before
  // generationConfig.maxOutputTokens (crates/cc-lb-engine/src/lifecycle.rs:7286-7297);
  // reserve_limit estimates with input 4000 (lifecycle.rs:132, 3920-3929); the body is
  // forwarded unchanged (RawPassthroughDialect lifecycle.rs:7089-7116; signer only sets
  // x-goog-api-key); provider_capability::body_is_consistency_safe returns true (27-36);
  // rejected reservations commit nothing (limit_engine.rs:551-559).
  result.checks.holdSizeSource = {
    field: 'max_tokens', value: maxOutputTokens, coldCapUsd, estimateMinUsd,
    citations: ['crates/cc-lb-engine/src/lifecycle.rs:7286-7297', 'crates/cc-lb-engine/src/lifecycle.rs:132,3920-3929',
      'crates/cc-lb-engine/src/lifecycle.rs:7089-7116', 'crates/cc-lb-engine/src/provider_capability.rs:27-36',
      'crates/cc-lb-control/src/api_keys/limit_engine.rs:418-436,551-559'],
  };
  if (!(estimateMinUsd > coldCapUsd)) {
    return done('BLOCKED', quotaBlocked('PROBE_PRICING', `estimate ${estimateMinUsd} does not exceed cold cap ${coldCapUsd}; probe could be admitted cold`));
  }
  if (!(estimateMaxUsd < remainingWarmUsd)) {
    return done('BLOCKED', quotaBlocked('PROBE_PRICING', `estimate ${estimateMaxUsd} not below remaining warm cap ${remainingWarmUsd}`));
  }
  if (before.blocked) return done('BLOCKED', quotaBlocked('BUDGET_BLOCKED', before.reasons.join(', ')));
  if (before.attempts + 1 > ctx.limits.client.maxGenerationAttempts) {
    return done('BLOCKED', quotaBlocked('BUDGET_BLOCKED', 'no generation attempt left for the provider probe'));
  }

  const baseline = await observabilityBaseline(ctx, 'quota-probe-baseline');
  const upstreamLabel = (key) => key.includes(`"${ctx.upstreamId}"`) || key.includes(`"${upstream.name}"`);
  const failoverBefore = metricSum(baseline.metrics.counters, 'cc_lb_upstream_failover_total').sum;
  const ttlEvictedBefore = metricSum(baseline.metrics.counters, 'cc_lb_limit_reservation_ttl_evicted_total').sum;

  // ---- bounded local poll
  const url = `${ctx.proxyUrl}/v1beta/models/${encodeURIComponent(model)}:generateContent`;
  const body = JSON.stringify({ max_tokens: maxOutputTokens, generationConfig: { maxOutputTokens: 8 }, contents: 'invalid-not-array' });
  const deadline = Date.now() + deadlineMs;
  let response = null;
  for (let i = 0; i < maxAttempts && Date.now() < deadline; i += 1) {
    let res;
    try {
      res = await httpRequest(url, {
        method: 'POST', body,
        headers: { 'x-goog-api-key': ctx.clientKey, 'content-type': 'application/json' },
        timeoutMs: Math.max(1_000, Math.min(20_000, deadline - Date.now())),
      });
    } catch (error) {
      // Transport outcome unknown: the request may have been dispatched.
      return done('BLOCKED', quotaBlocked('PROBE_TRANSPORT_UNKNOWN', `probe transport failed: ${error.message}`));
    }
    const google = isGoogleEnvelope(res.body);
    const message = typeof res.body?.error?.message === 'string' ? res.body.error.message : '';
    const requestId = res.headers['x-request-id'] ?? res.headers['request-id'] ?? null;
    // Local cost 429 (lifecycle.rs:7972-7977, 8004-8016): body {type:'error',error:{limit_kind:'cost_usd'}}.
    // Any other local limit kind (e.g. requests) is not retried.
    if (res.status === 429 && !google && res.body?.error?.limit_kind === 'cost_usd' && message.includes(LOCAL_COST_429_MESSAGE)) {
      result.localProbes.push({ at: nowIso(), status: 429, kind: 'local_cost_cold', requestId });
      const backoff = Math.min(250 * 2 ** i, 4_000, Math.max(0, deadline - Date.now()));
      if (backoff > 0) await sleep(backoff);
      continue;
    }
    response = { status: res.status, google, requestId, errorStatus: res.body?.error?.status ?? null, message: message.slice(0, 300) };
    break;
  }
  if (!response) {
    return done('BLOCKED', quotaBlocked('PROBE_COLD_TIMEOUT', `key stayed cold after ${result.localProbes.length} local probes within ${deadlineMs}ms`));
  }
  result.providerResponse = response;
  if (!response.google) {
    // Local non-cost response (requests 429, 503 CostUnavailable, local 4xx validation, ...).
    return done('BLOCKED', quotaBlocked('PROBE_LOCAL_REJECTION', `local cc-lb response ${response.status}: ${response.message}`, response));
  }
  result.providerAttempts = 1;
  if (response.status >= 200 && response.status < 300) {
    const invalid = quotaBlocked('PROBE_INVALID_GENERATION', 'provider accepted invalid probe body (paid, counted in budget)', response);
    invalid.verdict = 'INVALID';
    return done('INVALID', invalid);
  }
  if (response.status !== 400 || response.errorStatus !== 'INVALID_ARGUMENT') {
    return done('BLOCKED', quotaBlocked('PROBE_PROVIDER_ERROR', `provider ${response.status} ${response.errorStatus}`, response));
  }

  // ---- committed-event boundary: exactly one dispatched event, upstream_4xx, $0
  const evDeadline = Date.now() + 30_000;
  let cursor = baseline.eventCursor;
  const seen = new Map();
  let providerEvents = [];
  for (;;) {
    const page = await drainDelta(ctx, cursor, { principal_id: ctx.principalId });
    cursor = page.cursor;
    for (const e of page.events) seen.set(e.request_id || `${e.ts}-${seen.size}`, e);
    providerEvents = [...seen.values()].filter((e) => e.error_code !== 'limit_rejected');
    if (providerEvents.length >= 1 || Date.now() >= evDeadline) break;
    await sleep(500);
  }
  const localEvents = [...seen.values()].filter((e) => e.error_code === 'limit_rejected');
  await writeEvidence(path.join(ctx.evidenceDir, 'events-quota-probe.jsonl'),
    [...seen.values()].map((e) => JSON.stringify(redact(e))).join('\n') + (seen.size ? '\n' : ''));
  const ev = providerEvents[0];
  result.checks.events = {
    baselineCursor: baseline.eventCursor, finalCursor: cursor, localLimitRejected: localEvents.length,
    providerEvents: providerEvents.length,
    event: ev ? { request_id: ev.request_id, status: ev.status, error_code: ev.error_code ?? null, upstream_id: ev.upstream_id ?? null,
      cost_usd_micros: ev.cost_usd_micros ?? null, input_tokens: ev.input_tokens ?? null, output_tokens: ev.output_tokens ?? null } : null,
  };
  const zeroCost = ev && (Number(ev.cost_usd_micros ?? 0) === 0) && !(Number(ev.input_tokens) > 0) && !(Number(ev.output_tokens) > 0);
  if (providerEvents.length !== 1 || !ev || ev.status !== 400 || ev.error_code !== 'upstream_4xx'
    || ev.upstream_id !== ctx.upstreamId || !zeroCost
    || (response.requestId && ev.request_id && response.requestId !== ev.request_id)) {
    return done('BLOCKED', quotaBlocked('PROBE_EVENT_MISMATCH', 'committed event does not prove exactly one $0 upstream_4xx dispatch', result.checks.events));
  }

  // ---- observability, failover and upstream health unaffected
  const after = await metricsSnapshot(ctx, 'quota-probe-after');
  const drops = compareDropCounters(baseline.metrics, after);
  const failoverAfter = metricSum(after.counters, 'cc_lb_upstream_failover_total').sum;
  const breaker = metricSum(after.counters, 'cc_lb_circuit_breaker_state', upstreamLabel);
  const ttlEvictedAfter = metricSum(after.counters, 'cc_lb_limit_reservation_ttl_evicted_total').sum;
  result.checks.health = { dropIncreases: drops.increased, failoverBefore, failoverAfter, breakerSeen: breaker.seen, breakerState: breaker.sum,
    ttlEvictedBefore, ttlEvictedAfter,
    holdRelease: 'not directly observable: no concurrent limit/gauge; TTL sweep runs every 300s so an unchanged ttl_evicted counter is not proof of release. A leaked hold can only cause an earlier unpaid local cost 429 (BLOCKED).' };
  if (drops.increased.length > 0) return done('BLOCKED', quotaBlocked('PROBE_INVALIDATED', 'event-loss/drop counter increased', drops.increased));
  if (failoverAfter !== failoverBefore) return done('BLOCKED', quotaBlocked('PROBE_FAILOVER', 'upstream failover counter changed'));
  if (breaker.seen && breaker.sum !== 0) return done('BLOCKED', quotaBlocked('PROBE_UPSTREAM_HEALTH', `circuit breaker state ${breaker.sum} for upstream`));
  if (ttlEvictedAfter !== ttlEvictedBefore) return done('BLOCKED', quotaBlocked('PROBE_HOLD_NOT_RELEASED', 'limit reservation TTL eviction counter changed'));

  // ---- budget: exactly +1 attempt, cost unchanged, nothing unknown
  const afterBudget = await budgetStatus(ctx);
  result.checks.budget = { attemptsBefore: before.attempts, attemptsAfter: afterBudget.attempts, costBefore: before.costUsd,
    costAfter: afterBudget.costUsd, costKnown: afterBudget.costKnown };
  if (afterBudget.attempts !== before.attempts + 1 || afterBudget.costUsd !== before.costUsd || !afterBudget.costKnown) {
    return done('BLOCKED', quotaBlocked('PROBE_BUDGET_MISMATCH', 'budget did not settle at +1 attempt and $0', result.checks.budget));
  }
  return done('READY');
}

// ------------------------------------------------------- client runner

// Runs one bounded client turn. timeoutMs is clamped to turnTimeoutSecs.
// stdout/stderr are written redacted to evidence/<label>.{stdout,stderr}.
export async function runClient(ctx, { cmd, args = [], cwd, env, label, timeoutMs, stdin, enforceBudget = true }) {
  if (!label) throw new Error('runClient requires label');
  if (enforceBudget) await assertBudget(ctx);
  const limit = CLIENT_LIMITS.turnTimeoutSecs * 1000;
  const effectiveTimeout = Math.min(timeoutMs ?? limit, limit);
  const started = Date.now();
  const child = spawn(cmd, args, { cwd, env: env ?? clientEnvBase(ctx), stdio: ['pipe', 'pipe', 'pipe'], detached: true });
  const out = [];
  const err = [];
  child.stdout.on('data', (c) => out.push(c));
  child.stderr.on('data', (c) => err.push(c));
  if (stdin !== undefined) child.stdin.end(stdin); else child.stdin.end();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    try { process.kill(-child.pid, 'SIGTERM'); } catch { /* gone */ }
    setTimeout(() => { try { process.kill(-child.pid, 'SIGKILL'); } catch { /* gone */ } }, 5_000).unref();
  }, effectiveTimeout);
  const exit = await new Promise((resolve) => child.on('close', (code, signal) => resolve({ code, signal })));
  clearTimeout(timer);
  const stdout = Buffer.concat(out).toString('utf8');
  const stderr = Buffer.concat(err).toString('utf8');
  await writeEvidence(path.join(ctx.evidenceDir, `${label}.stdout`), stdout);
  await writeEvidence(path.join(ctx.evidenceDir, `${label}.stderr`), stderr);
  const stderrWarnings = redactText(stderr).split('\n').filter((l) => /\b(warn|warning|fallback|deprecated|retry|reconnect)/i.test(l)).slice(0, 200);
  return { ...exit, timedOut, durationMs: Date.now() - started, stdout: redactText(stdout), stderr: redactText(stderr), stderrWarnings };
}

// ------------------------------------------------------ credential scan

const CREDENTIAL_NAME_RE = /(^auth\.json$|oauth|credential|\.token$|token\.json$|keyring|keychain|refresh|session.*\.json$|^\.netrc$|application_default_credentials)/i;

// Lists files under dir whose names look like auth/OAuth/keyring material.
export async function scanCredentialFiles(dir, { maxEntries = 20_000 } = {}) {
  const hits = [];
  let seen = 0;
  async function walk(d) {
    let entries;
    try { entries = await readdir(d, { withFileTypes: true }); } catch { return; }
    for (const e of entries) {
      if (++seen > maxEntries) return;
      const p = path.join(d, e.name);
      if (e.isDirectory()) {
        if (e.name === 'node_modules' || e.name === '.git') continue;
        await walk(p);
      } else if (CREDENTIAL_NAME_RE.test(e.name)) {
        const st = await stat(p).catch(() => null);
        hits.push({ path: p, mode: st ? (st.mode & 0o777).toString(8) : null, size: st?.size ?? null });
      }
    }
  }
  await walk(dir);
  return hits;
}

// ----------------------------------------------------------- evidence

export async function finishEvidence(ctx, summary = {}) {
  let budget = null;
  let finalObservability = null;
  try { budget = await budgetStatus(ctx); } catch (error) { budget = { error: redactText(error.message) }; }
  try {
    const m = await metricsSnapshot(ctx, 'final');
    finalObservability = { metricsAt: m.at, dropCounters: m.dropCounters };
  } catch (error) {
    finalObservability = { error: redactText(error.message) };
  }
  const credentialFiles = await scanCredentialFiles(ctx.clientHome).catch(() => []);
  const doc = {
    ...publicCtx(ctx),
    limits: { server: ctx.limits.server, client: ctx.limits.client },
    budget,
    finalObservability,
    clientHomeCredentialFiles: credentialFiles,
    serverAlive: ctx.exitCode === undefined,
    finishedAt: nowIso(),
    ...redact(summary),
  };
  const file = path.join(ctx.root, 'summary.json');
  await writeEvidence(file, JSON.stringify(doc, null, 2));
  return { path: file, summary: doc };
}
