#!/usr/bin/env node
// Local boundary regression; never contacts an external provider or builds a binary.
// Required: CC_LB_BIN, CC_LB_PLUGIN_WASM, CC_LB_QA_OUTPUT (a new absolute directory).
// --retain keeps the child and HTTP provider alive until explicitly stopped by its owner.
// --legacy-fixture supports the retained pre-fix fixture (no negative filter probe).
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { access, appendFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { constants } from 'node:fs';

const retain = process.argv.includes('--retain');
const legacy = process.argv.includes('--legacy-fixture');
const ledger = process.env.CC_LB_QA_LEDGER ?? '/tmp/cc-lb-gemini-codex-qa-20261112.ledger';
function required(name) {
  const value = process.env[name];
  assert.ok(value && path.isAbsolute(value), `${name} must be an explicit absolute path`);
  return value;
}
const bin = required('CC_LB_BIN');
const wasm = required('CC_LB_PLUGIN_WASM');
const out = required('CC_LB_QA_OUTPUT');
await access(bin, constants.X_OK);
const wasmBytes = await readFile(wasm);
const record = async (kind, identifier, deleteCommand) => appendFile(ledger,
  `${JSON.stringify({ kind, identifier, deleteCommand, createdAt: new Date().toISOString() })}\n`, { mode: 0o600 });
const shellQuote = value => `'${value.replaceAll("'", "'\\''")}'`;

// Independent literal oracles, not computed by a shaper or response helper.
// Gemini: challenge-proxy/AP-1-controlled.json, shape_control (2026-11-15).
const GEMINI = '{"totalTokens":3}';
// OpenAI: admin-plugins/collision-openai-shape-only-control.json, output_item.done.
// Sanitize captured identifiers/model; remove fixture-injected qa_plugin and volatile metadata.
const OPENAI = '{"id":"resp_local","object":"response","status":"completed","model":"gpt-5.2","output":[{"type":"message","id":"msg_local","role":"assistant","status":"completed","content":[{"type":"output_text","text":"OK","annotations":[],"logprobs":[]}],"phase":"final_answer"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}';
const ANTHROPIC = '{"id":"msg_control","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"control"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":1}}';
const cases = [
  { provider: 'openai', route: '/v1/responses', request: '{"model":"gpt-5.2","input":"Say OK","stream":false}', response: OPENAI },
  { provider: 'gemini', route: '/v1beta/models/gemini-2.5-pro:countTokens', request: '{"contents":[{"role":"user","parts":[{"text":"Say OK"}]}]}', response: GEMINI },
  { provider: 'anthropic', route: '/v1/messages', request: '{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"control"}]}', response: ANTHROPIC },
];
const captures = [];
const provider = http.createServer(async (req, res) => {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const body = Buffer.concat(chunks).toString();
  // Never persist provider/client credentials, even though these credentials are synthetic.
  captures.push({ method: req.method, url: req.url, shape: req.headers['x-qa-shape'] ?? null, body });
  // Answer creation's discovery and inference probes on this loopback boundary.
  // The upstream creation requests below do not set a preflight bypass flag.
  let preflightBody;
  if (req.method === 'GET' && req.url === '/v1/models') {
    preflightBody = req.headers['x-api-key']
      ? '{"data":[{"id":"claude-sonnet-4-5","type":"model","display_name":"Local Claude","created_at":"2026-01-01T00:00:00Z"}],"has_more":false,"first_id":"claude-sonnet-4-5","last_id":"claude-sonnet-4-5"}'
      : '{"object":"list","data":[{"id":"gpt-5.2","object":"model"}]}';
  } else if (req.method === 'GET' && req.url === '/v1beta/models') {
    preflightBody = '{"models":[{"name":"models/gemini-2.5-pro","supportedGenerationMethods":["generateContent","countTokens"]}]}';
  } else if (req.method === 'POST' && req.url === '/v1/messages/count_tokens') {
    preflightBody = '{"input_tokens":3}';
  } else if (req.method === 'POST' && req.url === '/v1/chat/completions') {
    preflightBody = '{"id":"chatcmpl_local","object":"chat.completion","model":"gpt-5.2","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}';
  } else if (req.method === 'POST' && req.url === '/v1beta/models/gemini-2.5-pro:generateContent') {
    preflightBody = '{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1,"totalTokenCount":4}}';
  }
  if (preflightBody !== undefined) {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(preflightBody);
    return;
  }
  const match = cases.find(c => c.route === req.url);
  if (!match) { res.writeHead(404); res.end('unexpected local route'); return; }
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(match.response);
});
async function freePort() {
  const server = http.createServer();
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    return server.address().port;
  } finally {
    if (server.listening) await new Promise(resolve => server.close(resolve));
  }
}
let outputCreated = false;
let providerPort;
let proxyPort;
let adminPort;
let token;
let child;
let childClosed;
let childError;
// Keep diagnostics in memory: runtime logs can contain credential-bearing request context.
let diagnostics = '';
const config = path.join(out, 'cc-lb.toml');
const results = [];
const errors = [];
const resources = [];
const endpoints = {};
async function recordAdmin(kind, data) {
  const identifier = `${kind} id=${data.id}; admin=${endpoints.admin}; config=${config}`;
  // All admin objects are confined to this run's SQLite file and scratch tree.
  // Removing that tree after stopping its recorded processes removes these rows.
  await record(`admin-${kind}`, identifier, `# Stop recorded processes first; then remove isolated store: rm -r -- ${shellQuote(out)}`);
  resources.push({ kind, id: data.id });
}
async function createPrincipal(name) {
  const principal = (await admin('POST', '/admin/v1/principals', { name, kind: 'human', allowed_models: [], allowed_upstreams: [], default_limits: [] })).data;
  await recordAdmin('principal', principal);
  const key = (await admin('POST', `/admin/v1/principals/${principal.id}/keys`, { label: 'local-only' })).data;
  await recordAdmin('key', key);
  return { principal, key: key.plaintext_key };
}
function probeFailure(error, context) {
  failed = true;
  errors.push({ ...context, message: error.message });
  console.error(`${context.stage} ${context.provider}: ${error.message}`);
}
let failed = false;
async function admin(method, route, body, revision) {
  const response = await fetch(`http://127.0.0.1:${adminPort}${route}`, {
    method, signal: AbortSignal.timeout(10000),
    headers: { authorization: `Bearer ${token}`, ...(body ? { 'content-type': 'application/json' } : {}), ...(revision !== undefined ? { 'if-match': `"${revision}"` } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  assert.ok(response.ok, `${method} ${route}: ${response.status} ${text}`);
  return { status: response.status, data: text ? JSON.parse(text) : null };
}
try {
  await mkdir(out, { mode: 0o700 }); // Refuse reuse: never overwrite another run's evidence.
  outputCreated = true;
  await record('scratch-directory', out, `rm -r -- ${shellQuote(out)}`);
  await record('process', `harness pid=${process.pid}; output=${out}`, `kill -TERM ${process.pid}`);
  await new Promise((resolve, reject) => {
    provider.once('error', reject);
    provider.listen(0, '127.0.0.1', resolve);
  });
  providerPort = provider.address().port;
  await record('http-listener', `harness pid=${process.pid}; 127.0.0.1:${providerPort}`, `kill -TERM ${process.pid}`);
  proxyPort = await freePort();
  adminPort = await freePort();
  const metricsPort = await freePort();
  token = randomBytes(32).toString('hex');
  Object.assign(endpoints, { admin: `http://127.0.0.1:${adminPort}`, proxy: `http://127.0.0.1:${proxyPort}`, provider: `http://127.0.0.1:${providerPort}` });
  await writeFile(config, `[runtime]\ndata_dir = ${JSON.stringify(path.join(out, 'data'))}\n[listener]\nproxy_addr = "127.0.0.1:${proxyPort}"\nadmin_addr = "127.0.0.1:${adminPort}"\nmetrics_addr = "127.0.0.1:${metricsPort}"\n[storage]\nkind = "sqlite"\npath = ${JSON.stringify(path.join(out, 'cc-lb.sqlite'))}\n[aead]\nkey_env = "CC_LB_MASTER_KEY"\n[[admin.auth.providers]]\nkind = "static_token"\nid = "local"\ntoken_env = "CC_LB_ADMIN_TOKEN"\n`, { mode: 0o600 });
  child = spawn(bin, ['serve', '--config', config], {
    cwd: out,
    env: { ...process.env, CC_LB_ADMIN_TOKEN: token, CC_LB_MASTER_KEY: randomBytes(32).toString('hex'), RUST_LOG: 'warn' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.on('error', error => { childError = error; });
  childClosed = new Promise(resolve => child.once('close', resolve));
  for (const stream of [child.stdout, child.stderr]) stream.on('data', data => { diagnostics = (diagnostics + data).slice(-16000); });
  await record('process', `cc-lb pid=${child.pid}; config=${config}`, `kill -TERM ${child.pid}`);
  let ready = false;
  for (let i = 0; i < 120; i++) {
    if (childError) throw childError;
    assert.equal(child.exitCode, null, 'cc-lb exited before readiness');
    try { ready = (await fetch(`http://127.0.0.1:${adminPort}/admin/health`, { signal: AbortSignal.timeout(500) })).status === 200; } catch {}
    if (ready) break;
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  assert.ok(ready, 'cc-lb did not become ready');
  for (const [name, kind, syntheticKey] of [
    ['openai', 'openai_api_key', 'sk-local-synthetic-openai-key'],
    ['gemini', 'gemini_api_key', 'AIzaSyLocalSyntheticGeminiKey1234567890'],
    ['anthropic', 'anthropic_api_key', 'sk-ant-api03-local-synthetic-anthropic-key'],
  ]) {
    const upstream = (await admin('POST', '/admin/v1/upstreams', { name: `slots-${name}`, kind, base_url: `http://127.0.0.1:${providerPort}`, api_key_value: syntheticKey, warmup_enabled: false })).data;
    await recordAdmin('upstream', upstream);
  }
  const form = new FormData();
  form.append('bytes', new Blob([wasmBytes], { type: 'application/wasm' }), 'dual-slot.wasm');
  form.append('original_filename', 'dual-slot.wasm');
  const uploaded = await fetch(`http://127.0.0.1:${adminPort}/admin/v1/plugins/wasm`, { method: 'POST', headers: { authorization: `Bearer ${token}` }, body: form, signal: AbortSignal.timeout(30000) });
  assert.equal(uploaded.status, 201, 'WASM upload');
  const registry = await uploaded.json();
  await recordAdmin('registry', registry);
  const sibling = await createPrincipal('slots-independent-anthropic-control');
  for (const order of [['shape', 'router'], ['router', 'shape']]) {
    const { principal, key } = await createPrincipal(`slots-${order.join('-')}`);
    const entries = {};
    async function probe(stage, shape, filter) {
      for (const c of cases) {
        try {
        const before = captures.length;
        const response = await fetch(`http://127.0.0.1:${proxyPort}${c.route}`, { method: 'POST', signal: AbortSignal.timeout(15000), headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', 'anthropic-version': '2023-06-01' }, body: c.request });
        const text = await response.text();
        const result = { order, stage, provider: c.provider, status: response.status, body: text, transform: response.headers.get('x-qa-transform'), capture: captures.at(-1) };
        results.push(result);
        assert.equal(response.status, 200, JSON.stringify(result));
        assert.equal(text, c.response, `${stage}: exact ${c.provider} response body`);
        assert.equal(captures.length, before + 1, 'exactly one provider request');
        assert.equal(captures.at(-1).body, c.request, `${stage}: request body preserved`);
        const marked = shape && c.provider !== 'anthropic';
        assert.equal(captures.at(-1).shape, marked ? 'qualified-v2' : null, `${stage}: shape really executed (or provider isolated)`);
        assert.equal(response.headers.get('x-qa-transform'), marked ? c.provider : null, `${stage}: response hook marker`);
        if (filter && c.provider !== 'anthropic' && !legacy) {
          const rejectedBefore = captures.length;
          const rejected = await fetch(`http://127.0.0.1:${proxyPort}${c.route}`, { method: 'POST', signal: AbortSignal.timeout(15000), headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json', 'x-qa-reject': '1' }, body: c.request });
          const rejection = await rejected.text();
          results.push({ order, stage: `${stage}-filter-proof`, provider: c.provider, status: rejected.status, body: rejection });
          assert.ok(rejected.status >= 400, 'filter negative probe must reject');
          assert.equal(captures.length, rejectedBefore, 'filter rejected before HTTP boundary');
        }
        } catch (error) {
          probeFailure(error, { order, stage, provider: c.provider });
        }
      }
      const control = cases.find(c => c.provider === 'anthropic');
      try {
        const response = await fetch(`${endpoints.proxy}${control.route}`, {
          method: 'POST', signal: AbortSignal.timeout(15000),
          headers: { authorization: `Bearer ${sibling.key}`, 'content-type': 'application/json', 'anthropic-version': '2023-06-01' },
          body: control.request,
        });
        const body = await response.text();
        results.push({ order, stage, provider: 'anthropic-sibling', principalId: sibling.principal.id, status: response.status, body, transform: response.headers.get('x-qa-transform') });
        assert.equal(response.status, 200, 'independent Anthropic sibling status');
        assert.equal(body, ANTHROPIC, 'independent Anthropic sibling exact body');
        assert.equal(response.headers.get('x-qa-transform'), null, 'independent sibling has no plugin');
      } catch (error) {
        probeFailure(error, { order, stage, provider: 'anthropic-sibling' });
      }
    }
    await probe('untouched', false, false);
    for (const slot of order) {
      const attached = await admin('POST', `/admin/v1/principals/${principal.id}/plugin-chain`, { slot, wasm_registry_id: registry.id });
      assert.equal(attached.status, 201);
      entries[slot] = attached.data;
      await recordAdmin('chain', attached.data);
      await probe(`attached-${slot}`, Boolean(entries.shape), Boolean(entries.router));
    }
    // Both independent detach paths: retain shape in one ordering, filter in the other.
    const removed = order[1];
    await admin('DELETE', `/admin/v1/plugin-chain-entries/${entries[removed].id}`, undefined, entries[removed].revision);
    delete entries[removed];
    await probe(`detached-${removed}`, Boolean(entries.shape), Boolean(entries.router));
  }
  if (!failed) console.log(`PASS: plugin slot HTTP regression; evidence ${out}`);
} catch (error) {
  failed = true;
  errors.push({ stage: 'setup-or-lifecycle', message: error.message });
  console.error(error.message);
  // Deliberately do not print raw runtime diagnostics: they may include credentials.
  if (diagnostics) console.error('Runtime diagnostics captured in memory; omitted for credential safety.');
} finally {
  try {
    if (outputCreated) {
      await writeFile(path.join(out, 'results.json'), JSON.stringify({ passed: !failed, legacyFixture: legacy, binary: bin, wasm, config, endpoints, process: { harnessPid: process.pid, childPid: child?.pid, childExitCode: child?.exitCode, childSignalCode: child?.signalCode }, resources, errors, results, captures }, null, 2), { mode: 0o600 });
    }
    process.exitCode = failed ? 1 : 0;
  } catch (error) {
    process.exitCode = 1;
    console.error(`Unable to write results.json: ${error.message}`);
  } finally {
    if (retain) {
      console.log(`RETAINED: harness PID ${process.pid}, cc-lb PID ${child?.pid ?? 'not started'}, provider ${provider.listening ? `127.0.0.1:${providerPort}` : 'not listening'}; ledger ${ledger}`);
    } else {
      try {
        if (child) {
          child.kill('SIGTERM');
          await childClosed;
        }
      } finally {
        if (provider.listening) await new Promise(resolve => provider.close(resolve));
      }
      // Keep all evidence and ledger entries. File deletion requires the owner's cleanup signal.
    }
  }
}
