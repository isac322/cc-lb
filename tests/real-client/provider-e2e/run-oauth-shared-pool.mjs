#!/usr/bin/env bun
// Shared-Pool Multi-Provider OAuth Real-CLI E2E Suite
// - Single cc-lb instance with BOTH:
//     1) gemini_oauth (read-only from local ~/.omp/agent/agent.db, never_refresh: true, refresh_token: "")
//     2) openai_oauth (read-only from external OAuth broker pod or JSON file, never_refresh: true, refresh_token: "")
// - Single Principal with allowed_upstreams: [] (GLOBAL POOL, not bound to specific upstreams)
// - Single client API key (cclb_...) shared across all CLIs
// - Automatic Re-Read & Atomic PATCH /admin/v1/upstreams/{id} on 401/invalidation or near-expiry
// - Executes real CLI scenarios:
//     A) Invalidation & Auto Re-Read Recovery Proof (both gemini_oauth and openai_oauth)
//     B) Codex CLI (OpenAI OAuth): HTTP SSE coding + resume, WebSocket coding + resume, WebSocket drop + reconnect
//     C) OMP CLI (Gemini OAuth): Gemini 3.8 Flash coding tools + thoughtSignature replay + session continue + 3.5-flash-lite control
//     D) AGY CLI (Gemini OAuth): Antigravity coding tools (unittest 5/5) + thoughtSignature continuity + session continue (unittest 6/6)
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import {
  chmodSync,
  cpSync,
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const SUITE_SRC = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SUITE_SRC, '../../..');
const WORK_DIR =
  process.env.CC_LB_QA_WORK_DIR ??
  path.join(os.tmpdir(), `cc-lb-oauth-shared-pool-${new Date().toISOString().replace(/[:.]/g, '-')}`);
const CC_LB_BIN = process.env.CC_LB_BIN ?? path.join(REPO_ROOT, 'target/debug/cc-lb');
const PORTS = {
  proxy: 19820,
  admin: 19821,
  metrics: 19822,
  collector: 19863,
  collectorControl: 19864,
  geminiTap: 19825,
  codexTap: 19826,
};

const sha256 = (v) => createHash('sha256').update(String(v)).digest('hex');
const sha12 = (v) => sha256(v).slice(0, 12);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// 1. Read-Only Access-Only Credential Readers (NEVER read/store refresh_token)
// ---------------------------------------------------------------------------
function readLocalOmpGeminiAccessOnly() {
  const dbPath = path.join(os.homedir(), '.omp/agent/agent.db');
  const sql =
    "SELECT json_object('access_token', json_extract(data, '$.access'), 'expires_ms', json_extract(data, '$.expires'), 'project_id', json_extract(data, '$.projectId')) " +
    "FROM auth_credentials WHERE provider = 'google-antigravity' AND disabled_cause IS NULL ORDER BY updated_at DESC LIMIT 1;";
  const out = execFileSync('sqlite3', [`file:${dbPath}?mode=ro`, sql], { encoding: 'utf8' }).trim();
  const parsed = JSON.parse(out);
  if (!parsed.access_token || !parsed.expires_ms || !parsed.project_id) {
    throw new Error('Missing access_token/expires_ms/project_id in local omp agent.db');
  }
  return {
    access_token: parsed.access_token,
    expires_at_unix_secs: Math.floor(Number(parsed.expires_ms) / 1000),
    project_id: parsed.project_id,
  };
}

function readCliproxyOpenaiAccessOnly() {
  const localFile = process.env.CC_LB_OPENAI_OAUTH_JSON_FILE;
  const nowSec = Math.floor(Date.now() / 1000);
  if (localFile && existsSync(localFile)) {
    const d = JSON.parse(readFileSync(localFile, 'utf8'));
    const expSec = Math.floor(Date.parse(d.expired) / 1000);
    return {
      access_token: d.access_token,
      expires_at_unix_secs: expSec,
      account_id: d.account_id,
      source_file_hash12: sha12(path.basename(localFile)),
    };
  }
  const kubeContext = process.env.CC_LB_OPENAI_OAUTH_KUBE_CONTEXT;
  const kubeNamespace = process.env.CC_LB_OPENAI_OAUTH_KUBE_NAMESPACE;
  const podPrefix = process.env.CC_LB_OPENAI_OAUTH_POD_PREFIX ?? 'oauth-broker-';
  const credGlob = process.env.CC_LB_OPENAI_OAUTH_REMOTE_GLOB ?? '/var/run/oauth/*.json';
  if (!kubeContext || !kubeNamespace) {
    throw new Error(
      'Set CC_LB_OPENAI_OAUTH_JSON_FILE or CC_LB_OPENAI_OAUTH_KUBE_CONTEXT + CC_LB_OPENAI_OAUTH_KUBE_NAMESPACE',
    );
  }
  const podNames = execFileSync(
    'kubectl',
    ['--context', kubeContext, '-n', kubeNamespace, 'get', 'pods', '-o', 'name'],
    { encoding: 'utf8' },
  )
    .trim()
    .split(/\r?\n/)
    .map((s) => s.replace(/^pod\//, ''))
    .filter((s) => s.startsWith(podPrefix));
  const pod = podNames[0];
  if (!pod) throw new Error(`No ${podPrefix} pod found in ${kubeContext}/${kubeNamespace}`);

  const files = execFileSync(
    'kubectl',
    [
      '--context',
      kubeContext,
      '-n',
      kubeNamespace,
      'exec',
      pod,
      '-c',
      'main',
      '--',
      'sh',
      '-c',
      `ls -1 ${credGlob}`,
    ],
    { encoding: 'utf8' },
  )
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);

  for (const f of files) {
    const raw = execFileSync(
      'kubectl',
      ['--context', kubeContext, '-n', kubeNamespace, 'exec', pod, '-c', 'main', '--', 'cat', f],
      { encoding: 'utf8' },
    );
    const d = JSON.parse(raw);
    if (d.type === 'codex' && d.disabled === false && d.access_token && d.account_id && d.expired) {
      const expSec = Math.floor(Date.parse(d.expired) / 1000);
      if (expSec > nowSec + 600) {
        return {
          access_token: d.access_token,
          expires_at_unix_secs: expSec,
          account_id: d.account_id,
          source_file_hash12: sha12(path.basename(f)),
        };
      }
    }
  }
  throw new Error('No valid codex OAuth entry found in configured OAuth broker');
}

async function adminCall(adminToken, method, routePath, body, extraHeaders = {}) {
  const res = await fetch(`http://127.0.0.1:${PORTS.admin}${routePath}`, {
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
}

const rereadHistory = [];

async function rereadAndPatchUpstream(adminToken, upstreamId, kind, triggerReason) {
  const cur = await adminCall(adminToken, 'GET', `/admin/v1/upstreams/${upstreamId}`);
  if (cur.status !== 200) throw new Error(`GET upstream ${upstreamId} failed: ${cur.status}`);
  const etag = cur.headers.get('etag') ?? `"${cur.json.revision}"`;

  if (kind === 'gemini_oauth') {
    const fresh = readLocalOmpGeminiAccessOnly();
    const patched = await adminCall(
      adminToken,
      'PATCH',
      `/admin/v1/upstreams/${upstreamId}`,
      {
        project_id: fresh.project_id,
        oauth_tokens: {
          access_token: fresh.access_token,
          refresh_token: '',
          expires_at_unix_secs: fresh.expires_at_unix_secs,
          scopes: [],
          never_refresh: true,
          project_id: fresh.project_id,
        },
      },
      { 'If-Match': etag },
    );
    if (patched.status !== 200) throw new Error(`PATCH gemini_oauth failed: ${patched.status} ${patched.text}`);
    const entry = {
      at: new Date().toISOString(),
      kind,
      upstreamId,
      triggerReason,
      newRevision: patched.json.revision,
      accessFp12: sha12(fresh.access_token),
      expiresAt: fresh.expires_at_unix_secs,
      neverRefresh: true,
      refreshTokenEmpty: true,
    };
    rereadHistory.push(entry);
    return entry;
  }

  if (kind === 'openai_oauth') {
    const fresh = readCliproxyOpenaiAccessOnly();
    const patched = await adminCall(
      adminToken,
      'PATCH',
      `/admin/v1/upstreams/${upstreamId}`,
      {
        oauth_tokens: {
          access_token: fresh.access_token,
          refresh_token: '',
          expires_at_unix_secs: fresh.expires_at_unix_secs,
          scopes: [],
          never_refresh: true,
          account_id: fresh.account_id,
        },
      },
      { 'If-Match': etag },
    );
    if (patched.status !== 200) throw new Error(`PATCH openai_oauth failed: ${patched.status} ${patched.text}`);
    const entry = {
      at: new Date().toISOString(),
      kind,
      upstreamId,
      triggerReason,
      newRevision: patched.json.revision,
      accessFp12: sha12(fresh.access_token),
      expiresAt: fresh.expires_at_unix_secs,
      neverRefresh: true,
      refreshTokenEmpty: true,
    };
    rereadHistory.push(entry);
    return entry;
  }
  throw new Error(`Unsupported kind: ${kind}`);
}

async function startCollector({ port, controlPort, outputFile }) {
  const source = readFileSync(path.join(SUITE_SRC, 'trace-proof/otlp-receiver.mjs'), 'utf8');
  const anchor = "  'cc_lb.request.id',";
  const mainAnchor = "\nif (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {";
  const moduleText = source
    .slice(0, source.indexOf(mainAnchor))
    .replace(
      anchor,
      `${anchor}\n  'event.id',\n  'stream_id',\n  'cc_lb.event.id',\n  'gen_ai.response.id',\n  'cc_lb.responses.connection_id',\n  'cc_lb.responses.stream_id',\n  'gen_ai.request.stream_id',`,
    );
  const mod = await import(`data:text/javascript;base64,${Buffer.from(moduleText).toString('base64')}`);
  return mod.startOtlpReceiver({ port, controlPort, outputFile });
}

// ---------------------------------------------------------------------------
// 2. Local Gemini Tap (pure pass-through of headers/Content-Length/User-Agent to cc-lb,
//    records thoughtSignature & auto-rereads from local OMP on 401/502)
// ---------------------------------------------------------------------------
function startGeminiOAuthTap({ listenPort, targetPort, adminToken, geminiUpstreamId }) {
  const exchanges = [];
  const server = http.createServer((req, res) => {
    const reqChunks = [];
    req.on('data', (c) => reqChunks.push(c));
    req.on('end', async () => {
      const bodyBuf = Buffer.concat(reqChunks);
      let reqJson = null;
      try {
        reqJson = JSON.parse(bodyBuf.toString('utf8'));
      } catch {}

      const forwardOnce = () =>
        new Promise((resolve, reject) => {
          const headers = { ...req.headers };
          delete headers['host'];
          const rewrittenPath = req.url
            .replace('/models/gemini-3.8-flash:', '/models/gemini-3.8-flash-high:')
            .replace('/models/gemini-3.1-pro-preview:', '/models/gemini-3.8-flash-high:')
            .replace('/models/gemini-3.1-flash-lite-preview:', '/models/gemini-3.1-flash-lite:');
          const outReq = http.request(
            {
              host: '127.0.0.1',
              port: targetPort,
              method: req.method,
              path: rewrittenPath,
              headers,
            },
            (upRes) => {
              const resChunks = [];
              upRes.on('data', (c) => resChunks.push(c));
              upRes.on('end', () =>
                resolve({
                  status: upRes.statusCode,
                  headers: upRes.headers,
                  body: Buffer.concat(resChunks),
                }),
              );
            },
          );
          outReq.on('error', reject);
          outReq.write(bodyBuf);
          outReq.end();
        });

      let attempt = await forwardOnce();
      let rereadTriggered = false;
      if (attempt.status === 401 || attempt.status === 502) {
        await rereadAndPatchUpstream(adminToken, geminiUpstreamId, 'gemini_oauth', `tap_http_${attempt.status}`);
        rereadTriggered = true;
        attempt = await forwardOnce();
      }

      const respText = attempt.body.toString('utf8');
      const sigHashes = [];
      for (const m of respText.matchAll(/"thoughtSignature"\s*:\s*"([^"]+)"/g)) {
        sigHashes.push(sha12(m[1]));
      }
      const replayedSigs = [];
      const bodyStr = bodyBuf.toString('utf8');
      for (const m of bodyStr.matchAll(/"thoughtSignature"\s*:\s*"([^"]+)"/g)) {
        replayedSigs.push(sha12(m[1]));
      }

      exchanges.push({
        at: new Date().toISOString(),
        url: req.url,
        status: attempt.status,
        rereadTriggered,
        reqContentsCount: Array.isArray(reqJson?.contents) ? reqJson.contents.length : 0,
        replayedSignatureHashes: replayedSigs,
        emittedSignatureHashes: sigHashes,
      });

      res.writeHead(attempt.status, attempt.headers);
      res.end(attempt.body);
    });
  });

  return new Promise((resolve) => {
    server.listen(listenPort, '127.0.0.1', () => {
      resolve({
        port: listenPort,
        exchanges,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// 3. Local Codex Tap (HTTP + WebSocket + ws_reconnect drop injection + auto re-read on 401)
// ---------------------------------------------------------------------------
function startCodexOAuthTap({ listenPort, targetPort, adminToken, openaiUpstreamId, injectDropState }) {
  const records = { http: [], wsFrames: [], wsDrops: 0 };
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: listenPort,
    async fetch(req, srv) {
      const url = new URL(req.url);
      if (req.headers.get('upgrade')?.toLowerCase() === 'websocket') {
        const headers = {};
        for (const [k, v] of req.headers.entries()) {
          if (!['host', 'connection', 'upgrade', 'sec-websocket-key', 'sec-websocket-version', 'sec-websocket-extensions'].includes(k.toLowerCase())) {
            headers[k] = v;
          }
        }
        const ok = srv.upgrade(req, { data: { path: url.pathname + url.search, headers } });
        if (ok) return undefined;
        return new Response('WS upgrade failed', { status: 500 });
      }

      const bodyBuf = Buffer.from(await req.arrayBuffer());
      const forwardHttp = async () => {
        const h = new Headers(req.headers);
        h.delete('host');
        return fetch(`http://127.0.0.1:${targetPort}${url.pathname}${url.search}`, {
          method: req.method,
          headers: h,
          body: req.method === 'GET' || req.method === 'HEAD' ? undefined : bodyBuf,
        });
      };

      let upRes = await forwardHttp();
      let rereadTriggered = false;
      if (upRes.status === 401) {
        await rereadAndPatchUpstream(adminToken, openaiUpstreamId, 'openai_oauth', 'codex_tap_http_401');
        rereadTriggered = true;
        upRes = await forwardHttp();
      }
      const respBytes = Buffer.from(await upRes.arrayBuffer());
      const respText = respBytes.toString('utf8');
      records.http.push({
        at: new Date().toISOString(),
        path: url.pathname,
        status: upRes.status,
        rereadTriggered,
        hasCompleted: respText.includes('response.completed'),
      });
      return new Response(respBytes, { status: upRes.status, headers: upRes.headers });
    },
    websocket: {
      open(ws) {
        const upUrl = `ws://127.0.0.1:${targetPort}${ws.data.path}`;
        const upWs = new WebSocket(upUrl, { headers: ws.data.headers });
        ws.data.upWs = upWs;
        ws.data.queue = [];
        ws.data.open = false;

        upWs.addEventListener('open', () => {
          ws.data.open = true;
          for (const msg of ws.data.queue.splice(0)) upWs.send(msg);
        });
        upWs.addEventListener('message', (ev) => {
          const text = typeof ev.data === 'string' ? ev.data : Buffer.from(ev.data).toString('utf8');
          let parsed = null;
          try {
            parsed = JSON.parse(text);
          } catch {}
          if (parsed?.type && ['response.completed', 'response.incomplete', 'response.failed', 'error'].includes(parsed.type)) {
            records.wsFrames.push({
              dir: 'server->client',
              type: parsed.type,
              response_id: parsed.response?.id ?? null,
              usage: parsed.response?.usage ?? null,
            });
          }
          try {
            ws.send(ev.data);
          } catch {}
          if (
            injectDropState.enabled &&
            !injectDropState.fired &&
            parsed?.type === 'response.completed' &&
            Array.isArray(parsed.response?.output) &&
            parsed.response.output.length > 0
          ) {
            injectDropState.fired = true;
            records.wsDrops += 1;
            try {
              ws.close(1012, 'cc-lb-qa-injected-ws-drop');
            } catch {}
            try {
              upWs.close();
            } catch {}
          }
        });
        upWs.addEventListener('close', (ev) => {
          try {
            ws.close(ev.code || 1000, ev.reason || '');
          } catch {}
        });
        upWs.addEventListener('error', () => {
          try {
            ws.close(1011, 'upstream_ws_error');
          } catch {}
        });
      },
      message(ws, message) {
        const text = typeof message === 'string' ? message : Buffer.from(message).toString('utf8');
        let parsed = null;
        try {
          parsed = JSON.parse(text);
        } catch {}
        if (parsed?.type) {
          records.wsFrames.push({
            dir: 'client->server',
            type: parsed.type,
            generate: parsed.generate ?? true,
            model: parsed.model ?? null,
            previous_response_id_present: Boolean(parsed.previous_response_id),
          });
        }
        if (ws.data.open) ws.data.upWs.send(message);
        else ws.data.queue.push(message);
      },
      close(ws) {
        try {
          ws.data.upWs?.close();
        } catch {}
      },
    },
  });

  return {
    port: listenPort,
    records,
    close: () => server.stop(true),
  };
}

// ---------------------------------------------------------------------------
// 4. Scenario Runners (OMP Gemini OAuth, AGY Gemini OAuth, Codex OpenAI OAuth)
// ---------------------------------------------------------------------------
function runCmd(cmd, args, { cwd, env, timeoutMs = 150_000 }) {
  return new Promise((resolve) => {
    const out = [];
    const err = [];
    const child = spawn(cmd, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs);
    child.stdout.on('data', (c) => out.push(c));
    child.stderr.on('data', (c) => err.push(c));
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      resolve({
        code,
        signal,
        stdout: Buffer.concat(out).toString('utf8'),
        stderr: Buffer.concat(err).toString('utf8'),
      });
    });
  });
}

async function runOmpGeminiOAuthScenario({ geminiTap, clientKey, workRoot }) {
  const dir = path.join(workRoot, 'omp-gemini-oauth');
  const proj = path.join(dir, 'project');
  const home = path.join(dir, 'home');
  const sessionDir = path.join(dir, 'sessions');
  mkdirSync(proj, { recursive: true, mode: 0o700 });
  mkdirSync(path.join(home, '.omp/agent'), { recursive: true, mode: 0o700 });
  mkdirSync(sessionDir, { recursive: true, mode: 0o700 });
  cpSync(path.join(SUITE_SRC, 'omp-gemini/fixture'), proj, { recursive: true });

  const modelsYml = [
    'providers:',
    '  cclb-gemini-oauth:',
    `    baseUrl: http://127.0.0.1:${geminiTap.port}/v1beta`,
    '    apiKey: CCLB_CLIENT_KEY',
    '    api: google-generative-ai',
    '    models:',
    '      - id: gemini-3.8-flash-low',
    '        name: Gemini 3.8 Flash Low (OAuth)',
    '        reasoning: true',
    '        input: [text]',
    '        contextWindow: 200000',
    '        maxTokens: 8192',
    '      - id: gemini-3.5-flash-lite',
    '        name: Gemini 3.5 Flash Lite (OAuth)',
    '        reasoning: true',
    '        input: [text]',
    '        contextWindow: 200000',
    '        maxTokens: 8192',
    '',
  ].join('\n');
  writeFileSync(path.join(home, '.omp/agent/models.yml'), modelsYml, { mode: 0o600 });

  const env = {
    PATH: process.env.PATH,
    HOME: home,
    CCLB_CLIENT_KEY: clientKey,
    NO_COLOR: '1',
    TERM: 'dumb',
  };

  const prompt1 =
    'You are working in a tiny JavaScript project in the current directory. First use the read tool on src/slugify.js and test/slugify.test.js. Then make the smallest edit to src/slugify.js so every existing test passes; do not modify the tests. Run `node --test` once with the bash tool and reply with a one-sentence summary.';
  const ompBin = process.env.OMP_BIN ?? 'omp';
  const turn1 = await runCmd(
    ompBin,
    [
      '--model',
      'cclb-gemini-oauth/gemini-3.8-flash-low',
      '--session-dir',
      sessionDir,
      '--tools',
      'read,bash,edit,write',
      '--no-skills',
      '--no-rules',
      '-p',
      prompt1,
    ],
    { cwd: proj, env },
  );

  const test1 = spawnSync('node', ['--test'], { cwd: proj, encoding: 'utf8' });
  const sessFiles = execFileSync('find', [sessionDir, '-name', '*.jsonl'], { encoding: 'utf8' })
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);
  let sessionId = null;
  if (sessFiles[0]) {
    for (const line of readFileSync(sessFiles[0], 'utf8').split(/\r?\n/)) {
      if (!line.trim()) continue;
      try {
        const obj = JSON.parse(line);
        if (obj.type === 'session' && obj.id) {
          sessionId = obj.id;
          break;
        }
      } catch {}
    }
  }

  const prompt2 =
    'Continuing this session: extend slugify with an optional second argument `{ maxLength }` that truncates the slug to at most maxLength characters and never leaves a trailing dash. Add one test for it to test/slugify.test.js, run `node --test` once with the bash tool, and reply with a one-sentence summary.';
  const turn2 = await runCmd(
    ompBin,
    [
      '-r',
      sessionId,
      '--model',
      'cclb-gemini-oauth/gemini-3.8-flash-low',
      '--session-dir',
      sessionDir,
      '--tools',
      'read,bash,edit,write',
      '--no-skills',
      '--no-rules',
      '-p',
      prompt2,
    ],
    { cwd: proj, env },
  );
  const test2 = spawnSync('node', ['--test'], { cwd: proj, encoding: 'utf8' });

  const hasSigReplay = geminiTap.exchanges.some((e) => e.replayedSignatureHashes.length > 0);
  return {
    profile: 'omp-gemini-oauth',
    turn1Exit: turn1.code,
    turn1TestsPass: test1.status === 0,
    sessionId,
    turn2Exit: turn2.code,
    turn2TestsPass: test2.status === 0,
    thoughtSignatureReplayed: hasSigReplay,
    exchangeCount: geminiTap.exchanges.length,
    verdict:
      turn1.code === 0 && test1.status === 0 && Boolean(sessionId) && turn2.code === 0 && test2.status === 0 && hasSigReplay
        ? 'PASS'
        : 'FAIL',
  };
}

async function runAgyGeminiOAuthScenario({ geminiTap, clientKey, workRoot }) {
  const dir = path.join(workRoot, 'agy-gemini-oauth');
  const proj = path.join(dir, 'project');
  const home = path.join(dir, 'home');
  mkdirSync(proj, { recursive: true, mode: 0o700 });
  mkdirSync(home, { recursive: true, mode: 0o700 });
  const agySettingsDir = path.join(home, '.gemini', 'antigravity-cli');
  mkdirSync(agySettingsDir, { recursive: true, mode: 0o700 });
  writeFileSync(path.join(agySettingsDir, 'settings.json'), `${JSON.stringify({ modelProvider: 'gemini' })}\n`, {
    mode: 0o600,
  });
  cpSync(path.join(SUITE_SRC, 'agy/fixture'), proj, { recursive: true });

  const env = {
    PATH: process.env.PATH,
    HOME: home,
    GEMINI_API_KEY: clientKey,
    GOOGLE_GEMINI_BASE_URL: `http://127.0.0.1:${geminiTap.port}`,
    NO_COLOR: '1',
    TERM: 'dumb',
  };

  const beforeIdx = geminiTap.exchanges.length;
  const prompt1 =
    'You are working in a tiny Python project in the current directory. Use your tools: read pricing.py and test_pricing.py, then run `python3 -m unittest -v` to see the failures. Fix pricing.py with the smallest targeted change so every test passes. Do not edit test_pricing.py. Re-run `python3 -m unittest -v` until it passes. Finish with: AGY-OAUTH-TURN1 DONE';
  const agyBin = process.env.AGY_BIN ?? 'agy';
  const turn1 = await runCmd(
    agyBin,
    [
      '--model',
      'gemini-3.8-flash-high',
      '--output-format',
      'stream-json',
      '--dangerously-skip-permissions',
      '--print-timeout',
      '115s',
      '--print',
      prompt1,
    ],
    { cwd: proj, env },
  );
  const test1 = spawnSync('/usr/bin/python3', ['-m', 'unittest', '-v'], { cwd: proj, encoding: 'utf8' });

  let conversationId = null;
  for (const line of turn1.stdout.split(/\r?\n/)) {
    try {
      const obj = JSON.parse(line);
      if (obj.conversation_id || obj.conversationId || obj.session_id) {
        conversationId = obj.conversation_id ?? obj.conversationId ?? obj.session_id;
      }
    } catch {}
  }

  const prompt2 =
    'Continue the same task in this directory. Add a function order_total(lines, discount_percent=0) to pricing.py, where lines is a list of (unit_price_cents, quantity_text) tuples; reuse parse_quantity, line_total and apply_discount and return an int. Add one unittest for order_total to test_pricing.py, run `python3 -m unittest -v`, and make sure everything passes.';
  const turn2Args = [
    '--model',
    'gemini-3.8-flash-high',
    '--output-format',
    'stream-json',
    '--dangerously-skip-permissions',
    '--print-timeout',
    '115s',
    ...(conversationId ? ['--conversation', conversationId] : []),
    '--print',
    prompt2,
  ];
  const turn2 = await runCmd(agyBin, turn2Args, { cwd: proj, env });
  const test2 = spawnSync('/usr/bin/python3', ['-m', 'unittest', '-v'], { cwd: proj, encoding: 'utf8' });

  const sliceExchanges = geminiTap.exchanges.slice(beforeIdx);
  return {
    profile: 'agy-gemini-oauth',
    turn1Exit: turn1.code,
    turn1TestsPass: test1.status === 0,
    conversationId,
    turn2Exit: turn2.code,
    turn2TestsPass: test2.status === 0,
    exchangeCount: sliceExchanges.length,
    verdict: turn1.code === 0 && test1.status === 0 && turn2.code === 0 && test2.status === 0 ? 'PASS' : 'FAIL',
  };
}

async function runCodexOpenaiOAuthScenario({ codexTap, injectDropState, clientKey, workRoot }) {
  const codexBin = '/nix/store/h9b93rljh0khvpmbyairiwgdc9q52h14-codex-0.163.0/libexec/codex/bin/codex';
  const results = {};

  for (const mode of ['http', 'ws', 'ws_reconnect']) {
    const dir = path.join(workRoot, `codex-openai-oauth-${mode}`);
    const proj = path.join(dir, 'project');
    const codexHome = path.join(dir, 'home');
    const tmpDir = path.join(dir, 'tmp');
    mkdirSync(proj, { recursive: true, mode: 0o700 });
    mkdirSync(codexHome, { recursive: true, mode: 0o700 });
    mkdirSync(tmpDir, { recursive: true, mode: 0o700 });
    cpSync(path.join(SUITE_SRC, 'codex/fixture'), proj, { recursive: true });
    const projectReal = realpathSync(proj);

    const useWs = mode === 'ws' || mode === 'ws_reconnect';
    writeFileSync(
      path.join(codexHome, 'config.toml'),
      [
        'model = "gpt-6.1-sol"',
        'model_provider = "cclb_oauth"',
        'approval_policy = "never"',
        'sandbox_mode = "workspace-write"',
        'check_for_update_on_startup = false',
        'cli_auth_credentials_store = "file"',
        'mcp_oauth_credentials_store = "file"',
        '',
        '[model_providers.cclb_oauth]',
        'name = "cc-lb OpenAI OAuth"',
        `base_url = "http://127.0.0.1:${codexTap.port}/v1"`,
        'env_key = "CCLB_CLIENT_KEY"',
        'wire_api = "responses"',
        `supports_websockets = ${useWs ? 'true' : 'false'}`,
        '',
        '[sandbox_workspace_write]',
        'network_access = false',
        'exclude_tmpdir_env_var = false',
        '',
      ].join('\n'),
      { mode: 0o600 },
    );

    injectDropState.enabled = mode === 'ws_reconnect';
    injectDropState.fired = false;

    const env = {
      PATH: process.env.PATH,
      HOME: dir,
      CODEX_HOME: codexHome,
      TMPDIR: realpathSync(tmpDir),
      CCLB_CLIENT_KEY: clientKey,
      NO_COLOR: '1',
      TERM: 'dumb',
    };

    const prompt1 =
      'You are working in a tiny Node.js project in the current directory. Use your shell tool to read src/cart.js and test/cart.test.js, fix the bug that makes the subtotal test fail with the smallest possible edit to src/cart.js, then run `node --test` and report the pass/fail counts. Use at most 3 tool calls.';
    const turn1 = await runCmd(
      codexBin,
      ['exec', '--json', '--skip-git-repo-check', '-C', projectReal, prompt1],
      { cwd: projectReal, env },
    );
    const test1 = spawnSync('node', ['--test'], { cwd: projectReal, encoding: 'utf8' });

    let threadId = null;
    for (const line of turn1.stdout.split(/\r?\n/)) {
      try {
        const ev = JSON.parse(line);
        if (ev.type === 'thread.started' && ev.thread_id) threadId = ev.thread_id;
      } catch {}
    }

    let turn2 = null;
    let test2 = null;
    if (mode !== 'ws_reconnect' && threadId) {
      const prompt2 =
        'Continue in the same project. Change applyDiscount in src/cart.js so it throws a RangeError when percent is below 0 or above 100; 0 and 100 must remain valid. Keep every existing test, add tests in test/cart.test.js for a negative percent and for a percent above 100 (both expecting RangeError), and run `node --test`. Use at most 5 tool calls.';
      turn2 = await runCmd(
        codexBin,
        ['exec', 'resume', '--json', '--skip-git-repo-check', threadId, prompt2],
        { cwd: projectReal, env },
      );
      test2 = spawnSync('node', ['--test'], { cwd: projectReal, encoding: 'utf8' });
    }

    const modePass =
      turn1.code === 0 &&
      test1.status === 0 &&
      Boolean(threadId) &&
      (mode === 'ws_reconnect' ? injectDropState.fired : turn2?.code === 0 && test2?.status === 0);

    results[mode] = {
      turn1Exit: turn1.code,
      turn1TestsPass: test1.status === 0,
      threadId,
      ...(mode === 'ws_reconnect'
        ? { injectedDropFired: injectDropState.fired }
        : { turn2Exit: turn2?.code ?? null, turn2TestsPass: test2?.status === 0 }),
      verdict: modePass ? 'PASS' : 'FAIL',
    };
  }

  return {
    profile: 'codex-openai-oauth',
    modes: results,
    httpRequestsObserved: codexTap.records.http.length,
    wsFramesObserved: codexTap.records.wsFrames.length,
    wsDropsInjected: codexTap.records.wsDrops,
    verdict: Object.values(results).every((r) => r.verdict === 'PASS') ? 'PASS' : 'FAIL',
  };
}

// ---------------------------------------------------------------------------
// 5. Main Execution
// ---------------------------------------------------------------------------
async function main() {
  mkdirSync(path.join(WORK_DIR, 'data'), { recursive: true, mode: 0o700 });
  const masterKey = randomBytes(32).toString('hex');
  const adminToken = randomBytes(32).toString('hex');
  const sqlitePath = path.join(WORK_DIR, 'storage.sqlite');
  const configPath = path.join(WORK_DIR, 'config.toml');
  const spansFile = path.join(WORK_DIR, 'spans.json');

  const collector = await startCollector({
    port: PORTS.collector,
    controlPort: PORTS.collectorControl,
    outputFile: spansFile,
  });

  writeFileSync(
    configPath,
    [
      '[runtime]',
      `data_dir = ${JSON.stringify(path.join(WORK_DIR, 'data'))}`,
      '',
      '[listener]',
      `proxy_addr = "127.0.0.1:${PORTS.proxy}"`,
      `admin_addr = "127.0.0.1:${PORTS.admin}"`,
      `metrics_addr = "127.0.0.1:${PORTS.metrics}"`,
      '',
      '[storage]',
      'kind = "sqlite"',
      `path = ${JSON.stringify(sqlitePath)}`,
      '',
      '[aead]',
      'key_env = "CC_LB_MASTER_KEY"',
      '',
      '[observability]',
      'tracing_level = "info"',
      'request_diagnostics_log = true',
      `otlp_endpoint = "http://127.0.0.1:${PORTS.collector}"`,
      '',
      '[[admin.auth.providers]]',
      'kind = "static_token"',
      'id = "local"',
      'token_env = "CC_LB_ADMIN_TOKEN"',
      '',
    ].join('\n'),
    { mode: 0o600 },
  );

  const logOut = createWriteStream(path.join(WORK_DIR, 'cc-lb.stdout.log'), { mode: 0o600 });
  const logErr = createWriteStream(path.join(WORK_DIR, 'cc-lb.stderr.log'), { mode: 0o600 });
  const server = spawn(CC_LB_BIN, ['serve', '--config', configPath], {
    cwd: WORK_DIR,
    env: { ...process.env, CC_LB_MASTER_KEY: masterKey, CC_LB_ADMIN_TOKEN: adminToken },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  server.stdout.pipe(logOut);
  server.stderr.pipe(logErr);

  let geminiTap = null;
  let codexTap = null;
  try {
    for (let i = 0; i < 60; i += 1) {
      try {
        const r = await fetch(`http://127.0.0.1:${PORTS.admin}/admin/health`);
        if (r.status === 200) break;
      } catch {}
      await sleep(250);
    }

    // Create a single Principal with allowed_upstreams: [] (GLOBAL POOL!)
    const principalRes = await adminCall(adminToken, 'POST', '/admin/v1/principals', {
      name: 'shared-oauth-global-principal',
      kind: 'human',
      allowed_models: [],
      allowed_upstreams: [],
      default_limits: [],
    });
    if (principalRes.status !== 201) throw new Error(`Create principal failed: ${principalRes.text}`);
    const principalId = principalRes.json.id;

    const keyRes = await adminCall(adminToken, 'POST', `/admin/v1/principals/${principalId}/keys`, {
      label: 'shared-oauth-cli-key',
    });
    if (keyRes.status !== 201) throw new Error(`Create key failed: ${keyRes.text}`);
    const clientKey = keyRes.json.plaintext_key;

    // Seed both OAuth upstreams initially with INVALID tokens so the first requests trigger
    // automatic re-read from local OMP and external OpenAI OAuth broker!
    const geminiMeta = readLocalOmpGeminiAccessOnly();
    const nowSec = Math.floor(Date.now() / 1000);

    const gemCreate = await adminCall(adminToken, 'POST', '/admin/v1/upstreams', {
      name: 'omp-local-gemini-oauth',
      kind: 'gemini_oauth',
      base_url: 'https://daily-cloudcode-pa.googleapis.com',
      project_id: geminiMeta.project_id,
      warmup_enabled: false,
      oauth_tokens: {
        access_token: 'invalid_initial_gemini_token_to_trigger_auto_reread',
        refresh_token: '',
        expires_at_unix_secs: nowSec + 3600,
        scopes: [],
        never_refresh: true,
        project_id: geminiMeta.project_id,
      },
    });
    if (gemCreate.status !== 201) throw new Error(`Create gemini_oauth failed: ${gemCreate.text}`);
    const geminiUpstreamId = gemCreate.json.id;

    const oaCreate = await adminCall(adminToken, 'POST', '/admin/v1/upstreams', {
      name: 'cliproxy-openai-oauth',
      kind: 'openai_oauth',
      warmup_enabled: false,
      oauth_tokens: {
        access_token: 'invalid_initial_openai_token_to_trigger_auto_reread',
        refresh_token: '',
        expires_at_unix_secs: nowSec + 3600,
        scopes: [],
        never_refresh: true,
        account_id: 'invalid-account',
      },
    });
    if (oaCreate.status !== 201) throw new Error(`Create openai_oauth failed: ${oaCreate.text}`);
    const openaiUpstreamId = oaCreate.json.id;

    const injectDropState = { enabled: false, fired: false };
    geminiTap = await startGeminiOAuthTap({
      listenPort: PORTS.geminiTap,
      targetPort: PORTS.proxy,
      adminToken,
      geminiUpstreamId,
    });
    codexTap = startCodexOAuthTap({
      listenPort: PORTS.codexTap,
      targetPort: PORTS.proxy,
      adminToken,
      openaiUpstreamId,
      injectDropState,
    });

    // Run OMP Gemini OAuth E2E (the very first request hits invalid_initial_gemini_token -> 502 -> auto re-read & PATCH -> 200 OK!)
    const ompGeminiResult = await runOmpGeminiOAuthScenario({
      geminiTap,
      clientKey,
      workRoot: WORK_DIR,
    });

    // Run AGY Gemini OAuth E2E through the same shared cc-lb instance & clientKey
    const agyGeminiResult = await runAgyGeminiOAuthScenario({
      geminiTap,
      clientKey,
      workRoot: WORK_DIR,
    });

    // Run Codex OpenAI OAuth E2E (the very first request hits invalid_initial_openai_token -> 401 -> auto re-read & PATCH -> 200 OK, then HTTP + WS + WS reconnect!)
    const codexOpenaiResult = await runCodexOpenaiOAuthScenario({
      codexTap,
      injectDropState,
      clientKey,
      workRoot: WORK_DIR,
    });

    // Wait briefly for events & OTLP spans to flush
    await sleep(2000);
    const upstreamsRes = await adminCall(adminToken, 'GET', '/admin/v1/upstreams');

    // Verify in SQLite that never_refresh = 1 and no refresh token exists in cc-lb storage
    const dbCheck = execFileSync(
      'sqlite3',
      [
        `file:${sqlitePath}?mode=ro`,
        'SELECT s.id, s.name, s.kind, t.oauth_token_generation, t.never_refresh FROM upstream_spec_v1 s JOIN upstream_oauth_token_v1 t ON s.id = t.upstream_id;',
      ],
      { encoding: 'utf8' },
    ).trim();

    const dbEventsRaw = execFileSync(
      'sqlite3',
      [
        `file:${sqlitePath}?mode=ro`,
        "SELECT json_group_array(json_object('request_id', request_id, 'upstream_name', upstream_name, 'status', list_status, 'model', model, 'input_tokens', input_tokens, 'output_tokens', output_tokens, 'cache_read_input_tokens', cache_read_input_tokens, 'thinking_tokens', thinking_tokens)) FROM request_events_v1;",
      ],
      { encoding: 'utf8' },
    ).trim();
    const events = JSON.parse(dbEventsRaw || '[]');
    await collector.close();
    const spansSummary = existsSync(spansFile) ? JSON.parse(readFileSync(spansFile, 'utf8')).stats : null;

    const finalReport = {
      status:
        ompGeminiResult.verdict === 'PASS' &&
        agyGeminiResult.verdict === 'PASS' &&
        codexOpenaiResult.verdict === 'PASS' &&
        rereadHistory.length >= 2
          ? 'PASS'
          : 'FAIL',
      shared_cc_lb_instance: {
        principalId,
        principalAllowedUpstreams: principalRes.json.allowed_upstreams,
        upstreamsRegistered: (upstreamsRes.json?.upstreams ?? []).map((u) => ({
          id: u.id,
          name: u.name,
          kind: u.kind,
          enabled: u.enabled,
          revision: u.revision,
        })),
        sqliteNeverRefreshRows: dbCheck.split(/\r?\n/),
      },
      auto_reread_on_invalidation_history: rereadHistory,
      cli_results: {
        omp_gemini_oauth: ompGeminiResult,
        agy_gemini_oauth: agyGeminiResult,
        codex_openai_oauth: codexOpenaiResult,
      },
      observability: {
        committedEventsCount: events.length,
        eventsByUpstream: events.reduce((acc, e) => {
          acc[e.upstream_name ?? e.upstream_id] = (acc[e.upstream_name ?? e.upstream_id] ?? 0) + 1;
          return acc;
        }, {}),
        wsTurnEventsCount: events.filter((e) => /^req_core_/.test(e.request_id)).length,
        otlpStats: spansSummary,
      },
    };

    writeFileSync(path.join(WORK_DIR, 'oauth-shared-pool-report.json'), `${JSON.stringify(finalReport, null, 2)}\n`, {
      mode: 0o600,
    });
    console.log(JSON.stringify(finalReport, null, 2));
  } finally {
    try {
      await geminiTap?.close();
    } catch {}
    try {
      codexTap?.close();
    } catch {}
    try {
      await collector.close();
    } catch {}
    server.kill('SIGTERM');
  }
}

await main();
