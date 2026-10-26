#!/usr/bin/env bun
// Actual Antigravity CLI (agy) coding-agent QA through cc-lb with a real Gemini upstream.
//
// Topology: agy (fresh HOME, API-key mode, local principal key only)
//   -> labeled byte-transparent observer on 127.0.0.1:<ephemeral> (hash/boolean capture only, never rewrites bytes)
//   -> cc-lb proxy (ports from prepareSlice) -> real Gemini provider.
// AGY_OBSERVER=0 points agy straight at the proxy; thoughtSignature continuity then stays NOT_RUN.
//
// Flags used (all from `agy --help`): --model, --output-format stream-json, --dangerously-skip-permissions,
// --print-timeout, --log-file, --conversation <id>, --print <prompt> (kept last so it parses as either
// a string flag or a bool flag followed by a positional prompt).

import { spawn, spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';
import * as rt from '../qa-runtime.mjs';

process.umask(0o077);

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CAMPAIGN = path.dirname(HERE);
const CONTRACT = JSON.parse(fs.readFileSync(path.join(CAMPAIGN, 'contract.json'), 'utf8'));
const SLICE = CONTRACT.slices.find((s) => s.id === 'agy');
const BOUNDS = CONTRACT.paid_bounds;
const FIXTURE = path.join(HERE, 'fixture');

const AGY_BIN = process.env.AGY_BIN ?? '/tmp/cc-lb-antigravity-cli/agy';
// agy has no native label for gemini-3-pro-preview (binary only exposes e.g. gemini-3.1-pro-low,
// gemini-3.8-flash-high). Default: agy's own Pro low-thinking label; earlier agy captures sent it on the
// wire as gemini-3.1-pro-preview and used gemini-3.1-flash-lite-preview for auxiliary calls.
// Override both via AGY_MODEL_LABEL / AGY_WIRE_MODEL; the observed wire model is asserted below.
const AGY_MODEL_LABEL = process.env.AGY_MODEL_LABEL ?? 'gemini-3.1-pro-low';
const WIRE_MODEL = process.env.AGY_WIRE_MODEL ?? 'gemini-3.1-pro-preview';
const PRICED_MODELS = (process.env.AGY_PRICED_MODELS ?? 'gemini-3.1-flash-lite-preview')
  .split(',')
  .map((m) => m.trim())
  .filter(Boolean);
const USE_OBSERVER = process.env.AGY_OBSERVER !== '0';
const RUN_WALL_MS = BOUNDS.wall_seconds_per_cli_run * 1000;
const PRINT_TIMEOUT_S = BOUNDS.wall_seconds_per_cli_run - 5;
let GEN_CAP = BOUNDS.generation_attempts_per_slice;
const EVENT_DEADLINE_MS = 30_000;
const PYTHON = '/usr/bin/python3';

const TURN_PROMPTS = [
  [
    'You are working in a tiny Python project in the current directory.',
    'Use your tools: read pricing.py and test_pricing.py, then run `python3 -m unittest -v` to see the failures.',
    'Fix pricing.py with the smallest targeted change so every test passes. Do not edit test_pricing.py and do not touch files outside this directory.',
    'Re-run `python3 -m unittest -v` until it passes.',
    'Finish with one line exactly like: AGY-QA-TURN1 DONE tests=<passed>/<total>',
  ].join(' '),
  [
    'Continue the same task in this directory.',
    'Add a function order_total(lines, discount_percent=0) to pricing.py, where lines is a list of (unit_price_cents, quantity_text) tuples;',
    'it must reuse parse_quantity, line_total and apply_discount and return an int.',
    'Add one unittest for order_total to test_pricing.py, run `python3 -m unittest -v`, and make sure everything passes.',
    'Finish with one line exactly like: AGY-QA-TURN2 DONE tests=<passed>/<total>',
  ].join(' '),
];

// Driver-side acceptance check for turn 2 (never shown to the client).
const TURN2_CHECK = [
  'import pricing',
  'assert pricing.order_total([(250, "4"), (999, " 1 ")], 10) == 1799, pricing.order_total([(250, "4"), (999, " 1 ")], 10)',
  'assert pricing.order_total([(100, "2")]) == 200',
  'assert isinstance(pricing.order_total([(100, "2")], 15), int)',
  'print("order_total ok")',
].join('\n');

const shortHash = (value) => crypto.createHash('sha256').update(String(value)).digest('hex').slice(0, 16);
const nowIso = () => new Date().toISOString();

async function sha256File(file) {
  const hash = crypto.createHash('sha256');
  for await (const chunk of fs.createReadStream(file)) hash.update(chunk);
  return hash.digest('hex');
}

function writePrivate(file, content) {
  fs.writeFileSync(file, content, { mode: 0o600 });
  fs.chmodSync(file, 0o600);
}

function appendPrivate(file, line) {
  fs.appendFileSync(file, `${line}\n`, { mode: 0o600 });
}

// ---------------------------------------------------------------------------
// Passive HTTP/1.1 parser (observer only; never feeds bytes back to the wire).
// ---------------------------------------------------------------------------
class HttpStreamParser {
  constructor(kind, { onHead, onMessage }) {
    this.kind = kind;
    this.onHead = onHead;
    this.onMessage = onMessage;
    this.buf = Buffer.alloc(0);
    this.state = 'head';
    this.cur = null;
    this.remaining = 0;
  }

  push(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    this.drain();
  }

  end() {
    if (this.cur && this.state === 'close-body') this.finish(true);
    else if (this.cur) this.finish(false);
  }

  add(part) {
    if (part.length) this.cur.chunks.push(Buffer.from(part));
  }

  finish(complete = true) {
    const message = { head: this.cur.head, body: Buffer.concat(this.cur.chunks), complete, endAt: Date.now() };
    this.cur = null;
    this.state = 'head';
    this.onMessage?.(message);
  }

  drain() {
    for (;;) {
      if (this.state === 'broken') return;
      if (this.state === 'head') {
        const idx = this.buf.indexOf('\r\n\r\n');
        if (idx < 0) return;
        const text = this.buf.subarray(0, idx).toString('latin1');
        this.buf = this.buf.subarray(idx + 4);
        const [start, ...lines] = text.split('\r\n');
        const headers = {};
        for (const line of lines) {
          const colon = line.indexOf(':');
          if (colon <= 0) continue;
          const key = line.slice(0, colon).trim().toLowerCase();
          const value = line.slice(colon + 1).trim();
          headers[key] = headers[key] === undefined ? value : `${headers[key]}, ${value}`;
        }
        const head = { start, headers, at: Date.now() };
        if (this.kind === 'response') {
          const parts = start.split(' ');
          head.version = parts[0];
          head.status = Number(parts[1]);
          if (head.status >= 100 && head.status < 200 && head.status !== 101) continue;
        } else {
          const [method, target, version] = start.split(' ');
          Object.assign(head, { method, target, version });
        }
        this.cur = { head, chunks: [] };
        this.onHead?.(head);
        const te = String(headers['transfer-encoding'] ?? '').toLowerCase();
        if (te.includes('chunked')) this.state = 'chunk-size';
        else if (headers['content-length'] !== undefined) {
          this.remaining = Number(headers['content-length']);
          this.state = this.remaining > 0 ? 'cl-body' : 'done';
        } else if (this.kind === 'response' && head.status !== 204 && head.status !== 304) this.state = 'close-body';
        else this.state = 'done';
        if (this.state === 'done') this.finish();
        continue;
      }
      if (this.state === 'cl-body') {
        if (!this.buf.length) return;
        const take = Math.min(this.remaining, this.buf.length);
        this.add(this.buf.subarray(0, take));
        this.buf = this.buf.subarray(take);
        this.remaining -= take;
        if (this.remaining > 0) return;
        this.finish();
        continue;
      }
      if (this.state === 'chunk-size') {
        const idx = this.buf.indexOf('\r\n');
        if (idx < 0) return;
        const size = Number.parseInt(this.buf.subarray(0, idx).toString('latin1').split(';')[0].trim(), 16);
        this.buf = this.buf.subarray(idx + 2);
        if (Number.isNaN(size)) {
          this.state = 'broken';
          return;
        }
        if (size === 0) this.state = 'trailers';
        else {
          this.remaining = size;
          this.state = 'chunk-data';
        }
        continue;
      }
      if (this.state === 'chunk-data') {
        if (!this.buf.length) return;
        const take = Math.min(this.remaining, this.buf.length);
        this.add(this.buf.subarray(0, take));
        this.buf = this.buf.subarray(take);
        this.remaining -= take;
        if (this.remaining > 0) return;
        this.state = 'chunk-crlf';
        continue;
      }
      if (this.state === 'chunk-crlf') {
        if (this.buf.length < 2) return;
        this.buf = this.buf.subarray(2);
        this.state = 'chunk-size';
        continue;
      }
      if (this.state === 'trailers') {
        const idx = this.buf.indexOf('\r\n');
        if (idx < 0) return;
        this.buf = this.buf.subarray(idx + 2);
        if (idx === 0) this.finish();
        continue;
      }
      if (this.state === 'close-body') {
        this.add(this.buf);
        this.buf = Buffer.alloc(0);
        return;
      }
      return;
    }
  }
}

function decodeBody(buf, encoding) {
  const enc = String(encoding ?? '').toLowerCase().trim();
  try {
    if (!enc || enc === 'identity') return buf;
    if (enc === 'gzip' || enc === 'x-gzip') return zlib.gunzipSync(buf, { finishFlush: zlib.constants.Z_SYNC_FLUSH });
    if (enc === 'deflate') return zlib.inflateSync(buf, { finishFlush: zlib.constants.Z_SYNC_FLUSH });
    if (enc === 'br') return zlib.brotliDecompressSync(buf);
    if (enc === 'zstd' && typeof zlib.zstdDecompressSync === 'function') return zlib.zstdDecompressSync(buf);
  } catch {
    return null;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Gemini wire analysis: hashes, counts and booleans only.
// ---------------------------------------------------------------------------
function parseGeminiTarget(target) {
  const url = new URL(target ?? '/', 'http://observer.local');
  const match = url.pathname.match(/^\/(v1beta|v1alpha|v1)\/models\/([^:/]+):([A-Za-z]+)$/);
  return {
    pathname: url.pathname,
    apiVersion: match?.[1] ?? null,
    model: match?.[2] ?? null,
    rpc: match?.[3] ?? null,
    queryKeys: [...url.searchParams.keys()].sort(),
    alt: url.searchParams.get('alt'),
  };
}

const partSignature = (part) => part?.thoughtSignature ?? part?.thought_signature;
const partFunctionCall = (part) => part?.functionCall ?? part?.function_call;
const partFunctionResponse = (part) => part?.functionResponse ?? part?.function_response;

function analyzeGenerateRequest(body) {
  const out = {
    topLevelKeys: Object.keys(body).sort(),
    contents: 0,
    roles: {},
    modelTurns: 0,
    modelTurnsWithFunctionCall: 0,
    modelTurnsWithSignedFunctionCall: 0,
    functionCallParts: 0,
    functionCallPartsWithSignature: 0,
    functionResponseParts: 0,
    thoughtParts: 0,
    signatureHashes: [],
    functionCallSignatureHashes: [],
    systemInstruction: body.systemInstruction !== undefined || body.system_instruction !== undefined,
    functionDeclarations: 0,
    toolConfig: body.toolConfig !== undefined || body.tool_config !== undefined,
    cachedContentPresent: body.cachedContent !== undefined || body.cached_content !== undefined,
    generationConfig: null,
  };
  for (const content of Array.isArray(body.contents) ? body.contents : []) {
    out.contents += 1;
    const role = content?.role ?? 'unset';
    out.roles[role] = (out.roles[role] ?? 0) + 1;
    let hasCall = false;
    let hasSignedCall = false;
    for (const part of Array.isArray(content?.parts) ? content.parts : []) {
      const sig = partSignature(part);
      const call = partFunctionCall(part);
      if (call) {
        out.functionCallParts += 1;
        hasCall = true;
        if (typeof sig === 'string' && sig) {
          out.functionCallPartsWithSignature += 1;
          hasSignedCall = true;
          out.functionCallSignatureHashes.push(shortHash(sig));
        }
      }
      if (partFunctionResponse(part)) out.functionResponseParts += 1;
      if (part?.thought === true) out.thoughtParts += 1;
      if (typeof sig === 'string' && sig) out.signatureHashes.push(shortHash(sig));
    }
    if (role === 'model') {
      out.modelTurns += 1;
      if (hasCall) out.modelTurnsWithFunctionCall += 1;
      if (hasSignedCall) out.modelTurnsWithSignedFunctionCall += 1;
    }
  }
  for (const tool of Array.isArray(body.tools) ? body.tools : []) {
    const decls = tool?.functionDeclarations ?? tool?.function_declarations;
    if (Array.isArray(decls)) out.functionDeclarations += decls.length;
  }
  const gen = body.generationConfig ?? body.generation_config;
  if (gen && typeof gen === 'object') {
    const thinking = gen.thinkingConfig ?? gen.thinking_config;
    out.generationConfig = {
      keys: Object.keys(gen).sort(),
      maxOutputTokens: gen.maxOutputTokens ?? gen.max_output_tokens ?? null,
      thinkingConfig:
        thinking && typeof thinking === 'object'
          ? Object.fromEntries(Object.entries(thinking).filter(([, v]) => v === null || typeof v !== 'object'))
          : null,
    };
  }
  return out;
}

function pickUsage(usage) {
  const keys = [
    'promptTokenCount',
    'cachedContentTokenCount',
    'candidatesTokenCount',
    'thoughtsTokenCount',
    'toolUsePromptTokenCount',
    'totalTokenCount',
  ];
  return Object.fromEntries(keys.filter((k) => typeof usage?.[k] === 'number').map((k) => [k, usage[k]]));
}

function analyzeGenerateResponse(text, contentType) {
  const out = {
    payloads: 0,
    parseErrors: 0,
    responseIds: [],
    modelVersions: [],
    usage: null,
    finishReasons: [],
    functionCalls: [],
    textChars: 0,
    thoughtParts: 0,
    signatureHashes: [],
    functionCallSignatureHashes: [],
    error: null,
  };
  const objects = [];
  if (/event-stream/i.test(contentType ?? '')) {
    for (const block of text.split(/\r?\n\r?\n/)) {
      const data = block
        .split(/\r?\n/)
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).replace(/^ /, ''))
        .join('\n');
      if (!data.trim()) continue;
      try {
        objects.push(JSON.parse(data));
      } catch {
        out.parseErrors += 1;
      }
    }
  } else if (text.trim()) {
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) objects.push(...parsed);
      else objects.push(parsed);
    } catch {
      out.parseErrors += 1;
    }
  }
  for (const obj of objects) {
    out.payloads += 1;
    if (obj?.error) {
      out.error = {
        code: obj.error.code ?? null,
        status: obj.error.status ?? null,
        message: String(obj.error.message ?? '').slice(0, 200),
      };
      continue;
    }
    if (obj?.responseId && !out.responseIds.includes(obj.responseId)) out.responseIds.push(obj.responseId);
    if (obj?.modelVersion && !out.modelVersions.includes(obj.modelVersion)) out.modelVersions.push(obj.modelVersion);
    if (obj?.usageMetadata) out.usage = pickUsage(obj.usageMetadata);
    for (const cand of Array.isArray(obj?.candidates) ? obj.candidates : []) {
      if (cand?.finishReason) out.finishReasons.push(cand.finishReason);
      for (const part of Array.isArray(cand?.content?.parts) ? cand.content.parts : []) {
        const sig = partSignature(part);
        const call = partFunctionCall(part);
        if (call) out.functionCalls.push(String(call.name ?? ''));
        if (part?.thought === true) out.thoughtParts += 1;
        else if (typeof part?.text === 'string') out.textChars += part.text.length;
        if (typeof sig === 'string' && sig) {
          const h = shortHash(sig);
          out.signatureHashes.push(h);
          if (call) out.functionCallSignatureHashes.push(h);
        }
      }
    }
  }
  return out;
}

const SESSION_TURN_HEADER = /session|turn|conversation|thread|request-id|x-goog-request-params|traceparent/i;

function summarizeExchange(rec) {
  const target = parseGeminiTarget(rec.req?.head?.target);
  const reqHeaders = rec.req?.head?.headers ?? {};
  const generation = target.rpc === 'generateContent' || target.rpc === 'streamGenerateContent';
  const summary = {
    connId: rec.connId,
    seq: rec.seq,
    at: new Date(rec.reqHeadAt).toISOString(),
    method: rec.req?.head?.method ?? null,
    httpVersion: rec.req?.head?.version ?? null,
    ...target,
    generation,
    stream: target.rpc === 'streamGenerateContent',
    requestHeaderNames: Object.keys(reqHeaders).sort(),
    sessionTurnHeaderNames: Object.keys(reqHeaders).filter((k) => SESSION_TURN_HEADER.test(k)).sort(),
    auth: {
      xGoogApiKey: reqHeaders['x-goog-api-key'] !== undefined,
      authorizationHeader: reqHeaders.authorization !== undefined,
      keyQueryParam: target.queryKeys.includes('key'),
    },
    requestBytes: rec.req?.body?.length ?? 0,
    requestComplete: rec.req?.complete ?? false,
    blockedByDriverGenerationCap: rec.blocked === true,
    request: null,
    status: rec.resp?.head?.status ?? null,
    responseHttpVersion: rec.resp?.head?.version ?? null,
    responseHeaders: null,
    response: null,
    ttfbMs: rec.respHeadAt ? rec.respHeadAt - rec.reqHeadAt : null,
    durationMs: rec.resp?.endAt ? rec.resp.endAt - rec.reqHeadAt : null,
    responseComplete: rec.resp?.complete ?? false,
  };
  if (rec.req?.body?.length) {
    const decoded = decodeBody(rec.req.body, reqHeaders['content-encoding']);
    try {
      const json = decoded ? JSON.parse(decoded.toString('utf8')) : null;
      if (json && generation) summary.request = analyzeGenerateRequest(json);
      else if (json) summary.request = { topLevelKeys: Object.keys(json).sort() };
    } catch {
      summary.request = { unparsable: true };
    }
  }
  if (rec.resp) {
    const h = rec.resp.head.headers;
    summary.responseHeaders = {
      contentType: h['content-type'] ?? null,
      contentEncoding: h['content-encoding'] ?? null,
      transferEncoding: h['transfer-encoding'] ?? null,
      ccLbRequestId: h['request-id'] ?? null,
    };
    const decoded = decodeBody(rec.resp.body, h['content-encoding']);
    summary.response = decoded
      ? analyzeGenerateResponse(decoded.toString('utf8'), h['content-type'])
      : { undecodable: true, bytes: rec.resp.body.length };
  }
  return summary;
}

// ---------------------------------------------------------------------------
// Labeled transparent observer. Forwards bytes unchanged; only refuses to forward a
// new generation request once the driver's slice generation cap is reached.
// ---------------------------------------------------------------------------
function startObserver({ targetPort, targetHost, logPath, canStartGeneration, onGenerationBlocked }) {
  const exchanges = [];
  let connSeq = 0;
  let reqSeq = 0;
  let active = 0;
  const idleWaiters = [];
  const notifyIdle = () => {
    if (active === 0) while (idleWaiters.length) idleWaiters.shift()();
  };
  const record = (rec) => {
    const summary = summarizeExchange(rec);
    exchanges.push(summary);
    appendPrivate(logPath, JSON.stringify(summary));
  };

  const server = net.createServer((client) => {
    active += 1;
    const connId = ++connSeq;
    const upstream = net.connect({ port: targetPort, host: targetHost });
    const pending = [];
    let closed = false;
    const closeBoth = () => {
      if (closed) return;
      closed = true;
      reqParser.end();
      respParser.end();
      for (const rec of pending.splice(0)) record(rec);
      client.destroy();
      upstream.destroy();
      active -= 1;
      notifyIdle();
    };
    const reqParser = new HttpStreamParser('request', {
      onHead: (head) => {
        const rec = { connId, seq: ++reqSeq, req: { head }, reqHeadAt: head.at };
        const target = parseGeminiTarget(head.target);
        const generation = target.rpc === 'generateContent' || target.rpc === 'streamGenerateContent';
        if (generation && !canStartGeneration()) {
          rec.blocked = true;
          onGenerationBlocked();
        }
        pending.push(rec);
      },
      onMessage: (msg) => {
        const rec = pending.find((r) => r.req.head === msg.head);
        if (rec) Object.assign(rec.req, { body: msg.body, complete: msg.complete });
      },
    });
    const respParser = new HttpStreamParser('response', {
      onHead: (head) => {
        const rec = pending[0];
        if (rec) rec.respHeadAt = head.at;
      },
      onMessage: (msg) => {
        const rec = pending.shift();
        if (!rec) return;
        rec.resp = msg;
        record(rec);
      },
    });
    client.on('data', (chunk) => {
      reqParser.push(chunk);
      if (pending.some((r) => r.blocked)) {
        closeBoth();
        return;
      }
      if (!upstream.write(chunk)) client.pause();
    });
    upstream.on('drain', () => client.resume());
    upstream.on('data', (chunk) => {
      respParser.push(chunk);
      if (!client.write(chunk)) upstream.pause();
    });
    client.on('drain', () => upstream.resume());
    client.on('end', () => upstream.end());
    upstream.on('end', () => {
      respParser.end();
      client.end();
    });
    client.on('close', closeBoth);
    upstream.on('close', closeBoth);
    client.on('error', closeBoth);
    upstream.on('error', closeBoth);
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        port,
        url: `http://127.0.0.1:${port}`,
        exchanges,
        idle: (deadlineMs) =>
          new Promise((res) => {
            if (active === 0) return res(true);
            const timer = setTimeout(() => res(false), deadlineMs);
            idleWaiters.push(() => {
              clearTimeout(timer);
              res(true);
            });
          }),
        close: () => new Promise((res) => server.close(() => res())),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Client process helpers.
// ---------------------------------------------------------------------------
function buildClientEnv(ctx, session, baseUrl) {
  const base = typeof ctx.clientEnvBase === 'function' ? { ...ctx.clientEnvBase() } : {};
  const env = {
    ...base,
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin',
    HOME: session.home,
    TMPDIR: session.tmp,
    XDG_CONFIG_HOME: path.join(session.home, '.config'),
    XDG_CACHE_HOME: path.join(session.home, '.cache'),
    XDG_DATA_HOME: path.join(session.home, '.local', 'share'),
    LANG: 'en_US.UTF-8',
    TERM: 'dumb',
    NO_COLOR: '1',
    GEMINI_API_KEY: ctx.clientKey,
    GOOGLE_GEMINI_BASE_URL: baseUrl,
  };
  for (const key of Object.keys(env)) {
    if (
      /^(GOOGLE_(API_KEY|APPLICATION_CREDENTIALS|GENAI_USE_VERTEXAI|CLOUD_PROJECT|CLOUD_LOCATION|CLOUD_QUOTA_PROJECT|GENAI_.*)|AGY_ADC_AUTH|CLOUDSDK_.*|GCLOUD_.*|OPENAI_.*|ANTHROPIC_.*|CODEX_.*|PI_.*|HTTPS?_PROXY|ALL_PROXY|https?_proxy|all_proxy)$/.test(
        key,
      )
    ) {
      delete env[key];
    }
  }
  env.NO_PROXY = '127.0.0.1,localhost';
  return env;
}

function killGroup(pid, signal) {
  try {
    process.kill(-pid, signal);
  } catch {}
}

function survivingProcesses(marker) {
  const ps = spawnSync('/bin/ps', ['-axo', 'pid=,pgid=,command='], { encoding: 'utf8' });
  return (ps.stdout ?? '')
    .split('\n')
    .filter((line) => line.includes(marker))
    .map((line) => {
      const [pid, pgid, ...cmd] = line.trim().split(/\s+/);
      return { pid: Number(pid), pgid: Number(pgid), exe: path.basename(cmd[0] ?? '') };
    });
}

function parseClientStream(stdoutText, stderrText) {
  const events = [];
  let unparsable = 0;
  for (const line of stdoutText.split('\n')) {
    if (!line.trim()) continue;
    try {
      events.push(JSON.parse(line));
    } catch {
      unparsable += 1;
    }
  }
  const init = events.find((e) => e.event === 'init');
  const result = events.find((e) => e.event === 'result')?.result ?? null;
  const steps = events.filter((e) => e.event === 'step_update').map((e) => e.step_update);
  const toolSteps = steps
    .filter((s) => s.step_type === 'tool' && s.state === 'DONE')
    .map((s) => {
      const params = s.tool_info?.parameters ?? {};
      const brief = {};
      for (const [k, v] of Object.entries(params)) {
        if (typeof v === 'string') brief[k] = v.length > 240 ? `${v.slice(0, 240)}…` : v;
        else if (typeof v === 'number' || typeof v === 'boolean') brief[k] = v;
      }
      const output = typeof s.tool_info?.output === 'string' ? s.tool_info.output : '';
      return {
        stepIndex: s.step_index,
        tool: s.tool_name,
        parameters: brief,
        durationSeconds: s.duration_seconds ?? null,
        outputTail: output.slice(-600),
      };
    });
  const toolStarts = steps.filter((s) => s.step_type === 'tool' && s.state === 'ACTIVE').length;
  const agentResponses = steps
    .filter((s) => s.step_type === 'agent_response' && s.state === 'DONE')
    .map((s) => ({ stepIndex: s.step_index, usage: s.usage ?? null }));
  const stepIndexes = steps.map((s) => s.step_index).filter((n) => Number.isInteger(n));
  const agyError = stderrText
    .split('\n')
    .filter((l) => l.startsWith('AGY_ERROR:'))
    .map((l) => {
      try {
        const e = JSON.parse(l.slice('AGY_ERROR:'.length));
        return { short_error: String(e.short_error ?? '').slice(0, 300), status: e.status, retryable: e.retryable };
      } catch {
        return { short_error: l.slice(0, 300) };
      }
    });
  return {
    unparsableLines: unparsable,
    conversationId: init?.conversation_id ?? result?.conversation_id ?? null,
    initModel: init?.init?.model ?? null,
    initCwd: init?.init?.cwd ?? null,
    initToolCount: Array.isArray(init?.init?.tools) ? init.init.tools.length : null,
    stepIndexRange: stepIndexes.length ? [Math.min(...stepIndexes), Math.max(...stepIndexes)] : null,
    toolStarts,
    toolSteps,
    agentResponses,
    result: result
      ? {
          status: result.status,
          numTurns: result.num_turns ?? null,
          durationSeconds: result.duration_seconds ?? null,
          usage: result.usage ?? null,
          responseTail: String(result.response ?? '').slice(-400),
        }
      : null,
    agyErrors: agyError,
    // agy fell back to its interactive Google OAuth prompt (API-key mode inactive); never completed by the driver.
    interactiveAuthPrompted: /Authentication required\. Please visit the URL to log in/.test(stderrText),
  };
}

function runFixtureTests(work) {
  const env = { PATH: '/usr/bin:/bin', HOME: work, PYTHONDONTWRITEBYTECODE: '1' };
  const tests = spawnSync(PYTHON, ['-m', 'unittest', '-v'], { cwd: work, env, encoding: 'utf8', timeout: 30_000 });
  const combined = `${tests.stdout ?? ''}${tests.stderr ?? ''}`;
  const ran = combined.match(/Ran (\d+) tests?/);
  return {
    command: 'python3 -m unittest -v',
    exitCode: tests.status,
    ran: ran ? Number(ran[1]) : null,
    ok: tests.status === 0 && /\nOK\b/.test(combined),
    tail: combined.slice(-800),
  };
}

function runTurn2Check(work) {
  const env = { PATH: '/usr/bin:/bin', HOME: work, PYTHONDONTWRITEBYTECODE: '1' };
  const res = spawnSync(PYTHON, ['-c', TURN2_CHECK], { cwd: work, env, encoding: 'utf8', timeout: 30_000 });
  return { exitCode: res.status, ok: res.status === 0, tail: `${res.stdout ?? ''}${res.stderr ?? ''}`.slice(-400) };
}

function fixtureDiff(work, file) {
  const res = spawnSync('/usr/bin/diff', ['-ru', '-x', '__pycache__', '-x', '.*', FIXTURE, work], { encoding: 'utf8' });
  writePrivate(file, res.stdout ?? '');
  const changed = [...(res.stdout ?? '').matchAll(/^\+\+\+ (\S+)/gm)].map((m) => path.relative(work, m[1]));
  const onlyIn = [...(res.stdout ?? '').matchAll(/^Only in ([^:]+): (.+)$/gm)].map((m) => path.join(m[1], m[2]));
  return { diffFile: file, changedFiles: changed, onlyIn, exitCode: res.status };
}

// ---------------------------------------------------------------------------
// Event and evidence helpers.
// ---------------------------------------------------------------------------
const EVENT_FIELD =
  /^(request_id|ts_ms|event_kind|source_kind|principal_id|key_id|principal_kind|upstream_id|upstream_name|model|status|.*tokens.*|cost_.*|observed_session_id|thread_id|client_app|session_id_source|request_kind|reasoning_effort|error.*|.*upstream_host.*|.*response_id.*|stream.*|endpoint.*|api_family|transport.*|cache_state)$/;

function pickEvent(event) {
  return Object.fromEntries(Object.entries(event ?? {}).filter(([k, v]) => EVENT_FIELD.test(k) && (v === null || typeof v !== 'object' || k === 'cache_state')));
}

function usageAgreement(exchange, event) {
  const u = exchange?.response?.usage;
  if (!u || !event) return { verdict: 'unavailable' };
  const cached = u.cachedContentTokenCount ?? 0;
  const expected = {
    input_tokens: (u.promptTokenCount ?? 0) - cached,
    output_tokens: u.candidatesTokenCount ?? 0,
    cache_read_input_tokens: cached,
  };
  const mismatches = Object.entries(expected)
    .filter(([k, v]) => (event[k] ?? 0) !== v)
    .map(([k, v]) => ({ field: k, provider: v, ccLb: event[k] ?? null }));
  return { verdict: mismatches.length ? 'mismatch' : 'match', mismatches };
}

function signatureContinuity(agentExchanges) {
  // Every signature delivered to the client in response k must be replayed by the client in the next agent request.
  const pairs = [];
  for (let i = 0; i < agentExchanges.length - 1; i += 1) {
    const emitted = agentExchanges[i].response?.signatureHashes ?? [];
    const emittedCall = agentExchanges[i].response?.functionCallSignatureHashes ?? [];
    if (!emitted.length) continue;
    const next = agentExchanges[i + 1];
    const replayed = new Set(next.request?.signatureHashes ?? []);
    pairs.push({
      fromSeq: agentExchanges[i].seq,
      toSeq: next.seq,
      emitted: emitted.length,
      emittedOnFunctionCall: emittedCall.length,
      replayed: emitted.filter((h) => replayed.has(h)).length,
      missingFunctionCallSignatures: emittedCall.filter((h) => !replayed.has(h)).length,
      missingAny: emitted.filter((h) => !replayed.has(h)).length,
    });
  }
  return pairs;
}

const isAgentExchange = (x) => x.generation && (x.request?.functionDeclarations ?? 0) > 0;

function providerProof(exchanges) {
  const gens = exchanges.filter((x) => x.generation && !x.blockedByDriverGenerationCap);
  return gens.map((x) => ({
    seq: x.seq,
    model: x.model,
    status: x.status,
    providerResponseId: x.response?.responseIds?.[0] ?? null,
    usage: x.response?.usage ?? null,
    nonzeroUsage: (x.response?.usage?.totalTokenCount ?? x.response?.usage?.promptTokenCount ?? 0) > 0,
    error: x.response?.error ?? null,
  }));
}

// ---------------------------------------------------------------------------
// Main.
// ---------------------------------------------------------------------------
async function main() {
  const startedAt = nowIso();
  const ctx = await rt.prepareSlice({
    name: 'agy',
    ports: SLICE.ports,
    provider: 'gemini',
    model: WIRE_MODEL,
    pricedModels: PRICED_MODELS,
    resume: process.env.QA_RESUME === '1',
  });
  const summary = {
    slice: 'agy',
    startedAt,
    scenarios: Object.fromEntries(SLICE.scenarios.map((s) => [s, { verdict: 'NOT_RUN', reasons: [] }])),
    binary: { path: CONTRACT.artifact.path, sha256: ctx.binarySha256, expected: CONTRACT.artifact.expected_sha256 },
    client: null,
    observer: USE_OBSERVER
      ? { label: 'local transparent observer (client->cc-lb hop only); forwards bytes unchanged; hash/boolean capture only' }
      : { label: 'disabled (AGY_OBSERVER=0)' },
    principalId: ctx.principalId,
    upstreamId: ctx.upstreamId,
    model: { wire: WIRE_MODEL, clientLabel: AGY_MODEL_LABEL, ctxModel: ctx.model, pricedModels: ctx.pricedModels },
    limits: ctx.limits ?? null,
    keyId: ctx.keyId ?? null,
    upstream: null,
    turns: [],
    blocked: [],
  };
  // The only admin call the driver can make is one read (GET upstream, skipped on early BLOCKED). Principals/keys
  // exist solely in this slice's private SQLite created by prepareSlice; the driver never writes them.
  const storageUnderSliceRoot = path.resolve(ctx.storagePath).startsWith(`${path.resolve(ctx.root)}${path.sep}`);
  summary.sharedPrincipalsUnmodified = storageUnderSliceRoot;
  summary.principalFresh = storageUnderSliceRoot && Boolean(ctx.principalId);
  summary.sharedPrincipalsProof = {
    storagePath: ctx.storagePath,
    storageUnderSliceRoot,
    resumedSlice: process.env.QA_RESUME === '1',
    driverAdminCallsAllowed: [{ method: 'GET', path: `/admin/v1/upstreams/${ctx.upstreamId}`, write: false }],
    ref: 'prepare.json + resources.jsonl (sqlite_database, qa_sqlite_row_principal entries); parent baseline hash verifies shared stores separately',
  };
  const setScenario = (name, verdict, reasons) => {
    summary.scenarios[name] = { verdict, reasons };
  };

  if (ctx.binarySha256 !== CONTRACT.artifact.expected_sha256) {
    summary.blocked.push('cc-lb binary digest mismatch');
    for (const s of SLICE.scenarios) setScenario(s, 'BLOCKED', ['cc-lb binary digest mismatch']);
    return finish(ctx, summary);
  }
  if (!Array.isArray(ctx.limits?.server) || ctx.limits.server.length === 0 || ctx.blocked?.length) {
    const reason = ctx.blocked?.length
      ? `prepareSlice blocked: ${ctx.blocked.join('; ')}`
      : 'principal generation/USD server limits missing (paid_bounds.enforcement)';
    summary.blocked.push(reason);
    for (const s of SLICE.scenarios) setScenario(s, 'BLOCKED', [reason]);
    return finish(ctx, summary);
  }

  summary.client = {
    binary: AGY_BIN,
    sha256: await sha256File(AGY_BIN),
    flagsSource: 'agy --help',
    maxOutputTokens: 'client-limited: agy exposes no max-output flag; observed generationConfig recorded per request',
  };

  const ctxUpstream = (ctx.upstreams ?? []).find((u) => u.id === ctx.upstreamId) ?? null;
  const upstream = await rt.admin(ctx, `/admin/v1/upstreams/${ctx.upstreamId}`, 'GET');
  const ub = upstream?.body ?? {};
  const configuredBaseUrl = (typeof ub.base_url === 'string' && ub.base_url) || ctxUpstream?.baseUrl || null;
  const configuredHost = configuredBaseUrl ? new URL(configuredBaseUrl).hostname : ctxUpstream?.host ?? null;
  summary.upstream = {
    adminHttpStatus: upstream?.status ?? null,
    id: ctx.upstreamId,
    name: ub.name ?? ctxUpstream?.name ?? null,
    kind: ub.kind ?? ctxUpstream?.kind ?? null,
    configuredHost,
    note: 'configuration only; observed provider proof = per-generation providerResponseId/modelVersion/usageMetadata on the wire + cc-lb event upstream_id + cc-lb log host mentions',
  };
  if (!configuredHost || /^(127\.|localhost$|\[?::1\]?$)/.test(configuredHost)) {
    summary.blocked.push('upstream host missing or local; not a real provider');
    for (const s of SLICE.scenarios) setScenario(s, 'BLOCKED', ['upstream is not a real provider host']);
    return finish(ctx, summary);
  }
  summary.upstream.expectedProviderHost = configuredHost;

  // SETUP readiness (common runtime): one invalid-request Google 400 probe, no generation; not native coverage.
  // Only READY may launch agy; its provider attempt is deducted from the native generation gate.
  let readiness = null;
  let readinessError = null;
  try {
    readiness = await rt.ensureGeminiCliQuotaReady(ctx, {
      model: WIRE_MODEL,
      maxOutputTokens: 65535,
      deadlineMs: 30_000,
      maxAttempts: 8,
    });
  } catch (error) {
    readinessError = {
      code: error?.code ?? null,
      verdict: error?.verdict ?? null,
      detail: rt.redactText(String(error?.detail ?? error?.message ?? error)),
    };
  }
  const setupForwards = Number(readiness?.providerAttempts ?? Number.NaN);
  summary.setupReadiness = {
    label: 'SETUP probe (invalid-request Google 400, no generation); not native CLI coverage',
    verdict: readiness?.verdict ?? readinessError?.verdict ?? null,
    providerAttempts: Number.isFinite(setupForwards) ? setupForwards : null,
    checks: readiness?.checks ?? null,
    error: readinessError,
    proofRefs: ['evidence/gemini-quota-readiness.json', 'evidence/events-quota-probe.jsonl'],
  };
  if (readiness?.verdict !== 'READY' || !Number.isFinite(setupForwards) || setupForwards < 0 || setupForwards >= GEN_CAP) {
    const reason = `BLOCKED: setup quota readiness not proven (${readinessError?.code ?? readiness?.verdict ?? 'no verdict'})`;
    summary.blocked.push(reason);
    const verdict = readinessError?.verdict === 'INVALID' ? 'NOT_RUN' : 'BLOCKED';
    for (const s of SLICE.scenarios) setScenario(s, verdict, [reason]);
    return finish(ctx, summary);
  }
  GEN_CAP -= setupForwards;
  summary.nativeGenerationCap = GEN_CAP;

  // Allocate the first session id whose dirs do not exist yet; earlier (possibly unlaunched) sessions are kept untouched.
  let sessionNo = 1;
  while (
    fs.existsSync(path.join(ctx.root, 'sessions', `s${sessionNo}`)) ||
    fs.existsSync(path.join(ctx.clientHome, `agy-s${sessionNo}`))
  ) {
    sessionNo += 1;
  }
  const session = { id: `s${sessionNo}`, dir: path.join(ctx.root, 'sessions', `s${sessionNo}`) };
  summary.sessionId = session.id;
  if (typeof ctx.clientKey !== 'string' || !ctx.clientKey) {
    const reason = 'local principal client key missing from prepareSlice ctx';
    summary.blocked.push(reason);
    for (const s of SLICE.scenarios) setScenario(s, 'BLOCKED', [reason]);
    return finish(ctx, summary);
  }
  Object.assign(session, {
    // Under ctx.clientHome so finishEvidence's credential scan covers the client HOME.
    home: path.join(ctx.clientHome, `agy-${session.id}`),
    work: path.join(session.dir, 'work'),
    tmp: path.join(session.dir, 'tmp'),
    logs: path.join(session.dir, 'logs'),
  });
  if (fs.existsSync(session.dir) || fs.existsSync(session.home)) {
    throw new Error(`session dir or client home already exists (fresh run required): ${session.dir}`);
  }
  for (const d of [session.home, session.tmp, session.logs]) fs.mkdirSync(d, { recursive: true, mode: 0o700 });
  await rt.recordResource(ctx, {
    kind: 'scratch_directory',
    id: session.dir,
    delete: `After explicit cleanup signal and ownership verification: rm -rf ${session.dir} ${session.home}`,
    note: `agy fixture working copy, client logs, observer hash log; fresh client HOME ${session.home}`,
  });
  fs.cpSync(FIXTURE, session.work, { recursive: true });
  // agy's embedded changelog: API-key mode requires `modelProvider: "gemini"` in settings.json plus GEMINI_API_KEY and
  // GOOGLE_GEMINI_BASE_URL. Without it print mode skips API-key auth and falls back to interactive OAuth (session s2).
  // Non-credential client config only.
  const agySettingsDir = path.join(session.home, '.gemini', 'antigravity-cli');
  fs.mkdirSync(agySettingsDir, { recursive: true, mode: 0o700 });
  writePrivate(path.join(agySettingsDir, 'settings.json'), `${JSON.stringify({ modelProvider: 'gemini' })}\n`);
  summary.clientConfig = {
    file: path.join(agySettingsDir, 'settings.json'),
    content: { modelProvider: 'gemini' },
    source: 'agy binary embedded changelog (GEMINI_API_KEY support note); matches the earlier working QA home',
  };

  let generationCount = 0;
  let capHit = false;
  let currentChild = null;
  const observerLog = path.join(session.logs, 'observer.jsonl');
  let observer = null;
  if (USE_OBSERVER) {
    observer = await startObserver({
      targetPort: Number(new URL(ctx.proxyUrl).port),
      targetHost: new URL(ctx.proxyUrl).hostname,
      logPath: observerLog,
      canStartGeneration: () => {
        if (generationCount >= GEN_CAP) return false;
        generationCount += 1;
        return true;
      },
      onGenerationBlocked: () => {
        capHit = true;
        if (currentChild) killGroup(currentChild.pid, 'SIGTERM');
      },
    });
    summary.observer.port = observer.port;
    summary.observer.logFile = observerLog;
    await rt.recordResource(ctx, {
      kind: 'listener',
      id: `127.0.0.1:${observer.port}`,
      delete: 'Closes when the agy driver process exits; nothing to delete',
      note: 'agy labeled transparent observer (client->cc-lb), bytes forwarded unchanged',
    });
    await rt.recordResource(ctx, {
      kind: 'scratch_file',
      id: observerLog,
      delete: `After explicit cleanup signal: rm -f ${observerLog}`,
      note: '0600 hash/boolean exchange summaries only; no raw headers, bodies or signatures',
    });
  }
  const clientBaseUrl = observer ? observer.url : ctx.proxyUrl;

  let conversationId = null;
  const allEvents = [];
  for (let turn = 1; turn <= Math.min(BOUNDS.turns_per_session, TURN_PROMPTS.length); turn += 1) {
    const budget = await rt.budgetStatus(ctx, allEvents);
    if (budget?.blocked) {
      summary.blocked.push(`turn ${turn} skipped: ${JSON.stringify(budget.reasons ?? [])}`);
      break;
    }
    if (capHit || generationCount >= GEN_CAP) {
      summary.blocked.push(`turn ${turn} skipped: slice generation cap ${GEN_CAP} reached`);
      break;
    }
    if (turn > 1 && !conversationId) {
      summary.blocked.push(`turn ${turn} skipped: no conversation id from turn ${turn - 1}`);
      break;
    }
    const credFiles = [
      ...((await rt.scanCredentialFiles(session.home)) ?? []),
      ...((await rt.scanCredentialFiles(session.work)) ?? []),
    ];
    if (credFiles.length) {
      summary.blocked.push(`turn ${turn} skipped: credential-like files present in client home/work (${credFiles.length})`);
      break;
    }

    const turnRec = await runTurn({
      ctx,
      session,
      turn,
      conversationId,
      observer,
      clientBaseUrl,
      providerHost: summary.upstream.expectedProviderHost,
      setChild: (c) => (currentChild = c),
    });
    turnRec.capHit = capHit;
    summary.turns.push(turnRec);
    allEvents.push(...turnRec.ccLbEvents.rawForBudget);
    delete turnRec.ccLbEvents.rawForBudget;
    if (turn === 1) conversationId = turnRec.client.conversationId;
    if (turnRec.client.result?.status !== 'SUCCESS') break;
  }
  summary.generationRequestsObserved = generationCount;
  summary.budget = await rt.budgetStatus(ctx, allEvents);

  judge(summary, observer);
  if (observer) await observer.close();
  return finish(ctx, summary);
}

async function runTurn({ ctx, session, turn, conversationId, observer, clientBaseUrl, providerHost, setChild }) {
  const stdoutPath = path.join(session.logs, `turn${turn}.stdout.jsonl`);
  const stderrPath = path.join(session.logs, `turn${turn}.stderr`);
  const logPath = path.join(session.logs, `turn${turn}.agy.log`);
  const env = buildClientEnv(ctx, session, clientBaseUrl);
  const args = [
    '--model',
    AGY_MODEL_LABEL,
    '--output-format',
    'stream-json',
    '--dangerously-skip-permissions',
    '--print-timeout',
    `${PRINT_TIMEOUT_S}s`,
    '--log-file',
    logPath,
  ];
  if (conversationId) args.push('--conversation', conversationId);
  args.push('--print', TURN_PROMPTS[turn - 1]);

  const label = `agy-${session.id}-turn${turn}`;
  const baseline = await rt.observabilityBaseline(ctx, label);
  const ccLbLogOffset = fs.existsSync(ctx.logPath) ? fs.statSync(ctx.logPath).size : 0;
  const exchangeStart = observer ? observer.exchanges.length : 0;
  const out = fs.openSync(stdoutPath, 'w', 0o600);
  const err = fs.openSync(stderrPath, 'w', 0o600);
  const startedAt = Date.now();
  const child = spawn(AGY_BIN, args, { cwd: session.work, env, detached: true, stdio: ['ignore', out, err] });
  fs.closeSync(out);
  fs.closeSync(err);
  setChild(child);
  let killedBy = null;
  const timer = setTimeout(() => {
    killedBy = 'driver_wall_deadline';
    killGroup(child.pid, 'SIGTERM');
    setTimeout(() => killGroup(child.pid, 'SIGKILL'), 5000).unref();
  }, RUN_WALL_MS);
  const exit = await new Promise((resolve) => {
    child.on('exit', (code, signal) => resolve({ code, signal }));
    child.on('error', (error) => resolve({ code: null, signal: null, spawnError: error.code ?? String(error) }));
  });
  clearTimeout(timer);
  setChild(null);
  const wallMs = Date.now() - startedAt;
  killGroup(child.pid, 'SIGTERM');
  const observerIdle = observer ? await observer.idle(5000) : null;
  const survivors = survivingProcesses(session.home);
  for (const p of survivors) {
    await rt.recordResource(ctx, {
      kind: 'process',
      id: `pid ${p.pid} (${p.exe})`,
      delete: `After explicit cleanup signal and ownership verification: kill ${p.pid}`,
      note: `agy child process still alive after turn ${turn}`,
    });
  }
  const clientFiles = [stdoutPath, stderrPath, logPath].filter((f) => fs.existsSync(f));
  const clientKeyInClientOutput = clientFiles.some((f) => fs.readFileSync(f).includes(ctx.clientKey));

  const client = parseClientStream(fs.readFileSync(stdoutPath, 'utf8'), fs.readFileSync(stderrPath, 'utf8'));
  let clientVersion = null;
  if (fs.existsSync(logPath)) {
    clientVersion = fs.readFileSync(logPath, 'utf8').match(/Language server version: (\S+)/)?.[1] ?? null;
  }
  const tests = runFixtureTests(session.work);
  const turn2Check = turn === 2 ? runTurn2Check(session.work) : null;
  const diff = fixtureDiff(session.work, path.join(session.logs, `turn${turn}.fixture.diff`));

  const exchanges = observer ? observer.exchanges.slice(exchangeStart) : [];
  const requestIds = exchanges
    .filter((x) => x.generation && x.status !== null)
    .map((x) => x.responseHeaders?.ccLbRequestId)
    .filter(Boolean);
  const waited = await rt.waitForRequestEvents(ctx, {
    baseline,
    expectedRequestIds: requestIds,
    minCount: Math.max(requestIds.length, 1),
    deadlineMs: EVENT_DEADLINE_MS,
    label,
  });
  let ccLbLogProviderHostMentions = null;
  if (fs.existsSync(ctx.logPath) && providerHost) {
    const fd = fs.openSync(ctx.logPath, 'r');
    const size = fs.fstatSync(fd).size;
    const buf = Buffer.alloc(Math.max(size - ccLbLogOffset, 0));
    fs.readSync(fd, buf, 0, buf.length, ccLbLogOffset);
    fs.closeSync(fd);
    ccLbLogProviderHostMentions = buf.toString('utf8').split(providerHost).length - 1;
  }
  const events = (waited?.events ?? []).map(pickEvent);
  const eventById = new Map(events.map((e) => [e.request_id, e]));

  const agent = exchanges.filter(isAgentExchange);
  const perGeneration = exchanges
    .filter((x) => x.generation)
    .map((x) => {
      const ev = eventById.get(x.responseHeaders?.ccLbRequestId) ?? null;
      return {
        seq: x.seq,
        ccLbRequestId: x.responseHeaders?.ccLbRequestId ?? null,
        model: x.model,
        rpc: x.rpc,
        agentLoop: isAgentExchange(x),
        status: x.status,
        providerResponseId: x.response?.responseIds?.[0] ?? null,
        providerModelVersion: x.response?.modelVersions?.[0] ?? null,
        providerUsage: x.response?.usage ?? null,
        stream: x.stream,
        storePresent: x.request?.topLevelKeys?.includes('store') ?? null,
        previousResponseIdPresent: x.request?.topLevelKeys?.some((k) => /previous_?response_?id/i.test(k)) ?? null,
        sessionTurnHeaderNames: x.sessionTurnHeaderNames,
        functionCallPartsWithSignature: x.request?.functionCallPartsWithSignature ?? null,
        event: ev,
        usageAgreement: usageAgreement(x, ev),
        upstreamMatchesCtx: ev ? ev.upstream_id === ctx.upstreamId : null,
      };
    });

  return {
    turn,
    resumedConversationId: conversationId,
    args: args.slice(0, -1).concat(['<prompt>']),
    exit: { ...exit, killedBy, wallMs },
    clientVersion,
    clientKeyInClientOutput,
    ccLbLogProviderHostMentions,
    client,
    fixture: { tests, turn2Check, diff },
    observer: observer
      ? {
          idleAfterExit: observerIdle,
          exchanges: exchanges.length,
          generations: exchanges.filter((x) => x.generation).length,
          blockedByCap: exchanges.filter((x) => x.blockedByDriverGenerationCap).length,
          transport: [...new Set(exchanges.map((x) => `${x.httpVersion} ${x.responseHeaders?.contentType ?? 'no-response'}`))],
          endpoints: [...new Set(exchanges.map((x) => `${x.method} ${x.pathname}${x.alt ? `?alt=${x.alt}` : ''}`))],
          sessionTurnHeaderNames: [...new Set(exchanges.flatMap((x) => x.sessionTurnHeaderNames))],
          authShapes: [...new Set(exchanges.map((x) => JSON.stringify(x.auth)))],
          generationConfigs: [...new Set(agent.map((x) => JSON.stringify(x.request?.generationConfig ?? null)))],
          agentLoopRequests: agent.map((x) => ({
            seq: x.seq,
            status: x.status,
            contents: x.request?.contents,
            modelTurnsWithFunctionCall: x.request?.modelTurnsWithFunctionCall,
            modelTurnsWithSignedFunctionCall: x.request?.modelTurnsWithSignedFunctionCall,
            functionCallParts: x.request?.functionCallParts,
            functionCallPartsWithSignature: x.request?.functionCallPartsWithSignature,
            replayedSignatureHashes: x.request?.signatureHashes?.length ?? 0,
            responseFunctionCalls: x.response?.functionCalls ?? [],
            responseSignatureHashes: x.response?.signatureHashes?.length ?? 0,
            finishReasons: x.response?.finishReasons ?? [],
            error: x.response?.error ?? null,
          })),
          signatureContinuity: signatureContinuity(agent),
          providerProof: providerProof(exchanges),
        }
      : null,
    perGeneration,
    ccLbEvents: {
      label,
      timedOut: waited?.timedOut ?? null,
      count: events.length,
      expectedRequestIds: requestIds.length,
      missingRequestIds: (waited?.missing ?? []).length,
      dropIncreases: waited?.dropIncreases ?? [],
      criticalDropIncreases: waited?.criticalDropIncreases ?? [],
      invalidated: waited?.invalidated ?? null,
      rawForBudget: waited?.events ?? [],
    },
    survivors,
    _agentSignatures: {
      firstRequest: agent[0]?.request?.signatureHashes ?? [],
      emittedFunctionCall: agent.flatMap((x) => x.response?.functionCallSignatureHashes ?? []),
      emittedAll: agent.flatMap((x) => x.response?.signatureHashes ?? []),
    },
  };
}

function judge(summary, observer) {
  const [t1, t2] = summary.turns;
  const codingName = SLICE.scenarios[0];
  const sigName = SLICE.scenarios[1];
  const resumeName = SLICE.scenarios[2];
  const blockedReasons = summary.blocked.slice();

  // Entries prefixed "BLOCKED:" are prerequisite/budget conditions, not defects.
  const turnFailures = (t) => {
    const r = [];
    if (!t) return ['turn not executed'];
    if (t.exit.spawnError) r.push(`client spawn error ${t.exit.spawnError}`);
    if (t.client.interactiveAuthPrompted)
      r.push('BLOCKED: client setup: agy fell back to interactive Google OAuth (API-key mode not active); no OAuth completed');
    if (t.exit.killedBy === 'driver_wall_deadline') r.push(`BLOCKED: client exceeded ${RUN_WALL_MS}ms wall bound`);
    else if (t.exit.killedBy) r.push(`BLOCKED: client stopped by ${t.exit.killedBy}`);
    if (t.clientKeyInClientOutput) r.push('local principal key echoed in client stdout/stderr/log');
    if (t.client.result?.status !== 'SUCCESS') r.push(`client result ${t.client.result?.status ?? 'missing'}; agyErrors=${t.client.agyErrors.length}`);
    if (!t.fixture.tests.ok) r.push('independent fixture unittest run failed');
    for (const g of t.perGeneration) {
      const msg = String(g.event?.error ?? '') + JSON.stringify(t.observer?.providerProof?.find((p) => p.seq === g.seq)?.error ?? '');
      if (g.status === 429 && !g.providerResponseId) r.push(`BLOCKED: generation seq ${g.seq} HTTP 429 (principal budget/cold allowance or provider quota)`);
      else if (g.status === 404 && /model/i.test(msg)) r.push(`BLOCKED: generation seq ${g.seq} model ${g.model} unavailable at provider`);
      else if (g.status !== 200) r.push(`generation seq ${g.seq} HTTP ${g.status}`);
      else {
        if (!g.providerResponseId) r.push(`generation seq ${g.seq} missing provider responseId`);
        const total = g.providerUsage?.totalTokenCount ?? g.providerUsage?.promptTokenCount ?? 0;
        if (!(total > 0)) r.push(`generation seq ${g.seq} provider usage missing/zero`);
      }
      if (g.agentLoop && g.model !== WIRE_MODEL) r.push(`agent request seq ${g.seq} used wire model ${g.model}, expected ${WIRE_MODEL}`);
      if (!summary.model.pricedModels?.includes(g.model)) r.push(`BLOCKED: wire model ${g.model} not in priced catalog (cost proof unavailable)`);
      if (!g.event) r.push(`generation seq ${g.seq} has no committed cc-lb event`);
      else if (g.event.status !== g.status) r.push(`generation seq ${g.seq} event status ${g.event.status} != wire ${g.status}`);
      if (g.upstreamMatchesCtx === false) r.push(`generation seq ${g.seq} routed to unexpected upstream`);
      if (g.usageAgreement.verdict === 'mismatch') r.push(`generation seq ${g.seq} cc-lb usage mismatch ${JSON.stringify(g.usageAgreement.mismatches)}`);
    }
    if (t.perGeneration.length && !(t.ccLbLogProviderHostMentions > 0))
      r.push('note: cc-lb log shows no provider host mention for this turn (host proof relies on responseId/usage/event upstream_id)');
    if (t.ccLbEvents.timedOut) r.push(`request events incomplete within ${EVENT_DEADLINE_MS}ms (missing ${t.ccLbEvents.missingRequestIds})`);
    if (t.ccLbEvents.invalidated) r.push(`loss counters increased: ${JSON.stringify(t.ccLbEvents.dropIncreases)}`);
    return r;
  };
  const verdictOf = (reasons) => {
    const real = reasons.filter((x) => !x.startsWith('note:'));
    if (!real.length) return 'PASS';
    return real.some((x) => x.startsWith('BLOCKED:')) ? 'BLOCKED' : 'FAIL';
  };
  const toolEvidence = (t) => {
    const names = t.client.toolSteps.map((s) => s.tool);
    const ranTests = t.client.toolSteps.some((s) => /unittest/.test(String(s.parameters.CommandLine ?? '')));
    const edited = t.fixture.diff.changedFiles.length > 0;
    return { names, ranTests, edited, read: names.some((n) => /view|read|find|grep|list/i.test(n)) };
  };

  // Scenario 1: actual coding tools (turn 1 tool loop).
  if (!t1) setVerdict(summary, codingName, blockedReasons.length ? 'BLOCKED' : 'NOT_RUN', blockedReasons);
  else {
    const fails = turnFailures(t1);
    const tools = toolEvidence(t1);
    if (!tools.read) fails.push('no file-inspection tool observed');
    if (!tools.ranTests) fails.push('client did not run the fixture tests via a tool');
    if (!tools.edited) fails.push('no fixture change observed');
    if (t1.fixture.diff.changedFiles.includes('test_pricing.py')) fails.push('client edited the tests in turn 1 despite instruction');
    if (!t1.perGeneration.length) fails.push(observer ? 'no generation requests observed' : 'observer disabled: wire evidence unavailable');
    const verdict = observer ? verdictOf(fails) : 'NOT_RUN';
    setVerdict(summary, codingName, verdict, verdict === 'PASS' ? [`tools=${tools.names.join(',')}`, ...fails] : fails);
  }

  // Scenario 2: thoughtSignature continuity (client->cc-lb hop observed; cc-lb->upstream hop not observable).
  if (!observer) setVerdict(summary, sigName, 'NOT_RUN', ['observer disabled']);
  else if (!t1) setVerdict(summary, sigName, blockedReasons.length ? 'BLOCKED' : 'NOT_RUN', blockedReasons);
  else {
    const reasons = [];
    const pairs = summary.turns.flatMap((t) => t.observer.signatureContinuity);
    const emittedCall = summary.turns.reduce((n, t) => n + t._agentSignatures.emittedFunctionCall.length, 0);
    // Only provider-completed agent requests (HTTP 200 + provider responseId) can exercise signature continuity;
    // locally rejected (e.g. cc-lb limit 429) or dropped requests never reached a provider completion.
    const agentRequests = summary.turns.reduce(
      (n, t) => n + t.perGeneration.filter((g) => g.agentLoop && g.status === 200 && g.providerResponseId).length,
      0,
    );
    if (!emittedCall) reasons.push('provider emitted no functionCall thoughtSignature on the observed agent loop');
    for (const p of pairs) {
      if (p.missingFunctionCallSignatures) reasons.push(`client did not replay ${p.missingFunctionCallSignatures} functionCall signature(s) from seq ${p.fromSeq} in seq ${p.toSeq}`);
    }
    const replayedCalls = summary.turns.flatMap((t) => t.observer.agentLoopRequests).filter((r) => r.functionCallPartsWithSignature > 0).length;
    if (emittedCall && !replayedCalls) reasons.push('no follow-up request replayed a signed functionCall part');
    const codingVerdict = summary.scenarios[codingName]?.verdict;
    const verdict = !agentRequests
      ? codingVerdict === 'BLOCKED'
        ? 'BLOCKED'
        : 'NOT_RUN'
      : reasons.length
        ? 'FAIL'
        : 'PASS';
    if (!agentRequests)
      reasons.splice(0, reasons.length, 'no provider-completed agent generation (HTTP 200 + responseId) observed; signature continuity not exercised (not a proxy finding)');
    setVerdict(summary, sigName, verdict, [
      ...reasons,
      `pairs=${pairs.length} emittedFunctionCallSignatures=${emittedCall} requestsReplayingSignedFunctionCall=${replayedCalls}`,
      'cc-lb->provider hop: not observable (cc-lb records no signature evidence); provider accepted replay is shown by HTTP 200 + responseId on follow-up requests',
    ]);
  }

  // Scenario 3: session continue by exact conversation id.
  if (!t2) setVerdict(summary, resumeName, blockedReasons.length ? 'BLOCKED' : 'NOT_RUN', blockedReasons.length ? blockedReasons : ['turn 2 not executed']);
  else {
    const fails = turnFailures(t2);
    if (t2.client.conversationId !== t1.client.conversationId) fails.push('resumed conversation id differs from turn 1');
    if (t1.client.stepIndexRange && t2.client.stepIndexRange && t2.client.stepIndexRange[0] <= t1.client.stepIndexRange[1])
      fails.push('turn 2 step indexes do not continue turn 1 history');
    if (!t2.fixture.turn2Check?.ok) fails.push('driver order_total acceptance check failed');
    if (observer) {
      const firstReplay = new Set(t2._agentSignatures.firstRequest);
      const t1Calls = t1._agentSignatures.emittedFunctionCall;
      const missing = t1Calls.filter((h) => !firstReplay.has(h)).length;
      if (!t1Calls.length) fails.push('turn 1 emitted no functionCall signatures to carry across resume');
      else if (missing) fails.push(`first turn-2 request missing ${missing}/${t1Calls.length} turn-1 functionCall signatures (client->cc-lb hop)`);
    }
    const verdict = verdictOf(fails);
    setVerdict(summary, resumeName, verdict, verdict === 'PASS' ? [`conversation ${t2.client.conversationId} resumed by id`, ...fails] : fails);
  }
  for (const t of summary.turns) {
    t.signatureCounts = {
      firstRequestReplayed: t._agentSignatures.firstRequest.length,
      emittedFunctionCall: t._agentSignatures.emittedFunctionCall.length,
      emittedAll: t._agentSignatures.emittedAll.length,
    };
    delete t._agentSignatures;
  }
}

function setVerdict(summary, name, verdict, reasons) {
  summary.scenarios[name] = { verdict, reasons };
}

async function finish(ctx, summary) {
  summary.finishedAt = nowIso();
  const { path: summaryFile, summary: doc } = await rt.finishEvidence(ctx, summary);
  const verdicts = Object.fromEntries(Object.entries(doc.scenarios).map(([k, v]) => [k, v.verdict]));
  console.log(JSON.stringify({ slice: 'agy', verdicts, summaryFile }));
}

main().catch((error) => {
  const message = typeof rt.redactText === 'function' ? rt.redactText(String(error?.message ?? error)) : String(error?.message ?? error);
  const blocked = message.startsWith('BLOCKED') || (rt.CredentialError && error instanceof rt.CredentialError);
  const verdicts = Object.fromEntries(SLICE.scenarios.map((s) => [s, blocked ? 'BLOCKED' : 'NOT_RUN']));
  console.log(JSON.stringify({ slice: 'agy', verdicts, driverError: message }));
  process.exitCode = blocked ? 2 : 1;
});
